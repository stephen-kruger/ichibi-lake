/**
 * Unit tests for bulk import helpers: object-storage source validation and
 * the allow-list (src/storage.js), format detection (src/ingest.js,
 * src/console/data.js) and the import secret. No database or running gateway
 * is needed; test/test-import.js covers the endpoint end to end.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { parseImportSource, importAllowList, isImportSourceAllowed, buildImportSecretSql, S3_IMPORT_SECRET_NAME } from '../src/storage.js';
import { contentTypeFormat, readSourceRequest } from '../src/ingest.js';
import { detectImportFormat, importReader } from '../src/console/data.js';

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}

const rejects400 = (raw) => assert.throws(() => parseImportSource(raw), (e) => e.statusCode === 400, `expected 400 for ${JSON.stringify(raw)}`);

async function run() {
    console.log('\n▸ import: object-storage sources');
    await test('s3:// files and globs are accepted; the literal prefix stops at the first glob character', () => {
        assert.deepEqual(parseImportSource('s3://my-bucket/raw/2026/*.parquet'),
            { source: 's3://my-bucket/raw/2026/*.parquet', bucket: 'my-bucket', key: 'raw/2026/*.parquet', literalPrefix: 's3://my-bucket/raw/2026/' });
        assert.equal(parseImportSource(' s3://b.x/a/file.csv ').literalPrefix, 's3://b.x/a/file.csv');
        assert.equal(parseImportSource('s3://bkt/a/**/part-[0-9].json').literalPrefix, 's3://bkt/a/');
    });
    await test('local paths and other URL schemes are rejected (no local file read, no SSRF)', () => {
        for (const raw of ['/etc/passwd', 'data_files/x.parquet', 'file:///etc/passwd', 'http://169.254.169.254/latest/meta-data',
            'https://example.com/x.csv', 'gs://b/x.parquet', 'r2://b/x', 'S3://b/x', 's3:/b/x', 's3://b', 's3:///x', 's3://Bad_Bucket/x',
            '', null, 42, ['s3://b/x']]) rejects400(raw);
    });
    await test('traversal, empty segments, directories, quotes and control characters are rejected', () => {
        for (const raw of ['s3://bkt/raw/../secret/x.csv', 's3://bkt/./x', 's3://bkt//x', 's3://bkt/a//b', 's3://bkt/raw/',
            "s3://bkt/a'b.csv", 's3://bkt/a"b', 's3://bkt/a\\b', 's3://bkt/a\nb', `s3://bkt/${'x'.repeat(1100)}`]) rejects400(raw);
    });

    console.log('\n▸ import: allow-list');
    const env = { IMPORT_ALLOWED_SOURCES: 's3://landing/raw, s3://shared, ftp://nope, s3://lakebkt', DUCKLAKE_DATA_PATH: 's3://lakebkt/lake/', S3_ACCESS_KEY: 'k', S3_SECRET_KEY: 's' };
    const allowed = (src, e = env) => isImportSourceAllowed(parseImportSource(src), e);
    await test('entries are s3:// directory prefixes; invalid entries are ignored', () => {
        assert.deepEqual(importAllowList(env), ['s3://landing/raw/', 's3://shared/', 's3://lakebkt/']);
        assert.deepEqual(importAllowList({}), []);
    });
    await test('a source is allowed only when everything it can match is under an entry', () => {
        assert.equal(allowed('s3://landing/raw/a.csv'), true);
        assert.equal(allowed('s3://landing/raw/2026/*.parquet'), true);
        assert.equal(allowed('s3://landing/rawdata/a.csv'), false, 'prefix boundary');
        assert.equal(allowed('s3://landing/r*/a.csv'), false, 'glob before the boundary');
        assert.equal(allowed('s3://landing/other.csv'), false);
        assert.equal(allowed('s3://shared/x/y.json'), true);
        assert.equal(allowed('s3://landing/raw/a.csv', {}), false, 'empty allow-list');
    });
    await test('the lake\'s own data path is never allow-listed (would bypass table ACLs)', () => {
        assert.equal(allowed('s3://lakebkt/lake/main/t/x.parquet'), false);
        assert.equal(allowed('s3://lakebkt/*'), false, 'glob covering the lake prefix');
        assert.equal(allowed('s3://lakebkt/exports/x.parquet'), true);
    });

    console.log('\n▸ import: secret and readers');
    await test('the import secret reuses the S3_* settings with an s3:// scope and is created once per instance', () => {
        const sql = buildImportSecretSql({ S3_ENDPOINT: 'minio:9000', S3_ACCESS_KEY: 'k', S3_SECRET_KEY: "it's", S3_USE_SSL: 'false' });
        assert.match(sql, new RegExp(`^CREATE TEMPORARY SECRET IF NOT EXISTS ${S3_IMPORT_SECRET_NAME} \\(TYPE s3,`));
        assert.match(sql, /SECRET 'it''s'/);
        assert.match(sql, /ENDPOINT 'minio:9000'/);
        assert.match(sql, /URL_STYLE 'path'/);
        assert.match(sql, /SCOPE 's3:\/\/'\)$/);
        assert.match(buildImportSecretSql({}), /PROVIDER credential_chain/);
    });
    await test('readers quote the path and combine remote globs by name', () => {
        assert.equal(importReader('csv', "/tmp/a'b"), "read_csv('/tmp/a''b', auto_detect = true)");
        assert.equal(importReader('parquet', 's3://b/*.parquet', { remote: true }), "read_parquet('s3://b/*.parquet', union_by_name = true)");
        assert.equal(importReader('json', '/tmp/x'), "read_json_auto('/tmp/x')");
    });

    console.log('\n▸ import: formats');
    await test('Content-Type maps to a format; unknown types imply none', () => {
        assert.equal(contentTypeFormat('text/csv; charset=utf-8'), 'csv');
        assert.equal(contentTypeFormat('application/vnd.apache.parquet'), 'parquet');
        assert.equal(contentTypeFormat('application/x-ndjson'), 'json');
        assert.equal(contentTypeFormat('Application/JSON'), 'json');
        assert.equal(contentTypeFormat('application/octet-stream'), '');
        assert.equal(contentTypeFormat(undefined), '');
    });
    await test('format names accept ndjson / jsonl / tsv aliases', () => {
        assert.equal(detectImportFormat('ndjson'), 'json');
        assert.equal(detectImportFormat('JSONL'), 'json');
        assert.equal(detectImportFormat('tsv'), 'csv');
        assert.throws(() => detectImportFormat('xlsx'), (e) => e.statusCode === 400);
    });
    await test('only a small JSON object with a "source" key is a load-from-object-storage request', async () => {
        const dir = await fs.mkdtemp(path.join(os.tmpdir(), 'ichibi-ingest-test-'));
        const write = async (name, text) => { const f = path.join(dir, name); await fs.writeFile(f, text); return [f, Buffer.byteLength(text)]; };
        try {
            assert.deepEqual(await readSourceRequest(...await write('a', ' {"source":"s3://b/x.csv","mode":"append"}\n')), { source: 's3://b/x.csv', mode: 'append' });
            assert.equal(await readSourceRequest(...await write('b', '[{"source":"s3://b/x"}]')), null, 'JSON array is data');
            assert.equal(await readSourceRequest(...await write('c', '{"source":"a"}\n{"source":"b"}\n')), null, 'NDJSON is data');
            assert.equal(await readSourceRequest(...await write('d', '{"id":1}')), null, 'a one-row object without "source" is data');
            const [big] = await write('e', `{"source":"s3://b/x","pad":"${'x'.repeat(70 * 1024)}"}`);
            assert.equal(await readSourceRequest(big, 70 * 1024 + 30), null, 'large bodies are data');
        } finally {
            await fs.rm(dir, { recursive: true, force: true });
        }
    });

    console.log(`\n  passed: ${passed}\n  failed: ${failed}`);
    if (failed) process.exit(1);
}

run();
