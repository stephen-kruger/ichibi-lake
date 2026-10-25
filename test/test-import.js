/**
 * test/test-import.js
 *
 * Integration tests for bulk ingestion. Runs against a live gateway (default
 * http://localhost:3333, see _env.js).
 *
 *   node test/test-import.js
 *
 * What is tested:
 *   - POST /tables/:tableName/import with CSV, NDJSON, a JSON array and
 *     Parquet bodies; format from ?format= or Content-Type
 *   - modes: auto, create (409 when the table exists, caller becomes the ACL
 *     owner), append (schema evolution: new columns added), replace
 *   - RBAC: reader 403, writer allowed, missing key 401, system tables 404,
 *     invalid names 400
 *   - IMPORT_MAX_MB: declared Content-Length over the limit is 413; with
 *     IMPORT_MAX_MB set in this process's environment (<= 64) a streamed
 *     (chunked) body over the limit is 413 too
 *   - load from object storage ({"source": "s3://..."}): local paths and
 *     other URL schemes rejected, non-allow-listed prefixes 403 for writers.
 *     With TEST_S3_ENDPOINT set (a host-reachable S3/MinIO the gateway also
 *     uses), objects are uploaded and imported: allow-listed glob by a writer,
 *     non-allow-listed prefix by a superuser only.
 *       TEST_S3_ENDPOINT     e.g. http://localhost:9000
 *       TEST_S3_ACCESS_KEY / TEST_S3_SECRET_KEY / TEST_S3_REGION (us-east-1)
 *       TEST_S3_BUCKET       default ichibi-lake
 *       TEST_S3_ALLOWED      allow-listed prefix (must be in the gateway's
 *                            IMPORT_ALLOWED_SOURCES), default
 *                            s3://<bucket>/import-allowed/
 *   - Idempotency-Key on /upload, /kafka-sink and the import endpoint:
 *     retries replay the first result and insert once, a different body is
 *     409, keys are per API key, a failed request releases its key, and
 *     concurrent requests with one key insert once
 */

import http from 'http';
import { createHash, createHmac } from 'crypto';
import { API_KEY, BASE_URL } from './_env.js';

const RUN = Date.now();
const T = `import_test_${RUN}`;
const WRITER_KEY = `sk-test-import-writer-${RUN}`;
const READER_KEY = `sk-test-import-reader-${RUN}`;

const S3 = {
    endpoint: (process.env.TEST_S3_ENDPOINT || '').replace(/\/+$/, ''),
    accessKey: process.env.TEST_S3_ACCESS_KEY || '',
    secretKey: process.env.TEST_S3_SECRET_KEY || '',
    region: process.env.TEST_S3_REGION || 'us-east-1',
    bucket: process.env.TEST_S3_BUCKET || 'ichibi-lake',
};
S3.allowed = (process.env.TEST_S3_ALLOWED || `s3://${S3.bucket}/import-allowed/`).replace(/\/?$/, '/');

// ---------------------------------------------------------------------------
// Minimal test runner (same output style as the other suites)
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;
let skipped = 0;
const failures = [];

async function describe(name, fn) {
    console.log(`\n${name}`);
    await fn();
}

async function it(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`  ✓ ${name}`);
    } catch (err) {
        failed++;
        failures.push({ name, err });
        console.log(`  ✗ ${name}\n      ${err.message}`);
    }
}

function skip(name, why) {
    skipped++;
    console.log(`  - ${name} (skipped: ${why})`);
}

function assert(cond, msg) { if (!cond) throw new Error(msg || 'assertion failed'); }
function assertEqual(actual, expected, msg) {
    if (actual !== expected) throw new Error(`${msg || 'values differ'}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async function api(method, urlPath, body, key = API_KEY, headers = {}) {
    const res = await fetch(`${BASE_URL}${urlPath}`, {
        method,
        headers: { 'Content-Type': 'application/json', ...(key ? { 'x-api-key': key } : {}), ...headers },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json = null;
    try { json = await res.json(); } catch (_) { /* non-JSON */ }
    return { status: res.status, json, headers: res.headers };
}

/** POST raw bytes to the import endpoint. */
async function importRaw(table, body, { contentType, query = '', key = API_KEY, headers = {} } = {}) {
    const h = { ...(key ? { 'x-api-key': key } : {}), ...headers };
    if (contentType) h['Content-Type'] = contentType;
    const res = await fetch(`${BASE_URL}/tables/${table}/import${query ? `?${query}` : ''}`, { method: 'POST', headers: h, body });
    let json = null;
    try { json = await res.json(); } catch (_) { /* non-JSON */ }
    return { status: res.status, json, headers: res.headers };
}

const importSource = (table, body, key = API_KEY, headers = {}) =>
    importRaw(table, JSON.stringify(body), { contentType: 'application/json', key, headers });

async function sql(text) {
    const r = await api('POST', '/query', { sql: text });
    if (r.status !== 200) throw new Error(`query failed (${r.status}): ${JSON.stringify(r.json)}`);
    return r.json.rows;
}

async function count(table) {
    return Number((await sql(`SELECT count(*) AS n FROM "${table}"`))[0].n);
}

async function columns(table) {
    const r = await api('GET', `/tables/${table}/schema`);
    if (r.status !== 200) throw new Error(`schema failed (${r.status}): ${JSON.stringify(r.json)}`);
    return r.json.schema.map(c => c.column_name);
}

async function tableExists(table) {
    return (await sql(`SELECT count(*) AS n FROM information_schema.tables WHERE table_name = '${table}'`))[0].n > 0;
}

/**
 * POST with a declared Content-Length but send only a little of it, to check
 * that the gateway refuses before reading (fetch cannot lie about the length).
 */
function postDeclaredLength(urlPath, declared) {
    return new Promise((resolve, reject) => {
        const req = http.request(`${BASE_URL}${urlPath}`, {
            method: 'POST',
            headers: { 'x-api-key': API_KEY, 'Content-Type': 'text/csv', 'Content-Length': String(declared) },
        }, (res) => {
            let text = '';
            res.on('data', (c) => { text += c; });
            res.on('end', () => resolve({ status: res.statusCode, text }));
        });
        req.on('error', reject);
        req.write('id\n1\n');
    });
}

/** Chunked upload of `bytes` CSV bytes (no Content-Length). */
async function postChunked(table, bytes) {
    const line = Buffer.from('1234567890123456789012345678901234567890123456789012345678901234\n');
    let sent = 0;
    const stream = new ReadableStream({
        pull(controller) {
            if (sent === 0) { controller.enqueue(new TextEncoder().encode('id\n')); sent += 3; return; }
            if (sent >= bytes) { controller.close(); return; }
            const chunk = Buffer.alloc(1024 * 1024);
            for (let o = 0; o < chunk.length; o += line.length) line.copy(chunk, o);
            sent += chunk.length;
            controller.enqueue(chunk);
        },
    });
    try {
        const res = await fetch(`${BASE_URL}/tables/${table}/import`, {
            method: 'POST', headers: { 'x-api-key': API_KEY, 'Content-Type': 'text/csv' }, body: stream, duplex: 'half',
        });
        return { status: res.status, json: await res.json().catch(() => null) };
    } catch (err) {
        // The gateway may close the connection while the body is still being sent.
        return { status: 0, error: err };
    }
}

// --- S3 (MinIO) uploads with a minimal SigV4 signer -------------------------------

const sha256 = (data) => createHash('sha256').update(data).digest('hex');
const hmac = (key, data) => createHmac('sha256', key).update(data).digest();

async function s3Put(key, body) {
    const url = new URL(`${S3.endpoint}/${S3.bucket}/${key.split('/').map(encodeURIComponent).join('/')}`);
    const amzDate = new Date().toISOString().replace(/[:-]|\.\d{3}/g, '');
    const date = amzDate.slice(0, 8);
    const payloadHash = sha256(body);
    const signed = { host: url.host, 'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate };
    const names = Object.keys(signed).sort();
    const canonical = ['PUT', url.pathname, '', names.map(n => `${n}:${signed[n]}\n`).join(''), names.join(';'), payloadHash].join('\n');
    const scope = `${date}/${S3.region}/s3/aws4_request`;
    const toSign = ['AWS4-HMAC-SHA256', amzDate, scope, sha256(canonical)].join('\n');
    let k = hmac(`AWS4${S3.secretKey}`, date);
    for (const part of [S3.region, 's3', 'aws4_request']) k = hmac(k, part);
    const signature = createHmac('sha256', k).update(toSign).digest('hex');
    const res = await fetch(url, {
        method: 'PUT',
        headers: {
            'x-amz-content-sha256': payloadHash, 'x-amz-date': amzDate,
            Authorization: `AWS4-HMAC-SHA256 Credential=${S3.accessKey}/${scope}, SignedHeaders=${names.join(';')}, Signature=${signature}`,
        },
        body,
    });
    if (!res.ok) throw new Error(`S3 PUT ${key} failed: ${res.status} ${await res.text()}`);
}

// ---------------------------------------------------------------------------
// Setup / teardown
// ---------------------------------------------------------------------------

async function waitReady() {
    for (let i = 0; i < 90; i++) {
        try {
            const res = await fetch(`${BASE_URL}/health/ready`);
            if (res.status === 200) return;
        } catch (_) { /* not up yet */ }
        await new Promise(r => setTimeout(r, 1000));
    }
    throw new Error(`gateway at ${BASE_URL} did not become ready`);
}

async function setup() {
    await waitReady();
    for (const [apiKey, roles] of [[WRITER_KEY, ['writer']], [READER_KEY, ['reader']]]) {
        const r = await api('POST', '/admin/users', { apiKey, name: `import test ${roles[0]}`, roles });
        if (!r.json?.success) throw new Error(`could not create ${roles[0]} user: ${JSON.stringify(r.json)}`);
    }
}

async function teardown() {
    for (const key of [WRITER_KEY, READER_KEY]) {
        await api('DELETE', `/admin/users/${encodeURIComponent(key)}`).catch(() => {});
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function run() {
    console.log(`--- Bulk import integration tests against ${BASE_URL} ---`);
    await setup();

    const csv = 'id,name,score\n1,alice,10\n2,bob,20\n3,carol,30\n';

    await describe('Formats and modes', async () => {
        await it('CSV body (Content-Type text/csv) creates the table; the writer becomes its ACL owner', async () => {
            const r = await importRaw(T, csv, { contentType: 'text/csv', key: WRITER_KEY });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.created, true, 'created');
            assertEqual(r.json.mode, 'create', 'auto mode resolved to create');
            assertEqual(Number(r.json.inserted), 3, 'inserted');
            assertEqual(await count(T), 3, 'row count');
            const acl = await sql(`SELECT owner_keys FROM _table_acls WHERE table_name = '${T}'`);
            assertEqual(acl.length, 1, 'one ACL row');
            assert(JSON.parse(acl[0].owner_keys).includes(WRITER_KEY), `writer owns the table (${acl[0].owner_keys})`);
        });

        await it('NDJSON append (?format=ndjson) evolves the schema with new columns', async () => {
            const nd = '{"id":4,"name":"dave","score":40,"team":"red"}\n{"id":5,"name":"erin","score":50,"team":"blue"}\n';
            const r = await importRaw(T, nd, { contentType: 'application/octet-stream', query: 'format=ndjson&mode=append', key: WRITER_KEY });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.mode, 'append', 'mode');
            assertEqual(Number(r.json.inserted), 2, 'inserted');
            assert(r.json.added.includes('team'), `team added (${JSON.stringify(r.json.added)})`);
            assert((await columns(T)).includes('team'), 'team column exists');
            assertEqual(await count(T), 5, 'row count');
        });

        await it('JSON array body (Content-Type application/json) appends in auto mode', async () => {
            const arr = JSON.stringify([{ id: 6, name: 'frank', score: 60 }, { id: 7, name: 'grace', score: 70, team: 'red' }]);
            const r = await importRaw(T, arr, { contentType: 'application/json', key: WRITER_KEY });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.mode, 'append', 'auto mode resolved to append');
            assertEqual(await count(T), 7, 'row count');
            const red = await sql(`SELECT count(*) AS n FROM "${T}" WHERE team = 'red'`);
            assertEqual(Number(red[0].n), 2, 'values land in the evolved column');
        });

        await it('Parquet body (application/vnd.apache.parquet) round-trips an export', async () => {
            const exp = await fetch(`${BASE_URL}/tables/${T}/export?format=parquet`, { headers: { 'x-api-key': API_KEY } });
            assertEqual(exp.status, 200, 'export status');
            const buf = Buffer.from(await exp.arrayBuffer());
            const r = await importRaw(`${T}_pq`, buf, { contentType: 'application/vnd.apache.parquet', query: 'mode=create' });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(Number(r.json.inserted), 7, 'inserted');
            assertEqual(await count(`${T}_pq`), 7, 'row count');
        });

        await it('mode=create on an existing table is 409 and inserts nothing', async () => {
            const r = await importRaw(T, csv, { contentType: 'text/csv', query: 'mode=create', key: WRITER_KEY });
            assertEqual(r.status, 409, 'status');
            assertEqual(await count(T), 7, 'row count unchanged');
        });

        await it('mode=append on a missing table is 404', async () => {
            const r = await importRaw(`${T}_missing`, csv, { contentType: 'text/csv', query: 'mode=append', key: WRITER_KEY });
            assertEqual(r.status, 404, 'status');
            assert(!(await tableExists(`${T}_missing`)), 'table not created');
        });

        await it('mode=replace swaps the rows and keeps evolved columns', async () => {
            const r = await importRaw(T, 'id,name,score\n100,zed,1\n101,yan,2\n', { contentType: 'text/csv', query: 'mode=replace', key: WRITER_KEY });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.mode, 'replace', 'mode');
            assertEqual(Number(r.json.deleted), 7, 'deleted');
            assertEqual(Number(r.json.inserted), 2, 'inserted');
            assertEqual(await count(T), 2, 'row count');
            assert((await columns(T)).includes('team'), 'team column kept');
            const acl = await sql(`SELECT count(*) AS n FROM _table_acls WHERE table_name = '${T}'`);
            assertEqual(Number(acl[0].n), 1, 'ACL row kept, not duplicated');
        });

        await it('mode=replace on a missing table creates it', async () => {
            const r = await importRaw(`${T}_rep`, csv, { contentType: 'text/csv', query: 'mode=replace', key: WRITER_KEY });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.created, true, 'created');
            assertEqual(await count(`${T}_rep`), 3, 'row count');
        });

        await it('a body with no format hint is 400', async () => {
            const r = await importRaw(`${T}_nofmt`, csv, { contentType: 'application/octet-stream' });
            assertEqual(r.status, 400, 'status');
            assert(/format/i.test(r.json?.error || ''), `error mentions the format (${r.json?.error})`);
        });

        await it('an unreadable file is 400 and creates nothing', async () => {
            const r = await importRaw(`${T}_bad`, 'this is not parquet', { contentType: 'application/vnd.apache.parquet' });
            assertEqual(r.status, 400, 'status');
            assert(!(await tableExists(`${T}_bad`)), 'table not created');
        });

        await it('an unknown mode is 400', async () => {
            const r = await importRaw(T, csv, { contentType: 'text/csv', query: 'mode=merge' });
            assertEqual(r.status, 400, 'status');
        });
    });

    await describe('Access control', async () => {
        await it('a reader gets 403 and no table is created', async () => {
            const r = await importRaw(`${T}_reader`, csv, { contentType: 'text/csv', key: READER_KEY });
            assertEqual(r.status, 403, 'status');
            assert(!(await tableExists(`${T}_reader`)), 'table not created');
        });

        await it('a reader cannot append to an existing table', async () => {
            const r = await importRaw(T, csv, { contentType: 'text/csv', query: 'mode=append', key: READER_KEY });
            assertEqual(r.status, 403, 'status');
            assertEqual(await count(T), 2, 'row count unchanged');
        });

        await it('a missing API key is 401', async () => {
            const r = await importRaw(T, csv, { contentType: 'text/csv', key: null });
            assertEqual(r.status, 401, 'status');
        });

        await it('system tables are 404 for every role', async () => {
            for (const key of [API_KEY, WRITER_KEY]) {
                const r = await importRaw('_table_acls', csv, { contentType: 'text/csv', key });
                assertEqual(r.status, 404, 'status');
            }
        });

        await it('an invalid table name is 400', async () => {
            const r = await importRaw('bad-name', csv, { contentType: 'text/csv' });
            assertEqual(r.status, 400, 'status');
        });
    });

    await describe('Size limit (IMPORT_MAX_MB)', async () => {
        await it('a declared Content-Length over the limit is 413 before the body is read', async () => {
            const r = await postDeclaredLength(`/tables/${T}_huge/import`, 200 * 1024 * 1024 * 1024);
            assertEqual(r.status, 413, `status (${r.text})`);
            assert(!(await tableExists(`${T}_huge`)), 'table not created');
        });

        const limitMb = Number(process.env.IMPORT_MAX_MB);
        if (Number.isFinite(limitMb) && limitMb > 0 && limitMb <= 64) {
            await it(`a streamed body over ${limitMb} MB is 413 and creates nothing`, async () => {
                const r = await postChunked(`${T}_stream`, (limitMb + 1) * 1024 * 1024);
                assert(r.status === 413 || r.status === 0, `status 413 or connection closed (got ${r.status}: ${JSON.stringify(r.json)})`);
                assert(!(await tableExists(`${T}_stream`)), 'table not created');
            });
        } else {
            skip('a streamed body over the limit is 413', 'set IMPORT_MAX_MB (<= 64) to the gateway\'s value to run it');
        }
    });

    await describe('Load from object storage', async () => {
        await it('local paths, file:// and http(s):// sources are rejected, also for superusers', async () => {
            for (const source of ['/etc/passwd', 'file:///etc/passwd', 'http://169.254.169.254/latest/meta-data/', 'https://example.com/x.csv', 's3://b/../x.csv']) {
                const r = await importSource(`${T}_src`, { source, format: 'csv' });
                assertEqual(r.status, 400, `status for ${source}`);
            }
            assert(!(await tableExists(`${T}_src`)), 'table not created');
        });

        await it('a writer cannot import from a prefix that is not allow-listed', async () => {
            const r = await importSource(`${T}_src`, { source: `s3://${S3.bucket}/import-private-${RUN}/a.csv` }, WRITER_KEY);
            assertEqual(r.status, 403, `status (${JSON.stringify(r.json)})`);
            assert(/IMPORT_ALLOWED_SOURCES/.test(r.json?.error || ''), 'error names the allow-list');
        });

        await it('a reader cannot import from object storage at all', async () => {
            const r = await importSource(`${T}_src`, { source: `${S3.allowed}x.csv` }, READER_KEY);
            assertEqual(r.status, 403, 'status');
        });

        if (!S3.endpoint) {
            skip('object-storage imports against S3/MinIO', 'set TEST_S3_ENDPOINT, TEST_S3_ACCESS_KEY and TEST_S3_SECRET_KEY');
            return;
        }

        const allowedKey = `${S3.allowed.replace(`s3://${S3.bucket}/`, '')}${T}/`;
        const privateKey = `import-private/${T}/`;
        await s3Put(`${allowedKey}part-1.csv`, 'id,label\n1,one\n2,two\n');
        await s3Put(`${allowedKey}part-2.csv`, 'id,label,extra\n3,three,x\n');
        await s3Put(`${privateKey}data.ndjson`, '{"id":1,"v":"a"}\n{"id":2,"v":"b"}\n{"id":3,"v":"c"}\n');

        await it('a writer imports an allow-listed glob (files combined by column name)', async () => {
            const r = await importSource(`${T}_s3`, { source: `s3://${S3.bucket}/${allowedKey}*.csv` }, WRITER_KEY);
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.json.created, true, 'created');
            assertEqual(Number(r.json.inserted), 3, 'inserted');
            assertEqual(await count(`${T}_s3`), 3, 'row count');
            assert((await columns(`${T}_s3`)).includes('extra'), 'union of columns');
        });

        await it('appending from object storage uses the request\'s mode', async () => {
            const r = await importSource(`${T}_s3`, { source: `s3://${S3.bucket}/${allowedKey}part-1.csv`, mode: 'append' }, WRITER_KEY);
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(await count(`${T}_s3`), 5, 'row count');
        });

        await it('a superuser may import from a prefix that is not allow-listed; a writer may not', async () => {
            const source = `s3://${S3.bucket}/${privateKey}data.ndjson`;
            assertEqual((await importSource(`${T}_s3p`, { source }, WRITER_KEY)).status, 403, 'writer');
            const r = await importSource(`${T}_s3p`, { source });
            assertEqual(r.status, 200, `superuser (${JSON.stringify(r.json)})`);
            assertEqual(r.json.format, 'json', 'format from the extension');
            assertEqual(await count(`${T}_s3p`), 3, 'row count');
        });

        await it('a missing object is 400', async () => {
            const r = await importSource(`${T}_s3m`, { source: `s3://${S3.bucket}/${allowedKey}nope.parquet` }, WRITER_KEY);
            assertEqual(r.status, 400, `status (${JSON.stringify(r.json)})`);
        });
    });

    await describe('Idempotency-Key', async () => {
        const U = `${T}_idem`;

        await it('/upload: a retry replays the first result and inserts once', async () => {
            const key = `upload-${RUN}`;
            const body = [{ id: 1, v: 'a' }, { id: 2, v: 'b' }];
            const first = await api('POST', `/upload/${U}`, body, API_KEY, { 'Idempotency-Key': key });
            assertEqual(first.status, 200, `first (${JSON.stringify(first.json)})`);
            assertEqual(first.headers.get('idempotent-replayed'), null, 'first is not a replay');
            const second = await api('POST', `/upload/${U}`, body, API_KEY, { 'Idempotency-Key': key });
            assertEqual(second.status, 200, 'second status');
            assertEqual(second.headers.get('idempotent-replayed'), 'true', 'replay header');
            assertEqual(JSON.stringify(second.json), JSON.stringify(first.json), 'same body');
            assertEqual(await count(U), 2, 'inserted once');
        });

        await it('/upload: the same key with a different body is 409', async () => {
            const r = await api('POST', `/upload/${U}`, [{ id: 3, v: 'c' }], API_KEY, { 'Idempotency-Key': `upload-${RUN}` });
            assertEqual(r.status, 409, 'status');
            assertEqual(await count(U), 2, 'nothing inserted');
        });

        await it('/upload: the same key with a different target table is 409', async () => {
            const r = await api('POST', `/upload/${U}_other`, [{ id: 1, v: 'a' }, { id: 2, v: 'b' }], API_KEY, { 'Idempotency-Key': `upload-${RUN}` });
            assertEqual(r.status, 409, 'status');
        });

        await it('keys are scoped to the API key that sent them', async () => {
            const r = await api('POST', `/upload/${U}`, [{ id: 1, v: 'a' }, { id: 2, v: 'b' }], WRITER_KEY, { 'Idempotency-Key': `upload-${RUN}` });
            assertEqual(r.status, 200, `status (${JSON.stringify(r.json)})`);
            assertEqual(r.headers.get('idempotent-replayed'), null, 'not a replay of another key\'s request');
            assertEqual(await count(U), 4, 'inserted');
        });

        await it('a malformed Idempotency-Key is 400', async () => {
            const r = await api('POST', `/upload/${U}`, [{ id: 9 }], API_KEY, { 'Idempotency-Key': 'x'.repeat(300) });
            assertEqual(r.status, 400, 'status');
            assertEqual(await count(U), 4, 'nothing inserted');
        });

        await it('/kafka-sink: a retry replays and inserts once', async () => {
            const topic = `${T}_sink`;
            const msgs = [{ key: 'k1', value: { n: 1 } }, { key: 'k2', value: { n: 2 } }];
            const headers = { 'Idempotency-Key': `sink-${RUN}`, 'X-Kafka-Topic': topic };
            const first = await api('POST', '/kafka-sink', msgs, API_KEY, headers);
            assertEqual(first.status, 200, `first (${JSON.stringify(first.json)})`);
            const second = await api('POST', '/kafka-sink', msgs, API_KEY, headers);
            assertEqual(second.status, 200, 'second');
            assertEqual(second.headers.get('idempotent-replayed'), 'true', 'replay header');
            assertEqual(await count(topic), 2, 'inserted once');
            const other = await api('POST', '/kafka-sink', msgs, API_KEY, { ...headers, 'X-Kafka-Topic': `${topic}_b` });
            assertEqual(other.status, 409, 'same key, different topic');
        });

        const I = `${T}_idimp`;
        await it('import: a retry replays the first result and inserts once', async () => {
            const headers = { 'Idempotency-Key': `import-${RUN}` };
            const first = await importRaw(I, csv, { contentType: 'text/csv', headers });
            assertEqual(first.status, 200, `first (${JSON.stringify(first.json)})`);
            const second = await importRaw(I, csv, { contentType: 'text/csv', headers });
            assertEqual(second.status, 200, 'second');
            assertEqual(second.headers.get('idempotent-replayed'), 'true', 'replay header');
            assertEqual(JSON.stringify(second.json), JSON.stringify(first.json), 'same body');
            assertEqual(await count(I), 3, 'inserted once');
        });

        await it('import: the same key with a different body is 409', async () => {
            const r = await importRaw(I, 'id,name,score\n9,x,9\n', { contentType: 'text/csv', headers: { 'Idempotency-Key': `import-${RUN}` } });
            assertEqual(r.status, 409, 'status');
            assertEqual(await count(I), 3, 'nothing inserted');
        });

        await it('import: a failed request releases its key so the retry runs', async () => {
            const headers = { 'Idempotency-Key': `import-fail-${RUN}` };
            const target = `${T}_late`;
            const failedFirst = await importRaw(target, csv, { contentType: 'text/csv', query: 'mode=append', headers });
            assertEqual(failedFirst.status, 404, 'first fails: table missing');
            await importRaw(target, 'id,name,score\n', { contentType: 'text/csv', query: 'mode=create' });
            const retry = await importRaw(target, csv, { contentType: 'text/csv', query: 'mode=append', headers });
            assertEqual(retry.status, 200, `retry runs (${JSON.stringify(retry.json)})`);
            assertEqual(retry.headers.get('idempotent-replayed'), null, 'not a replay');
            assertEqual(await count(target), 3, 'rows inserted by the retry');
        });

        await it('import: concurrent requests with one key insert once', async () => {
            const target = `${T}_conc`;
            await importRaw(target, 'id,name,score\n', { contentType: 'text/csv', query: 'mode=create' });
            const headers = { 'Idempotency-Key': `import-conc-${RUN}` };
            const results = await Promise.all([1, 2, 3, 4, 5, 6, 7, 8].map(() => importRaw(target, csv, { contentType: 'text/csv', query: 'mode=append', headers })));
            const statuses = results.map(r => r.status);
            assert(statuses.every(s => s === 200 || s === 409), `only 200 / 409 (${statuses})`);
            const executed = results.filter(r => r.status === 200 && r.headers.get('idempotent-replayed') === null);
            assertEqual(executed.length, 1, `exactly one request executed (${statuses})`);
            assertEqual(await count(target), 3, 'inserted once');
            const after = await importRaw(target, csv, { contentType: 'text/csv', query: 'mode=append', headers });
            assertEqual(after.status, 200, 'a later retry');
            assertEqual(after.headers.get('idempotent-replayed'), 'true', 'is a replay');
        });

        if (S3.endpoint) {
            await it('import from object storage: a retry replays and inserts once', async () => {
                const key = `${S3.allowed.replace(`s3://${S3.bucket}/`, '')}${T}/idem.csv`;
                await s3Put(key, 'id\n1\n2\n');
                const body = { source: `s3://${S3.bucket}/${key}` };
                const headers = { 'Idempotency-Key': `import-s3-${RUN}` };
                const first = await importSource(`${T}_s3i`, body, WRITER_KEY, headers);
                assertEqual(first.status, 200, `first (${JSON.stringify(first.json)})`);
                const second = await importSource(`${T}_s3i`, body, WRITER_KEY, headers);
                assertEqual(second.headers.get('idempotent-replayed'), 'true', 'replay header');
                assertEqual(await count(`${T}_s3i`), 2, 'inserted once');
            });
        }
    });

    await teardown();

    console.log(`\n${passed} passed, ${failed} failed, ${skipped} skipped`);
    if (failed) {
        for (const f of failures) console.log(`  ✗ ${f.name}: ${f.err.message}`);
        process.exit(1);
    }
}

run().catch(async (err) => {
    console.error(err);
    await teardown().catch(() => {});
    process.exit(1);
});
