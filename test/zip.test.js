/**
 * Unit tests for src/zip.js (the streaming ZIP writer behind multi-table
 * exports) and src/unzip.js (the reader behind ZIP imports). Archives are read
 * back through the central directory, with inflate and CRC-32 checks.
 */
import assert from 'node:assert/strict';
import fsp from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { ZipWriter } from '../src/zip.js';
import { listZipEntries, extractZipEntry, readZipEntry } from '../src/unzip.js';
import { readZip } from './zip-reader.js';

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}

/** Collect an archive into one Buffer; `slow` simulates a client that applies backpressure. */
async function build(fn, { slow = false } = {}) {
    const parts = [];
    const zip = new ZipWriter(async (chunk) => {
        parts.push(Buffer.from(chunk));
        if (slow) await new Promise(r => setImmediate(r));
    });
    await fn(zip);
    await zip.finish();
    return Buffer.concat(parts);
}

/** Write a built archive to a temp file, run `fn(file, dir)`, then clean up. */
async function withArchive(buf, fn) {
    const dir = await fsp.mkdtemp(path.join(os.tmpdir(), 'ichibi-zip-test-'));
    const file = path.join(dir, 'archive.zip');
    await fsp.writeFile(file, buf);
    try { return await fn(file, dir); } finally { await fsp.rm(dir, { recursive: true, force: true }); }
}

async function run() {
    console.log('\n▸ zip: streaming writer');

    await test('stored and deflated entries round-trip with correct CRCs', async () => {
        const text = 'id,name\r\n' + Array.from({ length: 5000 }, (_, i) => `${i},row ${i}`).join('\r\n');
        const binary = Buffer.from(Array.from({ length: 70000 }, (_, i) => (i * 7919) & 0xff));
        const buf = await build(async (zip) => {
            await zip.add('a.csv', text, { compress: true });
            await zip.add('b.parquet', binary, { compress: false });
            await zip.add('manifest.json', JSON.stringify({ ok: true }));
        });
        const entries = readZip(buf);
        assert.deepEqual(entries.map(e => e.name), ['a.csv', 'b.parquet', 'manifest.json']);
        assert.equal(entries[0].method, 8);
        assert.equal(entries[0].data.toString(), text);
        assert.equal(entries[1].method, 0);
        assert.ok(entries[1].data.equals(binary));
        assert.deepEqual(JSON.parse(entries[2].data), { ok: true });
        assert.ok(buf.length < text.length / 2 + binary.length, 'deflate should shrink repetitive CSV');
    });

    await test('an entry written in many chunks under backpressure is intact', async () => {
        const chunks = Array.from({ length: 300 }, (_, i) => `line ${i} ${'x'.repeat(i % 50)}\n`);
        const buf = await build(async (zip) => {
            const e = await zip.entry('big.ndjson');
            for (const c of chunks) await e.write(c);
            await e.close();
        }, { slow: true });
        assert.equal(readZip(buf)[0].data.toString(), chunks.join(''));
    });

    await test('empty entries and an empty archive are valid', async () => {
        const buf = await build(async (zip) => { await zip.add('empty.csv', ''); });
        const [e] = readZip(buf);
        assert.equal(e.name, 'empty.csv');
        assert.equal(e.data.length, 0);
        assert.equal(readZip(await build(async () => {})).length, 0);
    });

    await test('UTF-8 names are kept', async () => {
        const buf = await build(async (zip) => { await zip.add('données_é.csv', 'x'); });
        assert.equal(readZip(buf)[0].name, 'données_é.csv');
    });

    await test('import reader lists, reads and extracts stored and deflated entries', async () => {
        const text = 'id,name\r\n1,one\r\n2,two\r\n';
        const binary = Buffer.from([0, 1, 2, 3, 255, 254]);
        const buf = await build(async (zip) => {
            await zip.add('a.csv', text, { compress: true });
            await zip.add('b.parquet', binary, { compress: false });
            await zip.add('empty.csv', '');
            await zip.add('manifest.json', JSON.stringify({ tables: [] }));
        });
        await withArchive(buf, async (file, dir) => {
            const entries = await listZipEntries(file);
            assert.deepEqual(entries.map(e => e.name), ['a.csv', 'b.parquet', 'empty.csv', 'manifest.json']);
            assert.deepEqual(JSON.parse((await readZipEntry(file, entries[3])).toString()), { tables: [] });
            const csv = path.join(dir, 'a.csv');
            const pq = path.join(dir, 'b.parquet');
            const empty = path.join(dir, 'empty.csv');
            assert.equal(await extractZipEntry(file, entries[0], csv), text.length);
            assert.equal(await fsp.readFile(csv, 'utf8'), text);
            assert.equal(await extractZipEntry(file, entries[1], pq), binary.length);
            assert.deepEqual(await fsp.readFile(pq), binary);
            assert.equal(await extractZipEntry(file, entries[2], empty), 0);
            assert.equal((await fsp.readFile(empty)).length, 0);
        });
    });

    await test('import reader rejects bad CRCs and oversized extraction', async () => {
        const text = 'id,name\r\n1,one\r\n';
        const buf = await build(async (zip) => { await zip.add('a.csv', text, { compress: false }); });
        await withArchive(buf, async (file, dir) => {
            const [entry] = await listZipEntries(file);
            await assert.rejects(extractZipEntry(file, entry, path.join(dir, 'small.csv'), { maxBytes: 2 }), /limit/);
            const local = Buffer.from(buf.subarray(entry.offset, entry.offset + 30));
            const start = entry.offset + 30 + local.readUInt16LE(26) + local.readUInt16LE(28);
            const broken = Buffer.from(buf);
            broken[start + 2] ^= 0xff;
            await fsp.writeFile(file, broken);
            await assert.rejects(extractZipEntry(file, entry, path.join(dir, 'broken.csv')), /CRC|could not extract|wrong size/);
        });
    });

    await test('more than 65535 entries switch to ZIP64 end records', async () => {
        const n = 65540;
        const buf = await build(async (zip) => {
            for (let i = 0; i < n; i++) await zip.add(`f${i}.txt`, '', { compress: false });
        });
        assert.equal(buf.readUInt16LE(buf.length - 22 + 10), 0xffff, 'classic EOCD count is saturated');
        const entries = readZip(buf);
        assert.equal(entries.length, n);
        assert.equal(entries[n - 1].name, `f${n - 1}.txt`);
        await withArchive(buf, async (file) => {
            const readEntries = await listZipEntries(file);
            assert.equal(readEntries.length, n);
            assert.equal(readEntries[n - 1].name, `f${n - 1}.txt`);
        });
    });

    await test('misuse is rejected', async () => {
        const zip = new ZipWriter(async () => {});
        await zip.entry('a');
        await assert.rejects(zip.entry('b'), /close the previous entry/);
        await assert.rejects(zip.finish(), /close the open entry/);
    });
}

run().then(() => {
    console.log('\n─── zip unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    if (failed > 0) process.exit(1);
}).catch((err) => { console.error(err); process.exit(1); });
