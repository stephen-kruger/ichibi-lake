/**
 * Unit tests for src/zip.js: the streaming ZIP writer behind multi-table
 * exports. Archives are read back with test/zip-reader.js (central directory,
 * inflate, CRC-32 check).
 */
import assert from 'node:assert/strict';
import { ZipWriter } from '../src/zip.js';
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

    await test('more than 65535 entries switch to ZIP64 end records', async () => {
        const n = 65540;
        const buf = await build(async (zip) => {
            for (let i = 0; i < n; i++) await zip.add(`f${i}.txt`, '', { compress: false });
        });
        assert.equal(buf.readUInt16LE(buf.length - 22 + 10), 0xffff, 'classic EOCD count is saturated');
        const entries = readZip(buf);
        assert.equal(entries.length, n);
        assert.equal(entries[n - 1].name, `f${n - 1}.txt`);
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
