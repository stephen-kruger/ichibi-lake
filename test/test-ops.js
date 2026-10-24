/**
 * test/test-ops.js
 *
 * Integration tests for getting data out of the gateway and operating it.
 * Runs against a live gateway (default http://localhost:3333, see _env.js).
 *
 *   node test/test-ops.js
 *
 * What is tested:
 *   - GET /health (liveness, no auth) and GET /health/ready (DuckLake + Postgres)
 *   - GET /metrics: auth required, Prometheus text format, expected families
 *   - POST /query NDJSON streaming of a large result with bounded gateway memory
 *   - POST /query row caps and truncation indicators (JSON and NDJSON), format
 *     negotiation, identical value serialisation in JSON and NDJSON
 *   - GET /tables/:tableName/export round trips for parquet, csv and ndjson,
 *     filters/sort/limit, truncation headers
 *   - RBAC on export: reader allowed, no-access key 403, system tables 404,
 *     missing key 401
 */

import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { DuckDBInstance } from '@duckdb/node-api';
import { API_KEY, BASE_URL } from './_env.js';

const T = `ops_test_${Date.now()}`;
const READER_KEY = `sk-test-ops-reader-${Date.now()}`;
const NOACCESS_KEY = `sk-test-ops-noaccess-${Date.now()}`;

// ---------------------------------------------------------------------------
// Minimal test runner (same output style as the other suites)
// ---------------------------------------------------------------------------

let passed = 0;
let failed = 0;
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

function assert(cond, msg) { if (!cond) throw new Error(msg || 'assertion failed'); }
function assertEqual(actual, expected, msg) {
    if (actual !== expected) throw new Error(`${msg || 'values differ'}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

function query(body, { key = API_KEY, accept, format } = {}) {
    const url = `${BASE_URL}/query${format ? `?format=${format}` : ''}`;
    const headers = { 'Content-Type': 'application/json', 'x-api-key': key };
    if (accept) headers.Accept = accept;
    return fetch(url, { method: 'POST', headers, body: JSON.stringify(body) });
}

async function ndjsonLines(res) {
    const text = await res.text();
    return text.split('\n').filter(Boolean).map(l => JSON.parse(l));
}

async function api(method, urlPath, body, key = API_KEY) {
    const res = await fetch(`${BASE_URL}${urlPath}`, {
        method,
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json = null;
    try { json = await res.json(); } catch (_) { /* non-JSON */ }
    return { status: res.status, json };
}

function exportUrl(table, params = {}) {
    const qs = new URLSearchParams(params).toString();
    return `${BASE_URL}/tables/${table}/export${qs ? `?${qs}` : ''}`;
}

async function metricValue(name) {
    const res = await fetch(`${BASE_URL}/metrics`, { headers: { 'x-api-key': API_KEY } });
    const text = await res.text();
    const line = text.split('\n').find(l => l.startsWith(`${name} `));
    return line ? Number(line.split(' ')[1]) : NaN;
}

// Parse CSV as written by src/export.js (RFC 4180 quoting, CRLF rows).
function parseCsv(text) {
    const rows = [];
    let row = [], field = '', quoted = false;
    for (let i = 0; i < text.length; i++) {
        const c = text[i];
        if (quoted) {
            if (c === '"' && text[i + 1] === '"') { field += '"'; i++; }
            else if (c === '"') quoted = false;
            else field += c;
        } else if (c === '"') quoted = true;
        else if (c === ',') { row.push(field); field = ''; }
        else if (c === '\r') { /* part of CRLF */ }
        else if (c === '\n') { row.push(field); rows.push(row); row = []; field = ''; }
        else field += c;
    }
    if (field || row.length) { row.push(field); rows.push(row); }
    return rows;
}

// ---------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------

const FIXTURE = [
    { id: 1, name: 'alpha', category: 'a', score: 10, note: 'plain' },
    { id: 2, name: 'beta', category: 'b', score: 20, note: 'has, comma' },
    { id: 3, name: 'gamma', category: 'a', score: 30, note: 'has "quotes"' },
    { id: 4, name: 'delta', category: 'b', score: 40, note: 'multi\nline' },
    { id: 5, name: 'epsilon', category: 'a', score: 50, note: null },
];

async function setup() {
    const up = await api('POST', `/upload/${T}`, FIXTURE);
    if (up.status !== 200) throw new Error(`fixture upload failed: ${up.status} ${JSON.stringify(up.json)}`);
    await api('POST', '/admin/users', { apiKey: READER_KEY, name: 'ops reader', roles: ['reader'] });
    await api('POST', '/admin/users', { apiKey: NOACCESS_KEY, name: 'ops no access', roles: [] });
}

async function teardown() {
    await api('DELETE', `/admin/users/${encodeURIComponent(READER_KEY)}`).catch(() => {});
    await api('DELETE', `/admin/users/${encodeURIComponent(NOACCESS_KEY)}`).catch(() => {});
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

async function run() {
    console.log(`--- Operations / data-out integration tests against ${BASE_URL} ---`);

    await describe('Health endpoints', async () => {
        await it('GET /health returns 200 without an API key', async () => {
            const res = await fetch(`${BASE_URL}/health`);
            assertEqual(res.status, 200, 'status');
            const body = await res.json();
            assertEqual(body.status, 'ok', 'status field');
            assert(typeof body.uptimeSeconds === 'number', 'uptimeSeconds present');
        });

        await it('GET /health/ready reports DuckLake and Postgres ok without an API key', async () => {
            // Startup (bootstrap + compaction) may still be running on a fresh stack.
            let body, status;
            for (let i = 0; i < 60; i++) {
                const res = await fetch(`${BASE_URL}/health/ready`);
                status = res.status;
                body = await res.json();
                if (status === 200) break;
                await new Promise(r => setTimeout(r, 1000));
            }
            assertEqual(status, 200, `readiness (${JSON.stringify(body)})`);
            assertEqual(body.checks.ducklake, 'ok', 'ducklake check');
            assertEqual(body.checks.postgres, 'ok', 'postgres check');
            assertEqual(body.checks.startup, 'ready', 'startup check');
        });
    });

    await setup();

    await describe('Metrics', async () => {
        await it('GET /metrics without credentials is 401', async () => {
            const res = await fetch(`${BASE_URL}/metrics`);
            assertEqual(res.status, 401, 'status');
        });

        await it('GET /metrics with a non-superuser key is 401', async () => {
            const res = await fetch(`${BASE_URL}/metrics`, { headers: { 'x-api-key': READER_KEY } });
            assertEqual(res.status, 401, 'status');
        });

        await it('GET /metrics with a superuser key returns valid Prometheus text', async () => {
            await fetch(`${BASE_URL}/tables/${T}`, { headers: { 'x-api-key': API_KEY } });
            const res = await fetch(`${BASE_URL}/metrics`, { headers: { Authorization: `Bearer ${API_KEY}` } });
            assertEqual(res.status, 200, 'status');
            assert(res.headers.get('content-type').startsWith('text/plain; version=0.0.4'), 'content type');
            const text = await res.text();
            const sampleRe = /^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\]|\\.)*",?)*\})? (-?[0-9.e+-]+|NaN|\+Inf|-Inf)$/;
            for (const line of text.trim().split('\n')) {
                if (line.startsWith('# HELP ') || line.startsWith('# TYPE ')) continue;
                assert(sampleRe.test(line), `malformed sample line: ${line}`);
            }
            for (const family of ['ichibi_http_requests_total', 'ichibi_http_request_duration_seconds', 'ichibi_user_query_active',
                'ichibi_user_query_queued', 'ichibi_user_query_timeouts_total', 'ichibi_export_active', 'ichibi_prune_jobs_total',
                'ichibi_kafka_consumer_lag', 'ichibi_ducklake_tables', 'ichibi_ducklake_data_files', 'process_resident_memory_bytes']) {
                assert(text.includes(`# TYPE ${family} `), `missing family ${family}`);
            }
            assert(text.includes('route="/tables/:tableName"'), 'request counted under its route template, not the raw URL');
            assert(!text.includes(T), 'raw table name must not appear as a label');
            assert(Number(text.match(/^ichibi_ducklake_tables (\d+)/m)?.[1]) >= 1, 'at least one lake table');
        });
    });

    await describe('POST /query streaming (NDJSON)', async () => {
        await it('streams a large result (4M rows, ~500 MB) with bounded gateway memory', async () => {
            const ROWS = 4_000_000;
            const sql = (n) => `SELECT range AS i, repeat('x', 100) AS pad FROM range(${n})`;
            // Warm up the user-query instance (first-query allocations are not growth).
            await (await query({ sql: sql(200_000) }, { format: 'ndjson' })).arrayBuffer();
            const baseline = await metricValue('process_resident_memory_bytes');
            const res = await query({ sql: sql(ROWS) }, { format: 'ndjson' });
            assertEqual(res.status, 200, 'status');
            assert(res.headers.get('content-type').startsWith('application/x-ndjson'), 'content type');

            // Sample the gateway's RSS while consuming the stream.
            let peak = baseline;
            let sampling = true;
            const sampler = (async () => {
                while (sampling) {
                    const v = await metricValue('process_resident_memory_bytes').catch(() => NaN);
                    if (v > peak) peak = v;
                    await new Promise(r => setTimeout(r, 200));
                }
            })();

            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buffered = '', lines = 0, bytes = 0, trailer = null, lastRow = null;
            for (;;) {
                const { value, done } = await reader.read();
                if (done) break;
                bytes += value.length;
                buffered += decoder.decode(value, { stream: true });
                const parts = buffered.split('\n');
                buffered = parts.pop();
                for (const p of parts) {
                    if (!p) continue;
                    const obj = JSON.parse(p);
                    if (obj._ichibi) trailer = obj._ichibi;
                    else { lines++; lastRow = obj; }
                }
            }
            sampling = false;
            await sampler;

            assertEqual(lines, ROWS, 'row lines');
            assert(trailer, 'trailer line present');
            assertEqual(trailer.rowCount, ROWS, 'trailer rowCount');
            assertEqual(trailer.truncated, false, 'trailer truncated');
            assertEqual(String(lastRow.i), String(ROWS - 1), 'last row value');
            const growthMb = (peak - baseline) / 1e6;
            console.log(`      (${(bytes / 1e6).toFixed(0)} MB streamed; gateway RSS ${(baseline / 1e6).toFixed(0)} MB -> peak ${(peak / 1e6).toFixed(0)} MB)`);
            // Materialising this result would take well over 1 GB; streaming
            // holds roughly one DuckDB chunk at a time (observed: ~30 MB growth
            // whether 100 MB or 1 GB is streamed).
            assert(growthMb < 150, `gateway RSS grew ${growthMb.toFixed(0)} MB while streaming ${(bytes / 1e6).toFixed(0)} MB`);
        });

        await it('Accept: application/x-ndjson selects NDJSON', async () => {
            const res = await query({ sql: 'SELECT range AS i FROM range(3)' }, { accept: 'application/x-ndjson' });
            assertEqual(res.status, 200, 'status');
            const lines = await ndjsonLines(res);
            assertEqual(lines.length, 4, '3 rows + trailer');
            assertEqual(lines[3]._ichibi.rowCount, 3, 'trailer rowCount');
        });

        await it('maxRows caps the stream and the trailer reports truncation', async () => {
            const res = await query({ sql: 'SELECT range AS i FROM range(100)', maxRows: 5 }, { format: 'ndjson' });
            const lines = await ndjsonLines(res);
            assertEqual(lines.length, 6, '5 rows + trailer');
            assertEqual(res.headers.get('x-row-cap'), '5', 'X-Row-Cap header');
            assertEqual(lines[5]._ichibi.truncated, true, 'truncated');
            assertEqual(lines[5]._ichibi.rowCount, 5, 'rowCount');
        });

        await it('an SQL error before any output is a JSON error status, not a 200 stream', async () => {
            const res = await query({ sql: 'SELECT * FROM no_such_table_xyz' }, { format: 'ndjson' });
            assert(res.status >= 400, `status ${res.status}`);
            const body = await res.json();
            assertEqual(body.success, false, 'success flag');
        });

        await it('JSON and NDJSON serialise BigInt, timestamp and BLOB values identically', async () => {
            const sql = "SELECT 9007199254740993::BIGINT AS big, TIMESTAMP '2024-02-03 04:05:06' AS ts, 'ab'::BLOB AS bin, [1, 2] AS lst";
            const json = await (await query({ sql })).json();
            const nd = await ndjsonLines(await query({ sql }, { format: 'ndjson' }));
            assertEqual(JSON.stringify(nd[0]), JSON.stringify(json.rows[0]), 'row serialisation');
            assertEqual(json.rows[0].big, '9007199254740993', 'BigInt as string');
            assertEqual(json.rows[0].bin, Buffer.from('ab').toString('base64'), 'BLOB as base64');
        });
    });

    await describe('POST /query row cap (JSON)', async () => {
        await it('small results are complete and report truncated=false', async () => {
            const res = await query({ sql: 'SELECT range AS i FROM range(100)' });
            const body = await res.json();
            assertEqual(body.rowCount, 100, 'rowCount');
            assertEqual(body.truncated, false, 'truncated');
            assertEqual(res.headers.get('x-result-truncated'), null, 'no truncation header');
        });

        await it('maxRows truncates and flags it in the body and a header', async () => {
            const res = await query({ sql: 'SELECT range AS i FROM range(100)', maxRows: 10 });
            const body = await res.json();
            assertEqual(body.rows.length, 10, 'rows');
            assertEqual(body.truncated, true, 'truncated');
            assertEqual(body.maxRows, 10, 'maxRows echoed');
            assertEqual(res.headers.get('x-result-truncated'), 'true', 'header');
        });

        await it('a cap that lands exactly on a chunk boundary is not reported as truncated', async () => {
            const exact = await (await query({ sql: 'SELECT range AS i FROM range(2048)', maxRows: 2048 })).json();
            assertEqual(exact.truncated, false, 'exactly cap rows');
            const over = await (await query({ sql: 'SELECT range AS i FROM range(2049)', maxRows: 2048 })).json();
            assertEqual(over.truncated, true, 'one row over cap');
            assertEqual(over.rowCount, 2048, 'rowCount');
        });

        await it('rejects invalid maxRows and unknown formats with 400', async () => {
            assertEqual((await query({ sql: 'SELECT 1', maxRows: 0 })).status, 400, 'maxRows 0');
            assertEqual((await query({ sql: 'SELECT 1' }, { format: 'arrow' })).status, 400, 'format=arrow');
        });

        await it('still refuses writes and non-superusers', async () => {
            assertEqual((await query({ sql: `DELETE FROM "${T}"` }, { format: 'ndjson' })).status, 403, 'write SQL');
            assertEqual((await query({ sql: 'SELECT 1' }, { key: READER_KEY, format: 'ndjson' })).status, 403, 'reader key');
        });
    });

    await describe('GET /tables/:tableName/export', async () => {
        await it('parquet round trip preserves rows and types', async () => {
            const res = await fetch(exportUrl(T, { format: 'parquet', sort: 'id' }), { headers: { 'x-api-key': API_KEY } });
            assertEqual(res.status, 200, 'status');
            assertEqual(res.headers.get('content-type'), 'application/vnd.apache.parquet', 'content type');
            assert(res.headers.get('content-disposition').includes(`${T}.parquet`), 'filename');
            assertEqual(res.headers.get('x-total-rows'), '5', 'X-Total-Rows');
            assertEqual(res.headers.get('x-export-truncated'), 'false', 'X-Export-Truncated');
            const buf = Buffer.from(await res.arrayBuffer());
            assertEqual(buf.subarray(0, 4).toString(), 'PAR1', 'parquet magic');
            const file = path.join(os.tmpdir(), `${T}.parquet`);
            await fs.writeFile(file, buf);
            try {
                const inst = await DuckDBInstance.create(':memory:');
                const conn = await inst.connect();
                const result = await conn.run(`SELECT id, name, note, score FROM read_parquet('${file}') ORDER BY id`);
                const rows = await result.getRowObjectsJson();
                assertEqual(rows.length, 5, 'row count');
                FIXTURE.forEach((f, i) => {
                    assertEqual(String(rows[i].id), String(f.id), `id ${i}`);
                    assertEqual(rows[i].name, f.name, `name ${i}`);
                    assertEqual(rows[i].note, f.note, `note ${i}`);
                    assertEqual(Number(rows[i].score), f.score, `score ${i}`);
                });
                conn.closeSync();
            } finally {
                await fs.unlink(file).catch(() => {});
            }
        });

        await it('csv round trip handles commas, quotes, newlines and NULLs', async () => {
            const res = await fetch(exportUrl(T, { format: 'csv', sort: 'id' }), { headers: { 'x-api-key': API_KEY } });
            assertEqual(res.status, 200, 'status');
            assert(res.headers.get('content-type').startsWith('text/csv'), 'content type');
            const rows = parseCsv(await res.text());
            const header = rows[0];
            const col = (name) => header.indexOf(name);
            assert(col('note') >= 0 && col('name') >= 0, 'header has columns');
            assertEqual(rows.length, 6, 'header + 5 rows');
            FIXTURE.forEach((f, i) => {
                assertEqual(rows[i + 1][col('name')], f.name, `name ${i}`);
                assertEqual(rows[i + 1][col('note')], f.note ?? '', `note ${i}`);
            });
        });

        await it('ndjson round trip', async () => {
            const res = await fetch(exportUrl(T, { format: 'ndjson', sort: 'id' }), { headers: { 'x-api-key': API_KEY } });
            assertEqual(res.status, 200, 'status');
            const lines = await ndjsonLines(res);
            assertEqual(lines.length, 5, 'rows');
            FIXTURE.forEach((f, i) => {
                assertEqual(lines[i].name, f.name, `name ${i}`);
                assertEqual(lines[i].note, f.note, `note ${i}`);
                assertEqual(Number(lines[i].score), f.score, `score ${i}`);
            });
        });

        await it('applies filters and sort like GET /tables/:tableName', async () => {
            const res = await fetch(exportUrl(T, { format: 'ndjson', category: 'a', sort: '-score' }), { headers: { 'x-api-key': API_KEY } });
            const lines = await ndjsonLines(res);
            assertEqual(lines.map(l => l.name).join(','), 'epsilon,gamma,alpha', 'filtered + sorted');
            assertEqual(res.headers.get('x-total-rows'), '3', 'X-Total-Rows');
        });

        await it('limit truncates and reports it in headers', async () => {
            const res = await fetch(exportUrl(T, { format: 'csv', sort: 'id', limit: '2' }), { headers: { 'x-api-key': API_KEY } });
            assertEqual(res.headers.get('x-total-rows'), '5', 'X-Total-Rows');
            assertEqual(res.headers.get('x-row-count'), '2', 'X-Row-Count');
            assertEqual(res.headers.get('x-export-truncated'), 'true', 'X-Export-Truncated');
            assertEqual(parseCsv(await res.text()).length, 3, 'header + 2 rows');
        });

        await it('rejects unknown formats with 400 and unknown tables with 404', async () => {
            assertEqual((await fetch(exportUrl(T, { format: 'xlsx' }), { headers: { 'x-api-key': API_KEY } })).status, 400, 'format');
            assertEqual((await fetch(exportUrl('no_such_table_xyz'), { headers: { 'x-api-key': API_KEY } })).status, 404, 'table');
        });
    });

    await describe('Export RBAC', async () => {
        await it('a reader can export', async () => {
            const res = await fetch(exportUrl(T, { format: 'ndjson' }), { headers: { 'x-api-key': READER_KEY } });
            assertEqual(res.status, 200, 'status');
            assertEqual((await ndjsonLines(res)).length, 5, 'rows');
        });

        await it('a key with no access gets 403', async () => {
            const res = await fetch(exportUrl(T, { format: 'csv' }), { headers: { 'x-api-key': NOACCESS_KEY } });
            assertEqual(res.status, 403, 'status');
        });

        await it('system tables are 404 even for a superuser', async () => {
            for (const sys of ['_rbac_users', '_table_acls']) {
                const res = await fetch(exportUrl(sys, { format: 'csv' }), { headers: { 'x-api-key': API_KEY } });
                assertEqual(res.status, 404, sys);
            }
        });

        await it('a missing API key gets 401', async () => {
            assertEqual((await fetch(exportUrl(T))).status, 401, 'status');
        });
    });

    await describe('Metrics after traffic', async () => {
        await it('exports and user queries are counted', async () => {
            const text = await (await fetch(`${BASE_URL}/metrics`, { headers: { 'x-api-key': API_KEY } })).text();
            assert(/^ichibi_export_total\{format="parquet",outcome="succeeded"\} [1-9]/m.test(text), 'parquet export counted');
            assert(/^ichibi_user_query_total\{outcome="succeeded"\} [1-9]/m.test(text), 'user queries counted');
            assert(text.includes('route="/tables/:tableName/export"'), 'export route label');
        });
    });
}

try {
    await run();
} catch (err) {
    failed++;
    failures.push({ name: 'suite', err });
    console.error(err);
} finally {
    await teardown();
}

console.log(`\n${passed} passed, ${failed} failed`);
if (failed) process.exit(1);
