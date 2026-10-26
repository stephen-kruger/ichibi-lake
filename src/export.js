/**
 * Table exports (GET /tables/:tableName/export, and the console's single and
 * multi-table downloads; runMultiExport() packs several tables into one ZIP).
 *
 * Exports run on the primary DuckDB instance, on a dedicated (unpooled)
 * connection, because the locked-down user-query instance cannot write files
 * (enable_external_access=false) and an export must not tie up a pooled
 * connection while a slow client downloads. The caller (src/index.js) builds
 * the SELECT from validated identifiers and parameter-bound filter values and
 * has already checked RBAC read access.
 *
 *   ndjson   streamed chunk by chunk; each line is DuckDB's to_json() of a row
 *            (ISO timestamps, nested types as JSON). No temp file.
 *   csv      streamed chunk by chunk with a header row; values use DuckDB's
 *            text form (as COPY ... (FORMAT csv) would write them). No temp file.
 *   parquet  Parquet needs its footer written last, so DuckDB COPYs the rows to
 *            a temp file under EXPORT_TMP_DIR, which is streamed to the client
 *            and deleted as soon as the response ends (or fails).
 *
 * CSV text values that a spreadsheet would run as a formula (starting with
 * =, +, -, @, tab or CR) are prefixed with an apostrophe; see csvField().
 *
 * BLOB columns are exported base64-encoded in csv/ndjson (as the JSON API
 * returns them) and as raw binary in Parquet.
 *
 * Every export is capped at EXPORT_MAX_ROWS rows (a request's ?limit= can only
 * lower it). The total matching row count is computed in the same transaction
 * before streaming, so the response headers say up front whether the file is
 * truncated: X-Total-Rows, X-Row-Count, X-Export-Truncated. Concurrent exports
 * are limited to EXPORT_CONCURRENCY (excess requests get 429) and each one to
 * EXPORT_TIMEOUT_MS wall-clock, which bounds temp-disk use to roughly
 * EXPORT_CONCURRENCY x (size of a capped Parquet export).
 */
import fs from 'fs';
import fsp from 'fs/promises';
import os from 'os';
import path from 'path';
import { randomUUID } from 'crypto';
import { once } from 'events';
import { pipeline } from 'stream/promises';
import { openDedicatedConnection } from './db.js';
import { ZipWriter } from './zip.js';

export const EXPORT_FORMATS = {
    parquet: { contentType: 'application/vnd.apache.parquet', ext: 'parquet' },
    csv: { contentType: 'text/csv; charset=utf-8', ext: 'csv' },
    ndjson: { contentType: 'application/x-ndjson', ext: 'ndjson' },
};

// Read on use so the admin console can change them live.
export const exportMaxRows = () => Math.max(1, parseInt(process.env.EXPORT_MAX_ROWS || '5000000', 10) || 5000000);
const exportConcurrency = () => Math.max(1, parseInt(process.env.EXPORT_CONCURRENCY || '2', 10) || 2);
const exportTimeoutMs = () => Math.max(1000, parseInt(process.env.EXPORT_TIMEOUT_MS || '600000', 10) || 600000);
const exportTmpDir = () => process.env.EXPORT_TMP_DIR || path.join(os.tmpdir(), 'ichibi-export');

let active = 0;
const counters = {};

/** Snapshot for GET /metrics. counters is keyed "format:outcome". */
export function exportStats() {
    return { active, concurrency: exportConcurrency(), counters: { ...counters } };
}

function count(format, outcome) {
    const key = `${format}:${outcome}`;
    counters[key] = (counters[key] || 0) + 1;
}

// Resolve once the socket can take more data, or reject if the client went away.
async function write(res, chunk) {
    if (res.destroyed) throw Object.assign(new Error('client disconnected'), { cancelled: true });
    if (!res.write(chunk)) {
        await Promise.race([once(res, 'drain'), once(res, 'close')]);
        if (res.destroyed) throw Object.assign(new Error('client disconnected'), { cancelled: true });
    }
}

// CSV injection: spreadsheets run a cell that starts with one of these as a
// formula (=HYPERLINK(...), =cmd|..., +, -, @), so a text value starting with
// one gets a leading apostrophe (OWASP's recommendation), which spreadsheets
// display as plain text. Only text values: numbers, dates and other typed
// values arrive as non-strings, and numeric-looking text such as "-5" cannot
// be a formula, so both are written unchanged.
const FORMULA_START_RE = /^[=+\-@\t\r]/;
const NUMERIC_TEXT_RE = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

export function csvField(value) {
    if (value === null || value === undefined) return '';
    let text = typeof value === 'string' ? value : String(value);
    if (typeof value === 'string' && FORMULA_START_RE.test(text) && !NUMERIC_TEXT_RE.test(text)) text = `'${text}`;
    return /[",\r\n]|^\s|\s$/.test(text) ? `"${text.replace(/"/g, '""')}"` : text;
}

/** COPY the rows to a new temp Parquet file and return its path. */
async function copyToParquet(conn, sql, params, types) {
    const dir = exportTmpDir();
    await fsp.mkdir(dir, { recursive: true });
    const file = path.join(dir, `${randomUUID()}.parquet`);
    await conn.run(`COPY (${sql}) TO '${file.replace(/'/g, "''")}' (FORMAT parquet, COMPRESSION zstd)`, params, types);
    return file;
}

/** Stream rows as CSV (with header) or NDJSON text to `emit`, chunk by chunk. */
async function streamText(conn, format, sql, params, types, emit) {
    const query = format === 'ndjson' ? `SELECT to_json(_r)::VARCHAR FROM (${sql}) _r` : sql;
    const result = await conn.stream(query, params, types);
    if (format === 'csv') await emit(result.columnNames().map(csvField).join(',') + '\r\n');
    for (;;) {
        const chunk = await result.fetchChunk();
        if (!chunk || chunk.rowCount === 0) break;
        const rows = chunk.getRows();
        await emit(format === 'ndjson'
            ? rows.map(r => r[0]).join('\n') + '\n'
            : rows.map(r => r.map(csvField).join(',')).join('\r\n') + '\r\n');
    }
}

/**
 * Run an export and write it to `res`.
 *
 *   selectSql   SELECT producing the rows (BLOB columns already base64() for
 *               csv/ndjson), with "?" placeholders for `params`
 *   countSql    SELECT count(*) over the same filters
 *   limit       requested row limit (null = EXPORT_MAX_ROWS)
 *   filename    download name without extension
 *
 * Errors raised before any byte is sent are thrown (with statusCode) for the
 * caller to turn into a JSON error; later errors abort the response.
 */
export async function runExport({ format, selectSql, countSql, params, types, limit, filename, res }) {
    const spec = EXPORT_FORMATS[format];
    if (active >= exportConcurrency()) {
        count(format, 'rejected');
        res.setHeader('Retry-After', '10');
        throw Object.assign(new Error(`Too many concurrent exports (EXPORT_CONCURRENCY=${exportConcurrency()}); retry later`), { statusCode: 429 });
    }
    active++;
    const cap = Math.min(exportMaxRows(), limit ?? Infinity);
    let conn;
    let tmpFile = null;
    let timer;
    let timedOut = false;
    const onClose = () => { if (!res.writableFinished) conn?.interrupt?.(); };
    res.on('close', onClose);
    try {
        conn = await openDedicatedConnection();
        timer = setTimeout(() => {
            timedOut = true;
            try { conn.interrupt(); } catch (_) { /* ignore */ }
            if (res.headersSent) res.destroy(new Error('export timed out'));
        }, exportTimeoutMs());

        // One transaction so the row count and the exported rows see the same snapshot.
        await conn.run('BEGIN TRANSACTION');
        const countResult = await conn.run(countSql, params, types);
        const total = Number((await countResult.getRows())[0][0]);
        const rowCount = Math.min(total, cap);
        const limitedSql = `${selectSql} LIMIT ${cap}`;

        if (format === 'parquet') tmpFile = await copyToParquet(conn, limitedSql, params, types);

        res.status(200);
        res.setHeader('Content-Type', spec.contentType);
        res.setHeader('Content-Disposition', `attachment; filename="${filename}.${spec.ext}"`);
        res.setHeader('X-Total-Rows', String(total));
        res.setHeader('X-Row-Count', String(rowCount));
        res.setHeader('X-Export-Truncated', String(total > rowCount));
        res.setHeader('X-Row-Cap', String(cap));

        if (format === 'parquet') {
            const { size } = await fsp.stat(tmpFile);
            res.setHeader('Content-Length', String(size));
            // pipeline() ends the response and destroys the file stream if the client goes away.
            try {
                await pipeline(fs.createReadStream(tmpFile), res);
            } catch (err) {
                throw Object.assign(err, { cancelled: res.destroyed });
            }
            await conn.run('COMMIT');
        } else {
            await streamText(conn, format, limitedSql, params, types, text => write(res, text));
            await conn.run('COMMIT');
            res.end();
        }
        count(format, 'succeeded');
    } catch (err) {
        if (timedOut) {
            count(format, 'timeout');
            err = Object.assign(new Error(`Export exceeded ${exportTimeoutMs()}ms timeout`), { statusCode: 504 });
        } else {
            count(format, err.cancelled || res.destroyed ? 'cancelled' : 'failed');
        }
        if (res.headersSent) {
            res.destroy(err);
            return;
        }
        throw err;
    } finally {
        clearTimeout(timer);
        res.off('close', onClose);
        if (conn) { try { conn.closeSync(); } catch (_) { /* ignore */ } }
        if (tmpFile) fsp.unlink(tmpFile).catch(() => {});
        active--;
    }
}

/**
 * Export several tables as one ZIP archive written to `res`: one file per table
 * (`<file>.<ext>`) plus manifest.json with each table's row counts and whether
 * it hit the row cap (a multi-file response cannot say that in headers).
 *
 *   tables   [{ name, file, selectSql, countSql }]  (no parameters: whole tables)
 *
 * Every table is read in ONE transaction, so the archive is a consistent
 * snapshot of the lake. It takes one EXPORT_CONCURRENCY slot; EXPORT_MAX_ROWS
 * applies per table, and EXPORT_TIMEOUT_MS per table (a whole-lake archive can
 * legitimately take much longer than any single export). Parquet files are
 * staged one at a time and stored as is (already compressed); CSV / NDJSON are
 * deflated. Errors before the first byte are thrown (with statusCode); later
 * errors abort the response, so the client sees a failed download rather than
 * a silently incomplete archive.
 */
export async function runMultiExport({ format, tables, filename, res }) {
    const spec = EXPORT_FORMATS[format];
    const metric = `zip-${format}`;
    if (active >= exportConcurrency()) {
        count(metric, 'rejected');
        res.setHeader('Retry-After', '10');
        throw Object.assign(new Error(`Too many concurrent exports (EXPORT_CONCURRENCY=${exportConcurrency()}); retry later`), { statusCode: 429 });
    }
    active++;
    const cap = exportMaxRows();
    let conn;
    let tmpFile = null;
    let timer;
    let timedOut = false;
    let entry = null;
    const armTimer = () => {
        clearTimeout(timer);
        timer = setTimeout(() => {
            timedOut = true;
            try { conn.interrupt(); } catch (_) { /* ignore */ }
            if (res.headersSent) res.destroy(new Error('export timed out'));
        }, exportTimeoutMs());
    };
    const onClose = () => { if (!res.writableFinished) conn?.interrupt?.(); };
    res.on('close', onClose);
    try {
        conn = await openDedicatedConnection();
        armTimer();
        await conn.run('BEGIN TRANSACTION');
        // Count first (cheap, from DuckLake metadata) so a broken table fails
        // the request with a JSON error before any byte is sent.
        const plan = [];
        for (const t of tables) {
            const total = Number((await (await conn.run(t.countSql)).getRows())[0][0]);
            plan.push({ ...t, total, rows: Math.min(total, cap) });
        }

        res.status(200);
        res.setHeader('Content-Type', 'application/zip');
        res.setHeader('Content-Disposition', `attachment; filename="${filename}.zip"`);
        res.setHeader('X-Table-Count', String(plan.length));
        res.setHeader('X-Row-Cap', String(cap));

        const zip = new ZipWriter(chunk => write(res, chunk));
        const startedAt = new Date();
        for (const t of plan) {
            armTimer();
            const sql = `${t.selectSql} LIMIT ${cap}`;
            const name = `${t.file}.${spec.ext}`;
            if (format === 'parquet') {
                tmpFile = await copyToParquet(conn, sql);
                entry = await zip.entry(name, { compress: false, date: startedAt });
                for await (const chunk of fs.createReadStream(tmpFile, { highWaterMark: 1 << 20 })) await entry.write(chunk);
                await entry.close();
                await fsp.unlink(tmpFile).catch(() => {});
                tmpFile = null;
            } else {
                entry = await zip.entry(name, { compress: true, date: startedAt });
                await streamText(conn, format, sql, undefined, undefined, text => entry.write(text));
                await entry.close();
            }
            entry = null;
        }
        await conn.run('COMMIT');
        const manifest = {
            exportedAt: startedAt.toISOString(),
            format,
            rowCap: cap,
            consistentSnapshot: true,
            tables: plan.map(t => ({ table: t.name, file: `${t.file}.${spec.ext}`, rows: t.rows, totalRows: t.total, truncated: t.total > t.rows })),
        };
        await zip.add('manifest.json', JSON.stringify(manifest, null, 2) + '\n', { date: startedAt });
        await zip.finish();
        res.end();
        count(metric, 'succeeded');
    } catch (err) {
        if (timedOut) {
            count(metric, 'timeout');
            err = Object.assign(new Error(`Export of one table exceeded the ${exportTimeoutMs()}ms timeout`), { statusCode: 504 });
        } else {
            count(metric, err.cancelled || res.destroyed ? 'cancelled' : 'failed');
        }
        if (entry) entry.abort();
        if (res.headersSent) {
            res.destroy(err);
            return;
        }
        throw err;
    } finally {
        clearTimeout(timer);
        res.off('close', onClose);
        if (conn) { try { conn.closeSync(); } catch (_) { /* ignore */ } }
        if (tmpFile) fsp.unlink(tmpFile).catch(() => {});
        active--;
    }
}
