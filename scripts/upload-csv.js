#!/usr/bin/env node
/**
 * Upload a CSV file to the ichibi-lake data lake.
 *
 * Streams the CSV, converts each row into a JSON object keyed by the header row,
 * and POSTs the records to `POST /upload/{tableName}` in batches. The file is
 * read as a stream (twice) so very large files (beyond V8's max string length)
 * work.
 *
 * Type handling: a first pass classifies each column across the whole file. A
 * column is emitted as numbers/booleans only when *every* non-empty value in it
 * qualifies (so genuinely numeric columns stay numeric in the lake); otherwise it
 * is emitted as text. Empty cells in a text column are sent as "" (never null) so
 * a column can never become an all-null column, which DuckDB's read_json_auto
 * would otherwise type as JSON — the cause of "Malformed JSON" cast errors when a
 * later batch sends real text. Integers too large for exact double precision are
 * kept as text to avoid silent rounding.
 *
 * Authentication uses the `x-api-key` header, whose value is read from `API_KEY`
 * in a `.env` file (repo root by default) or the surrounding environment.
 *
 * Usage:
 *   node scripts/upload-csv.js <tableName> <file.csv>
 *
 * Environment:
 *   API_KEY           API key sent as the `x-api-key` header (required).
 *   ICHIBI_LAKE_URL   Gateway base URL (default: http://localhost:3333).
 *   BATCH_SIZE        Rows per request (default: 5000).
 */

import fs from 'node:fs';
import path from 'node:path';

const REPO_ROOT = path.resolve(new URL('.', import.meta.url).pathname, '..');

/** Load KEY=VALUE pairs from a `.env` file without overriding existing env. */
function loadEnv(file) {
    if (!fs.existsSync(file)) return;
    for (const raw of fs.readFileSync(file, 'utf8').split('\n')) {
        const line = raw.trim();
        if (!line || line.startsWith('#')) continue;
        const eq = line.indexOf('=');
        if (eq === -1) continue;
        const key = line.slice(0, eq).trim();
        let val = line.slice(eq + 1).trim();
        if ((val.startsWith('"') && val.endsWith('"')) || (val.startsWith("'") && val.endsWith("'"))) {
            val = val.slice(1, -1);
        }
        if (!(key in process.env)) process.env[key] = val;
    }
}

/**
 * Streaming RFC-4180-ish CSV parser. Handles quotes, escaped quotes (`""`), and
 * embedded commas/newlines, maintaining parser state across chunk boundaries so
 * arbitrarily large files can be processed without buffering the whole file.
 * Call `push(chunk)` for each string chunk and `end()` once; completed rows are
 * delivered to the `onRow` callback as string arrays.
 */
function createCsvParser(onRow) {
    let field = '';
    let row = [];
    let inQuotes = false;
    let pendingQuote = false; // saw a `"` while inQuotes; deciding close vs. escaped `""`
    const endField = () => { row.push(field); field = ''; };
    return {
        push(chunk) {
            for (let i = 0; i < chunk.length; i++) {
                const ch = chunk[i];
                if (pendingQuote) {
                    pendingQuote = false;
                    if (ch === '"') { field += '"'; continue; }
                    inQuotes = false; // real close quote; fall through to handle `ch`
                }
                if (inQuotes) {
                    if (ch === '"') pendingQuote = true;
                    else field += ch;
                    continue;
                }
                if (ch === '"') inQuotes = true;
                else if (ch === ',') endField();
                else if (ch === '\r') { /* ignore */ }
                else if (ch === '\n') { endField(); onRow(row); row = []; }
                else field += ch;
            }
        },
        end() {
            if (field.length > 0 || row.length > 0) { endField(); onRow(row); }
        },
    };
}

/** Stream a CSV file through the parser, invoking `onRow` for every parsed row. */
async function streamCsv(csvPath, onRow, drain) {
    const parser = createCsvParser(onRow);
    const stream = fs.createReadStream(csvPath, { encoding: 'utf8', highWaterMark: 1 << 20 });
    for await (const chunk of stream) {
        parser.push(chunk);
        if (drain) await drain();
    }
    parser.end();
    if (drain) await drain();
}

const INT_RE = /^-?(0|[1-9][0-9]*)$/;
const NUM_RE = /^-?(0|[1-9][0-9]*)(\.[0-9]+)?([eE][+-]?[0-9]+)?$/;
const BOOL_RE = /^(true|false)$/i;

/**
 * First pass: classify every column across the whole file. Returns an array of
 * types ('integer' | 'float' | 'boolean' | 'string') aligned to the header, plus
 * the header itself and the number of data rows seen.
 */
async function classifyColumns(csvPath) {
    let header = null;
    let dataRows = 0;
    const stats = [];
    await streamCsv(csvPath, row => {
        if (header === null) {
            header = row.map(h => h.trim());
            for (let i = 0; i < header.length; i++) stats[i] = { hasValue: false, allInt: true, allNum: true, allBool: true };
            return;
        }
        dataRows += 1;
        for (let i = 0; i < header.length; i++) {
            const t = (row[i] ?? '').trim();
            if (t === '') continue;
            const s = stats[i];
            s.hasValue = true;
            if (s.allBool && !BOOL_RE.test(t)) s.allBool = false;
            if (s.allInt || s.allNum) {
                if (INT_RE.test(t)) {
                    if (!Number.isSafeInteger(Number(t))) { s.allInt = false; s.allNum = false; } // keep as text (precision)
                } else if (NUM_RE.test(t)) {
                    s.allInt = false; // decimal/exponent -> not an integer
                } else {
                    s.allInt = false; s.allNum = false; // non-numeric
                }
            }
        }
    });
    const types = stats.map(s => {
        if (!s || !s.hasValue) return 'string';
        if (s.allBool) return 'boolean';
        if (s.allInt) return 'integer';
        if (s.allNum) return 'float';
        return 'string';
    });
    return { header, types, dataRows };
}

/** Convert a raw cell to its typed JSON value. Text columns keep "" (never null). */
function coerceCell(raw, type) {
    const v = raw ?? '';
    if (type === 'string') return v;
    const t = v.trim();
    if (t === '') return null;
    if (type === 'boolean') return /^true$/i.test(t);
    return Number(t); // integer | float
}

async function postBatch(url, apiKey, records) {
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json; charset=utf-8', 'x-api-key': apiKey },
        body: JSON.stringify(records),
    });
    const text = await res.text();
    if (!res.ok) throw new Error(`HTTP ${res.status}: ${text}`);
    return text;
}

async function main() {
    const [tableName, csvPath] = process.argv.slice(2);
    if (!tableName || !csvPath) {
        console.error('Usage: node scripts/upload-csv.js <tableName> <file.csv>');
        process.exit(1);
    }

    loadEnv(path.join(REPO_ROOT, '.env'));
    const apiKey = process.env.API_KEY;
    if (!apiKey) {
        console.error('Error: API_KEY is not set. Add it to a .env file or the environment.');
        process.exit(1);
    }

    if (!fs.existsSync(csvPath)) {
        console.error(`Error: CSV file not found: ${csvPath}`);
        process.exit(1);
    }

    const baseUrl = (process.env.ICHIBI_LAKE_URL || 'http://localhost:3333').replace(/\/+$/, '');
    const batchSize = Math.max(1, Number(process.env.BATCH_SIZE) || 5000);
    const url = `${baseUrl}/upload/${encodeURIComponent(tableName)}`;

    console.log(`Scanning "${csvPath}" to infer column types...`);
    const { header, types, dataRows } = await classifyColumns(csvPath);
    if (header === null || dataRows === 0) {
        console.error('Error: CSV must contain a header row and at least one data row.');
        process.exit(1);
    }
    const typed = header.map((h, i) => `${h}:${types[i]}`).filter((_, i) => types[i] !== 'string');
    console.log(`Typed columns: ${typed.length ? typed.join(', ') : '(none; all text)'}`);
    console.log(`Uploading ${dataRows} row(s) to "${tableName}" at ${url}`);

    let firstRow = true;
    let buffer = [];
    let sent = 0;
    let batchNum = 0;
    const pending = [];

    const flush = async () => {
        const batch = buffer;
        buffer = [];
        batchNum += 1;
        const n = batchNum;
        const result = await postBatch(url, apiKey, batch);
        sent += batch.length;
        console.log(`  batch ${n}: ${batch.length} rows -> ${result}`);
    };
    const drain = async () => { if (pending.length) await Promise.all(pending.splice(0)); };

    await streamCsv(csvPath, row => {
        if (firstRow) { firstRow = false; return; } // skip header
        const obj = {};
        header.forEach((col, idx) => { obj[col] = coerceCell(row[idx], types[idx]); });
        buffer.push(obj);
        if (buffer.length >= batchSize) pending.push(flush());
    }, drain);

    if (buffer.length) await flush();
    await drain();

    console.log(`Done. Uploaded ${sent} record(s) in ${batchNum} batch(es).`);
}

main().catch(err => {
    console.error(`Upload failed: ${err.message}`);
    process.exit(1);
});
