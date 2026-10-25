/**
 * Bulk file ingestion shared by POST /tables/:tableName/import and the admin
 * console's import (POST /console/api/data/import). The import itself
 * (format readers, create/append/replace, schema evolution) is importFile()
 * in src/console/data.js; this module receives the upload and resolves the
 * source.
 */
import fs from 'fs';
import os from 'os';
import path from 'path';
import { randomUUID } from 'crypto';
import { pipeline } from 'stream/promises';
import { Transform } from 'stream';
import { buildImportSecretSql, importS3Config } from './storage.js';

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

/** Upper bound for one uploaded import file (IMPORT_MAX_MB, editable live in the console). */
export const importMaxBytes = () => Math.max(1, parseInt(process.env.IMPORT_MAX_MB || '1024', 10) || 1024) * 1024 * 1024;
const importTmpDir = () => process.env.IMPORT_TMP_DIR || path.join(os.tmpdir(), 'ichibi-import');

/** A new temp file path for an upload of `format` (the directory is created). */
export async function importTempFile(format) {
    const dir = importTmpDir();
    await fs.promises.mkdir(dir, { recursive: true });
    return path.join(dir, `${randomUUID()}.${format || 'bin'}`);
}

/**
 * Stream a raw request body to `file`, failing with 413 once it exceeds
 * `maxBytes` (checked against Content-Length up front and while streaming),
 * so an upload never sits in memory. `onChunk` sees every chunk (e.g. to hash
 * the body). Returns the number of bytes written.
 */
export async function receiveUpload(req, file, maxBytes = importMaxBytes(), { onChunk = null } = {}) {
    const tooLarge = () => httpError(413, `File too large (limit ${Math.round(maxBytes / 1048576)} MB, IMPORT_MAX_MB)`);
    const declared = Number(req.headers['content-length']);
    if (declared > maxBytes) throw tooLarge();
    let seen = 0;
    const limit = new Transform({
        transform(chunk, enc, cb) {
            seen += chunk.length;
            if (seen > maxBytes) return cb(tooLarge());
            if (onChunk) onChunk(chunk);
            cb(null, chunk);
        },
    });
    await pipeline(req, limit, fs.createWriteStream(file));
    if (seen === 0) throw httpError(400, 'The uploaded file is empty');
    return seen;
}

const CONTENT_TYPE_FORMATS = {
    'text/csv': 'csv',
    'application/csv': 'csv',
    'text/tab-separated-values': 'csv',
    'application/vnd.apache.parquet': 'parquet',
    'application/parquet': 'parquet',
    'application/x-parquet': 'parquet',
    'application/json': 'json',
    'application/x-ndjson': 'json',
    'application/ndjson': 'json',
    'application/jsonl': 'json',
    'application/x-jsonlines': 'json',
};

/** Import format implied by a Content-Type header ('' when it implies none). */
export function contentTypeFormat(contentType) {
    const mime = String(contentType || '').split(';')[0].trim().toLowerCase();
    return CONTENT_TYPE_FORMATS[mime] || '';
}

/**
 * A small JSON body of the form { "source": "s3://...", ... } asks the import
 * endpoint to load from object storage instead of carrying the data itself.
 * Returns the parsed object, or null when the upload is data.
 */
export async function readSourceRequest(file, size) {
    if (size > 64 * 1024) return null;
    const text = (await fs.promises.readFile(file, 'utf8')).trim();
    if (!text.startsWith('{')) return null;
    let body;
    try { body = JSON.parse(text); } catch (_) { return null; }
    if (!body || typeof body !== 'object' || Array.isArray(body) || !Object.prototype.hasOwnProperty.call(body, 'source')) return null;
    return body;
}

/**
 * Connection setup for reading s3:// import sources on the primary instance:
 * httpfs (pre-installed in the Docker image) and the import secret built from
 * the gateway's S3_* settings (src/storage.js). Throws 400 when the S3
 * settings are invalid.
 */
export async function prepareS3Import(conn) {
    let cfg;
    try { cfg = importS3Config(); } catch (err) { throw httpError(400, `Object storage is not configured correctly: ${err.message}`); }
    await conn.run('INSTALL httpfs; LOAD httpfs;');
    if (cfg.provider === 'credential_chain') await conn.run('INSTALL aws; LOAD aws;');
    await conn.run(buildImportSecretSql());
}
