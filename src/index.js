import express from 'express';
import cors from 'cors';
import compression from 'compression';
import fs from 'fs/promises';
import fsSync from 'fs';
import path from 'path';
import { randomUUID } from 'crypto';
import {
    withConnection,
    execute,
    executeWithParams,
    executeUserSql,
    getTableColumns,
    invalidateColumnCache,
    compactDuckLake,
    VARCHAR,
    BLOB,
    blobValue,
} from './db.js';
import { resolveTypeConflict, formatSchemaDefinition } from './schema-evolution.js';
import { startKafkaConsumer } from './kafka-consumer.js';
import { getKafka } from './kafka-producer.js';
import { migrateTimestamps } from './migrate-timestamps.js';
import { initAclTable, ensureTableAcl, checkReadAccess, checkWriteAccess, getTableAcl, updateTableAcl, invalidateAclCache, migrateToAcl } from './acl.js';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import { createHandler } from 'graphql-http/lib/use/express';
import { buildSchema } from 'graphql';

// Support BigInt serialization in JSON
BigInt.prototype.toJSON = function () { return this.toString(); };

const app = express();
const port = process.env.PORT || 3000;

app.use(cors());
app.use(compression());
// JSON body limit. Previously 2gb, which is large enough to let a single
// request exhaust the Node heap and trigger long GC pauses that look like
// the whole gateway hanging. Configure via JSON_BODY_LIMIT for stress runs.
const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || '50mb';
app.use(express.json({ limit: JSON_BODY_LIMIT }));
console.log(`[Server] JSON body limit: ${JSON_BODY_LIMIT}`);

// Identifier guard for table/column names received from URL paths or query
// parameters. Mirrors the existing isSafeIdentifier() check used for graph
// names but is applied uniformly across all SQL-building routes to remove
// the SQL-injection surface created by string-concatenated identifiers.
const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
function assertSafeIdentifier(name, kind = 'identifier') {
    if (typeof name !== 'string' || !SAFE_IDENTIFIER_RE.test(name)) {
        const err = new Error(`Invalid ${kind}: ${JSON.stringify(name)}`);
        err.statusCode = 400;
        throw err;
    }
    return name;
}

function requireTableAcl(tableName) {
    // Returns a middleware-like check for optional use in route handlers.
}

/**
 * Throw a 403 error if the requesting API key is not in the table's read ACL.
 */
async function requireReadAccess(tableName, req) {
    const hasAccess = await checkReadAccess(tableName, req.apiKey);
    if (!hasAccess) {
        const err = new Error(`Access denied: no read permission for table "${tableName}"`);
        err.statusCode = 403;
        throw err;
    }
}

/**
 * Throw a 403 error if the requesting API key is not in the table's write ACL.
 */
async function requireWriteAccess(tableName, req) {
    const hasAccess = await checkWriteAccess(tableName, req.apiKey);
    if (!hasAccess) {
        const err = new Error(`Access denied: no write permission for table "${tableName}"`);
        err.statusCode = 403;
        throw err;
    }
}

// --- Blob Storage Configuration ---
const USE_FILESYSTEM_BLOBS = process.env.USE_FILESYSTEM_BLOBS === '1';
const BLOBS_PATH = process.env.BLOBS_PATH || './data/blobs';

// --- Batch Ingest Guard ---
// Hard ceiling on how many rows a single /upload request may contain.
// Prevents the gateway from OOM-ing when clients accidentally (or maliciously)
// send massive arrays. Configure via MAX_BATCH_SIZE.
const MAX_BATCH_SIZE = Math.max(1, parseInt(process.env.MAX_BATCH_SIZE || '10000', 10));

// --- Read-only SQL enforcement ---
// User-supplied SQL endpoints (POST /query, the /graphql `sql` resolver and
// POST /graphs/:graphName/query) are intended to be read-only per swagger.yaml.
// Set SQL_READ_ONLY=false to disable the guard (e.g. for ops/migration work).
const SQL_READ_ONLY = String(process.env.SQL_READ_ONLY ?? 'true').toLowerCase() !== 'false';

// Single regex used for every user-supplied SQL path. Matches a write/DDL/admin
// keyword as a whole word so SELECTs containing the word inside a string
// literal (which is stripped first by stripSqlLiterals) are not blocked.
const FORBIDDEN_SQL_RE = /\b(CREATE|DROP|INSERT|UPDATE|DELETE|ALTER|ATTACH|DETACH|COPY|TRUNCATE|MERGE|REPLACE|GRANT|REVOKE|VACUUM|CHECKPOINT|INSTALL|LOAD|EXPORT|IMPORT|CALL|PRAGMA|SET|RESET|USE)\b/i;

// Strip SQL comments and quoted literals so the keyword scan only sees
// executable tokens. This keeps queries like SELECT 'DROP me' FROM t legal.
function stripSqlLiterals(sql) {
    return String(sql)
        .replace(/--[^\n]*/g, ' ')
        .replace(/\/\*[\s\S]*?\*\//g, ' ')
        .replace(/'(?:''|[^'])*'/g, "''")
        .replace(/"(?:""|[^"])*"/g, '""');
}

// Throws a 403-tagged Error when the SQL contains a forbidden keyword and the
// guard is enabled. Callers should translate err.statusCode into res.status.
function assertReadOnlySql(sql) {
    if (!SQL_READ_ONLY) return;
    const scrubbed = stripSqlLiterals(sql || '');
    const m = scrubbed.match(FORBIDDEN_SQL_RE);
    if (m) {
        const err = new Error(
            `Read-only enforcement: statement "${m[0].toUpperCase()}" is not allowed. ` +
            `Set SQL_READ_ONLY=false in the environment to disable this guard.`
        );
        err.statusCode = 403;
        throw err;
    }
}

console.log(`[Security] SQL_READ_ONLY=${SQL_READ_ONLY} (user-supplied SQL endpoints ${SQL_READ_ONLY ? 'will reject' : 'will accept'} DDL/DML).`);

// Ensure the blobs directory exists at startup when filesystem mode is enabled.
if (USE_FILESYSTEM_BLOBS) {
    fsSync.mkdirSync(BLOBS_PATH, { recursive: true });
    console.log(`[Blobs] Filesystem blob storage enabled. Directory: ${BLOBS_PATH}`);
}

// --- 1. Helper Functions ---

async function migrateExistingTables() {
    // No-op: ACL system replaces the _api_key column approach.
    // Existing tables are migrated by migrateToAcl().
}

/**
 * Apply DuckLake partitioning to a freshly-created table. By default we
 * partition by the `created` column. Override with DEFAULT_PARTITION_KEY=none
 * to disable, or with a comma-separated list of safe identifiers.
 *
 * Best-effort: any failure here is logged but does not fail the request,
 * because partitioning is a performance optimisation, not a correctness
 * requirement.
 */
const DEFAULT_PARTITION_KEY_RAW = (process.env.DEFAULT_PARTITION_KEY ?? 'created').trim();
async function applyDefaultPartitioning(tableName) {
    if (!DEFAULT_PARTITION_KEY_RAW || DEFAULT_PARTITION_KEY_RAW.toLowerCase() === 'none') return;
    const requested = DEFAULT_PARTITION_KEY_RAW.split(',').map(s => s.trim()).filter(Boolean);
    if (requested.length === 0) return;

    try {
        const cols = new Set(await getTableColumns(tableName));
        const present = requested.filter(c => SAFE_IDENTIFIER_RE.test(c) && cols.has(c));
        if (present.length === 0) return;
        const partitionClause = present.map(c => `"${c}"`).join(', ');
        await execute(`ALTER TABLE "${tableName}" SET PARTITIONED BY (${partitionClause})`);
        console.log(`[Partition] ${tableName} partitioned by (${present.join(', ')})`);
    } catch (err) {
        console.warn(`[Partition] Skipping partitioning on ${tableName}:`, err.message || err);
    }
}

// --- 2. Public Static Assets ---
app.get('/swagger.yaml', (req, res) => {
    res.setHeader('Content-Type', 'text/yaml');
    res.sendFile(path.join(process.cwd(), 'swagger.yaml'));
});

// --- 3. Authenticated API Routes ---

/**
 * Parse valid API keys from environment.
 */
function getValidApiKeys() {
    const multi = process.env.API_KEYS;
    if (multi) return new Set(multi.split(',').map(k => k.trim()).filter(Boolean));
    const single = process.env.API_KEY;
    if (single) return new Set([single]);
    return new Set();
}

app.use(['/graphql', '/query', '/kafka-sink', '/kafka-subscribe/:topicName', '/upload', '/upload/:tableName', '/tables', '/tables/:tableName', '/tables/:tableName/schema', '/tables/:tableName/blobs/:idValue/:blobColumn', '/blobs/:idValue/:blobColumn', '/tables/:tableName/blobs/:blobColumn', '/blobs/:blobColumn', '/tables/:tableName/records/:idValue', '/records/:idValue', '/graphs', '/graphs/:graphName', '/graphs/:graphName/query'], (req, res, next) => {
    const validKeys = getValidApiKeys();
    if (validKeys.size > 0) {
        const providedKey = req.headers['x-api-key'];
        if (!providedKey || !validKeys.has(providedKey)) {
            console.error(`[Auth] 401 Unauthorized from ${req.ip}. Rejected x-api-key: ${providedKey || 'NONE'}`);
            return res.status(401).json({ error: 'Unauthorized: Missing or invalid x-api-key' });
        }
        req.apiKey = providedKey;
    }
    next();
});

// POST /blobs/:blobColumn - Upload raw binary BLOB (default table, generated ID)
// POST /blobs/:idValue/:blobColumn - Upload raw binary BLOB (default table)
// POST /tables/:tableName/blobs/:blobColumn - Upload raw binary BLOB (generated ID)
// POST /tables/:tableName/blobs/:idValue/:blobColumn - Upload raw binary BLOB
app.post(['/tables/:tableName/blobs/:idValue/:blobColumn', '/blobs/:idValue/:blobColumn', '/tables/:tableName/blobs/:blobColumn', '/blobs/:blobColumn'], express.raw({ type: '*/*', limit: '2gb' }), async (req, res) => {
    try {
        const tableName = req.params.tableName || 'ichibi_table';
        const { blobColumn } = req.params;
        let { idValue } = req.params;
        const { idColumn = 'id', ...metadata } = req.query;
        const binaryData = req.body;

        assertSafeIdentifier(tableName, 'tableName');
        assertSafeIdentifier(blobColumn, 'blobColumn');
        assertSafeIdentifier(idColumn, 'idColumn');
        for (const k of Object.keys(metadata)) assertSafeIdentifier(k, 'metadata key');

        if (!idValue) {
            idValue = randomUUID();
        }

        if (!binaryData || binaryData.length === 0) {
            return res.status(400).json({ error: 'Empty binary data' });
        }

        // When filesystem mode is enabled, the blob column stores a file path (VARCHAR).
        // Column type checking/adjustment is handled consistently below.
        await withConnection(async (conn) => {
            // 0. Check table existence and ACL before proceeding.
            const tableExistsCheck = await executeWithParams(
                `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
                [tableName], [VARCHAR]
            );
            const tableExists = tableExistsCheck[0] && parseInt(tableExistsCheck[0].count) > 0;
            if (tableExists) {
                await requireWriteAccess(tableName, req);
            }

            // 1. Ensure row and metadata columns exist (shared logic for both modes).
            const checkResult = await executeWithParams(
                `SELECT count(*) as count FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                [tableName, blobColumn], [VARCHAR, VARCHAR]
            );
            const colExists = checkResult[0] && parseInt(checkResult[0].count) > 0;

            let tableCreated = false;
            if (!colExists) {
                if (tableExists) {
                    const colType = USE_FILESYSTEM_BLOBS ? 'VARCHAR' : 'BLOB';
                    await execute(`ALTER TABLE "${tableName}" ADD COLUMN "${blobColumn}" ${colType}`);

                    for (const metaCol of ['created', 'updated']) {
                        const metaColCheck = await executeWithParams(
                            `SELECT count(*) as count FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                            [tableName, metaCol], [VARCHAR, VARCHAR]
                        );
                        if (!metaColCheck[0] || parseInt(metaColCheck[0].count) === 0) {
                            await execute(`ALTER TABLE "${tableName}" ADD COLUMN "${metaCol}" TIMESTAMP`);
                        }
                    }

                    invalidateColumnCache(tableName);
                } else {
                    const colType = USE_FILESYSTEM_BLOBS ? 'VARCHAR' : 'BLOB';
                    await execute(`CREATE TABLE "${tableName}" ("${idColumn}" VARCHAR, "${blobColumn}" ${colType}, "created" TIMESTAMP, "updated" TIMESTAMP)`);
                    tableCreated = true;
                    await ensureTableAcl(tableName, req.apiKey);
                    invalidateColumnCache(tableName);
                }
            }

            // 1b. Ensure metadata columns exist
            for (const metaKey of Object.keys(metadata)) {
                const metaCheck = await executeWithParams(
                    `SELECT count(*) as count FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                    [tableName, metaKey], [VARCHAR, VARCHAR]
                );
                if (!metaCheck[0] || parseInt(metaCheck[0].count) === 0) {
                    await execute(`ALTER TABLE "${tableName}" ADD COLUMN "${metaKey}" VARCHAR`);
                    invalidateColumnCache(tableName);
                    console.log(`[Metadata] Added column ${metaKey} to ${tableName}`);
                }
            }

            // 1c. Apply default partitioning on freshly-created tables.
            if (tableCreated) {
                await applyDefaultPartitioning(tableName);
            }

            // 2. Check if record exists
            let existSql = `SELECT count(*) as count FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
            const existParams = [idValue];
            const existTypes = [VARCHAR];
            const existRes = await executeWithParams(existSql, existParams, existTypes);
            const recordExists = existRes[0] && parseInt(existRes[0].count) > 0;

            // 3. Persist blob data
            if (USE_FILESYSTEM_BLOBS) {
                // --- Filesystem mode: write binary data to a file, store the file path ---
                const filePath = path.join(BLOBS_PATH, idValue);
                await fs.writeFile(filePath, Buffer.from(binaryData));
                console.log(`[Blobs] Saved file to filesystem: ${filePath} (${binaryData.length} bytes)`);

                if (recordExists) {
                    // UPDATE: set blob file path + metadata
                    const now = new Date().toISOString();
                    const setClauses = [`"${blobColumn}" = ?`, `"updated" = ?`];
                    const params = [filePath, now];
                    const types = [VARCHAR, VARCHAR];

                    for (const [key, value] of Object.entries(metadata)) {
                        setClauses.push(`"${key}" = ?`);
                        params.push(String(value));
                        types.push(VARCHAR);
                    }

                    let updateSql = `UPDATE "${tableName}" SET ${setClauses.join(', ')} WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
                    params.push(idValue);
                    types.push(VARCHAR);

                    await conn.run(updateSql, params, types);
                } else {
                    // Ensure idColumn is VARCHAR to support UUID-based blob IDs
                    const idColCheck = await executeWithParams(
                        `SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                        [tableName, idColumn], [VARCHAR, VARCHAR]
                    );
                    const idColType = idColCheck[0]?.data_type;
                    if (idColType && (idColType.includes('INT') || idColType.includes('FLOAT') || idColType.includes('DOUBLE'))) {
                        console.log(`[Blobs] Converting ${idColumn} from ${idColType} to VARCHAR to accept UUID-based IDs`);
                        await conn.run(`ALTER TABLE "${tableName}" ALTER COLUMN "${idColumn}" SET DATA TYPE VARCHAR`);
                        invalidateColumnCache(tableName);
                    }

                    // INSERT: store file path + metadata
                    const now = new Date().toISOString();
                    const columns = [idColumn, blobColumn, 'created', 'updated'];
                    const placeholders = ['?', '?', '?', '?'];
                    const params = [idValue, filePath, now, now];
                    const types = [VARCHAR, VARCHAR, VARCHAR, VARCHAR];

                    for (const [key, value] of Object.entries(metadata)) {
                        columns.push(key);
                        placeholders.push('?');
                        params.push(String(value));
                        types.push(VARCHAR);
                    }

                    const insertSql = `INSERT INTO "${tableName}" ("${columns.join('", "')}") VALUES (${placeholders.join(', ')})`;
                    await conn.run(insertSql, params, types);
                }
            } else {
                // --- Legacy mode: inline BLOB in Parquet (original behaviour) ---
                // Reuse the same connection for both filesystem and legacy paths.
                // Opening a second raw connection concurrently causes DuckLake
                // catalog snapshot races ("Table with name ducklake_inlined_data_
                // ... does not exist" errors).
                if (recordExists) {
                    const now = new Date().toISOString();
                    const setClauses = [`"${blobColumn}" = ?`, `"updated" = ?`];
                    const params = [blobValue(binaryData), now];
                    const types = [BLOB, VARCHAR];

                    for (const [key, value] of Object.entries(metadata)) {
                        setClauses.push(`"${key}" = ?`);
                        params.push(String(value));
                        types.push(VARCHAR);
                    }

                    let updateSql = `UPDATE "${tableName}" SET ${setClauses.join(', ')} WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
                    params.push(idValue);
                    types.push(VARCHAR);

                    await conn.run(updateSql, params, types);
                } else {
                    // Ensure idColumn is VARCHAR to support UUID-based blob IDs
                    const idColCheck = await executeWithParams(
                        `SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                        [tableName, idColumn], [VARCHAR, VARCHAR]
                    );
                    const idColType = idColCheck[0]?.data_type;
                    if (idColType && (idColType.includes('INT') || idColType.includes('FLOAT') || idColType.includes('DOUBLE'))) {
                        console.log(`[Blobs] Converting ${idColumn} from ${idColType} to VARCHAR to accept UUID-based IDs`);
                        await conn.run(`ALTER TABLE "${tableName}" ALTER COLUMN "${idColumn}" SET DATA TYPE VARCHAR`);
                        invalidateColumnCache(tableName);
                    }

                    const now = new Date().toISOString();
                    const columns = [idColumn, blobColumn, 'created', 'updated'];
                    const placeholders = ['?', '?', '?', '?'];
                    const params = [idValue, blobValue(binaryData), now, now];
                    const types = [VARCHAR, BLOB, VARCHAR, VARCHAR];

                    for (const [key, value] of Object.entries(metadata)) {
                        columns.push(key);
                        placeholders.push('?');
                        params.push(String(value));
                        types.push(VARCHAR);
                    }

                    const insertSql = `INSERT INTO "${tableName}" ("${columns.join('", "')}") VALUES (${placeholders.join(', ')})`;
                    await conn.run(insertSql, params, types);
                }
            }
        });

        res.json({ success: true, message: `BLOB uploaded to ${tableName}.${blobColumn}`, id: idValue });
    } catch (error) {
        console.error('Blob upload error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /blobs/:idValue/:blobColumn - Stream raw binary BLOB (default table)
// GET /tables/:tableName/blobs/:idValue/:blobColumn - Stream raw binary BLOB
app.get(['/tables/:tableName/blobs/:idValue/:blobColumn', '/blobs/:idValue/:blobColumn'], async (req, res) => {
    try {
        const tableName = req.params.tableName || 'ichibi_table';
        const { idValue, blobColumn } = req.params;
        const { idColumn = 'id' } = req.query;

        assertSafeIdentifier(tableName, 'tableName');
        assertSafeIdentifier(blobColumn, 'blobColumn');
        assertSafeIdentifier(idColumn, 'idColumn');
        await requireReadAccess(tableName, req);

        if (USE_FILESYSTEM_BLOBS) {
            // --- Filesystem mode: read the file stored at the path in the blob column ---
            let sql = `SELECT "${blobColumn}" AS _path FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
            const params = [idValue];
            const types = [VARCHAR];

            const rows = await executeWithParams(sql, params, types);
            if (rows.length === 0) return res.status(404).json({ error: 'Record not found' });

            const filePath = rows[0]._path;
            if (!filePath) return res.status(404).json({ error: 'BLOB path is empty (no file stored for this record)' });

            const absolutePath = path.resolve(filePath);
            try {
                const stat = await fs.stat(absolutePath);
                const buffer = Buffer.from(await fs.readFile(absolutePath));
                console.log(`[Blobs] Streaming file from filesystem: ${absolutePath} (${stat.size} bytes)`);
                res.setHeader('Content-Type', 'application/octet-stream');
                res.setHeader('Content-Length', stat.size);
                return res.send(buffer);
            } catch (err) {
                console.error(`[Blobs] File not found: ${absolutePath}`, err.message);
                return res.status(404).json({ error: 'BLOB file not found on disk', path: absolutePath });
            }
        }

        // --- Legacy mode: BLOB stored inline in Parquet (original behaviour) ---
        let sql = `SELECT "${blobColumn}" FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
        const params = [idValue];
        const types = [VARCHAR];

        const rows = await executeWithParams(sql, params, types);
        if (rows.length === 0) return res.status(404).json({ error: 'Record not found' });

        const blobData = rows[0][blobColumn];
        if (blobData === null || blobData === undefined) return res.status(404).json({ error: 'BLOB is empty' });

        // If the value is already Base64 encoded (from our execute conversion), decode it for the stream
        const buffer = Buffer.from(blobData, 'base64');

        res.setHeader('Content-Type', 'application/octet-stream');
        res.setHeader('Content-Length', buffer.length);
        res.send(buffer);
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// DELETE /blobs/:idValue/:blobColumn - Delete record after clearing BLOB (default table)
// DELETE /tables/:tableName/blobs/:idValue/:blobColumn - Delete record after clearing BLOB
//
// Removes the backing file from disk when USE_FILESYSTEM_BLOBS=1, then
// deletes the entire record from the table. Returns 404 when the record
// is not visible to the caller's api key.
app.delete(['/tables/:tableName/blobs/:idValue/:blobColumn', '/blobs/:idValue/:blobColumn'], async (req, res) => {
    try {
        const tableName = req.params.tableName || 'ichibi_table';
        const { idValue, blobColumn } = req.params;
        const { idColumn = 'id' } = req.query;

        assertSafeIdentifier(tableName, 'tableName');
        assertSafeIdentifier(blobColumn, 'blobColumn');
        assertSafeIdentifier(idColumn, 'idColumn');
        await requireWriteAccess(tableName, req);

        // 1. Fetch the current blob value so we can clean up the on-disk
        // file in filesystem mode. Also serves as the existence check.
        let selectSql = `SELECT "${blobColumn}" AS _value FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
        const selectParams = [idValue];
        const selectTypes = [VARCHAR];

        const rows = await executeWithParams(selectSql, selectParams, selectTypes);
        if (rows.length === 0) return res.status(404).json({ success: false, error: 'Record not found' });

        const currentValue = rows[0]._value;

        // 2. Best-effort file removal when in filesystem mode. We tolerate
        // ENOENT so retries / partial state do not error out the caller.
        if (USE_FILESYSTEM_BLOBS && typeof currentValue === 'string' && currentValue.length > 0) {
            const absolutePath = path.resolve(currentValue);
            try {
                await fs.unlink(absolutePath);
                console.log(`[Blobs] Deleted file from filesystem: ${absolutePath}`);
            } catch (err) {
                if (err.code !== 'ENOENT') {
                    console.warn(`[Blobs] Failed to unlink ${absolutePath}:`, err.message || err);
                }
            }
        }

        // 3. Delete the entire record (no longer just NULL out the blob column).
        await withConnection(async (conn) => {
            let deleteSql = `DELETE FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
            const params = [idValue];
            const types = [VARCHAR];
            await conn.run(deleteSql, params, types);
        });

        res.json({ success: true, message: `Record ${idValue} deleted from ${tableName}` });
    } catch (error) {
        console.error('Blob delete error:', error);
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

/**
 * PATCH /tables/:tableName/records/:idValue
 * PATCH /records/:idValue
 * Updates arbitrary metadata for a specific record.
 */
app.patch(['/tables/:tableName/records/:idValue', '/records/:idValue'], async (req, res) => {
    try {
        const tableName = req.params.tableName || 'ichibi_table';
        const { idValue } = req.params;
        const { idColumn = 'id' } = req.query;
        const metadata = req.body;

        if (!metadata || typeof metadata !== 'object' || Array.isArray(metadata)) {
            return res.status(400).json({ error: 'Expected valid JSON object of properties to update' });
        }

        assertSafeIdentifier(tableName, 'tableName');
        assertSafeIdentifier(idColumn, 'idColumn');
        for (const k of Object.keys(metadata)) assertSafeIdentifier(k, 'metadata key');
        await requireWriteAccess(tableName, req);

        // 1. Ensure table and columns exist
        const tableCheck = await executeWithParams(
            `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
            [tableName], [VARCHAR]
        );
        if (!tableCheck[0] || parseInt(tableCheck[0].count) === 0) {
            return res.status(404).json({ error: `Table ${tableName} not found` });
        }

        const existingCols = new Set(await getTableColumns(tableName));
        const keys = Object.keys(metadata);
        for (const key of keys) {
            if (!existingCols.has(key)) {
                await execute(`ALTER TABLE "${tableName}" ADD COLUMN "${key}" VARCHAR`);
                invalidateColumnCache(tableName);
                console.log(`[Metadata] Added column ${key} to ${tableName} via PATCH`);
            }
        }

        // 2. Perform update
        await withConnection(async (conn) => {
            const setClauses = [];
            const params = [];
            const types = [];

            const now = new Date().toISOString();
            setClauses.push(`"updated" = ?`);
            params.push(now);
            types.push(VARCHAR);

            for (const [key, value] of Object.entries(metadata)) {
                setClauses.push(`"${key}" = ?`);
                params.push(String(value));
                types.push(VARCHAR);
            }

            if (setClauses.length === 1) {
                return res.status(400).json({ error: 'No properties provided for update' });
            }

            let updateSql = `UPDATE "${tableName}" SET ${setClauses.join(', ')} WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
            params.push(idValue);
            types.push(VARCHAR);

            const result = await conn.run(updateSql, params, types);
            res.json({ success: true, message: `Record ${idValue} updated in ${tableName}`, updatedFields: keys });
        });
    } catch (error) {
        console.error('Update error:', error);
        res.status(500).json({ success: false, error: error.message });
    }
});

/**
 * DELETE /tables/:tableName/records/:idValue
 * DELETE /records/:idValue
 * Deletes a specific record from a table.
 */
app.delete(['/tables/:tableName/records/:idValue', '/records/:idValue'], async (req, res) => {
    try {
        const tableName = req.params.tableName || 'ichibi_table';
        const { idValue } = req.params;
        const { idColumn = 'id' } = req.query;

        assertSafeIdentifier(tableName, 'tableName');
        assertSafeIdentifier(idColumn, 'idColumn');
        await requireWriteAccess(tableName, req);

        const tableCheck = await executeWithParams(
            `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
            [tableName], [VARCHAR]
        );
        if (!tableCheck[0] || parseInt(tableCheck[0].count) === 0) {
            return res.status(404).json({ error: `Table ${tableName} not found` });
        }

        let deleteSql = `DELETE FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`;
        const params = [idValue];
        const types = [VARCHAR];

        const result = await executeWithParams(deleteSql, params, types);
        res.json({ success: true, message: `Record ${idValue} deleted from ${tableName}` });
    } catch (error) {
        console.error('Delete error:', error);
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// GET /tables - List all tables
app.get('/tables', async (req, res) => {
    try {
        const dbList = await execute('PRAGMA database_list');
        console.log(`[Diagnostic] Database List: ${JSON.stringify(dbList)}`);

        const rows = await execute('SHOW TABLES');
        const uniqueTables = [...new Set(rows.map(r => r.name))];
        res.json({ success: true, tables: uniqueTables });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /tables/:tableName/schema - Get table schema
app.get('/tables/:tableName/schema', async (req, res) => {
    try {
        const { tableName } = req.params;
        assertSafeIdentifier(tableName, 'tableName');
        const rows = await executeWithParams(
            `SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = ?`,
            [tableName], [VARCHAR]
        );
        if (rows.length === 0) {
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        await requireReadAccess(tableName, req);
        res.json({ success: true, tableName, schema: rows });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// GET /tables/:tableName/acl - Read table ACL
app.get('/tables/:tableName/acl', async (req, res) => {
    try {
        const { tableName } = req.params;
        assertSafeIdentifier(tableName, 'tableName');
        const acl = await getTableAcl(tableName);
        if (!acl) {
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        await requireWriteAccess(tableName, req);
        const ownerCount = JSON.parse(acl.owner_keys || '[]').length;
        const readerCount = JSON.parse(acl.reader_keys || '[]').length;
        res.json({
            success: true,
            tableName,
            ownerKeys: JSON.parse(acl.owner_keys || '[]'),
            readerKeys: JSON.parse(acl.reader_keys || '[]'),
            ownerCount,
            readerCount,
        });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// PATCH /tables/:tableName/acl - Update table ACL
app.patch('/tables/:tableName/acl', async (req, res) => {
    try {
        const { tableName } = req.params;
        const { ownerKeys, readerKeys } = req.body;
        assertSafeIdentifier(tableName, 'tableName');
        if (!Array.isArray(ownerKeys) || !Array.isArray(readerKeys)) {
            return res.status(400).json({ success: false, error: 'ownerKeys and readerKeys must be arrays of strings' });
        }
        await requireWriteAccess(tableName, req);
        await updateTableAcl(tableName, ownerKeys, readerKeys);
        res.json({ success: true, message: `ACL updated for table "${tableName}"` });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// GET /tables/:tableName - Query table data
//
// Supported query parameters:
//   limit            page size (default 50)
//   offset           offset for classic LIMIT/OFFSET pagination
//   sort             sort column; prefix with - for DESC (e.g. ?sort=-created_at)
//   after            cursor value for keyset pagination; requires `sort`
//                    and skips rows where the sort column is <= (ASC) /
//                    >= (DESC) the supplied value. Avoids the O(offset)
//                    scan cost of deep LIMIT/OFFSET paging.
//   <column>=<val>   simple column-equality filters (parameter-bound)
app.get('/tables/:tableName', async (req, res) => {
    try {
        const { tableName } = req.params;
        assertSafeIdentifier(tableName, 'tableName');
        const { limit = 50, offset = 0, sort, after, ...filters } = req.query;

        // 1. Resolve column list from the cache instead of issuing a per-request
        // `SELECT * LIMIT 0` probe against the Parquet table.
        const columns = await getTableColumns(tableName);
        if (!columns || columns.length === 0) {
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        const columnSet = new Set(columns);
        await requireReadAccess(tableName, req);

        const whereClauses = [];
        const params = [];
        const types = [];

        // 2. Simple column equality filters
        for (const [key, value] of Object.entries(filters)) {
            if (columnSet.has(key)) {
                whereClauses.push(`"${key}" = ?`);
                params.push(String(value));
                types.push(VARCHAR);
            }
        }

        // 4. Sorting + optional keyset cursor
        let sortColName = null;
        let isDesc = false;
        if (sort) {
            isDesc = sort.startsWith('-');
            const colName = isDesc ? sort.substring(1) : sort;
            if (columnSet.has(colName)) sortColName = colName;
        }

        if (after !== undefined) {
            if (!sortColName) {
                return res.status(400).json({
                    success: false,
                    error: 'Keyset pagination ?after=... requires a valid ?sort=<column>',
                });
            }
            // ASC: row > cursor; DESC: row < cursor.
            whereClauses.push(`"${sortColName}" ${isDesc ? '<' : '>'} ?`);
            params.push(String(after));
            types.push(VARCHAR);
        }

        let sql = `SELECT * FROM "${tableName}"`;
        if (whereClauses.length > 0) sql += ` WHERE ${whereClauses.join(' AND ')}`;
        if (sortColName) sql += ` ORDER BY "${sortColName}" ${isDesc ? 'DESC' : 'ASC'}`;

        // 5. Pagination. With keyset (`after`) we drop OFFSET entirely so
        // deep pages stay O(limit) instead of O(offset+limit).
        const safeLimit = Math.max(0, parseInt(limit) || 0);
        const safeOffset = Math.max(0, parseInt(offset) || 0);
        sql += ` LIMIT ${safeLimit}`;
        if (after === undefined && safeOffset > 0) sql += ` OFFSET ${safeOffset}`;

        const rows = await executeWithParams(sql, params, types);

        // Surface a next-cursor when keyset paging is in use, so callers
        // don't have to know which column to read.
        let nextCursor = null;
        if (sortColName && rows.length === safeLimit) {
            const last = rows[rows.length - 1];
            nextCursor = last && last[sortColName] != null ? String(last[sortColName]) : null;
        }

        res.json({ success: true, tableName, rowCount: rows.length, rows, nextCursor });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// POST /query - Raw SQL query (read-only, isolated DuckDB instance with timeout)
app.post('/query', async (req, res) => {
    try {
        const { sql } = req.body;
        if (!sql) return res.status(400).json({ error: 'Missing sql in body' });
        assertReadOnlySql(sql);
        const rows = await executeUserSql(sql);
        res.json({ success: true, rows });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// ANY /graphql - DuckDB GraphQL endpoint
const schema = buildSchema(`
  scalar JSON

  type Query {
    sql(query: String!): JSON
    tables: [String]
  }
`);

const root = {
    sql: async ({ query }) => {
        try {
            assertReadOnlySql(query);
            // Routed through the isolated user instance so a runaway query
            // cannot block the primary REST/ingest connection pool.
            return await executeUserSql(query);
        } catch (error) {
            throw new Error(error.message);
        }
    },
    tables: async () => {
        try {
            const rows = await execute('SHOW TABLES');
            return [...new Set(rows.map(r => r.name))];
        } catch (error) {
            throw new Error(error.message);
        }
    }
};

app.all('/graphql', createHandler({
    schema: schema,
    rootValue: root,
}));

// --- Graph Query Endpoints (SQL/PGQ via DuckPGQ) ---

/**
 * Lightweight validation: graph, table and column identifiers must match
 * [A-Za-z_][A-Za-z0-9_]*. We do not quote them in emitted DDL because DuckPGQ's
 * parser is strict about graph identifiers.
 */
function isSafeIdentifier(name) {
    return typeof name === 'string' && /^[A-Za-z_][A-Za-z0-9_]*$/.test(name);
}

/**
 * Build a `CREATE PROPERTY GRAPH` statement from a JSON definition.
 */
function buildCreatePropertyGraphSql(def) {
    const { name, vertexTables, edgeTables } = def || {};
    if (!isSafeIdentifier(name)) {
        throw new Error('Invalid or missing "name" (must be a simple identifier).');
    }
    if (!Array.isArray(vertexTables) || vertexTables.length === 0) {
        throw new Error('"vertexTables" must be a non-empty array.');
    }
    if (!Array.isArray(edgeTables)) {
        throw new Error('"edgeTables" must be an array.');
    }

    const formatVertex = (v) => {
        if (typeof v === 'string') {
            if (!isSafeIdentifier(v)) throw new Error(`Invalid vertex table name: ${v}`);
            return v;
        }
        if (v && typeof v === 'object' && isSafeIdentifier(v.name)) {
            let out = v.name;
            // DuckPGQ only accepts `KEY (col)` in conjunction with an
            // explicit `LABEL`. When a label is not provided, omit KEY and
            // let DuckPGQ auto-discover the primary key.
            if (v.label) {
                if (!isSafeIdentifier(v.label)) throw new Error(`Invalid vertex label: ${v.label}`);
                if (v.key) {
                    if (!isSafeIdentifier(v.key)) throw new Error(`Invalid vertex key: ${v.key}`);
                    out += ` KEY (${v.key})`;
                }
                out += ` LABEL ${v.label}`;
            } else if (v.key) {
                // Key provided without label: silently drop it, since DuckPGQ
                // does not accept KEY without LABEL. Callers who need an
                // explicit key must also supply a label.
            }
            return out;
        }
        throw new Error('Vertex entries must be a string or { name, label?, key? }.');
    };

    const formatEdge = (e) => {
        if (!e || typeof e !== 'object') throw new Error('Edge entries must be objects.');
        const { name: edgeName, source, destination, sourceKey, destinationKey, sourceRef, destinationRef, label } = e;
        if (!isSafeIdentifier(edgeName)) throw new Error(`Invalid edge table name: ${edgeName}`);
        if (!isSafeIdentifier(source)) throw new Error(`Invalid edge source: ${source}`);
        if (!isSafeIdentifier(destination)) throw new Error(`Invalid edge destination: ${destination}`);

        let out = edgeName;
        if (label) {
            if (!isSafeIdentifier(label)) throw new Error(`Invalid edge label: ${label}`);
            out += ` LABEL ${label}`;
        }
        // SOURCE [KEY (col) REFERENCES] vertex [(ref)]
        out += ' SOURCE';
        if (sourceKey) {
            if (!isSafeIdentifier(sourceKey)) throw new Error(`Invalid sourceKey: ${sourceKey}`);
            out += ` KEY (${sourceKey}) REFERENCES ${source}`;
            if (sourceRef) {
                if (!isSafeIdentifier(sourceRef)) throw new Error(`Invalid sourceRef: ${sourceRef}`);
                out += ` (${sourceRef})`;
            }
        } else {
            out += ` ${source}`;
        }
        out += ' DESTINATION';
        if (destinationKey) {
            if (!isSafeIdentifier(destinationKey)) throw new Error(`Invalid destinationKey: ${destinationKey}`);
            out += ` KEY (${destinationKey}) REFERENCES ${destination}`;
            if (destinationRef) {
                if (!isSafeIdentifier(destinationRef)) throw new Error(`Invalid destinationRef: ${destinationRef}`);
                out += ` (${destinationRef})`;
            }
        } else {
            out += ` ${destination}`;
        }
        return out;
    };

    const vertexClause = `VERTEX TABLES (${vertexTables.map(formatVertex).join(', ')})`;
    const edgeClause = edgeTables.length > 0
        ? ` EDGE TABLES (${edgeTables.map(formatEdge).join(', ')})`
        : '';

    return `CREATE PROPERTY GRAPH ${name} ${vertexClause}${edgeClause}`;
}

// GET /graphs - List all property graphs
app.get('/graphs', async (req, res) => {
    try {
        // DuckPGQ persists property-graph definitions in an internal table
        // attached to the in-memory catalog. The exact schema name has
        // varied across versions; try the ones we know about and return
        // the first match. We also try a direct PRAGMA which some builds
        // expose.
        const candidateQueries = [
            "SELECT DISTINCT property_graph AS name FROM memory.__duckpgq_internal",
            "SELECT DISTINCT property_graph AS name FROM __duckpgq_internal",
            "SELECT property_graph_name AS name FROM __duckpgq_internal.property_graphs",
            "SELECT property_graph_name AS name FROM duckpgq_show_property_graphs()",
            "SELECT name FROM duckpgq_property_graphs()",
        ];
        let graphs = null;
        let lastErr;
        for (const q of candidateQueries) {
            try {
                graphs = await execute(q);
                break;
            } catch (err) {
                lastErr = err;
            }
        }
        if (graphs === null) {
            throw lastErr || new Error('Unable to enumerate property graphs');
        }
        res.json({ success: true, graphs });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST /graphs - Create a property graph
app.post('/graphs', async (req, res) => {
    try {
        const sql = buildCreatePropertyGraphSql(req.body);
        await execute(sql);
        res.json({ success: true, message: `Property graph ${req.body.name} created`, sql });
    } catch (error) {
        const status = /Invalid|must be/.test(error.message) ? 400 : 500;
        res.status(status).json({ success: false, error: error.message });
    }
});

// DELETE /graphs/:graphName - Drop a property graph
app.delete('/graphs/:graphName', async (req, res) => {
    try {
        const { graphName } = req.params;
        if (!isSafeIdentifier(graphName)) {
            return res.status(400).json({ success: false, error: 'Invalid graph name' });
        }
        await execute(`DROP PROPERTY GRAPH ${graphName}`);
        res.json({ success: true, message: `Property graph ${graphName} dropped` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST /graphs/:graphName/query - Run a SQL/PGQ pattern match query.
app.post('/graphs/:graphName/query', async (req, res) => {
    try {
        const { graphName } = req.params;
        if (!isSafeIdentifier(graphName)) {
            return res.status(400).json({ success: false, error: 'Invalid graph name' });
        }

        const body = req.body || {};
        const { match, where, columns, graphTable, limit } = body;

        let innerBody;
        if (graphTable && typeof graphTable === 'string' && graphTable.trim()) {
            innerBody = graphTable.trim();
        } else {
            if (!match || typeof match !== 'string' || !columns || typeof columns !== 'string') {
                return res.status(400).json({
                    success: false,
                    error: 'Provide either "graphTable" (raw) or both "match" and "columns".'
                });
            }
            innerBody = `MATCH ${match}`;
            if (where && typeof where === 'string' && where.trim()) {
                innerBody += ` WHERE ${where}`;
            }
            innerBody += ` COLUMNS (${columns})`;
        }

        let sql = `FROM GRAPH_TABLE (${graphName} ${innerBody})`;
        const lim = parseInt(limit);
        if (Number.isFinite(lim) && lim > 0) {
            sql += ` LIMIT ${lim}`;
        }

        assertReadOnlySql(sql);
        // Graph queries run on the isolated user instance so a heavy
        // GRAPH_TABLE traversal cannot stall the primary REST pool.
        const rows = await executeUserSql(sql);
        res.json({ success: true, rowCount: rows.length, rows, sql });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// Default table for POST /kafka-sink when neither X-Kafka-Topic header nor
// ?topic= query parameter is provided. Kept in sync with the Swagger
// description so the two cannot drift.
const KAFKA_SINK_DEFAULT_TOPIC = 'ichibi_table';

// Strict identifier check for the kafka-sink topic -> DuckDB / DuckLake table
// name mapping. 63-char cap matches PostgreSQL's NAMEDATALEN-1 default used by
// the DuckLake metadata catalog.
const KAFKA_TOPIC_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

// POST /kafka-sink - convenience endpoint for event stream ingestion
app.post('/kafka-sink', async (req, res) => {
    // Resolve target topic with explicit precedence:
    //   1. X-Kafka-Topic header (trimmed, non-empty)
    //   2. ?topic= query parameter (trimmed, non-empty; first value if repeated)
    //   3. KAFKA_SINK_DEFAULT_TOPIC
    const rawHeader = req.headers['x-kafka-topic'];
    const headerTopic = typeof rawHeader === 'string' ? rawHeader.trim() : '';

    const rawQuery = req.query.topic;
    let queryTopic = '';
    if (Array.isArray(rawQuery)) {
        const firstNonEmpty = rawQuery
            .map((v) => (typeof v === 'string' ? v.trim() : ''))
            .find((v) => v.length > 0);
        queryTopic = firstNonEmpty || '';
    } else if (typeof rawQuery === 'string') {
        queryTopic = rawQuery.trim();
    }

    let topic;
    if (headerTopic) {
        topic = headerTopic;
        if (queryTopic && queryTopic !== headerTopic) {
            console.warn(`[kafka-sink] X-Kafka-Topic header and ?topic= query disagree (header="${headerTopic}", query="${queryTopic}", ip=${req.ip}). Using header.`);
        }
    } else if (queryTopic) {
        topic = queryTopic;
    } else {
        topic = KAFKA_SINK_DEFAULT_TOPIC;
    }

    if (!KAFKA_TOPIC_IDENTIFIER_RE.test(topic)) {
        return res.status(400).json({
            error: `Invalid topic "${topic}". Must match ${KAFKA_TOPIC_IDENTIFIER_RE} (start with a letter or underscore, contain only letters/digits/underscore, max 63 chars).`
        });
    }

    const data = req.body;

    if (!Array.isArray(data)) {
        return res.status(400).json({ error: 'Expected an array of messages' });
    }

    try {
        const targetApiKey = req.apiKey || '';
        const tempFileName = `kafka_sink_${randomUUID()}.json`;
        const tempFilePath = path.join(process.cwd(), tempFileName);

        // Write incrementally to avoid memory exhaustion on large payloads
        const fileHandle = await fs.open(tempFilePath, 'w');
        try {
            for (const msg of data) {
                const row = {
                    _ingest_timestamp: new Date().toISOString(),
                    ...(typeof msg.value === 'object' ? msg.value : { value: msg.value }),
                    _kafka_key: msg.key,
                };
                await fileHandle.write(JSON.stringify(row) + '\n');
            }
        } finally {
            await fileHandle.close();
        }

        const checkResult = await executeWithParams(
            `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
            [topic], [VARCHAR]
        );
        const tableExists = checkResult[0] && parseInt(checkResult[0].count) > 0;

        const readOptions = `sample_size=-1`;
        let createdTable = false;

        await withConnection(async (conn) => {
            if (!tableExists) {
                await conn.run(`CREATE TABLE "${topic}" AS SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                createdTable = true;
                await ensureTableAcl(topic, targetApiKey);
                invalidateColumnCache(topic);
            } else {
                // Schema Evolution: add any columns present in the incoming
                // messages that the existing target table does not yet have.
                // For STRUCT columns with different shapes, widen the column
                // type to the union of both shapes so DuckDB can cast safely.
                const existingColsRows = await executeWithParams(
                    `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?`,
                    [topic], [VARCHAR]
                );
                const existingCols = new Map(existingColsRows.map(r => [r.column_name, r.data_type]));

                const newDataColsRows = await execute(`DESCRIBE SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                const newDataCols = newDataColsRows.map(r => ({ name: r.column_name, type: r.column_type }));

                const selectColumns = [];
                let schemaChanged = false;
                for (const col of newDataCols) {
                    const existingType = existingCols.get(col.name);
                    if (existingType) {
                        const resolution = resolveTypeConflict(existingType, col.type);
                        if (resolution.action === 'skip') {
                            console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${topic} for this batch: ${resolution.reason}`);
                            continue;
                        }
                        if (resolution.action === 'widen') {
                            console.log(`[Schema Evolution] Widening column "${col.name}" in ${topic} from ${existingType} to ${resolution.newType}`);
                            try {
                                await conn.run(`ALTER TABLE "${topic}" ALTER COLUMN "${col.name}" SET DATA TYPE ${resolution.newType}`);
                                schemaChanged = true;
                            } catch (alterErr) {
                                console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${topic} for this batch: ALTER failed (${alterErr.message || alterErr})`);
                                continue;
                            }
                        }
                    } else {
                        console.log(`[Schema Evolution] Adding column ${col.name} (${col.type}) to ${topic}`);
                        try {
                            await conn.run(`ALTER TABLE "${topic}" ADD COLUMN "${col.name}" ${col.type}`);
                            schemaChanged = true;
                        } catch (addErr) {
                            console.warn(`[Schema Evolution] Skipping new column "${col.name}" in ${topic} for this batch: ADD COLUMN failed (${addErr.message || addErr})`);
                            continue;
                        }
                    }
                    selectColumns.push(`"${col.name}"`);
                }

                if (schemaChanged) {
                    invalidateColumnCache(topic);
                    const schemaRows = await executeWithParams(
                        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position`,
                        [topic], [VARCHAR]
                    );
                    console.log(`[Schema Evolution] New schema for ${topic}:\n${formatSchemaDefinition(topic, schemaRows)}`);
                }

                await conn.run(`INSERT INTO "${topic}" BY NAME SELECT ${selectColumns.join(', ')} FROM read_json_auto('${tempFilePath}', ${readOptions})`);
            }
        });

        if (createdTable) await applyDefaultPartitioning(topic);

        await fs.unlink(tempFilePath);
        res.json({ success: true, message: `Sinked ${data.length} messages to ${topic}` });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// GET /kafka-subscribe/:topicName - SSE Kafka event stream
//
// Disconnect / resource-leak hardening (see kafka-architecture.md):
//   * Per-connection groupId by default so concurrent SSE subscribers from
//     the same API key do not force broker rebalances on every reconnect.
//   * `cancelled` flag checked after every async step in the startup IIFE so
//     a client disconnect during topic creation / metadata polling aborts
//     cleanly instead of leaking an orphan consumer into the broker group.
//   * 15s SSE keepalive comment so reverse proxies (nginx, Cloudflare) and
//     idle-timeout-aware clients don't silently drop the TCP connection.
//   * eachMessage honours `res.destroyed` and Node stream backpressure, so a
//     slow / gone client pauses the consumer's fetch loop instead of buffering.
//   * sessionTimeout = 10s (down from 30s) so any orphan consumer that does
//     slip through is evicted from the group much faster.
//   * Shared Kafka client instance (getKafka()) instead of `new Kafka(...)`
//     per request — one connection pool, one metadata refresh loop.
async function writeHistoricRowsToSSE(topicName, { res, isCancelled, since, sinceColumn }) {
    const tableName = topicName.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1');
    return withConnection(async (conn) => {
        const tableResult = await conn.run(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?`,
            [tableName], [VARCHAR]
        );
        const tableRows = await tableResult.getRows();
        if (tableRows.length === 0) return { count: 0, skipped: 0 };
        const quoted = `"${tableName.replace(/"/g, '""')}"`;
        const colResult = await conn.run(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ?`,
            [tableName], [VARCHAR]
        );
        const colRows = await colResult.getRows();
        const selectCols = colRows.map(([col, type]) => {
            const safeCol = col.replace(/"/g, '""');
            if (type.toUpperCase().startsWith('TIMESTAMP') || type.toUpperCase().startsWith('DATE')) {
                return `CAST("${safeCol}" AS VARCHAR) AS "${safeCol}"`;
            }
            return `"${safeCol}"`;
        });
        let sql = `SELECT ${selectCols.join(', ')} FROM ${quoted}`;
        const params = [];
        if (since) {
            const filterColumn = sinceColumn || 'updated';
            const sinceTs = new Date(since).toISOString();
            sql += ` WHERE "${filterColumn.replace(/"/g, '""')}" >= ? ORDER BY "${filterColumn.replace(/"/g, '""')}" ASC`;
            params.push(sinceTs);
        }
        const result = await conn.run(sql, params);
        const columns = result.columnNames();
        let count = 0;
        while (true) {
            if (isCancelled()) return { count };
            const chunk = await result.fetchChunk();
            if (!chunk || chunk.rowCount === 0) break;
            for (const row of chunk.getRows()) {
                const obj = {};
                for (let c = 0; c < columns.length; c++) {
                    const val = row[c];
                    if (val === null || val === undefined) { obj[columns[c]] = null; }
                    else if (typeof val === 'object' && val.constructor?.name === 'DuckDBBlobValue' && val.bytes) { obj[columns[c]] = Buffer.from(val.bytes).toString('base64'); }
                    else if (val instanceof Uint8Array || Buffer.isBuffer(val)) { obj[columns[c]] = Buffer.from(val).toString('base64'); }
                    else { obj[columns[c]] = val; }
                }
                try {
                    const ok = res.write(`data: ${JSON.stringify({ topic: topicName, partition: -1, offset: null, key: null, value: JSON.stringify(obj), timestamp: null })}\n\n`);
                    if (!ok && !res.destroyed) {
                        await new Promise(resolve => { const cb = () => { res.removeListener('drain', cb); res.removeListener('close', cb); resolve(); }; res.once('drain', cb); res.once('close', cb); });
                    }
                    count++;
                } catch (err) {
                    if (err.code === 'ERR_STREAM_DESTROYED' || err.message?.includes('destroyed')) return { count };
                    throw err;
                }
            }
        }
        return { count };
    });
}

app.get('/kafka-subscribe/:topicName', async (req, res) => {
    const { topicName } = req.params;

    const kafka = getKafka();
    if (!kafka) {
        return res.status(400).json({
            error: 'KAFKA_BROKERS is not configured on the server. Cannot subscribe to Kafka topics.'
        });
    }

    // Per-connection group ID by default. Sharing a group across concurrent
    // SSE connections from the same API key forces a broker rebalance every
    // time any one of them connects/disconnects. Clients that explicitly
    // want shared/round-robin delivery can still pass `?groupId=`.
    const baseGroupId = `ichibi-lake-sse-${(req.apiKey || 'anon').replace(/[^a-zA-Z0-9_-]/g, '_')}`;
    const groupId = req.query.groupId || `${baseGroupId}-${randomUUID().slice(0, 8)}`;
    // Parse the ?since= parameter to limit the retroactive dump to events
    // newer than a given timestamp.  Supports absolute ISO 8601 timestamps,
    // relative durations (e.g. 5m, 1h, 2d) or plain numbers (minutes).
    // When omitted, all events are dumped.
    const rawSince = req.query.since;
    let since = null;
    if (rawSince) {
        const sinceStr = String(rawSince);
        const isoDate = new Date(sinceStr);
        if (!isNaN(isoDate)) {
            since = isoDate.toISOString();
        } else {
            const relMatch = sinceStr.match(/^(\d+)([mhd])$/);
            if (relMatch) {
                const num = parseInt(relMatch[1]);
                const unit = { m: 60000, h: 3600000, d: 86400000 }[relMatch[2]];
                since = new Date(Date.now() - num * unit).toISOString();
            } else {
                const num = parseInt(sinceStr);
                if (!isNaN(num)) {
                    // If the raw number is ≥ year 2000 in epoch ms (1e12), treat as epoch ms;
                    // otherwise treat as minutes-ago (backwards compat).
                    const asMs = num >= 1e12 ? num : Date.now() - num * 60000;
                    since = new Date(asMs).toISOString();
                }
            }
        }
    }
    const sinceColumn = req.query.sinceColumn || null;

    res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'X-Accel-Buffering': 'no',
    });

    res.write(`event: connected\ndata: {"topic":"${topicName}","groupId":"${groupId}"}\n\n`);

    // Cancellation / cleanup state. The req.on('close') handler is registered
    // immediately (before any kafkajs call) so it always captures the live
    // references, even if the client disconnects during the up-to-30-second
    // metadata-poll loop further down.
    let cancelled = false;
    let cleanedUp = false;
    let admin = null;
    let consumer = null;

    // Keepalive: SSE comment lines are ignored by clients but keep the TCP
    // connection from idling out behind reverse proxies.
    const keepalive = setInterval(() => {
        if (!res.destroyed) res.write(':keepalive\n\n');
    }, 15000);

    const cleanup = async () => {
        if (cleanedUp) return;
        cleanedUp = true;
        cancelled = true;
        clearInterval(keepalive);
        if (admin) {
            const a = admin; admin = null;
            try { await a.disconnect(); } catch { /* ignore */ }
        }
        if (consumer) {
            const c = consumer; consumer = null;
            try { await c.disconnect(); } catch { /* ignore */ }
        }
    };

    req.on('close', cleanup);

    (async () => {
        try {
            admin = kafka.admin();
            await admin.connect();
            if (cancelled) return cleanup();

            await admin.createTopics({
                topics: [{ topic: topicName, numPartitions: 1, replicationFactor: 1 }],
            });
            if (cancelled) return cleanup();

            for (let i = 0; i < 30; i++) {
                if (cancelled) return cleanup();
                try {
                    await admin.fetchTopicMetadata({ topics: [topicName] });
                    break;
                } catch {
                    await new Promise(r => setTimeout(r, 1000));
                }
            }
            try { await admin.disconnect(); } catch { /* ignore */ }
            admin = null;
            if (cancelled) return cleanup();

            consumer = kafka.consumer({ groupId, sessionTimeout: 10000, heartbeatInterval: 3000 });
            await consumer.connect();
            if (cancelled) return cleanup();

            await consumer.subscribe({ topic: topicName, fromBeginning: true });
            if (cancelled) return cleanup();

            await consumer.run({
                eachMessage: async ({ topic, partition, message }) => {
                    if (cancelled || res.destroyed) return;
                    const payload = {
                        topic,
                        partition,
                        offset: message.offset,
                        key: message.key ? message.key.toString() : null,
                        value: message.value ? message.value.toString() : null,
                        timestamp: message.timestamp,
                    };
                    try {
                        const ok = res.write(`data: ${JSON.stringify(payload)}\n\n`);
                        // Propagate backpressure back to Kafka: kafkajs waits for
                        // this promise before fetching the next message, so a slow
                        // or gone client pauses the consumer rather than buffering.
                        if (!ok && !res.destroyed) {
                            await new Promise(resolve => {
                                const onDrain = () => { cleanupDrain(); resolve(); };
                                const onClose = () => { cleanupDrain(); resolve(); };
                                const cleanupDrain = () => {
                                    res.removeListener('drain', onDrain);
                                    res.removeListener('close', onClose);
                                };
                                res.once('drain', onDrain);
                                res.once('close', onClose);
                            });
                        }
                    } catch (err) {
                        // Stream destroyed between the res.destroyed guard and the
                        // actual write/drain wait. Swallow silently – cleanup() will
                        // disconnect the consumer.
                        if (err.code === 'ERR_STREAM_DESTROYED' || err.message?.includes('destroyed')) {
                            return;
                        }
                        throw err;
                    }
                },
            });

            writeHistoricRowsToSSE(topicName, { res, isCancelled: () => cancelled || res.destroyed, since, sinceColumn })
                .then(({ count = 0 }) => {
                    if (cancelled || res.destroyed) return;
                    res.write(`event: dump-complete\ndata: ${JSON.stringify({ table: topicName, rowsPublished: count })}\n\n`);
                })
                .catch(err => {
                    console.error(`[SSE] Failed to write historic DuckDB rows for ${topicName}:`, err.message);
                });

            return;
        } catch (err) {
            if (!res.destroyed) {
                try { res.write(`event: error\ndata: ${JSON.stringify({ error: err.message })}\n\n`); } catch { /* ignore */ }
                try { res.end(); } catch { /* ignore */ }
            }
            await cleanup();
        }
    })();
});

// POST /upload - Ingest data
app.post(['/upload', '/upload/:tableName'], async (req, res) => {
    const tableName = req.params.tableName || 'ichibi_table';
    const data = req.body;
    const tempFileName = `temp_${randomUUID()}.json`;
    const tempFilePath = path.join(process.cwd(), tempFileName);

    if (!Array.isArray(data)) {
        return res.status(400).json({ error: 'Expected an array of JSON objects' });
    }

    if (data.length === 0) {
        return res.json({ success: true, message: 'No data to insert' });
    }
    if (data.length > MAX_BATCH_SIZE) {
        return res.status(400).json({ success: false, error: `Batch too large. Maximum ${MAX_BATCH_SIZE} rows allowed.` });
    }

    try {
        const targetApiKey = req.apiKey || '';

        // Write incrementally to avoid memory exhaustion on large payloads
        const fileHandle = await fs.open(tempFilePath, 'w');
        try {
            for (const row of data) {
                await fileHandle.write(JSON.stringify(row) + '\n');
            }
        } finally {
            await fileHandle.close();
        }

        // Check if table exists
        assertSafeIdentifier(tableName, 'tableName');
        const checkResult = await executeWithParams(
            `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
            [tableName], [VARCHAR]
        );
        const tableExists = checkResult[0] && parseInt(checkResult[0].count) > 0;

        const readOptions = `sample_size=-1`;
        let createdTable = false;

        await withConnection(async (conn) => {
            if (!tableExists) {
                await conn.run(`CREATE TABLE "${tableName}" AS SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                createdTable = true;
                await ensureTableAcl(tableName, targetApiKey);
                invalidateColumnCache(tableName);
            } else {
                await requireWriteAccess(tableName, req);
                // Schema Evolution: Add missing columns.
                // For STRUCT columns with different shapes, widen the column
                // type to the union of both shapes so DuckDB can cast safely.
                const existingColsRows = await executeWithParams(
                    `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ?`,
                    [tableName], [VARCHAR]
                );
                const existingCols = new Map(existingColsRows.map(r => [r.column_name, r.data_type]));

                const newDataColsRows = await execute(`DESCRIBE SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                const newDataCols = newDataColsRows.map(r => ({ name: r.column_name, type: r.column_type }));

                const selectColumns = [];
                let schemaChanged = false;
                for (const col of newDataCols) {
                    const existingType = existingCols.get(col.name);
                    if (existingType) {
                        const resolution = resolveTypeConflict(existingType, col.type);
                        if (resolution.action === 'skip') {
                            console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ${resolution.reason}`);
                            continue;
                        }
                        if (resolution.action === 'widen') {
                            console.log(`[Schema Evolution] Widening column "${col.name}" in ${tableName} from ${existingType} to ${resolution.newType}`);
                            try {
                                await conn.run(`ALTER TABLE "${tableName}" ALTER COLUMN "${col.name}" SET DATA TYPE ${resolution.newType}`);
                                schemaChanged = true;
                            } catch (alterErr) {
                                console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ALTER failed (${alterErr.message || alterErr})`);
                                continue;
                            }
                        }
                    } else {
                        console.log(`[Schema Evolution] Adding column ${col.name} (${col.type}) to ${tableName}`);
                        try {
                            await conn.run(`ALTER TABLE "${tableName}" ADD COLUMN "${col.name}" ${col.type}`);
                            schemaChanged = true;
                        } catch (addErr) {
                            console.warn(`[Schema Evolution] Skipping new column "${col.name}" in ${tableName} for this batch: ADD COLUMN failed (${addErr.message || addErr})`);
                            continue;
                        }
                    }
                    selectColumns.push(`"${col.name}"`);
                }

                if (schemaChanged) {
                    invalidateColumnCache(tableName);
                    const schemaRows = await executeWithParams(
                        `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position`,
                        [tableName], [VARCHAR]
                    );
                    console.log(`[Schema Evolution] New schema for ${tableName}:\n${formatSchemaDefinition(tableName, schemaRows)}`);
                }

                await conn.run(`INSERT INTO "${tableName}" BY NAME SELECT ${selectColumns.join(', ')} FROM read_json_auto('${tempFilePath}', ${readOptions})`);
            }
        });

        if (createdTable) await applyDefaultPartitioning(tableName);

        res.json({ success: true, message: `Inserted ${data.length} records into ${tableName}` });
    } catch (error) {
        console.error('Upload error:', error);
        res.status(500).json({ success: false, error: error.message });
    } finally {
        try { await fs.unlink(tempFilePath); } catch (e) { }
    }
});

// --- 4. Swagger UI ---
const swaggerDocument = YAML.load(path.join(process.cwd(), 'swagger.yaml'));
app.use('/', swaggerUi.serve);
app.get('/', swaggerUi.setup(swaggerDocument));

// --- Start Server ---
app.listen(port, '0.0.0.0', async () => {
    console.log(`ichibi-lake server listening on 0.0.0.0:${port}`);
    try {
        await initAclTable();
        await migrateExistingTables();
        await migrateToAcl();
        await migrateTimestamps();
        // Compact tiny Parquet files left over from high-frequency one-row
        // ingestion. This is often the single biggest win for scan speed.
        await compactDuckLake();
        startKafkaConsumer().catch(err => console.error('Kafka Consumer Error:', err));
    } catch (err) {
        console.error('Startup Error:', err);
    }
});
