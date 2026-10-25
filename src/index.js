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
    streamUserSql,
    userQueryStats,
    getTableColumns,
    invalidateColumnCache,
    initGraphRegistry,
    saveGraphDefinition,
    loadGraphDefinition,
    listGraphDefinitions,
    deleteGraphDefinition,
    ensureGraphMaterialized,
    VARCHAR,
    BLOB,
    blobValue,
} from './db.js';
import { isSafeIdentifier, buildCreatePropertyGraphSql } from './graphs.js';
import { resolveTypeConflict, formatSchemaDefinition } from './schema-evolution.js';
import { startKafkaConsumer } from './kafka-consumer.js';
import { getKafka } from './kafka-producer.js';
import { migrateTimestamps } from './migrate-timestamps.js';
import { initAclTable, ensureTableAcl, checkReadAccess, checkWriteAccess, getTableAcl, updateTableAcl, invalidateAclCache, migrateToAcl } from './acl.js';
import { initRbacTables, bootstrapRbac, isValidApiKey, checkAccess, isRbacConfigured, legacyBootstrap, isSuperuserKey } from './rbac.js';
import { assertReadOnlySql } from './sql-guard.js';
import { retryOnConflict } from './conflict.js';
import adminRouter from './rbac-api.js';
import { createConsoleRouter, initConsole, loadSettingOverrides, publicBaseUrl } from './console/index.js';
import { EXPORT_FORMATS, runExport, exportStats } from './export.js';
import { httpMetricsMiddleware, renderMetrics } from './metrics.js';
import { importTempFile, importMaxBytes, receiveUpload, contentTypeFormat, readSourceRequest, prepareS3Import } from './ingest.js';
import { detectImportFormat, importFile } from './console/data.js';
import { parseImportSource, isImportSourceAllowed } from './storage.js';
import { beginIdempotentRequest, requestHasher, idempotentJson, idempotencyKeyOf } from './idempotency.js';
import { checkReadiness, lakeStats, getStartupState, setStartupState } from './health.js';
import swaggerUi from 'swagger-ui-express';
import YAML from 'yamljs';
import { createHandler } from 'graphql-http/lib/use/express';
import { buildSchema } from 'graphql';

// Support BigInt serialization in JSON
BigInt.prototype.toJSON = function () { return this.toString(); };

// Silence a benign, noisy warning from kafkajs. Its RequestQueue computes
// `throttledUntil - Date.now()` (RequestQueue.scheduleCheckPendingRequests);
// when not throttled and the pending queue is empty this is a large negative
// number passed straight to setTimeout, which Node clamps to 1ms and reports as
// a TimeoutNegativeWarning on every fulfilled request. We drop only that exact
// kafkajs-originated warning and let every other process warning through.
const _originalEmitWarning = process.emitWarning.bind(process);
process.emitWarning = (warning, ...args) => {
    const type = typeof args[0] === 'string' ? args[0] : (args[0] && args[0].type);
    if (type === 'TimeoutNegativeWarning' && (new Error().stack || '').includes('kafkajs')) {
        return;
    }
    return _originalEmitWarning(warning, ...args);
};

const app = express();
const port = process.env.PORT || 3000;

app.use(httpMetricsMiddleware);
app.use(compression());
// Admin console (password-protected web UI). Mounted before CORS so console
// responses never carry permissive cross-origin headers.
app.use('/console', createConsoleRouter());
app.use(cors());
// JSON body limit. Previously 2gb, which is large enough to let a single
// request exhaust the Node heap and trigger long GC pauses that look like
// the whole gateway hanging. Configure via JSON_BODY_LIMIT for stress runs.
const JSON_BODY_LIMIT = process.env.JSON_BODY_LIMIT || '50mb';
// POST /tables/:tableName/import streams its body (which may be a JSON array
// of any size up to IMPORT_MAX_MB) to a temp file itself, so the JSON parser
// must leave it alone.
const IMPORT_PATH_RE = /^\/tables\/[^/]+\/import\/?$/;
const jsonParser = express.json({ limit: JSON_BODY_LIMIT });
app.use((req, res, next) => (IMPORT_PATH_RE.test(req.path) ? next() : jsonParser(req, res, next)));
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
// System tables ("_"-prefixed: RBAC users and their API keys, ACLs, graph
// registry, gateway metadata) are never reachable through the table REST API,
// whatever the caller's role. They have dedicated, superuser-only APIs.
function isSystemTableName(name) {
    return typeof name === 'string' && name.startsWith('_');
}

function assertNotSystemTable(tableName) {
    if (isSystemTableName(tableName)) {
        const err = new Error(`Table "${tableName}" not found`);
        err.statusCode = 404;
        throw err;
    }
}

async function requireReadAccess(tableName, req) {
    assertNotSystemTable(tableName);
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
    assertNotSystemTable(tableName);
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
// send massive arrays. Configure via MAX_BATCH_SIZE. Read on use so the
// admin console can change it live.
const maxBatchSize = () => Math.max(1, parseInt(process.env.MAX_BATCH_SIZE || '10000', 10) || 10000);

// --- User-supplied SQL ---
// POST /query, the GraphQL `sql` resolver and the graph endpoints accept SQL
// (graph query fragments can embed subqueries), and RBAC roles / table ACLs
// cannot be applied to arbitrary SQL. They are therefore superuser-only, always
// read-only (assertReadOnlySql in src/sql-guard.js) and run on the locked-down
// user-query instance (src/db.js). Readers and writers use the table-scoped
// REST endpoints, which do check RBAC and ACLs.
async function requireSuperuser(req) {
    if (!(await isSuperuserKey(req.apiKey))) {
        const err = new Error('Forbidden: user-supplied SQL and graph management require a superuser API key. Use the table REST endpoints instead.');
        err.statusCode = 403;
        throw err;
    }
}

// Reject SQL/PGQ pattern fragments (match / where / columns / graphTable) that
// contain statement terminators or comment sequences. These are the injection
// vectors for the GRAPH_TABLE(...) body: a semicolon allows statement stacking
// (e.g. "...) ; SELECT * FROM _table_acls --") and comment sequences let an
// attacker smuggle tokens past the read-only keyword scanner (which strips
// comments before scanning). None of these appear in valid PGQ patterns
// (which use ->, ->+ and -[...]->), so rejecting them is safe.
const GRAPH_FRAGMENT_FORBIDDEN_RE = /;|--|\/\*|\*\//;
function assertSafeGraphFragment(value, kind = 'graph query fragment') {
    if (value == null) return;
    if (GRAPH_FRAGMENT_FORBIDDEN_RE.test(String(value))) {
        const err = new Error(
            `Invalid ${kind}: statement terminators (";") and SQL comments ("--", "/*", "*/") are not allowed.`
        );
        err.statusCode = 400;
        throw err;
    }
}

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
async function applyDefaultPartitioning(tableName) {
    // Read on use so the admin console can change it live.
    const DEFAULT_PARTITION_KEY_RAW = (process.env.DEFAULT_PARTITION_KEY ?? 'created').trim();
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

// --- Operations: health checks and metrics ---

// GET /health - liveness. No auth, no database access: answers as long as the
// Node event loop is responsive. Used by the compose healthcheck.
app.get('/health', (req, res) => {
    res.set('Cache-Control', 'no-store').json({ status: 'ok', uptimeSeconds: Math.round(process.uptime()) });
});

// GET /health/ready - readiness. No auth. 200 only when startup finished, the
// DuckLake catalog is attached on the primary instance and its Postgres
// metadata database answers a query; otherwise 503 with the failing check
// (see src/health.js).
app.get('/health/ready', async (req, res) => {
    const { ready, checks } = await checkReadiness();
    res.set('Cache-Control', 'no-store').status(ready ? 200 : 503).json({ status: ready ? 'ready' : 'not ready', checks });
});

// GET /metrics - Prometheus text format. With METRICS_TOKEN set, scrapers send
// "Authorization: Bearer <METRICS_TOKEN>". Without it, a superuser API key is
// required (x-api-key header or Bearer token), so metrics are never public by
// default.
app.get('/metrics', async (req, res) => {
    const bearer = (req.headers.authorization || '').match(/^Bearer\s+(.+)$/i)?.[1]?.trim();
    const token = process.env.METRICS_TOKEN;
    let allowed = false;
    if (token) {
        allowed = bearer === token;
    } else {
        const key = req.headers['x-api-key'] || bearer;
        try { allowed = !!key && await isSuperuserKey(key); } catch (_) { allowed = false; }
    }
    if (!allowed) {
        return res.status(401).set('WWW-Authenticate', 'Bearer').json({
            error: token ? 'Unauthorized: missing or wrong METRICS_TOKEN bearer token' : 'Unauthorized: /metrics requires a superuser API key (or set METRICS_TOKEN)',
        });
    }
    const body = renderMetrics({ userQuery: userQueryStats(), exports: exportStats(), lake: await lakeStats(), ready: getStartupState() === 'ready' });
    res.set('Content-Type', 'text/plain; version=0.0.4; charset=utf-8').set('Cache-Control', 'no-store').send(body);
});

// --- 3. Authenticated API Routes ---

/**
 * Auth middleware: validates x-api-key against the RBAC user table.
 * Falls back to legacy API_KEYS env var set when RBAC is not configured.
 */
app.use(['/admin', '/graphql', '/query', '/kafka-sink', '/kafka-subscribe/:topicName', '/upload', '/upload/:tableName', '/tables', '/tables/:tableName', '/tables/:tableName/schema', '/tables/:tableName/blobs/:idValue/:blobColumn', '/blobs/:idValue/:blobColumn', '/tables/:tableName/blobs/:blobColumn', '/blobs/:blobColumn', '/tables/:tableName/records/:idValue', '/records/:idValue', '/graphs', '/graphs/:graphName', '/graphs/:graphName/query'], async (req, res, next) => {
    const providedKey = req.headers['x-api-key'];
    if (!providedKey) {
        return res.status(401).json({ error: 'Unauthorized: Missing x-api-key header' });
    }

    try {
        const rbacValid = await isValidApiKey(providedKey);
        if (rbacValid) {
            req.apiKey = providedKey;
            return next();
        }
    } catch (_) {
        // If RBAC query fails (e.g. tables not initialized yet), fall through
    }

    const validKeys = (process.env.API_KEYS || process.env.API_KEY || '').split(',').map(k => k.trim()).filter(Boolean);
    if (validKeys.length > 0 && !validKeys.includes(providedKey)) {
        console.error(`[Auth] 401 Unauthorized from ${req.ip}. Rejected x-api-key: ${providedKey?.slice(0, 20)}...`);
        return res.status(401).json({ error: 'Unauthorized: Invalid x-api-key' });
    }

    if (validKeys.length > 0) {
        req.apiKey = providedKey;
        return next();
    }

    console.error(`[Auth] 401 Unauthorized from ${req.ip}. No valid API keys configured.`);
    return res.status(401).json({ error: 'Unauthorized: No API keys configured' });
});

// Refuse system tables on every route with a :tableName parameter before the
// handler runs (e.g. /upload/_rbac_users would otherwise reach the insert path).
app.param('tableName', (req, res, next, tableName) => {
    if (isSystemTableName(tableName)) return res.status(404).json({ success: false, error: `Table "${tableName}" not found` });
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
            // All schema probes and DDL below MUST run on this same `conn` — the
            // one that later performs the INSERT/UPDATE. DuckLake gives each
            // connection its own catalog snapshot, so running an
            // `ALTER TABLE ADD COLUMN` on a *different* pooled connection (via
            // execute()/executeWithParams()) can leave `conn` with a stale view
            // in which the freshly-added column silently binds to NULL instead of
            // persisting the supplied metadata. Keeping DDL + DML on one
            // connection guarantees the INSERT sees the columns it just created.
            const columnExists = async (col) => {
                const r = await conn.run(
                    `SELECT count(*) as count FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                    [tableName, col], [VARCHAR, VARCHAR]
                );
                const rows = await r.getRows();
                return rows && rows[0] && parseInt(rows[0][0]) > 0;
            };

            // 0. Check table existence and ACL before proceeding.
            const tableExistsResult = await conn.run(
                `SELECT count(*) as count FROM information_schema.tables WHERE table_name = ?`,
                [tableName], [VARCHAR]
            );
            const tableExistsRows = await tableExistsResult.getRows();
            const tableExists = tableExistsRows && tableExistsRows[0] && parseInt(tableExistsRows[0][0]) > 0;
            if (tableExists) {
                await requireWriteAccess(tableName, req);
            }

            // 1. Ensure row and metadata columns exist (shared logic for both modes).
            const colExists = await columnExists(blobColumn);

            let tableCreated = false;
            let raceExisting = false;
            if (!colExists) {
                if (!tableExists) {
                    const colType = USE_FILESYSTEM_BLOBS ? 'VARCHAR' : 'BLOB';
                    try {
                        await conn.run(`CREATE TABLE "${tableName}" ("${idColumn}" VARCHAR, "${blobColumn}" ${colType}, "created" TIMESTAMP, "updated" TIMESTAMP)`);
                        tableCreated = true;
                        await ensureTableAcl(tableName, req.apiKey);
                        invalidateColumnCache(tableName);
                    } catch (createErr) {
                        // A concurrent request created the table between our
                        // existence check and this CREATE. Treat it as an
                        // existing table: enforce write access before touching
                        // its data and fall through to the ALTER path below.
                        if (/already exists/i.test(createErr.message || '')) {
                            invalidateColumnCache(tableName);
                            await requireWriteAccess(tableName, req);
                            raceExisting = true;
                        } else {
                            throw createErr;
                        }
                    }
                }

                if (tableExists || raceExisting) {
                    const colType = USE_FILESYSTEM_BLOBS ? 'VARCHAR' : 'BLOB';
                    if (!(await columnExists(blobColumn))) {
                        await conn.run(`ALTER TABLE "${tableName}" ADD COLUMN "${blobColumn}" ${colType}`);
                    }

                    for (const metaCol of ['created', 'updated']) {
                        if (!(await columnExists(metaCol))) {
                            await conn.run(`ALTER TABLE "${tableName}" ADD COLUMN "${metaCol}" TIMESTAMP`);
                        }
                    }

                    invalidateColumnCache(tableName);
                }
            }

            // 1b. Ensure metadata columns exist
            for (const metaKey of Object.keys(metadata)) {
                if (!(await columnExists(metaKey))) {
                    await conn.run(`ALTER TABLE "${tableName}" ADD COLUMN "${metaKey}" VARCHAR`);
                    invalidateColumnCache(tableName);
                    console.log(`[Metadata] Added column ${metaKey} to ${tableName}`);
                }
            }

            // 1c. Apply default partitioning on freshly-created tables.
            if (tableCreated) {
                await applyDefaultPartitioning(tableName);
            }

            // 2. Check if record exists (on `conn` so it shares the catalog
            // snapshot with the DDL above and the INSERT/UPDATE below).
            const existResult = await conn.run(
                `SELECT count(*) as count FROM "${tableName}" WHERE CAST("${idColumn}" AS VARCHAR) = ?`,
                [idValue], [VARCHAR]
            );
            const existRows = await existResult.getRows();
            const recordExists = existRows && existRows[0] && parseInt(existRows[0][0]) > 0;

            // 3. Persist blob data
            if (USE_FILESYSTEM_BLOBS) {
                // --- Filesystem mode: write binary data to a file, store the file path ---
                
                // Validate and sanitize idValue for filesystem safety
                const safeId = idValue.replace(/[^a-zA-Z0-9\-_\.]/g, '_');
                const filePath = path.join(BLOBS_PATH, safeId);
                
                try {
                    await fs.writeFile(filePath, Buffer.from(binaryData));
                    console.log(`[Blobs] Saved file to filesystem: ${filePath} (${binaryData.length} bytes)`);
                } catch (fsErr) {
                    throw new Error(`Failed to write blob file: ${fsErr.message}`);
                }

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
                    const idColResult = await conn.run(
                        `SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                        [tableName, idColumn], [VARCHAR, VARCHAR]
                    );
                    const idColRows = await idColResult.getRows();
                    const idColType = idColRows && idColRows[0] ? idColRows[0][0] : undefined;
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
                    const idColResult = await conn.run(
                        `SELECT data_type FROM information_schema.columns WHERE table_name = ? AND column_name = ?`,
                        [tableName, idColumn], [VARCHAR, VARCHAR]
                    );
                    const idColRows = await idColResult.getRows();
                    const idColType = idColRows && idColRows[0] ? idColRows[0][0] : undefined;
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
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
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
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
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
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
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

// GET /tables - List tables the caller is allowed to read.
//
// Previously this returned every table (including internal "_"-prefixed
// bookkeeping tables like _table_acls / _rbac_config) to any authenticated
// key, which is an information-disclosure gap. We now hide internal tables and
// filter the remainder through the same read-ACL check used by the data
// endpoints so the catalog can never reveal tables the caller cannot access.
app.get('/tables', async (req, res) => {
    try {
        const dbList = await execute('PRAGMA database_list');
        console.log(`[Diagnostic] Database List: ${JSON.stringify(dbList)}`);

        const rows = await execute('SHOW TABLES');
        const candidates = [...new Set(rows.map(r => r.name))].filter(name => !name.startsWith('_'));

        const visible = [];
        for (const name of candidates) {
            if (await checkReadAccess(name, req.apiKey)) visible.push(name);
        }
        res.json({ success: true, tables: visible });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
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
// Build the filtered/sorted SELECT shared by GET /tables/:tableName and its
// /export variant. Throws 400 for ?after without a valid ?sort. Returns the
// WHERE / ORDER BY clauses with their bound parameters.
function buildTableFilter(columns, query) {
    const { sort, after, ...rest } = query;
    const columnSet = new Set(columns);
    const whereClauses = [];
    const params = [];
    const types = [];

    // Simple column equality filters (unknown parameters are ignored)
    for (const [key, value] of Object.entries(rest)) {
        if (columnSet.has(key)) {
            whereClauses.push(`"${key}" = ?`);
            params.push(String(value));
            types.push(VARCHAR);
        }
    }

    // Sorting + optional keyset cursor
    let sortColName = null;
    let isDesc = false;
    if (sort) {
        isDesc = sort.startsWith('-');
        const colName = isDesc ? sort.substring(1) : sort;
        if (columnSet.has(colName)) sortColName = colName;
    }

    if (after !== undefined) {
        if (!sortColName) {
            const err = new Error('Keyset pagination ?after=... requires a valid ?sort=<column>');
            err.statusCode = 400;
            throw err;
        }
        // ASC: row > cursor; DESC: row < cursor.
        whereClauses.push(`"${sortColName}" ${isDesc ? '<' : '>'} ?`);
        params.push(String(after));
        types.push(VARCHAR);
    }

    const where = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(' AND ')}` : '';
    const orderBy = sortColName ? ` ORDER BY "${sortColName}" ${isDesc ? 'DESC' : 'ASC'}` : '';
    return { where, orderBy, params, types, sortColName };
}

app.get('/tables/:tableName', async (req, res) => {
    try {
        const { tableName } = req.params;
        assertSafeIdentifier(tableName, 'tableName');
        const { limit = 50, offset = 0, ...query } = req.query;

        // 1. Resolve column list from the cache instead of issuing a per-request
        // `SELECT * LIMIT 0` probe against the Parquet table.
        const columns = await getTableColumns(tableName);
        if (!columns || columns.length === 0) {
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        await requireReadAccess(tableName, req);

        // 2. Filters, sorting and keyset cursor
        const { where, orderBy, params, types, sortColName } = buildTableFilter(columns, query);
        let sql = `SELECT * FROM "${tableName}"${where}${orderBy}`;

        // 3. Pagination. With keyset (`after`) we drop OFFSET entirely so
        // deep pages stay O(limit) instead of O(offset+limit).
        const safeLimit = Math.max(0, parseInt(limit) || 0);
        const safeOffset = Math.max(0, parseInt(offset) || 0);
        sql += ` LIMIT ${safeLimit}`;
        if (query.after === undefined && safeOffset > 0) sql += ` OFFSET ${safeOffset}`;

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

// GET /tables/:tableName/export?format=parquet|csv|ndjson - Download a whole
// (optionally filtered/sorted) table as a file. Same read-access check and the
// same filter/sort/after parameters as GET /tables/:tableName; ?limit= lowers
// the EXPORT_MAX_ROWS cap. See src/export.js for how each format is produced.
app.get('/tables/:tableName/export', async (req, res) => {
    try {
        const { tableName } = req.params;
        assertSafeIdentifier(tableName, 'tableName');
        const { format = 'parquet', limit, offset, ...query } = req.query;
        if (!Object.hasOwn(EXPORT_FORMATS, format)) {
            return res.status(400).json({ success: false, error: `Unsupported export format "${format}". Use one of: ${Object.keys(EXPORT_FORMATS).join(', ')}` });
        }
        let safeLimit = null;
        if (limit !== undefined) {
            safeLimit = parseInt(limit, 10);
            if (!Number.isInteger(safeLimit) || safeLimit < 0) {
                return res.status(400).json({ success: false, error: 'limit must be a non-negative integer' });
            }
        }

        const schema = await executeWithParams(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = ? ORDER BY ordinal_position`,
            [tableName], [VARCHAR]
        );
        if (schema.length === 0) {
            return res.status(404).json({ success: false, error: 'Table not found' });
        }
        await requireReadAccess(tableName, req);

        const { where, orderBy, params, types } = buildTableFilter(schema.map(c => c.column_name), query);
        // BLOBs travel base64-encoded in the text formats, like the JSON API.
        const selectList = schema.map(({ column_name: c, data_type: t }) =>
            format !== 'parquet' && /^BLOB/i.test(t) ? `base64("${c}") AS "${c}"` : `"${c}"`).join(', ');
        await runExport({
            format,
            selectSql: `SELECT ${selectList} FROM "${tableName}"${where}${orderBy}`,
            countSql: `SELECT count(*) FROM "${tableName}"${where}`,
            params, types,
            limit: safeLimit,
            filename: tableName,
            res,
        });
    } catch (error) {
        if (!res.headersSent) res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// POST /query - Raw SQL query (read-only, isolated DuckDB instance with timeout)
//
// Results are read from DuckDB in chunks and capped, so memory stays bounded:
//   JSON (default)  {success, rows, rowCount, truncated, maxRows}; at most
//                   QUERY_MAX_ROWS rows, within USER_QUERY_TIMEOUT_MS.
//   NDJSON          ?format=ndjson or Accept: application/x-ndjson. One row
//                   object per line, written as DuckDB produces them, then a
//                   final trailer line {"_ichibi":{rowCount,truncated,maxRows}}
//                   (plus "error" if the query failed mid-stream). At most
//                   QUERY_STREAM_MAX_ROWS rows, within USER_QUERY_STREAM_TIMEOUT_MS
//                   for the whole transfer.
// A positive integer `maxRows` in the body lowers (never raises) the cap.
const queryMaxRows = () => Math.max(1, parseInt(process.env.QUERY_MAX_ROWS || '100000', 10) || 100000);
const queryStreamMaxRows = () => Math.max(1, parseInt(process.env.QUERY_STREAM_MAX_ROWS || '10000000', 10) || 10000000);
const queryStreamTimeoutMs = () => Math.max(1000, parseInt(process.env.USER_QUERY_STREAM_TIMEOUT_MS || '300000', 10) || 300000);
const NDJSON_TYPE = 'application/x-ndjson';

function queryResponseFormat(req) {
    const requested = req.query.format;
    if (requested !== undefined) {
        if (requested === 'json' || requested === 'ndjson') return requested;
        const err = new Error(`Unsupported format "${requested}". Use json or ndjson.`);
        err.statusCode = 400;
        throw err;
    }
    return (req.headers.accept || '').includes(NDJSON_TYPE) ? 'ndjson' : 'json';
}

app.post('/query', async (req, res) => {
    let streaming = false;
    try {
        const { sql, maxRows } = req.body || {};
        if (!sql) return res.status(400).json({ error: 'Missing sql in body' });
        const format = queryResponseFormat(req);
        let requestedCap = Infinity;
        if (maxRows !== undefined) {
            requestedCap = Number(maxRows);
            if (!Number.isInteger(requestedCap) || requestedCap < 1) {
                return res.status(400).json({ success: false, error: 'maxRows must be a positive integer' });
            }
        }
        await requireSuperuser(req);
        assertReadOnlySql(sql);

        // Interrupt the query if the client goes away before we finish.
        const abort = new AbortController();
        res.on('close', () => { if (!res.writableFinished) abort.abort(); });

        if (format === 'json') {
            const cap = Math.min(queryMaxRows(), requestedCap);
            const rows = [];
            const { truncated } = await streamUserSql(sql, {
                maxRows: cap, signal: abort.signal,
                onRows: (batch) => { for (const r of batch) rows.push(r); },
            });
            if (truncated) res.setHeader('X-Result-Truncated', 'true');
            return res.json({ success: true, rows, rowCount: rows.length, truncated, maxRows: cap });
        }

        const cap = Math.min(queryStreamMaxRows(), requestedCap);
        const writeChunk = async (text) => {
            // The deadline or a disconnect can end the response while a chunk
            // is being converted; never write after that.
            if (res.writableEnded || res.destroyed) return;
            if (!res.write(text)) {
                await new Promise(resolve => {
                    const done = () => { res.off('drain', done); res.off('close', done); resolve(); };
                    res.on('drain', done);
                    res.on('close', done);
                });
            }
        };
        let summary;
        try {
            summary = await streamUserSql(sql, {
                maxRows: cap,
                timeoutMs: queryStreamTimeoutMs(),
                signal: abort.signal,
                // Headers go out only once the query has started producing a
                // result, so planning/binding errors still get a JSON error status.
                onColumns: () => {
                    streaming = true;
                    res.status(200);
                    res.setHeader('Content-Type', `${NDJSON_TYPE}; charset=utf-8`);
                    res.setHeader('X-Row-Cap', String(cap));
                },
                onRows: (batch) => writeChunk(batch.map(r => JSON.stringify(r)).join('\n') + '\n'),
            });
        } catch (err) {
            // Nothing sent yet (e.g. an execution error on the first chunk):
            // answer with a normal JSON error status instead of a 200 stream.
            if (!streaming || !res.headersSent) {
                res.removeHeader('Content-Type');
                res.removeHeader('X-Row-Cap');
                throw err;
            }
            if (!res.destroyed) res.end(JSON.stringify({ _ichibi: { error: err.message, status: err.statusCode || 500 } }) + '\n');
            return;
        }
        res.end(JSON.stringify({ _ichibi: { rowCount: summary.rowCount, truncated: summary.truncated, maxRows: cap } }) + '\n');
    } catch (error) {
        if (res.headersSent || res.destroyed) return;
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
    sql: async ({ query }, context) => {
        try {
            await requireSuperuser({ apiKey: context?.apiKey });
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
            // Hide internal "_"-prefixed bookkeeping tables (_table_acls,
            // _rbac_config, ...) from the catalog, matching GET /tables.
            return [...new Set(rows.map(r => r.name))].filter(name => !name.startsWith('_'));
        } catch (error) {
            throw new Error(error.message);
        }
    }
};

app.all('/graphql', createHandler({
    schema: schema,
    rootValue: root,
    // Expose the authenticated API key to resolvers (the sql resolver is
    // superuser-only).
    context: (req) => ({ apiKey: req.raw?.apiKey }),
}));

// --- Graph Query Endpoints (SQL/PGQ via DuckPGQ) ---
// Identifier validation (isSafeIdentifier) and CREATE PROPERTY GRAPH DDL
// generation (buildCreatePropertyGraphSql) live in src/graphs.js so they can be
// unit-tested in isolation and reused by the durable registry in src/db.js.

// GET /graphs - List all registered property graphs.
// The durable registry (DuckLake table) is the source of truth, so graphs are
// listed consistently regardless of which in-memory instance has materialized
// them.
app.get('/graphs', async (req, res) => {
    try {
        const graphs = await listGraphDefinitions();
        res.json({ success: true, graphs });
    } catch (error) {
        res.status(500).json({ success: false, error: error.message });
    }
});

// POST /graphs - Create (or replace) a property graph.
// The definition is validated, persisted to the durable registry, then
// materialized on the primary instance. Other instances materialize it lazily
// on first query (see ensureGraphMaterialized).
app.post('/graphs', async (req, res) => {
    try {
        await requireSuperuser(req);
        // Validate + build first so malformed bodies fail with 400 before we
        // touch the registry.
        const sql = buildCreatePropertyGraphSql(req.body);
        const name = req.body.name;
        const referenced = [
            ...(req.body.vertexTables || []).map(v => (typeof v === 'string' ? v : v?.name)),
            ...(req.body.edgeTables || []).flatMap(e => [e?.name, e?.source, e?.destination]),
        ];
        const system = referenced.find(isSystemTableName);
        if (system) return res.status(400).json({ success: false, error: `Invalid table "${system}": system tables cannot be used in property graphs` });

        await saveGraphDefinition(name, req.body, req.apiKey || null);

        // Materialize on the primary instance, replacing any prior version so
        // an edited definition takes effect immediately here.
        try { await execute(`DROP PROPERTY GRAPH ${name}`); } catch (_) { /* not yet materialized */ }
        await execute(sql);

        res.json({ success: true, message: `Property graph ${name} created`, sql });
    } catch (error) {
        const status = error.statusCode || (/Invalid|must be/.test(error.message) ? 400 : 500);
        res.status(status).json({ success: false, error: error.message });
    }
});

// DELETE /graphs/:graphName - Drop a property graph and remove its registration.
app.delete('/graphs/:graphName', async (req, res) => {
    try {
        const { graphName } = req.params;
        if (!isSafeIdentifier(graphName)) {
            return res.status(400).json({ success: false, error: 'Invalid graph name' });
        }
        await requireSuperuser(req);
        await deleteGraphDefinition(graphName);
        // Drop the materialized catalog entry on the primary instance too. The
        // user instance simply stops being able to re-materialize it.
        try { await execute(`DROP PROPERTY GRAPH ${graphName}`); } catch (_) { /* not materialized here */ }
        res.json({ success: true, message: `Property graph ${graphName} dropped` });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    }
});

// POST /graphs/:graphName/query - Run a SQL/PGQ pattern match query.
app.post('/graphs/:graphName/query', async (req, res) => {
    try {
        const { graphName } = req.params;
        if (!isSafeIdentifier(graphName)) {
            return res.status(400).json({ success: false, error: 'Invalid graph name' });
        }

        // Graph query fragments (match/where/columns/graphTable) can embed
        // subqueries over any table, so this is user-supplied SQL.
        await requireSuperuser(req);

        const body = req.body || {};
        const { match, where, columns, graphTable, limit } = body;

        // Block statement stacking / comment injection in the user-supplied
        // pattern fragments before they are concatenated into the GRAPH_TABLE
        // body below. assertReadOnlySql (further down) only blocks DDL/DML
        // keywords and cannot see tokens hidden behind a stacked statement.
        assertSafeGraphFragment(graphTable, 'graphTable');
        assertSafeGraphFragment(match, 'match');
        assertSafeGraphFragment(where, 'where');
        assertSafeGraphFragment(columns, 'columns');

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
        // GRAPH_TABLE traversal cannot stall the primary REST pool. The graph
        // is lazily (re)materialized from the durable registry on that
        // instance's connection just before the query runs — this is what makes
        // graphs created on the primary visible to the user instance. A missing
        // registration surfaces as a 404 via ensureGraphMaterialized.
        const rows = await executeUserSql(sql, {
            prepare: (conn) => ensureGraphMaterialized(conn, graphName),
        });
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

// POST /kafka-sink - convenience endpoint for event stream ingestion.
// Honours Idempotency-Key (src/idempotency.js); the topic header is part of
// the request identity.
app.post('/kafka-sink', idempotentJson(req => String(req.headers['x-kafka-topic'] || '')), async (req, res) => {
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
    // The topic names the target table; "_"-prefixed names are reserved for
    // system tables and must not be created or written through this endpoint.
    if (isSystemTableName(topic)) {
        return res.status(400).json({ error: `Invalid topic "${topic}": names starting with "_" are reserved for system tables.` });
    }

    const data = req.body;

    if (!Array.isArray(data)) {
        return res.status(400).json({ error: 'Expected an array of messages' });
    }

    const tempFileName = `kafka_sink_${randomUUID()}.json`;
    const tempFilePath = path.join(process.cwd(), tempFileName);

    try {
        const targetApiKey = req.apiKey || '';

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
            // Create the table when it does not appear to exist yet. Catch the
            // "already exists" race (concurrent sink/upload for the same topic)
            // and fall through to the existing-table path, which enforces write
            // access before appending the losing request's messages.
            let mustEvolveExisting = tableExists;
            if (!tableExists) {
                try {
                    await conn.run(`CREATE TABLE "${topic}" AS SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                    createdTable = true;
                    await ensureTableAcl(topic, targetApiKey);
                    invalidateColumnCache(topic);
                } catch (createErr) {
                    if (/already exists/i.test(createErr.message || '')) {
                        invalidateColumnCache(topic);
                        mustEvolveExisting = true;
                    } else {
                        throw createErr;
                    }
                }
            }

            if (mustEvolveExisting) {
                await requireWriteAccess(topic, req);
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

        res.json({ success: true, message: `Sinked ${data.length} messages to ${topic}` });
    } catch (error) {
        res.status(error.statusCode || 500).json({ success: false, error: error.message });
    } finally {
        // Always remove the spooled payload, even on the error paths, so a
        // failed sink cannot leak the request's JSON to disk indefinitely.
        try { await fs.unlink(tempFilePath); } catch (e) { /* already gone */ }
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
        const colTypeMap = new Map(colRows.map(([col, type]) => [col, type]));
        const selectCols = colRows.map(([col, type]) => {
            const safeCol = col.replace(/"/g, '""');
            if (type.toUpperCase().startsWith('TIMESTAMP') || type.toUpperCase().startsWith('DATE')) {
                return `CAST("${safeCol}" AS VARCHAR) AS "${safeCol}"`;
            }
            return `"${safeCol}"`;
        });
        let sql = `SELECT ${selectCols.join(', ')} FROM ${quoted}`;
        const params = [];
        // Only apply the `since` time filter when the target column actually
        // exists on the table. Tables without the filter column (default
        // `updated`) would otherwise raise "column does not exist" and abort
        // the dump before the dump-complete event is emitted.
        if (since) {
            const filterColumn = sinceColumn || 'updated';
            if (colTypeMap.has(filterColumn)) {
                const colType = (colTypeMap.get(filterColumn) || '').toUpperCase();
                // Epoch-seconds columns (e.g. BIGINT/INTEGER timestamps that
                // predate the timestamp migration) can't be compared against an
                // ISO-8601 string — DuckDB raises a conversion error and the
                // dump aborts. Detect numeric columns and bind epoch seconds;
                // otherwise bind an ISO string for native TIMESTAMP/DATE/VARCHAR.
                const isNumeric = !colType.startsWith('TIMESTAMP') && !colType.startsWith('DATE')
                    && /INT|DECIMAL|NUMERIC|DOUBLE|FLOAT|REAL/.test(colType);
                sql += ` WHERE "${filterColumn.replace(/"/g, '""')}" >= ? ORDER BY "${filterColumn.replace(/"/g, '""')}" ASC`;
                params.push(isNumeric ? Math.floor(new Date(since).getTime() / 1000) : new Date(since).toISOString());
            }
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

// POST /upload - Ingest data. Honours Idempotency-Key (src/idempotency.js).
app.post(['/upload', '/upload/:tableName'], idempotentJson(), async (req, res) => {
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
    if (data.length > maxBatchSize()) {
        return res.status(400).json({ success: false, error: `Batch too large. Maximum ${maxBatchSize()} rows allowed.` });
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
            // Attempt to create the table when it does not appear to exist yet.
            // A concurrent /upload for the same name can win the race between our
            // existence check above and this CREATE; we catch "already exists"
            // and fall through to the existing-table path, which enforces write
            // access before inserting the losing request's rows.
            let mustEvolveExisting = tableExists;
            if (!tableExists) {
                try {
                    await conn.run(`CREATE TABLE "${tableName}" AS SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                    createdTable = true;
                    await ensureTableAcl(tableName, targetApiKey);
                    invalidateColumnCache(tableName);
                } catch (createErr) {
                    if (/already exists/i.test(createErr.message || '')) {
                        invalidateColumnCache(tableName);
                        mustEvolveExisting = true;
                    } else {
                        throw createErr;
                    }
                }
            }

            if (mustEvolveExisting) {
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

// POST /tables/:tableName/import - bulk import of a file (CSV, Parquet, NDJSON
// or a JSON array) sent as the raw request body, or, with a JSON body
// { "source": "s3://bucket/prefix/*.parquet", ... }, of objects in object
// storage. Modes (?mode=): auto (default: create when missing, else append),
// create, append, replace. Runs on the primary instance via importFile()
// (src/console/data.js), shared with the admin console's import.
app.post('/tables/:tableName/import', async (req, res) => {
    const tableName = req.params.tableName;
    let file = null;
    let bodyRead = false;
    try {
        assertSafeIdentifier(tableName, 'tableName');
        // Check before accepting the upload, so a caller without write
        // access cannot make the gateway spool a large body.
        await requireWriteAccess(tableName, req);
        idempotencyKeyOf(req); // 400 for a malformed key, before the upload

        const contentType = req.headers['content-type'] || '';
        const hash = requestHasher(req);
        hash.update(`${contentType}\n`);
        file = await importTempFile('upload');
        const size = await receiveUpload(req, file, importMaxBytes(), { onChunk: (chunk) => hash.update(chunk) });
        bodyRead = true;
        if (await beginIdempotentRequest(req, res, hash.digest('hex'))) return;

        const q = req.query;
        const flag = (v, dflt) => (v === undefined || v === null || v === '' ? dflt : !(v === false || /^(0|false|no)$/i.test(String(v))));
        const sourceRequest = !q.format && contentTypeFormat(contentType) === 'json' ? await readSourceRequest(file, size) : null;
        let options;
        if (sourceRequest) {
            const parsed = parseImportSource(sourceRequest.source);
            if (!isImportSourceAllowed(parsed) && !(await isSuperuserKey(req.apiKey))) {
                const err = new Error('Forbidden: this source is not under a prefix in IMPORT_ALLOWED_SOURCES; only superusers may import from it.');
                err.statusCode = 403;
                throw err;
            }
            options = {
                source: parsed.source,
                remote: true,
                prepare: prepareS3Import,
                format: detectImportFormat(sourceRequest.format ? String(sourceRequest.format) : '', parsed.key.replace(/[*?[\]{}]+$/, '')),
                mode: String(sourceRequest.mode ?? q.mode ?? 'auto'),
                addColumns: flag(sourceRequest.addColumns ?? q.addColumns, true),
                dryRun: flag(sourceRequest.dryRun ?? q.dryRun, false),
            };
        } else {
            const explicit = q.format ? String(q.format) : contentTypeFormat(contentType);
            if (!explicit) {
                const err = new Error('Set the file format with ?format=csv|parquet|json|ndjson or a matching Content-Type (text/csv, application/vnd.apache.parquet, application/json, application/x-ndjson).');
                err.statusCode = 400;
                throw err;
            }
            options = {
                source: file,
                format: detectImportFormat(explicit),
                mode: String(q.mode || 'auto'),
                addColumns: flag(q.addColumns, true),
                dryRun: flag(q.dryRun, false),
            };
        }

        const { source, ...importOptions } = options;
        const result = await importFile(source, { ...importOptions, table: tableName });
        if (result.created) {
            await ensureTableAcl(tableName, req.apiKey || '');
            await applyDefaultPartitioning(tableName);
        }
        if (!result.dryRun) {
            console.log(`[Import] ${tableName}: ${result.inserted} row(s) ${result.mode === 'create' ? 'into new table' : `(${result.mode})`} from ${options.remote ? source : `upload (${size} bytes)`} as ${result.format}`);
        }
        res.json({ success: true, ...result, ...(options.remote ? { source } : {}) });
    } catch (error) {
        const status = error.statusCode || 500;
        if (status >= 500) console.error('Import error:', error);
        // Do not wait for (or keep the connection open for) a body that was
        // never read.
        if (!bodyRead) res.set('Connection', 'close');
        if (!res.headersSent) res.status(status).json({ success: false, error: error.message });
    } finally {
        if (file) fs.unlink(file).catch(() => {});
    }
});

// --- 4. Admin API (RBAC management) ---
app.use('/admin', adminRouter);

// --- 5. Swagger UI ---
const swaggerDocument = YAML.load(path.join(process.cwd(), 'swagger.yaml'));
app.use('/', swaggerUi.serve);
app.get('/', swaggerUi.setup(swaggerDocument));

// --- Start Server ---
/**
 * Startup steps, in order. Each is idempotent (CREATE ... IF NOT EXISTS,
 * check-then-insert bootstraps, migrations guarded by _gateway_meta), so a
 * step that loses a DuckLake commit race with another instance starting
 * against the same catalog is simply run again.
 */
const STARTUP_STEPS = [
    ['RBAC tables', initRbacTables],
    // Console overrides (e.g. an API_KEYS list edited in the console) must
    // be in effect before the bootstrap below promotes/re-activates keys.
    ['setting overrides', loadSettingOverrides],
    ['RBAC bootstrap', () => (isRbacConfigured() ? bootstrapRbac() : legacyBootstrap())],
    ['ACL table', initAclTable],
    ['graph registry', initGraphRegistry],
    ['table migration', migrateExistingTables],
    ['ACL migration', migrateToAcl],
    ['timestamp migration', migrateTimestamps],
    // Admin console: loads setting overrides (applied to process.env) and
    // starts the retention scheduler. Runs after RBAC/ACL bootstrap so an
    // API_KEYS override can reconcile against the bootstrapped users.
    // Also starts scheduled lake maintenance, which compacts small files
    // at startup and on its schedule (src/console/maintenance.js).
    ['admin console', initConsole],
];
const STARTUP_ATTEMPTS = parseInt(process.env.STARTUP_ATTEMPTS || '5', 10);

async function runStartupSteps() {
    for (const [name, step] of STARTUP_STEPS) {
        await retryOnConflict(step, { attempts: 8, label: `Startup: ${name}` });
    }
}

app.listen(port, '0.0.0.0', async () => {
    console.log(`ichibi-lake server listening on 0.0.0.0:${port}`);
    console.log(`[Server] Swagger UI:    ${publicBaseUrl()}/`);
    console.log(`[Server] Admin console: ${publicBaseUrl()}/console`);
    // A failed step would otherwise leave the gateway half-initialised (the
    // console answering 503 for good), so retry the whole sequence and, if it
    // still fails, exit and let the supervisor (Docker restart policy, PM2)
    // start a fresh process.
    for (let attempt = 1; ; attempt++) {
        try {
            await runStartupSteps();
            setStartupState('ready');
            break;
        } catch (err) {
            console.error(`Startup Error (attempt ${attempt}/${STARTUP_ATTEMPTS}):`, err);
            if (attempt >= STARTUP_ATTEMPTS) {
                setStartupState('failed');
                console.error('[Server] Startup failed; exiting so the process can be restarted.');
                process.exit(1);
            }
            await new Promise(r => setTimeout(r, 2000 * attempt));
        }
    }
    startKafkaConsumer().catch(err => console.error('Kafka Consumer Error:', err));
});
