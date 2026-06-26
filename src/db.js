import { DuckDBInstance, VARCHAR, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE } from '@duckdb/node-api';
import fs from 'fs/promises';
import zlib from 'zlib';

export { VARCHAR, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE };

let instance = null;
// Promise-based guard: prevents concurrent callers from racing into fromCache()
// while initialization is already in progress.
let initPromise = null;

// Per-table column-name cache. Avoids the per-request `SELECT * LIMIT 0` probe
// and repeated information_schema lookups. Invalidated by callers after any
// ALTER TABLE / CREATE TABLE on the named table.
const COLUMN_CACHE_TTL_MS = parseInt(process.env.COLUMN_CACHE_TTL_MS || '60000', 10);
const columnCache = new Map();

export function invalidateColumnCache(tableName) {
    if (tableName) columnCache.delete(tableName);
    else columnCache.clear();
}

// Primary-instance connection pool. DuckDB connections are lightweight, but
// the per-connection setup (USE, search_path, LOAD) adds up under load.
// We keep up to PRIMARY_POOL_SIZE warm connections and recycle them instead of
// opening + closing on every request.
const PRIMARY_POOL_SIZE = Math.max(1, parseInt(process.env.PRIMARY_POOL_SIZE || '4', 10));
const primaryPool = [];

// Explicitly sized Parquet row groups so DuckLake doesn't write millions of
// tiny row groups (which kills scan performance). Overridable via env var.
const PARQUET_ROW_GROUP_SIZE = Math.max(1000, parseInt(process.env.PARQUET_ROW_GROUP_SIZE || '100000', 10));

// Configure a fresh in-memory DuckDB instance to act as a DuckLake gateway.
// Used for both the primary instance (writes + reads) and the isolated user-
// query instance (read-only ad-hoc SQL) so they share the same setup path.
async function _attachDuckLake(label) {
    const pgHost = process.env.DUCKLAKE_PG_HOST || '0.0.0.0';
    const pgPort = process.env.DUCKLAKE_PG_PORT || '5432';
    const pgUser = process.env.DUCKLAKE_PG_USER || 'postgres';
    const pgPass = process.env.DUCKLAKE_PG_PASSWORD || 'postgres';
    const pgDb = process.env.DUCKLAKE_PG_DB || 'ducklake';
    const dataPath = process.env.DUCKLAKE_DATA_PATH || 'data_files/';

    const inst = await DuckDBInstance.create(':memory:', { allow_unsigned_extensions: 'true' });
    const conn = await inst.connect();

    console.log(`[DB:${label}] Initializing DuckLake connection to ${pgHost}:${pgPort} (data path: ${dataPath})...`);

    try {
        await fs.mkdir(dataPath, { recursive: true });
    } catch (mkErr) {
        console.warn(`[DB:${label}] Could not create data path ${dataPath}:`, mkErr.message || mkErr);
    }

    await conn.run('INSTALL ducklake; LOAD ducklake;');
    await conn.run('INSTALL postgres; LOAD postgres;');

    if (process.env.DISABLE_DUCKPGQ === '1') {
        console.warn(`[DB:${label}] DuckPGQ disabled via DISABLE_DUCKPGQ=1 (graph queries unavailable).`);
    } else {
        try {
            // DuckPGQ has no published binary for DuckDB ≥1.5.2, so we
            // download the last available build (v1.3.1) from the CWI S3
            // bucket and install it from a local path.
            const pgqUrl = 'http://duckpgq.s3.eu-north-1.amazonaws.com/v1.3.1/linux_arm64/duckpgq.duckdb_extension.gz';
            const pgqDest = '/tmp/duckpgq.duckdb_extension';
            try { await fs.unlink(pgqDest); } catch (_) { /* never mind */ }
            const resp = await fetch(pgqUrl);
            if (!resp.ok) throw new Error(`HTTP ${resp.status} fetching DuckPGQ extension`);
            const buf = Buffer.from(await resp.arrayBuffer());
            const decompressed = zlib.gunzipSync(buf);
            await fs.writeFile(pgqDest, decompressed);
            await conn.run(`FORCE INSTALL '${pgqDest}';`);
            await conn.run("LOAD 'duckpgq';");
            console.log(`[DB:${label}] DuckPGQ graph extension loaded.`);
        } catch (pgqErr) {
            console.warn(`[DB:${label}] DuckPGQ extension failed to load (graph queries disabled):`, pgqErr.message || pgqErr);
        }
    }

    const metadataPath = `postgres:host=${pgHost} port=${pgPort} user=${pgUser} password=${pgPass} dbname=${pgDb}`;
    await conn.run(`
        CREATE SECRET (
            TYPE ducklake,
            METADATA_PATH '${metadataPath}'
        );
    `);

    const escapedDataPath = dataPath.replace(/'/g, "''");
    await conn.run(`ATTACH 'ducklake:' AS ducklake (DATA_PATH '${escapedDataPath}', AUTOMATIC_MIGRATION true);`);
    await conn.run("USE ducklake;");

    console.log(`[DB:${label}] Successfully attached to remote DuckLake database '${pgDb}'.`);

    if (conn && typeof conn.close === 'function') {
        try { await conn.close(); } catch (e) { }
    }
    return inst;
}

async function getDbInstance() {
    // If we have an existing instance, verify it is still alive.
    // DuckDB invalidates the entire instance after a fatal error, so we must
    // detect that and recreate rather than hand out dead connections.
    if (instance) {
        try {
            const testConn = await instance.connect();
            try {
                await testConn.run('SELECT 1');
            } finally {
                if (testConn && typeof testConn.close === 'function') {
                    try { await testConn.close(); } catch (e) { }
                }
            }
            return instance;
        } catch (err) {
            const msg = err.message || String(err);
            if (msg.includes('database has been invalidated') || msg.includes('FATAL Error')) {
                console.warn('[DB] Existing DuckDB instance invalidated (fatal error), recreating...');
            } else {
                console.warn('[DB] DuckDB health check failed:', msg);
            }
            instance = null;
            initPromise = null;
        }
    }

    if (initPromise) return initPromise;

    initPromise = (async () => {
        try {
            instance = await _attachDuckLake('primary');
            return instance;
        } catch (err) {
            initPromise = null;
            instance = null;
            console.error('[DB] Failed to connect to DuckLake instance:', err);
            throw err;
        }
    })();

    return initPromise;
}

/**
 * Creates a fresh connection to the singleton instance.
 * Using per-request connections is safer for high-concurrency than a singleton connection.
 */
export async function getConnection() {
    const db = await getDbInstance();

    // Reuse a warmed connection from the pool if one is available.
    if (primaryPool.length > 0) {
        return primaryPool.pop();
    }

    const conn = await db.connect();
    // Ensure every connection defaults to the DuckLake catalog.
    // Testing shows that session state like 'USE' does not persist across connections.
    try {
        await conn.run("USE ducklake;");
        // Ensure tables in memory (like 'sardal_system_events') are queryable
        // without a prefix, making their location transparent to clients.
        await conn.run("SET search_path = 'ducklake.main,memory.main';");
    } catch (err) {
        console.error("[DB] Failed to set search path to DuckLake:", err);
    }
    // Ensure graph query support is available on this connection. The
    // extension was installed at startup; LOAD is cheap and idempotent.
    // Skip entirely when DISABLE_DUCKPGQ=1 so we never trigger a native
    // crash from a broken duckpgq binary on a per-request basis.
    if (process.env.DISABLE_DUCKPGQ !== '1') {
        try {
            await conn.run("LOAD duckpgq;");
        } catch (err) {
            // Non-fatal: graph endpoints will surface a clearer error if used.
        }
    }
    // NOTE: SET parquet_row_group_size is not recognised by some DuckDB
    // versions and would spam the logs with harmless warnings. Row-group
    // sizing for Parquet exports is controlled at COPY-time instead.
    // (The previous SET call was removed after producing the following log
    // noise: "Catalog Error: unrecognized configuration parameter ...".)
    return conn;
}

/**
 * Return a primary connection to the pool instead of closing it.
 * When the pool is full the connection is actually closed.
 */
export function releaseConnection(conn) {
    if (!conn) return;
    if (primaryPool.length < PRIMARY_POOL_SIZE) {
        primaryPool.push(conn);
    } else {
        if (typeof conn.close === 'function') {
            conn.close().catch(() => {});
        }
    }
}

// Convert a DuckDB result object into an array of plain JS objects.
// Shared between execute(), executeWithParams() and executeUserSql().
async function _readRows(result) {
    const columns = result.columnNames();
    const rowSet = await result.getRows();
    const results = [];
    if (!rowSet) return results;

    const convertValue = (val) => {
        if (val === null || val === undefined) return null;
        if (typeof val === 'object' && val.constructor.name === 'DuckDBBlobValue' && val.bytes) {
            return Buffer.from(val.bytes).toString('base64');
        }
        if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
            return Buffer.from(val).toString('base64');
        }
        return val;
    };

    if (typeof rowSet[Symbol.asyncIterator] === 'function' || typeof rowSet[Symbol.iterator] === 'function') {
        for await (const row of rowSet) {
            const obj = {};
            columns.forEach((col, idx) => { obj[col] = convertValue(row[idx]); });
            results.push(obj);
        }
    } else if (typeof rowSet.next === 'function') {
        let entry = rowSet.next();
        while (entry && !entry.done) {
            const row = entry.value;
            const obj = {};
            columns.forEach((col, idx) => { obj[col] = convertValue(row[idx]); });
            results.push(obj);
            entry = rowSet.next();
        }
    }
    return results;
}

/**
 * Helper to execute SQL and return rows as objects.
 * Automatically manages connection lifecycle for safety.
 */
export async function execute(sql) {
    let conn;
    try {
        conn = await getConnection();
        const result = await conn.run(sql);
        return await _readRows(result);
    } finally {
        releaseConnection(conn);
    }
}

/**
 * Parameter-bound variant of execute(). `params` is an array of values,
 * `types` is the matching array of DuckDB type constants. Prefer this
 * helper over string concatenation whenever values come from user input.
 */
export async function executeWithParams(sql, params, types) {
    let conn;
    try {
        conn = await getConnection();
        const result = await conn.run(sql, params, types);
        return await _readRows(result);
    } finally {
        releaseConnection(conn);
    }
}

/**
 * Managed connection wrapper for complex transactions.
 */
export async function withConnection(fn) {
    const conn = await getConnection();
    try {
        return await fn(conn);
    } finally {
        releaseConnection(conn);
    }
}

/**
 * Returns the ordered list of column names for a table, cached for
 * COLUMN_CACHE_TTL_MS. Callers that mutate the schema must invoke
 * invalidateColumnCache(tableName).
 */
export async function getTableColumns(tableName) {
    const now = Date.now();
    const cached = columnCache.get(tableName);
    if (cached && cached.expiresAt > now) return cached.columns;

    const rows = await executeWithParams(
        `SELECT column_name FROM information_schema.columns
         WHERE table_name = ? ORDER BY ordinal_position`,
        [tableName],
        [VARCHAR]
    );
    const columns = rows.map(r => r.column_name);
    columnCache.set(tableName, { columns, expiresAt: now + COLUMN_CACHE_TTL_MS });
    return columns;
}

// --- Isolated DuckDB instance for user-supplied SQL ---
// Heavy ad-hoc queries from /query, /graphql and /graphs/:graphName/query
// must not be able to stall the primary read/write instance that the
// REST endpoints depend on. We host them on a second in-memory DuckDB
// instance with the same DuckLake attachment, and gate them with a
// semaphore + wall-clock timeout.

let userInstance = null;
let userInitPromise = null;

async function getUserDbInstance() {
    if (userInstance) {
        try {
            const t = await userInstance.connect();
            try { await t.run('SELECT 1'); } finally {
                if (t && typeof t.close === 'function') { try { await t.close(); } catch (e) { } }
            }
            return userInstance;
        } catch (err) {
            console.warn('[DB:user] Health check failed, recreating instance:', err.message || err);
            userInstance = null;
            userInitPromise = null;
        }
    }
    if (userInitPromise) return userInitPromise;
    userInitPromise = (async () => {
        try {
            userInstance = await _attachDuckLake('user');
            return userInstance;
        } catch (err) {
            userInitPromise = null;
            userInstance = null;
            throw err;
        }
    })();
    return userInitPromise;
}

async function getUserConnection() {
    const db = await getUserDbInstance();
    const conn = await db.connect();
    try {
        await conn.run("USE ducklake;");
        await conn.run("SET search_path = 'ducklake.main,memory.main';");
    } catch (err) {
        console.error("[DB:user] Failed to set search path:", err);
    }
    if (process.env.DISABLE_DUCKPGQ !== '1') {
        try { await conn.run("LOAD duckpgq;"); } catch (err) { /* non-fatal */ }
    }
    // (Same note as in getConnection: parquet_row_group_size is not a
    // session-level parameter in current DuckDB versions.)
    return conn;
}

const USER_QUERY_CONCURRENCY = Math.max(1, parseInt(process.env.USER_QUERY_CONCURRENCY || '4', 10));
const USER_QUERY_TIMEOUT_MS = Math.max(1000, parseInt(process.env.USER_QUERY_TIMEOUT_MS || '30000', 10));

let userActive = 0;
const userQueue = [];

function acquireUserSlot() {
    if (userActive < USER_QUERY_CONCURRENCY) {
        userActive++;
        return Promise.resolve();
    }
    return new Promise(resolve => userQueue.push(() => { userActive++; resolve(); }));
}

function releaseUserSlot() {
    userActive--;
    const next = userQueue.shift();
    if (next) next();
}

/**
 * Runs read-only user-supplied SQL on the isolated user instance, with a
 * concurrency cap and a wall-clock timeout. When the timeout fires the
 * caller receives a 504-tagged error; the underlying DuckDB query is
 * interrupted on a best-effort basis (DuckDB does not guarantee that
 * every operator honours interruption immediately).
 */
export async function executeUserSql(sql, { timeoutMs = USER_QUERY_TIMEOUT_MS } = {}) {
    await acquireUserSlot();
    let conn;
    let timer;
    let timedOut = false;
    try {
        conn = await getUserConnection();

        // Engine-level deadline (DuckDB ≥ recent versions). If the SET fails
        // we silently fall back to the wall-clock race below.
        try { await conn.run(`SET max_execution_time = ${Number(timeoutMs)}`); } catch (e) { /* unsupported */ }

        const queryPromise = (async () => {
            const result = await conn.run(sql);
            return await _readRows(result);
        })();

        const timeoutPromise = new Promise((_, reject) => {
            timer = setTimeout(() => {
                timedOut = true;
                if (conn && typeof conn.interrupt === 'function') {
                    try { conn.interrupt(); } catch (e) { /* ignore */ }
                }
                const err = new Error(`Query exceeded ${timeoutMs}ms timeout`);
                err.statusCode = 504;
                reject(err);
            }, timeoutMs);
        });

        return await Promise.race([queryPromise, timeoutPromise]);
    } catch (err) {
        if (timedOut && !err.statusCode) err.statusCode = 504;
        throw err;
    } finally {
        if (timer) clearTimeout(timer);
        if (conn && typeof conn.close === 'function') {
            try { await conn.close(); } catch (e) { }
        }
        releaseUserSlot();
    }
}

/**
 * Run DuckLake maintenance (compaction, snapshot expiry, orphaned-file
 * cleanup) on the primary instance. Safe to call repeatedly.
 *
 * Invoked at startup so that gateways that have been ingesting one row
 * per request for a long time don't accumulate millions of tiny Parquet
 * files. Logs and swallows errors so a maintenance failure does not
 * prevent the gateway from coming up.
 */
export async function compactDuckLake() {
    const start = Date.now();
    console.log('[Compact] Running DuckLake CHECKPOINT (merge_adjacent_files + cleanup)...');
    try {
        await withConnection(async (conn) => {
            await conn.run('CHECKPOINT ducklake');
        });
        console.log(`[Compact] CHECKPOINT done in ${Date.now() - start}ms`);
        return;
    } catch (err) {
        console.warn('[Compact] CHECKPOINT failed, falling back to merge_adjacent_files:', err.message || err);
    }
    try {
        await withConnection(async (conn) => {
            await conn.run('CALL ducklake.merge_adjacent_files()');
        });
        console.log(`[Compact] merge_adjacent_files done in ${Date.now() - start}ms`);
    } catch (err) {
        console.warn('[Compact] merge_adjacent_files also failed:', err.message || err);
    }
}
