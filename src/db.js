import { DuckDBInstance, VARCHAR, BIGINT, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE } from '@duckdb/node-api';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { buildCreatePropertyGraphSql } from './graphs.js';

export { VARCHAR, BIGINT, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE };

let instance = null;
// Promise-based guard: prevents concurrent callers from racing into fromCache()
// while initialization is already in progress.
let initPromise = null;
// Monotonic generation counter for the primary instance. A DuckLake native
// fatal error (e.g. the DuckLakeInsert crash on tables with stale inline data)
// invalidates the ENTIRE DuckDBInstance, and every connection opened from it is
// permanently dead. When getDbInstance() rebuilds the instance we bump this
// counter; pooled connections are tagged with the generation they were born
// under (see getConnection/releaseConnection) so we can discard the poisoned
// ones instead of handing them back out.
let instanceGeneration = 0;

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

// Optimizers that must be disabled to avoid a DuckDB engine bug that corrupts
// VARCHAR columns read from DuckLake inline (Postgres-resident) row data under
// blocking operators (ORDER BY / DISTINCT / GROUP BY). DuckLake exposes
// *truncated* min/max string statistics for inline columns; DuckDB then (a)
// propagates those bounds via statistics_propagation and (b) compresses
// sort/group keys from them via compressed_materialization. Both paths
// mis-reconstruct the strings, yielding either single-character garbage (a
// 9-char person_id collapsing to "9") or truncation (e.g. "102020058" ->
// "1020200" when sorting on the column itself). Disabling statistics_propagation
// alone fixes every observed case; we also disable compressed_materialization so
// neither consumer of the bad statistics can resurface the bug.
//
// Exported so the regression test (test/order-by-corruption.test.js) asserts the
// workaround is applied through this exact production path: if the fix is removed
// or weakened, the test fails.
export const DISABLED_OPTIMIZERS = 'statistics_propagation,compressed_materialization';

// Apply the DISABLED_OPTIMIZERS workaround instance-wide. SET GLOBAL persists
// across every connection opened from the same DuckDBInstance, so this is run
// once per instance during attach. Non-destructive; logs and swallows errors so
// a setting failure does not prevent the gateway from coming up.
export async function applyOptimizerWorkaround(conn, label = 'db') {
    try {
        await conn.run(`SET GLOBAL disabled_optimizers = '${DISABLED_OPTIMIZERS}';`);
    } catch (optErr) {
        console.warn(`[DB:${label}] Could not disable corruption-triggering optimizers:`, optErr.message || optErr);
    }
}

// Since DuckDB 2.0, parser extensions must be activated per session via
// active_grammar_extensions (it cannot be SET GLOBAL), otherwise
// CREATE PROPERTY GRAPH / DROP PROPERTY GRAPH / GRAPH_TABLE fail with a parser
// error even though DuckPGQ is loaded. Enabling it has no effect on plain SQL,
// so it is applied unconditionally because callers cannot know in advance
// whether a statement uses SQL/PGQ.
//
// It must run as its OWN statement before the SQL that needs it: DuckDB parses
// a whole multi-statement string before executing any of it, so prepending
// "SET active_grammar_extensions = ...;" to the same string still fails to
// parse the PGQ part. And because a RESET (or a failed LOAD at connection
// creation) leaves a session without it, it is re-applied every time a
// connection is handed out rather than only once when the connection is
// created. Non-fatal: graph endpoints surface a clearer error if used.
export async function enableDuckPgqGrammar(conn) {
    if (process.env.DISABLE_DUCKPGQ === '1') return;
    try {
        await conn.run("SET active_grammar_extensions = ['duckpgq'];");
    } catch (err) {
        // The setting is unknown until duckpgq is loaded on this connection.
        try {
            await conn.run("LOAD duckpgq;");
            await conn.run("SET active_grammar_extensions = ['duckpgq'];");
        } catch (_) { /* non-fatal */ }
    }
}

// Load DuckPGQ on a freshly created connection and opt it in to its grammar.
async function loadDuckPgq(conn) {
    if (process.env.DISABLE_DUCKPGQ === '1') return;
    try { await conn.run("LOAD duckpgq;"); } catch (err) { /* non-fatal */ }
    await enableDuckPgqGrammar(conn);
}

// Admin-console state (logins, settings incl. the session secret, retention
// rules, audit log) lives in its own DuckLake catalog, stored in a separate
// Postgres schema with all rows inlined in Postgres (no Parquet files). Only
// the primary instance attaches it; the user-query instance, which runs
// user-supplied SQL, cannot see it.
export const CONSOLE_CATALOG = 'ichibi_console';

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
            // DuckPGQ ships from the DuckDB community extensions repo for this
            // DuckDB version. INSTALL resolves the correct platform/version
            // automatically; community extensions are enabled via
            // DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1 (see Dockerfile/compose).
            await conn.run('INSTALL duckpgq FROM community; LOAD duckpgq;');
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
    // DATA_INLINING_ROW_LIMIT 0 forces every write straight to Parquet instead
    // of DuckLake's inline (Postgres-resident) row storage. Inline data has a
    // read bug in our pinned DuckLake build: rows written to columns that were
    // added via ALTER TABLE ADD COLUMN (i.e. any schema-evolved column, such as
    // the arbitrary blob metadata columns) read back as NULL even though the
    // values are physically stored. Writing directly to Parquet reconstructs
    // evolved columns correctly on read. (This inline-read bug is distinct from
    // the CHECKPOINT/compaction crashes discussed in compactDuckLake() below and
    // package.json's "_comments" entry; it has not been re-verified against the
    // currently pinned DuckDB/DuckLake version.)
    await conn.run(`ATTACH 'ducklake:' AS ducklake (DATA_PATH '${escapedDataPath}', AUTOMATIC_MIGRATION true, DATA_INLINING_ROW_LIMIT 0);`);
    await conn.run("USE ducklake;");

    // Workaround for a DuckDB optimizer bug that corrupts inline DuckLake VARCHAR
    // columns under blocking operators. See DISABLED_OPTIMIZERS above for the full
    // rationale; applied instance-wide so every connection inherits it.
    await applyOptimizerWorkaround(conn, label);

    if (label === 'user') {
        // This instance runs user-supplied SQL. Lock it down once attached:
        // 1. Drop the DuckLake secret; it holds the Postgres connection string
        //    (with password) and is not needed after ATTACH.
        // 2. Restrict file access to the lake's data directory and disable all
        //    other external access (no read_text('/etc/...'), no reading the
        //    BLOB directory, no new network connections). This cannot be
        //    re-enabled while the instance runs.
        // The connection string remains visible to DuckDB introspection
        // functions, which src/sql-guard.js rejects.
        await conn.run('DROP SECRET __default_ducklake;');
        // The extension directory (binaries only) stays readable so LOAD and
        // duckdb_extensions() keep working on new connections.
        const extResult = await conn.run(`SELECT current_setting('extension_directory')`);
        const extSetting = String((await extResult.getRows())[0]?.[0] || '');
        const extensionDir = path.resolve(extSetting || path.join(os.homedir(), '.duckdb', 'extensions')) + path.sep;
        const allowed = [path.resolve(dataPath) + path.sep, extensionDir];
        const list = allowed.map(d => `'${d.replace(/'/g, "''")}'`).join(', ');
        await conn.run(`SET GLOBAL allowed_directories = [${list}];`);
        await conn.run('SET GLOBAL enable_external_access = false;');
        console.log(`[DB:${label}] External access disabled (file access limited to ${allowed.join(', ')}).`);
    } else {
        const consolePath = path.join(dataPath, '_console') + path.sep;
        try { await fs.mkdir(consolePath, { recursive: true }); } catch (e) { /* attach reports problems */ }
        await conn.run(`ATTACH 'ducklake:' AS ${CONSOLE_CATALOG} (DATA_PATH '${consolePath.replace(/'/g, "''")}', METADATA_SCHEMA '${CONSOLE_CATALOG}', AUTOMATIC_MIGRATION true, DATA_INLINING_ROW_LIMIT 1000000);`);
    }

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
            invalidatePrimaryInstance();
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

// Tear down the current primary instance and every connection pooled against
// it. Bumping the generation ensures any in-flight connection released after
// this point is discarded rather than re-pooled onto the dead instance.
function invalidatePrimaryInstance() {
    instanceGeneration++;
    instance = null;
    initPromise = null;
    while (primaryPool.length > 0) {
        const stale = primaryPool.pop();
        if (stale && typeof stale.close === 'function') {
            stale.close().catch(() => {});
        }
    }
}

/**
 * Creates a fresh connection to the singleton instance.
 * Using per-request connections is safer for high-concurrency than a singleton connection.
 */
export async function getConnection() {
    const db = await getDbInstance();

    // Reuse a warmed connection from the pool if one is available, but only if
    // it belongs to the current instance generation. A pooled connection from a
    // prior (crashed) instance is dead; drain and close any such stragglers so
    // we never hand a poisoned connection back to a caller.
    while (primaryPool.length > 0) {
        const pooled = primaryPool.pop();
        if (pooled && pooled._generation === instanceGeneration) {
            await enableDuckPgqGrammar(pooled);
            return pooled;
        }
        if (pooled && typeof pooled.close === 'function') {
            pooled.close().catch(() => {});
        }
    }

    const conn = await db.connect();
    // Tag the connection with the generation it was born under so
    // releaseConnection() and future getConnection() calls can detect and
    // discard it after an instance-invalidating fatal error.
    conn._generation = instanceGeneration;
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
    // Skipped entirely when DISABLE_DUCKPGQ=1 so we never trigger a native
    // crash from a broken duckpgq binary on a per-request basis.
    await loadDuckPgq(conn);
    // NOTE: SET parquet_row_group_size is not recognised by some DuckDB
    // versions and would spam the logs with harmless warnings. Row-group
    // sizing for Parquet exports is controlled at COPY-time instead.
    // (The previous SET call was removed after producing the following log
    // noise: "Catalog Error: unrecognized configuration parameter ...".)
    return conn;
}

/**
 * Open an unpooled connection on the primary instance, set up like a pooled
 * one. For long-running work (table exports) that should neither occupy a
 * pooled connection nor be handed back to the pool afterwards; the caller
 * must close() it.
 */
export async function openDedicatedConnection() {
    const db = await getDbInstance();
    const conn = await db.connect();
    await conn.run("USE ducklake;");
    await conn.run("SET search_path = 'ducklake.main,memory.main';");
    return conn;
}

/**
 * Return a primary connection to the pool instead of closing it.
 * When the pool is full the connection is actually closed.
 */
export function releaseConnection(conn) {
    if (!conn) return;
    // Never re-pool a connection from a superseded instance generation: the
    // instance it belongs to was invalidated by a fatal error and the
    // connection is dead. Close it instead.
    const stale = conn._generation !== instanceGeneration;
    if (!stale && primaryPool.length < PRIMARY_POOL_SIZE) {
        primaryPool.push(conn);
    } else {
        if (typeof conn.close === 'function') {
            conn.close().catch(() => {});
        }
    }
}

// Convert one DuckDB value for a JSON response: BLOBs become base64, every
// other value (BigInt, DuckDB timestamp/list/struct values) is left for
// JSON.stringify. Shared by the materialising and the streaming readers so both
// serialise identically.
function convertValue(val) {
    if (val === null || val === undefined) return null;
    if (typeof val === 'object' && val.constructor.name === 'DuckDBBlobValue' && val.bytes) {
        return Buffer.from(val.bytes).toString('base64');
    }
    if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
        return Buffer.from(val).toString('base64');
    }
    return val;
}

// Convert a DuckDB result object into an array of plain JS objects.
// Shared between execute() and executeWithParams().
async function _readRows(result) {
    const columns = result.columnNames();
    const rowSet = await result.getRows();
    const results = [];
    if (!rowSet) return results;

    if (typeof rowSet[Symbol.asyncIterator] === 'function' || typeof rowSet[Symbol.iterator] === 'function') {
        for (const row of rowSet) {
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
    await loadDuckPgq(conn);
    // (Same note as in getConnection: parquet_row_group_size is not a
    // session-level parameter in current DuckDB versions.)
    return conn;
}

// Read on use so the admin console can change them live.
const userQueryConcurrency = () => Math.max(1, parseInt(process.env.USER_QUERY_CONCURRENCY || '4', 10) || 4);
const userQueryTimeoutMs = () => Math.max(1000, parseInt(process.env.USER_QUERY_TIMEOUT_MS || '30000', 10) || 30000);

let userActive = 0;
const userQueue = [];

function acquireUserSlot() {
    if (userActive < userQueryConcurrency()) {
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

// Outcome counters for GET /metrics (per process, since start).
const userQueryCounters = { succeeded: 0, failed: 0, timedOut: 0, cancelled: 0 };

/** Snapshot of the user-query gate for GET /metrics. */
export function userQueryStats() {
    return {
        active: userActive,
        queued: userQueue.length,
        concurrency: userQueryConcurrency(),
        ...userQueryCounters,
    };
}

/**
 * Shared harness for SQL on the isolated user instance: takes a concurrency
 * slot, opens a fresh connection, runs the optional `prepare` hook, arms the
 * engine-level and wall-clock deadlines, then hands the connection to `fn`.
 * When the deadline passes the caller receives a 504-tagged error and the
 * DuckDB query is interrupted on a best-effort basis (DuckDB does not
 * guarantee that every operator honours interruption immediately). `signal`
 * (an AbortSignal) interrupts the query too, e.g. when the HTTP client
 * disconnects mid-stream.
 */
async function withUserConnection({ timeoutMs, prepare, signal }, fn) {
    await acquireUserSlot();
    let conn;
    let timer;
    let timedOut = false;
    let onAbort;
    try {
        conn = await getUserConnection();

        // Optional per-connection setup (e.g. lazily materializing a property
        // graph from the durable registry) that must run on this exact
        // connection before the user SQL — and before the deadline below.
        if (typeof prepare === 'function') {
            await prepare(conn);
        }

        // Engine-level deadline (DuckDB ≥ recent versions). If the SET fails
        // we silently fall back to the wall-clock race below.
        try { await conn.run(`SET max_execution_time = ${Number(timeoutMs)}`); } catch (e) { /* unsupported */ }

        // Re-assert the SQL/PGQ grammar immediately before the user SQL (after
        // any prepare hook), since we cannot tell whether it uses GRAPH_TABLE.
        await enableDuckPgqGrammar(conn);

        const interrupt = () => {
            if (conn && typeof conn.interrupt === 'function') {
                try { conn.interrupt(); } catch (e) { /* ignore */ }
            }
        };

        const timeoutPromise = new Promise((_, reject) => {
            timer = setTimeout(() => {
                timedOut = true;
                interrupt();
                const err = new Error(`Query exceeded ${timeoutMs}ms timeout`);
                err.statusCode = 504;
                reject(err);
            }, timeoutMs);
        });

        const racers = [fn(conn, () => timedOut || !!signal?.aborted), timeoutPromise];
        if (signal) {
            racers.push(new Promise((_, reject) => {
                onAbort = () => {
                    interrupt();
                    reject(Object.assign(new Error('Query cancelled: client disconnected'), { statusCode: 499, cancelled: true }));
                };
                if (signal.aborted) onAbort();
                else signal.addEventListener('abort', onAbort, { once: true });
            }));
        }

        const result = await Promise.race(racers);
        userQueryCounters.succeeded++;
        return result;
    } catch (err) {
        if (timedOut && !err.statusCode) err.statusCode = 504;
        if (timedOut) userQueryCounters.timedOut++;
        else if (err.cancelled) userQueryCounters.cancelled++;
        else userQueryCounters.failed++;
        throw err;
    } finally {
        if (timer) clearTimeout(timer);
        if (signal && onAbort) signal.removeEventListener('abort', onAbort);
        if (conn && typeof conn.close === 'function') {
            try { await conn.close(); } catch (e) { }
        }
        releaseUserSlot();
    }
}

/**
 * Runs read-only user-supplied SQL on the isolated user instance, with a
 * concurrency cap and a wall-clock timeout, and returns every row as a plain
 * object. Used by the GraphQL `sql` resolver and the graph query endpoint;
 * POST /query uses streamUserSql() so its memory use is bounded by a row cap.
 */
export async function executeUserSql(sql, { timeoutMs = userQueryTimeoutMs(), prepare = null } = {}) {
    return withUserConnection({ timeoutMs, prepare }, async (conn) => {
        const result = await conn.run(sql);
        return await _readRows(result);
    });
}

/**
 * Streams read-only user-supplied SQL from the isolated user instance in
 * DuckDB data chunks (~2048 rows each) instead of materialising the result,
 * under the same concurrency gate and deadline as executeUserSql().
 *
 *   onColumns(names)  called once before the first rows
 *   onRows(rows)      called per chunk with plain objects (values converted
 *                     exactly as executeUserSql() converts them); awaited, so
 *                     the caller can apply backpressure (e.g. wait for the
 *                     HTTP socket to drain) before the next chunk is fetched
 *   maxRows           stop after this many rows; `truncated` reports whether
 *                     more rows were available
 *   signal            AbortSignal that interrupts the query
 *
 * Memory use is bounded by one chunk plus whatever the caller buffers. Note
 * that blocking operators (ORDER BY, GROUP BY, ...) still need DuckDB's own
 * working memory before the first chunk is produced.
 *
 * Resolves to { columns, rowCount, truncated }.
 */
export async function streamUserSql(sql, { maxRows = Infinity, onColumns = null, onRows, timeoutMs = userQueryTimeoutMs(), signal = null } = {}) {
    return withUserConnection({ timeoutMs, signal }, async (conn, isCancelled) => {
        const result = await conn.stream(sql);
        const columns = result.columnNames();
        if (onColumns) await onColumns(columns);
        let rowCount = 0;
        let truncated = false;
        while (!isCancelled()) {
            const chunk = await result.fetchChunk();
            if (!chunk || chunk.rowCount === 0) break;
            if (rowCount >= maxRows) { truncated = true; break; }
            let rows = chunk.getRows();
            if (rowCount + rows.length > maxRows) {
                rows = rows.slice(0, maxRows - rowCount);
                truncated = true;
            }
            const objects = rows.map(row => {
                const obj = {};
                columns.forEach((col, idx) => { obj[col] = convertValue(row[idx]); });
                return obj;
            });
            rowCount += objects.length;
            await onRows(objects);
            if (truncated) break;
        }
        return { columns, rowCount, truncated };
    });
}

/**
 * Run DuckLake maintenance (flush inlined data, snapshot expiry, file
 * compaction, orphaned-file cleanup) on the primary instance. Safe to call
 * repeatedly.
 *
 * Invoked at startup so that gateways that have been ingesting one row
 * per request for a long time don't accumulate millions of tiny Parquet
 * files. Logs and swallows errors so a maintenance failure does not
 * prevent the gateway from coming up.
 *
 * This deliberately does NOT run a single `CHECKPOINT ducklake`. CHECKPOINT
 * is sugar for the same six calls below run back-to-back on one connection,
 * which is exactly what triggers duckdb/ducklake#1108: a schema-cache key
 * collision after DuckLake's automatic v0.3->v0.4 ducklake_schema_versions
 * migration, provoked by per-table schema evolution (we do ALTER TABLE ADD
 * COLUMN for blob metadata — see the DATA_INLINING_ROW_LIMIT comment above).
 * Per the upstream-confirmed workaround, ducklake_merge_adjacent_files and
 * ducklake_rewrite_data_files run on a fresh, unpooled connection per table so
 * no single connection's schema cache ever holds more than one table's entry.
 * See package.json's "_comments"."@duckdb/node-api" entry for the full history
 * (including the now-fixed #525 crash this replaced) and what to re-check
 * before changing this again.
 */
export async function compactDuckLake() {
    if (process.env.DISABLE_DUCKLAKE_COMPACTION === '1') {
        console.warn('[Compact] Skipped: DISABLE_DUCKLAKE_COMPACTION=1.');
        return;
    }
    const start = Date.now();
    console.log('[Compact] Running DuckLake maintenance...');
    let tableCount = 0;
    try {
        await withConnection(async (conn) => {
            await conn.run("CALL ducklake_flush_inlined_data('ducklake')");
            await expireSnapshots(conn);
        });

        const tables = await execute(
            "SELECT table_name FROM duckdb_tables() WHERE database_name = 'ducklake'"
        );
        tableCount = tables.length;

        for (const { table_name } of tables) {
            // Deliberately bypasses getConnection()/the pool: a genuinely fresh
            // DuckDBInstance connection is what keeps this table's schema-cache
            // entry isolated (see function doc above).
            const inst = await getDbInstance();
            const conn = await inst.connect();
            try {
                await conn.run('USE ducklake;');
                await conn.run("CALL ducklake_merge_adjacent_files('ducklake', ?)", [table_name], [VARCHAR]);
                await conn.run("CALL ducklake_rewrite_data_files('ducklake', ?)", [table_name], [VARCHAR]);
            } catch (err) {
                console.warn(`[Compact] Maintenance failed for table "${table_name}":`, err.message || err);
            } finally {
                try { await conn.close(); } catch (e) { }
            }
        }

        await withConnection(async (conn) => {
            await cleanupOldFiles(conn);
            await conn.run("CALL ducklake_delete_orphaned_files('ducklake')");
        });

        console.log(`[Compact] DuckLake maintenance done in ${Date.now() - start}ms (${tableCount} tables)`);
    } catch (err) {
        console.warn('[Compact] DuckLake maintenance failed:', err.message || err);
    }
}

/**
 * Days of DuckLake snapshot history to keep (SNAPSHOT_RETENTION_DAYS, editable
 * in the admin console). Unset means snapshots are never expired: time travel
 * is unlimited, but rows deleted by pruning keep occupying storage because old
 * snapshots still reference their files. Calling ducklake_expire_snapshots()
 * without older_than is a no-op unless the catalog has an expiry option set.
 */
export function snapshotRetentionDays() {
    const raw = String(process.env.SNAPSHOT_RETENTION_DAYS ?? '').trim();
    if (!raw) return null;
    const n = parseInt(raw, 10);
    return Number.isInteger(n) && n >= 0 ? n : null;
}

// Files no snapshot references are only deleted after this grace period, so a
// query that started before a rewrite/expiry can finish reading them.
const FILE_CLEANUP_GRACE_MINUTES = 5;

async function cleanupOldFiles(conn) {
    // With no arguments ducklake_cleanup_old_files() keeps recently scheduled
    // files much longer, so replaced files would linger on disk.
    await conn.run(`CALL ducklake_cleanup_old_files('ducklake', older_than => now() - INTERVAL ${FILE_CLEANUP_GRACE_MINUTES} MINUTE)`);
}

async function expireSnapshots(conn) {
    const days = snapshotRetentionDays();
    if (days === null) return false;
    await conn.run(`CALL ducklake_expire_snapshots('ducklake', older_than => now() - INTERVAL ${days} DAY)`);
    return true;
}

/**
 * Reclaim storage after rows were deleted from `tables`: rewrite their data
 * files without the deleted rows, expire snapshots past the retention window
 * (if one is set) and remove files no snapshot references any more. Each
 * rewrite uses a fresh connection, as in compactDuckLake() (ducklake#1108).
 * Returns a short human-readable summary.
 */
export async function reclaimTableStorage(tables) {
    let rewritten = 0;
    for (const tableName of tables) {
        const inst = await getDbInstance();
        const conn = await inst.connect();
        try {
            await conn.run('USE ducklake;');
            await conn.run("CALL ducklake_rewrite_data_files('ducklake', ?)", [tableName], [VARCHAR]);
            rewritten++;
        } finally {
            try { await conn.close(); } catch (e) { }
        }
    }
    let expired = false;
    await withConnection(async (conn) => {
        expired = await expireSnapshots(conn);
        await cleanupOldFiles(conn);
    });
    // Files replaced just now are still inside the grace period; delete them
    // once it has passed (startup maintenance also catches any left over).
    const deferred = setTimeout(() => {
        withConnection(cleanupOldFiles).catch(err => console.warn('[Prune] Deferred file cleanup failed:', err.message || err));
    }, (FILE_CLEANUP_GRACE_MINUTES + 1) * 60 * 1000);
    deferred.unref?.();
    const days = snapshotRetentionDays();
    return `rewrote data files for ${rewritten} table(s); ` + (expired
        ? `expired snapshots older than ${days} day(s); replaced files are deleted after a ${FILE_CLEANUP_GRACE_MINUTES}-minute grace period`
        : 'snapshot retention is not set, so files still referenced by older snapshots are kept');
}

// --- Durable property-graph registry ---
// DuckPGQ property graphs live in a DuckDB instance's in-memory catalog and
// therefore vanish on restart and are invisible to the isolated user-query
// instance. To make graphs a durable, cross-instance feature we persist their
// JSON definitions in a DuckLake table and lazily re-materialize the
// `CREATE PROPERTY GRAPH` statement on whichever instance needs it.

/**
 * Create the registry table if it does not yet exist. Persisted in DuckLake so
 * definitions survive restarts and are shared by every gateway instance.
 */
export async function initGraphRegistry() {
    await execute(`
        CREATE TABLE IF NOT EXISTS _graph_registry (
            name VARCHAR,
            definition VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR
        )
    `);
}

/**
 * Persist (upsert) a graph definition. DuckLake has no ON CONFLICT support, so
 * we delete-then-insert. `def` is the raw JSON body accepted by
 * buildCreatePropertyGraphSql().
 */
export async function saveGraphDefinition(name, def, createdBy = null) {
    const json = JSON.stringify(def);
    await executeWithParams(`DELETE FROM _graph_registry WHERE name = ?`, [name], [VARCHAR]);
    await executeWithParams(
        `INSERT INTO _graph_registry (name, definition, created_by) VALUES (?, ?, ?)`,
        [name, json, createdBy],
        [VARCHAR, VARCHAR, VARCHAR]
    );
}

/**
 * Return the parsed JSON definition for a graph, or null when it is not
 * registered.
 */
export async function loadGraphDefinition(name) {
    const rows = await executeWithParams(
        `SELECT definition FROM _graph_registry WHERE name = ?`,
        [name],
        [VARCHAR]
    );
    if (rows.length === 0) return null;
    try {
        return JSON.parse(rows[0].definition);
    } catch (_) {
        return null;
    }
}

/**
 * List every registered graph with its parsed definition and metadata.
 */
export async function listGraphDefinitions() {
    const rows = await execute(
        `SELECT name, definition, created_at, created_by FROM _graph_registry ORDER BY name`
    );
    return rows.map((r) => {
        let definition = null;
        try { definition = JSON.parse(r.definition); } catch (_) { /* leave null */ }
        return { name: r.name, created_at: r.created_at, created_by: r.created_by, definition };
    });
}

/**
 * Remove a graph definition from the registry. Materialized in-memory catalog
 * entries are dropped separately by the caller.
 */
export async function deleteGraphDefinition(name) {
    await executeWithParams(`DELETE FROM _graph_registry WHERE name = ?`, [name], [VARCHAR]);
}

/**
 * Ensure the named property graph exists in the catalog of `conn`'s instance,
 * recreating it from the durable registry when missing. Idempotent: DuckPGQ has
 * no `CREATE PROPERTY GRAPH IF NOT EXISTS`, so an "already exists" error is
 * treated as success. Throws a 404-tagged error when the graph is unregistered.
 */
export async function ensureGraphMaterialized(conn, name) {
    const def = await loadGraphDefinition(name);
    if (!def) {
        const err = new Error(`Property graph '${name}' is not registered.`);
        err.statusCode = 404;
        throw err;
    }
    const sql = buildCreatePropertyGraphSql(def);
    try {
        await conn.run(sql);
    } catch (err) {
        const msg = err.message || String(err);
        if (/already exists/i.test(msg)) return def;
        throw err;
    }
    return def;
}
