import { DuckDBInstance, VARCHAR, BIGINT, BOOLEAN, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE } from '@duckdb/node-api';
import fs from 'fs/promises';
import path from 'path';
import os from 'os';
import { buildCreatePropertyGraphSql } from './graphs.js';
import { storageConfig, describeStorage, buildS3SecretSql, joinDataPath, withTrailingSlash, dataInliningRowLimit } from './storage.js';
// Applies storage settings saved in the console to process.env on import, so
// it must stay ahead of anything that reads them.
import { pendingSwitch, confirmStorageConfig, noteAttachFailure } from './storage-config.js';

export { VARCHAR, BIGINT, BOOLEAN, BLOB, blobValue, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE };

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
const CONSOLE_INLINING_ROW_LIMIT = 1000000;

/** The DuckLake secret with the Postgres catalog connection (DUCKLAKE_PG_*). */
function ducklakeSecretSql() {
    const pgHost = process.env.DUCKLAKE_PG_HOST || '0.0.0.0';
    const pgPort = process.env.DUCKLAKE_PG_PORT || '5432';
    const pgUser = process.env.DUCKLAKE_PG_USER || 'postgres';
    const pgPass = process.env.DUCKLAKE_PG_PASSWORD || 'postgres';
    const pgDb = process.env.DUCKLAKE_PG_DB || 'ducklake';
    const metadataPath = `postgres:host=${pgHost} port=${pgPort} user=${pgUser} password=${pgPass} dbname=${pgDb}`;
    return `CREATE SECRET (TYPE ducklake, METADATA_PATH '${metadataPath.replace(/'/g, "''")}');`;
}

const sqlLit = (v) => `'${String(v).replace(/'/g, "''")}'`;
const sqlIdent = (v) => `"${String(v).replace(/"/g, '""')}"`;

/**
 * Move the lake to a new data location, once, before the primary instance
 * attaches (console Storage page: pending settings with a changed data path).
 *
 * DuckLake records the data path in the catalog and stores every file path
 * relative to it, so a lake can only move when no file has to: the console
 * allows a switch only while no user table has data files, and this re-checks
 * it against the OLD location before changing anything. User tables whose rows
 * are all inlined in the catalog move as they are. The gateway's own system
 * tables (and console tables) that do have files are copied into memory,
 * dropped and recreated from their DDL at the new location (DROP never reads
 * files; DELETE would). Then the catalog's recorded data path is updated, old
 * snapshots are expired and the deletion queue (paths at the old location) is
 * cleared, so nothing refers to the old location any more. Its files stay
 * where they are for the operator to remove.
 *
 * Throws (with `storageSwitchRefused` when the lake is not empty); the caller
 * reverts to the previous settings.
 */
async function runPendingStorageSwitch(sw) {
    const from = storageConfig({ ...process.env, ...sw.settings });
    const to = storageConfig(process.env);
    if (from.dataPath === to.dataPath) return null;
    // metaSchemas: where the catalog's own tables live in the metadata
    // database (hidden from duckdb_tables(), but addressable): "public" in
    // Postgres for the default schema, "main" for a DuckDB-file catalog.
    const catalogs = [
        { name: 'ducklake', from: from.dataPath, to: to.dataPath, metaSchemas: ['public', 'main'], opts: `DATA_INLINING_ROW_LIMIT ${dataInliningRowLimit()}` },
        { name: CONSOLE_CATALOG, from: joinDataPath(from.dataPath, '_console'), to: joinDataPath(to.dataPath, '_console'), metaSchemas: [CONSOLE_CATALOG], opts: `METADATA_SCHEMA '${CONSOLE_CATALOG}', DATA_INLINING_ROW_LIMIT ${CONSOLE_INLINING_ROW_LIMIT}` },
    ];
    console.log(`[Storage] Moving the lake from ${describeStorage(from)} to ${describeStorage(to)}...`);
    const inst = await DuckDBInstance.create(':memory:', { allow_unsigned_extensions: 'true' });
    const conn = await inst.connect();
    const rowsOf = async (sql) => (await conn.runAndReadAll(sql)).getRowObjectsJS();
    try {
        await conn.run('INSTALL ducklake; LOAD ducklake; INSTALL postgres; LOAD postgres;');
        if (from.kind === 's3' || to.kind === 's3') await conn.run('INSTALL httpfs; LOAD httpfs;');
        if (from.provider === 'credential_chain' || to.provider === 'credential_chain') await conn.run('INSTALL aws; LOAD aws;');
        // Each secret is scoped to its own data path, so both can coexist.
        if (from.kind === 's3') await conn.run(buildS3SecretSql(from, { name: 'ichibi_s3_from' }));
        if (to.kind === 's3') await conn.run(buildS3SecretSql(to, { name: 'ichibi_s3_to' }));
        await conn.run(ducklakeSecretSql());
        if (to.kind === 'local') for (const c of catalogs) await fs.mkdir(c.to, { recursive: true });

        // 1. Inventory at the old location.
        for (const c of catalogs) await conn.run(`ATTACH 'ducklake:' AS ${c.name} (DATA_PATH ${sqlLit(c.from)}, AUTOMATIC_MIGRATION true, ${c.opts})`);
        const blocking = [];
        const move = [];
        for (const c of catalogs) {
            const tables = await rowsOf(`SELECT schema_name, table_name, sql FROM duckdb_tables() WHERE database_name = ${sqlLit(c.name)}`);
            for (const t of tables) {
                const [f] = await rowsOf(`SELECT count(*)::BIGINT AS files, count(delete_file)::BIGINT AS deletes FROM ducklake_list_files(${sqlLit(c.name)}, ${sqlLit(t.table_name)}, schema => ${sqlLit(t.schema_name)})`);
                if (Number(f.files) + Number(f.deletes) === 0) continue;
                const system = c.name === CONSOLE_CATALOG || t.table_name.startsWith('_');
                if (!system) blocking.push(`${t.table_name} (${f.files} file(s))`);
                else move.push({ catalog: c.name, schema: t.schema_name, table: t.table_name, sql: t.sql, stash: `stash_${move.length}` });
            }
        }
        if (blocking.length) {
            const listed = blocking.slice(0, 10).join(', ') + (blocking.length > 10 ? ` and ${blocking.length - 10} more` : '');
            throw Object.assign(new Error(`The lake is not empty, so it cannot move to ${to.dataPath}: ${blocking.length} table(s) have data files (${listed}). ` +
                'Export and drop these tables, switch, then import them again.'), { storageSwitchRefused: true });
        }
        for (const m of move) {
            await conn.run(`CREATE TABLE memory.main.${m.stash} AS SELECT * FROM ${m.catalog}.${sqlIdent(m.schema)}.${sqlIdent(m.table)}`);
        }
        for (const c of catalogs) await conn.run(`DETACH ${c.name}`);

        // 2. Re-attach at the new location and record it in the catalog.
        for (const c of catalogs) {
            await conn.run(`ATTACH 'ducklake:' AS ${c.name} (DATA_PATH ${sqlLit(c.to)}, OVERRIDE_DATA_PATH true, AUTOMATIC_MIGRATION true, ${c.opts})`);
            for (const schema of c.metaSchemas) {
                const candidate = `${sqlIdent(`__ducklake_metadata_${c.name}`)}.${sqlIdent(schema)}`;
                try {
                    await conn.run(`SELECT value FROM ${candidate}.ducklake_metadata WHERE key = 'data_path'`);
                    c.meta = candidate;
                    break;
                } catch (_) { /* try the next schema */ }
            }
            if (!c.meta) throw new Error(`Cannot find the DuckLake metadata tables of ${c.name}`);
            // DuckLake records the path with a trailing separator.
            await conn.run(`UPDATE ${c.meta}.ducklake_metadata SET value = ${sqlLit(withTrailingSlash(c.to))} WHERE key = 'data_path'`);
        }

        // 3. Rewrite system tables that had files, each in its own transaction.
        for (const m of move) {
            await conn.run(`USE ${m.catalog}.${sqlIdent(m.schema)}`);
            await conn.run('BEGIN TRANSACTION');
            try {
                await conn.run(`DROP TABLE ${sqlIdent(m.table)}`);
                await conn.run(m.sql);
                await conn.run(`INSERT INTO ${sqlIdent(m.table)} SELECT * FROM memory.main.${m.stash}`);
                await conn.run('COMMIT');
            } catch (err) {
                await conn.run('ROLLBACK').catch(() => {});
                throw err;
            }
        }
        await conn.run('USE memory');

        // 4. Forget the old location: expire older snapshots (they reference
        // files there) and clear the deletion queue (paths there).
        for (const c of catalogs) {
            await conn.run(`CALL ducklake_expire_snapshots(${sqlLit(c.name)}, older_than => now())`);
            await conn.run(`DELETE FROM ${c.meta}.ducklake_files_scheduled_for_deletion`);
        }
        for (const c of catalogs) await conn.run(`DETACH ${c.name}`);
        const summary = { from: from.dataPath, to: to.dataPath, at: new Date().toISOString(), movedTables: move.map(m => `${m.catalog}.${m.table}`) };
        console.log(`[Storage] Lake moved to ${describeStorage(to)}` + (move.length ? `; rewrote ${move.map(m => m.table).join(', ')}` : '') +
            `. Files at the old location (${from.dataPath}) are no longer used and can be removed.`);
        return summary;
    } finally {
        try { conn.closeSync(); } catch (_) { /* ignore */ }
    }
}

// Configure a fresh in-memory DuckDB instance to act as a DuckLake gateway.
// Used for both the primary instance (writes + reads) and the isolated user-
// query instance (read-only ad-hoc SQL) so they share the same setup path.
async function _attachDuckLake(label) {
    const pgHost = process.env.DUCKLAKE_PG_HOST || '0.0.0.0';
    const pgPort = process.env.DUCKLAKE_PG_PORT || '5432';
    const pgDb = process.env.DUCKLAKE_PG_DB || 'ducklake';
    // Local directory or s3:// URL (see src/storage.js).
    const storage = storageConfig();
    const dataPath = storage.dataPath;
    const remote = storage.kind !== 'local';

    const inst = await DuckDBInstance.create(':memory:', { allow_unsigned_extensions: 'true' });
    const conn = await inst.connect();

    console.log(`[DB:${label}] Initializing DuckLake connection to ${pgHost}:${pgPort} (data: ${describeStorage(storage)})...`);

    if (!remote) {
        try {
            await fs.mkdir(dataPath, { recursive: true });
        } catch (mkErr) {
            console.warn(`[DB:${label}] Could not create data path ${dataPath}:`, mkErr.message || mkErr);
        }
    }

    await conn.run('INSTALL ducklake; LOAD ducklake;');
    await conn.run('INSTALL postgres; LOAD postgres;');
    if (remote) {
        // httpfs provides the s3:// file system; aws provides the credential
        // chain provider (env vars, ~/.aws, instance/task roles).
        await conn.run('INSTALL httpfs; LOAD httpfs;');
        if (storage.provider === 'credential_chain') await conn.run('INSTALL aws; LOAD aws;');
        // Temporary secret scoped to the data path. Its values are redacted in
        // duckdb_secrets(), and once external access is disabled on the
        // user-query instance (below) DuckDB refuses to display them at all.
        await conn.run(buildS3SecretSql(storage));
    }

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

    await conn.run(ducklakeSecretSql());

    const escapedDataPath = dataPath.replace(/'/g, "''");
    // Small writes are inlined: an INSERT of fewer than DATA_INLINING_ROW_LIMIT
    // rows is stored in the metadata catalog (Postgres) instead of creating a
    // Parquet file, and scheduled maintenance (src/console/maintenance.js)
    // flushes the inlined rows to Parquet in bulk. This is what keeps one-row-
    // per-request ingestion from producing one tiny file per request.
    //
    // History: this used to be forced to 0 because, on an older DuckLake build,
    // inlined rows in columns added by ALTER TABLE ADD COLUMN (the blob
    // metadata columns) read back as NULL. Re-verified fixed on DuckDB 2.0.0 /
    // DuckLake 1.1 (inline reads, reads on fresh connections, the user-query
    // instance and after ducklake_flush_inlined_data all return the values; the
    // blob metadata tests in test/test-blob.js and test/api.test.js pass with
    // inlining on). Set DATA_INLINING_ROW_LIMIT=0 to restore the old behaviour.
    const inliningLimit = dataInliningRowLimit();
    await conn.run(`ATTACH 'ducklake:' AS ducklake (DATA_PATH '${escapedDataPath}', AUTOMATIC_MIGRATION true, DATA_INLINING_ROW_LIMIT ${inliningLimit});`);
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
        // Object storage: the S3 secret stays (reads need it; its values are
        // never displayed, see above) and the bucket prefix is the only
        // remote location the instance may read.
        const allowed = [remote ? dataPath : path.resolve(dataPath) + path.sep, extensionDir];
        const list = allowed.map(d => `'${d.replace(/'/g, "''")}'`).join(', ');
        await conn.run(`SET GLOBAL allowed_directories = [${list}];`);
        await conn.run('SET GLOBAL enable_external_access = false;');
        console.log(`[DB:${label}] External access disabled (file access limited to ${allowed.join(', ')}).`);
    } else {
        const consolePath = joinDataPath(dataPath, '_console');
        if (!remote) {
            try { await fs.mkdir(consolePath, { recursive: true }); } catch (e) { /* attach reports problems */ }
        }
        await conn.run(`ATTACH 'ducklake:' AS ${CONSOLE_CATALOG} (DATA_PATH '${consolePath.replace(/'/g, "''")}', METADATA_SCHEMA '${CONSOLE_CATALOG}', AUTOMATIC_MIGRATION true, DATA_INLINING_ROW_LIMIT ${CONSOLE_INLINING_ROW_LIMIT});`);
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
            const sw = pendingSwitch();
            const lastSwitch = sw ? await runPendingStorageSwitch(sw) : null;
            instance = await _attachDuckLake('primary');
            confirmStorageConfig(lastSwitch ? { lastSwitch } : {});
            return instance;
        } catch (err) {
            initPromise = null;
            instance = null;
            console.error('[DB] Failed to connect to DuckLake instance:', err);
            // New storage settings from the console that do not work: restore
            // the previous ones and restart on them (Docker restart policy).
            if (noteAttachFailure(err, { fatal: !!err.storageSwitchRefused })) {
                console.error('[Storage] Exiting to restart on the previous storage settings.');
                setTimeout(() => process.exit(1), 200);
            }
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
 * Parse `sql` with DuckDB's own parser and return the statement tree as JSON
 * (json_serialize_sql). Runs on the primary instance: parsing executes
 * nothing, and the user-query instance stays reserved for running queries.
 * Used by the table-aware authorization of user-supplied SQL
 * (src/sql-tables.js).
 */
export async function serializeSql(sql) {
    const rows = await executeWithParams('SELECT json_serialize_sql(?) AS ast', [String(sql)], [VARCHAR]);
    const ast = rows[0]?.ast;
    return typeof ast === 'string' ? JSON.parse(ast) : ast;
}

/**
 * Look up a table in the lake's main schema, case-insensitively (as DuckDB
 * resolves identifiers). Returns { name, type } with the catalog spelling of
 * the name and its table_type ('BASE TABLE' or 'VIEW'), or null.
 */
export async function findLakeTable(name) {
    const rows = await executeWithParams(
        `SELECT table_name, table_type FROM information_schema.tables
         WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND lower(table_name) = lower(?)`,
        [String(name)], [VARCHAR]);
    return rows.length ? { name: rows[0].table_name, type: rows[0].table_type } : null;
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
 * Run DuckLake maintenance on the primary instance and report what it did:
 *   flush    write inlined (Postgres-resident) rows out to Parquet
 *   merge    merge adjacent small Parquet files (ducklake_merge_adjacent_files)
 *   rewrite  rewrite files with many deleted rows (ducklake_rewrite_data_files)
 *   expire   expire snapshots older than SNAPSHOT_RETENTION_DAYS (if set)
 *   cleanup  delete files no snapshot references any more, and orphaned
 *            files, older than the FILE_CLEANUP_GRACE_MINUTES grace period
 * Steps can be switched off individually. Per-table failures are collected
 * rather than thrown so one bad table does not stop the rest.
 *
 * Called only by src/console/maintenance.js, which claims each run (scheduled,
 * startup or manual) so exactly one gateway instance performs it.
 *
 * This deliberately does NOT run a single `CHECKPOINT ducklake`. CHECKPOINT
 * is sugar for the same calls run back-to-back on one connection, which is
 * exactly what triggers duckdb/ducklake#1108: a schema-cache key collision
 * after DuckLake's automatic v0.3->v0.4 ducklake_schema_versions migration,
 * provoked by per-table schema evolution (we do ALTER TABLE ADD COLUMN for
 * blob metadata). Per the upstream-confirmed workaround,
 * ducklake_merge_adjacent_files and ducklake_rewrite_data_files run on a
 * fresh, unpooled connection per table so no single connection's schema cache
 * ever holds more than one table's entry. See package.json's
 * "_comments"."@duckdb/node-api" entry for the full history (including the
 * now-fixed #525 crash this replaced) and what to re-check before changing
 * this again.
 */
export async function runMaintenance({
    flush = true, merge = true, rewrite = true, expire = true, cleanup = true, deleteThreshold = null,
} = {}) {
    const start = Date.now();
    const stats = {
        tables: 0, filesBefore: 0, filesAfter: 0, bytesBefore: 0, bytesAfter: 0,
        deleteFilesBefore: 0, deleteFilesAfter: 0, rowsFlushed: 0,
        filesMerged: 0, filesMergedInto: 0, filesRewritten: 0, filesRewrittenInto: 0,
        snapshotsExpired: null, filesCleaned: 0, orphansDeleted: 0, failures: [], durationMs: 0,
    };
    const fail = (table, step, err) => stats.failures.push({ table, step, error: String(err?.message || err).split('\n')[0] });

    const tables = (await execute(
        "SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'ducklake' ORDER BY schema_name, table_name"
    ));
    stats.tables = tables.length;
    const before = await lakeFileStats(tables);
    Object.assign(stats, { filesBefore: before.files, bytesBefore: before.bytes, deleteFilesBefore: before.deleteFiles });

    if (flush) {
        try {
            const rows = await execute("CALL ducklake_flush_inlined_data('ducklake')");
            stats.rowsFlushed = rows.reduce((n, r) => n + Number(r.rows_flushed || 0), 0);
        } catch (err) { fail(null, 'flush', err); }
    }

    if (merge || rewrite) {
        for (const { schema_name: schema, table_name: table } of tables) {
            // Deliberately bypasses getConnection()/the pool: a genuinely fresh
            // DuckDBInstance connection is what keeps this table's schema-cache
            // entry isolated (see function doc above).
            const inst = await getDbInstance();
            const conn = await inst.connect();
            try {
                await conn.run('USE ducklake;');
                if (merge) {
                    try {
                        const r = await _readRows(await conn.run(
                            "CALL ducklake_merge_adjacent_files('ducklake', ?, schema => ?)", [table, schema], [VARCHAR, VARCHAR]));
                        for (const row of r) { stats.filesMerged += Number(row.files_processed || 0); stats.filesMergedInto += Number(row.files_created || 0); }
                    } catch (err) { fail(table, 'merge', err); }
                }
                if (rewrite) {
                    try {
                        const threshold = deleteThreshold === null || deleteThreshold === undefined ? '' : `, delete_threshold => ${Number(deleteThreshold)}`;
                        const r = await _readRows(await conn.run(
                            `CALL ducklake_rewrite_data_files('ducklake', ?, schema => ?${threshold})`, [table, schema], [VARCHAR, VARCHAR]));
                        for (const row of r) { stats.filesRewritten += Number(row.files_processed || 0); stats.filesRewrittenInto += Number(row.files_created || 0); }
                    } catch (err) { fail(table, 'rewrite', err); }
                }
            } finally {
                try { await conn.close(); } catch (e) { }
            }
        }
    }

    if (expire) {
        try {
            await withConnection(async (conn) => {
                const days = snapshotRetentionDays();
                if (days === null) return;
                const r = await _readRows(await conn.run(`CALL ducklake_expire_snapshots('ducklake', older_than => now() - INTERVAL ${days} DAY)`));
                stats.snapshotsExpired = r.length;
            });
        } catch (err) { fail(null, 'expire', err); }
    }

    if (cleanup) {
        try {
            await withConnection(async (conn) => {
                stats.filesCleaned = (await _readRows(await conn.run(cleanupOldFilesSql()))).length;
                // Orphans get the same grace period: another gateway instance
                // may be writing a file that is not committed yet.
                stats.orphansDeleted = (await _readRows(await conn.run(
                    `CALL ducklake_delete_orphaned_files('ducklake', older_than => now() - INTERVAL ${FILE_CLEANUP_GRACE_MINUTES} MINUTE)`))).length;
            });
        } catch (err) { fail(null, 'cleanup', err); }
    }

    const after = await lakeFileStats(tables);
    Object.assign(stats, { filesAfter: after.files, bytesAfter: after.bytes, deleteFilesAfter: after.deleteFiles });
    stats.durationMs = Date.now() - start;
    return stats;
}

/** Active data/delete file counts and bytes across `tables` (current snapshot). */
async function lakeFileStats(tables) {
    const out = { files: 0, bytes: 0, deleteFiles: 0 };
    if (!tables.length) return out;
    await withConnection(async (conn) => {
        for (const { schema_name: schema, table_name: table } of tables) {
            try {
                const [row] = await _readRows(await conn.run(
                    `SELECT count(*) AS files, coalesce(sum(data_file_size_bytes), 0) AS bytes, count(delete_file) AS deletes
                     FROM ducklake_list_files('ducklake', ?, schema => ?)`, [table, schema], [VARCHAR, VARCHAR]));
                out.files += Number(row?.files || 0);
                out.bytes += Number(row?.bytes || 0);
                out.deleteFiles += Number(row?.deletes || 0);
            } catch (e) { /* table dropped meanwhile */ }
        }
    });
    return out;
}

/** Current file counts for the console (no changes). */
export async function getLakeFileStats() {
    const tables = await execute("SELECT schema_name, table_name FROM duckdb_tables() WHERE database_name = 'ducklake'");
    return { tables: tables.length, ...(await lakeFileStats(tables)) };
}

/** One-line summary of runMaintenance() stats for logs, the run log and the audit log. */
export function describeMaintenance(s) {
    const mb = (b) => `${(b / 1048576).toFixed(1)} MB`;
    const parts = [`${s.tables} table(s); data files ${s.filesBefore} -> ${s.filesAfter} (${mb(s.bytesBefore)} -> ${mb(s.bytesAfter)})`];
    if (s.rowsFlushed) parts.push(`flushed ${s.rowsFlushed} inlined row(s)`);
    if (s.filesMerged) parts.push(`merged ${s.filesMerged} file(s) into ${s.filesMergedInto}`);
    if (s.filesRewritten) parts.push(`rewrote ${s.filesRewritten} file(s) with deletes into ${s.filesRewrittenInto}`);
    if (s.snapshotsExpired) parts.push(`expired ${s.snapshotsExpired} snapshot(s)`);
    if (s.filesCleaned || s.orphansDeleted) parts.push(`deleted ${s.filesCleaned} old and ${s.orphansDeleted} orphaned file(s)`);
    if (s.failures.length) parts.push(`${s.failures.length} failure(s): ${s.failures.slice(0, 3).map(f => `${f.table ? `${f.table} ` : ''}${f.step}: ${f.error}`).join('; ')}`);
    return parts.join('; ');
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

// With no arguments ducklake_cleanup_old_files() keeps recently scheduled
// files much longer, so replaced files would linger on disk.
const cleanupOldFilesSql = () => `CALL ducklake_cleanup_old_files('ducklake', older_than => now() - INTERVAL ${FILE_CLEANUP_GRACE_MINUTES} MINUTE)`;

async function cleanupOldFiles(conn) {
    await conn.run(cleanupOldFilesSql());
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
 * rewrite uses a fresh connection, as in runMaintenance() (ducklake#1108).
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
    // once it has passed (scheduled maintenance also catches any left over).
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
