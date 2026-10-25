/**
 * Readiness and catalog-size checks shared by GET /health/ready, GET /metrics
 * (src/index.js) and the admin console Overview (src/console/index.js).
 */
import { execute } from './db.js';

// Startup progress of this process, set by the startup sequence in src/index.js:
// 'starting' -> 'ready', or 'failed' just before the process exits.
const startup = { state: 'starting' };
export const getStartupState = () => startup.state;
export const setStartupState = (state) => { startup.state = state; };

const CHECK_TIMEOUT_MS = 5000;

function withTimeout(promise, ms, what) {
    let timer;
    return Promise.race([
        promise,
        new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms}ms`)), ms); }),
    ]).finally(() => clearTimeout(timer));
}

const firstLine = (err) => String(err?.message || err).split('\n')[0];

/**
 * Run the readiness checks: startup finished, DuckLake attached on the primary
 * instance, and the metadata Postgres answering a query through DuckLake's own
 * connection. Each check times out after 5 s.
 * Returns { ready, checks: { startup, ducklake, postgres } }.
 */
export async function checkReadiness() {
    const checks = { startup: startup.state, ducklake: 'unknown', postgres: 'unknown' };
    try {
        const rows = await withTimeout(
            execute("SELECT count(*) AS n FROM duckdb_databases() WHERE database_name = 'ducklake' AND type = 'ducklake'"),
            CHECK_TIMEOUT_MS, 'DuckLake check');
        checks.ducklake = Number(rows[0]?.n) === 1 ? 'ok' : 'not attached';
    } catch (err) {
        checks.ducklake = `error: ${firstLine(err)}`;
    }
    try {
        await withTimeout(
            execute("SELECT * FROM postgres_query('__ducklake_metadata_ducklake', 'SELECT 1')"),
            CHECK_TIMEOUT_MS, 'Postgres check');
        checks.postgres = 'ok';
    } catch (err) {
        checks.postgres = `error: ${firstLine(err)}`;
    }
    const ready = checks.startup === 'ready' && checks.ducklake === 'ok' && checks.postgres === 'ok';
    return { ready, checks };
}

// DuckLake catalog size, cached so frequent scrapes and page loads do not query
// the metadata database every time.
const LAKE_STATS_TTL_MS = 30000;
let lakeStatsCache = { at: 0, value: null };

/** { tables, files, bytes } for user tables' data files, or null if the query failed. */
export async function lakeStats() {
    if (Date.now() - lakeStatsCache.at < LAKE_STATS_TTL_MS) return lakeStatsCache.value;
    let value = null;
    try {
        const rows = await withTimeout(execute(`
            SELECT count(*) FILTER (WHERE NOT starts_with(table_name, '_')) AS tables,
                   coalesce(sum(file_count), 0) AS files,
                   coalesce(sum(file_size_bytes), 0) AS bytes
            FROM ducklake_table_info('ducklake')`), CHECK_TIMEOUT_MS, 'DuckLake stats');
        value = { tables: Number(rows[0].tables), files: Number(rows[0].files), bytes: Number(rows[0].bytes) };
    } catch (err) {
        console.warn('[Health] DuckLake stats query failed:', firstLine(err));
    }
    lakeStatsCache = { at: Date.now(), value };
    return value;
}
