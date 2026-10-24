/**
 * Durable state for the admin console, kept in DuckLake (and therefore in the
 * shared Postgres catalog) so every gateway instance sees the same console
 * logins, settings overrides, retention rules and audit trail.
 */
import { randomBytes } from 'crypto';
import { execute, executeWithParams, VARCHAR, BIGINT, BOOLEAN, CONSOLE_CATALOG } from '../db.js';
import { SETTINGS, getSettingDef } from './registry.js';

// Console tables live in their own catalog (see CONSOLE_CATALOG in src/db.js),
// which the user-query instance never attaches.
const table = (name) => `${CONSOLE_CATALOG}.main.${name}`;
export const USERS = table('_console_users');
const SETTINGS_T = table('_console_settings');
// Legacy single-table retention rules (pre prune-rules); migrated on startup.
const LEGACY_RETENTION = table('_console_retention');
export const PRUNE_RULES = table('_console_prune_rules');
export const PRUNE_RUNS = table('_console_prune_runs');
export const MAINTENANCE = table('_console_maintenance');
const MAINTENANCE_ID = 'lake';
const AUDIT = table('_console_audit');
const CONSOLE_TABLES = ['_console_users', '_console_settings', '_console_audit'];

// Internal (non-registry) keys in _console_settings. Never returned by the API.
export const INTERNAL_KEYS = {
    sessionSecret: '__session_secret',
};

// Snapshot of process.env for registry keys taken before any override is
// applied, so "reset to default" can restore the docker/.env value.
const bootEnv = new Map(SETTINGS.map(s => [s.key, process.env[s.key]]));

// DuckDB returns TIMESTAMP values as objects that do not serialise to useful
// JSON; select them as ISO-8601 UTC strings instead. Timestamps are written
// with CURRENT_TIMESTAMP in the gateway's (UTC in Docker) session time zone.
const ts = (col) => `strftime(${col}, '%Y-%m-%dT%H:%M:%SZ') AS ${col}`;

// key -> { value, updated_at, updated_by } for the current overrides.
let overrides = new Map();

export async function initConsoleTables() {
    await execute(`CREATE TABLE IF NOT EXISTS ${USERS} (
        username VARCHAR,
        password_hash VARCHAR,
        role VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR,
        last_login TIMESTAMP
    )`);
    await execute(`CREATE TABLE IF NOT EXISTS ${SETTINGS_T} (
        key VARCHAR,
        value VARCHAR,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR
    )`);
    // Pruning rules and their run log. Times used for scheduling are epoch
    // milliseconds (UTC), independent of the DuckDB session time zone.
    await execute(`CREATE TABLE IF NOT EXISTS ${PRUNE_RULES} (
        id VARCHAR,
        name VARCHAR,
        table_pattern VARCHAR,
        exclude_pattern VARCHAR,
        date_column VARCHAR,
        max_age_value BIGINT,
        max_age_unit VARCHAR,
        epoch_unit VARCHAR,
        max_rows BIGINT,
        order_column VARCHAR,
        conditions VARCHAR,
        schedule VARCHAR,
        enabled VARCHAR,
        safety_limit BIGINT,
        reclaim_storage VARCHAR,
        next_run_ms BIGINT,
        claim_token VARCHAR,
        last_run_ms BIGINT,
        last_status VARCHAR,
        last_deleted BIGINT,
        created_ms BIGINT,
        created_by VARCHAR,
        updated_ms BIGINT,
        updated_by VARCHAR
    )`);
    // Added after the first prune-rules release (numeric epoch date columns).
    await execute(`ALTER TABLE ${PRUNE_RULES} ADD COLUMN IF NOT EXISTS epoch_unit VARCHAR`);
    await execute(`ALTER TABLE ${PRUNE_RULES} ADD COLUMN IF NOT EXISTS conditions VARCHAR`);
    await execute(`CREATE TABLE IF NOT EXISTS ${PRUNE_RUNS} (
        run_id VARCHAR,
        job_id VARCHAR,
        kind VARCHAR,
        rule_id VARCHAR,
        rule_name VARCHAR,
        table_name VARCHAR,
        trigger VARCHAR,
        status VARCHAR,
        started_ms BIGINT,
        finished_ms BIGINT,
        rows_by_age BIGINT,
        rows_by_count BIGINT,
        rows_by_match BIGINT,
        rows_deleted BIGINT,
        blob_files BIGINT,
        message VARCHAR,
        instance VARCHAR,
        username VARCHAR
    )`);
    await execute(`CREATE TABLE IF NOT EXISTS ${AUDIT} (
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        username VARCHAR,
        action VARCHAR,
        detail VARCHAR,
        ip VARCHAR
    )`);
    await execute(`ALTER TABLE ${PRUNE_RUNS} ADD COLUMN IF NOT EXISTS rows_by_match BIGINT`);
    // Scheduled lake maintenance (src/console/maintenance.js): one row. Its runs
    // go to PRUNE_RUNS with kind 'maintenance'.
    await execute(`CREATE TABLE IF NOT EXISTS ${MAINTENANCE} (
        id VARCHAR,
        enabled VARCHAR,
        schedule VARCHAR,
        steps VARCHAR,
        delete_threshold VARCHAR,
        run_at_startup VARCHAR,
        next_run_ms BIGINT,
        claim_token VARCHAR,
        lease_until_ms BIGINT,
        last_run_ms BIGINT,
        last_status VARCHAR,
        last_summary VARCHAR,
        updated_ms BIGINT,
        updated_by VARCHAR
    )`);
    await migrateFromMainCatalog();
    await migrateLegacyRetention();
}

/** Cron expression approximating the old global RETENTION_INTERVAL_MINUTES. */
export function intervalToCron(minutes) {
    const m = Number(minutes);
    if (!Number.isInteger(m) || m <= 0) return '0 * * * *';
    if (m < 60 && 60 % m === 0) return m === 1 ? '* * * * *' : `*/${m} * * * *`;
    if (m % 60 === 0) {
        const h = m / 60;
        if (h === 24) return '0 0 * * *';
        if (h < 24 && 24 % h === 0) return h === 1 ? '0 * * * *' : `0 */${h} * * *`;
    }
    return '0 * * * *';
}

/**
 * Convert legacy one-table-per-rule retention rows (in the main catalog from
 * early releases, or the console catalog from the previous release) into
 * prune rules, using the old global interval as each rule's schedule.
 */
async function migrateLegacyRetention() {
    const sources = [`ducklake.main._console_retention`, LEGACY_RETENTION];
    const catalogs = [['ducklake', '_console_retention'], [CONSOLE_CATALOG, '_console_retention']];
    const intervalRow = await executeWithParams(`SELECT value FROM ${SETTINGS_T} WHERE key = ?`, ['RETENTION_INTERVAL_MINUTES'], [VARCHAR]);
    const schedule = intervalToCron(intervalRow[0]?.value ?? process.env.RETENTION_INTERVAL_MINUTES ?? 60);
    for (let i = 0; i < sources.length; i++) {
        const [catalog, name] = catalogs[i];
        const exists = await executeWithParams(
            `SELECT count(*) AS n FROM information_schema.tables WHERE table_catalog = ? AND table_schema = 'main' AND table_name = ?`,
            [catalog, name], [VARCHAR, VARCHAR]);
        if (Number(exists[0]?.n || 0) === 0) continue;
        const legacy = await execute(`SELECT table_name, column_name, keep_days, enabled, updated_by FROM ${sources[i]}`);
        for (const r of legacy) {
            const already = await executeWithParams(`SELECT count(*) AS n FROM ${PRUNE_RULES} WHERE table_pattern = ? AND name = ?`,
                [r.table_name, r.table_name], [VARCHAR, VARCHAR]);
            if (Number(already[0]?.n || 0) > 0) continue;
            await createPruneRule({
                id: randomBytes(8).toString('hex'), name: r.table_name, tablePattern: r.table_name, excludePattern: '',
                dateColumn: r.column_name, maxAgeValue: Number(r.keep_days), maxAgeUnit: 'days', maxRows: null, orderColumn: null,
                schedule, enabled: r.enabled === 'true', safetyLimit: null, reclaimStorage: false,
                nextRunMs: null, user: r.updated_by || 'migration',
            });
        }
        await execute(`DROP TABLE ${sources[i]}`);
        console.log(`[Console] Converted ${legacy.length} legacy retention rule(s) from ${catalog} to prune rules (schedule ${schedule}).`);
    }
    await executeWithParams(`DELETE FROM ${SETTINGS_T} WHERE key = ?`, ['RETENTION_INTERVAL_MINUTES'], [VARCHAR]);
}

/**
 * Earlier releases kept the console tables in the main DuckLake catalog,
 * where user-supplied SQL could read them. Move their rows into the console
 * catalog once, then drop the originals. DuckDB cannot write two catalogs in
 * one transaction, so the steps are ordered to be safe to re-run: rows are
 * only copied while the destination is empty, and the source is dropped last.
 */
async function migrateFromMainCatalog() {
    for (const name of CONSOLE_TABLES) {
        const exists = await executeWithParams(
            `SELECT count(*) AS n FROM information_schema.tables WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND table_name = ?`,
            [name], [VARCHAR]);
        if (Number(exists[0]?.n || 0) === 0) continue;
        const target = table(name);
        const empty = Number((await execute(`SELECT count(*) AS n FROM ${target}`))[0]?.n || 0) === 0;
        if (empty) {
            await execute(`INSERT INTO ${target} BY NAME SELECT * FROM ducklake.main.${name}`);
        }
        await execute(`DROP TABLE ducklake.main.${name}`);
        console.log(`[Console] Moved ${name} to the ${CONSOLE_CATALOG} catalog.`);
    }
}

// --- Console users -----------------------------------------------------------

export async function countUsers() {
    const rows = await execute(`SELECT count(*) AS n FROM ${USERS}`);
    return Number(rows[0]?.n || 0);
}

export async function findUser(username) {
    const rows = await executeWithParams(
        `SELECT username, password_hash, role, ${ts('created_at')}, created_by, ${ts('last_login')}
         FROM ${USERS} WHERE lower(username) = lower(?)`,
        [username], [VARCHAR]
    );
    return rows[0] || null;
}

export async function listUsers() {
    return execute(
        `SELECT username, role, ${ts('created_at')}, created_by, ${ts('last_login')} FROM ${USERS} ORDER BY created_at`
    );
}

export async function createUser({ username, passwordHash, role, createdBy }) {
    await executeWithParams(
        `INSERT INTO ${USERS} (username, password_hash, role, created_by) VALUES (?, ?, ?, ?)`,
        [username, passwordHash, role, createdBy], [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
    );
}

export async function updateUserPassword(username, passwordHash) {
    await executeWithParams(
        `UPDATE ${USERS} SET password_hash = ? WHERE lower(username) = lower(?)`,
        [passwordHash, username], [VARCHAR, VARCHAR]
    );
}

export async function updateUserRole(username, role) {
    await executeWithParams(
        `UPDATE ${USERS} SET role = ? WHERE lower(username) = lower(?)`,
        [role, username], [VARCHAR, VARCHAR]
    );
}

export async function deleteUser(username) {
    await executeWithParams(
        `DELETE FROM ${USERS} WHERE lower(username) = lower(?)`,
        [username], [VARCHAR]
    );
}

export async function countAdmins() {
    const rows = await execute(`SELECT count(*) AS n FROM ${USERS} WHERE role = 'admin'`);
    return Number(rows[0]?.n || 0);
}

export async function touchLastLogin(username) {
    await executeWithParams(
        `UPDATE ${USERS} SET last_login = CURRENT_TIMESTAMP WHERE lower(username) = lower(?)`,
        [username], [VARCHAR]
    );
}

// --- Settings overrides ------------------------------------------------------

async function readRawSetting(key) {
    const rows = await executeWithParams(
        `SELECT value FROM ${SETTINGS_T} WHERE key = ?`, [key], [VARCHAR]
    );
    return rows.length ? rows[0].value : null;
}

async function writeRawSetting(key, value, updatedBy) {
    // DuckLake has no UPSERT; delete + insert under one key is sufficient for
    // this low-write table.
    await executeWithParams(`DELETE FROM ${SETTINGS_T} WHERE key = ?`, [key], [VARCHAR]);
    await executeWithParams(
        `INSERT INTO ${SETTINGS_T} (key, value, updated_by) VALUES (?, ?, ?)`,
        [key, value, updatedBy], [VARCHAR, VARCHAR, VARCHAR]
    );
}

/** Returns the session-signing secret, creating it on first use. */
export async function getOrCreateSessionSecret() {
    if (process.env.CONSOLE_SESSION_SECRET) return process.env.CONSOLE_SESSION_SECRET;
    let secret = await readRawSetting(INTERNAL_KEYS.sessionSecret);
    if (!secret) {
        secret = randomBytes(32).toString('hex');
        await writeRawSetting(INTERNAL_KEYS.sessionSecret, secret, 'system');
        // Another instance may have raced us; re-read so all instances agree.
        secret = await readRawSetting(INTERNAL_KEYS.sessionSecret) || secret;
    }
    return secret;
}

function applyToEnv(key) {
    const o = overrides.get(key);
    if (o) {
        process.env[key] = o.value;
    } else if (bootEnv.get(key) === undefined) {
        delete process.env[key];
    } else {
        process.env[key] = bootEnv.get(key);
    }
}

/**
 * Load overrides from DuckLake and apply them to process.env. Code that reads
 * a live setting from process.env on use picks the change up immediately.
 * Called at startup and periodically so other instances converge.
 */
export async function refreshOverrides() {
    const rows = await execute(
        `SELECT key, value, ${ts('updated_at')}, updated_by FROM ${SETTINGS_T} WHERE key NOT LIKE '\\_\\_%' ESCAPE '\\'`
    );
    const next = new Map();
    for (const r of rows) {
        const def = getSettingDef(r.key);
        if (def && def.editable) next.set(r.key, { value: r.value, updated_at: r.updated_at, updated_by: r.updated_by });
    }
    const touched = new Set([...overrides.keys(), ...next.keys()]);
    overrides = next;
    for (const key of touched) applyToEnv(key);
}

export async function setOverride(key, value, updatedBy) {
    await writeRawSetting(key, value, updatedBy);
    overrides.set(key, { value, updated_at: new Date().toISOString(), updated_by: updatedBy });
    applyToEnv(key);
}

export async function clearOverride(key) {
    await executeWithParams(`DELETE FROM ${SETTINGS_T} WHERE key = ?`, [key], [VARCHAR]);
    overrides.delete(key);
    applyToEnv(key);
}

/** Describe a setting's effective value and where it came from. */
export function describeSetting(def) {
    const o = overrides.get(def.key);
    const envValue = bootEnv.get(def.key);
    let value, source;
    if (o) { value = o.value; source = 'console'; }
    else if (envValue !== undefined && envValue !== '') { value = envValue; source = 'env'; }
    else { value = def.default ?? ''; source = 'default'; }
    return {
        value,
        source,
        envValue: envValue ?? null,
        updatedAt: o?.updated_at ?? null,
        updatedBy: o?.updated_by ?? null,
    };
}

// --- Prune rules -----------------------------------------------------------------

const num = (v) => (v === null || v === undefined ? null : Number(v));
const big = (v) => (v === null || v === undefined || v === '' ? null : BigInt(Math.trunc(Number(v))));

function parseConditions(text) {
    if (!text) return null;
    try { const c = JSON.parse(text); return c && Array.isArray(c.items) && c.items.length ? c : null; } catch { return null; }
}

function ruleFromRow(r) {
    return {
        id: r.id, name: r.name, tablePattern: r.table_pattern, excludePattern: r.exclude_pattern || '',
        dateColumn: r.date_column || null, maxAgeValue: num(r.max_age_value), maxAgeUnit: r.max_age_unit || 'days',
        epochUnit: r.epoch_unit || 'auto',
        maxRows: num(r.max_rows), orderColumn: r.order_column || null, conditions: parseConditions(r.conditions), schedule: r.schedule,
        enabled: r.enabled === 'true', safetyLimit: num(r.safety_limit), reclaimStorage: r.reclaim_storage === 'true',
        nextRunMs: num(r.next_run_ms), lastRunMs: num(r.last_run_ms), lastStatus: r.last_status || null,
        lastDeleted: num(r.last_deleted), createdMs: num(r.created_ms), createdBy: r.created_by,
        updatedMs: num(r.updated_ms), updatedBy: r.updated_by,
    };
}

const RULE_COLUMNS = `id, name, table_pattern, exclude_pattern, date_column, max_age_value, max_age_unit, epoch_unit, max_rows,
    order_column, conditions, schedule, enabled, safety_limit, reclaim_storage, next_run_ms, last_run_ms, last_status, last_deleted,
    created_ms, created_by, updated_ms, updated_by`;

export async function listPruneRules() {
    return (await execute(`SELECT ${RULE_COLUMNS} FROM ${PRUNE_RULES} ORDER BY lower(name)`)).map(ruleFromRow);
}

export async function getPruneRule(id) {
    const rows = await executeWithParams(`SELECT ${RULE_COLUMNS} FROM ${PRUNE_RULES} WHERE id = ?`, [id], [VARCHAR]);
    return rows.length ? ruleFromRow(rows[0]) : null;
}

export async function createPruneRule(rule) {
    const now = Date.now();
    await executeWithParams(
        `INSERT INTO ${PRUNE_RULES} (id, name, table_pattern, exclude_pattern, date_column, max_age_value, max_age_unit, epoch_unit, max_rows,
            order_column, conditions, schedule, enabled, safety_limit, reclaim_storage, next_run_ms, created_ms, created_by, updated_ms, updated_by)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [rule.id, rule.name, rule.tablePattern, rule.excludePattern || '', rule.dateColumn || null, big(rule.maxAgeValue),
            rule.maxAgeUnit || 'days', rule.epochUnit || 'auto', big(rule.maxRows), rule.orderColumn || null,
            rule.conditions ? JSON.stringify(rule.conditions) : null, rule.schedule, rule.enabled ? 'true' : 'false',
            big(rule.safetyLimit), rule.reclaimStorage ? 'true' : 'false', big(rule.nextRunMs), BigInt(now), rule.user, BigInt(now), rule.user],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT,
            BIGINT, VARCHAR, BIGINT, VARCHAR]);
}

export async function updatePruneRule(id, rule) {
    await executeWithParams(
        `UPDATE ${PRUNE_RULES} SET name = ?, table_pattern = ?, exclude_pattern = ?, date_column = ?, max_age_value = ?,
            max_age_unit = ?, epoch_unit = ?, max_rows = ?, order_column = ?, conditions = ?, schedule = ?, enabled = ?, safety_limit = ?, reclaim_storage = ?,
            next_run_ms = ?, updated_ms = ?, updated_by = ?
         WHERE id = ?`,
        [rule.name, rule.tablePattern, rule.excludePattern || '', rule.dateColumn || null, big(rule.maxAgeValue),
            rule.maxAgeUnit || 'days', rule.epochUnit || 'auto', big(rule.maxRows), rule.orderColumn || null,
            rule.conditions ? JSON.stringify(rule.conditions) : null, rule.schedule, rule.enabled ? 'true' : 'false',
            big(rule.safetyLimit), rule.reclaimStorage ? 'true' : 'false', big(rule.nextRunMs), BigInt(Date.now()), rule.user, id],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, BIGINT, BIGINT, VARCHAR, VARCHAR]);
}

export async function setRuleEnabled(id, enabled, nextRunMs, user) {
    await executeWithParams(
        `UPDATE ${PRUNE_RULES} SET enabled = ?, next_run_ms = ?, updated_ms = ?, updated_by = ? WHERE id = ?`,
        [enabled ? 'true' : 'false', big(nextRunMs), BigInt(Date.now()), user, id], [VARCHAR, BIGINT, BIGINT, VARCHAR, VARCHAR]);
}

export async function deletePruneRule(id) {
    await executeWithParams(`DELETE FROM ${PRUNE_RULES} WHERE id = ?`, [id], [VARCHAR]);
}

/** Rules that are enabled and due at `nowMs` (next_run_ms unset counts as due). */
export async function dueRuleIds(nowMs) {
    const rows = await executeWithParams(
        `SELECT id FROM ${PRUNE_RULES} WHERE enabled = 'true' AND (next_run_ms IS NULL OR next_run_ms <= ?)`,
        [BigInt(nowMs)], [BIGINT]);
    return rows.map(r => r.id);
}

/**
 * A commit that lost a race with another instance's write to the same rows
 * (DuckLake: "Transaction conflict ..."). For a claim this means another
 * instance got there first.
 */
export function isTransactionConflict(err) {
    return /Transaction conflict|Failed to commit DuckLake transaction/i.test(String(err?.message || err));
}

/** Run a write, retrying a few times when it loses a commit race (see isTransactionConflict). */
async function retryOnConflict(fn, attempts = 5) {
    for (let i = 1; ; i++) {
        try { return await fn(); } catch (err) {
            if (i >= attempts || !isTransactionConflict(err)) throw err;
            await new Promise(r => setTimeout(r, 100 * i + Math.floor(Math.random() * 100)));
        }
    }
}

/**
 * Claim a due rule for this instance. DuckLake does not make concurrent
 * conditional UPDATEs exclusive (both transactions can commit), so the claim
 * writes a unique token and re-reads it: only the instance whose token
 * survived runs the job. When DuckLake does detect the race, the losing
 * UPDATE fails to commit, which also means "not claimed". Returns true when
 * this instance owns the run.
 */
export async function claimRule(id, token, nowMs, nextRunMs) {
    try {
        await executeWithParams(
            `UPDATE ${PRUNE_RULES} SET claim_token = ?, next_run_ms = ?
             WHERE id = ? AND enabled = 'true' AND (next_run_ms IS NULL OR next_run_ms <= ?)`,
            [token, big(nextRunMs), id, BigInt(nowMs)], [VARCHAR, BIGINT, VARCHAR, BIGINT]);
    } catch (err) {
        if (isTransactionConflict(err)) return false;
        throw err;
    }
    const rows = await executeWithParams(`SELECT claim_token FROM ${PRUNE_RULES} WHERE id = ?`, [id], [VARCHAR]);
    return rows[0]?.claim_token === token;
}

export async function setRuleNextRun(id, nextRunMs) {
    await executeWithParams(`UPDATE ${PRUNE_RULES} SET next_run_ms = ? WHERE id = ?`, [big(nextRunMs), id], [BIGINT, VARCHAR]);
}

export async function recordRuleResult(id, status, deleted) {
    await executeWithParams(
        `UPDATE ${PRUNE_RULES} SET last_run_ms = ?, last_status = ?, last_deleted = ? WHERE id = ?`,
        [BigInt(Date.now()), status, big(deleted), id], [BIGINT, VARCHAR, BIGINT, VARCHAR]);
}

/** Point exact-name rules at a renamed table (wildcard rules re-match at run time). */
export async function renameTableInRules(oldName, newName) {
    await executeWithParams(`UPDATE ${PRUNE_RULES} SET table_pattern = ? WHERE lower(table_pattern) = lower(?)`,
        [newName, oldName], [VARCHAR, VARCHAR]);
}

// --- Prune runs (job log) ----------------------------------------------------------

export async function insertRun(run) {
    await executeWithParams(
        `INSERT INTO ${PRUNE_RUNS} (run_id, job_id, kind, rule_id, rule_name, table_name, trigger, status, started_ms, instance, username)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        [run.runId, run.jobId, run.kind, run.ruleId, run.ruleName, run.tableName || null, run.trigger, run.status,
            BigInt(run.startedMs), run.instance, run.username || null],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR]);
}

export async function finishRun(runId, r) {
    await executeWithParams(
        `UPDATE ${PRUNE_RUNS} SET status = ?, finished_ms = ?, rows_by_age = ?, rows_by_count = ?, rows_by_match = ?, rows_deleted = ?,
            blob_files = ?, message = ? WHERE run_id = ?`,
        [r.status, BigInt(Date.now()), big(r.rowsByAge), big(r.rowsByCount), big(r.rowsByMatch), big(r.rowsDeleted), big(r.blobFiles),
            String(r.message || '').slice(0, 2000), runId],
        [VARCHAR, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, BIGINT, VARCHAR, VARCHAR]);
}

/** Recent jobs (newest first) with their per-table runs. */
export async function listRuns({ ruleId = null, limit = 50 } = {}) {
    const n = Math.max(1, Math.min(500, Number(limit) || 50));
    const jobs = ruleId
        ? await executeWithParams(`SELECT * FROM ${PRUNE_RUNS} WHERE kind = 'job' AND rule_id = ? ORDER BY started_ms DESC LIMIT ${n}`, [ruleId], [VARCHAR])
        : await execute(`SELECT * FROM ${PRUNE_RUNS} WHERE kind = 'job' ORDER BY started_ms DESC LIMIT ${n}`);
    if (!jobs.length) return [];
    const ids = jobs.map(j => j.job_id);
    const tables = await executeWithParams(
        `SELECT * FROM ${PRUNE_RUNS} WHERE kind = 'table' AND job_id IN (${ids.map(() => '?').join(', ')}) ORDER BY table_name`,
        ids, ids.map(() => VARCHAR));
    const shape = (r) => ({
        runId: r.run_id, jobId: r.job_id, ruleId: r.rule_id, ruleName: r.rule_name, table: r.table_name, trigger: r.trigger,
        status: r.status, startedMs: num(r.started_ms), finishedMs: num(r.finished_ms), rowsByAge: num(r.rows_by_age),
        rowsByCount: num(r.rows_by_count), rowsByMatch: num(r.rows_by_match), rowsDeleted: num(r.rows_deleted), blobFiles: num(r.blob_files),
        message: r.message || '', instance: r.instance, username: r.username,
    });
    return jobs.map(j => ({ ...shape(j), tables: tables.filter(t => t.job_id === j.job_id).map(shape) }));
}

/** Mark runs left 'running' by a crashed or restarted instance as abandoned. */
export async function abandonStaleRuns(olderThanMs) {
    await executeWithParams(
        `UPDATE ${PRUNE_RUNS} SET status = 'abandoned', finished_ms = ?, message = 'The gateway stopped before this run finished.'
         WHERE status = 'running' AND started_ms < ?`,
        [BigInt(Date.now()), BigInt(olderThanMs)], [BIGINT, BIGINT]);
}

export async function abandonRunsOfHost(hostPrefix) {
    await executeWithParams(
        `UPDATE ${PRUNE_RUNS} SET status = 'abandoned', finished_ms = ?, message = 'The gateway restarted before this run finished.'
         WHERE status = 'running' AND starts_with(instance, ?)`,
        [BigInt(Date.now()), hostPrefix], [BIGINT, VARCHAR]);
}

/** Keep exact-name rules in step with a renamed column of that table. */
export async function renameColumnInRules(tableName, oldName, newName) {
    for (const col of ['date_column', 'order_column']) {
        await executeWithParams(
            `UPDATE ${PRUNE_RULES} SET ${col} = ? WHERE lower(table_pattern) = lower(?) AND ${col} = ?`,
            [newName, tableName, oldName], [VARCHAR, VARCHAR, VARCHAR]);
    }
}

export async function trimRuns(olderThanMs) {
    await executeWithParams(`DELETE FROM ${PRUNE_RUNS} WHERE started_ms < ? AND status <> 'running'`, [BigInt(olderThanMs)], [BIGINT]);
}

// --- Lake maintenance -----------------------------------------------------------

function maintenanceFromRow(r) {
    let steps = null;
    try { steps = JSON.parse(r.steps || 'null'); } catch { steps = null; }
    return {
        enabled: r.enabled === 'true', schedule: r.schedule, steps,
        deleteThreshold: r.delete_threshold === null || r.delete_threshold === undefined || r.delete_threshold === '' ? null : Number(r.delete_threshold),
        runAtStartup: r.run_at_startup !== 'false',
        nextRunMs: num(r.next_run_ms), leaseUntilMs: num(r.lease_until_ms), lastRunMs: num(r.last_run_ms),
        lastStatus: r.last_status || null, lastSummary: r.last_summary || '', updatedMs: num(r.updated_ms), updatedBy: r.updated_by || null,
    };
}

/**
 * Create the maintenance row with `defaults` unless it exists. Instances that
 * start together may both insert; the duplicates are removed again so every
 * instance claims the same single row.
 */
export async function ensureMaintenanceRow(defaults) {
    const count = async () => Number((await executeWithParams(`SELECT count(*) AS n FROM ${MAINTENANCE} WHERE id = ?`, [MAINTENANCE_ID], [VARCHAR]))[0]?.n || 0);
    if (await count() === 0) {
        await executeWithParams(
            `INSERT INTO ${MAINTENANCE} (id, enabled, schedule, steps, delete_threshold, run_at_startup, next_run_ms, updated_ms, updated_by)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
            [MAINTENANCE_ID, defaults.enabled ? 'true' : 'false', defaults.schedule, JSON.stringify(defaults.steps),
                defaults.deleteThreshold === null ? null : String(defaults.deleteThreshold), defaults.runAtStartup ? 'true' : 'false',
                big(defaults.nextRunMs), BigInt(Date.now()), 'system'],
            [VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR]);
    }
    if (await count() > 1) {
        await executeWithParams(`DELETE FROM ${MAINTENANCE} WHERE id = ? AND rowid <> (SELECT min(rowid) FROM ${MAINTENANCE} WHERE id = ?)`,
            [MAINTENANCE_ID, MAINTENANCE_ID], [VARCHAR, VARCHAR]);
    }
}

export async function getMaintenance() {
    const rows = await executeWithParams(`SELECT * FROM ${MAINTENANCE} WHERE id = ? LIMIT 1`, [MAINTENANCE_ID], [VARCHAR]);
    return rows.length ? maintenanceFromRow(rows[0]) : null;
}

export async function updateMaintenance(cfg, user) {
    await retryOnConflict(() => executeWithParams(
        `UPDATE ${MAINTENANCE} SET enabled = ?, schedule = ?, steps = ?, delete_threshold = ?, run_at_startup = ?, next_run_ms = ?,
            updated_ms = ?, updated_by = ? WHERE id = ?`,
        [cfg.enabled ? 'true' : 'false', cfg.schedule, JSON.stringify(cfg.steps), cfg.deleteThreshold === null ? null : String(cfg.deleteThreshold),
            cfg.runAtStartup ? 'true' : 'false', big(cfg.nextRunMs), BigInt(Date.now()), user, MAINTENANCE_ID],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, VARCHAR]));
}

export async function setMaintenanceNextRun(nextRunMs) {
    await executeWithParams(`UPDATE ${MAINTENANCE} SET next_run_ms = ? WHERE id = ?`, [big(nextRunMs), MAINTENANCE_ID], [BIGINT, VARCHAR]);
}

/**
 * Claim the maintenance run for this instance, with the same write-token-
 * and-re-read pattern as claimRule() (DuckLake does not make concurrent
 * conditional UPDATEs exclusive). A lease additionally stops any instance
 * from claiming while a run is in progress, however long it takes. Scheduled
 * claims need the job to be enabled and due; `force` (manual or startup runs)
 * only needs the lease to be free. A claim that fails to commit because
 * another instance's claim won (isTransactionConflict) is not an error.
 * Returns true when this instance owns it.
 */
export async function claimMaintenance(token, { nowMs, nextRunMs, leaseUntilMs, force = false }) {
    try {
        await executeWithParams(
            `UPDATE ${MAINTENANCE} SET claim_token = ?, next_run_ms = ?, lease_until_ms = ?
             WHERE id = ? AND (lease_until_ms IS NULL OR lease_until_ms <= ?)
               AND (? OR (enabled = 'true' AND (next_run_ms IS NULL OR next_run_ms <= ?)))`,
            [token, big(nextRunMs), big(leaseUntilMs), MAINTENANCE_ID, BigInt(nowMs), force, BigInt(nowMs)],
            [VARCHAR, BIGINT, BIGINT, VARCHAR, BIGINT, BOOLEAN, BIGINT]);
    } catch (err) {
        if (isTransactionConflict(err)) return false;
        throw err;
    }
    const rows = await executeWithParams(`SELECT claim_token FROM ${MAINTENANCE} WHERE id = ?`, [MAINTENANCE_ID], [VARCHAR]);
    return rows.length > 0 && rows.every(r => r.claim_token === token);
}

/** Release the lease and record the outcome (only if this instance holds the claim). */
export async function finishMaintenance(token, status, summary) {
    await retryOnConflict(() => executeWithParams(
        `UPDATE ${MAINTENANCE} SET lease_until_ms = NULL, last_run_ms = ?, last_status = ?, last_summary = ? WHERE id = ? AND claim_token = ?`,
        [BigInt(Date.now()), status, String(summary || '').slice(0, 2000), MAINTENANCE_ID, token], [BIGINT, VARCHAR, VARCHAR, VARCHAR, VARCHAR]));
}

/** Release a lease left by an earlier process on this host (crash/restart), as abandonRunsOfHost() does for runs. */
export async function releaseMaintenanceLeaseOfHost(hostPrefix) {
    await executeWithParams(
        `UPDATE ${MAINTENANCE} SET lease_until_ms = NULL, last_status = 'abandoned', last_summary = 'The gateway restarted before this run finished.'
         WHERE id = ? AND lease_until_ms IS NOT NULL AND starts_with(claim_token, ?)`,
        [MAINTENANCE_ID, hostPrefix], [VARCHAR, VARCHAR]);
}

/** Recent maintenance runs, newest first. */
export async function listMaintenanceRuns(limit = 20) {
    const n = Math.max(1, Math.min(200, Number(limit) || 20));
    const rows = await execute(`SELECT * FROM ${PRUNE_RUNS} WHERE kind = 'maintenance' ORDER BY started_ms DESC LIMIT ${n}`);
    return rows.map(r => ({
        runId: r.run_id, trigger: r.trigger, status: r.status, startedMs: num(r.started_ms), finishedMs: num(r.finished_ms),
        message: r.message || '', instance: r.instance, username: r.username,
    }));
}

// --- Audit log -----------------------------------------------------------------

export async function audit(username, action, detail = '', ip = '') {
    try {
        await executeWithParams(
            `INSERT INTO ${AUDIT} (username, action, detail, ip) VALUES (?, ?, ?, ?)`,
            [username || '', action, String(detail).slice(0, 2000), ip || ''],
            [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
        );
    } catch (err) {
        console.warn('[Console] Failed to write audit entry:', err.message || err);
    }
}

export async function listAudit(limit = 200) {
    const n = Math.max(1, Math.min(1000, Number(limit) || 200));
    return execute(`SELECT ${ts('ts')}, username, action, detail, ip FROM ${AUDIT} a ORDER BY a.ts DESC LIMIT ${n}`);
}
