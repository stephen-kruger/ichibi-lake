/**
 * Durable state for the admin console, kept in DuckLake (and therefore in the
 * shared Postgres catalog) so every gateway instance sees the same console
 * logins, settings overrides, retention rules and audit trail.
 */
import { randomBytes } from 'crypto';
import { execute, executeWithParams, VARCHAR, BIGINT, CONSOLE_CATALOG } from '../db.js';
import { SETTINGS, getSettingDef } from './registry.js';

// Console tables live in their own catalog (see CONSOLE_CATALOG in src/db.js),
// which the user-query instance never attaches.
const table = (name) => `${CONSOLE_CATALOG}.main.${name}`;
export const USERS = table('_console_users');
const SETTINGS_T = table('_console_settings');
export const RETENTION = table('_console_retention');
const AUDIT = table('_console_audit');
const CONSOLE_TABLES = ['_console_users', '_console_settings', '_console_retention', '_console_audit'];

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
    await execute(`CREATE TABLE IF NOT EXISTS ${RETENTION} (
        table_name VARCHAR,
        column_name VARCHAR,
        keep_days BIGINT,
        enabled VARCHAR,
        last_run TIMESTAMP,
        last_deleted BIGINT,
        last_error VARCHAR,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_by VARCHAR
    )`);
    await execute(`CREATE TABLE IF NOT EXISTS ${AUDIT} (
        ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        username VARCHAR,
        action VARCHAR,
        detail VARCHAR,
        ip VARCHAR
    )`);
    await migrateFromMainCatalog();
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

// --- Retention rules -----------------------------------------------------------

export async function listRetentionRules() {
    return execute(
        `SELECT table_name, column_name, keep_days, enabled, ${ts('last_run')}, last_deleted, last_error, ${ts('updated_at')}, updated_by
         FROM ${RETENTION} ORDER BY table_name`
    );
}

export async function getRetentionRule(tableName) {
    const rows = await executeWithParams(
        `SELECT table_name, column_name, keep_days, enabled, ${ts('last_run')}, last_deleted, last_error
         FROM ${RETENTION} WHERE table_name = ?`,
        [tableName], [VARCHAR]
    );
    return rows[0] || null;
}

export async function upsertRetentionRule({ tableName, columnName, keepDays, enabled, updatedBy }) {
    await executeWithParams(`DELETE FROM ${RETENTION} WHERE table_name = ?`, [tableName], [VARCHAR]);
    await executeWithParams(
        `INSERT INTO ${RETENTION} (table_name, column_name, keep_days, enabled, updated_by) VALUES (?, ?, ?, ?, ?)`,
        [tableName, columnName, BigInt(keepDays), enabled ? 'true' : 'false', updatedBy],
        [VARCHAR, VARCHAR, BIGINT, VARCHAR, VARCHAR]
    );
}

export async function deleteRetentionRule(tableName) {
    await executeWithParams(`DELETE FROM ${RETENTION} WHERE table_name = ?`, [tableName], [VARCHAR]);
}

export async function recordRetentionRun(tableName, deleted, error) {
    await executeWithParams(
        `UPDATE ${RETENTION} SET last_run = CURRENT_TIMESTAMP, last_deleted = ?, last_error = ? WHERE table_name = ?`,
        [BigInt(deleted), error || '', tableName], [BIGINT, VARCHAR, VARCHAR]
    );
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
