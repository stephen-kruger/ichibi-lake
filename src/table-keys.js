/**
 * Declared key columns per table, used by key-based ingestion
 * (POST /upload/:tableName?mode=upsert|dedupe, /kafka-sink and the Kafka
 * consumer; see src/ingest.js).
 *
 * Stored in the `_table_keys` system table in DuckLake so every gateway
 * instance sees the same keys. Not cached: upserts read the keys once per
 * batch, which is cheap, and a cache would let an instance upsert with keys
 * another instance has just changed.
 *
 * DuckLake has no PRIMARY KEY / UNIQUE constraints, so a key is a contract the
 * ingestion modes maintain, not something the catalog enforces. Plain appends
 * (mode=append, the console's Add row / import, PATCH) can still write rows
 * with a key that already exists.
 */
import { execute, executeWithParams, withConnection, VARCHAR } from './db.js';
import { retryOnConflict } from './conflict.js';

export const KEYS_TABLE = '_table_keys';
const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
// Key columns must compare by value; nested and binary types cannot.
const UNKEYABLE_TYPE_RE = /^(BLOB|BYTEA|STRUCT|MAP|UNION|JSON)\b|\[\]$|^[A-Z]+\[/i;

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

export async function initTableKeys() {
    await execute(`
        CREATE TABLE IF NOT EXISTS ${KEYS_TABLE} (
            table_name VARCHAR,
            key_columns VARCHAR,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by VARCHAR
        )
    `);
}

function parseKeys(raw) {
    try {
        const keys = JSON.parse(raw || '[]');
        return Array.isArray(keys) ? keys.map(String) : [];
    } catch { return []; }
}

/** Declared key columns of a table ([] when none). `conn` is optional. */
export async function getTableKeys(tableName, conn = null) {
    const sql = `SELECT key_columns FROM ${KEYS_TABLE} WHERE table_name = ? ORDER BY updated_at DESC LIMIT 1`;
    if (conn) {
        const result = await conn.run(sql, [tableName], [VARCHAR]);
        const rows = await result.getRows();
        return rows.length ? parseKeys(rows[0][0]) : [];
    }
    const rows = await executeWithParams(sql, [tableName], [VARCHAR]);
    return rows.length ? parseKeys(rows[0].key_columns) : [];
}

/**
 * Validate a requested key list (shape only). Returns the de-duplicated list.
 * Throws a 400 for anything that is not a list of safe identifiers.
 */
export function normaliseKeyList(keys) {
    if (typeof keys === 'string') keys = keys.split(',');
    if (!Array.isArray(keys)) throw httpError(400, '"keys" must be an array of column names');
    const out = [];
    for (const k of keys) {
        const name = typeof k === 'string' ? k.trim() : k;
        if (typeof name !== 'string' || !SAFE_IDENTIFIER_RE.test(name)) {
            throw httpError(400, `Invalid key column name: ${JSON.stringify(k)}`);
        }
        if (!out.includes(name)) out.push(name);
    }
    if (out.length > 16) throw httpError(400, 'At most 16 key columns are allowed');
    return out;
}

/**
 * Check `keys` against a table's columns ({name -> type}). Throws 400 when a
 * key column is missing or has a type that cannot be compared by value.
 */
export function assertKeysMatchColumns(tableName, keys, columnTypes) {
    for (const k of keys) {
        const type = columnTypes.get(k);
        if (!type) throw httpError(400, `Key column "${k}" does not exist in table "${tableName}"`);
        if (UNKEYABLE_TYPE_RE.test(String(type))) {
            throw httpError(400, `Column "${k}" (${type}) cannot be a key column: nested, JSON and BLOB columns cannot be compared by value`);
        }
    }
}

/** Store (or, with [], clear) the key columns of a table. */
export async function setTableKeys(tableName, keys, updatedBy = null) {
    await retryOnConflict(() => withConnection(async (conn) => {
        await conn.run('BEGIN TRANSACTION');
        try {
            await conn.run(`DELETE FROM ${KEYS_TABLE} WHERE table_name = ?`, [tableName], [VARCHAR]);
            if (keys.length) {
                await conn.run(
                    `INSERT INTO ${KEYS_TABLE} (table_name, key_columns, updated_by) VALUES (?, ?, ?)`,
                    [tableName, JSON.stringify(keys), updatedBy || ''], [VARCHAR, VARCHAR, VARCHAR]
                );
            }
            await conn.run('COMMIT');
        } catch (err) {
            await conn.run('ROLLBACK').catch(() => {});
            throw err;
        }
    }), { label: `table keys ${tableName}` });
}

/** Follow a column rename. Returns the new key list. */
export async function renameKeyColumn(tableName, oldName, newName) {
    const keys = await getTableKeys(tableName);
    if (!keys.includes(oldName)) return keys;
    const next = keys.map(k => (k === oldName ? newName : k));
    await setTableKeys(tableName, next, 'console');
    return next;
}

/**
 * Number of key values that occur more than once in the table (0 = keys are
 * unique). Reported when keys are declared so existing duplicates are
 * visible; upserts replace every row with a matching key, so they converge.
 */
export async function countDuplicateKeys(tableName, keys) {
    if (!keys.length) return 0;
    const cols = keys.map(k => `"${k}"`).join(', ');
    const rows = await execute(
        `SELECT count(*) AS n FROM (SELECT 1 FROM "${tableName}" GROUP BY ${cols} HAVING count(*) > 1)`
    );
    return Number(rows[0]?.n || 0);
}
