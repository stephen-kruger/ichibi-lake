/**
 * Table and data browser for the admin console: paged/sorted/filtered reads,
 * inline cell edits, row inserts/deletes and schema changes on DuckLake user
 * tables.
 *
 * Safety model
 * ------------
 * - System tables (names starting with "_": RBAC, ACLs, console logins with
 *   password hashes, settings with the session secret, ...) are never
 *   reachable here.
 * - Existing table/column names are only used after being matched against
 *   information_schema, then quoted with quoteIdent(). User tables can carry
 *   arbitrary column names (upload schema evolution adds JSON keys verbatim).
 *   NEW names (create/rename) must be safe identifiers.
 * - Types come from an allow-list (normaliseType); defaults are emitted as
 *   escaped, CAST string literals. Values are always bound parameters.
 * - Rows are addressed by DuckLake's `rowid`, which is stable across updates,
 *   compaction, schema changes and renames. Cell edits carry the value the
 *   user saw and only apply if it is unchanged (optimistic concurrency).
 */
import { withConnection, VARCHAR, BIGINT, invalidateColumnCache } from '../db.js';
import { invalidateAclCache } from '../acl.js';
import { invalidateRbacCache } from '../rbac.js';
import { listGraphDefinitions } from '../db.js';
import * as consoleStore from './store.js';
import { matchTables, isDateType, isEpochType } from './retention.js';
import { EXPORT_FORMATS, runExport } from '../export.js';
import { resolveTypeConflict } from '../schema-evolution.js';
import { getTableKeys, setTableKeys, normaliseKeyList, assertKeysMatchColumns, countDuplicateKeys, renameKeyColumn } from '../table-keys.js';
import { isTransactionConflict } from '../conflict.js';

export const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
export const PAGE_SIZES = [25, 50, 100, 250, 500];
const DISPLAY_LIMIT = 1000;          // characters per cell sent to the browser
const MAX_DELETE = 1000;

export function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

export function quoteIdent(name) {
    return `"${String(name).replace(/"/g, '""')}"`;
}

export function quoteLiteral(value) {
    return `'${String(value).replace(/'/g, "''")}'`;
}

export function isSystemTable(name) {
    return String(name).startsWith('_');
}

// --- Types ----------------------------------------------------------------------

const SIMPLE_TYPES = [
    'BOOLEAN', 'TINYINT', 'SMALLINT', 'INTEGER', 'BIGINT', 'HUGEINT',
    'UTINYINT', 'USMALLINT', 'UINTEGER', 'UBIGINT',
    'FLOAT', 'DOUBLE', 'VARCHAR', 'DATE', 'TIME', 'TIMESTAMP', 'TIMESTAMPTZ',
    'INTERVAL', 'UUID', 'BLOB', 'JSON',
];
const TYPE_ALIASES = {
    BOOL: 'BOOLEAN', INT: 'INTEGER', INT4: 'INTEGER', INT8: 'BIGINT', INT2: 'SMALLINT', INT1: 'TINYINT',
    LONG: 'BIGINT', SHORT: 'SMALLINT', REAL: 'FLOAT', FLOAT4: 'FLOAT', FLOAT8: 'DOUBLE', TEXT: 'VARCHAR',
    STRING: 'VARCHAR', DATETIME: 'TIMESTAMP', 'TIMESTAMP WITH TIME ZONE': 'TIMESTAMPTZ',
};
/** Types offered in the UI for new columns/tables. */
export const COLUMN_TYPES = [...SIMPLE_TYPES, 'DECIMAL(18,3)'];

/** Canonical, allow-listed type string, or null if not allowed. */
export function normaliseType(raw) {
    const t = String(raw || '').trim().toUpperCase().replace(/\s+/g, ' ');
    if (SIMPLE_TYPES.includes(t)) return t;
    if (TYPE_ALIASES[t]) return TYPE_ALIASES[t];
    const dec = t.match(/^(?:DECIMAL|NUMERIC)\s*\(\s*(\d{1,2})\s*,\s*(\d{1,2})\s*\)$/);
    if (dec) {
        const p = Number(dec[1]), s = Number(dec[2]);
        if (p >= 1 && p <= 38 && s <= p) return `DECIMAL(${p},${s})`;
    }
    return null;
}

/**
 * DuckLake only allows widening type changes. Targets offered per source type
 * (anything else is rejected by DuckLake; the server still passes its error on).
 */
const WIDENINGS = {
    TINYINT: ['SMALLINT', 'INTEGER', 'BIGINT'],
    SMALLINT: ['INTEGER', 'BIGINT'],
    INTEGER: ['BIGINT'],
    UTINYINT: ['USMALLINT', 'UINTEGER', 'UBIGINT', 'SMALLINT', 'INTEGER', 'BIGINT'],
    USMALLINT: ['UINTEGER', 'UBIGINT', 'INTEGER', 'BIGINT'],
    UINTEGER: ['UBIGINT', 'BIGINT'],
    FLOAT: ['DOUBLE'],
};
export function wideningTargets(type) {
    return WIDENINGS[String(type).toUpperCase()] || [];
}

const EDITABLE_TYPE_RE = /^(BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|UTINYINT|USMALLINT|UINTEGER|UBIGINT|FLOAT|DOUBLE|DECIMAL\(\d+,\d+\)|VARCHAR|DATE|TIME|TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ|TIMESTAMP_S|TIMESTAMP_MS|TIMESTAMP_NS|INTERVAL|UUID|JSON)$/i;
/** Scalar types that round-trip through text and can be edited inline. */
export function isEditableType(type) {
    return EDITABLE_TYPE_RE.test(String(type).trim());
}

function isTimestampType(type) {
    return /^TIMESTAMP/i.test(String(type));
}

// --- Filters ---------------------------------------------------------------------

export const FILTER_OPS = ['contains', 'equals', 'not_equals', 'starts_with', 'gt', 'gte', 'lt', 'lte', 'is_null', 'not_null'];

/**
 * Build a WHERE clause from validated filters. `columns` maps name -> type and
 * is the allow-list for column references. Returns { sql, params, types }.
 */
export function buildWhere(filters, search, columns) {
    const parts = [];
    const params = [];
    const types = [];
    const text = (col) => `CAST(${quoteIdent(col)} AS VARCHAR)`;
    for (const f of Array.isArray(filters) ? filters.slice(0, 20) : []) {
        if (!f || !columns.has(f.column)) throw httpError(400, `Unknown filter column: ${f?.column}`);
        if (!FILTER_OPS.includes(f.op)) throw httpError(400, `Unknown filter operator: ${f.op}`);
        const col = quoteIdent(f.column);
        const type = columns.get(f.column);
        const v = f.value === undefined || f.value === null ? '' : String(f.value);
        switch (f.op) {
            case 'is_null': parts.push(`${col} IS NULL`); continue;
            case 'not_null': parts.push(`${col} IS NOT NULL`); continue;
            case 'contains': parts.push(`${text(f.column)} ILIKE ?`); params.push(`%${escapeLike(v)}%`); break;
            case 'starts_with': parts.push(`${text(f.column)} ILIKE ?`); params.push(`${escapeLike(v)}%`); break;
            case 'equals': parts.push(`${text(f.column)} = ?`); params.push(v); break;
            case 'not_equals': parts.push(`${text(f.column)} IS DISTINCT FROM ?`); params.push(v); break;
            default: {
                const op = { gt: '>', gte: '>=', lt: '<', lte: '<=' }[f.op];
                // Compare in the column's own type so numbers/dates order correctly.
                const castType = isEditableType(type) ? type : 'VARCHAR';
                parts.push(`${col} ${op} TRY_CAST(? AS ${castType})`);
                params.push(v);
            }
        }
        types.push(VARCHAR);
    }
    const q = typeof search === 'string' ? search.trim() : '';
    if (q) {
        const searchable = [...columns.entries()].filter(([, t]) => !/^BLOB/i.test(t)).map(([n]) => n);
        if (searchable.length) {
            parts.push(`(${searchable.map(c => `${text(c)} ILIKE ?`).join(' OR ')})`);
            for (let i = 0; i < searchable.length; i++) { params.push(`%${escapeLike(q)}%`); types.push(VARCHAR); }
        }
    }
    return { sql: parts.length ? `WHERE ${parts.join(' AND ')}` : '', params, types };
}

function escapeLike(v) {
    // DuckDB ILIKE uses backslash as the default escape character.
    return v.replace(/[\\%_]/g, (m) => `\\${m}`);
}

// --- Catalog helpers ---------------------------------------------------------------

async function rows(conn, sql, params, types) {
    const result = params ? await conn.run(sql, params, types) : await conn.run(sql);
    const names = result.columnNames();
    return (await result.getRows()).map(r => Object.fromEntries(names.map((n, i) => [n, r[i]])));
}

async function scalar(conn, sql, params, types) {
    const result = params ? await conn.run(sql, params, types) : await conn.run(sql);
    const r = await result.getRows();
    return r.length ? r[0][0] : null;
}

async function describeColumns(conn, tableName) {
    return rows(conn,
        `SELECT column_name, data_type, is_nullable, column_default, ordinal_position
         FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ? ORDER BY ordinal_position`,
        [tableName], [VARCHAR]);
}

/** Validate a user table exists and is not a system table; returns its columns. */
async function requireTable(conn, tableName) {
    if (typeof tableName !== 'string' || !tableName || isSystemTable(tableName)) throw httpError(404, 'Table not found');
    const cols = await describeColumns(conn, tableName);
    if (cols.length === 0) throw httpError(404, `Table "${tableName}" not found`);
    return cols;
}

function columnMap(cols) {
    return new Map(cols.map(c => [c.column_name, String(c.data_type)]));
}

/** Names of registered property graphs that reference a table. */
export async function graphsReferencing(tableName) {
    const graphs = await listGraphDefinitions();
    const lower = String(tableName).toLowerCase();
    const refs = (def) => {
        const names = [];
        for (const v of def?.vertexTables || []) names.push(typeof v === 'string' ? v : v?.name);
        for (const e of def?.edgeTables || []) names.push(e?.name, e?.source, e?.destination);
        return names.filter(Boolean).map(n => String(n).toLowerCase());
    };
    return graphs.filter(g => refs(g.definition).includes(lower)).map(g => g.name);
}

async function assertNoGraphs(tableName, action) {
    const graphs = await graphsReferencing(tableName);
    if (graphs.length) {
        throw httpError(409, `Cannot ${action}: property graph(s) ${graphs.join(', ')} reference "${tableName}". Delete or redefine them first.`);
    }
}

// --- Reads -------------------------------------------------------------------------

export async function listTables() {
    return withConnection(async (conn) => {
        const cols = await rows(conn,
            `SELECT table_name, count(*) AS columns FROM information_schema.columns
             WHERE table_schema = 'main' GROUP BY table_name ORDER BY table_name`);
        const out = [];
        for (const c of cols) {
            if (isSystemTable(c.table_name)) continue;
            let rowCount = null;
            try { rowCount = Number(await scalar(conn, `SELECT count(*) FROM ${quoteIdent(c.table_name)}`)); } catch { /* unreadable */ }
            out.push({ name: c.table_name, columns: Number(c.columns), rows: rowCount });
        }
        return out;
    });
}

export async function getSchema(tableName) {
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const rowCount = Number(await scalar(conn, `SELECT count(*) FROM ${quoteIdent(tableName)}`));
        return {
            table: tableName,
            rows: rowCount,
            columns: cols.map(c => ({
                name: c.column_name,
                type: String(c.data_type),
                nullable: c.is_nullable !== 'NO',
                default: c.column_default === null || c.column_default === 'NULL' ? null : String(c.column_default),
                editable: isEditableType(c.data_type),
                widenTo: wideningTargets(c.data_type),
            })),
            graphs: await graphsReferencing(tableName),
            keys: await getTableKeys(tableName, conn),
            columnTypes: COLUMN_TYPES,
        };
    });
}

/**
 * One page of rows. Every cell is returned as text (or null) so any DuckDB
 * type renders and round-trips; long values are truncated for display and
 * flagged so the UI does not offer to edit them.
 */
export async function readRows(tableName, { page = 1, pageSize = 50, sort, filters, search } = {}) {
    const size = PAGE_SIZES.includes(Number(pageSize)) ? Number(pageSize) : 50;
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const types = columnMap(cols);
        const where = buildWhere(filters, search, types);
        const table = quoteIdent(tableName);

        const total = Number(await scalar(conn, `SELECT count(*) FROM ${table} ${where.sql}`, where.params.length ? where.params : undefined, where.types));
        const pages = Math.max(1, Math.ceil(total / size));
        const p = Math.min(Math.max(1, parseInt(page, 10) || 1), pages);

        let orderBy = 'rowid';
        if (sort && sort.column) {
            if (!types.has(sort.column)) throw httpError(400, `Unknown sort column: ${sort.column}`);
            const dir = sort.dir === 'desc' ? 'DESC' : 'ASC';
            orderBy = `${quoteIdent(sort.column)} ${dir} NULLS LAST, rowid`;
        }

        const select = cols.map((c, i) => {
            const id = quoteIdent(c.column_name);
            return /^BLOB/i.test(String(c.data_type))
                ? `CASE WHEN ${id} IS NULL THEN NULL ELSE '‹blob ' || octet_length(${id}) || ' bytes›' END AS c${i}`
                : `left(CAST(${id} AS VARCHAR), ${DISPLAY_LIMIT + 1}) AS c${i}`;
        });
        const data = await rows(conn,
            `SELECT rowid AS __rowid, ${select.join(', ')} FROM ${table} ${where.sql} ORDER BY ${orderBy} LIMIT ${size} OFFSET ${(p - 1) * size}`,
            where.params.length ? where.params : undefined, where.types);

        return {
            table: tableName,
            columns: cols.map(c => ({ name: c.column_name, type: String(c.data_type), editable: isEditableType(c.data_type) && !/^BLOB/i.test(String(c.data_type)) })),
            rows: data.map(r => {
                const values = [];
                const truncated = [];
                cols.forEach((c, i) => {
                    let v = r[`c${i}`];
                    if (v !== null && v !== undefined) {
                        v = String(v);
                        if (v.length > DISPLAY_LIMIT) { v = v.slice(0, DISPLAY_LIMIT); truncated.push(i); }
                    } else v = null;
                    values.push(v);
                });
                return { rowid: String(r.__rowid), values, truncated };
            }),
            total, page: p, pageSize: size, pages,
        };
    });
}

// --- Row writes ----------------------------------------------------------------------

function parseRowid(raw) {
    if (!/^\d{1,19}$/.test(String(raw))) throw httpError(400, 'Invalid row id');
    return BigInt(raw);
}

/**
 * Update one cell. `original` is the text value the user saw (null for NULL);
 * the update only applies if the cell still holds it.
 */
export async function updateCell(tableName, rowidRaw, { column, value, isNull, original }) {
    const rowid = parseRowid(rowidRaw);
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const types = columnMap(cols);
        if (!types.has(column)) throw httpError(400, `Unknown column: ${column}`);
        const type = types.get(column);
        if (!isEditableType(type)) throw httpError(400, `Column "${column}" (${type}) cannot be edited inline`);

        const sets = [isNull ? `${quoteIdent(column)} = NULL` : `${quoteIdent(column)} = CAST(? AS ${type})`];
        const params = isNull ? [] : [String(value ?? '')];
        const ptypes = isNull ? [] : [VARCHAR];
        // Mirror the REST PATCH endpoint: touch `updated` when the table has one.
        if (column !== 'updated' && types.has('updated') && isTimestampType(types.get('updated'))) {
            sets.push(`"updated" = CAST(CURRENT_TIMESTAMP AS ${types.get('updated')})`);
        }
        params.push(rowid, original === null || original === undefined ? null : String(original));
        ptypes.push(BIGINT, VARCHAR);

        let changed;
        try {
            changed = Number(await scalar(conn,
                `UPDATE ${quoteIdent(tableName)} SET ${sets.join(', ')}
                 WHERE rowid = ? AND CAST(${quoteIdent(column)} AS VARCHAR) IS NOT DISTINCT FROM ?`,
                params, ptypes));
        } catch (err) {
            throw httpError(400, cleanDuckDbError(err));
        }
        if (changed === 0) {
            const exists = Number(await scalar(conn, `SELECT count(*) FROM ${quoteIdent(tableName)} WHERE rowid = ?`, [rowid], [BIGINT]));
            throw httpError(409, exists ? 'This value was changed by someone else. Reload to see the latest data.' : 'This row no longer exists.');
        }
        const current = await scalar(conn,
            `SELECT left(CAST(${quoteIdent(column)} AS VARCHAR), ${DISPLAY_LIMIT}) FROM ${quoteIdent(tableName)} WHERE rowid = ?`, [rowid], [BIGINT]);
        return { value: current === null ? null : String(current) };
    });
}

/** Insert a row. `values` maps column -> { value, isNull }; omitted columns get their default. */
export async function insertRow(tableName, values) {
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const types = columnMap(cols);
        const names = [];
        const exprs = [];
        const params = [];
        const ptypes = [];
        for (const [col, entry] of Object.entries(values || {})) {
            if (!types.has(col)) throw httpError(400, `Unknown column: ${col}`);
            const type = types.get(col);
            if (!entry || entry.isNull) continue;
            const v = String(entry.value ?? '');
            if (v === '') continue;
            if (!isEditableType(type)) throw httpError(400, `Column "${col}" (${type}) cannot be set from the console`);
            names.push(quoteIdent(col));
            exprs.push(`CAST(? AS ${type})`);
            params.push(v); ptypes.push(VARCHAR);
        }
        // Fill gateway timestamp columns the way the ingestion API does.
        for (const auto of ['created', 'updated']) {
            if (types.has(auto) && isTimestampType(types.get(auto)) && !names.includes(quoteIdent(auto))) {
                names.push(quoteIdent(auto));
                exprs.push(`CAST(CURRENT_TIMESTAMP AS ${types.get(auto)})`);
            }
        }
        const sql = names.length
            ? `INSERT INTO ${quoteIdent(tableName)} (${names.join(', ')}) VALUES (${exprs.join(', ')})`
            : `INSERT INTO ${quoteIdent(tableName)} DEFAULT VALUES`;
        try {
            await conn.run(sql, params.length ? params : undefined, params.length ? ptypes : undefined);
        } catch (err) {
            throw httpError(400, cleanDuckDbError(err));
        }
        return { inserted: 1 };
    });
}

export async function deleteRows(tableName, rowidsRaw) {
    if (!Array.isArray(rowidsRaw) || rowidsRaw.length === 0) throw httpError(400, 'No rows selected');
    if (rowidsRaw.length > MAX_DELETE) throw httpError(400, `At most ${MAX_DELETE} rows can be deleted at once`);
    const ids = rowidsRaw.map(parseRowid);
    return withConnection(async (conn) => {
        await requireTable(conn, tableName);
        const deleted = Number(await scalar(conn,
            `DELETE FROM ${quoteIdent(tableName)} WHERE rowid IN (${ids.map(() => '?').join(', ')})`,
            ids, ids.map(() => BIGINT)));
        return { deleted };
    });
}

// --- Schema writes ---------------------------------------------------------------------

function requireNewName(name, kind) {
    if (typeof name !== 'string' || !SAFE_IDENTIFIER_RE.test(name)) {
        throw httpError(400, `${kind} names must start with a letter or underscore and contain only letters, digits and underscores (max 63)`);
    }
    if (kind === 'Table' && isSystemTable(name)) throw httpError(400, 'Table names starting with "_" are reserved for system tables');
    return name;
}

function requireType(raw) {
    const t = normaliseType(raw);
    if (!t) throw httpError(400, `Unsupported type: ${raw}`);
    return t;
}

function defaultClause(def, type) {
    if (def === undefined || def === null || def === '') return '';
    return ` DEFAULT CAST(${quoteLiteral(def)} AS ${type})`;
}

export function cleanDuckDbError(err) {
    return String(err?.message || err).split('\n')[0].replace(/^[A-Za-z ]+Error: /, '').slice(0, 300);
}

async function ddl(conn, sql) {
    try { await conn.run(sql); } catch (err) { throw httpError(400, cleanDuckDbError(err)); }
}

async function tableExists(conn, name) {
    return Number(await scalar(conn,
        `SELECT count(*) FROM information_schema.tables WHERE table_schema = 'main' AND lower(table_name) = lower(?)`,
        [name], [VARCHAR])) > 0;
}

/** Create a table. columns: [{ name, type, nullable, default }]. */
export async function createTable(name, columns) {
    requireNewName(name, 'Table');
    if (!Array.isArray(columns) || columns.length === 0) throw httpError(400, 'Add at least one column');
    if (columns.length > 200) throw httpError(400, 'Too many columns');
    const seen = new Set();
    const defs = columns.map((c) => {
        requireNewName(c?.name, 'Column');
        if (seen.has(c.name.toLowerCase())) throw httpError(400, `Duplicate column: ${c.name}`);
        seen.add(c.name.toLowerCase());
        const type = requireType(c.type);
        return `${quoteIdent(c.name)} ${type}${c.nullable === false ? ' NOT NULL' : ''}${defaultClause(c.default, type)}`;
    });
    return withConnection(async (conn) => {
        if (await tableExists(conn, name)) throw httpError(409, `Table "${name}" already exists`);
        await ddl(conn, `CREATE TABLE ${quoteIdent(name)} (${defs.join(', ')})`);
        return { table: name };
    });
}

export async function addColumn(tableName, { name, type: rawType, nullable, default: def }) {
    requireNewName(name, 'Column');
    const type = requireType(rawType);
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        if (cols.some(c => c.column_name.toLowerCase() === name.toLowerCase())) throw httpError(409, `Column "${name}" already exists`);
        await ddl(conn, `ALTER TABLE ${quoteIdent(tableName)} ADD COLUMN ${quoteIdent(name)} ${type}${defaultClause(def, type)}`);
        if (nullable === false) await ddl(conn, `ALTER TABLE ${quoteIdent(tableName)} ALTER COLUMN ${quoteIdent(name)} SET NOT NULL`);
        invalidateColumnCache(tableName);
        return { column: name };
    });
}

/**
 * Count non-NULL values of `column` that cannot be converted to `type`, with a
 * few distinct examples, without changing anything.
 */
async function conversionCheck(conn, tableName, column, type) {
    const t = quoteIdent(tableName);
    const c = quoteIdent(column);
    const counts = await rows(conn,
        `SELECT count(${c}) AS non_null,
                count(*) FILTER (WHERE ${c} IS NOT NULL AND TRY_CAST(${c} AS ${type}) IS NULL) AS invalid
         FROM ${t}`);
    const invalid = Number(counts[0].invalid);
    const samples = invalid === 0 ? [] : (await rows(conn,
        `SELECT DISTINCT left(CAST(${c} AS VARCHAR), 80) AS v FROM ${t}
         WHERE ${c} IS NOT NULL AND TRY_CAST(${c} AS ${type}) IS NULL LIMIT 5`)).map(r => String(r.v));
    return { nonNull: Number(counts[0].non_null), invalid, samples };
}

/** Dry run for a type change: how it would be applied and which values would fail. */
export async function previewTypeChange(tableName, column, rawType) {
    const type = requireType(rawType);
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const col = cols.find(c => c.column_name === column);
        if (!col) throw httpError(404, `Column "${column}" not found`);
        const from = String(col.data_type);
        if (type === (normaliseType(from) || from)) return { from, to: type, method: 'none', nonNull: 0, invalid: 0, samples: [] };
        const method = wideningTargets(from).includes(type) ? 'in_place' : 'convert';
        const check = method === 'in_place' ? { nonNull: null, invalid: 0, samples: [] } : await conversionCheck(conn, tableName, column, type);
        return { from, to: type, method, ...check };
    });
}

/**
 * Change a column to a type DuckLake cannot widen to in place, by rewriting it
 * inside one transaction: add a temporary column, copy the cast values, drop
 * the original and rename the copy. Row ids are preserved. The column moves to
 * the end of the table and loses its default (it may not fit the new type);
 * NOT NULL is re-applied. Strict by default: if any value cannot be converted
 * nothing changes, unless onInvalid === 'null' (such values become NULL).
 */
async function convertColumnType(conn, tableName, col, type, { onInvalid } = {}) {
    const column = col.column_name;
    await assertNoGraphs(tableName, 'convert a column');
    if (!isDateType(type) && !isEpochType(type)) {
        const rules = (await consoleStore.listPruneRules().catch(() => []))
            .filter(r => r.maxAgeValue && r.dateColumn === column && matchTables([tableName], r.tablePattern, r.excludePattern).length);
        if (rules.length) throw httpError(409, `Pruning rule(s) ${rules.map(r => `"${r.name}"`).join(', ')} prune "${tableName}" by the age of "${column}"; change them before converting it to a type that is not a date, timestamp or epoch number.`);
    }
    const lenient = onInvalid === 'null';
    if (!lenient) {
        const check = await conversionCheck(conn, tableName, column, type);
        if (check.invalid > 0) {
            throw httpError(400, `${check.invalid} value(s) in "${column}" cannot be converted to ${type} (e.g. ${check.samples.map(v => JSON.stringify(v)).join(', ')}). ` +
                'Fix them first, or choose to set them to NULL.');
        }
    }
    const t = quoteIdent(tableName);
    const c = quoteIdent(column);
    const tmp = quoteIdent(`__convert_${Date.now().toString(36)}`);
    await conn.run('BEGIN TRANSACTION');
    try {
        await conn.run(`ALTER TABLE ${t} ADD COLUMN ${tmp} ${type}`);
        await conn.run(`UPDATE ${t} SET ${tmp} = ${lenient ? 'TRY_CAST' : 'CAST'}(${c} AS ${type})`);
        await conn.run(`ALTER TABLE ${t} DROP COLUMN ${c}`);
        await conn.run(`ALTER TABLE ${t} RENAME COLUMN ${tmp} TO ${c}`);
        if (col.is_nullable === 'NO') await conn.run(`ALTER TABLE ${t} ALTER COLUMN ${c} SET NOT NULL`);
        await conn.run('COMMIT');
    } catch (err) {
        await conn.run('ROLLBACK').catch(() => {});
        throw httpError(400, cleanDuckDbError(err));
    }
}

/**
 * Alter a column. changes: { newName?, type?, nullable?, default?, onInvalid? };
 * pass default: null to drop the default. A type that is not a widening of
 * the current one is applied by convertColumnType() (onInvalid: 'null' sets
 * unconvertible values to NULL instead of refusing). Returns applied changes.
 */
export async function alterColumn(tableName, column, changes = {}) {
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        const col = cols.find(c => c.column_name === column);
        if (!col) throw httpError(404, `Column "${column}" not found`);
        const t = quoteIdent(tableName);
        let c = quoteIdent(column);
        const applied = [];
        let currentType = String(col.data_type);

        if (changes.type !== undefined && changes.type !== currentType) {
            const type = requireType(changes.type);
            if (type !== (normaliseType(currentType) || currentType)) {
                if (wideningTargets(currentType).includes(type)) {
                    // DuckLake can widen in place; the column keeps its position.
                    await ddl(conn, `ALTER TABLE ${t} ALTER COLUMN ${c} SET DATA TYPE ${type}`);
                    applied.push(`type ${currentType} -> ${type}`);
                } else {
                    await convertColumnType(conn, tableName, col, type, { onInvalid: changes.onInvalid });
                    applied.push(`converted ${currentType} -> ${type}`);
                }
                currentType = type;
            }
        }
        if (changes.default !== undefined) {
            if (changes.default === null || changes.default === '') {
                await ddl(conn, `ALTER TABLE ${t} ALTER COLUMN ${c} DROP DEFAULT`);
                applied.push('default dropped');
            } else {
                await ddl(conn, `ALTER TABLE ${t} ALTER COLUMN ${c} SET DEFAULT CAST(${quoteLiteral(changes.default)} AS ${currentType})`);
                applied.push(`default = ${changes.default}`);
            }
        }
        if (changes.nullable !== undefined && changes.nullable !== (col.is_nullable !== 'NO')) {
            await ddl(conn, `ALTER TABLE ${t} ALTER COLUMN ${c} ${changes.nullable ? 'DROP' : 'SET'} NOT NULL`);
            applied.push(changes.nullable ? 'nullable' : 'NOT NULL');
        }
        if (changes.newName !== undefined && changes.newName !== column) {
            requireNewName(changes.newName, 'Column');
            if (cols.some(x => x.column_name.toLowerCase() === changes.newName.toLowerCase() && x.column_name !== column)) {
                throw httpError(409, `Column "${changes.newName}" already exists`);
            }
            await assertNoGraphs(tableName, 'rename a column');
            await ddl(conn, `ALTER TABLE ${t} RENAME COLUMN ${c} TO ${quoteIdent(changes.newName)}`);
            applied.push(`renamed to ${changes.newName}`);
            c = quoteIdent(changes.newName);
            await renameKeyColumn(tableName, column, changes.newName);
            // Keep exact-name pruning rules pointing at the renamed column.
            await consoleStore.renameColumnInRules(tableName, column, changes.newName).catch(() => {});
        }
        invalidateColumnCache(tableName);
        return { applied };
    });
}

export async function dropColumn(tableName, column) {
    return withConnection(async (conn) => {
        const cols = await requireTable(conn, tableName);
        if (!cols.some(c => c.column_name === column)) throw httpError(404, `Column "${column}" not found`);
        if (cols.length === 1) throw httpError(400, 'Cannot drop the only column; drop the table instead');
        await assertNoGraphs(tableName, 'drop a column');
        if ((await getTableKeys(tableName, conn)).includes(column)) {
            throw httpError(409, `Cannot drop "${column}": it is a key column. Change the table's keys first.`);
        }
        await ddl(conn, `ALTER TABLE ${quoteIdent(tableName)} DROP COLUMN ${quoteIdent(column)}`);
        invalidateColumnCache(tableName);
        return { dropped: column };
    });
}

/**
 * Declare (or with [] clear) the key columns used by key-based ingestion
 * (POST /upload?mode=upsert|dedupe). Returns the keys and how many key values
 * already occur more than once.
 */
export async function setKeys(tableName, rawKeys) {
    const keys = normaliseKeyList(rawKeys ?? []);
    const cols = await withConnection(conn => requireTable(conn, tableName));
    assertKeysMatchColumns(tableName, keys, columnMap(cols));
    await setTableKeys(tableName, keys, 'console');
    return { keys, duplicateKeys: await countDuplicateKeys(tableName, keys) };
}

/** Tables keyed by table name that must follow a rename or be cleaned on drop. */
const TABLE_REFERENCES = ['_table_acls', '_rbac_table_permissions', '_table_keys'];

export async function renameTable(tableName, newName) {
    requireNewName(newName, 'Table');
    return withConnection(async (conn) => {
        await requireTable(conn, tableName);
        if (await tableExists(conn, newName)) throw httpError(409, `Table "${newName}" already exists`);
        await assertNoGraphs(tableName, 'rename this table');
        await ddl(conn, `ALTER TABLE ${quoteIdent(tableName)} RENAME TO ${quoteIdent(newName)}`);
        for (const ref of TABLE_REFERENCES) {
            await conn.run(`UPDATE ${ref} SET table_name = ? WHERE table_name = ?`, [newName, tableName], [VARCHAR, VARCHAR]).catch(() => {});
        }
        await consoleStore.renameTableInRules(tableName, newName).catch(() => {});
        invalidateColumnCache(tableName); invalidateColumnCache(newName);
        invalidateAclCache(); invalidateRbacCache();
        return { table: newName };
    });
}

export async function dropTable(tableName) {
    return withConnection(async (conn) => {
        await requireTable(conn, tableName);
        await assertNoGraphs(tableName, 'drop this table');
        await ddl(conn, `DROP TABLE ${quoteIdent(tableName)}`);
        for (const ref of TABLE_REFERENCES) {
            await conn.run(`DELETE FROM ${ref} WHERE table_name = ?`, [tableName], [VARCHAR]).catch(() => {});
        }
        invalidateColumnCache(tableName);
        invalidateAclCache(); invalidateRbacCache();
        return { dropped: tableName };
    });
}

// --- Export ----------------------------------------------------------------------------

/**
 * Stream a table to `res` as parquet, csv or ndjson, optionally limited to the
 * rows the data browser currently shows (same filters/search/sort as
 * readRows). Uses runExport() from src/export.js, so the gateway's export
 * limits (EXPORT_MAX_ROWS / EXPORT_CONCURRENCY / EXPORT_TIMEOUT_MS) and
 * truncation headers apply exactly as for GET /tables/:tableName/export.
 */
export async function exportTable(tableName, { format = 'parquet', filters, search, sort, limit } = {}, res) {
    if (!Object.hasOwn(EXPORT_FORMATS, format)) {
        throw httpError(400, `Unsupported export format "${format}". Use one of: ${Object.keys(EXPORT_FORMATS).join(', ')}`);
    }
    let rowLimit = null;
    if (limit !== undefined && limit !== null && limit !== '') {
        rowLimit = Number(limit);
        if (!Number.isInteger(rowLimit) || rowLimit < 0) throw httpError(400, 'limit must be a non-negative integer');
    }
    const cols = await withConnection(conn => requireTable(conn, tableName));
    const types = columnMap(cols);
    const where = buildWhere(filters, search, types);
    let orderBy = '';
    if (sort && sort.column) {
        if (!types.has(sort.column)) throw httpError(400, `Unknown sort column: ${sort.column}`);
        orderBy = ` ORDER BY ${quoteIdent(sort.column)} ${sort.dir === 'desc' ? 'DESC' : 'ASC'} NULLS LAST`;
    }
    // BLOBs travel base64-encoded in the text formats, like the JSON API.
    const select = cols.map(({ column_name: c, data_type: t }) =>
        format !== 'parquet' && /^BLOB/i.test(String(t)) ? `base64(${quoteIdent(c)}) AS ${quoteIdent(c)}` : quoteIdent(c)).join(', ');
    const table = quoteIdent(tableName);
    await runExport({
        format,
        selectSql: `SELECT ${select} FROM ${table} ${where.sql}${orderBy}`,
        countSql: `SELECT count(*) FROM ${table} ${where.sql}`,
        params: where.params, types: where.types,
        limit: rowLimit,
        filename: tableName,
        res,
    });
}

// --- Import ----------------------------------------------------------------------------

export const IMPORT_FORMATS = ['csv', 'parquet', 'json'];
const IMPORT_PREVIEW_ROWS = 20;
const IMPORT_PREVIEW_CHARS = 200;

const IMPORT_FORMAT_ALIASES = { ndjson: 'json', jsonl: 'json', tsv: 'csv' };

/** Format from an explicit choice, else from the file name's extension. */
export function detectImportFormat(format, filename = '') {
    if (format) {
        format = IMPORT_FORMAT_ALIASES[String(format).toLowerCase()] || String(format).toLowerCase();
        if (!IMPORT_FORMATS.includes(format)) throw httpError(400, `Unsupported import format "${format}". Use one of: ${IMPORT_FORMATS.join(', ')}`);
        return format;
    }
    const ext = String(filename).toLowerCase().split('.').pop();
    if (ext === 'csv' || ext === 'tsv' || ext === 'txt') return 'csv';
    if (ext === 'parquet' || ext === 'pq') return 'parquet';
    if (ext === 'json' || ext === 'ndjson' || ext === 'jsonl') return 'json';
    throw httpError(400, 'Could not tell the file format from its name; choose CSV, Parquet or JSON.');
}

/**
 * Table function reading an import source. `file` is a server-generated temp
 * path or an s3:// URL already checked by parseImportSource() (src/storage.js),
 * never raw user input. Remote sources may be globs over several files, which
 * are combined by column name.
 */
export function importReader(format, file, { remote = false } = {}) {
    const lit = quoteLiteral(file);
    const union = remote ? ', union_by_name = true' : '';
    if (format === 'csv') return `read_csv(${lit}, auto_detect = true${union})`;
    if (format === 'parquet') return `read_parquet(${lit}${union})`;
    return `read_json_auto(${lit}${union})`;
}

/**
 * Work out how incoming columns map onto an existing table, using the same
 * schema-evolution rules as POST /upload (src/schema-evolution.js):
 *   insert  same name; DuckDB casts the value (castFrom set when types differ)
 *   widen   STRUCT column widened to hold both shapes
 *   add     new column, added to the table (if addColumns)
 *   skip    cannot evolve, or a new column with addColumns off
 * Missing created/updated TIMESTAMP columns are filled with the import time,
 * as rows added through the API are.
 */
function planAppend(existingCols, incoming, addColumns) {
    const existing = new Map(existingCols.map(c => [c.column_name.toLowerCase(), c]));
    const columns = incoming.map(({ name, type }) => {
        const match = existing.get(name.toLowerCase());
        if (!match) return addColumns ? { name, type, action: 'add' } : { name, type, action: 'skip', reason: 'new column (adding columns is off)' };
        const target = String(match.data_type);
        const r = resolveTypeConflict(target, type);
        if (r.action === 'skip') return { name: match.column_name, type, action: 'skip', reason: r.reason };
        if (r.action === 'widen') return { name: match.column_name, type, action: 'widen', newType: r.newType, from: target };
        return { name: match.column_name, type, action: 'insert', castFrom: target.toUpperCase() === type.toUpperCase() ? null : target };
    });
    const incomingNames = new Set(incoming.map(c => c.name.toLowerCase()));
    const missing = existingCols.filter(c => !incomingNames.has(c.column_name.toLowerCase()));
    const autoFill = missing.filter(c => ['created', 'updated'].includes(c.column_name) && isTimestampType(c.data_type))
        .map(c => ({ name: c.column_name, type: String(c.data_type) }));
    return { columns, missing: missing.map(c => c.column_name).filter(n => !autoFill.some(a => a.name === n)), autoFill };
}

export const IMPORT_MODES = ['create', 'append', 'replace', 'auto'];

/**
 * Import a staged file or an object-storage source (CSV, Parquet or JSON /
 * NDJSON) on the primary instance. Shared by the console import and
 * POST /tables/:tableName/import.
 *   create   new table (409 if it exists)
 *   append   add rows to an existing table, with /upload's schema evolution
 *   replace  swap an existing table's rows for the file's in one transaction
 *            (schema evolves as for append; the table keeps its ACL and
 *            partitioning); creates the table when it does not exist
 *   auto     create when missing, else append
 * With dryRun nothing is written; the result describes the detected columns,
 * row count, a preview and, for append/replace, the per-column plan. The
 * result has created: true when this call created the table.
 * `prepare(conn)` runs first on the connection (e.g. to load httpfs and the
 * import secret for s3:// sources).
 */
export async function importFile(file, { format, table, mode = 'create', dryRun = false, addColumns = true, remote = false, prepare = null } = {}) {
    if (!IMPORT_MODES.includes(mode)) throw httpError(400, `mode must be one of: ${IMPORT_MODES.join(', ')}`);
    const reader = importReader(format, file, { remote });
    return withConnection(async (conn) => {
        if (prepare) await prepare(conn);
        let incoming;
        try {
            incoming = (await rows(conn, `DESCRIBE SELECT * FROM ${reader}`)).map(r => ({ name: String(r.column_name), type: String(r.column_type) }));
        } catch (err) {
            throw httpError(400, `Could not read the ${remote ? 'source' : 'file'} as ${format.toUpperCase()}: ${cleanDuckDbError(err)}`);
        }
        if (incoming.length === 0) throw httpError(400, 'The file has no columns');

        if (mode === 'auto' || mode === 'replace') {
            const exists = await tableExists(conn, table);
            if (!exists) mode = 'create';
            else if (mode === 'auto') mode = 'append';
        }
        let plan = null;
        if (mode === 'create') {
            requireNewName(table, 'Table');
            if (await tableExists(conn, table)) throw httpError(409, `Table "${table}" already exists; choose "append" to add rows to it`);
        } else {
            plan = planAppend(await requireTable(conn, table), incoming, addColumns);
            if (!plan.columns.some(c => c.action !== 'skip')) throw httpError(400, 'None of the file\'s columns can be imported into this table');
        }

        const totalRows = Number(await scalar(conn, `SELECT count(*) FROM ${reader}`));

        if (dryRun) {
            const sample = await rows(conn,
                `SELECT ${incoming.map((c, i) => `left(CAST(${quoteIdent(c.name)} AS VARCHAR), ${IMPORT_PREVIEW_CHARS}) AS c${i}`).join(', ')} FROM ${reader} LIMIT ${IMPORT_PREVIEW_ROWS}`);
            return {
                dryRun: true, format, mode, table, totalRows, columns: incoming,
                preview: sample.map(r => incoming.map((_, i) => (r[`c${i}`] === null || r[`c${i}`] === undefined ? null : String(r[`c${i}`])))),
                plan,
            };
        }

        const t = quoteIdent(table);
        if (mode === 'create') {
            try {
                await conn.run(`CREATE TABLE ${t} AS SELECT * FROM ${reader}`);
            } catch (err) {
                // Lost a race with a concurrent create of the same name.
                if (/already exists/i.test(err?.message || '')) throw httpError(409, `Table "${table}" already exists; choose "append" to add rows to it`);
                throw httpError(400, cleanDuckDbError(err));
            }
            invalidateColumnCache(table);
            return { table, mode, format, inserted: totalRows, columns: incoming.length, created: true };
        }

        const applied = { added: [], widened: [] };
        for (const c of plan.columns) {
            if (c.action === 'add') {
                await ddl(conn, `ALTER TABLE ${t} ADD COLUMN ${quoteIdent(c.name)} ${c.type}`);
                applied.added.push(c.name);
            } else if (c.action === 'widen') {
                await ddl(conn, `ALTER TABLE ${t} ALTER COLUMN ${quoteIdent(c.name)} SET DATA TYPE ${c.newType}`);
                applied.widened.push(c.name);
            }
        }
        if (applied.added.length || applied.widened.length) invalidateColumnCache(table);
        const select = plan.columns.filter(c => c.action !== 'skip').map(c => quoteIdent(c.name))
            .concat(plan.autoFill.map(c => `CAST(CURRENT_TIMESTAMP AS ${c.type}) AS ${quoteIdent(c.name)}`));
        let deleted = 0;
        try {
            if (mode === 'replace') {
                await conn.run('BEGIN TRANSACTION');
                deleted = Number(await scalar(conn, `SELECT count(*) FROM ${t}`));
                await conn.run(`DELETE FROM ${t}`);
            }
            await conn.run(`INSERT INTO ${t} BY NAME SELECT ${select.join(', ')} FROM ${reader}`);
            if (mode === 'replace') await conn.run('COMMIT');
        } catch (err) {
            if (mode === 'replace') await conn.run('ROLLBACK').catch(() => {});
            throw httpError(isTransactionConflict(err) ? 409 : 400, `Import failed, no rows were ${mode === 'replace' ? 'changed' : 'added'}: ${cleanDuckDbError(err)}` +
                (applied.added.length ? ` (column(s) ${applied.added.join(', ')} were already added)` : ''));
        }
        return {
            table, mode, format, inserted: totalRows, ...(mode === 'replace' ? { deleted } : {}), ...applied,
            skipped: plan.columns.filter(c => c.action === 'skip').map(c => c.name),
        };
    });
}
