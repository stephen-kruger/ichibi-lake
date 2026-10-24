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
import { RETENTION } from './store.js';

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

const DATE_TYPE_RE = /^(DATE|TIMESTAMP)/i;

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
    if (!DATE_TYPE_RE.test(type)) {
        const rule = await rows(conn, `SELECT 1 FROM ${RETENTION} WHERE table_name = ? AND column_name = ?`,
            [tableName, column], [VARCHAR, VARCHAR]).catch(() => []);
        if (rule.length) throw httpError(409, `A retention rule prunes "${tableName}" by "${column}"; remove the rule before converting it to a non-date type.`);
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
            // Keep retention rules pointing at the renamed column.
            await conn.run(`UPDATE ${RETENTION} SET column_name = ? WHERE table_name = ? AND column_name = ?`,
                [changes.newName, tableName, column], [VARCHAR, VARCHAR, VARCHAR]).catch(() => {});
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
        await ddl(conn, `ALTER TABLE ${quoteIdent(tableName)} DROP COLUMN ${quoteIdent(column)}`);
        await conn.run(`DELETE FROM ${RETENTION} WHERE table_name = ? AND column_name = ?`,
            [tableName, column], [VARCHAR, VARCHAR]).catch(() => {});
        invalidateColumnCache(tableName);
        return { dropped: column };
    });
}

/** Tables keyed by table name that must follow a rename or be cleaned on drop. */
const TABLE_REFERENCES = ['_table_acls', '_rbac_table_permissions', RETENTION];

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
