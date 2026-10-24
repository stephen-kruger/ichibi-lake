/**
 * Data pruning ("retention") engine.
 *
 * A rule selects tables with wildcard patterns (matched at run time, so new
 * tables are picked up automatically) and deletes rows that violate EITHER
 * criterion, like Kafka's time- and size-based retention:
 *   - age:   rows whose date/timestamp column is older than N hours/days
 *   - count: rows beyond the newest N, ordered by a column (or insertion order)
 *
 * Each rule has its own cron schedule (UTC). Every gateway instance runs the
 * scheduler; a due rule is claimed with a token-and-verify step so one
 * instance runs it (see store.claimRule). Jobs run in the background and are
 * recorded in the run log (_console_prune_runs) with per-table status.
 *
 * Safety: system tables ("_"-prefixed) never match; a rule may set a safety
 * limit that refuses to delete more than N rows from one table in one run;
 * a preview (dry run) shows what would be deleted. When filesystem BLOB
 * storage is enabled, files referenced by deleted rows are removed too.
 */
import fs from 'fs/promises';
import os from 'os';
import path from 'path';
import { randomBytes } from 'crypto';
import { execute, withConnection, VARCHAR, reclaimTableStorage, snapshotRetentionDays } from '../db.js';
import * as store from './store.js';
import { parseCron, nextRun, validateCron } from './cron.js';

const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const DATE_TYPES_RE = /^(DATE|TIMESTAMP(\s*WITH TIME ZONE)?|TIMESTAMP_(S|MS|NS)|TIMESTAMPTZ|DATETIME)$/i;
// Numeric columns holding Unix epoch times (e.g. BIGINT milliseconds).
const EPOCH_TYPES_RE = /^(TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|USMALLINT|UINTEGER|UBIGINT|UHUGEINT|FLOAT|DOUBLE|REAL|DECIMAL\(\d+,\s*\d+\))$/i;
export const EPOCH_UNITS = ['auto', 's', 'ms', 'us', 'ns'];
const EPOCH_UNIT_LABELS = { s: 'seconds', ms: 'milliseconds', us: 'microseconds', ns: 'nanoseconds' };

export function isDateType(type) { return DATE_TYPES_RE.test(String(type).trim()); }
export function isEpochType(type) { return EPOCH_TYPES_RE.test(String(type).trim()); }

/**
 * Infer the unit of a Unix epoch value from its magnitude. Any date from
 * 1973 to 5138 falls in exactly one band (e.g. now is ~1.7e9 s, 1.7e12 ms).
 */
export function detectEpochUnit(maxAbs) {
    const v = Math.abs(Number(maxAbs));
    if (!Number.isFinite(v)) return null;
    if (v < 1e11) return 's';
    if (v < 1e14) return 'ms';
    if (v < 1e17) return 'us';
    return 'ns';
}

export function epochUnitLabel(unit) { return EPOCH_UNIT_LABELS[unit] || unit; }

// --- Match conditions ---------------------------------------------------------------
// A rule may delete rows matching simple conditions. These are structured
// (column + operator + value), never SQL text: the server builds the SQL,
// columns are checked against each table and quoted, and values are always
// escaped string literals cast to the column's type.
export const CONDITION_OPS = {
    equals: 'equals', not_equals: 'does not equal', contains: 'contains', not_contains: 'does not contain',
    starts_with: 'starts with', ends_with: 'ends with', gt: '>', gte: '≥', lt: '<', lte: '≤',
    in: 'is one of', is_null: 'is NULL', not_null: 'is not NULL',
};
const NO_VALUE_CONDITION_OPS = new Set(['is_null', 'not_null']);
const MAX_CONDITIONS = 10;
const MAX_IN_VALUES = 50;
// Scalar types a literal can be cast to for typed comparisons.
const CASTABLE_TYPE_RE = /^(BOOLEAN|TINYINT|SMALLINT|INTEGER|BIGINT|HUGEINT|USMALLINT|UTINYINT|UINTEGER|UBIGINT|UHUGEINT|FLOAT|REAL|DOUBLE|DECIMAL\(\d+,\s*\d+\)|DATE|TIME|TIMESTAMP|TIMESTAMP WITH TIME ZONE|TIMESTAMPTZ|TIMESTAMP_S|TIMESTAMP_MS|TIMESTAMP_NS|UUID|INTERVAL)$/i;

const sqlLiteral = (value) => `'${String(value).replace(/'/g, "''")}'`;
const likeEscape = (value) => String(value).replace(/[\\%_]/g, (m) => `\\${m}`);

/** Validate conditions from API input. Returns null when there are none. */
export function normaliseConditions(input) {
    if (input === null || input === undefined) return null;
    const items = Array.isArray(input.items) ? input.items : [];
    if (!items.length) return null;
    if (items.length > MAX_CONDITIONS) throw httpError(400, `At most ${MAX_CONDITIONS} conditions per rule`);
    const out = items.map((c, i) => {
        const column = optionalColumn(c?.column, `column in condition ${i + 1}`);
        if (!column) throw httpError(400, `Condition ${i + 1} needs a column`);
        if (!Object.hasOwn(CONDITION_OPS, c.op)) throw httpError(400, `Condition ${i + 1} has an unknown operator`);
        let value = NO_VALUE_CONDITION_OPS.has(c.op) ? '' : String(c.value ?? '');
        if (value.length > 500 || /[\u0000-\u0008\u000b\u000c\u000e-\u001f]/.test(value)) throw httpError(400, `Condition ${i + 1} has an invalid value`);
        if (c.op === 'in') {
            const list = value.split(',').map(v => v.trim()).filter(Boolean);
            if (!list.length || list.length > MAX_IN_VALUES) throw httpError(400, `Condition ${i + 1}: list 1 to ${MAX_IN_VALUES} comma-separated values`);
            value = list.join(', ');
        }
        return { column, op: c.op, value };
    });
    return { match: input.match === 'any' ? 'any' : 'all', items: out };
}

/**
 * SQL predicate for a rule's conditions on one table, or { skip } when a
 * column is missing. `types` maps column name -> DuckDB type.
 */
export function buildConditionPredicate(conditions, types) {
    const parts = [];
    for (const c of conditions.items) {
        const type = String(types.get(c.column) ?? '').trim();
        if (!type) return { skip: `no "${c.column}" column (used in a match condition)` };
        const col = quoteIdent(c.column);
        const text = `CAST(${col} AS VARCHAR)`;
        const typed = CASTABLE_TYPE_RE.test(type);
        const lit = (v) => (typed ? `TRY_CAST(${sqlLiteral(v)} AS ${type})` : sqlLiteral(v));
        const target = typed ? col : text;
        switch (c.op) {
            case 'is_null': parts.push(`${col} IS NULL`); break;
            case 'not_null': parts.push(`${col} IS NOT NULL`); break;
            case 'equals': parts.push(`${target} = ${lit(c.value)}`); break;
            case 'not_equals': parts.push(`${target} IS DISTINCT FROM ${lit(c.value)}`); break;
            case 'gt': parts.push(`${target} > ${lit(c.value)}`); break;
            case 'gte': parts.push(`${target} >= ${lit(c.value)}`); break;
            case 'lt': parts.push(`${target} < ${lit(c.value)}`); break;
            case 'lte': parts.push(`${target} <= ${lit(c.value)}`); break;
            case 'in': parts.push(`${target} IN (${c.value.split(',').map(v => lit(v.trim())).join(', ')})`); break;
            case 'contains': parts.push(`${text} ILIKE ${sqlLiteral(`%${likeEscape(c.value)}%`)} ESCAPE '\\'`); break;
            case 'not_contains': parts.push(`(${text} IS NULL OR ${text} NOT ILIKE ${sqlLiteral(`%${likeEscape(c.value)}%`)} ESCAPE '\\')`); break;
            case 'starts_with': parts.push(`${text} ILIKE ${sqlLiteral(`${likeEscape(c.value)}%`)} ESCAPE '\\'`); break;
            case 'ends_with': parts.push(`${text} ILIKE ${sqlLiteral(`%${likeEscape(c.value)}`)} ESCAPE '\\'`); break;
            default: throw new Error(`Unknown operator ${c.op}`);
        }
    }
    // COALESCE: a comparison that cannot be evaluated (NULL) never deletes.
    return { sql: `COALESCE(${parts.map(p => `(${p})`).join(conditions.match === 'any' ? ' OR ' : ' AND ')}, false)` };
}

export function describeConditions(conditions) {
    if (!conditions) return '';
    return conditions.items.map(c => `${c.column} ${CONDITION_OPS[c.op]}${NO_VALUE_CONDITION_OPS.has(c.op) ? '' : ` "${c.value}"`}`)
        .join(conditions.match === 'any' ? ' or ' : ' and ');
}
export const MAX_AGE = { hours: 24 * 36500, days: 36500 };
const TICK_MS = 20 * 1000;
const STALE_RUN_MS = 6 * 3600 * 1000;
const RUN_HISTORY_MS = 90 * 24 * 3600 * 1000;

export const INSTANCE_ID = `${os.hostname()}:${process.pid}:${randomBytes(3).toString('hex')}`;

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

const quoteIdent = (name) => `"${String(name).replace(/"/g, '""')}"`;

/**
 * Tables pruning must never touch. Every gateway system table in the main
 * catalog starts with "_" (RBAC users/roles/permissions with API keys, table
 * ACLs, graph registry, migration markers); the explicit list is defence in
 * depth in case one is ever created without the prefix. Console logins
 * (password hashes), settings, rules and the audit log live in a separate
 * catalog (ichibi_console) that this engine never lists, and
 * memory-catalog tables are excluded because only catalog 'ducklake' is read.
 */
export const PROTECTED_TABLES = new Set([
    '_rbac_users', '_rbac_user_roles', '_rbac_table_permissions', '_table_acls', '_graph_registry', '_gateway_meta',
    '_console_users', '_console_settings', '_console_audit', '_console_prune_rules', '_console_prune_runs', '_console_retention',
]);

export function isSystemTable(name) {
    const n = String(name);
    return n.startsWith('_') || PROTECTED_TABLES.has(n.toLowerCase());
}

function assertPrunable(tableName) {
    if (isSystemTable(tableName) || !SAFE_IDENTIFIER_RE.test(String(tableName))) {
        throw new Error(`Refusing to prune protected or invalid table "${tableName}"`);
    }
}

// --- Table selectors ---------------------------------------------------------------

/** Split "logs_*, events" into trimmed pattern items. */
export function splitPatterns(text) {
    return String(text || '').split(',').map(s => s.trim()).filter(Boolean);
}

function globToRegex(glob) {
    const body = glob.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.');
    return new RegExp(`^${body}$`, 'i');
}

/**
 * User tables matched by the include patterns and not by the exclude
 * patterns. System tables never match, even with "*".
 */
export function matchTables(tableNames, includeText, excludeText = '') {
    const include = splitPatterns(includeText).map(globToRegex);
    const exclude = splitPatterns(excludeText).map(globToRegex);
    return tableNames
        .filter(t => !isSystemTable(t))
        .filter(t => include.some(r => r.test(t)) && !exclude.some(r => r.test(t)))
        .sort((a, b) => a.localeCompare(b));
}

// --- Rule validation -----------------------------------------------------------------

function optionalPositiveInt(value, label, max) {
    if (value === null || value === undefined || value === '') return null;
    const n = Number(value);
    if (!Number.isInteger(n) || n < 1 || n > max) throw httpError(400, `${label} must be a whole number between 1 and ${max.toLocaleString()}`);
    return n;
}

function optionalColumn(value, label) {
    if (value === null || value === undefined || String(value).trim() === '') return null;
    const v = String(value).trim();
    if (v.length > 128 || /[\u0000-\u001f]/.test(v)) throw httpError(400, `Invalid ${label}`);
    return v;
}

/** Validate and normalise a rule from API input. Throws a 400 error when invalid. */
export function normaliseRule(input = {}) {
    const name = String(input.name || '').trim();
    if (!name || name.length > 100) throw httpError(400, 'A rule name (up to 100 characters) is required');

    // One table per rule, so the rule's columns always fit that table's
    // schema (tables can name/type their date columns differently). Use
    // "Clone" in the console to apply similar rules to other tables.
    const table = String(input.table ?? input.tablePattern ?? '').trim();
    if (!table) throw httpError(400, 'Choose the table this rule prunes');
    if (!SAFE_IDENTIFIER_RE.test(table)) {
        throw httpError(400, `Invalid table "${table}": a rule prunes exactly one table (letters, digits and _; no wildcards or lists). Clone the rule for other tables.`);
    }
    if (isSystemTable(table)) throw httpError(400, `"${table}" is a system table and cannot be pruned`);

    const maxAgeUnit = input.maxAgeUnit === 'hours' ? 'hours' : 'days';
    const epochUnit = input.epochUnit === undefined || input.epochUnit === null || input.epochUnit === '' ? 'auto' : String(input.epochUnit);
    if (!EPOCH_UNITS.includes(epochUnit)) throw httpError(400, `Epoch unit must be one of: ${EPOCH_UNITS.join(', ')}`);
    const maxAgeValue = optionalPositiveInt(input.maxAgeValue, `Maximum age (${maxAgeUnit})`, MAX_AGE[maxAgeUnit]);
    const maxRows = optionalPositiveInt(input.maxRows, 'Rows to keep', 1e12);
    const dateColumn = optionalColumn(input.dateColumn, 'date column');
    const orderColumn = optionalColumn(input.orderColumn, 'order column');
    const conditions = normaliseConditions(input.conditions);
    if (maxAgeValue === null && maxRows === null && !conditions) throw httpError(400, 'Choose at least one criterion: a maximum age, a number of rows to keep, or match conditions');
    if (maxAgeValue !== null && !dateColumn) throw httpError(400, 'Age-based pruning needs the date/timestamp column to compare');

    const schedule = String(input.schedule || '').trim();
    const cronError = validateCron(schedule);
    if (cronError) throw httpError(400, `Invalid schedule: ${cronError}`);

    return {
        name,
        tablePattern: table,
        excludePattern: '',
        dateColumn: maxAgeValue !== null ? dateColumn : null,
        maxAgeValue, maxAgeUnit,
        epochUnit: maxAgeValue !== null ? epochUnit : 'auto',
        maxRows,
        orderColumn: maxRows !== null ? orderColumn : null,
        conditions,
        schedule,
        enabled: input.enabled !== false,
        safetyLimit: optionalPositiveInt(input.safetyLimit, 'Safety limit', 1e12),
        reclaimStorage: !!input.reclaimStorage,
    };
}

export function computeNextRun(schedule, from = new Date()) {
    const next = nextRun(parseCron(schedule), from);
    return next ? next.getTime() : null;
}

// --- SQL for one table -------------------------------------------------------------

/** SQL for the cutoff instant expressed as a Unix epoch number in `unit`. */
export function epochCutoff(unit, cutoffTimestampSql) {
    switch (unit) {
        case 's': return `epoch(${cutoffTimestampSql})`;
        case 'ms': return `epoch_ms(${cutoffTimestampSql})`;
        case 'us': return `epoch_us(${cutoffTimestampSql})`;
        case 'ns': return `epoch_ns(${cutoffTimestampSql})`;
        default: throw new Error(`Unknown epoch unit "${unit}"`);
    }
}

/**
 * Predicates selecting rows to delete from one table. `types` maps column
 * name -> DuckDB type for the table. For a numeric (epoch) date column,
 * `epochUnit` is the resolved unit ('s', 'ms', 'us' or 'ns'). Returns
 * { age, count, skip } where skip explains why the rule cannot apply.
 */
export function buildPredicates(rule, tableName, types, epochUnit = null) {
    let age = null;
    let count = null;
    if (rule.maxAgeValue) {
        const type = String(types.get(rule.dateColumn) ?? '').trim();
        if (!type) return { skip: `no "${rule.dateColumn}" column` };
        const n = Number(rule.maxAgeValue);
        const unit = rule.maxAgeUnit === 'hours' ? 'HOUR' : 'DAY';
        const cutoff = `CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL ${n} ${unit}`;
        const col = quoteIdent(rule.dateColumn);
        if (/^DATE$/i.test(type)) age = `${col} < CAST(${cutoff} AS DATE)`;
        else if (isDateType(type)) age = `${col} < ${cutoff}`;
        else if (isEpochType(type)) {
            if (!epochUnit || epochUnit === 'auto') return { skip: `"${rule.dateColumn}" is ${type}; its epoch unit is unknown` };
            age = `${col} < ${epochCutoff(epochUnit, cutoff)}`;
        } else return { skip: `"${rule.dateColumn}" is ${type}, not a date, timestamp or epoch number` };
    }
    if (rule.maxRows) {
        let order = 'rowid DESC';
        if (rule.orderColumn) {
            if (!types.has(rule.orderColumn)) return { skip: `no "${rule.orderColumn}" column to order by` };
            order = `${quoteIdent(rule.orderColumn)} DESC NULLS LAST, rowid DESC`;
        }
        count = `rowid IN (SELECT rowid FROM ${quoteIdent(tableName)} ORDER BY ${order} OFFSET ${Number(rule.maxRows)})`;
    }
    let match = null;
    if (rule.conditions && rule.conditions.items?.length) {
        const m = buildConditionPredicate(rule.conditions, types);
        if (m.skip) return { skip: m.skip };
        match = m.sql;
    }
    return { age, count, match, skip: null };
}

function wherePredicate({ age, count, match }) {
    return [age, count, match].filter(Boolean).map(p => `(${p})`).join(' OR ');
}

async function columnTypes(conn, tableName) {
    const result = await conn.run(
        `SELECT column_name, data_type FROM information_schema.columns
         WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND table_name = ?`,
        [tableName], [VARCHAR]);
    return new Map((await result.getRows()).map(r => [r[0], String(r[1])]));
}

async function firstRow(conn, sql) {
    const result = await conn.run(sql);
    return (await result.getRows())[0] || [];
}

async function userTableNames() {
    const rows = await execute(
        `SELECT table_name FROM information_schema.tables
         WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND table_type = 'BASE TABLE'`);
    return rows.map(r => r.table_name).filter(t => !isSystemTable(t) && SAFE_IDENTIFIER_RE.test(t));
}

/** Count what a rule would delete from one table (no changes). */
/**
 * For a numeric date column, the epoch unit to use: the rule's explicit
 * unit, or one detected from the largest absolute value. Returns
 * { unit, detected } or { unit: null } when the column has no values.
 */
async function resolveEpochUnit(conn, rule, tableName, types) {
    if (!rule.maxAgeValue || !isEpochType(types.get(rule.dateColumn) ?? '')) return { unit: null };
    if (rule.epochUnit && rule.epochUnit !== 'auto') return { unit: rule.epochUnit, detected: false };
    const [maxAbs] = await firstRow(conn, `SELECT max(abs(${quoteIdent(rule.dateColumn)})) FROM ${quoteIdent(tableName)}`);
    if (maxAbs === null || maxAbs === undefined) return { unit: null, empty: true };
    return { unit: detectEpochUnit(maxAbs), detected: true };
}

function describeDateColumn(rule, types, epoch) {
    if (!rule.maxAgeValue) return null;
    const type = String(types.get(rule.dateColumn) ?? '');
    if (!epoch.unit) return epoch.empty ? `${rule.dateColumn} (${type}) has no values` : `${rule.dateColumn} (${type})`;
    return `${rule.dateColumn} as epoch ${epochUnitLabel(epoch.unit)}${epoch.detected ? ' (detected)' : ''}`;
}

async function evaluateTable(conn, rule, tableName) {
    assertPrunable(tableName);
    const types = await columnTypes(conn, tableName);
    if (!types.size) return { table: tableName, status: 'skipped', reason: 'table no longer exists' };
    const epoch = await resolveEpochUnit(conn, rule, tableName, types);
    // An epoch column with no values: nothing can be old enough to delete,
    // but a row-count criterion may still apply.
    const effective = epoch.empty ? { ...rule, maxAgeValue: null } : rule;
    if (epoch.empty && !rule.maxRows && !rule.conditions) {
        const [rows] = await firstRow(conn, `SELECT count(*) FROM ${quoteIdent(tableName)}`);
        return { table: tableName, status: 'ok', rows: Number(rows), byAge: 0, byCount: 0, byMatch: 0, total: 0, types, dateInfo: describeDateColumn(rule, types, epoch), preds: null };
    }
    const preds = buildPredicates(effective, tableName, types, epoch.unit);
    if (preds.skip) return { table: tableName, status: 'skipped', reason: preds.skip };
    const t = quoteIdent(tableName);
    const [rows, byAge, byCount, byMatch, total] = await firstRow(conn,
        `SELECT count(*),
                ${preds.age ? `count(*) FILTER (WHERE ${preds.age})` : '0'},
                ${preds.count ? `count(*) FILTER (WHERE ${preds.count})` : '0'},
                ${preds.match ? `count(*) FILTER (WHERE ${preds.match})` : '0'},
                count(*) FILTER (WHERE ${wherePredicate(preds)})
         FROM ${t}`);
    return { table: tableName, status: 'ok', preds, rows: Number(rows), byAge: Number(byAge), byCount: Number(byCount), byMatch: Number(byMatch), total: Number(total), types,
        dateInfo: describeDateColumn(rule, types, epoch) };
}

/** Resolve a stored blob path and return it only if it lies inside blobsRoot. */
export function containedBlobPath(storedPath, blobsRoot) {
    if (typeof storedPath !== 'string' || !storedPath) return null;
    const root = path.resolve(blobsRoot);
    const resolved = path.resolve(storedPath);
    return resolved.startsWith(root + path.sep) ? resolved : null;
}

async function deleteRows(conn, tableName, evaluation) {
    assertPrunable(tableName); // re-checked at the point of deletion
    const t = quoteIdent(tableName);
    const where = wherePredicate(evaluation.preds);
    const blobFiles = new Set();
    if (process.env.USE_FILESYSTEM_BLOBS === '1') {
        const root = path.resolve(process.env.BLOBS_PATH || './data/blobs');
        for (const [col, type] of evaluation.types) {
            if (!/^VARCHAR/i.test(type)) continue;
            const result = await conn.run(`SELECT ${quoteIdent(col)} FROM ${t} WHERE (${where}) AND ${quoteIdent(col)} IS NOT NULL`);
            for (const [value] of await result.getRows()) {
                const p = containedBlobPath(value, root);
                if (p) blobFiles.add(p);
            }
        }
    }
    const [deleted] = await firstRow(conn, `DELETE FROM ${t} WHERE ${where}`);
    let removed = 0;
    for (const file of blobFiles) {
        try { await fs.unlink(file); removed++; } catch (err) {
            if (err.code !== 'ENOENT') console.warn(`[Prune] Could not remove blob file ${file}:`, err.message);
        }
    }
    return { deleted: Number(deleted || 0), blobFiles: removed };
}

// --- Preview -----------------------------------------------------------------------

/** True for rules saved before the one-table-per-rule change (patterns or lists). */
export function isLegacyMultiTable(rule) {
    return !!rule.excludePattern || !SAFE_IDENTIFIER_RE.test(String(rule.tablePattern || ''));
}

/** Dry run: which tables match and how many rows each would lose. */
export async function previewRule(input, { validated = false } = {}) {
    const rule = validated ? input : normaliseRule({ ...input, name: input.name || 'preview', schedule: input.schedule || '@daily' });
    const tables = matchTables(await userTableNames(), rule.tablePattern, rule.excludePattern);
    return withConnection(async (conn) => {
        const results = [];
        for (const table of tables) {
            const e = await evaluateTable(conn, rule, table);
            results.push({
                table, status: e.status, reason: e.reason || null, rows: e.rows ?? null,
                byAge: e.byAge ?? 0, byCount: e.byCount ?? 0, byMatch: e.byMatch ?? 0, total: e.total ?? 0, dateInfo: e.dateInfo || null,
                overLimit: !!(rule.safetyLimit && e.total > rule.safetyLimit),
            });
        }
        return { tables: results, totalRows: results.reduce((n, r) => n + r.total, 0) };
    });
}

// --- Jobs ----------------------------------------------------------------------------

const runningRules = new Set();

/**
 * Start a job for a rule in the background. Returns the job id immediately;
 * progress and results are written to the run log.
 */
export async function startJob(ruleId, { trigger = 'manual', username = null } = {}) {
    const rule = await store.getPruneRule(ruleId);
    if (!rule) throw httpError(404, 'Rule not found');
    if (runningRules.has(ruleId)) throw httpError(409, 'This rule is already running on this gateway');
    const jobId = randomBytes(8).toString('hex');
    runningRules.add(ruleId);
    await store.insertRun({ runId: jobId, jobId, kind: 'job', ruleId, ruleName: rule.name, trigger, status: 'running',
        startedMs: Date.now(), instance: INSTANCE_ID, username });
    runJob(jobId, rule, { trigger, username })
        .catch(err => console.error(`[Prune] Job ${jobId} crashed:`, err))
        .finally(() => runningRules.delete(ruleId));
    return jobId;
}

async function runJob(jobId, rule, { trigger, username }) {
    let deletedTotal = 0;
    let failures = 0;
    let processed = 0;
    let skipped = 0;
    const affected = [];
    let message = '';
    try {
        const tables = matchTables(await userTableNames(), rule.tablePattern, rule.excludePattern);
        if (!tables.length) message = 'No tables currently match this rule.';
        for (const table of tables) {
            const runId = randomBytes(8).toString('hex');
            await store.insertRun({ runId, jobId, kind: 'table', ruleId: rule.id, ruleName: rule.name, tableName: table,
                trigger, status: 'running', startedMs: Date.now(), instance: INSTANCE_ID, username });
            try {
                const result = await withConnection(async (conn) => {
                    const e = await evaluateTable(conn, rule, table);
                    if (e.status === 'skipped') return { status: 'skipped', message: e.reason };
                    if (rule.safetyLimit && e.total > rule.safetyLimit) {
                        return { status: 'failed', rowsByAge: e.byAge, rowsByCount: e.byCount, rowsByMatch: e.byMatch,
                            message: `Would delete ${e.total.toLocaleString()} rows, above the safety limit of ${rule.safetyLimit.toLocaleString()}; nothing was deleted.` };
                    }
                    const via = e.dateInfo ? ` Age by ${e.dateInfo}.` : '';
                    if (e.total === 0) return { status: 'succeeded', rowsByAge: 0, rowsByCount: 0, rowsByMatch: 0, rowsDeleted: 0, message: `Nothing to delete.${via}` };
                    const d = await deleteRows(conn, table, e);
                    return { status: 'succeeded', rowsByAge: e.byAge, rowsByCount: e.byCount, rowsByMatch: e.byMatch, rowsDeleted: d.deleted, blobFiles: d.blobFiles,
                        message: `Deleted ${d.deleted.toLocaleString()} of ${e.rows.toLocaleString()} rows.${via}` };
                });
                await store.finishRun(runId, result);
                if (result.status === 'skipped') skipped++;
                else if (result.status === 'failed') failures++;
                else { processed++; deletedTotal += result.rowsDeleted || 0; if (result.rowsDeleted) affected.push(table); }
            } catch (err) {
                failures++;
                await store.finishRun(runId, { status: 'failed', message: String(err.message || err).split('\n')[0] });
            }
        }
        if (rule.reclaimStorage && affected.length) {
            try { message = `Storage: ${await reclaimTableStorage(affected)}.`; }
            catch (err) { failures++; message = `Storage reclaim failed: ${String(err.message || err).split('\n')[0]}`; }
        }
    } catch (err) {
        failures++;
        message = String(err.message || err).split('\n')[0];
    }
    const status = failures ? (processed ? 'partial' : 'failed') : 'succeeded';
    const summary = `${processed} table(s) pruned, ${skipped} skipped, ${failures} failed; ${deletedTotal.toLocaleString()} row(s) deleted.` + (message ? ` ${message}` : '');
    await store.finishRun(jobId, { status, rowsDeleted: deletedTotal, message: summary });
    await store.recordRuleResult(rule.id, status, deletedTotal);
    if (deletedTotal > 0 || trigger === 'manual' || failures) {
        await store.audit(username || 'scheduler', 'prune.job', `${rule.name}: ${summary} (${trigger})`);
        console.log(`[Prune] ${rule.name}: ${summary} (${trigger})`);
    }
}

// --- Scheduler -------------------------------------------------------------------------

let timer = null;
let ticking = false;

async function tick() {
    if (ticking) return;
    ticking = true;
    try {
        const now = Date.now();
        await store.abandonStaleRuns(now - STALE_RUN_MS);
        for (const id of await store.dueRuleIds(now)) {
            if (runningRules.has(id)) continue;
            const rule = await store.getPruneRule(id);
            if (!rule) continue;
            let next = null;
            try { next = computeNextRun(rule.schedule, new Date(now)); } catch { next = null; }
            const token = `${INSTANCE_ID}:${randomBytes(4).toString('hex')}`;
            if (!rule.nextRunMs) {
                // New or migrated rule: schedule it rather than running at once.
                await store.setRuleNextRun(id, next);
                continue;
            }
            if (await store.claimRule(id, token, now, next)) {
                await startJob(id, { trigger: 'schedule' }).catch(err => console.warn(`[Prune] Could not start ${rule.name}:`, err.message));
            }
        }
        await store.trimRuns(now - RUN_HISTORY_MS);
    } catch (err) {
        console.warn('[Prune] Scheduler tick failed:', err.message || err);
    } finally {
        ticking = false;
    }
}

/** Start the per-rule scheduler. A random offset spreads instances apart. */
export async function startPruneScheduler() {
    // Runs left 'running' by an earlier process on this host (crash/restart).
    await store.abandonRunsOfHost(`${os.hostname()}:`).catch(() => {});
    if (timer) clearInterval(timer);
    const offset = Math.floor(Math.random() * 5000);
    setTimeout(() => {
        tick();
        timer = setInterval(tick, TICK_MS);
        timer.unref?.();
    }, offset).unref?.();
}

export function storageInfo() {
    const days = snapshotRetentionDays();
    return { snapshotRetentionDays: days };
}
