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
const PATTERN_ITEM_RE = /^[A-Za-z0-9_*?]{1,63}$/;
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

    const include = splitPatterns(input.tablePattern);
    if (!include.length) throw httpError(400, 'Enter at least one table name or pattern');
    const exclude = splitPatterns(input.excludePattern);
    for (const p of [...include, ...exclude]) {
        if (!PATTERN_ITEM_RE.test(p)) throw httpError(400, `Invalid table pattern "${p}": use letters, digits, _ and the wildcards * and ?`);
        if (isSystemTable(p)) throw httpError(400, `Patterns cannot start with "_" (reserved for system tables): "${p}"`);
    }

    const maxAgeUnit = input.maxAgeUnit === 'hours' ? 'hours' : 'days';
    const maxAgeValue = optionalPositiveInt(input.maxAgeValue, `Maximum age (${maxAgeUnit})`, MAX_AGE[maxAgeUnit]);
    const maxRows = optionalPositiveInt(input.maxRows, 'Rows to keep', 1e12);
    const dateColumn = optionalColumn(input.dateColumn, 'date column');
    const orderColumn = optionalColumn(input.orderColumn, 'order column');
    if (maxAgeValue === null && maxRows === null) throw httpError(400, 'Choose at least one criterion: a maximum age, a number of rows to keep, or both');
    if (maxAgeValue !== null && !dateColumn) throw httpError(400, 'Age-based pruning needs the date/timestamp column to compare');

    const schedule = String(input.schedule || '').trim();
    const cronError = validateCron(schedule);
    if (cronError) throw httpError(400, `Invalid schedule: ${cronError}`);

    return {
        name,
        tablePattern: include.join(', '),
        excludePattern: exclude.join(', '),
        dateColumn: maxAgeValue !== null ? dateColumn : null,
        maxAgeValue, maxAgeUnit,
        maxRows,
        orderColumn: maxRows !== null ? orderColumn : null,
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

/**
 * Predicates selecting rows to delete from one table. `types` maps column
 * name -> DuckDB type for the table. Returns { age, count, skip } where skip
 * explains why the rule cannot apply to this table.
 */
export function buildPredicates(rule, tableName, types) {
    let age = null;
    let count = null;
    if (rule.maxAgeValue) {
        const type = types.get(rule.dateColumn);
        if (!type) return { skip: `no "${rule.dateColumn}" column` };
        if (!DATE_TYPES_RE.test(String(type).trim())) return { skip: `"${rule.dateColumn}" is ${type}, not a date/timestamp` };
        const n = Number(rule.maxAgeValue);
        const unit = rule.maxAgeUnit === 'hours' ? 'HOUR' : 'DAY';
        age = /^DATE$/i.test(String(type).trim())
            ? `${quoteIdent(rule.dateColumn)} < CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL ${n} ${unit} AS DATE)`
            : `${quoteIdent(rule.dateColumn)} < CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL ${n} ${unit}`;
    }
    if (rule.maxRows) {
        let order = 'rowid DESC';
        if (rule.orderColumn) {
            if (!types.has(rule.orderColumn)) return { skip: `no "${rule.orderColumn}" column to order by` };
            order = `${quoteIdent(rule.orderColumn)} DESC NULLS LAST, rowid DESC`;
        }
        count = `rowid IN (SELECT rowid FROM ${quoteIdent(tableName)} ORDER BY ${order} OFFSET ${Number(rule.maxRows)})`;
    }
    return { age, count, skip: null };
}

function wherePredicate({ age, count }) {
    return [age, count].filter(Boolean).map(p => `(${p})`).join(' OR ');
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
async function evaluateTable(conn, rule, tableName) {
    assertPrunable(tableName);
    const types = await columnTypes(conn, tableName);
    if (!types.size) return { table: tableName, status: 'skipped', reason: 'table no longer exists' };
    const preds = buildPredicates(rule, tableName, types);
    if (preds.skip) return { table: tableName, status: 'skipped', reason: preds.skip };
    const t = quoteIdent(tableName);
    const [rows, byAge, byCount, total] = await firstRow(conn,
        `SELECT count(*),
                ${preds.age ? `count(*) FILTER (WHERE ${preds.age})` : '0'},
                ${preds.count ? `count(*) FILTER (WHERE ${preds.count})` : '0'},
                count(*) FILTER (WHERE ${wherePredicate(preds)})
         FROM ${t}`);
    return { table: tableName, status: 'ok', preds, rows: Number(rows), byAge: Number(byAge), byCount: Number(byCount), total: Number(total), types };
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

/** Dry run: which tables match and how many rows each would lose. */
export async function previewRule(input) {
    const rule = normaliseRule({ ...input, name: input.name || 'preview', schedule: input.schedule || '@daily' });
    const tables = matchTables(await userTableNames(), rule.tablePattern, rule.excludePattern);
    return withConnection(async (conn) => {
        const results = [];
        for (const table of tables) {
            const e = await evaluateTable(conn, rule, table);
            results.push({
                table, status: e.status, reason: e.reason || null, rows: e.rows ?? null,
                byAge: e.byAge ?? 0, byCount: e.byCount ?? 0, total: e.total ?? 0,
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
                        return { status: 'failed', rowsByAge: e.byAge, rowsByCount: e.byCount,
                            message: `Would delete ${e.total.toLocaleString()} rows, above the safety limit of ${rule.safetyLimit.toLocaleString()}; nothing was deleted.` };
                    }
                    if (e.total === 0) return { status: 'succeeded', rowsByAge: 0, rowsByCount: 0, rowsDeleted: 0, message: 'Nothing to delete.' };
                    const d = await deleteRows(conn, table, e);
                    return { status: 'succeeded', rowsByAge: e.byAge, rowsByCount: e.byCount, rowsDeleted: d.deleted, blobFiles: d.blobFiles,
                        message: `Deleted ${d.deleted.toLocaleString()} of ${e.rows.toLocaleString()} rows.` };
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
