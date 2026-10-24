/**
 * Date-based retention ("auto-prune"): each rule deletes rows of one table
 * whose timestamp/date column is older than `keep_days`. Rules live in
 * _console_retention and run on a timer (RETENTION_INTERVAL_MINUTES) on every
 * gateway instance; DELETE is idempotent, so concurrent runs are harmless.
 *
 * When filesystem BLOB storage is enabled, files referenced by pruned rows are
 * removed too (only paths that resolve inside BLOBS_PATH are ever unlinked).
 */
import fs from 'fs/promises';
import path from 'path';
import { execute, withConnection, VARCHAR } from '../db.js';
import { listRetentionRules, getRetentionRule, recordRetentionRun, audit } from './store.js';

const SAFE_IDENTIFIER_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;
const DATE_TYPES_RE = /^(DATE|TIMESTAMP(\s*WITH TIME ZONE)?|TIMESTAMP_(S|MS|NS)|TIMESTAMPTZ|DATETIME)$/i;
export const MAX_KEEP_DAYS = 36500;

export function isSystemTable(name) {
    return String(name).startsWith('_');
}

export function validateRule({ tableName, columnName, keepDays }) {
    if (!SAFE_IDENTIFIER_RE.test(String(tableName || ''))) return 'Invalid table name';
    if (isSystemTable(tableName)) return 'System tables cannot be pruned';
    if (!SAFE_IDENTIFIER_RE.test(String(columnName || ''))) return 'Invalid column name';
    const n = Number(keepDays);
    if (!Number.isInteger(n) || n < 1 || n > MAX_KEEP_DAYS) return `Keep days must be a whole number between 1 and ${MAX_KEEP_DAYS}`;
    return null;
}

/**
 * WHERE clause selecting rows older than the retention window. Identifiers are
 * validated by validateRule() and keepDays is an integer, so interpolation is
 * safe. DATE columns compare against CURRENT_DATE so whole days are kept.
 */
export function buildCutoffPredicate(columnName, dataType, keepDays) {
    const days = Number(keepDays);
    if (!SAFE_IDENTIFIER_RE.test(columnName) || !Number.isInteger(days)) throw new Error('Invalid retention rule');
    const col = `"${columnName}"`;
    if (/^DATE$/i.test(String(dataType).trim())) {
        return `${col} < CURRENT_DATE - INTERVAL ${days} DAY`;
    }
    return `${col} < CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL ${days} DAY`;
}

/** Resolve a stored blob path and return it only if it lies inside blobsRoot. */
export function containedBlobPath(storedPath, blobsRoot) {
    if (typeof storedPath !== 'string' || !storedPath) return null;
    const root = path.resolve(blobsRoot);
    const resolved = path.resolve(storedPath);
    return resolved.startsWith(root + path.sep) ? resolved : null;
}

/** User tables with the timestamp/date columns a rule can target. */
export async function listPrunableTables() {
    const rows = await execute(
        `SELECT table_name, column_name, data_type FROM information_schema.columns
         WHERE table_schema = 'main' ORDER BY table_name, ordinal_position`
    );
    const tables = new Map();
    for (const r of rows) {
        if (isSystemTable(r.table_name) || !SAFE_IDENTIFIER_RE.test(r.table_name)) continue;
        if (!tables.has(r.table_name)) tables.set(r.table_name, []);
        if (DATE_TYPES_RE.test(String(r.data_type).trim()) && SAFE_IDENTIFIER_RE.test(r.column_name)) {
            tables.get(r.table_name).push({ name: r.column_name, type: r.data_type });
        }
    }
    return [...tables.entries()].map(([name, dateColumns]) => ({ name, dateColumns }));
}

async function columnTypes(conn, tableName) {
    const result = await conn.run(
        `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ?`,
        [tableName], [VARCHAR]
    );
    const rows = await result.getRows();
    return new Map(rows.map(r => [r[0], String(r[1])]));
}

async function scalar(conn, sql) {
    const result = await conn.run(sql);
    const rows = await result.getRows();
    return rows.length ? rows[0][0] : null;
}

/**
 * Count (dryRun) or delete rows older than the window. Returns
 * { matched, deleted, blobFilesRemoved, cutoff }.
 */
export async function pruneTable({ tableName, columnName, keepDays }, { dryRun = false } = {}) {
    const invalid = validateRule({ tableName, columnName, keepDays });
    if (invalid) throw Object.assign(new Error(invalid), { statusCode: 400 });

    const useFsBlobs = process.env.USE_FILESYSTEM_BLOBS === '1';
    const blobsRoot = process.env.BLOBS_PATH || './data/blobs';

    return withConnection(async (conn) => {
        const types = await columnTypes(conn, tableName);
        if (types.size === 0) throw Object.assign(new Error(`Table "${tableName}" does not exist`), { statusCode: 404 });
        const dataType = types.get(columnName);
        if (!dataType) throw Object.assign(new Error(`Column "${columnName}" does not exist on "${tableName}"`), { statusCode: 400 });
        if (!DATE_TYPES_RE.test(dataType.trim())) {
            throw Object.assign(new Error(`Column "${columnName}" is ${dataType}, not a date/timestamp`), { statusCode: 400 });
        }

        const where = buildCutoffPredicate(columnName, dataType, keepDays);
        const cutoff = await scalar(conn, /^DATE$/i.test(dataType.trim())
            ? `SELECT CAST(CURRENT_DATE - INTERVAL ${Number(keepDays)} DAY AS VARCHAR)`
            : `SELECT CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL ${Number(keepDays)} DAY AS VARCHAR)`);
        const matched = Number(await scalar(conn, `SELECT count(*) FROM "${tableName}" WHERE ${where}`) || 0);
        if (dryRun || matched === 0) {
            return { matched, deleted: 0, blobFilesRemoved: 0, cutoff };
        }

        // Collect blob file paths referenced by the doomed rows before deleting.
        const blobFiles = new Set();
        if (useFsBlobs) {
            const prefix = path.resolve(blobsRoot);
            const varcharCols = [...types.entries()]
                .filter(([name, t]) => /^VARCHAR/i.test(t) && SAFE_IDENTIFIER_RE.test(name))
                .map(([name]) => name);
            for (const col of varcharCols) {
                const result = await conn.run(
                    `SELECT "${col}" FROM "${tableName}" WHERE ${where} AND "${col}" IS NOT NULL`
                );
                for (const [value] of await result.getRows()) {
                    const p = containedBlobPath(value, prefix);
                    if (p) blobFiles.add(p);
                }
            }
        }

        await conn.run(`DELETE FROM "${tableName}" WHERE ${where}`);

        let blobFilesRemoved = 0;
        for (const file of blobFiles) {
            try { await fs.unlink(file); blobFilesRemoved++; } catch (err) {
                if (err.code !== 'ENOENT') console.warn(`[Retention] Could not remove blob file ${file}:`, err.message);
            }
        }
        return { matched, deleted: matched, blobFilesRemoved, cutoff };
    });
}

let timer = null;
let running = false;

/** Run every enabled rule once. Safe to call concurrently (no overlap). */
export async function runAllRules({ trigger = 'schedule', username = 'system' } = {}) {
    if (running) return { skipped: true };
    running = true;
    const results = [];
    try {
        const rules = await listRetentionRules();
        for (const rule of rules) {
            if (rule.enabled !== 'true') continue;
            results.push(await runRule(rule.table_name, { trigger, username }));
        }
    } finally {
        running = false;
    }
    return { results };
}

export async function runRule(tableName, { trigger = 'manual', username = 'system', ip = '' } = {}) {
    const rule = await getRetentionRule(tableName);
    if (!rule) throw Object.assign(new Error(`No retention rule for "${tableName}"`), { statusCode: 404 });
    const spec = { tableName: rule.table_name, columnName: rule.column_name, keepDays: Number(rule.keep_days) };
    try {
        const r = await pruneTable(spec);
        await recordRetentionRun(tableName, r.deleted, '');
        if (r.deleted > 0 || trigger === 'manual') {
            await audit(username, 'retention.run',
                `${tableName}: deleted ${r.deleted} row(s) older than ${r.cutoff} (${trigger})` +
                (r.blobFilesRemoved ? `, removed ${r.blobFilesRemoved} blob file(s)` : ''), ip);
            console.log(`[Retention] ${tableName}: deleted ${r.deleted} row(s) older than ${spec.keepDays} day(s) (${trigger}).`);
        }
        return { table: tableName, ...r };
    } catch (err) {
        const message = err.message || String(err);
        console.warn(`[Retention] ${tableName} failed:`, message);
        await recordRetentionRun(tableName, 0, message).catch(() => {});
        return { table: tableName, error: message };
    }
}

function intervalMs() {
    const minutes = Math.max(1, parseInt(process.env.RETENTION_INTERVAL_MINUTES || '60', 10) || 60);
    return minutes * 60 * 1000;
}

/** (Re)start the scheduler; called at startup and when the interval changes. */
export function scheduleRetention() {
    if (timer) clearTimeout(timer);
    const tick = async () => {
        try { await runAllRules(); } catch (err) { console.warn('[Retention] Scheduled run failed:', err.message || err); }
        timer = setTimeout(tick, intervalMs());
        timer.unref?.();
    };
    timer = setTimeout(tick, intervalMs());
    timer.unref?.();
}
