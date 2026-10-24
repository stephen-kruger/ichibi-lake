/**
 * Scheduled lake maintenance: fixes the small-files problem and reclaims
 * storage. Each run (see runMaintenance() in src/db.js) flushes inlined rows to
 * Parquet, merges adjacent small files, rewrites files with many deleted rows,
 * expires snapshots past SNAPSHOT_RETENTION_DAYS and deletes unreferenced
 * files after a grace period.
 *
 * The job has one cron schedule (UTC), edited on the console's Retention page.
 * Every gateway instance runs the scheduler; a due run is claimed with the
 * token-and-verify step used for pruning rules plus a lease, so exactly one
 * instance runs it and runs never overlap (see store.claimMaintenance). Runs
 * are recorded in the run log (kind 'maintenance') and the audit log.
 */
import os from 'os';
import { randomBytes } from 'crypto';
import { runMaintenance, describeMaintenance, getLakeFileStats } from '../db.js';
import * as store from './store.js';
import { validateCron, nextRun, parseCron } from './cron.js';
import { INSTANCE_ID } from './retention.js';

export const MAINTENANCE_STEPS = {
    flush: 'Flush inlined rows to Parquet',
    merge: 'Merge adjacent small files',
    rewrite: 'Rewrite files with many deleted rows',
    expire: 'Expire snapshots older than the snapshot retention',
    cleanup: 'Delete unreferenced and orphaned files (after a 5-minute grace period)',
};

export const MAINTENANCE_DEFAULTS = Object.freeze({
    enabled: true,
    schedule: '0 * * * *',
    steps: Object.freeze({ flush: true, merge: true, rewrite: true, expire: true, cleanup: true }),
    deleteThreshold: null,
    runAtStartup: true,
});

const TICK_MS = 30 * 1000;
// A run holding the lease longer than this is assumed dead (instance crashed).
const LEASE_MS = 6 * 3600 * 1000;
// A startup run is skipped if a run finished this recently.
const STARTUP_SKIP_MS = 60 * 60 * 1000;
const RUN_ID = '__maintenance';
const RUN_NAME = 'Lake maintenance';

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

export const maintenanceDisabledByEnv = () => process.env.DISABLE_DUCKLAKE_COMPACTION === '1';

/** Validate and normalise maintenance settings from API input. Throws a 400 error when invalid. */
export function normaliseMaintenance(input = {}) {
    const schedule = String(input.schedule ?? MAINTENANCE_DEFAULTS.schedule).trim();
    const cronError = validateCron(schedule);
    if (cronError) throw httpError(400, `Invalid schedule: ${cronError}`);
    const steps = {};
    for (const key of Object.keys(MAINTENANCE_STEPS)) {
        const v = input.steps?.[key];
        steps[key] = v === undefined ? MAINTENANCE_DEFAULTS.steps[key] : !!v;
    }
    let deleteThreshold = null;
    if (input.deleteThreshold !== null && input.deleteThreshold !== undefined && input.deleteThreshold !== '') {
        deleteThreshold = Number(input.deleteThreshold);
        if (!Number.isFinite(deleteThreshold) || deleteThreshold <= 0 || deleteThreshold > 1) {
            throw httpError(400, 'Delete threshold must be a fraction above 0 and at most 1 (e.g. 0.5 for 50% of rows deleted)');
        }
    }
    return {
        enabled: input.enabled === undefined ? MAINTENANCE_DEFAULTS.enabled : !!input.enabled,
        schedule,
        steps,
        deleteThreshold,
        runAtStartup: input.runAtStartup === undefined ? MAINTENANCE_DEFAULTS.runAtStartup : !!input.runAtStartup,
    };
}

export function describeSteps(steps) {
    const on = Object.keys(MAINTENANCE_STEPS).filter(k => steps?.[k]);
    return on.length ? on.join(', ') : 'no steps';
}

function computeNext(schedule, from = new Date()) {
    try { return nextRun(parseCron(schedule), from)?.getTime() ?? null; } catch { return null; }
}

/** Current configuration (with the row created on first use). */
export async function getMaintenanceConfig() {
    let cfg = await store.getMaintenance();
    if (!cfg) {
        await store.ensureMaintenanceRow({ ...MAINTENANCE_DEFAULTS, nextRunMs: computeNext(MAINTENANCE_DEFAULTS.schedule) });
        cfg = await store.getMaintenance();
    }
    return { ...cfg, steps: { ...MAINTENANCE_DEFAULTS.steps, ...(cfg.steps || {}) } };
}

export async function saveMaintenanceConfig(input, user) {
    const cfg = normaliseMaintenance(input);
    await getMaintenanceConfig();
    await store.updateMaintenance({ ...cfg, nextRunMs: cfg.enabled ? computeNext(cfg.schedule) : null }, user);
    return cfg;
}

let running = false;

/**
 * Claim and run maintenance in the background. `trigger` is 'schedule',
 * 'startup' or 'manual'. Returns the run id, or null when another instance
 * holds the run (or, for 'schedule', it is not due).
 */
export async function startMaintenance({ trigger = 'manual', username = null } = {}) {
    if (maintenanceDisabledByEnv()) throw httpError(409, 'Maintenance is disabled by DISABLE_DUCKLAKE_COMPACTION=1');
    if (running) {
        if (trigger === 'manual') throw httpError(409, 'Maintenance is already running on this gateway');
        return null;
    }
    const cfg = await getMaintenanceConfig();
    const now = Date.now();
    const token = `${INSTANCE_ID}:${randomBytes(4).toString('hex')}`;
    const nextRunMs = trigger === 'schedule' ? computeNext(cfg.schedule, new Date(now)) : cfg.nextRunMs;
    const claimed = await store.claimMaintenance(token, { nowMs: now, nextRunMs, leaseUntilMs: now + LEASE_MS, force: trigger !== 'schedule' });
    if (!claimed) {
        if (trigger === 'manual') throw httpError(409, 'Maintenance is already running on another gateway instance');
        return null;
    }
    running = true;
    const runId = randomBytes(8).toString('hex');
    try {
        await store.insertRun({ runId, jobId: runId, kind: 'maintenance', ruleId: RUN_ID, ruleName: RUN_NAME, trigger, status: 'running',
            startedMs: now, instance: INSTANCE_ID, username });
    } catch (err) {
        running = false;
        await store.finishMaintenance(token, 'failed', `Could not record the run: ${err.message}`).catch(() => {});
        throw err;
    }
    runJob(runId, token, cfg, { trigger, username })
        .catch(err => console.error(`[Maintenance] Run ${runId} crashed:`, err))
        .finally(() => { running = false; });
    return runId;
}

async function runJob(runId, token, cfg, { trigger, username }) {
    let status = 'failed';
    let summary = '';
    try {
        console.log(`[Maintenance] Starting (${trigger}): ${describeSteps(cfg.steps)}`);
        const stats = await runMaintenance({ ...cfg.steps, deleteThreshold: cfg.deleteThreshold });
        status = stats.failures.length ? 'partial' : 'succeeded';
        summary = `${describeMaintenance(stats)}. Took ${Math.round(stats.durationMs / 1000)}s.`;
    } catch (err) {
        summary = String(err.message || err).split('\n')[0];
    }
    await store.finishRun(runId, { status, message: summary }).catch(err => console.warn('[Maintenance] Could not record run:', err.message));
    await store.finishMaintenance(token, status, summary).catch(err => console.warn('[Maintenance] Could not release lease:', err.message));
    await store.audit(username || 'scheduler', 'maintenance.run', `${status}: ${summary} (${trigger})`);
    console.log(`[Maintenance] ${status}: ${summary} (${trigger})`);
}

// --- Scheduler -----------------------------------------------------------------------

let timer = null;
// Pending first tick; tracked so a repeated start (startup retry) does not
// leave a second interval running.
let startTimer = null;
let ticking = false;

async function tick() {
    if (ticking || running || maintenanceDisabledByEnv()) return;
    ticking = true;
    try {
        const cfg = await getMaintenanceConfig();
        const now = Date.now();
        if (!cfg.enabled) return;
        if (!cfg.nextRunMs) {
            await store.setMaintenanceNextRun(computeNext(cfg.schedule, new Date(now)));
            return;
        }
        if (cfg.nextRunMs <= now && !(cfg.leaseUntilMs && cfg.leaseUntilMs > now)) {
            await startMaintenance({ trigger: 'schedule' });
        }
    } catch (err) {
        console.warn('[Maintenance] Scheduler tick failed:', err.message || err);
    } finally {
        ticking = false;
    }
}

/**
 * Start the maintenance scheduler. When the job is enabled and set to run at
 * startup, also claim a run now unless one finished within the last hour (so
 * rolling restarts of several instances do not each compact the lake).
 */
export async function startMaintenanceScheduler() {
    if (maintenanceDisabledByEnv()) {
        console.warn('[Maintenance] Disabled: DISABLE_DUCKLAKE_COMPACTION=1.');
        return;
    }
    await getMaintenanceConfig();
    // INSTANCE_ID starts with the host name; a lease held under this host
    // belongs to a process that is gone (one gateway process per host).
    await store.releaseMaintenanceLeaseOfHost(`${os.hostname()}:`).catch(() => {});
    const cfg = await getMaintenanceConfig();
    if (timer) clearInterval(timer);
    if (startTimer) clearTimeout(startTimer);
    timer = null;
    const offset = Math.floor(Math.random() * 5000);
    startTimer = setTimeout(() => {
        startTimer = null;
        tick();
        timer = setInterval(tick, TICK_MS);
        timer.unref?.();
    }, offset);
    startTimer.unref?.();
    if (cfg.enabled && cfg.runAtStartup && !(cfg.lastRunMs && Date.now() - cfg.lastRunMs < STARTUP_SKIP_MS)) {
        await startMaintenance({ trigger: 'startup' }).catch(err => console.warn('[Maintenance] Startup run not started:', err.message));
    }
}

/** Data for the console's Maintenance card. */
export async function maintenanceOverview() {
    const [config, runs, files] = await Promise.all([
        getMaintenanceConfig(),
        store.listMaintenanceRuns(10),
        getLakeFileStats().catch(() => null),
    ]);
    return {
        config, runs, files,
        steps: MAINTENANCE_STEPS,
        runningHere: running,
        disabledByEnv: maintenanceDisabledByEnv(),
        host: os.hostname(),
    };
}
