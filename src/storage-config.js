/**
 * Storage settings saved from the admin console (Storage page).
 *
 * Console settings normally live in DuckLake, but these are what the gateway
 * needs to OPEN DuckLake, so they are kept in a small JSON file instead:
 * STORAGE_CONFIG_FILE, default ./config/storage.json (in Docker /app/config,
 * bind-mounted to docker/data/config so it survives rebuilds). It is applied
 * to process.env when this module is first imported (src/db.js imports it),
 * before any other module reads the variables, and its values take precedence
 * over docker/.env. The Postgres catalog connection is never stored here.
 *
 * Saving is two-phase so a bad setting cannot lock the gateway out:
 *
 *   1. The console tests the new settings, writes them with `pending: true`
 *      plus the settings that were in effect (`previous`), and restarts.
 *   2. At startup a pending file is tried. confirmStorageConfig() (called once
 *      DuckLake is attached) clears `pending`. If attaching keeps failing,
 *      revertStorageConfig() restores `previous`, records `lastError` for the
 *      console to show, and the gateway exits so its supervisor restarts it
 *      on the last working settings.
 *
 * A changed DUCKLAKE_DATA_PATH also records `switchFrom` (the old path); the
 * primary instance then moves the lake to the new location at startup (see
 * runPendingStorageSwitch() in src/db.js), which the console only allows while
 * no user table has data files.
 */
import fs from 'fs';
import path from 'path';

/** Keys the Storage page may set. */
export const STORAGE_KEYS = [
    'DUCKLAKE_DATA_PATH',
    'S3_ENDPOINT', 'S3_REGION', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_SESSION_TOKEN', 'S3_URL_STYLE', 'S3_USE_SSL',
    'DATA_INLINING_ROW_LIMIT',
    'USE_FILESYSTEM_BLOBS', 'BLOBS_PATH',
];

/** Attach failures tolerated on a pending configuration before reverting it. */
export const PENDING_ATTEMPTS = 3;

export const storageConfigFile = () => path.resolve(process.env.STORAGE_CONFIG_FILE || path.join('config', 'storage.json'));

// Environment values as they were before the file was applied (".env" values
// for the console, and what a revert to "no saved settings" falls back to).
const bootEnv = Object.fromEntries(STORAGE_KEYS.map(k => [k, process.env[k]]));
let loaded = null;     // parsed file, or null
let applied = {};      // settings applied from the file
let trying = false;    // running on a pending (unconfirmed) configuration
let failures = 0;

export function readStorageFile(file = storageConfigFile()) {
    try {
        const doc = JSON.parse(fs.readFileSync(file, 'utf8'));
        return doc && typeof doc === 'object' ? doc : null;
    } catch (err) {
        if (err.code !== 'ENOENT') console.error(`[Storage] Ignoring unreadable ${file}: ${err.message}`);
        return null;
    }
}

/** Write atomically (temp file + rename) with owner-only permissions: it can hold S3 credentials. */
export function writeStorageFile(doc, file = storageConfigFile()) {
    fs.mkdirSync(path.dirname(file), { recursive: true });
    const tmp = `${file}.${process.pid}.tmp`;
    fs.writeFileSync(tmp, JSON.stringify(doc, null, 2) + '\n', { mode: 0o600 });
    fs.renameSync(tmp, file);
    loaded = doc;
}

function pick(settings) {
    const out = {};
    for (const k of STORAGE_KEYS) if (settings && settings[k] !== undefined && settings[k] !== null) out[k] = String(settings[k]);
    return out;
}

function setEnv(settings) {
    for (const k of STORAGE_KEYS) {
        if (settings[k] !== undefined) process.env[k] = settings[k];
        else if (bootEnv[k] === undefined) delete process.env[k];
        else process.env[k] = bootEnv[k];
    }
}

/** Apply the saved file to process.env. Runs once on import; exported for tests. */
export function applyStorageConfigFile(file = storageConfigFile()) {
    loaded = readStorageFile(file);
    applied = pick(loaded?.settings);
    trying = !!loaded?.pending;
    failures = 0;
    setEnv(applied);
    if (Object.keys(applied).length) {
        console.log(`[Storage] Using storage settings saved in the console (${file})${trying ? ' - new settings, not yet confirmed' : ''}.`);
    }
    return { loaded, applied, trying };
}

applyStorageConfigFile();

/** Effective value of each storage key and where it comes from. */
export function describeStorageSettings(defaults = {}) {
    const out = {};
    for (const k of STORAGE_KEYS) {
        if (applied[k] !== undefined) out[k] = { value: applied[k], source: 'console' };
        else if (bootEnv[k] !== undefined && bootEnv[k] !== '') out[k] = { value: bootEnv[k], source: 'env' };
        else out[k] = { value: defaults[k] ?? '', source: 'default' };
    }
    return out;
}

/** Values from docker/.env (before console settings were applied). */
export const envStorageSettings = () => ({ ...bootEnv });

export function storageFileStatus() {
    const file = storageConfigFile();
    let writable = false;
    try {
        fs.mkdirSync(path.dirname(file), { recursive: true });
        fs.accessSync(path.dirname(file), fs.constants.W_OK);
        writable = true;
    } catch { /* reported to the console */ }
    return {
        file,
        writable,
        exists: !!loaded,
        pending: trying,
        savedAt: loaded?.savedAt ?? null,
        savedBy: loaded?.savedBy ?? null,
        switchFrom: trying ? loaded?.switchFrom ?? null : null,
        lastError: loaded?.lastError ?? null,
        lastSwitch: loaded?.lastSwitch ?? null,
    };
}

/**
 * When this start must move the lake: the old data path and the complete
 * settings that were in effect for it (pending file with switchFrom).
 */
export function pendingSwitch() {
    if (!trying || !loaded?.switchFrom) return null;
    return { from: loaded.switchFrom.dataPath, settings: { ...loaded.switchFrom.settings } };
}

/**
 * Save new settings (console). `settings` holds every key the page manages
 * (values equal to docker/.env included, so the file is self-contained).
 * `effective` is what is in effect now: the old location's settings when
 * `switchFrom` (old data path) is given. The fallback is the settings saved
 * before, or none (docker/.env) when nothing was saved.
 */
export function saveStorageConfig(settings, { by, effective = {}, switchFrom = null } = {}) {
    const doc = {
        version: 1,
        settings: pick(settings),
        pending: true,
        previous: Object.keys(applied).length ? { ...applied } : null,
        switchFrom: switchFrom ? { dataPath: switchFrom, settings: pick(effective) } : null,
        savedAt: new Date().toISOString(),
        savedBy: by || null,
        lastError: null,
        lastSwitch: loaded?.lastSwitch ?? null,
    };
    writeStorageFile(doc);
    return doc;
}

/** Called once DuckLake attached on the pending settings. */
export function confirmStorageConfig(extra = {}) {
    if (!trying || !loaded) return;
    const doc = { ...loaded, pending: false, switchFrom: null, lastError: null, confirmedAt: new Date().toISOString(), ...extra };
    try {
        writeStorageFile(doc);
        console.log('[Storage] New storage settings confirmed.');
    } catch (err) {
        console.error('[Storage] Could not record the confirmed storage settings:', err.message);
    }
    trying = false;
}

/**
 * Count an attach failure. On a pending configuration, after PENDING_ATTEMPTS
 * failures (or immediately when `fatal`), restore the previous settings and
 * return true: the caller exits so the gateway restarts on them.
 */
export function noteAttachFailure(err, { fatal = false } = {}) {
    if (!trying || !loaded) return false;
    failures++;
    if (!fatal && failures < PENDING_ATTEMPTS) return false;
    revertStorageConfig(err);
    return true;
}

export function revertStorageConfig(err) {
    const failed = pick(loaded?.settings);
    const doc = {
        version: 1,
        settings: pick(loaded?.previous || {}),
        pending: false,
        previous: null,
        switchFrom: null,
        savedAt: loaded?.savedAt ?? null,
        savedBy: loaded?.savedBy ?? null,
        lastError: {
            at: new Date().toISOString(),
            message: String(err?.message || err).split('\n')[0].slice(0, 1000),
            settings: redact(failed),
        },
        lastSwitch: loaded?.lastSwitch ?? null,
    };
    try {
        writeStorageFile(doc);
        console.error(`[Storage] New storage settings failed (${doc.lastError.message}); restored the previous settings.`);
    } catch (writeErr) {
        console.error('[Storage] Could not restore the previous storage settings:', writeErr.message);
    }
    trying = false;
}

/** Copy without secrets (for logs, the audit trail and lastError). */
export function redact(settings) {
    const out = { ...settings };
    for (const k of ['S3_SECRET_KEY', 'S3_SESSION_TOKEN']) if (out[k]) out[k] = '********';
    return out;
}
