/**
 * Console Storage page: view, test and save the storage settings kept in
 * STORAGE_CONFIG_FILE (src/storage-config.js), then restart to apply them.
 *
 * The page edits three things:
 *   data files   local directory or S3-compatible bucket (DUCKLAKE_DATA_PATH,
 *                S3_*), plus the inlining row limit
 *   BLOBs        inside the tables or in a local directory
 *                (USE_FILESYSTEM_BLOBS, BLOBS_PATH)
 *   catalog      Postgres connection: read-only here (docker/.env); pointing
 *                it elsewhere opens a different lake.
 *
 * Moving the data files (a new local path or bucket/prefix) is only allowed
 * while no user table has data files: DuckLake stores file paths relative to
 * the data path, so files would otherwise have to be copied. The move itself
 * happens at the next start (runPendingStorageSwitch in src/db.js), which
 * re-checks this.
 */
import fs from 'fs';
import fsp from 'fs/promises';
import path from 'path';
import { randomUUID } from 'crypto';
import { DuckDBInstance } from '@duckdb/node-api';
import { execute } from '../db.js';
import { storageConfig, buildS3SecretSql, isObjectStorePath, describeStorage } from '../storage.js';
import {
    STORAGE_KEYS, describeStorageSettings, envStorageSettings, storageFileStatus, saveStorageConfig, redact,
} from '../storage-config.js';

const DEFAULTS = {
    DUCKLAKE_DATA_PATH: 'data_files/', S3_REGION: 'us-east-1', S3_USE_SSL: 'true',
    DATA_INLINING_ROW_LIMIT: '1000', BLOBS_PATH: './data/blobs',
};
const TEST_TIMEOUT_MS = 20000;
const MAX_INLINING_ROWS = 1000000;

export function httpError(statusCode, message, extra = {}) {
    return Object.assign(new Error(message), { statusCode, ...extra });
}

const isTrue = (v) => /^(1|true|yes|on)$/i.test(String(v ?? '').trim());

/** Effective storage settings as a flat { KEY: value } map. */
function effectiveSettings() {
    const d = describeStorageSettings(DEFAULTS);
    return Object.fromEntries(STORAGE_KEYS.map(k => [k, d[k].value]));
}

/** Settings map -> the shape the page edits (secrets never leave the server). */
function toForm(s) {
    const s3Path = isObjectStorePath(s.DUCKLAKE_DATA_PATH);
    return {
        backend: s3Path ? 's3' : 'local',
        localPath: s3Path ? '' : s.DUCKLAKE_DATA_PATH,
        s3: {
            path: s3Path ? s.DUCKLAKE_DATA_PATH : '',
            endpoint: s.S3_ENDPOINT || '',
            region: s.S3_REGION || '',
            accessKey: s.S3_ACCESS_KEY || '',
            hasSecretKey: !!s.S3_SECRET_KEY,
            hasSessionToken: !!s.S3_SESSION_TOKEN,
            urlStyle: s.S3_URL_STYLE || '',
            useSsl: s.S3_USE_SSL === '' ? true : isTrue(s.S3_USE_SSL),
        },
        inliningLimit: Number(s.DATA_INLINING_ROW_LIMIT || DEFAULTS.DATA_INLINING_ROW_LIMIT),
        blobs: { mode: s.USE_FILESYSTEM_BLOBS === '1' ? 'filesystem' : 'inline', path: s.BLOBS_PATH || DEFAULTS.BLOBS_PATH },
    };
}

/**
 * Page input -> settings map. Secrets left empty keep the current value; S3
 * fields are kept with a local backend too (bulk imports from s3:// use them).
 */
export function fromForm(form, current) {
    if (!form || typeof form !== 'object') throw httpError(400, 'Expected a storage configuration object');
    const s3 = form.s3 || {};
    const str = (v) => (v === undefined || v === null ? '' : String(v).trim());
    const out = { ...current };
    const backend = form.backend === 's3' ? 's3' : form.backend === 'local' ? 'local' : null;
    if (!backend) throw httpError(400, 'backend must be "local" or "s3"');
    if (backend === 'local') {
        const p = str(form.localPath);
        if (!p) throw httpError(400, 'Enter the local directory for the data files');
        if (isObjectStorePath(p)) throw httpError(400, 'A local directory cannot be a URL; choose "S3-compatible" for object storage');
        out.DUCKLAKE_DATA_PATH = p;
    } else {
        const p = str(s3.path);
        if (!/^s3:\/\/[^/]+/i.test(p)) throw httpError(400, 'The bucket path must look like s3://bucket/prefix/');
        out.DUCKLAKE_DATA_PATH = p.endsWith('/') ? p : `${p}/`;
    }
    out.S3_ENDPOINT = str(s3.endpoint);
    out.S3_REGION = str(s3.region);
    out.S3_ACCESS_KEY = str(s3.accessKey);
    if (s3.clearSecretKey) out.S3_SECRET_KEY = '';
    else if (str(s3.secretKey)) out.S3_SECRET_KEY = String(s3.secretKey);
    if (s3.clearSessionToken) out.S3_SESSION_TOKEN = '';
    else if (str(s3.sessionToken)) out.S3_SESSION_TOKEN = String(s3.sessionToken);
    if (!out.S3_ACCESS_KEY) { out.S3_SECRET_KEY = ''; out.S3_SESSION_TOKEN = ''; }
    const style = str(s3.urlStyle).toLowerCase();
    if (style && !['path', 'vhost'].includes(style)) throw httpError(400, 'URL style must be path, vhost or automatic');
    out.S3_URL_STYLE = style;
    out.S3_USE_SSL = s3.useSsl === false ? 'false' : 'true';

    const limit = Number(form.inliningLimit);
    if (!Number.isInteger(limit) || limit < 0 || limit > MAX_INLINING_ROWS) throw httpError(400, `The inlining row limit must be a whole number from 0 to ${MAX_INLINING_ROWS}`);
    out.DATA_INLINING_ROW_LIMIT = String(limit);

    const blobs = form.blobs || {};
    if (!['inline', 'filesystem'].includes(blobs.mode)) throw httpError(400, 'BLOB storage must be "inline" or "filesystem"');
    out.USE_FILESYSTEM_BLOBS = blobs.mode === 'filesystem' ? '1' : '';
    const blobPath = str(blobs.path);
    if (blobs.mode === 'filesystem' && !blobPath) throw httpError(400, 'Enter the BLOB directory');
    if (blobPath && isObjectStorePath(blobPath)) throw httpError(400, 'BLOB files go to a local (or mounted) directory, not a URL');
    out.BLOBS_PATH = blobPath || current.BLOBS_PATH;

    try { storageConfig(out); } catch (err) { throw httpError(400, err.message); }
    return out;
}

/**
 * The data path DuckLake will be given. Compared as a string on purpose:
 * DuckLake checks it against the path recorded in the catalog verbatim, so
 * even "data_files/" -> "/app/data_files/" is a move.
 */
const dataLocation = (settings) => storageConfig(settings).dataPath;

// --- Where things are stored ------------------------------------------------------

const inContainer = () => fs.existsSync('/.dockerenv') || fs.existsSync('/run/.containerenv');

/** In a container: is this directory on a mounted volume (survives re-creating the container)? */
function persistence(dir) {
    if (!inContainer()) return null;
    try {
        const abs = path.resolve(dir);
        const mounts = fs.readFileSync('/proc/self/mountinfo', 'utf8').split('\n')
            .map(l => l.split(' ')[4]).filter(Boolean)
            .map(m => m.replace(/\\040/g, ' '));
        const best = mounts.filter(m => abs === m || abs.startsWith(m.endsWith('/') ? m : `${m}/`))
            .sort((a, b) => b.length - a.length)[0] || '/';
        return { mounted: best !== '/', mountPoint: best };
    } catch { return null; }
}

/** Automatic restart needs a supervisor: Docker's restart policy or PM2. */
export function autoRestart() {
    if (process.env.STORAGE_AUTO_RESTART !== undefined) return isTrue(process.env.STORAGE_AUTO_RESTART);
    return inContainer() || process.env.pm_id !== undefined;
}

/** User tables that have data files (they block moving the data files). */
async function tablesWithFiles() {
    const rows = await execute(`SELECT table_name, file_count, file_size_bytes, delete_file_count FROM ducklake_table_info('ducklake')`);
    return rows.filter(r => !String(r.table_name).startsWith('_') && Number(r.file_count) + Number(r.delete_file_count) > 0)
        .map(r => ({ table: r.table_name, files: Number(r.file_count), bytes: Number(r.file_size_bytes) }))
        .sort((a, b) => b.bytes - a.bytes);
}

export async function storageOverview() {
    const describe = describeStorageSettings(DEFAULTS);
    const settings = Object.fromEntries(STORAGE_KEYS.map(k => [k, describe[k].value]));
    const sources = Object.fromEntries(STORAGE_KEYS.map(k => [k, describe[k].source]));
    const env = envStorageSettings();
    const envForm = toForm(Object.fromEntries(STORAGE_KEYS.map(k => [k, env[k] ?? DEFAULTS[k] ?? ''])));
    let blocking = [];
    let lakeError = null;
    try { blocking = await tablesWithFiles(); } catch (err) { lakeError = err.message; }
    const cfg = storageConfig(settings);
    return {
        form: toForm(settings),
        envForm,
        sources,
        active: { description: describeStorage(cfg), dataPath: cfg.dataPath, kind: cfg.kind },
        catalog: {
            host: process.env.DUCKLAKE_PG_HOST || '0.0.0.0',
            port: process.env.DUCKLAKE_PG_PORT || '5432',
            database: process.env.DUCKLAKE_PG_DB || 'ducklake',
            user: process.env.DUCKLAKE_PG_USER || 'postgres',
            passwordSet: !!process.env.DUCKLAKE_PG_PASSWORD,
        },
        lake: { tablesWithFiles: blocking.slice(0, 25), tablesWithFilesCount: blocking.length, canMove: !lakeError && blocking.length === 0, error: lakeError },
        persistence: {
            data: cfg.kind === 'local' ? persistence(cfg.dataPath) : null,
            blobs: settings.USE_FILESYSTEM_BLOBS === '1' ? persistence(settings.BLOBS_PATH) : null,
            configFile: persistence(path.dirname(storageFileStatus().file)),
        },
        status: storageFileStatus(),
        autoRestart: autoRestart(),
    };
}

// --- Connection test ----------------------------------------------------------------

async function withTimeout(promise, ms, onTimeout) {
    let timer;
    try {
        return await Promise.race([promise, new Promise((_, reject) => {
            timer = setTimeout(() => { onTimeout?.(); reject(new Error(`no answer within ${ms / 1000} s`)); }, ms);
        })]);
    } finally { clearTimeout(timer); }
}

async function testLocalDir(dir, label, { current = false } = {}) {
    const abs = path.resolve(dir);
    await fsp.mkdir(abs, { recursive: true });
    const probe = path.join(abs, `.ichibi-probe-${randomUUID()}`);
    await fsp.writeFile(probe, 'ok');
    const back = await fsp.readFile(probe, 'utf8');
    await fsp.unlink(probe);
    if (back !== 'ok') throw new Error('read back a different value');
    const entries = (await fsp.readdir(abs)).filter(n => !n.startsWith('.'));
    const p = persistence(abs);
    return {
        label, ok: true,
        detail: `${abs} is writable${current ? ' (the lake\'s current location)' : entries.length ? ` (contains ${entries.length} item(s))` : ' (empty)'}`,
        warning: p && !p.mounted ? `${abs} is not on a mounted volume: its files are lost when the container is re-created. In Docker use a path under a mounted directory (default /app/data_files, /app/blobs).` : null,
        entries: entries.length,
    };
}

/**
 * A few of the lake's current data files (absolute paths), from different
 * tables, to check that new connection details still reach them.
 */
const SAMPLE_FILES = 3;
async function sampleLakeFiles() {
    const tables = await execute(`SELECT table_name FROM ducklake_table_info('ducklake') WHERE file_count > 0 ORDER BY file_size_bytes DESC LIMIT ${SAMPLE_FILES}`);
    const files = [];
    for (const t of tables) {
        const [row] = await execute(`SELECT data_file FROM ducklake_list_files('ducklake', '${String(t.table_name).replace(/'/g, "''")}') LIMIT 1`);
        if (row?.data_file) files.push({ table: t.table_name, file: row.data_file });
    }
    return files;
}

async function testS3(cfg, { current = false, expectFiles = [] } = {}) {
    const inst = await DuckDBInstance.create(':memory:');
    const conn = await inst.connect();
    try {
        await conn.run('INSTALL httpfs; LOAD httpfs;');
        if (cfg.provider === 'credential_chain') await conn.run('INSTALL aws; LOAD aws;');
        await conn.run(buildS3SecretSql(cfg));
        await conn.run('SET http_retries = 1');
        const probe = `${cfg.dataPath}_ichibi_probe/${randomUUID()}.parquet`;
        const run = async () => {
            await conn.run(`COPY (SELECT 'ok' AS probe) TO '${probe.replace(/'/g, "''")}' (FORMAT parquet)`);
            const back = (await conn.runAndReadAll(`SELECT probe FROM read_parquet('${probe.replace(/'/g, "''")}')`)).getRows();
            if (back[0]?.[0] !== 'ok') throw new Error('read back a different value');
            // Other files at the location (earlier probes do not count).
            const files = (await conn.runAndReadAll(`SELECT count(*) FROM glob('${(cfg.dataPath + '**').replace(/'/g, "''")}') WHERE file NOT LIKE '%/_ichibi_probe/%'`)).getRows();
            return Number(files[0][0]);
        };
        const others = await withTimeout(run(), TEST_TIMEOUT_MS, () => { try { conn.interrupt(); } catch (_) { /* ignore */ } });
        // Same location, maybe new endpoint or credentials: the catalog lists
        // files there, so they must still be readable (a different server or
        // account with an identically named bucket would not have them).
        const unreadable = [];
        for (const f of expectFiles) {
            try {
                await withTimeout(conn.run(`SELECT num_rows FROM parquet_file_metadata('${f.file.replace(/'/g, "''")}')`), TEST_TIMEOUT_MS,
                    () => { try { conn.interrupt(); } catch (_) { /* ignore */ } });
            } catch (err) {
                unreadable.push(`${f.file} (table ${f.table}): ${String(err.message || err).split('\n')[0].slice(0, 200)}`);
            }
        }
        if (unreadable.length) {
            return {
                label: 'Data files', ok: false,
                detail: `${unreadable.length} of ${expectFiles.length} sampled data files of the lake are not readable with these settings, e.g. ${unreadable[0]}. ` +
                    'The catalog still lists them at this location, so the endpoint and credentials must reach the server and account that hold them.',
            };
        }
        return {
            label: 'Data files', ok: true,
            detail: `Wrote and read back a probe file in ${cfg.dataPath}` +
                (current ? ` (the lake's current location)${expectFiles.length ? `; ${expectFiles.length} of the lake's data files are readable` : ''}. Lake maintenance removes the probe.`
                    : `${others > 0 ? ` (the location already holds ${others} other file(s))` : ''}. Lake maintenance removes the probe once the lake uses this location.`),
            entries: Math.max(0, others),
        };
    } finally {
        try { conn.closeSync(); } catch (_) { /* ignore */ }
    }
}

/** Check that the settings can be used: the data location and the BLOB directory. */
export async function testStorage(settings) {
    const results = [];
    const cfg = storageConfig(settings);
    const current = dataLocation(effectiveSettings()) === cfg.dataPath;
    try {
        if (cfg.kind === 'local') results.push(await testLocalDir(cfg.dataPath, 'Data files', { current }));
        else results.push(await testS3(cfg, { current, expectFiles: current ? await sampleLakeFiles() : [] }));
    } catch (err) {
        results.push({ label: 'Data files', ok: false, detail: `${describeStorage(cfg)}: ${String(err.message || err).split('\n')[0]}` });
    }
    if (settings.USE_FILESYSTEM_BLOBS === '1') {
        try { results.push(await testLocalDir(settings.BLOBS_PATH, 'BLOB directory')); } catch (err) {
            results.push({ label: 'BLOB directory', ok: false, detail: `${settings.BLOBS_PATH}: ${err.message}` });
        }
    }
    return { ok: results.every(r => r.ok), results };
}

// --- Save ---------------------------------------------------------------------------

function changedKeys(before, after) {
    return STORAGE_KEYS.filter(k => String(before[k] ?? '') !== String(after[k] ?? ''));
}

/**
 * Validate, test and save new settings. Returns what changed; the caller
 * restarts the gateway when `restart` is true.
 */
export async function saveStorage(form, { by } = {}) {
    const current = effectiveSettings();
    const next = fromForm(form, current);
    const changed = changedKeys(current, next);
    if (!changed.length) throw httpError(400, 'Nothing changed');
    if (!storageFileStatus().writable) {
        throw httpError(500, `Cannot write ${storageFileStatus().file}. In Docker, mount a directory there (docker/compose.yaml maps ./data/config to /app/config).`);
    }

    const moving = dataLocation(current) !== dataLocation(next);
    if (moving) {
        const blocking = await tablesWithFiles();
        if (blocking.length) {
            throw httpError(409, `The data files can only move to a new location while the lake is empty, and ${blocking.length} table(s) have data files: ` +
                `${blocking.slice(0, 10).map(t => t.table).join(', ')}${blocking.length > 10 ? ', ...' : ''}. ` +
                'Export them (Tables & Data -> Export), drop them, switch, then import them again.', { tables: blocking.slice(0, 25) });
        }
    }
    const blobChanged = changed.includes('USE_FILESYSTEM_BLOBS') || (next.USE_FILESYSTEM_BLOBS === '1' && changed.includes('BLOBS_PATH'));
    if (blobChanged && !form.confirmBlobChange) {
        throw httpError(400, 'Confirm the BLOB storage change: BLOBs stored so far stay where they are, so they can no longer be read if their format or directory changes.', { needsConfirmation: 'blobs' });
    }

    // Test whatever the new settings would connect to.
    const connectionKeys = ['DUCKLAKE_DATA_PATH', 'S3_ENDPOINT', 'S3_REGION', 'S3_ACCESS_KEY', 'S3_SECRET_KEY', 'S3_SESSION_TOKEN', 'S3_URL_STYLE', 'S3_USE_SSL', 'USE_FILESYSTEM_BLOBS', 'BLOBS_PATH'];
    if (changed.some(k => connectionKeys.includes(k))) {
        const test = await testStorage(next);
        if (!test.ok) throw httpError(400, `Connection test failed: ${test.results.filter(r => !r.ok).map(r => `${r.label}: ${r.detail}`).join('; ')}`, { test });
        if (moving && test.results[0]?.entries > 0) {
            throw httpError(409, `The new data location is not empty (${test.results[0].entries} item(s)). Choose an empty directory or bucket prefix so this lake's files do not mix with others.`, { test });
        }
    }

    saveStorageConfig(next, { by, effective: current, switchFrom: moving ? storageConfig(current).dataPath : null });
    const shown = (s) => redact(Object.fromEntries(changed.map(k => [k, s[k]])));
    return {
        changed,
        moving,
        from: shown(current),
        to: shown(next),
        summary: changed.map(k => `${k}: ${shown(current)[k] || '(empty)'} -> ${shown(next)[k] || '(empty)'}`).join(', '),
    };
}

export async function testStorageForm(form) {
    return testStorage(fromForm(form, effectiveSettings()));
}
