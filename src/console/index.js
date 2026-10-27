/**
 * Admin console: a password-protected web UI at /console for managing gateway
 * settings, API keys, logins and data retention. Mounted by src/index.js
 * BEFORE the global CORS middleware, so console responses never carry
 * permissive CORS headers. See src/console/registry.js to add sections.
 */
import express, { Router } from 'express';
import os from 'os';
import fs from 'fs';
import { createHash, randomBytes } from 'crypto';
import { fileURLToPath } from 'url';
import { execute, userQueryStats } from '../db.js';
import { checkReadiness, lakeStats } from '../health.js';
import { exportStats } from '../export.js';
import { kafkaLagSnapshot } from '../metrics.js';
import { SECTIONS, SETTINGS, getSettingDef, normaliseSettingValue, maskSecret } from './registry.js';
import * as store from './store.js';
import {
    ROLES, SESSION_COOKIE, validateUsername, validatePassword, hashPassword, verifyPassword,
    getDummyHash, passwordFingerprint, createSessionToken, verifySessionToken, parseCookies,
    sessionCookie, LoginThrottle, safeEqual, generateSetupCode, generateApiKey,
} from './auth.js';
import { startMaintenanceScheduler, saveMaintenanceConfig, startMaintenance, maintenanceOverview, describeSteps } from './maintenance.js';
import { checkForUpdate } from './version.js';
import { isSystemTable, normaliseRule, computeNextRun, matchTables, previewRule, startJob, startPruneScheduler, storageInfo, isDateType, isEpochType, describeConditions, CONDITION_OPS, isLegacyMultiTable } from './retention.js';
import { nextRuns, validateCron, SCHEDULE_PRESETS } from './cron.js';
import * as data from './data.js';
import { storageOverview, testStorageForm, saveStorage, autoRestart } from './storage-admin.js';
import * as ai from './ai.js';
import { importTempFile, receiveUpload } from '../import.js';
import {
    listUsers as listRbacUsers, getUserRoles, createUser as createRbacUser, updateUser as updateRbacUser,
    setUserRoles, isRbacConfigured, syncLegacyApiKeys,
} from '../rbac.js';

const PUBLIC_DIR = fileURLToPath(new URL('../../public/console/', import.meta.url));
const PACKAGE = JSON.parse(fs.readFileSync(new URL('../../package.json', import.meta.url), 'utf8'));
const STARTED_AT = Date.now();
const SETTINGS_REFRESH_MS = 30 * 1000;
const RBAC_ROLES = ['superuser', 'writer', 'reader'];

let ready = false;
let sessionSecret = null;
let setupCode = null;
const throttle = new LoginThrottle();

// Side effects run after a live setting changes.
const SETTING_HOOKS = {};

/** Opaque, stable id for an API key so raw keys never appear in URLs. */
function keyId(apiKey) {
    return createHash('sha256').update(String(apiKey)).digest('hex').slice(0, 16);
}

/** One-line summary of a rule's criteria and schedule for the audit log. */
function describeRule(rule) {
    const parts = [`tables ${rule.tablePattern}${rule.excludePattern ? ` except ${rule.excludePattern}` : ''}`];
    if (rule.maxAgeValue) parts.push(`older than ${rule.maxAgeValue} ${rule.maxAgeUnit} by ${rule.dateColumn}${rule.epochUnit && rule.epochUnit !== 'auto' ? ` (epoch ${rule.epochUnit})` : ''}`);
    if (rule.maxRows) parts.push(`keep newest ${rule.maxRows} by ${rule.orderColumn || 'insertion order'}`);
    if (rule.conditions) parts.push(`matching ${describeConditions(rule.conditions)}`);
    parts.push(`schedule "${rule.schedule}"`, rule.enabled ? 'enabled' : 'disabled');
    if (rule.safetyLimit) parts.push(`safety limit ${rule.safetyLimit}`);
    if (rule.reclaimStorage) parts.push('reclaim storage');
    return parts.join(', ');
}

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

function clientIp(req) {
    return req.ip || req.socket?.remoteAddress || '';
}

/**
 * Externally reachable base URL, used only for log messages. Inside Docker
 * the process cannot see the published host port, so compose passes
 * PUBLIC_URL; natively it falls back to localhost on the listening port.
 */
export function publicBaseUrl() {
    return (process.env.PUBLIC_URL || `http://localhost:${process.env.PORT || 3000}`).trim().replace(/\/+$/, '');
}

function sessionTtlMs() {
    const hours = Math.max(1, parseInt(process.env.CONSOLE_SESSION_TTL_HOURS || '12', 10) || 12);
    return hours * 60 * 60 * 1000;
}

/**
 * Load console setting overrides into process.env. Called early in startup,
 * BEFORE the RBAC bootstrap: the bootstrap promotes and re-activates every
 * API_KEYS entry, so it must see the console's list, otherwise a key revoked
 * in the console would be re-activated on every restart.
 */
export async function loadSettingOverrides() {
    await store.initConsoleTables();
    await store.refreshOverrides();
}

/**
 * Make sure a setup code exists while no console login does (at boot, or
 * after every login was deleted via the CLI) and print it to the log.
 */
function ensureSetupCode() {
    if (setupCode) return;
    setupCode = process.env.CONSOLE_SETUP_TOKEN || generateSetupCode();
    const bar = '='.repeat(64);
    console.log(`\n${bar}\n[Console] First-run setup required. Open ${publicBaseUrl()}/console and enter this setup code:\n\n    ${process.env.CONSOLE_SETUP_TOKEN ? '(value of CONSOLE_SETUP_TOKEN)' : setupCode}\n\n${bar}\n`);
}

/**
 * Initialise console tables, load setting overrides and start background
 * jobs. Called from the gateway's startup sequence once DuckLake is attached.
 */
let refreshTimer = null;

export async function initConsole() {
    await store.initConsoleTables();
    await store.refreshOverrides();
    sessionSecret = await store.getOrCreateSessionSecret();

    if (await store.countUsers() === 0) ensureSetupCode();

    // initConsole() is re-run when a later startup step fails (src/index.js),
    // so start the refresh interval only once.
    if (!refreshTimer) {
        refreshTimer = setInterval(() => {
            store.refreshOverrides().catch(err => console.warn('[Console] Settings refresh failed:', err.message || err));
        }, SETTINGS_REFRESH_MS);
        refreshTimer.unref?.();
    }

    await startPruneScheduler();
    // Lake maintenance (flush/merge/rewrite/expire/cleanup); also performs the
    // startup compaction run, claimed so only one instance does it.
    await startMaintenanceScheduler();
    ready = true;
    console.log(`[Console] Admin console ready at ${publicBaseUrl()}/console`);
}

// --- Middleware -------------------------------------------------------------------

function securityHeaders(req, res, next) {
    res.setHeader('Content-Security-Policy',
        "default-src 'self'; img-src 'self' data:; style-src 'self'; script-src 'self'; " +
        "connect-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'self'");
    res.setHeader('X-Frame-Options', 'DENY');
    res.setHeader('X-Content-Type-Options', 'nosniff');
    res.setHeader('Referrer-Policy', 'no-referrer');
    next();
}

function requireReady(req, res, next) {
    if (!ready) return res.status(503).json({ error: 'The console is still starting; try again shortly.' });
    next();
}

/**
 * CSRF defence for state-changing requests: a custom header (which a cross-
 * site form or simple request cannot set) plus an Origin check. Session
 * cookies are also SameSite=Strict.
 */
function requireSameOrigin(req, res, next) {
    if (req.method === 'GET' || req.method === 'HEAD') return next();
    if (req.get('X-Console-Request') !== '1') {
        return res.status(403).json({ error: 'Missing X-Console-Request header' });
    }
    const origin = req.get('Origin');
    if (origin) {
        let originHost;
        try { originHost = new URL(origin).host; } catch { originHost = null; }
        if (originHost !== req.get('Host')) return res.status(403).json({ error: 'Cross-origin request rejected' });
    }
    next();
}

async function loadSession(req) {
    const token = parseCookies(req.headers.cookie)[SESSION_COOKIE];
    const claims = verifySessionToken(token, sessionSecret);
    if (!claims) return null;
    const user = await store.findUser(claims.username);
    if (!user || passwordFingerprint(user.password_hash) !== claims.fingerprint) return null;
    return { username: user.username, role: user.role };
}

async function requireSession(req, res, next) {
    try {
        const session = await loadSession(req);
        if (!session) return res.status(401).json({ error: 'Not signed in' });
        req.consoleUser = session;
        next();
    } catch (err) { next(err); }
}

function requireAdmin(req, res, next) {
    if (req.consoleUser?.role !== 'admin') return res.status(403).json({ error: 'Admin role required' });
    next();
}

function issueSession(req, res, user) {
    const ttl = sessionTtlMs();
    const token = createSessionToken({ username: user.username, passwordHash: user.password_hash, ttlMs: ttl, secret: sessionSecret });
    res.setHeader('Set-Cookie', sessionCookie(token, { maxAgeMs: ttl, secure: req.secure }));
}

const wrap = (fn) => (req, res, next) => Promise.resolve(fn(req, res, next)).catch(next);

// --- Helpers for API-key and settings payloads -------------------------------------------

function currentApiKeys() {
    return (process.env.API_KEYS || process.env.API_KEY || '').split(',').map(k => k.trim()).filter(Boolean);
}

/**
 * Where an RBAC user is defined, if not in the console. The startup bootstrap
 * re-activates these users and re-adds their roles from the environment, so
 * console changes to them would silently revert on restart.
 *   'env'      - RBAC_SUPERUSERS / RBAC_USERS in docker/.env
 *   'api-keys' - the API_KEYS list (legacy mode), managed on the API Keys page
 */
function rbacManagedBy(apiKey) {
    const superusers = (process.env.RBAC_SUPERUSERS || '').split(',').map(k => k.trim()).filter(Boolean);
    let users = [];
    try { users = JSON.parse(process.env.RBAC_USERS || '[]'); } catch { users = []; }
    const envKeys = new Set([...superusers, ...(Array.isArray(users) ? users : []).map(u => u?.apiKey || u?.api_key).filter(Boolean)]);
    if (envKeys.has(apiKey)) return 'env';
    if (!isRbacConfigured() && currentApiKeys().includes(apiKey)) return 'api-keys';
    return null;
}

async function saveApiKeys(keys, req, action, detail) {
    if (keys.length === 0 && !isRbacConfigured()) {
        throw httpError(400, 'At least one API key is required unless RBAC users are configured.');
    }
    const before = currentApiKeys();
    await store.setOverride('API_KEYS', keys.join(','), req.consoleUser.username);
    const { removed } = await syncLegacyApiKeys(keys, before);
    await store.audit(req.consoleUser.username, action, detail, clientIp(req));
    return { removed };
}

function settingPayload(def) {
    const d = store.describeSetting(def);
    const secret = def.type === 'secret';
    return {
        key: def.key,
        section: def.section,
        label: def.label,
        help: def.help || '',
        type: def.type,
        options: def.options || null,
        min: def.min ?? null,
        max: def.max ?? null,
        editable: !!def.editable,
        live: !!def.live,
        default: def.default ?? '',
        value: secret ? maskSecret(d.value) : d.value,
        source: d.source,
        envValue: secret ? (d.envValue ? maskSecret(d.envValue) : null) : d.envValue,
        updatedAt: d.updatedAt,
        updatedBy: d.updatedBy,
    };
}

// --- Router -----------------------------------------------------------------------

export function createConsoleRouter() {
    const router = Router();
    router.use(securityHeaders);

    const api = Router();
    api.use(express.json({ limit: '64kb' }));
    api.use((req, res, next) => { res.setHeader('Cache-Control', 'no-store'); next(); });
    api.use(requireReady);
    api.use(requireSameOrigin);

    // ---- Public: state, setup, login --------------------------------------------
    api.get('/state', wrap(async (req, res) => {
        const setupRequired = (await store.countUsers()) === 0;
        if (setupRequired) ensureSetupCode();
        const user = setupRequired ? null : await loadSession(req);
        res.json({ setupRequired, user, product: 'ichibi-lake', version: PACKAGE.version });
    }));

    api.post('/setup', wrap(async (req, res) => {
        const ip = clientIp(req);
        const wait = throttle.retryAfterSeconds([`setup:${ip}`]);
        if (wait) return res.status(429).set('Retry-After', String(wait)).json({ error: `Too many attempts. Try again in ${wait}s.` });
        if ((await store.countUsers()) > 0) return res.status(409).json({ error: 'Setup has already been completed.' });

        const { setupCode: code, username, password } = req.body || {};
        if (!setupCode || !safeEqual(code, setupCode)) {
            throttle.fail([`setup:${ip}`]);
            return res.status(403).json({ error: 'Invalid setup code. It is printed in the gateway log on startup.' });
        }
        const err = validateUsername(username) || validatePassword(password);
        if (err) return res.status(400).json({ error: err });

        const passwordHash = await hashPassword(password);
        if ((await store.countUsers()) > 0) return res.status(409).json({ error: 'Setup has already been completed.' });
        await store.createUser({ username, passwordHash, role: 'admin', createdBy: 'setup' });
        setupCode = null;
        throttle.succeed([`setup:${ip}`]);
        await store.audit(username, 'console.setup', 'Initial admin account created', ip);
        await store.touchLastLogin(username);
        issueSession(req, res, { username, password_hash: passwordHash });
        res.json({ success: true, user: { username, role: 'admin' } });
    }));

    api.post('/login', wrap(async (req, res) => {
        const ip = clientIp(req);
        const { username, password } = req.body || {};
        const keys = [`ip:${ip}`, `user:${String(username || '').toLowerCase()}`];
        const wait = throttle.retryAfterSeconds(keys);
        if (wait) return res.status(429).set('Retry-After', String(wait)).json({ error: `Too many failed attempts. Try again in ${wait}s.` });

        const user = typeof username === 'string' ? await store.findUser(username) : null;
        const ok = await verifyPassword(String(password ?? ''), user ? user.password_hash : await getDummyHash());
        if (!user || !ok) {
            throttle.fail(keys);
            await store.audit(String(username || '').slice(0, 64), 'console.login_failed', '', ip);
            return res.status(401).json({ error: 'Invalid username or password' });
        }
        throttle.succeed(keys);
        await store.touchLastLogin(user.username);
        await store.audit(user.username, 'console.login', '', ip);
        issueSession(req, res, user);
        res.json({ success: true, user: { username: user.username, role: user.role } });
    }));

    api.post('/logout', (req, res) => {
        res.setHeader('Set-Cookie', sessionCookie('', { maxAgeMs: 0, secure: req.secure }));
        res.json({ success: true });
    });

    // ---- Everything below requires a session ---------------------------------------
    api.use(requireSession);

    api.get('/me', (req, res) => res.json({ user: req.consoleUser }));

    api.get('/version', wrap(async (req, res) => {
        res.json(await checkForUpdate(PACKAGE.version));
    }));

    api.post('/me/password', wrap(async (req, res) => {
        const { currentPassword, newPassword } = req.body || {};
        const user = await store.findUser(req.consoleUser.username);
        if (!user || !(await verifyPassword(String(currentPassword ?? ''), user.password_hash))) {
            return res.status(400).json({ error: 'Current password is incorrect' });
        }
        const err = validatePassword(newPassword);
        if (err) return res.status(400).json({ error: err });
        const passwordHash = await hashPassword(newPassword);
        await store.updateUserPassword(user.username, passwordHash);
        await store.audit(user.username, 'console.password_changed', '', clientIp(req));
        // Changing the password invalidates other sessions; re-issue ours.
        issueSession(req, res, { username: user.username, password_hash: passwordHash });
        res.json({ success: true });
    }));

    // ---- Navigation and settings ------------------------------------------------
    api.get('/sections', (req, res) => {
        res.json({
            sections: SECTIONS,
            settings: SETTINGS.filter(s => !s.hidden).map(settingPayload),
        });
    });

    api.put('/settings/:key', requireAdmin, wrap(async (req, res) => {
        const def = getSettingDef(req.params.key);
        if (!def) return res.status(404).json({ error: 'Unknown setting' });
        if (def.hidden) return res.status(400).json({ error: `${def.label} is managed on its own page` });
        const { value, error } = normaliseSettingValue(def, req.body?.value);
        if (error) return res.status(400).json({ error });
        const before = store.describeSetting(def).value;
        await store.setOverride(def.key, value, req.consoleUser.username);
        SETTING_HOOKS[def.key]?.();
        await store.audit(req.consoleUser.username, 'setting.update', `${def.key}: ${before} -> ${value}`, clientIp(req));
        res.json({ success: true, setting: settingPayload(def) });
    }));

    api.delete('/settings/:key', requireAdmin, wrap(async (req, res) => {
        const def = getSettingDef(req.params.key);
        if (!def || !def.editable || def.hidden) return res.status(404).json({ error: 'Unknown or read-only setting' });
        await store.clearOverride(def.key);
        SETTING_HOOKS[def.key]?.();
        await store.audit(req.consoleUser.username, 'setting.reset', `${def.key} reset to environment/default`, clientIp(req));
        res.json({ success: true, setting: settingPayload(def) });
    }));

    // ---- Storage (settings file + restart; see src/console/storage-admin.js) ---------
    api.get('/storage', wrap(async (req, res) => {
        res.json(await storageOverview());
    }));

    api.post('/storage/test', requireAdmin, wrap(async (req, res) => {
        res.json(await testStorageForm(req.body));
    }));

    // Saves the settings as pending and, when a supervisor will start the
    // process again (Docker restart policy, PM2), restarts the gateway.
    api.put('/storage', requireAdmin, wrap(async (req, res) => {
        const result = await saveStorage(req.body, { by: req.consoleUser.username });
        const restart = autoRestart() && req.body?.restart !== false;
        await store.audit(req.consoleUser.username, 'storage.update',
            `${result.summary}${result.moving ? ' (data files move at restart)' : ''}${restart ? '; restarting' : '; restart required'}`, clientIp(req));
        if (restart) {
            console.log(`[Storage] Settings saved by ${req.consoleUser.username}; restarting to apply them.`);
            res.on('finish', () => setTimeout(() => process.exit(0), 300));
        }
        res.json({ success: true, ...result, restarting: restart });
    }));

    // ---- Overview -----------------------------------------------------------------
    api.get('/overview', wrap(async (req, res) => {
        const [version] = await execute(`SELECT version() AS v`);
        const tables = await execute(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'`
        );
        const ext = await execute(`SELECT loaded FROM duckdb_extensions() WHERE extension_name = 'duckpgq'`);
        const rules = await store.listPruneRules();
        const overridden = SETTINGS.filter(s => store.describeSetting(s).source === 'console').length;
        res.json({
            gateway: { version: PACKAGE.version, uptimeSeconds: Math.round((Date.now() - STARTED_AT) / 1000), hostname: os.hostname(), node: process.version },
            duckdb: { version: version?.v || '', duckpgqLoaded: !!ext[0]?.loaded },
            data: { userTables: tables.filter(t => !isSystemTable(t.table_name)).length },
            access: {
                apiKeys: currentApiKeys().length,
                rbacMode: isRbacConfigured() ? 'rbac' : 'legacy',
                rbacUsers: (await listRbacUsers()).length,
                consoleLogins: await store.countUsers(),
            },
            retention: { rules: rules.length, enabled: rules.filter(r => r.enabled).length },
            settings: { overridden },
            kafka: { brokers: process.env.KAFKA_BROKERS || '' },
            health: {
                ...(await checkReadiness()),
                userQuery: userQueryStats(),
                exports: exportStats(),
                lake: await lakeStats(),
                kafkaLag: kafkaLagSnapshot(),
            },
        });
    }));

    // ---- API keys -------------------------------------------------------------------
    api.get('/api-keys', wrap(async (req, res) => {
        const rbacUsers = await listRbacUsers();
        const byKey = new Map(rbacUsers.map(u => [u.api_key, u]));
        res.json({
            source: store.describeSetting(getSettingDef('API_KEYS')).source,
            rbacMode: isRbacConfigured(),
            keys: currentApiKeys().map(k => ({
                id: keyId(k),
                masked: maskSecret(k),
                rbacName: byKey.get(k)?.name || null,
            })),
        });
    }));

    api.post('/api-keys', requireAdmin, wrap(async (req, res) => {
        const supplied = typeof req.body?.key === 'string' ? req.body.key.trim() : '';
        if (supplied && !/^[A-Za-z0-9_\-.]{16,256}$/.test(supplied)) {
            return res.status(400).json({ error: 'Keys must be 16-256 characters: letters, digits, _ - .' });
        }
        const key = supplied || generateApiKey();
        const keys = currentApiKeys();
        if (keys.includes(key)) return res.status(409).json({ error: 'That key already exists' });
        await saveApiKeys([...keys, key], req, 'api_key.add', `Added ${maskSecret(key)}`);
        // The full key is returned exactly once, at creation time.
        res.json({ success: true, key, id: keyId(key) });
    }));

    // Drop the console override and go back to the docker/.env API_KEYS list.
    api.post('/api-keys/reset', requireAdmin, wrap(async (req, res) => {
        const before = currentApiKeys();
        await store.clearOverride('API_KEYS');
        const { removed } = await syncLegacyApiKeys(currentApiKeys(), before);
        await store.audit(req.consoleUser.username, 'api_key.reset', 'API keys reverted to docker/.env', clientIp(req));
        res.json({ success: true, removed });
    }));

    api.delete('/api-keys/:id', requireAdmin, wrap(async (req, res) => {
        const keys = currentApiKeys();
        const target = keys.find(k => keyId(k) === req.params.id);
        if (!target) return res.status(404).json({ error: 'API key not found' });
        const { removed } = await saveApiKeys(keys.filter(k => k !== target), req, 'api_key.revoke', `Revoked ${maskSecret(target)}`);
        res.json({ success: true, removed });
    }));

    // ---- RBAC users ---------------------------------------------------------------------
    api.get('/rbac/users', wrap(async (req, res) => {
        const users = await listRbacUsers();
        const out = [];
        for (const u of users) {
            out.push({
                id: keyId(u.api_key), masked: maskSecret(u.api_key), name: u.name, email: u.email,
                active: u.is_active === true || u.is_active === 'true', createdBy: u.created_by, createdAt: u.created_at ? String(u.created_at) : null,
                roles: await getUserRoles(u.api_key),
                managedBy: rbacManagedBy(u.api_key),
            });
        }
        res.json({ rbacMode: isRbacConfigured(), users: out });
    }));

    api.post('/rbac/users', requireAdmin, wrap(async (req, res) => {
        const { name, email, roles } = req.body || {};
        if (!name || typeof name !== 'string' || name.length > 128) return res.status(400).json({ error: 'A name is required' });
        const roleList = Array.isArray(roles) ? roles : [];
        if (roleList.some(r => !RBAC_ROLES.includes(r))) return res.status(400).json({ error: `Roles must be from: ${RBAC_ROLES.join(', ')}` });
        const apiKey = generateApiKey();
        await createRbacUser(apiKey, name, typeof email === 'string' ? email.slice(0, 256) : '', roleList, `console:${req.consoleUser.username}`);
        await store.audit(req.consoleUser.username, 'rbac.user_create', `${name} (${roleList.join(', ') || 'no roles'})`, clientIp(req));
        res.json({ success: true, apiKey, id: keyId(apiKey) });
    }));

    api.patch('/rbac/users/:id', requireAdmin, wrap(async (req, res) => {
        const users = await listRbacUsers();
        const user = users.find(u => keyId(u.api_key) === req.params.id);
        if (!user) return res.status(404).json({ error: 'User not found' });
        const managedBy = rbacManagedBy(user.api_key);
        if (managedBy === 'env') {
            return res.status(409).json({ error: 'This user is defined by RBAC_SUPERUSERS / RBAC_USERS in docker/.env and is re-applied on every restart. Change it there.' });
        }
        if (managedBy === 'api-keys') {
            return res.status(409).json({ error: 'This user comes from the API_KEYS list and is re-applied on every restart. Revoke the key on the API Keys page instead.' });
        }
        const { active, roles } = req.body || {};
        const changes = [];
        if (typeof active === 'boolean') {
            await updateRbacUser(user.api_key, { is_active: active });
            changes.push(active ? 'activated' : 'deactivated');
        }
        if (Array.isArray(roles)) {
            if (roles.some(r => !RBAC_ROLES.includes(r))) return res.status(400).json({ error: `Roles must be from: ${RBAC_ROLES.join(', ')}` });
            await setUserRoles(user.api_key, roles, `console:${req.consoleUser.username}`);
            changes.push(`roles=${roles.join('|') || 'none'}`);
        }
        await store.audit(req.consoleUser.username, 'rbac.user_update', `${user.name || maskSecret(user.api_key)}: ${changes.join(', ')}`, clientIp(req));
        res.json({ success: true });
    }));

    // ---- Console logins -------------------------------------------------------------------
    api.get('/logins', wrap(async (req, res) => {
        res.json({ roles: ROLES, users: await store.listUsers() });
    }));

    api.post('/logins', requireAdmin, wrap(async (req, res) => {
        const { username, password, role } = req.body || {};
        const err = validateUsername(username) || validatePassword(password) || (ROLES.includes(role) ? null : `Role must be one of: ${ROLES.join(', ')}`);
        if (err) return res.status(400).json({ error: err });
        if (await store.findUser(username)) return res.status(409).json({ error: 'That username is already taken' });
        await store.createUser({ username, passwordHash: await hashPassword(password), role, createdBy: req.consoleUser.username });
        await store.audit(req.consoleUser.username, 'login.create', `${username} (${role})`, clientIp(req));
        res.json({ success: true });
    }));

    api.patch('/logins/:username', requireAdmin, wrap(async (req, res) => {
        const user = await store.findUser(req.params.username);
        if (!user) return res.status(404).json({ error: 'Login not found' });
        const { role, password } = req.body || {};
        const changes = [];
        if (role !== undefined) {
            if (!ROLES.includes(role)) return res.status(400).json({ error: `Role must be one of: ${ROLES.join(', ')}` });
            if (user.role === 'admin' && role !== 'admin' && (await store.countAdmins()) <= 1) {
                return res.status(400).json({ error: 'At least one admin login must remain' });
            }
            await store.updateUserRole(user.username, role);
            changes.push(`role=${role}`);
        }
        if (password !== undefined) {
            const err = validatePassword(password);
            if (err) return res.status(400).json({ error: err });
            await store.updateUserPassword(user.username, await hashPassword(password));
            changes.push('password reset');
        }
        await store.audit(req.consoleUser.username, 'login.update', `${user.username}: ${changes.join(', ')}`, clientIp(req));
        res.json({ success: true });
    }));

    api.delete('/logins/:username', requireAdmin, wrap(async (req, res) => {
        const user = await store.findUser(req.params.username);
        if (!user) return res.status(404).json({ error: 'Login not found' });
        if (user.username.toLowerCase() === req.consoleUser.username.toLowerCase()) {
            return res.status(400).json({ error: 'You cannot delete your own login' });
        }
        if (user.role === 'admin' && (await store.countAdmins()) <= 1) {
            return res.status(400).json({ error: 'At least one admin login must remain' });
        }
        await store.deleteUser(user.username);
        await store.audit(req.consoleUser.username, 'login.delete', user.username, clientIp(req));
        res.json({ success: true });
    }));

    // ---- Pruning (retention rules, jobs, run log) ------------------------------------------
    async function prunableTables() {
        const rows = await execute(
            `SELECT table_name, column_name, data_type FROM information_schema.columns
             WHERE table_catalog = 'ducklake' AND table_schema = 'main' ORDER BY table_name, ordinal_position`);
        const tables = new Map();
        for (const r of rows) {
            if (isSystemTable(r.table_name)) continue;
            if (!tables.has(r.table_name)) tables.set(r.table_name, { name: r.table_name, columns: [], types: {}, dateColumns: [], epochColumns: [], numericColumns: [] });
            const t = tables.get(r.table_name);
            t.columns.push(r.column_name);
            t.types[r.column_name] = String(r.data_type);
            if (isDateType(r.data_type)) t.dateColumns.push(r.column_name);
            else if (isEpochType(r.data_type)) {
                // Numbers usable as Unix epoch times; names that suggest a time are listed first.
                if (/(time|date|_at$|^ts$|_ts$|^ts_|created|updated|epoch)/i.test(r.column_name)) t.epochColumns.push(r.column_name);
                else t.numericColumns.push(r.column_name);
            }
        }
        return [...tables.values()];
    }

    function ruleView(rule, tableNames) {
        return { ...rule, matches: matchTables(tableNames, rule.tablePattern, rule.excludePattern), legacyMultiTable: isLegacyMultiTable(rule) };
    }

    api.get('/retention', wrap(async (req, res) => {
        const tables = await prunableTables();
        const names = tables.map(t => t.name);
        res.json({
            rules: (await store.listPruneRules()).map(r => ruleView(r, names)),
            tables,
            presets: SCHEDULE_PRESETS,
            conditionOps: CONDITION_OPS,
            storage: { ...storageInfo(), setting: settingPayload(getSettingDef('SNAPSHOT_RETENTION_DAYS')) },
        });
    }));

    api.post('/retention/rules', requireAdmin, wrap(async (req, res) => {
        const rule = normaliseRule(req.body || {});
        const id = randomBytes(8).toString('hex');
        await store.createPruneRule({ ...rule, id, nextRunMs: rule.enabled ? computeNextRun(rule.schedule) : null, user: req.consoleUser.username });
        await store.audit(req.consoleUser.username, 'prune.rule_create', `${rule.name}: ${describeRule(rule)}`, clientIp(req));
        res.json({ success: true, id });
    }));

    api.put('/retention/rules/:id', requireAdmin, wrap(async (req, res) => {
        const existing = await store.getPruneRule(req.params.id);
        if (!existing) return res.status(404).json({ error: 'Rule not found' });
        const rule = normaliseRule(req.body || {});
        const reschedule = rule.schedule !== existing.schedule || rule.enabled !== existing.enabled || !existing.nextRunMs;
        const nextRunMs = !rule.enabled ? null : reschedule ? computeNextRun(rule.schedule) : existing.nextRunMs;
        await store.updatePruneRule(req.params.id, { ...rule, nextRunMs, user: req.consoleUser.username });
        await store.audit(req.consoleUser.username, 'prune.rule_update', `${rule.name}: ${describeRule(rule)}`, clientIp(req));
        res.json({ success: true });
    }));

    // Enable/disable only (used by the list toggle); works for every rule,
    // including multi-table rules saved before the one-table change.
    api.patch('/retention/rules/:id', requireAdmin, wrap(async (req, res) => {
        const existing = await store.getPruneRule(req.params.id);
        if (!existing) return res.status(404).json({ error: 'Rule not found' });
        if (typeof req.body?.enabled !== 'boolean') return res.status(400).json({ error: 'Send { "enabled": true | false }' });
        const enabled = req.body.enabled;
        await store.setRuleEnabled(existing.id, enabled, enabled ? computeNextRun(existing.schedule) : null, req.consoleUser.username);
        await store.audit(req.consoleUser.username, 'prune.rule_update', `${existing.name}: ${enabled ? 'enabled' : 'disabled'}`, clientIp(req));
        res.json({ success: true });
    }));

    // Dry run of a saved rule exactly as stored.
    api.post('/retention/rules/:id/preview', wrap(async (req, res) => {
        const existing = await store.getPruneRule(req.params.id);
        if (!existing) return res.status(404).json({ error: 'Rule not found' });
        res.json(await previewRule(existing, { validated: true }));
    }));

    api.delete('/retention/rules/:id', requireAdmin, wrap(async (req, res) => {
        const existing = await store.getPruneRule(req.params.id);
        if (!existing) return res.status(404).json({ error: 'Rule not found' });
        await store.deletePruneRule(req.params.id);
        await store.audit(req.consoleUser.username, 'prune.rule_delete', existing.name, clientIp(req));
        res.json({ success: true });
    }));

    api.post('/retention/rules/:id/run', requireAdmin, wrap(async (req, res) => {
        const jobId = await startJob(req.params.id, { trigger: 'manual', username: req.consoleUser.username });
        res.status(202).json({ success: true, jobId });
    }));

    // Dry run. Read-only, so viewers may use it too.
    api.post('/retention/preview', wrap(async (req, res) => {
        res.json(await previewRule(req.body || {}));
    }));

    api.post('/retention/schedule', wrap(async (req, res) => {
        const cron = String(req.body?.schedule || '');
        const error = validateCron(cron);
        if (error) return res.json({ valid: false, error });
        res.json({ valid: true, next: nextRuns(cron, 3).map(d => d.getTime()) });
    }));

    api.get('/retention/runs', wrap(async (req, res) => {
        res.json({ jobs: await store.listRuns({ ruleId: req.query.rule || null, limit: req.query.limit }) });
    }));

    // ---- Lake maintenance -------------------------------------------------------------------
    api.get('/maintenance', wrap(async (req, res) => {
        res.json(await maintenanceOverview());
    }));

    api.put('/maintenance', requireAdmin, wrap(async (req, res) => {
        const cfg = await saveMaintenanceConfig(req.body || {}, req.consoleUser.username);
        await store.audit(req.consoleUser.username, 'maintenance.update',
            `${cfg.enabled ? `enabled, ${cfg.schedule}` : 'disabled'}; steps: ${describeSteps(cfg.steps)}` +
            `${cfg.deleteThreshold !== null ? `; rewrite threshold ${cfg.deleteThreshold}` : ''}; at startup: ${cfg.runAtStartup ? 'yes' : 'no'}`, clientIp(req));
        res.json({ success: true });
    }));

    api.post('/maintenance/run', requireAdmin, wrap(async (req, res) => {
        const runId = await startMaintenance({ trigger: 'manual', username: req.consoleUser.username });
        res.status(202).json({ success: true, runId });
    }));

    // ---- Tables & data ------------------------------------------------------------------------
    // Reads are open to every console role; writes need admin and are audited
    // (cell values are deliberately not copied into the audit log).
    const audit = (req, action, detail) => store.audit(req.consoleUser.username, action, detail, clientIp(req));

    api.get('/data/tables', wrap(async (req, res) => {
        res.json({ tables: await data.listTables(), columnTypes: data.COLUMN_TYPES, pageSizes: data.PAGE_SIZES, filterOps: data.FILTER_OPS });
    }));

    api.post('/data/tables', requireAdmin, wrap(async (req, res) => {
        const { name, columns } = req.body || {};
        const result = await data.createTable(name, columns);
        await audit(req, 'table.create', `${name} (${(columns || []).map(c => `${c.name} ${c.type}`).join(', ')})`);
        res.json({ success: true, ...result });
    }));

    api.get('/data/tables/:table/schema', wrap(async (req, res) => {
        res.json(await data.getSchema(req.params.table));
    }));

    api.post('/data/tables/:table/rows/query', wrap(async (req, res) => {
        const { page, pageSize, sort, filters, search } = req.body || {};
        res.json(await data.readRows(req.params.table, { page, pageSize, sort, filters, search }));
    }));

    // Download several tables (?tables=a,b,c; omitted = every user table) as
    // one ZIP, read in one transaction. Read-only, so open to every console role.
    api.get('/data/export', wrap(async (req, res) => {
        const { format = 'parquet' } = req.query;
        const tables = req.query.tables ? String(req.query.tables).split(',').map(t => t.trim()).filter(Boolean) : [];
        await audit(req, 'tables.export', `${tables.length ? `${tables.length} table(s): ${tables.join(', ').slice(0, 500)}` : 'all tables'} as ${format} (zip)`);
        await data.exportTables(tables, { format: String(format) }, res);
    }));

    // Download a table (or the rows matching the grid's filters/search/sort).
    // Read-only, so open to every console role; audited like other data access
    // that leaves the console.
    api.get('/data/tables/:table/export', wrap(async (req, res) => {
        const { format = 'parquet', search = '', limit } = req.query;
        let filters = [];
        let sort = null;
        try {
            if (req.query.filters) filters = JSON.parse(String(req.query.filters));
            if (req.query.sort) sort = JSON.parse(String(req.query.sort));
        } catch { return res.status(400).json({ error: 'filters and sort must be JSON' }); }
        await audit(req, 'table.export', `${req.params.table} as ${format}` +
            (filters.length || search ? ` (filtered: ${filters.length} filter(s)${search ? ', search' : ''})` : ''));
        await data.exportTable(req.params.table, { format: String(format), filters, search: String(search), sort, limit }, res);
    }));

    // Import a CSV / Parquet / JSON file uploaded as the raw request body.
    // ?dryRun=1 only reads it and returns a preview and the schema plan; the
    // console then sends the same file again without dryRun to write it.
    api.post('/data/import', requireAdmin, wrap(async (req, res) => {
        const filename = String(req.query.filename || '');
        const format = data.detectImportFormat(req.query.format ? String(req.query.format) : '', filename);
        const options = {
            format,
            table: String(req.query.table || ''),
            mode: String(req.query.mode || 'create'),
            dryRun: req.query.dryRun === '1' || req.query.dryRun === 'true',
            addColumns: req.query.addColumns !== '0' && req.query.addColumns !== 'false',
        };
        const file = await importTempFile(format);
        try {
            await receiveUpload(req, file);
            const result = await data.importFile(file, options);
            if (!options.dryRun) {
                await audit(req, 'table.import', `${result.table}: ${result.inserted} row(s) from ${filename || 'upload'} (${format}, ${options.mode})` +
                    (result.added?.length ? `; added column(s) ${result.added.join(', ')}` : ''));
            }
            res.json({ success: true, ...result });
        } finally {
            fs.promises.unlink(file).catch(() => {});
        }
    }));

    api.post('/data/tables/:table/rows', requireAdmin, wrap(async (req, res) => {
        const result = await data.insertRow(req.params.table, req.body?.values);
        await audit(req, 'row.insert', req.params.table);
        res.json({ success: true, ...result });
    }));

    api.patch('/data/tables/:table/rows/:rowid', requireAdmin, wrap(async (req, res) => {
        const { column, value, isNull, original } = req.body || {};
        const result = await data.updateCell(req.params.table, req.params.rowid, { column, value, isNull: !!isNull, original });
        await audit(req, 'row.update', `${req.params.table} row ${req.params.rowid}: ${column}`);
        res.json({ success: true, ...result });
    }));

    api.post('/data/tables/:table/rows/delete', requireAdmin, wrap(async (req, res) => {
        const result = await data.deleteRows(req.params.table, req.body?.rowids);
        await audit(req, 'row.delete', `${req.params.table}: ${result.deleted} row(s)`);
        res.json({ success: true, ...result });
    }));

    api.post('/data/tables/:table/columns', requireAdmin, wrap(async (req, res) => {
        const result = await data.addColumn(req.params.table, req.body || {});
        await audit(req, 'schema.add_column', `${req.params.table}.${req.body?.name} ${req.body?.type}`);
        res.json({ success: true, ...result });
    }));

    api.patch('/data/tables/:table/columns/:column', requireAdmin, wrap(async (req, res) => {
        const result = await data.alterColumn(req.params.table, req.params.column, req.body || {});
        if (result.applied.length) await audit(req, 'schema.alter_column', `${req.params.table}.${req.params.column}: ${result.applied.join(', ')}`);
        res.json({ success: true, ...result });
    }));

    api.post('/data/tables/:table/columns/:column/type-preview', requireAdmin, wrap(async (req, res) => {
        res.json(await data.previewTypeChange(req.params.table, req.params.column, req.body?.type));
    }));

    api.put('/data/tables/:table/keys', requireAdmin, wrap(async (req, res) => {
        const result = await data.setKeys(req.params.table, req.body?.keys);
        await audit(req, 'schema.set_keys', `${req.params.table}: ${result.keys.length ? result.keys.join(', ') : '(none)'}`);
        res.json({ success: true, ...result });
    }));

    api.delete('/data/tables/:table/columns/:column', requireAdmin, wrap(async (req, res) => {
        const result = await data.dropColumn(req.params.table, req.params.column);
        await audit(req, 'schema.drop_column', `${req.params.table}.${req.params.column}`);
        res.json({ success: true, ...result });
    }));

    api.patch('/data/tables/:table', requireAdmin, wrap(async (req, res) => {
        const result = await data.renameTable(req.params.table, req.body?.newName);
        await audit(req, 'table.rename', `${req.params.table} -> ${req.body?.newName}`);
        res.json({ success: true, ...result });
    }));

    api.delete('/data/tables/:table', requireAdmin, wrap(async (req, res) => {
        // Typed confirmation guards against dropping the wrong table.
        if (req.body?.confirm !== req.params.table) return res.status(400).json({ error: 'Type the table name to confirm' });
        const result = await data.dropTable(req.params.table);
        await audit(req, 'table.drop', req.params.table);
        res.json({ success: true, ...result });
    }));

    // ---- AI: ask questions, saved functions, model settings (src/console/ai.js) --------------
    // Asking and running are read-only, so open to every console role; saving
    // functions and changing the model settings need admin. Unexpected errors
    // on these routes are shown with their message rather than "Internal
    // error": they involve an external model and user-written SQL, and the
    // message is what tells the user what to change.
    const aiRoute = (fn) => wrap(async (req, res, next) => {
        try { await fn(req, res, next); } catch (err) {
            if (!err.statusCode && !err.status) {
                console.error('[Console] AI error:', err);
                throw Object.assign(new Error(`Ask AI failed: ${String(err.message || err).split('\n')[0].slice(0, 300)}`), { statusCode: 500, expose: true });
            }
            throw err;
        }
    });
    const abortOnClose = (res) => {
        const abort = new AbortController();
        res.on('close', () => { if (!res.writableFinished) abort.abort(); });
        return abort.signal;
    };
    const parseParams = (raw) => {
        if (raw === undefined || raw === null || raw === '') return {};
        try { return typeof raw === 'string' ? JSON.parse(raw) : raw; } catch { throw httpError(400, 'params must be a JSON object'); }
    };

    api.get('/ai/settings', (req, res) => res.json(ai.settingsView()));

    api.put('/ai/settings', requireAdmin, aiRoute(async (req, res) => {
        const changed = await ai.saveSettings(req.body?.values, req.consoleUser.username);
        if (changed.length) await audit(req, 'ai.settings_update', `Changed ${changed.join(', ')}`);
        res.json({ success: true, changed, ...ai.settingsView() });
    }));

    api.delete('/ai/settings', requireAdmin, aiRoute(async (req, res) => {
        await ai.resetSettings();
        await audit(req, 'ai.settings_reset', 'AI settings reverted to docker/.env');
        res.json({ success: true, ...ai.settingsView() });
    }));

    api.post('/ai/settings/test', requireAdmin, aiRoute(async (req, res) => {
        res.json(await ai.testSettings(req.body?.values));
    }));

    api.post('/ai/ask', aiRoute(async (req, res) => {
        const question = String(req.body?.question ?? '');
        const result = await ai.ask({ question, history: req.body?.history }, { signal: abortOnClose(res) });
        await audit(req, 'ai.ask', `${question.slice(0, 300)} (${result.steps.length} tool call(s), ${result.usage.total} tokens)`);
        res.json(result);
    }));

    // Run SQL edited in the console (from an answer or a function being edited).
    api.post('/ai/run', aiRoute(async (req, res) => {
        // Unsaved SQL from the editor: typed with the definitions being edited.
        const defs = ai.normaliseParamDefs(req.body?.sql, req.body?.paramDefs);
        const sql = ai.bindParams(req.body?.sql, parseParams(req.body?.params), defs);
        res.json(await ai.runReadOnlySql(sql, { signal: abortOnClose(res) }));
    }));

    // Ask the model to turn a query's hard-coded values into {{parameters}}
    // (Save as function). Nothing is saved; the admin reviews the proposal.
    api.post('/ai/functions/suggest', requireAdmin, aiRoute(async (req, res) => {
        const proposal = await ai.suggestFunction({ sql: req.body?.sql, question: req.body?.question }, { signal: abortOnClose(res) });
        await audit(req, 'ai.function_suggest', `${proposal.params.length} parameter(s) proposed`);
        res.json({ proposal });
    }));

    // Turn parameters back into hard-coded values in SQL being edited
    // (Remove on the Parameters table). Nothing is saved.
    api.post('/ai/inline-params', requireAdmin, aiRoute(async (req, res) => {
        res.json({ sql: ai.inlineParams(req.body?.sql, req.body?.params, req.body?.names, req.body?.values) });
    }));

    api.get('/ai/functions', aiRoute(async (req, res) => {
        res.json({ functions: await ai.listFunctions() });
    }));

    api.get('/ai/functions/:id', aiRoute(async (req, res) => {
        res.json({ function: await ai.getFunction(req.params.id) });
    }));

    api.post('/ai/functions', requireAdmin, aiRoute(async (req, res) => {
        const fn = await ai.createFunction(req.body, req.consoleUser.username);
        await audit(req, 'ai.function_create', fn.name);
        res.json({ success: true, function: fn });
    }));

    api.put('/ai/functions/:id', requireAdmin, aiRoute(async (req, res) => {
        const fn = await ai.updateFunction(req.params.id, req.body, req.consoleUser.username);
        await audit(req, 'ai.function_update', fn.name);
        res.json({ success: true, function: fn });
    }));

    api.delete('/ai/functions/:id', requireAdmin, aiRoute(async (req, res) => {
        const fn = await ai.deleteFunction(req.params.id);
        await audit(req, 'ai.function_delete', fn.name);
        res.json({ success: true });
    }));

    api.post('/ai/functions/:id/run', aiRoute(async (req, res) => {
        const result = await ai.runFunction(req.params.id, parseParams(req.body?.params), { signal: abortOnClose(res) });
        await audit(req, 'ai.function_run', `${result.function.name}: ${result.rowCount}${result.truncated ? '+' : ''} row(s)`);
        res.json(result);
    }));

    // Full result as CSV (?params={"name":"value"}), up to EXPORT_MAX_ROWS rows.
    api.get('/ai/functions/:id/export', aiRoute(async (req, res) => {
        const fn = await ai.exportFunctionCsv(req.params.id, parseParams(req.query.params), res);
        await audit(req, 'ai.function_export', `${fn.name} as csv`);
    }));

    // ---- Audit ----------------------------------------------------------------------------
    api.get('/audit', wrap(async (req, res) => {
        res.json({ entries: await store.listAudit(req.query.limit) });
    }));

    api.use((req, res) => res.status(404).json({ error: 'Not found' }));
    // eslint-disable-next-line no-unused-vars
    api.use((err, req, res, next) => {
        const status = err.statusCode || err.status || 500;
        if (status >= 500 && !err.expose) console.error('[Console] API error:', err);
        else if (err.expose) console.warn('[Console] API error:', err.message);
        if (res.headersSent) return res.destroy(err);
        // Errors flagged `expose` (e.g. from the AI model API) describe an external
        // service, so their message is safe and useful to show.
        res.status(status).json({ error: status >= 500 && !err.expose ? 'Internal error' : err.message });
    });

    router.use('/api', api);
    // HTML/JS/CSS revalidate on every load (ETag) so upgrades are picked up
    // immediately; only images are cached.
    router.use(express.static(PUBLIC_DIR, {
        index: 'index.html',
        setHeaders: (res, filePath) => {
            res.setHeader('Cache-Control', filePath.endsWith('.png') ? 'public, max-age=86400' : 'no-cache');
        },
    }));
    return router;
}
