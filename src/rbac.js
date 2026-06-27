import { execute, executeWithParams, invalidateColumnCache, VARCHAR } from './db.js';

const RBAC_CACHE_TTL_MS = parseInt(process.env.RBAC_CACHE_TTL_MS || '60000', 10);
const userCache = new Map();
const roleCache = new Map();
const permCache = new Map();

function invalidateUserCache(apiKey) {
    if (apiKey) {
        userCache.delete(apiKey);
        roleCache.delete(apiKey);
        permCache.delete(apiKey);
    } else {
        userCache.clear();
        roleCache.clear();
        permCache.clear();
    }
}

export function invalidateRbacCache(apiKey) {
    invalidateUserCache(apiKey);
}

const SAFE_ROLES = new Set(['superuser', 'writer', 'reader']);
const SAFE_PERMISSIONS = new Set(['allow_read', 'allow_write', 'deny']);

function safeRole(role) {
    if (!SAFE_ROLES.has(role)) throw new Error(`Invalid role: ${role}`);
    return role;
}

function safePermission(permission) {
    if (!SAFE_PERMISSIONS.has(permission)) throw new Error(`Invalid permission: ${permission}`);
    return permission;
}

export async function initRbacTables() {
    await execute(`CREATE TABLE IF NOT EXISTS _rbac_users (
        api_key VARCHAR,
        name VARCHAR,
        email VARCHAR,
        is_active VARCHAR,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        created_by VARCHAR
    )`);

    await execute(`CREATE TABLE IF NOT EXISTS _rbac_user_roles (
        api_key VARCHAR,
        role VARCHAR,
        granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        granted_by VARCHAR
    )`);

    await execute(`CREATE TABLE IF NOT EXISTS _rbac_table_permissions (
        api_key VARCHAR,
        table_name VARCHAR,
        permission VARCHAR,
        granted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        granted_by VARCHAR
    )`);
}

export function isRbacConfigured() {
    if (process.env.RBAC_SUPERUSERS || process.env.RBAC_USERS) return true;
    return false;
}

function safeJsonParse(str, fallback) {
    if (!str) return fallback;
    try { return JSON.parse(str); } catch { return fallback; }
}

export async function bootstrapRbac() {
    const superusersRaw = (process.env.RBAC_SUPERUSERS || '').split(',').map(s => s.trim()).filter(Boolean);
    const usersRaw = safeJsonParse(process.env.RBAC_USERS, []);
    const tablePermsRaw = safeJsonParse(process.env.RBAC_TABLE_PERMISSIONS, []);

    console.log('[RBAC] Bootstrapping RBAC from environment...');

    await initRbacTables();

    for (const u of usersRaw) {
        const apiKey = u.apiKey || u.api_key;
        if (!apiKey) continue;
        const name = u.name || '';
        const email = u.email || '';
        const roles = Array.isArray(u.roles) ? u.roles : [];
        const superseded = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_users WHERE api_key = ?`,
            [apiKey], [VARCHAR]
        );
        const exists = superseded[0] && parseInt(superseded[0].count) > 0;
        if (exists) {
            await executeWithParams(
                `UPDATE _rbac_users SET name = ?, email = ?, is_active = 'true' WHERE api_key = ?`,
                [name, email, apiKey], [VARCHAR, VARCHAR, VARCHAR]
            );
        } else {
            await executeWithParams(
                `INSERT INTO _rbac_users (api_key, name, email, is_active, created_by) VALUES (?, ?, ?, 'true', ?)`,
                [apiKey, name, email, 'bootstrap'], [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
            );
        }
        for (const role of roles) {
            if (!SAFE_ROLES.has(role)) continue;
            const existing = await executeWithParams(
                `SELECT count(*) as count FROM _rbac_user_roles WHERE api_key = ? AND role = ?`,
                [apiKey, role], [VARCHAR, VARCHAR]
            );
            if (!(existing[0] && parseInt(existing[0].count) > 0)) {
                await executeWithParams(
                    `INSERT INTO _rbac_user_roles (api_key, role, granted_by) VALUES (?, ?, ?)`,
                    [apiKey, role, 'bootstrap'], [VARCHAR, VARCHAR, VARCHAR]
                );
            }
        }
        console.log(`[RBAC] Synced user: ${apiKey.slice(0, 20)}... (${roles.join(', ') || 'no roles'})`);
    }

    for (const key of superusersRaw) {
        if (!key) continue;
        const existing = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_users WHERE api_key = ?`,
            [key], [VARCHAR]
        );
        if (!(existing[0] && parseInt(existing[0].count) > 0)) {
            await executeWithParams(
                `INSERT INTO _rbac_users (api_key, name, is_active, created_by) VALUES (?, ?, 'true', ?)`,
                [key, 'auto-superuser', 'bootstrap'], [VARCHAR, VARCHAR, VARCHAR]
            );
        } else {
            await executeWithParams(
                `UPDATE _rbac_users SET is_active = 'true' WHERE api_key = ?`,
                [key], [VARCHAR]
            );
        }
        const roleExists = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_user_roles WHERE api_key = ? AND role = 'superuser'`,
            [key], [VARCHAR]
        );
        if (!(roleExists[0] && parseInt(roleExists[0].count) > 0)) {
            await executeWithParams(
                `INSERT INTO _rbac_user_roles (api_key, role, granted_by) VALUES (?, 'superuser', ?)`,
                [key, 'bootstrap'], [VARCHAR, VARCHAR]
            );
        }
        console.log(`[RBAC] Ensured superuser: ${key.slice(0, 20)}...`);
    }

    for (const p of tablePermsRaw) {
        const apiKey = p.apiKey || p.api_key;
        const tableName = p.tableName || p.table_name;
        const permission = p.permission;
        if (!apiKey || !tableName || !permission || !SAFE_PERMISSIONS.has(permission)) continue;
        const existing = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_table_permissions WHERE api_key = ? AND table_name = ?`,
            [apiKey, tableName], [VARCHAR, VARCHAR]
        );
        if (existing[0] && parseInt(existing[0].count) > 0) {
            await executeWithParams(
                `UPDATE _rbac_table_permissions SET permission = ? WHERE api_key = ? AND table_name = ?`,
                [permission, apiKey, tableName], [VARCHAR, VARCHAR, VARCHAR]
            );
        } else {
            await executeWithParams(
                `INSERT INTO _rbac_table_permissions (api_key, table_name, permission, granted_by) VALUES (?, ?, ?, ?)`,
                [apiKey, tableName, permission, 'bootstrap'], [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
            );
        }
        console.log(`[RBAC] Table permission: ${apiKey.slice(0, 20)}... ${permission} on ${tableName}`);
    }

    invalidateRbacCache();
    console.log('[RBAC] Bootstrap complete.');
}

export async function findUser(apiKey) {
    const now = Date.now();
    const cached = userCache.get(apiKey);
    if (cached && cached.expiresAt > now) return cached.user;

    const rows = await executeWithParams(
        `SELECT api_key, name, email, is_active, created_at, created_by FROM _rbac_users WHERE api_key = ?`,
        [apiKey], [VARCHAR]
    );
    if (rows.length === 0) {
        userCache.set(apiKey, { user: null, expiresAt: now + RBAC_CACHE_TTL_MS });
        return null;
    }
    const user = rows[0];
    userCache.set(apiKey, { user, expiresAt: now + RBAC_CACHE_TTL_MS });
    return user;
}

export async function getUserRoles(apiKey) {
    const now = Date.now();
    const cached = roleCache.get(apiKey);
    if (cached && cached.expiresAt > now) return cached.roles;

    const rows = await executeWithParams(
        `SELECT role FROM _rbac_user_roles WHERE api_key = ?`,
        [apiKey], [VARCHAR]
    );
    const roles = rows.map(r => r.role);
    roleCache.set(apiKey, { roles, expiresAt: now + RBAC_CACHE_TTL_MS });
    return roles;
}

export async function getTablePermission(apiKey, tableName) {
    const now = Date.now();
    const cacheKey = `${apiKey}::${tableName}`;
    const cached = permCache.get(cacheKey);
    if (cached && cached.expiresAt > now) return cached.permission;

    const rows = await executeWithParams(
        `SELECT permission FROM _rbac_table_permissions WHERE api_key = ? AND table_name = ?`,
        [apiKey, tableName], [VARCHAR, VARCHAR]
    );
    const permission = rows.length > 0 ? rows[0].permission : null;
    permCache.set(cacheKey, { permission, expiresAt: now + RBAC_CACHE_TTL_MS });
    return permission;
}

export async function checkAccess(apiKey, tableName, requiredAccess) {
    const user = await findUser(apiKey);
    if (!user) return false;
    const isActive = user.is_active === true || user.is_active === 'true';
    if (!isActive) return false;

    const roles = await getUserRoles(apiKey);
    const isSuperuser = roles.includes('superuser');

    const tablePerm = await getTablePermission(apiKey, tableName);

    if (isSuperuser) {
        return tablePerm !== 'deny';
    }

    if (tablePerm === 'deny') return false;

    if (requiredAccess === 'write') {
        if (tablePerm === 'allow_write') return true;
        if (roles.includes('writer')) return true;
        return false;
    }

    if (requiredAccess === 'read') {
        if (tablePerm === 'allow_read' || tablePerm === 'allow_write') return true;
        if (roles.includes('writer') || roles.includes('reader')) return true;
        return false;
    }

    return false;
}

export async function isValidApiKey(apiKey) {
    const user = await findUser(apiKey);
    if (!user) return false;
    return user.is_active === true || user.is_active === 'true';
}

export async function listUsers() {
    return execute(
        `SELECT api_key, name, email, is_active, created_at, created_by FROM _rbac_users ORDER BY created_at`
    );
}

export async function createUser(apiKey, name, email, roles, grantedBy) {
    const existing = await executeWithParams(
        `SELECT count(*) as count FROM _rbac_users WHERE api_key = ?`,
        [apiKey], [VARCHAR]
    );
    if (existing[0] && parseInt(existing[0].count) > 0) {
        throw new Error(`User already exists: ${apiKey.slice(0, 20)}...`);
    }

    await executeWithParams(
        `INSERT INTO _rbac_users (api_key, name, email, is_active, created_by) VALUES (?, ?, ?, 'true', ?)`,
        [apiKey, name || '', email || '', grantedBy || 'admin'],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
    );

    if (Array.isArray(roles)) {
        for (const role of roles) {
            safeRole(role);
            await executeWithParams(
                `INSERT INTO _rbac_user_roles (api_key, role, granted_by) VALUES (?, ?, ?)`,
                [apiKey, role, grantedBy || 'admin'], [VARCHAR, VARCHAR, VARCHAR]
            );
        }
    }

    invalidateUserCache(apiKey);
}

export async function updateUser(apiKey, updates) {
    const user = await findUser(apiKey);
    if (!user) throw new Error(`User not found: ${apiKey.slice(0, 20)}...`);

    const sets = [];
    const params = [];
    const types = [];
    if (updates.name !== undefined) { sets.push('name = ?'); params.push(updates.name); types.push(VARCHAR); }
    if (updates.email !== undefined) { sets.push('email = ?'); params.push(updates.email); types.push(VARCHAR); }
    if (updates.is_active !== undefined) { sets.push('is_active = ?'); params.push(updates.is_active ? 'true' : 'false'); types.push(VARCHAR); }

    if (sets.length > 0) {
        params.push(apiKey);
        types.push(VARCHAR);
        await executeWithParams(
            `UPDATE _rbac_users SET ${sets.join(', ')} WHERE api_key = ?`,
            params, types
        );
    }

    invalidateUserCache(apiKey);
}

export async function setUserRoles(apiKey, roles, grantedBy) {
    const user = await findUser(apiKey);
    if (!user) throw new Error(`User not found: ${apiKey.slice(0, 20)}...`);

    for (const role of roles) {
        safeRole(role);
    }

    await executeWithParams(
        `DELETE FROM _rbac_user_roles WHERE api_key = ?`,
        [apiKey], [VARCHAR]
    );

    for (const role of roles) {
        await executeWithParams(
            `INSERT INTO _rbac_user_roles (api_key, role, granted_by) VALUES (?, ?, ?)`,
            [apiKey, role, grantedBy || 'admin'], [VARCHAR, VARCHAR, VARCHAR]
        );
    }

    invalidateUserCache(apiKey);
}

export async function setTablePermission(apiKey, tableName, permission, grantedBy) {
    safePermission(permission);

    const existing = await executeWithParams(
        `SELECT count(*) as count FROM _rbac_table_permissions WHERE api_key = ? AND table_name = ?`,
        [apiKey, tableName], [VARCHAR, VARCHAR]
    );

    if (existing[0] && parseInt(existing[0].count) > 0) {
        await executeWithParams(
            `UPDATE _rbac_table_permissions SET permission = ? WHERE api_key = ? AND table_name = ?`,
            [permission, apiKey, tableName], [VARCHAR, VARCHAR, VARCHAR]
        );
    } else {
        await executeWithParams(
            `INSERT INTO _rbac_table_permissions (api_key, table_name, permission, granted_by) VALUES (?, ?, ?, ?)`,
            [apiKey, tableName, permission, grantedBy || 'admin'], [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
        );
    }

    invalidateUserCache(apiKey);
}

export async function listTablePermissions(tableName) {
    return executeWithParams(
        `SELECT p.api_key, p.table_name, p.permission, p.granted_at, u.name AS user_name
         FROM _rbac_table_permissions p
         JOIN _rbac_users u ON u.api_key = p.api_key
         WHERE p.table_name = ?
         ORDER BY p.granted_at`,
        [tableName], [VARCHAR]
    );
}

export async function listUserPermissions(apiKey) {
    return executeWithParams(
        `SELECT table_name, permission FROM _rbac_table_permissions WHERE api_key = ? ORDER BY table_name`,
        [apiKey], [VARCHAR]
    );
}

export async function legacyBootstrap() {
    const apiKeysRaw = process.env.API_KEYS || process.env.API_KEY || '';
    const keys = apiKeysRaw.split(',').map(k => k.trim()).filter(Boolean);
    if (keys.length === 0) return;

    console.log('[RBAC] Legacy bootstrap: promoting API_KEYS to superusers...');
    await initRbacTables();

    for (const key of keys) {
        const existing = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_users WHERE api_key = ?`,
            [key], [VARCHAR]
        );
        if (!(existing[0] && parseInt(existing[0].count) > 0)) {
            await executeWithParams(
                `INSERT INTO _rbac_users (api_key, name, created_by) VALUES (?, ?, ?)`,
                [key, 'legacy-user', 'legacy-bootstrap'], [VARCHAR, VARCHAR, VARCHAR]
            );
        }
        await executeWithParams(
            `UPDATE _rbac_users SET is_active = 'true' WHERE api_key = ?`,
            [key], [VARCHAR]
        );
        const roleExists = await executeWithParams(
            `SELECT count(*) as count FROM _rbac_user_roles WHERE api_key = ? AND role = 'superuser'`,
            [key], [VARCHAR]
        );
        if (!(roleExists[0] && parseInt(roleExists[0].count) > 0)) {
            await executeWithParams(
                `INSERT INTO _rbac_user_roles (api_key, role, granted_by) VALUES (?, 'superuser', ?)`,
                [key, 'legacy-bootstrap'], [VARCHAR, VARCHAR]
            );
        }
    }

    invalidateRbacCache();
    console.log(`[RBAC] Legacy bootstrap complete: ${keys.length} superuser(s)`);
}
