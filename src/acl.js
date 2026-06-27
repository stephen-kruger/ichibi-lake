import { execute, executeWithParams, invalidateColumnCache, VARCHAR } from './db.js';
import { checkAccess as rbacCheckAccess, isRbacConfigured } from './rbac.js';

const ACL_CACHE_TTL_MS = parseInt(process.env.ACL_CACHE_TTL_MS || '60000', 10);
const aclCache = new Map();

export function invalidateAclCache(tableName) {
    if (tableName) aclCache.delete(tableName);
    else aclCache.clear();
}

export async function initAclTable() {
    await execute(`
        CREATE TABLE IF NOT EXISTS _table_acls (
            table_name VARCHAR,
            owner_keys VARCHAR,
            reader_keys VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            created_by VARCHAR
        )
    `);
}

export async function getTableAcl(tableName) {
    const now = Date.now();
    const cached = aclCache.get(tableName);
    if (cached && cached.expiresAt > now) return cached.acl;

    const rows = await executeWithParams(
        `SELECT owner_keys, reader_keys FROM _table_acls WHERE table_name = ?`,
        [tableName], [VARCHAR]
    );

    if (rows.length === 0) return null;

    const acl = {
        owner_keys: JSON.parse(rows[0].owner_keys || '[]'),
        reader_keys: JSON.parse(rows[0].reader_keys || '[]'),
    };
    aclCache.set(tableName, { acl, expiresAt: now + ACL_CACHE_TTL_MS });
    return acl;
}

export async function ensureTableAcl(tableName, apiKey) {
    const existing = await getTableAcl(tableName);
    if (existing) return existing;

    const owner = apiKey || 'public';
    const ownerJson = JSON.stringify([owner]);
    const readerJson = JSON.stringify([]);
    await executeWithParams(
        `INSERT INTO _table_acls (table_name, owner_keys, reader_keys, created_by) VALUES (?, ?, ?, ?)`,
        [tableName, ownerJson, readerJson, owner],
        [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
    );
    invalidateAclCache(tableName);
    return { owner_keys: [owner], reader_keys: [] };
}

export async function checkReadAccess(tableName, apiKey) {
    try {
        const rbacResult = await rbacCheckAccess(apiKey, tableName, 'read');
        if (rbacResult) return true;
    } catch (_) {
        // RBAC check failed (tables not initialized), fall through
    }
    const acl = await getTableAcl(tableName);
    if (!acl) return false;
    const key = apiKey || 'public';
    return acl.owner_keys.includes(key) || acl.reader_keys.includes(key);
}

export async function checkWriteAccess(tableName, apiKey) {
    try {
        const rbacResult = await rbacCheckAccess(apiKey, tableName, 'write');
        if (rbacResult) return true;
    } catch (_) {
        // RBAC check failed (tables not initialized), fall through
    }
    const acl = await getTableAcl(tableName);
    if (!acl) return false;
    const key = apiKey || 'public';
    return acl.owner_keys.includes(key);
}

export async function updateTableAcl(tableName, ownerKeys, readerKeys) {
    const ownerJson = JSON.stringify(ownerKeys);
    const readerJson = JSON.stringify(readerKeys);
    const result = await executeWithParams(
        `UPDATE _table_acls SET owner_keys = ?, reader_keys = ? WHERE table_name = ?`,
        [ownerJson, readerJson, tableName],
        [VARCHAR, VARCHAR, VARCHAR]
    );
    invalidateAclCache(tableName);
}

export async function migrateToAcl() {
    const metaCheck = await execute(
        `SELECT count(*) as count FROM information_schema.tables WHERE table_schema = 'main' AND table_name = '_gateway_meta'`
    );
    if (metaCheck[0] && parseInt(metaCheck[0].count) > 0) {
        const migrated = await execute(`SELECT count(*) as count FROM _gateway_meta WHERE key = 'migration_v3_acl'`);
        if (migrated[0] && parseInt(migrated[0].count) > 0) {
            console.log('[Migration] ACL migration already completed. Skipping.');
            return;
        }
    }

    await initAclTable();
    console.log('[Migration] Running ACL migration (replacing _api_key column)...');

    const tables = await execute(
        `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_type = 'BASE TABLE'`
    );

    for (const row of tables) {
        const tableName = row.table_name;
        if (tableName.startsWith('_')) continue;

        const cols = await execute(
            `SELECT column_name FROM information_schema.columns WHERE table_name = '${tableName}' AND column_name = '_api_key'`
        );

        if (cols.length === 0) continue;

        const apiKeyRows = await execute(
            `SELECT DISTINCT _api_key FROM "${tableName}" WHERE _api_key IS NOT NULL AND _api_key != ''`
        );
        const keys = apiKeyRows.map(r => r._api_key);

        const ownerKeys = keys.length > 0 ? keys : ['public'];
        const ownerJson = JSON.stringify(ownerKeys);
        const readerJson = JSON.stringify([]);

        const existing = await executeWithParams(
            `SELECT count(*) as count FROM _table_acls WHERE table_name = ?`,
            [tableName], [VARCHAR]
        );
        if (existing[0] && parseInt(existing[0].count) === 0) {
            await executeWithParams(
                `INSERT INTO _table_acls (table_name, owner_keys, reader_keys, created_by) VALUES (?, ?, ?, ?)`,
                [tableName, ownerJson, readerJson, ownerKeys[0]],
                [VARCHAR, VARCHAR, VARCHAR, VARCHAR]
            );
            console.log(`[Migration] Created ACL for ${tableName} with owners: ${ownerKeys.join(', ')}`);
        }

        try {
            await execute(`ALTER TABLE "${tableName}" DROP COLUMN _api_key`);
            invalidateColumnCache(tableName);
            console.log(`[Migration] Dropped _api_key column from ${tableName}`);
        } catch (dropErr) {
            console.warn(`[Migration] Could not drop _api_key from ${tableName} (may be a partition key):`, dropErr.message);
        }
    }

    await execute(`CREATE TABLE IF NOT EXISTS _gateway_meta (key VARCHAR, migrated_at TIMESTAMP)`);
    const alreadyRecorded = await execute(`SELECT count(*) as count FROM _gateway_meta WHERE key = 'migration_v3_acl'`);
    if (alreadyRecorded[0] && parseInt(alreadyRecorded[0].count) === 0) {
        await execute(`INSERT INTO _gateway_meta VALUES ('migration_v3_acl', now())`);
    }
    console.log('[Migration] ACL migration complete.');
}
