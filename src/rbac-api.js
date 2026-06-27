import { Router } from 'express';
import {
    findUser,
    getUserRoles,
    checkAccess,
    listUsers,
    createUser,
    updateUser,
    setUserRoles,
    setTablePermission,
    listTablePermissions,
    listUserPermissions,
} from './rbac.js';

const router = Router();

async function requireSuperuser(req, res, next) {
    try {
        const roles = await getUserRoles(req.apiKey);
        if (!roles.includes('superuser')) {
            return res.status(403).json({ error: 'Forbidden: superuser access required' });
        }
        next();
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
}

// GET /admin/users — list all users
router.get('/users', requireSuperuser, async (req, res) => {
    try {
        const users = await listUsers();
        const enriched = [];
        for (const u of users) {
            const roles = await getUserRoles(u.api_key);
            enriched.push({ ...u, roles });
        }
        res.json({ success: true, users: enriched });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// POST /admin/users — create a new user
router.post('/users', requireSuperuser, async (req, res) => {
    try {
        const { apiKey, api_key, name, email, roles } = req.body;
        const key = apiKey || api_key;
        if (!key) return res.status(400).json({ error: 'apiKey is required' });
        if (!name) return res.status(400).json({ error: 'name is required' });
        await createUser(key, name, email, roles, req.apiKey);
        res.json({ success: true, message: 'User created' });
    } catch (err) {
        if (err.message.startsWith('User already exists')) {
            return res.status(409).json({ error: err.message });
        }
        res.status(500).json({ error: err.message });
    }
});

// GET /admin/users/:apiKey — get user details
router.get('/users/:apiKey', requireSuperuser, async (req, res) => {
    try {
        const user = await findUser(req.params.apiKey);
        if (!user) return res.status(404).json({ error: 'User not found' });
        const roles = await getUserRoles(req.params.apiKey);
        const permissions = await listUserPermissions(req.params.apiKey);
        res.json({ success: true, user: { ...user, roles, permissions } });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// PATCH /admin/users/:apiKey — update user name, email, or active status
router.patch('/users/:apiKey', requireSuperuser, async (req, res) => {
    try {
        const { name, email, is_active } = req.body;
        await updateUser(req.params.apiKey, { name, email, is_active });
        res.json({ success: true, message: 'User updated' });
    } catch (err) {
        if (err.message.startsWith('User not found')) {
            return res.status(404).json({ error: err.message });
        }
        res.status(500).json({ error: err.message });
    }
});

// DELETE /admin/users/:apiKey — deactivate a user (soft-delete)
router.delete('/users/:apiKey', requireSuperuser, async (req, res) => {
    try {
        await updateUser(req.params.apiKey, { is_active: false });
        res.json({ success: true, message: 'User deactivated' });
    } catch (err) {
        if (err.message.startsWith('User not found')) {
            return res.status(404).json({ error: err.message });
        }
        res.status(500).json({ error: err.message });
    }
});

// PUT /admin/users/:apiKey/roles — set roles for a user
router.put('/users/:apiKey/roles', requireSuperuser, async (req, res) => {
    try {
        const { roles } = req.body;
        if (!Array.isArray(roles)) return res.status(400).json({ error: 'roles must be an array' });
        await setUserRoles(req.params.apiKey, roles, req.apiKey);
        res.json({ success: true, message: 'Roles updated' });
    } catch (err) {
        if (err.message.startsWith('User not found')) {
            return res.status(404).json({ error: err.message });
        }
        if (err.message.startsWith('Invalid role')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: err.message });
    }
});

// PUT /admin/tables/:tableName/permissions — set per-table permission for a user
router.put('/tables/:tableName/permissions', requireSuperuser, async (req, res) => {
    try {
        const { apiKey, api_key, permission } = req.body;
        const key = apiKey || api_key;
        if (!key) return res.status(400).json({ error: 'apiKey is required' });
        if (!permission) return res.status(400).json({ error: 'permission is required' });
        await setTablePermission(key, req.params.tableName, permission, req.apiKey);
        res.json({ success: true, message: `Permission set to ${permission}` });
    } catch (err) {
        if (err.message.startsWith('Invalid permission')) {
            return res.status(400).json({ error: err.message });
        }
        res.status(500).json({ error: err.message });
    }
});

// GET /admin/tables/:tableName/permissions — list all permissions for a table
router.get('/tables/:tableName/permissions', requireSuperuser, async (req, res) => {
    try {
        const perms = await listTablePermissions(req.params.tableName);
        res.json({ success: true, permissions: perms });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// GET /admin/check — check your own access to a table (self-service)
router.get('/check', async (req, res) => {
    try {
        const { table, access = 'read' } = req.query;
        if (!table) return res.status(400).json({ error: 'table query parameter is required' });
        const allowed = await checkAccess(req.apiKey, table, access);
        res.json({ success: true, table, access, allowed });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

export default router;
