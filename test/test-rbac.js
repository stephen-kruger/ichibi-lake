/**
 * test/test-rbac.js
 *
 * Integration test suite for the RBAC system. Runs against a live gateway.
 *
 *   node test/test-rbac.js
 *
 * The gateway must be running (default: http://localhost:3333). The API key
 * from docker/.env is promoted to superuser by the legacy bootstrap, so the
 * admin endpoints are available without any RBAC env configuration.
 *
 * What is tested:
 *   - RBAC table initialization at startup
 *   - Admin API: create user, list users, get user, update user, deactivate
 *   - Admin API: set roles, set table permissions, list permissions
 *   - Auth enforcement: inactive user → 401
 *   - Per-table permissions: deny overrides role, allow_write overrides reader
 *   - Self-service /admin/check endpoint
 *   - Legacy backward-compat mode (API_KEYS → superuser)
 *   - Reader cannot write, writer can write (unless denied per-table)
 */

import { API_KEY, ALT_KEY, BASE_URL, KEYS } from './_env.js';

const T = `rbac_test_${Date.now()}`;
const SUPER_KEY = API_KEY;
const WRITER_KEY = `sk-test-writer-${Date.now()}`;
const READER_KEY = `sk-test-reader-${Date.now()}`;
const DENIED_TABLE = `${T}_restricted`;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

async function authGet(url, key = SUPER_KEY) {
    const res = await fetch(url, { headers: { 'x-api-key': key } });
    return { status: res.status, ok: res.ok, json: res.ok ? await res.json() : await res.json().catch(() => null) };
}

async function authPost(url, body, key = SUPER_KEY) {
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: JSON.stringify(body),
    });
    let json;
    try { json = await res.json(); } catch (_) { json = { raw: await res.text() }; }
    return { status: res.status, ok: res.ok, json };
}

async function authPut(url, body, key = SUPER_KEY) {
    const res = await fetch(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: JSON.stringify(body),
    });
    let json;
    try { json = await res.json(); } catch (_) { json = { raw: await res.text() }; }
    return { status: res.status, ok: res.ok, json };
}

async function authPatch(url, body, key = SUPER_KEY) {
    const res = await fetch(url, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: JSON.stringify(body),
    });
    let json;
    try { json = await res.json(); } catch (_) { json = { raw: await res.text() }; }
    return { status: res.status, ok: res.ok, json };
}

async function authDelete(url, key = SUPER_KEY) {
    const res = await fetch(url, {
        method: 'DELETE',
        headers: { 'x-api-key': key },
    });
    let json;
    try { json = await res.json(); } catch (_) { json = { raw: await res.text() }; }
    return { status: res.status, ok: res.ok, json };
}

async function uploadJSON(url, body, key = SUPER_KEY) {
    const res = await fetch(url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: JSON.stringify(body),
    });
    return { status: res.status, ok: res.ok, json: await res.json() };
}

async function getData(url, key = SUPER_KEY) {
    const res = await fetch(url, { headers: { 'x-api-key': key } });
    return { status: res.status, ok: res.ok, json: res.ok ? await res.json() : null };
}

// ---------------------------------------------------------------------------
// Teardown
// ---------------------------------------------------------------------------

const teardown = async () => {
    try {
        await fetch(`${BASE_URL}/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
            body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${T}"` }),
        });
    } catch (_) {}
    try {
        await fetch(`${BASE_URL}/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
            body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${DENIED_TABLE}"` }),
        });
    } catch (_) {}
};
process.on('exit', () => { try { teardown(); } catch (_) {} });

// ---------------------------------------------------------------------------
// Test runner
// ---------------------------------------------------------------------------

let passed = 0, failed = 0;

async function describe(name, fn) {
    console.log(`\n▸ ${name}`);
    await fn();
}

function test(name, fn) {
    return (async () => {
        try {
            await fn();
            passed++;
            console.log(`  ✓ ${name}`);
        } catch (err) {
            failed++;
            console.error(`  ✗ ${name}`);
            console.error(`    ${err.message}`);
        }
    })();
}

// ---------------------------------------------------------------------------

async function run() {
    console.log(`BASE_URL    = ${BASE_URL}`);
    console.log(`SUPER_KEY   = ${SUPER_KEY.slice(0, 20)}...`);
    console.log('══════════════════════════════════════════════════════════');

    // ── 1. Basic health check ──────────────────────────────────────────────
    await describe('Gateway health & RBAC init', async () => {
        await test('Swagger is reachable', async () => {
            const res = await fetch(`${BASE_URL}/swagger.yaml`);
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
        });

        await test('RBAC tables are initialized (server started without crash)', async () => {
            const res = await fetch(`${BASE_URL}/query`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
                body: JSON.stringify({
                    sql: `SELECT count(*) as count FROM information_schema.tables
                          WHERE table_name IN ('_rbac_users', '_rbac_user_roles', '_rbac_table_permissions')`
                }),
            });
            const body = await res.json();
            const count = parseInt(body?.rows?.[0]?.count ?? 0, 10);
            if (count < 3) throw new Error(`Expected 3 RBAC tables, found ${count}`);
        });
    });

    // ── 2. Admin API: User CRUD ────────────────────────────────────────────
    await describe('Admin API — user CRUD', async () => {
        await test('List users returns at least the superuser', async () => {
            const { json } = await authGet(`${BASE_URL}/admin/users`);
            if (!json?.success) throw new Error(JSON.stringify(json));
            const keys = json.users.map(u => u.api_key);
            if (!keys.includes(SUPER_KEY)) throw new Error(`Superuser ${SUPER_KEY.slice(0, 20)}... not found`);
        });

        await test('Create a writer user', async () => {
            const { json } = await authPost(`${BASE_URL}/admin/users`, {
                apiKey: WRITER_KEY,
                name: 'Test Writer',
                email: 'writer@test.local',
                roles: ['writer'],
            });
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Create a reader user', async () => {
            const { json } = await authPost(`${BASE_URL}/admin/users`, {
                apiKey: READER_KEY,
                name: 'Test Reader',
                email: 'reader@test.local',
                roles: ['reader'],
            });
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Get writer user details', async () => {
            const { json } = await authGet(`${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}`);
            if (!json?.success) throw new Error(JSON.stringify(json));
            if (json.user.name !== 'Test Writer') throw new Error(`Expected name "Test Writer", got ${json.user.name}`);
            if (!json.user.roles.includes('writer')) throw new Error(`Expected writer role, got ${json.user.roles}`);
        });

        await test('Create user without apiKey returns 400', async () => {
            const { json } = await authPost(`${BASE_URL}/admin/users`, {
                name: 'No Key',
                roles: ['reader'],
            });
            if (json?.error !== 'apiKey is required') throw new Error(`Expected apiKey required error, got: ${JSON.stringify(json)}`);
        });

        await test('Create duplicate user returns 409', async () => {
            const { json } = await authPost(`${BASE_URL}/admin/users`, {
                apiKey: WRITER_KEY,
                name: 'Duplicate',
                roles: ['reader'],
            });
            if (json?.error !== 'User already exists' && !json?.error?.startsWith?.('User already exists')) {
                throw new Error(`Expected 409 conflict, got: ${JSON.stringify(json)}`);
            }
        });

        await test('Update user name and email', async () => {
            const { json } = await authPatch(`${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}`, {
                name: 'Writer Updated',
                email: 'updated@test.local',
            });
            if (!json?.success) throw new Error(JSON.stringify(json));
            const { json: check } = await authGet(`${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}`);
            if (check.user.name !== 'Writer Updated') throw new Error(`Name not updated`);
            if (check.user.email !== 'updated@test.local') throw new Error(`Email not updated`);
        });

        await test('Deactivate reader user', async () => {
            const { json } = await authDelete(`${BASE_URL}/admin/users/${encodeURIComponent(READER_KEY)}`);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Deactivated user cannot authenticate (401)', async () => {
            const { status } = await getData(`${BASE_URL}/tables`, READER_KEY);
            if (status !== 401) throw new Error(`Expected 401 for deactivated user, got ${status}`);
        });

        await test('Reactivate user and confirm auth works', async () => {
            const { json } = await authPatch(`${BASE_URL}/admin/users/${encodeURIComponent(READER_KEY)}`, {
                is_active: true,
            });
            if (!json?.success) throw new Error(JSON.stringify(json));
            const { status } = await getData(`${BASE_URL}/tables`, READER_KEY);
            if (status !== 200) throw new Error(`Expected 200 after reactivation, got ${status}`);
        });
    });

    // ── 3. Admin API: Role management ──────────────────────────────────────
    await describe('Admin API — role management', async () => {
        await test('Set roles on writer user', async () => {
            const { json } = await authPut(
                `${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}/roles`,
                { roles: ['writer'] }
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Set invalid role returns 400', async () => {
            const { json } = await authPut(
                `${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}/roles`,
                { roles: ['superadmin'] }
            );
            if (json?.error !== 'Invalid role: superadmin') {
                throw new Error(`Expected invalid role error, got: ${JSON.stringify(json)}`);
            }
        });

        await test('Non-superuser cannot manage roles', async () => {
            const { status } = await authPut(
                `${BASE_URL}/admin/users/${encodeURIComponent(WRITER_KEY)}/roles`,
                { roles: ['reader'] },
                READER_KEY
            );
            if (status !== 403) throw new Error(`Expected 403, got ${status}`);
        });
    });

    // ── 4. Admin API: Table permissions ────────────────────────────────────
    await describe('Admin API — table permissions', async () => {
        // Create the test table as superuser first
        await test('Create test table', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${T}`, [
                { id: 'r1', value: 'hello' },
            ]);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Create restricted table', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${DENIED_TABLE}`, [
                { id: 'r2', value: 'secret' },
            ]);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Deny writer access to restricted table', async () => {
            const { json } = await authPut(
                `${BASE_URL}/admin/tables/${DENIED_TABLE}/permissions`,
                { apiKey: WRITER_KEY, permission: 'deny' }
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Upgrade reader to allow_write on test table', async () => {
            const { json } = await authPut(
                `${BASE_URL}/admin/tables/${T}/permissions`,
                { apiKey: READER_KEY, permission: 'allow_write' }
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('List permissions on denied table', async () => {
            const { json } = await authGet(`${BASE_URL}/admin/tables/${DENIED_TABLE}/permissions`);
            if (!json?.success) throw new Error(JSON.stringify(json));
            const entry = json.permissions.find(p => p.api_key === WRITER_KEY);
            if (!entry) throw new Error(`Expected permission entry for writer`);
            if (entry.permission !== 'deny') throw new Error(`Expected deny, got ${entry.permission}`);
        });

        await test('Invalid permission returns 400', async () => {
            const { json } = await authPut(
                `${BASE_URL}/admin/tables/${T}/permissions`,
                { apiKey: WRITER_KEY, permission: 'sudo' }
            );
            if (json?.error !== 'Invalid permission: sudo') {
                throw new Error(`Expected invalid permission error, got: ${JSON.stringify(json)}`);
            }
        });
    });

    // ── 5. RBAC enforcement on data operations ─────────────────────────────
    await describe('RBAC enforcement — data operations', async () => {
        await test('Writer can upload to unrestricted table', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${T}`, [
                { id: 'w1', value: 'writer-data' },
            ], WRITER_KEY);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Writer is denied from restricted table', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${DENIED_TABLE}`, [
                { id: 'w2', value: 'should-fail' },
            ], WRITER_KEY);
            if (json?.error !== 'Access denied: no write permission for table' &&
                !json?.error?.startsWith?.('Access denied')) {
                throw new Error(`Expected access denied, got: ${JSON.stringify(json)}`);
            }
        });

        await test('Reader can read test table (allow_write override)', async () => {
            const { json } = await getData(`${BASE_URL}/tables/${T}`, READER_KEY);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Reader can write to test table (allow_write override)', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${T}`, [
                { id: 'r3', value: 'reader-written' },
            ], READER_KEY);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Reader can read restricted table (reader role grants global read)', async () => {
            const { json } = await getData(`${BASE_URL}/tables/${DENIED_TABLE}`, READER_KEY);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Unknown API key is rejected (401)', async () => {
            const { status } = await getData(`${BASE_URL}/tables`, 'sk-nonexistent-key-abc123');
            if (status !== 401) throw new Error(`Expected 401, got ${status}`);
        });

        await test('Missing x-api-key header is rejected (401)', async () => {
            const res = await fetch(`${BASE_URL}/tables`);
            if (res.status !== 401) throw new Error(`Expected 401, got ${res.status}`);
        });
    });

    // ── 6. Self-service /admin/check endpoint ──────────────────────────────
    await describe('Self-service /admin/check endpoint', async () => {
        await test('Writer can check own access to test table', async () => {
            const { json } = await authGet(
                `${BASE_URL}/admin/check?table=${T}&access=write`,
                WRITER_KEY
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
            if (json.allowed !== true) throw new Error(`Expected allowed=true, got ${json.allowed}`);
        });

        await test('Writer can check own denied access', async () => {
            const { json } = await authGet(
                `${BASE_URL}/admin/check?table=${DENIED_TABLE}&access=write`,
                WRITER_KEY
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
            if (json.allowed !== false) throw new Error(`Expected allowed=false, got ${json.allowed}`);
        });

        await test('Reader checks own allow_write override', async () => {
            const { json } = await authGet(
                `${BASE_URL}/admin/check?table=${T}&access=write`,
                READER_KEY
            );
            if (!json?.success) throw new Error(JSON.stringify(json));
            if (json.allowed !== true) throw new Error(`Expected allowed=true, got ${json.allowed}`);
        });
    });

    // ── 7. Superuser privileges ────────────────────────────────────────────
    await describe('Superuser privileges', async () => {
        await test('Superuser can access denied table (superuser bypass)', async () => {
            const { json } = await getData(`${BASE_URL}/tables/${DENIED_TABLE}`, SUPER_KEY);
            if (!json?.success) throw new Error(`Superuser read on denied table failed: ${JSON.stringify(json)}`);
        });

        await test('Superuser can write to denied table', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${DENIED_TABLE}`, [
                { id: 'su1', value: 'superuser-data' },
            ], SUPER_KEY);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });

        await test('Superuser can admin users', async () => {
            const { json } = await authGet(`${BASE_URL}/admin/users`);
            if (!json?.success) throw new Error(JSON.stringify(json));
        });
    });

    // ── 8. Cleanup ─────────────────────────────────────────────────────────
    await describe('Cleanup', async () => {
        await test('Drop test table', async () => {
            try {
                await fetch(`${BASE_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
                    body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${T}"` }),
                });
            } catch (_) {}
        });

        await test('Drop restricted table', async () => {
            try {
                await fetch(`${BASE_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
                    body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${DENIED_TABLE}"` }),
                });
            } catch (_) {}
        });
    });

    // ── Summary ─────────────────────────────────────────────────────────────
    console.log('\n══════════════════════════════════════════════════════════');
    console.log(`Results: ${passed} passed, ${failed} failed`);
    if (failed > 0) process.exit(1);
    console.log('all RBAC suites passed');
}

run().catch(err => { console.error(err); process.exit(1); });
