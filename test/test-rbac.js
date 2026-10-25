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
 *   - Table-scoped SQL (/query incl. NDJSON streaming, GraphQL sql): writers
 *     and readers may SELECT the tables they can read; a denied table gets 403
 *     however it is referenced (join, subquery, CTE, UNION, quoting); system
 *     tables 404; other catalogs, table functions, writes and multiple
 *     statements are rejected. Superusers get any read-only SQL.
 *   - Property graphs (create, query, delete) stay superuser-only
 *   - System tables ("_"-prefixed) are unreachable through the table
 *     endpoints, /upload and /kafka-sink for every role
 */

import { API_KEY, ALT_KEY, BASE_URL, KEYS } from './_env.js';

const T = `rbac_test_${Date.now()}`;
const SUPER_KEY = API_KEY;
const WRITER_KEY = `sk-test-writer-${Date.now()}`;
const READER_KEY = `sk-test-reader-${Date.now()}`;
const DENIED_TABLE = `${T}_restricted`;
const READER_DENIED_TABLE = `${T}_reader_denied`;

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
    try {
        await fetch(`${BASE_URL}/query`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
            body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${READER_DENIED_TABLE}"` }),
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

        await test('Create a table denied to the reader', async () => {
            const { json } = await uploadJSON(`${BASE_URL}/upload/${READER_DENIED_TABLE}`, [
                { id: 'r2', value: 'reader-secret' },
            ]);
            if (!json?.success) throw new Error(JSON.stringify(json));
            const deny = await authPut(
                `${BASE_URL}/admin/tables/${READER_DENIED_TABLE}/permissions`,
                { apiKey: READER_KEY, permission: 'deny' }
            );
            if (!deny.json?.success) throw new Error(JSON.stringify(deny.json));
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
    await describe('Superuser-only features (graphs) and system tables', async () => {
        const ROLES = [['writer', WRITER_KEY], ['reader', READER_KEY]];
        const GRAPH = `rbac_graph_${Date.now()}`;

        for (const [role, key] of ROLES) {
            await test(`${role} cannot run SQL with GRAPH_TABLE outside the graph endpoints`, async () => {
                const r = await authPost(`${BASE_URL}/query`, { sql: `FROM GRAPH_TABLE (${GRAPH} MATCH (a:${T}) COLUMNS (a.id))` }, key);
                if (r.status === 200) throw new Error(`Expected a rejection, got 200: ${JSON.stringify(r.json)}`);
            });
            await test(`${role} cannot create, query or delete property graphs (403)`, async () => {
                const create = await authPost(`${BASE_URL}/graphs`, { name: `${GRAPH}_${role}`, vertexTables: [T], edgeTables: [] }, key);
                if (create.status !== 403) throw new Error(`create: expected 403, got ${create.status}`);
                const query = await authPost(`${BASE_URL}/graphs/${GRAPH}/query`, { match: `(a:${T})`, columns: 'a.id' }, key);
                if (query.status !== 403) throw new Error(`query: expected 403, got ${query.status}`);
                const del = await authDelete(`${BASE_URL}/graphs/${GRAPH}`, key);
                if (del.status !== 403) throw new Error(`delete: expected 403, got ${del.status}`);
            });
            await test(`${role} cannot read or write system tables (404 / 400)`, async () => {
                for (const path of ['/tables/_rbac_users', '/tables/_rbac_users/schema', '/tables/_table_acls/acl']) {
                    const r = await authGet(`${BASE_URL}${path}`, key);
                    if (r.status !== 404) throw new Error(`GET ${path}: expected 404, got ${r.status}`);
                }
                const up = await uploadJSON(`${BASE_URL}/upload/_rbac_users`, [{ api_key: 'x', name: 'x' }], key);
                if (up.status !== 404) throw new Error(`upload into _rbac_users: expected 404, got ${up.status}`);
                const sink = await authPost(`${BASE_URL}/kafka-sink?topic=_rbac_users`, [{ value: { a: 1 } }], key);
                if (sink.status !== 400) throw new Error(`kafka-sink into _rbac_users: expected 400, got ${sink.status}`);
            });
        }

        await test('writer keeps RBAC-checked REST read and write on user tables', async () => {
            const read = await getData(`${BASE_URL}/tables/${T}`, WRITER_KEY);
            if (!read.ok) throw new Error(`read: ${read.status}`);
            const write = await uploadJSON(`${BASE_URL}/upload/${T}`, [{ id: 'w-sec', value: 'still-allowed' }], WRITER_KEY);
            if (!write.json?.success) throw new Error(`write: ${JSON.stringify(write.json)}`);
        });

        await test('superuser can query tables denied to other users and system tables', async () => {
            for (const table of [DENIED_TABLE, READER_DENIED_TABLE, '_rbac_users']) {
                const r = await authPost(`${BASE_URL}/query`, { sql: `SELECT count(*) AS n FROM "${table}"` }, SUPER_KEY);
                if (r.status !== 200) throw new Error(`${table}: ${r.status} ${JSON.stringify(r.json)}`);
            }
        });

        await test('superuser can run read-only raw SQL', async () => {
            const r = await authPost(`${BASE_URL}/query`, { sql: `SELECT count(*) AS n FROM "${T}"` }, SUPER_KEY);
            if (r.status !== 200 || !(Number(r.json?.rows?.[0]?.n) > 0)) throw new Error(`${r.status}: ${JSON.stringify(r.json)}`);
            const gql = await authPost(`${BASE_URL}/graphql`, { query: '{ sql(query: "SELECT 1 AS one") }' }, SUPER_KEY);
            if (gql.json?.errors) throw new Error(`graphql: ${JSON.stringify(gql.json)}`);
        });

        await test('superuser raw SQL is still read-only and cannot introspect connection details', async () => {
            const write = await authPost(`${BASE_URL}/query`, { sql: `DELETE FROM "${T}"` }, SUPER_KEY);
            if (write.status !== 403) throw new Error(`DELETE: expected 403, got ${write.status}`);
            const intro = await authPost(`${BASE_URL}/query`, { sql: 'SELECT * FROM duckdb_databases()' }, SUPER_KEY);
            if (intro.status !== 403) throw new Error(`duckdb_databases(): expected 403, got ${intro.status}`);
        });

        await test('superuser can create, query and delete a property graph', async () => {
            const create = await authPost(`${BASE_URL}/graphs`, { name: GRAPH, vertexTables: [T], edgeTables: [] }, SUPER_KEY);
            if (create.status !== 200) throw new Error(`create: ${create.status} ${JSON.stringify(create.json)}`);
            const query = await authPost(`${BASE_URL}/graphs/${GRAPH}/query`, { match: `(a:${T})`, columns: 'a.id AS id' }, SUPER_KEY);
            if (query.status !== 200 || !Array.isArray(query.json?.rows) || query.json.rows.length === 0) {
                throw new Error(`query: ${query.status} ${JSON.stringify(query.json)}`);
            }
            const del = await authDelete(`${BASE_URL}/graphs/${GRAPH}`, SUPER_KEY);
            if (del.status !== 200) throw new Error(`delete: ${del.status}`);
        });

        await test('superuser cannot reach system tables through the table endpoints either (404)', async () => {
            const r = await authGet(`${BASE_URL}/tables/_rbac_users`, SUPER_KEY);
            if (r.status !== 404) throw new Error(`Expected 404, got ${r.status}`);
        });
    });

    await describe('Table-scoped SQL for writers and readers', async () => {
        // Each role has one table it is denied (RBAC 'deny', not in the ACL).
        const CASES = [['writer', WRITER_KEY, DENIED_TABLE], ['reader', READER_KEY, READER_DENIED_TABLE]];
        const query = (sql, key, format) => authPost(`${BASE_URL}/query${format ? `?format=${format}` : ''}`, { sql }, key);
        const gql = (sql, key) => authPost(`${BASE_URL}/graphql`, { query: 'query Q($q: String!) { sql(query: $q) }', variables: { q: sql } }, key);
        const expectStatus = async (sql, key, status, re) => {
            const r = await query(sql, key);
            if (r.status !== status) throw new Error(`${sql}\n    expected ${status}, got ${r.status}: ${JSON.stringify(r.json)}`);
            if (re && !re.test(r.json?.error || '')) throw new Error(`${sql}\n    unexpected error: ${r.json?.error}`);
            return r;
        };

        for (const [role, key, denied] of CASES) {
            const deniedRe = new RegExp(`no read permission for table "${denied}"`);

            await test(`${role} can SELECT an allowed table via /query`, async () => {
                const r = await expectStatus(`SELECT id, value FROM "${T}" ORDER BY id`, key, 200);
                if (!Array.isArray(r.json?.rows) || r.json.rows.length === 0) throw new Error(JSON.stringify(r.json));
                await expectStatus('SELECT 1 AS one', key, 200);
                await expectStatus(`WITH x AS (SELECT * FROM ${T}) SELECT count(*) AS n FROM x JOIN main.${T} USING (id)`, key, 200);
                await expectStatus(`SELECT * FROM ducklake.main.${T.toUpperCase()}`, key, 200);
            });

            await test(`${role} can stream an allowed table as NDJSON`, async () => {
                const res = await fetch(`${BASE_URL}/query?format=ndjson`, {
                    method: 'POST', headers: { 'Content-Type': 'application/json', 'x-api-key': key },
                    body: JSON.stringify({ sql: `SELECT id FROM "${T}"` }),
                });
                const text = await res.text();
                if (res.status !== 200) throw new Error(`${res.status}: ${text}`);
                const lines = text.trim().split('\n').map(l => JSON.parse(l));
                if (!lines.at(-1)?._ichibi || lines.length < 2) throw new Error(text);
            });

            await test(`${role} can SELECT an allowed table via GraphQL sql`, async () => {
                const r = await gql(`SELECT count(*) AS n FROM ${T}`, key);
                if (r.json?.errors || !(Number(r.json?.data?.sql?.[0]?.n) > 0)) throw new Error(JSON.stringify(r.json));
            });

            await test(`${role} gets 403 naming a denied table, however it is referenced`, async () => {
                for (const sql of [
                    `SELECT * FROM "${denied}"`,
                    `SELECT * FROM ${denied.toUpperCase()}`,
                    `SELECT * FROM ducklake.main.${denied}`,
                    `SELECT * FROM ${T} JOIN ${denied} USING (id)`,
                    `SELECT * FROM ${T} WHERE id IN (SELECT id FROM ${denied})`,
                    `SELECT (SELECT max(value) FROM ${denied}) AS v`,
                    `SELECT * FROM (SELECT * FROM (SELECT * FROM ${denied}) a) b`,
                    `WITH x AS (SELECT * FROM ${denied}) SELECT * FROM x`,
                    `WITH ${denied} AS (SELECT * FROM ${denied}) SELECT * FROM ${denied}`,
                    `SELECT id FROM ${T} UNION ALL SELECT id FROM ${denied}`,
                    `SELECT * FROM ${T} WHERE EXISTS (SELECT 1 FROM ${denied} /* comment */)`,
                    `DESCRIBE ${denied}`,
                ]) {
                    await expectStatus(sql, key, 403, deniedRe);
                }
            });

            await test(`${role}: a CTE named like a denied table only shadows it`, async () => {
                const r = await expectStatus(`WITH ${denied} AS (SELECT * FROM ${T}) SELECT count(*) AS n FROM ${denied}`, key, 200);
                if (!(Number(r.json?.rows?.[0]?.n) > 0)) throw new Error(JSON.stringify(r.json));
            });

            await test(`${role} gets the 403 on NDJSON and GraphQL too (nothing streamed)`, async () => {
                const r = await query(`SELECT * FROM ${denied}`, key, 'ndjson');
                if (r.status !== 403 || !deniedRe.test(r.json?.error || '')) throw new Error(`${r.status}: ${JSON.stringify(r.json)}`);
                const g = await gql(`SELECT * FROM ${T} WHERE id IN (SELECT id FROM ${denied})`, key);
                if (!g.json?.errors || !deniedRe.test(g.json.errors[0]?.message || '')) throw new Error(JSON.stringify(g.json));
            });

            await test(`${role}: system tables are not found, other catalogs refused`, async () => {
                for (const sql of ['SELECT * FROM _rbac_users', 'SELECT * FROM ducklake.main._table_acls',
                    `SELECT * FROM ${T} WHERE id IN (SELECT api_key FROM "_rbac_users")`, 'SELECT * FROM _graph_registry',
                    'SELECT * FROM duckdb_tables', `SELECT * FROM no_such_table_${Date.now()}`, "SELECT * FROM 'x.parquet'"]) {
                    await expectStatus(sql, key, 404, /not found/);
                }
                for (const sql of ['SELECT * FROM information_schema.tables', 'SELECT * FROM ichibi_console.main._console_users',
                    'SELECT * FROM __ducklake_metadata_ducklake.public.ducklake_table', 'SELECT * FROM memory.main.x',
                    'SHOW TABLES']) {
                    await expectStatus(sql, key, 403);
                }
            });

            await test(`${role}: table functions, writes and multiple statements are rejected`, async () => {
                for (const sql of ["SELECT * FROM read_parquet('/data/**/*.parquet')", "SELECT * FROM read_csv('/etc/passwd')",
                    "SELECT * FROM glob('/data/*')", `SELECT * FROM query('SELECT * FROM ${denied}')`, 'SELECT * FROM range(3)',
                    `WITH x AS (SELECT * FROM read_json('/etc/hosts')) SELECT * FROM x`]) {
                    await expectStatus(sql, key, 403, /Table function/);
                }
                await expectStatus(`DELETE FROM ${T}`, key, 403);
                await expectStatus(`SELECT * FROM ${T}; SELECT * FROM ${denied}`, key, 400, /one SQL statement/);
            });
        }

        await test('writer can query the table denied to the reader and vice versa', async () => {
            await expectStatus(`SELECT * FROM ${READER_DENIED_TABLE}`, WRITER_KEY, 200);
            await expectStatus(`SELECT * FROM ${DENIED_TABLE}`, READER_KEY, 200);
        });
    });

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

        await test('Drop reader-denied table', async () => {
            try {
                await fetch(`${BASE_URL}/query`, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json', 'x-api-key': SUPER_KEY },
                    body: JSON.stringify({ sql: `DROP TABLE IF EXISTS "${READER_DENIED_TABLE}"` }),
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
