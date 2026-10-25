/**
 * Integration tests for the admin console (/console) against a running stack.
 *
 * To avoid claiming first-run setup on a real deployment, the suite never
 * completes setup. It creates a temporary admin with the recovery CLI inside
 * the gateway container (docker exec) and deletes it at the end. Override the
 * container with CONSOLE_CONTAINER, or skip the CLI by providing existing
 * credentials via CONSOLE_ADMIN_USER / CONSOLE_ADMIN_PASSWORD.
 */
import { execFileSync } from 'child_process';
import { API_KEY, BASE_URL } from './_env.js';

const CONTAINER = process.env.CONSOLE_CONTAINER || 'ichibi-lake';
const TS = Date.now();

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }

/** Minimal cookie-aware client for the console API. */
function client() {
    let cookie = '';
    return async function call(method, path, body, { headers = {}, raw = false, rawBody } = {}) {
        const res = await fetch(`${BASE_URL}/console/api${path}`, {
            method,
            headers: {
                'Content-Type': rawBody !== undefined ? 'application/octet-stream' : 'application/json',
                'X-Console-Request': '1',
                ...(cookie ? { Cookie: cookie } : {}),
                ...headers,
            },
            body: rawBody !== undefined ? rawBody : body === undefined ? undefined : JSON.stringify(body),
        });
        const set = res.headers.get('set-cookie');
        if (set) cookie = set.split(';')[0].endsWith('=') ? '' : set.split(';')[0];
        if (raw) return res;
        const data = await res.json().catch(() => ({}));
        return { status: res.status, data };
    };
}

function cli(...args) {
    return execFileSync('docker', ['exec', CONTAINER, 'node', 'scripts/console-admin.js', ...args], { encoding: 'utf8' });
}

async function gateway(method, path, body, key = API_KEY) {
    const res = await fetch(`${BASE_URL}${path}`, {
        method,
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: res.status, data: await res.json().catch(() => ({})) };
}

async function run() {
    console.log('--- Starting Admin Console Integration Tests ---\n');

    console.log('▸ Static assets & public endpoints');
    await test('GET /console/ serves the app with a strict CSP', async () => {
        const res = await fetch(`${BASE_URL}/console/`);
        assert(res.status === 200, `status ${res.status}`);
        assert((await res.text()).includes('app.js'), 'index.html missing app.js');
        const csp = res.headers.get('content-security-policy') || '';
        assert(csp.includes("script-src 'self'") && csp.includes("frame-ancestors 'none'"), `CSP: ${csp}`);
        assert(!res.headers.get('access-control-allow-origin'), 'console must not send permissive CORS headers');
    });
    await test('logo is served', async () => {
        const res = await fetch(`${BASE_URL}/console/logo-320.png`);
        assert(res.status === 200 && res.headers.get('content-type') === 'image/png', `status ${res.status}`);
    });

    const anon = client();
    const state = await anon('GET', '/state');
    await test('GET /state reports setup status', () => assert(state.status === 200 && typeof state.data.setupRequired === 'boolean', JSON.stringify(state)));
    if (state.data.setupRequired) {
        await test('setup rejects a wrong setup code', async () => {
            const r = await anon('POST', '/setup', { setupCode: 'WRONG-CODE-0000', username: 'nobody', password: 'irrelevant-pass' });
            assert(r.status === 403, `status ${r.status}`);
        });
    }
    await test('protected endpoints require a session', async () => {
        assert((await anon('GET', '/sections')).status === 401, 'sections without session');
    });

    // --- Obtain admin credentials -------------------------------------------------
    let adminUser = process.env.CONSOLE_ADMIN_USER;
    let adminPass = process.env.CONSOLE_ADMIN_PASSWORD;
    let createdViaCli = false;
    if (!adminUser || !adminPass) {
        adminUser = `ci_admin_${TS}`;
        try {
            const out = cli('create', adminUser, '--role', 'admin');
            adminPass = (out.match(/Password: (\S+)/) || [])[1];
            createdViaCli = !!adminPass;
        } catch (err) {
            console.warn(`\n  ! Skipping authenticated console tests: could not create a temporary admin via docker exec ${CONTAINER} (${String(err.message).split('\n')[0]}).`);
            console.warn('    Set CONSOLE_ADMIN_USER / CONSOLE_ADMIN_PASSWORD to run them against this deployment.');
            return;
        }
    }

    const admin = client();
    try {
        console.log('\n▸ Authentication');
        await test('wrong password is rejected', async () => {
            assert((await client()('POST', '/login', { username: adminUser, password: 'definitely-wrong' })).status === 401, 'expected 401');
        });
        await test('login sets an HttpOnly SameSite=Strict session cookie', async () => {
            const res = await admin('POST', '/login', { username: adminUser, password: adminPass }, { raw: true });
            assert(res.status === 200, `status ${res.status}`);
            const c = res.headers.get('set-cookie') || '';
            assert(/HttpOnly/.test(c) && /SameSite=Strict/.test(c) && /Path=\/console/.test(c), c);
        });
        await test('GET /me returns the signed-in admin', async () => {
            const r = await admin('GET', '/me');
            assert(r.status === 200 && r.data.user.role === 'admin', JSON.stringify(r.data));
        });
        await test('state-changing requests need the X-Console-Request header', async () => {
            const r = await admin('PUT', '/settings/MAX_BATCH_SIZE', { value: 5 }, { headers: { 'X-Console-Request': '0' } });
            assert(r.status === 403, `status ${r.status}`);
        });
        await test('cross-origin requests are rejected', async () => {
            const r = await admin('PUT', '/settings/MAX_BATCH_SIZE', { value: 5 }, { headers: { Origin: 'https://evil.example' } });
            assert(r.status === 403, `status ${r.status}`);
        });

        console.log('\n▸ Settings');
        await test('sections and settings are listed; secrets are masked', async () => {
            const r = await admin('GET', '/sections');
            assert(r.status === 200 && r.data.sections.length > 5, 'sections');
            const pw = r.data.settings.find(s => s.key === 'DUCKLAKE_PG_PASSWORD');
            assert(pw && !/^postgres$/.test(pw.value), `password not masked: ${pw?.value}`);
            assert(!r.data.settings.some(s => s.key === 'API_KEYS' || s.key === 'RBAC_SUPERUSERS'), 'API_KEYS / RBAC_SUPERUSERS must not appear on settings pages');
            assert(!JSON.stringify(r.data).includes(API_KEY), 'an API key leaked into the settings payload');
            assert((await admin('PUT', '/settings/API_KEYS', { value: 'x'.repeat(20) })).status === 400, 'API_KEYS must not be editable via the generic settings endpoint');
        });
        await test('read-only settings cannot be changed', async () => {
            const r = await admin('PUT', '/settings/DUCKLAKE_PG_HOST', { value: 'evil' });
            assert(r.status === 400, `status ${r.status}`);
        });
        await test('invalid values are rejected', async () => {
            assert((await admin('PUT', '/settings/MAX_BATCH_SIZE', { value: -1 })).status === 400, 'negative');
            assert((await admin('PUT', '/settings/DEFAULT_PARTITION_KEY', { value: 'a; DROP TABLE x' })).status === 400, 'injection');
        });
        await test('MAX_BATCH_SIZE override applies live to /upload, and reset restores it', async () => {
            const rows = Array.from({ length: 5 }, (_, i) => ({ id: i }));
            assert((await admin('PUT', '/settings/MAX_BATCH_SIZE', { value: 3 })).status === 200, 'save');
            const blocked = await gateway('POST', `/upload/console_batch_${TS}`, rows);
            assert(blocked.status === 400 && /Maximum 3/.test(blocked.data.error), `expected 400, got ${blocked.status} ${JSON.stringify(blocked.data)}`);
            const reset = await admin('DELETE', '/settings/MAX_BATCH_SIZE');
            assert(reset.status === 200 && reset.data.setting.source !== 'console', 'reset');
            const ok = await gateway('POST', `/upload/console_batch_${TS}`, rows);
            assert(ok.status === 200, `upload after reset: ${ok.status} ${JSON.stringify(ok.data)}`);
        });

        console.log('\n▸ API keys');
        let newKey, newKeyId;
        await test('generated API key is returned once and works immediately', async () => {
            const r = await admin('POST', '/api-keys', {});
            assert(r.status === 200 && r.data.key, JSON.stringify(r.data));
            newKey = r.data.key; newKeyId = r.data.id;
            const listed = await admin('GET', '/api-keys');
            assert(listed.data.keys.some(k => k.id === newKeyId && !k.masked.includes(newKey)), 'listed masked');
            assert((await gateway('GET', '/tables', undefined, newKey)).status === 200, 'new key rejected');
        });
        await test('revoked API key is rejected immediately (RBAC row deactivated too)', async () => {
            assert((await admin('DELETE', `/api-keys/${newKeyId}`)).status === 200, 'revoke');
            const r = await gateway('GET', '/tables', undefined, newKey);
            assert(r.status === 401, `expected 401 after revoke, got ${r.status}`);
            assert((await gateway('GET', '/tables')).status === 200, 'original key must still work');
        });
        await test('reverting to docker/.env restores the env key list', async () => {
            const r = await admin('POST', '/api-keys/reset');
            assert(r.status === 200, `status ${r.status}`);
            const listed = await admin('GET', '/api-keys');
            assert(listed.data.source !== 'console', `source ${listed.data.source}`);
            assert((await gateway('GET', '/tables')).status === 200, 'original key must work');
        });

        await test('env-defined RBAC users are marked and cannot be changed from the console', async () => {
            const r = await admin('GET', '/rbac/users');
            const envKey = r.data.users.find(u => u.managedBy);
            assert(envKey, `expected an env/API_KEYS-managed user: ${JSON.stringify(r.data.users.map(u => [u.name, u.managedBy]))}`);
            const p = await admin('PATCH', `/rbac/users/${envKey.id}`, { active: false });
            assert(p.status === 409, `status ${p.status}`);
            assert((await gateway('GET', '/tables')).status === 200, 'env key must still work');
        });

        console.log('\n▸ Console logins');
        const viewer = `ci_viewer_${TS}`;
        const viewerPass = `viewer-pass-${TS}`;
        await test('admin can add a viewer login', async () => {
            const r = await admin('POST', '/logins', { username: viewer, password: viewerPass, role: 'viewer' });
            assert(r.status === 200, JSON.stringify(r.data));
        });
        await test('duplicate usernames are rejected', async () => {
            assert((await admin('POST', '/logins', { username: viewer, password: viewerPass, role: 'viewer' })).status === 409, 'expected 409');
        });
        await test('viewer can read but not change settings', async () => {
            const v = client();
            assert((await v('POST', '/login', { username: viewer, password: viewerPass })).status === 200, 'viewer login');
            assert((await v('GET', '/sections')).status === 200, 'viewer read');
            assert((await v('PUT', '/settings/MAX_BATCH_SIZE', { value: 10 })).status === 403, 'viewer write must be 403');
        });
        await test('password reset by an admin ends the old session', async () => {
            const v = client();
            await v('POST', '/login', { username: viewer, password: viewerPass });
            assert((await admin('PATCH', `/logins/${viewer}`, { password: `${viewerPass}-new` })).status === 200, 'reset');
            assert((await v('GET', '/me')).status === 401, 'old session should be invalid');
        });
        await test('login timestamps are returned as ISO-8601 strings', async () => {
            const r = await admin('GET', '/logins');
            const me = r.data.users.find(u => u.username === adminUser);
            assert(typeof me?.last_login === 'string' && /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$/.test(me.last_login), `last_login: ${JSON.stringify(me?.last_login)}`);
        });
        await test('admin cannot delete their own login', async () => {
            assert((await admin('DELETE', `/logins/${adminUser}`)).status === 400, 'expected 400');
        });
        await test('admin can delete the viewer', async () => {
            assert((await admin('DELETE', `/logins/${viewer}`)).status === 200, 'delete');
        });

        console.log('\n▸ Pruning');
        const now = new Date();
        const daysAgo = (d) => new Date(now.getTime() - d * 86400000).toISOString().replace('T', ' ').slice(0, 19);
        const P = `prune_${TS}`;
        const seed = (name, ages) => gateway('POST', `/upload/${name}`, ages.map((d, i) => ({ id: `${name}_${i}`, event_time: daysAgo(d) })));
        const ids = async (table) => ((await gateway('POST', '/query', { sql: `SELECT id FROM ${table} ORDER BY id` })).data.rows || []).map(r => r.id);
        async function waitForJob(jobId, timeoutMs = 60000) {
            const until = Date.now() + timeoutMs;
            while (Date.now() < until) {
                const jobs = (await admin('GET', '/retention/runs?limit=50')).data.jobs || [];
                const job = jobs.find(j => j.jobId === jobId);
                if (job && job.status !== 'running') return job;
                await new Promise(r => setTimeout(r, 1000));
            }
            throw new Error(`job ${jobId} did not finish within ${timeoutMs}ms`);
        }
        const createRule = async (rule) => {
            const r = await admin('POST', '/retention/rules', { schedule: '0 2 * * *', ...rule });
            assert(r.status === 200, `create rule: ${JSON.stringify(r.data)}`);
            return r.data.id;
        };
        const runRule = async (id) => {
            const r = await admin('POST', `/retention/rules/${id}/run`);
            assert(r.status === 202, `run: ${JSON.stringify(r.data)}`);
            return waitForJob(r.data.jobId);
        };
        const createdRules = [];

        await test('seed tables: two matching, one excluded, one without the date column', async () => {
            assert((await seed(`${P}_a`, [400, 100, 1])).status === 200, 'a');
            assert((await seed(`${P}_b`, [50, 40, 3, 2, 1])).status === 200, 'b');
            assert((await seed(`${P}_keep`, [400, 400])).status === 200, 'keep');
            assert((await gateway('POST', `/upload/${P}_nodate`, [{ id: 'x' }])).status === 200, 'nodate');
        });
        await test('a rule prunes one table: system tables, wildcards and lists are rejected', async () => {
            for (const table of ['_rbac_users', '_console_users', '_*', '*', `${P}_*`, `${P}_a, ${P}_b`]) {
                const r = await admin('POST', '/retention/rules', { name: 'bad', table, maxRows: 1, schedule: '@daily' });
                assert(r.status === 400, `${table} -> ${r.status}`);
                assert((await admin('POST', '/retention/preview', { table, maxRows: 1 })).status === 400, `preview ${table}`);
            }
        });
        await test('invalid rules and schedules are rejected', async () => {
            assert((await admin('POST', '/retention/rules', { name: 'x', table: `${P}_a`, schedule: '@daily' })).status === 400, 'no criterion');
            assert((await admin('POST', '/retention/rules', { name: 'x', table: `${P}_a`, maxRows: 1, schedule: '61 * * * *' })).status === 400, 'bad cron');
            const sched = await admin('POST', '/retention/schedule', { schedule: '*/15 * * * *' });
            assert(sched.data.valid && sched.data.next.length === 3, JSON.stringify(sched.data));
            assert((await admin('POST', '/retention/schedule', { schedule: 'nope' })).data.valid === false, 'invalid cron preview');
        });
        await test('preview: per-table counts; a table without the column is explained', async () => {
            const r = await admin('POST', '/retention/preview', { table: `${P}_a`, maxAgeValue: 30, dateColumn: 'event_time' });
            assert(r.data.tables.length === 1 && r.data.tables[0].total === 2, JSON.stringify(r.data.tables));
            const nd = await admin('POST', '/retention/preview', { table: `${P}_nodate`, maxAgeValue: 30, dateColumn: 'event_time' });
            assert(nd.data.tables[0].status === 'skipped' && /event_time/.test(nd.data.tables[0].reason), JSON.stringify(nd.data.tables[0]));
            assert((await ids(`${P}_a`)).length === 3, 'preview must not delete');
        });
        await test('age rule: run now deletes only the old rows of its table', async () => {
            const id = await createRule({ name: `age ${TS}`, table: `${P}_a`, maxAgeValue: 30, dateColumn: 'event_time' });
            createdRules.push(id);
            const job = await runRule(id);
            assert(job.status === 'succeeded' && job.rowsDeleted === 2 && job.tables.length === 1, JSON.stringify({ status: job.status, deleted: job.rowsDeleted, msg: job.message }));
            assert(job.tables[0].rowsByAge === 2, JSON.stringify(job.tables[0]));
            assert((await ids(`${P}_a`)).join() === `${P}_a_2`, `a: ${await ids(`${P}_a`)}`);
            assert((await ids(`${P}_b`)).length === 5 && (await ids(`${P}_keep`)).length === 2, 'other tables must be untouched');
            const rule = (await admin('GET', '/retention')).data.rules.find(r => r.id === id);
            assert(rule.lastStatus === 'succeeded' && rule.lastDeleted === 2 && rule.matches.length === 1 && !rule.legacyMultiTable, JSON.stringify(rule));
        });
        await test('cloning: the same settings saved for another table give an independent rule', async () => {
            const source = (await admin('GET', '/retention')).data.rules.find(r => r.id === createdRules[0]);
            const { id: _id, ...settings } = source;
            const cloneId = await createRule({ ...settings, name: `${source.name} (copy)`, table: `${P}_b` });
            createdRules.push(cloneId);
            const prev = await admin('POST', `/retention/rules/${cloneId}/preview`);
            assert(prev.data.tables.length === 1 && prev.data.tables[0].table === `${P}_b` && prev.data.tables[0].total === 2, JSON.stringify(prev.data));
            const rules = (await admin('GET', '/retention')).data.rules;
            assert(rules.find(r => r.id === createdRules[0]).tablePattern === `${P}_a`, 'source rule unchanged');
        });
        await test('the enable toggle works on its own and reschedules', async () => {
            const id = createdRules[1];
            assert((await admin('PATCH', `/retention/rules/${id}`, { enabled: false })).status === 200, 'disable');
            let r = (await admin('GET', '/retention')).data.rules.find(x => x.id === id);
            assert(!r.enabled && r.nextRunMs === null, JSON.stringify(r));
            assert((await admin('PATCH', `/retention/rules/${id}`, { enabled: true })).status === 200, 'enable');
            r = (await admin('GET', '/retention')).data.rules.find(x => x.id === id);
            assert(r.enabled && r.nextRunMs > Date.now(), JSON.stringify(r));
            assert((await admin('PATCH', `/retention/rules/${id}`, { enabled: 'yes' })).status === 400, 'non-boolean');
        });
        await test('count rule keeps the newest N rows by a column', async () => {
            assert((await seed(`${P}_c`, [5, 4, 3, 2, 1])).status === 200, 'seed c');
            const id = await createRule({ name: `count ${TS}`, table: `${P}_c`, maxRows: 2, orderColumn: 'event_time' });
            createdRules.push(id);
            const job = await runRule(id);
            assert(job.rowsDeleted === 3, JSON.stringify(job));
            assert((await ids(`${P}_c`)).join() === `${P}_c_3,${P}_c_4`, `c: ${await ids(`${P}_c`)}`);
        });
        await test('age and count combined: a row is deleted if it fails either', async () => {
            assert((await seed(`${P}_d`, [100, 10, 3, 2, 1])).status === 200, 'seed d');
            const prev = await admin('POST', '/retention/preview', { table: `${P}_d`, maxAgeValue: 30, dateColumn: 'event_time', maxRows: 3, orderColumn: 'event_time' });
            const t = prev.data.tables[0];
            assert(t.byAge === 1 && t.byCount === 2 && t.total === 2, JSON.stringify(t));
        });
        await test('safety limit refuses a run that would delete too much, deleting nothing', async () => {
            assert((await seed(`${P}_e`, [90, 80, 70])).status === 200, 'seed e');
            const id = await createRule({ name: `safety ${TS}`, table: `${P}_e`, maxAgeValue: 30, dateColumn: 'event_time', safetyLimit: 1 });
            createdRules.push(id);
            const job = await runRule(id);
            assert(job.status === 'failed' && job.tables[0].status === 'failed' && /safety limit/.test(job.tables[0].message), JSON.stringify(job));
            assert((await ids(`${P}_e`)).length === 3, 'rows were deleted despite the safety limit');
        });
        const epochAgo = (d, unit) => { const ms = now.getTime() - d * 86400000; return unit === 's' ? Math.floor(ms / 1000) : ms; };
        await test('epoch date columns: BIGINT milliseconds and seconds, unit auto-detected', async () => {
            const msRows = [90, 60, 1].map((d, i) => ({ id: `ms_${i}`, ts: epochAgo(d, 'ms') }));
            const sRows = [90, 60, 1].map((d, i) => ({ id: `s_${i}`, ts: epochAgo(d, 's') }));
            assert((await gateway('POST', `/upload/${P}_epoch_ms`, msRows)).status === 200, 'seed ms');
            assert((await gateway('POST', `/upload/${P}_epoch_s`, sRows)).status === 200, 'seed s');
            const schema = await admin('GET', `/data/tables/${P}_epoch_ms/schema`);
            const tsType = schema.data.columns.find(c => c.name === 'ts').type;
            assert(/INT/.test(tsType), `expected an integer epoch column, got ${tsType}`);
            const by = {};
            for (const t of [`${P}_epoch_ms`, `${P}_epoch_s`]) {
                by[t] = (await admin('POST', '/retention/preview', { table: t, maxAgeValue: 30, dateColumn: 'ts' })).data.tables[0];
            }
            assert(by[`${P}_epoch_ms`].total === 2 && /milliseconds \(detected\)/.test(by[`${P}_epoch_ms`].dateInfo), JSON.stringify(by[`${P}_epoch_ms`]));
            assert(by[`${P}_epoch_s`].total === 2 && /seconds \(detected\)/.test(by[`${P}_epoch_s`].dateInfo), JSON.stringify(by[`${P}_epoch_s`]));
        });
        await test('an explicit epoch unit overrides detection', async () => {
            const wrong = await admin('POST', '/retention/preview', { table: `${P}_epoch_ms`, maxAgeValue: 30, dateColumn: 'ts', epochUnit: 's' });
            assert(wrong.data.tables[0].total === 0 && /epoch seconds$/.test(wrong.data.tables[0].dateInfo), `ms values read as seconds should match nothing: ${JSON.stringify(wrong.data.tables[0])}`);
            const right = await admin('POST', '/retention/preview', { table: `${P}_epoch_ms`, maxAgeValue: 30, dateColumn: 'ts', epochUnit: 'ms' });
            assert(right.data.tables[0].total === 2, JSON.stringify(right.data.tables[0]));
        });
        await test('running an epoch rule deletes only the old rows and logs the unit used', async () => {
            let deleted = 0;
            for (const t of [`${P}_epoch_ms`, `${P}_epoch_s`]) {
                const id = await createRule({ name: `epoch ${t}`, table: t, maxAgeValue: 30, dateColumn: 'ts' });
                createdRules.push(id);
                const job = await runRule(id);
                assert(job.status === 'succeeded', JSON.stringify(job));
                assert(/Age by ts as epoch (milliseconds|seconds) \(detected\)/.test(job.tables[0].message), job.tables[0].message);
                deleted += job.rowsDeleted;
            }
            assert(deleted === 4, `deleted ${deleted}`);
            assert((await ids(`${P}_epoch_ms`)).join() === 'ms_2' && (await ids(`${P}_epoch_s`)).join() === 's_2', 'wrong rows remain');
        });
        await test('non-date, non-numeric columns are skipped with a reason', async () => {
            const r = await admin('POST', '/retention/preview', { table: `${P}_epoch_ms`, maxAgeValue: 30, dateColumn: 'id' });
            assert(r.data.tables[0].status === 'skipped' && /not a date, timestamp or epoch number/.test(r.data.tables[0].reason), JSON.stringify(r.data.tables[0]));
        });
        await test('match conditions: delete rows matching simple conditions', async () => {
            const rows = [
                { id: 'm1', level: 'debug', n: 5 }, { id: 'm2', level: 'info', n: 50 }, { id: 'm3', level: 'DEBUG', n: 500 },
                { id: 'm4', level: "x'); DROP TABLE t; --", n: 1 },
            ];
            assert((await gateway('POST', `/upload/${P}_match`, rows)).status === 200, 'seed');
            const cond = (items, match = 'all') => ({ table: `${P}_match`, conditions: { match, items } });
            const p1 = await admin('POST', '/retention/preview', cond([{ column: 'level', op: 'equals', value: 'debug' }]));
            assert(p1.data.tables[0].byMatch === 1 && p1.data.tables[0].total === 1, `equals is exact: ${JSON.stringify(p1.data.tables[0])}`);
            const p2 = await admin('POST', '/retention/preview', cond([{ column: 'level', op: 'contains', value: 'debug' }]));
            assert(p2.data.tables[0].total === 2, `contains is case-insensitive: ${JSON.stringify(p2.data.tables[0])}`);
            const p3 = await admin('POST', '/retention/preview', cond([{ column: 'level', op: 'contains', value: 'debug' }, { column: 'n', op: 'gt', value: '100' }]));
            assert(p3.data.tables[0].total === 1, `all: ${JSON.stringify(p3.data.tables[0])}`);
            const p4 = await admin('POST', '/retention/preview', cond([{ column: 'n', op: 'in', value: '5, 50' }, { column: 'level', op: 'equals', value: 'DEBUG' }], 'any'));
            assert(p4.data.tables[0].total === 3, `any: ${JSON.stringify(p4.data.tables[0])}`);
            const inj = await admin('POST', '/retention/preview', cond([{ column: 'level', op: 'equals', value: "x'); DROP TABLE t; --" }]));
            assert(inj.data.tables[0].total === 1, `an SQL-looking value is matched literally: ${JSON.stringify(inj.data.tables[0])}`);
            assert((await admin('POST', '/retention/preview', cond([{ column: 'level', op: 'raw', value: '1=1' }]))).status === 400, 'unknown operator rejected');
            assert((await admin('POST', '/retention/preview', cond([{ column: 'level; DROP TABLE x', op: 'equals', value: '1' }]))).data.tables[0].status === 'skipped',
                'unknown column skips the table');
            const id = await createRule({ name: `match ${TS}`, ...cond([{ column: 'level', op: 'contains', value: 'debug' }]) });
            createdRules.push(id);
            const job = await runRule(id);
            assert(job.rowsDeleted === 2 && job.tables[0].rowsByMatch === 2, JSON.stringify(job));
            assert((await ids(`${P}_match`)).join() === 'm2,m4', `remaining: ${await ids(`${P}_match`)}`);
        });
        await test('disabling a rule clears its next run; edits are validated', async () => {
            const id = createdRules[0];
            const rule = (await admin('GET', '/retention')).data.rules.find(r => r.id === id);
            assert((await admin('PUT', `/retention/rules/${id}`, { ...rule, enabled: false })).status === 200, 'disable');
            const after = (await admin('GET', '/retention')).data.rules.find(r => r.id === id);
            assert(!after.enabled && after.nextRunMs === null, JSON.stringify(after));
            assert((await admin('PUT', `/retention/rules/${id}`, { ...rule, table: '_rbac_users' })).status === 400, 'system table on edit');
            assert((await admin('PUT', `/retention/rules/${id}`, { ...rule, table: `${P}_*` })).status === 400, 'wildcard on edit');
        });
        await test('scheduled rules run automatically (every-minute schedule)', async () => {
            assert((await seed(`${P}_f`, [60, 1])).status === 200, 'seed f');
            const id = await createRule({ name: `sched ${TS}`, table: `${P}_f`, maxAgeValue: 30, dateColumn: 'event_time', schedule: '* * * * *' });
            createdRules.push(id);
            const until = Date.now() + 120000;
            let job = null;
            while (Date.now() < until && !job) {
                await new Promise(r => setTimeout(r, 3000));
                job = ((await admin('GET', `/retention/runs?rule=${id}`)).data.jobs || []).find(j => j.trigger === 'schedule' && j.status !== 'running');
            }
            assert(job, 'no scheduled run within 120s');
            assert(job.status === 'succeeded' && job.rowsDeleted === 1, JSON.stringify(job));
            const rule = (await admin('GET', '/retention')).data.rules.find(r => r.id === id);
            assert(rule.nextRunMs > Date.now() - 1000, 'next run should be scheduled in the future');
        });
        await test('legacy multi-table rules keep working and are flagged', async () => {
            // Simulate a rule saved before rules were limited to one table.
            const legacyId = `legacy${TS}`;
            const script = `import('./src/console/store.js').then(async (s) => { await s.createPruneRule({ id: '${legacyId}', name: 'legacy ${TS}', tablePattern: '${P}_g*', excludePattern: '', dateColumn: 'event_time', maxAgeValue: 30, maxAgeUnit: 'days', schedule: '@daily', enabled: true, user: 'test' }); process.exit(0); })`;
            execFileSync('docker', ['exec', CONTAINER, 'node', '-e', script]);
            createdRules.push(legacyId);
            assert((await seed(`${P}_g1`, [90, 1])).status === 200 && (await seed(`${P}_g2`, [90, 1])).status === 200, 'seed');
            const rule = (await admin('GET', '/retention')).data.rules.find(r => r.id === legacyId);
            assert(rule && rule.legacyMultiTable && rule.matches.length === 2, JSON.stringify(rule));
            assert((await admin('PATCH', `/retention/rules/${legacyId}`, { enabled: false })).status === 200, 'toggle works for legacy rules');
            const prev = await admin('POST', `/retention/rules/${legacyId}/preview`);
            assert(prev.status === 200 && prev.data.tables.length === 2 && prev.data.totalRows === 2, JSON.stringify(prev.data));
            const job = await runRule(legacyId);
            assert(job.status === 'succeeded' && job.rowsDeleted === 2, `legacy rule still runs on every matching table: ${JSON.stringify(job)}`);
            assert((await admin('PUT', `/retention/rules/${legacyId}`, rule)).status === 400, 'saving it unchanged requires choosing one table');
        });
        await test('rules can be deleted', async () => {
            for (const id of createdRules) assert((await admin('DELETE', `/retention/rules/${id}`)).status === 200, `delete ${id}`);
            const left = (await admin('GET', '/retention')).data.rules.filter(r => createdRules.includes(r.id));
            assert(left.length === 0, 'rules remain');
        });

        console.log('\n▸ Tables & data browser');
        const DT = `console_data_${TS}`;
        const enc = encodeURIComponent;
        const q = (body, table = DT) => admin('POST', `/data/tables/${enc(table)}/rows/query`, body);
        await test('seed a 120-row table (including a column name with a space)', async () => {
            const rows = Array.from({ length: 120 }, (_, i) => ({ id: `r${String(i + 1).padStart(3, '0')}`, n: i + 1, name: `name ${i + 1}`, 'first name': `fn${i + 1}` }));
            const r = await gateway('POST', `/upload/${DT}`, rows);
            assert(r.status === 200, JSON.stringify(r.data));
        });
        await test('table list includes user tables and hides system tables', async () => {
            const r = await admin('GET', '/data/tables');
            const t = r.data.tables.find(x => x.name === DT);
            assert(t && t.rows === 120 && t.columns === 4, JSON.stringify(t));
            assert(!r.data.tables.some(x => x.name.startsWith('_')), 'system tables listed');
        });
        await test('system tables are unreachable (schema, rows, edits)', async () => {
            assert((await admin('GET', '/data/tables/_console_users/schema')).status === 404, 'schema');
            assert((await q({}, '_console_users')).status === 404, 'rows');
            assert((await admin('PATCH', '/data/tables/_console_settings/rows/0', { column: 'value', value: 'x', original: null })).status === 404, 'edit');
            assert((await admin('DELETE', '/data/tables/_rbac_users', { confirm: '_rbac_users' })).status === 404, 'drop');
        });
        await test('paging: page size, totals, last page', async () => {
            const p1 = await q({ page: 1, pageSize: 50 });
            assert(p1.data.rows.length === 50 && p1.data.total === 120 && p1.data.pages === 3, JSON.stringify({ n: p1.data.rows?.length, t: p1.data.total, p: p1.data.pages }));
            const p3 = await q({ page: 3, pageSize: 50 });
            assert(p3.data.rows.length === 20 && p3.data.page === 3, `page3 rows ${p3.data.rows.length}`);
            const beyond = await q({ page: 99, pageSize: 50 });
            assert(beyond.data.page === 3, 'page clamps to the last page');
        });
        await test('sorting by a column (desc); unknown sort column rejected', async () => {
            const r = await q({ pageSize: 25, sort: { column: 'n', dir: 'desc' } });
            const nIdx = r.data.columns.findIndex(c => c.name === 'n');
            assert(r.data.rows[0].values[nIdx] === '120', `first n = ${r.data.rows[0].values[nIdx]}`);
            assert((await q({ sort: { column: 'nope' } })).status === 400, 'unknown sort column');
        });
        await test('filters and global search', async () => {
            const f = await q({ filters: [{ column: 'n', op: 'gte', value: '100' }] });
            assert(f.data.total === 21, `gte total ${f.data.total}`);
            const s = await q({ search: 'r05' });
            assert(s.data.total === 10, `search total ${s.data.total}`);
            const sp = await q({ filters: [{ column: 'first name', op: 'equals', value: 'fn7' }] });
            assert(sp.data.total === 1, `space column filter ${sp.data.total}`);
            assert((await q({ filters: [{ column: 'n; DROP TABLE x', op: 'equals', value: '1' }] })).status === 400, 'bad filter column');
        });
        let row1, cols;
        const val = (row, name) => row.values[cols.findIndex(c => c.name === name)];
        await test('inline edit updates a cell by rowid', async () => {
            const r = await q({ filters: [{ column: 'id', op: 'equals', value: 'r001' }] });
            cols = r.data.columns; row1 = r.data.rows[0];
            const u = await admin('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'name', value: 'edited', original: val(row1, 'name') });
            assert(u.status === 200 && u.data.value === 'edited', JSON.stringify(u.data));
            const after = await q({ filters: [{ column: 'id', op: 'equals', value: 'r001' }] });
            assert(val(after.data.rows[0], 'name') === 'edited' && after.data.rows[0].rowid === row1.rowid, 'value/rowid after edit');
        });
        await test('stale edits are rejected with 409 (optimistic concurrency)', async () => {
            const r = await admin('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'name', value: 'clobber', original: 'name 1' });
            assert(r.status === 409, `status ${r.status}`);
        });
        await test('cells can be set to NULL, and columns with spaces edited', async () => {
            const n = await admin('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'name', isNull: true, original: 'edited' });
            assert(n.status === 200 && n.data.value === null, JSON.stringify(n.data));
            const sp = await admin('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'first name', value: 'spaced', original: 'fn1' });
            assert(sp.status === 200 && sp.data.value === 'spaced', JSON.stringify(sp.data));
        });
        await test('invalid values for the column type are rejected', async () => {
            const r = await admin('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'n', value: 'not-a-number', original: '1' });
            assert(r.status === 400, `status ${r.status}`);
        });
        const viewer2 = `ci_viewer2_${TS}`;
        await test('viewers can browse but not edit data or schema', async () => {
            await admin('POST', '/logins', { username: viewer2, password: `viewer2-pass-${TS}`, role: 'viewer' });
            const v = client();
            await v('POST', '/login', { username: viewer2, password: `viewer2-pass-${TS}` });
            assert((await v('POST', `/data/tables/${DT}/rows/query`, {})).status === 200, 'viewer read');
            assert((await v('PATCH', `/data/tables/${DT}/rows/${row1.rowid}`, { column: 'name', value: 'x', original: null })).status === 403, 'viewer edit');
            assert((await v('POST', `/data/tables/${DT}/columns`, { name: 'x', type: 'VARCHAR' })).status === 403, 'viewer schema');
            await admin('DELETE', `/logins/${viewer2}`);
        });
        await test('insert and delete rows', async () => {
            assert((await admin('POST', `/data/tables/${DT}/rows`, { values: { id: { value: 'new' }, n: { value: '999' } } })).status === 200, 'insert');
            const ins = await q({ filters: [{ column: 'id', op: 'equals', value: 'new' }] });
            assert(ins.data.total === 1 && val(ins.data.rows[0], 'n') === '999', 'inserted row');
            const two = await q({ pageSize: 25 });
            const ids = two.data.rows.slice(0, 2).map(r => r.rowid);
            const d = await admin('POST', `/data/tables/${DT}/rows/delete`, { rowids: ids });
            assert(d.status === 200 && d.data.deleted === 2, JSON.stringify(d.data));
            assert((await q({})).data.total === 119, 'total after delete');
        });
        await test('schema: add a column with a default (backfills existing rows)', async () => {
            const r = await admin('POST', `/data/tables/${DT}/columns`, { name: 'score', type: 'DOUBLE', default: '1.5' });
            assert(r.status === 200, JSON.stringify(r.data));
            const s = await admin('GET', `/data/tables/${DT}/schema`);
            assert(s.data.columns.some(c => c.name === 'score' && c.type === 'DOUBLE'), 'column added');
            const any = await q({ pageSize: 25, filters: [{ column: 'score', op: 'equals', value: '1.5' }] });
            assert(any.data.total === 119, `backfilled rows: ${any.data.total}`);
        });
        await test('schema: rejects unsafe names and types, and duplicates', async () => {
            assert((await admin('POST', `/data/tables/${DT}/columns`, { name: 'bad name', type: 'VARCHAR' })).status === 400, 'name');
            assert((await admin('POST', `/data/tables/${DT}/columns`, { name: 'ok', type: 'VARCHAR; DROP TABLE x' })).status === 400, 'type');
            assert((await admin('POST', `/data/tables/${DT}/columns`, { name: 'score', type: 'VARCHAR' })).status === 409, 'duplicate');
        });
        await test('schema: rename, widen, default; unconvertible change refused', async () => {
            await admin('POST', `/data/tables/${DT}/columns`, { name: 'small', type: 'INTEGER' });
            const w = await admin('PATCH', `/data/tables/${DT}/columns/small`, { type: 'BIGINT', newName: 'big', default: '7' });
            assert(w.status === 200 && w.data.applied.length === 3, JSON.stringify(w.data));
            const bad = await admin('PATCH', `/data/tables/${DT}/columns/name`, { type: 'INTEGER' });
            assert(bad.status === 400 && /cannot be converted/i.test(bad.data.error), JSON.stringify(bad.data));
            const s = await admin('GET', `/data/tables/${DT}/schema`);
            const big = s.data.columns.find(c => c.name === 'big');
            assert(big && big.type === 'BIGINT' && big.default && big.default.includes('7'), JSON.stringify(big));
        });
        await test('type preview distinguishes in-place widening from conversion', async () => {
            await admin('POST', `/data/tables/${DT}/columns`, { name: 'tiny', type: 'SMALLINT' });
            const widen = await admin('POST', `/data/tables/${DT}/columns/tiny/type-preview`, { type: 'INTEGER' });
            assert(widen.data.method === 'in_place', JSON.stringify(widen.data));
            const conv = await admin('POST', `/data/tables/${DT}/columns/id/type-preview`, { type: 'INTEGER' });
            assert(conv.data.method === 'convert' && conv.data.invalid === conv.data.nonNull && conv.data.samples.length > 0, JSON.stringify(conv.data));
            await admin('DELETE', `/data/tables/${DT}/columns/tiny`);
        });
        await test('strict conversion refuses unconvertible values and changes nothing', async () => {
            const r = await admin('PATCH', `/data/tables/${DT}/columns/id`, { type: 'INTEGER' });
            assert(r.status === 400 && /cannot be converted/.test(r.data.error) && /"r\d{3}"|"new"/.test(r.data.error), JSON.stringify(r.data));
            const s = await admin('GET', `/data/tables/${DT}/schema`);
            assert(s.data.columns.find(c => c.name === 'id').type === 'VARCHAR', 'id type changed');
        });
        await test('converting BIGINT -> VARCHAR -> INTEGER preserves values and row ids', async () => {
            const before = await q({ filters: [{ column: 'id', op: 'equals', value: 'r003' }] });
            const rid = before.data.rows[0].rowid;
            const toText = await admin('PATCH', `/data/tables/${DT}/columns/n`, { type: 'VARCHAR' });
            assert(toText.status === 200 && /converted BIGINT -> VARCHAR/.test(toText.data.applied.join()), JSON.stringify(toText.data));
            let s = await admin('GET', `/data/tables/${DT}/schema`);
            assert(s.data.columns.at(-1).name === 'n' && s.data.columns.at(-1).type === 'VARCHAR', 'converted column moves to the end');
            const back = await admin('PATCH', `/data/tables/${DT}/columns/n`, { type: 'INTEGER' });
            assert(back.status === 200, JSON.stringify(back.data));
            const after = await q({ filters: [{ column: 'id', op: 'equals', value: 'r003' }] });
            const nIdx = after.data.columns.findIndex(c => c.name === 'n');
            assert(after.data.rows[0].rowid === rid && after.data.rows[0].values[nIdx] === '3' && after.data.columns[nIdx].type === 'INTEGER',
                JSON.stringify({ rid, row: after.data.rows[0], type: after.data.columns[nIdx]?.type }));
        });
        await test('lenient conversion sets unconvertible values to NULL', async () => {
            const r = await admin('PATCH', `/data/tables/${DT}/columns/name`, { type: 'INTEGER', onInvalid: 'null' });
            assert(r.status === 200, JSON.stringify(r.data));
            const nonNull = await q({ filters: [{ column: 'name', op: 'not_null' }] });
            assert(nonNull.data.total === 0, `non-null names: ${nonNull.data.total}`);
        });
        await test('schema: drop a column', async () => {
            assert((await admin('DELETE', `/data/tables/${DT}/columns/big`)).status === 200, 'drop');
            const s = await admin('GET', `/data/tables/${DT}/schema`);
            assert(!s.data.columns.some(c => c.name === 'big'), 'still present');
        });
        const DT2 = `${DT}_renamed`;
        await test('rename table moves its ACL; old name is gone', async () => {
            const r = await admin('PATCH', `/data/tables/${DT}`, { newName: DT2 });
            assert(r.status === 200, JSON.stringify(r.data));
            assert((await admin('GET', `/data/tables/${DT}/schema`)).status === 404, 'old name still exists');
            const acl = await gateway('POST', '/query', { sql: `SELECT count(*) AS n FROM _table_acls WHERE table_name = '${DT2}'` });
            assert(String(acl.data.rows?.[0]?.n) === '1', `acl rows for new name: ${JSON.stringify(acl.data)}`);
            assert((await gateway('GET', `/tables/${DT2}`)).status === 200, 'gateway REST reads renamed table');
        });
        await test('tables referenced by a property graph cannot be renamed or dropped', async () => {
            const g = `console_g_${TS}`;
            const created = await gateway('POST', '/graphs', { name: g, vertexTables: [DT2], edgeTables: [] });
            assert(created.status === 200, JSON.stringify(created.data));
            try {
                assert((await admin('PATCH', `/data/tables/${DT2}`, { newName: `${DT2}_x` })).status === 409, 'rename');
                assert((await admin('DELETE', `/data/tables/${DT2}`, { confirm: DT2 })).status === 409, 'drop');
                assert((await admin('DELETE', `/data/tables/${DT2}/columns/score`)).status === 409, 'drop column');
            } finally {
                await gateway('DELETE', `/graphs/${g}`);
            }
        });
        await test('drop table requires typing its name', async () => {
            assert((await admin('DELETE', `/data/tables/${DT2}`, { confirm: 'wrong' })).status === 400, 'wrong confirm');
            assert((await admin('DELETE', `/data/tables/${DT2}`, { confirm: DT2 })).status === 200, 'drop');
            assert((await admin('GET', `/data/tables/${DT2}/schema`)).status === 404, 'still exists');
        });
        await test('create a table; created/updated are filled on insert', async () => {
            const T3 = `console_new_${TS}`;
            const c = await admin('POST', '/data/tables', { name: T3, columns: [
                { name: 'id', type: 'VARCHAR', nullable: false }, { name: 'qty', type: 'INTEGER', default: '0' },
                { name: 'created', type: 'TIMESTAMP' }, { name: 'updated', type: 'TIMESTAMP' }] });
            assert(c.status === 200, JSON.stringify(c.data));
            assert((await admin('POST', '/data/tables', { name: T3, columns: [{ name: 'a', type: 'VARCHAR' }] })).status === 409, 'duplicate table');
            assert((await admin('POST', '/data/tables', { name: '_sneaky', columns: [{ name: 'a', type: 'VARCHAR' }] })).status === 400, 'reserved name');
            assert((await admin('POST', `/data/tables/${T3}/rows`, { values: { id: { value: 'a1' } } })).status === 200, 'insert');
            const r = await q({}, T3);
            const cc = r.data.columns;
            const row = r.data.rows[0];
            const get = (n) => row.values[cc.findIndex(x => x.name === n)];
            assert(get('qty') === '0' && get('created') && get('updated'), JSON.stringify(row));
            assert((await admin('DELETE', `/data/tables/${T3}`, { confirm: T3 })).status === 200, 'cleanup');
        });

        console.log('\n▸ Security hardening');
        const created = await admin('POST', '/rbac/users', { name: `ci-reader-${TS}`, roles: ['reader'] });
        const readerKey = created.data.apiKey;
        const readerId = created.data.id;
        const asReader = (method, path, body) => gateway(method, path, body, readerKey);
        const sq = (sql, key = API_KEY) => gateway('POST', '/query', { sql }, key);
        await test('system tables are unreachable over REST, even for superusers', async () => {
            for (const key of [readerKey, API_KEY]) {
                assert((await gateway('GET', '/tables/_rbac_users', undefined, key)).status === 404, 'GET table');
                assert((await gateway('GET', '/tables/_rbac_users/schema', undefined, key)).status === 404, 'schema');
                assert((await gateway('GET', '/tables/_table_acls/acl', undefined, key)).status === 404, 'acl');
            }
            assert((await asReader('POST', '/upload/_console_users', [{ username: 'x' }])).status === 404, 'upload into system table');
            assert((await asReader('POST', '/kafka-sink?topic=_rbac_users', [{ value: { a: 1 } }])).status === 400, 'kafka-sink into system table');
        });
        await test('readers keep RBAC-checked REST access to user tables', async () => {
            assert((await asReader('GET', `/tables/console_batch_${TS}`)).status === 200, 'reader read');
        });
        await test('user-supplied SQL and graph management are superuser-only', async () => {
            assert((await sq('SELECT 1', readerKey)).status === 403, '/query');
            const gql = await asReader('POST', '/graphql', { query: '{ sql(query: "SELECT 1 AS x") }' });
            assert(gql.data.errors && /superuser/i.test(gql.data.errors[0].message), `graphql: ${JSON.stringify(gql.data)}`);
            assert((await asReader('POST', '/graphs', { name: `g_${TS}`, vertexTables: [`console_batch_${TS}`], edgeTables: [] })).status === 403, 'create graph');
            assert((await asReader('POST', '/graphs/anything/query', { match: '(a)', columns: 'a.id' })).status === 403, 'graph query');
            assert((await asReader('DELETE', '/graphs/anything')).status === 403, 'delete graph');
        });
        await test('superuser raw SQL still works for user tables', async () => {
            const r = await sq(`SELECT count(*) AS n FROM console_batch_${TS}`);
            assert(r.status === 200 && Number(r.data.rows[0].n) >= 5, JSON.stringify(r.data));
        });
        await test('console tables are invisible to raw SQL (separate catalog)', async () => {
            for (const sql of ['SELECT * FROM ichibi_console.main._console_settings', 'SELECT * FROM _console_users', 'SELECT * FROM _console_settings']) {
                const r = await sq(sql);
                assert(r.status !== 200, `${sql} -> ${r.status} ${JSON.stringify(r.data).slice(0, 120)}`);
            }
        });
        await test('raw SQL cannot read server files or connection details', async () => {
            const file = await sq("SELECT content FROM read_text('/etc/hostname')");
            assert(file.status !== 200 && /Permission|disabled/i.test(file.data.error || ''), `read_text: ${JSON.stringify(file.data)}`);
            for (const sql of ['SELECT * FROM duckdb_databases()', 'SELECT * FROM "duckdb_databases"()', 'SELECT * FROM pragma_database_list',
                "SELECT current_setting('allowed_directories')", 'SELECT * FROM duckdb_settings()']) {
                assert((await sq(sql)).status === 403, `${sql} should be 403`);
            }
            const secrets = await sq('SELECT count(*) AS n FROM duckdb_secrets()');
            assert(secrets.status === 403, `duckdb_secrets: ${secrets.status}`);
        });
        await test('graphs cannot be defined over system tables', async () => {
            const r = await gateway('POST', '/graphs', { name: `sys_${TS}`, vertexTables: ['_rbac_users'], edgeTables: [] });
            assert(r.status === 400, `status ${r.status}`);
        });
        await test('deactivated reader key stops working', async () => {
            await admin('PATCH', `/rbac/users/${readerId}`, { active: false });
            assert((await asReader('GET', `/tables/console_batch_${TS}`)).status === 401, 'deactivated key');
        });

        console.log('\n▸ Health, export & import');
        const IMP = `console_import_${TS}`;
        const csv = 'id,name,score,note\r\n1,alpha,10,plain\r\n2,beta,20,"has, comma"\r\n3,gamma,30,"say ""hi"""\r\n';
        await test('overview includes readiness, query gate, exports and lake size', async () => {
            const r = await admin('GET', '/overview');
            const hl = r.data.health;
            assert(hl && hl.ready === true, `health: ${JSON.stringify(hl)}`);
            assert(hl.checks.ducklake === 'ok' && hl.checks.postgres === 'ok' && hl.checks.startup === 'ready', JSON.stringify(hl.checks));
            assert(typeof hl.userQuery.active === 'number' && typeof hl.userQuery.queued === 'number', JSON.stringify(hl.userQuery));
            assert(typeof hl.exports.active === 'number' && hl.exports.counters, JSON.stringify(hl.exports));
            assert(hl.lake && hl.lake.tables >= 0 && hl.lake.files >= 0, JSON.stringify(hl.lake));
            assert(Array.isArray(hl.kafkaLag), 'kafkaLag array');
        });
        await test('import dry run previews a CSV without writing', async () => {
            const r = await admin('POST', `/data/import?filename=people.csv&table=${IMP}&mode=create&dryRun=1`, undefined, { rawBody: csv });
            assert(r.status === 200, `status ${r.status} ${JSON.stringify(r.data)}`);
            assert(r.data.totalRows === 3 && r.data.format === 'csv', JSON.stringify(r.data));
            assert(r.data.columns.map(c => c.name).join(',') === 'id,name,score,note', JSON.stringify(r.data.columns));
            assert(r.data.preview[1][3] === 'has, comma' && r.data.preview[2][3] === 'say "hi"', JSON.stringify(r.data.preview));
            assert((await admin('GET', `/data/tables/${IMP}/schema`)).status === 404, 'dry run must not create the table');
        });
        await test('import creates a new table from CSV', async () => {
            const r = await admin('POST', `/data/import?filename=people.csv&table=${IMP}&mode=create`, undefined, { rawBody: csv });
            assert(r.status === 200 && r.data.inserted === 3, `status ${r.status} ${JSON.stringify(r.data)}`);
            const rows = await admin('POST', `/data/tables/${IMP}/rows/query`, { sort: { column: 'id', dir: 'asc' } });
            assert(rows.data.total === 3 && rows.data.rows[2].values[1] === 'gamma', JSON.stringify(rows.data));
        });
        await test('import into an existing name in create mode is 409', async () => {
            const r = await admin('POST', `/data/import?filename=people.csv&table=${IMP}&mode=create&dryRun=1`, undefined, { rawBody: csv });
            assert(r.status === 409, `status ${r.status}`);
        });
        await test('append dry run reports new columns; append adds them', async () => {
            const nd = '{"id": 4, "name": "delta", "score": 40, "extra": "x"}\n{"id": 5, "name": "epsilon", "score": 50, "extra": "y"}\n';
            const dry = await admin('POST', `/data/import?filename=more.ndjson&table=${IMP}&mode=append&dryRun=1`, undefined, { rawBody: nd });
            assert(dry.status === 200, `status ${dry.status} ${JSON.stringify(dry.data)}`);
            const extra = dry.data.plan.columns.find(c => c.name === 'extra');
            assert(extra?.action === 'add', JSON.stringify(dry.data.plan));
            assert(dry.data.plan.missing.includes('note'), 'note reported missing');
            const r = await admin('POST', `/data/import?filename=more.ndjson&table=${IMP}&mode=append`, undefined, { rawBody: nd });
            assert(r.status === 200 && r.data.inserted === 2 && r.data.added.includes('extra'), JSON.stringify(r.data));
            const schema = await admin('GET', `/data/tables/${IMP}/schema`);
            assert(schema.data.rows === 5 && schema.data.columns.some(c => c.name === 'extra'), JSON.stringify(schema.data));
        });
        await test('import round-trips a Parquet export', async () => {
            const res = await admin('GET', `/data/tables/${IMP}/export?format=parquet`, undefined, { raw: true });
            assert(res.status === 200 && res.headers.get('x-total-rows') === '5', `status ${res.status}`);
            const buf = Buffer.from(await res.arrayBuffer());
            assert(buf.subarray(0, 4).toString() === 'PAR1', 'parquet magic');
            const copy = `${IMP}_copy`;
            const r = await admin('POST', `/data/import?filename=x.parquet&table=${copy}&mode=create`, undefined, { rawBody: buf });
            assert(r.status === 200 && r.data.inserted === 5, JSON.stringify(r.data));
            await admin('DELETE', `/data/tables/${copy}`, { confirm: copy });
        });
        await test('export honours grid filters, sort and limit', async () => {
            const filters = encodeURIComponent(JSON.stringify([{ column: 'score', op: 'gte', value: '20' }]));
            const sort = encodeURIComponent(JSON.stringify({ column: 'score', dir: 'desc' }));
            const res = await admin('GET', `/data/tables/${IMP}/export?format=ndjson&filters=${filters}&sort=${sort}&limit=3`, undefined, { raw: true });
            assert(res.status === 200, `status ${res.status}`);
            assert(res.headers.get('x-total-rows') === '4' && res.headers.get('x-export-truncated') === 'true', 'headers');
            const lines = (await res.text()).trim().split('\n').map(l => JSON.parse(l));
            assert(lines.map(l => l.name).join(',') === 'epsilon,delta,gamma', JSON.stringify(lines));
        });
        await test('csv export has a header row and quotes values', async () => {
            const res = await admin('GET', `/data/tables/${IMP}/export?format=csv&sort=${encodeURIComponent('{"column":"id","dir":"asc"}')}`, undefined, { raw: true });
            const text = await res.text();
            assert(res.headers.get('content-disposition').includes(`${IMP}.csv`), 'filename');
            assert(text.startsWith('id,name,score,note,extra\r\n') && text.includes('"has, comma"') && text.includes('"say ""hi"""'), text);
        });
        await test('export and import refuse system tables and bad input', async () => {
            assert((await admin('GET', '/data/tables/_rbac_users/export?format=csv')).status === 404, 'system table export');
            assert((await admin('GET', `/data/tables/${IMP}/export?format=xlsx`)).status === 400, 'bad format');
            assert((await admin('POST', '/data/import?filename=x.csv&table=_evil&mode=create', undefined, { rawBody: csv })).status === 400, 'system table import');
            assert((await admin('POST', '/data/import?filename=x.csv&table=_rbac_users&mode=append', undefined, { rawBody: csv })).status === 404, 'append to system table');
            assert((await admin('POST', '/data/import?filename=x.bin&table=t1&mode=create', undefined, { rawBody: csv })).status === 400, 'unknown extension');
            assert((await admin('POST', `/data/import?filename=x.parquet&table=${IMP}_bad&mode=create`, undefined, { rawBody: 'not parquet' })).status === 400, 'unreadable file');
        });
        await test('viewers can export but not import', async () => {
            const name = `ci_viewer2_${TS}`, pass = `viewer2-pass-${TS}`;
            await admin('POST', '/logins', { username: name, password: pass, role: 'viewer' });
            try {
                const v = client();
                await v('POST', '/login', { username: name, password: pass });
                assert((await v('GET', `/data/tables/${IMP}/export?format=csv`, undefined, { raw: true })).status === 200, 'viewer export');
                assert((await v('POST', `/data/import?filename=x.csv&table=${IMP}&mode=append&dryRun=1`, undefined, { rawBody: csv })).status === 403, 'viewer import');
            } finally {
                await admin('DELETE', `/logins/${name}`);
            }
        });
        await admin('DELETE', `/data/tables/${IMP}`, { confirm: IMP });

        console.log('\n▸ Overview & audit');
        await test('overview reports gateway and DuckDB details', async () => {
            const r = await admin('GET', '/overview');
            assert(r.status === 200 && r.data.duckdb.version && r.data.gateway.version, JSON.stringify(r.data));
        });
        await test('audit log records console actions', async () => {
            const r = await admin('GET', '/audit?limit=100');
            const actions = new Set(r.data.entries.map(e => e.action));
            for (const a of ['console.login', 'setting.update', 'api_key.revoke', 'login.create', 'prune.rule_create', 'prune.job',
                'row.update', 'row.insert', 'row.delete', 'schema.add_column', 'schema.alter_column', 'table.rename', 'table.drop', 'table.create',
                'table.export', 'table.import']) {
                assert(actions.has(a), `missing audit action ${a}`);
            }
        });
        await test('logout ends the session', async () => {
            assert((await admin('POST', '/logout')).status === 200, 'logout');
            assert((await admin('GET', '/me')).status === 401, 'expected 401 after logout');
        });
    } finally {
        if (createdViaCli) {
            try { cli('delete', adminUser); } catch (err) { console.warn(`  ! Could not delete temporary admin ${adminUser}: ${err.message}`); }
        }
    }
}

run()
    .then(() => {
        console.log(`\n--- Console tests: ${passed} passed, ${failed} failed ---`);
        if (failed > 0) process.exit(1);
        console.log('--- ALL CONSOLE TESTS PASSED ---');
    })
    .catch(err => { console.error('Unhandled error:', err); process.exit(1); });
