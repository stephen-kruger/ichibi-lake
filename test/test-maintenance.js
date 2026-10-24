/**
 * Integration tests for scheduled lake maintenance and data inlining against a
 * running stack (local disk or object storage; the suite does not care which).
 *
 *  - small writes are inlined (no Parquet file per request) and read back,
 *    including values in columns added later by ALTER TABLE ADD COLUMN;
 *  - a maintenance run flushes inlined rows and merges small files, reducing
 *    the table's data-file count, and is recorded in the run and audit logs;
 *  - overlapping runs are refused; invalid settings are rejected.
 *
 * Needs a console admin: a temporary one is created with the recovery CLI in
 * the gateway container (CONSOLE_CONTAINER, default ichibi-lake), or provide
 * CONSOLE_ADMIN_USER / CONSOLE_ADMIN_PASSWORD.
 */
import { execFileSync } from 'child_process';
import { API_KEY, BASE_URL } from './_env.js';

const CONTAINER = process.env.CONSOLE_CONTAINER || 'ichibi-lake';
const TS = Date.now();
const TABLE = `maint_files_${TS}`;
const INLINE = `maint_inline_${TS}`;

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function client() {
    let cookie = '';
    return async function call(method, path, body) {
        const res = await fetch(`${BASE_URL}/console/api${path}`, {
            method,
            headers: { 'Content-Type': 'application/json', 'X-Console-Request': '1', ...(cookie ? { Cookie: cookie } : {}) },
            body: body === undefined ? undefined : JSON.stringify(body),
        });
        const set = res.headers.get('set-cookie');
        if (set) cookie = set.split(';')[0];
        return { status: res.status, data: await res.json().catch(() => ({})) };
    };
}

async function gateway(method, path, body) {
    const res = await fetch(`${BASE_URL}${path}`, {
        method,
        headers: { 'Content-Type': 'application/json', 'x-api-key': API_KEY },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    return { status: res.status, data: await res.json().catch(() => ({})) };
}

const query = async (sql) => {
    const r = await gateway('POST', '/query', { sql });
    if (r.status !== 200) throw new Error(`query failed (${r.status}): ${JSON.stringify(r.data)}`);
    return r.data.rows;
};
const fileCount = async (table) => Number((await query(`SELECT count(*) AS n FROM ducklake_list_files('ducklake', '${table}')`))[0].n);
const cli = (...args) => execFileSync('docker', ['exec', CONTAINER, 'node', 'scripts/console-admin.js', ...args], { encoding: 'utf8' });

async function waitForRun(admin, runId, timeoutMs = 180000) {
    const until = Date.now() + timeoutMs;
    while (Date.now() < until) {
        const run = (await admin('GET', '/maintenance')).data.runs?.find(r => r.runId === runId);
        if (run && run.status !== 'running') return run;
        await sleep(1500);
    }
    throw new Error(`maintenance run ${runId} did not finish in ${timeoutMs / 1000}s`);
}

async function startRun(admin) {
    // A scheduled or startup run may hold the lease; wait for it.
    for (let i = 0; i < 120; i++) {
        const r = await admin('POST', '/maintenance/run');
        if (r.status === 202) return r.data.runId;
        if (r.status !== 409) throw new Error(`run: ${r.status} ${JSON.stringify(r.data)}`);
        await sleep(2000);
    }
    throw new Error('maintenance stayed busy');
}

async function run() {
    console.log('--- Starting Lake Maintenance Integration Tests ---\n');

    let adminUser = process.env.CONSOLE_ADMIN_USER;
    let adminPass = process.env.CONSOLE_ADMIN_PASSWORD;
    let createdViaCli = false;
    if (!adminUser || !adminPass) {
        adminUser = `ci_maint_${TS}`;
        try {
            adminPass = (cli('create', adminUser, '--role', 'admin').match(/Password: (\S+)/) || [])[1];
            createdViaCli = !!adminPass;
        } catch (err) {
            console.warn(`  ! Skipping: could not create a temporary admin via docker exec ${CONTAINER} (${String(err.message).split('\n')[0]}).`);
            return;
        }
    }
    const admin = client();
    try {
        await test('admin login', async () => assert((await admin('POST', '/login', { username: adminUser, password: adminPass })).status === 200, 'login'));

        console.log('\n▸ Data inlining');
        await test('single-row writes are inlined: no Parquet file per request', async () => {
            for (let i = 0; i < 5; i++) {
                const r = await gateway('POST', `/upload/${INLINE}`, [{ id: `r${i}`, v: `value ${i}` }]);
                assert(r.status === 200, `upload ${i}: ${r.status} ${JSON.stringify(r.data)}`);
            }
            const files = await fileCount(INLINE);
            const limit = Number((await admin('GET', '/sections')).data.settings?.find(s => s.key === 'DATA_INLINING_ROW_LIMIT')?.value ?? 1000);
            if (limit > 1) assert(files === 0, `expected inlined rows (0 files), found ${files} file(s)`);
        });
        await test('inlined rows in a column added later (ALTER TABLE ADD COLUMN) read back', async () => {
            const r = await gateway('POST', `/upload/${INLINE}`, [{ id: 'late', v: 'x', added_later: 'evolved value' }]);
            assert(r.status === 200, `upload: ${r.status} ${JSON.stringify(r.data)}`);
            const rows = await query(`SELECT added_later FROM ${INLINE} WHERE id = 'late'`);
            assert(rows[0]?.added_later === 'evolved value', `read back ${JSON.stringify(rows)}`);
        });

        console.log('\n▸ Settings');
        let original;
        await test('GET /maintenance returns the configuration, steps and file counts', async () => {
            const r = await admin('GET', '/maintenance');
            assert(r.status === 200, `status ${r.status}`);
            original = r.data.config;
            assert(typeof original.schedule === 'string' && original.steps && 'flush' in original.steps, JSON.stringify(original));
            assert(r.data.steps.merge && r.data.files && Number.isFinite(r.data.files.files), JSON.stringify(r.data));
        });
        await test('invalid schedule and threshold are rejected', async () => {
            assert((await admin('PUT', '/maintenance', { ...original, schedule: '61 * * * *' })).status === 400, 'cron');
            assert((await admin('PUT', '/maintenance', { ...original, deleteThreshold: 2 })).status === 400, 'threshold');
        });
        await test('settings are saved and audited', async () => {
            assert((await admin('PUT', '/maintenance', { ...original, schedule: '17 3 * * *', deleteThreshold: 0.5 })).status === 200, 'save');
            const cfg = (await admin('GET', '/maintenance')).data.config;
            assert(cfg.schedule === '17 3 * * *' && cfg.deleteThreshold === 0.5 && cfg.nextRunMs > Date.now(), JSON.stringify(cfg));
            const audit = (await admin('GET', '/audit?limit=50')).data.entries;
            assert(audit.some(e => e.action === 'maintenance.update' && e.detail.includes('17 3 * * *')), 'audit entry missing');
        });

        console.log('\n▸ Small files');
        let before = 0;
        await test('batch writes above the inlining limit create one file each', async () => {
            const rows = (k) => Array.from({ length: 1200 }, (_, i) => ({ id: `${k}_${i}`, n: i, label: `batch ${k}` }));
            for (let k = 0; k < 4; k++) {
                const r = await gateway('POST', `/upload/${TABLE}`, rows(k));
                assert(r.status === 200, `upload ${k}: ${r.status} ${JSON.stringify(r.data)}`);
            }
            before = await fileCount(TABLE);
            assert(before >= 4, `expected at least 4 files, found ${before}`);
        });
        await test('a maintenance run merges them and inlined rows are flushed', async () => {
            const runId = await startRun(admin);
            const done = await waitForRun(admin, runId);
            assert(['succeeded', 'partial'].includes(done.status), `run ${done.status}: ${done.message}`);
            const after = await fileCount(TABLE);
            assert(after < before, `file count did not drop: ${before} -> ${after} (${done.message})`);
            assert(Number((await query(`SELECT count(*) AS n FROM ${TABLE}`))[0].n) === 4800, 'row count changed');
            assert(await fileCount(INLINE) >= 1, 'inlined rows were not flushed to Parquet');
            const rows = await query(`SELECT added_later FROM ${INLINE} WHERE id = 'late'`);
            assert(rows[0]?.added_later === 'evolved value', `after flush ${JSON.stringify(rows)}`);
            assert(/data files \d+ -> \d+/.test(done.message), done.message);
        });
        await test('the run is in the audit log and the config shows the last run', async () => {
            const audit = (await admin('GET', '/audit?limit=50')).data.entries;
            assert(audit.some(e => e.action === 'maintenance.run' && /\(manual\)$/.test(e.detail)), 'audit entry missing');
            const cfg = (await admin('GET', '/maintenance')).data.config;
            assert(cfg.lastRunMs && ['succeeded', 'partial'].includes(cfg.lastStatus) && !cfg.leaseUntilMs, JSON.stringify(cfg));
        });
        await test('a second run while one is in progress is refused (409)', async () => {
            const runId = await startRun(admin);
            const second = await admin('POST', '/maintenance/run');
            const first = await waitForRun(admin, runId);
            // A very small lake can finish before the second request arrives.
            assert(second.status === 409 || (second.status === 202 && first.status !== 'running'), `second run: ${second.status}`);
            if (second.status === 202) await waitForRun(admin, second.data.runId);
        });
        await test('maintenance runs are kept out of the pruning job list', async () => {
            const jobs = (await admin('GET', '/retention/runs?limit=100')).data.jobs;
            assert(!jobs.some(j => j.ruleId === '__maintenance'), 'maintenance run listed as a pruning job');
        });

        if (original) await admin('PUT', '/maintenance', original);
    } finally {
        for (const t of [TABLE, INLINE]) await admin('DELETE', `/data/tables/${t}`, { confirm: t }).catch(() => {});
        if (createdViaCli) {
            try { cli('delete', adminUser); } catch (err) { console.warn(`  ! Could not delete temporary admin ${adminUser}: ${err.message}`); }
        }
    }
}

run()
    .then(() => {
        console.log(`\n--- Maintenance tests: ${passed} passed, ${failed} failed ---`);
        if (failed > 0) process.exit(1);
        console.log('--- ALL MAINTENANCE TESTS PASSED ---');
    })
    .catch(err => { console.error('Unhandled error:', err); process.exit(1); });
