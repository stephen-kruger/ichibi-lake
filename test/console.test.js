/**
 * Unit tests for the admin console's pure logic: password hashing, session
 * tokens, cookies, login throttling, settings validation and the retention
 * SQL/path helpers. No database or running gateway is needed.
 */
import assert from 'node:assert/strict';
import path from 'node:path';
import {
    hashPassword, verifyPassword, createSessionToken, verifySessionToken, passwordFingerprint,
    parseCookies, sessionCookie, LoginThrottle, validateUsername, validatePassword, safeEqual,
    generateSetupCode, generateApiKey,
} from '../src/console/auth.js';
import { SECTIONS, SETTINGS, getSettingDef, normaliseSettingValue, maskSecret } from '../src/console/registry.js';
import { containedBlobPath, isSystemTable, matchTables, normaliseRule, buildPredicates, PROTECTED_TABLES } from '../src/console/retention.js';
import { parseCron, nextRun, nextRuns, validateCron } from '../src/console/cron.js';
import { intervalToCron } from '../src/console/store.js';
import { normaliseType, quoteIdent, quoteLiteral, buildWhere, wideningTargets, isEditableType, cleanDuckDbError } from '../src/console/data.js';

let passed = 0, failed = 0;

async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`  ✓ ${name}`);
    } catch (err) {
        failed++;
        console.error(`  ✗ ${name}`);
        console.error(`    ${err.message}`);
    }
}

async function run() {
    console.log('\n▸ console: password hashing');
    const hash = await hashPassword('correct horse battery');
    await test('hash uses the scrypt$N$r$p$salt$key format', () => {
        assert.match(hash, /^scrypt\$\d+\$\d+\$\d+\$[A-Za-z0-9+/=]+\$[A-Za-z0-9+/=]+$/);
    });
    await test('correct password verifies', async () => assert.equal(await verifyPassword('correct horse battery', hash), true));
    await test('wrong password is rejected', async () => assert.equal(await verifyPassword('correct horse batterz', hash), false));
    await test('same password hashes differently (random salt)', async () => assert.notEqual(await hashPassword('correct horse battery'), hash));
    await test('malformed stored hash is rejected, not thrown', async () => {
        assert.equal(await verifyPassword('x', 'plaintext'), false);
        assert.equal(await verifyPassword('x', null), false);
    });

    console.log('\n▸ console: session tokens');
    const secret = 'unit-test-secret';
    const now = 1_700_000_000_000;
    const token = createSessionToken({ username: 'admin', passwordHash: hash, ttlMs: 60_000, secret, now });
    await test('valid token round-trips username and fingerprint', () => {
        const claims = verifySessionToken(token, secret, now + 1000);
        assert.equal(claims.username, 'admin');
        assert.equal(claims.fingerprint, passwordFingerprint(hash));
    });
    await test('expired token is rejected', () => assert.equal(verifySessionToken(token, secret, now + 60_001), null));
    await test('token signed with another secret is rejected', () => assert.equal(verifySessionToken(token, 'other', now), null));
    await test('tampered payload is rejected', () => {
        const [body, sig] = token.split('.');
        const forged = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(body, 'base64url')), u: 'root' })).toString('base64url');
        assert.equal(verifySessionToken(`${forged}.${sig}`, secret, now), null);
    });
    await test('garbage tokens are rejected', () => {
        for (const t of [undefined, '', 'abc', '.', 'a.b', `${'x'.repeat(10)}.`]) assert.equal(verifySessionToken(t, secret, now), null);
    });
    await test('password change changes the fingerprint (invalidates sessions)', async () => {
        assert.notEqual(passwordFingerprint(hash), passwordFingerprint(await hashPassword('another password!')));
    });

    console.log('\n▸ console: cookies and input validation');
    await test('session cookie is HttpOnly, SameSite=Strict and scoped to /console', () => {
        const c = sessionCookie('tok', { maxAgeMs: 3_600_000, secure: false });
        assert.match(c, /HttpOnly/); assert.match(c, /SameSite=Strict/); assert.match(c, /Path=\/console/); assert.match(c, /Max-Age=3600/);
        assert.doesNotMatch(c, /Secure/);
        assert.match(sessionCookie('tok', { maxAgeMs: 1, secure: true }), /Secure/);
    });
    await test('parseCookies handles multiple, encoded and malformed cookies', () => {
        assert.deepEqual(parseCookies('a=1; ichibi_console=x%2Fy; junk; b=%E0%A4%A'), { a: '1', ichibi_console: 'x/y' });
        assert.deepEqual(parseCookies(undefined), {});
    });
    await test('usernames are validated', () => {
        assert.equal(validateUsername('admin'), null);
        assert.equal(validateUsername('ops.team@corp'), null);
        for (const bad of ['ab', 'has space', 'x'.repeat(65), '<script>', null]) assert.ok(validateUsername(bad), String(bad));
    });
    await test('passwords need at least 10 characters', () => {
        assert.ok(validatePassword('short'));
        assert.equal(validatePassword('long enough!'), null);
    });
    await test('safeEqual compares exactly', () => {
        assert.equal(safeEqual('ABCD-EFGH', 'ABCD-EFGH'), true);
        assert.equal(safeEqual('ABCD-EFGH', 'ABCD-EFGX'), false);
        assert.equal(safeEqual('ABCD', undefined), false);
    });
    await test('setup codes and API keys have the expected shape', () => {
        assert.match(generateSetupCode(), /^[A-Z2-9]{4}-[A-Z2-9]{4}-[A-Z2-9]{4}$/);
        assert.match(generateApiKey(), /^ichibi_[A-Za-z0-9_-]{32}$/);
        assert.notEqual(generateApiKey(), generateApiKey());
    });

    console.log('\n▸ console: login throttle');
    await test('locks out after 5 failures and reports a wait', () => {
        let t = 0;
        const th = new LoginThrottle({ maxFailures: 5, windowMs: 1000, now: () => t });
        for (let i = 0; i < 4; i++) th.fail(['ip:1']);
        assert.equal(th.retryAfterSeconds(['ip:1']), 0);
        th.fail(['ip:1']);
        assert.ok(th.retryAfterSeconds(['ip:1']) > 0);
        t = 1001;
        assert.equal(th.retryAfterSeconds(['ip:1']), 0, 'window expires');
    });
    await test('throttles by any of the keys (IP or username)', () => {
        const th = new LoginThrottle({ maxFailures: 2 });
        th.fail(['ip:a', 'user:bob']); th.fail(['ip:b', 'user:bob']);
        assert.ok(th.retryAfterSeconds(['ip:c', 'user:bob']) > 0);
        assert.equal(th.retryAfterSeconds(['ip:c', 'user:alice']), 0);
    });
    await test('success clears failures', () => {
        const th = new LoginThrottle({ maxFailures: 2 });
        th.fail(['ip:1']); th.succeed(['ip:1']); th.fail(['ip:1']);
        assert.equal(th.retryAfterSeconds(['ip:1']), 0);
    });

    console.log('\n▸ console: settings registry');
    await test('every setting belongs to a declared section', () => {
        const ids = new Set(SECTIONS.map(s => s.id));
        for (const s of SETTINGS) assert.ok(ids.has(s.section), `${s.key} -> ${s.section}`);
    });
    await test('every editable setting is live (overrides load after startup constants)', () => {
        for (const s of SETTINGS.filter(x => x.editable)) assert.equal(s.live, true, s.key);
    });
    await test('booleans, numbers and enums are normalised and bounded', () => {
        const bool = { key: 'EXAMPLE_FLAG', type: 'boolean', editable: true, live: true };
        assert.deepEqual(normaliseSettingValue(bool, true), { value: 'true' });
        assert.deepEqual(normaliseSettingValue(bool, 'FALSE'), { value: 'false' });
        assert.ok(normaliseSettingValue(bool, 'yes').error);
        assert.deepEqual(normaliseSettingValue(getSettingDef('MAX_BATCH_SIZE'), '500'), { value: '500' });
        assert.ok(normaliseSettingValue(getSettingDef('MAX_BATCH_SIZE'), 0).error);
        assert.ok(normaliseSettingValue(getSettingDef('MAX_BATCH_SIZE'), 1.5).error);
    });
    await test('read-only settings cannot be changed', () => {
        assert.match(normaliseSettingValue(getSettingDef('DUCKLAKE_PG_PASSWORD'), 'x').error, /read-only/);
    });
    await test('custom validators run (partition key identifiers)', () => {
        assert.deepEqual(normaliseSettingValue(getSettingDef('DEFAULT_PARTITION_KEY'), 'created, region'), { value: 'created, region' });
        assert.deepEqual(normaliseSettingValue(getSettingDef('DEFAULT_PARTITION_KEY'), 'none'), { value: 'none' });
        assert.ok(normaliseSettingValue(getSettingDef('DEFAULT_PARTITION_KEY'), 'created; DROP TABLE x').error);
    });
    await test('maskSecret hides all but a short prefix/suffix', () => {
        assert.equal(maskSecret('ichibi_abcdefghijklmnop'), 'ichibi…••••op');
        assert.equal(maskSecret('short'), '•••••');
        assert.equal(maskSecret(''), '');
    });

    console.log('\n▸ console: pruning — table selectors and protected tables');
    const tables = ['events', 'events_2026', 'sensor_a', 'sensor_b', 'sensor_archive', 'logs', '_rbac_users', '_table_acls',
        '_console_users', '_console_settings', '_graph_registry', '_gateway_meta'];
    await test('wildcards match by name, case-insensitively, with exclusions', () => {
        assert.deepEqual(matchTables(tables, 'sensor_*'), ['sensor_a', 'sensor_archive', 'sensor_b']);
        assert.deepEqual(matchTables(tables, 'sensor_*', '*_archive'), ['sensor_a', 'sensor_b']);
        assert.deepEqual(matchTables(tables, 'EVENTS, logs'), ['events', 'logs']);
        assert.deepEqual(matchTables(tables, 'events_????'), ['events_2026']);
    });
    await test('system and password tables never match, even with "*"', () => {
        const all = matchTables(tables, '*');
        assert.deepEqual(all, ['events', 'events_2026', 'logs', 'sensor_a', 'sensor_archive', 'sensor_b']);
        assert.deepEqual(matchTables(tables, '*users*, *acl*, *console*, *registry*, *meta*'), []);
        for (const t of ['_rbac_users', '_console_users', '_console_settings', '_table_acls', '_graph_registry', '_gateway_meta']) {
            assert.ok(isSystemTable(t), t);
            assert.ok(PROTECTED_TABLES.has(t), `${t} in PROTECTED_TABLES`);
        }
    });
    await test('rules cannot target system tables or use unsafe patterns', () => {
        const base = { name: 'r', maxAgeValue: 1, dateColumn: 'created', schedule: '@daily' };
        assert.throws(() => normaliseRule({ ...base, tablePattern: '_rbac_users' }), /reserved for system tables/);
        assert.throws(() => normaliseRule({ ...base, tablePattern: '_*' }), /reserved for system tables/);
        assert.throws(() => normaliseRule({ ...base, tablePattern: 'events; DROP TABLE x' }), /Invalid table pattern/);
        assert.throws(() => normaliseRule({ ...base, tablePattern: 'events', excludePattern: 'a"b' }), /Invalid table pattern/);
    });

    console.log('\n▸ console: pruning — rule validation and SQL');
    await test('a rule needs a criterion, and age needs a date column', () => {
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', schedule: '@daily' }), /at least one criterion/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxAgeValue: 5, schedule: '@daily' }), /date\/timestamp column/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxRows: 0, schedule: '@daily' }), /Rows to keep/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxRows: 10, schedule: 'every day' }), /Invalid schedule/);
        const ok = normaliseRule({ name: ' r ', tablePattern: 'a, b ,c', maxAgeValue: 12, maxAgeUnit: 'hours', dateColumn: 'ts', maxRows: 100, schedule: '0 2 * * *' });
        assert.equal(ok.tablePattern, 'a, b, c');
        assert.equal(ok.name, 'r');
        assert.equal(ok.maxAgeUnit, 'hours');
    });
    const types = new Map([['created', 'TIMESTAMP'], ['day', 'DATE'], ['name', 'VARCHAR'], ['seq', 'BIGINT']]);
    await test('age predicate compares in whole units, per column type', () => {
        const ts = buildPredicates({ maxAgeValue: 30, maxAgeUnit: 'days', dateColumn: 'created' }, 'events', types);
        assert.equal(ts.age, `"created" < CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL 30 DAY`);
        const hours = buildPredicates({ maxAgeValue: 6, maxAgeUnit: 'hours', dateColumn: 'created' }, 'events', types);
        assert.match(hours.age, /INTERVAL 6 HOUR$/);
        const date = buildPredicates({ maxAgeValue: 7, maxAgeUnit: 'days', dateColumn: 'day' }, 'events', types);
        assert.equal(date.age, `"day" < CAST(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL 7 DAY AS DATE)`);
    });
    await test('count predicate keeps the newest N by column or insertion order', () => {
        const byCol = buildPredicates({ maxRows: 1000, orderColumn: 'seq' }, 'events', types);
        assert.equal(byCol.count, `rowid IN (SELECT rowid FROM "events" ORDER BY "seq" DESC NULLS LAST, rowid DESC OFFSET 1000)`);
        const byInsert = buildPredicates({ maxRows: 5 }, 'events', types);
        assert.equal(byInsert.count, `rowid IN (SELECT rowid FROM "events" ORDER BY rowid DESC OFFSET 5)`);
    });
    await test('tables without the needed columns are skipped with a reason', () => {
        assert.match(buildPredicates({ maxAgeValue: 1, dateColumn: 'missing' }, 't', types).skip, /no "missing" column/);
        assert.match(buildPredicates({ maxAgeValue: 1, dateColumn: 'name' }, 't', types).skip, /not a date/);
        assert.match(buildPredicates({ maxRows: 1, orderColumn: 'nope' }, 't', types).skip, /no "nope" column/);
    });

    console.log('\n▸ console: pruning — schedules (cron, UTC)');
    const at = (iso) => new Date(iso);
    await test('next run for common schedules', () => {
        assert.equal(nextRun('*/15 * * * *', at('2026-10-24T10:07:30Z')).toISOString(), '2026-10-24T10:15:00.000Z');
        assert.equal(nextRun('0 2 * * *', at('2026-10-24T10:00:00Z')).toISOString(), '2026-10-25T02:00:00.000Z');
        assert.equal(nextRun('0 3 * * 0', at('2026-10-24T10:00:00Z')).toISOString(), '2026-10-25T03:00:00.000Z'); // Sunday
        assert.equal(nextRun('0 4 1 * *', at('2026-10-24T10:00:00Z')).toISOString(), '2026-11-01T04:00:00.000Z');
        assert.equal(nextRun('@hourly', at('2026-12-31T23:30:00Z')).toISOString(), '2027-01-01T00:00:00.000Z');
    });
    await test('next run is strictly after the reference time', () => {
        assert.equal(nextRun('0 * * * *', at('2026-10-24T10:00:00Z')).toISOString(), '2026-10-24T11:00:00.000Z');
    });
    await test('ranges, lists, steps and names', () => {
        assert.deepEqual([...parseCron('0-10/5 * * * *').minutes], [0, 5, 10]);
        assert.deepEqual([...parseCron('0 9-17/4 * * MON-FRI').weekdays], [1, 2, 3, 4, 5]);
        assert.deepEqual([...parseCron('0 0 * JAN,JUL *').months], [1, 7]);
        assert.ok(parseCron('0 0 * * 7').weekdays.has(0), '7 means Sunday');
        assert.equal(nextRuns('0 12 * * *', 3, at('2026-10-24T13:00:00Z')).length, 3);
    });
    await test('day-of-month OR day-of-week when both are restricted (standard cron)', () => {
        // 13th of the month OR Friday: from Thu 2026-10-22 the next match is Fri 23rd.
        assert.equal(nextRun('0 0 13 * 5', at('2026-10-22T12:00:00Z')).toISOString(), '2026-10-23T00:00:00.000Z');
    });
    await test('invalid schedules are rejected with a reason', () => {
        assert.match(validateCron('* * * *'), /5 fields/);
        assert.match(validateCron('60 * * * *'), /minute must be 0-59/);
        assert.match(validateCron('*/0 * * * *'), /Invalid step/);
        assert.match(validateCron('0 0 * * FUNDAY'), /Invalid day of week/);
        assert.equal(validateCron('0 0 31 2 *'), null);
        assert.equal(nextRun('0 0 31 2 *', at('2026-01-01T00:00:00Z')), null, 'Feb 31 never occurs');
    });
    await test('the old global interval converts to an equivalent cron', () => {
        assert.equal(intervalToCron(60), '0 * * * *');
        assert.equal(intervalToCron(15), '*/15 * * * *');
        assert.equal(intervalToCron(360), '0 */6 * * *');
        assert.equal(intervalToCron(1440), '0 0 * * *');
        assert.equal(intervalToCron(7), '0 * * * *');
    });
    await test('blob paths are only accepted inside the blobs root', () => {
        const root = path.resolve('/app/blobs');
        assert.equal(containedBlobPath('/app/blobs/abc', root), path.join(root, 'abc'));
        assert.equal(containedBlobPath('/app/blobs/../etc/passwd', root), null);
        assert.equal(containedBlobPath('/app/blobs', root), null);
        assert.equal(containedBlobPath('/app/blobsother/x', root), null);
        assert.equal(containedBlobPath('hello world', root), null);
        assert.equal(containedBlobPath(null, root), null);
    });

    console.log('\n\u25b8 console: data browser helpers');
    await test('types are allow-listed and normalised', () => {
        assert.equal(normaliseType('integer'), 'INTEGER');
        assert.equal(normaliseType(' text '), 'VARCHAR');
        assert.equal(normaliseType('timestamp with time zone'), 'TIMESTAMPTZ');
        assert.equal(normaliseType('decimal(10, 2)'), 'DECIMAL(10,2)');
        assert.equal(normaliseType('DECIMAL(40,2)'), null);
        assert.equal(normaliseType('DECIMAL(5,6)'), null);
        for (const bad of ['VARCHAR; DROP TABLE x', 'INTEGER DEFAULT 1', 'STRUCT(a INT)', '', null]) assert.equal(normaliseType(bad), null, String(bad));
    });
    await test('identifiers and literals are quoted with escaping', () => {
        assert.equal(quoteIdent('first name'), '"first name"');
        assert.equal(quoteIdent('a"b'), '"a""b"');
        assert.equal(quoteLiteral("O'Brien"), "'O''Brien'");
    });
    await test('only widening type changes are offered', () => {
        assert.deepEqual(wideningTargets('INTEGER'), ['BIGINT']);
        assert.deepEqual(wideningTargets('float'), ['DOUBLE']);
        assert.deepEqual(wideningTargets('VARCHAR'), []);
    });
    await test('scalar types are inline-editable; complex and binary types are not', () => {
        for (const t of ['VARCHAR', 'BIGINT', 'DECIMAL(18,3)', 'TIMESTAMP WITH TIME ZONE', 'BOOLEAN', 'UUID']) assert.ok(isEditableType(t), t);
        for (const t of ['BLOB', 'STRUCT(a INTEGER)', 'INTEGER[]', 'MAP(VARCHAR, INTEGER)']) assert.ok(!isEditableType(t), t);
    });
    const cols = new Map([['id', 'VARCHAR'], ['n', 'BIGINT'], ['first name', 'VARCHAR'], ['payload', 'BLOB']]);
    await test('filters become parameterised SQL over allow-listed columns', () => {
        const w = buildWhere([
            { column: 'id', op: 'contains', value: '50%_off' },
            { column: 'n', op: 'gte', value: '10' },
            { column: 'first name', op: 'is_null' },
        ], '', cols);
        assert.equal(w.sql, `WHERE CAST("id" AS VARCHAR) ILIKE ? AND "n" >= TRY_CAST(? AS BIGINT) AND "first name" IS NULL`);
        assert.deepEqual(w.params, ['%50\\%\\_off%', '10']);
    });
    await test('unknown filter columns and operators are rejected', () => {
        assert.throws(() => buildWhere([{ column: 'nope', op: 'equals', value: 'x' }], '', cols), /Unknown filter column/);
        assert.throws(() => buildWhere([{ column: 'id', op: 'drop', value: 'x' }], '', cols), /Unknown filter operator/);
        assert.throws(() => buildWhere([{ column: 'id"; DROP TABLE x; --', op: 'equals', value: 'x' }], '', cols), /Unknown filter column/);
    });
    await test('global search covers every non-binary column', () => {
        const w = buildWhere([], 'abc', cols);
        assert.equal(w.sql, `WHERE (CAST("id" AS VARCHAR) ILIKE ? OR CAST("n" AS VARCHAR) ILIKE ? OR CAST("first name" AS VARCHAR) ILIKE ?)`);
        assert.equal(w.params.length, 3);
        assert.equal(buildWhere([], '   ', cols).sql, '');
    });
    await test('DuckDB error messages are shortened for display', () => {
        assert.equal(cleanDuckDbError(new Error('Catalog Error: Cannot change type\nLINE 1: ...')), 'Cannot change type');
    });
}

run()
    .then(() => {
        console.log('\n─── console unit tests ───');
        console.log(`  passed: ${passed}`);
        console.log(`  failed: ${failed}`);
        if (failed > 0) process.exit(1);
    })
    .catch(err => { console.error('Unhandled error:', err); process.exit(1); });
