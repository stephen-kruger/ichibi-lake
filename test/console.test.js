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
import { containedBlobPath, isSystemTable, matchTables, normaliseRule, buildPredicates, PROTECTED_TABLES, isLegacyMultiTable, detectEpochUnit, isEpochType, isDateType, normaliseConditions, buildConditionPredicate } from '../src/console/retention.js';
import { parseCron, nextRun, nextRuns, validateCron } from '../src/console/cron.js';
import { intervalToCron } from '../src/console/store.js';
import { normaliseType, quoteIdent, quoteLiteral, buildWhere, wideningTargets, isEditableType, cleanDuckDbError, detectImportFormat } from '../src/console/data.js';
import { compareVersions, checkForUpdate, resetUpdateCheckCache } from '../src/console/version.js';
import { csvField } from '../src/export.js';

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
    await test('a rule prunes exactly one table: no wildcards, lists or system tables', () => {
        const base = { name: 'r', maxAgeValue: 1, dateColumn: 'created', schedule: '@daily' };
        assert.throws(() => normaliseRule({ ...base, table: '_rbac_users' }), /system table/);
        assert.throws(() => normaliseRule({ ...base, table: '_console_users' }), /system table/);
        assert.throws(() => normaliseRule({ ...base, table: 'sensor_*' }), /exactly one table/);
        assert.throws(() => normaliseRule({ ...base, table: 'a, b' }), /exactly one table/);
        assert.throws(() => normaliseRule({ ...base, table: 'events; DROP TABLE x' }), /exactly one table/);
        assert.throws(() => normaliseRule({ ...base }), /Choose the table/);
        const ok = normaliseRule({ ...base, table: 'events', excludePattern: '*_archive' });
        assert.equal(ok.tablePattern, 'events');
        assert.equal(ok.excludePattern, '', 'exclusions do not apply to single-table rules');
        assert.equal(normaliseRule({ ...base, tablePattern: 'events' }).tablePattern, 'events', 'tablePattern is accepted as an alias');
    });
    await test('rules saved with patterns are recognised as legacy multi-table rules', () => {
        assert.equal(isLegacyMultiTable({ tablePattern: 'events', excludePattern: '' }), false);
        assert.equal(isLegacyMultiTable({ tablePattern: 'sensor_*', excludePattern: '' }), true);
        assert.equal(isLegacyMultiTable({ tablePattern: 'a, b', excludePattern: '' }), true);
        assert.equal(isLegacyMultiTable({ tablePattern: 'events', excludePattern: 'x' }), true);
    });

    console.log('\n▸ console: pruning — rule validation and SQL');
    await test('a rule needs a criterion, and age needs a date column', () => {
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', schedule: '@daily' }), /at least one criterion/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxAgeValue: 5, schedule: '@daily' }), /date\/timestamp column/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxRows: 0, schedule: '@daily' }), /Rows to keep/);
        assert.throws(() => normaliseRule({ name: 'r', tablePattern: 'events', maxRows: 10, schedule: 'every day' }), /Invalid schedule/);
        const ok = normaliseRule({ name: ' r ', table: ' events ', maxAgeValue: 12, maxAgeUnit: 'hours', dateColumn: 'ts', maxRows: 100, schedule: '0 2 * * *' });
        assert.equal(ok.tablePattern, 'events');
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
    await test('epoch units are detected from the magnitude of the values', () => {
        const now = Date.UTC(2026, 9, 24);
        assert.equal(detectEpochUnit(now / 1000), 's');
        assert.equal(detectEpochUnit(now), 'ms');
        assert.equal(detectEpochUnit(now * 1000), 'us');
        assert.equal(detectEpochUnit(BigInt(now) * 1000000n), 'ns');
        assert.equal(detectEpochUnit(Date.UTC(1975, 0, 1) / 1000), 's');
        assert.equal(detectEpochUnit(Date.UTC(1975, 0, 1)), 'ms', '1975 in ms is still above the seconds band');
        assert.equal(detectEpochUnit('not a number'), null);
    });
    await test('numeric epoch columns compare against the cutoff in their unit', () => {
        const epochTypes = new Map([['ts', 'BIGINT'], ['secs', 'INTEGER'], ['f', 'DOUBLE'], ['d', 'DECIMAL(18,3)']]);
        const rule = { maxAgeValue: 30, maxAgeUnit: 'days', dateColumn: 'ts' };
        assert.equal(buildPredicates(rule, 't', epochTypes, 'ms').age, `"ts" < epoch_ms(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL 30 DAY)`);
        assert.equal(buildPredicates({ ...rule, dateColumn: 'secs' }, 't', epochTypes, 's').age, `"secs" < epoch(CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL 30 DAY)`);
        assert.match(buildPredicates(rule, 't', epochTypes, 'us').age, /epoch_us\(/);
        assert.match(buildPredicates(rule, 't', epochTypes, 'ns').age, /epoch_ns\(/);
        assert.match(buildPredicates({ ...rule, dateColumn: 'f' }, 't', epochTypes, 's').age, /^"f" < epoch\(/);
        assert.match(buildPredicates(rule, 't', epochTypes, null).skip, /epoch unit is unknown/);
        for (const t of ['BIGINT', 'INTEGER', 'UBIGINT', 'DOUBLE', 'DECIMAL(18,3)']) assert.ok(isEpochType(t), t);
        for (const t of ['VARCHAR', 'BOOLEAN', 'TIMESTAMP']) assert.ok(!isEpochType(t), t);
        assert.ok(isDateType('TIMESTAMP WITH TIME ZONE') && isDateType('DATE') && !isDateType('BIGINT'));
    });
    await test('the epoch unit is validated and defaults to auto', () => {
        const base = { name: 'r', tablePattern: 'events', maxAgeValue: 1, dateColumn: 'ts', schedule: '@daily' };
        assert.equal(normaliseRule(base).epochUnit, 'auto');
        assert.equal(normaliseRule({ ...base, epochUnit: 'ms' }).epochUnit, 'ms');
        assert.throws(() => normaliseRule({ ...base, epochUnit: 'minutes' }), /Epoch unit/);
    });
    const ctypes = new Map([['level', 'VARCHAR'], ['n', 'BIGINT'], ['ts', 'TIMESTAMP'], ['meta', 'STRUCT(a INTEGER)']]);
    await test('match conditions become typed, escaped SQL — values can never inject', () => {
        const c = normaliseConditions({ match: 'all', items: [{ column: 'level', op: 'equals', value: "x'); DROP TABLE t; --" }] });
        const sql = buildConditionPredicate(c, ctypes).sql;
        assert.equal(sql, `COALESCE((CAST("level" AS VARCHAR) = 'x''); DROP TABLE t; --'), false)`);
        assert.equal(buildConditionPredicate(normaliseConditions({ items: [{ column: 'n', op: 'gte', value: '10' }] }), ctypes).sql,
            `COALESCE(("n" >= TRY_CAST('10' AS BIGINT)), false)`);
        assert.equal(buildConditionPredicate(normaliseConditions({ items: [{ column: 'level', op: 'contains', value: '50%_off' }] }), ctypes).sql,
            `COALESCE((CAST("level" AS VARCHAR) ILIKE '%50\\%\\_off%' ESCAPE '\\'), false)`);
        assert.match(buildConditionPredicate(normaliseConditions({ match: 'any', items: [{ column: 'n', op: 'in', value: '1, 2,3' }, { column: 'level', op: 'is_null' }] }), ctypes).sql,
            /\("n" IN \(TRY_CAST\('1' AS BIGINT\), TRY_CAST\('2' AS BIGINT\), TRY_CAST\('3' AS BIGINT\)\)\) OR \("level" IS NULL\)/);
        assert.match(buildConditionPredicate(normaliseConditions({ items: [{ column: 'meta', op: 'equals', value: 'x' }] }), ctypes).sql,
            /CAST\("meta" AS VARCHAR\) = 'x'/, 'complex types compare as text');
        assert.match(buildConditionPredicate(normaliseConditions({ items: [{ column: 'a"b', op: 'equals', value: 'x' }] }), new Map([['a"b', 'VARCHAR']])).sql,
            /CAST\("a""b" AS VARCHAR\)/, 'column names are quoted with escaping');
    });
    await test('conditions reject unknown operators, missing columns and oversized input', () => {
        assert.throws(() => normaliseConditions({ items: [{ column: 'level', op: 'raw_sql', value: '1=1' }] }), /unknown operator/);
        assert.throws(() => normaliseConditions({ items: [{ column: '', op: 'equals', value: 'x' }] }), /needs a column/);
        assert.throws(() => normaliseConditions({ items: Array.from({ length: 11 }, () => ({ column: 'level', op: 'is_null' })) }), /At most 10/);
        assert.throws(() => normaliseConditions({ items: [{ column: 'level', op: 'equals', value: 'x'.repeat(501) }] }), /invalid value/);
        assert.equal(normaliseConditions({ items: [] }), null);
        assert.match(buildConditionPredicate(normaliseConditions({ items: [{ column: 'nope', op: 'is_null' }] }), ctypes).skip, /no "nope" column/);
        const base = { name: 'r', tablePattern: 'events', schedule: '@daily' };
        assert.equal(normaliseRule({ ...base, conditions: { items: [{ column: 'level', op: 'equals', value: 'debug' }] } }).conditions.match, 'all',
            'conditions alone are a valid criterion');
    });
    await test('match conditions combine with the other criteria using OR', () => {
        const p = buildPredicates({ maxAgeValue: 7, dateColumn: 'ts', conditions: normaliseConditions({ items: [{ column: 'level', op: 'equals', value: 'debug' }] }) }, 't', ctypes);
        assert.ok(p.age && p.match && !p.skip, JSON.stringify(p));
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
        assert.equal(w.sql, `WHERE CAST("id" AS VARCHAR) ILIKE ? ESCAPE '\\' AND "n" >= TRY_CAST(? AS BIGINT) AND "first name" IS NULL`);
        assert.deepEqual(w.params, ['%50\\%\\_off%', '10']);
    });
    await test('unknown filter columns and operators are rejected', () => {
        assert.throws(() => buildWhere([{ column: 'nope', op: 'equals', value: 'x' }], '', cols), /Unknown filter column/);
        assert.throws(() => buildWhere([{ column: 'id', op: 'drop', value: 'x' }], '', cols), /Unknown filter operator/);
        assert.throws(() => buildWhere([{ column: 'id"; DROP TABLE x; --', op: 'equals', value: 'x' }], '', cols), /Unknown filter column/);
    });
    await test('global search covers every non-binary column', () => {
        const w = buildWhere([], 'abc', cols);
        assert.equal(w.sql, `WHERE (CAST("id" AS VARCHAR) ILIKE ? ESCAPE '\\' OR CAST("n" AS VARCHAR) ILIKE ? ESCAPE '\\' OR CAST("first name" AS VARCHAR) ILIKE ? ESCAPE '\\')`);
        assert.equal(w.params.length, 3);
        assert.equal(buildWhere([], '   ', cols).sql, '');
    });
    await test('search and filters match values containing _, % and \\ when run in DuckDB', async () => {
        // DuckDB has no default LIKE escape character: without ESCAPE '\' the
        // escaped "\_" never matched, so e.g. MQTT topics found nothing.
        const { DuckDBInstance } = await import('@duckdb/node-api');
        const conn = await (await DuckDBInstance.create(':memory:')).connect();
        await conn.run(`CREATE TABLE t (id VARCHAR, n BIGINT)`);
        await conn.run(`INSERT INTO t VALUES
            ('homeassistant/binary_sensor/occupancysensor_occupancy/last_changed', 1),
            ('homeassistant/binaryXsensor/other', 2), ('50% off', 3), ('500 off', 4), ('C:\\temp\\x', 5)`);
        const tcols = new Map([['id', 'VARCHAR'], ['n', 'BIGINT']]);
        const ids = async (filters, search) => {
            const w = buildWhere(filters, search, tcols);
            const r = await conn.runAndReadAll(`SELECT n FROM t ${w.sql} ORDER BY n`, w.params, w.types);
            return r.getRows().map(row => Number(row[0]));
        };
        assert.deepEqual(await ids([], 'homeassistant/binary_sensor/occupancysensor_occupancy/last_changed'), [1]);
        assert.deepEqual(await ids([], 'binary_sensor'), [1], '"_" is literal, not a wildcard');
        assert.deepEqual(await ids([{ column: 'id', op: 'contains', value: 'occupancy/last_changed' }]), [1]);
        assert.deepEqual(await ids([{ column: 'id', op: 'starts_with', value: 'homeassistant/binary_' }]), [1]);
        assert.deepEqual(await ids([], '50%'), [3], '"%" is literal');
        assert.deepEqual(await ids([], 'C:\\temp'), [5], 'backslash is literal');
        assert.deepEqual(await ids([], 'HOMEASSISTANT'), [1, 2], 'still case-insensitive');
        conn.closeSync();
    });
    await test('CSV export neutralises spreadsheet formulas in text values only', () => {
        assert.equal(csvField('=HYPERLINK("http://x","y")'), `"'=HYPERLINK(""http://x"",""y"")"`);
        for (const v of ['+1+2', '-2+3', '@SUM(A1)', '\tcmd', '-']) assert.equal(csvField(v), `'${v}`, v);
        assert.equal(csvField('\r=1'), `"'\r=1"`, 'CR start: prefixed and quoted');
        for (const v of ['-5', '+3.5e2', '-.5', '1e-3']) assert.equal(csvField(v), v, `numeric text ${v} stays`);
        assert.equal(csvField(-5), '-5', 'numbers are not text');
        assert.equal(csvField(12n), '12');
        assert.equal(csvField('a=b'), 'a=b', 'only a leading character counts');
        assert.equal(csvField('has, comma'), '"has, comma"');
        assert.equal(csvField(null), '');
    });
    await test('DuckDB error messages are shortened for display', () => {
        assert.equal(cleanDuckDbError(new Error('Catalog Error: Cannot change type\nLINE 1: ...')), 'Cannot change type');
    });
    await test('import format is taken from the choice or the file extension', () => {
        assert.equal(detectImportFormat('', 'data.CSV'), 'csv');
        assert.equal(detectImportFormat('', 'x.tsv'), 'csv');
        assert.equal(detectImportFormat('', 'x.parquet'), 'parquet');
        assert.equal(detectImportFormat('', 'events.jsonl'), 'json');
        assert.equal(detectImportFormat('parquet', 'misnamed.csv'), 'parquet');
        assert.throws(() => detectImportFormat('', 'x.xlsx'), /format/);
        assert.throws(() => detectImportFormat('xlsx', 'x.csv'), /Unsupported/);
    });

    console.log('\n▸ console: update check');
    await test('dotted versions compare numerically, missing parts count as 0', () => {
        assert.equal(compareVersions('1.6.25.1', '1.6.25'), 1);
        assert.equal(compareVersions('1.6.10', '1.6.9'), 1);
        assert.equal(compareVersions('v1.6.25', '1.6.25.0'), 0);
        assert.equal(compareVersions('1.6.24.9', '1.6.25'), -1);
        assert.equal(compareVersions('1.6.x', '1.6.25'), null);
        assert.equal(compareVersions(undefined, '1.0'), null);
    });
    const fakeFetch = (version, calls) => async () => { calls.n++; return { ok: true, json: async () => ({ version }) }; };
    await test('newer remote version reports an update and the update command', async () => {
        resetUpdateCheckCache();
        const info = await checkForUpdate('1.6.25.1', { fetchImpl: fakeFetch('1.7.0', { n: 0 }), now: 1000 });
        assert.equal(info.latest, '1.7.0');
        assert.equal(info.updateAvailable, true);
        assert.equal(info.updateCommand, 'git pull && docker compose up -d --build');
        assert.match(info.repoUrl, /github\.com\/stephen-kruger\/ichibi-lake$/);
    });
    await test('same or older remote version reports no update', async () => {
        resetUpdateCheckCache();
        assert.equal((await checkForUpdate('1.6.25.1', { fetchImpl: fakeFetch('1.6.25.1', { n: 0 }), now: 1000 })).updateAvailable, false);
        resetUpdateCheckCache();
        assert.equal((await checkForUpdate('1.6.25.1', { fetchImpl: fakeFetch('1.6.24', { n: 0 }), now: 1000 })).updateAvailable, false);
    });
    await test('GitHub is contacted at most once per cache period', async () => {
        resetUpdateCheckCache();
        const calls = { n: 0 };
        const f = fakeFetch('1.7.0', calls);
        await Promise.all([checkForUpdate('1.0', { fetchImpl: f, now: 0 }), checkForUpdate('1.0', { fetchImpl: f, now: 0 })]);
        await checkForUpdate('1.0', { fetchImpl: f, now: 60 * 60 * 1000 });
        assert.equal(calls.n, 1);
        await checkForUpdate('1.0', { fetchImpl: f, now: 7 * 60 * 60 * 1000 });
        assert.equal(calls.n, 2);
    });
    await test('network errors and bad responses report latest = null, not an error', async () => {
        resetUpdateCheckCache();
        const down = await checkForUpdate('1.0', { fetchImpl: async () => { throw new Error('offline'); }, now: 0 });
        assert.deepEqual([down.latest, down.updateAvailable], [null, false]);
        resetUpdateCheckCache();
        const notFound = await checkForUpdate('1.0', { fetchImpl: async () => ({ ok: false }), now: 0 });
        assert.equal(notFound.latest, null);
        resetUpdateCheckCache();
        const garbage = await checkForUpdate('1.0', { fetchImpl: fakeFetch('<html>', { n: 0 }), now: 0 });
        assert.equal(garbage.latest, null);
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
