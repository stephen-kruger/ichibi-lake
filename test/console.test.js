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
import { buildCutoffPredicate, containedBlobPath, validateRule, isSystemTable } from '../src/console/retention.js';
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

    console.log('\n▸ console: retention helpers');
    await test('rule validation rejects unsafe identifiers, system tables and bad day counts', () => {
        assert.equal(validateRule({ tableName: 'events', columnName: 'created', keepDays: 30 }), null);
        assert.ok(validateRule({ tableName: 'events"; DROP', columnName: 'created', keepDays: 30 }));
        assert.ok(validateRule({ tableName: '_rbac_users', columnName: 'created_at', keepDays: 30 }));
        assert.ok(validateRule({ tableName: 'events', columnName: 'created', keepDays: 0 }));
        assert.ok(validateRule({ tableName: 'events', columnName: 'created', keepDays: 2.5 }));
        assert.ok(isSystemTable('_console_audit'));
    });
    await test('cutoff predicate uses whole days and the right clock per type', () => {
        assert.equal(buildCutoffPredicate('created', 'TIMESTAMP', 30), `"created" < CAST(CURRENT_TIMESTAMP AS TIMESTAMP) - INTERVAL 30 DAY`);
        assert.equal(buildCutoffPredicate('day', 'DATE', 7), `"day" < CURRENT_DATE - INTERVAL 7 DAY`);
        assert.throws(() => buildCutoffPredicate('x"y', 'DATE', 7));
        assert.throws(() => buildCutoffPredicate('x', 'DATE', '7; DROP'));
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
