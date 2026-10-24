/**
 * Unit tests for object-storage configuration (src/storage.js) and the lake
 * maintenance settings (src/console/maintenance.js). No database or running
 * gateway is needed.
 */
import assert from 'node:assert/strict';
import path from 'node:path';
import {
    storageConfig, buildS3SecretSql, isObjectStorePath, joinDataPath, withTrailingSlash, describeStorage, dataInliningRowLimit, S3_SECRET_NAME,
} from '../src/storage.js';
import { normaliseMaintenance, describeSteps, MAINTENANCE_DEFAULTS, MAINTENANCE_STEPS } from '../src/console/maintenance.js';
import { isTransactionConflict } from '../src/console/store.js';

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}

async function run() {
    console.log('\n▸ storage: data path');
    await test('local paths are not object storage', () => {
        assert.equal(isObjectStorePath('/app/data_files'), false);
        assert.equal(isObjectStorePath('data_files/'), false);
        assert.equal(isObjectStorePath('C:\\lake'), false);
        assert.equal(isObjectStorePath('s3://bucket/lake/'), true);
    });
    await test('default is the local data_files/ directory', () => {
        assert.deepEqual(storageConfig({}), { kind: 'local', dataPath: 'data_files/' });
        assert.deepEqual(storageConfig({ DUCKLAKE_DATA_PATH: '/app/data_files' }), { kind: 'local', dataPath: '/app/data_files' });
    });
    await test('s3 data path always ends with exactly one slash', () => {
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/lake', S3_ACCESS_KEY: 'k', S3_SECRET_KEY: 's' }).dataPath, 's3://b/lake/');
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/lake/', S3_ACCESS_KEY: 'k', S3_SECRET_KEY: 's' }).dataPath, 's3://b/lake/');
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b' }).dataPath, 's3://b/');
        assert.equal(withTrailingSlash('x/'), 'x/');
    });
    await test('unsupported URL schemes are rejected with guidance', () => {
        assert.throws(() => storageConfig({ DUCKLAKE_DATA_PATH: 'gs://b/lake/' }), /s3:\/\/ URL/);
        assert.throws(() => storageConfig({ DUCKLAKE_DATA_PATH: 'https://example.com/x' }), /not supported/);
    });
    await test('sub-directories join without collapsing the URL', () => {
        assert.equal(joinDataPath('s3://b/lake/', '_console'), 's3://b/lake/_console/');
        assert.equal(joinDataPath('s3://b/lake', '_console'), 's3://b/lake/_console/');
        assert.equal(joinDataPath('/app/data_files', '_console'), path.join('/app/data_files', '_console') + path.sep);
    });

    console.log('\n▸ storage: S3 settings');
    const minio = { DUCKLAKE_DATA_PATH: 's3://lake/data/', S3_ENDPOINT: 'http://minio:9000/', S3_ACCESS_KEY: 'minioadmin', S3_SECRET_KEY: "it's secret", S3_USE_SSL: 'false' };
    await test('MinIO: endpoint scheme stripped, path style and no TLS', () => {
        const c = storageConfig(minio);
        assert.equal(c.kind, 's3');
        assert.equal(c.bucket, 'lake');
        assert.equal(c.endpoint, 'minio:9000');
        assert.equal(c.urlStyle, 'path');
        assert.equal(c.useSsl, false);
        assert.equal(c.region, 'us-east-1');
        assert.equal(c.provider, 'config');
    });
    await test('AWS: no endpoint means vhost style, TLS and the credential chain without a key', () => {
        const c = storageConfig({ DUCKLAKE_DATA_PATH: 's3://prod-lake/x/', S3_REGION: 'eu-west-2' });
        assert.equal(c.urlStyle, 'vhost');
        assert.equal(c.useSsl, true);
        assert.equal(c.provider, 'credential_chain');
        assert.equal(c.region, 'eu-west-2');
        assert.match(buildS3SecretSql(c), /PROVIDER credential_chain, REFRESH auto/);
        assert.doesNotMatch(buildS3SecretSql(c), /KEY_ID|ENDPOINT/);
    });
    await test('S3_PATH_STYLE is accepted as an alias; S3_URL_STYLE wins and is validated', () => {
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/', S3_PATH_STYLE: 'true' }).urlStyle, 'path');
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/', S3_ENDPOINT: 'x:1', S3_PATH_STYLE: 'false' }).urlStyle, 'vhost');
        assert.equal(storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/', S3_PATH_STYLE: 'true', S3_URL_STYLE: 'vhost' }).urlStyle, 'vhost');
        assert.throws(() => storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/', S3_URL_STYLE: 'virtual' }), /path.*vhost/);
    });
    await test('a key without a secret is a configuration error', () => {
        assert.throws(() => storageConfig({ DUCKLAKE_DATA_PATH: 's3://b/', S3_ACCESS_KEY: 'k' }), /S3_SECRET_KEY/);
    });
    await test('secret SQL is temporary, scoped to the data path and escapes quotes', () => {
        const sql = buildS3SecretSql(storageConfig(minio));
        assert.match(sql, new RegExp(`^CREATE OR REPLACE TEMPORARY SECRET ${S3_SECRET_NAME} \\(TYPE s3,`));
        assert.match(sql, /SECRET 'it''s secret'/);
        assert.match(sql, /ENDPOINT 'minio:9000'/);
        assert.match(sql, /URL_STYLE 'path', USE_SSL false, SCOPE 's3:\/\/lake\/data\/'\)$/);
        assert.equal(buildS3SecretSql({ kind: 'local', dataPath: 'x' }), null);
    });
    await test('session tokens are passed through', () => {
        assert.match(buildS3SecretSql(storageConfig({ ...minio, S3_SESSION_TOKEN: 'tok' })), /SESSION_TOKEN 'tok'/);
    });
    await test('the storage description never contains credentials', () => {
        const d = describeStorage(storageConfig(minio));
        assert.match(d, /s3:\/\/lake\/data\/ on http:\/\/minio:9000/);
        assert.doesNotMatch(d, /minioadmin|secret'/);
        assert.equal(describeStorage(storageConfig({})), 'local directory data_files/');
    });

    console.log('\n▸ storage: data inlining');
    await test('inlining defaults to 1000 rows, 0 disables, junk falls back', () => {
        assert.equal(dataInliningRowLimit({}), 1000);
        assert.equal(dataInliningRowLimit({ DATA_INLINING_ROW_LIMIT: '0' }), 0);
        assert.equal(dataInliningRowLimit({ DATA_INLINING_ROW_LIMIT: '50' }), 50);
        assert.equal(dataInliningRowLimit({ DATA_INLINING_ROW_LIMIT: 'lots' }), 1000);
        assert.equal(dataInliningRowLimit({ DATA_INLINING_ROW_LIMIT: '-1' }), 1000);
        assert.equal(dataInliningRowLimit({ DATA_INLINING_ROW_LIMIT: '99999999' }), 1_000_000);
    });

    console.log('\n▸ maintenance: settings');
    await test('empty input gives the defaults (hourly, every step)', () => {
        const m = normaliseMaintenance({});
        assert.equal(m.schedule, MAINTENANCE_DEFAULTS.schedule);
        assert.equal(m.enabled, true);
        assert.equal(m.runAtStartup, true);
        assert.equal(m.deleteThreshold, null);
        assert.deepEqual(Object.keys(m.steps), Object.keys(MAINTENANCE_STEPS));
        assert.ok(Object.values(m.steps).every(Boolean));
    });
    await test('steps can be switched off individually; unknown steps are dropped', () => {
        const m = normaliseMaintenance({ steps: { merge: false, cleanup: 0, bogus: true } });
        assert.equal(m.steps.merge, false);
        assert.equal(m.steps.cleanup, false);
        assert.equal(m.steps.flush, true);
        assert.equal('bogus' in m.steps, false);
        assert.equal(describeSteps(m.steps), 'flush, rewrite, expire');
        assert.equal(describeSteps({}), 'no steps');
    });
    await test('schedule must be valid cron', () => {
        assert.throws(() => normaliseMaintenance({ schedule: '61 * * * *' }), (e) => e.statusCode === 400 && /schedule/i.test(e.message));
        assert.equal(normaliseMaintenance({ schedule: '@daily' }).schedule, '@daily');
    });
    await test('delete threshold is a fraction in (0, 1]', () => {
        assert.equal(normaliseMaintenance({ deleteThreshold: 0.5 }).deleteThreshold, 0.5);
        assert.equal(normaliseMaintenance({ deleteThreshold: '' }).deleteThreshold, null);
        for (const bad of [0, -0.1, 1.5, 'x']) {
            assert.throws(() => normaliseMaintenance({ deleteThreshold: bad }), (e) => e.statusCode === 400, `threshold ${bad}`);
        }
    });
    await test('enabled and runAtStartup are booleans', () => {
        const m = normaliseMaintenance({ enabled: false, runAtStartup: false });
        assert.equal(m.enabled, false);
        assert.equal(m.runAtStartup, false);
    });

    console.log('\n▸ maintenance: claims');
    await test('a lost commit race counts as "claimed elsewhere", other errors do not', () => {
        assert.equal(isTransactionConflict(new Error('TransactionContext Error: Failed to commit: Failed to commit DuckLake transaction.\nTransaction conflict - attempting to insert into table with index "6" - but another transaction has deleted inlined data from it')), true);
        assert.equal(isTransactionConflict(new Error('Catalog Error: Table does not exist')), false);
        assert.equal(isTransactionConflict(null), false);
    });

    console.log(`\n  passed: ${passed}\n  failed: ${failed}`);
    if (failed) process.exit(1);
}

run();
