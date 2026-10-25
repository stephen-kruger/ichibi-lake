/**
 * Unit tests for src/storage-config.js (storage settings saved from the
 * console: two-phase save, confirm, revert after failed starts) and the form
 * validation in src/console/storage-admin.js. No Docker needed.
 */
import assert from 'node:assert/strict';
import fs from 'fs';
import os from 'os';
import path from 'path';

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'ichibi-storage-cfg-'));
const file = path.join(dir, 'storage.json');
process.env.STORAGE_CONFIG_FILE = file;
process.env.DUCKLAKE_DATA_PATH = '/env/data';
delete process.env.S3_SECRET_KEY;

const cfg = await import('../src/storage-config.js');
const { fromForm } = await import('../src/console/storage-admin.js');

let passed = 0, failed = 0;
async function test(name, fn) {
    try { await fn(); passed++; console.log(`  ✓ ${name}`); }
    catch (err) { failed++; console.error(`  ✗ ${name}\n    ${err.message}`); }
}
const restart = () => cfg.applyStorageConfigFile(file);   // what a new process does on import
const S3 = { DUCKLAKE_DATA_PATH: 's3://bucket/lake/', S3_ENDPOINT: 'minio:9000', S3_ACCESS_KEY: 'k', S3_SECRET_KEY: 'secret!', S3_USE_SSL: 'false' };

async function run() {
    console.log('\n▸ storage-config: saved storage settings');

    await test('without a file, docker/.env values are used', () => {
        restart();
        assert.equal(process.env.DUCKLAKE_DATA_PATH, '/env/data');
        assert.equal(cfg.describeStorageSettings().DUCKLAKE_DATA_PATH.source, 'env');
        assert.equal(cfg.storageFileStatus().exists, false);
    });

    await test('a save is pending until confirmed, then sticks', () => {
        cfg.saveStorageConfig(S3, { by: 'admin', effective: { DUCKLAKE_DATA_PATH: '/env/data' }, switchFrom: '/env/data' });
        assert.equal((fs.statSync(file).mode & 0o777).toString(8), '600', 'owner-only permissions');
        restart();
        assert.equal(process.env.DUCKLAKE_DATA_PATH, 's3://bucket/lake/');
        assert.equal(process.env.S3_SECRET_KEY, 'secret!');
        assert.equal(cfg.describeStorageSettings().DUCKLAKE_DATA_PATH.source, 'console');
        assert.deepEqual(cfg.pendingSwitch(), { from: '/env/data', settings: { DUCKLAKE_DATA_PATH: '/env/data' } });
        assert.equal(cfg.storageFileStatus().pending, true);
        cfg.confirmStorageConfig({ lastSwitch: { from: '/env/data', to: 's3://bucket/lake/' } });
        restart();
        assert.equal(cfg.storageFileStatus().pending, false);
        assert.equal(cfg.pendingSwitch(), null, 'a confirmed file does not move the lake again');
        assert.equal(cfg.storageFileStatus().lastSwitch.to, 's3://bucket/lake/');
        assert.equal(process.env.DUCKLAKE_DATA_PATH, 's3://bucket/lake/');
    });

    await test('failed starts on new settings revert to the previous saved ones', () => {
        cfg.saveStorageConfig({ ...S3, S3_ENDPOINT: 'wrong:1' }, { by: 'admin', effective: S3 });
        restart();
        assert.equal(process.env.S3_ENDPOINT, 'wrong:1');
        assert.equal(cfg.noteAttachFailure(new Error('connection refused')), false, '1st failure: retry');
        assert.equal(cfg.noteAttachFailure(new Error('connection refused')), false, '2nd failure: retry');
        assert.equal(cfg.noteAttachFailure(new Error('connection refused')), true, `${cfg.PENDING_ATTEMPTS}rd failure: revert`);
        restart();
        assert.equal(process.env.S3_ENDPOINT, 'minio:9000');
        const st = cfg.storageFileStatus();
        assert.equal(st.pending, false);
        assert.match(st.lastError.message, /connection refused/);
        assert.equal(st.lastError.settings.S3_SECRET_KEY, '********', 'secrets are redacted in lastError');
        assert.equal(st.lastError.settings.S3_ENDPOINT, 'wrong:1');
    });

    await test('a refused move reverts at once; with nothing saved before, docker/.env applies again', () => {
        fs.unlinkSync(file);
        restart();
        cfg.saveStorageConfig({ DUCKLAKE_DATA_PATH: '/new/place' }, { effective: { DUCKLAKE_DATA_PATH: '/env/data' }, switchFrom: '/env/data' });
        restart();
        assert.equal(process.env.DUCKLAKE_DATA_PATH, '/new/place');
        assert.equal(cfg.noteAttachFailure(new Error('not empty'), { fatal: true }), true);
        restart();
        assert.equal(process.env.DUCKLAKE_DATA_PATH, '/env/data');
        assert.equal(cfg.describeStorageSettings().DUCKLAKE_DATA_PATH.source, 'env');
    });

    await test('failures on a confirmed configuration never revert', () => {
        restart();
        assert.equal(cfg.noteAttachFailure(new Error('db down'), { fatal: true }), false);
    });

    console.log('\n▸ storage-admin: form validation');
    const current = { DUCKLAKE_DATA_PATH: '/app/data_files', S3_SECRET_KEY: 'keep-me', S3_ACCESS_KEY: 'k', BLOBS_PATH: '/app/blobs', USE_FILESYSTEM_BLOBS: '1' };
    const base = { backend: 's3', localPath: '', s3: { path: 's3://b/lake', endpoint: 'minio:9000', region: '', accessKey: 'k', secretKey: '', urlStyle: '', useSsl: false }, inliningLimit: 1000, blobs: { mode: 'filesystem', path: '/app/blobs' } };

    await test('S3 form: trailing slash added, empty secret keeps the saved one', () => {
        const s = fromForm(base, current);
        assert.equal(s.DUCKLAKE_DATA_PATH, 's3://b/lake/');
        assert.equal(s.S3_SECRET_KEY, 'keep-me');
        assert.equal(s.S3_USE_SSL, 'false');
        assert.equal(s.USE_FILESYSTEM_BLOBS, '1');
    });
    await test('no access key means the credential chain: secrets are dropped', () => {
        const s = fromForm({ ...base, s3: { ...base.s3, accessKey: '' } }, current);
        assert.equal(s.S3_SECRET_KEY, '');
    });
    await test('invalid input is rejected with 400', () => {
        const bad = (form, re) => assert.throws(() => fromForm(form, current), (e) => e.statusCode === 400 && re.test(e.message));
        bad({ ...base, s3: { ...base.s3, path: 'bucket/lake' } }, /s3:\/\/bucket/);
        bad({ ...base, backend: 'local', localPath: 's3://x/' }, /cannot be a URL/);
        bad({ ...base, backend: 'local', localPath: ' ' }, /local directory/);
        bad({ ...base, backend: 'ftp' }, /backend/);
        bad({ ...base, inliningLimit: -1 }, /inlining/);
        bad({ ...base, s3: { ...base.s3, urlStyle: 'weird' } }, /URL style/);
        bad({ ...base, blobs: { mode: 'filesystem', path: '' } }, /BLOB directory/);
    });
    await test('BLOBs inside tables clear the filesystem flag', () => {
        const s = fromForm({ ...base, blobs: { mode: 'inline', path: '' } }, current);
        assert.equal(s.USE_FILESYSTEM_BLOBS, '');
        assert.equal(s.BLOBS_PATH, '/app/blobs');
    });
}

run().then(() => {
    fs.rmSync(dir, { recursive: true, force: true });
    console.log('\n─── storage-config unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    if (failed > 0) process.exit(1);
}).catch((err) => { console.error(err); process.exit(1); });
