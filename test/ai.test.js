/**
 * Unit tests for the console's Ask AI module (src/console/ai.js): saved-
 * function parameters, value conversion, AI settings validation and the
 * connection test against a mock OpenAI-compatible server. No database or
 * running gateway is needed.
 */
import assert from 'node:assert/strict';
import http from 'node:http';

// Settings snapshot docker/.env values at import time, so set the key first.
process.env.OPENAI_API_KEY = 'sk-test-1234567890abcdef';
const { extractParams, bindParams, plainValue, aiConfig, isConfigured, testSettings, settingsView, AI_SETTINGS } = await import('../src/console/ai.js');
const { SECTIONS, getSettingDef, normaliseSettingValue } = await import('../src/console/registry.js');

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

/** Minimal OpenAI-compatible server; `behaviour` decides each response. */
function mockServer(behaviour) {
    const requests = [];
    const server = http.createServer((req, res) => {
        let body = '';
        req.on('data', (c) => { body += c; });
        req.on('end', () => {
            const parsed = body ? JSON.parse(body) : null;
            requests.push({ method: req.method, url: req.url, auth: req.headers.authorization || null, body: parsed });
            const [status, payload] = behaviour(req, parsed);
            res.writeHead(status, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify(payload));
        });
    });
    return new Promise(resolve => server.listen(0, '127.0.0.1', () => resolve({
        url: `http://127.0.0.1:${server.address().port}/v1`, requests, close: () => new Promise(r => server.close(r)),
    })));
}

const goodModel = (req, body) => {
    if (req.url === '/v1/models') return [200, { data: [{ id: 'test-model' }, { id: 'other' }] }];
    if (body?.tool_choice) {
        return [200, { model: 'test-model', choices: [{ message: { role: 'assistant', content: null, tool_calls: [{ id: 'c1', type: 'function', function: { name: 'ping', arguments: '{"message":"hello"}' } }] } }] }];
    }
    return [200, { model: 'test-model', choices: [{ message: { role: 'assistant', content: 'OK' } }] }];
};

async function run() {
    console.log('\n▸ ai: saved-function parameters');
    await test('parameters are found once each, in order of first use', () => {
        assert.deepEqual(extractParams('SELECT * FROM t WHERE a = {{region}} AND b > {{ since }} OR a = {{region}}'), ['region', 'since']);
        assert.deepEqual(extractParams('SELECT 1'), []);
    });
    await test('values are bound as escaped string literals', () => {
        assert.equal(bindParams('WHERE a = {{x}}', { x: "o'brien" }), "WHERE a = 'o''brien'");
        assert.equal(bindParams('WHERE a = {{x}}', { x: "'; DROP TABLE t; --" }), "WHERE a = '''; DROP TABLE t; --'");
    });
    await test('validation binds every parameter as NULL', () => {
        assert.equal(bindParams('a = {{x}} AND b = {{y}}', null), 'a = NULL AND b = NULL');
    });
    await test('a missing or empty value is a 400', () => {
        assert.throws(() => bindParams('a = {{x}}', {}), (e) => e.statusCode === 400 && /\{\{x\}\}/.test(e.message));
        assert.throws(() => bindParams('a = {{x}}', { x: '' }), (e) => e.statusCode === 400);
    });

    console.log('\n▸ ai: result values');
    await test('BigInts become numbers when safe, strings otherwise', () => {
        assert.equal(plainValue(42n), 42);
        assert.equal(plainValue(2n ** 60n), '1152921504606846976');
    });
    await test('objects with their own toString (DuckDB values) become strings', () => {
        assert.equal(plainValue({ toString: () => '2026-10-26 10:00:00' }), '2026-10-26 10:00:00');
        assert.equal(plainValue({ a: 1n }), '{"a":"1"}');
        assert.equal(plainValue(null), null);
        assert.equal(plainValue(NaN), 'NaN');
    });

    console.log('\n▸ ai: settings');
    await test('the AI section and its settings are registered and managed on their own page', () => {
        assert.ok(SECTIONS.some(s => s.id === 'ai-settings' && s.kind === 'custom'));
        assert.deepEqual(AI_SETTINGS.map(s => s.key), ['OPENAI_BASE_URL', 'OPENAI_API_KEY', 'OPENAI_MODEL', 'AI_REQUEST_TIMEOUT_MS', 'AI_MAX_STEPS', 'AI_SAMPLE_ROWS']);
        assert.ok(AI_SETTINGS.every(s => s.editable && s.live && s.hidden));
    });
    await test('base URL must be http(s) and the model is required', () => {
        assert.ok(normaliseSettingValue(getSettingDef('OPENAI_BASE_URL'), 'ftp://x').error);
        assert.equal(normaliseSettingValue(getSettingDef('OPENAI_BASE_URL'), ' http://localhost:11434/v1 ').value, 'http://localhost:11434/v1');
        assert.ok(normaliseSettingValue(getSettingDef('OPENAI_MODEL'), '').error);
        assert.ok(normaliseSettingValue(getSettingDef('AI_SAMPLE_ROWS'), '501').error);
    });
    await test('defaults make a usable configuration; the key is never returned', () => {
        const cfg = aiConfig();
        assert.equal(cfg.baseUrl, 'https://api.openai.com/v1');
        assert.ok(isConfigured(cfg));
        const view = settingsView().settings.find(s => s.key === 'OPENAI_API_KEY');
        assert.equal(view.value, '');
        assert.equal(view.source, 'env');
        assert.ok(view.hasValue && !view.masked.includes('1234567890'));
        assert.ok(!JSON.stringify(settingsView()).includes('1234567890'));
    });

    console.log('\n▸ ai: connection test');
    await test('a working server passes every step and gets the key as a Bearer token', async () => {
        const srv = await mockServer(goodModel);
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'test-model', OPENAI_API_KEY: 'sk-abc' });
            assert.equal(r.ok, true, JSON.stringify(r));
            assert.deepEqual(r.results.map(x => x.label), ['Models list', 'Chat completion', 'Tool calling']);
            assert.ok(srv.requests.every(q => q.auth === 'Bearer sk-abc'));
            assert.equal(srv.requests[1].body.model, 'test-model');
        } finally { await srv.close(); }
    });
    await test('an unknown model name is a warning on the models list', async () => {
        const srv = await mockServer(goodModel);
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'nope' });
            assert.match(r.results[0].warning, /not in the list/);
            assert.equal(srv.requests[0].auth, 'Bearer sk-test-1234567890abcdef', 'an empty key field keeps the saved key');
        } finally { await srv.close(); }
    });
    await test('a rejected key fails with the server\'s message', async () => {
        const srv = await mockServer(() => [401, { error: { message: 'Incorrect API key provided' } }]);
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'm', OPENAI_API_KEY: 'bad' });
            assert.equal(r.ok, false);
            assert.match(r.results[0].detail, /rejected.*Incorrect API key/);
        } finally { await srv.close(); }
    });
    await test('a model without tool calling fails the tool step', async () => {
        const srv = await mockServer((req) => (req.url === '/v1/models' ? [404, {}] : [200, { choices: [{ message: { content: 'OK' } }] }]));
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'm' });
            assert.equal(r.ok, false);
            assert.equal(r.results[0].ok, true, 'a missing /models is not fatal');
            assert.match(r.results[2].detail, /tool/);
        } finally { await srv.close(); }
    });
    await test('OpenRouter provider errors name the provider, its message and a hint', async () => {
        const srv = await mockServer(() => [502, { error: { code: 502, message: 'Provider returned error',
            metadata: { provider_name: 'Chutes', raw: '{"error":{"message":"upstream model overloaded"}}' } } }]);
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'm' });
            const chatStep = r.results.find(x => x.label === 'Chat completion');
            assert.match(chatStep.detail, /HTTP 502: Provider returned error \(provider: Chutes\): upstream model overloaded\. The provider behind the API failed/);
        } finally { await srv.close(); }
    });
    await test('an error inside an HTTP 200 body is reported, not ignored', async () => {
        const srv = await mockServer((req) => (req.url === '/v1/models' ? [404, {}]
            : [200, { error: { code: 429, message: 'Rate limit exceeded: free-models-per-day' } }]));
        try {
            const r = await testSettings({ OPENAI_BASE_URL: srv.url, OPENAI_MODEL: 'm' });
            assert.match(r.results.find(x => x.label === 'Chat completion').detail, /HTTP 429: Rate limit exceeded.*Rate limited; wait/);
        } finally { await srv.close(); }
    });
    await test('a timeout while the answer is still coming is reported as a timeout', async () => {
        // Headers at once, body after the deadline, as OpenRouter does for slow models.
        const server = http.createServer((req, res) => {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.write(' ');
            setTimeout(() => res.end('{"choices":[{"message":{"content":"OK"}}]}'), 1500);
        });
        await new Promise(r => server.listen(0, '127.0.0.1', r));
        try {
            const r = await testSettings({ OPENAI_BASE_URL: `http://127.0.0.1:${server.address().port}/v1`, OPENAI_MODEL: 'm', AI_REQUEST_TIMEOUT_MS: 1000 });
            assert.match(r.results[0].detail, /did not answer within 1 s.*raise the timeout/);
        } finally {
            server.closeAllConnections();
            await new Promise(r => server.close(r));
        }
    });
    await test('an unreachable server reports a connection error', async () => {
        const r = await testSettings({ OPENAI_BASE_URL: 'http://127.0.0.1:9/v1', OPENAI_MODEL: 'm' });
        assert.equal(r.ok, false);
        assert.match(r.results[0].detail, /Could not reach/);
    });
    await test('invalid form values are rejected before any request', async () => {
        await assert.rejects(testSettings({ OPENAI_BASE_URL: 'not a url' }), (e) => e.statusCode === 400);
        await assert.rejects(testSettings({ SOMETHING_ELSE: 'x' }), (e) => e.statusCode === 400);
    });

    console.log('\n─── ai unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed ? 1 : 0);
}

run();
