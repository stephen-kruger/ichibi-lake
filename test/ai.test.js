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
const { extractParams, bindParams, plainValue, aiConfig, isConfigured, testSettings, settingsView, AI_SETTINGS, terms, scoreTable, schemaText, pickExamples, paramLiteral, normaliseParamDefs, functionSignature, PARAM_TYPES, inlineParams } = await import('../src/console/ai.js');
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

    console.log('\n▸ ai: typed parameters');
    const lit = (type, v) => paramLiteral({ name: 'p', type }, v);
    const rejects = (type, v, re) => assert.throws(() => lit(type, v), (e) => e.statusCode === 400 && /\{\{p\}\}/.test(e.message) && re.test(e.message), `${type} ${v}`);
    await test('each type becomes a validated SQL literal', () => {
        assert.equal(lit('text', "it's"), "'it''s'");
        assert.equal(lit('number', ' 10 '), '10');
        assert.equal(lit('number', '-2.5e3'), '-2.5e3');
        assert.equal(lit('date', '2024-02-29'), "DATE '2024-02-29'");
        assert.equal(lit('timestamp', '2024-02-03T04:05'), "TIMESTAMP '2024-02-03 04:05'");
        assert.equal(lit('timestamp', '2024-02-03 04:05:06.123Z'), "TIMESTAMPTZ '2024-02-03 04:05:06.123Z'");
        assert.equal(lit('boolean', 'Yes'), 'TRUE');
        assert.equal(lit('boolean', '0'), 'FALSE');
        assert.equal(lit('list', "eu, us ,o'x,"), "'eu', 'us', 'o''x'");
        assert.equal(lit('list', ['a', 'b']), "'a', 'b'");
        assert.deepEqual(PARAM_TYPES, ['text', 'number', 'date', 'timestamp', 'boolean', 'list']);
    });
    await test('values that do not match the type are rejected, so they can never inject SQL', () => {
        rejects('number', '1; DROP TABLE x', /not a number/);
        rejects('number', '1 OR 1=1', /not a number/);
        rejects('date', '2023-02-29', /not a date/);
        rejects('date', "2024-01-01' OR '1'='1", /not a date/);
        rejects('timestamp', '2024-01-01 25:00', /not a timestamp/);
        rejects('boolean', 'maybe', /true or false/);
        rejects('list', ' , ', /empty/);
    });
    await test('defaults make parameters optional; a missing value without one is required', () => {
        const defs = [{ name: 'n', type: 'number', default: '10' }, { name: 'region', type: 'text', default: null }];
        assert.equal(bindParams('SELECT * FROM t WHERE r = {{region}} LIMIT {{n}}', { region: 'eu' }, defs), "SELECT * FROM t WHERE r = 'eu' LIMIT 10");
        assert.equal(bindParams('SELECT {{n}}', { n: '5' }, defs), 'SELECT 5');
        assert.throws(() => bindParams('SELECT {{region}}', {}, defs), /A value for \{\{region\}\} is required/);
        assert.equal(bindParams('SELECT {{n}}, {{d}}, {{b}}, {{l}}', null, [{ name: 'n', type: 'number' }, { name: 'd', type: 'date' }, { name: 'b', type: 'boolean' }, { name: 'l', type: 'list' }]),
            "SELECT 1, DATE '2000-01-01', TRUE, NULL", 'validation literals parse in every position');
        assert.equal(bindParams('SELECT * FROM t WHERE id IN ({{ids}})', { ids: '1,2' }, [{ name: 'ids', type: 'list' }]), "SELECT * FROM t WHERE id IN ('1', '2')");
    });
    await test('definitions follow the SQL; invalid ones are rejected when saving, ignored when reading', () => {
        const sql = 'SELECT * FROM t WHERE a = {{a}} AND b = {{b}}';
        const defs = normaliseParamDefs(sql, [{ name: 'b', type: 'date', default: '2024-01-01', description: ' since ' }, { name: 'gone', type: 'number' }]);
        assert.deepEqual(defs, [
            { name: 'a', type: 'text', default: null, description: '' },
            { name: 'b', type: 'date', default: '2024-01-01', description: 'since' },
        ]);
        assert.throws(() => normaliseParamDefs(sql, [{ name: 'a', type: 'money' }]), /type must be one of/);
        assert.throws(() => normaliseParamDefs(sql, [{ name: 'a', type: 'number', default: 'ten' }]), /not a number \(default value\)/);
        assert.deepEqual(normaliseParamDefs(sql, [{ name: 'a', type: 'number', default: 'ten' }], { strict: false })[0], { name: 'a', type: 'number', default: null, description: '' });
    });
    await test('removing parameters puts typed hard-coded values back', () => {
        const sql = 'SELECT * FROM t WHERE region IN ({{regions}}) AND d >= {{since}} AND x = {{keep}} LIMIT {{n}}';
        const defs = [
            { name: 'regions', type: 'list', default: "eu, o'x" },
            { name: 'since', type: 'date', default: '2024-01-01' },
            { name: 'keep', type: 'text', default: 'k' },
            { name: 'n', type: 'number', default: null },
        ];
        assert.equal(inlineParams(sql, defs, ['regions', 'since', 'n'], { n: '5' }),
            "SELECT * FROM t WHERE region IN ('eu', 'o''x') AND d >= DATE '2024-01-01' AND x = {{keep}} LIMIT 5");
        assert.equal(inlineParams('SELECT {{a}}, {{a}}', [{ name: 'a', type: 'text', default: 'z' }], ['a']), "SELECT 'z', 'z'", 'every occurrence');
        assert.throws(() => inlineParams(sql, defs, ['n']), /\{\{n\}\} has no default/);
        assert.throws(() => inlineParams(sql, defs, ['n'], { n: 'five' }), /not a number/);
        assert.equal(inlineParams(sql, defs, []), sql, 'nothing selected: unchanged');
    });
    await test('function signatures for the model show types and defaults', () => {
        assert.equal(functionSignature({ name: 'Orders', params: [{ name: 'region', type: 'text', default: null }, { name: 'n', type: 'number', default: '10' }] }), 'Orders(region text, n number = "10")');
        assert.equal(functionSignature({ name: 'All', params: [] }), 'All()');
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

    console.log('\n▸ ai: table selection and worked examples');
    const table = (name, columns, rows = 10) => ({ name, rows, columns: columns.map(c => `${c} VARCHAR`) });
    await test('names are split into singular words without common English words', () => {
        assert.deepEqual([...terms('How many sensor_readings per deviceId in the last 7 days?')].sort(), ['device', 'id', 'reading', 'sensor']);
        assert.deepEqual([...terms('batteries status')].sort(), ['battery', 'status']);
    });
    await test('table-name matches outweigh column matches', () => {
        const q = terms('average temperature per device');
        assert.ok(scoreTable(table('device_temperature', ['id']), q) > scoreTable(table('events', ['device', 'temperature']), q));
        assert.equal(scoreTable(table('orders', ['id', 'total']), q), 0);
        assert.ok(scoreTable(table('ha_states', ['x']), terms('rows in ha_states'), 'rows in ha_states') >= 5, 'exact table name in the question');
    });
    await test('a schema that fits is listed in full', () => {
        const text = schemaText([table('a', ['x', 'y']), table('b', ['z'])], 'anything');
        assert.match(text, /^- a \(10 rows\): x VARCHAR, y VARCHAR\n- b \(10 rows\): z VARCHAR$/);
    });
    await test('a large schema keeps columns for the relevant tables and names for the rest', () => {
        const wide = (name) => table(name, Array.from({ length: 300 }, (_, i) => `column_number_${i}`));
        const tables = [wide('orders'), wide('customers'), ...Array.from({ length: 30 }, (_, i) => wide(`filler_${i}`)), table('energy_meter', ['kwh', 'meter_id'])];
        const text = schemaText(tables, 'total kwh per energy meter');
        assert.ok(text.length < 26000, `length ${text.length}`);
        assert.match(text, /Tables that look relevant to the question, with their columns:\n- energy_meter \(10 rows\): kwh VARCHAR, meter_id VARCHAR/);
        assert.match(text, /call describe_table for their columns\):[\s\S]*- orders \(10 rows\), 300 columns/);
        for (const t of tables) assert.ok(text.includes(`- ${t.name} `), `${t.name} is listed`);
    });
    await test('without a matching table, a large schema lists names only', () => {
        const wide = (name) => table(name, Array.from({ length: 300 }, (_, i) => `c_${i}`));
        const text = schemaText(Array.from({ length: 20 }, (_, i) => wide(`t${i}`)), 'hello');
        assert.match(text, /^The schema is too large to list in full.\nTables \(call describe_table/);
    });
    await test('worked examples: relevant first, then recent; stale and long ones skipped', () => {
        const tables = [table('energy_meter', ['kwh']), table('orders', ['total'])];
        const fns = [
            { name: 'Orders per day', question: 'orders per day', sql: 'SELECT date_trunc(\'day\', created) d, count(*) FROM orders GROUP BY d', updatedMs: 1 },
            { name: 'Energy use', question: 'daily kwh used by each energy meter', sql: 'SELECT sum(kwh) FROM energy_meter', updatedMs: 2 },
            { name: 'Old', question: 'kwh from the old table', sql: 'SELECT * FROM dropped_table', updatedMs: 9 },
            { name: 'Huge', question: 'kwh', sql: `SELECT ${'1 + '.repeat(600)}1 FROM energy_meter`, updatedMs: 8 },
            { name: 'Recent', question: 'something else', sql: 'SELECT count(*) FROM "orders"', updatedMs: 5 },
        ];
        const picked = pickExamples(fns, 'how much kwh did each meter use this week?', tables, 2).map(f => f.name);
        assert.deepEqual(picked, ['Energy use', 'Recent']);
        assert.deepEqual(pickExamples(fns, 'x', tables, 4).map(f => f.name), ['Recent', 'Energy use', 'Orders per day']);
        assert.deepEqual(pickExamples([], 'kwh', tables), []);
        assert.deepEqual(pickExamples([{ name: 'n', sql: 'SELECT 1 FROM orders_archive' }], 'x', tables), [], 'a longer name is not a match');
    });

    console.log('\n─── ai unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed ? 1 : 0);
}

run();
