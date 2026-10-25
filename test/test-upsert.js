/**
 * test/test-upsert.js
 *
 * Integration tests for key-based ingestion against a live gateway:
 *
 *   node test/test-upsert.js
 *
 *   - PUT/GET /tables/:tableName/keys (validation, unkeyable types)
 *   - POST /upload/:tableName?mode=upsert replaces matching keys, inserts new ones
 *   - composite keys, ?key= declaring keys on table creation
 *   - duplicates within a batch (last wins, ?orderBy= highest wins)
 *   - `created` preserved and `updated` set on replaced rows
 *   - schema evolution during an upsert
 *   - mode=dedupe inserts only new keys
 *   - missing / NULL key column -> 400, no declared keys -> 400, key mismatch -> 409
 *   - /kafka-sink?mode=upsert
 *   - RBAC: a reader gets 403 for upserts and for PUT keys
 *   - concurrent upserts to the same table end consistent (no duplicate keys,
 *     no lost batches: every request either succeeds or reports 409)
 */
import { API_KEY, BASE_URL } from './_env.js';

const RUN = Date.now();
const T = (name) => `ups_${name}_${RUN}`;
const READER_KEY = `sk-test-ups-reader-${RUN}`;

let passed = 0;
let failed = 0;

async function test(name, fn) {
    try {
        await fn();
        console.log(`  ✔ ${name}`);
        passed++;
    } catch (err) {
        console.log(`  ✘ ${name}\n      ${err.message}`);
        failed++;
    }
}

function assert(cond, msg) {
    if (!cond) throw new Error(msg);
}

async function call(method, path, body, key = API_KEY) {
    const res = await fetch(`${BASE_URL}${path}`, {
        method,
        headers: { 'Content-Type': 'application/json', 'x-api-key': key },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    let json = null;
    try { json = await res.json(); } catch { /* not JSON */ }
    return { status: res.status, json };
}

const upload = (table, rows, qs = '', key) => call('POST', `/upload/${table}${qs}`, rows, key);

async function sql(query) {
    const r = await call('POST', '/query', { sql: query });
    if (r.status !== 200) throw new Error(`SQL ${query} -> ${r.status} ${JSON.stringify(r.json)}`);
    return r.json.rows ?? r.json.data ?? r.json;
}

async function rowsOf(table, order = 'id') {
    return sql(`SELECT * FROM "${table}" ORDER BY ${order}`);
}

function expectOk(r, what) {
    assert(r.status === 200 && r.json?.success, `${what}: ${r.status} ${JSON.stringify(r.json)}`);
    return r.json;
}

async function main() {
    console.log(`\nKey-based ingestion (upsert / dedupe) against ${BASE_URL}\n`);

    // ---------------------------------------------------------------------
    await test('PUT /tables/:t/keys declares keys; GET returns them; schema lists them', async () => {
        const t = T('keys');
        expectOk(await upload(t, [{ id: 1, name: 'a', meta: { x: 1 } }]), 'seed');
        const put = expectOk(await call('PUT', `/tables/${t}/keys`, { keys: ['id'] }), 'PUT keys');
        assert(JSON.stringify(put.keys) === '["id"]' && put.duplicateKeys === 0, JSON.stringify(put));
        const get = expectOk(await call('GET', `/tables/${t}/keys`), 'GET keys');
        assert(JSON.stringify(get.keys) === '["id"]', JSON.stringify(get));
        const schema = expectOk(await call('GET', `/tables/${t}/schema`), 'schema');
        assert(JSON.stringify(schema.keys) === '["id"]', JSON.stringify(schema.keys));
        // Validation
        assert((await call('PUT', `/tables/${t}/keys`, { keys: ['nope'] })).status === 400, 'unknown column must be 400');
        assert((await call('PUT', `/tables/${t}/keys`, { keys: ['meta'] })).status === 400, 'STRUCT key must be 400');
        assert((await call('PUT', `/tables/${t}/keys`, { keys: 'id; drop' })).status === 400, 'unsafe name must be 400');
        assert((await call('PUT', `/tables/no_such_table_${RUN}/keys`, { keys: ['id'] })).status === 404, 'missing table must be 404');
        // Duplicate report
        expectOk(await upload(t, [{ id: 1, name: 'dup' }]), 'append duplicate');
        const dup = expectOk(await call('PUT', `/tables/${t}/keys`, { keys: ['id'] }), 'PUT keys again');
        assert(dup.duplicateKeys === 1 && dup.warning, `expected duplicate warning: ${JSON.stringify(dup)}`);
        // Clear
        const cleared = expectOk(await call('PUT', `/tables/${t}/keys`, { keys: [] }), 'clear keys');
        assert(cleared.keys.length === 0, 'keys cleared');
        assert((await upload(t, [{ id: 1 }], '?mode=upsert')).status === 400, 'upsert without keys must be 400');
    });

    // ---------------------------------------------------------------------
    await test('upsert replaces rows with matching keys and inserts new ones', async () => {
        const t = T('basic');
        expectOk(await upload(t, [{ id: 1, name: 'a', score: 10 }, { id: 2, name: 'b', score: 20 }]), 'seed');
        expectOk(await call('PUT', `/tables/${t}/keys`, { keys: ['id'] }), 'keys');
        const r = expectOk(await upload(t, [{ id: 2, name: 'B', score: 21 }, { id: 3, name: 'c', score: 30 }], '?mode=upsert'), 'upsert');
        assert(r.mode === 'upsert' && r.inserted === 1 && r.updated === 1, JSON.stringify(r));
        const rows = await rowsOf(t);
        assert(rows.length === 3, `expected 3 rows, got ${JSON.stringify(rows)}`);
        assert(rows[0].name === 'a' && rows[1].name === 'B' && Number(rows[1].score) === 21 && rows[2].name === 'c', JSON.stringify(rows));
    });

    // ---------------------------------------------------------------------
    await test('upsert replaces the whole row (columns missing from the batch become NULL)', async () => {
        const t = T('replace');
        expectOk(await upload(t, [{ id: 1, name: 'a', note: 'keep?' }]), 'seed');
        expectOk(await call('PUT', `/tables/${t}/keys`, { keys: ['id'] }), 'keys');
        expectOk(await upload(t, [{ id: 1, name: 'a2' }], '?mode=upsert'), 'upsert');
        const rows = await rowsOf(t);
        assert(rows.length === 1 && rows[0].name === 'a2' && rows[0].note === null, JSON.stringify(rows));
    });

    // ---------------------------------------------------------------------
    await test('composite keys, declared with ?key= on table creation', async () => {
        const t = T('composite');
        const c = expectOk(await upload(t, [
            { region: 'eu', device: 'd1', temp: 1 },
            { region: 'us', device: 'd1', temp: 2 },
        ], '?mode=upsert&key=region,device'), 'create');
        assert(c.inserted === 2 && JSON.stringify(c.keys) === '["region","device"]', JSON.stringify(c));
        const keys = expectOk(await call('GET', `/tables/${t}/keys`), 'keys');
        assert(JSON.stringify(keys.keys) === '["region","device"]', JSON.stringify(keys));
        const r = expectOk(await upload(t, [
            { region: 'eu', device: 'd1', temp: 10 },
            { region: 'eu', device: 'd2', temp: 20 },
        ], '?mode=upsert'), 'upsert');
        assert(r.inserted === 1 && r.updated === 1, JSON.stringify(r));
        const rows = await rowsOf(t, 'region, device');
        assert(rows.length === 3, JSON.stringify(rows));
        const byKey = Object.fromEntries(rows.map(x => [`${x.region}/${x.device}`, Number(x.temp)]));
        assert(byKey['eu/d1'] === 10 && byKey['eu/d2'] === 20 && byKey['us/d1'] === 2, JSON.stringify(byKey));
        // A different key list for a keyed table is a conflict
        assert((await upload(t, [{ region: 'eu', device: 'd1' }], '?mode=upsert&key=device')).status === 409, 'key mismatch must be 409');
    });

    // ---------------------------------------------------------------------
    await test('duplicate keys within a batch: last occurrence wins, ?orderBy= highest wins', async () => {
        const t = T('batchdup');
        const r = expectOk(await upload(t, [
            { id: 1, v: 'first', version: 3 },
            { id: 2, v: 'only', version: 1 },
            { id: 1, v: 'last', version: 1 },
        ], '?mode=upsert&key=id'), 'create');
        assert(r.inserted === 2 && r.duplicates === 1, JSON.stringify(r));
        let rows = await rowsOf(t);
        assert(rows.length === 2 && rows[0].v === 'last', JSON.stringify(rows));

        const o = expectOk(await upload(t, [
            { id: 1, v: 'v5', version: 5 },
            { id: 1, v: 'v9', version: 9 },
            { id: 1, v: 'v7', version: 7 },
        ], '?mode=upsert&orderBy=version'), 'orderBy');
        assert(o.updated === 1 && o.duplicates === 2, JSON.stringify(o));
        rows = await rowsOf(t);
        assert(rows.length === 2 && rows[0].v === 'v9', JSON.stringify(rows));
        assert((await upload(t, [{ id: 1 }], '?mode=upsert&orderBy=nope')).status === 400, 'unknown orderBy must be 400');
    });

    // ---------------------------------------------------------------------
    await test('created is preserved and updated is set on replaced rows', async () => {
        const t = T('stamps');
        expectOk(await upload(t, [{ id: 1, name: 'a', created: '2020-01-01T00:00:00', updated: '2020-01-01T00:00:00' }]), 'seed');
        // JSON inference makes ISO strings TIMESTAMP columns.
        const schema = expectOk(await call('GET', `/tables/${t}/schema`), 'schema');
        const types = Object.fromEntries(schema.schema.map(c => [c.column_name, c.data_type]));
        assert(/TIMESTAMP/.test(types.created), `created should be TIMESTAMP, got ${types.created}`);
        expectOk(await call('PUT', `/tables/${t}/keys`, { keys: ['id'] }), 'keys');
        const before = Date.now();
        expectOk(await upload(t, [
            { id: 1, name: 'a2', created: '2030-05-05T00:00:00' },
            { id: 2, name: 'b' },
        ], '?mode=upsert'), 'upsert');
        const rows = await sql(`SELECT id, name, CAST(created AS VARCHAR) AS created, epoch_ms(updated) AS updated_ms FROM "${t}" ORDER BY id`);
        assert(rows.length === 2, JSON.stringify(rows));
        assert(rows[0].name === 'a2' && String(rows[0].created).startsWith('2020-01-01'), `created must be kept: ${JSON.stringify(rows[0])}`);
        assert(Number(rows[0].updated_ms) >= before - 120000, `updated must be set to now: ${JSON.stringify(rows[0])}`);
        assert(rows[1].created !== null && rows[1].updated_ms !== null, `new row gets created/updated: ${JSON.stringify(rows[1])}`);
    });

    // ---------------------------------------------------------------------
    await test('schema evolution during an upsert adds new columns', async () => {
        const t = T('evolve');
        expectOk(await upload(t, [{ id: 1, name: 'a' }], '?mode=upsert&key=id'), 'create');
        const r = expectOk(await upload(t, [{ id: 1, name: 'a2', email: 'a@x', tags: { tier: 'gold' } }, { id: 2, name: 'b', email: 'b@x' }], '?mode=upsert'), 'upsert');
        assert(r.inserted === 1 && r.updated === 1, JSON.stringify(r));
        const rows = await rowsOf(t);
        assert(rows.length === 2 && rows[0].email === 'a@x' && rows[1].email === 'b@x', JSON.stringify(rows));
        const tier = await sql(`SELECT tags.tier AS tier FROM "${t}" WHERE id = 1`);
        assert(tier[0]?.tier === 'gold', JSON.stringify(tier));
    });

    // ---------------------------------------------------------------------
    await test('dedupe inserts only rows whose key does not exist yet', async () => {
        const t = T('dedupe');
        expectOk(await upload(t, [{ id: 1, v: 'orig' }], '?mode=dedupe&key=id'), 'create');
        const r = expectOk(await upload(t, [{ id: 1, v: 'new' }, { id: 2, v: 'first' }, { id: 2, v: 'second' }], '?mode=dedupe'), 'dedupe');
        assert(r.mode === 'dedupe' && r.inserted === 1 && r.skipped === 1 && r.duplicates === 1, JSON.stringify(r));
        const rows = await rowsOf(t);
        assert(rows.length === 2 && rows[0].v === 'orig' && rows[1].v === 'first', JSON.stringify(rows));
    });

    // ---------------------------------------------------------------------
    await test('missing or NULL key column -> 400, nothing written', async () => {
        const t = T('missingkey');
        expectOk(await upload(t, [{ id: 1, name: 'a' }], '?mode=upsert&key=id'), 'create');
        const missing = await upload(t, [{ name: 'no id' }], '?mode=upsert');
        assert(missing.status === 400 && /Key column "id" is missing/.test(missing.json?.error), JSON.stringify(missing));
        const nulls = await upload(t, [{ id: 2, name: 'ok' }, { id: null, name: 'null id' }], '?mode=upsert');
        assert(nulls.status === 400 && /NULL/.test(nulls.json?.error), JSON.stringify(nulls));
        const noKeysNew = await upload(T('nokeys'), [{ id: 1 }], '?mode=upsert');
        assert(noKeysNew.status === 400, `new table without ?key must be 400: ${noKeysNew.status}`);
        const badMode = await upload(t, [{ id: 1 }], '?mode=merge');
        assert(badMode.status === 400, `invalid mode must be 400: ${badMode.status}`);
        const rows = await rowsOf(t);
        assert(rows.length === 1, `nothing must be written: ${JSON.stringify(rows)}`);
    });

    // ---------------------------------------------------------------------
    await test('/kafka-sink?mode=upsert keys on a message field', async () => {
        const t = T('sink');
        const first = await call('POST', `/kafka-sink?topic=${t}&mode=upsert&key=device`, [
            { key: 'k1', value: { device: 'd1', temp: 1 } },
            { key: 'k2', value: { device: 'd2', temp: 2 } },
        ]);
        expectOk(first, 'sink create');
        const r = expectOk(await call('POST', `/kafka-sink?topic=${t}&mode=upsert`, [{ key: 'k3', value: { device: 'd1', temp: 9 } }]), 'sink upsert');
        assert(r.updated === 1 && r.inserted === 0, JSON.stringify(r));
        const rows = await rowsOf(t, 'device');
        assert(rows.length === 2 && Number(rows[0].temp) === 9 && rows[0]._kafka_key === 'k3', JSON.stringify(rows));
    });

    // ---------------------------------------------------------------------
    await test('RBAC: a reader gets 403 for upserts and PUT keys', async () => {
        const t = T('rbac');
        expectOk(await upload(t, [{ id: 1, v: 'a' }], '?mode=upsert&key=id'), 'create');
        const created = await call('POST', '/admin/users', { apiKey: READER_KEY, name: 'Upsert Reader', roles: ['reader'] });
        assert(created.json?.success, `create reader: ${JSON.stringify(created.json)}`);
        const read = await call('GET', `/tables/${t}/keys`, undefined, READER_KEY);
        assert(read.status === 200, `reader can read keys: ${read.status}`);
        const up = await upload(t, [{ id: 1, v: 'hacked' }], '?mode=upsert', READER_KEY);
        assert(up.status === 403, `reader upsert: expected 403, got ${up.status}`);
        const dd = await upload(t, [{ id: 5, v: 'x' }], '?mode=dedupe', READER_KEY);
        assert(dd.status === 403, `reader dedupe: expected 403, got ${dd.status}`);
        const put = await call('PUT', `/tables/${t}/keys`, { keys: ['v'] }, READER_KEY);
        assert(put.status === 403, `reader PUT keys: expected 403, got ${put.status}`);
        const rows = await rowsOf(t);
        assert(rows.length === 1 && rows[0].v === 'a', JSON.stringify(rows));
        await call('DELETE', `/admin/users/${encodeURIComponent(READER_KEY)}`);
    });

    // ---------------------------------------------------------------------
    await test('concurrent upserts to the same table end consistent', async () => {
        const t = T('concurrent');
        expectOk(await upload(t, [{ id: 0, writer: -1, seq: 0 }], '?mode=upsert&key=id'), 'create');
        const WRITERS = 8;
        const KEYS = 20;
        // Every writer upserts all keys 0..KEYS-1 (heavy overlap) plus one
        // private key, so the final table must have KEYS + WRITERS rows.
        const results = await Promise.all(Array.from({ length: WRITERS }, (_, w) => {
            const rows = Array.from({ length: KEYS }, (_, k) => ({ id: k, writer: w, seq: k }));
            rows.push({ id: 1000 + w, writer: w, seq: -1 });
            return upload(t, rows, '?mode=upsert');
        }));
        const ok = results.filter(r => r.status === 200);
        const conflicts = results.filter(r => r.status === 409);
        const other = results.filter(r => r.status !== 200 && r.status !== 409);
        assert(other.length === 0, `unexpected responses: ${JSON.stringify(other.map(r => [r.status, r.json]))}`);
        assert(ok.length >= 1, 'at least one writer must succeed');
        const rows = await sql(`SELECT id, count(*) AS n, min(writer) AS w FROM "${t}" GROUP BY id ORDER BY id`);
        const dupes = rows.filter(r => Number(r.n) !== 1);
        assert(dupes.length === 0, `duplicate keys after concurrent upserts: ${JSON.stringify(dupes)}`);
        const privateIds = new Set(rows.filter(r => Number(r.id) >= 1000).map(r => Number(r.id) - 1000));
        // Exactly the successful writers' private keys exist: no lost
        // batch, and nothing from a failed (409) batch.
        const succeeded = results.map((r, i) => (r.status === 200 ? i : null)).filter(i => i !== null);
        assert(succeeded.every(i => privateIds.has(i)), `lost a successful batch: ok=${succeeded} present=${[...privateIds]}`);
        assert([...privateIds].every(i => succeeded.includes(i)), `a failed batch was partially written: ok=${succeeded} present=${[...privateIds]}`);
        assert(rows.filter(r => Number(r.id) < KEYS).length === KEYS, `expected ${KEYS} shared keys`);
        // Each shared key holds exactly one successful writer's version, and
        // all rows of a winning batch are consistent per key.
        const shared = await sql(`SELECT DISTINCT writer FROM "${t}" WHERE id < ${KEYS}`);
        assert(shared.every(r => succeeded.includes(Number(r.writer))), `shared keys written by a failed batch: ${JSON.stringify(shared)}`);
        console.log(`      (${ok.length} succeeded, ${conflicts.length} reported 409 after retries)`);
    });

    // ---------------------------------------------------------------------
    await test('concurrent dedupe of the same new keys never inserts a key twice', async () => {
        const t = T('concurrent_dedupe');
        expectOk(await upload(t, [{ id: -1 }], '?mode=dedupe&key=id'), 'create');
        const results = await Promise.all(Array.from({ length: 6 }, (_, w) =>
            upload(t, Array.from({ length: 10 }, (_, k) => ({ id: k, writer: w })), '?mode=dedupe')));
        assert(results.every(r => r.status === 200 || r.status === 409), JSON.stringify(results.map(r => r.status)));
        const inserted = results.filter(r => r.status === 200).reduce((n, r) => n + r.json.inserted, 0);
        const rows = await sql(`SELECT id, count(*) AS n FROM "${t}" GROUP BY id HAVING count(*) > 1`);
        assert(rows.length === 0, `duplicate keys: ${JSON.stringify(rows)}`);
        assert(inserted === 10, `exactly 10 new keys must be inserted in total, got ${inserted}`);
    });

    console.log(`\n${passed} passed, ${failed} failed\n`);
    if (failed > 0) process.exit(1);
}

main().catch((err) => {
    console.error(err);
    process.exit(1);
});
