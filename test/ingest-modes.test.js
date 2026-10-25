/**
 * Unit tests for the key-based ingestion option parsers:
 * parseIngestMode() (src/ingest.js), normaliseKeyList() /
 * assertKeysMatchColumns() (src/table-keys.js) and the Kafka consumer's
 * per-topic KAFKA_INGEST_MODES setting (src/kafka-consumer.js).
 *
 * Pure in-process tests — no database or broker required.
 */

import assert from 'node:assert/strict';
import { parseIngestMode } from '../src/ingest.js';
import { normaliseKeyList, assertKeysMatchColumns } from '../src/table-keys.js';
import { parseIngestModes, topicIngestMode } from '../src/kafka-consumer.js';

let passed = 0, failed = 0;

function test(name, fn) {
    try {
        fn();
        console.log(`  ✔ ${name}`);
        passed++;
    } catch (err) {
        console.log(`  ✘ ${name}\n      ${err.message}`);
        failed++;
    }
}

const status = (fn) => { try { fn(); return 200; } catch (e) { return e.statusCode; } };

console.log('\ningest mode / key parsing\n');

test('parseIngestMode defaults to append and validates', () => {
    assert.equal(parseIngestMode(undefined), 'append');
    assert.equal(parseIngestMode(''), 'append');
    assert.equal(parseIngestMode('UPSERT'), 'upsert');
    assert.equal(parseIngestMode('dedupe'), 'dedupe');
    assert.equal(status(() => parseIngestMode('merge')), 400);
});

test('normaliseKeyList accepts arrays and comma lists, rejects unsafe names', () => {
    assert.deepEqual(normaliseKeyList(['id']), ['id']);
    assert.deepEqual(normaliseKeyList('region, device,region'), ['region', 'device']);
    assert.deepEqual(normaliseKeyList([]), []);
    assert.equal(status(() => normaliseKeyList(['id"; DROP'])), 400);
    assert.equal(status(() => normaliseKeyList({ id: 1 })), 400);
    assert.equal(status(() => normaliseKeyList([1])), 400);
});

test('assertKeysMatchColumns rejects missing and unkeyable columns', () => {
    const cols = new Map([['id', 'BIGINT'], ['meta', 'STRUCT(a INTEGER)'], ['tags', 'VARCHAR[]'], ['b', 'BLOB'], ['ts', 'TIMESTAMP']]);
    assert.equal(status(() => assertKeysMatchColumns('t', ['id', 'ts'], cols)), 200);
    assert.equal(status(() => assertKeysMatchColumns('t', ['nope'], cols)), 400);
    for (const k of ['meta', 'tags', 'b']) assert.equal(status(() => assertKeysMatchColumns('t', [k], cols)), 400, k);
});

test('KAFKA_INGEST_MODES parses topic=mode[:keys] entries', () => {
    const m = parseIngestModes('customers=upsert:customer_id, devices=DEDUPE:device_id+region, my-topic=upsert, *=append, bad, x=merge');
    assert.deepEqual(m.get('customers'), { mode: 'upsert', keys: ['customer_id'] });
    assert.deepEqual(m.get('devices'), { mode: 'dedupe', keys: ['device_id', 'region'] });
    assert.deepEqual(m.get('my_topic'), { mode: 'upsert', keys: null });
    assert.deepEqual(m.get('*'), { mode: 'append', keys: null });
    assert.equal(m.has('bad'), false);
    assert.equal(m.has('x'), false);
});

test('topicIngestMode falls back to * and then append', () => {
    const saved = process.env.KAFKA_INGEST_MODES;
    try {
        process.env.KAFKA_INGEST_MODES = 'customers=upsert:id';
        assert.deepEqual(topicIngestMode('customers'), { mode: 'upsert', keys: ['id'] });
        assert.deepEqual(topicIngestMode('other'), { mode: 'append', keys: null });
        process.env.KAFKA_INGEST_MODES = 'customers=upsert:id,*=dedupe:event_id';
        assert.deepEqual(topicIngestMode('other'), { mode: 'dedupe', keys: ['event_id'] });
    } finally {
        if (saved === undefined) delete process.env.KAFKA_INGEST_MODES; else process.env.KAFKA_INGEST_MODES = saved;
    }
});

console.log(`\n${passed} passed, ${failed} failed\n`);
if (failed > 0) process.exit(1);
