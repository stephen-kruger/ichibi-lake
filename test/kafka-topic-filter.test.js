/**
 * Unit tests for buildTopicSubscriptionRegex() in src/kafka-consumer.js.
 *
 * Pure in-process tests — no broker required. Guards the regression where a
 * ".*" subscription matched Kafka's internal __consumer_offsets topic, so the
 * consumer was assigned the partition holding its own offset commits. With
 * autocommit on, reading that partition triggered a commit, which appended to
 * the same partition, which was read again — a self-amplifying loop that
 * bloated __consumer_offsets and pegged CPU on both the gateway and the broker
 * with zero clients connected.
 */

import assert from 'node:assert/strict';
import { buildTopicSubscriptionRegex } from '../src/kafka-consumer.js';

let passed = 0, failed = 0;

function test(name, fn) {
    try {
        fn();
        passed++;
        console.log(`  \u2713 ${name}`);
    } catch (err) {
        failed++;
        console.error(`  \u2717 ${name}`);
        console.error(`    ${err.message}`);
    }
}

const INTERNAL_TOPICS = ['__consumer_offsets', '__transaction_state', '__cluster_metadata'];

console.log('\n\u25b8 kafka-topic-filter: internal topics are never subscribed');

test('".*" matches application topics but excludes internal ones', () => {
    const re = buildTopicSubscriptionRegex('.*');
    assert.ok(re.test('kafka_sse_test_1782481054956'));
    assert.ok(re.test('ichibi_table'));
    for (const t of INTERNAL_TOPICS) {
        assert.equal(re.test(t), false, `should not match ${t}`);
    }
});

test('default (no argument) behaves like ".*" and still excludes internal topics', () => {
    const re = buildTopicSubscriptionRegex();
    assert.ok(re.test('events_login'));
    for (const t of INTERNAL_TOPICS) {
        assert.equal(re.test(t), false, `should not match ${t}`);
    }
});

test('a custom prefix pattern keeps its semantics and excludes internal topics', () => {
    const re = buildTopicSubscriptionRegex('events_.*');
    assert.ok(re.test('events_login'));
    assert.equal(re.test('orders_created'), false);
    assert.equal(re.test('kafka_sse_test_1'), false);
    for (const t of INTERNAL_TOPICS) {
        assert.equal(re.test(t), false, `should not match ${t}`);
    }
});

test('an anchored pattern still matches its target and excludes internal topics', () => {
    const re = buildTopicSubscriptionRegex('^foo$');
    assert.ok(re.test('foo'));
    assert.equal(re.test('foobar'), false);
    for (const t of INTERNAL_TOPICS) {
        assert.equal(re.test(t), false, `should not match ${t}`);
    }
});

test('an alternation pattern is grouped correctly and excludes internal topics', () => {
    const re = buildTopicSubscriptionRegex('a|b');
    assert.ok(re.test('a'));
    assert.ok(re.test('b'));
    for (const t of INTERNAL_TOPICS) {
        assert.equal(re.test(t), false, `should not match ${t}`);
    }
});

test('a topic that merely contains "__" (not a prefix) is still allowed', () => {
    const re = buildTopicSubscriptionRegex('.*');
    assert.ok(re.test('my__topic'));
    assert.ok(re.test('a__b'));
});

console.log('\n\u2500\u2500\u2500 kafka-topic-filter unit tests \u2500\u2500\u2500');
console.log(`  passed: ${passed}`);
console.log(`  failed: ${failed}`);
process.exit(failed === 0 ? 0 : 1);
