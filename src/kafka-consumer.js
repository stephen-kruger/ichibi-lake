import { Kafka } from 'kafkajs';
import { recordKafkaLag } from './metrics.js';
import { ensureTableAcl } from './acl.js';
import { ingestRows, describeIngest, INGEST_MODES } from './ingest.js';

// Store batches in memory
const BATCH_SIZE = parseInt(process.env.KAFKA_BATCH_SIZE || '100');
const BATCH_TIMEOUT = parseInt(process.env.KAFKA_BATCH_TIMEOUT_MS || '5000');
let batches = {}; // Map of topic -> array of messages
let timeouts = {}; // Map of topic -> timeout ID

// Mirrors the header set by src/kafka-producer.js publishTableToTopic().
// Messages bearing this header are produced by the gateway itself when an SSE
// client subscribes, and must not be re-ingested or a feedback loop forms
// (table -> kafka -> table -> kafka ...).
const SOURCE_HEADER_KEY = 'x-ichibi-lake-source';
const SOURCE_HEADER_VALUE_SKIP = 'sse-republish';

/**
 * Build the topic subscription regex from a user-supplied pattern, always
 * excluding Kafka's internal "__"-prefixed topics (e.g. __consumer_offsets,
 * __transaction_state).
 *
 * A bare ".*" matches them, which makes the consumer get assigned every
 * __consumer_offsets partition — including the one holding this very group's
 * commits. With autocommit on, it then reads that partition, commits a new
 * offset, which appends to the same partition, which it reads again: a
 * self-amplifying loop that bloats __consumer_offsets and pegs CPU on both
 * this process and the broker even with zero clients. The negative lookahead
 * is applied on top of the user's pattern so the guard holds no matter what
 * KAFKA_TOPIC_REGEX is set to.
 */
export function buildTopicSubscriptionRegex(userPattern = '.*') {
    return new RegExp(`^(?!__)(?:${userPattern})`);
}

async function flushBatch(topic) {
    if (!batches[topic] || batches[topic].length === 0) return;

    // Copy and clear the batch
    const dataToInsert = [...batches[topic]];
    batches[topic] = [];

    const tableName = topic.replace(/[^a-zA-Z0-9_]/g, '_'); // Sanitize topic name
    // "_"-prefixed names are reserved for system tables (RBAC users, ACLs,
    // graph registry, ...); never ingest a topic into one of them.
    if (tableName.startsWith('_')) {
        console.warn(`[Kafka] Skipping ${dataToInsert.length} message(s) from topic "${topic}": table names starting with "_" are reserved.`);
        return;
    }

    const { mode, keys } = topicIngestMode(topic);
    try {
        const result = await ingestRows(tableName, dataToInsert, {
            mode,
            keys,
            keysAreDefault: true,
            tempPrefix: 'kafka_temp',
            updatedBy: 'kafka-internal',
            onCreated: () => ensureTableAcl(tableName, 'kafka-internal'),
        });
        if (result.created) {
            console.log(`[Kafka] Created table ${tableName}: ${describeIngest(result)}.`);
        } else {
            console.log(`[Kafka] ${describeIngest(result)}. First record: ${JSON.stringify(dataToInsert[0])}`);
        }
    } catch (e) {
        console.error(`[Kafka Consumer Error] Failed writing ${dataToInsert.length} message(s) (mode=${mode}) to ${tableName}:`, e.message || e);
    }
}

/**
 * Parse KAFKA_INGEST_MODES: comma-separated `topic=mode[:key1+key2]` entries,
 * e.g. `customers=upsert:customer_id,devices=dedupe:device_id+region`.
 * `topic` is the (sanitised) table name; `*` sets the default for every other
 * topic. Keys are only used to declare them when the table has none yet;
 * declared keys (PUT /tables/:tableName/keys) take precedence. Invalid entries
 * are ignored with a warning.
 */
export function parseIngestModes(raw = '') {
    const out = new Map();
    for (const entry of String(raw).split(',').map(s => s.trim()).filter(Boolean)) {
        const m = entry.match(/^([A-Za-z0-9_.*-]+)\s*=\s*([a-z]+)(?::([A-Za-z0-9_+]+))?$/i);
        if (!m || !INGEST_MODES.includes(m[2].toLowerCase())) {
            console.warn(`[Kafka] Ignoring invalid KAFKA_INGEST_MODES entry "${entry}" (expected topic=append|upsert|dedupe[:key1+key2]).`);
            continue;
        }
        const keys = m[3] ? m[3].split('+').filter(Boolean) : null;
        out.set(m[1] === '*' ? '*' : m[1].replace(/[^a-zA-Z0-9_]/g, '_'), { mode: m[2].toLowerCase(), keys });
    }
    return out;
}

/** Ingestion mode for a topic. Read on use so the console can change it live. */
export function topicIngestMode(topic) {
    const modes = parseIngestModes(process.env.KAFKA_INGEST_MODES || '');
    const tableName = topic.replace(/[^a-zA-Z0-9_]/g, '_');
    return modes.get(tableName) || modes.get('*') || { mode: 'append', keys: null };
}

export async function startKafkaConsumer() {
    const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : [];
    if (brokers.length === 0) {
        console.log('[Kafka] KAFKA_BROKERS not set. Consumer will not be started.');
        return;
    }

    const clientId = process.env.KAFKA_CLIENT_ID || 'ducklake-consumer';
    const groupId = process.env.KAFKA_GROUP_ID || 'ducklake-ingestion-group';
    // Build the subscription regex, always excluding Kafka's internal
    // "__"-prefixed topics. See buildTopicSubscriptionRegex() for why.
    const topicRegex = buildTopicSubscriptionRegex(process.env.KAFKA_TOPIC_REGEX || '.*');
    // How often kafkajs re-fetches cluster metadata. This also gates how
    // quickly a regex-subscribed consumer picks up newly-created topics
    // (kafkajs default is 5 minutes, which is far too slow for the dynamic
    // topic creation pattern used by /kafka-subscribe and /kafka-sink).
    const metadataMaxAge = parseInt(process.env.KAFKA_METADATA_MAX_AGE_MS || '10000', 10);

    const kafka = new Kafka({ clientId, brokers });
    const consumer = kafka.consumer({ groupId, metadataMaxAge });

    await consumer.connect();
    console.log(`[Kafka] Consumer connected to ${brokers.join(', ')}`);

    await consumer.subscribe({ topic: topicRegex, fromBeginning: false });

    // Per-partition lag for GET /metrics; kafkajs reports it after every batch,
    // so this costs no extra broker round trips.
    consumer.on(consumer.events.END_BATCH_PROCESS, ({ payload }) => {
        recordKafkaLag(payload.topic, payload.partition, payload.offsetLag);
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            if (topic.startsWith('__')) return;

            // Skip messages republished by the SSE producer to break the
            // table -> kafka -> table feedback loop.
            const sourceHdr = message.headers && message.headers[SOURCE_HEADER_KEY];
            if (sourceHdr && sourceHdr.toString() === SOURCE_HEADER_VALUE_SKIP) {
                return;
            }

            try {
                // Parse the message value, assume JSON for now
                const valueStr = message.value.toString();
                const jsonValue = JSON.parse(valueStr);

                if (!batches[topic]) {
                    batches[topic] = [];
                }

                // Normalize the record to avoid type conflicts with existing tables.
                // _ingest_timestamp and _kafka_key are system-managed columns; we
                // always override them so that DuckDB never sees unexpected structs
                // (e.g. {micros: VARCHAR}) that cannot be cast to TIMESTAMP.
                const record = { ...jsonValue };
                record._ingest_timestamp = new Date().toISOString();
                if (message.key != null) {
                    record._kafka_key = message.key.toString();
                }
                batches[topic].push(record);

                if (batches[topic].length >= BATCH_SIZE) {
                    if (timeouts[topic]) {
                        clearTimeout(timeouts[topic]);
                        timeouts[topic] = null;
                    }
                    await flushBatch(topic);
                } else if (!timeouts[topic]) {
                    timeouts[topic] = setTimeout(() => {
                        timeouts[topic] = null;
                        flushBatch(topic);
                    }, BATCH_TIMEOUT);
                }
            } catch (err) {
                console.error(`[Kafka] Failed parsing message on topic ${topic}:`, err);
            }
        },
    });
}
