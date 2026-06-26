import { Kafka } from 'kafkajs';
import { withConnection } from './db.js';
import { resolveTypeConflict, formatSchemaDefinition } from './schema-evolution.js';
import { ensureTableAcl } from './acl.js';
import { randomUUID } from 'crypto';
import fs from 'fs/promises';
import path from 'path';

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

    const tempFileName = `kafka_temp_${randomUUID()}.json`;
    const tempFilePath = path.join(process.cwd(), tempFileName);

    try {
        await fs.writeFile(tempFilePath, JSON.stringify(dataToInsert));

        // withConnection guarantees the DuckDB connection is closed in a
        // finally, avoiding a per-batch native-handle leak (previous code
        // called getConnection() and never released it).
        await withConnection(async (connection) => {
            // Check whether the target table already exists. `getRows()` returns a
            // Promise in @duckdb/node-api; forgetting to await it (as the previous
            // implementation did) meant `tableExists` was always false, so every
            // batch after the first hit "Catalog Error: Table ... already exists!".
            const checkSql = `SELECT count(*) FROM information_schema.tables WHERE table_name = '${tableName}'`;
            const checkResult = await connection.run(checkSql);
            const checkRows = await checkResult.getRows();
            const countVal = checkRows[0] && checkRows[0][0] != null ? checkRows[0][0] : 0;
            const tableExists = Number(countVal) > 0;

            const readOptions = 'sample_size=-1';

            try {
                if (!tableExists) {
                    await connection.run(`CREATE TABLE ${tableName} AS SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                    await ensureTableAcl(tableName, 'kafka-internal');
                    console.log(`[Kafka] Created table ${tableName} and inserted ${dataToInsert.length} records.`);
                    return;
                }
            } catch (createErr) {
                // Defensive fallback: if a concurrent flush (e.g. multi-partition
                // topic) raced us to CREATE TABLE, treat the table as existing
                // and proceed with the safe INSERT path below.
                if (!tableExists && /already exists/i.test(createErr.message || '')) {
                    console.log(`[Kafka] Race resolved for ${tableName}, falling back to INSERT.`);
                    tableExists = true;
                } else {
                    throw createErr;
                }
            }

            if (tableExists) {
                // Schema Evolution: add any columns present in the incoming
                // messages that the existing target table does not yet have.
                // For STRUCT columns with different shapes, widen the column
                // type to the union of both shapes so DuckDB can cast safely.
                const existingResult = await connection.run(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName}'`);
                const existingRows = await existingResult.getRows();
                const existingCols = new Map(existingRows.map(r => [r[0], r[1]]));

                const newDataResult = await connection.run(`DESCRIBE SELECT * FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                const newDataRows = await newDataResult.getRows();
                const newDataCols = newDataRows.map(r => ({ name: r[0], type: r[1] }));

                const selectColumns = [];
                let schemaChanged = false;
                for (const col of newDataCols) {
                    const existingType = existingCols.get(col.name);
                    if (existingType) {
                        const resolution = resolveTypeConflict(existingType, col.type);
                        if (resolution.action === 'skip') {
                            console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ${resolution.reason}`);
                            continue;
                        }
                        if (resolution.action === 'widen') {
                            console.log(`[Schema Evolution] Widening column "${col.name}" in ${tableName} from ${existingType} to ${resolution.newType}`);
                            try {
                                await connection.run(`ALTER TABLE ${tableName} ALTER COLUMN "${col.name}" SET DATA TYPE ${resolution.newType}`);
                                schemaChanged = true;
                            } catch (alterErr) {
                                console.warn(`[Schema Evolution] Skipping column "${col.name}" in ${tableName} for this batch: ALTER failed (${alterErr.message || alterErr})`);
                                continue;
                            }
                        }
                    } else {
                        // Brand-new column – add it to the table first.
                        console.log(`[Schema Evolution] Adding column ${col.name} (${col.type}) to ${tableName}`);
                        try {
                            await connection.run(`ALTER TABLE ${tableName} ADD COLUMN "${col.name}" ${col.type}`);
                            schemaChanged = true;
                        } catch (addErr) {
                            console.warn(`[Schema Evolution] Skipping new column "${col.name}" in ${tableName} for this batch: ADD COLUMN failed (${addErr.message || addErr})`);
                            continue;
                        }
                    }
                    selectColumns.push(`"${col.name}"`);
                }

                if (schemaChanged) {
                    const schemaResult = await connection.run(`SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '${tableName}' ORDER BY ordinal_position`);
                    const schemaRows = await schemaResult.getRows();
                    const columns = schemaRows.map(r => ({ column_name: r[0], data_type: r[1] }));
                    console.log(`[Schema Evolution] New schema for ${tableName}:\n${formatSchemaDefinition(tableName, columns)}`);
                }

                await connection.run(`INSERT INTO ${tableName} BY NAME SELECT ${selectColumns.join(', ')} FROM read_json_auto('${tempFilePath}', ${readOptions})`);
                console.log(`[Kafka] Appended ${dataToInsert.length} records into ${tableName}. First record: ${JSON.stringify(dataToInsert[0])}`);
            }
        });
    } catch (e) {
        console.error(`[Kafka Consumer Error] Failed inserting to ${tableName}:`, e);
    } finally {
        fs.unlink(tempFilePath).catch(err => console.error("Could not remove temp file", err));
    }
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
