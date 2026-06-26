import { Kafka, CompressionTypes } from 'kafkajs';
import { withConnection, VARCHAR, TIMESTAMP, TIMESTAMPTZ, TIMESTAMP_S, TIMESTAMP_MS, TIMESTAMP_NS, DATE } from './db.js';

let producer = null;
let kafkaInstance = null;

// Size-aware batching limits. Broker default `message.max.bytes` is 1 MiB; we
// stay just below that by default so an unconfigured broker still works.
const MAX_MESSAGE_BYTES = parseInt(process.env.KAFKA_MAX_MESSAGE_BYTES || '950000', 10);
const MAX_BATCH_BYTES = parseInt(process.env.KAFKA_MAX_BATCH_BYTES || '950000', 10);
const MAX_BATCH_COUNT = parseInt(process.env.KAFKA_BATCH_SIZE || '100', 10);
const MAX_REQUEST_SIZE = parseInt(process.env.KAFKA_MAX_REQUEST_SIZE || '10485760', 10);

const COMPRESSION_MAP = {
    none: CompressionTypes.None,
    gzip: CompressionTypes.GZIP,
    snappy: CompressionTypes.Snappy,
    lz4: CompressionTypes.LZ4,
    zstd: CompressionTypes.ZSTD,
};
const COMPRESSION = COMPRESSION_MAP[(process.env.KAFKA_PRODUCER_COMPRESSION || 'gzip').toLowerCase()] ?? CompressionTypes.GZIP;

// Sentinel header attached to every message produced by publishTableToTopic().
// The internal ingestion consumer (src/kafka-consumer.js) inspects this and
// drops the message, breaking the SSE-republish -> internal-consume feedback
// loop that would otherwise re-insert a table's own rows back into itself
// every time a client subscribes to /kafka-subscribe/<topic>.
const SOURCE_HEADER_KEY = 'x-ichibi-lake-source';
const SOURCE_HEADER_VALUE = 'sse-republish';

function sanitizeTableName(topicName) {
    return topicName.replace(/[^a-zA-Z0-9_]/g, '_').replace(/^(\d)/, '_$1');
}

// Shared kafkajs Kafka client. Reused for the producer below and for the
// per-request admin + consumer instances created by the SSE subscribe
// handler in src/index.js, so we don't pay connection-pool / metadata-loop
// overhead per HTTP request.
export function getKafka() {
    if (kafkaInstance) return kafkaInstance;
    const brokers = process.env.KAFKA_BROKERS ? process.env.KAFKA_BROKERS.split(',') : [];
    if (brokers.length === 0) return null;
    kafkaInstance = new Kafka({
        clientId: process.env.KAFKA_CLIENT_ID || 'ichibi-lake-sse',
        brokers,
        retry: {
            initialRetryTime: 500,
            retries: 15,
            maxRetryTime: 30000,
        },
    });
    return kafkaInstance;
}

async function getProducer() {
    if (producer) return producer;
    const kafka = getKafka();
    if (!kafka) return null;
    producer = kafka.producer({
        maxRequestSize: MAX_REQUEST_SIZE,
        connectionTimeout: 10000,
        requestTimeout: 30000,
    });
    await producer.connect();
    return producer;
}

function messageBytes(msg) {
    return Buffer.byteLength(msg.key, 'utf8') + Buffer.byteLength(msg.value, 'utf8');
}

export async function publishTableToTopic(topicName, options = {}) {
    const { isCancelled = () => false, since = null, sinceColumn = null } = options;

    const p = await getProducer();
    if (!p || isCancelled()) return { count: 0, skipped: 0 };

    const tableName = sanitizeTableName(topicName);

    return withConnection(async (conn) => {
        const tableResult = await conn.run(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = 'main' AND table_name = ?`,
            [tableName],
            [VARCHAR]
        );
        const tableRows = await tableResult.getRows();
        if (tableRows.length === 0) return { count: 0, skipped: 0 };

        const quoted = `"${tableName.replace(/"/g, '""')}"`;

        // Cast TIMESTAMP/DATE columns to VARCHAR so the DuckDB driver returns
        // strings instead of broken Date objects (the driver treats internal
        // epoch microseconds as milliseconds, producing +058412-… dates).
        const colResult = await conn.run(
            `SELECT column_name, data_type FROM information_schema.columns WHERE table_schema = 'main' AND table_name = ?`,
            [tableName],
            [VARCHAR]
        );
        const colRows = await colResult.getRows();
        const selectCols = colRows.map(([col, type]) => {
            const safeCol = col.replace(/"/g, '""');
            if (type.toUpperCase().startsWith('TIMESTAMP') || type.toUpperCase().startsWith('DATE')) {
                return `CAST("${safeCol}" AS VARCHAR) AS "${safeCol}"`;
            }
            return `"${safeCol}"`;
        });
        let sql = `SELECT ${selectCols.join(', ')} FROM ${quoted}`;
        const params = [];
        const types = [];

        if (since) {
            const filterColumn = sinceColumn || 'updated';
            const safeCol = filterColumn.replace(/"/g, '""');

            let sinceTs;
            if (/^\d+$/.test(since)) {
                sinceTs = new Date(Date.now() - Number(since) * 60000).toISOString();
            } else if (/^(\d+)(m|h|d)$/i.test(since)) {
                const match = since.match(/^(\d+)(m|h|d)$/i);
                const amount = Number(match[1]);
                const unit = match[2].toLowerCase();
                const ms = { m: 60000, h: 3600000, d: 86400000 }[unit] * amount;
                sinceTs = new Date(Date.now() - ms).toISOString();
            } else {
                sinceTs = new Date(since).toISOString();
            }
            sql += ` WHERE "${safeCol}" >= ? ORDER BY "${safeCol}" ASC`;
            params.push(sinceTs);
        }

        const result = await conn.run(sql, params, types);
        const columns = result.columnNames();

        let count = 0;
        let skipped = 0;
        let batch = [];
        let batchBytes = 0;
        let rowIndex = 0;

        const flush = async () => {
            if (batch.length === 0) return;
            try {
                await p.send({ topic: topicName, compression: COMPRESSION, messages: batch });
            } catch (err) {
                if (/write after end|Connection error|ECONNRESET|ENOTCONNECTED/i.test(err.message || '')) {
                    console.warn(`[kafka-producer] Producer send failed (${err.message}). Resetting producer connection.`);
                    await disconnectProducer();
                }
                throw err;
            }
            batch = [];
            batchBytes = 0;
        };

        while (true) {
            if (isCancelled()) return { count, skipped, cancelled: true };
            const chunk = await result.fetchChunk();
            if (!chunk || chunk.rowCount === 0) break;
            const chunkRows = chunk.getRows();

            for (let i = 0; i < chunkRows.length; i++) {
                const row = chunkRows[i];
                const obj = {};
                for (let c = 0; c < columns.length; c++) {
                    const val = row[c];
                    if (val === null || val === undefined) {
                        obj[columns[c]] = null;
                    } else if (typeof val === 'object' && val.constructor && val.constructor.name === 'DuckDBBlobValue' && val.bytes) {
                        obj[columns[c]] = Buffer.from(val.bytes).toString('base64');
                    } else if (val instanceof Uint8Array || Buffer.isBuffer(val)) {
                        obj[columns[c]] = Buffer.from(val).toString('base64');
                    } else {
                        obj[columns[c]] = val;
                    }
                }

                const msg = {
                    key: String(rowIndex),
                    value: JSON.stringify(obj),
                    headers: { [SOURCE_HEADER_KEY]: SOURCE_HEADER_VALUE },
                };
                const msgBytes = messageBytes(msg);

                if (msgBytes > MAX_MESSAGE_BYTES) {
                    console.warn(`[kafka-producer] Skipping oversized row ${rowIndex} in "${tableName}" (${msgBytes} bytes > ${MAX_MESSAGE_BYTES})`);
                    skipped++;
                    rowIndex++;
                    continue;
                }

                if (batch.length > 0 && (batchBytes + msgBytes > MAX_BATCH_BYTES || batch.length >= MAX_BATCH_COUNT)) {
                    if (isCancelled()) return { count, skipped, cancelled: true };
                    await flush();
                }

                batch.push(msg);
                batchBytes += msgBytes;
                count++;
                rowIndex++;
            }
        }

        if (isCancelled()) return { count, skipped, cancelled: true };
        await flush();

        return { count, skipped };
    });
}

export async function disconnectProducer() {
    if (producer) {
        await producer.disconnect();
        producer = null;
    }
}