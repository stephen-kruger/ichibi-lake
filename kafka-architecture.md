# Kafka Architecture

## Overview

The gateway integrates Apache Kafka in two ways:

1. **Internal consumer** (`src/kafka-consumer.js`) — background process that subscribes to all topics, batches messages, and ingests them into DuckDB tables.
2. **SSE subscription endpoint** (`GET /kafka-subscribe/:topicName`) — per-request consumer that streams Kafka messages to HTTP clients via Server-Sent Events.

A **producer layer** (`src/kafka-producer.js`) bridges DuckDB → Kafka, enabling the SSE endpoint to replay existing DuckDB data into a Kafka topic on subscribe.

## Components

### 1. Internal Consumer (`src/kafka-consumer.js`)

- Started automatically in `src/index.js` when `KAFKA_BROKERS` is set
- Subscribes to all topics matching `KAFKA_TOPIC_REGEX` (default: `.*`)
- Batches messages in memory, flushes to DuckDB via `read_json_auto` on batch size or timeout
- Skips internal topics (prefix `__`) to avoid parsing `__consumer_offsets` binary data
- Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | (empty) | Comma-separated broker list; if empty, consumer not started |
| `KAFKA_CLIENT_ID` | `ducklake-consumer` | Kafka client ID |
| `KAFKA_GROUP_ID` | `ducklake-ingestion-group` | Consumer group ID |
| `KAFKA_TOPIC_REGEX` | `.*` | Regex to match topics to subscribe to |
| `KAFKA_BATCH_SIZE` | `100` | Messages per batch flush |
| `KAFKA_BATCH_TIMEOUT_MS` | `5000` | Max wait before flushing a batch |
| `KAFKA_METADATA_MAX_AGE_MS` | `10000` | How often kafkajs refreshes cluster metadata. Also controls how quickly the regex-subscribed consumer picks up newly-created topics (kafkajs default is 5 minutes). |

### 2. SSE Subscription Endpoint (`GET /kafka-subscribe/:topicName`)

- Creates a **per-request Kafka consumer** that streams events to the client
- Reuses a single module-scoped `Kafka` client (`getKafka()` in `kafka-producer.js`) — one connection pool / metadata loop, not one-per-request
- Uses the admin API (`admin.createTopics`) to ensure the topic exists before subscribing
- Polls metadata until the partition leader is available (up to 30 retries, 1s apart)
- Calls `publishTableToTopic()` asynchronously to replay DuckDB rows into the topic; cancelled via `isCancelled` if the client disconnects mid-dump
- Sends a `dump-complete` SSE event after the DuckDB dump finishes
- Query parameters:

| Param | Default | Description |
|-------|---------|-------------|
| `since` | (none) | When set, limits the retroactive DuckDB dump to rows newer than this timestamp. Supports ISO 8601, relative durations (`5m`, `1h`, `2d`), or plain minutes. When omitted, all rows are dumped. |
| `sinceColumn` | `updated` | Explicit column name to filter on during the historic dump. Defaults to `updated`. |
| `groupId` | `ichibi-lake-sse-<sanitized-api-key>-<random>` | Per-connection unique by default. Pass an explicit value to share a group across connections. |

#### Disconnect handling

The handler is hardened against client-disconnect leaks (orphan consumers stuck in broker groups, which were the source of broker CPU thrashing):

- A `cancelled` flag is checked after **every** `await` in the startup IIFE (admin connect, `createTopics`, the metadata-poll loop, consumer connect, subscribe). If the client disconnects during any of these, the in-flight admin/consumer is disconnected and the IIFE bails out instead of joining the broker group with no listener.
- `req.on('close')` registers a cleanup handler **before** any kafkajs call, so the live admin/consumer references are always reachable.
- `eachMessage` short-circuits on `res.destroyed` and honours Node stream backpressure (`await once(res, 'drain')` if `res.write` returns `false`). kafkajs awaits the `eachMessage` promise before fetching the next message, so backpressure propagates back to the broker's fetch loop instead of buffering messages in Node memory.
- A 15-second SSE keepalive comment (`:keepalive\n\n`) is emitted so reverse proxies (nginx, Cloudflare) with idle timeouts do not silently drop the TCP connection.
- `sessionTimeout` is `10s` (down from 30s) and `heartbeatInterval` is `3s`, so any orphan consumer that does slip through is evicted from the group much faster.

#### SSE Event Format

```
event: connected
data: {"topic":"<topicName>","groupId":"<groupId>"}

data: {"topic":"...","partition":0,"offset":"0","key":"...","value":"...","timestamp":"..."}

event: dump-complete
data: {"table":"<topicName>","rowsPublished":3}

event: error
data: {"error":"<message>"}
```

### 3. Producer Layer (`src/kafka-producer.js`)

- Singleton Kafka producer connected on first use
- `publishTableToTopic(topicName)`:
  1. Sanitizes the topic name to a valid DuckDB identifier
  2. Checks if a DuckDB table with that name exists in the `main` schema
  3. Reads all rows via `SELECT * FROM` the table
  4. Builds size-aware batches (flushed on `KAFKA_BATCH_SIZE` count or `KAFKA_MAX_BATCH_BYTES` accumulated bytes)
  5. Drops any single row whose serialized JSON exceeds `KAFKA_MAX_MESSAGE_BYTES` (logged as a warning) instead of failing the whole dump
  6. Sends each batch with `KAFKA_PRODUCER_COMPRESSION` compression
  7. Returns `{ count, skipped }` — number of rows published and number skipped for being too large
- Environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_MAX_MESSAGE_BYTES` | `950000` | Max serialized bytes per single Kafka message. Rows exceeding this are skipped. Keep below the broker's `message.max.bytes`. |
| `KAFKA_MAX_BATCH_BYTES` | `950000` | Max accumulated bytes per produce request before forcing a flush. |
| `KAFKA_MAX_REQUEST_SIZE` | `10485760` | `maxRequestSize` passed to the kafkajs producer. Must be ≤ broker `message.max.bytes`. |
| `KAFKA_PRODUCER_COMPRESSION` | `gzip` | One of `none`, `gzip`, `snappy`, `lz4`, `zstd`. `snappy`/`lz4`/`zstd` require registering an external codec in kafkajs. |

> When raising these limits, also raise the broker-side `message.max.bytes` and `replica.fetch.max.bytes` (see Docker Compose Configuration below).

## Data Flow

```
 Client
   |
   | GET /kafka-subscribe/my_topic
   v
 Express Route (src/index.js)
   |
   ├── admin.createTopics("my_topic")
   ├── admin.fetchTopicMetadata("my_topic")  ── wait for leader
   |
   ├── Kafka Consumer (per-request)
   |     └── subscribe("my_topic")
   |     └── run({ eachMessage → SSE })
   |
   └── publishTableToTopic("my_topic")  ── non-blocking
         └── query DuckDB "SELECT * FROM my_topic"
         └── Kafka Producer → topic "my_topic"
               └── Consumer picks up → SSE data: events
               └── dump-complete event

 Kafka Broker
   ├── ← Internal Consumer (background)
   |     └── batches messages
   |     └── flushes to DuckDB table
   |
   └── ← External producers (other services)
```

## Docker Compose Configuration

Both `docker/compose.yaml` and `docker-tailscale/compose.yaml` include:

- **Kafka service**: `apache/kafka:4.3.1` in KRaft mode (single node, no ZooKeeper)
- Two listeners exposed:
  - `PLAINTEXT` on `9092`, advertised as `kafka:9092` — used by the gateway and any other client running inside the docker network.
  - `EXTERNAL` on `9094`, advertised as `localhost:9094` — used by clients running on the docker host (e.g. the node integration tests in `test/test-kafka-sse.js`). Required because kafkajs follows the broker-advertised address, and `kafka` does not resolve outside the docker network.
- Healthcheck via `/opt/kafka/bin/kafka-topics.sh --list`
- KRaft logs bind-mounted to `./data/kafka` (via `KAFKA_LOG_DIRS=/var/lib/kafka/data`) so retention behaviour is observable on the host.
- Environment:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_NODE_ID: 1
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_MESSAGE_MAX_BYTES: 10485760
KAFKA_REPLICA_FETCH_MAX_BYTES: 10485760
KAFKA_LOG_DIRS: /var/lib/kafka/data
KAFKA_LOG_RETENTION_HOURS: ${KAFKA_LOG_RETENTION_HOURS:-168}
KAFKA_LOG_RETENTION_BYTES: ${KAFKA_LOG_RETENTION_BYTES:-1073741824}
KAFKA_LOG_SEGMENT_BYTES: ${KAFKA_LOG_SEGMENT_BYTES:-268435456}
KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: ${KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS:-60000}
CLUSTER_ID: ichibi-lake-cluster
```

`KAFKA_MESSAGE_MAX_BYTES` (broker `message.max.bytes`) caps the size of a single produce request the broker will accept. It must be ≥ the client's `KAFKA_MAX_REQUEST_SIZE`; otherwise the broker returns `MESSAGE_TOO_LARGE` (kafkajs error: `The request included a message larger than the max message size the server will accept`). `KAFKA_REPLICA_FETCH_MAX_BYTES` is raised in lockstep so replication can fetch the larger records.

### Log retention

The `KAFKA_LOG_RETENTION_*` / `KAFKA_LOG_SEGMENT_BYTES` settings bound on-disk usage for `cleanup.policy=delete` topics (the default). A segment must roll (`KAFKA_LOG_SEGMENT_BYTES`, default 256 MiB) before it is eligible for deletion, so segment size is kept well below retention. Defaults cap each partition at 1 GiB and 7 days, with the retention check running every 60 s. Override any of them via the matching variable in `.env`.

## Key Fixes

| Issue | Cause | Fix |
|-------|-------|-----|
| Container exits with `controller.listener.names` error | KRaft mode requires explicit `KAFKA_CONTROLLER_LISTENER_NAMES` | Added `KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER` |
| Healthcheck always failing | `kafka-topics.sh` not on `$PATH` | Use full path `/opt/kafka/bin/kafka-topics.sh` |
| Consumer group not available | `__consumer_offsets` topic defaults to RF=3 on single broker | Added `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1` |
| `This server does not host this topic-partition` | Consumer connects before partition leader is elected | Use `admin.createTopics()` + `admin.fetchTopicMetadata()` polling before subscribing |
| Internal consumer spamming `Failed parsing message` on `__consumer_offsets` | Internal binary topic data can't be parsed as JSON | Skip topics with `__` prefix |

## Running Kafka SSE Tests

### Prerequisites

- Docker Compose stack running: `docker compose -f docker/compose.yaml up -d`
- Both `kafka` and `app` services healthy
- Kafka broker accessible at `localhost:9092` (mapped from container)

### How the test works (`test-kafka-sse.js`)

1. Uploads 3 test rows to a new DuckDB table via `POST /upload/<tableName>`
2. Subscribes to a Kafka topic matching the table name via `GET /kafka-subscribe/<topicName>?since=1h`
3. Waits for the `dump-complete` SSE event confirming all rows were published
4. Verifies all 3 rows arrive as Kafka messages through the SSE stream
5. Exits cleanly

### Running

```bash
# Via npm script:
npm run test:kafka-sse

# Directly:
node test-kafka-sse.js

# With custom URL or API key:
BASE_URL=http://my-gateway:3333 API_KEY=my-key node test-kafka-sse.js
```

### Expected output

```
--- Kafka SSE Integration Tests ---

[Test 1] DuckDB data dump via Kafka on SSE subscribe
  Uploading 3 rows to table "kafka_sse_test_12345"...
  OK: All 3 rows received via SSE

--- ALL KAFKA SSE TESTS PASSED ---
```

### Troubleshooting

| Symptom | Likely cause |
|---------|-------------|
| Timed out waiting for dump-complete | Kafka broker not reachable; check `docker inspect ichibi-lake-kafka` for health |
| "This server does not host this topic-partition" | Topic not yet created or partition leader not elected; admin.createTopics with metadata polling should handle this |
| SSE stream returns `event: error` immediately | `KAFKA_BROKERS` not configured in Docker env |
| App logs: `Failed parsing message on topic __consumer_offsets` | Running old image without the `__` prefix filter — rebuild with `docker compose build app` |
