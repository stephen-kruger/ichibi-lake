# ichibi-lake Gateway V1.0.20
![alt text](graphics/ichibi-lake-2.jpg "iChibi Lake")

This is a DuckDB based data lake implementation. Fully featured, supports everything you need out the box to build out a data lake for your AI transformation.

## 1. Quick Start
To run, simply go into the docker directory and run 

```docker compose up --build```

- [http://localhost:4123](http://localhost:4123) to view the DuckDB web ui
- [http://localhost:3333](http://localhost:3333)   to view the Swagger documentation


## 1. Authentication & Authorization

### 1.1 API Key Authentication
All gateway methods require an API key passed as a header:

- **Header Name**: `x-api-key`
- **Example**: `x-api-key: ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13`
- Requests without a valid key are rejected with **HTTP 401**.

### 1.2 Per-Table Access Control (ACL)
Access to individual tables is governed by **Access Control Lists** rather than the
previous per-row `_api_key` column. Each table has two lists of API keys:

| Role | Permission | Endpoints |
| :--- | :--- | :--- |
| **Owner** | Read + Write | All operations (query, upload, blob, patch, delete, manage ACL) |
| **Reader** | Read only | `GET /tables/:tableName`, `GET /tables/:tableName/schema`, blob downloads |
| *(none)* | No access | Requests are rejected with **HTTP 403** |

- The API key that **creates** a table is automatically added as an owner.
- The internal Kafka consumer registers tables under the `kafka-internal` key.
- ACL entries are cached in memory with a configurable TTL (default 60s).

### 1.3 ACL Management Endpoints

**Get the current ACL for a table:**

```bash
curl -H "x-api-key: YOUR_KEY" \
  "http://ichibi-lake:3333/tables/my_table/acl"
```

*Response: `{ "success": true, "ownerKeys": ["key1", "key2"], "readerKeys": ["key3"] }`*

**Update the ACL (requires owner access):**

```bash
curl -X PATCH "http://ichibi-lake:3333/tables/my_table/acl" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"ownerKeys": ["key1", "key2"], "readerKeys": ["key3", "key4"]}'
```

*Response: `{ "success": true, "message": "ACL updated for table \"my_table\"" }`*

> **Migration note:** If you have existing tables created under the old per-row
> `_api_key` model, the gateway automatically migrates them on startup:
> distinct `_api_key` values are promoted to owners in the ACL,
> and the `_api_key` column is dropped.

## 3. Binary (BLOB) Asset Management

The gateway provides dedicated endpoints for uploading and streaming raw binary data (images, documents, models).

### 3.0 Blob Storage Backend

Controlled by the `USE_FILESYSTEM_BLOBS` environment variable in `docker/compose.yaml`.

| Mode | `USE_FILESYSTEM_BLOBS` | Where BLOBs Live | DuckLake Table Column |
| :--- | :--- | :--- | :--- |
| **Filesystem (default)** | `1` | `./data/blobs/` (bind mount, `BLOBS_PATH`) | `VARCHAR` — stores the file path |
| **Inline Parquet** | `0` / unset | Inside Parquet files in `./data/ducklake/` | `BLOB` — embedded as `BYTE_ARRAY` |

**Filesystem mode** is the recommended default: binary files are written as standalone files to the `BLOBS_PATH` directory, and only a reference path is stored in DuckLake. This keeps Parquet rows small and makes it easy to inspect, back up, or migrate blob files independently of the catalog.

### 3.1 Uploading BLOBs
You can upload raw binary data to a specific table or the default `ichibi_table`.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/blobs/:blobColumn` | Upload to default table with **auto-generated UUID**. |
| `POST` | `/blobs/:idValue/:blobColumn` | Upload to default table with a specific ID. |
| `POST` | `/tables/:tableName/blobs/:blobColumn` | Upload to custom table with **auto-generated UUID**. |
| `POST` | `/tables/:tableName/blobs/:idValue/:blobColumn` | Upload to custom table with a specific ID. |

**Key Features:**
- **Dynamic Metadata**: Attach arbitrary properties via query parameters (e.g., `?owner=sara&type=profile`). These are automatically saved as columns.
- **Auto-Schema**: The table and columns (including BLOB path and metadata) are created automatically if they don't exist.

**Example: Anonymous Upload with Metadata**
```bash
curl -X POST "http://ichibi-lake:3333/blobs/avatar?owner=marwan&importance=high" \
     -H "x-api-key: YOUR_KEY" \
     -H "Content-Type: application/octet-stream" \
     --data-binary "@/path/to/image.png"
```
*Response: `{ "success": true, "id": "6bbf0bd4-..." }`*

### 3.2 Downloading BLOBs
Retrieve raw binary data directly into your application.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/blobs/:idValue/:blobColumn` | Stream BLOB from the default table. |
| `GET` | `/tables/:tableName/blobs/:idValue/:blobColumn` | Stream BLOB from a custom table. |

**Example:**
```bash
curl -H "x-api-key: YOUR_KEY" \
     "http://ichibi-lake:3333/blobs/6bbf0bd4-.../avatar" \
     --output my_download.png
```

---

## 4. Record Metadata Updates

Perform atomic updates to any record's attributes using JSON.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `PATCH` | `/records/:idValue` | Update record in the default `ichibi_table`. |
| `PATCH` | `/tables/:tableName/records/:idValue` | Update record in a specific table. |

**Key Features:**
- **Atomic Modification**: Only the fields provided in the JSON body are updated.
- **Dynamic Schema**: If a provided field doesn't exist, the gateway automatically adds a `VARCHAR` column to the table.
- **ACL-Protected**: The requester must have **owner** (write) access to the table via its ACL.

**Example: Update Status and Owner**
```bash
curl -X PATCH "http://ichibi-lake:3333/records/6bbf0bd4-..." \
     -H "x-api-key: YOUR_KEY" \
     -H "Content-Type: application/json" \
     -d '{"status": "verified", "owner": "ahmad", "verified_at": "2024-04-10"}'
```

---

## 5. Data Discovery & REST API

Query and manage the structure of your data lake using standard RESTful patterns.

### 5.1 Discovery Endpoints
- **List Tables**: `GET /tables`
- **Get Schema**: `GET /tables/:tableName/schema`

### 4.2 Querying Data
Query tables using URL parameters for filtering, sorting, and pagination.
Results are scoped by the requester's ACL — the caller must have at least
**read** access to the table. Unauthorized tables return HTTP 403.

**Example: Filter by Metadata and Sort**
```bash
curl -H "x-api-key: YOUR_KEY" \
     "http://ichibi-lake:3333/tables/ichibi_table?owner=marwan&importance=high&limit=10&sort=-created_at"
```

---

## 5. Event Ingestion (JSON)

### 5.1 `POST /upload` or `/upload/:tableName`
Upload a JSON array of objects. New tables are created automatically with type inference
and the caller is registered as an owner in the table's ACL.

```bash
curl -X POST http://ichibi-lake:3333/upload/user_stats \
     -H "x-api-key: YOUR_KEY" \
     -H "Content-Type: application/json" \
     -d '[{"user": "duck", "points": 100}, {"user": "lake", "points": 250}]'
```

### 5.2 `POST /query` (Raw SQL)
Execute arbitrary read-only SQL commands.
```bash
curl -X POST http://ichibi-lake:3333/query \
     -H "x-api-key: YOUR_KEY" \
     -H "Content-Type: application/json" \
     -d '{"sql": "SELECT category, count(*) FROM ichibi_table GROUP BY 1"}'
```

---

## 6. Kafka Integration

### 6.1 HTTP Sink (`/kafka-sink`)
Designed for Kafka Connect. Automatically flattens `.value` schemas.
New tables are registered in the ACL under the `kafka-internal` key.
```bash
curl -X POST http://ichibi-lake:3333/kafka-sink \
     -H "x-api-key: YOUR_KEY" \
     -H "X-Kafka-Topic: logs" \
     -d '[{"value": {"level": "info", "msg": "started"}}]'
```

### 6.2 Internal Background Consumer
Enable native consumption by setting `KAFKA_BROKERS` in `ecosystem.config.cjs`. The background loop will automatically ingest events into tables matching their topics.

### 6.3 SSE Subscription (`GET /kafka-subscribe/:topicName`)
Subscribe to a Kafka topic in real-time using Server-Sent Events (SSE). Requires `KAFKA_BROKERS` to be configured.

```bash
curl -H "x-api-key: YOUR_KEY" -N \
  "http://ichibi-lake:3333/kafka-subscribe/my-topic"
```

Each event is a JSON object streamed as an SSE `data:` field:
```
data: {"topic":"my-topic","partition":0,"offset":"42","key":"mykey","value":"{\"msg\":\"hello\"}","timestamp":"1712345678000"}
```

**Parameters:**
- `?groupId=` — Override the consumer group (default: `ichibi-lake-sse-<sanitized-api-key>`) 
- `?since=` — Publish only DuckDB rows newer than this timestamp into Kafka before subscribing. Supports:
  - **Absolute ISO 8601** (e.g. `?since=2024-06-09T10:00:00Z`)
  - **Relative duration** (e.g. `?since=5m`, `?since=1h`, `?since=2d`)
  - **Plain number** (treated as minutes, e.g. `?since=5`)
  - When omitted, **all** DuckDB rows are dumped.
- `?sinceColumn=` — Explicit column name to filter on (default: `updated`)

By default, all historic DuckDB data is dumped on subscribe.

---



## 7. Testing

```sh
npm test                       # run all suites (requires Docker: app, db, kafka)
npm run test:unit              # unit tests only
npm run test:api               # integration tests
npm run test:rest              # REST API tests
npm run test:blob              # BLOB tests
npm run test:graphs            # graph query tests
npm run test:kafka-sink        # Kafka sink tests
npm run test:kafka-sse         # Kafka SSE tests
npm run test:kafka-retroactive # Kafka retroactive tests
```
