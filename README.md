# ichibi-lake Gateway v1.6.25.1

![ichibi-lake](graphics/ichibi-lake-2.jpg "iChibi Lake")

A feature-rich DuckDB-based data lake gateway with REST API, blob storage, schema evolution, Kafka integration, GraphQL, graph (PGQ) query support, role-based access control and a built-in admin console (data browser, retention/pruning, settings) — ready for AI/ML data pipelines.

---

## Table of Contents

- [1. Quick Start](#1-quick-start)
- [2. Architecture](#2-architecture)
- [3. Environment Variables](#3-environment-variables)
  - [3.1 Core / Server](#31-core--server)
  - [3.2 DuckLake (Postgres Metadata)](#32-ducklake-postgres-metadata)
  - [3.2.1 Object Storage (S3 / MinIO)](#321-object-storage-s3--minio)
  - [3.3 Authentication](#33-authentication)
  - [3.4 Blob Storage](#34-blob-storage)
  - [3.5 SQL Enforcement](#35-sql-enforcement)
  - [3.6 Performance & Caching](#36-performance--caching)
  - [3.7 Kafka Integration](#37-kafka-integration)
  - [3.8 Docker Compose / Deployment](#38-docker-compose--deployment)
  - [3.9 Admin Console](#39-admin-console)
- [4. Deployment](#4-deployment)
  - [4.1 Docker (recommended)](#41-docker-recommended)
  - [4.2 Native Node.js](#42-native-nodejs)
  - [4.3 Production Checklist](#43-production-checklist)
  - [4.4 SuperLake Wizard](#44-superlake-wizard)
  - [4.5 Upgrading](#45-upgrading)
  - [4.6 Object Storage (S3 / MinIO)](#46-object-storage-s3--minio)
  - [4.7 Lake Maintenance (Small Files)](#47-lake-maintenance-small-files)
- [5. Authentication & Authorization](#5-authentication--authorization)
  - [5.1 API Key Authentication](#51-api-key-authentication)
  - [5.2 Per-Table Access Control (ACL)](#52-per-table-access-control-acl)
  - [5.3 Role-Based Access Control (RBAC)](#53-role-based-access-control-rbac)
  - [5.4 ACL Management Endpoints](#54-acl-management-endpoints)
- [6. Binary (BLOB) Asset Management](#6-binary-blob-asset-management)
  - [6.1 Blob Storage Backend](#61-blob-storage-backend)
  - [6.2 Uploading BLOBs](#62-uploading-blobs)
  - [6.3 Downloading BLOBs](#63-downloading-blobs)
  - [6.4 Deleting BLOBs](#64-deleting-blobs)
- [7. Record Metadata Updates](#7-record-metadata-updates)
- [8. Data Discovery & REST API](#8-data-discovery--rest-api)
  - [8.1 Exporting Tables](#81-exporting-tables)
- [9. Event Ingestion](#9-event-ingestion)
  - [9.1 JSON Upload](#91-json-upload)
  - [9.2 Upserts & Deduplication (Key Columns)](#92-upserts--deduplication-key-columns)
  - [9.3 Raw SQL Query](#93-raw-sql-query)
  - [9.4 Bulk Import (Files & Object Storage)](#94-bulk-import-files--object-storage)
  - [9.5 Idempotent Retries](#95-idempotent-retries)
- [10. Kafka Integration](#10-kafka-integration)
  - [10.1 HTTP Sink](#101-http-sink-post-kafka-sink)
  - [10.2 Internal Background Consumer](#102-internal-background-consumer)
  - [10.3 SSE Subscription](#103-sse-subscription-get-kafka-subscribetopicname)
- [11. GraphQL](#11-graphql)
- [12. Graph Queries (SQL/PGQ)](#12-graph-queries-sqlpgq)
  - [12.1 Architecture: Durable Registry & Lazy Materialization](#121-architecture-durable-registry--lazy-materialization)
  - [12.2 Defining a Property Graph](#122-defining-a-property-graph)
  - [12.3 Querying a Graph](#123-querying-a-graph)
  - [12.4 Listing & Deleting Graphs](#124-listing--deleting-graphs)
- [13. Admin Console](#13-admin-console)
  - [13.1 Accessing the Console](#131-accessing-the-console)
  - [13.2 Features](#132-features)
  - [13.3 Security Model](#133-security-model)
  - [13.4 Recovery CLI](#134-recovery-cli)
  - [13.5 Extending the Console](#135-extending-the-console)
- [14. Operations: Health, Metrics & Limits](#14-operations-health-metrics--limits)
  - [14.1 Health Checks](#141-health-checks)
  - [14.2 Prometheus Metrics](#142-prometheus-metrics)
  - [14.3 Result Size Limits](#143-result-size-limits)
- [15. Testing](#15-testing)

---

## 1. Quick Start

```sh
cd docker
docker compose up --build
```

Once running:

| Service | URL |
| :--- | :--- |
| REST API / Swagger UI | [http://localhost:3333](http://localhost:3333) |
| Admin console | [http://localhost:3333/console](http://localhost:3333/console) (see [§13](#13-admin-console)) |
| OpenAPI spec | [http://localhost:3333/swagger.yaml](http://localhost:3333/swagger.yaml) |

Default API key for development:

```
ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13
```

**Admin console: first sign-in.** There is no default console password. The first visitor creates the admin account using a one-time setup code from the gateway log:

1. Get the setup code:
   ```sh
   docker logs ichibi-lake 2>&1 | grep -A3 "First-run setup"
   ```
2. Open [http://localhost:3333/console](http://localhost:3333/console), enter the code, and choose an admin username and password (at least 10 characters).
3. You are signed in. Next time, sign in with that username and password.

Full instructions, including adding logins and resetting a forgotten password, are in [§13.1](#131-accessing-the-console).

---

## 2. Architecture

ichibi-lake is a Node.js gateway that wraps a **DuckDB** in-memory instance backed by the **DuckLake** storage extension. Parquet data files are persisted to a local directory or to **S3-compatible object storage** (AWS S3, MinIO, R2; see [§4.6](#46-object-storage-s3--minio)); table metadata (schemas, ACLs, and graph definitions) and small not-yet-flushed writes live in a companion **PostgreSQL** database. BLOBs can be stored as standalone files or inline in Parquet. Scheduled **lake maintenance** keeps the number of Parquet files small ([§4.7](#47-lake-maintenance-small-files)).

```
Client → REST/GraphQL ──┐
Browser → /console ─────┼─► ichibi-lake gateway ─► DuckDB (DuckLake) ─► Parquet files (disk or s3://)
                        │          │                       └── PostgreSQL (metadata + inlined rows)
                        │          ├── Kafka (sink / SSE / consumer)
                        │          └── Filesystem (blobs)
```

The gateway runs two DuckDB instances: a **primary** instance (connection pool) for REST reads and writes, ingestion, DDL, the admin console and registry writes, and an isolated, locked-down **user-query** instance that runs user-supplied SQL and graph queries (table-scoped for non-superusers, read-only, no file or network access outside the lake; see [§3.5](#35-sql-enforcement)), so heavy queries cannot stall ingestion. Durable state (table data, ACLs, RBAC, and **property-graph definitions**) is persisted through DuckLake so it survives restarts and is shared across instances. Admin-console state (logins, settings, pruning rules, audit log) lives in a separate DuckLake catalog, `ichibi_console`, that only the primary instance attaches. Property graphs are an exception that DuckPGQ stores only in the in-memory catalog of whichever DuckDB instance created them — ichibi-lake works around this with a **durable graph registry** (see [§12.1](#121-architecture-durable-registry--lazy-materialization)).

---

## 3. Environment Variables

### 3.1 Core / Server

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `3000` | Port the Node gateway listens on |
| `JSON_BODY_LIMIT` | `50mb` | Max JSON request body size (e.g. `2gb`, `50mb`) |
| `MAX_BATCH_SIZE` | `10000` | Max rows per `/upload` request; prevents OOM. Also editable live in the console. |
| `INGEST_CONFLICT_RETRIES` | `10` | Attempts for an upsert/dedupe batch that loses a DuckLake commit race with a concurrent write to the same table, before it fails with HTTP 409 ([§9.2](#92-upserts--deduplication-key-columns)). Also editable live in the console. |
| `IMPORT_MAX_MB` | `1024` | Largest file accepted by `POST /tables/:tableName/import` and the console import (HTTP 413 above it). Also editable live in the console. |
| `IMPORT_TMP_DIR` | *OS temp dir*`/ichibi-import` | Where uploaded import files are staged (deleted after each request) |
| `IMPORT_ALLOWED_SOURCES` | *(empty)* | Comma-separated `s3://bucket/prefix/` entries any writer may import from ([§9.4](#94-bulk-import-files--object-storage)). Other `s3://` sources need a superuser key. |
| `IDEMPOTENCY_TTL_HOURS` | `24` | How long an `Idempotency-Key` result is replayed ([§9.5](#95-idempotent-retries)). Also editable live in the console. |
| `NODE_ENV` | `production` | Node environment |

### 3.2 DuckLake (Postgres Metadata)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DUCKLAKE_PG_HOST` | `0.0.0.0` | Postgres host for DuckLake metadata |
| `DUCKLAKE_PG_PORT` | `5432` | Postgres port |
| `DUCKLAKE_PG_USER` | `postgres` | Postgres user |
| `DUCKLAKE_PG_PASSWORD` | `postgres` | Postgres password |
| `DUCKLAKE_PG_DB` | `ducklake` | Postgres database name |
| `DUCKLAKE_DATA_PATH` | `data_files/` | Where DuckLake writes Parquet data files: a local directory, or an `s3://bucket/prefix/` URL for object storage ([§3.2.1](#321-object-storage-s3--minio)) |
| `DATA_INLINING_ROW_LIMIT` | `1000` | A write of fewer rows than this is stored in the Postgres catalog instead of creating a Parquet file; [lake maintenance](#47-lake-maintenance-small-files) flushes these rows to Parquet in bulk. `0` = every write creates a Parquet file (the behaviour before this setting existed) |
| `DISABLE_DUCKPGQ` | *(unset)* | Set to `1` to disable the DuckPGQ graph extension |
| `DISABLE_DUCKLAKE_COMPACTION` | *(unset)* | Set to `1` to stop all [lake maintenance](#47-lake-maintenance-small-files) runs (scheduled, startup and manual). Emergency switch only: without maintenance, inlined rows are never flushed and small files are never merged |
| `STARTUP_ATTEMPTS` | `5` | How often the startup sequence (RBAC/ACL bootstrap, migrations, admin console) is attempted before the process exits so its supervisor can restart it. Each step is also retried on DuckLake transaction conflicts, which occur when several instances start against one catalog at the same moment |

The DuckDB extensions the gateway needs (`ducklake`, `postgres`, `duckpgq`, and `httpfs`/`aws` for object storage) are pre-installed in the Docker image. When running natively they are downloaded on first start (or run `node scripts/install-extensions.js` once).

#### 3.2.1 Object Storage (S3 / MinIO)

Used when `DUCKLAKE_DATA_PATH` is an `s3://` URL. S3-compatible stores (MinIO, Cloudflare R2, Ceph, GCS interoperability) use `s3://` with `S3_ENDPOINT`. See [§4.6](#46-object-storage-s3--minio). These settings, `DUCKLAKE_DATA_PATH`, `DATA_INLINING_ROW_LIMIT` and the BLOB settings can also be set on the console's **Storage** page ([§13.2](#132-features)); values saved there take precedence over `docker/.env`.

| Variable | Default | Description |
| :--- | :--- | :--- |
| `S3_ENDPOINT` | *(empty: AWS)* | `host:port` of an S3-compatible server, e.g. `minio:9000` (an `http(s)://` prefix is ignored; use `S3_USE_SSL`) |
| `S3_REGION` | `us-east-1` | Bucket region |
| `S3_ACCESS_KEY` | *(empty)* | Access key id. Empty: DuckDB's AWS **credential chain** (environment `AWS_*` variables, `~/.aws`, EC2 instance / ECS task / IRSA role) |
| `S3_SECRET_KEY` | *(empty)* | Secret access key (required with `S3_ACCESS_KEY`) |
| `S3_SESSION_TOKEN` | *(empty)* | Session token for temporary credentials |
| `S3_URL_STYLE` | `path` with an endpoint, `vhost` without | `path` (MinIO and most self-hosted stores) or `vhost` (AWS). `S3_PATH_STYLE=true\|false` is accepted as an alias |
| `S3_USE_SSL` | `true` | `false` for a plain-HTTP endpoint such as a local MinIO |
| `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` / `MINIO_BUCKET` | `ichibi-lake` / `change-me-minio-secret` / `ichibi-lake` | Compose `minio` profile only: credentials of the bundled MinIO server and the bucket created at startup |
| `MINIO_IMAGE` / `MINIO_PORT` / `MINIO_CONSOLE_PORT` | `cgr.dev/chainguard/minio:latest` / `9000` / `9001` | Compose `minio` profile only: image and published host ports |
| `MINIO_BIND_ADDRESS` | `127.0.0.1` | Compose `minio` profile only: host address the MinIO ports are bound to. `0.0.0.0` makes them reachable from other machines |

### 3.3 Authentication

| Variable | Default | Description |
| :--- | :--- | :--- |
| `API_KEYS` | — | Comma-separated list of valid API keys (preferred). Each request must pass one via the `x-api-key` header. When no RBAC variables are set, these keys are promoted to RBAC superusers at startup. Also manageable on the console's **API Keys** page. |
| `API_KEY` | — | Single API key (fallback if `API_KEYS` is unset) |

**RBAC (Role-Based Access Control) — optional, adds role-based authorization on top of API keys:**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `RBAC_SUPERUSERS` | — | Comma-separated list of API keys to promote to `superuser` on startup |
| `RBAC_USERS` | — | JSON array of additional users. Each entry: `{"apiKey":"...","name":"...","email":"...","roles":["writer"\|"reader"]}` |
| `RBAC_TABLE_PERMISSIONS` | — | JSON array of per-table overrides. Each entry: `{"apiKey":"...","tableName":"...","permission":"allow_read"\|"allow_write"\|"deny"}` |
| `RBAC_CACHE_TTL_MS` | `60000` | RBAC authorization cache TTL (ms). Reduce for faster propagation of role changes |

See [§5.3](#53-role-based-access-control-rbac) for details on roles, the admin API, and coexistence with legacy `API_KEYS`.

### 3.4 Blob Storage

| Variable | Default | Description |
| :--- | :--- | :--- |
| `USE_FILESYSTEM_BLOBS` | `0` | `1` = store BLOBs as separate files on disk; `0` / unset = inline in Parquet as `BLOB` |
| `BLOBS_PATH` | `./data/blobs` | Directory for filesystem BLOB files |

Filesystem BLOBs always use a **local (or mounted) directory**, also when the lake's Parquet files are on object storage. With several gateway hosts, either mount a shared directory at `BLOBS_PATH` or use `USE_FILESYSTEM_BLOBS=0`, which stores BLOBs inside the lake (and therefore in the bucket).

### 3.5 SQL Enforcement

User-supplied SQL (`POST /query`, the GraphQL `sql` resolver, and the graph endpoints `POST /graphs`, `DELETE /graphs/:name`, `POST /graphs/:name/query`) is locked down in layers:

1. **Table-scoped for non-superusers.** Superuser keys (the RBAC `superuser` role, or any `API_KEYS` key when RBAC is not configured) may run any read-only SQL. Every other key (writers, readers, ACL-only keys) may run SQL on exactly the tables it can read: the gateway parses the query with DuckDB's own parser (`json_serialize_sql`), collects every table it references (FROM, JOINs, subqueries in any clause, CTEs, set operations, `DESCRIBE`/`SUMMARIZE`) and checks each one with the same RBAC-then-ACL rule as `GET /tables/:name`. Any denied table fails the whole query with HTTP 403 naming it, before anything runs (so NDJSON streams never start). CTE names are resolved the way DuckDB binds them: `WITH secret AS (SELECT * FROM allowed) SELECT * FROM secret` reads only `allowed`, while `WITH secret AS (SELECT * FROM secret) ...` reads the real table `secret`. For non-superusers the parser also rejects, failing closed:
   - anything but a single `SELECT` statement (multiple statements: 400; writes, `EXPLAIN`, `SET`, `PIVOT` statements: 403);
   - every table function (`read_parquet`, `read_csv`, `read_text`, `glob`, `query`, `range`, `unnest` in FROM, `duckdb_*()`, `ducklake_*()`): 403;
   - tables outside `ducklake.main` (`t`, `main.t`, `ducklake.t` and `ducklake.main.t` are fine; `information_schema`, `pg_catalog`, `memory`, `system`, `temp`, the console catalog and the DuckLake metadata catalog are not): 403;
   - system tables (`_`-prefixed), unknown tables, file paths such as `'x.parquet'` and DuckDB's catalog views such as `duckdb_tables`: 404, the same answer as the REST API for a missing table; views: 403; `SHOW TABLES`: 403 (use `GET /tables`);
   - any construct the walker does not recognise: 400.

   **Graph queries stay superuser-only.** DuckPGQ parses `GRAPH_TABLE`, but `json_serialize_sql` cannot serialize it ("Cannot serialize a table function with process-local bind input", DuckDB 2.0.0), so the tables read by a graph query's `match`/`where`/`columns` fragments (which can contain subqueries) cannot be determined. Creating and deleting graphs is superuser-only as well.
2. **Always read-only.** DDL/DML/admin keywords are rejected with HTTP 403. There is no setting to turn this off; change data through the REST API (upload, records, blobs) or the [admin console](#13-admin-console).
3. **Locked-down engine.** The SQL runs on a separate DuckDB instance that has dropped its DuckLake secret, can read only the lake's data directory (no `read_text('/etc/...')`, no BLOB directory, no network), and does not attach the admin console's catalog. With object storage, the only remote location it can read is the lake's own `s3://bucket/prefix/` (other prefixes, other buckets and `http(s)://` are refused). It keeps the S3 secret, because reading the lake needs it, but DuckDB only ever shows it redacted and refuses to display it unredacted once external access is disabled.
4. **No connection-string introspection.** `duckdb_databases()`, `pragma_database_list`, `duckdb_settings()`, `current_setting()`, `duckdb_secrets()` and `which_secret()` are rejected, because DuckDB would reveal the metadata Postgres password through them.

Layers 2 to 4 apply to every key, superusers included, on top of layer 1.

**System tables** (names starting with `_`: RBAC users and their API keys, ACLs, the graph registry) are never reachable through the table REST endpoints, for any key, nor through SQL for non-superusers, and cannot be targets of `/upload`, `/kafka-sink`, `/tables/:tableName/import` or the Kafka consumer. They are managed through the superuser-only admin API and the console.

### 3.6 Performance & Caching

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PRIMARY_POOL_SIZE` | `4` | Number of warm DuckDB connections kept in the pool |
| `USER_QUERY_CONCURRENCY` | `4` | Max concurrent user SQL / graph queries. Also editable live in the console. |
| `USER_QUERY_TIMEOUT_MS` | `30000` | Timeout for user SQL / graph queries (ms), including JSON `POST /query`. Also editable live in the console. |
| `USER_QUERY_STREAM_TIMEOUT_MS` | `300000` | Timeout for a whole NDJSON `POST /query` response, transfer included (ms). Also editable live in the console. |
| `QUERY_MAX_ROWS` | `100000` | Row cap for JSON `POST /query` responses; larger results are truncated and flagged. Also editable live in the console. |
| `QUERY_STREAM_MAX_ROWS` | `10000000` | Row cap for NDJSON `POST /query` responses. Also editable live in the console. |
| `EXPORT_MAX_ROWS` | `5000000` | Row cap for `GET /tables/:tableName/export`. Also editable live in the console. |
| `EXPORT_CONCURRENCY` | `2` | Table exports allowed at once; further requests get HTTP 429. Also editable live in the console. |
| `EXPORT_TIMEOUT_MS` | `600000` | Timeout for one export, download included (ms); for a console multi-table ZIP, per table. Also editable live in the console. |
| `EXPORT_TMP_DIR` | *OS temp dir*`/ichibi-export` | Where Parquet exports are staged before streaming (deleted after each response) |
| `COLUMN_CACHE_TTL_MS` | `60000` | Per-table column schema cache TTL (ms) |
| `ACL_CACHE_TTL_MS` | `60000` | ACL cache TTL (ms) |
| `DEFAULT_PARTITION_KEY` | `created` | Column(s) to partition new tables by. Comma-separated for multi-column partitioning. Set to `none` to disable. Also editable live in the console. |

### 3.7 Kafka Integration

**Consumer (background ingestion):**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | — | Comma-separated broker list (e.g. `kafka:9092`). Leave empty to disable. |
| `KAFKA_CLIENT_ID` | `ducklake-consumer` | Client ID for the consumer. The same variable also sets the producer's client ID (see below), so one value applies to both. |
| `KAFKA_GROUP_ID` | `ducklake-ingestion-group` | Consumer group ID |
| `KAFKA_TOPIC_REGEX` | `.*` | Regex to match topics for auto-ingestion |
| `KAFKA_BATCH_SIZE` | `100` | Max messages per consumer batch (also caps the producer's messages per batch) |
| `KAFKA_BATCH_TIMEOUT_MS` | `5000` | Max wait to complete a batch (ms) |
| `KAFKA_METADATA_MAX_AGE_MS` | `10000` | Metadata refresh interval (ms) |
| `KAFKA_INGEST_MODES` | — | Per-topic ingestion mode: comma-separated `topic=append\|upsert\|dedupe[:key1+key2]`, `*` for all other topics (e.g. `customers=upsert:customer_id,*=append`). Editable live in the console. See [§10.2](#102-internal-background-consumer). |

**Producer / SSE:**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_CLIENT_ID` | `ichibi-lake-sse` | Producer client ID when the variable is unset (one variable for both; see above) |
| `KAFKA_MAX_MESSAGE_BYTES` | `950000` | Max size of a single produced message |
| `KAFKA_MAX_BATCH_BYTES` | `950000` | Max batch size for produced messages |
| `KAFKA_MAX_REQUEST_SIZE` | `10485760` | Max produce request size (should match broker's `KAFKA_MESSAGE_MAX_BYTES`) |
| `KAFKA_PRODUCER_COMPRESSION` | `gzip` | Compression: `gzip`, `snappy`, `lz4`, `zstd`, or `none` |

### 3.8 Docker Compose / Deployment

| Variable | Default | Description |
| :--- | :--- | :--- |
| `POSTGRES_PASSWORD` | `postgres` | Password of the bundled Postgres, used by both the `db` and `app` services. Applied by Postgres only when it initialises an empty data directory: see [§4.3](#43-production-checklist) to change it on an existing lake |
| `KAFKA_LOG_RETENTION_HOURS` | `48` | Kafka log retention in hours |
| `KAFKA_LOG_RETENTION_BYTES` | `1073741824` | Kafka log retention in bytes per partition |
| `KAFKA_LOG_SEGMENT_BYTES` | `268435456` | Kafka segment size before roll |
| `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS` | `60000` | Kafka retention check interval |

### 3.9 Admin Console

| Variable | Default | Description |
| :--- | :--- | :--- |
| `CONSOLE_SETUP_TOKEN` | *(generated)* | One-time code required to create the first console admin. When unset, a random code is printed to the gateway log on startup while no console login exists. |
| `CONSOLE_SESSION_SECRET` | *(generated)* | HMAC key for console session cookies. When unset, one is generated and stored in the console catalog (shared by all instances). |
| `CONSOLE_SESSION_TTL_HOURS` | `12` | Console session lifetime. Also editable in the console. |
| `STORAGE_CONFIG_FILE` | `./config/storage.json` | Where the **Storage** page saves its settings (in Docker `/app/config/storage.json`, bind-mounted to `docker/data/config`). Written with mode 0600 because it can hold S3 credentials. |
| `STORAGE_AUTO_RESTART` | *(detected)* | Whether **Save & restart** on the Storage page restarts the gateway. Default: on in a container (Docker restarts it) or under PM2, off otherwise (save, then restart yourself). `1`/`0` forces it. |
| `METRICS_TOKEN` | *(unset)* | Bearer token for `GET /metrics`. When unset, `/metrics` requires a superuser API key. See [§14.2](#142-prometheus-metrics). |
| `PUBLIC_URL` | `http://localhost:<port>` | Externally reachable base URL, used in startup log messages (Swagger UI and admin console links). Docker Compose defaults it to the published host port (`http://localhost:3333`); set it to your real address (e.g. `https://lake.example.com`) when deployed. |
| `SNAPSHOT_RETENTION_DAYS` | *(unset)* | Days of DuckLake snapshot history (time travel) to keep. When set, [lake maintenance](#47-lake-maintenance-small-files) and pruning rules with *reclaim storage* expire older snapshots and delete unreferenced files, freeing the space used by deleted rows. Unset: snapshots are never expired. Also editable in the console. |

---

## 4. Deployment

### 4.1 Docker (recommended)

```sh
# 1. Clone the repository
git clone <repo-url> ichibi-lake
cd ichibi-lake

# 2. (Optional) Customize environment
cp docker/.env.example docker/.env
# Edit docker/.env to set API_KEYS, KAFKA_BROKERS, etc.

# 3. Start all services
cd docker
docker compose up --build -d

# 4. Verify health
docker compose ps
curl -sf http://localhost:3333/health/ready   # {"status":"ready",...} once startup has finished
```

The compose stack starts three services:

| Service | Container | Purpose |
| :--- | :--- | :--- |
| `app` | `ichibi-lake` | Node.js REST gateway |
| `db` | `ichibi-lake-postgres` | Postgres 17 (DuckLake metadata) |
| `kafka` | `ichibi-lake-kafka` | Kafka 4.3.1 (event ingestion) |
| `minio` *(profile `minio`)* | `ichibi-lake-minio` | Optional S3-compatible object storage for the lake ([§4.6](#46-object-storage-s3--minio)) |

**Persistent volumes** are mounted under `docker/data/`:

```
docker/data/
├── blobs/       # BLOB files (when USE_FILESYSTEM_BLOBS=1)
├── ducklake/    # Parquet data files (console data is stored in Postgres, schema ichibi_console)
├── postgres/    # Postgres data directory
├── kafka/       # Kafka KRaft logs
└── minio/       # MinIO objects (only with the minio profile)
```

**Stopping:**

```sh
docker compose down        # Stops services
docker compose down -v     # Stops services AND removes volumes (destroys data)
```

### 4.2 Native Node.js

```sh
# Prerequisites: Node.js 22+, a Postgres 17 instance, and (optionally) a Kafka broker.

# 1. Install dependencies
npm ci --omit=dev

# 2. Set environment variables
export PORT=3000
export API_KEYS="ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13"
export DUCKLAKE_PG_HOST=localhost
export DUCKLAKE_PG_PORT=5432
export DUCKLAKE_PG_USER=postgres
export DUCKLAKE_PG_PASSWORD=postgres
export DUCKLAKE_PG_DB=ducklake
export DUCKLAKE_DATA_PATH=./data_files
# Optional: download the DuckDB extensions now instead of on first start
node scripts/install-extensions.js

# 3. Launch
node src/index.js
```

For production with process management, use **PM2**:

```sh
npm install -g pm2
pm2 start ecosystem.config.cjs
pm2 save
pm2 startup
```

### 4.3 Production Checklist

- **Change API keys** — Set `API_KEYS` to your own values (never use the test key)
- **Tune `JSON_BODY_LIMIT`** — Match your expected payload size; too large invites OOM
- **Tune `MAX_BATCH_SIZE`** — Prevent a single upload from exhausting memory
- **Postgres logs** — The compose.yaml configures Postgres with conservative logging (no per-query logging, 2-second slow-query threshold, 7-day log rotation)
- **Kafka retention** — Set `KAFKA_LOG_RETENTION_HOURS` and `KAFKA_LOG_RETENTION_BYTES` to bound disk usage
- **Use a reverse proxy** — Place behind nginx, Caddy, or a cloud LB for TLS termination and rate limiting
- **Claim the admin console** — Complete first-run setup at `/console` right after deploying, and serve it over HTTPS (session cookies are marked `Secure` when the request is HTTPS)
- **Exposed ports** — `compose.yaml` publishes only the gateway (`3333`) on every interface. Postgres has no published port and sits on an internal network that only the gateway joins (use `docker compose exec db psql -U postgres ducklake` for ad-hoc access). Kafka has no published ports either (it has no authentication; send messages through `POST /kafka-sink`), and the optional MinIO ports (`9000`/`9001`) are bound to `127.0.0.1`. The gateway container drops all Linux capabilities, and no container can gain privileges (`no-new-privileges`)
- **Change the Postgres password** — set `POSTGRES_PASSWORD` in `docker/.env` (the default is `postgres`). Postgres only applies it when it creates an empty data directory, so on an existing lake run `docker compose exec db psql -U postgres -c "ALTER USER postgres PASSWORD '<new>'"` first, then set `POSTGRES_PASSWORD=<new>` and `docker compose up -d`
- **Wire up health checks and metrics** — point your load balancer / orchestrator readiness probe at `GET /health/ready` and liveness at `GET /health`; set `METRICS_TOKEN` and scrape `GET /metrics` (see [§14](#14-operations-health-metrics--limits))
- **Decide on snapshot retention if you prune data** — set `SNAPSHOT_RETENTION_DAYS` so deleted rows actually free storage (see [§13.2](#132-features))
- **Put the lake on object storage** for durability and multi-host deployments — see [§4.6](#46-object-storage-s3--minio). Keep [lake maintenance](#47-lake-maintenance-small-files) enabled; with inlining it is what turns small writes into Parquet files
- **Back up Postgres, not just the bucket** — the catalog is the source of truth for which files make up each table, and it also holds inlined rows that maintenance has not flushed yet

### 4.4 SuperLake Wizard

For advanced multi-node or S3-backed deployments, the built-in SuperLake wizard guides you through configuration:

```sh
npm run superlake
```

Interactive options:

| Option | Description |
| :--- | :--- |
| **Single-host scale-out** | Several gateway containers on one host sharing one data directory and one Postgres |
| **Multi-host NFS** | One storage host exports the data directory over NFS; gateway hosts mount it |
| **Multi-host MinIO** | Self-hosted S3-compatible storage on your LAN (uses the built-in object-storage support, [§4.6](#46-object-storage-s3--minio)) |
| **Multi-host SSHFS** | Mount the data directory over SSH (FUSE), no extra services |
| **Federation** | Separate lakes per host, cross-attached as peers for cross-lake queries |

The wizard writes `compose.yaml`, `.env` and a README for the chosen layout.

---

### 4.5 Upgrading

Existing data is migrated **automatically** on first start: the gateway attaches DuckLake with `AUTOMATIC_MIGRATION true`, so a newer DuckDB/DuckLake upgrades the catalog schema in Postgres in place. No manual steps are required, but note:

- **Migration is one-way.** Upgrading to the DuckDB 2.0 release moves the catalog to DuckLake format 1.1, which older (1.5.x) gateways and DuckDB CLIs cannot open (`Only DuckLake versions ... are supported`). **Back up the Postgres metadata database before upgrading** if you may need to roll back:
  ```sh
  docker exec ichibi-lake-postgres pg_dump -U postgres ducklake > ducklake-backup.sql
  ```
- **Stop every old instance first.** Do not run old and new gateways against the same catalog (e.g. a rolling deploy) — old instances fail once the catalog is migrated.
- **First start may be slower** on large catalogs, since migration runs during startup before requests are served.
- **Admin console data moves to its own catalog.** Console logins, settings and the audit log from earlier releases are moved from the main catalog to `ichibi_console` on first start, and old single-table retention rules are converted into pruning rules (`RETENTION_INTERVAL_MINUTES` becomes each rule's schedule; the setting is gone). Console logins and passwords keep working.
- **Storage settings from the console need a new mount.** `docker/compose.yaml` now maps `./data/config` to `/app/config`, where the Storage page saves its settings. If you keep your own compose file, add that volume; without it the page reports that settings cannot be saved (or would not survive re-creating the container).
- **The bundled DuckDB web UI was removed** (it had no authentication). Use the console's **Tables & Data** page instead.
- **Raw SQL is now superuser-only.** `POST /query`, the GraphQL `sql` query and graph create/delete/query reject non-superuser keys with HTTP 403 (since then, `/query` and GraphQL `sql` accept non-superusers again, scoped to the tables they can read) (default `API_KEYS` keys are superusers, so single-key setups are unaffected). `SQL_READ_ONLY` was removed; user-supplied SQL is always read-only. See [§3.5](#35-sql-enforcement).
- **Tested path:** 1.5.4 → 2.0.0 (tables, blobs, schema-evolved columns and registered property graphs all survive). Upgrading directly from older releases (DuckDB 1.5.0) passes through additional catalog migrations that have not been verified; take a backup first.
- **Rebuild the image** (`docker compose up -d --build --remove-orphans`; `--remove-orphans` also removes the retired `duckdb-ui` container from older releases). DuckDB extensions (`ducklake`, `postgres`, `duckpgq`) are pre-installed at image build time for the pinned DuckDB version, so containers never download them at startup.

### 4.6 Object Storage (S3 / MinIO)

Set `DUCKLAKE_DATA_PATH` to an `s3://bucket/prefix/` URL (or choose **S3-compatible** on the console's Storage page, [§13.2](#132-features)) and the gateway writes all Parquet files there. Postgres stays the catalog. Both DuckDB instances (primary and user-query) get a temporary DuckDB S3 secret **scoped to that prefix**, created from the `S3_*` variables ([§3.2.1](#321-object-storage-s3--minio)). Nothing is written to disk.

**Bundled MinIO (single host / trying it out).** In `docker/.env`:

```sh
COMPOSE_PROFILES=minio                  # also start the minio + minio-init services
MINIO_ROOT_USER=ichibi-lake
MINIO_ROOT_PASSWORD=<a long random secret>
MINIO_BUCKET=ichibi-lake                # created by minio-init (idempotent)

DUCKLAKE_DATA_PATH=s3://ichibi-lake/lake/
S3_ENDPOINT=minio:9000
S3_ACCESS_KEY=ichibi-lake               # = MINIO_ROOT_USER (or a dedicated MinIO user)
S3_SECRET_KEY=<the same secret>
S3_USE_SSL=false
```

Then `docker compose up -d --build`. MinIO keeps its data in `docker/data/minio/`; its web console is on `http://localhost:9001` (bound to `127.0.0.1`; see `MINIO_BIND_ADDRESS`). The `minio/minio` images are no longer published on Docker Hub or quay.io, so the profile uses Chainguard's build (`cgr.dev/chainguard/minio`, set `MINIO_IMAGE` to use another).

**AWS S3.** Leave `S3_ENDPOINT` empty and set `S3_REGION`. Either set `S3_ACCESS_KEY`/`S3_SECRET_KEY`, or leave them empty to use the credential chain (instance/task role). The IAM policy needs `s3:ListBucket` on the bucket and `s3:GetObject`, `s3:PutObject` and `s3:DeleteObject` on `prefix/*`.

**Other S3-compatible stores** (R2, Ceph, Wasabi, GCS interoperability): `S3_ENDPOINT` = the provider's host, plus `S3_URL_STYLE` as the provider documents.

Notes:

- **Start new lakes on object storage.** The data path is recorded in the catalog (`ducklake_metadata`, key `data_path`) when the lake is created, and changing `DUCKLAKE_DATA_PATH` later does not move existing files. Migrating an existing lake (copy `docker/data/ducklake/` to the bucket prefix with the gateway stopped, then repoint the catalog's `data_path`) is a manual operation that has not been verified with this release; take a Postgres backup first and test on a copy.
- The console's own catalog (`ichibi_console`) keeps every row in Postgres; its data path is `<prefix>_console/`, which stays empty.
- **BLOB files** (`USE_FILESYSTEM_BLOBS=1`) stay on the local `BLOBS_PATH` directory ([§3.4](#34-blob-storage)).
- All gateway instances must use the same `DUCKLAKE_DATA_PATH` and catalog. Object storage removes the shared-filesystem requirement of multi-instance deployments.
- **Manual check** (what the integration run against the minio profile does): `docker compose --profile minio up -d --build`, then `npm test`, and confirm that new files appear under `docker/data/minio/<bucket>/lake/main/<table>/` and that the gateway log shows `data: s3://... on http://minio:9000`.

### 4.7 Lake Maintenance (Small Files)

Many small writes (one row per REST request or Kafka message) would otherwise create one tiny Parquet file each, and query speed drops as file counts grow. ichibi-lake prevents this in two layers:

1. **Data inlining.** A write of fewer than `DATA_INLINING_ROW_LIMIT` (default 1000) rows is stored in the Postgres catalog. It is immediately visible to every query, but creates no file.
2. **Scheduled maintenance** (console: **Retention & Pruning → Lake maintenance**; default hourly, `0 * * * *` UTC). Each run:
   - **flushes** inlined rows to Parquet,
   - **merges** adjacent small files per table,
   - **rewrites** files with many deleted rows (threshold configurable; DuckLake's default when empty),
   - **expires** snapshots older than `SNAPSHOT_RETENTION_DAYS` (only if set),
   - **deletes** files no snapshot references, and orphaned files, after a 5-minute grace period (so running queries and in-flight writes on other instances are not affected).

   Each step can be switched off. A run also happens at gateway start, unless one finished in the last hour (this replaces the old unconditional startup compaction).

Runs are coordinated across gateway instances. A due run is claimed with a write-token-and-verify step and a lease (6 hours, released when the run ends, or when the instance that held it restarts), so exactly one instance runs it and runs never overlap. **Run now** is refused (HTTP 409) while a run is in progress anywhere. Every run is logged on the card with before/after data-file counts and sizes, and in the **Audit Log** (`maintenance.run`; settings changes as `maintenance.update`). Merge and rewrite use a fresh connection per table to avoid the DuckLake schema-cache issue duckdb/ducklake#1108 (see `package.json` `_comments`). `DISABLE_DUCKLAKE_COMPACTION=1` stops all runs.

Console API (admin session): `GET /console/api/maintenance` (settings, recent runs, current file counts), `PUT /console/api/maintenance` (`{ enabled, schedule, steps: { flush, merge, rewrite, expire, cleanup }, deleteThreshold, runAtStartup }`) and `POST /console/api/maintenance/run`.

## 5. Authentication & Authorization

### 5.1 API Key Authentication

All API endpoints require a valid API key passed as an HTTP header. The exceptions are the Swagger UI (`/`), the OpenAPI spec (`/swagger.yaml`), the health checks (`/health`, `/health/ready`), `/metrics` (superuser key or `METRICS_TOKEN`, see [§14.2](#142-prometheus-metrics)) and the admin console (`/console`), which has its own logins (see [§13](#13-admin-console)):

- **Header**: `x-api-key`
- **Example**: `x-api-key: ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13`
- **Unauthorized**: HTTP 401

Configure valid keys via one or both of the following mechanisms (they are **complementary**, not exclusive):

1. **Legacy `API_KEYS` / `API_KEY` env vars** — keys from these environment variables are accepted at authentication time. When **no** RBAC variables are set, they are also promoted to **superusers** in the RBAC tables on startup.
2. **RBAC environment variables** (`RBAC_SUPERUSERS`, `RBAC_USERS`, etc.) — see [§5.3](#53-role-based-access-control-rbac).

The authentication flow is: **RBAC tables first**, then fall back to the legacy `API_KEYS` env-var set. This means admin-created RBAC users can authenticate even without being listed in `API_KEYS`.

### 5.2 Per-Table Access Control (ACL)

Each table has an ACL with two roles:

| Role | Permission | Allowed Operations |
| :--- | :--- | :--- |
| **Owner** | Read + Write | Read rows, upload, blob, patch, delete, manage ACL |
| **Reader** | Read only | `GET /tables/:name`, `GET /tables/:name/schema`, blob downloads |
| *(none)* | No access | HTTP 403 |

- The API key that **creates** a table is automatically added as an owner.
- The internal Kafka consumer registers tables under the `kafka-internal` key.
- ACLs are cached in memory (configurable `ACL_CACHE_TTL_MS`, default 60s).

**Migration from the legacy `_api_key` column model:** On startup, the gateway automatically migrates existing tables — distinct `_api_key` values are promoted to ACL owners, and the column is dropped.

**ACL + RBAC coexistence:** The legacy ACL system (`_table_acls` DuckDB table) is checked as a fallback if no RBAC permission is found. This ensures backward compatibility for tables that predate the RBAC system.

### 5.3 Role-Based Access Control (RBAC)

The RBAC system adds fine-grained role- and per-table-based authorization on top of the legacy API key authentication. It stores roles and table permissions in DuckDB (`_rbac_users`, `_rbac_user_roles`, `_rbac_table_permissions`) and is persisted across restarts via DuckLake.

#### Roles

| Role | Read Access | Write Access | Notes |
| :--- | :--- | :--- | :--- |
| **superuser** | All tables | All tables | Bypasses all per-table restrictions unless an explicit `deny` is set. Can manage users and permissions via the admin API. |
| **writer** | All tables | All tables (unless denied) | Has global read + write access to every table. |
| **reader** | All tables | None (unless granted via per-table `allow_write`) | Has global read access to every table. |

"All tables" means user tables, through the table REST endpoints and through SQL: writers and readers may run read-only `SELECT`s via `/query` and GraphQL `sql` on exactly the tables they can read (a per-table `deny` applies to SQL too, including tables reached through a join, subquery or CTE). Superusers may run any read-only SQL. Graph management and graph queries are **superuser-only**, and system tables (`_`-prefixed) are never accessible to non-superusers; see [§3.5](#35-sql-enforcement).

#### Per-Table Permission Overrides

Permissions override the role-based default for a specific user + table combination:

| Permission | Effect |
| :--- | :--- |
| `allow_read` | Grants read access (redundant for writers/readers; useful for keys with no RBAC role) |
| `allow_write` | Grants write access (useful for upgrading a reader on a specific table) |
| `deny` | Denies all access to the table, even for superusers |

#### Admin API

The RBAC admin API is mounted at `/admin/` and requires superuser authentication:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/admin/users` | List all users |
| `POST` | `/admin/users` | Create a user (`{ apiKey, name, email, roles }`) |
| `GET` | `/admin/users/:apiKey` | Get user details |
| `PATCH` | `/admin/users/:apiKey` | Update user fields (`{ name, email, is_active }`) |
| `DELETE` | `/admin/users/:apiKey` | Delete a user |
| `PUT` | `/admin/users/:apiKey/roles` | Set roles (`{ roles: ["writer"] }`) |
| `GET` | `/admin/tables/:tableName/permissions` | List all permissions for a table |
| `PUT` | `/admin/tables/:tableName/permissions` | Set a user's permission (`{ apiKey, permission: "deny" }`) |
| `GET` | `/admin/check` | Self-service: check the caller's own access to a table (`?table=...`) |

#### Coexistence with Legacy `API_KEYS`

- `API_KEYS` and RBAC are **complementary**. You can use one, the other, or both.
- At startup the gateway runs **one** bootstrap: if `RBAC_SUPERUSERS` or `RBAC_USERS` is set, `bootstrapRbac()` syncs those users; otherwise `legacyBootstrap()` promotes every `API_KEYS` key to **superuser**. Existing single-key deployments keep working without configuration changes.
- The authentication middleware tries RBAC first (`isValidApiKey()`), then falls back to the legacy `API_KEYS` env-var `Set`. Keys from either source are accepted.
- ACL data access functions (`checkReadAccess`, `checkWriteAccess`) delegate to RBAC first; if RBAC returns `false` or the RBAC tables are not initialized, they fall through to the legacy `_table_acls` system for backward compatibility.

#### Example: Env-Var-Only Mode (Backward Compatible)

Set only `API_KEYS` — all keys become superusers automatically:

```sh
API_KEYS="key1,key2"
```

No RBAC env vars needed. The legacy bootstrap promotes both keys to superusers.

#### Example: RBAC-Only Mode

```sh
RBAC_SUPERUSERS="sk-super-01"
RBAC_USERS='[
  {"apiKey":"sk-writer-01","name":"Alice","email":"alice@example.com","roles":["writer"]},
  {"apiKey":"sk-reader-01","name":"Bob","email":"bob@example.com","roles":["reader"]}
]'
RBAC_TABLE_PERMISSIONS='[
  {"apiKey":"sk-reader-01","tableName":"payments","permission":"deny"}
]'
```

Users defined in env vars are re-applied (re-activated, roles re-added) on every restart, so change them in the environment; the admin console shows them read-only. Users created through the admin API or the console are fully manageable at runtime.

#### Example: Hybrid Mode

```sh
API_KEYS="sk-deploy-key"                          # authenticates, but is NOT a superuser here
RBAC_SUPERUSERS="sk-admin-01"                     # superuser
RBAC_USERS='[{"apiKey":"sk-writer-01","name":"Writer","roles":["writer"]}]'
```

All three keys authenticate. Because RBAC variables are set, the legacy bootstrap does not run: `sk-admin-01` is a superuser and `sk-writer-01` can write to all tables, but `sk-deploy-key` has no RBAC role. It can only reach tables whose ACL lists it (for example tables it created) and cannot use superuser-only features such as property graphs; its `/query` SQL is limited to those tables. To make it a superuser, add it to `RBAC_SUPERUSERS` as well.

### 5.4 ACL Management Endpoints

**Get current ACL:**

```sh
curl -H "x-api-key: YOUR_KEY" \
  "http://ichibi-lake:3333/tables/my_table/acl"
```

*Response:* `{ "success": true, "ownerKeys": ["key1"], "readerKeys": ["key2"] }`

**Update ACL (requires owner access):**

```sh
curl -X PATCH "http://ichibi-lake:3333/tables/my_table/acl" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"ownerKeys": ["key1"], "readerKeys": ["key2", "key3"]}'
```

---

## 6. Binary (BLOB) Asset Management

### 6.1 Blob Storage Backend

Controlled by `USE_FILESYSTEM_BLOBS`:

| Mode | `USE_FILESYSTEM_BLOBS` | Where BLOBs Live | Table Column Type |
| :--- | :--- | :--- | :--- |
| **Filesystem (recommended)** | `1` | `./data/blobs/` (bind mount, `BLOBS_PATH`) | `VARCHAR` — stores the file path |
| **Inline Parquet** | `0` / unset | Inside Parquet files | `BLOB` — embedded as `BYTE_ARRAY` |

Filesystem mode is recommended: binary files remain independent of the Parquet catalog, simplifying inspection, backup, and migration.

### 6.2 Uploading BLOBs

Upload raw binary data with optional metadata via query parameters:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/blobs/:blobColumn` | Default table, auto-generated UUID |
| `POST` | `/blobs/:idValue/:blobColumn` | Default table, specific ID |
| `POST` | `/tables/:tableName/blobs/:blobColumn` | Custom table, auto-generated UUID |
| `POST` | `/tables/:tableName/blobs/:idValue/:blobColumn` | Custom table, specific ID |

**Key Features:**
- **Dynamic Metadata** — Attach arbitrary properties via query parameters (`?owner=sara&type=profile`). These become columns automatically.
- **Auto-Schema** — Tables and columns are created on first write if they don't exist.
- **Auto-Partitioning** — New tables are partitioned by the column set in `DEFAULT_PARTITION_KEY` (default: `created`).

```sh
curl -X POST "http://ichibi-lake:3333/blobs/avatar?owner=marwan&importance=high" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/octet-stream" \
  --data-binary "@/path/to/image.png"
```

### 6.3 Downloading BLOBs

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/blobs/:idValue/:blobColumn` | Default table |
| `GET` | `/tables/:tableName/blobs/:idValue/:blobColumn` | Custom table |

```sh
curl -H "x-api-key: YOUR_KEY" \
  "http://ichibi-lake:3333/blobs/6bbf0bd4-.../avatar" \
  --output my_download.png
```

### 6.4 Deleting BLOBs

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `DELETE` | `/blobs/:idValue/:blobColumn` | Default table |
| `DELETE` | `/tables/:tableName/blobs/:idValue/:blobColumn` | Custom table |

Removes the backing file (in filesystem mode) and deletes the record.

---

## 7. Record Metadata Updates

Update any record's columns atomically:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `PATCH` | `/records/:idValue` | Default table (`ichibi_table`) |
| `PATCH` | `/tables/:tableName/records/:idValue` | Custom table |

- Only the fields in the JSON body are changed (partial update).
- Unknown fields automatically create a `VARCHAR` column (dynamic schema).
- Requires **owner** access.

```sh
curl -X PATCH "http://ichibi-lake:3333/records/6bbf0bd4-..." \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"status": "verified", "owner": "ahmad", "verified_at": "2024-04-10"}'
```

---

## 8. Data Discovery & REST API

Query and browse the data lake with standard REST patterns.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/tables` | List the tables the caller can read (system tables are never listed) |
| `GET` | `/tables/:tableName/schema` | Get column names and types |
| `GET` | `/tables/:tableName` | Query table data with filters, sorting, pagination |
| `GET` | `/tables/:tableName/export` | Download the (filtered, sorted) table as Parquet, CSV or NDJSON (see [§8.1](#81-exporting-tables)) |

**Query parameters for `GET /tables/:tableName`:**

| Parameter | Example | Description |
| :--- | :--- | :--- |
| `?<column>=<value>` | `?owner=marwan` | Filter by column value (equality); repeat for several columns |
| `?limit=` | `?limit=100` | Max rows (default: **50**) |
| `?sort=` | `?sort=-created_at` | Sort by column; prefix `-` for descending |
| `?offset=` | `?offset=50` | Pagination offset |
| `?after=` | `?after=2024-06-01T10:00:00` | Keyset pagination: rows after this value of the `sort` column (requires `sort`). Faster than `offset` for deep pages |

*Response:* `{ "success": true, "tableName": "...", "rowCount": 10, "rows": [...], "nextCursor": "..." }`. With `?sort=`, `nextCursor` is the last row's sort value; pass it as `?after=` to fetch the next page.

Access is checked per table (RBAC role, per-table permission, then ACL); system tables (`_`-prefixed) return 404 for every key.

```sh
curl -H "x-api-key: YOUR_KEY" \
  "http://ichibi-lake:3333/tables/ichibi_table?owner=marwan&limit=10&sort=-created_at"
```

`GET /tables/:tableName` is for browsing and paging. To take whole tables out of the lake use the export endpoint below, and for ad-hoc SQL over large results use NDJSON streaming on `POST /query` ([§9.3](#93-raw-sql-query)).

### 8.1 Exporting Tables

`GET /tables/:tableName/export?format=parquet|csv|ndjson` streams the table to the client as a file download (`Content-Disposition: attachment; filename="<table>.<ext>"`). It needs read access to the table, exactly like `GET /tables/:tableName` (readers can export; keys without access get 403; system tables return 404), and accepts the same `?<column>=<value>` filters, `?sort=` and `?after=`.

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `?format=` | `parquet` | `parquet` (`application/vnd.apache.parquet`, zstd-compressed), `csv` (`text/csv`, header row, RFC 4180 quoting) or `ndjson` (`application/x-ndjson`, one JSON object per line) |
| `?limit=` | `EXPORT_MAX_ROWS` | Lower the row cap for this request (it cannot raise it) |
| `?<column>=`, `?sort=`, `?after=` | — | As for `GET /tables/:tableName` |

Response headers report the size before the body starts, so a client can tell whether it received everything:

| Header | Meaning |
| :--- | :--- |
| `X-Total-Rows` | Rows matching the filters |
| `X-Row-Count` | Rows in this file (`min(X-Total-Rows, cap)`) |
| `X-Export-Truncated` | `true` when the cap cut the export short — narrow the filters or page with `?sort=` + `?after=` |
| `X-Row-Cap` | The cap that applied |

The count and the rows are read in one transaction, so they come from the same snapshot.

```sh
curl -H "x-api-key: YOUR_KEY" -OJ \
  "http://ichibi-lake:3333/tables/ichibi_table/export?format=parquet&owner=marwan"
```

**How the bytes are produced, and size limits:**

- **CSV and NDJSON** are streamed straight from DuckDB in chunks of ~2,048 rows with HTTP backpressure, so the gateway never holds more than a chunk and nothing is written to disk. Values use DuckDB's text form (timestamps like `2024-02-03 04:05:06`); NDJSON uses DuckDB's `to_json()`, so nested types are real JSON. BLOB columns are base64-encoded, as in the JSON API.
- **Parquet** cannot be streamed (its footer is written last), so DuckDB writes the file to `EXPORT_TMP_DIR`, the gateway streams it with a `Content-Length` and deletes it when the response ends or fails. Temporary disk use is therefore at most `EXPORT_CONCURRENCY` capped exports at a time.
- Exports run on the primary DuckDB instance (the locked-down user-query instance cannot write files) on a dedicated connection, so a slow download does not hold a pooled connection. They are limited to `EXPORT_CONCURRENCY` at once (HTTP 429 with `Retry-After` beyond that), `EXPORT_TIMEOUT_MS` each (the connection is closed mid-download if exceeded), and `EXPORT_MAX_ROWS` rows. Disconnecting interrupts the query.

---

## 9. Event Ingestion

### 9.1 JSON Upload

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/upload` | Upload JSON array to `ichibi_table` |
| `POST` | `/upload/:tableName` | Upload JSON array to a specific table |

New tables are created automatically with type inference; the caller is registered as an ACL owner. New columns in later uploads are added automatically (schema evolution). Batches larger than `MAX_BATCH_SIZE` are rejected, and table names starting with `_` (reserved for system tables) return 404. Send an `Idempotency-Key` header to make retries safe ([§9.5](#95-idempotent-retries)). For files and large loads use the bulk import endpoint ([§9.4](#94-bulk-import-files--object-storage)).

```sh
curl -X POST "http://ichibi-lake:3333/upload/user_stats" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '[{"user": "duck", "points": 100}, {"user": "lake", "points": 250}]'
```

By default every row is appended. `?mode=upsert` and `?mode=dedupe` write by key instead ([§9.2](#92-upserts--deduplication-key-columns)).

### 9.2 Upserts & Deduplication (Key Columns)

Appending suits events; entities whose rows change over time (customers, devices, dimension tables) need writes by key. A table can declare one or more **key columns**, and `POST /upload/:tableName` and `POST /kafka-sink` take a `mode`:

| `mode` | Effect |
| :--- | :--- |
| `append` (default) | Insert every row. |
| `upsert` | Replace the rows whose key matches an incoming row; insert rows with a new key. |
| `dedupe` | Insert only rows whose key does not exist yet; rows with an existing key are skipped. |

**Declaring keys**

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/tables/:tableName/keys` | Declared key columns (read access). Also returned as `keys` by `GET /tables/:tableName/schema`. |
| `PUT` | `/tables/:tableName/keys` | Body `{"keys": ["region", "device_id"]}` declares a (composite) key; `{"keys": []}` clears it. Requires write access (table owner or RBAC writer). |

```sh
curl -X PUT "http://ichibi-lake:3333/tables/customers/keys" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '{"keys": ["customer_id"]}'
# {"success":true,"tableName":"customers","keys":["customer_id"],"duplicateKeys":0}
```

Key columns must exist and be comparable by value (not STRUCT, LIST, MAP, JSON or BLOB). The response reports `duplicateKeys`, the number of key values that already occur more than once (plus a `warning`); an upsert replaces *all* rows with a matching key, so such duplicates disappear as their keys are written again. Keys can also be declared on the first keyed write with `?key=`: `?key=customer_id` or `?key=region,device_id` (in key order). This is the only way to upsert into a table that does not exist yet; the table is then created from the batch with that key. Keys live in the `_table_keys` system table, so every gateway instance shares them. In the admin console they are set on a table's **Schema** tab ([§13.2](#132-features)). DuckLake has no unique constraints: keys are maintained by the keyed modes, not enforced. `mode=append`, `PATCH /tables/:tableName/records/:id`, and the console's **Add row** and **Import** can still write a key that exists.

**Upsert**

```sh
curl -X POST "http://ichibi-lake:3333/upload/customers?mode=upsert" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '[{"customer_id": 2, "name": "Bea", "tier": "gold"}, {"customer_id": 3, "name": "Cy"}]'
# {"success":true,"message":"Upserted 2 records into customers: 1 inserted, 1 updated",
#  "mode":"upsert","keys":["customer_id"],"received":2,"inserted":1,"updated":1,"skipped":0,"duplicates":0}
```

- **Whole-row replace.** A matching row is replaced by the incoming row: columns the incoming row does not carry become NULL. Send the full entity, not a partial patch (use `PATCH /tables/:tableName/records/:id` to change single fields).
- **`created` / `updated`.** When the table has a `created` and/or `updated` column (TIMESTAMP, DATE or VARCHAR), a replaced row keeps its existing `created` value, `updated` is set to the current time, and inserted rows get the current time in both (an incoming `created` is used for new rows).
- **Duplicates within a batch.** The last occurrence of a key in the batch wins. With `?orderBy=<column>` the row with the highest value of that column wins instead (ties: the later row), e.g. `?orderBy=version` or `?orderBy=event_time`. Collapsed rows are reported as `duplicates`.
- **Schema evolution** works as for appends: new columns are added and STRUCT columns widened before the write.
- **Validation (HTTP 400, nothing written).** A batch without a key column, a row with a NULL key value, an `orderBy` column missing from the batch, a keyed write to a table with no declared keys and no `?key=`, or an unknown `mode`. A `?key=` that differs from the table's declared keys returns 409.
- **RBAC.** Keyed writes need the same write access as appends; readers get 403.

**Dedupe**

```sh
curl -X POST "http://ichibi-lake:3333/upload/events?mode=dedupe&key=event_id" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '[{"event_id": "e1", "kind": "click"}, {"event_id": "e1", "kind": "click"}]'
# ..."inserted":1,"skipped":0,"duplicates":1
```

Only rows whose key is not in the table yet are inserted (`skipped` counts the others); existing rows are never changed. Within a batch the **first** occurrence of a key wins (or the highest `?orderBy=` value). Use it for idempotent re-delivery: replaying a batch inserts nothing new.

**`/kafka-sink`** takes the same `mode`, `key` and `orderBy` query parameters. The key refers to fields of the flattened record value, or to `_kafka_key` to key on the Kafka message key:

```sh
curl -X POST "http://ichibi-lake:3333/kafka-sink?topic=devices&mode=upsert&key=_kafka_key" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '[{"key": "dev-1", "value": {"fw": "2.1", "online": true}}]'
```

**How it works & concurrency.** A keyed batch is spooled to a temp file, deduplicated with a window function and written with one `MERGE INTO` (DuckDB 2.0 supports it on DuckLake tables) inside a transaction on one connection, so it is all-or-nothing. DuckLake commits optimistically and detects conflicts per **table**: when two keyed writes to the same table overlap (from this or another gateway instance, even with disjoint keys), one commits and the other fails with a DuckLake transaction conflict, is rolled back and retried, up to `INGEST_CONFLICT_RETRIES` times (default 10, jittered backoff, editable live on the console's **Ingestion** page). If every attempt loses, the request returns **HTTP 409** and nothing from the batch was written, so retry it; rows are never silently lost or half-written, and keys never end up duplicated by concurrent keyed writes. Writes to one table are therefore serialised: throughput comes from larger batches, not from many concurrent small upserts to the same table. Keyed and plain writes do not protect each other: a concurrent `mode=append` that commits after an upsert does not conflict with it and can add a row with an existing key. Each keyed write deletes and re-inserts the matched rows, so frequently upserted tables accumulate delete files; scheduled lake maintenance ([§4.7](#47-lake-maintenance-small-files)) rewrites them.

### 9.3 Raw SQL Query

```sh
curl -X POST "http://ichibi-lake:3333/query" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT category, count(*) FROM ichibi_table GROUP BY 1"}'
```

Any key may query: superusers run any read-only SQL; other keys may run a single `SELECT` over the tables they can read, and get HTTP 403 naming the first table they cannot (see [§3.5](#35-sql-enforcement) for what else is rejected). User-supplied SQL is always read-only: DDL/DML statements and connection-revealing functions are rejected with HTTP 403 (see [§3.5](#35-sql-enforcement)). Queries run on the isolated user-query instance, at most `USER_QUERY_CONCURRENCY` at once (the rest queue).

Results are read from DuckDB chunk by chunk and capped, so a large result can no longer exhaust the gateway's memory. Two response formats:

**JSON (default)** — `{ "success": true, "rows": [...], "rowCount": 100, "truncated": false, "maxRows": 100000 }`. At most `QUERY_MAX_ROWS` rows within `USER_QUERY_TIMEOUT_MS`. When the result had more rows, `truncated` is `true` and the response carries `X-Result-Truncated: true`.

**NDJSON (streaming)** — request it with `?format=ndjson` or `Accept: application/x-ndjson`. Each row is written as soon as DuckDB produces it, one JSON object per line, and the last line is always a trailer:

```sh
curl -N -X POST "http://ichibi-lake:3333/query?format=ndjson" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM ichibi_table"}'
```

```
{"id":"1","owner":"marwan",...}
{"id":"2","owner":"ahmad",...}
{"_ichibi":{"rowCount":2,"truncated":false,"maxRows":10000000}}
```

At most `QUERY_STREAM_MAX_ROWS` rows (`X-Row-Cap` header), within `USER_QUERY_STREAM_TIMEOUT_MS` for the whole transfer. Errors before the first row (bad SQL, unknown table) still return a JSON error with the usual status code; an error after streaming started (for example the timeout) ends the body with `{"_ichibi":{"error":"...","status":504}}` instead of the normal trailer, so a response without a trailer line was cut off. The gateway applies backpressure: it fetches the next chunk only when the client has read the previous one, and a client disconnect interrupts the query and frees its slot.

Both formats accept `"maxRows": <n>` in the body to lower (never raise) the cap. Values serialise identically in both: BIGINTs as strings, BLOBs as base64, other DuckDB types (timestamps, lists, structs) as DuckDB's JSON value objects, as before. Arrow IPC output is not offered yet; use the Parquet table export for typed bulk data. Memory note: blocking operators (`ORDER BY`, `GROUP BY`, `DISTINCT`) still need DuckDB working memory for their input before the first row is produced; streaming bounds the gateway's memory, not DuckDB's.

### 9.4 Bulk Import (Files & Object Storage)

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/tables/:tableName/import` | Load a CSV, Parquet, NDJSON or JSON-array file (raw request body), or objects from S3 |

**Uploading a file.** Send the file itself as the request body (not multipart). The gateway streams it to a temp file (`IMPORT_TMP_DIR`), so the size is bounded by `IMPORT_MAX_MB` (HTTP 413 above it), not by memory or `JSON_BODY_LIMIT`, then loads it with DuckDB: `read_csv` (header, delimiter and types auto-detected), `read_parquet` or `read_json_auto` (a JSON array or one object per line). The format comes from `?format=csv|parquet|json|ndjson`, or else from the `Content-Type` (`text/csv`, `application/vnd.apache.parquet`, `application/json`, `application/x-ndjson`).

```sh
curl -X POST "http://ichibi-lake:3333/tables/sales/import" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: text/csv" \
  --data-binary @sales-2026-10.csv

curl -X POST "http://ichibi-lake:3333/tables/events/import?mode=append" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/vnd.apache.parquet" \
  --data-binary @events.parquet
```

Use `--data-binary` (not `-d`, which strips newlines).

**Modes** (`?mode=`):

| Mode | Table missing | Table exists |
| :--- | :--- | :--- |
| `auto` (default) | create | append |
| `create` | create | 409 |
| `append` | 404 | append |
| `replace` | create | delete all rows and insert the new ones, in one transaction |

A table created by an import gets the caller as its ACL owner and the default partitioning (`DEFAULT_PARTITION_KEY`), as with `/upload`. `append` and `replace` follow `/upload`'s schema-evolution rules (`src/schema-evolution.js`): new columns are added (or skipped with `?addColumns=false`), STRUCT columns are widened, values are cast to the table's column types, and columns that cannot be evolved are skipped and listed in `skipped`. `created`/`updated` timestamp columns missing from the file are filled with the import time. `replace` keeps the table's columns, ACL and partitioning. The insert is one statement, so a row that cannot be converted fails the whole import and nothing is written. `?dryRun=true` only reads the data and returns the detected columns, row count, a 20-row preview and the per-column plan.

```json
{ "success": true, "table": "events", "mode": "append", "format": "parquet", "inserted": 120000,
  "added": ["region"], "widened": [], "skipped": [] }
```

**Loading from object storage.** Send a small JSON object with a `source` instead of data:

```sh
curl -X POST "http://ichibi-lake:3333/tables/events/import" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -d '{"source": "s3://landing/events/2026-10/*.parquet", "mode": "append"}'
```

The body may also carry `format`, `mode`, `addColumns` and `dryRun` (taking precedence over the query string). The format defaults to the source's file extension. Globs over several files are combined by column name. Objects are read on the primary DuckDB instance with the gateway's own S3 settings (`S3_ENDPOINT`, `S3_REGION`, `S3_ACCESS_KEY`/`S3_SECRET_KEY` or the AWS credential chain, `S3_URL_STYLE`, `S3_USE_SSL`; see [§4.6](#46-object-storage-s3--minio)), also when the lake itself is on local disk.

Source rules:
- Only `s3://bucket/key` URLs (a file or a glob) are accepted. Local paths, `file://`, `http(s)://` and other schemes, `.`/`..` segments, quotes and control characters are rejected with 400, for every key, so the endpoint cannot be used to read the gateway's files or to make it fetch arbitrary URLs.
- Writers may import only from prefixes in `IMPORT_ALLOWED_SOURCES` (e.g. `s3://landing/events/,s3://partner-drop/`); everything a glob can match must lie under an entry. Any other `s3://` source needs a superuser key (403 otherwise).
- The lake's own data path (`DUCKLAKE_DATA_PATH`) never counts as allow-listed: reading its Parquet files directly would bypass table ACLs.

Requires write access to the target table (readers get 403); `_`-prefixed system tables return 404. The admin console's **Import** ([§13.2](#132-features)) uses the same import code. Supports `Idempotency-Key` ([§9.5](#95-idempotent-retries)).

### 9.5 Idempotent Retries

`POST /upload`, `POST /kafka-sink` and `POST /tables/:tableName/import` accept an `Idempotency-Key` header (1-255 printable ASCII characters, e.g. a UUID per batch). Retry a failed or timed-out request with the same key:

- The first request with a key runs normally. When it succeeds (2xx), its status and response are stored.
- A later request with the same key and the same request (method, path, query, body, and for `/kafka-sink` the `X-Kafka-Topic` header) returns the stored response with `Idempotent-Replayed: true` and writes nothing.
- The same key with a different request is 409. While the first request is still running, a retry gets 409 with `Retry-After`.
- A request that fails (non-2xx, or the client disconnects) releases its key, so a retry runs normally.

Keys are scoped to the API key that sends them and kept for `IDEMPOTENCY_TTL_HOURS` (default 24). They are stored in the console catalog (`_console_idempotency`), so every gateway instance on the same catalog sees them; when two instances receive the same key at the same moment, only the first claim runs. A claim whose process died mid-request stops blocking its key after one hour.

```sh
curl -X POST "http://ichibi-lake:3333/upload/user_stats" \
  -H "x-api-key: YOUR_KEY" -H "Content-Type: application/json" \
  -H "Idempotency-Key: 7d1c0a52-batch-0042" \
  -d '[{"user": "duck", "points": 100}]'
```

---

## 10. Kafka Integration

### 10.1 HTTP Sink (`POST /kafka-sink`)

Designed for Kafka Connect: accepts an array of Kafka records, flattens each record's `value` into columns (plus `_ingest_timestamp` and `_kafka_key`), and appends them to a table named after the topic (creating it, and evolving its schema, as needed). The topic is taken from the `X-Kafka-Topic` header, else the `?topic=` query parameter, else `ichibi_table`. The caller's API key becomes the ACL owner of tables it creates; topic names starting with `_` are rejected with HTTP 400.

```sh
curl -X POST "http://ichibi-lake:3333/kafka-sink" \
  -H "x-api-key: YOUR_KEY" \
  -H "X-Kafka-Topic: logs" \
  -d '[{"value": {"level": "info", "msg": "started"}}]'
```

### 10.2 Internal Background Consumer

Enable by setting `KAFKA_BROKERS` in the environment. The consumer automatically ingests events from topics matching `KAFKA_TOPIC_REGEX` into DuckDB tables (one table per topic, registered under the `kafka-internal` ACL key). Kafka's internal `__` topics are never subscribed, and topics whose table name would start with `_` are skipped.

By default each batch is appended. `KAFKA_INGEST_MODES` (also editable live on the console's **Kafka** settings page) switches topics to key-based ingestion ([§9.2](#92-upserts--deduplication-key-columns)): comma-separated `topic=mode[:key1+key2]` entries, where `topic` is the table name (non-alphanumeric characters replaced by `_`) and `*` sets the default for every other topic:

```sh
KAFKA_INGEST_MODES=customers=upsert:customer_id,devices=upsert:_kafka_key,clicks=dedupe:event_id+ts
```

The keys in the setting are declared on the table when it has none yet (for example when the first batch creates it); keys declared with `PUT /tables/:tableName/keys` or the console take precedence. Within a batch the last message for a key wins (first for `dedupe`). Conflicting concurrent writes are retried as in [§9.2](#92-upserts--deduplication-key-columns); a batch that still fails (including a batch with a missing or NULL key) is logged as `[Kafka Consumer Error]` and not written, as for any other failed batch.

Configure via environment variables in [§3.7](#37-kafka-integration).

### 10.3 SSE Subscription (`GET /kafka-subscribe/:topicName`)

Subscribe to a Kafka topic in real-time via Server-Sent Events. Requires `KAFKA_BROKERS` to be set.

```sh
curl -H "x-api-key: YOUR_KEY" -N \
  "http://ichibi-lake:3333/kafka-subscribe/my-topic"
```

Each event is streamed as an SSE `data:` field:

```
data: {"topic":"my-topic","partition":0,"offset":"42","key":"mykey","value":"{\"msg\":\"hello\"}","timestamp":"1712345678000"}
```

**Parameters:**

| Parameter | Default | Description |
| :--- | :--- | :--- |
| `?groupId=` | `ichibi-lake-sse-<sanitized-api-key>-<random>` | Consumer group. By default each connection gets its own group, so concurrent subscribers do not trigger rebalances; pass a fixed value to share delivery |
| `?since=` | *(all data)* | Publish DuckDB rows newer than this timestamp before subscribing. Supports absolute ISO 8601 (`2024-06-09T10:00:00Z`), relative duration (`5m`, `1h`, `2d`), or plain number in minutes (`5`). |
| `?sinceColumn=` | `updated` | Column name to filter `?since=` on |

---

## 11. GraphQL

A GraphQL endpoint is available at `/graphql` (`GET` and `POST`, via `graphql-http`) with two queries:

| Query | Returns | Access |
| :--- | :--- | :--- |
| `tables` | Names of user tables (system tables are hidden) | Any valid API key |
| `sql(query: String!)` | Rows of a read-only SQL query, as JSON | Same rules as `POST /query`: any SQL for superusers, table-scoped `SELECT`s for other keys ([§3.5](#35-sql-enforcement)) |

```sh
curl -X POST "http://ichibi-lake:3333/graphql" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"query": "{ tables sql(query: \"SELECT count(*) AS n FROM ichibi_table\") }"}'
```

*Response:* `{ "data": { "tables": ["ichibi_table", ...], "sql": [{ "n": "42" }] } }`. Errors (for example a reader querying a table it is denied) are returned in the standard GraphQL `errors` array.

---

## 12. Graph Queries (SQL/PGQ)

ichibi-lake supports graph property queries via the **DuckPGQ** community extension (SQL/PGQ). Define named graphs over your relational tables and query them with PGQ `GRAPH_TABLE` syntax. The extension is loaded automatically at startup and can be disabled with `DISABLE_DUCKPGQ=1`.

> **Compatibility:** The gateway is pinned to **DuckDB 2.0.0** (`@duckdb/node-api` 2.0.0-r.1), which has a matching DuckPGQ community build. DuckDB 2.0 requires grammar extensions to be enabled per session, so every gateway connection runs `SET active_grammar_extensions = ['duckpgq']` after `LOAD duckpgq`. If you bump DuckDB to a version without a DuckPGQ build, the extension will be unavailable and graph endpoints return an error indicating DuckPGQ is not loaded.

| Method | Endpoint | Description | Access |
| :--- | :--- | :--- | :--- |
| `POST` | `/graphs` | Create (or replace) a named graph from table relationships | Superuser |
| `GET` | `/graphs` | List all registered graphs with their definitions | Any valid key |
| `DELETE` | `/graphs/:graphName` | Drop a graph and remove its registration | Superuser |
| `POST` | `/graphs/:graphName/query` | Execute a SQL/PGQ pattern query against the graph | Superuser |

Creating, deleting and querying graphs is superuser-only because query fragments can contain subqueries over any table and `GRAPH_TABLE` cannot be parsed into an AST for table-level authorization ([§3.5](#35-sql-enforcement)). System tables (`_`-prefixed) cannot be used in graph definitions.

### 12.1 Architecture: Durable Registry & Lazy Materialization

DuckPGQ registers a property graph only in the **in-memory catalog** of the single DuckDB instance that ran the `CREATE PROPERTY GRAPH` statement. It is **not** persisted into DuckLake. Without intervention this causes two problems: graph definitions vanish on every restart or instance recreation, and a graph created on the primary instance is invisible to the isolated user-query instance that actually runs `GRAPH_TABLE` queries.

ichibi-lake solves this with a **durable graph registry** plus **lazy materialization**:

1. **Registry (source of truth).** Each definition is stored as JSON in a persistent `_graph_registry` table in DuckLake (`name`, `definition`, `created_at`, `created_by`). Because it lives in DuckLake, it survives restarts and is shared by every gateway instance.
2. **Write path.** `POST /graphs` validates the body, builds the `CREATE PROPERTY GRAPH` DDL, persists it to the registry, then materializes it on the **primary** instance (dropping any prior version so edits take effect immediately).
3. **Read path.** `POST /graphs/:graphName/query` runs on the isolated **user-query** instance. Just before executing, a `prepare` hook calls `ensureGraphMaterialized`, which loads the definition from the registry and (re)creates the graph in that instance's catalog if missing. An "already exists" error is treated as success (DuckPGQ has no `CREATE ... IF NOT EXISTS`), and an unregistered graph surfaces as **HTTP 404**.

```
POST /graphs ─► validate ─► _graph_registry (DuckLake, durable) ─► CREATE on primary
                                      │
POST /graphs/:name/query ─► user instance ─► ensureGraphMaterialized() ◄─ reads registry
                                      └─► graph (re)created in user catalog ─► GRAPH_TABLE runs
```

The net effect: graphs created on the primary are queryable from the user instance, and definitions survive instance recreation. The registry — not any in-memory catalog — is the single source of truth, so `DELETE` removes the registration and a subsequent query returns 404.

### 12.2 Defining a Property Graph

`POST /graphs` maps relational tables to graph vertices and edges. The body fields are:

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | string | Graph name. Must be a simple identifier (`[A-Za-z_][A-Za-z0-9_]*`). |
| `vertexTables` | array | Non-empty list of vertex tables. Each entry is a table-name string, or an object `{ name, label?, key? }`. A `key` is only emitted when an explicit `label` is also given (DuckPGQ requires `KEY` to accompany `LABEL`); otherwise DuckPGQ auto-discovers the primary key. |
| `edgeTables` | array | List of edge definitions (may be empty). |

Each `edgeTables` entry is an object:

| Field | Type | Description |
| :--- | :--- | :--- |
| `name` | string | Edge table name (required). |
| `source` | string | Source vertex table (required). |
| `destination` | string | Destination vertex table (required). |
| `sourceKey` | string | Foreign-key column on the edge table referencing the source. |
| `sourceRef` | string | Referenced column on the source table (used with `sourceKey`). |
| `destinationKey` | string | Foreign-key column on the edge table referencing the destination. |
| `destinationRef` | string | Referenced column on the destination table (used with `destinationKey`). |
| `label` | string | Optional edge label. |

All identifiers are validated; malformed bodies, and definitions that reference system tables, return **HTTP 400**. Re-posting an existing `name` replaces the definition. Tables used by a graph cannot be renamed or dropped (nor their columns renamed, dropped or converted) from the admin console until the graph is deleted.

```sh
curl -X POST "http://ichibi-lake:3333/graphs" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "social",
    "vertexTables": ["users"],
    "edgeTables": [
      {
        "name": "follows",
        "source": "users",
        "destination": "users",
        "sourceKey": "src_id",
        "sourceRef": "id",
        "destinationKey": "dst_id",
        "destinationRef": "id"
      }
    ]
  }'
```

### 12.3 Querying a Graph

`POST /graphs/:graphName/query` runs a SQL/PGQ pattern match. Provide **either** the structured fields or a raw `graphTable` clause:

| Field | Type | Description |
| :--- | :--- | :--- |
| `match` | string | PGQ `MATCH` pattern, e.g. `(a:users)-[f:follows]->(b:users)`. |
| `columns` | string | Projection list for the `COLUMNS (...)` clause. Required with `match`. |
| `where` | string | Optional `WHERE` predicate. |
| `graphTable` | string | Raw body placed inside `GRAPH_TABLE (graphName ...)`. Mutually exclusive with `match`/`columns`. |
| `limit` | number | Optional row limit. |

You must supply either `graphTable`, or **both** `match` and `columns`; otherwise the request returns **HTTP 400**. Queries are subject to the always-on read-only guard ([§3.5](#35-sql-enforcement)). Querying a graph that is not registered returns **HTTP 404**.

**Basic MATCH:**

```sh
curl -X POST "http://ichibi-lake:3333/graphs/social/query" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "match": "(a:users)-[f:follows]->(b:users)",
    "columns": "a.name AS follower, b.name AS followed"
  }'
```

**Shortest path:**

```sh
curl -X POST "http://ichibi-lake:3333/graphs/social/query" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "match": "p = ANY SHORTEST (a:users)-[f:follows]->+ (b:users)",
    "where": "a.name = '\''Alice'\''",
    "columns": "path_length(p) AS distance"
  }'
```

*Response:* `{ "success": true, "rowCount": 2, "rows": [...], "sql": "FROM GRAPH_TABLE (...)" }`

### 12.4 Listing & Deleting Graphs

`GET /graphs` returns every registered graph from the durable registry, regardless of which instance has it materialized:

```sh
curl -H "x-api-key: YOUR_KEY" "http://ichibi-lake:3333/graphs"
```

*Response:*

```json
{
  "success": true,
  "graphs": [
    { "name": "social", "created_at": "...", "created_by": "...", "definition": { "name": "social", "vertexTables": ["users"], "edgeTables": [ ... ] } }
  ]
}
```

`DELETE /graphs/:graphName` removes the registration (source of truth) and drops the materialized catalog entry on the primary instance. After deletion the graph is absent from the listing and querying it returns **HTTP 404**.

```sh
curl -X DELETE "http://ichibi-lake:3333/graphs/social" -H "x-api-key: YOUR_KEY"
```

---

## 13. Admin Console

A password-protected web console at **`/console`** (e.g. [http://localhost:3333/console](http://localhost:3333/console)) for managing the gateway without editing `docker/.env`: a table and data browser with inline and schema editing, scheduled age- and count-based pruning, API keys, RBAC users, console logins, live settings and an audit log.

### 13.1 Accessing the Console

The console is served by the gateway itself, on the **same port as the REST API and Swagger UI**, at the `/console` path:

| How you run it | Console URL |
| :--- | :--- |
| Docker Compose (default) | [http://localhost:3333/console](http://localhost:3333/console): the container listens on 3000, published on host port 3333 |
| Docker Compose with `PORT=8080` in `docker/.env` | http://localhost:8080/console |
| Natively (`node src/index.js`) | http://localhost:3000/console, or your `PORT` |
| Deployed | `https://<your-host>/console`; set `PUBLIC_URL` so the log shows it |

The gateway prints the full link at startup:

```
[Server] Swagger UI:    http://localhost:3333/
[Server] Admin console: http://localhost:3333/console
```

The console uses its own usernames and passwords; **API keys do not work for the console**, and the console has **no default password**.

#### First time: create the admin account

While no console login exists, the console opens on a **Set up the admin console** screen that requires a one-time **setup code**.

1. **Find the setup code** in the gateway log. It is printed when the gateway starts with no console logins (and once more if every login is later deleted):
   ```sh
   docker logs ichibi-lake 2>&1 | grep -A3 "First-run setup"
   ```
   ```
   [Console] First-run setup required. Open http://localhost:3333/console and enter this setup code:

       GTSX-ZN49-V5M9
   ```
   Running natively (`node src/index.js`)? The same message is printed to the terminal.
2. **Open `/console`**, then enter:
   - **Setup code**: the code from step 1 (case-sensitive, including the dashes).
   - **Admin username**: defaults to `admin`; 3–64 characters from letters, digits and `_ . @ -`.
   - **Password** and **Confirm password**: at least 10 characters.
3. Select **Create admin account**. You are signed in straight away, and the setup code stops working.

To choose the setup code yourself, for example in automated deployments where reading logs is awkward, set it before starting the gateway:

```sh
# docker/.env
CONSOLE_SETUP_TOKEN=pick-a-long-random-value
```

The code exists so that whoever first reaches a freshly deployed gateway cannot claim it. Complete setup promptly after deploying.

#### Signing in

Open `/console` and enter your username and password. Usernames are not case-sensitive. A session lasts **12 hours** by default (an admin can change this under **Settings → Security → Console session lifetime**), after which you are asked to sign in again. Use the sign-out button next to your name at the bottom of the side menu to end a session early.

After **5 failed attempts** from the same IP address, or for the same username, sign-in is blocked for 15 minutes.

#### Adding more people

An admin adds logins under **Access → Console Logins**: enter a username, an initial password and a role, then select **Add login**.

| Role | Can do |
| :--- | :--- |
| `admin` | Everything: edit data and schemas, manage pruning rules, settings, API keys, RBAC users and logins |
| `viewer` | Read-only: browse every page, including table data and pruning previews, but cannot change anything |

Share the initial password with the person securely and ask them to change it under **System → My Account**.

#### Changing or resetting a password

- **Your own password**: **System → My Account → Change password** (requires your current password). Your other sessions, for example on another device, are signed out.
- **Someone else's password** (admins): **Access → Console Logins → Reset password** next to their name. Their existing sessions end immediately.

#### Forgotten password or locked out

If no admin can sign in, reset a password from the server with the recovery CLI. It prints a new random password:

```sh
docker exec ichibi-lake node scripts/console-admin.js list                     # show console logins
docker exec ichibi-lake node scripts/console-admin.js reset-password admin     # new random password for "admin"
docker exec ichibi-lake node scripts/console-admin.js create rescue --role admin
```

Sign in with the printed password, then change it under **My Account**. If every console login is deleted (`... console-admin.js delete <name>`), the next visit to `/console` shows the setup screen again, and a new setup code is printed to the log. Being throttled after failed attempts clears by itself after 15 minutes, or immediately when the gateway restarts.

#### Accessing the console remotely

The console is available wherever the gateway's port is reachable. Before exposing it beyond a trusted network:

- **Restrict who can reach it** (firewall, IP allow-list or VPN), since the console manages API keys and can delete data.
- **Prefer HTTPS.** Session cookies are marked `Secure` only when the request reaches the gateway over HTTPS.
- The console assumes clients connect to the gateway directly. Behind a reverse proxy it would see every client as the proxy's address, so the per-IP login throttle (5 failures / 15 minutes) would apply to all users at once.

#### Troubleshooting

| Symptom | Cause / fix |
| :--- | :--- |
| No "First-run setup" line in the log | Setup has already been completed. Sign in, or use the recovery CLI above. If the log has rotated, run `docker restart ichibi-lake`; the code is printed again while no login exists |
| "Invalid setup code" | Use the code from the *most recent* startup of *this* instance. Each gateway instance prints its own code, and `CONSOLE_SETUP_TOKEN` overrides the printed one |
| "The console is still starting" | The gateway is still attaching DuckLake; wait a few seconds and reload |
| "Too many failed attempts" | Wait 15 minutes, restart the gateway, or reset the password with the CLI |
| Signed out unexpectedly | The session expired, your password was changed or reset, or your login was deleted |

### 13.2 Features

The side menu groups pages by area. **Admins** can change everything; **viewers** have read-only access.

| Page | What it does |
| :--- | :--- |
| **Overview** | Version, uptime, DuckDB/DuckPGQ status, table, key and rule counts, and a **Health & load** card: the `/health/ready` checks, user-query load (running / queued / timeouts), table exports in progress, lake file count and size, and Kafka consumer lag |
| **API Keys** | Generate (shown once) and revoke `x-api-key` values; revert to the `docker/.env` list |
| **RBAC Users** | Create role-based API users (superuser / writer / reader), change roles, activate or deactivate. Users defined by `RBAC_SUPERUSERS` / `RBAC_USERS` in `docker/.env`, or by the `API_KEYS` list, are re-applied on every restart, so they are shown read-only with where to change them |
| **Console Logins** | Add console users, change roles, reset passwords, delete logins (at least one admin always remains) |
| **Tables & Data** | Browse every user table with paging, sorting, per-column filters and full-text search; edit cells inline; add and delete rows; view and edit schemas; create, rename and drop tables; **export** one table or several at once (one ZIP) and **import** CSV / Parquet / JSON files (see below) |
| **Retention & Pruning** | Per-table rules that delete rows older than a given age, beyond the newest *N* rows, and/or matching simple conditions, each on its own schedule; preview, **Run now**, **Clone**, and a run log (see below). Also the **Lake maintenance** schedule and log ([§4.7](#47-lake-maintenance-small-files)) |
| **Storage** | Where the data files (local disk or S3-compatible) and BLOBs are kept, editable, plus the read-only Postgres catalog and snapshot retention (see below) |
| **Security / Ingestion / Query Engine / Kafka** | Effective configuration, showing each value's source (default, `docker/.env`, or console override) |
| **Audit Log** | Who changed what, from which IP, across all instances |
| **My Account** | Change your own password |
| **API Reference** | Opens the Swagger UI (the gateway root, `/`) in a new tab |

**How settings work.** Console changes are stored as overrides in DuckLake, so every gateway instance shares them (instances refresh every 30 s) and they survive restarts and image rebuilds. Only settings the gateway reads on use are editable, and they **apply immediately**:

| Editable live | Read-only (set in `docker/.env`, restart required) |
| :--- | :--- |
| `API_KEYS` (via the API Keys page), `MAX_BATCH_SIZE`, `DEFAULT_PARTITION_KEY`, `USER_QUERY_TIMEOUT_MS`, `USER_QUERY_CONCURRENCY`, `CONSOLE_SESSION_TTL_HOURS`, `SNAPSHOT_RETENTION_DAYS` | Database connection, Kafka, `JSON_BODY_LIMIT`, pool sizes, RBAC bootstrap variables |

Storage settings are the exception: they are needed to open the lake, so the **Storage** page saves them to a file and applies them with a restart (below).

Resetting a setting removes the override and returns to the `docker/.env` value or the default. Overrides are loaded at startup before the RBAC bootstrap, so an API key list edited in the console (including revoked keys) survives restarts. Revoking an API key also deactivates the RBAC user that the legacy bootstrap created for it; otherwise the key would keep working.

**Tables & Data.** Open **Data → Tables & Data**, pick a table, and switch between the **Data** and **Schema** tabs. Each table has its own link (`#/tables/<name>`, `#/tables/<name>/schema`).

- **Browsing**: choose 25–500 rows per page and jump to any page; click a column header to sort (ascending, descending, off); search across all columns; and stack filters (*contains, equals, does not equal, starts with, >, ≥, <, ≤, is NULL, is not NULL*). Values longer than 1,000 characters are truncated for display.
- **Editing** (admins): double-click a cell, or focus it and press **Enter**. **Enter** saves, **Tab** saves and moves to the next cell, **Esc** cancels; **NULL** sets the cell to NULL. Values are converted to the column's type, so an invalid value is rejected with DuckDB's message. If someone else changed the cell since you loaded the page, the edit is refused rather than overwriting their change. A table's `updated` timestamp column is set automatically on edit, and `created`/`updated` on rows added with **Add row**. Scalar types are editable; STRUCT, LIST, MAP and BLOB columns are read-only in the grid. Rows are identified by DuckLake's stable `rowid`, so edits never hit the wrong row even in tables without a primary key.
- **Schema** (admins): add columns (with an optional default that backfills existing rows), rename columns, change defaults and NULL-ability, change types and drop columns. In the **Edit column** dialog every type can be chosen:
  - **(in place)**: widening changes DuckLake supports natively (e.g. `INTEGER → BIGINT`, `FLOAT → DOUBLE`). Data and column position are unchanged.
  - **(convert)**: any other change (e.g. `VARCHAR → INTEGER`, `BIGINT → VARCHAR`). The console copies the converted values into a new column, drops the old one, and renames the new one, all in **one transaction**, so nothing changes if any step fails. Row ids are preserved; the column **moves to the end** of the table and **loses its default**; NOT NULL is kept. Use **Check values** first: if any value cannot be converted (e.g. `"n/a"` to INTEGER), the change is refused with a count and examples unless you tick **Set values that cannot be converted to NULL**. Converting is blocked while a property graph references the table, or while a pruning rule prunes this table by the age of the column and the new type is not a date/timestamp.
  - **Key columns**: the **Key columns** card shows the table's declared key (marked *key* in the column list) and **Edit keys** sets or clears it; for a composite key, tick the columns in key order. Keys drive `?mode=upsert` / `?mode=dedupe` ingestion ([§9.2](#92-upserts--deduplication-key-columns)). Saving reports how many key values already occur more than once. A key column cannot be dropped until it is removed from the key; renaming a key column or the table updates the key. Changes are audited.
- **Tables** (admins): create tables with a column builder, rename them (table ACLs, RBAC table permissions and pruning rules follow the new name; API clients using the old name will fail), and drop them, which requires typing the table name. Tables referenced by a property graph cannot be renamed or dropped, and their columns cannot be renamed or dropped, until the graph is removed.
- **Export** (all console users): **Export** on a table's page downloads it as Parquet, CSV or NDJSON, optionally only the rows matching the grid's current filters and search, in its sort order, with an optional row limit. It uses the same engine and limits as `GET /tables/:tableName/export` ([§8.1](#81-exporting-tables)): streamed, capped at `EXPORT_MAX_ROWS`, at most `EXPORT_CONCURRENCY` at once. Exports are recorded in the audit log.
- **Export several tables** (all console users): **Export** on the table list downloads the ticked tables (all by default; filter the list and use **Select all** / **Select none**) as one ZIP file, `lake-export-<UTC time>.zip`, with a Parquet, CSV or NDJSON file per table and a `manifest.json` listing each table's row count and whether it was truncated. All tables are read in **one transaction**, so the files are consistent with each other even while data is being written. CSV and NDJSON files are compressed in the ZIP; Parquet files are stored as they are (already compressed). The ZIP is streamed, with Parquet tables staged one at a time in `EXPORT_TMP_DIR`. The whole download takes one `EXPORT_CONCURRENCY` slot, and `EXPORT_MAX_ROWS` and `EXPORT_TIMEOUT_MS` apply **per table**. System tables are never included. Recorded in the audit log as `tables.export`. Console API: `GET /console/api/data/export?format=parquet|csv|ndjson&tables=a,b` (omit `tables` for every user table). For lakes of tens of GB, back up the storage and the Postgres catalog instead of downloading through the browser.
- **Import** (admins): **Import** on the table list (new table or append) or on a table's page (append) loads a **CSV** (header, delimiter and column types detected automatically), **Parquet** or **JSON** file (an array of objects, or one object per line). **Preview** reads the file without writing anything and shows the detected columns and types, the row count, the first 20 rows and, when appending, what happens to each column: *matches*, *cast to* the table's type, *widened* (STRUCT), *column will be added*, or *skipped*, plus columns missing from the file (left NULL; `created`/`updated` timestamp columns are filled with the import time). Appending follows the same schema-evolution rules as `POST /upload`; untick **Add columns** to skip new columns instead. The insert is a single statement, so a row that cannot be converted fails the whole import and no rows are added. Files are limited to `IMPORT_MAX_MB` (default 1024 MB, editable on the **Ingestion** page), streamed to a temp file (`IMPORT_TMP_DIR`, default the OS temp dir) and deleted afterwards; nothing is kept between Preview and Import (the browser sends the file again), so it works behind a load balancer. Imported tables, like console-created ones, have no ACL row. Imports are recorded in the audit log.
- **Not shown**: system tables (prefixed `_`, e.g. RBAC users, ACLs, console logins and settings) never appear here; they are managed on their own pages. Tables created from the console have no ACL row, so only RBAC superusers can reach them through the REST API until an ACL is set (`PATCH /tables/:name/acl`).

Every change (cell edits, inserts, deletes, schema and table changes) is recorded in the audit log with the table, row and column, but not the cell values.

**Storage.** The page has one card per part of the storage, and each shows only the settings of the option in use:

- **Data files**: **Local disk** (a directory; in Docker use a mounted one, `/app/data_files` is persisted to `docker/data/ducklake`) or **S3-compatible** (bucket path `s3://bucket/prefix/`, endpoint, region, access and secret key, URL style, TLS). With local disk, the S3 connection settings are still available under *S3 credentials for bulk imports*, because imports from `s3://` use them. Also the **inlining row limit**.
- **BLOB storage**: **Inside tables** or **Local directory**. Existing BLOBs are not moved or converted, so changing this needs an explicit confirmation: BLOBs stored before can no longer be read through the API afterwards.
- **Metadata catalog (Postgres)**: read-only. The catalog *is* the lake (tables, schemas, snapshots, console logins and settings), so pointing the gateway at another Postgres is a deployment decision made in `docker/.env` (`DUCKLAKE_PG_*`, `POSTGRES_PASSWORD`).
- **Snapshots & maintenance**: `SNAPSHOT_RETENTION_DAYS` (applies immediately) and the maintenance kill switch.

**Test connection** writes and reads back a probe file at the data location (S3: under `_ichibi_probe/`, removed by lake maintenance; local: deleted straight away) and checks the BLOB directory. In Docker it also warns when a directory is not on a mounted volume. **Save & restart** runs the same test, saves the settings to `STORAGE_CONFIG_FILE` (mode 0600; secrets left empty keep their saved value and are never sent back to the browser), restarts the gateway, and reloads the page when it is back. Saved values take precedence over `docker/.env`; **Use docker/.env values** fills the form with those values. Changes are recorded in the audit log (`storage.update`, secrets masked).

- **Safe apply.** New settings are first saved as *pending* next to the settings in effect. If the gateway cannot open the lake with them (three attempts, or at once for a refused move), it restores the previous settings, restarts on them, and the page shows what failed. A successful start confirms the new settings.
- **Moving the data files** (to another directory, bucket or prefix, or between disk and S3) is only allowed **while no user table has data files**. DuckLake records the data location in the catalog and stores every file path relative to it, so a lake with files would have to be copied. The page shows which tables block a move; to move anyway, export them (Tables & Data → Export), drop them, switch, and import them again. Rows that are still inlined in the catalog move with the lake. The new location must be empty. At the next start the gateway re-checks this against the old location, rewrites the gateway's own system tables at the new location, records the new path in the catalog and expires older snapshots, which refer to the old location. Files at the old location are left in place for you to delete.
- **Connection details** (credentials, endpoint, region, URL style, TLS) can be changed at any time, with the same test and restart. Because the catalog keeps listing the lake's files at the same `s3://` location, the test also reads the footers of a few existing data files (from the largest tables) through the new connection, and refuses the change if they cannot be read, e.g. when the endpoint now points to a different server or account that happens to have a bucket of the same name.
- **Several gateway instances** sharing one catalog each read their own `STORAGE_CONFIG_FILE`. Configure storage in `docker/.env` for such deployments: after a move, instances still on the old path refuse to start (DuckLake rejects a data path that does not match the catalog) until they are given the new one.
- **Without Docker or PM2** the gateway cannot restart itself: **Save** stores the settings and you restart the process (set `STORAGE_AUTO_RESTART=1` if a supervisor restarts it).

**Retention & Pruning.** A rule deletes rows that meet **any** of its ticked criteria (age, row count, match conditions), like Kafka's time- and size-based retention:

| Setting | Meaning |
| :--- | :--- |
| **Table** | Each rule prunes **one table**, chosen from a searchable list that shows each table's date columns (or typed, for a table that does not exist yet). Keeping rules to one table means the column settings always fit that table's schema; to apply similar settings elsewhere, use **Clone** on the rule, pick another table and save. |
| **Older than** *N* hours/days, based on a column | Deletes rows whose date column is older than the cutoff. The column is chosen from a dropdown of the table's actual columns with their types, dates and timestamps first, then numbers (e.g. `created — TIMESTAMP`, `ts — BIGINT, epoch`); *Other…* accepts a name for a table that does not exist yet. If the column is missing when the rule runs, the table is skipped and reported. |
| ↳ Numeric (epoch) date columns | `BIGINT`, `INTEGER`, `DOUBLE` and other number columns are treated as Unix epoch times. The unit is **auto-detected** per table from the largest value (below 10¹¹ → seconds, 10¹⁴ → milliseconds, 10¹⁷ → microseconds, otherwise nanoseconds), or set explicitly. The run log and preview show which interpretation was used, e.g. *ts as epoch milliseconds (detected)*. |
| **Beyond the newest** *N* rows, ordered by a column | Keeps the newest *N* rows by that column (chosen from a dropdown; newest first; ties and the default use insertion order) and deletes the rest. |
| **Matching** *all*/*any* of these conditions | Deletes rows matching simple conditions: column (from a dropdown) + operator (*equals, does not equal, contains, does not contain, starts with, ends with, >, ≥, <, ≤, is one of, is NULL, is not NULL*) + value, up to 10 conditions. **No SQL is accepted**: the server builds the query, quotes column names, casts values to the column's type, and treats every value as a literal, so text like `'); DROP TABLE x; --` is just matched as a value. Text operators are case-insensitive; a comparison that cannot be evaluated (e.g. a non-numeric value against a number column) never deletes. |
| **Schedule** | Per rule, as a 5-field cron expression in **UTC** (presets: every 15 minutes, hourly, every 6 hours, daily 02:00, weekly, monthly). The editor shows the next run times. |
| **Safety limit** | Optional. If a run would delete more than this many rows from one table, that table is left untouched and the run is marked failed. |
| **Reclaim storage** | After deleting, rewrite the affected tables' data files and expire snapshots older than `SNAPSHOT_RETENTION_DAYS`. Without snapshot expiry, deleted rows still occupy storage because older snapshots reference them (time travel). |

- **Preview** (also in the editor) is a dry run showing how many rows each criterion would delete (by age, by count, by match). **Run now** starts a job in the background. **Clone** opens the editor pre-filled with a copy of the rule, so the same settings can be saved for another table.
- **Activity** lists recent jobs (manual or scheduled) with status (`running`, `succeeded`, `partial`, `failed`, `abandoned`), rows deleted and duration; expand a job for per-table counts (by age / by count / by match), skips and errors. The page refreshes itself while a job runs. Job outcomes and rule changes are also written to the audit log. Run history is kept for 90 days.
- **Rules saved with several tables** (wildcards or lists, from the release that briefly allowed them) keep running unchanged and are marked *multiple tables* in the list. The toggle and Preview still work; saving one requires choosing a single table, and Clone covers the others.
- **Multiple gateway instances** each run the scheduler; a due rule is claimed with a token that is re-read after commit, so only one instance runs it. Jobs interrupted by a restart are marked `abandoned`.
- **Never pruned:** system tables (anything starting with `_`, plus an explicit protected list covering RBAC users and API keys, ACLs, the graph registry and console tables). Console logins, password hashes and settings are in a separate catalog the pruning engine cannot see. Rules for `_` tables are rejected, and the protected list is checked again at deletion time.
- Filesystem BLOB files referenced by deleted rows are removed too. Deleted rows cannot be recovered except from backups or, within the snapshot retention window, DuckLake time travel.
- Rules from earlier releases (one table each, with the global `RETENTION_INTERVAL_MINUTES`) are converted automatically: the interval becomes each rule's cron schedule.

### 13.3 Security Model

- **Passwords** are hashed with scrypt (N=2¹⁵, r=8, p=1, per-user salt).
- **Sessions** are HMAC-signed, `HttpOnly`, `SameSite=Strict` cookies scoped to `/console` (and `Secure` over HTTPS). Changing a password signs out that user's other sessions.
- **CSRF**: state-changing requests must carry an `X-Console-Request` header and a same-origin `Origin`. The console is mounted before the global CORS middleware, so it never sends permissive CORS headers.
- **Brute force**: logins are throttled per IP and per username (5 failures per 15 minutes), as are setup attempts.
- **Browser hardening**: strict Content-Security-Policy (no inline script or style), `X-Frame-Options: DENY`, `no-referrer`. API responses are `no-store`.
- Secrets (API keys, the Postgres password) are masked in the UI. A generated key is shown exactly once.
- **Console data is isolated.** Console logins, settings (including the session-signing secret), pruning rules and their run log, and the audit log live in a separate DuckLake catalog (`ichibi_console`, Postgres schema `ichibi_console`, rows stored inline in Postgres). Only the primary gateway instance attaches it; user-supplied SQL runs on an instance that cannot see it ([§3.5](#35-sql-enforcement)). Releases before this change kept these tables in the main catalog; they are moved automatically on first start.

### 13.4 Recovery CLI

`scripts/console-admin.js` runs inside the container against the same catalog (see [§13.1](#131-accessing-the-console) for when to use it):

```sh
docker exec ichibi-lake node scripts/console-admin.js list
docker exec ichibi-lake node scripts/console-admin.js reset-password admin
docker exec ichibi-lake node scripts/console-admin.js create alice --role viewer
docker exec ichibi-lake node scripts/console-admin.js delete alice
```

Without `--password`, a strong random password is generated and printed. If every login is deleted, the next visit to `/console` runs setup again, with a fresh code printed to the log.

### 13.5 Extending the Console

The menu and settings forms are driven by metadata in [`src/console/registry.js`](src/console/registry.js):

- **New settings category**: add a `SECTIONS` entry with `kind: 'settings'` and `SETTINGS` entries pointing at it. The UI renders the form from each setting's `type` (`boolean`, `number`, `enum`, `string`, `list`, `secret`) with validation (`min`/`max`/`options`/`validate`). Mark a setting `editable: true` only if the code reads `process.env.X` *on use*, not into a startup constant.
- **New custom page**: add a section with `kind: 'custom'`, add its API routes in [`src/console/index.js`](src/console/index.js) (behind `requireSession`, and `requireAdmin` for changes, and audited with `store.audit`), then register the view in [`public/console/app.js`](public/console/app.js):

```js
registerView('my-page', async (ctx) => {
    const data = await api('GET', '/my-page');
    return [pageHeader('My Page', 'What it does'), h('div', { class: 'card' }, /* ... */)];
});
```

Larger views can live in their own file under `public/console/views/` (as `tables.js` does), added as another `<script defer>` after `app.js` in `public/console/index.html`; they use the same globals (`registerView`, `h`, `api`, `modal`, ...). `ctx.params` holds the route segments after the section id, and `ctx.navigate(...)` changes the route.

Build DOM with `h()`, never `innerHTML`: the CSP and the XSS model rely on it.

---

## 14. Operations: Health, Metrics & Limits

### 14.1 Health Checks

| Endpoint | Auth | Description |
| :--- | :--- | :--- |
| `GET /health` | none | **Liveness.** `200 {"status":"ok","uptimeSeconds":...}` while the Node process is responsive. Touches no database, so it stays up when Postgres is down. The compose healthcheck uses it. |
| `GET /health/ready` | none | **Readiness.** `200 {"status":"ready","checks":{...}}` only when the startup sequence (RBAC/ACL bootstrap, migrations, admin console and maintenance scheduler) has finished, the DuckLake catalog is attached, and the metadata Postgres answers a query through DuckLake's own connection. Otherwise `503` with the failing check, e.g. `{"startup":"ready","ducklake":"ok","postgres":"error: ..."}`. Each check times out after 5 s. |

Use `/health` for restart decisions and `/health/ready` for routing traffic (load balancer, Kubernetes readiness probe). Neither reveals data or configuration.

### 14.2 Prometheus Metrics

`GET /metrics` returns Prometheus text format (`text/plain; version=0.0.4`). Metrics are per gateway process and reset on restart, so scrape every instance.

**Authentication.** Metrics are never public by default:

- With `METRICS_TOKEN` set, the scraper must send `Authorization: Bearer <METRICS_TOKEN>` (Prometheus: `authorization: { credentials: ... }` in the scrape config). API keys are not accepted then, so a scraper token cannot be used against the data API and vice versa.
- Without it, a **superuser** API key is required, as `x-api-key` or as a bearer token.

Other callers get 401. Metrics do not contain table names, keys or query text (routes are labelled by template, e.g. `/tables/:tableName`), but do reveal traffic and catalog size.

```yaml
scrape_configs:
  - job_name: ichibi-lake
    authorization: { credentials: "<METRICS_TOKEN>" }
    static_configs: [{ targets: ["ichibi-lake:3000"] }]
```

| Metric | Type | Description |
| :--- | :--- | :--- |
| `ichibi_http_requests_total{method,route,status}` | counter | Requests by route template |
| `ichibi_http_request_duration_seconds{method,route}` | histogram | Latency until the response finished (for streams: the whole transfer) |
| `ichibi_user_query_active` / `_queued` / `_concurrency` | gauge | User SQL gate: running, waiting, configured limit |
| `ichibi_user_query_total{outcome}` | counter | Finished user SQL queries: `succeeded`, `failed`, `timeout`, `cancelled` (client disconnected) |
| `ichibi_user_query_timeouts_total` | counter | User SQL queries that hit their timeout |
| `ichibi_export_active`, `ichibi_export_total{format,outcome}` | gauge, counter | Table exports in progress / finished (`succeeded`, `failed`, `timeout`, `cancelled`, `rejected`). Console multi-table downloads are counted with `format="zip-parquet"`, `"zip-csv"` or `"zip-ndjson"` |
| `ichibi_prune_jobs_total{status}`, `ichibi_prune_rows_deleted_total` | counter | Pruning jobs run by this instance (`succeeded`, `partial`, `failed`) and rows they deleted |
| `ichibi_kafka_consumer_lag{topic,partition}` | gauge | Consumer offset lag as of the last processed batch (reported by kafkajs; no extra broker calls). Absent until a batch has been processed |
| `ichibi_ducklake_up`, `ichibi_ducklake_tables`, `ichibi_ducklake_data_files`, `ichibi_ducklake_data_bytes` | gauge | Catalog size from `ducklake_table_info()`, cached for 30 s; `ichibi_ducklake_up` is 0 if that query failed |
| `ichibi_startup_complete` | gauge | 1 once the startup sequence finished |
| `process_resident_memory_bytes`, `nodejs_heap_used_bytes`, `process_uptime_seconds` | gauge | Process basics |

Useful alerts: `ichibi_user_query_queued > 0` for several minutes (raise `USER_QUERY_CONCURRENCY` or find the slow queries), a rising `ichibi_user_query_timeouts_total`, `ichibi_prune_jobs_total{status="failed"}` increasing, `ichibi_kafka_consumer_lag` growing, and `ichibi_ducklake_data_files` growing much faster than `ichibi_ducklake_data_bytes` (many small files: compaction is not keeping up).

### 14.3 Result Size Limits

| Path | Row cap | Time limit | Memory |
| :--- | :--- | :--- | :--- |
| `GET /tables/:tableName` | `?limit=` (default 50) | — | Whole page in memory; keep pages small |
| `POST /query` (JSON) | `QUERY_MAX_ROWS` (100,000) | `USER_QUERY_TIMEOUT_MS` (30 s) | Up to the cap in memory |
| `POST /query` (NDJSON) | `QUERY_STREAM_MAX_ROWS` (10,000,000) | `USER_QUERY_STREAM_TIMEOUT_MS` (5 min, whole transfer) | ~one chunk (measured: ~30 MB extra RSS whether 100 MB or 1 GB is streamed) |
| `GET /tables/:tableName/export` (CSV/NDJSON) | `EXPORT_MAX_ROWS` (5,000,000) | `EXPORT_TIMEOUT_MS` (10 min) | ~one chunk, no temp files |
| `GET /tables/:tableName/export` (Parquet) | `EXPORT_MAX_ROWS` | `EXPORT_TIMEOUT_MS` | ~one read buffer; one temp file per export in `EXPORT_TMP_DIR` |
| Console multi-table ZIP export | `EXPORT_MAX_ROWS` per table | `EXPORT_TIMEOUT_MS` per table | ~one chunk; Parquet: one temp file at a time |

Every cap reports truncation (`truncated` / trailer line / `X-Export-Truncated`), never silently drops rows. All limits are editable live on the console's **Query Engine** page except `EXPORT_TMP_DIR`. GraphQL `sql` and graph queries still return complete results within `USER_QUERY_TIMEOUT_MS`.

## 15. Testing

```sh
npm test                       # Run all suites, including RBAC and console (requires Docker: app, db, kafka)
npm run test:unit              # Unit tests only (schema evolution, graph DDL, topic filter, ORDER BY, PGQ grammar, console, SQL guard, SQL table extraction, storage, import, ingest modes)
npm run test:graphs-unit       # Graph DDL generation unit tests (no Docker needed)
npm run test:pgq-grammar       # DuckDB 2.0 SQL/PGQ grammar opt-in tests (no Docker needed)
npm run test:console-unit      # Admin console auth, settings, data browser, pruning and cron unit tests (no Docker needed)
npm run test:sql-guard         # Read-only / introspection guard for user-supplied SQL (no Docker needed)
npm run test:sql-tables        # Table extraction for table-scoped SQL, against DuckDB's parser (no Docker needed)
npm run test:storage           # Object-storage config, S3 secret SQL and maintenance settings (no Docker needed)
npm run test:zip               # Streaming ZIP writer used by multi-table exports (no Docker needed)
npm run test:storage-config    # Storage page: saved settings, confirm/revert after failed starts, form validation (no Docker needed)
npm run test:maintenance       # Data inlining and scheduled lake maintenance (file counts drop), against the running stack
npm run test:orderby           # DuckLake ORDER BY corruption regression (no Docker needed)
npm run test:kafka-topic-filter # Kafka topic subscription filter (no Docker needed)
npm run test:ingest-modes      # Ingestion mode / key list / KAFKA_INGEST_MODES parsing (no Docker needed)
npm run test:upsert            # Key columns, upsert / dedupe (composite keys, batch duplicates, created/updated, schema evolution, RBAC, concurrent upserts)
npm run test:console           # Admin console integration tests (uses docker exec for a temporary admin)
npm run test:ops               # Health, metrics, NDJSON streaming (4M rows, bounded memory), row caps, table export round trips and RBAC
npm run test:import            # Bulk import (formats, modes, schema evolution, RBAC, size limit, S3 sources, allow-list) and Idempotency-Key
npm run test:import-unit       # Import source validation, allow-list and format detection (no Docker needed)
npm run test:rbac              # RBAC: superuser / writer / reader permissions, per-table overrides, table-scoped SQL, superuser-only graphs, system tables
npm run test:api               # Integration tests
npm run test:rest              # REST API tests
npm run test:blob              # BLOB tests
npm run test:graphs            # Graph query integration tests (registry + lazy materialization)
npm run test:kafka-sink        # Kafka sink tests
npm run test:kafka-sse         # Kafka SSE tests
npm run test:kafka-retroactive # Kafka retroactive (history dump) tests
npm run test:stress            # Stress test (customize via env vars)
```

Integration tests expect the Docker stack to be running, and read the API keys and port from `docker/.env`. To target another deployment, set `BASE_URL`, `API_KEY` (a superuser key) and optionally `ALT_API_KEY`. The console suite creates a temporary admin with `docker exec` in the container named by `CONSOLE_CONTAINER` (default `ichibi-lake`) and deletes it afterwards; alternatively set `CONSOLE_ADMIN_USER` / `CONSOLE_ADMIN_PASSWORD` to use an existing login.

The import suite (`test/test-import.js`) runs its object-storage cases only when it can upload test files itself: set `TEST_S3_ENDPOINT` (host-reachable, e.g. `http://localhost:9000` for the bundled MinIO), `TEST_S3_ACCESS_KEY`, `TEST_S3_SECRET_KEY` and optionally `TEST_S3_BUCKET` (default `ichibi-lake`), and run the gateway with `IMPORT_ALLOWED_SOURCES` containing `s3://<bucket>/import-allowed/` (or set `TEST_S3_ALLOWED` to your allow-listed prefix). Set `IMPORT_MAX_MB` to the gateway's value (at most 64) to also test the streamed size limit.
