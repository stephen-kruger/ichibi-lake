# ichibi-lake Gateway v1.6.18

![ichibi-lake](graphics/ichibi-lake-2.jpg "iChibi Lake")

A feature-rich DuckDB-based data lake gateway with REST API, blob storage, schema evolution, Kafka integration, GraphQL, and graph (PGQ) query support — ready for AI/ML data pipelines.

---

## Table of Contents

- [1. Quick Start](#1-quick-start)
- [2. Architecture](#2-architecture)
- [3. Environment Variables](#3-environment-variables)
  - [3.1 Core / Server](#31-core--server)
  - [3.2 DuckLake (Postgres Metadata)](#32-ducklake-postgres-metadata)
  - [3.3 Authentication](#33-authentication)
  - [3.4 Blob Storage](#34-blob-storage)
  - [3.5 SQL Enforcement](#35-sql-enforcement)
  - [3.6 Performance & Caching](#36-performance--caching)
  - [3.7 Kafka Integration](#37-kafka-integration)
  - [3.8 Docker Compose / Deployment](#38-docker-compose--deployment)
- [4. Deployment](#4-deployment)
  - [4.1 Docker (recommended)](#41-docker-recommended)
  - [4.2 Native Node.js](#42-native-nodejs)
  - [4.3 Production Checklist](#43-production-checklist)
  - [4.4 SuperLake Wizard](#44-superlake-wizard)
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
- [9. Event Ingestion](#9-event-ingestion)
  - [9.1 JSON Upload](#91-json-upload)
  - [9.2 Raw SQL Query](#92-raw-sql-query)
- [10. Kafka Integration](#10-kafka-integration)
  - [10.1 HTTP Sink](#101-http-sink)
  - [10.2 Internal Background Consumer](#102-internal-background-consumer)
  - [10.3 SSE Subscription](#103-sse-subscription)
- [11. GraphQL](#11-graphql)
- [12. Graph Queries (SQL/PGQ)](#12-graph-queries-sqlpgq)
  - [12.1 Architecture: Durable Registry & Lazy Materialization](#121-architecture-durable-registry--lazy-materialization)
  - [12.2 Defining a Property Graph](#122-defining-a-property-graph)
  - [12.3 Querying a Graph](#123-querying-a-graph)
  - [12.4 Listing & Deleting Graphs](#124-listing--deleting-graphs)
- [13. Testing](#13-testing)

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
| DuckDB Web UI (read/write) | [http://localhost:4213](http://localhost:4213) |
| OpenAPI spec | [http://localhost:3333/swagger.yaml](http://localhost:3333/swagger.yaml) |

Default API key for development:

```
ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13
```

---

## 2. Architecture

ichibi-lake is a Node.js gateway that wraps a **DuckDB** in-memory instance backed by the **DuckLake** storage extension. Parquet data files are persisted to disk; table metadata (schemas, ACLs, and graph definitions) lives in a companion **PostgreSQL** database. BLOBs can be stored as standalone files or inline in Parquet.

```
Client → REST/GraphQL → ichibi-lake gateway → DuckDB (DuckLake) → Parquet files
                              │                       └── PostgreSQL (metadata)
                              ├── Kafka (sink / SSE / consumer)
                              └── Filesystem (blobs)
```

The gateway maintains two classes of DuckDB connection: a **primary** pool for DDL, ingestion, and registry writes, and an isolated **user-query** instance for read-only SQL and graph traversals so heavy queries cannot stall ingestion. Durable state (table data, ACLs, RBAC, and **property-graph definitions**) is persisted through DuckLake so it survives restarts and is shared across instances. Property graphs are an exception that DuckPGQ stores only in the in-memory catalog of whichever DuckDB instance created them — ichibi-lake works around this with a **durable graph registry** (see [§12.1](#121-architecture-durable-registry--lazy-materialization)).

---

## 3. Environment Variables

### 3.1 Core / Server

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PORT` | `3000` | Port the Node gateway listens on |
| `JSON_BODY_LIMIT` | `50mb` | Max JSON request body size (e.g. `2gb`, `50mb`) |
| `MAX_BATCH_SIZE` | `10000` | Max rows per `/upload` request; prevents OOM |
| `NODE_ENV` | `production` | Node environment |

### 3.2 DuckLake (Postgres Metadata)

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DUCKLAKE_PG_HOST` | `0.0.0.0` | Postgres host for DuckLake metadata |
| `DUCKLAKE_PG_PORT` | `5432` | Postgres port |
| `DUCKLAKE_PG_USER` | `postgres` | Postgres user |
| `DUCKLAKE_PG_PASSWORD` | `postgres` | Postgres password |
| `DUCKLAKE_PG_DB` | `ducklake` | Postgres database name |
| `DUCKLAKE_DATA_PATH` | `data_files/` | Directory for DuckLake Parquet data files |
| `DISABLE_DUCKPGQ` | *(unset)* | Set to `1` to disable the DuckPGQ graph extension |
| `DUCKDB_ALLOW_COMMUNITY_EXTENSIONS` | `1` | Allow community DuckDB extensions (DuckPGQ) |

### 3.3 Authentication

| Variable | Default | Description |
| :--- | :--- | :--- |
| `API_KEYS` | — | Comma-separated list of valid API keys (preferred). Each request must pass one via the `x-api-key` header. |
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

### 3.5 SQL Enforcement

| Variable | Default | Description |
| :--- | :--- | :--- |
| `SQL_READ_ONLY` | `true` | When `true`, DDL/DML keywords are rejected on user-supplied SQL endpoints (`/query`, GraphQL `sql` resolver, `/graphs/:name/query`). Set to `false` to allow arbitrary statements (ops only). |

### 3.6 Performance & Caching

| Variable | Default | Description |
| :--- | :--- | :--- |
| `PRIMARY_POOL_SIZE` | `4` | Number of warm DuckDB connections kept in the pool |
| `PARQUET_ROW_GROUP_SIZE` | `100000` | Target rows per Parquet row group |
| `USER_QUERY_CONCURRENCY` | `4` | Max concurrent user SQL queries |
| `USER_QUERY_TIMEOUT_MS` | `30000` | Timeout for user SQL queries (ms) |
| `COLUMN_CACHE_TTL_MS` | `60000` | Per-table column schema cache TTL (ms) |
| `ACL_CACHE_TTL_MS` | `60000` | ACL cache TTL (ms) |
| `DEFAULT_PARTITION_KEY` | `created` | Column(s) to partition new tables by. Comma-separated for multi-column partitioning. Set to `none` to disable. |

### 3.7 Kafka Integration

**Consumer (background ingestion):**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BROKERS` | — | Comma-separated broker list (e.g. `kafka:9092`). Leave empty to disable. |
| `KAFKA_CLIENT_ID` | `ducklake-consumer` | Kafka consumer client ID |
| `KAFKA_GROUP_ID` | `ducklake-ingestion-group` | Consumer group ID |
| `KAFKA_TOPIC_REGEX` | `.*` | Regex to match topics for auto-ingestion |
| `KAFKA_BATCH_SIZE` | `100` | Max messages per batch |
| `KAFKA_BATCH_TIMEOUT_MS` | `5000` | Max wait to complete a batch (ms) |
| `KAFKA_METADATA_MAX_AGE_MS` | `10000` | Metadata refresh interval (ms) |

**Producer / SSE:**

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_CLIENT_ID` | `ichibi-lake-sse` | Producer client ID |
| `KAFKA_MAX_MESSAGE_BYTES` | `950000` | Max size of a single produced message |
| `KAFKA_MAX_BATCH_BYTES` | `950000` | Max batch size for produced messages |
| `KAFKA_MAX_REQUEST_SIZE` | `10485760` | Max produce request size (should match broker's `KAFKA_MESSAGE_MAX_BYTES`) |
| `KAFKA_PRODUCER_COMPRESSION` | `gzip` | Compression: `gzip`, `snappy`, `lz4`, `zstd`, or `none` |

### 3.8 Docker Compose / Deployment

| Variable | Default | Description |
| :--- | :--- | :--- |
| `DUCKDB_UI_ENABLED` | `1` | Set to `0` to skip starting the DuckDB UI service |
| `DUCKDB_UI_PORT` | `4213` | Host port for the DuckDB web UI |
| `KAFKA_LOG_RETENTION_HOURS` | `48` | Kafka log retention in hours |
| `KAFKA_LOG_RETENTION_BYTES` | `1073741824` | Kafka log retention in bytes per partition |
| `KAFKA_LOG_SEGMENT_BYTES` | `268435456` | Kafka segment size before roll |
| `KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS` | `60000` | Kafka retention check interval |
| `DOCKER_HOST_UID` | `1000` | Host UID for permission mapping (`.env.example`) |
| `DOCKER_HOST_GID` | `1000` | Host GID for permission mapping (`.env.example`) |

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
curl -sf http://localhost:3333/swagger.yaml > /dev/null && echo "OK"
```

The compose stack starts four services:

| Service | Container | Purpose |
| :--- | :--- | :--- |
| `app` | `ichibi-lake` | Node.js REST gateway |
| `db` | `ichibi-lake-postgres` | Postgres 17 (DuckLake metadata) |
| `kafka` | `ichibi-lake-kafka` | Kafka 4.3.1 (event ingestion) |
| `duckdb-ui` | `ichibi-lake-duckdb-ui` | DuckDB web UI (optional) |

**Persistent volumes** are mounted under `docker/data/`:

```
docker/data/
├── blobs/       # BLOB files (when USE_FILESYSTEM_BLOBS=1)
├── ducklake/    # Parquet data files
├── postgres/    # Postgres data directory
└── kafka/       # Kafka KRaft logs
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
export DUCKDB_ALLOW_COMMUNITY_EXTENSIONS=1

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
- **Secure the DuckDB UI** — The UI (port 4213) has **no authentication** and full read/write access. Keep it behind a VPN, auth proxy, or disable it (`DUCKDB_UI_ENABLED=0`)
- **Set `SQL_READ_ONLY=true`** (default) — Prevents accidental or malicious DDL/DML via user-facing SQL endpoints
- **Tune `JSON_BODY_LIMIT`** — Match your expected payload size; too large invites OOM
- **Tune `MAX_BATCH_SIZE`** — Prevent a single upload from exhausting memory
- **Postgres logs** — The compose.yaml configures Postgres with conservative logging (no per-query logging, 2-second slow-query threshold, 7-day log rotation)
- **Kafka retention** — Set `KAFKA_LOG_RETENTION_HOURS` and `KAFKA_LOG_RETENTION_BYTES` to bound disk usage
- **Use a reverse proxy** — Place behind nginx, Caddy, or a cloud LB for TLS termination and rate limiting

### 4.4 SuperLake Wizard

For advanced multi-node or S3-backed deployments, the built-in SuperLake wizard guides you through configuration:

```sh
npm run superlake
```

Interactive options:

| Option | Description |
| :--- | :--- |
| **SSHFS mount** | Mount a remote DuckLake data directory over SSHFS for a shared-nothing multi-node setup |
| **S3 / MinIO** | Configure DuckDB's `httpfs` with S3-compatible storage (MinIO, AWS S3, etc.) |
| **Federation** | Connect multiple DuckLake instances as peers using `ATTACH` for cross-instance queries |

---

## 5. Authentication & Authorization

### 5.1 API Key Authentication

All API endpoints require a valid API key passed as an HTTP header:

- **Header**: `x-api-key`
- **Example**: `x-api-key: ICHIBI_LAKE_TEST_7a96c3e2-b1fe-4705-9064-fc1882674b13`
- **Unauthorized**: HTTP 401

Configure valid keys via one or both of the following mechanisms (they are **complementary**, not exclusive):

1. **Legacy `API_KEYS` / `API_KEY` env vars** — keys from these environment variables are accepted at authentication time and automatically promoted to **superusers** in the RBAC tables on startup.
2. **RBAC environment variables** (`RBAC_SUPERUSERS`, `RBAC_USERS`, etc.) — see [§5.3](#53-role-based-access-control-rbac).

The authentication flow is: **RBAC tables first**, then fall back to the legacy `API_KEYS` env-var set. This means admin-created RBAC users can authenticate even without being listed in `API_KEYS`.

### 5.2 Per-Table Access Control (ACL)

Each table has an ACL with two roles:

| Role | Permission | Allowed Operations |
| :--- | :--- | :--- |
| **Owner** | Read + Write | Query, upload, blob, patch, delete, manage ACL |
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
- On startup, `legacyBootstrap()` promotes all keys from the `API_KEYS` env var to **superusers** in RBAC tables. This ensures existing deployments keep working without configuration changes.
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

Users created via env vars are also manageable through the admin API at runtime.

#### Example: Hybrid Mode

```sh
API_KEYS="sk-deploy-key"                          # becomes a superuser
RBAC_SUPERUSERS="sk-admin-01"                     # another superuser
RBAC_USERS='[{"apiKey":"sk-writer-01","name":"Writer","roles":["writer"]}]'
```

All three keys are valid. `sk-deploy-key` and `sk-admin-01` are superusers; `sk-writer-01` can write to all tables by default.

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
| `GET` | `/tables` | List all tables (requires at least read access) |
| `GET` | `/tables/:tableName/schema` | Get column names and types |
| `GET` | `/tables/:tableName` | Query table data with filters, sorting, pagination |

**Query parameters for `GET /tables/:tableName`:**

| Parameter | Example | Description |
| :--- | :--- | :--- |
| `?owner=marwan` | `?owner=marwan` | Filter by column value (equality) |
| `?limit=100` | `?limit=100` | Max rows (default: all) |
| `?sort=-created_at` | `?sort=-created_at` | Sort by column; prefix `-` for descending |
| `?offset=50` | `?offset=50` | Pagination offset |

Results are scoped by ACL — only tables the caller has read access to are visible.

```sh
curl -H "x-api-key: YOUR_KEY" \
  "http://ichibi-lake:3333/tables/ichibi_table?owner=marwan&limit=10&sort=-created_at"
```

---

## 9. Event Ingestion

### 9.1 JSON Upload

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/upload` | Upload JSON array to `ichibi_table` |
| `POST` | `/upload/:tableName` | Upload JSON array to a specific table |

New tables are created automatically with type inference; the caller is registered as an ACL owner.

```sh
curl -X POST "http://ichibi-lake:3333/upload/user_stats" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '[{"user": "duck", "points": 100}, {"user": "lake", "points": 250}]'
```

### 9.2 Raw SQL Query

```sh
curl -X POST "http://ichibi-lake:3333/query" \
  -H "x-api-key: YOUR_KEY" \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT category, count(*) FROM ichibi_table GROUP BY 1"}'
```

When `SQL_READ_ONLY=true` (default), DDL/DML statements are rejected with HTTP 403.

---

## 10. Kafka Integration

### 10.1 HTTP Sink (`POST /kafka-sink`)

Designed for Kafka Connect. Automatically flattens `.value` schemas. New tables are registered under the `kafka-internal` ACL key.

```sh
curl -X POST "http://ichibi-lake:3333/kafka-sink" \
  -H "x-api-key: YOUR_KEY" \
  -H "X-Kafka-Topic: logs" \
  -d '[{"value": {"level": "info", "msg": "started"}}]'
```

### 10.2 Internal Background Consumer

Enable by setting `KAFKA_BROKERS` in the environment. The consumer automatically ingests events from matching Kafka topics into DuckDB tables (one table per topic).

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
| `?groupId=` | `ichibi-lake-sse-<sanitized-api-key>` | Override consumer group |
| `?since=` | *(all data)* | Publish DuckDB rows newer than this timestamp before subscribing. Supports absolute ISO 8601 (`2024-06-09T10:00:00Z`), relative duration (`5m`, `1h`, `2d`), or plain number in minutes (`5`). |
| `?sinceColumn=` | `updated` | Column name to filter `?since=` on |

---

## 11. GraphQL

A GraphQL endpoint is available at `/graphql` with a dynamic schema built from DuckDB tables. Each table exposes query fields with filtering, sorting, and pagination. An embedded `sql` resolver allows raw DuckDB SQL within GraphQL queries.

```graphql
{
  ichibi_table(limit: 5, where: { owner: "marwan" }, orderBy: { created_at: DESC }) {
    id
    owner
    created_at
  }
}
```

---

## 12. Graph Queries (SQL/PGQ)

ichibi-lake supports graph property queries via the **DuckPGQ** community extension (SQL/PGQ). Define named graphs over your relational tables and query them with PGQ `GRAPH_TABLE` syntax. The extension is loaded automatically at startup and can be disabled with `DISABLE_DUCKPGQ=1`.

> **Compatibility:** DuckPGQ is pinned to **DuckDB 1.5.0**. Running a different DuckDB version may leave the extension unavailable; graph endpoints then return an error indicating DuckPGQ is not loaded.

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `POST` | `/graphs` | Create (or replace) a named graph from table relationships |
| `GET` | `/graphs` | List all registered graphs with their definitions |
| `DELETE` | `/graphs/:graphName` | Drop a graph and remove its registration |
| `POST` | `/graphs/:graphName/query` | Execute a SQL/PGQ pattern query against the graph |

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

All identifiers are validated; malformed bodies return **HTTP 400**. Re-posting an existing `name` replaces the definition.

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

You must supply either `graphTable`, or **both** `match` and `columns`; otherwise the request returns **HTTP 400**. Queries are subject to the read-only guard (`SQL_READ_ONLY`). Querying a graph that is not registered returns **HTTP 404**.

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

## 13. Testing

```sh
npm test                       # Run all suites (requires Docker: app, db, kafka)
npm run test:unit              # Unit tests only (schema evolution, graph DDL, topic filter)
npm run test:graphs-unit       # Graph DDL generation unit tests (no Docker needed)
npm run test:api               # Integration tests
npm run test:rest              # REST API tests
npm run test:blob              # BLOB tests
npm run test:graphs            # Graph query integration tests (registry + lazy materialization)
npm run test:kafka-sink        # Kafka sink tests
npm run test:kafka-sse         # Kafka SSE tests
npm run test:kafka-retroactive # Kafka retroactive (history dump) tests
npm run test:stress            # Stress test (customize via env vars)
```

Integration tests expect the Docker stack to be running. Set `BASE_URL` and `API_KEY` environment variables to target a remote or custom deployment.
