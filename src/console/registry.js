/**
 * Admin console registry: the side-menu sections and the settings they expose.
 *
 * Extending the console
 * ---------------------
 * - A new settings category needs only metadata: add a section with
 *   `kind: 'settings'` and add SETTINGS entries whose `section` matches its id.
 *   The UI renders the form generically from each setting's `type`.
 * - A bespoke page (tables, actions, ...) uses `kind: 'custom'`; register the
 *   matching view in public/console/app.js via `registerView(id, render)` and
 *   add its API routes in src/console/router.js.
 *
 * Setting fields
 * --------------
 *   key          Environment variable name. The effective value is the console
 *                override (if any) ?? process.env at boot ?? `default`.
 *   section      Section id it belongs to.
 *   label/help   UI text.
 *   type         'string' | 'number' | 'boolean' | 'enum' | 'list' | 'secret'
 *   default      Value used when neither an override nor the env var is set.
 *   editable     true: can be changed from the console and is stored as an
 *                override in DuckLake (shared by every gateway instance).
 *                false: shown read-only; change it in docker/.env and restart.
 *   live         Informational: an editable setting takes effect without a
 *                restart (the code reads it on use). All editable settings
 *                must be live, because overrides are loaded after the DB is up,
 *                i.e. after startup-time constants have been evaluated.
 *   hidden       Not shown on settings pages; edited only by its own page
 *                (e.g. API_KEYS on the API Keys page). Still stored and loaded
 *                as an override like any editable setting.
 *   min/max      Number bounds. `options` lists enum values.
 *   validate     Optional (value) => error string | null for extra checks.
 */

export const SECTIONS = [
    { id: 'overview',   group: 'General', title: 'Overview',          icon: 'dashboard', kind: 'custom' },
    { id: 'api-keys',   group: 'Access',  title: 'API Keys',          icon: 'key',       kind: 'custom' },
    { id: 'rbac-users', group: 'Access',  title: 'RBAC Users',        icon: 'users',     kind: 'custom' },
    { id: 'logins',     group: 'Access',  title: 'Console Logins',    icon: 'lock',      kind: 'custom' },
    { id: 'tables',     group: 'Data',    title: 'Tables & Data',     icon: 'table',     kind: 'custom' },
    { id: 'retention',  group: 'Data',    title: 'Retention & Pruning', icon: 'calendar', kind: 'custom' },
    { id: 'security',   group: 'Settings', title: 'Security',         icon: 'shield',    kind: 'settings' },
    { id: 'ingestion',  group: 'Settings', title: 'Ingestion',        icon: 'upload',    kind: 'settings' },
    { id: 'query',      group: 'Settings', title: 'Query Engine',     icon: 'search',    kind: 'settings' },
    { id: 'storage',    group: 'Settings', title: 'Storage',          icon: 'database',  kind: 'settings' },
    { id: 'kafka',      group: 'Settings', title: 'Kafka',            icon: 'stream',    kind: 'settings' },
    { id: 'audit',      group: 'System',  title: 'Audit Log',         icon: 'list',      kind: 'custom' },
    { id: 'account',    group: 'System',  title: 'My Account',        icon: 'user',      kind: 'custom' },
];

const IDENT_LIST_RE = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/;

export const SETTINGS = [
    // --- Security ---------------------------------------------------------
    {
        key: 'CONSOLE_SESSION_TTL_HOURS', section: 'security', type: 'number', default: '12',
        min: 1, max: 720, editable: true, live: true, label: 'Console session lifetime (hours)',
        help: 'How long a console login stays valid. Applies to new logins.',
    },
    {
        key: 'METRICS_TOKEN', section: 'security', type: 'secret', default: '',
        editable: false, label: 'Metrics bearer token',
        help: 'Bearer token for GET /metrics. When unset, /metrics requires a superuser API key.',
    },
    {
        // Edited on the API Keys page (which also reconciles RBAC rows), never
        // through the generic settings form.
        key: 'API_KEYS', section: 'security', type: 'list', editable: true, live: true, hidden: true,
        label: 'API keys',
    },

    // --- Ingestion ----------------------------------------------------------
    {
        key: 'MAX_BATCH_SIZE', section: 'ingestion', type: 'number', default: '10000',
        min: 1, max: 10000000, editable: true, live: true, label: 'Max rows per upload',
        help: 'Requests to /upload with more rows than this are rejected with HTTP 400.',
    },
    {
        key: 'IMPORT_MAX_MB', section: 'ingestion', type: 'number', default: '1024',
        min: 1, max: 102400, editable: true, live: true, label: 'Max console import size (MB)',
        help: 'Largest file an admin can import on the Tables & Data page.',
    },
    {
        key: 'DEFAULT_PARTITION_KEY', section: 'ingestion', type: 'string', default: 'created',
        editable: true, live: true, label: 'Default partition key',
        help: 'Column(s) new tables are partitioned by (comma-separated), or "none".',
        validate: (v) => {
            if (v.trim().toLowerCase() === 'none' || v.trim() === '') return null;
            const bad = v.split(',').map(s => s.trim()).filter(s => !IDENT_LIST_RE.test(s));
            return bad.length ? `Invalid column name(s): ${bad.join(', ')}` : null;
        },
    },
    {
        key: 'JSON_BODY_LIMIT', section: 'ingestion', type: 'string', default: '50mb',
        editable: false, label: 'JSON body limit', help: 'Maximum JSON request size.',
    },

    // --- Query engine -------------------------------------------------------
    {
        key: 'USER_QUERY_TIMEOUT_MS', section: 'query', type: 'number', default: '30000',
        min: 1000, max: 3600000, editable: true, live: true, label: 'User query timeout (ms)',
        help: 'Wall-clock limit for user-supplied SQL and graph queries.',
    },
    {
        key: 'USER_QUERY_CONCURRENCY', section: 'query', type: 'number', default: '4',
        min: 1, max: 256, editable: true, live: true, label: 'User query concurrency',
        help: 'Maximum user SQL queries executing at once; the rest queue.',
    },
    {
        key: 'USER_QUERY_STREAM_TIMEOUT_MS', section: 'query', type: 'number', default: '300000',
        min: 1000, max: 86400000, editable: true, live: true, label: 'Streaming query timeout (ms)',
        help: 'Wall-clock limit for a whole NDJSON POST /query response, including the transfer.',
    },
    {
        key: 'QUERY_MAX_ROWS', section: 'query', type: 'number', default: '100000',
        min: 1, max: 100000000, editable: true, live: true, label: 'Max rows per JSON query',
        help: 'Row cap for POST /query JSON responses; larger results are truncated and flagged.',
    },
    {
        key: 'QUERY_STREAM_MAX_ROWS', section: 'query', type: 'number', default: '10000000',
        min: 1, max: 10000000000, editable: true, live: true, label: 'Max rows per streamed query',
        help: 'Row cap for POST /query NDJSON responses.',
    },
    {
        key: 'EXPORT_MAX_ROWS', section: 'query', type: 'number', default: '5000000',
        min: 1, max: 10000000000, editable: true, live: true, label: 'Max rows per table export',
        help: 'Row cap for GET /tables/:tableName/export; larger exports are truncated and flagged in headers.',
    },
    {
        key: 'EXPORT_CONCURRENCY', section: 'query', type: 'number', default: '2',
        min: 1, max: 64, editable: true, live: true, label: 'Concurrent table exports',
        help: 'Exports running at once; further requests get 429.',
    },
    {
        key: 'EXPORT_TIMEOUT_MS', section: 'query', type: 'number', default: '600000',
        min: 1000, max: 86400000, editable: true, live: true, label: 'Table export timeout (ms)',
        help: 'Wall-clock limit for one export, including the download.',
    },
    {
        key: 'EXPORT_TMP_DIR', section: 'query', type: 'string', default: '',
        editable: false, label: 'Export temp directory', help: 'Where Parquet exports are staged (default: the OS temp dir).',
    },
    {
        key: 'PRIMARY_POOL_SIZE', section: 'query', type: 'number', default: '4',
        editable: false, label: 'Primary connection pool size',
    },
    {
        key: 'COLUMN_CACHE_TTL_MS', section: 'query', type: 'number', default: '60000',
        editable: false, label: 'Column cache TTL (ms)',
    },
    {
        key: 'DISABLE_DUCKPGQ', section: 'query', type: 'boolean', default: '',
        editable: false, label: 'DuckPGQ disabled',
    },

    // --- Storage ------------------------------------------------------------
    {
        key: 'DUCKLAKE_DATA_PATH', section: 'storage', type: 'string', default: 'data_files/', editable: false, label: 'DuckLake data path',
        help: 'Local directory or s3:// URL (S3, MinIO, R2...). Object storage uses the S3_* settings below.',
    },
    { key: 'S3_ENDPOINT', section: 'storage', type: 'string', default: '', editable: false, label: 'S3 endpoint', help: 'host:port of an S3-compatible server; empty for AWS S3.' },
    { key: 'S3_REGION', section: 'storage', type: 'string', default: 'us-east-1', editable: false, label: 'S3 region' },
    { key: 'S3_ACCESS_KEY', section: 'storage', type: 'secret', default: '', editable: false, label: 'S3 access key', help: 'Empty: the AWS credential chain (environment, ~/.aws, instance or task role).' },
    { key: 'S3_SECRET_KEY', section: 'storage', type: 'secret', default: '', editable: false, label: 'S3 secret key' },
    { key: 'S3_URL_STYLE', section: 'storage', type: 'string', default: '', editable: false, label: 'S3 URL style', help: 'Empty: path with an endpoint, vhost for AWS.' },
    { key: 'S3_USE_SSL', section: 'storage', type: 'boolean', default: 'true', editable: false, label: 'S3 uses TLS' },
    {
        key: 'DATA_INLINING_ROW_LIMIT', section: 'storage', type: 'number', default: '1000', editable: false, label: 'Data inlining row limit',
        help: 'Writes of fewer rows than this are kept in the metadata catalog until lake maintenance flushes them to Parquet, instead of creating one small file each. 0 disables inlining.',
    },
    { key: 'USE_FILESYSTEM_BLOBS', section: 'storage', type: 'boolean', default: '', editable: false, label: 'Filesystem BLOB storage', help: 'BLOB files always go to a local (or mounted) directory, also when the lake uses object storage.' },
    { key: 'BLOBS_PATH', section: 'storage', type: 'string', default: './data/blobs', editable: false, label: 'BLOB directory' },
    { key: 'DISABLE_DUCKLAKE_COMPACTION', section: 'storage', type: 'boolean', default: '', editable: false, label: 'Lake maintenance disabled', help: 'Emergency switch: no scheduled, startup or manual maintenance runs.' },
    { key: 'DUCKLAKE_PG_HOST', section: 'storage', type: 'string', default: '0.0.0.0', editable: false, label: 'Metadata Postgres host' },
    { key: 'DUCKLAKE_PG_PORT', section: 'storage', type: 'number', default: '5432', editable: false, label: 'Metadata Postgres port' },
    { key: 'DUCKLAKE_PG_DB', section: 'storage', type: 'string', default: 'ducklake', editable: false, label: 'Metadata database' },
    { key: 'DUCKLAKE_PG_USER', section: 'storage', type: 'string', default: 'postgres', editable: false, label: 'Metadata user' },
    { key: 'DUCKLAKE_PG_PASSWORD', section: 'storage', type: 'secret', default: 'postgres', editable: false, label: 'Metadata password' },

    // --- Kafka --------------------------------------------------------------
    { key: 'KAFKA_BROKERS', section: 'kafka', type: 'list', default: '', editable: false, label: 'Brokers' },
    { key: 'KAFKA_TOPIC_REGEX', section: 'kafka', type: 'string', default: '.*', editable: false, label: 'Topic regex' },
    { key: 'KAFKA_GROUP_ID', section: 'kafka', type: 'string', default: 'ducklake-ingestion-group', editable: false, label: 'Consumer group' },
    { key: 'KAFKA_BATCH_SIZE', section: 'kafka', type: 'number', default: '100', editable: false, label: 'Batch size' },
    { key: 'KAFKA_BATCH_TIMEOUT_MS', section: 'kafka', type: 'number', default: '5000', editable: false, label: 'Batch timeout (ms)' },
    { key: 'KAFKA_PRODUCER_COMPRESSION', section: 'kafka', type: 'enum', options: ['gzip', 'snappy', 'lz4', 'zstd', 'none'], default: 'gzip', editable: false, label: 'Producer compression' },

    {
        key: 'SNAPSHOT_RETENTION_DAYS', section: 'storage', type: 'number', default: '',
        min: 0, max: 3650, editable: true, live: true, label: 'Snapshot retention (days)',
        help: 'DuckLake keeps every snapshot (time travel) until it is expired, so pruned rows keep using storage. ' +
            'When set, lake maintenance and pruning rules with "reclaim storage" expire snapshots older than this ' +
            'and delete files no longer referenced. Empty: never expire (unlimited time travel, storage is not reclaimed).',
    },
];

const SETTINGS_BY_KEY = new Map(SETTINGS.map(s => [s.key, s]));

export function getSettingDef(key) {
    return SETTINGS_BY_KEY.get(key) || null;
}

/**
 * Validate and normalise a value for an editable setting. Returns
 * { value } with the canonical string form, or { error }.
 */
export function normaliseSettingValue(def, raw) {
    if (!def) return { error: 'Unknown setting' };
    if (!def.editable) return { error: `${def.key} is read-only; set it in docker/.env` };
    if (raw === null || raw === undefined) return { error: 'A value is required' };
    let value;
    switch (def.type) {
        case 'boolean': {
            if (typeof raw === 'boolean') value = raw ? 'true' : 'false';
            else if (/^(true|false)$/i.test(String(raw))) value = String(raw).toLowerCase();
            else return { error: 'Expected true or false' };
            break;
        }
        case 'number': {
            const n = Number(raw);
            if (!Number.isFinite(n) || !Number.isInteger(n)) return { error: 'Expected a whole number' };
            if (def.min !== undefined && n < def.min) return { error: `Must be at least ${def.min}` };
            if (def.max !== undefined && n > def.max) return { error: `Must be at most ${def.max}` };
            value = String(n);
            break;
        }
        case 'enum': {
            if (!def.options.includes(String(raw))) return { error: `Must be one of: ${def.options.join(', ')}` };
            value = String(raw);
            break;
        }
        case 'list': {
            const items = (Array.isArray(raw) ? raw : String(raw).split(','))
                .map(s => String(s).trim()).filter(Boolean);
            value = items.join(',');
            break;
        }
        default:
            value = String(raw).trim();
    }
    if (value.length > 4096) return { error: 'Value is too long' };
    if (typeof def.validate === 'function') {
        const err = def.validate(value);
        if (err) return { error: err };
    }
    return { value };
}

/** Mask a secret for display: keep a short prefix so keys stay recognisable. */
export function maskSecret(value) {
    if (!value) return '';
    return String(value).split(',').map(v => {
        const s = v.trim();
        if (s.length <= 8) return '•'.repeat(s.length);
        return `${s.slice(0, 6)}…${'•'.repeat(4)}${s.slice(-2)}`;
    }).join(', ');
}
