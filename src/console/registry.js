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
    { key: 'DUCKLAKE_DATA_PATH', section: 'storage', type: 'string', default: 'data_files/', editable: false, label: 'DuckLake data path' },
    { key: 'USE_FILESYSTEM_BLOBS', section: 'storage', type: 'boolean', default: '', editable: false, label: 'Filesystem BLOB storage' },
    { key: 'BLOBS_PATH', section: 'storage', type: 'string', default: './data/blobs', editable: false, label: 'BLOB directory' },
    { key: 'DISABLE_DUCKLAKE_COMPACTION', section: 'storage', type: 'boolean', default: '', editable: false, label: 'Startup compaction disabled' },
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

    // --- Retention (rendered on the Retention page, not a settings form) ------
    {
        key: 'RETENTION_INTERVAL_MINUTES', section: 'retention', type: 'number', default: '60',
        min: 1, max: 10080, editable: true, live: true, label: 'Prune interval (minutes)',
        help: 'How often enabled retention rules run.',
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
