/**
 * Minimal Prometheus text-format (v0.0.4) metrics for GET /metrics, with no
 * extra dependency. Everything here is per gateway process and resets on
 * restart; scrape each instance separately.
 */

const DURATION_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60];

// key = JSON of label values -> { labels, value } / histogram state
const httpRequests = new Map();
const httpDurations = new Map();
const pruneJobs = new Map();
const kafkaLag = new Map();
let pruneRowsDeleted = 0;

function inc(map, labels, by = 1) {
    const key = JSON.stringify(labels);
    const entry = map.get(key);
    if (entry) entry.value += by;
    else map.set(key, { labels, value: by });
}

function observe(map, labels, seconds) {
    const key = JSON.stringify(labels);
    let h = map.get(key);
    if (!h) {
        h = { labels, buckets: new Array(DURATION_BUCKETS.length).fill(0), sum: 0, count: 0 };
        map.set(key, h);
    }
    for (let i = 0; i < DURATION_BUCKETS.length; i++) if (seconds <= DURATION_BUCKETS[i]) h.buckets[i]++;
    h.sum += seconds;
    h.count++;
}

/** Route template for labels: the matched Express route, never the raw URL (unbounded cardinality). */
function routeLabel(req) {
    if (!req.route) return req.baseUrl ? `${req.baseUrl}/*` : 'unmatched';
    const p = req.route.path;
    return (req.baseUrl || '') + (Array.isArray(p) ? p[0] : String(p));
}

/** Express middleware recording request count and latency by method/route/status. */
export function httpMetricsMiddleware(req, res, next) {
    const start = process.hrtime.bigint();
    res.on('finish', () => {
        const seconds = Number(process.hrtime.bigint() - start) / 1e9;
        const route = routeLabel(req);
        inc(httpRequests, { method: req.method, route, status: String(res.statusCode) });
        observe(httpDurations, { method: req.method, route }, seconds);
    });
    next();
}

/** Called by the pruning job runner when a job finishes. */
export function recordPruneJob(status, rowsDeleted = 0) {
    inc(pruneJobs, { status });
    pruneRowsDeleted += Number(rowsDeleted) || 0;
}

/** Called by the Kafka consumer after each batch (kafkajs END_BATCH_PROCESS). */
export function recordKafkaLag(topic, partition, lag) {
    kafkaLag.set(`${topic}:${partition}`, { labels: { topic, partition: String(partition) }, value: Number(lag) || 0 });
}

/** Current Kafka consumer lag per topic partition (for the console Overview). */
export function kafkaLagSnapshot() {
    return [...kafkaLag.values()].map(({ labels, value }) => ({ topic: labels.topic, partition: Number(labels.partition), lag: value }));
}

// --- Text exposition ------------------------------------------------------------

function escapeLabel(v) {
    return String(v).replace(/\\/g, '\\\\').replace(/\n/g, '\\n').replace(/"/g, '\\"');
}

function fmtLabels(labels) {
    const parts = Object.entries(labels).map(([k, v]) => `${k}="${escapeLabel(v)}"`);
    return parts.length ? `{${parts.join(',')}}` : '';
}

function fmtNumber(n) {
    if (n === Infinity) return '+Inf';
    if (n === -Infinity) return '-Inf';
    return Number.isNaN(n) ? 'NaN' : String(n);
}

class Writer {
    constructor() { this.lines = []; }
    family(name, type, help) {
        this.lines.push(`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`);
        return this;
    }
    sample(name, labels, value) {
        this.lines.push(`${name}${fmtLabels(labels)} ${fmtNumber(value)}`);
        return this;
    }
    gauge(name, help, value, labels = {}) {
        return this.family(name, 'gauge', help).sample(name, labels, value);
    }
    counter(name, help, value, labels = {}) {
        return this.family(name, 'counter', help).sample(name, labels, value);
    }
    toString() { return this.lines.join('\n') + '\n'; }
}

/**
 * Render the full exposition. `sources` supplies snapshots owned by other
 * modules so this file has no import cycles:
 *   userQuery  userQueryStats() from src/db.js
 *   exports    exportStats() from src/export.js
 *   lake       { tables, files, bytes } or null when the catalog query failed
 *   ready      boolean, whether the startup sequence has finished
 */
export function renderMetrics({ userQuery, exports, lake, ready }) {
    const w = new Writer();

    w.family('ichibi_http_requests_total', 'counter', 'HTTP requests by method, route template and status code.');
    for (const { labels, value } of httpRequests.values()) w.sample('ichibi_http_requests_total', labels, value);

    w.family('ichibi_http_request_duration_seconds', 'histogram', 'HTTP request latency (until the response finished) by method and route template.');
    for (const h of httpDurations.values()) {
        DURATION_BUCKETS.forEach((le, i) => w.sample('ichibi_http_request_duration_seconds_bucket', { ...h.labels, le: String(le) }, h.buckets[i]));
        w.sample('ichibi_http_request_duration_seconds_bucket', { ...h.labels, le: '+Inf' }, h.count);
        w.sample('ichibi_http_request_duration_seconds_sum', h.labels, h.sum);
        w.sample('ichibi_http_request_duration_seconds_count', h.labels, h.count);
    }

    w.gauge('ichibi_user_query_active', 'User SQL queries executing on the isolated instance.', userQuery.active);
    w.gauge('ichibi_user_query_queued', 'User SQL queries waiting for a concurrency slot.', userQuery.queued);
    w.gauge('ichibi_user_query_concurrency', 'Configured USER_QUERY_CONCURRENCY.', userQuery.concurrency);
    w.family('ichibi_user_query_total', 'counter', 'Finished user SQL queries by outcome (succeeded, failed, timeout, cancelled).');
    w.sample('ichibi_user_query_total', { outcome: 'succeeded' }, userQuery.succeeded);
    w.sample('ichibi_user_query_total', { outcome: 'failed' }, userQuery.failed);
    w.sample('ichibi_user_query_total', { outcome: 'timeout' }, userQuery.timedOut);
    w.sample('ichibi_user_query_total', { outcome: 'cancelled' }, userQuery.cancelled);
    w.counter('ichibi_user_query_timeouts_total', 'User SQL queries that hit the timeout.', userQuery.timedOut);

    w.gauge('ichibi_export_active', 'Table exports in progress.', exports.active);
    w.family('ichibi_export_total', 'counter', 'Finished table exports by format and outcome.');
    for (const [key, value] of Object.entries(exports.counters)) {
        const [format, outcome] = key.split(':');
        w.sample('ichibi_export_total', { format, outcome }, value);
    }

    w.family('ichibi_prune_jobs_total', 'counter', 'Pruning jobs run by this instance, by final status.');
    for (const { labels, value } of pruneJobs.values()) w.sample('ichibi_prune_jobs_total', labels, value);
    w.counter('ichibi_prune_rows_deleted_total', 'Rows deleted by pruning jobs run by this instance.', pruneRowsDeleted);

    w.family('ichibi_kafka_consumer_lag', 'gauge', 'Kafka consumer offset lag per topic partition, as of the last processed batch.');
    for (const { labels, value } of kafkaLag.values()) w.sample('ichibi_kafka_consumer_lag', labels, value);

    w.gauge('ichibi_ducklake_up', 'Whether the last DuckLake catalog query for these metrics succeeded.', lake ? 1 : 0);
    if (lake) {
        w.gauge('ichibi_ducklake_tables', 'User tables in the DuckLake catalog (excluding "_" system tables).', lake.tables);
        w.gauge('ichibi_ducklake_data_files', 'Parquet data files referenced by the current DuckLake snapshot.', lake.files);
        w.gauge('ichibi_ducklake_data_bytes', 'Total size of those data files in bytes.', lake.bytes);
    }
    w.gauge('ichibi_startup_complete', 'Whether the gateway startup sequence (RBAC/ACL bootstrap, migrations, admin console) has finished.', ready ? 1 : 0);

    const mem = process.memoryUsage();
    w.gauge('process_resident_memory_bytes', 'Resident memory size in bytes.', mem.rss);
    w.gauge('nodejs_heap_used_bytes', 'V8 heap used in bytes.', mem.heapUsed);
    w.gauge('process_uptime_seconds', 'Seconds since the gateway process started.', Math.round(process.uptime()));
    return w.toString();
}
