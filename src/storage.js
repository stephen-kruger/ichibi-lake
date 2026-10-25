/**
 * Where DuckLake keeps its Parquet data files: a local directory (the default)
 * or an S3 / S3-compatible bucket (AWS S3, MinIO, Cloudflare R2, ...).
 *
 * Pure helpers, no DuckDB access, so they can be unit tested
 * (test/storage.test.js). src/db.js uses them while attaching DuckLake.
 *
 * Object storage is selected by giving DUCKLAKE_DATA_PATH an s3:// URL
 * (e.g. s3://my-bucket/lake/). Credentials and endpoint come from:
 *
 *   S3_ENDPOINT        host[:port] of an S3-compatible server (MinIO, R2...).
 *                      Empty for AWS S3.
 *   S3_REGION          default us-east-1
 *   S3_ACCESS_KEY      access key id. Empty: use the AWS credential chain
 *   S3_SECRET_KEY      secret access key        (env, ~/.aws, IAM role ...)
 *   S3_SESSION_TOKEN   optional, for temporary credentials
 *   S3_URL_STYLE       'path' or 'vhost'. Default: path with S3_ENDPOINT,
 *                      vhost without (AWS). S3_PATH_STYLE=true|false is
 *                      accepted as an alias.
 *   S3_USE_SSL         default true
 */

import path from 'path';

const OBJECT_STORE_RE = /^[a-z][a-z0-9+.-]*:\/\//i;
const S3_RE = /^s3:\/\/([^/]+)(\/.*)?$/i;

/** True for a URL-style data path (s3://, gs://, ...), false for a local path. */
export function isObjectStorePath(dataPath) {
    return OBJECT_STORE_RE.test(String(dataPath || ''));
}

/** Directory-style path with exactly one trailing separator. */
export function withTrailingSlash(p) {
    const s = String(p || '');
    return s.endsWith('/') ? s : `${s}/`;
}

/**
 * Join a sub-directory onto a data path (result ends with a separator). path.join() would collapse the "//"
 * of a URL (s3://b/x -> s3:/b/x), so URLs are joined as strings.
 */
export function joinDataPath(base, sub) {
    if (isObjectStorePath(base)) return withTrailingSlash(`${withTrailingSlash(base)}${sub}`);
    return path.join(base, sub) + path.sep;
}

const bool = (v, dflt) => {
    if (v === undefined || v === null || String(v).trim() === '') return dflt;
    return /^(1|true|yes|on)$/i.test(String(v).trim());
};

/**
 * Resolve the storage configuration from the environment. Throws with a
 * readable message when DUCKLAKE_DATA_PATH is a URL scheme that is not
 * supported.
 */
export function storageConfig(env = process.env) {
    const raw = String(env.DUCKLAKE_DATA_PATH || 'data_files/').trim();
    if (!isObjectStorePath(raw)) return { kind: 'local', dataPath: raw };

    const m = raw.match(S3_RE);
    if (!m) {
        throw new Error(`DUCKLAKE_DATA_PATH "${raw}" is not supported: use a local directory or an s3:// URL ` +
            '(S3-compatible stores such as MinIO, R2 or GCS interoperability use s3:// with S3_ENDPOINT).');
    }
    const endpoint = String(env.S3_ENDPOINT || '').trim().replace(/^https?:\/\//i, '').replace(/\/+$/, '');
    let urlStyle = String(env.S3_URL_STYLE || '').trim().toLowerCase();
    if (!urlStyle && env.S3_PATH_STYLE !== undefined && String(env.S3_PATH_STYLE).trim() !== '') {
        urlStyle = bool(env.S3_PATH_STYLE, false) ? 'path' : 'vhost';
    }
    if (!urlStyle) urlStyle = endpoint ? 'path' : 'vhost';
    if (!['path', 'vhost'].includes(urlStyle)) throw new Error(`S3_URL_STYLE must be "path" or "vhost", got "${env.S3_URL_STYLE}"`);
    const keyId = String(env.S3_ACCESS_KEY || '').trim();
    const secret = String(env.S3_SECRET_KEY || '');
    if (keyId && !secret) throw new Error('S3_ACCESS_KEY is set but S3_SECRET_KEY is empty');
    return {
        kind: 's3',
        // Always a directory prefix: it is also the secret's scope and the
        // user-query instance's allowed prefix, and without the slash
        // s3://b/lake would also allow s3://b/lake-other/.
        dataPath: withTrailingSlash(raw),
        bucket: m[1],
        endpoint,
        region: String(env.S3_REGION || '').trim() || 'us-east-1',
        keyId,
        secret,
        sessionToken: String(env.S3_SESSION_TOKEN || ''),
        urlStyle,
        useSsl: bool(env.S3_USE_SSL, true),
        provider: keyId ? 'config' : 'credential_chain',
    };
}

const lit = (v) => `'${String(v).replace(/'/g, "''")}'`;

/** Name of the DuckDB secret holding the object-store credentials. */
export const S3_SECRET_NAME = 'ichibi_s3';

/**
 * CREATE SECRET statement for an s3 storage config, scoped to the data path so
 * it is only ever used for the lake's own objects. A temporary (in-memory)
 * secret: nothing is written to ~/.duckdb.
 */
export function buildS3SecretSql(cfg, { name = S3_SECRET_NAME, scope = cfg?.dataPath, ifNotExists = false } = {}) {
    if (!cfg || cfg.kind !== 's3') return null;
    const opts = ['TYPE s3'];
    if (cfg.provider === 'credential_chain') {
        // REFRESH auto renews temporary (role) credentials before they expire.
        opts.push('PROVIDER credential_chain', 'REFRESH auto');
    } else {
        opts.push(`KEY_ID ${lit(cfg.keyId)}`, `SECRET ${lit(cfg.secret)}`);
        if (cfg.sessionToken) opts.push(`SESSION_TOKEN ${lit(cfg.sessionToken)}`);
    }
    opts.push(`REGION ${lit(cfg.region)}`);
    if (cfg.endpoint) opts.push(`ENDPOINT ${lit(cfg.endpoint)}`);
    opts.push(`URL_STYLE ${lit(cfg.urlStyle)}`, `USE_SSL ${cfg.useSsl ? 'true' : 'false'}`, `SCOPE ${lit(scope)}`);
    const create = ifNotExists ? 'CREATE TEMPORARY SECRET IF NOT EXISTS' : 'CREATE OR REPLACE TEMPORARY SECRET';
    return `${create} ${name} (${opts.join(', ')})`;
}

// --- Bulk import from object storage (POST /tables/:tableName/import) ------------

/** Name of the DuckDB secret used to read import sources (primary instance only). */
export const S3_IMPORT_SECRET_NAME = 'ichibi_s3_import';

/**
 * S3 settings for reading import sources: the same S3_* credentials, endpoint
 * and URL style as the lake, also when the lake itself is on local disk.
 */
export function importS3Config(env = process.env) {
    return storageConfig({ ...env, DUCKLAKE_DATA_PATH: 's3://import/' });
}

/**
 * Secret for import reads. Scoped to all of s3:// so allow-listed buckets
 * outside the lake are readable; DuckDB picks the secret with the longest
 * matching scope, so the lake's own secret still wins for its prefix.
 */
export function buildImportSecretSql(env = process.env) {
    return buildS3SecretSql(importS3Config(env), { name: S3_IMPORT_SECRET_NAME, scope: 's3://', ifNotExists: true });
}

const S3_SOURCE_RE = /^s3:\/\/([a-z0-9][a-z0-9.-]{1,61}[a-z0-9])\/(.+)$/;
const GLOB_CHARS_RE = /[*?[\]{}]/;

/**
 * Validate an import source. Only s3:// URLs with a plain bucket name and a
 * non-empty key (or glob) are accepted: no local paths, no file://, http(s)://
 * or other schemes, no "." / ".." segments, backslashes, quotes or control
 * characters. Throws an Error with statusCode 400 otherwise. Returns
 * { source, bucket, key, literalPrefix } where literalPrefix is the part
 * before the first glob character, i.e. everything the source can match
 * starts with it.
 */
export function parseImportSource(raw) {
    const fail = (msg) => { throw Object.assign(new Error(msg), { statusCode: 400 }); };
    const source = typeof raw === 'string' ? raw.trim() : '';
    if (!source) fail('"source" must be an s3:// URL');
    if (source.length > 1024) fail('"source" is too long');
    const m = source.match(S3_SOURCE_RE);
    if (!m) fail('Only s3://bucket/key sources are supported (no local paths or other URL schemes)');
    const [, bucket, key] = m;
    // eslint-disable-next-line no-control-regex
    if (/[\x00-\x1f\x7f\\'"`]/.test(key)) fail('"source" contains characters that are not allowed');
    const segments = key.split('/');
    if (segments.some((seg, i) => seg === '.' || seg === '..' || (seg === '' && i < segments.length - 1))) {
        fail('"source" must not contain empty, "." or ".." path segments');
    }
    if (key.endsWith('/')) fail('"source" must name a file or a glob such as s3://bucket/prefix/*.parquet, not a directory');
    const g = source.search(GLOB_CHARS_RE);
    return { source, bucket, key, literalPrefix: g === -1 ? source : source.slice(0, g) };
}

/**
 * IMPORT_ALLOWED_SOURCES: comma-separated s3:// prefixes non-superusers may
 * import from. A bare bucket (s3://b) means the whole bucket; every entry is
 * treated as a directory prefix (s3://b/raw means s3://b/raw/...).
 */
export function importAllowList(env = process.env) {
    return String(env.IMPORT_ALLOWED_SOURCES || '').split(',').map(s => s.trim()).filter(Boolean)
        .filter(s => /^s3:\/\/[a-z0-9][a-z0-9.-]{1,61}[a-z0-9](\/.*)?$/.test(s))
        .map(withTrailingSlash);
}

/**
 * True when everything a parsed source can match lies under an allow-listed
 * prefix and outside the lake's own data path (reading the lake's Parquet
 * files directly would bypass table ACLs).
 */
export function isImportSourceAllowed(parsed, env = process.env) {
    const prefix = parsed.literalPrefix;
    if (!importAllowList(env).some(entry => prefix.startsWith(entry))) return false;
    let lake = null;
    try { lake = storageConfig(env); } catch (_) { /* invalid lake config: nothing to protect */ }
    if (lake?.kind === 's3' && (prefix.startsWith(lake.dataPath) || lake.dataPath.startsWith(prefix))) return false;
    return true;
}

/** Human-readable location for logs and the console (never includes credentials). */
export function describeStorage(cfg) {
    if (cfg.kind === 'local') return `local directory ${cfg.dataPath}`;
    const where = cfg.endpoint ? `${cfg.useSsl ? 'https' : 'http'}://${cfg.endpoint}` : `AWS S3 (${cfg.region})`;
    return `${cfg.dataPath} on ${where}, ${cfg.urlStyle}-style, ${cfg.provider === 'config' ? 'static key' : 'credential chain'}`;
}

/**
 * Rows written by a single INSERT below this count are kept in the metadata
 * catalog (Postgres) instead of creating a Parquet file; scheduled maintenance
 * flushes them to Parquet in bulk. 0 disables inlining.
 */
export function dataInliningRowLimit(env = process.env) {
    const raw = String(env.DATA_INLINING_ROW_LIMIT ?? '').trim();
    if (raw === '') return 1000;
    const n = Number(raw);
    return Number.isInteger(n) && n >= 0 ? Math.min(n, 1_000_000) : 1000;
}
