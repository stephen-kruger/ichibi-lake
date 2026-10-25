/**
 * Idempotency-Key support for ingestion endpoints (POST /upload,
 * POST /kafka-sink, POST /tables/:tableName/import).
 *
 * A client that retries a write after a timeout or dropped connection sends
 * the same `Idempotency-Key` header. The first request with a key claims it;
 * when it succeeds (2xx) its status and JSON body are stored, and every later
 * request with that key and the same request hash gets the stored response
 * back (header `Idempotent-Replayed: true`) without writing anything again.
 *   - same key, different body / target       -> 409
 *   - same key while the first is still running -> 409 (Retry-After)
 *   - first request failed (non-2xx, crashed)   -> the key is released and a
 *                                                  retry runs normally
 * Keys are scoped to the API key that sent them and expire after
 * IDEMPOTENCY_TTL_HOURS (default 24).
 *
 * Records live in the console catalog (`_console_idempotency`, see
 * src/console/store.js), shared by every gateway instance on the catalog.
 * DuckLake has no unique constraints and does not serialise concurrent
 * conditional writes, so rows are append-only: a request appends a 'pending'
 * claim, re-reads, and owns the key only if its claim is the first live one
 * to have committed.
 */
import os from 'os';
import { createHash, randomUUID } from 'crypto';
import * as store from './console/store.js';

const KEY_RE = /^[\x21-\x7e]{1,255}$/;
// A claim whose request never finished (process killed mid-request) stops
// blocking the key after this long.
const PENDING_LEASE_MS = 60 * 60 * 1000;
// Responses larger than this are not stored; the key is released instead.
const MAX_STORED_RESPONSE = 256 * 1024;
const PURGE_INTERVAL_MS = 10 * 60 * 1000;
const HOST = `${os.hostname()}:${process.pid}`;

let lastPurgeMs = 0;

function ttlMs() {
    const hours = Number(process.env.IDEMPOTENCY_TTL_HOURS);
    return (Number.isFinite(hours) && hours > 0 ? Math.min(hours, 24 * 365) : 24) * 60 * 60 * 1000;
}

function httpError(statusCode, message) {
    return Object.assign(new Error(message), { statusCode });
}

/** Incremental SHA-256 over the parts that identify a request. */
export function requestHasher(req) {
    const hash = createHash('sha256');
    hash.update(`${req.method} ${req.baseUrl || ''}${req.path}\n`);
    const query = Object.entries(req.query || {}).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0));
    hash.update(`${JSON.stringify(query)}\n`);
    return hash;
}

/** Hash for a request whose body express.json() already parsed. */
export function jsonRequestHash(req, extra = '') {
    const hash = requestHasher(req);
    hash.update(`${extra}\n`);
    hash.update(JSON.stringify(req.body ?? null));
    return hash.digest('hex');
}

function scopeOf(req) {
    return createHash('sha256').update(String(req.apiKey || '')).digest('hex').slice(0, 32);
}

/** Validated Idempotency-Key header value, null when absent. Throws 400 when malformed. */
export function idempotencyKeyOf(req) {
    const raw = req.headers['idempotency-key'];
    if (raw === undefined) return null;
    const key = String(raw).trim();
    if (!KEY_RE.test(key)) throw httpError(400, 'Idempotency-Key must be 1-255 printable ASCII characters');
    return key;
}

/**
 * Summarise the rows for one key: the live tokens in commit order with their
 * final state ('done' wins over 'released' wins over 'pending').
 */
function tokensOf(rows, nowMs) {
    const byToken = new Map();
    for (const r of rows) {
        const t = byToken.get(r.token) || { token: r.token, firstSeq: r.seq, firstMs: r.createdMs, requestHash: r.requestHash, state: 'pending' };
        if (r.state === 'done') Object.assign(t, { state: 'done', statusCode: r.statusCode, response: r.response });
        else if (r.state === 'released' && t.state !== 'done') t.state = 'released';
        byToken.set(r.token, t);
    }
    return [...byToken.values()]
        .filter(t => t.state === 'done' || (t.state === 'pending' && t.firstMs + PENDING_LEASE_MS > nowMs))
        .sort((a, b) => a.firstSeq - b.firstSeq);
}

function replay(res, owner) {
    res.status(owner.statusCode || 200).set('Idempotent-Replayed', 'true').type('application/json').send(owner.response);
}

function conflict(res, owner) {
    if (owner.state === 'pending') {
        return res.status(409).set('Retry-After', '5').json({ success: false, error: 'A request with this Idempotency-Key is still in progress; retry later.' });
    }
    return res.status(409).json({ success: false, error: 'This Idempotency-Key was already used for a different request.' });
}

function purgeSoon(nowMs) {
    if (nowMs - lastPurgeMs < PURGE_INTERVAL_MS) return;
    lastPurgeMs = nowMs;
    store.purgeIdempotency(nowMs).catch(err => console.warn('[Idempotency] Purge of expired keys failed:', err.message || err));
}

/**
 * Claim the request's Idempotency-Key (if it sent one). Returns true when the
 * response was already sent (a replay or a 409) and the handler must stop.
 * Otherwise the handler runs as usual: its first res.json() call stores a 2xx
 * response under the key (before it is sent) or releases the key.
 */
export async function beginIdempotentRequest(req, res, requestHash) {
    const key = idempotencyKeyOf(req);
    if (!key) return false;
    const scope = scopeOf(req);
    const nowMs = Date.now();
    purgeSoon(nowMs);

    const decide = (tokens) => {
        const owner = tokens[0];
        if (!owner) return null;
        if (owner.requestHash !== requestHash) { conflict(res, { state: 'done' }); return true; }
        if (owner.state === 'done') { replay(res, owner); return true; }
        return owner;
    };

    const existing = decide(tokensOf(await store.idempotencyRows(scope, key, nowMs), nowMs));
    if (existing === true) return true;
    if (existing) { conflict(res, existing); return true; }

    const token = `${HOST}:${randomUUID()}`;
    const expiresMs = nowMs + ttlMs();
    const row = { scope, key, requestHash, token, createdMs: nowMs, expiresMs };
    await store.appendIdempotencyRow({ ...row, state: 'pending' });
    // Another request may have claimed the key at the same moment: the claim
    // committed first wins and the others back off. Ordering by commit (row
    // id), not by request time, is what makes this safe: if our re-read missed
    // another claim, that claim committed after ours and will see ours.
    const winner = decide(tokensOf(await store.idempotencyRows(scope, key, Date.now()), Date.now()));
    if (winner !== null && winner !== true && winner.token !== token) {
        await store.appendIdempotencyRow({ ...row, state: 'released', createdMs: Date.now() });
        conflict(res, winner);
        return true;
    }
    if (winner === true) {
        await store.appendIdempotencyRow({ ...row, state: 'released', createdMs: Date.now() });
        return true;
    }

    let settled = false;
    const release = () => store.appendIdempotencyRow({ ...row, state: 'released', createdMs: Date.now() })
        .catch(err => console.warn(`[Idempotency] Could not release key: ${err.message || err}`));
    const originalJson = res.json.bind(res);
    res.json = (body) => {
        res.json = originalJson;
        settled = true;
        const statusCode = res.statusCode;
        const response = JSON.stringify(body);
        const done = statusCode >= 200 && statusCode < 300 && response !== undefined && response.length <= MAX_STORED_RESPONSE
            ? store.appendIdempotencyRow({ ...row, state: 'done', statusCode, response, createdMs: Date.now() })
                .catch((err) => { console.warn(`[Idempotency] Could not store result: ${err.message || err}`); return release(); })
            : release();
        done.finally(() => originalJson(body));
        return res;
    };
    // The client went away (or the handler streamed / errored without JSON):
    // free the key so a retry is not blocked until the lease runs out.
    res.on('close', () => { if (!settled) { settled = true; release(); } });
    return false;
}

/** Express middleware for routes whose JSON body is already parsed. */
export function idempotentJson(extra = () => '') {
    return async (req, res, next) => {
        try {
            if (req.headers['idempotency-key'] === undefined) return next();
            if (await beginIdempotentRequest(req, res, jsonRequestHash(req, extra(req)))) return;
            next();
        } catch (err) {
            res.status(err.statusCode || 500).json({ success: false, error: err.message });
        }
    };
}
