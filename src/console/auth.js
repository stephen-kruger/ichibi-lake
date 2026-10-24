/**
 * Console authentication primitives. Pure functions (hashing, tokens, input
 * validation, throttling) are exported for unit testing; nothing here touches
 * the database.
 */
import { scrypt, randomBytes, timingSafeEqual, createHmac, createHash } from 'crypto';
import { promisify } from 'util';

const scryptAsync = promisify(scrypt);

// scrypt parameters: N=2^15, r=8, p=1 (~32 MiB, ~50-100 ms per hash).
const SCRYPT = { N: 32768, r: 8, p: 1, keyLen: 64, maxmem: 64 * 1024 * 1024 };

export const ROLES = ['admin', 'viewer'];
export const SESSION_COOKIE = 'ichibi_console';
const USERNAME_RE = /^[A-Za-z0-9_.@-]{3,64}$/;
export const MIN_PASSWORD_LENGTH = 10;

export function validateUsername(username) {
    if (typeof username !== 'string' || !USERNAME_RE.test(username)) {
        return 'Username must be 3-64 characters: letters, digits, and _ . @ -';
    }
    return null;
}

export function validatePassword(password) {
    if (typeof password !== 'string' || password.length < MIN_PASSWORD_LENGTH) {
        return `Password must be at least ${MIN_PASSWORD_LENGTH} characters`;
    }
    if (password.length > 256) return 'Password is too long';
    return null;
}

export async function hashPassword(password) {
    const salt = randomBytes(16);
    const key = await scryptAsync(password, salt, SCRYPT.keyLen, SCRYPT);
    return `scrypt$${SCRYPT.N}$${SCRYPT.r}$${SCRYPT.p}$${salt.toString('base64')}$${key.toString('base64')}`;
}

export async function verifyPassword(password, stored) {
    if (typeof password !== 'string' || typeof stored !== 'string') return false;
    const parts = stored.split('$');
    if (parts.length !== 6 || parts[0] !== 'scrypt') return false;
    const [, N, r, p, saltB64, keyB64] = parts;
    const expected = Buffer.from(keyB64, 'base64');
    const key = await scryptAsync(password, Buffer.from(saltB64, 'base64'), expected.length, {
        N: Number(N), r: Number(r), p: Number(p), maxmem: SCRYPT.maxmem,
    });
    return key.length === expected.length && timingSafeEqual(key, expected);
}

// A password hash is never used to spend a real check on unknown usernames;
// verifying against this dummy keeps login timing uniform.
let dummyHash = null;
export async function getDummyHash() {
    if (!dummyHash) dummyHash = await hashPassword(randomBytes(16).toString('hex'));
    return dummyHash;
}

/**
 * Fingerprint of the stored password hash embedded in session tokens, so a
 * password change (or user deletion) invalidates every existing session.
 */
export function passwordFingerprint(passwordHash) {
    return createHash('sha256').update(String(passwordHash)).digest('base64url').slice(0, 16);
}

function sign(payload, secret) {
    return createHmac('sha256', secret).update(payload).digest('base64url');
}

export function createSessionToken({ username, passwordHash, ttlMs, secret, now = Date.now() }) {
    const body = Buffer.from(JSON.stringify({
        u: username,
        v: passwordFingerprint(passwordHash),
        exp: now + ttlMs,
    })).toString('base64url');
    return `${body}.${sign(body, secret)}`;
}

/** Returns { username, fingerprint } for a valid, unexpired token, else null. */
export function verifySessionToken(token, secret, now = Date.now()) {
    if (typeof token !== 'string') return null;
    const dot = token.indexOf('.');
    if (dot <= 0) return null;
    const body = token.slice(0, dot);
    const sig = Buffer.from(token.slice(dot + 1));
    const expected = Buffer.from(sign(body, secret));
    if (sig.length !== expected.length || !timingSafeEqual(sig, expected)) return null;
    let payload;
    try { payload = JSON.parse(Buffer.from(body, 'base64url').toString('utf8')); } catch { return null; }
    if (!payload || typeof payload.u !== 'string' || typeof payload.exp !== 'number') return null;
    if (payload.exp <= now) return null;
    return { username: payload.u, fingerprint: payload.v, exp: payload.exp };
}

export function parseCookies(header) {
    const out = {};
    if (!header) return out;
    for (const part of String(header).split(';')) {
        const eq = part.indexOf('=');
        if (eq < 0) continue;
        const k = part.slice(0, eq).trim();
        if (!k) continue;
        try { out[k] = decodeURIComponent(part.slice(eq + 1).trim()); } catch { /* skip malformed */ }
    }
    return out;
}

export function sessionCookie(token, { maxAgeMs, secure }) {
    const attrs = [
        `${SESSION_COOKIE}=${encodeURIComponent(token)}`,
        'Path=/console',
        'HttpOnly',
        'SameSite=Strict',
        `Max-Age=${Math.max(0, Math.floor(maxAgeMs / 1000))}`,
    ];
    if (secure) attrs.push('Secure');
    return attrs.join('; ');
}

/**
 * Sliding-window login throttle keyed by client (IP) and by username, so an
 * attacker cannot brute-force one account from many IPs or many accounts
 * from one IP. Returns seconds to wait (0 = allowed).
 */
export class LoginThrottle {
    constructor({ maxFailures = 5, windowMs = 15 * 60 * 1000, now = () => Date.now() } = {}) {
        this.maxFailures = maxFailures;
        this.windowMs = windowMs;
        this.now = now;
        this.failures = new Map();
    }

    _recent(key) {
        const cutoff = this.now() - this.windowMs;
        const list = (this.failures.get(key) || []).filter(t => t > cutoff);
        if (list.length) this.failures.set(key, list); else this.failures.delete(key);
        return list;
    }

    retryAfterSeconds(keys) {
        let wait = 0;
        for (const key of keys) {
            const list = this._recent(key);
            if (list.length >= this.maxFailures) {
                wait = Math.max(wait, Math.ceil((list[0] + this.windowMs - this.now()) / 1000));
            }
        }
        return wait;
    }

    fail(keys) {
        for (const key of keys) {
            const list = this._recent(key);
            list.push(this.now());
            this.failures.set(key, list);
        }
    }

    succeed(keys) {
        for (const key of keys) this.failures.delete(key);
    }
}

/** Constant-time string comparison for setup codes. */
export function safeEqual(a, b) {
    const ba = Buffer.from(String(a ?? ''));
    const bb = Buffer.from(String(b ?? ''));
    return ba.length === bb.length && timingSafeEqual(ba, bb);
}

export function generateSetupCode() {
    // 12 chars from an unambiguous alphabet, grouped for easy copying.
    const alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
    const bytes = randomBytes(12);
    let code = '';
    for (let i = 0; i < 12; i++) code += alphabet[bytes[i] % alphabet.length];
    return `${code.slice(0, 4)}-${code.slice(4, 8)}-${code.slice(8)}`;
}

export function generateApiKey() {
    return `ichibi_${randomBytes(24).toString('base64url')}`;
}
