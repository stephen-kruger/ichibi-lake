/**
 * Update check for the admin console: compares the running version with the
 * version in package.json on the GitHub repository's main branch (the
 * project publishes no releases or tags). Results are cached so the console
 * contacts GitHub at most once per CACHE_MS, and every failure is reported as
 * "unknown" rather than an error.
 */
export const REPO_URL = 'https://github.com/stephen-kruger/ichibi-lake';
export const UPDATE_COMMAND = 'git pull && docker compose up -d --build';
const REMOTE_PACKAGE_URL = 'https://raw.githubusercontent.com/stephen-kruger/ichibi-lake/main/package.json';
const CACHE_MS = 6 * 60 * 60 * 1000;
const FAILURE_CACHE_MS = 30 * 60 * 1000;
const TIMEOUT_MS = 5000;

/** Compare dotted numeric versions ("1.6.25.1"); returns -1, 0 or 1, or null if either is unparseable. */
export function compareVersions(a, b) {
    const parse = (v) => {
        const s = String(v ?? '').trim().replace(/^v/i, '');
        return /^\d+(\.\d+)*$/.test(s) ? s.split('.').map(Number) : null;
    };
    const x = parse(a), y = parse(b);
    if (!x || !y) return null;
    for (let i = 0; i < Math.max(x.length, y.length); i++) {
        const d = (x[i] || 0) - (y[i] || 0);
        if (d) return d > 0 ? 1 : -1;
    }
    return 0;
}

let cache = null; // { at, ttl, latest }
let pending = null;

async function fetchLatestVersion(fetchImpl) {
    try {
        const res = await fetchImpl(REMOTE_PACKAGE_URL, {
            headers: { Accept: 'application/json' },
            signal: AbortSignal.timeout(TIMEOUT_MS),
        });
        if (!res.ok) return null;
        const version = (await res.json())?.version;
        return compareVersions(version, version) === 0 ? String(version) : null;
    } catch {
        return null;
    }
}

/**
 * Report whether a newer version than `current` is available.
 * @returns {Promise<{current, latest, updateAvailable, repoUrl, updateCommand}>}
 *   `latest` is null when GitHub could not be reached.
 */
export async function checkForUpdate(current, { fetchImpl = globalThis.fetch, now = Date.now() } = {}) {
    if (!cache || now - cache.at >= cache.ttl) {
        pending ||= fetchLatestVersion(fetchImpl).then((latest) => {
            cache = { at: now, ttl: latest ? CACHE_MS : FAILURE_CACHE_MS, latest };
            return latest;
        }).finally(() => { pending = null; });
        await pending;
    }
    const latest = cache.latest;
    return {
        current,
        latest,
        updateAvailable: !!latest && compareVersions(latest, current) === 1,
        repoUrl: REPO_URL,
        updateCommand: UPDATE_COMMAND,
    };
}

/** Test hook: forget the cached result. */
export function resetUpdateCheckCache() {
    cache = null;
    pending = null;
}
