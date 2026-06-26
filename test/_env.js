/**
 * Shared test bootstrap: resolves API keys and base URL from docker/.env so
 * every integration suite authorises against the same live gateway the
 * docker compose stack is configured to accept.  No mocking — all tests
 * here are expected to run against the running container.
 *
 *   API_KEY     — primary key, first entry in docker/.env API_KEYS=
 *   ALT_API_KEY — second entry if present, otherwise falls back to API_KEY
 *   BASE_URL    — defaults to http://localhost:${PORT:-3333} where PORT comes
 *                 from docker/.env's PORT or TAILSCALE_PORT entry
 *
 * Each value may be overridden with the matching environment variable.
 */
import { readFileSync } from 'fs';

function readDockerEnv() {
    try {
        const envPath = new URL('../docker/.env', import.meta.url);
        return readFileSync(envPath, 'utf8');
    } catch (_) { return ''; }
}

const ENV_TEXT = readDockerEnv();

function envValue(name) {
    const m = ENV_TEXT.match(new RegExp(`^${name}=(.*)$`, 'm'));
    return m ? m[1].trim() : '';
}

const KEYS_RAW = envValue('API_KEYS');
export const KEYS = KEYS_RAW ? KEYS_RAW.split(',').map(s => s.trim()).filter(Boolean) : [];

const PORT_FROM_ENV = envValue('PORT') || envValue('TAILSCALE_PORT') || '3333';

export const API_KEY  = process.env.API_KEY     || KEYS[0] || '';
export const ALT_KEY  = process.env.ALT_API_KEY || KEYS[1] || API_KEY;
export const BASE_URL = process.env.BASE_URL    || `http://localhost:${PORT_FROM_ENV}`;
