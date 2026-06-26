/**
 * File-system and formatting helpers shared by all wizard option modules.
 */

import fs from 'node:fs/promises';
import path from 'node:path';
import { c } from './prompts.js';

/**
 * Workspace root = parent of the `scripts/` directory.
 * Resolved from this file's location so the wizard works regardless of cwd.
 */
export const REPO_ROOT = path.resolve(new URL('.', import.meta.url).pathname, '..', '..');
export const OUTPUT_BASE = path.join(REPO_ROOT, 'superlake');

/**
 * Ensure the per-option output directory exists.
 * Returns the absolute path and a relative path (for log messages).
 */
export async function ensureOutputDir(slug) {
    const abs = path.join(OUTPUT_BASE, slug);
    await fs.mkdir(abs, { recursive: true });
    return { abs, rel: path.relative(REPO_ROOT, abs) };
}

/**
 * Write a file under the given output dir. Returns the relative path written.
 * If `executable` is true, chmod 0755 (for .sh helpers).
 */
export async function writeOutput(outDir, name, content, { executable = false } = {}) {
    const full = path.join(outDir.abs, name);
    await fs.writeFile(full, content, 'utf8');
    if (executable) await fs.chmod(full, 0o755);
    const rel = path.join(outDir.rel, name);
    console.log(c.green('  ✓ wrote ') + rel);
    return rel;
}

/**
 * Conservative LAN-IP autodetection. Walks os.networkInterfaces() and returns
 * the first non-internal IPv4 that is not in the Docker / link-local ranges.
 * Returns null if nothing plausible is found; the wizard then asks the user.
 */
export async function detectLanIp() {
    const os = await import('node:os');
    const ifs = os.networkInterfaces();
    const candidates = [];
    for (const list of Object.values(ifs)) {
        if (!list) continue;
        for (const a of list) {
            if (a.family !== 'IPv4' || a.internal) continue;
            // Skip docker bridges and link-local.
            if (/^(172\.1[6-9]|172\.2[0-9]|172\.3[0-1])\./.test(a.address)) continue;
            if (/^169\.254\./.test(a.address)) continue;
            candidates.push(a.address);
        }
    }
    // Prefer 10/8 then 192.168/16 then 100.64/10 (Tailscale CGNAT) then anything.
    const score = ip =>
        /^10\./.test(ip)      ? 0 :
        /^192\.168\./.test(ip) ? 1 :
        /^100\.(6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\./.test(ip) ? 2 : 3;
    candidates.sort((a, b) => score(a) - score(b));
    return candidates[0] || null;
}

/**
 * Render an env-file. Accepts {KEY: value, ...}. Skips undefined values.
 * `comments` is an optional {KEY: 'leading comment'} map; comments are emitted
 * on the line above the variable.
 */
export function renderEnv(vars, comments = {}, header = '') {
    const lines = [];
    if (header) {
        for (const h of header.split('\n')) lines.push('# ' + h);
        lines.push('');
    }
    for (const [k, v] of Object.entries(vars)) {
        if (v === undefined || v === null) continue;
        if (comments[k]) {
            for (const cmt of String(comments[k]).split('\n')) lines.push('# ' + cmt);
        }
        // Quote values that contain spaces, =, or shell-significant chars.
        const needsQuotes = /[\s=#"'$`\\]/.test(String(v));
        lines.push(needsQuotes ? `${k}="${String(v).replace(/"/g, '\\"')}"` : `${k}=${v}`);
    }
    return lines.join('\n') + '\n';
}

/**
 * Tiny YAML-string helper: indent every line of `text` by `n` spaces.
 */
export function indent(text, n) {
    const pad = ' '.repeat(n);
    return text.split('\n').map(l => l.length ? pad + l : l).join('\n');
}

/**
 * Print a final summary block with consistent formatting.
 */
export function summary(title, lines) {
    console.log('\n' + c.cyan('═'.repeat(60)));
    console.log(c.bold('  ' + title));
    console.log(c.cyan('═'.repeat(60)));
    for (const line of lines) console.log(line);
    console.log('');
}
