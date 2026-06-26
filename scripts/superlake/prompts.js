/**
 * Tiny readline-based prompt helpers used by the SuperLake wizard.
 * No third-party dependencies: relies only on node:readline/promises.
 */

import { createInterface } from 'node:readline/promises';
import { stdin, stdout } from 'node:process';

let rl = null;

// Non-TTY mode: read every line of stdin upfront and serve answers from the
// buffer. readline/promises.question() hangs forever on EOF mid-session, so
// piping a scripted answer file otherwise deadlocks the wizard.
let scriptedQueue = null;
let scriptedIdx   = 0;

async function ensureScriptedQueue() {
    if (scriptedQueue !== null) return;
    if (stdin.isTTY) { scriptedQueue = []; return; }
    const chunks = [];
    for await (const chunk of stdin) chunks.push(chunk);
    const buf = Buffer.concat(chunks).toString('utf8');
    scriptedQueue = buf.length === 0 ? [] : buf.replace(/\r\n/g, '\n').split('\n');
    // A trailing newline produces an empty final element; drop it.
    if (scriptedQueue.length && scriptedQueue[scriptedQueue.length - 1] === '') {
        scriptedQueue.pop();
    }
}

function nextScripted() {
    if (!scriptedQueue || scriptedIdx >= scriptedQueue.length) return undefined;
    return scriptedQueue[scriptedIdx++];
}

function getRl() {
    if (!rl) rl = createInterface({ input: stdin, output: stdout });
    return rl;
}

async function ask(promptText) {
    if (!stdin.isTTY) {
        await ensureScriptedQueue();
        const v = nextScripted();
        if (v === undefined) {
            throw new Error('Scripted input exhausted at prompt: ' + promptText.trim());
        }
        // Echo the prompt + answer so scripted runs are still readable.
        stdout.write(promptText + v + '\n');
        return v;
    }
    return getRl().question(promptText);
}

export function closePrompts() {
    if (rl) { rl.close(); rl = null; }
}

// ANSI helpers (no-op when stdout is not a TTY).
const isTty = stdout.isTTY;
export const c = {
    bold:    s => isTty ? `\x1b[1m${s}\x1b[0m`  : s,
    dim:     s => isTty ? `\x1b[2m${s}\x1b[0m`  : s,
    cyan:    s => isTty ? `\x1b[36m${s}\x1b[0m` : s,
    green:   s => isTty ? `\x1b[32m${s}\x1b[0m` : s,
    yellow:  s => isTty ? `\x1b[33m${s}\x1b[0m` : s,
    red:     s => isTty ? `\x1b[31m${s}\x1b[0m` : s,
};

export function banner(title, subtitle = '') {
    const bar = '═'.repeat(Math.max(title.length, subtitle.length) + 4);
    console.log('\n' + c.cyan(bar));
    console.log('  ' + c.bold(title));
    if (subtitle) console.log('  ' + c.dim(subtitle));
    console.log(c.cyan(bar) + '\n');
}

export function section(label) {
    console.log('\n' + c.bold(c.yellow('▸ ' + label)));
}

/**
 * Free-form text prompt. Returns the trimmed answer or the default.
 */
export async function prompt(question, defaultValue) {
    const suffix = defaultValue !== undefined && defaultValue !== ''
        ? c.dim(` [${defaultValue}]`)
        : '';
    const answer = (await ask(`${question}${suffix}: `)).trim();
    if (answer === '' && defaultValue !== undefined) return String(defaultValue);
    return answer;
}

/**
 * Same as prompt() but loops until a non-empty value is supplied.
 */
export async function promptRequired(question, defaultValue) {
    while (true) {
        const v = await prompt(question, defaultValue);
        if (v && v.length > 0) return v;
        console.log(c.red('  ↳ A value is required.'));
    }
}

/**
 * Integer prompt with inclusive bounds.
 */
export async function promptInt(question, defaultValue, { min = 1, max = 65535 } = {}) {
    while (true) {
        const raw = await prompt(question, defaultValue);
        const n = parseInt(raw, 10);
        if (Number.isFinite(n) && n >= min && n <= max) return n;
        console.log(c.red(`  ↳ Enter an integer between ${min} and ${max}.`));
    }
}

/**
 * Yes/No prompt. Returns boolean.
 */
export async function promptYN(question, defaultYes = true) {
    const sfx = defaultYes ? c.dim('[Y/n]') : c.dim('[y/N]');
    const raw = (await ask(`${question} ${sfx}: `)).trim().toLowerCase();
    if (!raw) return defaultYes;
    return raw === 'y' || raw === 'yes';
}

/**
 * Numbered single-choice picker. Returns 1-based index.
 * `choices` is an array of strings.
 */
export async function promptChoice(question, choices, defaultIdx = 1) {
    console.log(c.bold(question));
    choices.forEach((label, i) => {
        const marker = (i + 1) === defaultIdx ? c.cyan('●') : ' ';
        console.log(`  ${marker} ${i + 1}) ${label}`);
    });
    while (true) {
        const raw = await prompt('Choice', defaultIdx);
        const n = parseInt(raw, 10);
        if (Number.isFinite(n) && n >= 1 && n <= choices.length) return n;
        console.log(c.red(`  ↳ Enter 1..${choices.length}.`));
    }
}

/**
 * Comma-separated list prompt; trims empties.
 */
export async function promptList(question, defaultValue = '') {
    const raw = await prompt(question, defaultValue);
    return raw.split(',').map(s => s.trim()).filter(Boolean);
}

/**
 * Generate a random hex token (used for API keys / MinIO secrets).
 */
export function randomToken(bytes = 24) {
    return Array.from({ length: bytes }, () =>
        Math.floor(Math.random() * 256).toString(16).padStart(2, '0')
    ).join('');
}
