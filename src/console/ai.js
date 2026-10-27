/**
 * "AI" console pages: ask questions about the lake in plain language, answered
 * by an OpenAI-compatible chat model that writes and runs DuckDB SQL through
 * tool calls, and save the resulting SQL as named "functions" to run again
 * later (optionally with {{parameters}}).
 *
 * Configuration
 * -------------
 * OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL and the AI_* limits are normal
 * registry settings (src/console/registry.js, section 'ai-settings', hidden
 * from the generic settings pages): docker/.env supplies the defaults and the
 * AI Settings page stores console overrides, read from process.env on use.
 *
 * Safety model
 * ------------
 * - Every statement, whether written by the model, edited by a person or
 *   stored in a saved function, runs through runReadOnlySql(): the read-only
 *   guard (src/sql-guard.js), then DuckDB's own parse with the table-scoped
 *   policy used for non-superuser API keys (src/sql-tables.js: one SELECT, no
 *   table functions, only ducklake.main tables, no "_" system tables), then
 *   execution on the isolated user-query instance with its timeout and row cap.
 * - What the model sees is limited to table schemas, row counts and at most
 *   AI_SAMPLE_ROWS rows per result (0: no data values at all).
 * - Parameters in saved functions are substituted as escaped string literals
 *   before the checks above, so a value can never change the statement.
 */
import { randomBytes } from 'crypto';
import { execute, serializeSql, findLakeTable, streamUserSql } from '../db.js';
import { assertReadOnlySql } from '../sql-guard.js';
import { analyzeUserSql } from '../sql-tables.js';
import { getTableKeys } from '../table-keys.js';
import { SETTINGS, normaliseSettingValue, maskSecret } from './registry.js';
import * as store from './store.js';
import { listTables, quoteIdent, quoteLiteral, isSystemTable, httpError, cleanDuckDbError } from './data.js';

export const AI_SETTINGS = SETTINGS.filter(s => s.section === 'ai-settings');
const AI_KEYS = new Set(AI_SETTINGS.map(s => s.key));

const DISPLAY_MAX_ROWS = 1000;       // rows returned to the browser per result
const MODEL_CELL_CHARS = 200;        // characters per cell sent to the model
const SCHEMA_PROMPT_CHARS = 24000;   // budget for the table list in the system prompt
const EXAMPLE_COUNT = 4;             // saved functions shown as worked examples
const EXAMPLE_SQL_CHARS = 1500;      // per example; longer ones are skipped
const CALLABLE_COUNT = 25;           // saved functions offered to the model as run_function targets
const HISTORY_TURNS = 10;
const HISTORY_CHARS = 4000;
const QUESTION_CHARS = 4000;
const SQL_CHARS = 20000;
const PARAM_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_]{0,62})\s*\}\}/g;

// --- Configuration ----------------------------------------------------------------

/** Effective AI configuration (console override ?? docker/.env ?? default). */
export function aiConfig(overrides = {}) {
    const get = (key) => {
        if (overrides[key] !== undefined) return String(overrides[key]);
        const def = AI_SETTINGS.find(s => s.key === key);
        return store.describeSetting(def).value;
    };
    const int = (key, fallback) => {
        const n = parseInt(get(key), 10);
        return Number.isFinite(n) ? n : fallback;
    };
    return {
        baseUrl: get('OPENAI_BASE_URL').trim().replace(/\/+$/, ''),
        apiKey: get('OPENAI_API_KEY').trim(),
        model: get('OPENAI_MODEL').trim(),
        timeoutMs: int('AI_REQUEST_TIMEOUT_MS', 120000),
        maxSteps: int('AI_MAX_STEPS', 8),
        sampleRows: int('AI_SAMPLE_ROWS', 20),
    };
}

export function isConfigured(cfg = aiConfig()) {
    return !!(cfg.baseUrl && cfg.model);
}

/** The AI settings for the settings page, secrets masked. */
export function settingsView() {
    return {
        configured: isConfigured(),
        settings: AI_SETTINGS.map(def => {
            const d = store.describeSetting(def);
            const secret = def.type === 'secret';
            return {
                key: def.key, label: def.label, help: def.help || '', type: def.type, default: def.default ?? '',
                min: def.min ?? null, max: def.max ?? null,
                value: secret ? '' : d.value,
                hasValue: !!d.value,
                masked: secret ? maskSecret(d.value) : null,
                source: d.source,
                envValue: secret ? (d.envValue ? maskSecret(d.envValue) : null) : d.envValue,
                updatedAt: d.updatedAt, updatedBy: d.updatedBy,
            };
        }),
    };
}

/**
 * Validate a { KEY: value } map from the settings form. An empty secret means
 * "keep the current value" and is left out. Returns { KEY: canonical value }.
 */
function normaliseForm(values) {
    if (!values || typeof values !== 'object') throw httpError(400, 'Send { "values": { "OPENAI_MODEL": "...", ... } }');
    const out = {};
    for (const [key, raw] of Object.entries(values)) {
        if (!AI_KEYS.has(key)) throw httpError(400, `Unknown AI setting ${key}`);
        const def = AI_SETTINGS.find(s => s.key === key);
        if (def.type === 'secret' && (raw === '' || raw === null || raw === undefined)) continue;
        // Optional settings may be blanked (the API key is handled above).
        const { value, error } = normaliseSettingValue(def, raw);
        if (error) throw httpError(400, `${def.label}: ${error}`);
        out[key] = value;
    }
    return out;
}

/** Save the changed values as console overrides. Returns the changed keys. */
export async function saveSettings(values, username) {
    const form = normaliseForm(values);
    const changed = [];
    for (const [key, value] of Object.entries(form)) {
        const def = AI_SETTINGS.find(s => s.key === key);
        const current = store.describeSetting(def);
        if (current.value === value && current.source === 'console') continue;
        if (current.value === value && def.type !== 'secret') continue;
        await store.setOverride(key, value, username);
        changed.push(key);
    }
    return changed;
}

/** Drop every AI console override (back to docker/.env or defaults). */
export async function resetSettings() {
    for (const def of AI_SETTINGS) await store.clearOverride(def.key);
}

// --- Model API --------------------------------------------------------------------

/**
 * Error from the model API. Its message is shown in the console as is
 * (`expose`), unlike other 5xx errors, because it describes the configured
 * external service rather than the gateway.
 */
function modelError(statusCode, message, extra = {}) {
    return Object.assign(httpError(statusCode, message), { expose: true }, extra);
}

// What to do about common model API statuses (OpenAI and OpenRouter semantics).
const STATUS_HINTS = {
    400: 'Check that the model name is a chat model with tool support.',
    401: 'The API key is missing or invalid; check it on the AI Settings page.',
    402: 'The account has no credits left; add credits or choose a free model.',
    403: 'The API key may not use this model, or the request was blocked by moderation.',
    404: 'The model or API base URL was not found; check both on the AI Settings page.',
    408: 'The model took too long; try again or raise the timeout on the AI Settings page.',
    413: 'The request is too large for this model; ask a narrower question or start a new conversation.',
    429: 'Rate limited; wait a moment and try again (free models have tight limits), or choose another model.',
    500: 'The model provider had an internal error; try again or choose another model.',
    502: 'The provider behind the API failed or is down; try again or choose another model.',
    503: 'No provider is available for this model right now; try again later or choose another model.',
};

/** Human-readable detail from an OpenAI/OpenRouter error body. */
function errorDetail(data, text, statusText) {
    const err = data?.error;
    let detail = (typeof err === 'string' ? err : err?.message) || data?.message || data?.detail || text.slice(0, 300) || statusText || 'no details';
    // OpenRouter names the upstream provider and passes on its raw error.
    const meta = err?.metadata;
    if (meta?.provider_name) detail += ` (provider: ${meta.provider_name})`;
    if (meta?.raw) {
        let raw = meta.raw;
        if (typeof raw === 'string') { try { raw = JSON.parse(raw); } catch { /* keep text */ } }
        const rawMsg = typeof raw === 'string' ? raw : raw?.error?.message || raw?.message || JSON.stringify(raw);
        if (rawMsg && !detail.includes(rawMsg)) detail += `: ${String(rawMsg).slice(0, 300)}`;
    }
    if (Array.isArray(meta?.reasons) && meta.reasons.length) detail += ` (${meta.reasons.join(', ')})`;
    return detail;
}

function statusError(status, detail) {
    const hint = STATUS_HINTS[status] || (status >= 500 ? STATUS_HINTS[500] : '');
    return modelError(502, `The model API returned HTTP ${status}: ${detail}${hint ? `. ${hint}` : ''}`, { upstreamStatus: status });
}

async function modelRequest(cfg, method, path, body, signal) {
    const timeout = AbortSignal.timeout(cfg.timeoutMs);
    // The body is read under the same deadline: OpenRouter sends the headers
    // at once and the answer when the model is done, so a timeout usually
    // fires while reading it.
    let res, text;
    try {
        res = await fetch(`${cfg.baseUrl}${path}`, {
            method,
            headers: {
                'Content-Type': 'application/json',
                ...(cfg.apiKey ? { Authorization: `Bearer ${cfg.apiKey}` } : {}),
            },
            body: body === undefined ? undefined : JSON.stringify(body),
            signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
        });
        text = await res.text();
    } catch (err) {
        if (timeout.aborted) {
            throw modelError(504, `The model did not answer within ${Math.round(cfg.timeoutMs / 1000)} s (model request timeout on the AI Settings page). ` +
                'Slow or "thinking" models can need longer: raise the timeout, choose a faster model, or ask a narrower question.');
        }
        if (signal?.aborted) throw Object.assign(httpError(499, 'Cancelled'), { cancelled: true });
        if (res) throw modelError(502, `The connection to the model API broke while reading its answer: ${err.cause?.message || err.message}. Try again.`);
        throw modelError(502, `Could not reach the model API at ${cfg.baseUrl}: ${err.cause?.message || err.message}. Check the API base URL; from Docker, use host.docker.internal instead of localhost.`);
    }
    let data = null;
    try { data = JSON.parse(text); } catch { /* not JSON */ }
    if (!res.ok) throw statusError(res.status, errorDetail(data, text, res.statusText));
    if (!data) throw modelError(502, `The model API returned a response that is not JSON: ${text.slice(0, 200) || '(empty)'}. Check the API base URL (it usually ends in /v1).`);
    // OpenRouter can answer HTTP 200 with an error in the body or in the choice.
    const bodyError = data.error || data.choices?.[0]?.error;
    if (bodyError && !data.choices?.[0]?.message?.content && !data.choices?.[0]?.message?.tool_calls) {
        const code = Number(bodyError.code);
        throw statusError(Number.isInteger(code) && code >= 400 ? code : 502, errorDetail({ error: bodyError }, '', ''));
    }
    return data;
}

function chat(cfg, body, signal) {
    return modelRequest(cfg, 'POST', '/chat/completions', { model: cfg.model, ...body }, signal);
}

/**
 * Check a configuration (unsaved form values merged over the saved ones): the
 * models list, a plain completion and a forced tool call, which Ask AI needs.
 * Returns { ok, results: [{ label, ok, detail, warning? }] }.
 */
export async function testSettings(values) {
    const cfg = aiConfig(normaliseForm(values || {}));
    const results = [];
    const step = async (label, fn) => {
        const started = Date.now();
        try {
            const r = await fn();
            results.push({ label, ok: true, detail: `${r.detail} (${Date.now() - started} ms)`, warning: r.warning || null });
            return true;
        } catch (err) {
            results.push({ label, ok: false, detail: err.message });
            return false;
        }
    };
    if (!isConfigured(cfg)) return { ok: false, results: [{ label: 'Configuration', ok: false, detail: 'Set the API base URL and a model.' }] };

    await step('Models list', async () => {
        try {
            const data = await modelRequest(cfg, 'GET', '/models');
            const ids = (Array.isArray(data.data) ? data.data : Array.isArray(data.models) ? data.models : []).map(m => m?.id || m?.name).filter(Boolean);
            return {
                detail: `${ids.length} model(s) available`,
                warning: ids.length && !ids.includes(cfg.model) ? `"${cfg.model}" is not in the list; check the model name.` : null,
            };
        } catch (err) {
            // Some compatible servers have no /models; only a rejected key is fatal.
            if (err.upstreamStatus === 401 || err.upstreamStatus === 403) throw new Error(`The API key was rejected: ${err.message}`);
            if (err.upstreamStatus) return { detail: 'not offered by this server', warning: err.message };
            throw err;
        }
    });
    const chatOk = await step('Chat completion', async () => {
        const data = await chat(cfg, { messages: [{ role: 'user', content: 'Reply with the single word OK.' }] });
        const reply = String(data.choices?.[0]?.message?.content ?? '').trim();
        if (!data.choices?.length) throw new Error('The response has no choices');
        return { detail: `${data.model || cfg.model} replied "${reply.slice(0, 60)}"` };
    });
    if (chatOk) {
        await step('Tool calling', async () => {
            const data = await chat(cfg, {
                messages: [{ role: 'user', content: 'Call the ping tool with message "hello".' }],
                tools: [{ type: 'function', function: { name: 'ping', description: 'Connectivity check', parameters: { type: 'object', properties: { message: { type: 'string' } }, required: ['message'] } } }],
                tool_choice: { type: 'function', function: { name: 'ping' } },
            });
            const call = data.choices?.[0]?.message?.tool_calls?.[0];
            if (!call) throw new Error('The model did not call the tool. Ask AI needs a model with tool (function) calling.');
            return { detail: `called ${call.function?.name}` };
        });
    }
    return { ok: results.every(r => r.ok), results };
}

// --- Running SQL safely ----------------------------------------------------------

/** JSON-safe, display-friendly form of a DuckDB value. */
export function plainValue(v) {
    if (v === null || v === undefined) return null;
    if (typeof v === 'bigint') return Number.isSafeInteger(Number(v)) ? Number(v) : v.toString();
    if (typeof v === 'number') return Number.isFinite(v) ? v : String(v);
    if (typeof v === 'string' || typeof v === 'boolean') return v;
    if (typeof v.toString === 'function' && v.toString !== Object.prototype.toString) return v.toString();
    try { return JSON.stringify(v, (k, x) => (typeof x === 'bigint' ? x.toString() : x)); } catch { return String(v); }
}

/** Throw unless `sql` is one read-only SELECT over user lake tables. Returns the tables it reads. */
export async function checkSql(sql) {
    const text = String(sql ?? '').trim().replace(/;\s*$/, '');
    if (!text) throw httpError(400, 'The SQL is empty');
    if (text.length > SQL_CHARS) throw httpError(400, `The SQL is longer than ${SQL_CHARS} characters`);
    assertReadOnlySql(text);
    let parsed;
    try { parsed = await serializeSql(text); } catch (err) { throw httpError(400, cleanDuckDbError(err)); }
    if (parsed?.error) throw httpError(400, String(parsed.error_message || 'The SQL could not be parsed').slice(0, 300));
    const tables = analyzeUserSql(parsed);
    for (const name of tables) {
        const t = await findLakeTable(name);
        if (!t || isSystemTable(t.name)) throw httpError(404, `Table "${name}" not found`);
    }
    return { sql: text, tables };
}

/**
 * Check and run read-only SQL on the user-query instance. Returns
 * { sql, columns, rows (arrays), rowCount, truncated, ms }.
 */
export async function runReadOnlySql(sql, { maxRows = DISPLAY_MAX_ROWS, signal = null } = {}) {
    const checked = await checkSql(sql);
    const started = Date.now();
    const rows = [];
    let columns = [];
    let truncated = false;
    try {
        const r = await streamUserSql(checked.sql, {
            maxRows, signal,
            onColumns: (c) => { columns = c; },
            onRows: (batch) => { for (const o of batch) rows.push(columns.map(c => plainValue(o[c]))); },
        });
        truncated = r.truncated;
    } catch (err) {
        if (err.statusCode && err.statusCode !== 500) throw err;
        throw httpError(400, cleanDuckDbError(err));
    }
    return { sql: checked.sql, columns, rows, rowCount: rows.length, truncated, ms: Date.now() - started };
}

// --- Saved-function parameters ------------------------------------------------------

/** Distinct {{name}} parameters in order of first use. */
export function extractParams(sql) {
    const names = [];
    for (const m of String(sql ?? '').matchAll(PARAM_RE)) if (!names.includes(m[1])) names.push(m[1]);
    return names;
}

// Parameter types. Each value becomes a SQL literal of that type (validated
// first, so it can only ever be a literal), which makes LIMIT {{n}}, date
// arithmetic and IN ({{ids}}) work. Untyped parameters are text.
export const PARAM_TYPES = ['text', 'number', 'date', 'timestamp', 'boolean', 'list'];
const NUMBER_RE = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;
const DATE_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
const TIMESTAMP_RE = /^(\d{4}-\d{2}-\d{2})(?:[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.(\d{1,6}))?)?)?(Z|[+-]\d{2}:?\d{2})?$/;
const MAX_VALUE_CHARS = 1000;
const MAX_LIST_ITEMS = 1000;

function validDate(y, m, d) {
    const dt = new Date(Date.UTC(+y, +m - 1, +d));
    return dt.getUTCFullYear() === +y && dt.getUTCMonth() === +m - 1 && dt.getUTCDate() === +d;
}

/** The SQL literal for one parameter value; throws 400 naming the parameter. */
export function paramLiteral(def, raw) {
    const bad = (msg) => httpError(400, `{{${def.name}}}: ${msg}`);
    const text = Array.isArray(raw) ? raw.map(String).join(',') : String(raw);
    if (text.length > MAX_VALUE_CHARS * (def.type === 'list' ? 10 : 1)) throw bad('the value is too long');
    const v = text.trim();
    switch (def.type || 'text') {
        case 'text': return quoteLiteral(String(raw));
        case 'number':
            if (!NUMBER_RE.test(v)) throw bad(`"${v}" is not a number`);
            return v;
        case 'date': {
            const m = v.match(DATE_RE);
            if (!m || !validDate(m[1], m[2], m[3])) throw bad(`"${v}" is not a date (YYYY-MM-DD)`);
            return `DATE '${v}'`;
        }
        case 'timestamp': {
            const m = v.match(TIMESTAMP_RE);
            const [y, mo, d] = m ? m[1].split('-') : [];
            if (!m || !validDate(y, mo, d) || +(m[2] || 0) > 23 || +(m[3] || 0) > 59 || +(m[4] || 0) > 59) {
                throw bad(`"${v}" is not a timestamp (YYYY-MM-DD HH:MM[:SS], optionally with Z or +HH:MM)`);
            }
            return `${m[6] ? 'TIMESTAMPTZ' : 'TIMESTAMP'} '${v.replace('T', ' ')}'`;
        }
        case 'boolean':
            if (/^(true|1|yes|on)$/i.test(v)) return 'TRUE';
            if (/^(false|0|no|off)$/i.test(v)) return 'FALSE';
            throw bad(`"${v}" is not true or false`);
        case 'list': {
            const items = (Array.isArray(raw) ? raw.map(String) : v.split(',')).map(x => x.trim()).filter(x => x !== '');
            if (!items.length) throw bad('the list is empty');
            if (items.length > MAX_LIST_ITEMS) throw bad(`more than ${MAX_LIST_ITEMS} items`);
            return items.map(quoteLiteral).join(', ');
        }
        default: throw bad(`unknown type ${def.type}`);
    }
}

const isEmptyValue = (v) => v === undefined || v === null || (typeof v === 'string' && v.trim() === '') || (Array.isArray(v) && v.length === 0);

/**
 * Definitions for the {{parameters}} used in `sql`, in order of first use:
 * [{ name, type, default (string or null), description }]. Definitions for
 * names the SQL does not use are dropped; parameters without one are text
 * without a default. With `strict`, invalid definitions throw 400 (saving);
 * otherwise they fall back to text (reading stored functions).
 */
export function normaliseParamDefs(sql, defs, { strict = true } = {}) {
    const given = new Map();
    for (const d of Array.isArray(defs) ? defs : []) if (d && typeof d.name === 'string') given.set(d.name, d);
    return extractParams(sql).map(name => {
        const d = given.get(name) || {};
        const type = PARAM_TYPES.includes(d.type) ? d.type : 'text';
        if (d.type !== undefined && d.type !== null && d.type !== '' && !PARAM_TYPES.includes(d.type) && strict) {
            throw httpError(400, `{{${name}}}: type must be one of ${PARAM_TYPES.join(', ')}`);
        }
        const description = String(d.description ?? '').trim().slice(0, 300);
        let dflt = isEmptyValue(d.default) ? null : (Array.isArray(d.default) ? d.default.join(', ') : String(d.default));
        if (dflt !== null) {
            try { paramLiteral({ name, type }, dflt); } catch (err) {
                if (strict) throw httpError(400, `${err.message} (default value)`);
                dflt = null;
            }
        }
        return { name, type, default: dflt, description };
    });
}

/**
 * Replace each {{name}} with a literal: the given value, else the parameter's
 * default; a parameter with neither is a 400. A placeholder stands for a
 * whole value: write `name ILIKE '%' || {{q}} || '%'`, not `'%{{q}}%'`, and
 * `IN ({{ids}})` for a list. `values === null` substitutes a neutral literal
 * of each type, for validating the SQL.
 */
export function bindParams(sql, values, defs = []) {
    const byName = new Map((Array.isArray(defs) ? defs : []).map(d => [d.name, d]));
    return String(sql ?? '').replace(PARAM_RE, (_, name) => {
        const def = byName.get(name) || { name, type: 'text', default: null };
        if (values === null) {
            return { number: '1', boolean: 'TRUE', date: "DATE '2000-01-01'", timestamp: "TIMESTAMP '2000-01-01 00:00:00'" }[def.type] || 'NULL';
        }
        let v = values?.[name];
        if (isEmptyValue(v)) {
            if (def.default === null || def.default === undefined) throw httpError(400, `A value for {{${name}}} is required`);
            v = def.default;
        }
        return paramLiteral(def, v);
    });
}

/**
 * The reverse of a parameter: replace the {{placeholders}} in `names` with
 * hard-coded literals (the value given, else the parameter's default), as a
 * run would insert them. Other placeholders are left as they are. Used to
 * undo a suggested or unwanted parameter in the editor; nothing is saved.
 */
export function inlineParams(sql, defs, names, values = {}) {
    const wanted = new Set(Array.isArray(names) ? names.map(String) : []);
    const byName = new Map((Array.isArray(defs) ? defs : []).filter(d => d && typeof d.name === 'string').map(d => [d.name, d]));
    const missing = [];
    const out = String(sql ?? '').replace(PARAM_RE, (match, name) => {
        if (!wanted.has(name)) return match;
        const d = byName.get(name) || {};
        const def = { name, type: PARAM_TYPES.includes(d.type) ? d.type : 'text' };
        let v = values?.[name];
        if (isEmptyValue(v)) v = d.default;
        if (isEmptyValue(v)) { missing.push(name); return match; }
        return paramLiteral(def, v);
    });
    if (missing.length) {
        throw httpError(400, `${[...new Set(missing)].map(n => `{{${n}}}`).join(', ')} ha${missing.length > 1 ? 've' : 's'} no default: enter the value to put back`);
    }
    return out;
}

// --- Saved functions -----------------------------------------------------------------

function normaliseFunction(body) {
    const name = String(body?.name ?? '').trim();
    const description = String(body?.description ?? '').trim();
    const sql = String(body?.sql ?? '').trim().replace(/;\s*$/, '');
    if (!name || name.length > 100) throw httpError(400, 'A name of 1-100 characters is required');
    if (description.length > 1000) throw httpError(400, 'The description is longer than 1000 characters');
    return {
        name, description, sql,
        question: String(body?.question ?? '').trim().slice(0, QUESTION_CHARS),
        paramDefs: normaliseParamDefs(sql, body?.params),
    };
}

async function assertUniqueName(name, exceptId = null) {
    const clash = (await store.listAiFunctions()).find(f => f.name.toLowerCase() === name.toLowerCase() && f.id !== exceptId);
    if (clash) throw httpError(409, `A function named "${clash.name}" already exists`);
}

const withParams = ({ paramDefs, ...fn }) => ({ ...fn, params: normaliseParamDefs(fn.sql, paramDefs, { strict: false }) });

export async function listFunctions() {
    return (await store.listAiFunctions()).map(withParams);
}

export async function getFunction(id) {
    const fn = await store.getAiFunction(id);
    if (!fn) throw httpError(404, 'Function not found');
    return withParams(fn);
}

export async function createFunction(body, username) {
    const fn = normaliseFunction(body);
    await checkSql(bindParams(fn.sql, null, fn.paramDefs));
    await assertUniqueName(fn.name);
    const id = randomBytes(8).toString('hex');
    await store.createAiFunction({ ...fn, id, user: username });
    return getFunction(id);
}

export async function updateFunction(id, body, username) {
    await getFunction(id);
    const fn = normaliseFunction(body);
    await checkSql(bindParams(fn.sql, null, fn.paramDefs));
    await assertUniqueName(fn.name, id);
    await store.updateAiFunction(id, { ...fn, user: username });
    return getFunction(id);
}

export async function deleteFunction(id) {
    const fn = await getFunction(id);
    await store.deleteAiFunction(id);
    return fn;
}

export async function runFunction(id, params, { signal } = {}) {
    const fn = await getFunction(id);
    return { function: fn, ...(await runReadOnlySql(bindParams(fn.sql, params || {}, fn.params), { signal })) };
}

function csvCell(v) {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** Stream a function's full result as CSV, up to EXPORT_MAX_ROWS rows. */
export async function exportFunctionCsv(id, params, res) {
    const fn = await getFunction(id);
    const { sql } = await checkSql(bindParams(fn.sql, params || {}, fn.params));
    const maxRows = Math.max(1, parseInt(process.env.EXPORT_MAX_ROWS || '5000000', 10) || 5000000);
    const abort = new AbortController();
    res.on('close', () => { if (!res.writableFinished) abort.abort(); });
    const file = `${fn.name.replace(/[^A-Za-z0-9_.-]+/g, '_').replace(/^_+|_+$/g, '') || 'query'}.csv`;
    let columns = [];
    const write = async (text) => {
        if (res.writableEnded || res.destroyed) return;
        if (!res.write(text)) await new Promise(resolve => res.once('drain', resolve));
    };
    try {
        const r = await streamUserSql(sql, {
            maxRows, signal: abort.signal, timeoutMs: Math.max(1000, parseInt(process.env.EXPORT_TIMEOUT_MS || '600000', 10) || 600000),
            onColumns: async (c) => {
                columns = c;
                res.setHeader('Content-Type', 'text/csv; charset=utf-8');
                res.setHeader('Content-Disposition', `attachment; filename="${file}"`);
                await write(columns.map(csvCell).join(',') + '\r\n');
            },
            onRows: async (batch) => {
                await write(batch.map(o => columns.map(c => csvCell(plainValue(o[c]))).join(',')).join('\r\n') + '\r\n');
            },
        });
        if (r.truncated) console.warn(`[AI] CSV export of "${fn.name}" truncated at ${maxRows} rows`);
        res.end();
    } catch (err) {
        if (res.headersSent) return res.destroy(err);
        if (err.statusCode && err.statusCode !== 500) throw err;
        throw httpError(400, cleanDuckDbError(err));
    }
    return fn;
}

// --- Asking the model ------------------------------------------------------------------

async function lakeSchema() {
    const cols = await execute(
        `SELECT table_name, column_name, data_type FROM information_schema.columns
         WHERE table_catalog = 'ducklake' AND table_schema = 'main' ORDER BY table_name, ordinal_position`);
    const counts = new Map((await listTables()).map(t => [t.name, t.rows]));
    const tables = new Map();
    for (const c of cols) {
        if (isSystemTable(c.table_name)) continue;
        if (!tables.has(c.table_name)) tables.set(c.table_name, { name: c.table_name, rows: counts.get(c.table_name) ?? null, columns: [] });
        // Written as they must appear in SQL: quoted unless a plain lowercase word.
        const col = /^[a-z_][a-z0-9_]*$/.test(c.column_name) ? c.column_name : quoteIdent(c.column_name);
        tables.get(c.table_name).columns.push(`${col} ${c.data_type}`);
    }
    return [...tables.values()];
}

// --- Relevance (table selection and worked examples) --------------------------------
//
// Deterministic word matching, no extra model call: names are split into words
// (snake_case, camelCase, digits), lower-cased and singularised, and common
// English words are dropped.

const STOP_WORDS = new Set(('a an and are as at be by can do does did for from has have how i in is it its many me much my ' +
    'of on or per show than that the their them there these this to was were what when where which who why with ' +
    'all any each every give get list find tell count total number average avg sum top most least last first ' +
    'over under between since before after about into out up down day days week weeks month months year years ' +
    'hour hours minute minutes today yesterday time times value values data table tables row rows column columns').split(' '));

export function terms(text) {
    const out = new Set();
    const words = String(text ?? '')
        .replace(/([a-z])([A-Z])/g, '$1 $2')
        .toLowerCase()
        .split(/[^a-z0-9]+/);
    for (let w of words) {
        if (w.length < 2 || STOP_WORDS.has(w) || /^\d+$/.test(w)) continue;
        if (w.length > 3 && w.endsWith('ies')) w = `${w.slice(0, -3)}y`;
        else if (w.length > 3 && w.endsWith('s') && !/(ss|us|is)$/.test(w)) w = w.slice(0, -1);
        out.add(w);
    }
    return out;
}

/** How well a table matches the question's words: table-name words count most. */
export function scoreTable(table, qTerms, questionText = '') {
    if (!qTerms.size) return 0;
    let score = 0;
    for (const t of terms(table.name)) if (qTerms.has(t)) score += 3;
    const colTerms = new Set();
    for (const c of table.columns) for (const t of terms(String(c).split(' ')[0])) colTerms.add(t);
    for (const t of colTerms) if (qTerms.has(t)) score += 1;
    if (questionText && String(questionText).toLowerCase().includes(table.name.toLowerCase())) score += 5;
    return score;
}

/**
 * Table list for the system prompt. Everything with full column lists when it
 * fits SCHEMA_PROMPT_CHARS; otherwise the tables that best match the question
 * (and recent follow-ups) get their columns within the budget and the rest are
 * listed by name for describe_table.
 */
export function schemaText(tables, question = '') {
    if (!tables.length) return 'There are no user tables yet.';
    const rows = (t) => (t.rows === null ? '' : ` (${Number(t.rows).toLocaleString('en-US')} rows)`);
    const fullLine = (t) => `- ${t.name}${rows(t)}: ${t.columns.join(', ')}`;
    const nameLine = (t) => `- ${t.name}${rows(t)}, ${t.columns.length} columns`;
    const full = tables.map(fullLine).join('\n');
    if (full.length <= SCHEMA_PROMPT_CHARS) return full;

    const qTerms = terms(question);
    const ranked = tables.map((t, i) => ({ t, i, score: scoreTable(t, qTerms, question) }))
        .sort((a, b) => b.score - a.score || a.i - b.i);
    const detailed = [];
    const rest = [];
    let used = tables.reduce((n, t) => n + nameLine(t).length + 1, 0);   // every table appears at least by name
    for (const r of ranked) {
        const extra = fullLine(r.t).length - nameLine(r.t).length;
        if (r.score > 0 && used + extra <= SCHEMA_PROMPT_CHARS) { detailed.push(r.t); used += extra; }
        else rest.push(r.t);
    }
    const parts = ['The schema is too large to list in full.'];
    if (detailed.length) parts.push('Tables that look relevant to the question, with their columns:', detailed.map(fullLine).join('\n'));
    if (rest.length) parts.push(`${detailed.length ? 'Other tables' : 'Tables'} (call describe_table for their columns):`, rest.sort((a, b) => a.name.localeCompare(b.name)).map(nameLine).join('\n'));
    return parts.join('\n');
}

/**
 * Saved functions as worked question -> SQL examples: those sharing the most
 * words with the question first, topped up with the most recently updated.
 * Functions over tables that no longer exist, and very long ones, are left out.
 */
export function pickExamples(functions, question, tables, count = EXAMPLE_COUNT, { maxSqlChars = EXAMPLE_SQL_CHARS } = {}) {
    const tableNames = tables.map(t => t.name.toLowerCase());
    const usable = (Array.isArray(functions) ? functions : []).filter(f => {
        const sql = String(f.sql || '');
        if (!sql || sql.length > maxSqlChars) return false;
        const lower = sql.toLowerCase();
        return tableNames.some(n => new RegExp(`(^|[^a-z0-9_])"?${n.replace(/[^a-z0-9_]/g, '.')}"?([^a-z0-9_]|$)`).test(lower));
    });
    const qTerms = terms(question);
    const scored = usable.map(f => {
        const fTerms = terms(`${f.question || ''} ${f.name || ''} ${f.description || ''}`);
        let score = 0;
        for (const t of fTerms) if (qTerms.has(t)) score++;
        return { f, score };
    });
    const relevant = scored.filter(x => x.score > 0).sort((a, b) => b.score - a.score || (b.f.updatedMs || 0) - (a.f.updatedMs || 0));
    const recent = scored.filter(x => x.score === 0).sort((a, b) => (b.f.updatedMs || 0) - (a.f.updatedMs || 0));
    return [...relevant, ...recent].slice(0, count).map(x => x.f);
}

/** "name(region text, since date = '2024-01-01')" for the prompt. */
export function functionSignature(fn) {
    const params = (fn.params || []).map(p => `${p.name} ${p.type}${p.default !== null && p.default !== undefined ? ` = ${JSON.stringify(p.default)}` : ''}`);
    return `${fn.name}(${params.join(', ')})`;
}

function callableText(functions) {
    if (!functions.length) return '';
    const lines = functions.map(f => `- ${functionSignature(f)}${f.description ? `: ${f.description}` : ''}`);
    return `\nSaved functions you can call with run_function (pass parameters by name; ones with a default are optional). ` +
        `Prefer a function when it answers the question as asked; otherwise write SQL.\n${lines.join('\n')}\n`;
}

function examplesText(examples) {
    if (!examples.length) return '';
    const blocks = examples.map(f => {
        const q = f.question || [f.name, f.description].filter(Boolean).join(': ');
        return `Question: ${q}\nSQL:\n${f.sql}`;
    });
    return `\nWorked examples: queries saved by the users of this lake (as reusable functions), with the question each answers. ` +
        `Use them for the conventions of these tables, not as answers to copy. {{name}} marks a parameter of a saved function; ` +
        `in run_sql write the actual value instead.\n\n${blocks.join('\n\n')}\n`;
}

function systemPrompt(tables, cfg, { question = '', examples = [], callable = [] } = {}) {
    const data = cfg.sampleRows > 0
        ? `Tool results show you at most ${cfg.sampleRows} rows of each result, with long values cut short; use aggregates, ORDER BY and LIMIT rather than reading raw rows.`
        : 'For privacy you are never shown data values: tool results give only column names and row counts. Write the query that answers the question; the user sees its result.';
    return `You are the data analyst built into the admin console of ichibi-lake, a data lake gateway. Data is stored with DuckLake (Parquet files plus a Postgres catalog) and queried with DuckDB. You answer questions about the data by writing and running SQL with the run_sql tool.

Rules for SQL:
- Dialect: DuckDB SQL. Exactly one read-only SELECT per run_sql call (WITH ... SELECT is fine).
- Use only the tables listed below, by bare name. Double-quote identifiers that are not plain lowercase words or that are SQL keywords.
- Not allowed: DDL or DML, table functions in FROM (read_csv, read_parquet, range, glob, ...), information_schema and duckdb_* catalogs, system tables (names starting with "_").
- A guard rejects these words anywhere outside string literals and quoted identifiers: CREATE DROP INSERT UPDATE DELETE ALTER ATTACH DETACH COPY TRUNCATE MERGE REPLACE GRANT REVOKE VACUUM CHECKPOINT INSTALL LOAD EXPORT IMPORT CALL PRAGMA SET RESET USE. For example use regexp_replace() instead of replace(), and quote a column called "set".
- Tables filled through the REST API usually have a "created" timestamp (when the row was written) and keyed tables an "updated" timestamp. BLOB columns hold binary data or a file path; leave them out unless asked.
- If a query fails, read the error, fix the SQL and try again. Do not invent tables or columns; call describe_table when unsure.
- Useful DuckDB functions: date_trunc('day', ts) for grouping by period, ts >= now() - INTERVAL 7 DAY for recent rows, epoch_ms(n) / to_timestamp(n) for numeric epochs, strftime() for formatting, regexp_matches() and ILIKE for text, json_extract_string() for JSON text.

${data}

When you have the answer, reply briefly in plain text: the answer itself, then one or two sentences on how it was computed and any caveats. The result of your last successful run_sql is shown to the user as a table and can be saved as a reusable function, so make that last query the one that best answers the question, and do not repeat its rows in your reply. If the question cannot be answered from these tables, say so.

Current time (UTC): ${new Date().toISOString()}

${examplesText(examples)}${callableText(callable)}
Tables in the lake:
${schemaText(tables, question)}`;
}

const RUN_FUNCTION_TOOL = {
    type: 'function',
    function: {
        name: 'run_function',
        description: 'Run one of the saved functions listed in the instructions, with parameter values, and return the column names, row count and the first rows.',
        parameters: {
            type: 'object',
            properties: {
                name: { type: 'string', description: 'The function name, exactly as listed' },
                params: { type: 'object', description: 'Parameter values by name, e.g. {"region": "eu", "since": "2024-01-01"}', additionalProperties: true },
            },
            required: ['name'],
        },
    },
};

const TOOLS = [
    {
        type: 'function',
        function: {
            name: 'run_sql',
            description: 'Run one read-only DuckDB SELECT over the lake tables and return the column names, row count and the first rows.',
            parameters: {
                type: 'object',
                properties: {
                    sql: { type: 'string', description: 'A single SELECT statement' },
                    purpose: { type: 'string', description: 'What this query is for, in a few words' },
                },
                required: ['sql'],
            },
        },
    },
    {
        type: 'function',
        function: {
            name: 'describe_table',
            description: 'Columns and types, row count, key columns and a few sample rows of one table.',
            parameters: { type: 'object', properties: { table: { type: 'string' } }, required: ['table'] },
        },
    },
];

function forModel(result, cfg) {
    const out = {
        columns: result.columns,
        rowCount: result.truncated ? `more than ${result.rowCount}` : result.rowCount,
    };
    if (cfg.sampleRows > 0) {
        out.rows = result.rows.slice(0, cfg.sampleRows).map(r => r.map(v => (typeof v === 'string' && v.length > MODEL_CELL_CHARS ? `${v.slice(0, MODEL_CELL_CHARS)}…` : v)));
        if (result.rowCount > out.rows.length) out.note = `Only the first ${out.rows.length} rows are shown.`;
    }
    return out;
}

async function describeTable(name, cfg, signal) {
    const t = await findLakeTable(String(name || ''));
    if (!t || isSystemTable(t.name)) throw httpError(404, `Table "${name}" not found`);
    const cols = (await execute(
        `SELECT column_name, data_type, is_nullable FROM information_schema.columns
         WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND table_name = ${quoteLiteral(t.name)} ORDER BY ordinal_position`))
        .map(c => ({ name: c.column_name, type: String(c.data_type), nullable: c.is_nullable !== 'NO' }));
    const count = await runReadOnlySql(`SELECT count(*) AS n FROM ${quoteIdent(t.name)}`, { signal });
    const out = { table: t.name, rows: count.rows[0]?.[0] ?? null, columns: cols, keyColumns: await getTableKeys(t.name) };
    const n = Math.min(5, cfg.sampleRows);
    if (n > 0) {
        const sample = await runReadOnlySql(`SELECT * FROM ${quoteIdent(t.name)} LIMIT ${n}`, { signal });
        out.sampleRows = forModel(sample, { sampleRows: n }).rows;
    }
    return out;
}

function parseArgs(call) {
    try { return JSON.parse(call.function?.arguments || '{}') || {}; } catch { return {}; }
}

/** One earlier exchange, re-sent so follow-up questions have context. */
function historyMessages(history) {
    const turns = (Array.isArray(history) ? history : []).slice(-HISTORY_TURNS);
    const out = [];
    for (const t of turns) {
        const q = String(t?.question ?? '').slice(0, HISTORY_CHARS);
        if (!q) continue;
        let a = String(t?.answer ?? '').slice(0, HISTORY_CHARS);
        if (t?.sql) a += `\n\nSQL of the result shown:\n${String(t.sql).slice(0, HISTORY_CHARS)}`;
        out.push({ role: 'user', content: q }, { role: 'assistant', content: a || '(no answer)' });
    }
    return out;
}

/**
 * Answer a question with the tool-calling loop. Returns
 * { answer, steps: [{ tool, sql?, table?, purpose?, ok, error?, rowCount?, truncated?, ms }],
 *   result (the last successful run_sql, for display), model, usage }.
 */
export async function ask({ question, history }, { signal = null } = {}) {
    const cfg = aiConfig();
    if (!isConfigured(cfg)) throw httpError(400, 'AI is not configured. Set the API base URL and model on the AI Settings page.');
    const q = String(question ?? '').trim();
    if (!q) throw httpError(400, 'Ask a question');
    if (q.length > QUESTION_CHARS) throw httpError(400, `Questions are limited to ${QUESTION_CHARS} characters`);

    // Relevance uses the question plus recent follow-up questions, so "and per
    // week?" keeps the tables the conversation is about.
    const context = [...(Array.isArray(history) ? history : []).slice(-3).map(t => String(t?.question ?? '')), q].join(' ');
    const tables = await lakeSchema();
    let functions = [];
    try { functions = (await store.listAiFunctions()).map(withParams); } catch { /* functions are optional */ }
    const examples = pickExamples(functions, context, tables);
    // Callable: the most relevant functions whose tables exist, any length.
    const callable = pickExamples(functions, context, tables, CALLABLE_COUNT, { maxSqlChars: Infinity });
    const tools = callable.length ? [...TOOLS, RUN_FUNCTION_TOOL] : TOOLS;
    const messages = [
        { role: 'system', content: systemPrompt(tables, cfg, { question: context, examples, callable }) },
        ...historyMessages(history),
        { role: 'user', content: q },
    ];
    const steps = [];
    const usage = { prompt: 0, completion: 0, total: 0 };
    let result = null;
    let model = cfg.model;

    for (let round = 0; ; round++) {
        const lastRound = round >= cfg.maxSteps;
        if (lastRound) messages.push({ role: 'user', content: 'You have used all your tool calls. Answer now with what you have.' });
        const data = await chat(cfg, { messages, tools, tool_choice: lastRound ? 'none' : 'auto' }, signal);
        model = data.model || model;
        usage.prompt += data.usage?.prompt_tokens || 0;
        usage.completion += data.usage?.completion_tokens || 0;
        usage.total += data.usage?.total_tokens || 0;
        const msg = data.choices?.[0]?.message;
        if (!msg) throw modelError(502, `The model API returned no message (finish reason: ${data.choices?.[0]?.finish_reason || 'none'}). Try again or choose another model.`);
        const calls = Array.isArray(msg.tool_calls) ? msg.tool_calls : [];
        if (!calls.length || lastRound) {
            return { answer: String(msg.content ?? '').trim(), steps, result, model, usage };
        }
        messages.push({ role: 'assistant', content: msg.content ?? null, tool_calls: calls });
        for (const call of calls) {
            const args = parseArgs(call);
            const name = call.function?.name;
            const started = Date.now();
            const stepInfo = { tool: name, ok: true };
            let content;
            try {
                if (name === 'run_sql') {
                    Object.assign(stepInfo, { sql: String(args.sql ?? ''), purpose: String(args.purpose ?? '').slice(0, 200) });
                    const r = await runReadOnlySql(args.sql, { signal });
                    Object.assign(stepInfo, { sql: r.sql, rowCount: r.rowCount, truncated: r.truncated });
                    result = r;
                    content = forModel(r, cfg);
                } else if (name === 'run_function' && callable.length) {
                    const wanted = String(args.name ?? '').trim().toLowerCase();
                    const fn = functions.find(f => f.name.toLowerCase() === wanted);
                    stepInfo.function = String(args.name ?? '');
                    stepInfo.params = args.params && typeof args.params === 'object' ? args.params : {};
                    if (!fn) throw httpError(404, `No saved function named "${args.name}"`);
                    stepInfo.function = fn.name;
                    const r = await runReadOnlySql(bindParams(fn.sql, stepInfo.params, fn.params), { signal });
                    Object.assign(stepInfo, { sql: r.sql, rowCount: r.rowCount, truncated: r.truncated });
                    result = { ...r, functionId: fn.id, functionName: fn.name };
                    content = forModel(r, cfg);
                } else if (name === 'describe_table') {
                    stepInfo.table = String(args.table ?? '');
                    content = await describeTable(args.table, cfg, signal);
                } else {
                    throw httpError(400, `Unknown tool ${name}`);
                }
            } catch (err) {
                if (err.cancelled || signal?.aborted) throw err;
                stepInfo.ok = false;
                stepInfo.error = String(err.message || err).slice(0, 500);
                content = { error: stepInfo.error };
            }
            stepInfo.ms = Date.now() - started;
            steps.push(stepInfo);
            messages.push({ role: 'tool', tool_call_id: call.id, content: JSON.stringify(content) });
        }
    }
}

// --- Suggesting parameters for a query --------------------------------------------------

const PROPOSE_TOOL = {
    type: 'function',
    function: {
        name: 'propose_function',
        description: 'The query rewritten as a reusable function with {{parameters}}.',
        parameters: {
            type: 'object',
            properties: {
                name: { type: 'string', description: 'Short name, e.g. "Daily orders by region"' },
                description: { type: 'string', description: 'One sentence: what it returns' },
                sql: { type: 'string', description: 'The query with {{name}} placeholders' },
                params: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            name: { type: 'string' },
                            type: { type: 'string', enum: PARAM_TYPES },
                            default: { type: 'string', description: 'The value the original query used' },
                            description: { type: 'string' },
                        },
                        required: ['name', 'type', 'default'],
                    },
                },
            },
            required: ['name', 'sql', 'params'],
        },
    },
};

const SUGGEST_PROMPT = `You turn a DuckDB SQL query into a reusable saved function with parameters.

Replace the hard-coded values a user would want to change between runs (dates and times, ids, names, categories, thresholds, LIMIT counts) with {{name}} placeholders. Leave everything else exactly as it is: the same tables, columns, joins, grouping and ordering. Do not parameterise identifiers (table or column names), keywords or SQL functions, and do not add parameters the query does not need; a query with no such values gets no parameters.

A placeholder replaces a whole value including its quotes, so 'eu' becomes {{region}} and DATE '2024-01-01' or '2024-01-01' becomes {{since}}. Inside a pattern use concatenation: name ILIKE '%' || {{q}} || '%'. A list of values becomes IN ({{ids}}) with type list.

Parameter names: lowercase snake_case. Types: text, number, date (YYYY-MM-DD), timestamp (YYYY-MM-DD HH:MM:SS), boolean, list (comma-separated values). Give each the value from the original query as its default, so running the function with no values returns the original result, and a short description.

Also give the function a short name and a one-sentence description. Answer by calling propose_function.`;

/**
 * Ask the model to parameterise `sql` (Save as function). Returns
 * { name, description, sql, params } after checking the proposal is valid
 * read-only SQL; one retry with the error when it is not. The model only
 * sees the query, the question and the referenced tables' columns.
 */
export async function suggestFunction({ sql, question }, { signal = null } = {}) {
    const cfg = aiConfig();
    if (!isConfigured(cfg)) throw httpError(400, 'AI is not configured. Set the API base URL and model on the AI Settings page.');
    const checked = await checkSql(bindParams(sql, null, normaliseParamDefs(sql, [], { strict: false })));
    const cols = [];
    for (const name of checked.tables) {
        const t = await findLakeTable(name);
        if (!t) continue;
        const rows = await execute(`SELECT column_name, data_type FROM information_schema.columns
            WHERE table_catalog = 'ducklake' AND table_schema = 'main' AND table_name = ${quoteLiteral(t.name)} ORDER BY ordinal_position`);
        cols.push(`- ${t.name}: ${rows.map(r => `${r.column_name} ${r.data_type}`).join(', ')}`);
    }
    const messages = [
        { role: 'system', content: SUGGEST_PROMPT },
        { role: 'user', content: `${question ? `The query answers: ${String(question).slice(0, QUESTION_CHARS)}\n\n` : ''}Tables:\n${cols.join('\n') || '(none)'}\n\nQuery:\n${String(sql).slice(0, SQL_CHARS)}` },
    ];
    let lastError = null;
    for (let attempt = 0; attempt < 2; attempt++) {
        const data = await chat(cfg, { messages, tools: [PROPOSE_TOOL], tool_choice: { type: 'function', function: { name: 'propose_function' } } }, signal);
        const msg = data.choices?.[0]?.message;
        const call = (msg?.tool_calls || []).find(c => c.function?.name === 'propose_function');
        if (!call) throw modelError(502, 'The model did not propose a function. Try again or choose a model with tool calling.');
        const p = parseArgs(call);
        try {
            const proposal = {
                name: String(p.name ?? '').trim().slice(0, 100),
                description: String(p.description ?? '').trim().slice(0, 1000),
                sql: String(p.sql ?? '').trim().replace(/;\s*$/, ''),
            };
            proposal.params = normaliseParamDefs(proposal.sql, p.params);
            await checkSql(bindParams(proposal.sql, null, proposal.params));
            if (proposal.params.every(d => d.default !== null)) await checkSql(bindParams(proposal.sql, {}, proposal.params));
            return proposal;
        } catch (err) {
            lastError = err;
            messages.push({ role: 'assistant', content: msg.content ?? null, tool_calls: [call] },
                { role: 'tool', tool_call_id: call.id, content: JSON.stringify({ error: String(err.message || err).slice(0, 500) }) },
                { role: 'user', content: 'That proposal is not valid. Fix it and call propose_function again.' });
        }
    }
    throw httpError(502, `The model's proposal was not valid: ${String(lastError?.message || lastError).slice(0, 300)}`);
}
