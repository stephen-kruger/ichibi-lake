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
const SCHEMA_PROMPT_CHARS = 24000;   // full column lists up to this size, else names only
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

/**
 * Replace {{name}} with the value as an escaped SQL string literal (DuckDB
 * casts it implicitly where a number or date is compared). A placeholder
 * stands for a whole value: write `name ILIKE '%' || {{q}} || '%'`, not
 * `'%{{q}}%'`. `values === null` substitutes NULL, for validating the SQL.
 */
export function bindParams(sql, values) {
    return String(sql ?? '').replace(PARAM_RE, (_, name) => {
        if (values === null) return 'NULL';
        const v = values?.[name];
        if (v === undefined || v === null || v === '') throw httpError(400, `A value for {{${name}}} is required`);
        if (String(v).length > 1000) throw httpError(400, `The value for {{${name}}} is too long`);
        return quoteLiteral(v);
    });
}

// --- Saved functions -----------------------------------------------------------------

function normaliseFunction(body) {
    const name = String(body?.name ?? '').trim();
    const description = String(body?.description ?? '').trim();
    const sql = String(body?.sql ?? '').trim().replace(/;\s*$/, '');
    if (!name || name.length > 100) throw httpError(400, 'A name of 1-100 characters is required');
    if (description.length > 1000) throw httpError(400, 'The description is longer than 1000 characters');
    return { name, description, sql, question: String(body?.question ?? '').trim().slice(0, QUESTION_CHARS) };
}

async function assertUniqueName(name, exceptId = null) {
    const clash = (await store.listAiFunctions()).find(f => f.name.toLowerCase() === name.toLowerCase() && f.id !== exceptId);
    if (clash) throw httpError(409, `A function named "${clash.name}" already exists`);
}

const withParams = (fn) => ({ ...fn, params: extractParams(fn.sql) });

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
    await checkSql(bindParams(fn.sql, null));
    await assertUniqueName(fn.name);
    const id = randomBytes(8).toString('hex');
    await store.createAiFunction({ ...fn, id, user: username });
    return getFunction(id);
}

export async function updateFunction(id, body, username) {
    await getFunction(id);
    const fn = normaliseFunction(body);
    await checkSql(bindParams(fn.sql, null));
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
    return { function: fn, ...(await runReadOnlySql(bindParams(fn.sql, params || {}), { signal })) };
}

function csvCell(v) {
    if (v === null || v === undefined) return '';
    const s = String(v);
    return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** Stream a function's full result as CSV, up to EXPORT_MAX_ROWS rows. */
export async function exportFunctionCsv(id, params, res) {
    const fn = await getFunction(id);
    const { sql } = await checkSql(bindParams(fn.sql, params || {}));
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
        tables.get(c.table_name).columns.push(`${c.column_name} ${c.data_type}`);
    }
    return [...tables.values()];
}

function schemaText(tables) {
    if (!tables.length) return 'There are no user tables yet.';
    const rows = (t) => (t.rows === null ? '' : ` (${Number(t.rows).toLocaleString('en-US')} rows)`);
    const full = tables.map(t => `- ${t.name}${rows(t)}: ${t.columns.join(', ')}`).join('\n');
    if (full.length <= SCHEMA_PROMPT_CHARS) return full;
    return 'The schema is too large to list in full; call describe_table for the columns of a table.\n' +
        tables.map(t => `- ${t.name}${rows(t)}, ${t.columns.length} columns`).join('\n');
}

function systemPrompt(tables, cfg) {
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

${data}

When you have the answer, reply briefly in plain text: the answer itself, then one or two sentences on how it was computed and any caveats. The result of your last successful run_sql is shown to the user as a table and can be saved as a reusable function, so make that last query the one that best answers the question, and do not repeat its rows in your reply. If the question cannot be answered from these tables, say so.

Current time (UTC): ${new Date().toISOString()}

Tables in the lake:
${schemaText(tables)}`;
}

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

    const messages = [
        { role: 'system', content: systemPrompt(await lakeSchema(), cfg) },
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
        const data = await chat(cfg, { messages, tools: TOOLS, tool_choice: lastRound ? 'none' : 'auto' }, signal);
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
