/*
 * "AI" console sections: Ask AI (questions answered by an OpenAI-compatible
 * model that writes and runs read-only SQL), Saved Functions (named SQL with
 * optional {{parameters}}, run on demand or downloaded as CSV) and AI Settings
 * (model endpoint, key and limits, with a connection test). Server side:
 * src/console/ai.js. Loaded after app.js and uses its helpers (h, api, icon,
 * modal, toast, withBusy, field, pageHeader, sourceBadge, fmtDate).
 */
'use strict';

(function () {
    const enc = encodeURIComponent;
    const PARAM_RE = /\{\{\s*([A-Za-z_][A-Za-z0-9_]{0,62})\s*\}\}/g;

    // The conversation survives navigating between console pages (not a reload).
    const conversation = [];

    function paramsOf(sql) {
        const names = [];
        for (const m of String(sql || '').matchAll(PARAM_RE)) if (!names.includes(m[1])) names.push(m[1]);
        return names;
    }

    function sqlEditor(sql, { readOnly = false, rows = 6 } = {}) {
        const ta = h('textarea', { class: 'sql-editor', spellcheck: 'false', rows, readOnly });
        ta.value = sql || '';
        return ta;
    }

    function cellText(v) {
        if (v === null || v === undefined) return null;
        return typeof v === 'object' ? JSON.stringify(v) : String(v);
    }

    /** Result grid for { columns, rows (arrays), rowCount, truncated, ms }. */
    function resultView(r) {
        if (!r) return h('div');
        const summary = h('div', { class: 'muted small result-summary' },
            `${r.rowCount.toLocaleString()} row(s)`, r.truncated ? ` (first ${r.rowCount.toLocaleString()} shown; more available)` : '',
            Number.isFinite(r.ms) ? ` · ${r.ms} ms` : '');
        if (!r.columns.length) return h('div', {}, summary);
        const body = r.rows.map((row, i) => h('tr', {},
            h('td', { class: 'rownum' }, String(i + 1)),
            row.map(v => {
                const t = cellText(v);
                return h('td', { class: `cell ${typeof v === 'number' ? 'num' : ''}`, title: t && t.length > 60 ? t.slice(0, 2000) : undefined },
                    t === null ? h('span', { class: 'null' }, 'NULL') : t.length > 300 ? `${t.slice(0, 300)}…` : t);
            })));
        return h('div', {}, summary,
            h('div', { class: 'grid-wrap result-grid' },
                h('table', { class: 'grid' },
                    h('thead', {}, h('tr', {}, h('th', { class: 'rownum' }, '#'), r.columns.map(c => h('th', {}, c)))),
                    h('tbody', {}, body))));
    }

    /** Inputs for the {{parameters}} of `sql`; sync() follows edits to the SQL. */
    function paramForm(initialSql) {
        const inputs = new Map();
        let names = [];
        const el = h('div', { class: 'form-grid params' });
        const sync = (sql) => {
            names = paramsOf(sql);
            for (const n of names) if (!inputs.has(n)) inputs.set(n, h('input', { type: 'text', placeholder: 'value' }));
            el.replaceChildren(...names.map(n => field(`{{${n}}}`, inputs.get(n))));
            el.hidden = names.length === 0;
        };
        sync(initialSql);
        return { el, sync, values: () => Object.fromEntries(names.map(n => [n, inputs.get(n).value])) };
    }

    async function saveFunctionDialog({ sql, question }) {
        const name = h('input', { type: 'text', maxlength: 100, required: true, placeholder: 'e.g. Daily orders by region' });
        const description = h('input', { type: 'text', maxlength: 1000, value: question || '' });
        const editor = sqlEditor(sql, { rows: 8 });
        const ok = await modal({
            title: 'Save as function', wide: true,
            body: h('div', {},
                field('Name', name),
                field('Description', description),
                field('SQL', editor, 'Use {{name}} for a value to fill in at each run, e.g. WHERE region = {{region}} or name ILIKE \'%\' || {{q}} || \'%\'.')),
            actions: [{ label: 'Cancel', value: false }, { label: 'Save function', value: true, primary: true }],
        });
        if (!ok) return null;
        try {
            const res = await api('POST', '/ai/functions', { name: name.value, description: description.value, sql: editor.value, question });
            toast(`Saved function "${res.function.name}"`, 'success');
            return res.function;
        } catch (err) {
            toast(err.message, 'error');
            return null;
        }
    }

    function notConfigured(ctx) {
        return h('div', { class: 'callout' }, 'No model is configured yet. ',
            ctx.isAdmin ? h('a', { href: '#/ai-settings' }, 'Open AI Settings') : 'Ask an admin to configure it on the AI Settings page',
            ' to set an OpenAI-compatible API (OPENAI_BASE_URL, OPENAI_API_KEY, OPENAI_MODEL in docker/.env, or saved in the console).');
    }

    // ------------------------------------------------------------------ Ask AI

    function answerText(text) {
        // Plain text with ``` fenced blocks shown as code.
        const parts = String(text || '').split(/```[a-zA-Z]*\n?/);
        return h('div', { class: 'ai-answer' }, parts.map((p, i) => (i % 2 ? h('pre', { class: 'code' }, p.replace(/\n$/, '')) : p)));
    }

    function stepsView(steps) {
        if (!steps?.length) return null;
        return h('details', { class: 'advanced ai-steps' },
            h('summary', {}, `How it was answered: ${steps.length} tool call(s)`),
            h('ol', { class: 'steps' }, steps.map(s => h('li', {},
                h('span', { class: `badge ${s.ok ? 'ok' : 'off'}` }, s.ok ? 'ok' : 'failed'), ' ',
                h('strong', {}, s.tool === 'describe_table' ? `describe_table ${s.table}` : 'run_sql'),
                s.purpose ? h('span', { class: 'muted' }, ` — ${s.purpose}`) : null,
                s.ok && s.tool === 'run_sql' ? h('span', { class: 'muted' }, ` · ${s.rowCount}${s.truncated ? '+' : ''} row(s) · ${s.ms} ms`) : null,
                s.sql ? h('pre', { class: 'code' }, s.sql) : null,
                s.error ? h('div', { class: 'error-text small' }, s.error) : null))));
    }

    function turnView(turn, ctx) {
        const el = h('div', { class: 'card ai-turn' });
        const render = () => {
            const parts = [h('div', { class: 'ai-question' }, h('span', { class: 'avatar' }, ctx.user.username.slice(0, 1)), h('div', {}, turn.question))];
            if (turn.pending) {
                parts.push(h('div', { class: 'muted ai-pending' }, h('span', { class: 'spinner' }), 'Thinking… the model may look at tables and run several queries.'));
            } else if (turn.error) {
                parts.push(h('div', { class: 'callout error' }, turn.error));
            } else {
                parts.push(answerText(turn.answer || '(The model gave no text answer.)'));
                parts.push(h('div', { class: 'muted small' }, [turn.model, turn.usage?.total ? `${turn.usage.total.toLocaleString()} tokens` : null].filter(Boolean).join(' · ')));
                parts.push(stepsView(turn.steps));
                if (turn.result) parts.push(resultPanel(turn, ctx));
            }
            el.replaceChildren(h('div', { class: 'card-body row-gap' }, parts));
        };
        render();
        turn.rerender = render;
        return el;
    }

    function resultPanel(turn, ctx) {
        const editor = sqlEditor(turn.result.sql);
        const out = h('div', {}, resultView(turn.result));
        const run = h('button', { class: 'btn small', type: 'button' }, icon('play'), 'Run');
        run.addEventListener('click', () => withBusy(run, async () => {
            const r = await api('POST', '/ai/run', { sql: editor.value });
            turn.result = r;
            out.replaceChildren(resultView(r));
        }));
        const save = h('button', { class: 'btn small primary', type: 'button', disabled: !ctx.isAdmin, title: ctx.isAdmin ? undefined : 'Admins can save functions' },
            icon('bolt'), 'Save as function');
        save.addEventListener('click', async () => {
            const fn = await saveFunctionDialog({ sql: editor.value, question: turn.question });
            if (fn) turn.savedAs = fn;
            if (fn) note.replaceChildren('Saved as ', h('a', { href: `#/ai-functions/${enc(fn.id)}` }, fn.name), '.');
        });
        const note = h('span', { class: 'muted small' }, turn.savedAs ? ['Saved as ', h('a', { href: `#/ai-functions/${enc(turn.savedAs.id)}` }, turn.savedAs.name), '.'] : '');
        return h('div', { class: 'ai-result' },
            h('div', { class: 'label-row' }, h('span', { class: 'label' }, 'Result query'), h('div', { class: 'action-row' }, note, run, save)),
            editor, out);
    }

    registerView('ai-query', async (ctx) => {
        const cfg = await api('GET', '/ai/settings');
        const thread = h('div', { class: 'ai-thread' }, conversation.map(t => turnView(t, ctx)));
        const input = h('textarea', { class: 'ai-input', rows: 3, placeholder: 'e.g. Which 10 customers ordered the most last month, and how much did they spend?' });
        const askBtn = h('button', { class: 'btn primary', type: 'submit', disabled: !cfg.configured }, icon('sparkle'), 'Ask');

        const send = async () => {
            const question = input.value.trim();
            if (!question || askBtn.disabled) return;
            const history = conversation.filter(t => !t.pending && !t.error)
                .map(t => ({ question: t.question, answer: t.answer, sql: t.result?.sql || null }));
            const turn = { question, pending: true };
            conversation.push(turn);
            thread.appendChild(turnView(turn, ctx));
            input.value = '';
            thread.lastChild.scrollIntoView({ behavior: 'smooth', block: 'start' });
            askBtn.disabled = true;
            try {
                Object.assign(turn, await api('POST', '/ai/ask', { question, history }));
            } catch (err) {
                turn.error = err.message || String(err);
            } finally {
                turn.pending = false;
                askBtn.disabled = false;
                turn.rerender();
            }
        };
        input.addEventListener('keydown', (e) => {
            if (e.key === 'Enter' && !e.shiftKey && !e.isComposing) { e.preventDefault(); send(); }
        });
        const form = h('form', { class: 'ai-composer', onsubmit: (e) => { e.preventDefault(); send(); } },
            input, h('div', { class: 'action-row' }, h('span', { class: 'muted small spacer' }, 'Enter to ask · Shift+Enter for a new line. Follow-up questions keep the conversation\'s context.'), askBtn));

        const clear = h('button', { class: 'btn', type: 'button', disabled: !conversation.length }, icon('refresh'), 'New conversation');
        clear.addEventListener('click', () => { conversation.length = 0; ctx.reload(); });
        setTimeout(() => input.focus(), 0);
        return [
            pageHeader('Ask AI', 'Ask questions about your data in plain language. The model sees your table schemas, writes DuckDB SQL and runs it read-only on the lake; save any result query as a function to run again later.', clear),
            cfg.configured ? null : notConfigured(ctx),
            thread,
            h('div', { class: 'card sticky-actions' }, h('div', { class: 'card-body' }, form)),
        ];
    });

    // --------------------------------------------------------- Saved functions

    function functionList(ctx, data) {
        const add = h('button', { class: 'btn primary', type: 'button', disabled: !ctx.isAdmin, onclick: () => ctx.navigate('ai-functions', 'new') }, icon('plus'), 'New function');
        const rows = data.functions.map(f => h('tr', { class: 'clickable', onclick: () => ctx.navigate('ai-functions', f.id) },
            h('td', {}, h('div', { class: 'rule-name' }, f.name), f.description ? h('div', { class: 'muted small' }, f.description) : null),
            h('td', { class: 'mono small' }, f.params.length ? f.params.map(p => `{{${p}}}`).join(' ') : h('span', { class: 'muted' }, '—')),
            h('td', { class: 'muted small' }, fmtDate(new Date(f.updatedMs).toISOString()), h('div', {}, f.updatedBy || '')),
            h('td', { class: 'actions' }, h('button', { class: 'btn small', type: 'button' }, icon('play'), 'Open'))));
        return [
            pageHeader('Saved Functions', 'Named SQL queries, saved from Ask AI or written here, to run on demand. They are read-only and run with the same checks and limits as user SQL.', add),
            h('div', { class: 'card' }, h('div', { class: 'card-body flush table-wrap' },
                rows.length
                    ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Function'), h('th', {}, 'Parameters'), h('th', {}, 'Updated'), h('th', {}, ''))), h('tbody', {}, rows))
                    : h('div', { class: 'empty' }, 'No saved functions yet. Ask a question on the ', h('a', { href: '#/ai-query' }, 'Ask AI'), ' page and save its result query.'))),
        ];
    }

    async function functionDetail(ctx, id) {
        const isNew = id === 'new';
        const fn = isNew ? { name: '', description: '', sql: 'SELECT ', params: [], question: '' } : (await api('GET', `/ai/functions/${enc(id)}`)).function;
        const ro = !ctx.isAdmin;
        const name = h('input', { type: 'text', value: fn.name, maxlength: 100, readOnly: ro });
        const description = h('input', { type: 'text', value: fn.description, maxlength: 1000, readOnly: ro });
        const editor = sqlEditor(fn.sql, { readOnly: ro, rows: 10 });
        const params = paramForm(fn.sql);
        const out = h('div');
        const unsaved = () => isNew || editor.value.trim() !== fn.sql.trim();

        const download = h('a', { class: 'btn', download: '' }, icon('download'), 'Download CSV');
        const syncDownload = () => {
            download.hidden = unsaved();
            download.href = `api/ai/functions/${enc(id)}/export?params=${enc(JSON.stringify(params.values()))}`;
        };
        editor.addEventListener('input', () => { params.sync(editor.value); syncDownload(); });
        params.el.addEventListener('input', syncDownload);
        syncDownload();

        const run = h('button', { class: 'btn primary', type: 'button' }, icon('play'), 'Run');
        run.addEventListener('click', () => withBusy(run, async () => {
            const body = { params: params.values() };
            const r = unsaved()
                ? await api('POST', '/ai/run', { sql: editor.value, ...body })
                : await api('POST', `/ai/functions/${enc(id)}/run`, body);
            out.replaceChildren(resultView(r));
        }));

        const save = h('button', { class: 'btn primary', type: 'button', hidden: ro }, isNew ? 'Create function' : 'Save changes');
        save.addEventListener('click', () => withBusy(save, async () => {
            const body = { name: name.value, description: description.value, sql: editor.value, question: fn.question };
            const res = isNew ? await api('POST', '/ai/functions', body) : await api('PUT', `/ai/functions/${enc(id)}`, body);
            toast(`Saved "${res.function.name}"`, 'success');
            if (isNew) ctx.navigate('ai-functions', res.function.id);
            else ctx.reload();
        }));
        const del = h('button', { class: 'btn danger', type: 'button', hidden: ro || isNew }, icon('trash'), 'Delete');
        del.addEventListener('click', async () => {
            if (!(await confirmDialog('Delete function', `Delete the saved function "${fn.name}"?`, { confirmLabel: 'Delete', danger: true }))) return;
            await withBusy(del, async () => {
                await api('DELETE', `/ai/functions/${enc(id)}`);
                toast('Function deleted', 'success');
                ctx.navigate('ai-functions');
            });
        });

        return [
            h('div', { class: 'crumbs' }, h('a', { href: '#/ai-functions' }, 'Saved Functions'), ' / '),
            pageHeader(isNew ? 'New function' : fn.name, fn.description || null, del),
            h('div', { class: 'card' },
                h('div', { class: 'card-header' }, h('h2', {}, 'Definition'),
                    isNew ? null : h('span', { class: 'hint' }, `Updated ${fmtDate(new Date(fn.updatedMs).toISOString())} by ${fn.updatedBy || '?'}`)),
                h('div', { class: 'card-body' },
                    h('div', { class: 'form-grid' }, field('Name', name), field('Description', description)),
                    field('SQL', editor, 'One read-only SELECT over lake tables. Use {{name}} for a value to fill in at each run; it is inserted as a quoted literal, e.g. WHERE region = {{region}} or created >= {{since}}.'),
                    fn.question ? h('p', { class: 'muted small card-intro' }, `Saved from the question: “${fn.question}”`) : null,
                    h('div', { class: 'action-row' }, save))),
            h('div', { class: 'card' },
                h('div', { class: 'card-header' }, h('h2', {}, 'Run'), h('span', { class: 'hint' }, 'Results show up to 1,000 rows; Download CSV returns them all (up to EXPORT_MAX_ROWS).')),
                h('div', { class: 'card-body row-gap' }, params.el, h('div', { class: 'action-row' }, run, download), out)),
        ];
    }

    registerView('ai-functions', async (ctx) => {
        if (ctx.params[0]) return functionDetail(ctx, ctx.params[0]);
        return functionList(ctx, await api('GET', '/ai/functions'));
    });

    // ------------------------------------------------------------- AI settings

    registerView('ai-settings', async (ctx) => {
        const data = await api('GET', '/ai/settings');
        const ro = !ctx.isAdmin;
        const inputs = new Map();
        const rows = data.settings.map(s => {
            const input = s.type === 'secret'
                ? h('input', { type: 'password', autocomplete: 'new-password', readOnly: ro, placeholder: s.hasValue ? `${s.masked} (unchanged)` : 'not set' })
                : h('input', { type: s.type === 'number' ? 'number' : 'text', value: s.value, readOnly: ro, min: s.min ?? undefined, max: s.max ?? undefined, placeholder: s.default || undefined });
            inputs.set(s.key, input);
            return h('div', { class: 'setting' },
                h('div', {}, h('div', { class: 'title' }, s.label), h('div', { class: 'key' }, s.key), s.help ? h('div', { class: 'desc' }, s.help) : null),
                h('div', { class: 'control' }, input,
                    h('div', { class: 'meta' }, sourceBadge(s),
                        s.source === 'console' && s.envValue !== null ? h('div', {}, `docker/.env value: ${s.envValue || '(empty)'}`) : null)));
        });
        const values = () => Object.fromEntries([...inputs].map(([k, i]) => [k, i.value]));

        const results = h('div', { class: 'test-results' });
        const test = h('button', { class: 'btn', type: 'button', hidden: ro }, 'Test connection');
        test.addEventListener('click', () => withBusy(test, async () => {
            results.replaceChildren(h('div', { class: 'muted ai-pending' }, h('span', { class: 'spinner' }), 'Testing…'));
            try {
                const r = await api('POST', '/ai/settings/test', { values: values() });
                results.replaceChildren(h('div', { class: `callout ${r.ok ? 'info' : 'error'}` },
                    h('strong', {}, r.ok ? 'Connection test passed' : 'Connection test failed'),
                    h('ul', { class: 'plain' }, r.results.map(x => h('li', {},
                        h('span', { class: `badge ${x.ok ? 'ok' : 'off'}` }, x.ok ? 'ok' : 'failed'), ' ', h('strong', {}, `${x.label}: `), x.detail,
                        x.warning ? h('div', { class: 'warn-text' }, x.warning) : null)))));
            } catch (err) {
                results.replaceChildren(h('div', { class: 'callout error' }, err.message));
            }
        }));
        const save = h('button', { class: 'btn primary', type: 'button', hidden: ro }, 'Save');
        save.addEventListener('click', () => withBusy(save, async () => {
            const r = await api('PUT', '/ai/settings', { values: values() });
            toast(r.changed.length ? 'AI settings saved' : 'Nothing changed', 'success');
            ctx.reload();
        }));
        const overridden = data.settings.some(s => s.source === 'console');
        const revert = h('button', { class: 'btn', type: 'button', hidden: ro || !overridden }, 'Revert to docker/.env');
        revert.addEventListener('click', async () => {
            if (!(await confirmDialog('Revert AI settings', 'Remove every AI setting saved in the console and use the docker/.env values (or defaults) again?', { confirmLabel: 'Revert', danger: true }))) return;
            await withBusy(revert, async () => {
                await api('DELETE', '/ai/settings');
                toast('AI settings reverted to docker/.env', 'success');
                ctx.reload();
            });
        });

        return [
            pageHeader('AI Settings', 'The OpenAI-compatible model used by Ask AI. Values come from docker/.env unless saved here; saved values are stored in the console catalog, shared by every instance and apply immediately.'),
            ro ? h('div', { class: 'callout info' }, 'You have read-only access. Ask an admin to change these settings.') : null,
            h('div', { class: 'card' },
                h('div', { class: 'card-header' }, h('h2', {}, 'Model API'), h('span', { class: `badge ${data.configured ? 'ok' : 'off'}` }, data.configured ? 'configured' : 'not configured')),
                h('div', { class: 'card-body flush' }, rows),
                h('div', { class: 'card-body' },
                    h('p', { class: 'muted small card-intro' }, 'The model receives your table names, column names and types, row counts and up to "Rows shared with the model" rows of each query result. It never receives system tables, API keys or console data. Test connection uses the values in the form, including unsaved changes; an empty API key field keeps the saved key.'),
                    h('div', { class: 'action-row' }, test, save, revert), results)),
        ];
    });
})();
