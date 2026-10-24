/*
 * "Retention & Pruning" console section: pruning rules (wildcard table
 * selectors, age and/or row-count criteria, per-rule cron schedule), a dry-run
 * preview, background jobs and the run log. Loaded after app.js and uses its
 * helpers (h, api, icon, modal, toast, withBusy, field, pageHeader, ...).
 */
'use strict';

(function () {
    const enc = encodeURIComponent;
    const STATUS_TONE = { succeeded: 'ok', running: 'env', partial: 'console', failed: 'off', skipped: '', abandoned: 'off' };

    function fmtMs(ms) {
        if (!ms) return '—';
        return new Date(Number(ms)).toLocaleString();
    }
    function fmtDuration(a, b) {
        if (!a || !b) return '—';
        const s = Math.max(0, Math.round((b - a) / 1000));
        return s < 60 ? `${s}s` : s < 3600 ? `${Math.floor(s / 60)}m ${s % 60}s` : `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
    }
    function badge(status) {
        return h('span', { class: `badge ${STATUS_TONE[status] ?? ''}` }, status || 'never run');
    }
    const n = (v) => Number(v || 0).toLocaleString();

    function describeCriteria(rule) {
        const parts = [];
        const EPOCH = { s: 'epoch seconds', ms: 'epoch ms', us: 'epoch µs', ns: 'epoch ns' };
        if (rule.maxAgeValue) parts.push(`older than ${n(rule.maxAgeValue)} ${rule.maxAgeUnit} (by ${rule.dateColumn}${EPOCH[rule.epochUnit] ? `, ${EPOCH[rule.epochUnit]}` : ''})`);
        if (rule.maxRows) parts.push(`beyond the newest ${n(rule.maxRows)} rows (by ${rule.orderColumn || 'insertion order'})`);
        if (rule.conditions?.items?.length) parts.push(`matching ${describeConditions(rule.conditions)}`);
        return `Delete rows ${parts.join(' or ')}`;
    }

    /**
     * Column dropdown rebuilt from the selected tables. Groups list column
     * names with how many of the selected tables have them; "Other…" reveals a
     * text box for columns not present yet (e.g. wildcard rules for future
     * tables). The current value is kept across rebuilds, flagged if missing.
     */
    function columnChooser({ value = '', emptyLabel = null, onChange = () => {} }) {
        const select = h('select', { class: 'column-select' });
        const other = h('input', { type: 'text', class: 'narrow', placeholder: 'column name', hidden: true });
        let current = value || '';
        // Until the user picks a column, follow the table selection: rebuild()
        // re-chooses the most common supported column. A saved value counts as
        // a choice and is kept.
        let userChosen = !!value;
        const sync = () => { other.hidden = select.value !== '__other__'; onChange(); };
        select.addEventListener('change', () => { userChosen = true; if (select.value !== '__other__') current = select.value; else other.focus(); sync(); });
        other.addEventListener('input', () => { userChosen = true; current = other.value.trim(); onChange(); });
        return {
            el: h('span', { class: 'column-chooser' }, select, other),
            get value() { return select.value === '__other__' ? other.value.trim() : select.value; },
            set disabled(d) { select.disabled = d; other.disabled = d; },
            rebuild(groups, tableCount) {
                const label = (c) => (c.detail ? `${c.name} — ${c.detail}` : tableCount > 1 ? `${c.name} — ${c.count} of ${tableCount} tables` : c.name);
                const known = new Set(groups.flatMap(g => g.items.map(i => i.name)));
                if (!userChosen) current = '';
                const options = [];
                if (emptyLabel !== null) options.push(h('option', { value: '' }, emptyLabel));
                for (const g of groups) {
                    if (g.items.length) options.push(h('optgroup', { label: g.label }, g.items.map(c => h('option', { value: c.name }, label(c)))));
                }
                if (current && !known.has(current)) {
                    options.push(h('optgroup', { label: 'Not in the selected tables' }, h('option', { value: current }, `${current} (not found)`)));
                }
                options.push(h('option', { value: '__other__' }, 'Other…'));
                select.replaceChildren(...options);
                if (current) select.value = current;
                else if (emptyLabel !== null) select.value = '';
                else {
                    // Default to the most common supported column.
                    const first = groups.flatMap(g => g.items)[0];
                    select.value = first ? first.name : '__other__';
                    current = first ? first.name : '';
                }
                if (select.value === '__other__') other.value = current;
                sync();
            },
        };
    }

    const NO_VALUE_OPS = new Set(['is_null', 'not_null']);
    let CONDITION_OPS = {};
    function describeConditions(c) {
        return c.items.map(i => `${i.column} ${CONDITION_OPS[i.op] || i.op}${NO_VALUE_OPS.has(i.op) ? '' : ` "${i.value}"`}`).join(c.match === 'any' ? ' or ' : ' and ');
    }

    let pollTimer = null;
    function stopPolling() { if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; } }

    registerView('retention', async (ctx) => {
        stopPolling();
        const data = await api('GET', '/retention');
        CONDITION_OPS = data.conditionOps || {};
        const tableNames = data.tables.map(t => t.name);
        const presetLabel = (cron) => data.presets.find(p => p.cron === cron)?.label || null;

        // ---------------------------------------------------------------- rules
        const newRule = h('button', { class: 'btn primary', type: 'button', disabled: !ctx.isAdmin,
            onclick: () => editRule(ctx, data, null) }, icon('plus'), 'New rule');

        const rows = data.rules.map(rule => {
            const toggle = h('label', { class: 'switch', title: rule.enabled ? 'Enabled' : 'Disabled' },
                h('input', { type: 'checkbox', checked: rule.enabled, disabled: !ctx.isAdmin }), h('span', {}));
            toggle.querySelector('input').addEventListener('change', (e) => withBusy(null, async () => {
                await api('PATCH', `/retention/rules/${rule.id}`, { enabled: e.target.checked });
                toast(`Rule ${e.target.checked ? 'enabled' : 'disabled'}`, 'success');
                ctx.reload();
            }));
            const run = h('button', { class: 'btn small', type: 'button', disabled: !ctx.isAdmin }, 'Run now');
            run.addEventListener('click', async () => {
                const ok = await confirmDialog('Run rule now', `Run "${rule.name}" on ${rule.matches.length} table(s) now? Deleted rows cannot be recovered. Use Preview first to see what would be deleted.`, { confirmLabel: 'Run now', danger: true });
                if (!ok) return;
                await withBusy(run, async () => {
                    await api('POST', `/retention/rules/${rule.id}/run`);
                    toast(`"${rule.name}" started; progress appears under Activity.`, 'success');
                    ctx.reload();
                });
            });
            const preview = h('button', { class: 'btn small', type: 'button', onclick: () => previewDialog(rule, `/retention/rules/${rule.id}/preview`) }, 'Preview');
            const clone = h('button', { class: 'btn small', type: 'button', disabled: !ctx.isAdmin, title: 'Create a new rule with these settings',
                onclick: () => editRule(ctx, data, rule, { clone: true }) }, 'Clone');
            const edit = h('button', { class: 'btn small', type: 'button', disabled: !ctx.isAdmin, onclick: () => editRule(ctx, data, rule) }, 'Edit');
            const del = h('button', { class: 'btn small danger', type: 'button', disabled: !ctx.isAdmin }, 'Delete');
            del.addEventListener('click', async () => {
                if (!(await confirmDialog('Delete rule', `Delete the rule "${rule.name}"? Existing data is not affected.`, { confirmLabel: 'Delete', danger: true }))) return;
                await withBusy(del, async () => { await api('DELETE', `/retention/rules/${rule.id}`); toast('Rule deleted', 'success'); ctx.reload(); });
            });
            return h('tr', {},
                h('td', {},
                    h('div', { class: 'rule-name' }, rule.name),
                    h('div', { class: 'mono muted' }, rule.tablePattern, rule.excludePattern ? ` (except ${rule.excludePattern})` : ''),
                    rule.legacyMultiTable
                        ? h('div', { class: 'small' }, h('span', { class: 'badge console', title: 'Saved before rules were limited to one table. It still runs; editing it requires choosing one table (use Clone for the others).' }, `multiple tables (${rule.matches.length})`))
                        : (rule.matches.length ? null : h('div', { class: 'muted small' }, 'Table does not exist yet'))),
                h('td', { class: 'small' }, describeCriteria(rule),
                    rule.safetyLimit ? h('div', { class: 'muted' }, `Safety limit: ${n(rule.safetyLimit)} rows per table`) : null,
                    rule.reclaimStorage ? h('div', { class: 'muted' }, 'Reclaims storage after pruning') : null),
                h('td', { class: 'small' }, h('div', {}, presetLabel(rule.schedule) || h('span', { class: 'mono' }, rule.schedule)),
                    h('div', { class: 'muted' }, rule.enabled ? `Next: ${fmtMs(rule.nextRunMs)}` : 'Paused')),
                h('td', { class: 'small' }, badge(rule.lastStatus),
                    rule.lastRunMs ? h('div', { class: 'muted' }, `${fmtMs(rule.lastRunMs)} · ${n(rule.lastDeleted)} deleted`) : null),
                h('td', {}, toggle),
                h('td', { class: 'actions' }, run, preview, clone, edit, del));
        });

        const rulesCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Rules'), h('span', { class: 'hint' }, 'Rows meeting any ticked criterion are deleted. Schedules are in UTC.')),
            h('div', { class: 'card-body flush table-wrap' }, rows.length
                ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Rule & table'), h('th', {}, 'Criteria'), h('th', {}, 'Schedule'), h('th', {}, 'Last run'), h('th', {}, 'On'), h('th', {}, ''))), h('tbody', {}, rows))
                : h('div', { class: 'empty' }, 'No pruning rules yet. Create one to delete old or excess rows automatically.')));

        // -------------------------------------------------------------- storage
        const days = data.storage.snapshotRetentionDays;
        const storageCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Storage reclaim'),
                h('span', { class: `badge ${days === null ? 'console' : 'ok'}` }, days === null ? 'snapshots kept forever' : `snapshots kept ${days} day(s)`)),
            h('div', { class: 'card-body' }, h('div', { class: 'help' },
                'Deleting rows does not free disk space by itself: DuckLake keeps old snapshots for time travel, and they still reference the deleted rows. ' +
                'Rules with "Reclaim storage" rewrite the affected tables after pruning, and expire snapshots older than the retention below. ' +
                'Leave it empty to keep unlimited history (storage is then never reclaimed).')),
            h('div', { class: 'card-body flush' }, settingRows([data.storage.setting], ctx.isAdmin, ctx.reload)));

        // ------------------------------------------------------------- activity
        const ruleFilter = h('select', { class: 'compact' }, h('option', { value: '' }, 'All rules'),
            data.rules.map(r => h('option', { value: r.id }, r.name)));
        const activityBody = h('div', { class: 'card-body flush table-wrap' });
        const expanded = new Set();
        const route = location.hash;

        async function loadRuns() {
            stopPolling();
            if (location.hash !== route) return;
            let jobs;
            try { jobs = (await api('GET', `/retention/runs?limit=40${ruleFilter.value ? `&rule=${enc(ruleFilter.value)}` : ''}`)).jobs; }
            catch (err) { activityBody.replaceChildren(h('div', { class: 'callout error' }, err.message)); return; }
            drawRuns(jobs);
            // Keep refreshing while anything is running.
            if (jobs.some(j => j.status === 'running' || j.tables.some(t => t.status === 'running'))) {
                pollTimer = setTimeout(loadRuns, 3000);
            }
        }

        function drawRuns(jobs) {
            if (!jobs.length) { activityBody.replaceChildren(h('div', { class: 'empty' }, 'No pruning jobs have run yet.')); return; }
            const body = [];
            for (const job of jobs) {
                const open = expanded.has(job.jobId);
                const toggle = h('button', { class: 'btn small', type: 'button', 'aria-expanded': String(open),
                    onclick: () => { open ? expanded.delete(job.jobId) : expanded.add(job.jobId); drawRuns(jobs); } }, open ? '▾' : '▸');
                const done = job.tables.filter(t => t.status !== 'running').length;
                body.push(h('tr', { class: 'job-row' },
                    h('td', {}, toggle),
                    h('td', { class: 'small' }, fmtMs(job.startedMs)),
                    h('td', {}, job.ruleName, h('div', { class: 'muted small' }, job.trigger === 'manual' ? `manual · ${job.username || ''}` : job.trigger)),
                    h('td', {}, badge(job.status), job.status === 'running' ? h('div', { class: 'muted small' }, `${done} of ${job.tables.length} table(s)`) : null),
                    h('td', { class: 'num' }, job.tables.length),
                    h('td', { class: 'num' }, n(job.rowsDeleted)),
                    h('td', { class: 'small' }, job.status === 'running' ? 'running…' : fmtDuration(job.startedMs, job.finishedMs)),
                    h('td', { class: 'small muted' }, job.message)));
                if (open) {
                    body.push(h('tr', { class: 'job-detail' }, h('td', { colspan: 8 },
                        job.tables.length ? h('table', { class: 'inner' },
                            h('thead', {}, h('tr', {}, h('th', {}, 'Table'), h('th', {}, 'Status'), h('th', { class: 'num' }, 'By age'), h('th', { class: 'num' }, 'By count'), h('th', { class: 'num' }, 'By match'), h('th', { class: 'num' }, 'Deleted'), h('th', {}, 'Details'))),
                            h('tbody', {}, job.tables.map(t => h('tr', {},
                                h('td', { class: 'mono' }, t.table), h('td', {}, badge(t.status)),
                                h('td', { class: 'num' }, n(t.rowsByAge)), h('td', { class: 'num' }, n(t.rowsByCount)), h('td', { class: 'num' }, n(t.rowsByMatch)), h('td', { class: 'num' }, n(t.rowsDeleted)),
                                h('td', { class: 'small muted' }, t.message, t.blobFiles ? ` ${n(t.blobFiles)} blob file(s) removed.` : '')))))
                            : h('div', { class: 'muted' }, 'No tables were processed.'))));
                }
            }
            activityBody.replaceChildren(h('table', {},
                h('thead', {}, h('tr', {}, h('th', {}, ''), h('th', {}, 'Started'), h('th', {}, 'Rule'), h('th', {}, 'Status'), h('th', { class: 'num' }, 'Tables'), h('th', { class: 'num' }, 'Deleted'), h('th', {}, 'Duration'), h('th', {}, 'Summary'))),
                h('tbody', {}, body)));
        }
        ruleFilter.addEventListener('change', loadRuns);
        const refresh = h('button', { class: 'btn small', type: 'button', title: 'Refresh', onclick: loadRuns }, icon('refresh'));
        const activityCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Activity'), h('div', { class: 'grid-toolbar-row' }, ruleFilter, refresh)),
            activityBody);
        await loadRuns();

        return [
            pageHeader('Retention & Pruning', 'Delete old, excess or matching rows automatically. Each rule prunes one table on its own schedule; use Clone to reuse a rule for another table. System tables are never pruned.', newRule),
            rulesCard, activityCard, storageCard,
        ];
    });

    // =========================================================================
    // Preview (dry run)
    // =========================================================================

    function previewTable(result) {
        if (!result.tables.length) return h('div', { class: 'empty' }, 'No tables match.');
        return h('div', { class: 'table-wrap form-scroll' }, h('table', {},
            h('thead', {}, h('tr', {}, h('th', {}, 'Table'), h('th', { class: 'num' }, 'Rows'), h('th', { class: 'num' }, 'By age'), h('th', { class: 'num' }, 'By count'), h('th', { class: 'num' }, 'By match'), h('th', { class: 'num' }, 'Would delete'), h('th', {}, ''))),
            h('tbody', {}, result.tables.map(t => h('tr', {},
                h('td', { class: 'mono' }, t.table),
                h('td', { class: 'num' }, t.rows === null ? '—' : n(t.rows)),
                h('td', { class: 'num' }, n(t.byAge)), h('td', { class: 'num' }, n(t.byCount)), h('td', { class: 'num' }, n(t.byMatch)),
                h('td', { class: 'num' }, h('strong', {}, n(t.total))),
                h('td', { class: 'small' }, t.status === 'skipped' ? h('span', { class: 'muted' }, `Skipped: ${t.reason}`)
                    : [t.overLimit ? h('span', { class: 'badge off' }, 'over safety limit') : null,
                        t.dateInfo ? h('span', { class: 'muted' }, ` by ${t.dateInfo}`) : null]))))));
    }

    async function previewDialog(spec, endpoint = '/retention/preview') {
        let result;
        try { result = await api('POST', endpoint, endpoint === '/retention/preview' ? spec : undefined); }
        catch (err) { return toast(err.message, 'error'); }
        await modal({
            title: `Preview: ${spec.name || 'rule'}`,
            body: h('div', { class: 'row-gap' },
                h('div', { class: 'callout info' }, `Running now would delete ${n(result.totalRows)} row(s) across ${result.tables.filter(t => t.status === 'ok').length} table(s). Nothing has been deleted.`),
                previewTable(result)),
            wide: true,
        });
    }

    // =========================================================================
    // Rule editor
    // =========================================================================

    async function editRule(ctx, data, rule, { clone = false } = {}) {
        let values = rule ? { ...rule } : {
            name: '', tablePattern: '', excludePattern: '', maxAgeValue: 30, maxAgeUnit: 'days', dateColumn: '',
            maxRows: null, orderColumn: '', schedule: '0 2 * * *', enabled: true, safetyLimit: null, reclaimStorage: false,
        };
        if (clone) values.name = `${rule.name} (copy)`;
        const isEdit = !!rule && !clone;
        // Reopen the editor with the entered values if saving fails.
        for (;;) {
            values = await ruleForm(data, values, isEdit, clone);
            if (!values) return;
            try {
                if (isEdit) await api('PUT', `/retention/rules/${rule.id}`, values);
                else await api('POST', '/retention/rules', values);
                toast(`Rule "${values.name}" saved`, 'success');
                ctx.reload();
                return;
            } catch (err) {
                toast(err.message, 'error');
            }
        }
    }

    function ruleForm(data, v, isEdit, isClone = false) {
        const name = h('input', { type: 'text', value: v.name, placeholder: 'e.g. Sensor data – 90 days' });
        const singleTable = /^[A-Za-z_][A-Za-z0-9_]{0,62}$/.test(v.tablePattern || '') && !v.excludePattern;
        let selectedTable = singleTable ? v.tablePattern : '';
        const legacyNote = !singleTable && v.tablePattern
            ? h('div', { class: 'callout' }, `This rule was saved with several tables (${v.tablePattern}${v.excludePattern ? ` except ${v.excludePattern}` : ''}). Rules now prune one table each: choose the table to keep here, and use Clone for the others.`)
            : null;
        const tableInfo = h('div', { class: 'help' });

        const useAge = h('input', { type: 'checkbox', checked: !!v.maxAgeValue });
        const ageValue = h('input', { type: 'number', min: 1, value: v.maxAgeValue || 30, class: 'narrow' });
        const ageUnit = h('select', { class: 'narrow' }, ['days', 'hours'].map(u => h('option', { value: u, selected: v.maxAgeUnit === u }, u)));
        const epochNames = new Set(); // columns that are numbers in the selected tables
        const epochRow = h('div', { class: 'criterion indent' });
        const dateColumn = columnChooser({
            value: v.dateColumn || '',
            onChange: () => { epochRow.hidden = !(epochNames.has(dateColumn.value) || !dateColumnKnown(dateColumn.value)); },
        });
        const epochUnit = h('select', { class: 'narrow', title: 'Only used when the column is a number (Unix epoch time)' },
            [['auto', 'auto-detect'], ['s', 'seconds'], ['ms', 'milliseconds'], ['us', 'microseconds'], ['ns', 'nanoseconds']]
                .map(([val, label]) => h('option', { value: val, selected: (v.epochUnit || 'auto') === val }, label)));

        const useCount = h('input', { type: 'checkbox', checked: !!v.maxRows });
        const maxRows = h('input', { type: 'number', min: 1, value: v.maxRows || 1000000, class: 'narrow' });
        epochRow.append(h('span', { class: 'muted small' }, 'Column is a number (Unix epoch); its unit is'), epochUnit);
        const orderColumn = columnChooser({ value: v.orderColumn || '', emptyLabel: '(insertion order)' });
        let dateColumnKnown = () => true;

        // --- Match conditions: structured column / operator / value rows (no SQL text).
        const useMatch = h('input', { type: 'checkbox', checked: !!v.conditions?.items?.length });
        const matchMode = h('select', { class: 'narrow' }, h('option', { value: 'all', selected: v.conditions?.match !== 'any' }, 'all'),
            h('option', { value: 'any', selected: v.conditions?.match === 'any' }, 'any'));
        const condRows = h('div', { class: 'cond-rows' });
        let condColumnGroups = [];
        let condTableCount = 0;
        const conditionRows = [];
        const addCondition = (c = {}) => {
            const col = columnChooser({ value: c.column || '' });
            const op = h('select', { class: 'narrow' }, Object.entries(CONDITION_OPS).map(([k, label]) => h('option', { value: k, selected: (c.op || 'equals') === k }, label)));
            const val = h('input', { type: 'text', class: 'narrow', value: c.value || '', placeholder: 'value' });
            const syncOp = () => { val.hidden = NO_VALUE_OPS.has(op.value); val.placeholder = op.value === 'in' ? 'a, b, c' : 'value'; };
            op.addEventListener('change', syncOp);
            const row = { col, op, val, el: null };
            const remove = h('button', { class: 'btn small danger', type: 'button', title: 'Remove condition', onclick: () => {
                conditionRows.splice(conditionRows.indexOf(row), 1); row.el.remove();
            } }, '×');
            row.el = h('div', { class: 'criterion cond-row' }, col.el, op, val, remove);
            conditionRows.push(row);
            condRows.appendChild(row.el);
            col.rebuild(condColumnGroups, condTableCount);
            syncOp();
        };
        const addCondBtn = h('button', { class: 'btn small', type: 'button', onclick: () => addCondition() }, icon('plus'), 'Add condition');

        const preset = h('select', {}, data.presets.map(p => h('option', { value: p.cron, selected: p.cron === v.schedule }, p.label)),
            h('option', { value: '__custom__', selected: !data.presets.some(p => p.cron === v.schedule) }, 'Custom (cron)'));
        const cron = h('input', { type: 'text', value: v.schedule, class: 'mono narrow' });
        const cronOut = h('div', { class: 'help' });

        const safety = h('input', { type: 'number', min: 1, value: v.safetyLimit || '', placeholder: 'no limit', class: 'narrow' });
        const reclaim = h('input', { type: 'checkbox', checked: !!v.reclaimStorage });
        const enabled = h('input', { type: 'checkbox', checked: v.enabled !== false });
        const previewOut = h('div', {});

        // --- Table: one per rule. Choosing it rebuilds every column dropdown
        // from that table's actual columns and types.
        const lower = (x) => x.toLowerCase();
        const findTable = (nm) => data.tables.find(t => lower(t.name) === lower(nm || ''));
        const syncColumns = () => {
            const t = findTable(selectedTable);
            const type = (c) => (t && t.types ? t.types[c] : '');
            const items = (cols, suffix = '') => cols.map(c => ({ name: c, count: 1, detail: `${type(c)}${suffix}` }));
            tableInfo.textContent = !selectedTable ? 'Choose the table to prune. To apply similar rules to other tables, save this one and use Clone.'
                : t ? `${selectedTable}: ${t.columns.length} column(s)` : `${selectedTable} does not exist yet; the rule applies once it is created. Columns cannot be listed, so use Other… to type them.`;
            const dateGroups = t ? [
                { label: 'Dates & timestamps', items: items(t.dateColumns) },
                { label: 'Epoch numbers (likely)', items: items(t.epochColumns || [], ', epoch') },
                { label: 'Other numbers (epoch)', items: items(t.numericColumns || [], ', epoch?') },
            ] : [];
            epochNames.clear();
            if (t) for (const c of [...(t.epochColumns || []), ...(t.numericColumns || [])]) epochNames.add(c);
            const supported = new Set(dateGroups.flatMap(g => g.items.map(i => i.name)));
            dateColumnKnown = (c) => supported.has(c);
            dateColumn.rebuild(dateGroups, 1);
            const allCols = t ? [{ label: 'Columns', items: items(t.columns) }] : [];
            orderColumn.rebuild(allCols, 1);
            condColumnGroups = allCols;
            condTableCount = 1;
            for (const r of conditionRows) r.col.rebuild(condColumnGroups, condTableCount);
            drawTables();
        };
        const tableFilter = h('input', { type: 'text', placeholder: 'Filter tables…', class: 'picker-filter' });
        const tableList = h('div', { class: 'picker-list', role: 'radiogroup', 'aria-label': 'Table' });
        const otherTable = h('input', { type: 'text', class: 'narrow', placeholder: 'or type a table name' });
        const drawTables = () => {
            const q = lower(tableFilter.value.trim());
            const missing = selectedTable && !findTable(selectedTable) ? [{ name: selectedTable, missing: true, dateColumns: [], epochColumns: [] }] : [];
            const shown = [...missing, ...data.tables].filter(t => !q || lower(t.name).includes(q));
            tableList.replaceChildren(...(shown.length ? shown.map(t => {
                const on = lower(t.name) === lower(selectedTable);
                const radio = h('input', { type: 'radio', name: 'prune-table', checked: on });
                radio.addEventListener('change', () => { selectedTable = t.name; otherTable.value = ''; syncColumns(); });
                return h('label', { class: `picker-row ${on ? 'on' : ''}` }, radio, h('span', { class: 'mono' }, t.name),
                    t.missing ? h('span', { class: 'badge console' }, 'not found') : null,
                    h('span', { class: 'muted small picker-cols' }, t.missing ? '' : [
                        t.dateColumns.length ? `date: ${t.dateColumns.join(', ')}` : null,
                        t.epochColumns?.length ? `epoch: ${t.epochColumns.join(', ')}` : null,
                    ].filter(Boolean).join(' · ') || 'no date columns'));
            }) : [h('div', { class: 'muted small' }, data.tables.length ? 'No tables match the filter.' : 'No user tables exist yet.')]));
        };
        tableFilter.addEventListener('input', drawTables);
        otherTable.addEventListener('change', () => { if (otherTable.value.trim()) { selectedTable = otherTable.value.trim(); syncColumns(); } });
        const tablePicker = h('div', { class: 'table-picker' },
            h('div', { class: 'picker-head' }, tableFilter, h('span', { class: 'spacer' }), otherTable),
            tableList);
        syncColumns();

        let cronTimer = null;
        const syncCron = () => {
            clearTimeout(cronTimer);
            cronTimer = setTimeout(async () => {
                try {
                    const r = await api('POST', '/retention/schedule', { schedule: cron.value });
                    cronOut.className = r.valid ? 'help' : 'help error-text';
                    cronOut.textContent = r.valid ? `Next runs (shown in your time zone): ${r.next.map(fmtMs).join(' · ')}` : r.error;
                } catch (err) { cronOut.textContent = err.message; }
            }, 250);
        };
        preset.addEventListener('change', () => { if (preset.value !== '__custom__') cron.value = preset.value; syncCron(); });
        cron.addEventListener('input', () => { preset.value = data.presets.some(p => p.cron === cron.value) ? cron.value : '__custom__'; syncCron(); });
        syncCron();

        const toggleCriteria = () => {
            for (const el of [ageValue, ageUnit, dateColumn, epochUnit]) el.disabled = !useAge.checked;
            for (const el of [maxRows, orderColumn]) el.disabled = !useCount.checked;
            condRows.classList.toggle('disabled', !useMatch.checked);
            for (const r of conditionRows) { r.col.disabled = !useMatch.checked; r.op.disabled = !useMatch.checked; r.val.disabled = !useMatch.checked; }
            matchMode.disabled = !useMatch.checked; addCondBtn.disabled = !useMatch.checked;
        };
        useAge.addEventListener('change', toggleCriteria);
        useCount.addEventListener('change', toggleCriteria);
        useMatch.addEventListener('change', () => { if (useMatch.checked && !conditionRows.length) addCondition(); toggleCriteria(); });
        for (const c of v.conditions?.items || []) addCondition(c);
        toggleCriteria();

        const read = () => ({
            name: name.value.trim(),
            table: selectedTable,
            maxAgeValue: useAge.checked ? Number(ageValue.value) : null,
            maxAgeUnit: ageUnit.value,
            dateColumn: useAge.checked ? dateColumn.value : null,
            epochUnit: epochUnit.value,
            maxRows: useCount.checked ? Number(maxRows.value) : null,
            orderColumn: useCount.checked ? orderColumn.value || null : null,
            conditions: useMatch.checked && conditionRows.length
                ? { match: matchMode.value, items: conditionRows.map(r => ({ column: r.col.value, op: r.op.value, value: r.val.value })) }
                : null,
            schedule: cron.value.trim(),
            safetyLimit: safety.value ? Number(safety.value) : null,
            reclaimStorage: reclaim.checked,
            enabled: enabled.checked,
        });

        const previewBtn = h('button', { class: 'btn small', type: 'button' }, 'Preview what would be deleted');
        previewBtn.addEventListener('click', () => withBusy(previewBtn, async () => {
            previewOut.replaceChildren(h('div', { class: 'muted' }, 'Counting…'));
            const r = await api('POST', '/retention/preview', read());
            previewOut.replaceChildren(h('div', { class: 'callout info' }, `Would delete ${n(r.totalRows)} row(s) now.`), previewTable(r));
        }).catch(() => previewOut.replaceChildren()));

        const body = h('div', { class: 'rule-form' },
            field('Name', name),
            legacyNote,
            h('div', { class: 'field' }, h('label', { class: 'label' }, 'Table'), tablePicker, tableInfo),
            h('fieldset', {}, h('legend', {}, 'Delete rows that are…'),
                h('div', { class: 'criterion' }, h('label', { class: 'checkbox' }, useAge, 'older than'), ageValue, ageUnit, h('span', {}, 'based on column'), dateColumn.el),
                epochRow,
                h('div', { class: 'criterion' }, h('label', { class: 'checkbox' }, useCount, 'beyond the newest'), maxRows, h('span', {}, 'rows, ordered by'), orderColumn.el),
                h('div', { class: 'criterion' }, h('label', { class: 'checkbox' }, useMatch, 'matching'), matchMode, h('span', {}, 'of these conditions:'), addCondBtn),
                condRows,
                h('div', { class: 'help' }, 'With more than one ticked, a row is deleted if it meets any of them. Conditions compare values only; SQL is never accepted.')),
            h('fieldset', {}, h('legend', {}, 'Schedule (UTC)'),
                h('div', { class: 'criterion' }, preset, cron), cronOut),
            h('fieldset', {}, h('legend', {}, 'Safety and storage'),
                h('div', { class: 'criterion' }, h('span', {}, 'Stop if a run would delete more than'), safety, h('span', {}, 'rows')),
                h('label', { class: 'checkbox' }, reclaim, 'Reclaim storage after pruning (rewrites the affected tables; see Storage reclaim)'),
                h('label', { class: 'checkbox' }, enabled, 'Enabled')),
            previewBtn, previewOut);

        return modal({
            title: isEdit ? `Edit rule: ${v.name}` : isClone ? 'Clone pruning rule' : 'New pruning rule',
            body,
            actions: [{ label: 'Cancel', value: null }, { label: isEdit ? 'Save rule' : 'Create rule', value: 'save', primary: true }],
            wide: true,
        }).then(choice => (choice === 'save' ? read() : null));
    }

    window.addEventListener('hashchange', () => { if (!location.hash.startsWith('#/retention')) stopPolling(); });
})();
