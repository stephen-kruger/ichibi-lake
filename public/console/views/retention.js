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

    // Same matching rules as the server (src/console/retention.js).
    function globRegex(glob) {
        return new RegExp(`^${glob.replace(/[.+^${}()|[\]\\]/g, '\\$&').replace(/\*/g, '.*').replace(/\?/g, '.')}$`, 'i');
    }
    function splitPatterns(text) { return String(text || '').split(',').map(s => s.trim()).filter(Boolean); }
    function matchTables(names, include, exclude) {
        const inc = splitPatterns(include).map(globRegex);
        const exc = splitPatterns(exclude).map(globRegex);
        return names.filter(t => !t.startsWith('_') && inc.some(r => r.test(t)) && !exc.some(r => r.test(t))).sort();
    }

    function describeCriteria(rule) {
        const parts = [];
        if (rule.maxAgeValue) parts.push(`older than ${n(rule.maxAgeValue)} ${rule.maxAgeUnit} (by ${rule.dateColumn})`);
        if (rule.maxRows) parts.push(`beyond the newest ${n(rule.maxRows)} rows (by ${rule.orderColumn || 'insertion order'})`);
        return `Delete rows ${parts.join(' or ')}`;
    }

    let pollTimer = null;
    function stopPolling() { if (pollTimer) { clearTimeout(pollTimer); pollTimer = null; } }

    registerView('retention', async (ctx) => {
        stopPolling();
        const data = await api('GET', '/retention');
        const tableNames = data.tables.map(t => t.name);
        const presetLabel = (cron) => data.presets.find(p => p.cron === cron)?.label || null;

        // ---------------------------------------------------------------- rules
        const newRule = h('button', { class: 'btn primary', type: 'button', disabled: !ctx.isAdmin,
            onclick: () => editRule(ctx, data, null) }, icon('plus'), 'New rule');

        const rows = data.rules.map(rule => {
            const toggle = h('label', { class: 'switch', title: rule.enabled ? 'Enabled' : 'Disabled' },
                h('input', { type: 'checkbox', checked: rule.enabled, disabled: !ctx.isAdmin }), h('span', {}));
            toggle.querySelector('input').addEventListener('change', (e) => withBusy(null, async () => {
                await api('PUT', `/retention/rules/${rule.id}`, { ...rule, enabled: e.target.checked });
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
            const preview = h('button', { class: 'btn small', type: 'button', onclick: () => previewDialog(rule) }, 'Preview');
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
                    h('div', { class: 'muted small', title: rule.matches.join(', ') || 'No tables match' },
                        rule.matches.length ? `${rule.matches.length} table(s): ${rule.matches.slice(0, 4).join(', ')}${rule.matches.length > 4 ? ', …' : ''}` : 'No tables match yet')),
                h('td', { class: 'small' }, describeCriteria(rule),
                    rule.safetyLimit ? h('div', { class: 'muted' }, `Safety limit: ${n(rule.safetyLimit)} rows per table`) : null,
                    rule.reclaimStorage ? h('div', { class: 'muted' }, 'Reclaims storage after pruning') : null),
                h('td', { class: 'small' }, h('div', {}, presetLabel(rule.schedule) || h('span', { class: 'mono' }, rule.schedule)),
                    h('div', { class: 'muted' }, rule.enabled ? `Next: ${fmtMs(rule.nextRunMs)}` : 'Paused')),
                h('td', { class: 'small' }, badge(rule.lastStatus),
                    rule.lastRunMs ? h('div', { class: 'muted' }, `${fmtMs(rule.lastRunMs)} · ${n(rule.lastDeleted)} deleted`) : null),
                h('td', {}, toggle),
                h('td', { class: 'actions' }, run, preview, edit, del));
        });

        const rulesCard = h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Rules'), h('span', { class: 'hint' }, 'Rows matching either criterion are deleted. Schedules are in UTC.')),
            h('div', { class: 'card-body flush table-wrap' }, rows.length
                ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Rule & tables'), h('th', {}, 'Criteria'), h('th', {}, 'Schedule'), h('th', {}, 'Last run'), h('th', {}, 'On'), h('th', {}, ''))), h('tbody', {}, rows))
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
                            h('thead', {}, h('tr', {}, h('th', {}, 'Table'), h('th', {}, 'Status'), h('th', { class: 'num' }, 'By age'), h('th', { class: 'num' }, 'By count'), h('th', { class: 'num' }, 'Deleted'), h('th', {}, 'Details'))),
                            h('tbody', {}, job.tables.map(t => h('tr', {},
                                h('td', { class: 'mono' }, t.table), h('td', {}, badge(t.status)),
                                h('td', { class: 'num' }, n(t.rowsByAge)), h('td', { class: 'num' }, n(t.rowsByCount)), h('td', { class: 'num' }, n(t.rowsDeleted)),
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
            pageHeader('Retention & Pruning', 'Delete old or excess rows automatically. Each rule selects tables by name or wildcard (new tables are picked up automatically) and runs on its own schedule. System tables are never pruned.', newRule),
            rulesCard, activityCard, storageCard,
        ];
    });

    // =========================================================================
    // Preview (dry run)
    // =========================================================================

    function previewTable(result) {
        if (!result.tables.length) return h('div', { class: 'empty' }, 'No tables match.');
        return h('div', { class: 'table-wrap form-scroll' }, h('table', {},
            h('thead', {}, h('tr', {}, h('th', {}, 'Table'), h('th', { class: 'num' }, 'Rows'), h('th', { class: 'num' }, 'By age'), h('th', { class: 'num' }, 'By count'), h('th', { class: 'num' }, 'Would delete'), h('th', {}, ''))),
            h('tbody', {}, result.tables.map(t => h('tr', {},
                h('td', { class: 'mono' }, t.table),
                h('td', { class: 'num' }, t.rows === null ? '—' : n(t.rows)),
                h('td', { class: 'num' }, n(t.byAge)), h('td', { class: 'num' }, n(t.byCount)),
                h('td', { class: 'num' }, h('strong', {}, n(t.total))),
                h('td', { class: 'small' }, t.status === 'skipped' ? h('span', { class: 'muted' }, `Skipped: ${t.reason}`)
                    : t.overLimit ? h('span', { class: 'badge off' }, 'over safety limit') : null))))));
    }

    async function previewDialog(spec) {
        let result;
        try { result = await api('POST', '/retention/preview', spec); }
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

    async function editRule(ctx, data, rule) {
        let values = rule ? { ...rule } : {
            name: '', tablePattern: '', excludePattern: '', maxAgeValue: 30, maxAgeUnit: 'days', dateColumn: 'created',
            maxRows: null, orderColumn: '', schedule: '0 2 * * *', enabled: true, safetyLimit: null, reclaimStorage: false,
        };
        // Reopen the editor with the entered values if saving fails.
        for (;;) {
            values = await ruleForm(data, values, !!rule);
            if (!values) return;
            try {
                if (rule) await api('PUT', `/retention/rules/${rule.id}`, values);
                else await api('POST', '/retention/rules', values);
                toast(`Rule "${values.name}" saved`, 'success');
                ctx.reload();
                return;
            } catch (err) {
                toast(err.message, 'error');
            }
        }
    }

    function ruleForm(data, v, isEdit) {
        const tableNames = data.tables.map(t => t.name);
        const name = h('input', { type: 'text', value: v.name, placeholder: 'e.g. Sensor data – 90 days' });
        const include = h('input', { type: 'text', value: v.tablePattern, placeholder: 'e.g. sensor_*, events' });
        const exclude = h('input', { type: 'text', value: v.excludePattern || '', placeholder: 'optional, e.g. *_archive' });
        const matchOut = h('div', { class: 'help' });

        const useAge = h('input', { type: 'checkbox', checked: !!v.maxAgeValue });
        const ageValue = h('input', { type: 'number', min: 1, value: v.maxAgeValue || 30, class: 'narrow' });
        const ageUnit = h('select', { class: 'narrow' }, ['days', 'hours'].map(u => h('option', { value: u, selected: v.maxAgeUnit === u }, u)));
        const dateColumn = h('input', { type: 'text', value: v.dateColumn || 'created', list: 'prune-date-columns', class: 'narrow' });
        const dateList = h('datalist', { id: 'prune-date-columns' });

        const useCount = h('input', { type: 'checkbox', checked: !!v.maxRows });
        const maxRows = h('input', { type: 'number', min: 1, value: v.maxRows || 1000000, class: 'narrow' });
        const orderColumn = h('input', { type: 'text', value: v.orderColumn || '', list: 'prune-order-columns', placeholder: 'insertion order', class: 'narrow' });
        const orderList = h('datalist', { id: 'prune-order-columns' });

        const preset = h('select', {}, data.presets.map(p => h('option', { value: p.cron, selected: p.cron === v.schedule }, p.label)),
            h('option', { value: '__custom__', selected: !data.presets.some(p => p.cron === v.schedule) }, 'Custom (cron)'));
        const cron = h('input', { type: 'text', value: v.schedule, class: 'mono narrow' });
        const cronOut = h('div', { class: 'help' });

        const safety = h('input', { type: 'number', min: 1, value: v.safetyLimit || '', placeholder: 'no limit', class: 'narrow' });
        const reclaim = h('input', { type: 'checkbox', checked: !!v.reclaimStorage });
        const enabled = h('input', { type: 'checkbox', checked: v.enabled !== false });
        const previewOut = h('div', {});

        // Live table-match feedback and column suggestions.
        const syncMatches = () => {
            const m = matchTables(tableNames, include.value, exclude.value);
            matchOut.textContent = !include.value.trim() ? 'Comma-separated names or wildcards (* and ?). Tables created later that match are included automatically.'
                : m.length ? `Matches ${m.length} table(s) now: ${m.slice(0, 12).join(', ')}${m.length > 12 ? ', …' : ''}` : 'No tables match yet (the rule still applies to matching tables created later).';
            const matched = data.tables.filter(t => m.includes(t.name));
            const dates = [...new Set(matched.flatMap(t => t.dateColumns))];
            const all = [...new Set(matched.flatMap(t => t.columns))];
            dateList.replaceChildren(...dates.map(c => h('option', { value: c })));
            orderList.replaceChildren(...all.map(c => h('option', { value: c })));
        };
        include.addEventListener('input', syncMatches);
        exclude.addEventListener('input', syncMatches);
        syncMatches();

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
            for (const el of [ageValue, ageUnit, dateColumn]) el.disabled = !useAge.checked;
            for (const el of [maxRows, orderColumn]) el.disabled = !useCount.checked;
        };
        useAge.addEventListener('change', toggleCriteria);
        useCount.addEventListener('change', toggleCriteria);
        toggleCriteria();

        const read = () => ({
            name: name.value.trim(),
            tablePattern: include.value,
            excludePattern: exclude.value,
            maxAgeValue: useAge.checked ? Number(ageValue.value) : null,
            maxAgeUnit: ageUnit.value,
            dateColumn: useAge.checked ? dateColumn.value.trim() : null,
            maxRows: useCount.checked ? Number(maxRows.value) : null,
            orderColumn: useCount.checked ? orderColumn.value.trim() || null : null,
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
            h('div', { class: 'form-grid' }, field('Tables', include), field('Except', exclude)),
            matchOut,
            h('fieldset', {}, h('legend', {}, 'Delete rows that are…'),
                h('div', { class: 'criterion' }, h('label', { class: 'checkbox' }, useAge, 'older than'), ageValue, ageUnit, h('span', {}, 'based on column'), dateColumn, dateList),
                h('div', { class: 'criterion' }, h('label', { class: 'checkbox' }, useCount, 'beyond the newest'), maxRows, h('span', {}, 'rows, ordered by'), orderColumn, orderList),
                h('div', { class: 'help' }, 'With both ticked, a row is deleted if it fails either one.')),
            h('fieldset', {}, h('legend', {}, 'Schedule (UTC)'),
                h('div', { class: 'criterion' }, preset, cron), cronOut),
            h('fieldset', {}, h('legend', {}, 'Safety and storage'),
                h('div', { class: 'criterion' }, h('span', {}, 'Stop if a run would delete more than'), safety, h('span', {}, 'rows from one table')),
                h('label', { class: 'checkbox' }, reclaim, 'Reclaim storage after pruning (rewrites the affected tables; see Storage reclaim)'),
                h('label', { class: 'checkbox' }, enabled, 'Enabled')),
            previewBtn, previewOut);

        return modal({
            title: isEdit ? `Edit rule: ${v.name}` : 'New pruning rule',
            body,
            actions: [{ label: 'Cancel', value: null }, { label: isEdit ? 'Save rule' : 'Create rule', value: 'save', primary: true }],
            wide: true,
        }).then(choice => (choice === 'save' ? read() : null));
    }

    window.addEventListener('hashchange', () => { if (!location.hash.startsWith('#/retention')) stopPolling(); });
})();
