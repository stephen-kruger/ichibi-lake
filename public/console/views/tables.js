/*
 * "Tables & Data" console section: table list, paged data grid with inline
 * editing, and schema viewing/editing. Loaded after app.js and uses its
 * helpers (h, api, icon, modal, toast, withBusy, field, pageHeader, ...).
 *
 * Routes:  #/tables                  table list
 *          #/tables/<name>           data grid
 *          #/tables/<name>/schema    schema editor
 */
'use strict';

(function () {
    const enc = encodeURIComponent;
    const OP_LABELS = {
        contains: 'contains', equals: 'equals', not_equals: 'does not equal', starts_with: 'starts with',
        gt: '>', gte: '≥', lt: '<', lte: '≤', is_null: 'is NULL', not_null: 'is not NULL',
    };
    const NO_VALUE_OPS = new Set(['is_null', 'not_null']);

    // Grid state survives switching between a table's Data and Schema tabs.
    const gridState = new Map();
    function stateFor(table) {
        if (!gridState.has(table)) gridState.set(table, { page: 1, pageSize: 50, sort: null, filters: [], search: '', selected: new Set() });
        return gridState.get(table);
    }

    let meta = { columnTypes: [], pageSizes: [25, 50, 100, 250, 500], filterOps: Object.keys(OP_LABELS) };

    registerView('tables', async (ctx) => {
        const [table, tab] = ctx.params;
        if (!table) return renderTableList(ctx);
        return renderTable(ctx, table, tab === 'schema' ? 'schema' : 'data');
    });

    // =========================================================================
    // Table list
    // =========================================================================

    async function renderTableList(ctx) {
        const res = await api('GET', '/data/tables');
        meta = { columnTypes: res.columnTypes, pageSizes: res.pageSizes, filterOps: res.filterOps };
        const filter = h('input', { type: 'text', placeholder: 'Filter tables…', class: 'toolbar-search' });
        const body = h('tbody');
        const draw = () => {
            const q = filter.value.trim().toLowerCase();
            const list = res.tables.filter(t => !q || t.name.toLowerCase().includes(q));
            body.replaceChildren(...(list.length ? list.map(t => h('tr', { class: 'clickable', onclick: () => ctx.navigate('tables', t.name) },
                h('td', {}, h('a', { href: `#/tables/${enc(t.name)}`, onclick: (e) => e.stopPropagation() }, t.name)),
                h('td', { class: 'num' }, t.rows === null ? '—' : t.rows.toLocaleString()),
                h('td', { class: 'num' }, t.columns),
                h('td', { class: 'actions' },
                    h('a', { class: 'btn small', href: `#/tables/${enc(t.name)}`, onclick: (e) => e.stopPropagation() }, 'Browse'),
                    h('a', { class: 'btn small', href: `#/tables/${enc(t.name)}/schema`, onclick: (e) => e.stopPropagation() }, 'Schema'))))
                : [h('tr', {}, h('td', { colspan: 4, class: 'empty' }, res.tables.length ? 'No tables match.' : 'No tables yet. Upload data through the API or create a table.'))]));
        };
        filter.addEventListener('input', draw);
        draw();

        const create = h('button', { class: 'btn primary', type: 'button', disabled: !ctx.isAdmin, onclick: () => createTableDialog(ctx) }, icon('plus'), 'New table');
        return [
            pageHeader('Tables & Data', 'Browse, edit and restructure DuckLake tables. System tables (prefixed "_") are managed on their own pages and hidden here.', create),
            h('div', { class: 'card' },
                h('div', { class: 'card-header' }, h('h2', {}, `${res.tables.length} table(s)`), filter),
                h('div', { class: 'card-body flush table-wrap' },
                    h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Table'), h('th', { class: 'num' }, 'Rows'), h('th', { class: 'num' }, 'Columns'), h('th', {}, ''))), body))),
        ];
    }

    async function createTableDialog(ctx) {
        const name = h('input', { type: 'text', placeholder: 'e.g. sensor_readings' });
        const rows = h('div', { class: 'col-editor' });
        const addRow = (preset = {}) => {
            const n = h('input', { type: 'text', placeholder: 'column_name', value: preset.name || '' });
            const t = typeSelect(preset.type || 'VARCHAR');
            const nullable = h('input', { type: 'checkbox', checked: preset.nullable !== false });
            const def = h('input', { type: 'text', placeholder: 'default (optional)' });
            const remove = h('button', { class: 'btn small danger', type: 'button', title: 'Remove column', onclick: () => row.remove() }, '×');
            const row = h('div', { class: 'col-editor-row' }, n, t, h('label', { class: 'checkbox' }, nullable, 'nullable'), def, remove);
            row._read = () => ({ name: n.value.trim(), type: t.value, nullable: nullable.checked, default: def.value });
            rows.appendChild(row);
        };
        addRow({ name: 'id', type: 'VARCHAR' });
        addRow({ name: 'created', type: 'TIMESTAMP' });
        addRow({ name: 'updated', type: 'TIMESTAMP' });
        const more = h('button', { class: 'btn small', type: 'button', onclick: () => addRow() }, icon('plus'), 'Add column');
        const ok = await modal({
            title: 'New table',
            body: h('div', { class: 'row-gap' },
                field('Table name', name, 'Letters, digits and underscores; must not start with "_".'),
                h('div', {}, h('label', { class: 'label' }, 'Columns'), rows, more),
                h('div', { class: 'help' }, 'created/updated TIMESTAMP columns are filled automatically by the ingestion API and by rows added here.')),
            actions: [{ label: 'Cancel', value: false }, { label: 'Create table', value: true, primary: true }],
            wide: true,
        });
        if (!ok) return;
        const columns = [...rows.children].map(r => r._read()).filter(c => c.name);
        await withBusy(null, async () => {
            await api('POST', '/data/tables', { name: name.value.trim(), columns });
            toast(`Table ${name.value.trim()} created`, 'success');
            ctx.navigate('tables', name.value.trim(), 'schema');
        });
    }

    function typeSelect(selected, options) {
        const opts = options || meta.columnTypes;
        const list = opts.includes(selected) ? opts : [selected, ...opts];
        return h('select', {}, list.map(t => h('option', { value: t, selected: t === selected }, t)));
    }

    // =========================================================================
    // Single table: header + tabs
    // =========================================================================

    async function renderTable(ctx, table, tab) {
        const schema = await api('GET', `/data/tables/${enc(table)}/schema`);
        if (schema.columnTypes) meta.columnTypes = schema.columnTypes;

        const actions = [];
        if (ctx.isAdmin) {
            actions.push(h('button', { class: 'btn', type: 'button', onclick: () => renameTableDialog(ctx, table, schema) }, icon('edit'), 'Rename'));
            actions.push(h('button', { class: 'btn danger', type: 'button', onclick: () => dropTableDialog(ctx, table, schema) }, icon('trash'), 'Drop'));
        }
        const header = pageHeader(table, `${schema.rows.toLocaleString()} row(s) · ${schema.columns.length} column(s)`, ...actions);
        header.querySelector('.page-title').prepend(h('div', { class: 'crumbs' }, h('a', { href: '#/tables' }, 'Tables'), ' / '));

        const tabs = h('div', { class: 'tabs', role: 'tablist' },
            h('a', { href: `#/tables/${enc(table)}`, class: tab === 'data' ? 'active' : '', role: 'tab' }, 'Data'),
            h('a', { href: `#/tables/${enc(table)}/schema`, class: tab === 'schema' ? 'active' : '', role: 'tab' }, 'Schema'));

        const graphWarning = schema.graphs.length
            ? h('div', { class: 'callout' }, `Referenced by property graph(s): ${schema.graphs.join(', ')}. Renaming or dropping this table, or renaming or dropping its columns, is blocked until those graphs are removed.`)
            : null;

        const content = tab === 'schema' ? renderSchema(ctx, table, schema) : await renderGrid(ctx, table, schema);
        return [header, tabs, graphWarning, content];
    }

    async function renameTableDialog(ctx, table) {
        const input = h('input', { type: 'text', value: table });
        const ok = await modal({
            title: `Rename ${table}`,
            body: h('div', { class: 'row-gap' }, field('New name', input),
                h('div', { class: 'callout' }, 'API clients using the old name will get "table not found". Table ACLs, RBAC table permissions and retention rules move to the new name.')),
            actions: [{ label: 'Cancel', value: false }, { label: 'Rename', value: true, primary: true }],
        });
        if (!ok || input.value.trim() === table) return;
        await withBusy(null, async () => {
            const res = await api('PATCH', `/data/tables/${enc(table)}`, { newName: input.value.trim() });
            gridState.delete(table);
            toast(`Renamed to ${res.table}`, 'success');
            ctx.navigate('tables', res.table);
        });
    }

    async function dropTableDialog(ctx, table, schema) {
        const input = h('input', { type: 'text', autocomplete: 'off', placeholder: table });
        const ok = await modal({
            title: `Drop ${table}?`,
            body: h('div', { class: 'row-gap' },
                h('div', { class: 'callout error' }, `This permanently deletes the table and all ${schema.rows.toLocaleString()} row(s). It cannot be undone.`),
                field(`Type ${table} to confirm`, input)),
            actions: [{ label: 'Cancel', value: false }, { label: 'Drop table', value: true, danger: true }],
        });
        if (!ok) return;
        if (input.value !== table) return toast('The name did not match; nothing was dropped.', 'error');
        await withBusy(null, async () => {
            await api('DELETE', `/data/tables/${enc(table)}`, { confirm: input.value });
            gridState.delete(table);
            toast(`Dropped ${table}`, 'success');
            ctx.navigate('tables');
        });
    }

    // =========================================================================
    // Data grid
    // =========================================================================

    async function renderGrid(ctx, table, schema) {
        const st = stateFor(table);
        const colInfo = new Map(schema.columns.map(c => [c.name, c]));
        const wrap = h('div', { class: 'card grid-card' });
        let current = null;   // last response from /rows/query

        // --- Toolbar ---------------------------------------------------------
        const search = h('input', { type: 'text', class: 'toolbar-search', placeholder: 'Search all columns…', value: st.search });
        let searchTimer = null;
        search.addEventListener('input', () => {
            clearTimeout(searchTimer);
            searchTimer = setTimeout(() => { st.search = search.value; st.page = 1; load(); }, 300);
        });

        const filterCol = h('select', {}, schema.columns.map(c => h('option', { value: c.name }, c.name)));
        const filterOp = h('select', {}, meta.filterOps.map(op => h('option', { value: op }, OP_LABELS[op] || op)));
        const filterVal = h('input', { type: 'text', placeholder: 'value' });
        filterOp.addEventListener('change', () => { filterVal.disabled = NO_VALUE_OPS.has(filterOp.value); });
        const addFilter = h('button', { class: 'btn small', type: 'submit' }, icon('filter'), 'Add filter');
        const filterForm = h('form', { class: 'filter-form', onsubmit: (e) => {
            e.preventDefault();
            st.filters.push({ column: filterCol.value, op: filterOp.value, value: NO_VALUE_OPS.has(filterOp.value) ? '' : filterVal.value });
            filterVal.value = '';
            st.page = 1;
            load();
        } }, filterCol, filterOp, filterVal, addFilter);
        const chips = h('div', { class: 'chips' });

        const refresh = h('button', { class: 'btn small', type: 'button', title: 'Reload', onclick: () => load() }, icon('refresh'));
        const addRow = h('button', { class: 'btn small primary', type: 'button', disabled: !ctx.isAdmin, onclick: () => insertRowDialog(table, schema, load) }, icon('plus'), 'Add row');
        const delSel = h('button', { class: 'btn small danger', type: 'button', disabled: true }, icon('trash'), 'Delete selected');
        delSel.addEventListener('click', async () => {
            const ids = [...st.selected];
            if (!ids.length) return;
            if (!(await confirmDialog('Delete rows', `Delete ${ids.length} selected row(s) from ${table}? This cannot be undone.`, { confirmLabel: 'Delete', danger: true }))) return;
            await withBusy(delSel, async () => {
                const res = await api('POST', `/data/tables/${enc(table)}/rows/delete`, { rowids: ids });
                st.selected.clear();
                toast(`Deleted ${res.deleted} row(s)`, 'success');
                load();
            });
        });

        const toolbar = h('div', { class: 'grid-toolbar' },
            h('div', { class: 'grid-toolbar-row' }, search, h('div', { class: 'spacer' }), refresh, ctx.isAdmin ? delSel : null, ctx.isAdmin ? addRow : null),
            h('div', { class: 'grid-toolbar-row' }, filterForm),
            chips);

        // --- Grid + pager ----------------------------------------------------
        const gridWrap = h('div', { class: 'grid-wrap' });
        const pager = h('div', { class: 'pager' });
        wrap.append(toolbar, gridWrap, pager);

        function drawChips() {
            chips.replaceChildren(...st.filters.map((f, i) => h('span', { class: 'chip' },
                `${f.column} ${OP_LABELS[f.op] || f.op}${NO_VALUE_OPS.has(f.op) ? '' : ` "${f.value}"`}`,
                h('button', { type: 'button', title: 'Remove filter', 'aria-label': 'Remove filter', onclick: () => { st.filters.splice(i, 1); st.page = 1; load(); } }, '×'))));
            if (st.filters.length > 1) chips.appendChild(h('button', { class: 'btn small', type: 'button', onclick: () => { st.filters = []; st.page = 1; load(); } }, 'Clear filters'));
        }

        function updateSelectionUi() {
            delSel.disabled = st.selected.size === 0;
            delSel.lastChild.textContent = st.selected.size ? `Delete selected (${st.selected.size})` : 'Delete selected';
        }

        async function load() {
            drawChips();
            gridWrap.classList.add('loading');
            try {
                current = await api('POST', `/data/tables/${enc(table)}/rows/query`, {
                    page: st.page, pageSize: st.pageSize, sort: st.sort, filters: st.filters, search: st.search,
                });
                st.page = current.page;
                // Drop selections that are no longer visible.
                const visible = new Set(current.rows.map(r => r.rowid));
                for (const id of st.selected) if (!visible.has(id)) st.selected.delete(id);
                drawGrid();
                drawPager();
            } catch (err) {
                gridWrap.replaceChildren(h('div', { class: 'callout error' }, err.message));
            } finally {
                gridWrap.classList.remove('loading');
                updateSelectionUi();
            }
        }

        function drawGrid() {
            const cols = current.columns;
            const allBox = h('input', { type: 'checkbox', 'aria-label': 'Select all rows on this page',
                checked: current.rows.length > 0 && current.rows.every(r => st.selected.has(r.rowid)) });
            allBox.addEventListener('change', () => {
                for (const r of current.rows) allBox.checked ? st.selected.add(r.rowid) : st.selected.delete(r.rowid);
                drawGrid(); updateSelectionUi();
            });
            const head = h('tr', {},
                ctx.isAdmin ? h('th', { class: 'sel' }, allBox) : null,
                h('th', { class: 'rownum' }, '#'),
                cols.map(c => {
                    const sorted = st.sort && st.sort.column === c.name ? st.sort.dir : null;
                    return h('th', { class: `sortable ${sorted ? 'sorted' : ''}`, title: `Sort by ${c.name}`, onclick: () => {
                        st.sort = !sorted ? { column: c.name, dir: 'asc' } : sorted === 'asc' ? { column: c.name, dir: 'desc' } : null;
                        st.page = 1; load();
                    } },
                    h('div', { class: 'th-name' }, c.name, sorted ? h('span', { class: 'sort-ind' }, sorted === 'asc' ? ' ▲' : ' ▼') : null),
                    h('div', { class: 'th-type' }, c.type));
                }));

            const offset = (current.page - 1) * current.pageSize;
            const body = current.rows.map((row, ri) => {
                const box = h('input', { type: 'checkbox', checked: st.selected.has(row.rowid), 'aria-label': `Select row ${offset + ri + 1}` });
                box.addEventListener('change', () => { box.checked ? st.selected.add(row.rowid) : st.selected.delete(row.rowid); updateSelectionUi(); });
                return h('tr', { class: st.selected.has(row.rowid) ? 'selected' : '' },
                    ctx.isAdmin ? h('td', { class: 'sel' }, box) : null,
                    h('td', { class: 'rownum' }, offset + ri + 1),
                    cols.map((c, ci) => dataCell(row, c, ci)));
            });

            gridWrap.replaceChildren(current.rows.length
                ? h('table', { class: 'grid' }, h('thead', {}, head), h('tbody', {}, body))
                : h('div', { class: 'empty' }, current.total === 0 && !st.filters.length && !st.search ? 'This table is empty.' : 'No rows match the current search and filters.'));
        }

        function cellContent(value, truncated) {
            if (value === null) return h('span', { class: 'null' }, 'NULL');
            return truncated ? [value, h('span', { class: 'muted' }, '…')] : value;
        }

        function dataCell(row, col, ci) {
            const value = row.values[ci];
            const truncated = row.truncated.includes(ci);
            const editable = ctx.isAdmin && col.editable && !truncated;
            const td = h('td', {
                class: `cell ${editable ? 'editable' : ''}`,
                title: truncated ? 'Value truncated for display; too long to edit here.' : (value === null ? 'NULL' : value),
                tabindex: editable ? '0' : undefined,
            }, cellContent(value, truncated));
            if (editable) {
                const start = () => startEdit(td, row, col, ci);
                td.addEventListener('dblclick', start);
                td.addEventListener('keydown', (e) => { if (e.key === 'Enter' || e.key === 'F2') { e.preventDefault(); start(); } });
            }
            return td;
        }

        function startEdit(td, row, col, ci) {
            if (td.classList.contains('editing')) return;
            const original = row.values[ci];
            const info = colInfo.get(col.name);
            const isBool = /^BOOLEAN$/i.test(col.type);
            let input;
            if (isBool) {
                input = h('select', {},
                    h('option', { value: 'true', selected: original === 'true' }, 'true'),
                    h('option', { value: 'false', selected: original === 'false' }, 'false'),
                    info?.nullable !== false ? h('option', { value: '__NULL__', selected: original === null }, 'NULL') : null);
            } else {
                input = h('input', { type: 'text', value: original ?? '', placeholder: original === null ? 'NULL' : '' });
            }
            const nullBtn = !isBool && info?.nullable !== false
                ? h('button', { class: 'btn small', type: 'button', title: 'Set to NULL', onmousedown: (e) => e.preventDefault(), onclick: () => save({ setNull: true }) }, 'NULL')
                : null;
            td.classList.add('editing');
            td.replaceChildren(h('div', { class: 'cell-editor' }, input, nullBtn));
            input.focus();
            if (input.select) input.select();

            let done = false;
            const cancel = () => {
                if (done) return; done = true;
                td.classList.remove('editing');
                td.replaceChildren(cellContent(row.values[ci], false));
                td.focus();
            };
            async function save({ setNull = false, next = false } = {}) {
                if (done) return;
                let isNull = setNull;
                let value = input.value;
                if (isBool && value === '__NULL__') isNull = true;
                if (!isNull && value === (original ?? '') && original !== null) { cancel(); if (next) focusNext(td); return; }
                done = true;
                input.disabled = true;
                try {
                    const res = await api('PATCH', `/data/tables/${enc(table)}/rows/${row.rowid}`, { column: col.name, value, isNull, original });
                    row.values[ci] = res.value;
                    td.classList.remove('editing');
                    td.replaceChildren(cellContent(res.value, false));
                    td.title = res.value === null ? 'NULL' : res.value;
                    td.classList.add('saved');
                    setTimeout(() => td.classList.remove('saved'), 1200);
                    if (next) focusNext(td); else td.focus();
                } catch (err) {
                    done = false;
                    input.disabled = false;
                    input.focus();
                    toast(err.message, 'error');
                    if (err.status === 409) {
                        const reload = await confirmDialog('Row changed', `${err.message}`, { confirmLabel: 'Reload page' });
                        if (reload) load();
                    }
                }
            }
            input.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') { e.preventDefault(); save(); }
                else if (e.key === 'Escape') { e.preventDefault(); cancel(); }
                else if (e.key === 'Tab') { e.preventDefault(); save({ next: true }); }
            });
            input.addEventListener('blur', () => { setTimeout(() => { if (!done && document.activeElement !== input) save(); }, 0); });
            if (isBool) input.addEventListener('change', () => save());
        }

        function focusNext(td) {
            let n = td.nextElementSibling;
            while (n && !n.classList.contains('editable')) n = n.nextElementSibling;
            if (n) { n.focus(); n.dispatchEvent(new MouseEvent('dblclick')); }
        }

        function drawPager() {
            const { total, page, pages, pageSize } = current;
            const from = total === 0 ? 0 : (page - 1) * pageSize + 1;
            const to = Math.min(total, page * pageSize);
            const go = (p) => { st.page = p; load(); };
            const pageInput = h('input', { type: 'number', min: 1, max: pages, value: page, class: 'page-input', 'aria-label': 'Page' });
            pageInput.addEventListener('change', () => go(Math.min(pages, Math.max(1, parseInt(pageInput.value, 10) || 1))));
            const sizeSel = h('select', { 'aria-label': 'Rows per page' }, meta.pageSizes.map(n => h('option', { value: n, selected: n === pageSize }, `${n} / page`)));
            sizeSel.addEventListener('change', () => { st.pageSize = Number(sizeSel.value); st.page = 1; load(); });
            const btn = (label, p, disabled, title) => h('button', { class: 'btn small', type: 'button', disabled, title, onclick: () => go(p) }, label);
            pager.replaceChildren(
                h('div', { class: 'muted' }, total === 0 ? '0 rows' : `${from.toLocaleString()}–${to.toLocaleString()} of ${total.toLocaleString()}`),
                h('div', { class: 'spacer' }),
                sizeSel,
                btn('«', 1, page <= 1, 'First page'),
                btn('‹', page - 1, page <= 1, 'Previous page'),
                h('span', { class: 'page-of' }, 'Page ', pageInput, ` of ${pages.toLocaleString()}`),
                btn('›', page + 1, page >= pages, 'Next page'),
                btn('»', pages, page >= pages, 'Last page'));
        }

        await load();
        return [ctx.isAdmin ? null : h('div', { class: 'callout info' }, 'You have read-only access.'), wrap,
            ctx.isAdmin ? h('div', { class: 'muted hint-line' }, 'Double-click a cell (or press Enter) to edit. Enter saves, Tab saves and moves right, Esc cancels.') : null];
    }

    async function insertRowDialog(table, schema, reload) {
        const inputs = new Map();
        const fields = schema.columns.map(c => {
            const auto = /^(created|updated)$/.test(c.name) && /^TIMESTAMP/i.test(c.type);
            let input;
            if (!c.editable) {
                input = h('input', { type: 'text', disabled: true, placeholder: `${c.type}: not settable here` });
            } else if (/^BOOLEAN$/i.test(c.type)) {
                input = h('select', {}, h('option', { value: '' }, '(NULL / default)'), h('option', { value: 'true' }, 'true'), h('option', { value: 'false' }, 'false'));
            } else {
                input = h('input', { type: 'text', placeholder: auto ? 'now (automatic)' : (c.default !== null ? `default: ${c.default}` : 'NULL') });
            }
            if (c.editable) inputs.set(c.name, input);
            return field(`${c.name}`, input, `${c.type}${c.nullable ? '' : ' · required'}`);
        });
        const ok = await modal({
            title: `Add row to ${table}`,
            body: h('div', { class: 'row-gap form-scroll' }, h('div', { class: 'help' }, 'Leave a field empty for NULL or the column default.'), fields),
            actions: [{ label: 'Cancel', value: false }, { label: 'Add row', value: true, primary: true }],
            wide: true,
        });
        if (!ok) return;
        const values = {};
        for (const [name, input] of inputs) if (input.value !== '') values[name] = { value: input.value };
        await withBusy(null, async () => {
            await api('POST', `/data/tables/${enc(table)}/rows`, { values });
            toast('Row added', 'success');
            reload();
        });
    }

    // =========================================================================
    // Schema editor
    // =========================================================================

    function renderSchema(ctx, table, schema) {
        const rows = schema.columns.map((c, i) => h('tr', {},
            h('td', { class: 'rownum' }, i + 1),
            h('td', { class: 'mono' }, c.name),
            h('td', { class: 'mono' }, c.type),
            h('td', {}, c.nullable ? h('span', { class: 'muted' }, 'nullable') : h('span', { class: 'badge' }, 'NOT NULL')),
            h('td', { class: 'mono' }, c.default === null ? h('span', { class: 'muted' }, '—') : c.default),
            h('td', {}, c.editable ? h('span', { class: 'muted' }, 'yes') : h('span', { class: 'muted', title: 'Complex and binary types are shown read-only in the grid' }, 'read-only')),
            h('td', { class: 'actions' },
                h('button', { class: 'btn small', type: 'button', disabled: !ctx.isAdmin, onclick: () => editColumnDialog(ctx, table, c) }, 'Edit'),
                h('button', { class: 'btn small danger', type: 'button', disabled: !ctx.isAdmin || schema.columns.length === 1, onclick: () => dropColumnDialog(ctx, table, c) }, 'Drop'))));

        const add = h('button', { class: 'btn primary small', type: 'button', disabled: !ctx.isAdmin, onclick: () => addColumnDialog(ctx, table) }, icon('plus'), 'Add column');
        return h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Columns'), add),
            h('div', { class: 'card-body flush table-wrap' },
                h('table', {}, h('thead', {}, h('tr', {}, h('th', { class: 'rownum' }, '#'), h('th', {}, 'Name'), h('th', {}, 'Type'), h('th', {}, 'Null'), h('th', {}, 'Default'), h('th', {}, 'Inline edit'), h('th', {}, ''))),
                    h('tbody', {}, rows))),
            h('div', { class: 'card-body' }, h('div', { class: 'help' },
                'Type changes: widening changes (e.g. INTEGER → BIGINT, FLOAT → DOUBLE) apply in place. Any other change converts the column\'s values in one transaction and moves the column to the end of the table.')));
    }

    async function addColumnDialog(ctx, table) {
        const name = h('input', { type: 'text', placeholder: 'column_name' });
        const type = typeSelect('VARCHAR');
        const nullable = h('input', { type: 'checkbox', checked: true });
        const def = h('input', { type: 'text', placeholder: 'optional' });
        const ok = await modal({
            title: `Add column to ${table}`,
            body: h('div', {}, field('Name', name), field('Type', type), field('Default value', def, 'Existing rows get this value (or NULL when empty).'),
                h('label', { class: 'checkbox' }, nullable, 'Allow NULL')),
            actions: [{ label: 'Cancel', value: false }, { label: 'Add column', value: true, primary: true }],
        });
        if (!ok) return;
        await withBusy(null, async () => {
            await api('POST', `/data/tables/${enc(table)}/columns`, { name: name.value.trim(), type: type.value, nullable: nullable.checked, default: def.value });
            toast(`Column ${name.value.trim()} added`, 'success');
            ctx.reload();
        });
    }

    async function editColumnDialog(ctx, table, col) {
        const name = h('input', { type: 'text', value: col.name });

        // Every type is selectable: widenings apply in place, anything else is
        // converted by rewriting the column (see the note shown below).
        const types = [col.type, ...meta.columnTypes.filter(t => t !== col.type)];
        const label = (t) => t === col.type ? `${t} (current)` : col.widenTo.includes(t) ? `${t} (in place)` : `${t} (convert)`;
        const type = h('select', {}, types.map(t => h('option', { value: t, selected: t === col.type }, label(t))));

        const nullable = h('input', { type: 'checkbox', checked: col.nullable });
        const def = h('input', { type: 'text', value: col.default ?? '', placeholder: 'none' });
        const lenient = h('input', { type: 'checkbox' });
        const previewOut = h('div', { class: 'muted' });
        const checkBtn = h('button', { class: 'btn small', type: 'button' }, 'Check values');
        checkBtn.addEventListener('click', () => withBusy(checkBtn, async () => {
            const r = await api('POST', `/data/tables/${enc(table)}/columns/${enc(col.name)}/type-preview`, { type: type.value });
            previewOut.className = r.invalid ? 'callout error' : 'callout info';
            previewOut.textContent = r.invalid
                ? `${r.invalid.toLocaleString()} of ${r.nonNull.toLocaleString()} value(s) cannot be converted to ${r.to}, e.g. ${r.samples.map(v => JSON.stringify(v)).join(', ')}.`
                : `All ${r.nonNull.toLocaleString()} non-NULL value(s) convert to ${r.to}.`;
        }));
        const convertPanel = h('div', { class: 'callout convert-panel' },
            h('div', {}, h('strong', {}, 'This rewrites the column. '),
                'Values are converted in one transaction, so nothing changes if it fails. The column moves to the end of the table and its default is removed; NOT NULL is kept. API clients that rely on the old type may break.'),
            h('div', { class: 'convert-actions' }, checkBtn, h('label', { class: 'checkbox' }, lenient, 'Set values that cannot be converted to NULL')),
            previewOut);
        const typeHelp = h('div', { class: 'help' });
        const syncType = () => {
            const t = type.value;
            const converting = t !== col.type && !col.widenTo.includes(t);
            convertPanel.hidden = !converting;
            previewOut.textContent = ''; previewOut.className = 'muted';
            typeHelp.textContent = t === col.type ? '' : converting ? '' : 'Widening change: applied in place, data and position unchanged.';
        };
        type.addEventListener('change', syncType);
        syncType();

        const ok = await modal({
            title: `Edit column ${col.name}`,
            body: h('div', {},
                field('Name', name, 'Renaming breaks API clients and queries that use the old name.'),
                h('div', { class: 'field' }, h('label', {}, 'Type', type), typeHelp),
                convertPanel,
                field('Default value', def, 'Leave empty to remove the default.'),
                h('label', { class: 'checkbox' }, nullable, 'Allow NULL')),
            actions: [{ label: 'Cancel', value: false }, { label: 'Save changes', value: true, primary: true }],
            wide: true,
        });
        if (!ok) return;
        const changes = {};
        if (name.value.trim() !== col.name) changes.newName = name.value.trim();
        if (type.value !== col.type) {
            changes.type = type.value;
            if (lenient.checked) changes.onInvalid = 'null';
        }
        if (nullable.checked !== col.nullable) changes.nullable = nullable.checked;
        if (def.value !== (col.default ?? '')) changes.default = def.value === '' ? null : def.value;
        if (!Object.keys(changes).length) return;
        await withBusy(null, async () => {
            const res = await api('PATCH', `/data/tables/${enc(table)}/columns/${enc(col.name)}`, changes);
            toast(res.applied.length ? `Updated: ${res.applied.join(', ')}` : 'No changes', 'success');
            ctx.reload();
        });
    }

    async function dropColumnDialog(ctx, table, col) {
        const ok = await confirmDialog(`Drop column ${col.name}?`,
            `This permanently deletes the "${col.name}" column and its data from ${table}. It cannot be undone.`, { confirmLabel: 'Drop column', danger: true });
        if (!ok) return;
        await withBusy(null, async () => {
            await api('DELETE', `/data/tables/${enc(table)}/columns/${enc(col.name)}`);
            toast(`Column ${col.name} dropped`, 'success');
            ctx.reload();
        });
    }
})();
