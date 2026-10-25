/*
 * ichibi-lake admin console (vanilla JS, no build step).
 *
 * Adding a page: declare the section in src/console/registry.js. Sections with
 * kind "settings" render automatically from their settings metadata; for kind
 * "custom", register a view here:
 *
 *     registerView('my-section', async (ctx) => {
 *         // ctx.section, ctx.user, ctx.isAdmin, ctx.settings(sectionId), ctx.reload()
 *         return h('div', {}, 'Hello');   // returns a Node (or array of Nodes)
 *     });
 *
 * All DOM is built with h() / textContent; never assign untrusted data to
 * innerHTML (the page also runs under a strict Content-Security-Policy).
 */
'use strict';

// ---------------------------------------------------------------------------
// DOM helpers
// ---------------------------------------------------------------------------

function h(tag, attrs, ...children) {
    const el = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) {
        if (v === undefined || v === null || v === false) continue;
        if (k === 'class') el.className = v;
        else if (k === 'dataset') Object.assign(el.dataset, v);
        else if (k.startsWith('on') && typeof v === 'function') el.addEventListener(k.slice(2), v);
        else if (k === 'value') el.value = v;
        else if (k === 'checked' || k === 'disabled' || k === 'readOnly' || k === 'selected') el[k] = !!v;
        else el.setAttribute(k, v === true ? '' : String(v));
    }
    appendChildren(el, children);
    return el;
}

function appendChildren(el, children) {
    for (const c of children.flat(Infinity)) {
        if (c === null || c === undefined || c === false) continue;
        el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
    }
}

const SVG_NS = 'http://www.w3.org/2000/svg';
const ICONS = {
    dashboard: 'M3 13h8V3H3v10zm0 8h8v-6H3v6zm10 0h8V11h-8v10zm0-18v6h8V3h-8z',
    key: 'M12.65 10A6 6 0 1 0 12.65 14H17v4h4v-4h2v-4H12.65zM7 14a2 2 0 1 1 0-4 2 2 0 0 1 0 4z',
    users: 'M16 11c1.66 0 3-1.34 3-3s-1.34-3-3-3-3 1.34-3 3 1.34 3 3 3zm-8 0c1.66 0 3-1.34 3-3S9.66 5 8 5 5 6.34 5 8s1.34 3 3 3zm0 2c-2.33 0-7 1.17-7 3.5V19h14v-2.5C15 14.17 10.33 13 8 13zm8 0c-.29 0-.62.02-.97.05 1.16.84 1.97 1.97 1.97 3.45V19h6v-2.5c0-2.33-4.67-3.5-7-3.5z',
    lock: 'M18 8h-1V6A5 5 0 0 0 7 6v2H6a2 2 0 0 0-2 2v10a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V10a2 2 0 0 0-2-2zM9 6a3 3 0 0 1 6 0v2H9V6zm3 11a2 2 0 1 1 0-4 2 2 0 0 1 0 4z',
    calendar: 'M19 4h-1V2h-2v2H8V2H6v2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V6a2 2 0 0 0-2-2zm0 16H5V9h14v11zM7 11h5v5H7z',
    shield: 'M12 1 3 5v6c0 5.55 3.84 10.74 9 12 5.16-1.26 9-6.45 9-12V5l-9-4zm-2 16-4-4 1.41-1.41L10 14.17l6.59-6.59L18 9l-8 8z',
    upload: 'M5 20h14v-2H5v2zm0-10h4v6h6v-6h4l-7-7-7 7z',
    download: 'M19 9h-4V3H9v6H5l7 7 7-7zM5 18v2h14v-2H5z',
    search: 'M15.5 14h-.79l-.28-.27A6.47 6.47 0 0 0 16 9.5 6.5 6.5 0 1 0 9.5 16c1.61 0 3.09-.59 4.23-1.57l.27.28v.79l5 4.99L20.49 19l-4.99-5zm-6 0C7.01 14 5 11.99 5 9.5S7.01 5 9.5 5 14 7.01 14 9.5 11.99 14 9.5 14z',
    database: 'M12 2C7.58 2 4 3.79 4 6v12c0 2.21 3.58 4 8 4s8-1.79 8-4V6c0-2.21-3.58-4-8-4zm0 2c3.87 0 6 1.5 6 2s-2.13 2-6 2-6-1.5-6-2 2.13-2 6-2zm6 14c0 .5-2.13 2-6 2s-6-1.5-6-2v-2.23C7.61 16.55 9.72 17 12 17s4.39-.45 6-1.23V18zm0-5c0 .5-2.13 2-6 2s-6-1.5-6-2v-2.23C7.61 11.55 9.72 12 12 12s4.39-.45 6-1.23V13z',
    stream: 'M3 6h18v2H3V6zm0 5h12v2H3v-2zm0 5h18v2H3v-2z',
    list: 'M3 13h2v-2H3v2zm0 4h2v-2H3v2zm0-8h2V7H3v2zm4 4h14v-2H7v2zm0 4h14v-2H7v2zM7 7v2h14V7H7z',
    user: 'M12 12a4 4 0 1 0 0-8 4 4 0 0 0 0 8zm0 2c-2.67 0-8 1.34-8 4v2h16v-2c0-2.66-5.33-4-8-4z',
    logout: 'M10.09 15.59 11.5 17l5-5-5-5-1.41 1.41L12.67 11H3v2h9.67l-2.58 2.59zM19 3H5a2 2 0 0 0-2 2v4h2V5h14v14H5v-4H3v4a2 2 0 0 0 2 2h14a2 2 0 0 0 2-2V5a2 2 0 0 0-2-2z',
    menu: 'M3 18h18v-2H3v2zm0-5h18v-2H3v2zm0-7v2h18V6H3z',
    plus: 'M19 13h-6v6h-2v-6H5v-2h6V5h2v6h6v2z',
    table: 'M3 3h18a1 1 0 0 1 1 1v16a1 1 0 0 1-1 1H3a1 1 0 0 1-1-1V4a1 1 0 0 1 1-1zm1 5v4h7V8H4zm9 0v4h7V8h-7zm-9 6v5h7v-5H4zm9 0v5h7v-5h-7z',
    trash: 'M6 19a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2V7H6v12zM19 4h-3.5l-1-1h-5l-1 1H5v2h14V4z',
    edit: 'M3 17.25V21h3.75L17.81 9.94l-3.75-3.75L3 17.25zM20.71 7.04a1 1 0 0 0 0-1.41l-2.34-2.34a1 1 0 0 0-1.41 0l-1.83 1.83 3.75 3.75 1.83-1.83z',
    filter: 'M10 18h4v-2h-4v2zM3 6v2h18V6H3zm3 7h12v-2H6v2z',
    refresh: 'M17.65 6.35A7.96 7.96 0 0 0 12 4a8 8 0 1 0 7.73 10h-2.08A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35z',
};

function icon(name) {
    const svg = document.createElementNS(SVG_NS, 'svg');
    svg.setAttribute('viewBox', '0 0 24 24');
    svg.setAttribute('fill', 'currentColor');
    svg.setAttribute('aria-hidden', 'true');
    const path = document.createElementNS(SVG_NS, 'path');
    path.setAttribute('d', ICONS[name] || ICONS.list);
    svg.appendChild(path);
    return svg;
}

function fmtDate(value) {
    if (!value) return '—';
    const d = new Date(String(value).replace(' ', 'T') + (/[zZ]|[+-]\d\d:?\d\d$/.test(value) ? '' : 'Z'));
    return Number.isNaN(d.getTime()) ? String(value) : d.toLocaleString();
}

function fmtDuration(seconds) {
    const d = Math.floor(seconds / 86400), hr = Math.floor((seconds % 86400) / 3600), m = Math.floor((seconds % 3600) / 60);
    return d ? `${d}d ${hr}h` : hr ? `${hr}h ${m}m` : `${m}m`;
}

// ---------------------------------------------------------------------------
// API client
// ---------------------------------------------------------------------------

class ApiError extends Error {
    constructor(status, message) { super(message); this.status = status; }
}

async function api(method, path, body) {
    const res = await fetch(`api${path}`, {
        method,
        credentials: 'same-origin',
        headers: { 'Content-Type': 'application/json', 'X-Console-Request': '1' },
        body: body === undefined ? undefined : JSON.stringify(body),
    });
    let data = {};
    try { data = await res.json(); } catch { /* empty body */ }
    if (res.status === 401 && path !== '/login' && path !== '/state' && state.user) {
        state.user = null;
        toast('Your session has ended. Please sign in again.', 'error');
        render();
    }
    if (!res.ok) throw new ApiError(res.status, data.error || `Request failed (${res.status})`);
    return data;
}

// ---------------------------------------------------------------------------
// Feedback: toasts and modals
// ---------------------------------------------------------------------------

function toast(message, kind = '') {
    const el = h('div', { class: `toast ${kind}`, role: 'status' }, message);
    document.getElementById('toasts').appendChild(el);
    setTimeout(() => el.remove(), kind === 'error' ? 6000 : 3500);
}

function modal({ title, body, actions, wide = false }) {
    return new Promise((resolve) => {
        const close = (value) => { backdrop.remove(); document.removeEventListener('keydown', onKey); resolve(value); };
        const onKey = (e) => { if (e.key === 'Escape') close(null); };
        const footer = (actions || [{ label: 'Close', value: null, primary: true }]).map(a =>
            h('button', { class: `btn ${a.primary ? 'primary' : ''} ${a.danger ? 'danger' : ''}`, type: 'button', onclick: () => close(a.value) }, a.label));
        const backdrop = h('div', { class: 'modal-backdrop', onclick: (e) => { if (e.target === backdrop) close(null); } },
            h('div', { class: `modal ${wide ? 'wide' : ''}`, role: 'dialog', 'aria-modal': 'true' },
                h('div', { class: 'modal-header' }, h('h2', {}, title)),
                h('div', { class: 'modal-body' }, body),
                h('div', { class: 'modal-footer' }, footer)));
        document.addEventListener('keydown', onKey);
        document.body.appendChild(backdrop);
        backdrop.querySelector('.modal-footer .primary, .modal-footer .btn')?.focus();
    });
}

function confirmDialog(title, message, { confirmLabel = 'Confirm', danger = false } = {}) {
    return modal({
        title,
        body: h('p', {}, message),
        actions: [{ label: 'Cancel', value: false }, { label: confirmLabel, value: true, primary: !danger, danger }],
    });
}

function showSecretOnce(title, secret, note) {
    const copyBtn = h('button', { class: 'btn small', type: 'button', onclick: async () => {
        try { await navigator.clipboard.writeText(secret); toast('Copied to clipboard', 'success'); }
        catch { toast('Copy failed; select the text manually', 'error'); }
    } }, 'Copy');
    return modal({
        title,
        body: h('div', { class: 'row-gap' },
            h('div', { class: 'callout' }, note || 'Copy this value now. It will not be shown again.'),
            h('div', { class: 'secret-box' }, secret),
            h('div', { class: 'right' }, copyBtn)),
        actions: [{ label: 'Done', value: true, primary: true }],
    });
}

/** Wrap an async action: disables the button, shows errors as toasts. */
async function withBusy(button, fn) {
    if (button) button.disabled = true;
    try { return await fn(); }
    catch (err) { toast(err.message || String(err), 'error'); return undefined; }
    finally { if (button) button.disabled = false; }
}

// ---------------------------------------------------------------------------
// App state and routing
// ---------------------------------------------------------------------------

const state = { user: null, setupRequired: false, version: '', sections: [], settings: [] };
const views = new Map();

function registerView(id, render) { views.set(id, render); }

/** Hash route segments, e.g. #/tables/events/schema -> ['tables', 'events', 'schema']. */
function routeParts() {
    return location.hash.replace(/^#\/?/, '').split('/').filter(Boolean).map(p => {
        try { return decodeURIComponent(p); } catch { return p; }
    });
}

function currentSectionId() {
    const id = routeParts()[0];
    return state.sections.some(s => s.id === id) ? id : (state.sections[0]?.id || 'overview');
}

function navigate(...parts) {
    location.hash = '#/' + parts.map(p => encodeURIComponent(p)).join('/');
}

async function loadSections() {
    const data = await api('GET', '/sections');
    state.sections = data.sections;
    state.settings = data.settings;
}

const app = () => document.getElementById('app');

async function render() {
    const root = app();
    root.className = '';
    root.replaceChildren();
    if (state.setupRequired) return root.appendChild(renderSetup());
    if (!state.user) return root.appendChild(renderLogin());
    await loadSections();
    root.appendChild(renderShell());
    await renderSection();
}

// ---------------------------------------------------------------------------
// Auth screens
// ---------------------------------------------------------------------------

function authCard(title, subtitle, form) {
    return h('div', { class: 'auth' },
        h('div', { class: 'auth-card' },
            h('img', { src: 'logo-320.png', alt: 'ichibi-lake', class: 'logo' }),
            h('h1', {}, title),
            h('p', { class: 'subtitle' }, subtitle),
            form));
}

function field(label, input, help) {
    return h('div', { class: 'field' }, h('label', {}, label, input), help ? h('div', { class: 'help' }, help) : null);
}

function renderSetup() {
    const code = h('input', { type: 'text', autocomplete: 'one-time-code', required: true, placeholder: 'XXXX-XXXX-XXXX' });
    const username = h('input', { type: 'text', autocomplete: 'username', required: true, value: 'admin' });
    const password = h('input', { type: 'password', autocomplete: 'new-password', required: true, minlength: 10 });
    const confirm = h('input', { type: 'password', autocomplete: 'new-password', required: true });
    const submit = h('button', { class: 'btn primary block', type: 'submit' }, 'Create admin account');
    const form = h('form', { onsubmit: (e) => {
        e.preventDefault();
        if (password.value !== confirm.value) return toast('Passwords do not match', 'error');
        withBusy(submit, async () => {
            const res = await api('POST', '/setup', { setupCode: code.value.trim(), username: username.value.trim(), password: password.value });
            state.setupRequired = false;
            state.user = res.user;
            toast('Welcome! Your admin account is ready.', 'success');
            location.hash = '#/overview';
            await render();
        });
    } },
    h('div', { class: 'callout info' }, 'First-time setup. The setup code is printed in the gateway log when it starts (or is the value of CONSOLE_SETUP_TOKEN).'),
    field('Setup code', code),
    field('Admin username', username),
    field('Password', password, 'At least 10 characters.'),
    field('Confirm password', confirm),
    submit);
    return authCard('Set up the admin console', `ichibi-lake gateway v${state.version}`, form);
}

function renderLogin() {
    const username = h('input', { type: 'text', autocomplete: 'username', required: true });
    const password = h('input', { type: 'password', autocomplete: 'current-password', required: true });
    const submit = h('button', { class: 'btn primary block', type: 'submit' }, 'Sign in');
    const form = h('form', { onsubmit: (e) => {
        e.preventDefault();
        withBusy(submit, async () => {
            const res = await api('POST', '/login', { username: username.value.trim(), password: password.value });
            state.user = res.user;
            await render();
        });
    } }, field('Username', username), field('Password', password), submit);
    setTimeout(() => username.focus(), 0);
    return authCard('Admin console', 'Sign in to manage this gateway', form);
}

// ---------------------------------------------------------------------------
// Shell: sidebar + main area
// ---------------------------------------------------------------------------

let mainEl = null;
let shellEl = null;

function renderShell() {
    const groups = [];
    for (const s of state.sections) {
        let g = groups.find(x => x.name === s.group);
        if (!g) groups.push(g = { name: s.group, items: [] });
        g.items.push(s);
    }
    const nav = h('nav', { class: 'nav' }, groups.map(g =>
        h('div', { class: 'nav-group' },
            h('div', { class: 'nav-group-title' }, g.name),
            g.items.map(s => h('a', { href: `#/${s.id}`, dataset: { id: s.id } }, icon(s.icon), s.title)))));

    const logout = h('button', { class: 'icon-btn', title: 'Sign out', 'aria-label': 'Sign out', onclick: async () => {
        await api('POST', '/logout').catch(() => {});
        state.user = null;
        render();
    } }, icon('logout'));

    // On narrow screens the sidebar is a drawer; tapping the page closes it.
    mainEl = h('main', { class: 'main', onclick: (e) => {
        if (shellEl.classList.contains('menu-open') && !e.target.closest('.menu-toggle')) shellEl.classList.remove('menu-open');
    } });
    shellEl = h('div', { class: 'shell' },
        h('aside', { class: 'sidebar' },
            h('div', { class: 'brand' },
                h('img', { src: 'logo-320.png', alt: '' }),
                h('div', {}, h('div', { class: 'name' }, 'ichibi-lake'), h('div', { class: 'tag' }, `Admin console · v${state.version}`))),
            nav,
            h('div', { class: 'sidebar-footer' },
                h('div', { class: 'avatar' }, state.user.username.slice(0, 1)),
                h('div', { class: 'who' }, h('div', {}, state.user.username), h('div', {}, state.user.role)),
                logout)),
        mainEl);
    return shellEl;
}

function pageHeader(title, description, ...actions) {
    const menu = h('button', { class: 'btn menu-toggle', type: 'button', 'aria-label': 'Menu', onclick: () => shellEl.classList.toggle('menu-open') }, icon('menu'));
    return h('div', { class: 'page-header' },
        menu,
        h('div', { class: 'page-title' }, h('h1', {}, title), description ? h('p', {}, description) : null),
        actions.length ? h('div', { class: 'page-actions' }, actions) : null);
}

async function renderSection() {
    if (!mainEl) return;
    const id = currentSectionId();
    const section = state.sections.find(s => s.id === id);
    shellEl.classList.remove('menu-open');
    for (const a of shellEl.querySelectorAll('.nav a')) a.classList.toggle('active', a.dataset.id === id);
    document.title = `${section.title} · ichibi-lake Console`;

    const route = location.hash;
    const ctx = {
        section,
        params: routeParts().slice(1),
        navigate,
        user: state.user,
        isAdmin: state.user.role === 'admin',
        settings: (sectionId) => state.settings.filter(s => s.section === sectionId),
        reload: () => renderSection(),
    };
    const renderer = views.get(id) || (section.kind === 'settings' ? renderSettingsSection : null);
    mainEl.replaceChildren(h('div', { class: 'muted' }, 'Loading…'));
    try {
        const content = renderer ? await renderer(ctx) : h('div', { class: 'empty' }, 'This section has no view yet.');
        if (location.hash !== route) return; // navigated away meanwhile
        mainEl.replaceChildren();
        appendChildren(mainEl, [content]);
    } catch (err) {
        if (err.status === 401) return;
        mainEl.replaceChildren(pageHeader(section.title), h('div', { class: 'callout error' }, err.message || String(err)));
    }
}

window.addEventListener('hashchange', () => renderSection());

// ---------------------------------------------------------------------------
// Generic settings renderer (sections with kind: "settings")
// ---------------------------------------------------------------------------

function sourceBadge(setting) {
    if (setting.source === 'console') return h('span', { class: 'badge console', title: `Set in console by ${setting.updatedBy || '?'} at ${fmtDate(setting.updatedAt)}` }, 'console override');
    if (setting.source === 'env') return h('span', { class: 'badge env' }, 'docker/.env');
    return h('span', { class: 'badge' }, 'default');
}

function settingControl(setting, isAdmin, onSaved) {
    const editable = setting.editable && isAdmin;
    let input;
    const getValue = () => {
        if (setting.type === 'boolean') return input.querySelector('input').checked;
        return input.value;
    };

    if (setting.type === 'boolean') {
        const checked = String(setting.value).toLowerCase() === 'true' || setting.value === '1';
        input = h('label', { class: 'switch' },
            h('input', { type: 'checkbox', checked, disabled: !editable }), h('span', {}));
    } else if (setting.type === 'enum') {
        input = h('select', { disabled: !editable }, setting.options.map(o => h('option', { value: o, selected: o === setting.value }, o)));
    } else {
        input = h('input', {
            type: setting.type === 'number' ? 'number' : 'text',
            value: setting.value,
            readOnly: !editable,
            min: setting.min ?? undefined,
            max: setting.max ?? undefined,
        });
    }

    const row = h('div', { class: 'control-row' }, input);
    if (editable) {
        const save = h('button', { class: 'btn primary small', type: 'button' }, 'Save');
        save.addEventListener('click', () => withBusy(save, async () => {
            const res = await api('PUT', `/settings/${setting.key}`, { value: getValue() });
            Object.assign(setting, res.setting);
            toast(`${setting.label} saved`, 'success');
            onSaved();
        }));
        row.appendChild(save);
        if (setting.source === 'console') {
            const reset = h('button', { class: 'btn small', type: 'button', title: 'Remove the console override' }, 'Reset');
            reset.addEventListener('click', () => withBusy(reset, async () => {
                const res = await api('DELETE', `/settings/${setting.key}`);
                Object.assign(setting, res.setting);
                toast(`${setting.label} reset`, 'success');
                onSaved();
            }));
            row.appendChild(reset);
        }
    }

    const meta = [];
    meta.push(sourceBadge(setting));
    if (setting.editable) meta.push(' ', h('span', { class: 'badge live' }, 'applies immediately'));
    else meta.push(' ', h('span', { class: 'muted' }, 'Read-only here: change in docker/.env and restart.'));
    if (setting.source === 'console' && setting.envValue !== null) meta.push(h('div', {}, `docker/.env value: ${setting.envValue || '(empty)'}`));

    return h('div', { class: 'control' }, row, h('div', { class: 'meta' }, meta));
}

function settingRows(settings, isAdmin, rerender) {
    return settings.map(s => h('div', { class: 'setting' },
        h('div', {},
            h('div', { class: 'title' }, s.label),
            h('div', { class: 'key' }, s.key),
            s.help ? h('div', { class: 'desc' }, s.help) : null),
        settingControl(s, isAdmin, rerender)));
}

function renderSettingsSection(ctx) {
    const settings = ctx.settings(ctx.section.id);
    const editable = settings.filter(s => s.editable);
    const readOnly = settings.filter(s => !s.editable);
    const parts = [pageHeader(ctx.section.title,
        'Effective configuration for this gateway. Console overrides are stored in DuckLake and shared by every instance.')];
    if (!ctx.isAdmin && editable.length) parts.push(h('div', { class: 'callout info' }, 'You have read-only access. Ask an admin to change settings.'));
    if (editable.length) {
        parts.push(h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Editable settings')),
            h('div', { class: 'card-body flush' }, settingRows(editable, ctx.isAdmin, ctx.reload))));
    }
    if (readOnly.length) {
        parts.push(h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Environment'), h('span', { class: 'hint' }, 'Set in docker/.env; restart required')),
            h('div', { class: 'card-body flush' }, settingRows(readOnly, ctx.isAdmin, ctx.reload))));
    }
    return parts;
}

// ---------------------------------------------------------------------------
// Overview
// ---------------------------------------------------------------------------

function stat(label, value, sub, tone = 'accent') {
    return h('div', { class: `stat ${tone}` }, h('div', { class: 'label' }, label), h('div', { class: 'value' }, value), sub ? h('div', { class: 'sub' }, sub) : null);
}

function fmtBytes(n) {
    if (!Number.isFinite(n)) return '—';
    const units = ['B', 'KB', 'MB', 'GB', 'TB'];
    let i = 0;
    while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
    return `${n.toFixed(i ? 1 : 0)} ${units[i]}`;
}

function checkBadge(value) {
    const ok = value === 'ok' || value === 'ready';
    return h('span', { class: `badge ${ok ? 'ok' : value === 'starting' ? 'warn' : 'off'}`, title: ok ? '' : value }, ok ? value : value.split(':')[0]);
}

function healthCard(hl) {
    const uq = hl.userQuery;
    const exportCounts = Object.entries(hl.exports.counters || {});
    const exportsDone = exportCounts.filter(([k]) => k.endsWith(':succeeded')).reduce((n, [, v]) => n + v, 0);
    const exportsFailed = exportCounts.filter(([k]) => /:(failed|timeout)$/.test(k)).reduce((n, [, v]) => n + v, 0);
    const exportsRejected = exportCounts.filter(([k]) => k.endsWith(':rejected')).reduce((n, [, v]) => n + v, 0);
    const lag = [...(hl.kafkaLag || [])].sort((a, b) => b.lag - a.lag);
    const totalLag = lag.reduce((n, p) => n + p.lag, 0);
    const warn = (cond, text) => (cond ? h('span', { class: 'badge warn' }, text) : null);
    const problems = Object.entries(hl.checks).filter(([, v]) => v !== 'ok' && v !== 'ready');
    return h('div', { class: 'card' },
        h('div', { class: 'card-header' }, h('h2', {}, 'Health & load'),
            h('span', { class: `badge ${hl.ready ? 'ok' : 'off'}` }, hl.ready ? 'ready' : 'not ready')),
        h('div', { class: 'card-body' },
            problems.length ? h('div', { class: 'callout error' }, problems.map(([k, v]) => h('div', {}, `${k}: ${v}`))) : null,
            h('dl', { class: 'kv' },
                h('dt', {}, 'Readiness checks'), h('dd', { class: 'badges' },
                    ...Object.entries(hl.checks).map(([k, v]) => h('span', { class: 'check' }, `${k} `, checkBadge(v)))),
                h('dt', {}, 'User SQL queries'), h('dd', {},
                    `${uq.active} running · ${uq.queued} queued (limit ${uq.concurrency}) `,
                    warn(uq.queued > 0, 'queueing'),
                    h('div', { class: 'muted' }, `since start: ${uq.succeeded} ok · ${uq.failed} failed · ${uq.timedOut} timed out · ${uq.cancelled} cancelled `,
                        warn(uq.timedOut > 0, 'timeouts'))),
                h('dt', {}, 'Table exports'), h('dd', {},
                    `${hl.exports.active} running (limit ${hl.exports.concurrency})`,
                    h('div', { class: 'muted' }, `since start: ${exportsDone} completed · ${exportsFailed} failed · ${exportsRejected} rejected (busy)`)),
                h('dt', {}, 'Lake storage'), h('dd', {}, hl.lake
                    ? `${hl.lake.files.toLocaleString()} Parquet data file(s) · ${fmtBytes(hl.lake.bytes)}`
                    : h('span', { class: 'badge off' }, 'catalog query failed')),
                h('dt', {}, 'Kafka consumer lag'), h('dd', {}, lag.length
                    ? [`${totalLag.toLocaleString()} message(s) behind `, warn(totalLag > 0, 'lagging'),
                        h('div', { class: 'muted' }, lag.slice(0, 5).map(p => `${p.topic}[${p.partition}]: ${p.lag.toLocaleString()}`).join(' · ') + (lag.length > 5 ? ` · +${lag.length - 5} more` : ''))]
                    : h('span', { class: 'muted' }, 'no batches consumed since start')),
            )));
}

registerView('overview', async () => {
    const o = await api('GET', '/overview');
    const refresh = h('button', { class: 'btn', type: 'button', onclick: () => renderSection() }, icon('refresh'), 'Refresh');
    return [
        pageHeader('Overview', 'Health and configuration at a glance.', refresh),
        h('div', { class: 'stats' },
            stat('User tables', o.data.userTables, 'in the DuckLake catalog'),
            stat('API keys', o.access.apiKeys, `${o.access.rbacUsers} RBAC user(s) · ${o.access.rbacMode} mode`, 'ochre'),
            stat('Retention rules', o.retention.rules, `${o.retention.enabled} enabled`, 'olive'),
            stat('Console logins', o.access.consoleLogins, `${o.settings.overridden} setting override(s)`),
        ),
        healthCard(o.health),
        h('div', { class: 'card' },
            h('div', { class: 'card-header' }, h('h2', {}, 'Gateway')),
            h('div', { class: 'card-body' },
                h('dl', { class: 'kv' },
                    h('dt', {}, 'Version'), h('dd', {}, `v${o.gateway.version}`),
                    h('dt', {}, 'Uptime'), h('dd', {}, fmtDuration(o.gateway.uptimeSeconds)),
                    h('dt', {}, 'Host'), h('dd', {}, o.gateway.hostname),
                    h('dt', {}, 'Node.js'), h('dd', {}, o.gateway.node),
                    h('dt', {}, 'DuckDB'), h('dd', {}, o.duckdb.version),
                    h('dt', {}, 'DuckPGQ (graphs)'), h('dd', {}, h('span', { class: `badge ${o.duckdb.duckpgqLoaded ? 'ok' : 'off'}` }, o.duckdb.duckpgqLoaded ? 'loaded' : 'not loaded')),
                    h('dt', {}, 'Kafka brokers'), h('dd', {}, o.kafka.brokers || h('span', { class: 'muted' }, 'not configured')),
                ))),
    ];
});

// ---------------------------------------------------------------------------
// API keys
// ---------------------------------------------------------------------------

function revertButton(ctx) {
    const btn = h('button', { class: 'btn small', type: 'button' }, 'Revert to docker/.env');
    btn.addEventListener('click', async () => {
        if (!(await confirmDialog('Revert API keys', 'Discard the console list and use API_KEYS from docker/.env again? Keys that exist only in the console list will stop working.', { confirmLabel: 'Revert', danger: true }))) return;
        await withBusy(btn, async () => {
            await api('POST', '/api-keys/reset');
            toast('API keys reverted to docker/.env', 'success');
            ctx.reload();
        });
    });
    return btn;
}

registerView('api-keys', async (ctx) => {
    const data = await api('GET', '/api-keys');
    const add = h('button', { class: 'btn primary', type: 'button', disabled: !ctx.isAdmin }, icon('plus'), 'Generate key');
    add.addEventListener('click', () => withBusy(add, async () => {
        const res = await api('POST', '/api-keys', {});
        await showSecretOnce('New API key', res.key, 'Send this key in the x-api-key header. Copy it now; it will not be shown again.');
        ctx.reload();
    }));

    const rows = data.keys.map(k => {
        const revoke = h('button', { class: 'btn small danger', type: 'button', disabled: !ctx.isAdmin }, 'Revoke');
        revoke.addEventListener('click', async () => {
            const ok = await confirmDialog('Revoke API key', `Revoke ${k.masked}? Clients using it will immediately get HTTP 401.`, { confirmLabel: 'Revoke', danger: true });
            if (!ok) return;
            await withBusy(revoke, async () => {
                await api('DELETE', `/api-keys/${k.id}`);
                toast('API key revoked', 'success');
                ctx.reload();
            });
        });
        return h('tr', {}, h('td', { class: 'mono' }, k.masked), h('td', {}, k.rbacName || h('span', { class: 'muted' }, '—')), h('td', { class: 'actions' }, revoke));
    });

    return [
        pageHeader('API Keys', 'Keys accepted in the x-api-key header (the API_KEYS list). Changes apply immediately on every instance.', add),
        data.source === 'console'
            ? h('div', { class: 'callout' }, 'This list is a console override of API_KEYS in docker/.env. ',
                ctx.isAdmin ? revertButton(ctx) : null)
            : h('div', { class: 'callout info' },
                'These keys currently come from docker/.env. The first change made here becomes a console override that replaces the API_KEYS value.'),
        data.rbacMode ? h('div', { class: 'callout' }, 'RBAC is configured via RBAC_SUPERUSERS / RBAC_USERS. Manage role-based users on the RBAC Users page; API_KEYS remain valid as a fallback.') : null,
        h('div', { class: 'card' },
            h('div', { class: 'card-body flush table-wrap' },
                rows.length
                    ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Key'), h('th', {}, 'RBAC name'), h('th', {}, ''))), h('tbody', {}, rows))
                    : h('div', { class: 'empty' }, 'No API keys configured.'))),
    ];
});

// ---------------------------------------------------------------------------
// RBAC users
// ---------------------------------------------------------------------------

const RBAC_ROLES = ['superuser', 'writer', 'reader'];

registerView('rbac-users', async (ctx) => {
    const data = await api('GET', '/rbac/users');

    const name = h('input', { type: 'text', placeholder: 'e.g. ingestion-service', required: true });
    const email = h('input', { type: 'email', placeholder: 'optional' });
    const role = h('select', {}, RBAC_ROLES.map(r => h('option', { value: r, selected: r === 'reader' }, r)));
    const create = h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Create user');
    const form = h('form', { class: 'inline-form', onsubmit: (e) => {
        e.preventDefault();
        withBusy(create, async () => {
            const res = await api('POST', '/rbac/users', { name: name.value.trim(), email: email.value.trim(), roles: [role.value] });
            await showSecretOnce('New user API key', res.apiKey, `API key for "${name.value.trim()}". Copy it now; it will not be shown again.`);
            ctx.reload();
        });
    } }, field('Name', name), field('Email', email), field('Role', role), h('div', { class: 'field action' }, create));

    const MANAGED = {
        env: { badge: 'docker/.env', title: 'Defined by RBAC_SUPERUSERS / RBAC_USERS in docker/.env and re-applied on every restart. Change it there.' },
        'api-keys': { badge: 'API Keys page', title: 'Comes from the API_KEYS list and is re-applied on every restart. Revoke the key on the API Keys page.' },
    };
    const rows = data.users.map(u => {
        const managed = MANAGED[u.managedBy] || null;
        const locked = !ctx.isAdmin || !!managed;
        const roleSelect = h('select', { disabled: locked, title: managed ? managed.title : undefined },
            h('option', { value: '', selected: u.roles.length === 0 }, '(none)'),
            RBAC_ROLES.map(r => h('option', { value: r, selected: u.roles[0] === r }, r)));
        roleSelect.addEventListener('change', () => withBusy(roleSelect, async () => {
            await api('PATCH', `/rbac/users/${u.id}`, { roles: roleSelect.value ? [roleSelect.value] : [] });
            toast('Role updated', 'success');
        }));
        const toggle = h('button', { class: `btn small ${u.active ? 'danger' : ''}`, type: 'button', disabled: locked, title: managed ? managed.title : undefined }, u.active ? 'Deactivate' : 'Activate');
        toggle.addEventListener('click', () => withBusy(toggle, async () => {
            await api('PATCH', `/rbac/users/${u.id}`, { active: !u.active });
            toast(u.active ? 'User deactivated' : 'User activated', 'success');
            ctx.reload();
        }));
        return h('tr', {},
            h('td', {}, h('div', {}, u.name || h('span', { class: 'muted' }, '(unnamed)')), h('div', { class: 'mono muted' }, u.masked)),
            h('td', {}, roleSelect, u.roles.length > 1 ? h('div', { class: 'muted' }, `also: ${u.roles.slice(1).join(', ')}`) : null),
            h('td', {}, h('span', { class: `badge ${u.active ? 'ok' : 'off'}` }, u.active ? 'active' : 'inactive')),
            h('td', {}, managed
                ? (u.managedBy === 'api-keys' ? h('a', { href: '#/api-keys', class: 'badge env', title: managed.title }, managed.badge) : h('span', { class: 'badge env', title: managed.title }, managed.badge))
                : h('span', { class: 'muted' }, 'console')),
            h('td', { class: 'actions' }, toggle));
    });

    return [
        pageHeader('RBAC Users', 'Role-based API users. Superusers can read and write every table; writers and readers follow per-table ACLs. Users defined in docker/.env or the API Keys list are re-applied on every restart, so they are read-only here.'),
        ctx.isAdmin ? h('div', { class: 'card' }, h('div', { class: 'card-header' }, h('h2', {}, 'New user')), h('div', { class: 'card-body' }, form)) : null,
        h('div', { class: 'card' },
            h('div', { class: 'card-body flush table-wrap' },
                rows.length
                    ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'User'), h('th', {}, 'Role'), h('th', {}, 'Status'), h('th', {}, 'Managed in'), h('th', {}, ''))), h('tbody', {}, rows))
                    : h('div', { class: 'empty' }, 'No RBAC users yet.'))),
    ];
});

// ---------------------------------------------------------------------------
// Console logins
// ---------------------------------------------------------------------------

registerView('logins', async (ctx) => {
    const data = await api('GET', '/logins');

    const username = h('input', { type: 'text', required: true, autocomplete: 'off' });
    const password = h('input', { type: 'password', required: true, autocomplete: 'new-password', minlength: 10 });
    const role = h('select', {}, data.roles.map(r => h('option', { value: r }, r)));
    const create = h('button', { class: 'btn primary', type: 'submit' }, icon('plus'), 'Add login');
    const form = h('form', { class: 'inline-form', onsubmit: (e) => {
        e.preventDefault();
        withBusy(create, async () => {
            await api('POST', '/logins', { username: username.value.trim(), password: password.value, role: role.value });
            toast(`Login ${username.value.trim()} created`, 'success');
            ctx.reload();
        });
    } }, field('Username', username), field('Password', password), field('Role', role), h('div', { class: 'field action' }, create));

    const rows = data.users.map(u => {
        const self = u.username.toLowerCase() === ctx.user.username.toLowerCase();
        const roleSelect = h('select', { disabled: !ctx.isAdmin }, data.roles.map(r => h('option', { value: r, selected: r === u.role }, r)));
        roleSelect.addEventListener('change', () => withBusy(roleSelect, async () => {
            try {
                await api('PATCH', `/logins/${encodeURIComponent(u.username)}`, { role: roleSelect.value });
                toast('Role updated', 'success');
            } finally { ctx.reload(); }
        }));
        const resetPw = h('button', { class: 'btn small', type: 'button', disabled: !ctx.isAdmin }, 'Reset password');
        resetPw.addEventListener('click', async () => {
            const input = h('input', { type: 'password', autocomplete: 'new-password', minlength: 10 });
            const ok = await modal({ title: `Reset password for ${u.username}`, body: field('New password', input, 'At least 10 characters. Their existing sessions will end.'),
                actions: [{ label: 'Cancel', value: false }, { label: 'Reset password', value: true, primary: true }] });
            if (!ok) return;
            await withBusy(resetPw, async () => {
                await api('PATCH', `/logins/${encodeURIComponent(u.username)}`, { password: input.value });
                toast('Password reset', 'success');
            });
        });
        const del = h('button', { class: 'btn small danger', type: 'button', disabled: !ctx.isAdmin || self }, 'Delete');
        del.addEventListener('click', async () => {
            if (!(await confirmDialog('Delete login', `Delete the console login "${u.username}"?`, { confirmLabel: 'Delete', danger: true }))) return;
            await withBusy(del, async () => {
                await api('DELETE', `/logins/${encodeURIComponent(u.username)}`);
                toast('Login deleted', 'success');
                ctx.reload();
            });
        });
        return h('tr', {},
            h('td', {}, u.username, self ? h('span', { class: 'muted' }, ' (you)') : null),
            h('td', {}, roleSelect),
            h('td', { class: 'muted' }, fmtDate(u.last_login)),
            h('td', { class: 'muted' }, u.created_by || '—'),
            h('td', { class: 'actions' }, resetPw, del));
    });

    return [
        pageHeader('Console Logins', 'People who can sign in to this console. Admins can change settings; viewers have read-only access.'),
        ctx.isAdmin ? h('div', { class: 'card' }, h('div', { class: 'card-header' }, h('h2', {}, 'Add a login')), h('div', { class: 'card-body' }, form)) : null,
        h('div', { class: 'card' },
            h('div', { class: 'card-body flush table-wrap' },
                h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Username'), h('th', {}, 'Role'), h('th', {}, 'Last sign-in'), h('th', {}, 'Created by'), h('th', {}, ''))), h('tbody', {}, rows)))),
    ];
});

// ---------------------------------------------------------------------------
// Audit log
// ---------------------------------------------------------------------------

registerView('audit', async () => {
    const data = await api('GET', '/audit?limit=300');
    const rows = data.entries.map(e => h('tr', {},
        h('td', { class: 'muted' }, fmtDate(e.ts)),
        h('td', {}, e.username || '—'),
        h('td', { class: 'mono' }, e.action),
        h('td', {}, e.detail || ''),
        h('td', { class: 'muted mono' }, e.ip || '')));
    return [
        pageHeader('Audit Log', 'Recent console activity across all instances (newest first).',
            h('button', { class: 'btn', type: 'button', onclick: () => renderSection() }, icon('refresh'), 'Refresh')),
        h('div', { class: 'card' },
            h('div', { class: 'card-body flush table-wrap' },
                rows.length
                    ? h('table', {}, h('thead', {}, h('tr', {}, h('th', {}, 'Time'), h('th', {}, 'User'), h('th', {}, 'Action'), h('th', {}, 'Detail'), h('th', {}, 'IP'))), h('tbody', {}, rows))
                    : h('div', { class: 'empty' }, 'No activity recorded yet.'))),
    ];
});

// ---------------------------------------------------------------------------
// My account
// ---------------------------------------------------------------------------

registerView('account', async (ctx) => {
    const current = h('input', { type: 'password', autocomplete: 'current-password', required: true });
    const next = h('input', { type: 'password', autocomplete: 'new-password', required: true, minlength: 10 });
    const confirm = h('input', { type: 'password', autocomplete: 'new-password', required: true });
    const save = h('button', { class: 'btn primary', type: 'submit' }, 'Change password');
    const form = h('form', { onsubmit: (e) => {
        e.preventDefault();
        if (next.value !== confirm.value) return toast('New passwords do not match', 'error');
        withBusy(save, async () => {
            await api('POST', '/me/password', { currentPassword: current.value, newPassword: next.value });
            form.reset();
            toast('Password changed. Other sessions have been signed out.', 'success');
        });
    } }, field('Current password', current), field('New password', next, 'At least 10 characters.'), field('Confirm new password', confirm), save);

    return [
        pageHeader('My Account', `Signed in as ${ctx.user.username} (${ctx.user.role}).`),
        h('div', { class: 'card' }, h('div', { class: 'card-header' }, h('h2', {}, 'Change password')), h('div', { class: 'card-body' }, form)),
    ];
});

// ---------------------------------------------------------------------------
// Boot
// ---------------------------------------------------------------------------

(async function boot() {
    try {
        const s = await api('GET', '/state');
        state.setupRequired = s.setupRequired;
        state.user = s.user;
        state.version = s.version;
    } catch (err) {
        app().replaceChildren(h('div', { class: 'auth' }, h('div', { class: 'auth-card' },
            h('img', { src: 'logo-320.png', alt: 'ichibi-lake', class: 'logo' }),
            h('div', { class: 'callout error' }, err.message || 'Unable to reach the gateway.'),
            h('button', { class: 'btn block', type: 'button', onclick: () => location.reload() }, 'Retry'))));
        return;
    }
    render();
})();
