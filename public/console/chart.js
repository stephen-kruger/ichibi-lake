/*
 * Charts for query results in the console (Ask AI and Saved Functions).
 *
 * A chart is a small spec { type, x, y: [columns], series, title } over a
 * result's columns: chosen by the model (the chart tool in src/console/ai.js),
 * stored with a saved function, picked here by autoChart() when there is none,
 * or changed by the person with the controls above the chart. Charts are drawn
 * as inline SVG with no library and no inline styles, so they work under the
 * console's Content-Security-Policy; colours are the --series-N tokens in
 * styles.css (validated categorical palette, light and dark steps).
 *
 * Loaded after app.js (uses h()). The parts without DOM (columnKinds,
 * autoChart, resolveChart, fixSpec, chartData, niceTicks, timeTicks) are
 * unit-tested in test/chart.test.js.
 */
'use strict';

(function (root) {
    const SVG_NS = 'http://www.w3.org/2000/svg';
    const TYPES = [['bar', 'Bar'], ['line', 'Line'], ['area', 'Area'], ['pie', 'Pie'], ['scatter', 'Scatter']];
    const MAX_SERIES = 8;          // palette slots; more series fold into "Other"
    const MAX_SCATTER_SERIES = 3;  // the palette's first three slots stay distinct all-pairs
    const MAX_PIE_SLICES = 8;
    const MAX_BARS = 60;           // categories drawn by a bar chart
    const SAMPLE_ROWS = 500;       // rows looked at to classify a column
    const CHAR_W = 6.5;            // rough width of an 11px label character
    const OTHER = 'Other';

    const NUM_RE = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][-+]?\d+)?$/;
    const DATE_RE = /^\d{4}-\d{2}(?:-\d{2}(?:[ T]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)?(?:Z|[+-]\d{2}(?::?\d{2})?)?)?$/;
    const PERIOD_RE = /^(?:year|quarter|month|week|day|hour|weekday|day_of_week|dow|yr)$/i;
    const ID_RE = /^id$|_id$|^id_/i;
    const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];

    // --- Values ---------------------------------------------------------------------

    /** Number for a numeric cell (DECIMAL and HUGEINT arrive as strings), else null. */
    function toNum(v) {
        if (typeof v === 'number') return Number.isFinite(v) ? v : null;
        if (typeof v === 'string' && NUM_RE.test(v.trim())) return Number(v);
        return null;
    }

    /** Epoch ms for a DATE/TIMESTAMP cell (ISO-like string, UTC unless it says otherwise), else NaN. */
    function toTime(v) {
        if (typeof v !== 'string') return NaN;
        let s = v.trim();
        if (!DATE_RE.test(s)) return NaN;
        if (/^\d{4}-\d{2}$/.test(s)) s += '-01';
        s = s.replace(' ', 'T');
        if (!s.includes('T')) return Date.parse(s);  // date only: UTC midnight
        if (/T[\d:.]+[+-]\d{2}$/.test(s)) s += ':00';
        else if (/T[\d:.]+[+-]\d{4}$/.test(s)) s = `${s.slice(0, -2)}:${s.slice(-2)}`;
        else if (!/(Z|[+-]\d{2}:\d{2})$/.test(s)) s += 'Z';
        return Date.parse(s);
    }

    function keyOf(v) {
        return v !== null && typeof v === 'object' ? JSON.stringify(v) : String(v);
    }

    // --- Choosing a chart -----------------------------------------------------------

    /** Per column: { name, kind: number|time|text|empty, distinct (capped at 51), period, idLike }. */
    function columnKinds(columns, rows) {
        return columns.map((name, i) => {
            let n = 0, nums = 0, ints = 0, times = 0;
            const seen = new Set();
            for (const row of rows) {
                const v = row[i];
                if (v === null || v === undefined || v === '') continue;
                n++;
                if (seen.size <= 50) seen.add(keyOf(v));
                const num = toNum(v);
                if (num !== null) { nums++; if (Number.isInteger(num)) ints++; }
                else if (!Number.isNaN(toTime(v))) times++;
                if (n >= SAMPLE_ROWS) break;
            }
            const kind = !n ? 'empty' : nums === n ? 'number' : times === n ? 'time' : 'text';
            return { name, kind, distinct: seen.size, period: kind === 'number' && ints === n && PERIOD_RE.test(name), idLike: ID_RE.test(name) };
        });
    }

    /** A sensible chart for the result, or null when a table says it better. */
    function autoChart(columns, rows) {
        if (!columns?.length || !rows || rows.length < 2) return null;
        const cols = columnKinds(columns, rows);
        const time = cols.find(c => c.kind === 'time') || cols.find(c => c.period);
        const measures = cols.filter(c => c.kind === 'number' && !c.idLike && c !== time);
        if (!measures.length) return null;
        const texts = cols.filter(c => c.kind === 'text');
        const splitter = (exclude) => texts.find(c => c !== exclude && c.distinct > 1 && c.distinct <= MAX_SERIES);
        if (time) {
            const series = measures.length === 1 ? splitter(null) : null;
            return { type: 'line', x: time.name, y: series ? [measures[0].name] : measures.slice(0, 4).map(c => c.name), series: series?.name || null, title: '' };
        }
        if (texts.length) {
            const x = texts[0];
            // Long format (category, group, value): the second text column splits it.
            const series = measures.length === 1 && x.distinct < rows.length ? splitter(x) : null;
            return { type: 'bar', x: x.name, y: series ? [measures[0].name] : measures.slice(0, 3).map(c => c.name), series: series?.name || null, title: '' };
        }
        if (measures.length >= 2) return { type: 'scatter', x: measures[0].name, y: [measures[1].name], series: null, title: '' };
        return null;
    }

    /** Spec made consistent (pie: one y; series: one y; no column twice), or null. */
    function fixSpec(spec) {
        if (!spec || !TYPES.some(([t]) => t === spec.type) || !spec.x) return null;
        const series = spec.type === 'pie' || !spec.series || spec.series === spec.x ? null : spec.series;
        let y = [...new Set((Array.isArray(spec.y) ? spec.y : [spec.y]).filter(c => c && c !== spec.x && c !== series))];
        if (spec.type === 'pie' || series) y = y.slice(0, 1);
        if (!y.length) return null;
        return { type: spec.type, x: spec.x, y, series, title: spec.title || '' };
    }

    /** The given spec when its columns are all in the result, else the automatic one. */
    function resolveChart(spec, columns, rows) {
        const fixed = fixSpec(spec);
        if (fixed && [fixed.x, ...fixed.y, fixed.series].filter(Boolean).every(c => columns.includes(c))) return fixed;
        return autoChart(columns, rows);
    }

    // --- Shaping the data -----------------------------------------------------------

    function foldSeries(series, cap) {
        if (series.length <= cap) return series;
        const total = (s) => s.values.reduce((a, v) => a + Math.abs(v || 0), 0);
        const keep = new Set([...series].sort((a, b) => total(b) - total(a)).slice(0, cap - 1));
        const other = { name: OTHER, other: true, values: series[0].values.map(() => null) };
        for (const s of series) {
            if (keep.has(s)) continue;
            s.values.forEach((v, i) => { if (v !== null) other.values[i] = (other.values[i] || 0) + v; });
        }
        return [...series.filter(s => keep.has(s)), other];
    }

    /**
     * Drawable data for a spec: { type, xKind (category|time|number), xs: [{ key, pos, label }],
     * series: [{ name, values (per x), other }], points (scatter), note }.
     */
    function chartData(spec, columns, rows) {
        const ix = (c) => columns.indexOf(c);
        const xi = ix(spec.x), si = spec.series ? ix(spec.series) : -1, yis = spec.y.map(ix);
        const xCol = columnKinds([spec.x], rows.map(r => [r[xi]]))[0];
        const notes = [];

        if (spec.type === 'scatter') {
            const label = columns.findIndex((c, i) => i !== xi && !yis.includes(i) && i !== si);
            const groups = new Map();
            const points = [];
            for (const row of rows) {
                const x = toNum(row[xi]);
                if (x === null) continue;
                spec.y.forEach((yName, j) => {
                    const y = toNum(row[yis[j]]);
                    if (y === null) return;
                    const group = si >= 0 ? (row[si] === null ? '(null)' : keyOf(row[si])) : yName;
                    if (!groups.has(group)) groups.set(group, groups.size);
                    points.push({ x, y, group, label: label >= 0 && row[label] !== null ? keyOf(row[label]) : null, yName });
                });
            }
            let names = [...groups.keys()];
            if (names.length > MAX_SCATTER_SERIES) {
                const kept = new Set(names.slice(0, MAX_SCATTER_SERIES - 1));
                for (const p of points) if (!kept.has(p.group)) p.group = OTHER;
                names = [...kept, OTHER];
            }
            return { type: 'scatter', xKind: 'number', points, series: names.map(n => ({ name: n, other: n === OTHER })), note: '' };
        }

        const continuous = spec.type === 'line' || spec.type === 'area';
        const xKind = continuous && xCol.kind === 'time' ? 'time' : continuous && xCol.kind === 'number' ? 'number' : 'category';
        const xs = [];
        const xIndex = new Map();
        const byName = new Map();
        const add = (name, k, v) => {
            let s = byName.get(name);
            if (!s) { s = { name, values: [] }; byName.set(name, s); }
            if (v !== null) s.values[k] = (s.values[k] || 0) + v;
        };
        let dateOnly = true;
        for (const row of rows) {
            const xv = row[xi];
            if (xv === null || xv === undefined) continue;
            let pos = null;
            if (xKind === 'time') pos = toTime(xv);
            else if (xKind === 'number') pos = toNum(xv);
            if (xKind !== 'category' && (pos === null || Number.isNaN(pos))) continue;
            if (xCol.kind === 'time' && toTime(xv) % DAY) dateOnly = false;
            const key = xKind === 'category' ? keyOf(xv) : pos;
            let k = xIndex.get(key);
            if (k === undefined) {
                k = xs.length;
                xIndex.set(key, k);
                xs.push({ key, pos, label: xKind === 'category' ? keyOf(xv) : null });
            }
            if (si >= 0) add(row[si] === null ? '(null)' : keyOf(row[si]), k, toNum(row[yis[0]]));
            else spec.y.forEach((y, j) => add(y, k, toNum(row[yis[j]])));
        }
        let series = [...byName.values()].map(s => ({ name: s.name, values: xs.map((_, k) => (s.values[k] === undefined ? null : s.values[k])) }));
        let order = xs.map((_, k) => k);
        if (xKind !== 'category') order.sort((a, b) => xs[a].pos - xs[b].pos);
        for (const x of xs) {
            if (xCol.kind === 'time') x.label = fmtTime(xKind === 'time' ? x.pos : toTime(x.key), dateOnly);
            else if (xKind === 'number') x.label = fmtFull(x.pos);
        }

        if (spec.type === 'pie') {
            const vals = series[0]?.values || [];
            const dropped = order.filter(k => !(vals[k] > 0)).length;
            order = order.filter(k => vals[k] > 0).sort((a, b) => vals[b] - vals[a]);
            if (dropped) notes.push(`${dropped} zero, negative or empty value(s) left out`);
            if (order.length > MAX_PIE_SLICES) {
                const rest = order.slice(MAX_PIE_SLICES - 1);
                const otherVal = rest.reduce((a, k) => a + vals[k], 0);
                notes.push(`${rest.length} smallest grouped as "${OTHER}"`);
                order = order.slice(0, MAX_PIE_SLICES - 1);
                xs.push({ key: OTHER, label: OTHER, other: true });
                vals.push(otherVal);
                order.push(xs.length - 1);
            }
            series = series.slice(0, 1).map(s => ({ ...s, values: vals }));
        } else if (spec.type === 'bar' && order.length > MAX_BARS) {
            notes.push(`First ${MAX_BARS} of ${order.length} categories shown`);
            order = order.slice(0, MAX_BARS);
        }
        const outXs = order.map(k => xs[k]);
        series = series.map(s => ({ ...s, values: order.map(k => s.values[k]) }));
        if (series.length > MAX_SERIES) notes.push(`${series.length - MAX_SERIES + 1} smaller series grouped as "${OTHER}"`);
        series = foldSeries(series, MAX_SERIES);
        return { type: spec.type, xKind, xIsTime: xCol.kind === 'time' || PERIOD_RE.test(spec.x), xs: outXs, series, note: notes.join(' · ') };
    }

    // --- Scales and formats ---------------------------------------------------------

    function niceStep(span, count) {
        const raw = span / Math.max(1, count);
        const p = 10 ** Math.floor(Math.log10(raw));
        const f = raw / p;
        return (f < 1.5 ? 1 : f < 3 ? 2 : f < 7 ? 5 : 10) * p;
    }

    /** Round axis bounds and ticks covering [min, max]. */
    function niceTicks(min, max, count = 5) {
        if (!Number.isFinite(min) || !Number.isFinite(max)) { min = 0; max = 1; }
        if (min === max) {
            const d = min === 0 ? 1 : Math.abs(min) * 0.1;
            if (min >= 0 && min - d < 0) min = 0; else min -= d;
            max += d;
        }
        const step = niceStep(max - min, count);
        const lo = Math.floor(min / step) * step, hi = Math.ceil(max / step) * step;
        const ticks = [];
        for (let v = lo; v <= hi + step / 2; v += step) ticks.push(Number(v.toPrecision(12)));
        return { lo, hi, step, ticks };
    }

    const SEC = 1000, MIN = 60 * SEC, HOUR = 60 * MIN, DAY = 24 * HOUR;
    const TIME_STEPS = [
        ['s', 1, SEC], ['s', 5, 5 * SEC], ['s', 15, 15 * SEC], ['s', 30, 30 * SEC],
        ['m', 1, MIN], ['m', 5, 5 * MIN], ['m', 15, 15 * MIN], ['m', 30, 30 * MIN],
        ['h', 1, HOUR], ['h', 3, 3 * HOUR], ['h', 6, 6 * HOUR], ['h', 12, 12 * HOUR],
        ['d', 1, DAY], ['d', 2, 2 * DAY], ['d', 7, 7 * DAY],
        ['mo', 1, 30 * DAY], ['mo', 3, 91 * DAY], ['mo', 6, 182 * DAY],
        ...[1, 2, 5, 10, 20, 50, 100, 200, 500].map(n => ['y', n, n * 365 * DAY]),
    ];

    /** Calendar-aligned UTC ticks over [lo, hi]: [{ t, label }]. */
    function timeTicks(lo, hi, count = 6) {
        const span = Math.max(hi - lo, SEC);
        const [unit, n] = TIME_STEPS.find(([, , ms]) => span / ms <= count) || TIME_STEPS[TIME_STEPS.length - 1];
        const ticks = [];
        if (unit === 'mo' || unit === 'y') {
            const d = new Date(lo);
            let y = d.getUTCFullYear(), m = unit === 'y' ? 0 : d.getUTCMonth();
            if (unit === 'y') y = Math.ceil(y / n) * n; else m = Math.ceil(m / n) * n;
            for (let t = Date.UTC(y, m, 1); ; t = Date.UTC(y, m, 1)) {
                if (t >= lo && t <= hi) ticks.push(t);
                if (t > hi || ticks.length > 50) break;
                if (unit === 'y') y += n; else m += n;
            }
        } else {
            const ms = { s: SEC, m: MIN, h: HOUR, d: DAY }[unit] * n;
            for (let t = Math.ceil(lo / ms) * ms; t <= hi && ticks.length <= 50; t += ms) ticks.push(t);
        }
        const fmt = (t) => {
            const d = new Date(t);
            if (unit === 'y') return String(d.getUTCFullYear());
            if (unit === 'mo') return `${MONTHS[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
            if (unit === 'd') return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}`;
            const hm = `${String(d.getUTCHours()).padStart(2, '0')}:${String(d.getUTCMinutes()).padStart(2, '0')}`;
            return unit === 's' ? `${hm}:${String(d.getUTCSeconds()).padStart(2, '0')}` : hm;
        };
        return ticks.map(t => ({ t, label: fmt(t) }));
    }

    function fmtTime(t, dateOnly) {
        const iso = new Date(t).toISOString();
        return dateOnly ? iso.slice(0, 10) : `${iso.slice(0, 10)} ${iso.slice(11, 16)}`;
    }

    function fmtFull(v) {
        return v === null || v === undefined ? '–' : v.toLocaleString(undefined, { maximumFractionDigits: 4 });
    }

    /** Tick formatter for ticks `step` apart: compact from 1,000 up (1K, 2.5M). */
    function tickFormat(ticks, step) {
        const big = Math.max(...ticks.map(Math.abs)) >= 1000;
        const digits = Math.max(0, Math.min(6, -Math.floor(Math.log10(step))));
        const nf = big ? new Intl.NumberFormat(undefined, { notation: 'compact', maximumFractionDigits: 1 })
            : new Intl.NumberFormat(undefined, { maximumFractionDigits: digits });
        return (v) => nf.format(v);
    }

    const linear = (d0, d1, r0, r1) => (v) => (d1 === d0 ? (r0 + r1) / 2 : r0 + ((v - d0) / (d1 - d0)) * (r1 - r0));
    const r1 = (n) => Math.round(n * 10) / 10;
    const clip = (text, chars) => (text.length > chars ? `${text.slice(0, Math.max(1, chars - 1))}…` : text);
    const seriesClass = (s, i) => (s.other ? 'so' : `s${i % MAX_SERIES}`);

    // --- Drawing --------------------------------------------------------------------

    function s(tag, attrs, ...children) {
        const el = document.createElementNS(SVG_NS, tag);
        for (const [k, v] of Object.entries(attrs || {})) if (v !== undefined && v !== null && v !== false) el.setAttribute(k, String(v));
        for (const c of children.flat(Infinity)) if (c !== null && c !== undefined && c !== false) el.appendChild(c instanceof Node ? c : document.createTextNode(String(c)));
        return el;
    }

    /** Rectangle with the two corners on `side` (the data end) rounded. */
    function barPath(x, y, w, hgt, side, r) {
        r = Math.max(0, Math.min(r, w / 2, hgt / 2));
        const [a, b, c, d] = [r1(x), r1(y), r1(x + w), r1(y + hgt)];
        if (side === 'top') return `M${a},${d}V${b + r}Q${a},${b} ${a + r},${b}H${c - r}Q${c},${b} ${c},${b + r}V${d}Z`;
        if (side === 'bottom') return `M${a},${b}V${d - r}Q${a},${d} ${a + r},${d}H${c - r}Q${c},${d} ${c},${d - r}V${b}Z`;
        if (side === 'right') return `M${a},${b}H${c - r}Q${c},${b} ${c},${b + r}V${d - r}Q${c},${d} ${c - r},${d}H${a}Z`;
        return `M${c},${b}H${a + r}Q${a},${b} ${a},${b + r}V${d - r}Q${a},${d} ${a + r},${d}H${c}Z`;
    }

    function tooltip(box) {
        const el = h('div', { class: 'chart-tip', hidden: true });
        box.appendChild(el);
        return {
            show(x, y, title, rows) {
                el.replaceChildren(...[
                    title ? h('div', { class: 'chart-tip-title' }, title) : null,
                    ...rows.map(r => h('div', { class: 'chart-tip-row' },
                        r.cls ? h('span', { class: `swatch ${r.cls}` }) : null,
                        h('span', { class: 'chart-tip-name' }, r.name),
                        h('span', { class: 'chart-tip-value' }, r.value))),
                ].filter(Boolean));
                el.hidden = false;
                const bw = box.clientWidth, tw = el.offsetWidth, th = el.offsetHeight;
                let left = x + 14;
                if (left + tw > bw) left = x - tw - 14;
                el.style.left = `${Math.max(0, left)}px`;
                el.style.top = `${Math.max(0, Math.min(y - th / 2, box.clientHeight - th))}px`;
            },
            hide() { el.hidden = true; },
        };
    }

    function pointer(e, box) {
        const r = box.getBoundingClientRect();
        return [e.clientX - r.left, e.clientY - r.top];
    }

    function yAxis(g, yt, y, left, right, fmt) {
        for (const t of yt.ticks) {
            const py = r1(y(t));
            g.appendChild(s('line', { class: t === 0 ? 'chart-axis' : 'chart-grid', x1: left, x2: right, y1: py, y2: py }));
            g.appendChild(s('text', { x: left - 6, y: py, 'text-anchor': 'end', 'dominant-baseline': 'middle' }, fmt(t)));
        }
    }

    function valueRange(data, zero) {
        let min = Infinity, max = -Infinity;
        for (const sr of data.series) for (const v of sr.values) if (v !== null) { if (v < min) min = v; if (v > max) max = v; }
        if (min === Infinity) { min = 0; max = 1; }
        if (zero) { min = Math.min(0, min); max = Math.max(0, max); }
        return [min, max];
    }

    function leftMargin(yt, fmt) {
        return Math.max(...yt.ticks.map(t => fmt(t).length)) * CHAR_W + 12;
    }

    /** Vertical bars, lines and areas over one x axis, with an index hover. */
    function drawXY(box, data, width) {
        const H = 300, mt = 12, mb = 28, mr = 16;
        const isBar = data.type === 'bar';
        const [min, max] = valueRange(data, isBar || data.type === 'area');
        const yt = niceTicks(min, max, 5);
        const fmt = tickFormat(yt.ticks, yt.step);
        const ml = leftMargin(yt, fmt);
        const pw = Math.max(40, width - ml - mr), ph = H - mt - mb;
        const y = linear(yt.lo, yt.hi, mt + ph, mt);
        const n = data.xs.length;
        const svg = s('svg', { class: 'chart-svg', width, height: H, viewBox: `0 0 ${width} ${H}`, role: 'img' });
        const axis = s('g', { class: 'chart-axes' });
        svg.appendChild(axis);
        yAxis(axis, yt, y, ml, ml + pw, fmt);

        let px, band = 0, xScaleTicks = [];
        if (data.xKind === 'category') {
            band = pw / Math.max(1, n);
            px = (i) => ml + band * (i + 0.5);
            const maxChars = Math.max(3, Math.floor(band / CHAR_W) - 1);
            const longest = Math.max(...data.xs.map(x => x.label.length));
            const step = Math.max(1, Math.ceil((Math.min(longest, 14) * CHAR_W + 8) / band));
            data.xs.forEach((x, i) => { if (i % step === 0) xScaleTicks.push({ p: px(i), label: clip(x.label, step > 1 ? 14 : Math.min(14, maxChars)) }); });
        } else {
            let lo = data.xs[0]?.pos ?? 0, hi = data.xs[n - 1]?.pos ?? 1;
            if (lo === hi) { lo -= 1; hi += 1; }
            const xs = linear(lo, hi, ml, ml + pw);
            px = (i) => xs(data.xs[i].pos);
            const count = Math.max(2, Math.floor(pw / 90));
            if (data.xKind === 'time') xScaleTicks = timeTicks(lo, hi, count).map(t => ({ p: xs(t.t), label: t.label }));
            else {
                const xt = niceTicks(lo, hi, count), xf = tickFormat(xt.ticks, xt.step);
                xScaleTicks = xt.ticks.filter(t => t >= lo && t <= hi).map(t => ({ p: xs(t), label: xf(t) }));
            }
        }
        for (const t of xScaleTicks) axis.appendChild(s('text', { x: r1(t.p), y: H - mb + 16, 'text-anchor': 'middle' }, t.label));

        const base = y(Math.max(yt.lo, Math.min(0, yt.hi)));
        const marks = s('g', {});
        svg.appendChild(marks);
        const m = data.series.length;
        const hoverBand = s('rect', { class: 'chart-hover-band', y: mt, height: ph, width: 0, hidden: true });
        if (isBar) {
            marks.appendChild(hoverBand);
            const inner = Math.max(1, Math.min(24, (band * 0.8 - 2 * (m - 1)) / m));
            const group = m * inner + (m - 1) * 2;
            data.series.forEach((sr, j) => {
                const cls = seriesClass(sr, j);
                sr.values.forEach((v, i) => {
                    if (v === null) return;
                    const x0 = px(i) - group / 2 + j * (inner + 2);
                    const top = Math.min(y(v), base), hgt = Math.max(1, Math.abs(y(v) - base));
                    marks.appendChild(s('path', { class: `mark ${cls}`, d: barPath(x0, top, inner, hgt, v >= 0 ? 'top' : 'bottom', 4) }));
                });
            });
        } else {
            const markers = n <= 40;
            data.series.forEach((sr, j) => {
                const cls = seriesClass(sr, j);
                const segs = [];
                let cur = null;
                sr.values.forEach((v, i) => {
                    if (v === null) { cur = null; return; }
                    if (!cur) { cur = []; segs.push(cur); }
                    cur.push([r1(px(i)), r1(y(v))]);
                });
                const g = s('g', { class: cls });
                for (const seg of segs) {
                    const line = seg.map(([a, b], k) => `${k ? 'L' : 'M'}${a},${b}`).join('');
                    if (data.type === 'area') g.appendChild(s('path', { class: 'area', d: `${line}L${seg[seg.length - 1][0]},${r1(base)}L${seg[0][0]},${r1(base)}Z` }));
                    g.appendChild(s('path', { class: 'line', d: line }));
                    if (markers || seg.length === 1) for (const [a, b] of seg) g.appendChild(s('circle', { class: 'dot', cx: a, cy: b, r: 4 }));
                }
                marks.appendChild(g);
            });
        }

        // Hover: nearest x position.
        const cross = s('line', { class: 'chart-cross', y1: mt, y2: mt + ph, hidden: true });
        const hoverDots = s('g', {});
        svg.append(cross, hoverDots);
        const hit = s('rect', { class: 'chart-hit', x: ml, y: mt, width: pw, height: ph });
        svg.appendChild(hit);
        box.appendChild(svg);
        const tip = tooltip(box);
        const pxs = data.xs.map((_, i) => px(i));
        const nearest = (mx) => {
            if (data.xKind === 'category') return Math.max(0, Math.min(n - 1, Math.floor((mx - ml) / band)));
            let lo = 0, hi = n - 1;
            while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (pxs[mid] < mx) lo = mid; else hi = mid; }
            return Math.abs(pxs[lo] - mx) <= Math.abs(pxs[hi] - mx) ? lo : hi;
        };
        hit.addEventListener('mousemove', (e) => {
            if (!n) return;
            const [mx, my] = pointer(e, box);
            const i = nearest(mx);
            if (isBar) {
                hoverBand.setAttribute('x', r1(ml + band * i));
                hoverBand.setAttribute('width', r1(band));
                hoverBand.removeAttribute('hidden');
            } else {
                cross.setAttribute('x1', r1(pxs[i]));
                cross.setAttribute('x2', r1(pxs[i]));
                cross.removeAttribute('hidden');
                hoverDots.replaceChildren(...data.series.map((sr, j) => (sr.values[i] === null ? null
                    : s('circle', { class: `dot hover ${seriesClass(sr, j)}`, cx: r1(pxs[i]), cy: r1(y(sr.values[i])), r: 5 }))).filter(Boolean));
            }
            tip.show(pxs[i], my, data.xs[i].label, data.series.map((sr, j) => ({ cls: m > 1 ? seriesClass(sr, j) : null, name: sr.name, value: fmtFull(sr.values[i]) })));
        });
        hit.addEventListener('mouseleave', () => {
            tip.hide();
            hoverBand.setAttribute('hidden', '');
            cross.setAttribute('hidden', '');
            hoverDots.replaceChildren();
        });
    }

    /** Horizontal bars for many or long category names, value labels at the tips. */
    function drawHBar(box, data, width) {
        const m = data.series.length, n = data.xs.length;
        const inner = m === 1 ? 18 : Math.max(6, Math.min(14, 40 / m));
        const group = m * inner + (m - 1) * 2;
        const rowH = group + 10;
        const mt = 4, mb = 26, mr = m === 1 ? 56 : 16;
        const H = mt + n * rowH + mb;
        const longest = Math.max(...data.xs.map(x => x.label.length));
        const ml = Math.min(width * 0.4, longest * CHAR_W + 14);
        const pw = Math.max(40, width - ml - mr);
        const [min, max] = valueRange(data, true);
        const xt = niceTicks(min, max, Math.max(2, Math.floor(pw / 80)));
        const fmt = tickFormat(xt.ticks, xt.step);
        const x = linear(xt.lo, xt.hi, ml, ml + pw);
        const base = x(Math.max(xt.lo, Math.min(0, xt.hi)));
        const svg = s('svg', { class: 'chart-svg', width, height: H, viewBox: `0 0 ${width} ${H}`, role: 'img' });
        const axis = s('g', { class: 'chart-axes' });
        svg.appendChild(axis);
        for (const t of xt.ticks) {
            const p = r1(x(t));
            axis.appendChild(s('line', { class: t === 0 ? 'chart-axis' : 'chart-grid', x1: p, x2: p, y1: mt, y2: mt + n * rowH }));
            axis.appendChild(s('text', { x: p, y: H - mb + 16, 'text-anchor': 'middle' }, fmt(t)));
        }
        const hoverBand = s('rect', { class: 'chart-hover-band', x: 0, width, height: rowH, hidden: true });
        svg.appendChild(hoverBand);
        const maxChars = Math.floor((ml - 12) / CHAR_W);
        const values = s('g', { class: 'chart-values' });
        data.xs.forEach((cat, i) => {
            const cy = mt + rowH * (i + 0.5);
            axis.appendChild(s('text', { x: ml - 8, y: r1(cy), 'text-anchor': 'end', 'dominant-baseline': 'middle' }, clip(cat.label, maxChars)));
            data.series.forEach((sr, j) => {
                const v = sr.values[i];
                if (v === null) return;
                const y0 = cy - group / 2 + j * (inner + 2);
                const left = Math.min(x(v), base), w = Math.max(1, Math.abs(x(v) - base));
                svg.appendChild(s('path', { class: `mark ${seriesClass(sr, j)}`, d: barPath(left, y0, w, inner, v >= 0 ? 'right' : 'left', 4) }));
                if (m === 1) values.appendChild(s('text', { x: r1(v >= 0 ? x(v) + 6 : x(v) - 6), y: r1(cy), 'text-anchor': v >= 0 ? 'start' : 'end', 'dominant-baseline': 'middle' }, fmt(v)));
            });
        });
        svg.appendChild(values);
        const hit = s('rect', { class: 'chart-hit', x: 0, y: mt, width, height: n * rowH });
        svg.appendChild(hit);
        box.appendChild(svg);
        const tip = tooltip(box);
        hit.addEventListener('mousemove', (e) => {
            const [mx, my] = pointer(e, box);
            const i = Math.max(0, Math.min(n - 1, Math.floor((my - mt) / rowH)));
            hoverBand.setAttribute('y', r1(mt + rowH * i));
            hoverBand.removeAttribute('hidden');
            tip.show(mx, my, data.xs[i].label, data.series.map((sr, j) => ({ cls: m > 1 ? seriesClass(sr, j) : null, name: sr.name, value: fmtFull(sr.values[i]) })));
        });
        hit.addEventListener('mouseleave', () => { tip.hide(); hoverBand.setAttribute('hidden', ''); });
    }

    function drawPie(box, data, width) {
        const vals = data.series[0]?.values || [];
        const total = vals.reduce((a, v) => a + v, 0);
        const H = 280, R = 120, r0 = 72;
        const cx = width / 2, cy = H / 2;
        const svg = s('svg', { class: 'chart-svg', width, height: H, viewBox: `0 0 ${width} ${H}`, role: 'img' });
        const tip = tooltip(box);
        const pt = (a, r) => `${r1(cx + r * Math.sin(a))},${r1(cy - r * Math.cos(a))}`;
        let a0 = 0;
        data.xs.forEach((x, i) => {
            const frac = total ? vals[i] / total : 0;
            const a1 = a0 + frac * Math.PI * 2;
            const cls = x.other ? 'so' : `s${i % MAX_SERIES}`;
            const d = frac >= 0.9999
                ? `M${pt(0, R)}A${R},${R} 0 1 1 ${pt(Math.PI, R)}A${R},${R} 0 1 1 ${pt(0, R)}M${pt(0, r0)}A${r0},${r0} 0 1 0 ${pt(Math.PI, r0)}A${r0},${r0} 0 1 0 ${pt(0, r0)}Z`
                : `M${pt(a0, R)}A${R},${R} 0 ${a1 - a0 > Math.PI ? 1 : 0} 1 ${pt(a1, R)}L${pt(a1, r0)}A${r0},${r0} 0 ${a1 - a0 > Math.PI ? 1 : 0} 0 ${pt(a0, r0)}Z`;
            const slice = s('path', { class: `mark slice ${cls}`, d, 'fill-rule': 'evenodd' });
            slice.addEventListener('mousemove', (e) => {
                const [mx, my] = pointer(e, box);
                tip.show(mx, my, x.label, [{ cls, name: data.series[0].name, value: `${fmtFull(vals[i])} (${(frac * 100).toFixed(1)}%)` }]);
            });
            slice.addEventListener('mouseleave', () => tip.hide());
            svg.appendChild(slice);
            a0 = a1;
        });
        svg.appendChild(s('text', { class: 'chart-total', x: r1(cx), y: r1(cy - 4), 'text-anchor': 'middle' }, tickFormat([total], 1)(total)));
        svg.appendChild(s('text', { x: r1(cx), y: r1(cy + 14), 'text-anchor': 'middle' }, 'total'));
        box.insertBefore(svg, box.firstChild);
    }

    function drawScatter(box, data, width, spec) {
        const H = 320, mt = 12, mb = 40, mr = 16;
        const pts = data.points;
        const ext = (k) => { let lo = Infinity, hi = -Infinity; for (const p of pts) { if (p[k] < lo) lo = p[k]; if (p[k] > hi) hi = p[k]; } return lo === Infinity ? [0, 1] : [lo, hi]; };
        const yt = niceTicks(...ext('y'), 5);
        const fmt = tickFormat(yt.ticks, yt.step);
        const ml = leftMargin(yt, fmt);
        const pw = Math.max(40, width - ml - mr), ph = H - mt - mb;
        const xt = niceTicks(...ext('x'), Math.max(2, Math.floor(pw / 90)));
        const xf = tickFormat(xt.ticks, xt.step);
        const x = linear(xt.lo, xt.hi, ml, ml + pw), y = linear(yt.lo, yt.hi, mt + ph, mt);
        const svg = s('svg', { class: 'chart-svg', width, height: H, viewBox: `0 0 ${width} ${H}`, role: 'img' });
        const axis = s('g', { class: 'chart-axes' });
        svg.appendChild(axis);
        yAxis(axis, yt, y, ml, ml + pw, fmt);
        for (const t of xt.ticks) axis.appendChild(s('text', { x: r1(x(t)), y: mt + ph + 16, 'text-anchor': 'middle' }, xf(t)));
        axis.appendChild(s('text', { class: 'chart-axis-title', x: ml + pw, y: H - 4, 'text-anchor': 'end' }, `${spec.x} →`));
        const cls = new Map(data.series.map((sr, j) => [sr.name, seriesClass(sr, j)]));
        const g = s('g', {});
        const placed = pts.map(p => ({ ...p, px: x(p.x), py: y(p.y) }));
        for (const p of placed) g.appendChild(s('circle', { class: `dot ${cls.get(p.group)}`, cx: r1(p.px), cy: r1(p.py), r: 4 }));
        svg.appendChild(g);
        const ring = s('circle', { class: 'dot hover', r: 6, hidden: true });
        svg.appendChild(ring);
        const hit = s('rect', { class: 'chart-hit', x: ml, y: mt, width: pw, height: ph });
        svg.appendChild(hit);
        box.appendChild(svg);
        const tip = tooltip(box);
        hit.addEventListener('mousemove', (e) => {
            const [mx, my] = pointer(e, box);
            let best = null, bd = 24 * 24;
            for (const p of placed) { const d = (p.px - mx) ** 2 + (p.py - my) ** 2; if (d < bd) { bd = d; best = p; } }
            if (!best) { tip.hide(); ring.setAttribute('hidden', ''); return; }
            ring.setAttribute('cx', r1(best.px));
            ring.setAttribute('cy', r1(best.py));
            ring.setAttribute('class', `dot hover ${cls.get(best.group)}`);
            ring.removeAttribute('hidden');
            const rows = [{ name: spec.x, value: fmtFull(best.x) }, { name: best.yName, value: fmtFull(best.y) }];
            if (spec.series) rows.push({ cls: cls.get(best.group), name: spec.series, value: best.group });
            tip.show(best.px, best.py, best.label, rows);
        });
        hit.addEventListener('mouseleave', () => { tip.hide(); ring.setAttribute('hidden', ''); });
    }

    function legend(data) {
        if (data.type === 'pie') {
            const vals = data.series[0]?.values || [];
            const total = vals.reduce((a, v) => a + v, 0);
            return h('div', { class: 'chart-legend' }, data.xs.map((x, i) => h('span', { class: 'chart-key' },
                h('span', { class: `swatch ${x.other ? 'so' : `s${i % MAX_SERIES}`}` }), x.label,
                h('span', { class: 'muted' }, ` ${total ? ((vals[i] / total) * 100).toFixed(1) : 0}%`))));
        }
        if (data.series.length < 2) return null;
        return h('div', { class: 'chart-legend' }, data.series.map((sr, j) => h('span', { class: 'chart-key' }, h('span', { class: `swatch ${seriesClass(sr, j)}` }), sr.name)));
    }

    /** Draw `spec` over the result into `plot` at its current width. */
    function draw(plot, spec, result) {
        const width = Math.floor(plot.clientWidth);
        if (!width) return;
        const data = chartData(spec, result.columns, result.rows);
        plot.replaceChildren();
        const empty = spec.type === 'scatter' ? !data.points.length : !data.xs.length || data.series.every(sr => sr.values.every(v => v === null));
        if (empty) { plot.appendChild(h('div', { class: 'muted small chart-empty' }, 'No numeric values to chart for these columns.')); return data; }
        if (data.type === 'bar' && !data.xIsTime && (data.xs.length > 12 || Math.max(...data.xs.map(x => x.label.length)) > 12)) drawHBar(plot, data, width);
        else if (data.type === 'bar' || data.type === 'line' || data.type === 'area') drawXY(plot, data, width);
        else if (data.type === 'pie') drawPie(plot, data, width);
        else drawScatter(plot, data, width, spec);
        return data;
    }

    // --- Panel ----------------------------------------------------------------------

    /**
     * Chart with its controls for a result { columns, rows }, or null when no
     * chart fits. `state.chart` is the chosen spec (null: automatic) and is
     * updated when the person changes the controls; `state.initial` is what
     * Reset returns to.
     */
    function chartView(result, state) {
        if (!resolveChart(state.chart, result.columns, result.rows)) return null;
        const kinds = columnKinds(result.columns, result.rows);
        const numeric = kinds.filter(c => c.kind === 'number').map(c => c.name);
        const el = h('div', { class: 'chart' });
        let last = { width: 0 };

        const render = () => {
            const spec = resolveChart(state.chart, result.columns, result.rows);
            const set = (patch) => { state.chart = fixSpec({ ...spec, ...patch }) || spec; render(); };
            const select = (label, value, options, onChange) => h('label', { class: 'chart-control' }, h('span', {}, label),
                h('select', { onchange: (e) => onChange(e.target.value) }, options.map(([v, text]) => h('option', { value: v, selected: v === value }, text))));
            const yChoices = numeric.filter(c => c !== spec.x && c !== spec.series);
            const single = spec.type === 'pie' || !!spec.series;
            const yControl = single
                ? select('Value', spec.y[0], yChoices.map(c => [c, c]), (v) => set({ y: [v] }))
                : h('div', { class: 'chart-control' }, h('span', {}, 'Values'), h('div', { class: 'chart-ys' }, yChoices.map(c => h('label', { class: 'chart-y' },
                    h('input', { type: 'checkbox', checked: spec.y.includes(c), onchange: (e) => {
                        const y = e.target.checked ? [...spec.y, c] : spec.y.filter(v => v !== c);
                        if (y.length) set({ y }); else e.target.checked = true;
                    } }), c))));
            const seriesOptions = [['', 'None'], ...kinds.filter(c => c.name !== spec.x && !spec.y.includes(c.name) && c.kind !== 'empty' && c.distinct > 1).map(c => [c.name, c.name])];
            const reset = h('button', { class: 'btn small', type: 'button', hidden: JSON.stringify(state.chart) === JSON.stringify(state.initial), onclick: () => { state.chart = state.initial; render(); } },
                icon('refresh'), 'Reset');
            const controls = h('div', { class: 'chart-controls' },
                select('Chart', spec.type, TYPES, (v) => set({ type: v })),
                select(spec.type === 'pie' ? 'Slices' : 'X axis', spec.x, result.columns.map(c => [c, c]), (v) => set({ x: v, y: spec.y.filter(c => c !== v).length ? spec.y.filter(c => c !== v) : yChoices.filter(c => c !== v).slice(0, 1) })),
                yControl,
                spec.type === 'pie' ? null : select('Split by', spec.series || '', seriesOptions, (v) => set({ series: v || null, y: spec.y.slice(0, v ? 1 : 8) })),
                reset);
            const caption = spec.title || (spec.y.length === 1 ? `${spec.y[0]}${spec.series ? ` by ${spec.series}` : ''}` : '');
            const plot = h('div', { class: 'chart-plot' });
            const note = h('div', { class: 'muted small' });
            const legendSlot = h('div');
            el.replaceChildren(...[controls, caption ? h('div', { class: 'chart-title' }, caption) : null, legendSlot, plot, note].filter(Boolean));
            const paint = () => {
                const data = draw(plot, spec, result);
                if (!data) return;
                legendSlot.replaceChildren(legend(data) || '');
                note.textContent = data.note || '';
            };
            last = { width: 0, paint };
            requestAnimationFrame(() => { last.width = plot.clientWidth; paint(); });
        };

        // Redraw at the new width when the panel is resized (or first shown).
        new ResizeObserver(() => {
            const plot = el.querySelector('.chart-plot');
            if (!plot || !last.paint || !plot.clientWidth || plot.clientWidth === last.width) return;
            last.width = plot.clientWidth;
            last.paint();
        }).observe(el);
        render();
        return el;
    }

    root.LakeChart = { TYPES, columnKinds, autoChart, fixSpec, resolveChart, chartData, niceTicks, timeTicks, toTime, toNum, chartView };
})(window);
