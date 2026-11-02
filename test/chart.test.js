/**
 * Unit tests for the console's result charts (public/console/chart.js): how a
 * chart is picked for a result, how specs are fixed up and how the data is
 * shaped for drawing. The browser script runs in a vm with a stub window; the
 * drawing itself needs a DOM and is not covered here.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const code = fs.readFileSync(new URL('../public/console/chart.js', import.meta.url), 'utf8');
const sandbox = { window: {} };
vm.runInNewContext(code, sandbox);
const { autoChart, fixSpec, resolveChart, chartData, columnKinds, niceTicks, timeTicks, toTime } = sandbox.window.LakeChart;
// Objects from the vm have another realm's prototypes; compare plain copies.
const plain = (v) => JSON.parse(JSON.stringify(v));

let passed = 0, failed = 0;

async function test(name, fn) {
    try {
        await fn();
        passed++;
        console.log(`  ✓ ${name}`);
    } catch (err) {
        failed++;
        console.error(`  ✗ ${name}`);
        console.error(`    ${err.message}`);
    }
}

async function run() {
    console.log('\n▸ chart: column kinds');
    await test('numbers (also DECIMAL strings), timestamps and text are told apart', () => {
        const kinds = columnKinds(['id', 'day', 'total', 'name', 'year', 'empty'], [
            [1, '2024-01-01', '12.50', 'a', 2023, null],
            [2, '2024-01-02 10:00:00', '7', 'b', 2024, null],
        ]);
        assert.deepEqual(plain(kinds.map(k => k.kind)), ['number', 'time', 'number', 'text', 'number', 'empty']);
        assert.equal(kinds[0].idLike, true);
        assert.equal(kinds[4].period, true);
    });
    await test('timestamps are read as UTC unless they carry an offset', () => {
        assert.equal(toTime('2024-03-01'), Date.UTC(2024, 2, 1));
        assert.equal(toTime('2024-03-01 12:30:00'), Date.UTC(2024, 2, 1, 12, 30));
        assert.equal(toTime('2024-03-01 12:30:00.123456+02'), Date.UTC(2024, 2, 1, 10, 30, 0, 123));
        assert.equal(toTime('2024-03'), Date.UTC(2024, 2, 1));
        assert.ok(Number.isNaN(toTime('next tuesday')));
    });

    console.log('\n▸ chart: automatic choice');
    await test('time plus measures gives a line chart', () => {
        assert.deepEqual(plain(autoChart(['day', 'orders', 'revenue'], [['2024-01-01', 3, 10], ['2024-01-02', 4, 12]])),
            { type: 'line', x: 'day', y: ['orders', 'revenue'], series: null, title: '' });
    });
    await test('long-format time series is split by its text column', () => {
        const rows = [['2024-01-01', 'eu', 3], ['2024-01-01', 'us', 5], ['2024-01-02', 'eu', 4], ['2024-01-02', 'us', 6]];
        assert.deepEqual(plain(autoChart(['day', 'region', 'n'], rows)), { type: 'line', x: 'day', y: ['n'], series: 'region', title: '' });
    });
    await test('categories plus a measure give a bar chart; ids are not measures', () => {
        assert.deepEqual(plain(autoChart(['customer_id', 'name', 'spend'], [[1, 'Ann', 10], [2, 'Bob', 20]])),
            { type: 'bar', x: 'name', y: ['spend'], series: null, title: '' });
    });
    await test('a numeric year column is used as the x axis', () => {
        assert.equal(autoChart(['year', 'sales'], [[2022, 1], [2023, 2]]).x, 'year');
    });
    await test('two measures alone give a scatter', () => {
        assert.equal(autoChart(['height', 'weight'], [[1, 2], [3, 4]]).type, 'scatter');
    });
    await test('no chart for one row, no measures or text-only results', () => {
        assert.equal(autoChart(['name', 'n'], [['a', 1]]), null);
        assert.equal(autoChart(['name', 'city'], [['a', 'x'], ['b', 'y']]), null);
        assert.equal(autoChart(['id', 'name'], [[1, 'a'], [2, 'b']]), null);
        assert.equal(autoChart([], []), null);
    });

    console.log('\n▸ chart: specs');
    await test('fixSpec keeps a pie or split chart to one value column', () => {
        assert.deepEqual(plain(fixSpec({ type: 'pie', x: 'a', y: ['b', 'c'], series: 'd' })), { type: 'pie', x: 'a', y: ['b'], series: null, title: '' });
        assert.deepEqual(plain(fixSpec({ type: 'bar', x: 'a', y: ['a', 'b', 'c'], series: 'd' })).y, ['b']);
        assert.equal(fixSpec({ type: 'bar', x: 'a', y: ['a'] }), null);
        assert.equal(fixSpec({ type: 'radar', x: 'a', y: ['b'] }), null);
    });
    await test('resolveChart falls back to automatic when the columns changed', () => {
        const rows = [['x', 1], ['y', 2]];
        assert.equal(resolveChart({ type: 'pie', x: 'k', y: ['v'] }, ['k', 'v'], rows).type, 'pie');
        assert.equal(resolveChart({ type: 'pie', x: 'gone', y: ['v'] }, ['k', 'v'], rows).type, 'bar');
        assert.equal(resolveChart(null, ['k', 'v'], rows).type, 'bar');
    });

    console.log('\n▸ chart: data');
    await test('line data is sorted by time and pivoted by series', () => {
        const rows = [['2024-01-02', 'eu', 4], ['2024-01-01', 'eu', 3], ['2024-01-01', 'us', 5]];
        const d = chartData({ type: 'line', x: 'day', y: ['n'], series: 'region' }, ['day', 'region', 'n'], rows);
        assert.equal(d.xKind, 'time');
        assert.deepEqual(plain(d.xs.map(x => x.label)), ['2024-01-01', '2024-01-02']);
        assert.deepEqual(plain(d.series), [{ name: 'eu', values: [3, 4] }, { name: 'us', values: [5, null] }]);
    });
    await test('more than eight series fold the smallest into Other', () => {
        const rows = Array.from({ length: 10 }, (_, i) => ['a', `s${i}`, i + 1]);
        const d = chartData({ type: 'bar', x: 'k', y: ['v'], series: 's' }, ['k', 's', 'v'], rows);
        assert.equal(d.series.length, 8);
        assert.equal(d.series[7].name, 'Other');
        assert.equal(d.series[7].values[0], 1 + 2 + 3);
    });
    await test('pie slices are sorted, non-positive values dropped, the tail grouped', () => {
        const rows = Array.from({ length: 10 }, (_, i) => [`k${i}`, i]);
        const d = chartData({ type: 'pie', x: 'k', y: ['v'] }, ['k', 'v'], rows);
        assert.equal(d.xs.length, 8);
        assert.equal(d.xs[0].label, 'k9');
        assert.equal(d.xs[7].label, 'Other');
        assert.equal(d.series[0].values[7], 1 + 2, 'k2 and k1; k0 is left out');
        assert.match(d.note, /left out/);
    });
    await test('bar charts are capped at 60 categories with a note', () => {
        const rows = Array.from({ length: 80 }, (_, i) => [`k${i}`, i]);
        const d = chartData({ type: 'bar', x: 'k', y: ['v'] }, ['k', 'v'], rows);
        assert.equal(d.xs.length, 60);
        assert.match(d.note, /First 60 of 80/);
    });
    await test('scatter points skip non-numeric rows', () => {
        const d = chartData({ type: 'scatter', x: 'a', y: ['b'] }, ['name', 'a', 'b'], [['p', 1, 2], ['q', null, 3], ['r', '4.5', 6]]);
        assert.deepEqual(plain(d.points.map(p => [p.x, p.y, p.label])), [[1, 2, 'p'], [4.5, 6, 'r']]);
    });

    console.log('\n▸ chart: axes');
    await test('niceTicks rounds the bounds to clean steps', () => {
        assert.deepEqual(plain(niceTicks(0, 97, 5)), { lo: 0, hi: 100, step: 20, ticks: [0, 20, 40, 60, 80, 100] });
        assert.deepEqual(plain(niceTicks(5, 5)).ticks.length > 1, true);
        assert.deepEqual(plain(niceTicks(-0.3, 0.7, 5)).ticks, [-0.4, -0.2, 0, 0.2, 0.4, 0.6, 0.8]);
    });
    await test('timeTicks pick calendar steps for the span', () => {
        const months = timeTicks(Date.UTC(2024, 0, 15), Date.UTC(2024, 11, 20), 6);
        assert.deepEqual(plain(months.map(t => t.label)), ['Apr 2024', 'Jul 2024', 'Oct 2024']);
        const days = timeTicks(Date.UTC(2024, 0, 1), Date.UTC(2024, 0, 5), 6);
        assert.deepEqual(plain(days.map(t => t.label)), ['1 Jan', '2 Jan', '3 Jan', '4 Jan', '5 Jan']);
        const years = timeTicks(Date.UTC(2010, 0, 1), Date.UTC(2024, 0, 1), 6);
        assert.ok(years.every(t => /^\d{4}$/.test(t.label)) && years.length <= 6);
    });

    console.log('\n─── chart unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed ? 1 : 0);
}

run();
