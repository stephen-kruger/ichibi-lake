/**
 * Unit tests for the console's temporal display formatting
 * (public/console/dates.js). The browser module runs in a vm; only the pure
 * date parsing/formatting is covered here, not the DOM.
 */
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const code = fs.readFileSync(new URL('../public/console/dates.js', import.meta.url), 'utf8');
const sandbox = { window: {} };
vm.runInNewContext(code, sandbox);
const { isTemporalType, fmtTemporal, parseInstant, displayValue } = sandbox.window.LakeDates;

// Same options as dates.js; the locale/time zone come from the test machine.
const dateFmt = new Intl.DateTimeFormat(undefined, { year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC' });
const dateTimeFmt = new Intl.DateTimeFormat(undefined, {
    year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', second: '2-digit',
});

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
    console.log('\n▸ dates: temporal columns');
    await test('DATE, TIMESTAMP variants and DATETIME are temporal; text and numbers are not', () => {
        for (const t of ['DATE', 'TIMESTAMP', 'TIMESTAMP_S', 'TIMESTAMP WITH TIME ZONE', 'TIMESTAMPTZ', 'DATETIME']) {
            assert.equal(isTemporalType(t), true, t);
        }
        for (const t of ['VARCHAR', 'BIGINT', 'TIME', '', null]) assert.equal(isTemporalType(t), false, String(t));
    });

    console.log('\n▸ dates: parsing');
    await test('a timestamp without an offset is read as UTC', () => {
        assert.equal(parseInstant('2024-03-01 12:30:00').getTime(), Date.UTC(2024, 2, 1, 12, 30));
        assert.equal(parseInstant('2024-03-01T12:30:00.123456').getTime(), Date.UTC(2024, 2, 1, 12, 30, 0, 123));
    });
    await test('DuckDB offsets are honoured in +HH, +HH:MM and HHMM forms', () => {
        assert.equal(parseInstant('2024-03-01 12:30:00+02').getTime(), Date.UTC(2024, 2, 1, 10, 30));
        assert.equal(parseInstant('2024-03-01 12:30:00+05:30').getTime(), Date.UTC(2024, 2, 1, 7, 0));
        assert.equal(parseInstant('2024-03-01 12:30:00-0430').getTime(), Date.UTC(2024, 2, 1, 17, 0));
        assert.equal(parseInstant('2024-03-01 12:30:00Z').getTime(), Date.UTC(2024, 2, 1, 12, 30));
    });
    await test('non-timestamps are rejected', () => {
        assert.equal(parseInstant('next tuesday'), null);
        assert.equal(parseInstant('2024-03-01'), null);
        assert.equal(parseInstant('infinity'), null);
    });

    console.log('\n▸ dates: display');
    await test('DATE values use the locale but never move to another day', () => {
        const expected = dateFmt.format(new Date(Date.UTC(2024, 2, 1)));
        assert.equal(fmtTemporal('2024-03-01', 'DATE'), expected);
        // A DATE stored in a TIMESTAMP column keeps the same calendar day.
        assert.equal(fmtTemporal('2024-03-01', 'TIMESTAMP'), expected);
    });
    await test('timestamps are shown in the browser time zone and locale', () => {
        const expected = dateTimeFmt.format(new Date(Date.UTC(2024, 2, 1, 12, 30)));
        assert.equal(fmtTemporal('2024-03-01 12:30:00', 'TIMESTAMP'), expected);
        assert.equal(fmtTemporal('2024-03-01T12:30:00Z', 'TIMESTAMP WITH TIME ZONE'), expected);
    });
    await test('a known non-temporal column is left untouched', () => {
        assert.equal(displayValue('2024-03-01 12:30:00', 'VARCHAR'), '2024-03-01 12:30:00');
    });
    await test('without a column type, ISO-shaped strings are still recognised', () => {
        assert.equal(displayValue('2024-03-01', undefined), dateFmt.format(new Date(Date.UTC(2024, 2, 1))));
    });
    await test('NULL and unparseable temporal values pass through', () => {
        assert.equal(fmtTemporal(null, 'DATE'), null);
        assert.equal(displayValue(null, 'TIMESTAMP'), null);
        assert.equal(displayValue('infinity', 'TIMESTAMP'), 'infinity');
    });

    console.log('\n─── dates unit tests ───');
    console.log(`  passed: ${passed}`);
    console.log(`  failed: ${failed}`);
    process.exit(failed ? 1 : 0);
}

run();
