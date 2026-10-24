/**
 * Minimal 5-field cron (minute hour day-of-month month day-of-week), evaluated
 * in UTC. Supports `*`, numbers, ranges `a-b`, lists `a,b`, steps `*\/n` and
 * `a-b/n`, month/day names (JAN, MON, ...) and the macros @hourly, @daily,
 * @weekly, @monthly. As in standard cron, when both day-of-month and
 * day-of-week are restricted, a day matches if EITHER matches.
 */

const FIELDS = [
    { name: 'minute', min: 0, max: 59 },
    { name: 'hour', min: 0, max: 23 },
    { name: 'day of month', min: 1, max: 31 },
    { name: 'month', min: 1, max: 12, names: ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC'] },
    { name: 'day of week', min: 0, max: 6, names: ['SUN', 'MON', 'TUE', 'WED', 'THU', 'FRI', 'SAT'] },
];

const MACROS = {
    '@hourly': '0 * * * *',
    '@daily': '0 0 * * *',
    '@midnight': '0 0 * * *',
    '@weekly': '0 0 * * 0',
    '@monthly': '0 0 1 * *',
};

function parseValue(token, field) {
    const upper = token.toUpperCase();
    if (field.names) {
        const idx = field.names.indexOf(upper);
        if (idx >= 0) return idx + field.min;
    }
    if (!/^\d+$/.test(token)) throw new Error(`Invalid ${field.name} value "${token}"`);
    let n = Number(token);
    if (field.name === 'day of week' && n === 7) n = 0; // 7 is also Sunday
    if (n < field.min || n > field.max) throw new Error(`${field.name} must be ${field.min}-${field.max}, got ${token}`);
    return n;
}

function parseField(text, field) {
    const values = new Set();
    for (const part of text.split(',')) {
        if (!part) throw new Error(`Empty list item in ${field.name}`);
        const [rangePart, stepPart] = part.split('/');
        const step = stepPart === undefined ? 1 : Number(stepPart);
        if (!Number.isInteger(step) || step < 1) throw new Error(`Invalid step "${stepPart}" in ${field.name}`);
        let lo, hi;
        if (rangePart === '*') { lo = field.min; hi = field.max; }
        else if (rangePart.includes('-')) {
            const [a, b] = rangePart.split('-');
            lo = parseValue(a, field); hi = parseValue(b, field);
            if (lo > hi) throw new Error(`Invalid range "${rangePart}" in ${field.name}`);
        } else {
            lo = parseValue(rangePart, field);
            hi = stepPart === undefined ? lo : field.max;
        }
        for (let v = lo; v <= hi; v += step) values.add(v);
    }
    return values;
}

/** Parse a cron expression. Throws with a readable message when invalid. */
export function parseCron(expression) {
    const raw = String(expression || '').trim();
    const expanded = MACROS[raw.toLowerCase()] || raw;
    const parts = expanded.split(/\s+/);
    if (parts.length !== 5) throw new Error('A schedule needs 5 fields: minute hour day-of-month month day-of-week');
    const sets = parts.map((p, i) => parseField(p, FIELDS[i]));
    return {
        expression: raw,
        minutes: sets[0], hours: sets[1], days: sets[2], months: sets[3], weekdays: sets[4],
        dayRestricted: parts[2] !== '*',
        weekdayRestricted: parts[4] !== '*',
    };
}

export function validateCron(expression) {
    try { parseCron(expression); return null; } catch (err) { return err.message; }
}

function dayMatches(cron, d) {
    const dom = cron.days.has(d.getUTCDate());
    const dow = cron.weekdays.has(d.getUTCDay());
    if (cron.dayRestricted && cron.weekdayRestricted) return dom || dow;
    if (cron.dayRestricted) return dom;
    if (cron.weekdayRestricted) return dow;
    return true;
}

/** Next matching time strictly after `from` (a Date), or null within ~5 years. */
export function nextRun(expression, from = new Date()) {
    const cron = typeof expression === 'string' ? parseCron(expression) : expression;
    const d = new Date(from.getTime());
    d.setUTCSeconds(0, 0);
    d.setUTCMinutes(d.getUTCMinutes() + 1);
    const limit = from.getTime() + 5 * 366 * 24 * 3600 * 1000;
    while (d.getTime() <= limit) {
        if (!cron.months.has(d.getUTCMonth() + 1)) {
            d.setUTCMonth(d.getUTCMonth() + 1, 1); d.setUTCHours(0, 0, 0, 0); continue;
        }
        if (!dayMatches(cron, d)) {
            d.setUTCDate(d.getUTCDate() + 1); d.setUTCHours(0, 0, 0, 0); continue;
        }
        if (!cron.hours.has(d.getUTCHours())) {
            d.setUTCHours(d.getUTCHours() + 1, 0, 0, 0); continue;
        }
        if (!cron.minutes.has(d.getUTCMinutes())) {
            d.setUTCMinutes(d.getUTCMinutes() + 1, 0, 0); continue;
        }
        return d;
    }
    return null;
}

/** The next `count` run times after `from`. */
export function nextRuns(expression, count = 3, from = new Date()) {
    const cron = parseCron(expression);
    const out = [];
    let t = from;
    for (let i = 0; i < count; i++) {
        const n = nextRun(cron, t);
        if (!n) break;
        out.push(n);
        t = n;
    }
    return out;
}

/** Schedule presets offered in the console. */
export const SCHEDULE_PRESETS = [
    { label: 'Every 15 minutes', cron: '*/15 * * * *' },
    { label: 'Hourly', cron: '0 * * * *' },
    { label: 'Every 6 hours', cron: '0 */6 * * *' },
    { label: 'Daily at 02:00 UTC', cron: '0 2 * * *' },
    { label: 'Weekly, Sunday 03:00 UTC', cron: '0 3 * * 0' },
    { label: 'Monthly, 1st at 04:00 UTC', cron: '0 4 1 * *' },
];
