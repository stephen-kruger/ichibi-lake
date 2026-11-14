/*
 * Browser-local display for DATE and TIMESTAMP values in the console.
 *
 * Query results and table cells otherwise show DuckDB's text form, which uses
 * the gateway's time zone (UTC in Docker): a TIMESTAMP has no zone and a
 * TIMESTAMPTZ is rendered with the server's offset. Both are displayed here in
 * the browser's locale and time zone instead.
 *
 * DATE values are calendar dates: they are formatted for the locale but never
 * shifted across time zones, so 2024-03-01 stays 1 March everywhere. A
 * TIMESTAMP without an offset is treated as UTC, the convention used by
 * ingestion and by the result charts (see chart.js toTime()).
 *
 * Loaded before app.js. The parts without DOM are unit-tested in
 * test/dates.test.js.
 */
'use strict';

(function (root) {
    const DATE_ONLY_RE = /^(\d{4})-(\d{2})-(\d{2})$/;
    const TIMESTAMP_RE = /^(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2})(?::(\d{2})(?:\.\d+)?)?(?:\s*(Z|[+-]\d{2}(?::?\d{2})?))?$/i;

    // Intl formatters are comparatively expensive; the grid can format
    // thousands of cells, so create each one once per page load.
    let dateFmt = null;
    let dateTimeFmt = null;

    function dateFormatter() {
        return dateFmt ||= new Intl.DateTimeFormat(undefined, {
            year: 'numeric', month: 'short', day: 'numeric', timeZone: 'UTC',
        });
    }

    function dateTimeFormatter() {
        return dateTimeFmt ||= new Intl.DateTimeFormat(undefined, {
            year: 'numeric', month: 'short', day: 'numeric',
            hour: '2-digit', minute: '2-digit', second: '2-digit',
        });
    }

    /** True for column types whose values are dates or timestamps. */
    function isTemporalType(type) {
        const t = String(type || '').trim();
        return /^DATE$/i.test(t) || /^TIMESTAMP/i.test(t) || /^DATETIME$/i.test(t);
    }

    /**
     * 'date', 'timestamp' or null. A known non-temporal type never converts;
     * without type information (some query results) an ISO-shaped string is
     * recognised by its form.
     */
    function temporalKind(value, type) {
        const t = String(type || '').trim();
        if (t) {
            if (/^DATE$/i.test(t)) return 'date';
            if (/^TIMESTAMP/i.test(t) || /^DATETIME$/i.test(t)) return 'timestamp';
            return null;
        }
        if (typeof value !== 'string') return null;
        const s = value.trim();
        if (TIMESTAMP_RE.test(s)) return 'timestamp';
        if (DATE_ONLY_RE.test(s)) return 'date';
        return null;
    }

    /**
     * Epoch-ms Date for an ISO-like timestamp. DuckDB writes TIMESTAMPTZ
     * offsets as +HH, +HH:MM or Z, and a timestamp without an offset is UTC.
     * Returns null for anything that is not a timestamp.
     */
    function parseInstant(value) {
        let s = String(value).trim().replace(' ', 'T');
        if (!TIMESTAMP_RE.test(s)) return null;
        if (/z$/i.test(s) || /[+-]\d{2}:\d{2}$/.test(s)) {
            // Already carries an explicit offset.
        } else if (/[+-]\d{2}$/.test(s)) s += ':00';
        else if (/[+-]\d{4}$/.test(s)) s = `${s.slice(0, -2)}:${s.slice(-2)}`;
        else s += 'Z';
        const ms = Date.parse(s);
        return Number.isNaN(ms) ? null : new Date(ms);
    }

    /**
     * Locale/time-zone formatted display text, or null when `value` is not a
     * temporal value (so the caller can show the stored text unchanged).
     */
    function fmtTemporal(value, type) {
        if (value === null || value === undefined) return null;
        const kind = temporalKind(value, type);
        if (!kind) return null;
        const s = String(value).trim();
        const dateOnly = DATE_ONLY_RE.exec(s);
        if (kind === 'date' || (dateOnly && !TIMESTAMP_RE.test(s))) {
            if (!dateOnly) return null;
            // Format in UTC: a date is a calendar day, not an instant, so it
            // must not move to the previous/next day in another time zone.
            return dateFormatter().format(new Date(Date.UTC(+dateOnly[1], +dateOnly[2] - 1, +dateOnly[3])));
        }
        const d = parseInstant(s);
        return d ? dateTimeFormatter().format(d) : null;
    }

    /** `value` formatted when it is temporal, otherwise returned unchanged. */
    function displayValue(value, type) {
        const out = fmtTemporal(value, type);
        return out === null ? value : out;
    }

    root.LakeDates = { isTemporalType, fmtTemporal, parseInstant, displayValue };
})(window);
