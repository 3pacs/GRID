/**
 * Consistent time/date formatting utilities using Intl.DateTimeFormat.
 *
 * All formatters are module-level singletons — instantiated once, reused on
 * every call.  Import only what you need; tree-shaking will drop the rest.
 */

// "Mar 31, 2026"
const dateFormatter = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', year: 'numeric',
});

// "Mar 31"  (no year — useful for compact labels)
const shortDateFormatter = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric',
});

// "14:05"
const timeFormatter = new Intl.DateTimeFormat('en-US', {
    hour: '2-digit', minute: '2-digit', hour12: false,
});

// "14:05:22"
const timeWithSecondsFormatter = new Intl.DateTimeFormat('en-US', {
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});

// "Mar 31, 14:05"
const dateTimeFormatter = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
});

// "Mon, Mar 31, 2026, 14:05"
const fullDateTimeFormatter = new Intl.DateTimeFormat('en-US', {
    weekday: 'short', month: 'short', day: 'numeric',
    year: 'numeric', hour: '2-digit', minute: '2-digit', hour12: false,
});

// "Monday, March 31, 2026"
const longDateFormatter = new Intl.DateTimeFormat('en-US', {
    weekday: 'long', year: 'numeric', month: 'long', day: 'numeric',
});

// "March 2026"
const monthYearFormatter = new Intl.DateTimeFormat('en-US', {
    month: 'long', year: 'numeric',
});

// UTC clock — "14:05:22"
const utcClockFormatter = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'UTC', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false,
});

const relativeFormatter = new Intl.RelativeTimeFormat('en', { numeric: 'auto' });

/** Coerce value to Date; returns null if invalid. */
function toDate(d) {
    if (d == null) return null;
    const dt = d instanceof Date ? d : new Date(d);
    return isNaN(dt.getTime()) ? null : dt;
}

/** "Mar 31, 2026" */
export function formatDate(d) {
    const dt = toDate(d);
    return dt ? dateFormatter.format(dt) : '';
}

/** "Mar 31" */
export function formatShortDate(d) {
    const dt = toDate(d);
    return dt ? shortDateFormatter.format(dt) : '';
}

/** "14:05" */
export function formatTime(d) {
    const dt = toDate(d);
    return dt ? timeFormatter.format(dt) : '';
}

/** "14:05:22" */
export function formatTimeWithSeconds(d) {
    const dt = toDate(d);
    return dt ? timeWithSecondsFormatter.format(dt) : '';
}

/** "Mar 31, 14:05" */
export function formatDateTime(d) {
    const dt = toDate(d);
    return dt ? dateTimeFormatter.format(dt) : '';
}

/** "Mon, Mar 31, 2026, 14:05" */
export function formatFullDateTime(d) {
    const dt = toDate(d);
    return dt ? fullDateTimeFormatter.format(dt) : '';
}

/** "Monday, March 31, 2026" */
export function formatLongDate(d) {
    const dt = toDate(d);
    return dt ? longDateFormatter.format(dt) : '';
}

/** "March 2026" */
export function formatMonthYear(d) {
    const dt = toDate(d);
    return dt ? monthYearFormatter.format(dt) : '';
}

/** UTC HH:MM:SS string — for live clock displays. */
export function formatUtcClock(d) {
    const dt = toDate(d);
    return dt ? utcClockFormatter.format(dt) : '';
}

/**
 * Human-relative label: "just now", "5m ago", "3h ago", "2d ago".
 * Falls back to formatDateTime for older values.
 */
export function formatRelative(d) {
    const dt = toDate(d);
    if (!dt) return '';
    const diffMs = Date.now() - dt.getTime();
    const diffMin = Math.floor(diffMs / 60_000);
    const diffHr  = Math.floor(diffMs / 3_600_000);
    if (diffMin  <  1) return 'just now';
    if (diffMin  < 60) return relativeFormatter.format(-diffMin,  'minute');
    if (diffHr   < 24) return relativeFormatter.format(-diffHr,   'hour');
    const diffDay = Math.floor(diffMs / 86_400_000);
    if (diffDay  < 30) return relativeFormatter.format(-diffDay,  'day');
    return formatDate(dt);
}
