/**
 * Null-safe text helpers for route props and API string fields.
 *
 * Route props arrive as `null` when absent (app.jsx keeps `useState(null)`),
 * and a destructuring default (`selectedTicker = ''`) only replaces
 * `undefined`, so `selectedTicker.trim()` crashed Timeline, Causal Map and
 * Influence in production. API string fields (label, type, direction, ...)
 * can be `null` for the same reason.
 *
 * Per docs/reference/AVAILABILITY_CONTRACT.md a missing value must render as
 * an explicit "unknown"/"--", never as an empty string that hides the gap.
 */

export const UNKNOWN_TEXT = 'unknown';

/** Trimmed string when `value` is a non-empty string, else `null`. */
export function asText(value) {
    if (typeof value !== 'string') return null;
    const trimmed = value.trim();
    return trimmed === '' ? null : trimmed;
}

/** Normalized ticker from a route prop; `''` when absent or not a string. */
export function routeTicker(value) {
    return (asText(value) || '').toUpperCase();
}

/** Trimmed route/search text; `''` when absent or not a string. */
export function routeText(value) {
    return asText(value) || '';
}

/** Display text for an API field: the value, or an explicit fallback. */
export function textOr(value, fallback = UNKNOWN_TEXT) {
    const text = asText(value);
    return text === null ? fallback : text;
}

/** Upper-cased display text for an API field, or an explicit fallback. */
export function upperOr(value, fallback = UNKNOWN_TEXT.toUpperCase()) {
    const text = asText(value);
    return text === null ? fallback : text.toUpperCase();
}

/** `snake_case` API identifier as words, or an explicit fallback. */
export function humanizeOr(value, fallback = UNKNOWN_TEXT) {
    const text = asText(value);
    return text === null ? fallback : text.replace(/_/g, ' ');
}

/** Lower-cased text for matching/filtering; `''` (matches nothing) when absent. */
export function lowerText(value) {
    return typeof value === 'string' ? value.toLowerCase() : '';
}
