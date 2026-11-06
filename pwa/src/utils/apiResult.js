/**
 * Helpers for the `api.js` result contract.
 *
 * `api._fetch` never rejects on network/HTTP/parse failure: it resolves an
 * `{ error: true, status, message }` marker. Code that does
 * `api.getX().catch(() => fallback)` therefore never sees the failure and
 * renders the marker as if it were an empty payload ("No data", "0 active").
 * These helpers make the failure explicit, and bound how long a view waits.
 *
 * Kept out of api.js on purpose: many tests replace `../api.js` with a mock
 * that only provides `api`, and views must keep working under those mocks.
 */

/** UI wait bound for slow analytical endpoints (some take 15 s to 2+ min). */
export const VIEW_REQUEST_TIMEOUT_MS = 45000;

/** True for an api.js failure marker (or a backend `{ error: "..." }` body). */
export function isApiError(value) {
    if (!value || typeof value !== 'object') return false;
    return value.error === true || (typeof value.error === 'string' && value.error.trim() !== '');
}

/** Human-readable reason from a failure marker, thrown error, or string. */
export function apiErrorMessage(value, fallback = 'request failed') {
    if (value == null) return fallback;
    if (typeof value === 'string') return value.trim() || fallback;
    if (value instanceof Error) return value.message || fallback;
    if (typeof value === 'object') {
        for (const key of ['message', 'reason', 'detail']) {
            if (typeof value[key] === 'string' && value[key].trim()) return value[key].trim();
        }
        if (typeof value.error === 'string' && value.error.trim()) return value.error.trim();
        if (value.status) return `${fallback} (HTTP ${value.status})`;
    }
    return fallback;
}

/**
 * Resolve `promise`, or a timeout marker after `ms`.
 *
 * The underlying request is NOT aborted: a GET that finishes later still
 * fills api.js's response cache, so a Retry picks it up instead of starting
 * over. Only the view stops waiting and says why.
 */
export function withRequestTimeout(promise, { ms = VIEW_REQUEST_TIMEOUT_MS, label = 'request' } = {}) {
    let timer;
    const timeout = new Promise((resolve) => {
        timer = setTimeout(() => {
            resolve({
                error: true,
                status: 0,
                code: 'request_timeout',
                message: `${label} did not respond within ${Math.round(ms / 1000)} s; the server may still be computing it. Retry shortly.`,
            });
        }, ms);
    });
    return Promise.race([Promise.resolve(promise), timeout]).finally(() => clearTimeout(timer));
}

/**
 * Await an api call with a timeout and throw on a failure marker, so
 * `useAsyncData` / try-catch callers reach their error state.
 */
export async function requireApi(promise, { label = 'request', ms } = {}) {
    const result = await withRequestTimeout(promise, { label, ms });
    if (isApiError(result)) {
        const err = new Error(apiErrorMessage(result, `${label} failed`));
        err.status = result.status;
        err.code = result.code;
        throw err;
    }
    return result;
}

/**
 * Await an optional api call: `{ value, error }` where exactly one is set.
 * Use for secondary inputs whose absence should be reported, not hidden.
 */
export async function optionalApi(promise, { label = 'request', ms } = {}) {
    try {
        const result = await withRequestTimeout(promise, { label, ms });
        if (isApiError(result)) return { value: null, error: apiErrorMessage(result, `${label} failed`) };
        return { value: result, error: null };
    } catch (err) {
        return { value: null, error: apiErrorMessage(err, `${label} failed`) };
    }
}
