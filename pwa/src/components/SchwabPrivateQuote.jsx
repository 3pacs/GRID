import React, { useEffect, useRef, useState } from 'react';
import { Lock, RefreshCw } from 'lucide-react';
import { api } from '../api.js';

/**
 * Private Schwab quote check (admin only; server flag gated).
 *
 * Newly wired 2026-10-31 over /api/v1/schwab/private. Renders the availability
 * contract as-is: a row is labelled LIVE only when the server says
 * `live === true` and `quote_status === 'fresh'`, the per-request timing state is
 * trusted, and `server age_seconds + elapsed <= stale_after_seconds`.
 *
 * Timing (r3–r5). `elapsed` is a conservative whole-request bound: the monotonic
 * clock (`performance.now`) is read before the request is sent (`start`), again
 * when the response arrives, and on every 1 s timer tick. Each observation is
 * checked against the latest observed valid reading for the same request
 * (`last`): a non-finite reading, a reading before `start`, or any decrease
 * against `last` marks the request's timing untrusted; a reading equal to `last`
 * is trusted and never lowers `elapsed`; a greater reading advances it. An
 * untrusted state is sticky for that request and only a later successful,
 * independently timed read resets it. There is no Date.now fallback. A fresh row
 * with untrusted timing is shown STALE with the reason "local clock untrusted;
 * freshness unbounded" and is never LIVE; nothing ever promotes back to LIVE.
 *
 * Trade-off: because `elapsed` spans the whole request, part of the interval it
 * adds may already be inside the server's `age_seconds`; the bound therefore ages
 * a row out early rather than late. That is deliberate.
 *
 * Observability boundary: a clock can only be checked at the readings actually
 * taken. A backwards movement that recovers between two observations, or a
 * monotonic clock that pauses during suspend, is not detectable from these
 * samples and is not claimed to be covered. No LIVE guarantee is made for those
 * cases; if one is needed it requires a separate trustworthy bound or marking
 * the quote non-LIVE on resume, neither of which this card provides.
 *
 * Requests do not overlap (the button is disabled while one is in flight) and a
 * request sequence number isolates a late response or timer from a newer
 * request; a response settling after unmount installs no timer and updates no
 * state. Missing values render as "—", never 0; counts use the server's `*_exact`
 * decimal strings.
 */

export const STATUS_LABELS = {
    fresh: 'FRESH',
    stale: 'STALE',
    delayed: 'DELAYED',
    missing: 'MISSING',
    invalid: 'INVALID',
    unavailable: 'UNAVAILABLE',
};

export const AGE_TICK_MS = 1000;
export const CLOCK_UNTRUSTED_REASON = 'local clock untrusted; freshness unbounded';

/** Monotonic milliseconds, or NaN when no trustworthy monotonic clock exists (no Date.now fallback). */
export function monotonicNow() {
    if (typeof performance === 'undefined' || typeof performance.now !== 'function') return NaN;
    let value;
    try { value = performance.now(); } catch { return NaN; }
    return typeof value === 'number' && Number.isFinite(value) ? value : NaN;
}

function finiteNumber(value) {
    return typeof value === 'number' && Number.isFinite(value);
}

/** True when `elapsedSeconds` is a trusted, finite, non-negative interval. */
export function isTrustedElapsed(elapsedSeconds) {
    return finiteNumber(elapsedSeconds) && elapsedSeconds >= 0;
}

/**
 * Per-request timing state (pure). `start` and `last` are monotonic ms readings;
 * `elapsed` is seconds since `start` (never decreasing); `trusted` is false once
 * any observation is invalid or decreasing, and stays false (sticky).
 */
export function startTiming(startMs) {
    if (!finiteNumber(startMs)) {
        return { start: null, last: null, elapsed: null, trusted: false, reason: 'invalid start reading' };
    }
    return { start: startMs, last: startMs, elapsed: 0, trusted: true, reason: null };
}

/** Observe one more clock reading for a request. Pure; returns the next state. */
export function observeTiming(state, readingMs) {
    if (!state || typeof state !== 'object') {
        return { start: null, last: null, elapsed: null, trusted: false, reason: 'no timing state' };
    }
    if (!state.trusted) return state; // sticky
    const untrusted = reason => ({ ...state, elapsed: null, trusted: false, reason });
    if (!finiteNumber(readingMs)) return untrusted('invalid clock reading');
    if (readingMs < state.start) return untrusted('reading before request start');
    if (readingMs < state.last) return untrusted('reading decreased against previous reading');
    const elapsed = (readingMs - state.start) / 1000;
    return { ...state, last: readingMs, elapsed: elapsed > state.elapsed ? elapsed : state.elapsed };
}

/**
 * Seconds elapsed on the monotonic clock since `startMs`, or `null` when the
 * interval cannot be trusted (non-finite start, no clock, non-finite reading, or
 * a reading earlier than the start). Convenience over startTiming/observeTiming
 * for a single observation; never 0 for an unknown interval.
 */
export function elapsedSince(startMs) {
    return observeTiming(startTiming(startMs), monotonicNow()).elapsed;
}

function num(value, digits = 2) {
    if (finiteNumber(value)) return value.toFixed(digits);
    return '—';
}

export function count(value, exact) {
    if (typeof exact === 'string' && /^[0-9]+$/.test(exact)) return exact;
    if (typeof value === 'number' && Number.isSafeInteger(value)) return String(value);
    if (typeof value === 'number' && Number.isInteger(value)) return 'n/a (>2^53)';
    return '—';
}

/**
 * Describe one server row at `elapsedSeconds` after the request was sent. Pure.
 * A fresh row whose `age_seconds + elapsed` exceeds `stale_after_seconds`
 * is shown STALE (aged out locally) and is never live. An untrusted
 * `elapsedSeconds` (null, NaN, negative, non-finite) is a clock failure: a
 * fresh row is then shown STALE with an explicit reason and is never live.
 * Nothing promotes.
 */
export function describeRow(row, elapsedSeconds = null) {
    if (!row || typeof row !== 'object') {
        return { symbol: '?', label: 'UNAVAILABLE', live: false, measured: false, reason: 'no row', agedOut: false, clockUntrusted: false };
    }
    const status = typeof row.quote_status === 'string' ? row.quote_status : 'unavailable';
    const freshness = row.freshness && typeof row.freshness === 'object' ? row.freshness : {};
    const trusted = isTrustedElapsed(elapsedSeconds);
    const serverAge = finiteNumber(freshness.age_seconds) ? freshness.age_seconds : null;
    const staleAfter = finiteNumber(freshness.stale_after_seconds) ? freshness.stale_after_seconds : null;
    const effectiveAge = serverAge === null || !trusted ? null : serverAge + elapsedSeconds;
    const clockUntrusted = status === 'fresh' && !trusted;
    // A fresh row with unknown age, unknown gate or untrusted elapsed time cannot be bounded: never live.
    const agedOut = status === 'fresh' && (clockUntrusted || effectiveAge === null || staleAfter === null || effectiveAge > staleAfter);
    const effectiveStatus = agedOut ? 'stale' : status;
    const live = row.live === true && status === 'fresh' && !agedOut;
    const measured = effectiveStatus === 'fresh' || effectiveStatus === 'stale' || effectiveStatus === 'delayed';
    return {
        symbol: typeof row.symbol === 'string' ? row.symbol : '?',
        providerSymbol: typeof row.provider_symbol === 'string' ? row.provider_symbol : null,
        label: live ? 'LIVE' : (STATUS_LABELS[effectiveStatus] || 'UNAVAILABLE'),
        live,
        measured,
        agedOut,
        clockUntrusted,
        reason: clockUntrusted
            ? CLOCK_UNTRUSTED_REASON
            : (agedOut ? 'aged out since evaluation' : (typeof row.reason === 'string' ? row.reason : null)),
        asOf: typeof freshness.as_of === 'string' ? freshness.as_of : null,
        vintage: typeof freshness.vintage === 'string' ? freshness.vintage : null,
        evaluatedAt: typeof freshness.evaluated_at === 'string' ? freshness.evaluated_at : null,
        ageSeconds: effectiveAge,
        last: measured ? num(row.last) : '—',
        bid: measured ? num(row.bid) : '—',
        ask: measured ? num(row.ask) : '—',
        bidSize: measured ? count(row.bid_size, row.bid_size_exact) : '—',
        askSize: measured ? count(row.ask_size, row.ask_size_exact) : '—',
    };
}

export function describeError(result) {
    const status = result && typeof result.status === 'number' ? result.status : 0;
    if (status === 401 || status === 403) return 'Admin sign-in required for private quotes.';
    if (status === 503) return 'Private quotes are disabled or not configured on the server.';
    if (status === 422) return 'Symbol list rejected by the server.';
    if (status === 0) return 'Network error reaching GRID.';
    return `Private quote request failed (${status}).`;
}

export default function SchwabPrivateQuote({ symbols = ['SPY'] }) {
    const [busy, setBusy] = useState(false);
    const [message, setMessage] = useState('Not read yet. Private, admin-only; never published.');
    const [rows, setRows] = useState([]);
    const [elapsed, setElapsed] = useState(null); // seconds since the accepted request was sent, or null = untrusted
    const timingRef = useRef(null);   // timing state of the accepted (latest) request
    const requestSeqRef = useRef(0);  // isolates late responses/timers of older requests
    const mountedRef = useRef(false);

    useEffect(() => {
        mountedRef.current = true;
        return () => { mountedRef.current = false; };
    }, []);

    useEffect(() => {
        if (rows.length === 0) return undefined;
        const seq = requestSeqRef.current;
        const tick = () => {
            if (!mountedRef.current || seq !== requestSeqRef.current) return;
            const next = observeTiming(timingRef.current, monotonicNow());
            timingRef.current = next;
            setElapsed(next.elapsed);
        };
        const id = setInterval(tick, AGE_TICK_MS);
        return () => clearInterval(id);
    }, [rows]);

    const read = async () => {
        if (busy) return; // requests never overlap
        const seq = requestSeqRef.current + 1;
        requestSeqRef.current = seq;
        const timingAtStart = startTiming(monotonicNow()); // before the request leaves
        setBusy(true);
        let result;
        try {
            result = await api.getSchwabPrivateQuotes(symbols);
        } catch {
            result = { error: true, status: 0 };
        }
        if (!mountedRef.current || seq !== requestSeqRef.current) return; // settled after unmount or superseded: touch nothing
        if (!result || result.error) {
            timingRef.current = null;
            setRows([]);
            setElapsed(null);
            setMessage(describeError(result));
            setBusy(false);
            return;
        }
        const view = result.view && typeof result.view === 'object' ? result.view : {};
        const viewRows = view.rows && typeof view.rows === 'object' ? view.rows : {};
        const timing = observeTiming(timingAtStart, monotonicNow()); // response reading is the second observation
        timingRef.current = timing;
        setElapsed(timing.elapsed);
        setRows(Object.keys(viewRows).sort().map(key => viewRows[key]));
        setMessage(`Evaluated at ${typeof view.evaluated_at === 'string' ? view.evaluated_at : '—'} · source ${typeof view.source === 'string' ? view.source : '—'} · redistribution ${typeof view.redistribution === 'string' ? view.redistribution : '—'}`);
        setBusy(false);
    };

    const described = rows.map(row => describeRow(row, elapsed));
    const timingReason = timingRef.current && !timingRef.current.trusted ? timingRef.current.reason : null;

    return (
        <div className="ty-oracle-card ty-schwab-private-card" data-testid="schwab-private-quote" data-timing-reason={timingReason || ''}>
            <div className="ty-card-title">
                <Lock size={17} />
                <strong>Private Schwab quote check</strong>
            </div>
            <p className="ty-muted">{message}</p>
            {described.length > 0 && (
                <table className="ty-schwab-private-rows">
                    <thead>
                        <tr><th>Symbol</th><th>Status</th><th>Last</th><th>Bid</th><th>Ask</th><th>As of (provider)</th><th>Received (GRID)</th></tr>
                    </thead>
                    <tbody>
                        {described.map(row => (
                            <tr key={row.symbol} data-status={row.label.toLowerCase()} data-aged-out={row.agedOut ? 'true' : 'false'} data-clock-untrusted={row.clockUntrusted ? 'true' : 'false'}>
                                <td>{row.symbol}{row.providerSymbol && row.providerSymbol !== row.symbol ? ` (${row.providerSymbol})` : ''}</td>
                                <td><span className={`ty-schwab-badge${row.live ? ' is-live' : ''}`}>{row.label}</span>{row.reason ? ` ${row.reason}` : ''}</td>
                                <td>{row.last}</td>
                                <td>{row.bid}</td>
                                <td>{row.ask}</td>
                                <td>{row.asOf || '—'}</td>
                                <td>{row.vintage || '—'}</td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
            <button onClick={read} disabled={busy}>
                <RefreshCw size={16} />
                {busy ? 'Reading…' : 'Read private quote'}
            </button>
        </div>
    );
}
