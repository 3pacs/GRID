import React, { useEffect, useRef, useState } from 'react';
import { Lock, RefreshCw } from 'lucide-react';
import { api } from '../api.js';

/**
 * Private Schwab quote check (admin only; server flag gated).
 *
 * Newly wired 2026-10-31 over /api/v1/schwab/private. Renders the availability
 * contract as-is: a row is labelled LIVE only when the server says
 * `live === true` and `quote_status === 'fresh'` AND the row has not aged past
 * `stale_after_seconds` since the server evaluated it (r2). Ageing uses the
 * server's `age_seconds` plus monotonic elapsed time measured locally with
 * `performance.now()` from the moment the request is **sent** (r3), so the
 * request/response transit is inside the bound: a value that is 5 s old at the
 * server and takes 30 s to arrive is 35 s old on first render and is shown STALE,
 * never LIVE. A local timer re-evaluates every second and is cleared on unmount,
 * on error and when the rows are replaced. Nothing is polled from the provider
 * and nothing ever promotes back to LIVE: once aged out, a row stays STALE until
 * the admin reads again.
 *
 * Clock trust (r4): elapsed time is only ever trusted from `performance.now()`.
 * If that clock is unavailable or non-finite, if the request start is not
 * finite, or if the clock reads earlier than the request start (rollback), the
 * elapsed value is `null` = untrusted, and a fresh row is shown STALE with the
 * reason "local clock untrusted; freshness unbounded" and is never LIVE. There
 * is no Date.now fallback. Untrusted is sticky until the next successful read. Missing values render as "—", never 0; counts that JSON
 * cannot carry exactly use the server's `*_exact` decimal strings.
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

/** Monotonic milliseconds, or NaN when no trustworthy monotonic clock exists (no Date.now fallback). */
export function monotonicNow() {
    if (typeof performance === 'undefined' || typeof performance.now !== 'function') return NaN;
    let value;
    try { value = performance.now(); } catch { return NaN; }
    return typeof value === 'number' && Number.isFinite(value) ? value : NaN;
}

/**
 * Seconds elapsed on the monotonic clock since `startMs`, or `null` when the
 * interval cannot be trusted: non-finite start, no monotonic clock, non-finite
 * reading, or a reading earlier than the start (clock rollback). Never 0 for
 * an unknown interval. Pure apart from the clock read.
 */
export function elapsedSince(startMs) {
    if (typeof startMs !== 'number' || !Number.isFinite(startMs)) return null;
    const now = monotonicNow();
    if (!Number.isFinite(now)) return null;
    const seconds = (now - startMs) / 1000;
    if (!Number.isFinite(seconds) || seconds < 0) return null;
    return seconds;
}

/** True when `elapsedSeconds` is a trusted, finite, non-negative interval. */
export function isTrustedElapsed(elapsedSeconds) {
    return typeof elapsedSeconds === 'number' && Number.isFinite(elapsedSeconds) && elapsedSeconds >= 0;
}

function num(value, digits = 2) {
    if (typeof value === 'number' && Number.isFinite(value)) return value.toFixed(digits);
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
    const serverAge = typeof freshness.age_seconds === 'number' && Number.isFinite(freshness.age_seconds) ? freshness.age_seconds : null;
    const staleAfter = typeof freshness.stale_after_seconds === 'number' && Number.isFinite(freshness.stale_after_seconds) ? freshness.stale_after_seconds : null;
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
            ? 'local clock untrusted; freshness unbounded'
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
    const [elapsed, setElapsed] = useState(null); // seconds since the request was sent, or null = untrusted
    const requestStartRef = useRef(null); // monotonic ms when the request was SENT (r3)

    useEffect(() => {
        if (rows.length === 0) return undefined;
        const tick = () => {
            setElapsed(prev => {
                if (prev === null) return null; // untrusted is sticky until the next successful read
                const seconds = elapsedSince(requestStartRef.current);
                if (seconds === null) return null; // rollback or clock loss after first render: fail closed
                return seconds > prev ? seconds : prev; // monotonic: never moves back
            });
        };
        const id = setInterval(tick, AGE_TICK_MS);
        return () => clearInterval(id);
    }, [rows]);

    const read = async () => {
        setBusy(true);
        const start = monotonicNow(); // before the request leaves: transit time counts as age; NaN = untrusted
        try {
            const result = await api.getSchwabPrivateQuotes(symbols);
            if (!result || result.error) {
                requestStartRef.current = null;
                setRows([]);
                setElapsed(null);
                setMessage(describeError(result));
                return;
            }
            const view = result.view && typeof result.view === 'object' ? result.view : {};
            const viewRows = view.rows && typeof view.rows === 'object' ? view.rows : {};
            requestStartRef.current = Number.isFinite(start) ? start : null;
            setElapsed(elapsedSince(start)); // first render includes the transit; null when the clock cannot be trusted
            setRows(Object.keys(viewRows).sort().map(key => viewRows[key]));
            setMessage(`Evaluated at ${typeof view.evaluated_at === 'string' ? view.evaluated_at : '—'} · source ${typeof view.source === 'string' ? view.source : '—'} · redistribution ${typeof view.redistribution === 'string' ? view.redistribution : '—'}`);
        } finally {
            setBusy(false);
        }
    };

    const described = rows.map(row => describeRow(row, elapsed));

    return (
        <div className="ty-oracle-card ty-schwab-private-card" data-testid="schwab-private-quote">
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
