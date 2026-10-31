import React, { useState } from 'react';
import { Lock, RefreshCw } from 'lucide-react';
import { api } from '../api.js';

/**
 * Private Schwab quote check (admin only; server flag gated).
 *
 * Newly wired 2026-10-31 over /api/v1/schwab/private. Renders the availability
 * contract as-is: a row is labelled LIVE only when the server says
 * `live === true` and `quote_status === 'fresh'`; every other status shows its
 * own label (STALE / DELAYED / MISSING / INVALID / UNAVAILABLE) with the
 * provider quote time (as-of) and GRID receipt time (vintage). Missing values
 * render as "—", never 0. Nothing here is cached or shared.
 */

export const STATUS_LABELS = {
    fresh: 'FRESH',
    stale: 'STALE',
    delayed: 'DELAYED',
    missing: 'MISSING',
    invalid: 'INVALID',
    unavailable: 'UNAVAILABLE',
};

function num(value, digits = 2) {
    if (typeof value === 'number' && Number.isFinite(value)) return value.toFixed(digits);
    if (typeof value === 'bigint') return value.toString();
    return '—';
}

function count(value) {
    if (typeof value === 'number' && Number.isSafeInteger(value)) return String(value);
    if (typeof value === 'number' && Number.isInteger(value)) return 'n/a (>2^53)';
    return '—';
}

export function describeRow(row) {
    if (!row || typeof row !== 'object') {
        return { symbol: '?', label: 'UNAVAILABLE', live: false, measured: false, reason: 'no row' };
    }
    const status = typeof row.quote_status === 'string' ? row.quote_status : 'unavailable';
    const live = row.live === true && status === 'fresh';
    const measured = status === 'fresh' || status === 'stale' || status === 'delayed';
    const freshness = row.freshness && typeof row.freshness === 'object' ? row.freshness : {};
    return {
        symbol: typeof row.symbol === 'string' ? row.symbol : '?',
        providerSymbol: typeof row.provider_symbol === 'string' ? row.provider_symbol : null,
        label: live ? 'LIVE' : (STATUS_LABELS[status] || 'UNAVAILABLE'),
        live,
        measured,
        reason: typeof row.reason === 'string' ? row.reason : null,
        asOf: typeof freshness.as_of === 'string' ? freshness.as_of : null,
        vintage: typeof freshness.vintage === 'string' ? freshness.vintage : null,
        ageSeconds: typeof freshness.age_seconds === 'number' ? freshness.age_seconds : null,
        last: measured ? num(row.last) : '—',
        bid: measured ? num(row.bid) : '—',
        ask: measured ? num(row.ask) : '—',
        bidSize: measured ? count(row.bid_size) : '—',
        askSize: measured ? count(row.ask_size) : '—',
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

    const read = async () => {
        setBusy(true);
        try {
            const result = await api.getSchwabPrivateQuotes(symbols);
            if (!result || result.error) {
                setRows([]);
                setMessage(describeError(result));
                return;
            }
            const view = result.view && typeof result.view === 'object' ? result.view : {};
            const viewRows = view.rows && typeof view.rows === 'object' ? view.rows : {};
            setRows(Object.keys(viewRows).sort().map(key => describeRow(viewRows[key])));
            setMessage(`Evaluated at ${typeof view.evaluated_at === 'string' ? view.evaluated_at : '—'} · source ${typeof view.source === 'string' ? view.source : '—'} · redistribution ${typeof view.redistribution === 'string' ? view.redistribution : '—'}`);
        } finally {
            setBusy(false);
        }
    };

    return (
        <div className="ty-oracle-card ty-schwab-private-card" data-testid="schwab-private-quote">
            <div className="ty-card-title">
                <Lock size={17} />
                <strong>Private Schwab quote check</strong>
            </div>
            <p className="ty-muted">{message}</p>
            {rows.length > 0 && (
                <table className="ty-schwab-private-rows">
                    <thead>
                        <tr><th>Symbol</th><th>Status</th><th>Last</th><th>Bid</th><th>Ask</th><th>As of (provider)</th><th>Received (GRID)</th></tr>
                    </thead>
                    <tbody>
                        {rows.map(row => (
                            <tr key={row.symbol} data-status={row.label.toLowerCase()}>
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
