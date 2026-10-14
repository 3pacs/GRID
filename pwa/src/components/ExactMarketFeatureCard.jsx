import React from 'react';
import { colors, tokens } from '../styles/shared.js';

export function presentScalar(value) {
    if (value == null) return 'Unavailable';
    if (typeof value === 'string') return value;
    if (typeof value === 'number' && Number.isFinite(value)) return String(value);
    return 'Unsupported scalar';
}

export function marketEvidenceModel(analysis, ticker) {
    const evidence = analysis?.market_features;
    if (!evidence || analysis?.ticker !== ticker || evidence.ticker !== ticker
        || evidence.schema !== 'watchlist-existing-evidence-v1') return null;
    return evidence;
}

export default function ExactMarketFeatureCard({ analysis, ticker, loading, overview }) {
    const evidence = marketEvidenceModel(analysis, ticker);
    const daily = evidence?.daily;
    const receipt = evidence?.briefing_receipt;
    const narrativeReason = evidence && receipt ? receipt.reason ?? null : null;
    const overviewReason = evidence ? overview?.reason ?? null : null;
    const rows = Array.isArray(daily?.rows) ? daily.rows : [];
    return (
        <section aria-label="Existing price evidence" style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.md, padding: '12px', marginBottom: '12px',
        }}>
            <h3 style={{ margin: '0 0 8px', fontSize: '14px', color: colors.text }}>Price evidence</h3>
            <div role="status" style={{ fontSize: '12px', color: colors.textDim }}>
                {loading ? 'Loading existing evidence' : daily ? `Daily evidence: ${daily.status}` : 'Daily evidence unknown: no matching response'}
            </div>
            {daily && <>
                <p>Source: {daily.source ?? 'Unknown'} · As of: {daily.as_of ?? 'Unknown'}
                    {daily.served_from_cache ? ' · Cached response' : ''}</p>
                <p>Freshness: {daily.freshness?.status ?? 'unknown'} — {daily.freshness?.reason ?? 'freshness_policy_not_supplied'}</p>
                <p>{daily.reason} · Returned rows: {daily.returned_row_count}</p>
                <p>Existing scalar representation; not qualified intraday data.</p>
                {rows.length > 0 && <details>
                    <summary>Daily values ({rows.length}) — latest returned: {presentScalar(rows[rows.length - 1]?.value)}</summary>
                    <table><thead><tr><th>As of</th><th>Existing value</th></tr></thead>
                        <tbody>{rows.map((row, i) => <tr key={`${row.as_of ?? 'unknown'}:${i}`}>
                            <td>{row.as_of ?? 'Unknown'}</td><td>{presentScalar(row.value)}</td>
                        </tr>)}</tbody>
                    </table>
                </details>}
            </>}
            <p>Opening range / trade VWAP / HLC3-volume proxy: unavailable — qualified_intraday_result_absent</p>
            {receipt != null
                ? <p>Briefing receipt status: {receipt.status ?? 'Unknown'} · Reason: {narrativeReason == null ? 'Unknown' : String(narrativeReason)}{receipt.as_of ? ` · As of: ${receipt.as_of}` : ''}</p>
                : <p>No briefing receipt supplied by this response.</p>}
            {overviewReason != null && <p>Overview reason: {String(overviewReason)}</p>}
        </section>
    );
}
