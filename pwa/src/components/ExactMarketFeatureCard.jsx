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

function calendarDay(value) {
    if (typeof value !== 'string' || !/^\d{4}-\d{2}-\d{2}$/.test(value)
        || value.startsWith('0000-')) return null;
    const instant = Date.parse(`${value}T00:00:00Z`);
    return Number.isFinite(instant) && new Date(instant).toISOString().slice(0, 10) === value
        ? value : null;
}

function utcCutoffDay(value) {
    if (typeof value !== 'string') return null;
    const parts = value.match(/^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,6})?(Z|[+-]\d{2}:\d{2})$/);
    if (!parts || !calendarDay(parts[1]) || Number(parts[2]) > 23
        || Number(parts[3]) > 59 || Number(parts[4]) > 59) return null;
    if (parts[5] !== 'Z' && (Number(parts[5].slice(1, 3)) > 23 || Number(parts[5].slice(4)) > 59)) return null;
    const instant = Date.parse(value);
    return Number.isFinite(instant) ? calendarDay(new Date(instant).toISOString().slice(0, 10)) : null;
}

function storedDateProof(item, snapshot) {
    const start = calendarDay(snapshot.start_date), cutoff = utcCutoffDay(snapshot.as_of);
    const observation = calendarDay(item.obs_date), release = calendarDay(item.release_date);
    const vintage = calendarDay(item.vintage_date);
    // Stored date columns have day precision. No relative publication/clock
    // ordering or freshness policy is inferred from their consistency.
    return Boolean(start && cutoff && observation && release && vintage
        && start <= observation && observation <= cutoff && release <= cutoff && vintage <= cutoff);
}

function dateDiagnostic(value) {
    return value == null ? 'Unknown' : typeof value === 'string' ? value : 'Invalid date';
}

export function storedSourceModel(analysis, ticker) {
    const snapshot = marketEvidenceModel(analysis, ticker)?.stored_source_evidence;
    if (!snapshot || snapshot.schema !== 'stored-source-snapshot-v1'
        || snapshot.ticker !== ticker || !Array.isArray(snapshot.selections)
        || snapshot.selections.length > 7) return null;
    const binding = snapshot.request_binding;
    if (!binding || binding.ticker !== ticker || binding.as_of !== snapshot.as_of
        || binding.start_date !== snapshot.start_date || binding.vintage_policy !== snapshot.vintage_policy
        || !Array.isArray(binding.selections) || binding.selections.length > 7) return null;
    if (snapshot.selections.length && (!utcCutoffDay(snapshot.as_of) || !calendarDay(snapshot.start_date)
        || !['FIRST_RELEASE', 'LATEST_AS_OF'].includes(snapshot.vintage_policy))) return null;
    const keys = ['source_key', 'scope', 'ticker_or_cusip', 'series_id', 'expected_source_name', 'feature_id', 'feature_name'];
    const seen = new Set();
    for (const item of snapshot.selections) {
        const selection = binding.selections.find(candidate => candidate.source_key === item.source_key);
        if (!selection || seen.has(item.source_key)
            || keys.some(key => (selection[key] ?? null) !== (item[key] ?? null))) return null;
        seen.add(item.source_key);
    }
    return snapshot;
}

export default function ExactMarketFeatureCard({ analysis, ticker, loading, overview }) {
    const evidence = marketEvidenceModel(analysis, ticker);
    const daily = evidence?.daily;
    const receipt = evidence?.briefing_receipt;
    const stored = storedSourceModel(analysis, ticker);
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
            {evidence?.stored_source_evidence && !stored && <p>Stored source evidence unavailable — response_context_mismatch</p>}
            {stored && <section aria-label="Stored source context">
                <h4>Stored source context</h4>
                <p>Requested cutoff: {dateDiagnostic(stored.as_of)} · Observation range starts: {dateDiagnostic(stored.start_date)} · Vintage policy: {stored.vintage_policy ?? 'Unknown'}</p>
                <p>Declared stored observations, not currentness, provider publication or qualified intraday proof.</p>
                {stored.reason && <p>Stored read: {stored.status ?? 'unavailable'} — {stored.reason}</p>}
                {stored.selections.map((item, index) => {
                    const datesValid = storedDateProof(item, stored);
                    const sourceValid = item.status === 'available' && item.reason === 'resolved_observation'
                        && item.actual_source?.name != null
                        && item.actual_source.name === item.expected_source_name;
                    const showValue = sourceValid && datesValid;
                    const refusal = sourceValid && !datesValid ? 'invalid_date_context' : 'invalid_source_context';
                    return <section key={`${item.source_key}:${index}`} aria-label={`Stored source ${item.source_key}`}>
                        <p>Source key: {item.source_key} · Scope: {item.scope ?? 'Unknown'} · Selection: {item.ticker_or_cusip ?? 'Not supplied'}</p>
                        <p>Status: {item.status === 'available' && !showValue ? 'unavailable' : item.status ?? 'unavailable'} — {item.status === 'available' && !showValue ? refusal : item.reason ?? 'Unknown'}</p>
                        <p>Stored value: {showValue ? presentScalar(item.value) : 'Unavailable'}</p>
                        <p>Series: {item.series_id ?? 'Unknown'} · Feature: {item.feature_name ?? 'Unknown'}</p>
                        <p>Resolved source: {item.actual_source?.name ?? 'Unknown'} · Expected source: {item.expected_source_name ?? 'Unknown'}</p>
                        <p>Observation date: {dateDiagnostic(item.obs_date)} · Storage release date: {dateDiagnostic(item.release_date)} · Vintage date: {dateDiagnostic(item.vintage_date)}</p>
                        <p>Freshness: {item.freshness?.status ?? 'unknown'} — {item.freshness?.reason ?? 'freshness_policy_absent'}</p>
                        <p>Conflict: {item.conflict_flag == null ? 'Unknown' : String(item.conflict_flag)} · Storage clock: {item.storage_clock ?? 'Unknown'} · Lineage: {item.lineage ?? 'Unknown'}</p>
                    </section>;
                })}
            </section>}
            <p>Opening range / trade VWAP / HLC3-volume proxy: unavailable — qualified_intraday_result_absent</p>
            {receipt != null
                ? <p>Briefing receipt status: {receipt.status ?? 'Unknown'} · Reason: {narrativeReason == null ? 'Unknown' : String(narrativeReason)}{receipt.as_of ? ` · As of: ${receipt.as_of}` : ''}</p>
                : <p>No briefing receipt supplied by this response.</p>}
            {overviewReason != null && <p>Overview reason: {String(overviewReason)}</p>}
        </section>
    );
}
