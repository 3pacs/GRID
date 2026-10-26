/**
 * GodView (G9) — Fed net liquidity, CFTC positioning and modeled dealer gamma
 * from /api/v1/godview/latest, with honest per-pillar states.
 *
 * Every pillar renders exactly what the API says:
 *   available   — values plus observation date, release, acquisition, basis, provenance
 *   stale       — the same values, greyed, with "stale" and the data's own date
 *   partial     — CFTC only: the markets that have rows; the rest read "--  no data"
 *   unavailable — "--" and the reason (never 0, "neutral" or "stable")
 * GEX is always labelled a modeled estimate, including while unavailable.
 */
import React, { useEffect } from 'react';
import { Eye, RefreshCw } from 'lucide-react';
import useGodViewStore from '../stores/godViewStore.js';
import ErrorState from '../components/ErrorState.jsx';
import GEXProvenance from '../components/GEXProvenance.jsx';
import { colors, shared, tokens } from '../styles/shared.js';

const DASH = '--';

const REASON_TEXT = {
    schema_not_migrated: 'God-view schema not migrated yet (provenance columns and run ledger missing)',
    never_run: 'Writer has never run',
    writer_failed: 'Last writer run failed',
    inputs_missing: 'Last writer run found its inputs missing',
    inputs_stale: 'Last writer run found its inputs stale',
    non_session: 'Last writer run was on a non-session day',
    no_completed_capture: 'No completed options-chain capture for the session',
    no_verified_spot: 'No verified spot price receipt',
    blocked_by_legacy_rows: 'Blocked by unverified legacy rows until they are archived',
    no_rows_available_at_as_of: 'No verified row was available at this time',
    no_row_for_market: 'no data',
    stale: 'stale',
    partial_coverage: 'partial coverage',
};

const STATUS_STYLE = {
    available: { label: 'AVAILABLE', color: colors.green },
    stale: { label: 'STALE', color: colors.yellow },
    partial: { label: 'PARTIAL', color: colors.yellow },
    unavailable: { label: 'UNAVAILABLE', color: colors.textMuted },
};

export function reasonText(reason) {
    if (!reason) return null;
    return REASON_TEXT[reason] || reason.replace(/_/g, ' ');
}

function isNum(v) {
    return typeof v === 'number' && Number.isFinite(v);
}

/** USD millions -> $T / $B / $M; null stays "--". */
export function fmtUsdMillions(v, { signed = false } = {}) {
    if (!isNum(v)) return DASH;
    const sign = v < 0 ? '-' : (signed && v > 0 ? '+' : '');
    const a = Math.abs(v);
    if (a >= 1e6) return `${sign}$${(a / 1e6).toFixed(3)}T`;
    if (a >= 1e3) return `${sign}$${(a / 1e3).toFixed(1)}B`;
    return `${sign}$${a.toFixed(0)}M`;
}

export function fmtNum(v, digits = 2, { signed = false } = {}) {
    if (!isNum(v)) return DASH;
    const s = v.toFixed(digits);
    return signed && v > 0 ? `+${s}` : s;
}

function fmtInt(v) {
    return isNum(v) ? Math.round(v).toLocaleString('en-US') : DASH;
}

function fmtPct(v, digits = 1) {
    return isNum(v) ? `${v.toFixed(digits)}%` : DASH;
}

function fmtLabel(v) {
    return typeof v === 'string' && v ? v.replace(/_/g, ' ').toLowerCase() : DASH;
}

function StatusBadge({ status }) {
    const s = STATUS_STYLE[status] || STATUS_STYLE.unavailable;
    return (
        <span
            data-testid="status-badge"
            style={{ ...shared.badge(`${s.color}33`), color: s.color, fontFamily: colors.mono, fontSize: '10px' }}
        >
            {s.label}
        </span>
    );
}

function Metric({ label, value, dim }) {
    return (
        <div style={{ minWidth: '110px' }}>
            <div style={{ ...shared.value, color: dim ? colors.textMuted : colors.text }}>{value}</div>
            <div style={shared.metricLabel}>{label}</div>
        </div>
    );
}

/** Observation / release / acquisition / basis / provenance / last-run line. */
function ProvenanceLine({ pillar }) {
    const run = pillar.last_run;
    const parts = [];
    if (pillar.as_of) parts.push(`obs ${pillar.as_of}`);
    if (pillar.release_at) parts.push(`published ${pillar.release_at}`);
    if (pillar.available_at) parts.push(`acquired ${pillar.available_at}`);
    if (pillar.availability_basis) parts.push(`basis ${pillar.availability_basis.replace(/_/g, ' ')}`);
    if (pillar.provenance) parts.push(`provenance ${pillar.provenance}`);
    parts.push(run ? `last writer run ${run.status} ${run.finished_at || ''}`.trim() : 'no writer run recorded');
    return (
        <div data-testid="provenance-line" style={{ fontSize: '11px', color: colors.textMuted, marginTop: tokens.space.sm, overflowWrap: 'anywhere' }}>
            {parts.join(' · ')}
        </div>
    );
}

function PillarCard({ title, pillar, testId, children, footer = null }) {
    const status = pillar?.status || 'unavailable';
    const reason = reasonText(pillar?.reason);
    return (
        <section data-testid={testId} data-status={status} style={shared.card}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', flexWrap: 'wrap' }}>
                <div style={{ ...shared.sectionTitle, marginBottom: 0 }}>{title}</div>
                <StatusBadge status={status} />
            </div>
            {status === 'unavailable' ? (
                <div style={{ marginTop: tokens.space.sm }}>
                    <div style={{ ...shared.value, fontSize: '20px', color: colors.textMuted }}>{DASH}</div>
                    <div data-testid="unavailable-reason" style={{ fontSize: '12px', color: colors.textDim }}>
                        Unavailable: {reason || 'reason not reported'}
                    </div>
                </div>
            ) : (
                <>
                    {status !== 'available' && reason && (
                        <div style={{ fontSize: '12px', color: colors.yellow, marginTop: tokens.space.xs }}>
                            {status === 'stale' ? `Stale: latest data ${pillar.as_of}` : reason}
                        </div>
                    )}
                    {children}
                </>
            )}
            {footer}
            {pillar && <ProvenanceLine pillar={pillar} />}
        </section>
    );
}

function FedPillar({ pillar }) {
    const d = pillar?.data;
    const dim = pillar?.status === 'stale';
    return (
        <PillarCard title="FED NET LIQUIDITY" pillar={pillar} testId="pillar-fed">
            {d && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '14px', marginTop: tokens.space.sm }}>
                    <Metric label={`Net liquidity (H.4.1 ${d.obs_date})`} value={fmtUsdMillions(d.net_liquidity_usd_m)} dim={dim} />
                    <Metric label="1-week change" value={fmtUsdMillions(d.delta_1w_m, { signed: true })} dim={dim} />
                    <Metric label="4-week change" value={fmtUsdMillions(d.delta_4w_m, { signed: true })} dim={dim} />
                    <Metric label="Regime (4-week change)" value={fmtLabel(d.liquidity_regime)} dim={dim} />
                    <Metric label="Fed assets (WALCL)" value={fmtUsdMillions(d.walcl_usd_m)} dim={dim} />
                    <Metric label="TGA (WTREGEN)" value={fmtUsdMillions(d.tga_usd_m)} dim={dim} />
                    <Metric label="Reverse repo (RRP)" value={fmtUsdMillions(d.rrp_usd_m)} dim={dim} />
                    <Metric label="RRP % of trailing peak" value={fmtPct(d.rrp_as_pct_of_peak)} dim={dim} />
                </div>
            )}
        </PillarCard>
    );
}

function CftcPillar({ pillar }) {
    const markets = pillar?.data?.markets || [];
    const cov = pillar?.coverage;
    const th = { textAlign: 'right', padding: '4px 6px', fontWeight: 600, color: colors.textMuted, whiteSpace: 'nowrap' };
    const td = { textAlign: 'right', padding: '4px 6px', fontFamily: colors.mono, whiteSpace: 'nowrap' };
    return (
        <PillarCard title="CFTC POSITIONING (NON-COMMERCIAL)" pillar={pillar} testId="pillar-cftc">
            {cov && (
                <div data-testid="cftc-coverage" style={{ fontSize: '12px', color: colors.textDim, marginTop: tokens.space.xs }}>
                    {cov.available + cov.stale} of {cov.tracked} markets reported
                    {cov.stale ? ` (${cov.stale} stale)` : ''}
                </div>
            )}
            {markets.length > 0 && (
                <div style={{ overflowX: 'auto', marginTop: tokens.space.sm }}>
                    <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '12px' }}>
                        <thead>
                            <tr>
                                <th style={{ ...th, textAlign: 'left' }}>Market</th>
                                <th style={th}>Report</th>
                                <th style={th}>Net spec</th>
                                <th style={th}>% OI</th>
                                <th style={th}>z 1y</th>
                                <th style={th}>z 3y</th>
                                <th style={th}>Pctile 3y</th>
                                <th style={{ ...th, textAlign: 'left' }}>Crowding</th>
                            </tr>
                        </thead>
                        <tbody>
                            {markets.map((m) => {
                                const missing = !m.available;
                                const color = missing || m.status === 'stale' ? colors.textMuted : colors.text;
                                return (
                                    <tr key={m.market} data-testid={`cftc-row-${m.market}`} data-status={m.status} style={{ color, borderTop: `1px solid ${colors.border}` }}>
                                        <td style={{ ...td, textAlign: 'left', fontFamily: colors.sans }} title={m.market_name || ''}>
                                            {m.market}
                                            {m.status === 'stale' ? ' (stale)' : ''}
                                        </td>
                                        <td style={td}>{m.report_date || DASH}</td>
                                        <td style={td}>{missing ? DASH : fmtInt(m.noncommercial_net)}</td>
                                        <td style={td}>{missing ? DASH : fmtPct(m.spec_net_pct_oi)}</td>
                                        <td style={td}>{missing ? DASH : fmtNum(m.z_score_1y, 2, { signed: true })}</td>
                                        <td style={td}>{missing ? DASH : fmtNum(m.z_score_3y, 2, { signed: true })}</td>
                                        <td style={td}>{missing ? DASH : fmtPct(m.percentile_3y, 0)}</td>
                                        <td style={{ ...td, textAlign: 'left', fontFamily: colors.sans }}>
                                            {missing ? reasonText(m.reason) : fmtLabel(m.crowding_regime)}
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            )}
        </PillarCard>
    );
}

function GexPillar({ pillar }) {
    const d = pillar?.data;
    const dim = pillar?.status === 'stale';
    const note = pillar?.model_note || d?.model_note;
    // The modeled label is a footer so it shows in every state, unavailable included.
    const footer = (
        <div data-testid="gex-model-note" style={{ fontSize: '11px', color: colors.textDim, marginTop: tokens.space.xs }}>
            {note || 'Modeled estimate; not measured positioning.'}
            {d?.sign_convention ? ` Sign convention: ${d.sign_convention}.` : ''}
        </div>
    );
    return (
        <PillarCard title={`DEALER GAMMA (${pillar?.ticker || 'SPY'}, MODELED)`} pillar={pillar} testId="pillar-gex" footer={footer}>
            {d && (
                <>
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '14px', marginTop: tokens.space.sm }}>
                        <Metric label={`Aggregate GEX (session ${d.obs_date})`} value={fmtNum(d.gex_aggregate, 0)} dim={dim} />
                        <Metric label="Normalized GEX" value={fmtNum(d.gex_normalized, 3)} dim={dim} />
                        <Metric label="Gamma flip" value={fmtNum(d.gamma_flip, 2)} dim={dim} />
                        <Metric label="Regime (modeled)" value={fmtLabel(d.regime)} dim={dim} />
                        <Metric label="Put wall" value={fmtNum(d.put_wall, 2)} dim={dim} />
                        <Metric label="Call wall" value={fmtNum(d.call_wall, 2)} dim={dim} />
                        <Metric label="Reference spot" value={fmtNum(d.spot, 2)} dim={dim} />
                    </div>
                    <GEXProvenance
                        data={{
                            basis: d.basis,
                            chain_snap_date: d.obs_date,
                            chain_capture_completed_at: d.chain_capture_completed_at,
                            spot_source: d.spot_source,
                            spot_basis: d.spot_basis,
                            spot_obs_date: d.spot_obs_date,
                            spot_available_at: d.spot_available_at,
                        }}
                    />
                </>
            )}
        </PillarCard>
    );
}

export default function GodView() {
    const latest = useGodViewStore((s) => s.latest);
    const loading = useGodViewStore((s) => s.loading);
    const error = useGodViewStore((s) => s.error);
    const loadLatest = useGodViewStore((s) => s.loadLatest);

    useEffect(() => {
        loadLatest();
    }, [loadLatest]);

    const pillars = latest?.pillars || {};

    return (
        <div style={shared.container}>
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' }}>
                <div style={{ ...shared.header, display: 'flex', alignItems: 'center', gap: '8px', marginBottom: tokens.space.sm }}>
                    <Eye size={20} /> God View
                </div>
                <button type="button" onClick={() => loadLatest()} style={shared.buttonSmall} disabled={loading} aria-label="Refresh god view">
                    <RefreshCw size={12} />
                </button>
            </div>
            {latest && (
                <div data-testid="godview-as-of" style={{ fontSize: '11px', color: colors.textMuted, marginBottom: tokens.space.md }}>
                    Point in time: {latest.as_of}
                    {latest.as_of_source === 'server_now' ? ' (server now)' : ''}
                    {latest.schema && !latest.schema.migrated ? ' · god-view schema not migrated' : ''}
                </div>
            )}
            {error && <ErrorState title="God view unavailable" error={error} onRetry={() => loadLatest()} />}
            {loading && !latest && !error && (
                <div data-testid="godview-loading" style={{ color: colors.textMuted, fontSize: '13px' }}>Loading god view…</div>
            )}
            {latest && (
                <>
                    <FedPillar pillar={pillars.fed_liquidity} />
                    <CftcPillar pillar={pillars.cftc} />
                    <GexPillar pillar={pillars.dealer_gex} />
                </>
            )}
        </div>
    );
}
