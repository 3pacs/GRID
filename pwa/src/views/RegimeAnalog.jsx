/**
 * RegimeAnalog — Conditional forecast engine view.
 *
 * Shows the current macro state vector, matched historical episodes,
 * conditional outcome distributions, regime classification, and
 * TimesFM foundation model comparison — all side by side.
 */
import React, { useState, useEffect, useCallback, useMemo } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

// ── Axis labels ─────────────────────────────────────────────────────────

const AXIS_COLORS = {
    risk_appetite: { risk_on: '#10B981', risk_off: '#EF4444', neutral: '#F59E0B' },
    monetary_policy: { tightening: '#EF4444', easing: '#10B981', holding: '#3B82F6' },
    economic_cycle: { expansion: '#10B981', contraction: '#EF4444', late_cycle: '#F97316', early_cycle: '#06B6D4', mixed: '#8B5CF6' },
    stress_level: { stress: '#EF4444', complacency: '#F59E0B', normal: '#10B981' },
    data_credibility: { credible: '#10B981', moderate_divergence: '#F59E0B', high_divergence: '#EF4444' },
};

const AXIS_LABELS = {
    risk_appetite: 'Risk Appetite',
    monetary_policy: 'Monetary Policy',
    economic_cycle: 'Economic Cycle',
    stress_level: 'Stress Level',
    data_credibility: 'Data Credibility',
};

const DIRECTION_COLORS = {
    BULLISH: colors.green,
    BEARISH: colors.red,
    NEUTRAL: colors.textMuted,
};

const CONFIDENCE_COLORS = {
    HIGH: colors.green,
    MEDIUM: colors.yellow,
    LOW: '#F97316',
    UNRELIABLE: colors.red,
};

// ── Styles ──────────────────────────────────────────────────────────────

const s = {
    container: { padding: tokens.space.lg, maxWidth: '1200px', margin: '0 auto', minHeight: '100vh' },
    title: { fontSize: '22px', fontWeight: 800, color: '#E8F0F8', fontFamily: mono, letterSpacing: '3px' },
    subtitle: { fontSize: '12px', color: colors.textMuted, fontFamily: mono, letterSpacing: '1px', marginTop: '4px' },
    sectionTitle: { ...shared.sectionTitle, marginTop: '24px', marginBottom: '10px', fontSize: '11px' },
    tabs: { ...shared.tabs, marginBottom: '4px', marginTop: '20px' },
    tab: (active) => shared.tab(active),

    // Regime bar
    regimeBar: {
        display: 'flex', gap: '8px', flexWrap: 'wrap', marginBottom: '16px',
    },
    regimeChip: (color) => ({
        padding: '8px 14px', borderRadius: tokens.radius.md,
        background: `${color}15`, border: `1px solid ${color}40`,
        fontFamily: mono, fontSize: '11px', fontWeight: 700,
        color, textTransform: 'uppercase', letterSpacing: '1px',
    }),

    // Score row
    scoreRow: {
        display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)',
        gap: '10px', marginBottom: '16px',
    },
    scoreCard: {
        ...shared.cardGradient,
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', padding: '14px 10px', textAlign: 'center',
    },
    bigNumber: { fontSize: '24px', fontWeight: 800, fontFamily: mono, lineHeight: 1.1 },
    bigLabel: { fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px', color: colors.textMuted, marginTop: '4px', fontFamily: mono },

    // Forecast table
    forecastTable: { width: '100%', borderCollapse: 'collapse', fontFamily: mono, fontSize: '11px' },
    th: {
        padding: '8px 10px', textAlign: 'right', fontSize: '9px', fontWeight: 700,
        letterSpacing: '1px', color: colors.textMuted, borderBottom: `1px solid ${colors.border}`,
    },
    td: (highlight) => ({
        padding: '6px 10px', textAlign: 'right', fontSize: '12px',
        color: highlight ? '#E8F0F8' : colors.textDim,
        fontWeight: highlight ? 700 : 400,
        borderBottom: `1px solid ${colors.borderSubtle}`,
    }),

    // Episode card
    episodeCard: {
        ...shared.card, padding: '10px 14px', marginBottom: '4px',
        display: 'grid', gridTemplateColumns: '100px 1fr 80px 80px',
        alignItems: 'center', gap: '10px', cursor: 'pointer',
        transition: 'all 0.15s ease',
    },

    // State vector bar
    dimBar: {
        display: 'flex', alignItems: 'center', gap: '6px',
        padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    dimName: { fontSize: '10px', fontFamily: mono, color: colors.textDim, width: '180px', flexShrink: 0 },
    dimBarFill: (val, maxVal) => {
        const pct = Math.min(Math.abs(val || 0) / (maxVal || 1) * 100, 100);
        const isNeg = (val || 0) < 0;
        return {
            height: '14px', borderRadius: '2px',
            width: `${pct}%`,
            background: isNeg ? `${colors.red}60` : `${colors.accent}60`,
            marginLeft: isNeg ? 'auto' : '0',
        };
    },

    // Loading
    loadingBar: { height: '2px', background: colors.bg, borderRadius: '1px', marginBottom: '16px', overflow: 'hidden' },
    loadingFill: { height: '100%', background: colors.accent, borderRadius: '1px', animation: 'loadSlide 1.5s ease infinite', width: '40%' },
};


// ── Helper components ───────────────────────────────────────────────────

function ReturnCell({ value, isAbsolute, ticker }) {
    if (value == null) return <td style={s.td(false)}>--</td>;
    const formatted = isAbsolute
        ? `${value >= 0 ? '+' : ''}${value.toFixed(1)}pts`
        : `${(value * 100).toFixed(1)}%`;
    const color = value > 0.005 ? colors.green : value < -0.005 ? colors.red : colors.textDim;
    return <td style={{ ...s.td(true), color }}>{formatted}</td>;
}

function AgreementBadge({ score, n }) {
    const pct = Math.round(score * 100);
    const color = pct >= 80 ? colors.green : pct >= 60 ? colors.yellow : pct >= 40 ? '#F97316' : colors.red;
    return (
        <span style={{
            padding: '2px 8px', borderRadius: '999px', fontSize: '9px',
            fontWeight: 700, fontFamily: mono, background: `${color}20`, color,
        }}>
            {pct}% ({n})
        </span>
    );
}


// ── Main Component ──────────────────────────────────────────────────────

export default function RegimeAnalog({ onNavigate }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [activeTab, setActiveTab] = useState('forecast');

    useEffect(() => {
        loadData();
    }, []);

    async function loadData() {
        setLoading(true);
        try {
            const result = await api.getRegimeAnalogs(20);
            if (result?.error) {
                setError(result.error);
            } else {
                setData(result);
            }
        } catch (err) {
            setError(err.message);
        } finally {
            setLoading(false);
        }
    }

    if (loading) {
        return (
            <div style={s.container}>
                <div style={s.title}>REGIME ANALOG ENGINE</div>
                <div style={s.subtitle}>Computing macro state vector, matching historical episodes...</div>
                <div style={s.loadingBar}><div style={s.loadingFill} /></div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={s.container}>
                <div style={s.title}>REGIME ANALOG ENGINE</div>
                <div style={{ ...shared.card, borderLeft: `3px solid ${colors.red}`, marginTop: '16px', padding: '16px' }}>
                    <div style={{ color: colors.red, fontFamily: mono, fontSize: '13px' }}>Error: {error}</div>
                </div>
            </div>
        );
    }

    const regime = data?.regime;
    const forecast = data?.forecast;
    const matches = data?.matches;
    const timesfm = data?.timesfm;
    const ABSOLUTE_TICKERS = ['VIX', 'HY_SPREAD'];

    return (
        <div style={s.container}>
            {/* Header */}
            <div style={s.title}>REGIME ANALOG ENGINE</div>
            <div style={s.subtitle}>
                {matches?.n_matches || 0} historical analogs matched | {forecast?.confidence_level || '--'} confidence
            </div>

            {/* Regime Classification Bar */}
            <div style={{ marginTop: '16px' }}>
                <div style={s.regimeBar}>
                    {regime?.axes?.map((ax) => {
                        const axisColors = AXIS_COLORS[ax.axis] || {};
                        const color = axisColors[ax.label] || colors.textMuted;
                        return (
                            <div key={ax.axis} style={s.regimeChip(color)}>
                                <div style={{ fontSize: '8px', color: colors.textMuted, letterSpacing: '1.5px', marginBottom: '2px' }}>
                                    {AXIS_LABELS[ax.axis] || ax.axis}
                                </div>
                                {ax.label} <span style={{ opacity: 0.6, fontWeight: 400 }}>{Math.round(ax.confidence * 100)}%</span>
                            </div>
                        );
                    })}
                </div>
            </div>

            {/* Score Row */}
            <div style={s.scoreRow}>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: CONFIDENCE_COLORS[forecast?.confidence_level] || colors.textMuted }}>
                        {forecast?.confidence_level || '--'}
                    </div>
                    <div style={s.bigLabel}>CONFIDENCE</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: colors.accent }}>{matches?.n_matches || 0}</div>
                    <div style={s.bigLabel}>ANALOGS</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: colors.text }}>{matches?.effective_sample_size?.toFixed(1) || '--'}</div>
                    <div style={s.bigLabel}>EFF. SAMPLE</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: colors.green }}>{((matches?.mean_quality || 0) * 100).toFixed(0)}%</div>
                    <div style={s.bigLabel}>MATCH QUALITY</div>
                </div>
            </div>

            {/* Regime Summary */}
            {forecast?.regime_summary && (
                <div style={{ ...shared.card, borderLeft: `3px solid ${colors.accent}`, padding: '12px 16px', marginBottom: '12px' }}>
                    <div style={{ fontSize: '12px', fontFamily: mono, color: colors.text, lineHeight: 1.5 }}>
                        {forecast.regime_summary}
                    </div>
                </div>
            )}

            {/* Disagreement Flags */}
            {forecast?.disagreement_flags?.length > 0 && (
                <div style={{ marginBottom: '12px' }}>
                    {forecast.disagreement_flags.slice(0, 5).map((flag, i) => (
                        <div key={i} style={{
                            ...shared.card, borderLeft: `3px solid ${colors.yellow}`,
                            padding: '8px 14px', marginBottom: '4px',
                            fontSize: '11px', fontFamily: mono, color: colors.yellow, lineHeight: 1.4,
                        }}>
                            {flag}
                        </div>
                    ))}
                </div>
            )}

            {/* Tabs */}
            <div style={s.tabs}>
                {[
                    { id: 'forecast', label: 'Conditional Forecast' },
                    { id: 'episodes', label: `Episodes (${matches?.n_matches || 0})` },
                    { id: 'state', label: 'State Vector' },
                    { id: 'drivers', label: 'Match Drivers' },
                    ...(timesfm?.available ? [{ id: 'timesfm', label: 'TimesFM Compare' }] : []),
                ].map(tab => (
                    <button key={tab.id} style={s.tab(activeTab === tab.id)}
                        onClick={() => setActiveTab(tab.id)}>
                        {tab.label}
                    </button>
                ))}
            </div>

            {/* ── Forecast Tab ── */}
            {activeTab === 'forecast' && forecast?.outcomes && (
                <>
                    <div style={s.sectionTitle}>CONDITIONAL OUTCOME DISTRIBUTIONS</div>
                    {Object.entries(forecast.outcomes).map(([ticker, horizons]) => {
                        if (!horizons?.length || horizons[0]?.n_episodes === 0) return null;
                        const isAbs = ABSOLUTE_TICKERS.includes(ticker);
                        return (
                            <div key={ticker} style={{ marginBottom: '20px' }}>
                                <div style={{ fontSize: '13px', fontWeight: 700, fontFamily: mono, color: '#E8F0F8', marginBottom: '6px' }}>
                                    {ticker}
                                </div>
                                <table style={s.forecastTable}>
                                    <thead>
                                        <tr>
                                            <th style={{ ...s.th, textAlign: 'left' }}>HORIZON</th>
                                            <th style={s.th}>MEDIAN</th>
                                            <th style={s.th}>WEIGHTED</th>
                                            <th style={s.th}>10th</th>
                                            <th style={s.th}>90th</th>
                                            <th style={s.th}>AGREE</th>
                                            <th style={s.th}>DIR</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {horizons.map(h => {
                                            if (h.n_episodes === 0) return null;
                                            const dirColor = DIRECTION_COLORS[h.direction] || colors.textDim;
                                            return (
                                                <tr key={h.horizon_days}>
                                                    <td style={{ ...s.td(false), textAlign: 'left', color: colors.text }}>
                                                        {h.horizon_days}d
                                                    </td>
                                                    <ReturnCell value={h.median_return} isAbsolute={isAbs} ticker={ticker} />
                                                    <ReturnCell value={h.quality_weighted_mean} isAbsolute={isAbs} ticker={ticker} />
                                                    <ReturnCell value={h.percentiles?.['10']} isAbsolute={isAbs} ticker={ticker} />
                                                    <ReturnCell value={h.percentiles?.['90']} isAbsolute={isAbs} ticker={ticker} />
                                                    <td style={s.td(false)}>
                                                        <AgreementBadge score={h.agreement_score} n={h.n_episodes} />
                                                    </td>
                                                    <td style={{ ...s.td(true), color: dirColor }}>{h.direction}</td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        );
                    })}
                </>
            )}

            {/* ── Episodes Tab ── */}
            {activeTab === 'episodes' && matches?.episodes && (
                <>
                    <div style={s.sectionTitle}>MATCHED HISTORICAL EPISODES</div>
                    {/* Header */}
                    <div style={{ ...s.episodeCard, background: 'none', border: 'none', cursor: 'default', padding: '4px 14px' }}>
                        <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>DATE</span>
                        <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px' }}>TOP DRIVERS</span>
                        <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>QUALITY</span>
                        <span style={{ fontSize: '8px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1px', textAlign: 'right' }}>SPY +30d</span>
                    </div>
                    {matches.episodes.map((ep, i) => {
                        const spy30 = ep.forward_returns?.SPY?.['30'];
                        const spyColor = spy30 > 0 ? colors.green : spy30 < 0 ? colors.red : colors.textDim;
                        const drivers = Object.keys(ep.top_drivers || {}).slice(0, 3).join(', ');
                        return (
                            <div key={i} style={s.episodeCard}
                                onMouseEnter={(e) => { e.currentTarget.style.borderColor = `${colors.accent}40`; }}
                                onMouseLeave={(e) => { e.currentTarget.style.borderColor = ''; }}
                            >
                                <span style={{ fontSize: '12px', fontWeight: 700, fontFamily: mono, color: colors.text }}>
                                    {ep.as_of_date}
                                </span>
                                <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textDim }}>
                                    {drivers}
                                </span>
                                <span style={{ fontSize: '12px', fontFamily: mono, color: colors.accent, textAlign: 'right' }}>
                                    {(ep.match_quality * 100).toFixed(0)}%
                                </span>
                                <span style={{ fontSize: '12px', fontWeight: 700, fontFamily: mono, color: spyColor, textAlign: 'right' }}>
                                    {spy30 != null ? `${(spy30 * 100).toFixed(1)}%` : '--'}
                                </span>
                            </div>
                        );
                    })}
                </>
            )}

            {/* ── State Vector Tab ── */}
            {activeTab === 'state' && data?.matches?.episodes && (
                <>
                    <div style={s.sectionTitle}>CURRENT MACRO STATE VECTOR (24 DIMENSIONS)</div>
                    {(() => {
                        // Get dimensions from the first-level state vector data
                        // We need to reconstruct from regime data
                        const dims = Object.entries(
                            // Try to get from the API response
                            data?.regime?.axes?.reduce((acc, ax) => {
                                ax.drivers?.forEach(d => { acc[d] = 1; });
                                return acc;
                            }, {}) || {}
                        );

                        // Show match driver importance as a proxy for state visualization
                        const importance = matches?.top_dimensions || {};
                        const maxImp = Math.max(...Object.values(importance), 0.01);

                        return Object.entries(importance).map(([dim, imp]) => (
                            <div key={dim} style={s.dimBar}>
                                <span style={s.dimName}>{dim.replace(/_/g, ' ')}</span>
                                <div style={{ flex: 1, display: 'flex', alignItems: 'center' }}>
                                    <div style={{
                                        height: '14px', borderRadius: '2px',
                                        width: `${(imp / maxImp) * 100}%`,
                                        background: `${colors.accent}60`,
                                        transition: 'width 0.3s ease',
                                    }} />
                                </div>
                                <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textDim, width: '50px', textAlign: 'right' }}>
                                    {(imp * 100).toFixed(1)}%
                                </span>
                            </div>
                        ));
                    })()}
                </>
            )}

            {/* ── Drivers Tab ── */}
            {activeTab === 'drivers' && forecast?.dominant_drivers && (
                <>
                    <div style={s.sectionTitle}>WHAT'S DRIVING THE MATCHES</div>
                    <div style={{ ...shared.card, padding: '16px' }}>
                        {forecast.dominant_drivers.map((driver, i) => (
                            <div key={i} style={{
                                padding: '8px 0',
                                borderBottom: i < forecast.dominant_drivers.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                                fontSize: '13px', fontFamily: mono, color: colors.text,
                            }}>
                                <span style={{ color: colors.accent, fontWeight: 700, marginRight: '8px' }}>{i + 1}.</span>
                                {driver}
                            </div>
                        ))}
                    </div>

                    <div style={s.sectionTitle}>REGIME CLASSIFICATION DETAIL</div>
                    {regime?.axes?.map(ax => (
                        <div key={ax.axis} style={{ ...shared.card, padding: '12px 16px', marginBottom: '6px' }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <div>
                                    <div style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted, letterSpacing: '1.5px' }}>
                                        {AXIS_LABELS[ax.axis] || ax.axis}
                                    </div>
                                    <div style={{
                                        fontSize: '14px', fontWeight: 700, fontFamily: mono, marginTop: '2px',
                                        color: (AXIS_COLORS[ax.axis] || {})[ax.label] || colors.text,
                                    }}>
                                        {ax.label.toUpperCase()}
                                    </div>
                                </div>
                                <div style={{
                                    fontSize: '22px', fontWeight: 800, fontFamily: mono,
                                    color: (AXIS_COLORS[ax.axis] || {})[ax.label] || colors.text,
                                }}>
                                    {Math.round(ax.confidence * 100)}%
                                </div>
                            </div>
                            {ax.drivers?.length > 0 && (
                                <div style={{ marginTop: '6px', fontSize: '10px', fontFamily: mono, color: colors.textDim }}>
                                    Drivers: {ax.drivers.join(', ')}
                                </div>
                            )}
                        </div>
                    ))}
                </>
            )}

            {/* ── TimesFM Tab ── */}
            {activeTab === 'timesfm' && timesfm?.available && (
                <>
                    <div style={s.sectionTitle}>TIMESFM 2.5 FOUNDATION MODEL — SIDE-BY-SIDE</div>
                    <div style={{
                        ...shared.card, borderLeft: `3px solid ${colors.yellow}`,
                        padding: '10px 14px', marginBottom: '12px', fontSize: '11px',
                        fontFamily: mono, color: colors.yellow, lineHeight: 1.4,
                    }}>
                        TimesFM is a univariate foundation model — it sees price history only, no macro context.
                        The analog engine uses 24 macro dimensions + 1,918 historical episodes.
                        Compare to see where they agree and disagree.
                    </div>

                    {Object.entries(timesfm.forecasts || {}).map(([ticker, tfmData]) => {
                        const analogDists = forecast?.outcomes?.[ticker] || [];
                        const isAbs = ABSOLUTE_TICKERS.includes(ticker);
                        return (
                            <div key={ticker} style={{ marginBottom: '20px' }}>
                                <div style={{ fontSize: '13px', fontWeight: 700, fontFamily: mono, color: '#E8F0F8', marginBottom: '2px' }}>
                                    {ticker} <span style={{ fontSize: '10px', color: colors.textMuted, fontWeight: 400 }}>
                                        current: {tfmData.current?.toFixed(2)} | ctx: {tfmData.context_length?.toLocaleString()}
                                    </span>
                                </div>
                                <table style={s.forecastTable}>
                                    <thead>
                                        <tr>
                                            <th style={{ ...s.th, textAlign: 'left' }}>HORIZON</th>
                                            <th style={{ ...s.th, color: colors.accent }}>ANALOG</th>
                                            <th style={{ ...s.th, color: '#EC4899' }}>TIMESFM</th>
                                            <th style={s.th}>TFM RANGE</th>
                                            <th style={s.th}>AGREE?</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {Object.entries(tfmData.horizons || {}).map(([h, hData]) => {
                                            const horizon = parseInt(h);
                                            const analogH = analogDists.find(d => d.horizon_days === horizon);
                                            const analogVal = analogH?.quality_weighted_mean;

                                            const tfmVal = isAbs ? hData.change : hData.return;
                                            const tfmLo = hData.low_10;
                                            const tfmHi = hData.high_90;

                                            // Do they agree on direction?
                                            let agreeLabel = '--';
                                            let agreeColor = colors.textMuted;
                                            if (analogVal != null && tfmVal != null) {
                                                const bothSameDir = (analogVal > 0 && tfmVal > 0) || (analogVal < 0 && tfmVal < 0);
                                                agreeLabel = bothSameDir ? 'YES' : 'NO';
                                                agreeColor = bothSameDir ? colors.green : colors.red;
                                            }

                                            return (
                                                <tr key={h}>
                                                    <td style={{ ...s.td(false), textAlign: 'left', color: colors.text }}>{h}d</td>
                                                    <td style={{ ...s.td(true), color: colors.accent }}>
                                                        {analogVal != null
                                                            ? isAbs ? `${analogVal >= 0 ? '+' : ''}${analogVal.toFixed(1)}pts` : `${(analogVal * 100).toFixed(1)}%`
                                                            : '--'}
                                                    </td>
                                                    <td style={{ ...s.td(true), color: '#EC4899' }}>
                                                        {isAbs
                                                            ? `${tfmVal >= 0 ? '+' : ''}${tfmVal?.toFixed(1)}pts`
                                                            : `${(tfmVal * 100)?.toFixed(1)}%`}
                                                    </td>
                                                    <td style={s.td(false)}>
                                                        {tfmLo != null ? `${tfmLo.toFixed(1)} - ${tfmHi.toFixed(1)}` : '--'}
                                                    </td>
                                                    <td style={{ ...s.td(true), color: agreeColor, fontWeight: 700 }}>
                                                        {agreeLabel}
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        );
                    })}
                </>
            )}

            <div style={{ height: '40px' }} />
        </div>
    );
}
