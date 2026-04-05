import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

const ALERT_COLORS = {
    CONVICTION: '#22C55E',
    FIRE: '#F59E0B',
    SCALE: '#3B82F6',
    PILOT: '#8B5CF6',
    WATCH: '#5A7080',
    PASS: '#3A4A5A',
};

const REGIME_COLORS = {
    GROWTH: '#22C55E',
    NEUTRAL: '#3B82F6',
    FRAGILE: '#F59E0B',
    CRISIS: '#EF4444',
    UNCALIBRATED: '#5A7080',
};

function MetricTile({ label, value, sub, color }) {
    return (
        <div style={shared.metric}>
            <div style={{ ...shared.metricValue, color: color || '#E8F0F8' }}>
                {value ?? '--'}
            </div>
            <div style={shared.metricLabel}>{label}</div>
            {sub && (
                <div style={{
                    fontSize: tokens.fontSize.xs, color: colors.textMuted,
                    marginTop: '2px', fontFamily: colors.mono,
                }}>
                    {sub}
                </div>
            )}
        </div>
    );
}

function RiskMetricsPanel({ regime, snapshot }) {
    const vixFeature = snapshot?.features?.find(f => f.name === 'vix_spot');
    const hyFeature = snapshot?.features?.find(f => f.name === 'hy_oas_spread');
    const stressFeature = snapshot?.features?.find(f => f.name === 'ofr_financial_stress');
    const ycFeature = snapshot?.features?.find(f => f.name === 'yld_curve_2s10s');

    const regimeState = regime?.state || 'UNCALIBRATED';
    const regimeColor = REGIME_COLORS[regimeState] || '#5A7080';
    const confidence = regime?.confidence != null
        ? `${Math.round(regime.confidence * 100)}%`
        : '--';

    function formatVal(feat) {
        if (feat?.value == null) return '--';
        return Number(feat.value).toFixed(2);
    }

    function zColor(feat) {
        const z = feat?.z_score;
        if (z == null) return '#E8F0F8';
        if (Math.abs(z) > 2) return colors.red;
        if (Math.abs(z) > 1) return colors.yellow;
        return '#E8F0F8';
    }

    function zLabel(feat) {
        const z = feat?.z_score;
        if (z == null) return null;
        return `z=${z > 0 ? '+' : ''}${z.toFixed(2)}`;
    }

    return (
        <div style={shared.card}>
            <div style={shared.sectionTitle}>MARKET RISK INDICATORS</div>
            <div style={shared.metricGrid}>
                <MetricTile
                    label="Regime"
                    value={regimeState}
                    sub={`${confidence} conf`}
                    color={regimeColor}
                />
                <MetricTile
                    label="VIX"
                    value={formatVal(vixFeature)}
                    sub={zLabel(vixFeature)}
                    color={zColor(vixFeature)}
                />
                <MetricTile
                    label="HY Spread"
                    value={formatVal(hyFeature)}
                    sub={zLabel(hyFeature)}
                    color={zColor(hyFeature)}
                />
                <MetricTile
                    label="Fin Stress"
                    value={formatVal(stressFeature)}
                    sub={zLabel(stressFeature)}
                    color={zColor(stressFeature)}
                />
                <MetricTile
                    label="Yield Curve"
                    value={formatVal(ycFeature)}
                    sub={zLabel(ycFeature)}
                    color={zColor(ycFeature)}
                />
            </div>
        </div>
    );
}

function ConvictionTable({ reports }) {
    if (!reports || reports.length === 0) {
        return (
            <div style={{
                ...shared.card, textAlign: 'center',
                color: colors.textMuted, fontSize: tokens.fontSize.md,
                padding: '32px',
            }}>
                No tickers above conviction threshold
            </div>
        );
    }

    return (
        <div style={shared.card}>
            <div style={shared.sectionTitle}>CONVICTION SCORES</div>
            <div style={{ overflowX: 'auto' }}>
                <table style={{
                    width: '100%', borderCollapse: 'collapse',
                    fontFamily: colors.mono, fontSize: tokens.fontSize.sm,
                }}>
                    <thead>
                        <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                            {['Ticker', 'Score', 'Confidence', 'Level', 'Top Signal'].map(h => (
                                <th key={h} style={{
                                    textAlign: 'left', padding: '8px 10px',
                                    color: colors.textMuted, fontWeight: 600,
                                    fontSize: tokens.fontSize.xs,
                                    letterSpacing: '0.5px',
                                }}>
                                    {h}
                                </th>
                            ))}
                        </tr>
                    </thead>
                    <tbody>
                        {reports.map((r) => {
                            const alertColor = ALERT_COLORS[r.alert_level] || '#5A7080';
                            const topSignal = _extractTopSignal(r.layers);
                            return (
                                <tr key={r.ticker} style={{
                                    borderBottom: `1px solid ${colors.borderSubtle}`,
                                }}>
                                    <td style={{
                                        padding: '10px', fontWeight: 700,
                                        color: '#E8F0F8',
                                    }}>
                                        {r.ticker}
                                    </td>
                                    <td style={{
                                        padding: '10px', fontWeight: 700,
                                        color: alertColor,
                                    }}>
                                        {r.total_score}
                                    </td>
                                    <td style={{ padding: '10px', color: colors.textDim }}>
                                        {r.confidence_pct.toFixed(0)}%
                                    </td>
                                    <td style={{ padding: '10px' }}>
                                        <span style={{
                                            ...shared.badge(alertColor),
                                            fontSize: '9px', padding: '2px 6px',
                                        }}>
                                            {r.alert_level}
                                        </span>
                                    </td>
                                    <td style={{
                                        padding: '10px', color: colors.textMuted,
                                        fontSize: tokens.fontSize.xs,
                                        maxWidth: '200px',
                                        whiteSpace: 'nowrap', overflow: 'hidden',
                                        textOverflow: 'ellipsis',
                                    }}>
                                        {topSignal}
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function LayerBreakdown({ layers }) {
    if (!layers || layers.length === 0) return null;

    return (
        <div style={{ display: 'grid', gap: tokens.space.sm }}>
            {layers.map((layer) => {
                const pct = layer.max_score > 0
                    ? (layer.score / layer.max_score) * 100
                    : 0;
                const barColor = pct > 60 ? colors.green
                    : pct > 30 ? colors.yellow
                    : colors.red;

                return (
                    <div key={layer.name} style={{
                        ...shared.card, padding: '10px 14px',
                        opacity: layer.data_available ? 1 : 0.5,
                    }}>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center', marginBottom: '6px',
                        }}>
                            <span style={{
                                fontSize: tokens.fontSize.sm, fontWeight: 600,
                                color: '#E8F0F8', fontFamily: colors.mono,
                            }}>
                                {layer.name}
                            </span>
                            <span style={{
                                fontSize: tokens.fontSize.sm, color: barColor,
                                fontFamily: colors.mono, fontWeight: 700,
                            }}>
                                {layer.score.toFixed(1)}/{layer.max_score.toFixed(1)}
                            </span>
                        </div>
                        <div style={{
                            height: '4px', borderRadius: '2px',
                            background: colors.border, overflow: 'hidden',
                        }}>
                            <div style={{
                                height: '100%', width: `${pct}%`,
                                background: barColor, borderRadius: '2px',
                                transition: 'width 0.4s ease',
                            }} />
                        </div>
                        {layer.signals.length > 0 && (
                            <div style={{
                                marginTop: '6px', fontSize: tokens.fontSize.xs,
                                color: colors.textMuted, lineHeight: '1.5',
                            }}>
                                {layer.signals.join(' | ')}
                            </div>
                        )}
                        {!layer.data_available && (
                            <div style={{
                                marginTop: '4px', fontSize: tokens.fontSize.xs,
                                color: colors.textMuted, fontStyle: 'italic',
                            }}>
                                No data available
                            </div>
                        )}
                    </div>
                );
            })}
        </div>
    );
}

function TickerDetail({ ticker, onBack }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        setLoading(true);
        setError(null);
        api.getConvictionTicker(ticker)
            .then(d => {
                if (d.error) { setError(d.message); return; }
                setData(d);
            })
            .catch(e => setError(e.message))
            .finally(() => setLoading(false));
    }, [ticker]);

    const alertColor = data ? (ALERT_COLORS[data.alert_level] || '#5A7080') : '#5A7080';

    return (
        <div>
            <button onClick={onBack} style={{
                ...shared.buttonSmall, background: colors.card,
                color: colors.textDim, marginBottom: tokens.space.md,
            }}>
                Back to overview
            </button>

            {loading && (
                <div style={{
                    color: colors.textMuted, textAlign: 'center',
                    padding: '40px', fontSize: tokens.fontSize.md,
                }}>
                    Loading {ticker} conviction data...
                </div>
            )}

            {error && (
                <div style={{ ...shared.card, ...shared.error }}>
                    Failed to load conviction for {ticker}: {error}
                </div>
            )}

            {data && !loading && (
                <>
                    <div style={shared.card}>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center',
                        }}>
                            <div>
                                <div style={{
                                    fontSize: tokens.fontSize.xl, fontWeight: 700,
                                    color: '#E8F0F8', fontFamily: colors.mono,
                                }}>
                                    {data.ticker}
                                </div>
                                <div style={{
                                    fontSize: tokens.fontSize.sm, color: colors.textMuted,
                                    marginTop: '4px',
                                }}>
                                    {data.timestamp}
                                </div>
                            </div>
                            <div style={{ textAlign: 'right' }}>
                                <div style={{
                                    fontSize: '28px', fontWeight: 700,
                                    color: alertColor, fontFamily: colors.mono,
                                }}>
                                    {data.total_score}
                                </div>
                                <span style={{
                                    ...shared.badge(alertColor),
                                    fontSize: '10px',
                                }}>
                                    {data.alert_level}
                                </span>
                            </div>
                        </div>
                    </div>
                    <LayerBreakdown layers={data.layers} />
                </>
            )}
        </div>
    );
}

function _extractTopSignal(layers) {
    if (!layers) return '--';
    for (const layer of layers) {
        if (layer.signals && layer.signals.length > 0) {
            return layer.signals[0];
        }
    }
    return '--';
}

export default function RiskView() {
    const [regime, setRegime] = useState(null);
    const [snapshot, setSnapshot] = useState(null);
    const [conviction, setConviction] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedTicker, setSelectedTicker] = useState(null);

    useEffect(() => {
        setLoading(true);
        setError(null);

        Promise.allSettled([
            api.getCurrent(),
            api.getSignalSnapshot(),
            api.getConvictionScores(20),
        ]).then(([regimeRes, snapRes, convRes]) => {
            if (regimeRes.status === 'fulfilled' && !regimeRes.value?.error) {
                setRegime(regimeRes.value);
            }
            if (snapRes.status === 'fulfilled' && !snapRes.value?.error) {
                setSnapshot(snapRes.value);
            }
            if (convRes.status === 'fulfilled' && !convRes.value?.error) {
                setConviction(convRes.value);
            }

            const allFailed = regimeRes.status === 'rejected'
                && snapRes.status === 'rejected'
                && convRes.status === 'rejected';
            if (allFailed) {
                setError('Failed to load risk data');
            }
        }).finally(() => setLoading(false));
    }, []);

    if (selectedTicker) {
        return (
            <div style={shared.container}>
                <div style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '14px', color: '#5A7080',
                    letterSpacing: '2px', marginBottom: tokens.space.lg,
                }}>
                    RISK / {selectedTicker}
                </div>
                <TickerDetail
                    ticker={selectedTicker}
                    onBack={() => setSelectedTicker(null)}
                />
            </div>
        );
    }

    return (
        <div style={shared.container}>
            <div style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '14px', color: '#5A7080',
                letterSpacing: '2px', marginBottom: tokens.space.lg,
            }}>
                RISK DASHBOARD
            </div>

            {loading && (
                <div style={{
                    color: colors.textMuted, textAlign: 'center',
                    padding: '40px', fontSize: tokens.fontSize.md,
                }}>
                    Loading risk data...
                </div>
            )}

            {error && !loading && (
                <div style={{ ...shared.card, ...shared.error }}>
                    {error}
                </div>
            )}

            {!loading && (
                <>
                    <RiskMetricsPanel regime={regime} snapshot={snapshot} />

                    {conviction?.reports && (
                        <div onClick={(e) => {
                            const row = e.target.closest('tr');
                            if (!row) return;
                            const ticker = row.querySelector('td')?.textContent;
                            if (ticker) setSelectedTicker(ticker.trim());
                        }} style={{ cursor: 'pointer' }}>
                            <ConvictionTable reports={conviction.reports} />
                        </div>
                    )}

                    {!conviction?.reports && !loading && (
                        <div style={{
                            ...shared.card, textAlign: 'center',
                            color: colors.textMuted, padding: '32px',
                            fontSize: tokens.fontSize.md,
                        }}>
                            Conviction scorer unavailable -- check backend logs
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
