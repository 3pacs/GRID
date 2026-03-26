/**
 * TimeframeComparison — shows a feature across 5D/5W/3M/1Y/5Y side by side.
 *
 * Each panel shows a mini sparkline, current value, and % change for that period.
 * Makes it immediately obvious if current conditions match a historical pattern.
 * Embeddable in Flows, WatchlistAnalysis, or standalone.
 */
import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const PERIODS = ['5d', '5w', '3m', '1y', '5y'];
const PERIOD_LABELS = { '5d': '5 Day', '5w': '5 Week', '3m': '3 Month', '1y': '1 Year', '5y': '5 Year' };

function MiniSparkline({ values, height = 40 }) {
    if (!values || values.length < 2) {
        return <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textMuted, fontSize: '9px' }}>No data</div>;
    }
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const w = 100;
    const isUp = values[values.length - 1] >= values[0];
    const lineColor = isUp ? '#22C55E' : '#EF4444';

    const points = values.map((v, i) => {
        const x = (i / (values.length - 1)) * w;
        const y = height - ((v - min) / range) * (height - 6) - 3;
        return `${x},${y}`;
    }).join(' ');

    return (
        <svg viewBox={`0 0 ${w} ${height}`} width="100%" height={height} preserveAspectRatio="none" style={{ display: 'block' }}>
            <linearGradient id={`tfc-${isUp ? 'g' : 'r'}`} x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={lineColor} stopOpacity="0.12" />
                <stop offset="100%" stopColor={lineColor} stopOpacity="0" />
            </linearGradient>
            <polygon points={`0,${height} ${points} ${w},${height}`} fill={`url(#tfc-${isUp ? 'g' : 'r'})`} />
            <polyline points={points} fill="none" stroke={lineColor} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
        </svg>
    );
}

function changeInterpretation(pct, period) {
    if (pct == null) return '';
    const abs = Math.abs(pct);
    const dir = pct >= 0 ? 'up' : 'down';
    if (abs > 20) return `Significant move ${dir} over ${PERIOD_LABELS[period] || period}`;
    if (abs > 10) return `Notable ${dir}trend`;
    if (abs > 3) return `Moderate ${dir} pressure`;
    return 'Relatively stable';
}

export default function TimeframeComparison({ feature, periods = PERIODS, compact = false }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (feature) loadData();
    }, [feature]);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.getFeatureTimeframes(feature, periods.join(','));
            setData(result);
        } catch (err) {
            setError(err.message || 'Failed');
        }
        setLoading(false);
    };

    if (!feature) return null;

    if (loading) {
        return <div style={{ color: colors.textMuted, fontSize: '11px', padding: '12px', textAlign: 'center' }}>Loading timeframes...</div>;
    }

    if (error) {
        return <div style={{ color: colors.red, fontSize: '11px', padding: '8px' }}>{error}</div>;
    }

    if (!data?.periods || Object.keys(data.periods).length === 0) return null;

    const activePeriods = periods.filter(p => data.periods[p] && data.periods[p].values?.length > 0);

    return (
        <div style={{
            background: colors.card, borderRadius: '10px',
            border: `1px solid ${colors.border}`, overflow: 'hidden',
        }}>
            <div style={{
                padding: '10px 14px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
                <div>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    }}>TIMEFRAME COMPARISON</span>
                    <span style={{ fontSize: '10px', color: colors.textMuted, marginLeft: '8px' }}>{feature}</span>
                </div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: compact ? `repeat(${Math.min(activePeriods.length, 3)}, 1fr)` : `repeat(${activePeriods.length}, 1fr)`,
                gap: '0',
            }}>
                {activePeriods.map((period, i) => {
                    const pd = data.periods[period];
                    const pct = pd.change_pct;
                    const pctColor = pct > 0 ? '#22C55E' : pct < 0 ? '#EF4444' : colors.textMuted;
                    return (
                        <div key={period} style={{
                            padding: '10px 12px',
                            borderRight: i < activePeriods.length - 1 ? `1px solid ${colors.border}` : 'none',
                        }}>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.textMuted, marginBottom: '6px', fontFamily: "'JetBrains Mono', monospace" }}>
                                {PERIOD_LABELS[period] || period}
                            </div>
                            <MiniSparkline values={pd.values} height={compact ? 30 : 40} />
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '6px' }}>
                                <span style={{ fontSize: '11px', fontWeight: 600, color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                    {pd.end?.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                                </span>
                                <span style={{ fontSize: '11px', fontWeight: 700, color: pctColor, fontFamily: "'JetBrains Mono', monospace" }}>
                                    {pct > 0 ? '+' : ''}{pct?.toFixed(1)}%
                                </span>
                            </div>
                            <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '2px' }}>
                                {changeInterpretation(pct, period)}
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
