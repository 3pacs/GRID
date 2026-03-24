import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { shared, colors } from '../styles/shared.js';

/**
 * Assign a category to a feature based on its name.
 * Groups features for the heatmap grid layout.
 */
function categorize(featureName) {
    const name = (featureName || '').toLowerCase();
    if (name.includes('rate') || name.includes('yield') || name.includes('treasury') || name.includes('spread') || name.includes('dgs') || name.includes('t10y'))
        return 'Rates';
    if (name.includes('cpi') || name.includes('pce') || name.includes('inflation') || name.includes('breakeven'))
        return 'Inflation';
    if (name.includes('gdp') || name.includes('employment') || name.includes('payroll') || name.includes('unrate') || name.includes('claims') || name.includes('labor') || name.includes('nfp'))
        return 'Employment';
    if (name.includes('sentiment') || name.includes('vix') || name.includes('fear') || name.includes('confidence') || name.includes('umich') || name.includes('aaii'))
        return 'Sentiment';
    if (name.includes('dollar') || name.includes('dxy') || name.includes('eur') || name.includes('jpy') || name.includes('gbp') || name.includes('fx'))
        return 'FX';
    if (name.includes('oil') || name.includes('gold') || name.includes('copper') || name.includes('wti') || name.includes('brent') || name.includes('commodity') || name.includes('wheat') || name.includes('corn'))
        return 'Commodities';
    if (name.includes('sp500') || name.includes('nasdaq') || name.includes('equity') || name.includes('russell') || name.includes('dow') || name.includes('stock'))
        return 'Equities';
    if (name.includes('credit') || name.includes('hy') || name.includes('ig') || name.includes('default') || name.includes('oas'))
        return 'Credit';
    if (name.includes('money') || name.includes('m2') || name.includes('liquidity') || name.includes('reserve') || name.includes('fed'))
        return 'Monetary';
    if (name.includes('pmi') || name.includes('ism') || name.includes('industrial') || name.includes('manufacturing') || name.includes('production'))
        return 'Manufacturing';
    if (name.includes('housing') || name.includes('permits') || name.includes('starts') || name.includes('existing') || name.includes('new_home'))
        return 'Housing';
    return 'Other';
}

const categoryOrder = [
    'Rates', 'Inflation', 'Employment', 'Sentiment', 'Equities',
    'Credit', 'FX', 'Commodities', 'Monetary', 'Manufacturing',
    'Housing', 'Other',
];

/**
 * Return a CSS color for a z-score value.
 * Green for positive, red for negative, neutral for near-zero.
 */
function zScoreColor(zScore) {
    if (zScore == null || isNaN(zScore)) return colors.card;
    const clamped = Math.max(-3, Math.min(3, zScore));
    const intensity = Math.abs(clamped) / 3;
    if (clamped > 0.1) {
        const r = Math.round(13 + (34 - 13) * (1 - intensity));
        const g = Math.round(51 + (197 - 51) * intensity);
        const b = Math.round(32 + (94 - 32) * (1 - intensity));
        return `rgb(${r}, ${g}, ${b})`;
    }
    if (clamped < -0.1) {
        const r = Math.round(59 + (239 - 59) * intensity);
        const g = Math.round(17 + (68 - 17) * (1 - intensity));
        const b = Math.round(17 + (68 - 17) * (1 - intensity));
        return `rgb(${r}, ${g}, ${b})`;
    }
    return colors.card;
}

function zScoreTextColor(zScore) {
    if (zScore == null || isNaN(zScore)) return colors.textMuted;
    const abs = Math.abs(zScore);
    if (abs > 1.5) return '#fff';
    return colors.text;
}

export default function Heatmap() {
    const { addNotification, setLoading } = useStore();
    const [features, setFeatures] = useState([]);
    const [error, setError] = useState(null);
    const [loading, setLocalLoading] = useState(true);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLocalLoading(true);
        setLoading('heatmap', true);
        setError(null);
        try {
            const data = await api.getSignalSnapshot();
            setFeatures(data.features || []);
        } catch (err) {
            setError(err.message || 'Failed to load feature snapshot');
            addNotification('error', 'Failed to load heatmap data');
        }
        setLocalLoading(false);
        setLoading('heatmap', false);
    };

    // Group features by category
    const grouped = {};
    for (const f of features) {
        const cat = categorize(f.feature_name || f.name || '');
        if (!grouped[cat]) grouped[cat] = [];
        grouped[cat].push(f);
    }

    // Sort categories by defined order
    const sortedCategories = categoryOrder.filter(c => grouped[c]);
    // Add any categories not in the order
    for (const c of Object.keys(grouped)) {
        if (!sortedCategories.includes(c)) sortedCategories.push(c);
    }

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                <h1 style={shared.header}>Feature Heatmap</h1>
                <button onClick={loadData} style={shared.buttonSmall}>
                    Refresh
                </button>
            </div>

            {/* Legend */}
            <div style={{
                ...shared.card, display: 'flex', justifyContent: 'center',
                alignItems: 'center', gap: '8px', padding: '10px 16px',
            }}>
                <span style={{ fontSize: '11px', color: colors.textMuted }}>Negative</span>
                <div style={{
                    display: 'flex', gap: '2px',
                }}>
                    {[-3, -2, -1, 0, 1, 2, 3].map(z => (
                        <div
                            key={z}
                            style={{
                                width: '24px', height: '14px', borderRadius: '3px',
                                background: zScoreColor(z),
                            }}
                        />
                    ))}
                </div>
                <span style={{ fontSize: '11px', color: colors.textMuted }}>Positive</span>
            </div>

            {loading ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                    Loading feature data...
                </div>
            ) : error ? (
                <div style={shared.error}>{error}</div>
            ) : features.length === 0 ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                    No feature data available. Run inference to generate a snapshot.
                </div>
            ) : (
                sortedCategories.map(cat => (
                    <div key={cat} style={{ marginTop: '16px' }}>
                        <div style={{
                            fontSize: '11px', fontWeight: 700, color: colors.textMuted,
                            letterSpacing: '1.5px', marginBottom: '8px',
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {cat.toUpperCase()} ({grouped[cat].length})
                        </div>
                        <div style={{
                            display: 'grid',
                            gridTemplateColumns: 'repeat(auto-fill, minmax(100px, 1fr))',
                            gap: '6px',
                        }}>
                            {grouped[cat].map((f, i) => {
                                const zScore = f.z_score ?? f.zscore ?? f.value ?? null;
                                const name = f.feature_name || f.name || `feature_${i}`;
                                const shortName = name.length > 14 ? name.substring(0, 12) + '..' : name;
                                return (
                                    <div
                                        key={name}
                                        title={`${name}: ${zScore != null ? zScore.toFixed(3) : 'N/A'}`}
                                        style={{
                                            background: zScoreColor(zScore),
                                            borderRadius: '8px',
                                            padding: '10px 8px',
                                            textAlign: 'center',
                                            border: `1px solid ${colors.border}`,
                                            cursor: 'default',
                                        }}
                                    >
                                        <div style={{
                                            fontSize: '10px', color: zScoreTextColor(zScore),
                                            fontFamily: colors.mono,
                                            overflow: 'hidden', textOverflow: 'ellipsis',
                                            whiteSpace: 'nowrap',
                                        }}>
                                            {shortName}
                                        </div>
                                        <div style={{
                                            fontSize: '14px', fontWeight: 700,
                                            color: zScoreTextColor(zScore),
                                            fontFamily: colors.mono,
                                            marginTop: '4px',
                                        }}>
                                            {zScore != null ? (zScore >= 0 ? '+' : '') + zScore.toFixed(2) : '--'}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                ))
            )}

            {/* Summary */}
            {features.length > 0 && (
                <div style={{
                    ...shared.card, marginTop: '16px',
                }}>
                    <div style={shared.metricGrid}>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{features.length}</div>
                            <div style={shared.metricLabel}>Features</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{sortedCategories.length}</div>
                            <div style={shared.metricLabel}>Categories</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={{
                                ...shared.metricValue,
                                color: colors.green,
                            }}>
                                {features.filter(f => (f.z_score ?? f.zscore ?? f.value ?? 0) > 0.5).length}
                            </div>
                            <div style={shared.metricLabel}>Positive</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={{
                                ...shared.metricValue,
                                color: colors.red,
                            }}>
                                {features.filter(f => (f.z_score ?? f.zscore ?? f.value ?? 0) < -0.5).length}
                            </div>
                            <div style={shared.metricLabel}>Negative</div>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
