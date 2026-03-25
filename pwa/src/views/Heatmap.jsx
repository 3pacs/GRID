/**
 * GRID Feature Heatmap — Mobile-first z-score overview.
 *
 * Shows all features grouped by category, sorted by z-score magnitude.
 * Each feature is a tappable row with a z-score bar. Tap to expand
 * for more detail. Summary stats at top.
 */
import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

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

function zColor(z) {
    if (z == null || isNaN(z)) return colors.textMuted;
    if (z > 1.5) return colors.green;
    if (z > 0.5) return '#4ADE80';
    if (z < -1.5) return colors.red;
    if (z < -0.5) return '#F87171';
    return colors.textDim;
}

function FeatureRow({ f, expanded, onToggle }) {
    const z = f.z_score ?? f.zscore ?? f.value ?? null;
    const name = f.feature_name || f.name || '?';
    const barPct = z != null ? Math.min(100, (Math.abs(z) / 3) * 100) : 0;
    const color = zColor(z);
    const isPositive = z != null && z > 0;

    return (
        <div onClick={onToggle} style={{
            padding: '12px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
            cursor: 'pointer',
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: tokens.space.sm }}>
                <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{
                        fontSize: tokens.fontSize.sm, color: colors.text,
                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                    }}>
                        {name}
                    </div>
                </div>
                <span style={{
                    fontSize: tokens.fontSize.md, fontFamily: colors.mono,
                    fontWeight: 700, color, flexShrink: 0, width: '52px', textAlign: 'right',
                }}>
                    {z != null ? (z >= 0 ? '+' : '') + z.toFixed(2) : '--'}
                </span>
            </div>
            {/* Z-score bar */}
            <div style={{
                height: '6px', borderRadius: '3px', background: colors.bg,
                marginTop: tokens.space.xs, position: 'relative', overflow: 'hidden',
            }}>
                <div style={{
                    position: 'absolute',
                    left: isPositive ? '50%' : undefined,
                    right: isPositive ? undefined : '50%',
                    top: 0, bottom: 0,
                    width: `${barPct / 2}%`,
                    background: color,
                    borderRadius: '3px',
                    transition: `width ${tokens.transition.normal}`,
                }} />
                <div style={{
                    position: 'absolute', left: '50%', top: 0, bottom: 0,
                    width: '1px', background: colors.border,
                }} />
            </div>
            {expanded && z != null && (
                <div style={{
                    display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
                    gap: tokens.space.sm, marginTop: tokens.space.md,
                    padding: tokens.space.sm, background: colors.bg,
                    borderRadius: tokens.radius.sm,
                }}>
                    <div>
                        <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Value</div>
                        <div style={{ fontSize: tokens.fontSize.sm, fontFamily: colors.mono, color: colors.text }}>
                            {f.value != null ? (typeof f.value === 'number' ? f.value.toFixed(4) : f.value) : '--'}
                        </div>
                    </div>
                    <div>
                        <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Z-Score</div>
                        <div style={{ fontSize: tokens.fontSize.sm, fontFamily: colors.mono, color }}>
                            {z.toFixed(3)}
                        </div>
                    </div>
                    <div>
                        <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Signal</div>
                        <div style={{ fontSize: tokens.fontSize.sm, fontWeight: 600, color }}>
                            {Math.abs(z) > 2 ? (z > 0 ? 'STRONG +' : 'STRONG -') : Math.abs(z) > 1 ? (z > 0 ? 'MILD +' : 'MILD -') : 'NEUTRAL'}
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

export default function Heatmap() {
    const { addNotification, setLoading } = useStore();
    const [features, setFeatures] = useState([]);
    const [error, setError] = useState(null);
    const [loading, setLocalLoading] = useState(true);
    const [expandedFeature, setExpandedFeature] = useState(null);
    const [sortMode, setSortMode] = useState('magnitude'); // 'magnitude' | 'category'
    const { isMobile } = useDevice();

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

    // Compute summary stats
    const positive = features.filter(f => (f.z_score ?? f.zscore ?? f.value ?? 0) > 0.5).length;
    const negative = features.filter(f => (f.z_score ?? f.zscore ?? f.value ?? 0) < -0.5).length;
    const extreme = features.filter(f => Math.abs(f.z_score ?? f.zscore ?? f.value ?? 0) > 2.5).length;

    // Group and sort
    const grouped = {};
    for (const f of features) {
        const cat = categorize(f.feature_name || f.name || '');
        if (!grouped[cat]) grouped[cat] = [];
        grouped[cat].push(f);
    }
    // Sort within each group by |z-score|
    for (const cat of Object.keys(grouped)) {
        grouped[cat].sort((a, b) => {
            const az = Math.abs(a.z_score ?? a.zscore ?? a.value ?? 0);
            const bz = Math.abs(b.z_score ?? b.zscore ?? b.value ?? 0);
            return bz - az;
        });
    }
    const sortedCategories = categoryOrder.filter(c => grouped[c]);
    for (const c of Object.keys(grouped)) {
        if (!sortedCategories.includes(c)) sortedCategories.push(c);
    }

    // Flat sorted by magnitude
    const allSorted = [...features].sort((a, b) => {
        const az = Math.abs(a.z_score ?? a.zscore ?? a.value ?? 0);
        const bz = Math.abs(b.z_score ?? b.zscore ?? b.value ?? 0);
        return bz - az;
    });

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.md }}>
                <div style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
                    color: colors.textMuted, letterSpacing: '2px',
                }}>
                    FEATURE HEATMAP
                </div>
                <button onClick={loadData} style={shared.buttonSmall}>Refresh</button>
            </div>

            {/* Summary strip */}
            {features.length > 0 && (
                <div style={{ ...shared.metricGrid, marginBottom: tokens.space.lg }}>
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{features.length}</div>
                        <div style={shared.metricLabel}>Features</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.green }}>{positive}</div>
                        <div style={shared.metricLabel}>Bullish</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.red }}>{negative}</div>
                        <div style={shared.metricLabel}>Bearish</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.yellow }}>{extreme}</div>
                        <div style={shared.metricLabel}>Extreme</div>
                    </div>
                </div>
            )}

            {/* Sort toggle */}
            {features.length > 0 && (
                <div style={{ ...shared.tabs, marginBottom: tokens.space.md }}>
                    <button onClick={() => setSortMode('magnitude')} style={shared.tab(sortMode === 'magnitude')}>
                        By Magnitude
                    </button>
                    <button onClick={() => setSortMode('category')} style={shared.tab(sortMode === 'category')}>
                        By Category
                    </button>
                </div>
            )}

            {/* Legend */}
            {features.length > 0 && (
                <div style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    gap: tokens.space.sm, marginBottom: tokens.space.lg,
                }}>
                    <span style={{ fontSize: tokens.fontSize.xs, color: colors.red }}>-3</span>
                    <div style={{
                        flex: 1, maxWidth: '200px', height: '6px', borderRadius: '3px',
                        background: 'linear-gradient(to right, #EF4444, #080C10 50%, #22C55E)',
                    }} />
                    <span style={{ fontSize: tokens.fontSize.xs, color: colors.green }}>+3</span>
                </div>
            )}

            {loading ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px', fontSize: tokens.fontSize.md }}>
                    Loading feature data...
                </div>
            ) : error ? (
                <div style={shared.error}>{error}</div>
            ) : features.length === 0 ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px', fontSize: tokens.fontSize.md }}>
                    No feature data available. Run inference to generate a snapshot.
                </div>
            ) : sortMode === 'magnitude' ? (
                /* Flat list sorted by |z-score| */
                <div style={shared.card}>
                    {allSorted.map((f, i) => {
                        const name = f.feature_name || f.name || `feature_${i}`;
                        return (
                            <FeatureRow
                                key={name} f={f}
                                expanded={expandedFeature === name}
                                onToggle={() => setExpandedFeature(expandedFeature === name ? null : name)}
                            />
                        );
                    })}
                </div>
            ) : (
                /* Grouped by category */
                sortedCategories.map(cat => (
                    <div key={cat} style={{ marginBottom: tokens.space.md }}>
                        <div style={{
                            fontSize: tokens.fontSize.sm, fontWeight: 700, color: colors.textMuted,
                            letterSpacing: '1.5px', marginBottom: tokens.space.xs,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {cat.toUpperCase()} ({grouped[cat].length})
                        </div>
                        <div style={shared.card}>
                            {grouped[cat].map((f, i) => {
                                const name = f.feature_name || f.name || `feature_${i}`;
                                return (
                                    <FeatureRow
                                        key={name} f={f}
                                        expanded={expandedFeature === name}
                                        onToggle={() => setExpandedFeature(expandedFeature === name ? null : name)}
                                    />
                                );
                            })}
                        </div>
                    </div>
                ))
            )}
        </div>
    );
}
