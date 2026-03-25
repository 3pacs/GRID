/**
 * GRID Intelligence Feed — Associations View
 *
 * Replaces the old tab-based correlation/clustering UI with a prioritized
 * card feed. Each card is a self-contained insight sorted by importance.
 * Designed mobile-first for 390px iPhone screens.
 */
import React, { useEffect, useState, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens, shared } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

// ── Priority scoring ────────────────────────────────────────────────
function scoreCard(card) {
    switch (card.type) {
        case 'anomaly': return 100 + Math.abs(card.zscore || 0) * 10;
        case 'cluster_transition': return 90;
        case 'structure_change': return 80 + (card.magnitude || 0) * 5;
        case 'cross_family': return 50 + Math.abs(card.corr || 0) * 40;
        case 'regime_flip': return 40 + Math.abs(card.delta || 0) * 10;
        case 'feature_spotlight': return 20 + Math.abs(card.zscore || 0) * 5;
        default: return 0;
    }
}

// ── Filter definitions ──────────────────────────────────────────────
const FILTERS = [
    { id: 'all', label: 'All' },
    { id: 'anomalies', label: 'Anomalies' },
    { id: 'discoveries', label: 'Discoveries' },
    { id: 'regime', label: 'Regime' },
    { id: 'features', label: 'Features' },
];

const filterMatch = (card, filter) => {
    if (filter === 'all') return true;
    if (filter === 'anomalies') return card.type === 'anomaly';
    if (filter === 'discoveries') return card.type === 'cross_family' || card.type === 'structure_change';
    if (filter === 'regime') return card.type === 'regime_flip' || card.type === 'cluster_transition';
    if (filter === 'features') return card.type === 'feature_spotlight';
    return true;
};

// ── Mini Sparkline ──────────────────────────────────────────────────
function MiniSparkline({ values, height = 24, width = 56 }) {
    if (!values || values.length < 2) return null;
    const max = Math.max(...values.map(Math.abs), 0.001);
    const trend = values[values.length - 1] > values[0];
    const barColor = trend ? colors.green : colors.red;
    const barW = Math.max(2, (width / values.length) - 1);

    return (
        <div style={{
            display: 'flex', alignItems: 'flex-end', gap: '1px',
            height: `${height}px`, width: `${width}px`, flexShrink: 0,
        }}>
            {values.map((v, i) => (
                <div key={i} style={{
                    width: `${barW}px`,
                    height: `${Math.max(2, (Math.abs(v) / max) * height)}px`,
                    background: barColor,
                    borderRadius: '1px',
                    opacity: 0.4 + (i / values.length) * 0.6,
                }} />
            ))}
        </div>
    );
}

// ── Family Chip ─────────────────────────────────────────────────────
function FamilyChip({ family }) {
    const familyColors = {
        rates: '#3B82F6', credit: '#EF4444', equity: '#22C55E', vol: '#F59E0B',
        fx: '#06B6D4', commodity: '#A855F7', sentiment: '#EC4899', macro: '#8B5CF6',
        crypto: '#F97316', alternative: '#6366F1', flows: '#14B8A6', systemic: '#DC2626',
    };
    const bg = familyColors[family] || colors.textMuted;
    return (
        <span style={{
            fontSize: tokens.fontSize.xs, fontFamily: colors.mono,
            padding: '2px 6px', borderRadius: tokens.radius.sm,
            background: `${bg}20`, color: bg, fontWeight: 600,
        }}>
            {family}
        </span>
    );
}

// ── Card: Anomaly ───────────────────────────────────────────────────
function AnomalyInsightCard({ card }) {
    const [expanded, setExpanded] = useState(false);
    const isExtreme = (card.severity || '') === 'extreme';
    const accentColor = isExtreme ? colors.red : colors.yellow;
    const barPct = Math.min(100, (Math.abs(card.zscore || 0) / 5) * 100);

    return (
        <div onClick={() => setExpanded(!expanded)} style={{
            ...shared.card, borderLeft: `3px solid ${accentColor}`,
            cursor: 'pointer',
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.sm }}>
                <span style={{ fontSize: tokens.fontSize.lg, fontWeight: 700, color: colors.text }}>{card.feature}</span>
                <span style={{
                    fontSize: tokens.fontSize.xs, fontWeight: 700, padding: '3px 8px',
                    borderRadius: tokens.radius.sm, background: `${accentColor}22`, color: accentColor,
                }}>
                    {isExtreme ? 'EXTREME' : 'ELEVATED'}
                </span>
            </div>
            <div style={{
                height: '24px', borderRadius: tokens.radius.sm, background: colors.bg,
                position: 'relative', overflow: 'hidden', marginBottom: tokens.space.sm,
            }}>
                <div style={{
                    position: 'absolute', left: 0, top: 0, bottom: 0,
                    width: `${barPct}%`, background: accentColor, borderRadius: tokens.radius.sm,
                    transition: `width ${tokens.transition.normal}`,
                }} />
                <span style={{
                    position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)',
                    fontSize: tokens.fontSize.xs, color: '#fff', fontFamily: colors.mono, fontWeight: 600,
                }}>
                    z = {(card.zscore || 0).toFixed(2)}
                </span>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: tokens.space.sm }}>
                {[
                    { label: 'Current', value: card.current },
                    { label: 'Mean', value: card.mean },
                    { label: 'Std', value: card.std },
                ].map(m => (
                    <div key={m.label}>
                        <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>{m.label}</div>
                        <div style={{ fontSize: tokens.fontSize.md, fontFamily: colors.mono, color: colors.text }}>
                            {typeof m.value === 'number' ? m.value.toFixed(2) : m.value ?? '--'}
                        </div>
                    </div>
                ))}
            </div>
            {expanded && card.broken_correlations?.length > 0 && (
                <div style={{ marginTop: tokens.space.md, paddingTop: tokens.space.sm, borderTop: `1px solid ${colors.borderSubtle}` }}>
                    <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.xs, letterSpacing: '0.5px' }}>
                        BROKEN CORRELATIONS
                    </div>
                    {card.broken_correlations.map((bc, j) => (
                        <div key={j} style={{
                            display: 'flex', justifyContent: 'space-between', padding: '4px 0',
                            fontSize: tokens.fontSize.sm,
                        }}>
                            <span style={{ color: colors.textDim }}>{bc.partner}</span>
                            <span style={{ fontFamily: colors.mono }}>
                                <span style={{ color: colors.textMuted }}>{bc.historical_corr}</span>
                                <span style={{ color: colors.textMuted, margin: '0 4px' }}>&rarr;</span>
                                <span style={{ color: colors.yellow }}>{bc.recent_corr}</span>
                            </span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// ── Card: Cross-Family Discovery ────────────────────────────────────
function CrossFamilyCard({ card }) {
    const corrColor = (card.corr || 0) > 0 ? colors.green : colors.red;
    return (
        <div style={{ ...shared.card, borderLeft: `3px solid ${colors.accent}` }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div style={{ flex: 1 }}>
                    <div style={{ fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text, marginBottom: '2px' }}>
                        {card.a} &times; {card.b}
                    </div>
                    <div style={{ display: 'flex', gap: tokens.space.xs, marginTop: tokens.space.xs }}>
                        <FamilyChip family={card.family_a} />
                        <span style={{ color: colors.textMuted, fontSize: tokens.fontSize.xs }}>&times;</span>
                        <FamilyChip family={card.family_b} />
                    </div>
                </div>
                <div style={{ textAlign: 'right', flexShrink: 0, marginLeft: tokens.space.md }}>
                    <div style={{
                        fontSize: tokens.fontSize.xl, fontWeight: 800, fontFamily: colors.mono, color: corrColor,
                    }}>
                        {(card.corr || 0) > 0 ? '+' : ''}{(card.corr || 0).toFixed(3)}
                    </div>
                    <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>correlation</div>
                </div>
            </div>
        </div>
    );
}

// ── Card: Regime Flip ───────────────────────────────────────────────
function RegimeFlipCard({ card }) {
    const regimeColors = {
        GROWTH: colors.green, NEUTRAL: colors.accent, FRAGILE: colors.yellow, CRISIS: colors.red,
    };
    const color = regimeColors[card.regime] || colors.textMuted;
    const arrow = (card.after || 0) > (card.before || 0) ? '↑' : '↓';
    return (
        <div style={{ ...shared.card, borderLeft: `3px solid ${color}` }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div>
                    <div style={{ fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text }}>{card.feature}</div>
                    <div style={{ display: 'flex', gap: tokens.space.xs, alignItems: 'center', marginTop: '2px' }}>
                        <span style={{
                            fontSize: tokens.fontSize.xs, padding: '2px 6px', borderRadius: tokens.radius.sm,
                            background: `${color}22`, color, fontWeight: 600,
                        }}>
                            {card.regime}
                        </span>
                    </div>
                </div>
                <div style={{ textAlign: 'right', fontFamily: colors.mono }}>
                    <span style={{ color: colors.textMuted, fontSize: tokens.fontSize.sm }}>{(card.before || 0).toFixed(2)}</span>
                    <span style={{ color, fontSize: tokens.fontSize.lg, fontWeight: 700, margin: '0 4px' }}>{arrow}</span>
                    <span style={{ color, fontSize: tokens.fontSize.lg, fontWeight: 700 }}>{(card.after || 0).toFixed(2)}</span>
                </div>
            </div>
        </div>
    );
}

// ── Card: Feature Spotlight ─────────────────────────────────────────
function FeatureSpotlightCard({ card }) {
    const [expanded, setExpanded] = useState(false);
    const zColor = Math.abs(card.zscore || 0) > 2 ? (card.zscore > 0 ? colors.green : colors.red) : colors.textDim;
    return (
        <div onClick={() => setExpanded(!expanded)} style={{
            ...shared.card, cursor: 'pointer',
            background: expanded ? colors.cardElevated : colors.card,
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: tokens.space.sm }}>
                <div style={{ flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: tokens.space.sm }}>
                        <span style={{ fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text }}>{card.feature}</span>
                        <FamilyChip family={card.family} />
                    </div>
                </div>
                <span style={{
                    fontSize: tokens.fontSize.md, fontFamily: colors.mono, fontWeight: 600, color: zColor,
                }}>
                    {(card.zscore || 0) > 0 ? '+' : ''}{(card.zscore || 0).toFixed(2)}
                </span>
                {card.sparkline && <MiniSparkline values={card.sparkline} />}
            </div>
            {expanded && (
                <div style={{ marginTop: tokens.space.md, paddingTop: tokens.space.sm, borderTop: `1px solid ${colors.borderSubtle}` }}>
                    {card.top_correlations?.length > 0 && (
                        <div style={{ marginBottom: tokens.space.sm }}>
                            <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.xs }}>TOP CORRELATIONS</div>
                            {card.top_correlations.map((tc, i) => (
                                <div key={i} style={{ display: 'flex', justifyContent: 'space-between', padding: '3px 0', fontSize: tokens.fontSize.sm }}>
                                    <span style={{ color: colors.textDim }}>{tc.name}</span>
                                    <span style={{ fontFamily: colors.mono, color: tc.corr > 0 ? colors.green : colors.red }}>
                                        {tc.corr > 0 ? '+' : ''}{tc.corr.toFixed(3)}
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}
                    {card.regime_behavior && (
                        <div>
                            <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.xs }}>REGIME BEHAVIOR</div>
                            <div style={{ fontSize: tokens.fontSize.sm, color: colors.textDim }}>{card.regime_behavior}</div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

// ── Card: Structure Change ──────────────────────────────────────────
function StructureChangeCard({ card }) {
    return (
        <div style={{ ...shared.card, borderLeft: `3px solid ${colors.yellow}` }}>
            <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, letterSpacing: '0.5px', marginBottom: tokens.space.xs }}>
                MARKET STRUCTURE
            </div>
            <div style={{ fontSize: tokens.fontSize.lg, fontWeight: 700, color: colors.text, marginBottom: tokens.space.sm }}>
                Dimensionality: {card.prev_dims} &rarr; {card.new_dims}
            </div>
            <div style={{ fontSize: tokens.fontSize.sm, color: colors.textDim }}>
                {card.new_dims < card.prev_dims
                    ? 'Market is compressing — fewer independent factors driving prices.'
                    : 'Market is expanding — more independent factors at play.'}
                {card.redundant_count > 0 && ` (${card.redundant_count} redundant pairs detected)`}
            </div>
        </div>
    );
}

// ── Card Router ─────────────────────────────────────────────────────
function InsightCard({ card }) {
    switch (card.type) {
        case 'anomaly': return <AnomalyInsightCard card={card} />;
        case 'cross_family': return <CrossFamilyCard card={card} />;
        case 'regime_flip': return <RegimeFlipCard card={card} />;
        case 'feature_spotlight': return <FeatureSpotlightCard card={card} />;
        case 'structure_change': return <StructureChangeCard card={card} />;
        default: return null;
    }
}

// ── Data Transform: API responses → card objects ────────────────────
function buildCards(anomalies, correlations, regimeFeatures, smartHeatmap) {
    const cards = [];

    // Anomaly cards
    if (anomalies?.anomalies) {
        for (const a of anomalies.anomalies) {
            cards.push({
                type: 'anomaly', id: `anom-${a.feature}`,
                feature: a.feature, zscore: a.zscore, severity: a.severity,
                current: a.current_value, mean: a.historical_mean, std: a.historical_std,
                broken_correlations: a.broken_correlations || [],
            });
        }
    }

    // Cross-family discovery cards (only "interesting" pairs)
    if (correlations?.strong_pairs) {
        for (const p of correlations.strong_pairs.filter(p => p.kind === 'interesting').slice(0, 15)) {
            cards.push({
                type: 'cross_family', id: `corr-${p.a}-${p.b}`,
                a: p.a, b: p.b, corr: p.corr,
                family_a: p.family_a || '', family_b: p.family_b || '',
            });
        }
    }

    // Regime flip cards — features whose z-score sign changes between regimes
    if (regimeFeatures?.regimes) {
        const regimes = regimeFeatures.regimes;
        const regimeNames = Object.keys(regimes);
        // Compare each regime's top features against NEUTRAL baseline
        const baseline = regimes['NEUTRAL'] || regimes[regimeNames[0]] || [];
        const baselineMap = {};
        for (const f of baseline) baselineMap[f.feature] = f.avg_zscore;

        for (const regime of regimeNames) {
            if (regime === 'NEUTRAL') continue;
            for (const f of (regimes[regime] || []).slice(0, 5)) {
                const baseZ = baselineMap[f.feature] || 0;
                if ((baseZ > 0 && f.avg_zscore < -0.3) || (baseZ < 0 && f.avg_zscore > 0.3)) {
                    cards.push({
                        type: 'regime_flip', id: `flip-${regime}-${f.feature}`,
                        feature: f.feature, regime,
                        before: baseZ, after: f.avg_zscore,
                        delta: f.avg_zscore - baseZ,
                    });
                }
            }
        }
    }

    // Structure change card from smart heatmap
    if (smartHeatmap?.filtered_count != null && smartHeatmap?.total_count) {
        const redundant = (smartHeatmap.redundant_pairs || []).length;
        if (redundant > 0) {
            cards.push({
                type: 'structure_change', id: 'structure',
                prev_dims: smartHeatmap.total_count,
                new_dims: smartHeatmap.filtered_count,
                magnitude: redundant,
                redundant_count: redundant,
            });
        }
    }

    // Feature spotlight cards from z-scores
    if (smartHeatmap?.z_scores) {
        for (const z of smartHeatmap.z_scores) {
            // Find top correlations for this feature from the matrix
            let topCorrs = [];
            if (correlations?.features && correlations?.matrix) {
                const idx = correlations.features.indexOf(z.feature);
                if (idx >= 0) {
                    const row = correlations.matrix[idx] || [];
                    const pairs = row.map((c, j) => ({ name: correlations.features[j], corr: c }))
                        .filter((p, j) => j !== idx && Math.abs(p.corr) > 0.3)
                        .sort((a, b) => Math.abs(b.corr) - Math.abs(a.corr))
                        .slice(0, 3);
                    topCorrs = pairs;
                }
            }

            // Build regime behavior summary
            let regimeBehavior = '';
            if (regimeFeatures?.regimes) {
                for (const [regime, feats] of Object.entries(regimeFeatures.regimes)) {
                    const match = feats.find(f => f.feature === z.feature);
                    if (match && Math.abs(match.avg_zscore) > 1.0) {
                        regimeBehavior += `${regime}: ${match.avg_zscore > 0 ? '+' : ''}${match.avg_zscore.toFixed(1)}  `;
                    }
                }
            }

            cards.push({
                type: 'feature_spotlight', id: `feat-${z.feature}`,
                feature: z.feature, family: z.family || 'other',
                zscore: z.zscore,
                sparkline: null, // would need time-series endpoint
                top_correlations: topCorrs,
                regime_behavior: regimeBehavior || null,
            });
        }
    }

    // Score and sort
    cards.sort((a, b) => scoreCard(b) - scoreCard(a));
    return cards;
}

// ── Main Component ──────────────────────────────────────────────────
export default function Associations({ onNavigate }) {
    const { addNotification } = useStore();
    const [cards, setCards] = useState([]);
    const [filter, setFilter] = useState('all');
    const [loading, setLoading] = useState(true);
    const { isMobile } = useDevice();

    useEffect(() => {
        loadAll();
    }, []);

    const loadAll = async () => {
        setLoading(true);
        try {
            const [anomalies, correlations, regimeFeatures, smartHeatmap] = await Promise.all([
                api.getAnomalies().catch(() => null),
                api.getCorrelationMatrix().catch(() => null),
                api.getRegimeFeatures().catch(() => null),
                api.getSmartHeatmap(null, true).catch(() => null),
            ]);
            setCards(buildCards(anomalies, correlations, regimeFeatures, smartHeatmap));
        } catch (err) {
            addNotification('error', 'Failed to load association data');
        }
        setLoading(false);
    };

    const filtered = cards.filter(c => filterMatch(c, filter));

    const filterCounts = {};
    for (const f of FILTERS) {
        filterCounts[f.id] = f.id === 'all' ? cards.length : cards.filter(c => filterMatch(c, f.id)).length;
    }

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{
                fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
                color: colors.textMuted, letterSpacing: '2px', marginBottom: tokens.space.md,
            }}>
                ASSOCIATIONS
            </div>

            {/* Filter chips */}
            <div style={{
                ...shared.tabs, marginBottom: tokens.space.lg,
            }}>
                {FILTERS.map(f => (
                    <button key={f.id} onClick={() => setFilter(f.id)} style={{
                        padding: '8px 14px', borderRadius: tokens.radius.md,
                        fontSize: tokens.fontSize.sm, fontWeight: 600, cursor: 'pointer',
                        border: 'none', fontFamily: colors.sans, whiteSpace: 'nowrap',
                        minHeight: tokens.minTouch, display: 'inline-flex', alignItems: 'center',
                        gap: tokens.space.xs, transition: `all ${tokens.transition.fast}`,
                        background: filter === f.id ? colors.accent : colors.card,
                        color: filter === f.id ? '#fff' : colors.textMuted,
                        boxShadow: filter === f.id ? '0 2px 8px rgba(26,110,191,0.3)' : 'none',
                    }}>
                        {f.label}
                        <span style={{
                            fontSize: '10px', opacity: 0.7,
                            fontFamily: colors.mono,
                        }}>
                            {filterCounts[f.id] || 0}
                        </span>
                    </button>
                ))}
            </div>

            {/* Loading */}
            {loading && (
                <div style={{ textAlign: 'center', padding: '40px 0', color: colors.textMuted, fontSize: tokens.fontSize.md }}>
                    Scanning market structure...
                </div>
            )}

            {/* Feed */}
            {!loading && filtered.length === 0 && (
                <div style={{ textAlign: 'center', padding: '40px 0', color: colors.textMuted, fontSize: tokens.fontSize.md }}>
                    No insights for this filter. Try "All".
                </div>
            )}

            {!loading && filtered.map(card => (
                <InsightCard key={card.id} card={card} />
            ))}

            {/* Legacy link */}
            {!loading && (
                <div style={{
                    textAlign: 'center', padding: `${tokens.space.xl} 0`,
                    marginTop: tokens.space.lg,
                }}>
                    <span
                        onClick={() => onNavigate?.('associations-legacy')}
                        style={{
                            fontSize: tokens.fontSize.sm, color: colors.textMuted,
                            cursor: 'pointer', textDecoration: 'underline',
                            textDecorationColor: colors.border,
                        }}
                    >
                        Open classic correlation matrix view &rarr;
                    </span>
                </div>
            )}
        </div>
    );
}
