/**
 * GRID Heatmap — Full-page z-score heatmap with correlation matrix.
 *
 * Two modes:
 *   1. Z-Score Grid: dense tile grid of all features colored by z-score
 *   2. Correlation Matrix: NxN grid showing feature-pair correlations
 *
 * Data from /api/v1/discovery/smart-heatmap (orthogonal features + matrix)
 * and /api/v1/signals/snapshot (all feature z-scores).
 */
import React, { useEffect, useState, useRef, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';
import ViewHelp from '../components/ViewHelp.jsx';

/* ── colour helpers ─────────────────────────────────────────────── */

function zToColor(z, alpha = 1) {
    if (z == null || isNaN(z)) return colors.border;
    const c = Math.max(-3, Math.min(3, z));
    const t = (c + 3) / 6;
    let r, g, b;
    if (t < 0.5) {
        const s = t / 0.5;
        r = Math.round(220 * (1 - s) + 30 * s);
        g = Math.round(50 * (1 - s) + 40 * s);
        b = Math.round(50 * (1 - s) + 55 * s);
    } else {
        const s = (t - 0.5) / 0.5;
        r = Math.round(30 * (1 - s) + 34 * s);
        g = Math.round(40 * (1 - s) + 197 * s);
        b = Math.round(55 * (1 - s) + 94 * s);
    }
    return alpha < 1 ? `rgba(${r},${g},${b},${alpha})` : `rgb(${r},${g},${b})`;
}

function corrToColor(corr) {
    if (corr == null || isNaN(corr)) return colors.border;
    const c = Math.max(-1, Math.min(1, corr));
    const t = (c + 1) / 2;
    let r, g, b;
    if (t < 0.5) {
        const s = t / 0.5;
        r = Math.round(59 * (1 - s) + 20 * s);
        g = Math.round(130 * (1 - s) + 30 * s);
        b = Math.round(246 * (1 - s) + 55 * s);
    } else {
        const s = (t - 0.5) / 0.5;
        r = Math.round(20 * (1 - s) + 239 * s);
        g = Math.round(30 * (1 - s) + 68 * s);
        b = Math.round(55 * (1 - s) + 68 * s);
    }
    return `rgb(${r},${g},${b})`;
}

/* ── categorize features ────────────────────────────────────────── */

function categorize(name) {
    const n = (name || '').toLowerCase();
    if (/rate|yield|treasury|dgs|t10y|t5y/.test(n)) return 'Rates';
    if (/cpi|pce|inflation|breakeven/.test(n)) return 'Inflation';
    if (/gdp|employ|payroll|unrate|claims|labor|nfp/.test(n)) return 'Employment';
    if (/sentiment|vix|fear|confidence|umich|aaii|put_call/.test(n)) return 'Sentiment';
    if (/dollar|dxy|eur|jpy|gbp|fx/.test(n)) return 'FX';
    if (/oil|gold|copper|wti|brent|commod|wheat|corn|natural_gas/.test(n)) return 'Commodities';
    if (/sp500|nasdaq|equity|russell|dow|stock|wilshire/.test(n)) return 'Equities';
    if (/credit|hy|ig|default|oas|baml|ted/.test(n)) return 'Credit';
    if (/money|m2|liquidity|reserve|fed_fund|monetary/.test(n)) return 'Monetary';
    if (/pmi|ism|industrial|manufacturing|production/.test(n)) return 'Manufacturing';
    if (/housing|permits|starts|existing|new_home/.test(n)) return 'Housing';
    return 'Other';
}

const CATEGORY_ORDER = [
    'Rates', 'Inflation', 'Employment', 'Sentiment', 'Equities',
    'Credit', 'FX', 'Commodities', 'Monetary', 'Manufacturing',
    'Housing', 'Other',
];

const CATEGORY_COLORS = {
    Rates: '#8B5CF6', Inflation: '#EF4444', Employment: '#F59E0B',
    Sentiment: '#22C55E', Equities: '#3B82F6', Credit: '#EC4899',
    FX: '#06B6D4', Commodities: '#F59E0B', Monetary: '#8B5CF6',
    Manufacturing: '#6366F1', Housing: '#14B8A6', Other: '#6B7280',
};

function shortLabel(name) {
    return (name || '')
        .replace(/^fred_|^yf_/i, '')
        .replace(/_/g, ' ')
        .toUpperCase()
        .substring(0, 12);
}

/* ── Z-Score Grid ───────────────────────────────────────────────── */

function ZScoreGrid({ features, isMobile }) {
    const [hoveredIdx, setHoveredIdx] = useState(null);

    // Group by category
    const grouped = {};
    for (const f of features) {
        const cat = categorize(f.feature_name || f.name || '');
        if (!grouped[cat]) grouped[cat] = [];
        grouped[cat].push(f);
    }
    // Sort within each group by |z-score| desc
    for (const cat of Object.keys(grouped)) {
        grouped[cat].sort((a, b) => {
            const az = Math.abs(a.z_score ?? a.zscore ?? 0);
            const bz = Math.abs(b.z_score ?? b.zscore ?? 0);
            return bz - az;
        });
    }
    const sortedCats = CATEGORY_ORDER.filter(c => grouped[c]);
    for (const c of Object.keys(grouped)) {
        if (!sortedCats.includes(c)) sortedCats.push(c);
    }

    const cols = isMobile ? 4 : 6;

    return (
        <div>
            {sortedCats.map(cat => (
                <div key={cat} style={{ marginBottom: '16px' }}>
                    <div style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                        color: CATEGORY_COLORS[cat] || colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace",
                        marginBottom: '6px',
                        display: 'flex', alignItems: 'center', gap: '8px',
                    }}>
                        <span>{cat.toUpperCase()}</span>
                        <span style={{ fontSize: '9px', color: colors.textMuted, fontWeight: 400 }}>
                            {grouped[cat].length}
                        </span>
                    </div>
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: `repeat(${cols}, 1fr)`,
                        gap: '2px',
                    }}>
                        {grouped[cat].map((f, i) => {
                            const z = f.z_score ?? f.zscore ?? null;
                            const name = shortLabel(f.feature_name || f.name);
                            const bg = zToColor(z);
                            const globalIdx = `${cat}-${i}`;
                            const isHovered = hoveredIdx === globalIdx;

                            return (
                                <div
                                    key={globalIdx}
                                    onMouseEnter={() => setHoveredIdx(globalIdx)}
                                    onMouseLeave={() => setHoveredIdx(null)}
                                    title={`${f.feature_name || f.name}\nz-score: ${z != null ? z.toFixed(3) : '--'}\nvalue: ${f.value ?? '--'}`}
                                    style={{
                                        background: bg,
                                        borderRadius: '3px',
                                        padding: '5px 3px',
                                        textAlign: 'center',
                                        minHeight: '42px',
                                        display: 'flex',
                                        flexDirection: 'column',
                                        justifyContent: 'center',
                                        cursor: 'default',
                                        opacity: z != null ? 1 : 0.25,
                                        transform: isHovered ? 'scale(1.08)' : 'scale(1)',
                                        transition: 'transform 0.12s ease',
                                        zIndex: isHovered ? 2 : 1,
                                        position: 'relative',
                                        boxShadow: isHovered ? '0 2px 8px rgba(0,0,0,0.5)' : 'none',
                                    }}
                                >
                                    <div title={name} style={{
                                        fontSize: '8px', fontWeight: 700, color: '#fff',
                                        fontFamily: "'JetBrains Mono', monospace",
                                        lineHeight: '1.2',
                                        textShadow: '0 1px 3px rgba(0,0,0,0.7)',
                                        overflow: 'hidden', textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap', textTransform: 'uppercase',
                                    }}>
                                        {name}
                                    </div>
                                    <div style={{
                                        fontSize: '11px', fontWeight: 700, color: '#fff',
                                        fontFamily: "'JetBrains Mono', monospace",
                                        lineHeight: '1.3',
                                        textShadow: '0 1px 3px rgba(0,0,0,0.7)',
                                    }}>
                                        {z != null ? (z >= 0 ? '+' : '') + z.toFixed(1) : ''}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            ))}
        </div>
    );
}

/* ── Correlation Matrix (canvas-rendered) ───────────────────────── */

function CorrelationMatrix({ features, matrix, isMobile }) {
    const canvasRef = useRef(null);
    const containerRef = useRef(null);
    const [tooltip, setTooltip] = useState(null);

    const n = features.length;
    const cellSize = isMobile ? 8 : Math.max(6, Math.min(14, Math.floor(600 / n)));
    const labelSpace = isMobile ? 0 : 100;
    const canvasW = labelSpace + n * cellSize;
    const canvasH = labelSpace + n * cellSize;

    const draw = useCallback(() => {
        const canvas = canvasRef.current;
        if (!canvas || !matrix.length) return;
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        canvas.width = canvasW * dpr;
        canvas.height = canvasH * dpr;
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, canvasW, canvasH);

        // Draw cells
        for (let i = 0; i < n; i++) {
            for (let j = 0; j < n; j++) {
                const val = matrix[i]?.[j];
                ctx.fillStyle = corrToColor(val);
                ctx.fillRect(labelSpace + j * cellSize, labelSpace + i * cellSize, cellSize, cellSize);
            }
        }

        // Draw labels (only if space allows)
        if (!isMobile && cellSize >= 8) {
            ctx.fillStyle = colors.textMuted;
            ctx.font = `${Math.min(9, cellSize - 1)}px 'JetBrains Mono', monospace`;
            ctx.textAlign = 'right';
            ctx.textBaseline = 'middle';
            for (let i = 0; i < n; i++) {
                const label = shortLabel(features[i]).substring(0, 12);
                ctx.fillText(label, labelSpace - 4, labelSpace + i * cellSize + cellSize / 2);
            }
        }
    }, [matrix, features, n, cellSize, canvasW, canvasH, isMobile, labelSpace]);

    useEffect(() => { draw(); }, [draw]);

    const handleMouseMove = (e) => {
        const canvas = canvasRef.current;
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const y = e.clientY - rect.top;
        const scaleX = canvasW / rect.width;
        const scaleY = canvasH / rect.height;
        const col = Math.floor((x * scaleX - labelSpace) / cellSize);
        const row = Math.floor((y * scaleY - labelSpace) / cellSize);
        if (row >= 0 && row < n && col >= 0 && col < n) {
            const val = matrix[row]?.[col];
            setTooltip({
                x: e.clientX, y: e.clientY,
                text: `${shortLabel(features[row])} x ${shortLabel(features[col])}: ${val != null ? val.toFixed(3) : '--'}`,
            });
        } else {
            setTooltip(null);
        }
    };

    return (
        <div ref={containerRef} style={{ position: 'relative' }}>
            <div style={{ overflowX: 'auto', overflowY: 'auto', maxHeight: '70vh' }}>
                <canvas
                    ref={canvasRef}
                    style={{ width: canvasW, height: canvasH, display: 'block' }}
                    onMouseMove={handleMouseMove}
                    onMouseLeave={() => setTooltip(null)}
                />
            </div>
            {tooltip && (
                <div style={{
                    position: 'fixed', left: tooltip.x + 12, top: tooltip.y - 30,
                    background: '#000', color: '#fff', padding: '4px 8px',
                    borderRadius: '4px', fontSize: '11px', pointerEvents: 'none',
                    fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap',
                    zIndex: 100, boxShadow: '0 2px 8px rgba(0,0,0,0.5)',
                }}>
                    {tooltip.text}
                </div>
            )}
            {/* Legend */}
            <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                gap: '8px', marginTop: '12px',
            }}>
                <span style={{ fontSize: '10px', color: '#3B82F6', fontFamily: "'JetBrains Mono', monospace" }}>-1</span>
                <div style={{
                    width: '120px', height: '6px', borderRadius: '3px',
                    background: 'linear-gradient(to right, #3B82F6, #141E37 50%, #EF4444)',
                }} />
                <span style={{ fontSize: '10px', color: '#EF4444', fontFamily: "'JetBrains Mono', monospace" }}>+1</span>
            </div>
        </div>
    );
}

/* ── Main Heatmap View ──────────────────────────────────────────── */

export default function Heatmap() {
    const { addNotification, setLoading } = useStore();
    const { isMobile } = useDevice();

    const [mode, setMode] = useState('zscore'); // 'zscore' | 'correlation'
    const [features, setFeatures] = useState([]);
    const [corrFeatures, setCorrFeatures] = useState([]);
    const [matrix, setMatrix] = useState([]);
    const [families, setFamilies] = useState([]);
    const [familyFilter, setFamilyFilter] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLocalLoading] = useState(true);
    const [stats, setStats] = useState({});

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLocalLoading(true);
        setLoading('heatmap', true);
        setError(null);
        try {
            const [snapshot, heatmap] = await Promise.all([
                api.getSignalSnapshot().catch(() => ({ features: [] })),
                api.getSmartHeatmap(familyFilter, true).catch(() => ({
                    features: [], matrix: [], z_scores: [], families: [],
                })),
            ]);

            const feats = snapshot.features || [];
            setFeatures(feats);
            setCorrFeatures(heatmap.features || []);
            setMatrix(heatmap.matrix || []);
            setFamilies(heatmap.families || []);

            // Stats
            const zScores = feats.map(f => f.z_score ?? f.zscore ?? null).filter(z => z != null);
            setStats({
                total: feats.length,
                bullish: zScores.filter(z => z > 0.5).length,
                bearish: zScores.filter(z => z < -0.5).length,
                extreme: zScores.filter(z => Math.abs(z) > 2.5).length,
                orthogonal: (heatmap.features || []).length,
            });
        } catch (err) {
            setError(err.message || 'Failed to load heatmap data');
            addNotification('error', 'Heatmap load failed');
        }
        setLocalLoading(false);
        setLoading('heatmap', false);
    };

    // Reload when family filter changes
    useEffect(() => {
        if (!loading) loadData();
    }, [familyFilter]);

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginBottom: tokens.space.md,
            }}>
                <div style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
                    color: colors.textMuted, letterSpacing: '2px',
                }}>
                    HEATMAP
                </div>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                    <ViewHelp id="heatmap" />
                    <button onClick={loadData} style={shared.buttonSmall}>Refresh</button>
                </div>
            </div>

            {/* Summary */}
            {stats.total > 0 && (
                <div style={{ ...shared.metricGrid, marginBottom: tokens.space.lg }}>
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{stats.total}</div>
                        <div style={shared.metricLabel}>Features</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.green }}>{stats.bullish}</div>
                        <div style={shared.metricLabel}>Bullish</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.red }}>{stats.bearish}</div>
                        <div style={shared.metricLabel}>Bearish</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.yellow }}>{stats.extreme}</div>
                        <div style={shared.metricLabel}>Extreme</div>
                    </div>
                </div>
            )}

            {/* Mode toggle */}
            <div style={{ ...shared.tabs, marginBottom: tokens.space.md }}>
                <button onClick={() => setMode('zscore')} style={shared.tab(mode === 'zscore')}>
                    Z-Scores
                </button>
                <button onClick={() => setMode('correlation')} style={shared.tab(mode === 'correlation')}>
                    Correlations
                </button>
            </div>

            {/* Family filter (correlation mode) */}
            {mode === 'correlation' && families.length > 0 && (
                <div style={{
                    display: 'flex', gap: '6px', flexWrap: 'wrap',
                    marginBottom: tokens.space.md,
                }}>
                    <button
                        onClick={() => setFamilyFilter(null)}
                        style={{
                            ...pillStyle,
                            background: familyFilter === null ? colors.accent : colors.card,
                            color: familyFilter === null ? '#fff' : colors.textMuted,
                        }}
                    >All</button>
                    {families.map(fam => (
                        <button
                            key={fam}
                            onClick={() => setFamilyFilter(fam)}
                            style={{
                                ...pillStyle,
                                background: familyFilter === fam ? colors.accent : colors.card,
                                color: familyFilter === fam ? '#fff' : colors.textMuted,
                            }}
                        >{fam}</button>
                    ))}
                </div>
            )}

            {/* Z-Score legend */}
            {mode === 'zscore' && features.length > 0 && (
                <div style={{
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    gap: '8px', marginBottom: tokens.space.lg,
                }}>
                    <span style={{ fontSize: '10px', color: colors.red, fontFamily: "'JetBrains Mono', monospace" }}>-3</span>
                    <div style={{
                        flex: 1, maxWidth: '200px', height: '6px', borderRadius: '3px',
                        background: 'linear-gradient(to right, #DC3232, #1E2837 50%, #22C55E)',
                    }} />
                    <span style={{ fontSize: '10px', color: colors.green, fontFamily: "'JetBrains Mono', monospace" }}>+3</span>
                </div>
            )}

            {/* Content */}
            {loading ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                    Loading...
                </div>
            ) : error ? (
                <div style={shared.error}>{error}</div>
            ) : mode === 'zscore' ? (
                features.length > 0 ? (
                    <ZScoreGrid features={features} isMobile={isMobile} />
                ) : (
                    <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                        No feature data. Run inference to generate a snapshot.
                    </div>
                )
            ) : (
                matrix.length > 0 ? (
                    <CorrelationMatrix features={corrFeatures} matrix={matrix} isMobile={isMobile} />
                ) : (
                    <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>
                        No correlation data available.
                    </div>
                )
            )}
        </div>
    );
}

const pillStyle = {
    border: `1px solid ${colors.border}`,
    borderRadius: tokens.radius.pill,
    padding: '6px 14px',
    fontSize: '11px',
    fontFamily: "'JetBrains Mono', monospace",
    cursor: 'pointer',
    minHeight: '32px',
    transition: `all ${tokens.transition.fast}`,
};
