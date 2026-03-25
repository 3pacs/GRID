import React, { useEffect, useState, useRef, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, tokens, shared } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

// ---------- shared local styles ----------
const s = {
    container: { ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg,
        color: colors.textMuted, letterSpacing: '2px', marginBottom: tokens.space.lg,
    },
    card: { ...shared.card },
    tabs: { ...shared.tabs },
    tab: shared.tab,
    empty: { color: colors.textMuted, textAlign: 'center', padding: '40px 0', fontSize: tokens.fontSize.md },
    loading: { color: colors.textMuted, textAlign: 'center', padding: '40px 0', fontSize: tokens.fontSize.md },
    sectionTitle: { ...shared.sectionTitle },
    select: {
        ...shared.input, maxWidth: '260px', display: 'inline-block',
        marginRight: tokens.space.sm, marginBottom: tokens.space.sm,
    },
    barOuter: {
        background: colors.bg, borderRadius: tokens.radius.sm, height: '24px',
        position: 'relative', overflow: 'hidden', marginTop: tokens.space.xs,
    },
    barInner: (width, color) => ({
        position: 'absolute', left: 0, top: 0, bottom: 0,
        width: `${Math.min(100, Math.abs(width))}%`,
        background: color, borderRadius: tokens.radius.sm,
        transition: `width ${tokens.transition.normal}`,
    }),
    barLabel: {
        position: 'absolute', right: '8px', top: '50%', transform: 'translateY(-50%)',
        fontSize: tokens.fontSize.xs, color: '#fff', fontFamily: colors.mono,
    },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '12px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        minHeight: tokens.minTouch,
    },
    mono: { fontFamily: colors.mono, fontSize: tokens.fontSize.md },
    badge: (bg, fg) => ({
        display: 'inline-flex', alignItems: 'center', padding: '3px 10px',
        borderRadius: tokens.radius.sm, fontSize: tokens.fontSize.xs,
        fontWeight: 600, background: bg, color: fg, fontFamily: colors.mono,
    }),
};

const TABS = [
    { id: 'heatmap', label: 'Correlations', short: 'Corr' },
    { id: 'clusters', label: 'Clusters', short: 'Clust' },
    { id: 'regimes', label: 'Regime Fingerprints', short: 'Regime' },
    { id: 'anomalies', label: 'Anomalies', short: 'Anom' },
    { id: 'lag', label: 'Lag Explorer', short: 'Lag' },
];

// ---------- Correlation Heatmap ----------
function HeatmapCanvas({ features, matrix, onCellClick }) {
    const canvasRef = useRef(null);
    const wrapperRef = useRef(null);
    const [tooltip, setTooltip] = useState(null);
    const [containerWidth, setContainerWidth] = useState(600);
    const { isMobile } = useDevice();
    const n = features.length;

    // Measure container
    useEffect(() => {
        if (!wrapperRef.current) return;
        const measure = () => {
            const w = wrapperRef.current?.clientWidth || 600;
            setContainerWidth(w);
        };
        measure();
        window.addEventListener('resize', measure);
        return () => window.removeEventListener('resize', measure);
    }, []);

    const labelSpace = isMobile && n > 12 ? 80 : 120;
    const maxLabelChars = isMobile && n > 12 ? 10 : 16;
    const available = containerWidth - labelSpace;
    const cellSize = Math.max(16, Math.min(40, Math.floor(available / (n || 1))));
    const width = labelSpace + n * cellSize;
    const height = labelSpace + n * cellSize;

    const corrColor = useCallback((v) => {
        if (v > 0) {
            const t = Math.min(v, 1);
            return `rgba(34,197,94,${t * 0.85 + 0.15})`;
        } else {
            const t = Math.min(-v, 1);
            return `rgba(239,68,68,${t * 0.85 + 0.15})`;
        }
    }, []);

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas || n === 0) return;
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        canvas.width = width * dpr;
        canvas.height = height * dpr;
        canvas.style.width = `${width}px`;
        canvas.style.height = `${height}px`;
        ctx.scale(dpr, dpr);

        ctx.fillStyle = colors.bg;
        ctx.fillRect(0, 0, width, height);

        for (let i = 0; i < n; i++) {
            for (let j = 0; j < n; j++) {
                const v = matrix[i]?.[j] ?? 0;
                const x = labelSpace + j * cellSize;
                const y = labelSpace + i * cellSize;
                const gap = Math.max(1, Math.floor(cellSize * 0.06));

                ctx.fillStyle = corrColor(v);
                ctx.beginPath();
                const r = Math.min(3, cellSize * 0.15);
                const w = cellSize - gap;
                const h = cellSize - gap;
                ctx.roundRect(x, y, w, h, r);
                ctx.fill();

                if (i !== j && Math.abs(v) > 0.7) {
                    ctx.strokeStyle = colors.yellow;
                    ctx.lineWidth = 1.5;
                    ctx.beginPath();
                    ctx.roundRect(x, y, w, h, r);
                    ctx.stroke();
                }
            }
        }

        const labelFontSize = Math.max(11, Math.min(13, cellSize - 2));
        ctx.font = `${labelFontSize}px "IBM Plex Mono", monospace`;
        ctx.fillStyle = colors.textMuted;

        // Top labels (rotated)
        ctx.save();
        ctx.textAlign = 'left';
        for (let j = 0; j < n; j++) {
            ctx.save();
            ctx.translate(labelSpace + j * cellSize + cellSize / 2, labelSpace - 6);
            ctx.rotate(-Math.PI / 4);
            const label = features[j].length > maxLabelChars
                ? features[j].slice(0, maxLabelChars - 1) + '..'
                : features[j];
            ctx.fillText(label, 0, 0);
            ctx.restore();
        }
        ctx.restore();

        // Left labels
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        for (let i = 0; i < n; i++) {
            const label = features[i].length > maxLabelChars
                ? features[i].slice(0, maxLabelChars - 1) + '..'
                : features[i];
            ctx.fillText(label, labelSpace - 6, labelSpace + i * cellSize + cellSize / 2);
        }
    }, [features, matrix, n, cellSize, width, height, corrColor, labelSpace, maxLabelChars]);

    const getCellFromEvent = (e) => {
        const rect = canvasRef.current.getBoundingClientRect();
        const clientX = e.touches ? e.touches[0].clientX : e.clientX;
        const clientY = e.touches ? e.touches[0].clientY : e.clientY;
        const x = clientX - rect.left - labelSpace;
        const y = clientY - rect.top - labelSpace;
        const j = Math.floor(x / cellSize);
        const i = Math.floor(y / cellSize);
        return { i, j, clientX: clientX - rect.left, clientY: clientY - rect.top };
    };

    const handlePointerDown = (e) => {
        const { i, j } = getCellFromEvent(e);
        if (i >= 0 && i < n && j >= 0 && j < n && i !== j) {
            onCellClick?.(features[i], features[j]);
        }
    };

    const handlePointerMove = (e) => {
        const { i, j, clientX, clientY } = getCellFromEvent(e);
        if (i >= 0 && i < n && j >= 0 && j < n) {
            setTooltip({
                x: clientX, y: clientY,
                text: `${features[i]} × ${features[j]}: ${(matrix[i]?.[j] ?? 0).toFixed(3)}`,
            });
        } else {
            setTooltip(null);
        }
    };

    return (
        <div ref={wrapperRef} style={{ position: 'relative', overflowX: 'auto', maxWidth: '100%', WebkitOverflowScrolling: 'touch' }}>
            <canvas
                ref={canvasRef}
                onPointerDown={handlePointerDown}
                onPointerMove={handlePointerMove}
                onPointerLeave={() => setTooltip(null)}
                style={{ cursor: 'crosshair', display: 'block', touchAction: 'pan-y' }}
            />
            {/* Color legend */}
            <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'center',
                gap: tokens.space.sm, marginTop: tokens.space.md, padding: `0 ${labelSpace}px`,
            }}>
                <span style={{ fontSize: tokens.fontSize.xs, color: colors.red, fontFamily: colors.mono }}>-1</span>
                <div style={{
                    flex: 1, maxWidth: '200px', height: '8px', borderRadius: tokens.radius.sm,
                    background: 'linear-gradient(to right, #EF4444, #080C10 50%, #22C55E)',
                }} />
                <span style={{ fontSize: tokens.fontSize.xs, color: colors.green, fontFamily: colors.mono }}>+1</span>
            </div>
            {tooltip && (
                <div style={{
                    position: 'absolute', left: tooltip.x + 12, top: tooltip.y - 32,
                    background: colors.glassOverlay,
                    backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
                    border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.md, padding: '8px 14px',
                    fontSize: tokens.fontSize.sm, color: colors.text,
                    fontFamily: colors.mono, pointerEvents: 'none',
                    whiteSpace: 'nowrap', zIndex: 10,
                    boxShadow: colors.shadow.md,
                }}>
                    {tooltip.text}
                </div>
            )}
        </div>
    );
}

function CorrelationTab() {
    const { addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [selectedPair, setSelectedPair] = useState(null);
    const [lagData, setLagData] = useState(null);
    const [lagLoading, setLagLoading] = useState(false);
    const [useSmartFilter, setUseSmartFilter] = useState(true);
    const [family, setFamily] = useState('');
    const [families, setFamilies] = useState([]);
    const { isMobile } = useDevice();

    useEffect(() => { loadData(); }, [useSmartFilter, family]);

    const loadData = async () => {
        setLoading(true);
        try {
            if (useSmartFilter) {
                const d = await api.getSmartHeatmap(family || null, true);
                setData(d);
                if (d.families?.length) setFamilies(d.families);
            } else {
                const d = await api.getCorrelationMatrix();
                setData(d);
            }
        } catch (err) {
            addNotification('error', 'Failed to load correlation matrix');
        }
        setLoading(false);
    };

    const handleCellClick = async (a, b) => {
        setSelectedPair({ a, b });
        setLagLoading(true);
        try {
            const d = await api.getLagAnalysis(a, b);
            setLagData(d);
        } catch (err) {
            addNotification('error', `Lag analysis failed: ${err.message}`);
            setLagData(null);
        }
        setLagLoading(false);
    };

    if (loading) return <div style={s.loading}>Loading correlation matrix...</div>;
    if (!data || !data.features?.length) return <div style={s.empty}>No feature data available</div>;

    return (
        <div>
            <div style={s.card}>
                <div style={s.sectionTitle}>CORRELATION HEATMAP</div>
                <div style={{
                    display: 'flex', flexWrap: 'wrap', gap: tokens.space.sm,
                    alignItems: 'center', marginBottom: tokens.space.md,
                }}>
                    <button
                        onClick={() => setUseSmartFilter(!useSmartFilter)}
                        style={{
                            ...shared.buttonSmall,
                            background: useSmartFilter ? colors.accent : colors.card,
                            border: `1px solid ${useSmartFilter ? colors.accent : colors.border}`,
                            color: useSmartFilter ? '#fff' : colors.textMuted,
                        }}
                    >
                        {useSmartFilter ? 'Orthogonal' : 'All Features'}
                    </button>
                    {useSmartFilter && families.length > 0 && (
                        <select
                            value={family}
                            onChange={e => setFamily(e.target.value)}
                            style={{ ...s.select, maxWidth: isMobile ? '100%' : '180px', marginBottom: 0 }}
                        >
                            <option value="">All Families</option>
                            {families.map(f => <option key={f} value={f}>{f}</option>)}
                        </select>
                    )}
                    {data?.filtered_count != null && (
                        <span style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>
                            {data.filtered_count}/{data.total_count} features
                        </span>
                    )}
                </div>
                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.sm }}>
                    Tap a cell to see lag analysis. Yellow borders = |corr| &gt; 0.7
                </div>
                <HeatmapCanvas
                    features={data.features}
                    matrix={data.matrix}
                    onCellClick={handleCellClick}
                />
            </div>

            {data.strong_pairs?.length > 0 && (() => {
                const kindColors = {
                    interesting: { border: colors.accent, label: 'CROSS-FAMILY', labelBg: '#1A6EBF22', labelFg: colors.accent },
                    expected: { border: colors.borderSubtle, label: 'SAME FAMILY', labelBg: '#5A708022', labelFg: colors.textMuted },
                    trivial: { border: colors.borderSubtle, label: 'DUPLICATE', labelBg: '#5A708015', labelFg: '#4A6070' },
                };
                const interesting = data.strong_pairs.filter(p => p.kind === 'interesting');
                const expected = data.strong_pairs.filter(p => p.kind === 'expected');
                const pairCounts = data.pair_counts || {};
                return (
                    <>
                        <div style={s.card}>
                            <div style={s.sectionTitle}>
                                CROSS-FAMILY DISCOVERIES ({interesting.length})
                            </div>
                            <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginBottom: tokens.space.sm }}>
                                Correlations between different asset classes — these are the real insights.
                                {pairCounts.trivial > 0 && ` (${pairCounts.trivial} trivial duplicates hidden)`}
                            </div>
                            {interesting.length === 0 && (
                                <div style={s.empty}>No cross-family correlations above threshold</div>
                            )}
                            {interesting.slice(0, 20).map((p, i) => (
                                <div key={i} style={{
                                    ...s.row, cursor: 'pointer',
                                    borderLeft: `3px solid ${colors.accent}`,
                                    paddingLeft: tokens.space.md,
                                }} onClick={() => handleCellClick(p.a, p.b)}>
                                    <div style={{ flex: 1 }}>
                                        <div style={{ fontSize: tokens.fontSize.sm, color: colors.text }}>
                                            {p.a} &harr; {p.b}
                                        </div>
                                        <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginTop: '2px' }}>
                                            {p.family_a || '?'} × {p.family_b || '?'}
                                        </div>
                                    </div>
                                    <span style={{
                                        ...s.mono,
                                        color: p.corr > 0 ? colors.green : colors.red,
                                        fontWeight: 600,
                                    }}>
                                        {p.corr > 0 ? '+' : ''}{p.corr.toFixed(3)}
                                    </span>
                                </div>
                            ))}
                        </div>

                        {expected.length > 0 && (
                            <div style={s.card}>
                                <div style={s.sectionTitle}>
                                    SAME-FAMILY PAIRS ({expected.length})
                                </div>
                                {expected.slice(0, 10).map((p, i) => (
                                    <div key={i} style={{
                                        ...s.row, cursor: 'pointer', opacity: 0.7,
                                    }} onClick={() => handleCellClick(p.a, p.b)}>
                                        <span style={{ fontSize: tokens.fontSize.sm, color: colors.textDim }}>
                                            {p.a} &harr; {p.b}
                                        </span>
                                        <span style={{
                                            ...s.mono, fontSize: tokens.fontSize.sm,
                                            color: colors.textMuted,
                                        }}>
                                            {p.corr > 0 ? '+' : ''}{p.corr.toFixed(3)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </>
                );
            })()}

            {selectedPair && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>
                        LAG ANALYSIS: {selectedPair.a} vs {selectedPair.b}
                    </div>
                    {lagLoading && <div style={s.loading}>Computing lag analysis...</div>}
                    {lagData && !lagLoading && <LagChart data={lagData} />}
                </div>
            )}
        </div>
    );
}

// ---------- Lag Chart ----------
function LagChart({ data }) {
    if (!data?.lags?.length) return <div style={s.empty}>No lag data</div>;

    const maxCorr = Math.max(...data.lags.map(l => Math.abs(l.correlation)), 0.01);

    return (
        <div>
            <div style={{
                fontSize: tokens.fontSize.sm, color: colors.accent,
                marginBottom: tokens.space.sm, fontFamily: colors.mono,
            }}>
                {data.direction} (strength: {data.strength})
            </div>
            <div style={{ maxHeight: '360px', overflowY: 'auto' }}>
                {data.lags.map(l => {
                    const isOptimal = l.lag === data.optimal_lag;
                    return (
                        <div key={l.lag} style={{
                            display: 'flex', alignItems: 'center', gap: tokens.space.sm,
                            marginBottom: tokens.space.xs,
                        }}>
                            <span style={{
                                ...s.mono, width: '44px', textAlign: 'right',
                                fontSize: tokens.fontSize.sm, color: isOptimal ? colors.accent : colors.textMuted,
                                fontWeight: isOptimal ? 700 : 400,
                            }}>
                                {l.lag > 0 ? `+${l.lag}` : l.lag}
                            </span>
                            <div style={{
                                flex: 1, height: '28px', position: 'relative',
                                background: colors.bg, borderRadius: tokens.radius.sm,
                                boxShadow: isOptimal ? '0 0 8px rgba(26,110,191,0.35)' : 'none',
                            }}>
                                {l.correlation >= 0 ? (
                                    <div style={{
                                        position: 'absolute', left: '50%', top: 0, bottom: 0,
                                        width: `${(l.correlation / maxCorr) * 50}%`,
                                        background: isOptimal ? colors.accent : colors.green,
                                        borderRadius: `0 ${tokens.radius.sm} ${tokens.radius.sm} 0`,
                                        transition: `width ${tokens.transition.normal}`,
                                    }} />
                                ) : (
                                    <div style={{
                                        position: 'absolute', right: '50%', top: 0, bottom: 0,
                                        width: `${(-l.correlation / maxCorr) * 50}%`,
                                        background: isOptimal ? colors.accent : colors.red,
                                        borderRadius: `${tokens.radius.sm} 0 0 ${tokens.radius.sm}`,
                                        transition: `width ${tokens.transition.normal}`,
                                    }} />
                                )}
                                <div style={{
                                    position: 'absolute', left: '50%', top: 0, bottom: 0,
                                    width: '1px', background: colors.border,
                                }} />
                            </div>
                            <span style={{
                                ...s.mono, width: '52px', fontSize: tokens.fontSize.sm,
                                color: isOptimal ? colors.accent : colors.textDim,
                                fontWeight: isOptimal ? 700 : 400,
                            }}>
                                {l.correlation.toFixed(3)}
                            </span>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}

// ---------- Clusters Tab ----------
function ClustersTab() {
    const { addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const { isMobile } = useDevice();

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        try {
            setData(await api.getAssociationClusters());
        } catch (err) {
            addNotification('error', 'Failed to load clusters');
        }
        setLoading(false);
    };

    if (loading) return <div style={s.loading}>Loading cluster data...</div>;
    if (!data || !data.clusters?.length) {
        return <div style={s.empty}>{data?.message || 'No cluster data. Run clustering from Discovery page.'}</div>;
    }

    const clusterColors = ['#22C55E', '#3B82F6', '#F59E0B', '#EF4444', '#8B5CF6', '#6366F1'];

    return (
        <div>
            <div style={{ ...shared.metricGrid, marginBottom: tokens.space.lg }}>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{data.n_clusters}</div>
                    <div style={shared.metricLabel}>Clusters</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{data.n_observations || '-'}</div>
                    <div style={shared.metricLabel}>Observations</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>
                        {data.variance_explained ? `${(data.variance_explained * 100).toFixed(0)}%` : '-'}
                    </div>
                    <div style={shared.metricLabel}>Variance</div>
                </div>
            </div>

            <div style={s.sectionTitle}>CLUSTER MAP</div>
            <div style={{
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : 'repeat(auto-fill, minmax(160px, 1fr))',
                gap: tokens.space.md, marginBottom: tokens.space.lg,
            }}>
                {data.clusters.map((c, i) => {
                    const color = clusterColors[i % clusterColors.length];
                    return (
                        <div key={c.id} style={{
                            ...shared.cardGradient,
                            borderLeft: `3px solid ${color}`,
                            padding: '14px 16px',
                        }}>
                            <div style={{
                                display: 'flex', alignItems: 'center', gap: tokens.space.sm,
                                marginBottom: tokens.space.xs,
                            }}>
                                <div style={{
                                    width: '8px', height: '8px', borderRadius: '50%',
                                    background: color, flexShrink: 0,
                                }} />
                                <span style={{ fontSize: '14px', fontWeight: 700, color }}>
                                    {c.label}
                                </span>
                            </div>
                            <div style={{ fontSize: tokens.fontSize.sm, color: colors.textMuted }}>
                                Persistence: {typeof c.persistence === 'number' ? c.persistence.toFixed(1) : '-'} days
                            </div>
                        </div>
                    );
                })}
            </div>

            {data.transition_matrix?.length > 0 && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>TRANSITION PROBABILITIES</div>
                    {isMobile ? (
                        /* Mobile: card list instead of table */
                        <div style={{ display: 'flex', flexDirection: 'column', gap: tokens.space.sm }}>
                            {data.transition_matrix.map((row, i) =>
                                row.map((val, j) => {
                                    if (i === j || val < 0.05) return null;
                                    const pct = (val * 100).toFixed(0);
                                    return (
                                        <div key={`${i}-${j}`} style={{
                                            display: 'flex', justifyContent: 'space-between',
                                            alignItems: 'center', padding: '10px 12px',
                                            background: colors.bg, borderRadius: tokens.radius.sm,
                                        }}>
                                            <span style={{ fontSize: tokens.fontSize.sm, color: colors.text }}>
                                                <span style={{ color: clusterColors[i % clusterColors.length], fontWeight: 600 }}>
                                                    {data.clusters[i]?.label || `C${i}`}
                                                </span>
                                                {' → '}
                                                <span style={{ color: clusterColors[j % clusterColors.length], fontWeight: 600 }}>
                                                    {data.clusters[j]?.label || `C${j}`}
                                                </span>
                                            </span>
                                            <span style={{ ...s.mono, color: colors.text, fontWeight: 600 }}>
                                                {pct}%
                                            </span>
                                        </div>
                                    );
                                })
                            )}
                        </div>
                    ) : (
                        /* Desktop: matrix table */
                        <div style={{ overflowX: 'auto' }}>
                            <table style={{
                                width: '100%', borderCollapse: 'separate',
                                borderSpacing: '2px', fontSize: tokens.fontSize.sm,
                                fontFamily: colors.mono,
                            }}>
                                <thead>
                                    <tr>
                                        <th style={{ padding: '8px 10px', color: colors.textMuted, textAlign: 'left' }}>
                                            From \ To
                                        </th>
                                        {data.clusters.map((c, i) => (
                                            <th key={i} style={{
                                                padding: '8px 10px',
                                                color: clusterColors[i % clusterColors.length],
                                            }}>
                                                {c.label}
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {data.transition_matrix.map((row, i) => (
                                        <tr key={i}>
                                            <td style={{
                                                padding: '8px 10px',
                                                color: clusterColors[i % clusterColors.length],
                                                fontWeight: 600,
                                            }}>
                                                {data.clusters[i]?.label || `C${i}`}
                                            </td>
                                            {row.map((val, j) => {
                                                const pct = (val * 100).toFixed(0);
                                                const opacity = Math.max(0.1, val);
                                                return (
                                                    <td key={j} style={{
                                                        padding: '8px 10px', textAlign: 'center',
                                                        borderRadius: tokens.radius.sm,
                                                        background: i === j
                                                            ? `rgba(34,197,94,${opacity * 0.3})`
                                                            : `rgba(239,68,68,${opacity * 0.15})`,
                                                        color: colors.text,
                                                    }}>
                                                        {pct}%
                                                    </td>
                                                );
                                            })}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    )}
                </div>
            )}

            {data.inter_cluster_distances?.length > 0 && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>INTER-CLUSTER DISTANCES</div>
                    {data.inter_cluster_distances.map((d, i) => (
                        <div key={i} style={s.row}>
                            <span style={{ fontSize: tokens.fontSize.md, color: colors.text }}>
                                {data.clusters[d.from]?.label || `C${d.from}`} &harr;{' '}
                                {data.clusters[d.to]?.label || `C${d.to}`}
                            </span>
                            <span style={{ ...s.mono, color: colors.textDim, fontSize: tokens.fontSize.sm }}>
                                dist: {d.distance} | prob: {(d.transition_prob * 100).toFixed(0)}%
                            </span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// ---------- Regime Fingerprints Tab ----------
function RegimeFingerprintsTab() {
    const { addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        try {
            setData(await api.getRegimeFeatures());
        } catch (err) {
            addNotification('error', 'Failed to load regime features');
        }
        setLoading(false);
    };

    if (loading) return <div style={s.loading}>Loading regime fingerprints...</div>;
    if (!data?.regimes || Object.keys(data.regimes).length === 0) {
        return <div style={s.empty}>{data?.message || 'No regime data available'}</div>;
    }

    const regimeColors = {
        GROWTH: '#22C55E', NEUTRAL: '#3B82F6', FRAGILE: '#F59E0B', CRISIS: '#EF4444',
        RECOVERY: '#8B5CF6', EXPANSION: '#22C55E', CONTRACTION: '#EF4444',
    };

    const regimeNames = Object.keys(data.regimes);

    return (
        <div>
            {regimeNames.map(regime => {
                const feats = data.regimes[regime];
                const color = regimeColors[regime] || colors.accent;
                const maxZ = Math.max(...feats.map(f => Math.abs(f.avg_zscore)), 0.01);

                return (
                    <div key={regime} style={s.card}>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center', marginBottom: tokens.space.md,
                        }}>
                            <span style={{ fontSize: tokens.fontSize.lg, fontWeight: 700, color }}>
                                {regime}
                            </span>
                            <span style={s.badge(color + '33', color)}>
                                {feats[0]?.frequency || 0} days
                            </span>
                        </div>
                        {feats.slice(0, 10).map((f, i) => (
                            <div key={i} style={{ marginBottom: '6px' }}>
                                <div style={{
                                    display: 'flex', justifyContent: 'space-between',
                                    fontSize: tokens.fontSize.sm,
                                }}>
                                    <span style={{ color: colors.textDim }}>{f.feature}</span>
                                    <span style={{
                                        ...s.mono, fontSize: tokens.fontSize.sm,
                                        color: f.avg_zscore > 0 ? colors.green : colors.red,
                                    }}>
                                        {f.avg_zscore > 0 ? '+' : ''}{f.avg_zscore.toFixed(2)}
                                    </span>
                                </div>
                                <div style={s.barOuter}>
                                    <div style={s.barInner(
                                        (Math.abs(f.avg_zscore) / maxZ) * 100,
                                        f.avg_zscore > 0 ? colors.green : colors.red,
                                    )} />
                                </div>
                            </div>
                        ))}
                    </div>
                );
            })}

            {data.regimes['GROWTH'] && data.regimes['CRISIS'] && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>GROWTH vs CRISIS</div>
                    {(() => {
                        const growthMap = {};
                        data.regimes['GROWTH'].forEach(f => { growthMap[f.feature] = f.avg_zscore; });
                        const crisisMap = {};
                        data.regimes['CRISIS'].forEach(f => { crisisMap[f.feature] = f.avg_zscore; });
                        const allFeats = new Set([...Object.keys(growthMap), ...Object.keys(crisisMap)]);
                        const flippers = [];
                        allFeats.forEach(feat => {
                            const g = growthMap[feat] || 0;
                            const c = crisisMap[feat] || 0;
                            if ((g > 0 && c < 0) || (g < 0 && c > 0)) {
                                flippers.push({ feature: feat, growth: g, crisis: c });
                            }
                        });
                        flippers.sort((a, b) => Math.abs(b.growth - b.crisis) - Math.abs(a.growth - a.crisis));

                        if (flippers.length === 0) {
                            return <div style={s.empty}>No sign-flipping features detected</div>;
                        }

                        return (
                            <div>
                                <div style={{
                                    display: 'flex', justifyContent: 'flex-end', gap: tokens.space.lg,
                                    marginBottom: tokens.space.sm, fontSize: tokens.fontSize.xs,
                                }}>
                                    <span style={{ color: colors.green, width: '70px', textAlign: 'right' }}>GROWTH</span>
                                    <span style={{ color: colors.red, width: '70px', textAlign: 'right' }}>CRISIS</span>
                                </div>
                                {flippers.slice(0, 10).map((f, i) => (
                                    <div key={i} style={s.row}>
                                        <span style={{ fontSize: tokens.fontSize.md, color: colors.text, flex: 1 }}>
                                            {f.feature}
                                        </span>
                                        <span style={{
                                            ...s.mono, fontSize: tokens.fontSize.md,
                                            color: colors.green, width: '70px', textAlign: 'right',
                                        }}>
                                            {f.growth > 0 ? '+' : ''}{f.growth.toFixed(2)}
                                        </span>
                                        <span style={{
                                            ...s.mono, fontSize: tokens.fontSize.md,
                                            color: colors.red, width: '70px', textAlign: 'right',
                                        }}>
                                            {f.crisis > 0 ? '+' : ''}{f.crisis.toFixed(2)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        );
                    })()}
                </div>
            )}
        </div>
    );
}

// ---------- Anomalies Tab ----------
function AnomaliesTab() {
    const { addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        try {
            setData(await api.getAnomalies());
        } catch (err) {
            addNotification('error', 'Failed to load anomalies');
        }
        setLoading(false);
    };

    if (loading) return <div style={s.loading}>Scanning for anomalies...</div>;
    if (!data?.anomalies?.length) return <div style={s.empty}>No anomalous features detected</div>;

    return (
        <div>
            <div style={{ fontSize: tokens.fontSize.sm, color: colors.textMuted, marginBottom: tokens.space.md }}>
                Threshold: {data.threshold}σ | {data.anomalies.length} anomalies detected
            </div>
            {data.anomalies.map((a, i) => {
                const isExtreme = a.severity === 'extreme';
                const severityColor = isExtreme ? colors.red : colors.yellow;
                const severityBg = isExtreme ? colors.redBg : colors.yellowBg;
                const maxAbsZ = 5;
                const barWidth = Math.min(100, (Math.abs(a.zscore) / maxAbsZ) * 100);

                return (
                    <div key={i} style={{
                        ...s.card,
                        borderLeft: `3px solid ${severityColor}`,
                    }}>
                        <div style={{
                            display: 'flex', justifyContent: 'space-between',
                            alignItems: 'center', marginBottom: tokens.space.sm,
                        }}>
                            <span style={{ fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text }}>
                                {a.feature}
                            </span>
                            <span style={s.badge(severityBg, severityColor)}>
                                {a.severity.toUpperCase()}
                            </span>
                        </div>

                        <div style={s.barOuter}>
                            <div style={s.barInner(barWidth, severityColor)} />
                            <div style={s.barLabel}>
                                z={a.zscore.toFixed(2)}
                            </div>
                        </div>

                        <div style={{
                            display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
                            gap: tokens.space.sm, marginTop: tokens.space.md,
                        }}>
                            <div>
                                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Current</div>
                                <div style={{ ...s.mono, fontSize: tokens.fontSize.md }}>{a.current_value}</div>
                            </div>
                            <div>
                                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Mean</div>
                                <div style={{ ...s.mono, fontSize: tokens.fontSize.md }}>{a.historical_mean}</div>
                            </div>
                            <div>
                                <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted }}>Std</div>
                                <div style={{ ...s.mono, fontSize: tokens.fontSize.md }}>{a.historical_std}</div>
                            </div>
                        </div>

                        {a.broken_correlations?.length > 0 && (
                            <div style={{ marginTop: tokens.space.md }}>
                                <div style={{
                                    fontSize: tokens.fontSize.xs, color: colors.textMuted,
                                    marginBottom: tokens.space.xs, letterSpacing: '0.5px',
                                }}>
                                    BROKEN CORRELATIONS
                                </div>
                                {a.broken_correlations.map((bc, j) => (
                                    <div key={j} style={{
                                        fontSize: tokens.fontSize.sm,
                                        display: 'flex', justifyContent: 'space-between',
                                        padding: '4px 0',
                                    }}>
                                        <span style={{ color: colors.textDim }}>{bc.partner}</span>
                                        <span style={{ fontFamily: colors.mono, fontSize: tokens.fontSize.sm }}>
                                            <span style={{ color: colors.textMuted }}>hist: {bc.historical_corr}</span>
                                            {' '}
                                            <span style={{ color: colors.yellow }}>now: {bc.recent_corr}</span>
                                        </span>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                );
            })}
        </div>
    );
}

// ---------- Lag Explorer Tab ----------
function LagExplorerTab() {
    const { addNotification } = useStore();
    const [features, setFeatures] = useState([]);
    const [featureA, setFeatureA] = useState('');
    const [featureB, setFeatureB] = useState('');
    const [maxLag, setMaxLag] = useState(10);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const { isMobile } = useDevice();

    useEffect(() => {
        api.getCorrelationMatrix().then(d => {
            if (d?.features?.length) {
                setFeatures(d.features);
                setFeatureA(d.features[0]);
                setFeatureB(d.features[Math.min(1, d.features.length - 1)]);
            }
        }).catch(() => {});
    }, []);

    const runAnalysis = async () => {
        if (!featureA || !featureB || featureA === featureB) {
            addNotification('warning', 'Select two different features');
            return;
        }
        setLoading(true);
        try {
            setData(await api.getLagAnalysis(featureA, featureB, maxLag));
        } catch (err) {
            addNotification('error', `Lag analysis failed: ${err.message}`);
        }
        setLoading(false);
    };

    return (
        <div>
            <div style={s.card}>
                <div style={s.sectionTitle}>SELECT FEATURES</div>
                <div style={{
                    display: 'flex', flexDirection: isMobile ? 'column' : 'row',
                    flexWrap: 'wrap', gap: tokens.space.sm, alignItems: isMobile ? 'stretch' : 'flex-end',
                }}>
                    <div style={{ flex: isMobile ? 'auto' : 1 }}>
                        <label style={shared.label}>Feature A</label>
                        <select style={s.select} value={featureA} onChange={e => setFeatureA(e.target.value)}>
                            {features.map(f => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div style={{ flex: isMobile ? 'auto' : 1 }}>
                        <label style={shared.label}>Feature B</label>
                        <select style={s.select} value={featureB} onChange={e => setFeatureB(e.target.value)}>
                            {features.map(f => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div style={{ flex: isMobile ? 'auto' : '0 0 auto' }}>
                        <label style={shared.label}>Max Lag</label>
                        <select style={{ ...s.select, maxWidth: isMobile ? '100%' : '80px' }}
                            value={maxLag} onChange={e => setMaxLag(+e.target.value)}>
                            {[5, 10, 15, 20, 30].map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                    </div>
                    <button
                        style={{ ...shared.button, marginBottom: tokens.space.sm }}
                        onClick={runAnalysis}
                        disabled={loading}
                    >
                        {loading ? 'Analyzing...' : 'Analyze'}
                    </button>
                </div>
            </div>

            {data && !loading && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>CROSS-CORRELATION</div>
                    <LagChart data={data} />
                </div>
            )}
        </div>
    );
}

// ---------- Main Component ----------
export default function Associations() {
    const [tab, setTab] = useState('heatmap');
    const { isMobile } = useDevice();

    const renderTab = () => {
        switch (tab) {
            case 'heatmap': return <CorrelationTab />;
            case 'clusters': return <ClustersTab />;
            case 'regimes': return <RegimeFingerprintsTab />;
            case 'anomalies': return <AnomaliesTab />;
            case 'lag': return <LagExplorerTab />;
            default: return <CorrelationTab />;
        }
    };

    return (
        <div style={s.container}>
            <div style={s.title}>ASSOCIATIONS</div>
            <div style={s.tabs}>
                {TABS.map(t => (
                    <button key={t.id} onClick={() => setTab(t.id)} style={s.tab(tab === t.id)}>
                        {isMobile ? t.short : t.label}
                    </button>
                ))}
            </div>
            {renderTab()}
        </div>
    );
}
