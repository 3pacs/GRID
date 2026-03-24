import React, { useEffect, useState, useRef, useCallback } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { colors, shared } from '../styles/shared.js';

// ---------- shared local styles ----------
const s = {
    container: { ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    card: { ...shared.card },
    tabs: { ...shared.tabs },
    tab: shared.tab,
    empty: { color: colors.textMuted, textAlign: 'center', padding: '40px 0', fontSize: '13px' },
    loading: { color: colors.textMuted, textAlign: 'center', padding: '40px 0', fontSize: '13px' },
    sectionTitle: { ...shared.sectionTitle },
    select: {
        ...shared.input, maxWidth: '260px', display: 'inline-block',
        marginRight: '8px', marginBottom: '8px',
    },
    barOuter: {
        background: colors.bg, borderRadius: '4px', height: '18px', position: 'relative',
        overflow: 'hidden', marginTop: '4px',
    },
    barInner: (width, color) => ({
        position: 'absolute', left: 0, top: 0, bottom: 0,
        width: `${Math.min(100, Math.abs(width))}%`,
        background: color, borderRadius: '4px', transition: 'width 0.3s',
    }),
    barLabel: {
        position: 'absolute', right: '6px', top: '1px',
        fontSize: '10px', color: '#fff', fontFamily: colors.mono,
    },
    row: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '8px 0', borderBottom: `1px solid ${colors.border}`,
    },
    mono: { fontFamily: colors.mono, fontSize: '13px' },
    badge: (bg, fg) => ({
        display: 'inline-block', padding: '2px 8px', borderRadius: '4px',
        fontSize: '10px', fontWeight: 600, background: bg, color: fg,
        fontFamily: colors.mono,
    }),
};

const TABS = [
    { id: 'heatmap', label: 'Correlations' },
    { id: 'clusters', label: 'Clusters' },
    { id: 'regimes', label: 'Regime Fingerprints' },
    { id: 'anomalies', label: 'Anomalies' },
    { id: 'lag', label: 'Lag Explorer' },
];

// ---------- Correlation Heatmap ----------
function HeatmapCanvas({ features, matrix, onCellClick }) {
    const canvasRef = useRef(null);
    const [tooltip, setTooltip] = useState(null);
    const n = features.length;
    const cellSize = Math.max(14, Math.min(40, Math.floor(600 / (n || 1))));
    const labelSpace = 120;
    const width = labelSpace + n * cellSize;
    const height = labelSpace + n * cellSize;

    const corrColor = useCallback((v) => {
        if (v > 0) {
            const t = Math.min(v, 1);
            return `rgba(34,197,94,${t * 0.9 + 0.1})`;
        } else {
            const t = Math.min(-v, 1);
            return `rgba(239,68,68,${t * 0.9 + 0.1})`;
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

        // Clear
        ctx.fillStyle = colors.bg;
        ctx.fillRect(0, 0, width, height);

        // Draw cells
        for (let i = 0; i < n; i++) {
            for (let j = 0; j < n; j++) {
                const v = matrix[i]?.[j] ?? 0;
                const x = labelSpace + j * cellSize;
                const y = labelSpace + i * cellSize;

                ctx.fillStyle = corrColor(v);
                ctx.fillRect(x, y, cellSize - 1, cellSize - 1);

                // Strong pair border
                if (i !== j && Math.abs(v) > 0.7) {
                    ctx.strokeStyle = '#F59E0B';
                    ctx.lineWidth = 1.5;
                    ctx.strokeRect(x, y, cellSize - 1, cellSize - 1);
                }
            }
        }

        // Draw labels (top)
        ctx.save();
        ctx.font = `${Math.min(10, cellSize - 2)}px "IBM Plex Mono", monospace`;
        ctx.fillStyle = colors.textMuted;
        ctx.textAlign = 'left';
        for (let j = 0; j < n; j++) {
            ctx.save();
            ctx.translate(labelSpace + j * cellSize + cellSize / 2, labelSpace - 4);
            ctx.rotate(-Math.PI / 4);
            const label = features[j].length > 16 ? features[j].slice(0, 15) + '..' : features[j];
            ctx.fillText(label, 0, 0);
            ctx.restore();
        }
        // Left labels
        ctx.textAlign = 'right';
        ctx.textBaseline = 'middle';
        for (let i = 0; i < n; i++) {
            const label = features[i].length > 16 ? features[i].slice(0, 15) + '..' : features[i];
            ctx.fillText(label, labelSpace - 4, labelSpace + i * cellSize + cellSize / 2);
        }
        ctx.restore();
    }, [features, matrix, n, cellSize, width, height, corrColor]);

    const handleClick = (e) => {
        const rect = canvasRef.current.getBoundingClientRect();
        const x = e.clientX - rect.left - labelSpace;
        const y = e.clientY - rect.top - labelSpace;
        const j = Math.floor(x / cellSize);
        const i = Math.floor(y / cellSize);
        if (i >= 0 && i < n && j >= 0 && j < n && i !== j) {
            onCellClick?.(features[i], features[j]);
        }
    };

    const handleMove = (e) => {
        const rect = canvasRef.current.getBoundingClientRect();
        const x = e.clientX - rect.left - labelSpace;
        const y = e.clientY - rect.top - labelSpace;
        const j = Math.floor(x / cellSize);
        const i = Math.floor(y / cellSize);
        if (i >= 0 && i < n && j >= 0 && j < n) {
            setTooltip({
                x: e.clientX - rect.left,
                y: e.clientY - rect.top,
                text: `${features[i]} x ${features[j]}: ${(matrix[i]?.[j] ?? 0).toFixed(3)}`,
            });
        } else {
            setTooltip(null);
        }
    };

    return (
        <div style={{ position: 'relative', overflowX: 'auto', maxWidth: '100%' }}>
            <canvas
                ref={canvasRef}
                onClick={handleClick}
                onMouseMove={handleMove}
                onMouseLeave={() => setTooltip(null)}
                style={{ cursor: 'crosshair', display: 'block' }}
            />
            {tooltip && (
                <div style={{
                    position: 'absolute', left: tooltip.x + 12, top: tooltip.y - 28,
                    background: '#0D1520', border: `1px solid ${colors.border}`,
                    borderRadius: '6px', padding: '4px 10px', fontSize: '11px',
                    color: colors.text, fontFamily: colors.mono, pointerEvents: 'none',
                    whiteSpace: 'nowrap', zIndex: 10,
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

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        try {
            const d = await api.getCorrelationMatrix();
            setData(d);
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
                <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '8px' }}>
                    Click a cell to see lag analysis. Yellow borders = |corr| &gt; 0.7
                </div>
                <HeatmapCanvas
                    features={data.features}
                    matrix={data.matrix}
                    onCellClick={handleCellClick}
                />
            </div>

            {data.strong_pairs?.length > 0 && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>STRONG PAIRS ({data.strong_pairs.length})</div>
                    {data.strong_pairs.slice(0, 15).map((p, i) => (
                        <div key={i} style={{
                            ...s.row, cursor: 'pointer',
                        }} onClick={() => handleCellClick(p.a, p.b)}>
                            <span style={{ fontSize: '12px', color: colors.text }}>
                                {p.a} &harr; {p.b}
                            </span>
                            <span style={{
                                ...s.mono,
                                color: p.corr > 0 ? colors.green : colors.red,
                            }}>
                                {p.corr > 0 ? '+' : ''}{p.corr.toFixed(3)}
                            </span>
                        </div>
                    ))}
                </div>
            )}

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

// ---------- Lag Chart (bar chart via CSS) ----------
function LagChart({ data }) {
    if (!data?.lags?.length) return <div style={s.empty}>No lag data</div>;

    const maxCorr = Math.max(...data.lags.map(l => Math.abs(l.correlation)), 0.01);

    return (
        <div>
            <div style={{ fontSize: '12px', color: colors.accent, marginBottom: '8px', fontFamily: colors.mono }}>
                {data.direction} (strength: {data.strength})
            </div>
            <div style={{ maxHeight: '300px', overflowY: 'auto' }}>
                {data.lags.map(l => (
                    <div key={l.lag} style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '2px' }}>
                        <span style={{ ...s.mono, width: '40px', textAlign: 'right', fontSize: '10px', color: colors.textMuted }}>
                            {l.lag > 0 ? `+${l.lag}` : l.lag}
                        </span>
                        <div style={{ flex: 1, height: '14px', position: 'relative', background: colors.bg, borderRadius: '2px' }}>
                            {l.correlation >= 0 ? (
                                <div style={{
                                    position: 'absolute', left: '50%', top: 0, bottom: 0,
                                    width: `${(l.correlation / maxCorr) * 50}%`,
                                    background: l.lag === data.optimal_lag ? colors.accent : colors.green,
                                    borderRadius: '0 2px 2px 0',
                                }} />
                            ) : (
                                <div style={{
                                    position: 'absolute', right: '50%', top: 0, bottom: 0,
                                    width: `${(-l.correlation / maxCorr) * 50}%`,
                                    background: l.lag === data.optimal_lag ? colors.accent : colors.red,
                                    borderRadius: '2px 0 0 2px',
                                }} />
                            )}
                            <div style={{
                                position: 'absolute', left: '50%', top: 0, bottom: 0,
                                width: '1px', background: colors.border,
                            }} />
                        </div>
                        <span style={{ ...s.mono, width: '48px', fontSize: '10px', color: colors.textDim }}>
                            {l.correlation.toFixed(3)}
                        </span>
                    </div>
                ))}
            </div>
        </div>
    );
}

// ---------- Clusters Tab ----------
function ClustersTab() {
    const { addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);

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
            <div style={{ ...shared.metricGrid, marginBottom: '16px' }}>
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
            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: '10px', marginBottom: '16px' }}>
                {data.clusters.map((c, i) => (
                    <div key={c.id} style={{
                        ...s.card, borderLeft: `3px solid ${clusterColors[i % clusterColors.length]}`,
                        padding: '12px',
                    }}>
                        <div style={{ fontSize: '13px', fontWeight: 700, color: clusterColors[i % clusterColors.length], marginBottom: '4px' }}>
                            {c.label}
                        </div>
                        <div style={{ fontSize: '11px', color: colors.textMuted }}>
                            Persistence: {typeof c.persistence === 'number' ? c.persistence.toFixed(1) : '-'} days
                        </div>
                    </div>
                ))}
            </div>

            {data.transition_matrix?.length > 0 && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>TRANSITION PROBABILITIES</div>
                    <div style={{ overflowX: 'auto' }}>
                        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px', fontFamily: colors.mono }}>
                            <thead>
                                <tr>
                                    <th style={{ padding: '4px 8px', color: colors.textMuted, textAlign: 'left' }}>From \ To</th>
                                    {data.clusters.map((c, i) => (
                                        <th key={i} style={{ padding: '4px 8px', color: clusterColors[i % clusterColors.length] }}>
                                            {c.label}
                                        </th>
                                    ))}
                                </tr>
                            </thead>
                            <tbody>
                                {data.transition_matrix.map((row, i) => (
                                    <tr key={i}>
                                        <td style={{ padding: '4px 8px', color: clusterColors[i % clusterColors.length], fontWeight: 600 }}>
                                            {data.clusters[i]?.label || `C${i}`}
                                        </td>
                                        {row.map((val, j) => {
                                            const pct = (val * 100).toFixed(0);
                                            const opacity = Math.max(0.1, val);
                                            return (
                                                <td key={j} style={{
                                                    padding: '4px 8px', textAlign: 'center',
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
                </div>
            )}

            {data.inter_cluster_distances?.length > 0 && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>INTER-CLUSTER DISTANCES</div>
                    {data.inter_cluster_distances.map((d, i) => (
                        <div key={i} style={s.row}>
                            <span style={{ fontSize: '12px', color: colors.text }}>
                                {data.clusters[d.from]?.label || `C${d.from}`} &harr;{' '}
                                {data.clusters[d.to]?.label || `C${d.to}`}
                            </span>
                            <span style={{ ...s.mono, color: colors.textDim }}>
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
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                            <span style={{ fontSize: '14px', fontWeight: 700, color }}>
                                {regime}
                            </span>
                            <span style={s.badge(color + '33', color)}>
                                {feats[0]?.frequency || 0} days
                            </span>
                        </div>
                        {feats.slice(0, 10).map((f, i) => (
                            <div key={i} style={{ marginBottom: '4px' }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '11px' }}>
                                    <span style={{ color: colors.textDim }}>{f.feature}</span>
                                    <span style={{
                                        ...s.mono, fontSize: '10px',
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

            {/* Side-by-side comparison if GROWTH and CRISIS exist */}
            {data.regimes['GROWTH'] && data.regimes['CRISIS'] && (
                <div style={s.card}>
                    <div style={s.sectionTitle}>GROWTH vs CRISIS COMPARISON</div>
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

                        return flippers.slice(0, 10).map((f, i) => (
                            <div key={i} style={s.row}>
                                <span style={{ fontSize: '12px', color: colors.text, flex: 1 }}>
                                    {f.feature}
                                </span>
                                <span style={{ ...s.mono, fontSize: '11px', color: colors.green, width: '60px', textAlign: 'right' }}>
                                    {f.growth > 0 ? '+' : ''}{f.growth.toFixed(2)}
                                </span>
                                <span style={{ ...s.mono, fontSize: '11px', color: colors.red, width: '60px', textAlign: 'right' }}>
                                    {f.crisis > 0 ? '+' : ''}{f.crisis.toFixed(2)}
                                </span>
                            </div>
                        ));
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
            <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '12px' }}>
                Threshold: {data.threshold} sigma | {data.anomalies.length} anomalies detected
            </div>
            {data.anomalies.map((a, i) => {
                const isExtreme = a.severity === 'extreme';
                const severityColor = isExtreme ? colors.red : colors.yellow;
                const severityBg = isExtreme ? colors.redBg : colors.yellowBg;
                const maxAbsZ = 5;
                const barWidth = Math.min(100, (Math.abs(a.zscore) / maxAbsZ) * 100);

                return (
                    <div key={i} style={s.card}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                            <span style={{ fontSize: '13px', fontWeight: 600, color: colors.text }}>
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

                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '8px', marginTop: '8px' }}>
                            <div>
                                <div style={{ fontSize: '10px', color: colors.textMuted }}>Current</div>
                                <div style={{ ...s.mono, fontSize: '12px' }}>{a.current_value}</div>
                            </div>
                            <div>
                                <div style={{ fontSize: '10px', color: colors.textMuted }}>Mean</div>
                                <div style={{ ...s.mono, fontSize: '12px' }}>{a.historical_mean}</div>
                            </div>
                            <div>
                                <div style={{ fontSize: '10px', color: colors.textMuted }}>Std</div>
                                <div style={{ ...s.mono, fontSize: '12px' }}>{a.historical_std}</div>
                            </div>
                        </div>

                        {a.broken_correlations?.length > 0 && (
                            <div style={{ marginTop: '8px' }}>
                                <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px' }}>
                                    BROKEN CORRELATIONS
                                </div>
                                {a.broken_correlations.map((bc, j) => (
                                    <div key={j} style={{ fontSize: '11px', display: 'flex', justifyContent: 'space-between', padding: '2px 0' }}>
                                        <span style={{ color: colors.textDim }}>{bc.partner}</span>
                                        <span style={{ fontFamily: colors.mono, fontSize: '10px' }}>
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

    useEffect(() => {
        // Load feature list from correlation matrix endpoint
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
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', alignItems: 'flex-end' }}>
                    <div>
                        <label style={shared.label}>Feature A</label>
                        <select style={s.select} value={featureA} onChange={e => setFeatureA(e.target.value)}>
                            {features.map(f => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div>
                        <label style={shared.label}>Feature B</label>
                        <select style={s.select} value={featureB} onChange={e => setFeatureB(e.target.value)}>
                            {features.map(f => <option key={f} value={f}>{f}</option>)}
                        </select>
                    </div>
                    <div>
                        <label style={shared.label}>Max Lag</label>
                        <select style={{ ...s.select, maxWidth: '80px' }} value={maxLag} onChange={e => setMaxLag(+e.target.value)}>
                            {[5, 10, 15, 20, 30].map(v => <option key={v} value={v}>{v}</option>)}
                        </select>
                    </div>
                    <button
                        style={{ ...shared.button, marginBottom: '8px' }}
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
                        {t.label}
                    </button>
                ))}
            </div>
            {renderTab()}
        </div>
    );
}
