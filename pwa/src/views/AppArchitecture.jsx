/**
 * AppArchitecture.jsx — Meta-view of the GRID system itself.
 *
 * D3-powered visualization showing all modules, data flows,
 * connections, and health status. Blueprint aesthetic.
 */

import React, { useEffect, useRef, useState, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

/* ═══════════════════════════════════════════
   Blueprint design tokens
   ═══════════════════════════════════════════ */

const BP = {
    bg: '#0A1628',
    gridLine: 'rgba(26, 80, 140, 0.12)',
    gridLineMajor: 'rgba(26, 80, 140, 0.22)',
    layerBg: 'rgba(16, 32, 56, 0.6)',
    layerBorder: '#1A3A60',
    layerLabel: '#3A7AC0',
    nodeHealthy: '#22C55E',
    nodeStale: '#F59E0B',
    nodeBroken: '#EF4444',
    nodeNew: '#5A7080',
    nodeUnknown: '#3A5060',
    flowGreen: '#22C55E',
    flowBlue: '#3B82F6',
    flowPurple: '#8B5CF6',
    flowGold: '#F59E0B',
    text: '#C8D8E8',
    textDim: '#6A8AA8',
    textBright: '#E8F0F8',
    accent: '#1A6EBF',
    mono: "'JetBrains Mono', 'IBM Plex Mono', monospace",
    sans: "'IBM Plex Sans', sans-serif",
};

const STATUS_COLORS = {
    healthy: BP.nodeHealthy,
    stale: BP.nodeStale,
    broken: BP.nodeBroken,
    new: BP.nodeNew,
    unknown: BP.nodeUnknown,
};

/* ═══════════════════════════════════════════
   Stats Bar
   ═══════════════════════════════════════════ */

function StatsBar({ stats }) {
    if (!stats) return null;

    const tiles = [
        { label: 'MODULES', value: stats.total_modules, color: BP.accent },
        { label: 'PULLERS', value: stats.total_pullers, color: BP.flowGreen },
        { label: 'FEATURES', value: stats.total_features?.toLocaleString(), color: BP.flowBlue },
        { label: 'RAW ROWS', value: stats.total_raw ? (stats.total_raw / 1e6).toFixed(1) + 'M' : '0', color: BP.flowPurple },
        { label: 'RESOLVED', value: stats.total_resolved ? (stats.total_resolved / 1e3).toFixed(0) + 'K' : '0', color: BP.flowGold },
        { label: 'ENDPOINTS', value: stats.api_endpoints, color: BP.accent },
        { label: 'VIEWS', value: stats.frontend_views, color: BP.flowBlue },
        { label: 'TESTS', value: stats.tests, color: BP.flowGreen },
    ];

    return (
        <div style={s.statsBar}>
            {tiles.map(t => (
                <div key={t.label} style={s.statTile}>
                    <div style={{ ...s.statValue, color: t.color }}>{t.value}</div>
                    <div style={s.statLabel}>{t.label}</div>
                </div>
            ))}
        </div>
    );
}

/* ═══════════════════════════════════════════
   Gap Badge
   ═══════════════════════════════════════════ */

function GapBadge({ gaps }) {
    if (!gaps || gaps.length === 0) return null;
    return (
        <div style={s.gapBadge}>
            <span style={s.gapDot} />
            {gaps.length} gap{gaps.length > 1 ? 's' : ''} detected
        </div>
    );
}

/* ═══════════════════════════════════════════
   Detail Panel (right sidebar on click)
   ═══════════════════════════════════════════ */

function DetailPanel({ node, flows, onClose }) {
    if (!node) return null;

    const statusColor = STATUS_COLORS[node.status] || BP.nodeUnknown;
    const incoming = flows?.filter(f => f.to === node.id) || [];
    const outgoing = flows?.filter(f => f.from === node.id) || [];

    return (
        <div style={s.detailPanel}>
            <div style={s.detailHeader}>
                <div style={s.detailTitle}>{node.label || node.id}</div>
                <button style={s.detailClose} onClick={onClose}>x</button>
            </div>
            <div style={s.detailRow}>
                <span style={s.detailLabel}>Type</span>
                <span style={s.detailValue}>{node.type}</span>
            </div>
            <div style={s.detailRow}>
                <span style={s.detailLabel}>Status</span>
                <span style={{ ...s.detailValue, color: statusColor }}>{node.status || 'unknown'}</span>
            </div>
            {node.last_run && (
                <div style={s.detailRow}>
                    <span style={s.detailLabel}>Last Active</span>
                    <span style={s.detailValue}>{node.last_run}</span>
                </div>
            )}
            {node.rows != null && (
                <div style={s.detailRow}>
                    <span style={s.detailLabel}>Rows</span>
                    <span style={s.detailValue}>{Number(node.rows).toLocaleString()}</span>
                </div>
            )}
            {node.source_type && (
                <div style={s.detailRow}>
                    <span style={s.detailLabel}>Data Type</span>
                    <span style={s.detailValue}>{node.source_type}</span>
                </div>
            )}

            {incoming.length > 0 && (
                <div style={{ marginTop: '12px' }}>
                    <div style={s.detailSectionTitle}>FEEDS FROM</div>
                    {incoming.map((f, i) => (
                        <div key={i} style={s.detailFlowItem}>
                            <span style={{ color: f.color || BP.textDim }}>{f.from}</span>
                            <span style={{ color: BP.textDim, fontSize: '10px', margin: '0 6px' }}>{f.label}</span>
                        </div>
                    ))}
                </div>
            )}
            {outgoing.length > 0 && (
                <div style={{ marginTop: '12px' }}>
                    <div style={s.detailSectionTitle}>FEEDS INTO</div>
                    {outgoing.map((f, i) => (
                        <div key={i} style={s.detailFlowItem}>
                            <span style={{ color: f.color || BP.textDim }}>{f.to}</span>
                            <span style={{ color: BP.textDim, fontSize: '10px', margin: '0 6px' }}>{f.label}</span>
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════
   D3 System Diagram (canvas-based)
   ═══════════════════════════════════════════ */

function SystemDiagram({ data, onNodeClick, selectedNode }) {
    const canvasRef = useRef(null);
    const animRef = useRef(null);
    const particlesRef = useRef([]);
    const layoutRef = useRef(null);
    const hoverRef = useRef(null);

    // Build layout from data
    const buildLayout = useCallback((canvas, modules, flows) => {
        if (!canvas || !modules) return null;

        const W = canvas.width;
        const H = canvas.height;
        const layerCount = modules.length;
        const layerH = Math.min(100, (H - 60) / layerCount);
        const layerGap = (H - layerCount * layerH - 40) / (layerCount + 1);
        const PADDING = 40;

        const nodeMap = {};
        const layers = modules.map((mod, li) => {
            const y = 20 + (li + 1) * layerGap + li * layerH;
            const children = mod.children || [];
            const cols = Math.min(children.length, Math.floor((W - PADDING * 2) / 90));
            const nodeW = Math.min(80, (W - PADDING * 2) / Math.max(children.length, 1) - 8);

            const nodes = children.map((child, ci) => {
                const row = Math.floor(ci / cols);
                const col = ci % cols;
                const totalInRow = Math.min(cols, children.length - row * cols);
                const rowW = totalInRow * (nodeW + 8);
                const startX = (W - rowW) / 2;
                const x = startX + col * (nodeW + 8) + nodeW / 2;
                const ny = y + 24 + row * 28;

                const nodeInfo = {
                    ...child,
                    x,
                    y: ny,
                    w: nodeW,
                    h: 20,
                    layerId: mod.id,
                };
                nodeMap[child.id] = nodeInfo;
                return nodeInfo;
            });

            return {
                id: mod.id,
                label: mod.label,
                x: PADDING,
                y,
                w: W - PADDING * 2,
                h: layerH,
                nodes,
            };
        });

        // Build flow lines connecting nodes
        const flowLines = (flows || []).map(f => {
            const from = nodeMap[f.from];
            const to = nodeMap[f.to];
            if (!from || !to) return null;
            return {
                x1: from.x,
                y1: from.y + from.h / 2,
                x2: to.x,
                y2: to.y - to.h / 2,
                color: f.color || '#3B82F6',
                label: f.label,
            };
        }).filter(Boolean);

        return { layers, nodeMap, flowLines };
    }, []);

    // Initialize particles along flow lines
    const initParticles = useCallback((flowLines) => {
        const particles = [];
        if (!flowLines) return particles;
        flowLines.forEach((line, fi) => {
            const count = 2;
            for (let i = 0; i < count; i++) {
                particles.push({
                    flowIdx: fi,
                    t: (i / count) + Math.random() * 0.1,
                    speed: 0.002 + Math.random() * 0.002,
                    color: line.color,
                    size: 2 + Math.random() * 1.5,
                });
            }
        });
        return particles;
    }, []);

    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas || !data?.modules) return;

        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        const ctx = canvas.getContext('2d');
        ctx.scale(dpr, dpr);

        const W = rect.width;
        const H = rect.height;

        const layout = buildLayout(
            { width: W, height: H },
            data.modules,
            data.data_flows
        );
        layoutRef.current = layout;

        if (!layout) return;

        particlesRef.current = initParticles(layout.flowLines);

        const drawGrid = () => {
            ctx.strokeStyle = BP.gridLine;
            ctx.lineWidth = 0.5;
            for (let x = 0; x < W; x += 30) {
                ctx.beginPath();
                ctx.moveTo(x, 0);
                ctx.lineTo(x, H);
                ctx.stroke();
            }
            for (let y = 0; y < H; y += 30) {
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(W, y);
                ctx.stroke();
            }
            // Major grid
            ctx.strokeStyle = BP.gridLineMajor;
            for (let x = 0; x < W; x += 150) {
                ctx.beginPath();
                ctx.moveTo(x, 0);
                ctx.lineTo(x, H);
                ctx.stroke();
            }
            for (let y = 0; y < H; y += 150) {
                ctx.beginPath();
                ctx.moveTo(0, y);
                ctx.lineTo(W, y);
                ctx.stroke();
            }
        };

        const drawLayers = () => {
            layout.layers.forEach(layer => {
                // Layer background
                ctx.fillStyle = BP.layerBg;
                ctx.strokeStyle = BP.layerBorder;
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.roundRect(layer.x, layer.y, layer.w, layer.h, 6);
                ctx.fill();
                ctx.stroke();

                // Layer label
                ctx.font = `bold 9px ${BP.mono}`;
                ctx.fillStyle = BP.layerLabel;
                ctx.textAlign = 'left';
                ctx.fillText(layer.label.toUpperCase(), layer.x + 8, layer.y + 14);
            });
        };

        const drawNodes = () => {
            layout.layers.forEach(layer => {
                layer.nodes.forEach(node => {
                    const isSelected = selectedNode?.id === node.id;
                    const isGap = node.status === 'new' || node.status === 'broken';
                    const statusColor = STATUS_COLORS[node.status] || BP.nodeUnknown;
                    const isHovered = hoverRef.current?.id === node.id;

                    // Node rect
                    ctx.fillStyle = isSelected
                        ? 'rgba(26, 110, 191, 0.25)'
                        : isHovered
                            ? 'rgba(26, 110, 191, 0.15)'
                            : 'rgba(13, 21, 32, 0.9)';
                    ctx.strokeStyle = isSelected ? BP.accent : statusColor;
                    ctx.lineWidth = isSelected ? 2 : 1;

                    // Pulsing broken/new nodes
                    if (isGap) {
                        const pulse = Math.sin(Date.now() / 500) * 0.3 + 0.7;
                        ctx.globalAlpha = pulse;
                        ctx.strokeStyle = BP.nodeBroken;
                        ctx.setLineDash([3, 3]);
                    }

                    ctx.beginPath();
                    ctx.roundRect(node.x - node.w / 2, node.y - node.h / 2, node.w, node.h, 4);
                    ctx.fill();
                    ctx.stroke();

                    if (isGap) {
                        ctx.globalAlpha = 1;
                        ctx.setLineDash([]);
                    }

                    // Status dot
                    ctx.fillStyle = statusColor;
                    ctx.beginPath();
                    ctx.arc(node.x - node.w / 2 + 6, node.y, 3, 0, Math.PI * 2);
                    ctx.fill();

                    // Node label
                    ctx.font = `10px ${BP.mono}`;
                    ctx.fillStyle = BP.text;
                    ctx.textAlign = 'left';
                    const label = (node.label || node.id).substring(0, Math.floor(node.w / 6));
                    ctx.fillText(label, node.x - node.w / 2 + 14, node.y + 3);
                });
            });
        };

        const drawFlows = () => {
            layout.flowLines.forEach(line => {
                ctx.strokeStyle = line.color + '30';
                ctx.lineWidth = 1;
                ctx.beginPath();
                ctx.moveTo(line.x1, line.y1);
                // Bezier curve
                const midY = (line.y1 + line.y2) / 2;
                ctx.bezierCurveTo(line.x1, midY, line.x2, midY, line.x2, line.y2);
                ctx.stroke();
            });
        };

        const drawParticles = () => {
            const particles = particlesRef.current;
            particles.forEach(p => {
                p.t += p.speed;
                if (p.t > 1) p.t -= 1;

                const line = layout.flowLines[p.flowIdx];
                if (!line) return;

                // Position along bezier
                const t = p.t;
                const midY = (line.y1 + line.y2) / 2;
                const it = 1 - t;
                const x = it * it * it * line.x1 + 3 * it * it * t * line.x1 + 3 * it * t * t * line.x2 + t * t * t * line.x2;
                const y = it * it * it * line.y1 + 3 * it * it * t * midY + 3 * it * t * t * midY + t * t * t * line.y2;

                ctx.fillStyle = p.color;
                ctx.globalAlpha = 0.8;
                ctx.beginPath();
                ctx.arc(x, y, p.size, 0, Math.PI * 2);
                ctx.fill();
                ctx.globalAlpha = 1;
            });
        };

        const animate = () => {
            ctx.clearRect(0, 0, W, H);
            drawGrid();
            drawFlows();
            drawLayers();
            drawNodes();
            drawParticles();
            animRef.current = requestAnimationFrame(animate);
        };

        animate();

        return () => {
            if (animRef.current) cancelAnimationFrame(animRef.current);
        };
    }, [data, selectedNode, buildLayout, initParticles]);

    // Handle clicks
    const handleClick = useCallback((e) => {
        const layout = layoutRef.current;
        if (!layout) return;
        const rect = canvasRef.current.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;

        for (const layer of layout.layers) {
            for (const node of layer.nodes) {
                if (
                    mx >= node.x - node.w / 2 &&
                    mx <= node.x + node.w / 2 &&
                    my >= node.y - node.h / 2 &&
                    my <= node.y + node.h / 2
                ) {
                    onNodeClick?.(node);
                    return;
                }
            }
        }
        onNodeClick?.(null);
    }, [onNodeClick]);

    // Handle hover
    const handleMouseMove = useCallback((e) => {
        const layout = layoutRef.current;
        if (!layout) return;
        const rect = canvasRef.current.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;
        let found = null;

        for (const layer of layout.layers) {
            for (const node of layer.nodes) {
                if (
                    mx >= node.x - node.w / 2 &&
                    mx <= node.x + node.w / 2 &&
                    my >= node.y - node.h / 2 &&
                    my <= node.y + node.h / 2
                ) {
                    found = node;
                    break;
                }
            }
            if (found) break;
        }

        hoverRef.current = found;
        canvasRef.current.style.cursor = found ? 'pointer' : 'default';
    }, []);

    return (
        <canvas
            ref={canvasRef}
            onClick={handleClick}
            onMouseMove={handleMouseMove}
            style={s.canvas}
        />
    );
}

/* ═══════════════════════════════════════════
   Main Component
   ═══════════════════════════════════════════ */

export default function AppArchitecture() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedNode, setSelectedNode] = useState(null);

    useEffect(() => {
        let cancelled = false;
        async function load() {
            setLoading(true);
            const res = await api.getArchitecture();
            if (cancelled) return;
            if (res?.error) {
                setError(res.message || 'Failed to load architecture');
            } else {
                setData(res);
            }
            setLoading(false);
        }
        load();
        return () => { cancelled = true; };
    }, []);

    if (loading) {
        return (
            <div style={s.page}>
                <div style={s.loading}>
                    <div style={s.loadingPulse} />
                    INTROSPECTING SYSTEM...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={s.page}>
                <div style={s.errorBox}>{error}</div>
            </div>
        );
    }

    return (
        <div style={s.page}>
            {/* Header */}
            <div style={s.header}>
                <div>
                    <div style={s.title}>SYSTEM ARCHITECTURE</div>
                    <div style={s.subtitle}>Live introspection of the GRID intelligence platform</div>
                </div>
                <GapBadge gaps={data?.gaps} />
            </div>

            {/* Stats Bar */}
            <StatsBar stats={data?.stats} />

            {/* Main content: diagram + detail panel */}
            <div style={s.mainArea}>
                <div style={s.diagramContainer}>
                    <SystemDiagram
                        data={data}
                        onNodeClick={setSelectedNode}
                        selectedNode={selectedNode}
                    />
                </div>
                <DetailPanel
                    node={selectedNode}
                    flows={data?.data_flows}
                    onClose={() => setSelectedNode(null)}
                />
            </div>

            {/* Gap list */}
            {data?.gaps?.length > 0 && (
                <div style={s.gapSection}>
                    <div style={s.gapSectionTitle}>DETECTED GAPS</div>
                    <div style={s.gapList}>
                        {data.gaps.map((g, i) => (
                            <div key={i} style={s.gapItem}>
                                <span style={{ ...s.gapItemDot, background: STATUS_COLORS[g.status] || BP.nodeNew }} />
                                <span style={s.gapItemLabel}>{g.label}</span>
                                <span style={s.gapItemLayer}>{g.layer}</span>
                                <span style={{ ...s.gapItemStatus, color: STATUS_COLORS[g.status] || BP.nodeNew }}>
                                    {g.status}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════
   Styles
   ═══════════════════════════════════════════ */

const s = {
    page: {
        background: BP.bg,
        minHeight: '100vh',
        padding: '16px',
        fontFamily: BP.sans,
        color: BP.text,
    },
    loading: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '12px',
        height: '60vh',
        fontFamily: BP.mono,
        fontSize: '13px',
        color: BP.textDim,
        letterSpacing: '2px',
    },
    loadingPulse: {
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        background: BP.accent,
        animation: 'pulse 1.5s ease-in-out infinite',
    },
    errorBox: {
        background: 'rgba(239, 68, 68, 0.1)',
        border: '1px solid #EF4444',
        borderRadius: '8px',
        padding: '16px',
        fontFamily: BP.mono,
        fontSize: '13px',
        color: '#EF4444',
    },
    header: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        marginBottom: '16px',
    },
    title: {
        fontFamily: BP.mono,
        fontSize: '18px',
        fontWeight: 700,
        color: BP.textBright,
        letterSpacing: '3px',
    },
    subtitle: {
        fontFamily: BP.mono,
        fontSize: '11px',
        color: BP.textDim,
        marginTop: '4px',
        letterSpacing: '1px',
    },

    /* Stats bar */
    statsBar: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(90px, 1fr))',
        gap: '8px',
        marginBottom: '16px',
    },
    statTile: {
        background: 'rgba(16, 32, 56, 0.6)',
        border: `1px solid ${BP.layerBorder}`,
        borderRadius: '8px',
        padding: '10px 8px',
        textAlign: 'center',
    },
    statValue: {
        fontSize: '20px',
        fontWeight: 700,
        fontFamily: BP.mono,
    },
    statLabel: {
        fontSize: '9px',
        fontWeight: 600,
        letterSpacing: '1.5px',
        color: BP.textDim,
        marginTop: '4px',
        fontFamily: BP.mono,
    },

    /* Gap badge */
    gapBadge: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '6px',
        background: 'rgba(239, 68, 68, 0.12)',
        border: '1px solid rgba(239, 68, 68, 0.3)',
        borderRadius: '20px',
        padding: '6px 14px',
        fontFamily: BP.mono,
        fontSize: '11px',
        fontWeight: 600,
        color: '#EF4444',
    },
    gapDot: {
        width: '6px',
        height: '6px',
        borderRadius: '50%',
        background: '#EF4444',
        animation: 'pulse 1.5s ease-in-out infinite',
    },

    /* Main area */
    mainArea: {
        display: 'flex',
        gap: '12px',
        minHeight: '500px',
    },
    diagramContainer: {
        flex: 1,
        background: BP.bg,
        border: `1px solid ${BP.layerBorder}`,
        borderRadius: '8px',
        overflow: 'hidden',
        position: 'relative',
    },
    canvas: {
        width: '100%',
        height: '100%',
        minHeight: '500px',
        display: 'block',
    },

    /* Detail panel */
    detailPanel: {
        width: '280px',
        flexShrink: 0,
        background: 'rgba(16, 32, 56, 0.6)',
        border: `1px solid ${BP.layerBorder}`,
        borderRadius: '8px',
        padding: '16px',
        overflowY: 'auto',
        maxHeight: '600px',
    },
    detailHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        marginBottom: '12px',
    },
    detailTitle: {
        fontFamily: BP.mono,
        fontSize: '14px',
        fontWeight: 700,
        color: BP.textBright,
        wordBreak: 'break-word',
    },
    detailClose: {
        background: 'none',
        border: 'none',
        color: BP.textDim,
        fontSize: '16px',
        cursor: 'pointer',
        fontFamily: BP.mono,
        padding: '2px 6px',
    },
    detailRow: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '6px 0',
        borderBottom: `1px solid ${BP.layerBorder}`,
    },
    detailLabel: {
        fontFamily: BP.mono,
        fontSize: '10px',
        fontWeight: 600,
        color: BP.textDim,
        letterSpacing: '1px',
    },
    detailValue: {
        fontFamily: BP.mono,
        fontSize: '12px',
        color: BP.text,
    },
    detailSectionTitle: {
        fontFamily: BP.mono,
        fontSize: '9px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: BP.accent,
        marginBottom: '6px',
    },
    detailFlowItem: {
        display: 'flex',
        alignItems: 'center',
        fontFamily: BP.mono,
        fontSize: '11px',
        padding: '3px 0',
    },

    /* Gap section */
    gapSection: {
        marginTop: '16px',
        background: 'rgba(239, 68, 68, 0.05)',
        border: '1px solid rgba(239, 68, 68, 0.15)',
        borderRadius: '8px',
        padding: '14px',
    },
    gapSectionTitle: {
        fontFamily: BP.mono,
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '2px',
        color: '#EF4444',
        marginBottom: '10px',
    },
    gapList: {
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
    },
    gapItem: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        fontFamily: BP.mono,
        fontSize: '11px',
    },
    gapItemDot: {
        width: '6px',
        height: '6px',
        borderRadius: '50%',
        flexShrink: 0,
    },
    gapItemLabel: {
        color: BP.text,
        flex: 1,
    },
    gapItemLayer: {
        color: BP.textDim,
        fontSize: '10px',
    },
    gapItemStatus: {
        fontWeight: 600,
        fontSize: '10px',
        letterSpacing: '0.5px',
    },
};
