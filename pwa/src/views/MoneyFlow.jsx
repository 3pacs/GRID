/**
 * MoneyFlow — Global Money Flow Visualization.
 *
 * Full-page, immersive view showing how money flows through the global
 * financial system: Central Banks -> Banking -> Markets -> Sectors.
 *
 * Features:
 *   - Vertical flow diagram with D3 (layers, curved links, animated particles)
 *   - Signal overlays with trust scores (insider, congressional, dark pool, whale, convergence)
 *   - LLM-generated narrative panel
 *   - Interactive drill-down on nodes and links
 *   - "Levers" sidebar showing top market-moving forces
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

// ── Layer colors (vertical gradient feel) ─────────────────────────────
const LAYER_COLORS = {
    central_banks: '#EF4444',
    banking:       '#F59E0B',
    markets:       '#3B82F6',
    sectors:       '#8B5CF6',
};

const LAYER_GLOW = {
    central_banks: 'rgba(239, 68, 68, 0.25)',
    banking:       'rgba(245, 158, 11, 0.25)',
    markets:       'rgba(59, 130, 246, 0.25)',
    sectors:       'rgba(139, 92, 246, 0.25)',
};

const FLOW_COLORS = {
    inflow: '#22C55E',
    outflow: '#EF4444',
};

const SIGNAL_ICONS = {
    congressional_signal: '\u{1F3DB}\uFE0F',   // congressional
    insider_signal:       '\u{1F464}',           // insider
    dark_pool:            '\u{1F30A}',           // dark pool
    dark_pool_signal:     '\u{1F30A}',
    whale_flow:           '\u{1F40B}',           // whale
    convergence:          '\u26A1',              // convergence
    gex_regime:           '\u{1F4CA}',           // GEX
};

const SIGNAL_LABELS = {
    congressional_signal: 'Congressional',
    insider_signal:       'Insider',
    dark_pool:            'Dark Pool',
    dark_pool_signal:     'Dark Pool',
    whale_flow:           'Whale Flow',
    gex_regime:           'GEX Regime',
};

// ── Formatting helpers ────────────────────────────────────────────────
function fmt(val, opts = {}) {
    if (val == null) return '--';
    const { type = 'number', compact = false } = opts;
    if (type === 'pct') return `${val >= 0 ? '+' : ''}${(val * 100).toFixed(1)}%`;
    if (type === 'money') {
        const abs = Math.abs(val);
        if (abs >= 1e12) return `$${(val / 1e12).toFixed(1)}T`;
        if (abs >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
        if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
        return `$${val.toLocaleString()}`;
    }
    if (compact) {
        const abs = Math.abs(val);
        if (abs >= 1e12) return `${(val / 1e12).toFixed(1)}T`;
        if (abs >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
        if (abs >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
        if (abs >= 1e3) return `${(val / 1e3).toFixed(1)}K`;
    }
    return typeof val === 'number' ? val.toLocaleString() : String(val);
}

function signalColor(signal) {
    if (!signal) return colors.textMuted;
    const s = String(signal).toLowerCase();
    if (s.includes('buy') || s.includes('accum') || s.includes('call') || s.includes('inject') || s.includes('expand'))
        return '#22C55E';
    if (s.includes('sell') || s.includes('drain') || s.includes('put') || s.includes('contract'))
        return '#EF4444';
    if (s.includes('above') || s.includes('slightly_bullish'))
        return '#4ADE80';
    if (s.includes('slightly_bearish'))
        return '#F97316';
    return colors.textMuted;
}


// ── Main Component ────────────────────────────────────────────────────
export default function MoneyFlow({ onNavigate } = {}) {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [hoveredNode, setHoveredNode] = useState(null);
    const [selectedNode, setSelectedNode] = useState(null);
    const [hoveredFlow, setHoveredFlow] = useState(null);
    const [dimensions, setDimensions] = useState({ width: 900, height: 700 });

    // ── Load data ─────────────────────────────────────────────────────
    const loadData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const d = await api.getMoneyMap();
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load money flow data');
        }
        setLoading(false);
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    useEffect(() => {
        if (containerRef.current) {
            const w = containerRef.current.clientWidth;
            setDimensions({
                width: Math.max(360, Math.min(1200, w - 32)),
                height: Math.max(500, Math.min(900, window.innerHeight - 240)),
            });
        }
        const onResize = () => {
            if (containerRef.current) {
                const w = containerRef.current.clientWidth;
                setDimensions({
                    width: Math.max(360, Math.min(1200, w - 32)),
                    height: Math.max(500, Math.min(900, window.innerHeight - 240)),
                });
            }
        };
        window.addEventListener('resize', onResize);
        return () => window.removeEventListener('resize', onResize);
    }, []);

    // ── D3 Rendering ──────────────────────────────────────────────────
    useEffect(() => {
        if (!data || !svgRef.current) return;

        const { layers = [], flows = [] } = data;
        if (!layers.length) return;

        const { width, height } = dimensions;
        const margin = { top: 24, right: 16, bottom: 24, left: 16 };
        const innerW = width - margin.left - margin.right;
        const innerH = height - margin.top - margin.bottom;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        // Defs for gradients and glow
        const defs = svg.append('defs');

        // Glow filter
        const glow = defs.append('filter').attr('id', 'glow');
        glow.append('feGaussianBlur').attr('stdDeviation', '4').attr('result', 'blur');
        glow.append('feMerge')
            .selectAll('feMergeNode')
            .data(['blur', 'SourceGraphic'])
            .join('feMergeNode')
            .attr('in', d => d);

        // Link gradient (inflow)
        const gradIn = defs.append('linearGradient')
            .attr('id', 'flow-inflow').attr('x1', '0%').attr('y1', '0%').attr('x2', '0%').attr('y2', '100%');
        gradIn.append('stop').attr('offset', '0%').attr('stop-color', '#22C55E').attr('stop-opacity', 0.6);
        gradIn.append('stop').attr('offset', '100%').attr('stop-color', '#22C55E').attr('stop-opacity', 0.2);

        // Link gradient (outflow)
        const gradOut = defs.append('linearGradient')
            .attr('id', 'flow-outflow').attr('x1', '0%').attr('y1', '0%').attr('x2', '0%').attr('y2', '100%');
        gradOut.append('stop').attr('offset', '0%').attr('stop-color', '#EF4444').attr('stop-opacity', 0.6);
        gradOut.append('stop').attr('offset', '100%').attr('stop-color', '#EF4444').attr('stop-opacity', 0.2);

        const g = svg.append('g')
            .attr('transform', `translate(${margin.left},${margin.top})`);

        // Layout: position each layer vertically, nodes horizontally within layer
        const layerCount = layers.length;
        const layerHeight = 60;
        const layerGap = (innerH - layerCount * layerHeight) / Math.max(1, layerCount - 1);
        const nodeHeight = 50;
        const nodeMinWidth = 100;
        const nodeMaxWidth = 200;
        const nodePadding = 12;

        // Build positioned node map
        const allNodes = [];
        const nodeById = {};

        layers.forEach((layer, li) => {
            const y = li * (layerHeight + layerGap);
            const nodes = layer.nodes || [];
            const nodeCount = Math.max(1, nodes.length);
            const availW = innerW - nodePadding * 2;
            const nodeW = Math.max(nodeMinWidth, Math.min(nodeMaxWidth, (availW - (nodeCount - 1) * nodePadding) / nodeCount));
            const totalNodesW = nodeCount * nodeW + (nodeCount - 1) * nodePadding;
            const startX = (innerW - totalNodesW) / 2;

            nodes.forEach((node, ni) => {
                const x = startX + ni * (nodeW + nodePadding);
                const positioned = {
                    ...node,
                    layerId: layer.id,
                    layerLabel: layer.label,
                    x, y, w: nodeW, h: nodeHeight,
                    cx: x + nodeW / 2,
                    cy: y + nodeHeight / 2,
                };
                allNodes.push(positioned);
                nodeById[node.id] = positioned;
            });
        });

        // Draw layer labels
        layers.forEach((layer, li) => {
            const y = li * (layerHeight + layerGap);
            g.append('text')
                .attr('x', 0)
                .attr('y', y - 6)
                .attr('font-size', '9px')
                .attr('font-weight', 700)
                .attr('letter-spacing', '1.5px')
                .attr('fill', LAYER_COLORS[layer.id] || colors.textMuted)
                .attr('font-family', "'JetBrains Mono', monospace")
                .text(layer.label.toUpperCase());
        });

        // Draw links (curved vertical paths)
        const linkGroup = g.append('g').attr('class', 'links');
        const maxFlowVol = Math.max(1, ...flows.map(f => Math.abs(f.volume || 1)));

        flows.forEach(flow => {
            const src = nodeById[flow.from];
            const tgt = nodeById[flow.to];
            if (!src || !tgt) return;

            const thickness = Math.max(2, Math.min(14, (Math.abs(flow.volume || 1) / maxFlowVol) * 14));
            const isInflow = flow.direction === 'inflow';
            const flowColor = isInflow ? FLOW_COLORS.inflow : FLOW_COLORS.outflow;

            // Cubic bezier from bottom of source to top of target
            const x1 = src.cx;
            const y1 = src.y + src.h;
            const x2 = tgt.cx;
            const y2 = tgt.y;
            const midY = (y1 + y2) / 2;

            const path = `M ${x1} ${y1} C ${x1} ${midY}, ${x2} ${midY}, ${x2} ${y2}`;

            const linkPath = linkGroup.append('path')
                .attr('d', path)
                .attr('fill', 'none')
                .attr('stroke', flowColor)
                .attr('stroke-width', thickness)
                .attr('stroke-opacity', 0.3)
                .attr('stroke-linecap', 'round')
                .style('cursor', 'pointer');

            linkPath
                .on('mouseenter', function () {
                    d3.select(this).attr('stroke-opacity', 0.7).attr('filter', 'url(#glow)');
                    setHoveredFlow(flow);
                })
                .on('mouseleave', function () {
                    d3.select(this).attr('stroke-opacity', 0.3).attr('filter', null);
                    setHoveredFlow(null);
                });

            // Animated particles along the path
            const pathEl = linkPath.node();
            const totalLen = pathEl.getTotalLength();
            if (totalLen > 0) {
                const particleCount = Math.max(1, Math.min(4, Math.round(thickness / 4)));
                for (let pi = 0; pi < particleCount; pi++) {
                    const particle = linkGroup.append('circle')
                        .attr('r', Math.max(1.5, thickness / 4))
                        .attr('fill', flowColor)
                        .attr('opacity', 0.7);

                    const duration = 2500 + pi * 600;
                    const delay = pi * (duration / particleCount);

                    function animateParticle() {
                        const startPt = pathEl.getPointAtLength(0);
                        particle
                            .attr('cx', startPt.x)
                            .attr('cy', startPt.y)
                            .attr('opacity', 0)
                            .transition()
                            .delay(delay)
                            .duration(duration)
                            .ease(d3.easeLinear)
                            .attrTween('cx', () => (t) => pathEl.getPointAtLength(t * totalLen).x)
                            .attrTween('cy', () => (t) => pathEl.getPointAtLength(t * totalLen).y)
                            .attrTween('opacity', () => (t) => t < 0.1 ? t * 7 : t > 0.9 ? (1 - t) * 7 : 0.7)
                            .on('end', animateParticle);
                    }
                    animateParticle();
                }
            }
        });

        // Draw nodes
        const nodeGroup = g.append('g').attr('class', 'nodes');

        allNodes.forEach(node => {
            const layerColor = LAYER_COLORS[node.layerId] || '#5A7080';
            const glowColor = LAYER_GLOW[node.layerId] || 'rgba(90,112,128,0.2)';
            const hasSignals = _nodeHasSignals(node);

            const ng = nodeGroup.append('g')
                .style('cursor', 'pointer')
                .on('mouseenter', () => setHoveredNode(node))
                .on('mouseleave', () => setHoveredNode(null))
                .on('click', () => setSelectedNode(prev => prev?.id === node.id ? null : node));

            // Background glow for nodes with signals
            if (hasSignals) {
                ng.append('rect')
                    .attr('x', node.x - 3)
                    .attr('y', node.y - 3)
                    .attr('width', node.w + 6)
                    .attr('height', node.h + 6)
                    .attr('rx', 12)
                    .attr('fill', glowColor)
                    .attr('filter', 'url(#glow)');
            }

            // Node rect with gradient
            const gradId = `nodeGrad-${node.id}`;
            const ng_grad = defs.append('linearGradient')
                .attr('id', gradId)
                .attr('x1', '0%').attr('y1', '0%').attr('x2', '100%').attr('y2', '100%');
            ng_grad.append('stop').attr('offset', '0%').attr('stop-color', '#0D1520');
            ng_grad.append('stop').attr('offset', '100%').attr('stop-color', '#111B2A');

            ng.append('rect')
                .attr('x', node.x)
                .attr('y', node.y)
                .attr('width', node.w)
                .attr('height', node.h)
                .attr('rx', 8)
                .attr('fill', `url(#${gradId})`)
                .attr('stroke', layerColor)
                .attr('stroke-width', 1.5)
                .attr('stroke-opacity', 0.6);

            // Node label
            ng.append('text')
                .attr('x', node.cx)
                .attr('y', node.y + 16)
                .attr('text-anchor', 'middle')
                .attr('font-size', '10px')
                .attr('font-weight', 600)
                .attr('fill', '#E8F0F8')
                .attr('font-family', "'JetBrains Mono', monospace")
                .text(_truncate(node.label, 18));

            // Key metric below label
            const keyMetric = _getKeyMetric(node);
            if (keyMetric) {
                ng.append('text')
                    .attr('x', node.cx)
                    .attr('y', node.y + 30)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '9px')
                    .attr('fill', keyMetric.color || colors.textDim)
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .text(keyMetric.text);
            }

            // Signal indicator
            const signalMetric = _getSignalMetric(node);
            if (signalMetric) {
                ng.append('text')
                    .attr('x', node.cx)
                    .attr('y', node.y + 42)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '8px')
                    .attr('fill', signalColor(signalMetric))
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .text(String(signalMetric).toUpperCase().replace(/_/g, ' '));
            }

            // Signal icons (top-right of node)
            const signals = _getActiveSignals(node);
            signals.forEach((sig, si) => {
                ng.append('text')
                    .attr('x', node.x + node.w - 4 - si * 14)
                    .attr('y', node.y - 2)
                    .attr('font-size', '11px')
                    .attr('text-anchor', 'end')
                    .text(sig.icon);
            });
        });

    }, [data, dimensions]);

    // ── Helper: detect signals on a node ──────────────────────────────
    function _nodeHasSignals(node) {
        const m = node.metrics || {};
        return Object.keys(SIGNAL_ICONS).some(k => m[k] && m[k] !== 'normal' && m[k] !== 'neutral' && m[k] !== 'stable');
    }

    function _getActiveSignals(node) {
        const m = node.metrics || {};
        const signals = [];
        Object.entries(SIGNAL_ICONS).forEach(([key, icon]) => {
            if (m[key] && m[key] !== 'normal' && m[key] !== 'neutral' && m[key] !== 'stable') {
                signals.push({ key, icon, value: m[key], label: SIGNAL_LABELS[key] || key });
            }
        });
        return signals;
    }

    function _getKeyMetric(node) {
        const m = node.metrics || {};
        if (m.net_liquidity != null) {
            return { text: `Net Liq: ${fmt(m.net_liquidity, { compact: true })}`, color: colors.textDim };
        }
        if (m.total_credit != null) {
            return { text: `Credit: ${fmt(m.total_credit, { compact: true })}`, color: colors.textDim };
        }
        if (m.price != null) {
            const ch = m.price_change_1m;
            const chStr = ch != null ? ` (${fmt(ch, { type: 'pct' })})` : '';
            return { text: `${fmt(m.price, { compact: true })}${chStr}`, color: ch > 0 ? '#22C55E' : ch < 0 ? '#EF4444' : colors.textDim };
        }
        if (m.price_change_1m != null) {
            return { text: `1m: ${fmt(m.price_change_1m, { type: 'pct' })}`, color: m.price_change_1m > 0 ? '#22C55E' : '#EF4444' };
        }
        return null;
    }

    function _getSignalMetric(node) {
        const m = node.metrics || {};
        return m.signal || m.gex_regime || null;
    }

    function _truncate(str, max) {
        if (!str) return '';
        return str.length > max ? str.substring(0, max - 2) + '..' : str;
    }

    // ── Render ────────────────────────────────────────────────────────
    if (!loading && error) {
        return (
            <div style={{ padding: '24px', color: colors.red }}>
                <div style={S.header}>GLOBAL MONEY FLOW</div>
                <div>{error}</div>
                <button onClick={loadData} style={{ ...S.btn, marginTop: '12px' }}>Retry</button>
            </div>
        );
    }

    const intelligence = data?.intelligence || {};
    const levers = data?.levers || [];
    const narrative = intelligence.narrative;
    const convergenceAlerts = intelligence.convergence_alerts || [];

    return (
        <div ref={containerRef} style={S.page}>
            {/* ── Header ─────────────────────────────────────────────── */}
            <div style={S.headerBar}>
                <div>
                    <div style={S.header}>GLOBAL MONEY FLOW</div>
                    <div style={S.subtitle}>
                        Where the money is, where it's going, and why
                    </div>
                </div>
                <button onClick={loadData} style={S.btn}>
                    {loading ? 'Loading...' : 'Refresh'}
                </button>
            </div>

            {/* ── Main content: flow diagram + levers sidebar ────────── */}
            <div style={S.mainRow}>
                {/* Flow diagram */}
                <div style={S.diagramContainer}>
                    {loading && !data ? (
                        <div style={S.loading}>
                            <div style={S.loadingDot} />
                            Mapping global money flows...
                        </div>
                    ) : (
                        <svg
                            ref={svgRef}
                            width={dimensions.width}
                            height={dimensions.height}
                            style={{ display: 'block', background: 'transparent' }}
                        />
                    )}

                    {/* Hover tooltip */}
                    {(hoveredNode || hoveredFlow) && (
                        <div style={S.tooltip}>
                            {hoveredNode && (
                                <div>
                                    <span style={{ color: LAYER_COLORS[hoveredNode.layerId] || colors.accent, fontWeight: 700 }}>
                                        {hoveredNode.label}
                                    </span>
                                    <span style={{ color: colors.textMuted, marginLeft: '8px', fontSize: '10px' }}>
                                        {hoveredNode.layerLabel}
                                    </span>
                                    <div style={{ marginTop: '4px' }}>
                                        {Object.entries(hoveredNode.metrics || {}).map(([k, v]) => (
                                            v != null && (
                                                <div key={k} style={{ fontSize: '10px', color: colors.textDim }}>
                                                    <span style={{ color: colors.textMuted }}>{k.replace(/_/g, ' ')}: </span>
                                                    <span style={{ color: signalColor(v) }}>
                                                        {typeof v === 'number' ? fmt(v, { compact: true }) : String(v)}
                                                    </span>
                                                </div>
                                            )
                                        ))}
                                    </div>
                                </div>
                            )}
                            {hoveredFlow && (
                                <div>
                                    <span style={{ color: FLOW_COLORS[hoveredFlow.direction], fontWeight: 700 }}>
                                        {hoveredFlow.from} {'\u2192'} {hoveredFlow.to}
                                    </span>
                                    <div style={{ fontSize: '10px', color: colors.textDim, marginTop: '2px' }}>
                                        {hoveredFlow.label || `${hoveredFlow.direction}: vol ${fmt(hoveredFlow.volume, { compact: true })}`}
                                    </div>
                                    {hoveredFlow.change != null && (
                                        <div style={{ fontSize: '10px', color: hoveredFlow.change > 0 ? '#22C55E' : '#EF4444' }}>
                                            Change: {fmt(hoveredFlow.change, { type: 'pct' })}
                                        </div>
                                    )}
                                </div>
                            )}
                        </div>
                    )}
                </div>

                {/* Levers sidebar */}
                <div style={S.leversSidebar}>
                    <div style={S.leversTitle}>THE LEVERS</div>
                    <div style={{ fontSize: '9px', color: colors.textMuted, marginBottom: '12px' }}>
                        Top forces moving markets now
                    </div>
                    {levers.length === 0 && !loading && (
                        <div style={{ fontSize: '10px', color: colors.textMuted }}>No lever data available</div>
                    )}
                    {levers.map((lever, i) => (
                        <div key={i} style={S.leverCard}>
                            <div style={S.leverRank}>{i + 1}</div>
                            <div style={{ flex: 1 }}>
                                <div style={S.leverName}>{lever.name}</div>
                                <div style={S.leverMagnitude}>
                                    {lever.magnitude_label}
                                </div>
                                <div style={S.leverBar}>
                                    <div style={{
                                        ...S.leverBarFill,
                                        width: `${Math.min(100, (lever.impact_score || 0) * 10)}%`,
                                        background: lever.direction === 'drain' || lever.direction === 'contracting' || lever.direction === 'outflow' || lever.direction === 'sell'
                                            ? '#EF4444' : '#22C55E',
                                    }} />
                                </div>
                                <div style={S.leverSource}>{lever.source}</div>
                            </div>
                            <div style={{
                                ...S.leverDirection,
                                color: lever.direction === 'drain' || lever.direction === 'contracting' || lever.direction === 'outflow' || lever.direction === 'sell'
                                    ? '#EF4444' : '#22C55E',
                            }}>
                                {lever.direction === 'drain' || lever.direction === 'contracting' || lever.direction === 'outflow' || lever.direction === 'sell'
                                    ? '\u25BC' : '\u25B2'}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {/* ── Selected node detail panel ───────────────────────────── */}
            {selectedNode && (
                <div style={S.detailPanel}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div style={{ fontSize: '12px', fontWeight: 700, color: LAYER_COLORS[selectedNode.layerId] || colors.accent }}>
                            {selectedNode.label}
                            <span style={{ color: colors.textMuted, fontWeight: 400, marginLeft: '8px', fontSize: '10px' }}>
                                {selectedNode.layerLabel}
                            </span>
                        </div>
                        <button onClick={() => setSelectedNode(null)} style={S.closeBtn}>{'\u2715'}</button>
                    </div>
                    <div style={S.metricGrid}>
                        {Object.entries(selectedNode.metrics || {}).map(([k, v]) => (
                            v != null && (
                                <div key={k} style={S.metricCell}>
                                    <div style={S.metricLabel}>{k.replace(/_/g, ' ')}</div>
                                    <div style={{ ...S.metricValue, color: signalColor(v) }}>
                                        {typeof v === 'number' ? fmt(v, { compact: true }) : String(v)}
                                    </div>
                                </div>
                            )
                        ))}
                    </div>
                    {/* Signal badges */}
                    {_getActiveSignals(selectedNode).length > 0 && (
                        <div style={{ display: 'flex', gap: '6px', marginTop: '8px', flexWrap: 'wrap' }}>
                            {_getActiveSignals(selectedNode).map((sig, i) => (
                                <div key={i} style={{
                                    ...S.signalBadge,
                                    borderColor: signalColor(sig.value),
                                    color: signalColor(sig.value),
                                }}>
                                    <span>{sig.icon}</span>
                                    <span>{sig.label}: {String(sig.value).replace(/_/g, ' ')}</span>
                                </div>
                            ))}
                        </div>
                    )}
                    {/* Sector deep dive link */}
                    {selectedNode.layerId === 'sectors' && (
                        <button
                            onClick={() => {
                                const sectorLabel = selectedNode.label || selectedNode.name;
                                if (onNavigate) {
                                    onNavigate('sector-dive', sectorLabel);
                                } else {
                                    window.location.hash = `#/sector-dive/${encodeURIComponent(sectorLabel)}`;
                                }
                            }}
                            style={{
                                marginTop: '10px', width: '100%', padding: '8px 12px',
                                background: `${colors.accent}20`, border: `1px solid ${colors.accent}`,
                                borderRadius: '6px', color: colors.accent, cursor: 'pointer',
                                fontFamily: "'JetBrains Mono', monospace", fontSize: '11px', fontWeight: 600,
                            }}
                        >
                            Deep Dive: {selectedNode.label} Sector
                        </button>
                    )}
                </div>
            )}

            {/* ── Narrative panel ──────────────────────────────────────── */}
            <div style={S.narrativePanel}>
                <div style={S.narrativeHeader}>
                    <div style={S.sectionTitle}>FLOW NARRATIVE</div>
                    {convergenceAlerts.length > 0 && (
                        <div style={S.convergenceBadge}>
                            {'\u26A1'} {convergenceAlerts.length} convergence alert{convergenceAlerts.length > 1 ? 's' : ''}
                        </div>
                    )}
                </div>
                <div style={S.narrativeText}>
                    {narrative || (loading ? 'Generating narrative...' : 'No narrative available.')}
                </div>

                {/* Convergence alerts */}
                {convergenceAlerts.length > 0 && (
                    <div style={{ marginTop: '10px' }}>
                        <div style={{ fontSize: '9px', fontWeight: 700, color: '#F59E0B', letterSpacing: '1px', marginBottom: '6px' }}>
                            CONVERGENCE ALERTS
                        </div>
                        {convergenceAlerts.slice(0, 5).map((alert, i) => (
                            <div key={i} style={S.alertRow}>
                                <span style={{ fontWeight: 700, color: '#E8F0F8' }}>{alert.ticker}</span>
                                <span style={{
                                    padding: '1px 6px', borderRadius: '3px', fontSize: '9px', fontWeight: 700,
                                    background: alert.direction === 'BUY' ? '#22C55E20' : '#EF444420',
                                    color: alert.direction === 'BUY' ? '#22C55E' : '#EF4444',
                                }}>
                                    {alert.direction}
                                </span>
                                <span style={{ color: colors.textMuted }}>
                                    {alert.source_count} sources
                                </span>
                                {alert.combined_confidence != null && (
                                    <span style={{ color: colors.textDim }}>
                                        conf: {(alert.combined_confidence * 100).toFixed(0)}%
                                    </span>
                                )}
                            </div>
                        ))}
                    </div>
                )}

                {/* Trusted signals */}
                {(intelligence.trusted_signals || []).length > 0 && (
                    <div style={{ marginTop: '10px' }}>
                        <div style={{ fontSize: '9px', fontWeight: 700, color: colors.accent, letterSpacing: '1px', marginBottom: '6px' }}>
                            TRUSTED SIGNALS
                        </div>
                        {intelligence.trusted_signals.slice(0, 5).map((sig, i) => (
                            <div key={i} style={S.alertRow}>
                                <span style={{ color: colors.textDim, fontSize: '9px' }}>{sig.source_type}</span>
                                <span style={{ fontWeight: 700, color: '#E8F0F8' }}>{sig.ticker}</span>
                                <span style={{
                                    color: sig.direction === 'BUY' ? '#22C55E' : '#EF4444',
                                    fontWeight: 600,
                                }}>
                                    {sig.direction}
                                </span>
                                <span style={{ color: colors.textMuted }}>
                                    trust: {(sig.trust_score * 100).toFixed(0)}%
                                </span>
                            </div>
                        ))}
                    </div>
                )}
            </div>

            {/* ── Legend ────────────────────────────────────────────────── */}
            <div style={S.legend}>
                <span style={{ color: '#22C55E' }}>{'\u2500\u2500\u2500'} Inflow</span>
                <span style={{ color: '#EF4444' }}>{'\u2500\u2500\u2500'} Outflow</span>
                <span>{'\u25CF'} Animated particles = flow direction</span>
                <span>Click nodes for detail</span>
                <span style={{ marginLeft: 'auto' }}>
                    {data?.timestamp ? `Updated: ${new Date(data.timestamp).toLocaleTimeString()}` : ''}
                </span>
            </div>
        </div>
    );
}


// ── Styles ────────────────────────────────────────────────────────────
const S = {
    page: {
        padding: '16px',
        maxWidth: '1400px',
        margin: '0 auto',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    headerBar: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'flex-start',
        marginBottom: '16px',
    },
    header: {
        fontSize: '20px',
        fontWeight: 700,
        color: '#E8F0F8',
        letterSpacing: '2px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    subtitle: {
        fontSize: '11px',
        color: colors.textMuted,
        marginTop: '2px',
    },
    btn: {
        background: colors.accent,
        color: '#fff',
        border: 'none',
        borderRadius: '6px',
        padding: '8px 16px',
        fontSize: '12px',
        fontWeight: 600,
        cursor: 'pointer',
        fontFamily: "'JetBrains Mono', monospace",
    },
    closeBtn: {
        background: 'none',
        border: 'none',
        color: colors.textMuted,
        cursor: 'pointer',
        fontSize: '14px',
        padding: '4px 8px',
    },
    mainRow: {
        display: 'flex',
        gap: '16px',
        alignItems: 'flex-start',
    },
    diagramContainer: {
        flex: 1,
        background: colors.card,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        overflow: 'hidden',
        position: 'relative',
    },
    loading: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        gap: '10px',
        height: '400px',
        color: colors.textMuted,
        fontSize: '13px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    loadingDot: {
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        background: colors.accent,
        animation: 'pulse 1.5s ease infinite',
    },
    tooltip: {
        position: 'absolute',
        top: '8px',
        left: '8px',
        background: 'rgba(13, 21, 32, 0.95)',
        backdropFilter: 'blur(8px)',
        border: `1px solid ${colors.border}`,
        borderRadius: '8px',
        padding: '10px 14px',
        fontSize: '11px',
        fontFamily: "'JetBrains Mono', monospace",
        maxWidth: '320px',
        zIndex: 10,
        pointerEvents: 'none',
        boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
    },
    leversSidebar: {
        width: '260px',
        minWidth: '220px',
        background: colors.card,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        padding: '16px',
        flexShrink: 0,
    },
    leversTitle: {
        fontSize: '11px',
        fontWeight: 700,
        letterSpacing: '2px',
        color: '#F59E0B',
        fontFamily: "'JetBrains Mono', monospace",
        marginBottom: '4px',
    },
    leverCard: {
        display: 'flex',
        gap: '10px',
        alignItems: 'flex-start',
        padding: '10px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    leverRank: {
        width: '20px',
        height: '20px',
        borderRadius: '50%',
        background: '#1A284080',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        fontSize: '10px',
        fontWeight: 700,
        color: colors.textDim,
        flexShrink: 0,
        fontFamily: "'JetBrains Mono', monospace",
    },
    leverName: {
        fontSize: '11px',
        fontWeight: 600,
        color: '#E8F0F8',
        fontFamily: "'JetBrains Mono', monospace",
    },
    leverMagnitude: {
        fontSize: '10px',
        color: colors.textDim,
        marginTop: '2px',
    },
    leverBar: {
        width: '100%',
        height: '3px',
        background: colors.bg,
        borderRadius: '2px',
        overflow: 'hidden',
        marginTop: '4px',
    },
    leverBarFill: {
        height: '100%',
        borderRadius: '2px',
        transition: 'width 0.5s ease',
    },
    leverSource: {
        fontSize: '8px',
        color: colors.textMuted,
        marginTop: '2px',
    },
    leverDirection: {
        fontSize: '14px',
        fontWeight: 700,
        flexShrink: 0,
    },
    detailPanel: {
        background: colors.cardElevated,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        padding: '14px 18px',
        marginTop: '12px',
    },
    metricGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(130px, 1fr))',
        gap: '8px',
        marginTop: '10px',
    },
    metricCell: {
        background: colors.bg,
        borderRadius: '8px',
        padding: '8px 10px',
    },
    metricLabel: {
        fontSize: '9px',
        color: colors.textMuted,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
    },
    metricValue: {
        fontSize: '13px',
        fontWeight: 600,
        fontFamily: "'JetBrains Mono', monospace",
        marginTop: '2px',
    },
    signalBadge: {
        display: 'inline-flex',
        alignItems: 'center',
        gap: '4px',
        padding: '3px 8px',
        borderRadius: '6px',
        border: '1px solid',
        fontSize: '9px',
        fontFamily: "'JetBrains Mono', monospace",
        background: 'rgba(0,0,0,0.2)',
    },
    narrativePanel: {
        background: colors.card,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        padding: '16px 20px',
        marginTop: '12px',
    },
    narrativeHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        marginBottom: '8px',
    },
    sectionTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: "'JetBrains Mono', monospace",
    },
    convergenceBadge: {
        fontSize: '10px',
        fontWeight: 600,
        color: '#F59E0B',
        background: '#F59E0B15',
        padding: '3px 8px',
        borderRadius: '6px',
    },
    narrativeText: {
        fontSize: '13px',
        color: colors.textDim,
        lineHeight: '1.7',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    alertRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '4px 0',
        fontSize: '11px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    legend: {
        display: 'flex',
        gap: '16px',
        alignItems: 'center',
        padding: '10px 0',
        fontSize: '9px',
        color: colors.textMuted,
        fontFamily: "'JetBrains Mono', monospace",
        flexWrap: 'wrap',
    },
};
