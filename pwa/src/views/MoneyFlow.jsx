/**
 * MoneyFlow — Global Money Flow Visualization.
 *
 * Full-page, immersive view showing how money flows through the global
 * financial system: Central Banks -> Banking -> Markets -> Sectors.
 *
 * Features:
 *   - Vertical flow diagram with D3 (layers, curved links, animated particles)
 *   - Real dollar amounts on every link ($2.3B, $450M, $12K formatting)
 *   - Link thickness proportional to dollar volume (log scale)
 *   - Signal overlays with trust scores (insider, congressional, dark pool, whale, convergence)
 *   - Hover detail: exact amount, sources, confidence, weekly trend
 *   - Sector rotation panel: net flow per sector (green inflow, red outflow)
 *   - Actor tier breakdown (institutional, congressional, insider)
 *   - Time slider: 30/60/90 day flow animation
 *   - Momentum badges on sector nodes
 *   - LLM-generated narrative panel
 *   - Interactive drill-down on nodes and links
 *   - "Levers" sidebar showing top market-moving forces
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
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

const TIME_PERIODS = [
    { label: '30D', days: 30 },
    { label: '60D', days: 60 },
    { label: '90D', days: 90 },
];

// ── Formatting helpers ────────────────────────────────────────────────

/** Format dollar values: $2.3B, $450M, $12K */
function _fmt(val) {
    if (val == null || isNaN(val)) return '--';
    const abs = Math.abs(val);
    const sign = val < 0 ? '-' : '';
    if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(1)}T`;
    if (abs >= 1e9) return `${sign}$${(abs / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `${sign}$${(abs / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `${sign}$${(abs / 1e3).toFixed(0)}K`;
    return `${sign}$${abs.toFixed(0)}`;
}

/** Format with sign prefix for flow labels */
function _fmtSigned(val) {
    if (val == null || isNaN(val)) return '--';
    const prefix = val > 0 ? '+' : '';
    return prefix + _fmt(val);
}

function fmt(val, opts = {}) {
    if (val == null) return '--';
    const { type = 'number', compact = false } = opts;
    if (type === 'pct') return `${val >= 0 ? '+' : ''}${(val * 100).toFixed(1)}%`;
    if (type === 'money') return _fmt(val);
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

/** Log-scale link thickness. Keeps small flows visible while capping huge ones. */
function logThickness(volume, maxVolume) {
    if (!volume || volume <= 0) return 2;
    const logVal = Math.log10(Math.max(1, Math.abs(volume)));
    const logMax = Math.log10(Math.max(1, maxVolume));
    if (logMax === 0) return 4;
    const ratio = logVal / logMax;
    return Math.max(2, Math.min(16, ratio * 16));
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

    // Aggregated flow data
    const [aggData, setAggData] = useState(null);
    const [aggLoading, setAggLoading] = useState(false);
    const [selectedDays, setSelectedDays] = useState(30);
    const [tierOpen, setTierOpen] = useState(false);

    // Time slider state
    const [timeSliderValue, setTimeSliderValue] = useState(2); // index: 0=30, 1=60, 2=90
    const [animating, setAnimating] = useState(false);
    const animRef = useRef(null);

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

    const loadAggregated = useCallback(async (days) => {
        setAggLoading(true);
        try {
            const d = await api.getAggregatedFlows(null, 'weekly', days);
            if (!d.error) setAggData(d);
        } catch (_) { /* graceful degradation */ }
        setAggLoading(false);
    }, []);

    useEffect(() => { loadData(); }, [loadData]);
    useEffect(() => { loadAggregated(selectedDays); }, [loadAggregated, selectedDays]);

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

    // ── Sector rotation data (memoized from aggData) ─────────────────
    const sectorRotation = useMemo(() => {
        if (!aggData?.by_sector) return [];
        return Object.entries(aggData.by_sector)
            .filter(([name]) => name !== 'Unknown')
            .map(([name, sd]) => ({
                name,
                net_flow: sd.net_flow || 0,
                direction: sd.direction,
                acceleration: sd.acceleration,
                acceleration_pct: sd.acceleration_pct || 0,
                this_week_flow: sd.this_week_flow || 0,
                inflow: sd.inflow || 0,
                outflow: sd.outflow || 0,
                source_breakdown: sd.source_breakdown || {},
                top_actors: sd.top_actors || [],
            }))
            .sort((a, b) => Math.abs(b.net_flow) - Math.abs(a.net_flow));
    }, [aggData]);

    const actorTiers = useMemo(() => {
        if (!aggData?.by_actor_tier) return {};
        return aggData.by_actor_tier;
    }, [aggData]);

    // Top leaving and entering labels
    const rotationLabel = useMemo(() => {
        if (!sectorRotation.length) return null;
        const leaving = sectorRotation.filter(s => s.net_flow < 0).sort((a, b) => a.net_flow - b.net_flow);
        const entering = sectorRotation.filter(s => s.net_flow > 0).sort((a, b) => b.net_flow - a.net_flow);
        const parts = [];
        if (leaving.length > 0) {
            parts.push(`Money is leaving ${leaving[0].name} (${_fmtSigned(leaving[0].this_week_flow)}/week)`);
        }
        if (entering.length > 0) {
            parts.push(`entering ${entering[0].name} (${_fmtSigned(entering[0].this_week_flow)}/week)`);
        }
        return parts.join(' and ');
    }, [sectorRotation]);

    // Max bar width reference
    const maxSectorFlow = useMemo(() => {
        if (!sectorRotation.length) return 1;
        return Math.max(1, ...sectorRotation.map(s => Math.abs(s.net_flow)));
    }, [sectorRotation]);

    // ── Time slider animation ────────────────────────────────────────
    const startAnimation = useCallback(() => {
        if (animating) {
            clearInterval(animRef.current);
            setAnimating(false);
            return;
        }
        setAnimating(true);
        setTimeSliderValue(0);
        setSelectedDays(30);
        let step = 0;
        animRef.current = setInterval(() => {
            step++;
            if (step >= TIME_PERIODS.length) {
                clearInterval(animRef.current);
                setAnimating(false);
                return;
            }
            setTimeSliderValue(step);
            setSelectedDays(TIME_PERIODS[step].days);
        }, 2500);
    }, [animating]);

    useEffect(() => {
        return () => { if (animRef.current) clearInterval(animRef.current); };
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

        // Draw links (curved vertical paths) with real dollar amounts
        const linkGroup = g.append('g').attr('class', 'links');
        const maxFlowVol = Math.max(1, ...flows.map(f => Math.abs(f.amount_usd || f.volume || 1)));

        flows.forEach(flow => {
            const src = nodeById[flow.from];
            const tgt = nodeById[flow.to];
            if (!src || !tgt) return;

            const rawVol = Math.abs(flow.amount_usd || flow.volume || 1);
            const thickness = logThickness(rawVol, maxFlowVol);
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

            // Dollar label on the link midpoint
            const labelX = (x1 + x2) / 2;
            const labelY = midY;
            const dollarLabel = _fmt(flow.amount_usd || flow.volume);

            // Background rect for readability
            linkGroup.append('rect')
                .attr('x', labelX - 28)
                .attr('y', labelY - 8)
                .attr('width', 56)
                .attr('height', 16)
                .attr('rx', 4)
                .attr('fill', 'rgba(8, 12, 16, 0.85)')
                .attr('stroke', flowColor)
                .attr('stroke-width', 0.5)
                .attr('stroke-opacity', 0.4)
                .style('pointer-events', 'none');

            linkGroup.append('text')
                .attr('x', labelX)
                .attr('y', labelY + 4)
                .attr('text-anchor', 'middle')
                .attr('font-size', '9px')
                .attr('font-weight', 600)
                .attr('fill', flowColor)
                .attr('font-family', "'JetBrains Mono', monospace")
                .style('pointer-events', 'none')
                .text(dollarLabel);

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

        // Momentum lookup from aggregated data
        const sectorMomentum = {};
        if (aggData?.by_sector) {
            Object.entries(aggData.by_sector).forEach(([name, sd]) => {
                sectorMomentum[name.toLowerCase()] = sd.acceleration;
            });
        }

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

            // Momentum badge for sector nodes
            if (node.layerId === 'sectors') {
                const nodeLabel = (node.label || '').toLowerCase();
                const momentum = sectorMomentum[nodeLabel];
                if (momentum === 'accelerating' || momentum === 'decelerating') {
                    const isUp = momentum === 'accelerating';
                    const badgeColor = isUp ? '#22C55E' : '#EF4444';
                    const arrow = isUp ? '\u25B2' : '\u25BC';

                    ng.append('rect')
                        .attr('x', node.x - 1)
                        .attr('y', node.y - 1)
                        .attr('width', 16)
                        .attr('height', 14)
                        .attr('rx', 4)
                        .attr('fill', isUp ? 'rgba(34,197,94,0.15)' : 'rgba(239,68,68,0.15)')
                        .attr('stroke', badgeColor)
                        .attr('stroke-width', 0.8);

                    ng.append('text')
                        .attr('x', node.x + 7)
                        .attr('y', node.y + 10)
                        .attr('text-anchor', 'middle')
                        .attr('font-size', '9px')
                        .attr('fill', badgeColor)
                        .attr('font-weight', 700)
                        .text(arrow);
                }
            }
        });

    }, [data, dimensions, aggData]);

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

            {/* ── Time slider ─────────────────────────────────────────── */}
            <div style={S.timeSliderBar}>
                <div style={S.timeSliderLabel}>TIME WINDOW</div>
                <div style={S.timeSliderControls}>
                    {TIME_PERIODS.map((p, i) => (
                        <button
                            key={p.days}
                            style={{
                                ...S.timeBtn,
                                background: selectedDays === p.days ? colors.accent : colors.bg,
                                color: selectedDays === p.days ? '#fff' : colors.textMuted,
                                borderColor: selectedDays === p.days ? colors.accent : colors.border,
                            }}
                            onClick={() => { setSelectedDays(p.days); setTimeSliderValue(i); }}
                        >
                            {p.label}
                        </button>
                    ))}
                    <button
                        style={{
                            ...S.timeBtn,
                            background: animating ? '#EF4444' : colors.bg,
                            color: animating ? '#fff' : colors.textMuted,
                            borderColor: animating ? '#EF4444' : colors.border,
                            marginLeft: '8px',
                        }}
                        onClick={startAnimation}
                    >
                        {animating ? 'Stop' : '\u25B6 Animate'}
                    </button>
                </div>
                <input
                    type="range"
                    min={0}
                    max={TIME_PERIODS.length - 1}
                    value={timeSliderValue}
                    onChange={e => {
                        const idx = parseInt(e.target.value);
                        setTimeSliderValue(idx);
                        setSelectedDays(TIME_PERIODS[idx].days);
                    }}
                    style={S.slider}
                />
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

                    {/* Hover tooltip — enhanced with dollar details */}
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
                                    {/* Exact dollar amount */}
                                    <div style={{ fontSize: '13px', fontWeight: 700, color: '#E8F0F8', marginTop: '4px' }}>
                                        {_fmt(hoveredFlow.amount_usd || hoveredFlow.volume)}
                                    </div>
                                    {/* Source feeds that contributed */}
                                    {hoveredFlow.sources && hoveredFlow.sources.length > 0 && (
                                        <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '4px' }}>
                                            <span style={{ color: colors.textDim }}>Sources: </span>
                                            {hoveredFlow.sources.join(', ')}
                                        </div>
                                    )}
                                    {/* Confidence level */}
                                    {hoveredFlow.confidence != null && (
                                        <div style={{ fontSize: '9px', color: colors.textDim, marginTop: '2px' }}>
                                            <span style={{ color: colors.textMuted }}>Confidence: </span>
                                            <span style={{
                                                color: hoveredFlow.confidence >= 0.7 ? '#22C55E'
                                                    : hoveredFlow.confidence >= 0.4 ? '#F59E0B' : '#EF4444',
                                            }}>
                                                {(hoveredFlow.confidence * 100).toFixed(0)}%
                                            </span>
                                        </div>
                                    )}
                                    {/* Weekly trend */}
                                    {hoveredFlow.change != null && (
                                        <div style={{ fontSize: '9px', marginTop: '2px' }}>
                                            <span style={{ color: colors.textMuted }}>Weekly trend: </span>
                                            <span style={{ color: hoveredFlow.change > 0 ? '#22C55E' : '#EF4444', fontWeight: 600 }}>
                                                {hoveredFlow.change > 0.05 ? 'Accelerating' : hoveredFlow.change < -0.05 ? 'Decelerating' : 'Stable'}
                                                {' '}{fmt(hoveredFlow.change, { type: 'pct' })}
                                            </span>
                                        </div>
                                    )}
                                    {/* Fallback: old-style label */}
                                    {!hoveredFlow.amount_usd && !hoveredFlow.sources && hoveredFlow.label && (
                                        <div style={{ fontSize: '10px', color: colors.textDim, marginTop: '2px' }}>
                                            {hoveredFlow.label}
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

            {/* ── Sector Rotation Panel ───────────────────────────────── */}
            <div style={S.rotationPanel}>
                <div style={S.sectionTitle}>SECTOR ROTATION</div>
                {aggLoading && !aggData && (
                    <div style={{ fontSize: '10px', color: colors.textMuted, padding: '12px 0' }}>Loading sector data...</div>
                )}
                {rotationLabel && (
                    <div style={S.rotationLabel}>{rotationLabel}</div>
                )}
                {sectorRotation.length > 0 && (
                    <div style={S.rotationBars}>
                        {sectorRotation.map(sector => {
                            const isPositive = sector.net_flow >= 0;
                            const barPct = Math.min(100, (Math.abs(sector.net_flow) / maxSectorFlow) * 100);
                            const barColor = isPositive ? '#22C55E' : '#EF4444';
                            const accelIcon = sector.acceleration === 'accelerating' ? ' \u25B2'
                                : sector.acceleration === 'decelerating' ? ' \u25BC' : '';

                            return (
                                <div key={sector.name} style={S.rotationRow}>
                                    <div style={S.rotationSectorName}>{sector.name}</div>
                                    <div style={S.rotationBarContainer}>
                                        <div style={{
                                            ...S.rotationBarFill,
                                            width: `${barPct}%`,
                                            background: barColor,
                                        }} />
                                    </div>
                                    <div style={{
                                        ...S.rotationAmount,
                                        color: barColor,
                                    }}>
                                        {_fmtSigned(sector.net_flow)}
                                        <span style={{ fontSize: '8px', color: barColor, opacity: 0.7 }}>{accelIcon}</span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                )}
                {sectorRotation.length === 0 && !aggLoading && (
                    <div style={{ fontSize: '10px', color: colors.textMuted, padding: '8px 0' }}>No sector rotation data available</div>
                )}
            </div>

            {/* ── Actor Tier Breakdown (collapsible) ──────────────────── */}
            <div style={S.tierPanel}>
                <div
                    style={S.tierHeader}
                    onClick={() => setTierOpen(prev => !prev)}
                >
                    <div style={S.sectionTitle}>ACTOR TIER BREAKDOWN</div>
                    <span style={{ color: colors.textMuted, fontSize: '12px', cursor: 'pointer' }}>
                        {tierOpen ? '\u25B2' : '\u25BC'}
                    </span>
                </div>
                {tierOpen && (
                    <div style={S.tierContent}>
                        {Object.entries(actorTiers).map(([tier, td]) => {
                            if (!td || td.net_flow === 0) return null;
                            const isPositive = td.net_flow >= 0;

                            // Build tier summary line
                            let summary = '';
                            const sectorBd = td.sector_breakdown || {};
                            const topSector = Object.entries(sectorBd)
                                .filter(([name]) => name !== 'Unknown')
                                .sort((a, b) => Math.abs(b[1]) - Math.abs(a[1]))[0];

                            const tierLabel = tier.charAt(0).toUpperCase() + tier.slice(1);

                            if (tier === 'institutional') {
                                summary = `${tierLabel}: ${_fmtSigned(td.net_flow)} into ${topSector ? topSector[0] : 'markets'}`;
                            } else if (tier === 'individual') {
                                // Break out congressional and insider from actor list
                                const actors = td.top_actors || [];
                                const congActors = actors.filter(a => (a.name || '').toLowerCase().includes('congress'));
                                const insiderActors = actors.filter(a => !(a.name || '').toLowerCase().includes('congress'));
                                const parts = [`${tierLabel}: ${_fmtSigned(td.net_flow)}`];
                                if (congActors.length > 0) {
                                    const congTotal = congActors.reduce((s, a) => s + Math.abs(a.net_flow), 0);
                                    parts.push(`Congressional: ${_fmt(congTotal)} (${congActors.length} member${congActors.length > 1 ? 's' : ''})`);
                                }
                                if (insiderActors.length > 0) {
                                    const insTotal = insiderActors.reduce((s, a) => s + Math.abs(a.net_flow), 0);
                                    const selling = insiderActors.filter(a => a.net_flow < 0).length;
                                    if (selling > insiderActors.length / 2) {
                                        parts.push(`Insider: ${_fmt(insTotal)} selling${topSector ? ' in ' + topSector[0] : ''} (cluster pattern)`);
                                    } else {
                                        parts.push(`Insider: ${_fmt(insTotal)}${topSector ? ' in ' + topSector[0] : ''}`);
                                    }
                                }
                                summary = parts.join(' | ');
                            } else {
                                summary = `${tierLabel}: ${_fmtSigned(td.net_flow)}${topSector ? ' into ' + topSector[0] : ''}`;
                            }

                            return (
                                <div key={tier} style={S.tierRow}>
                                    <div style={{
                                        ...S.tierDot,
                                        background: isPositive ? '#22C55E' : '#EF4444',
                                    }} />
                                    <div style={S.tierSummary}>{summary}</div>
                                    <div style={{
                                        ...S.tierAmount,
                                        color: isPositive ? '#22C55E' : '#EF4444',
                                    }}>
                                        {td.weekly_rate != null ? `${_fmtSigned(td.weekly_rate)}/wk` : ''}
                                    </div>
                                </div>
                            );
                        })}
                        {Object.keys(actorTiers).length === 0 && (
                            <div style={{ fontSize: '10px', color: colors.textMuted }}>No actor tier data available</div>
                        )}
                    </div>
                )}
            </div>

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
                <span>Link thickness = dollar volume (log scale)</span>
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
    // ── Time slider ──────────────────────────────────────────────
    timeSliderBar: {
        background: colors.card,
        borderRadius: '10px',
        border: `1px solid ${colors.border}`,
        padding: '10px 16px',
        marginBottom: '12px',
        display: 'flex',
        alignItems: 'center',
        gap: '16px',
        flexWrap: 'wrap',
    },
    timeSliderLabel: {
        fontSize: '9px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: "'JetBrains Mono', monospace",
    },
    timeSliderControls: {
        display: 'flex',
        gap: '4px',
        alignItems: 'center',
    },
    timeBtn: {
        padding: '5px 12px',
        borderRadius: '6px',
        fontSize: '10px',
        fontWeight: 600,
        cursor: 'pointer',
        border: '1px solid',
        fontFamily: "'JetBrains Mono', monospace",
        transition: 'all 0.15s ease',
    },
    slider: {
        flex: 1,
        minWidth: '100px',
        maxWidth: '200px',
        accentColor: colors.accent,
        height: '4px',
    },
    // ── Main layout ──────────────────────────────────────────────
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
        maxWidth: '360px',
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
    // ── Sector Rotation ──────────────────────────────────────────
    rotationPanel: {
        background: colors.card,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        padding: '16px 20px',
        marginTop: '12px',
    },
    rotationLabel: {
        fontSize: '11px',
        color: colors.textDim,
        lineHeight: '1.5',
        marginTop: '4px',
        marginBottom: '12px',
        fontFamily: "'IBM Plex Sans', sans-serif",
        fontStyle: 'italic',
    },
    rotationBars: {
        display: 'flex',
        flexDirection: 'column',
        gap: '6px',
    },
    rotationRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
    },
    rotationSectorName: {
        width: '140px',
        fontSize: '11px',
        fontWeight: 600,
        color: '#E8F0F8',
        fontFamily: "'JetBrains Mono', monospace",
        textAlign: 'right',
        flexShrink: 0,
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
    },
    rotationBarContainer: {
        flex: 1,
        height: '10px',
        background: colors.bg,
        borderRadius: '4px',
        overflow: 'hidden',
    },
    rotationBarFill: {
        height: '100%',
        borderRadius: '4px',
        transition: 'width 0.6s ease, background 0.3s ease',
    },
    rotationAmount: {
        width: '80px',
        fontSize: '10px',
        fontWeight: 700,
        fontFamily: "'JetBrains Mono', monospace",
        textAlign: 'right',
        flexShrink: 0,
    },
    // ── Actor Tier ───────────────────────────────────────────────
    tierPanel: {
        background: colors.card,
        borderRadius: '12px',
        border: `1px solid ${colors.border}`,
        marginTop: '12px',
        overflow: 'hidden',
    },
    tierHeader: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '14px 20px',
        cursor: 'pointer',
        userSelect: 'none',
    },
    tierContent: {
        padding: '0 20px 16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '8px',
    },
    tierRow: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    tierDot: {
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        flexShrink: 0,
    },
    tierSummary: {
        flex: 1,
        fontSize: '11px',
        color: colors.textDim,
        fontFamily: "'JetBrains Mono', monospace",
        lineHeight: '1.4',
    },
    tierAmount: {
        fontSize: '10px',
        fontWeight: 700,
        fontFamily: "'JetBrains Mono', monospace",
        flexShrink: 0,
    },
    // ── Narrative ────────────────────────────────────────────────
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
