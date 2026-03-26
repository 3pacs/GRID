/**
 * CapitalFlowSankey — 4D interactive capital flow visualization.
 *
 * D3 Sankey diagram showing money flowing between Market → Sectors → Subsectors → Actors.
 * - Click any node to expand/collapse detail
 * - Zoom and pan
 * - Time slider scrubs through historical snapshots
 * - Color: green = inflow (outperforming SPY), red = outflow
 * - Link width = magnitude of flow
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { sankey as d3Sankey, sankeyLinkHorizontal } from 'd3-sankey';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const LEVEL_COLORS = {
    root: '#1A6EBF',
    sector: '#8B5CF6',
    subsector: '#06B6D4',
    actor: '#F59E0B',
};

const FLOW_COLORS = {
    inflow: '#22C55E',
    outflow: '#EF4444',
};

export default function CapitalFlowSankey() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [expandedNodes, setExpandedNodes] = useState(new Set(['root', 'sector']));
    const [hoveredNode, setHoveredNode] = useState(null);
    const [hoveredLink, setHoveredLink] = useState(null);
    const [selectedSnapshot, setSelectedSnapshot] = useState(null);
    const [dimensions, setDimensions] = useState({ width: 800, height: 500 });

    useEffect(() => { loadData(); }, []);

    useEffect(() => {
        if (containerRef.current) {
            const w = containerRef.current.clientWidth;
            setDimensions({ width: Math.max(600, w), height: Math.max(400, Math.min(700, w * 0.65)) });
        }
    }, []);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const d = await api.getSankeyData(selectedSnapshot);
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load');
        }
        setLoading(false);
    };

    // Filter nodes/links based on expansion level
    const getFilteredData = useCallback(() => {
        if (!data) return { nodes: [], links: [] };

        const visibleLevels = expandedNodes;
        const visibleNodes = data.nodes.filter(n => visibleLevels.has(n.level));
        const visibleIds = new Set(visibleNodes.map(n => n.id));

        // Remap links: if target not visible, collapse to its parent
        const nodeById = {};
        data.nodes.forEach(n => { nodeById[n.id] = n; });

        const filteredLinks = [];
        const linkMap = {};

        data.links.forEach(link => {
            let src = link.source;
            let tgt = link.target;

            // Walk up to visible parent if node is hidden
            if (!visibleIds.has(src)) return;
            if (!visibleIds.has(tgt)) {
                // Find the parent at a visible level
                const tgtNode = nodeById[tgt];
                if (!tgtNode) return;
                // For actors → collapse to subsector, for subsectors → collapse to sector
                const parentLink = data.links.find(l =>
                    l.target === tgt && visibleIds.has(l.source)
                );
                if (parentLink) tgt = parentLink.source;
                else return;
            }

            if (src === tgt) return;

            const key = `${src}-${tgt}`;
            if (linkMap[key]) {
                linkMap[key].value += link.value;
            } else {
                linkMap[key] = { ...link, source: src, target: tgt };
                filteredLinks.push(linkMap[key]);
            }
        });

        // Reindex for d3-sankey (needs consecutive 0-based indices)
        const nodeMap = {};
        const reindexed = [];
        visibleNodes.forEach((n, i) => {
            nodeMap[n.id] = i;
            reindexed.push({ ...n, index: i });
        });

        const reindexedLinks = filteredLinks
            .filter(l => nodeMap[l.source] !== undefined && nodeMap[l.target] !== undefined)
            .map(l => ({
                ...l,
                source: nodeMap[l.source],
                target: nodeMap[l.target],
            }));

        return { nodes: reindexed, links: reindexedLinks };
    }, [data, expandedNodes]);

    // Render Sankey
    useEffect(() => {
        if (!data || !svgRef.current) return;

        const { nodes, links } = getFilteredData();
        if (nodes.length === 0) return;

        const { width, height } = dimensions;
        const margin = { top: 20, right: 30, bottom: 20, left: 30 };

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const g = svg.append('g')
            .attr('transform', `translate(${margin.left},${margin.top})`);

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.3, 4])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
            });
        svg.call(zoom);

        const innerW = width - margin.left - margin.right;
        const innerH = height - margin.top - margin.bottom;

        // Build Sankey layout
        const sankeyLayout = d3Sankey()
            .nodeId(d => d.index)
            .nodeWidth(18)
            .nodePadding(8)
            .nodeAlign(d3.sankeyLeft || ((node, n) => node.depth))
            .extent([[0, 0], [innerW, innerH]]);

        let graph;
        try {
            graph = sankeyLayout({
                nodes: nodes.map(d => ({ ...d })),
                links: links.map(d => ({ ...d })),
            });
        } catch (e) {
            console.warn('Sankey layout failed:', e);
            return;
        }

        // Links
        const link = g.append('g')
            .attr('fill', 'none')
            .selectAll('g')
            .data(graph.links)
            .join('g');

        link.append('path')
            .attr('d', sankeyLinkHorizontal())
            .attr('stroke', d => d.direction === 'inflow' ? FLOW_COLORS.inflow : FLOW_COLORS.outflow)
            .attr('stroke-width', d => Math.max(2, d.width))
            .attr('stroke-opacity', 0.35)
            .style('cursor', 'pointer')
            .on('mouseenter', function(event, d) {
                d3.select(this).attr('stroke-opacity', 0.7);
                setHoveredLink(d);
            })
            .on('mouseleave', function() {
                d3.select(this).attr('stroke-opacity', 0.35);
                setHoveredLink(null);
            });

        // Nodes
        const node = g.append('g')
            .selectAll('g')
            .data(graph.nodes)
            .join('g')
            .style('cursor', 'pointer')
            .on('click', (event, d) => {
                // Toggle expansion of next level
                const nextLevel = d.level === 'root' ? 'sector'
                    : d.level === 'sector' ? 'subsector'
                    : d.level === 'subsector' ? 'actor' : null;
                if (nextLevel) {
                    setExpandedNodes(prev => {
                        const next = new Set(prev);
                        if (next.has(nextLevel)) next.delete(nextLevel);
                        else next.add(nextLevel);
                        return next;
                    });
                }
            })
            .on('mouseenter', (event, d) => setHoveredNode(d))
            .on('mouseleave', () => setHoveredNode(null));

        node.append('rect')
            .attr('x', d => d.x0)
            .attr('y', d => d.y0)
            .attr('height', d => Math.max(1, d.y1 - d.y0))
            .attr('width', d => d.x1 - d.x0)
            .attr('fill', d => LEVEL_COLORS[d.level] || '#5A7080')
            .attr('rx', 3)
            .attr('opacity', 0.9);

        node.append('text')
            .attr('x', d => d.x0 < innerW / 2 ? d.x1 + 6 : d.x0 - 6)
            .attr('y', d => (d.y1 + d.y0) / 2)
            .attr('dy', '0.35em')
            .attr('text-anchor', d => d.x0 < innerW / 2 ? 'start' : 'end')
            .attr('font-size', d => d.level === 'actor' ? '9px' : d.level === 'subsector' ? '10px' : '11px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', '#C8D8E8')
            .text(d => {
                const name = d.name.includes('/') ? d.name.split('/').pop() : d.name;
                return name.length > 20 ? name.substring(0, 18) + '..' : name;
            });

    }, [data, expandedNodes, dimensions, getFilteredData]);

    const toggleLevel = (level) => {
        setExpandedNodes(prev => {
            const next = new Set(prev);
            if (next.has(level)) next.delete(level);
            else next.add(level);
            return next;
        });
    };

    return (
        <div ref={containerRef} style={{
            background: colors.card, borderRadius: '12px',
            border: `1px solid ${colors.border}`, overflow: 'hidden',
        }}>
            {/* Header */}
            <div style={{
                padding: '12px 16px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
                <div>
                    <div style={{
                        fontSize: '11px', fontWeight: 700, color: colors.accent,
                        letterSpacing: '1.5px', fontFamily: "'JetBrains Mono', monospace",
                    }}>CAPITAL FLOW MAP</div>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>
                        Click nodes to expand · Scroll to zoom · Drag to pan
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '4px' }}>
                    {['sector', 'subsector', 'actor'].map(level => (
                        <button key={level} onClick={() => toggleLevel(level)}
                            style={{
                                background: expandedNodes.has(level) ? `${LEVEL_COLORS[level]}30` : 'transparent',
                                border: `1px solid ${expandedNodes.has(level) ? LEVEL_COLORS[level] : colors.border}`,
                                borderRadius: '4px', padding: '3px 8px', fontSize: '9px',
                                color: expandedNodes.has(level) ? LEVEL_COLORS[level] : colors.textMuted,
                                cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                textTransform: 'capitalize',
                            }}>
                            {level}s
                        </button>
                    ))}
                    <button onClick={loadData} style={{
                        background: colors.accent, border: 'none', borderRadius: '4px',
                        padding: '3px 10px', fontSize: '9px', color: '#fff',
                        cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {loading ? '...' : 'Refresh'}
                    </button>
                </div>
            </div>

            {/* Tooltip */}
            {(hoveredNode || hoveredLink) && (
                <div style={{
                    padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                    background: colors.bg, fontSize: '11px',
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    {hoveredNode && (
                        <span>
                            <span style={{ color: LEVEL_COLORS[hoveredNode.level], fontWeight: 600 }}>
                                {hoveredNode.name}
                            </span>
                            <span style={{ color: colors.textMuted, marginLeft: '8px' }}>
                                {hoveredNode.level} · click to {expandedNodes.has(
                                    hoveredNode.level === 'sector' ? 'subsector' : 'actor'
                                ) ? 'collapse' : 'expand'} detail
                            </span>
                        </span>
                    )}
                    {hoveredLink && (
                        <span>
                            <span style={{
                                color: hoveredLink.direction === 'inflow' ? FLOW_COLORS.inflow : FLOW_COLORS.outflow,
                                fontWeight: 600,
                            }}>
                                {hoveredLink.label || `${hoveredLink.direction}: ${hoveredLink.flow_pct?.toFixed(1)}% vs SPY`}
                            </span>
                            {hoveredLink.ticker && (
                                <span style={{ color: colors.textMuted, marginLeft: '8px' }}>
                                    {hoveredLink.actor_name} ({hoveredLink.ticker})
                                </span>
                            )}
                        </span>
                    )}
                </div>
            )}

            {/* Time slider */}
            {data?.snapshots?.length > 0 && (
                <div style={{
                    padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', alignItems: 'center', gap: '10px',
                }}>
                    <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap' }}>
                        TIME
                    </span>
                    <input
                        type="range"
                        min={0}
                        max={data.snapshots.length - 1}
                        value={data.snapshots.findIndex(s => s.date === selectedSnapshot) || 0}
                        onChange={(e) => {
                            const idx = parseInt(e.target.value);
                            const snap = data.snapshots[idx];
                            if (snap) {
                                setSelectedSnapshot(snap.date);
                                loadData();
                            }
                        }}
                        style={{ flex: 1, accentColor: colors.accent }}
                    />
                    <span style={{ fontSize: '10px', color: colors.text, fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap' }}>
                        {selectedSnapshot || data?.as_of || 'today'}
                    </span>
                </div>
            )}

            {/* ═══ ACTIONABLE SETUPS ═══ */}
            {data?.setups?.length > 0 && (
                <div style={{ padding: '12px 16px', borderBottom: `1px solid ${colors.border}` }}>
                    <div style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', marginBottom: '8px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        ACTIONABLE SETUPS
                        {data.posture && (
                            <span style={{
                                marginLeft: '10px', padding: '2px 6px', borderRadius: '3px', fontSize: '9px',
                                background: data.posture === 'RISK-ON' ? '#22C55E20' : data.posture === 'RISK-OFF' ? '#EF444420' : '#F59E0B20',
                                color: data.posture === 'RISK-ON' ? '#22C55E' : data.posture === 'RISK-OFF' ? '#EF4444' : '#F59E0B',
                            }}>{data.posture}</span>
                        )}
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                        {data.setups.slice(0, 8).map((s, i) => {
                            const actionColors = {
                                BUY: { bg: '#22C55E15', border: '#22C55E40', text: '#22C55E' },
                                WATCH: { bg: '#F59E0B15', border: '#F59E0B40', text: '#F59E0B' },
                                AVOID: { bg: '#EF444415', border: '#EF444440', text: '#EF4444' },
                                OPTIONS: { bg: '#8B5CF615', border: '#8B5CF640', text: '#8B5CF6' },
                            };
                            const ac = actionColors[s.action_type] || actionColors.WATCH;
                            return (
                                <div key={`${s.ticker}-${i}`} style={{
                                    background: ac.bg, border: `1px solid ${ac.border}`,
                                    borderRadius: '8px', padding: '10px 12px',
                                }}>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                            <span style={{ fontSize: '13px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                                                {s.ticker}
                                            </span>
                                            <span style={{
                                                fontSize: '9px', padding: '1px 5px', borderRadius: '3px',
                                                background: ac.text + '20', color: ac.text, fontWeight: 700,
                                                fontFamily: "'JetBrains Mono', monospace",
                                            }}>{s.action_type}</span>
                                            <span style={{ fontSize: '10px', color: colors.textMuted }}>{s.name}</span>
                                        </div>
                                        <span style={{
                                            fontSize: '11px', fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                                            color: s.perf_vs_spy >= 0 ? '#22C55E' : '#EF4444',
                                        }}>
                                            {s.perf_vs_spy >= 0 ? '+' : ''}{s.perf_vs_spy}% vs SPY
                                        </span>
                                    </div>
                                    <div style={{ fontSize: '11px', color: '#C8D8E8', lineHeight: '1.4' }}>
                                        {s.action}
                                    </div>
                                    <div style={{ display: 'flex', gap: '4px', marginTop: '4px', flexWrap: 'wrap' }}>
                                        {s.themes.map(t => (
                                            <span key={t} style={{
                                                fontSize: '8px', padding: '1px 5px', borderRadius: '3px',
                                                background: '#1A284080', color: colors.textMuted,
                                                fontFamily: "'JetBrains Mono', monospace",
                                            }}>{t}</span>
                                        ))}
                                        {s.options && (
                                            <span style={{
                                                fontSize: '8px', padding: '1px 5px', borderRadius: '3px',
                                                background: '#1A284080', color: s.options.pcr > 1.2 ? '#EF4444' : s.options.pcr < 0.7 ? '#22C55E' : colors.textMuted,
                                                fontFamily: "'JetBrains Mono', monospace",
                                            }}>P/C {s.options.pcr?.toFixed(2)}</span>
                                        )}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* SVG Canvas */}
            {loading ? (
                <div style={{ padding: '40px', textAlign: 'center', color: colors.textMuted }}>Loading flow data...</div>
            ) : error ? (
                <div style={{ padding: '16px', color: colors.red, fontSize: '12px' }}>{error}</div>
            ) : (
                <svg
                    ref={svgRef}
                    width={dimensions.width}
                    height={dimensions.height}
                    style={{ background: 'transparent', display: 'block' }}
                />
            )}

            {/* Legend */}
            <div style={{
                padding: '8px 16px', borderTop: `1px solid ${colors.border}`,
                display: 'flex', gap: '16px', fontSize: '9px', color: colors.textMuted,
                fontFamily: "'JetBrains Mono', monospace",
            }}>
                <span><span style={{ color: FLOW_COLORS.inflow }}>---</span> Inflow (outperforming SPY)</span>
                <span><span style={{ color: FLOW_COLORS.outflow }}>---</span> Outflow (underperforming SPY)</span>
                <span style={{ marginLeft: 'auto' }}>
                    SPY 30d: <span style={{ color: data?.spy_30d >= 0 ? FLOW_COLORS.inflow : FLOW_COLORS.outflow }}>
                        {data?.spy_30d >= 0 ? '+' : ''}{data?.spy_30d}%
                    </span>
                </span>
                <span>{data?.node_count} nodes · {data?.link_count} links</span>
            </div>
        </div>
    );
}
