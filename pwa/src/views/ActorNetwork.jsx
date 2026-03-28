/**
 * ActorNetwork -- Full-page D3 force-directed graph of the financial power structure.
 *
 * Shows actors as nodes (sized by influence, colored by tier), connections as
 * links (thickness by strength), wealth flow particles, and pocket-lining alerts.
 *
 * Click a node to show the detail panel. Hover for tooltip. Zoom + pan.
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

// ── Tier colors ──
const TIER_COLORS = {
    sovereign: '#FFD700',
    regional: '#3B82F6',
    institutional: '#8B5CF6',
    individual: '#06B6D4',
};

const TIER_LABELS = {
    sovereign: 'Sovereign',
    regional: 'Regional',
    institutional: 'Institutional',
    individual: 'Individual',
};

const CATEGORY_LABELS = {
    central_bank: 'Central Banks',
    government: 'Politicians',
    fund: 'Funds',
    corporation: 'Corporations',
    insider: 'Insiders',
    politician: 'Politicians',
    activist: 'Activists',
    swf: 'SWFs',
};

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

// ── Styles ──
const S = {
    page: {
        position: 'relative',
        height: 'calc(100vh - 70px)',
        display: 'flex',
        flexDirection: 'column',
        background: colors.bg,
        overflow: 'hidden',
    },
    filterBar: {
        display: 'flex',
        alignItems: 'center',
        gap: '8px',
        padding: '10px 16px',
        borderBottom: `1px solid ${colors.border}`,
        background: colors.card,
        flexWrap: 'wrap',
        zIndex: 10,
    },
    filterGroup: {
        display: 'flex',
        alignItems: 'center',
        gap: '4px',
    },
    filterLabel: {
        fontSize: '9px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.textMuted,
        fontFamily: MONO,
        marginRight: '2px',
    },
    filterBtn: (active) => ({
        background: active ? `${colors.accent}30` : 'transparent',
        border: `1px solid ${active ? colors.accent : colors.border}`,
        borderRadius: '4px',
        padding: '4px 10px',
        fontSize: '10px',
        color: active ? '#E8F0F8' : colors.textMuted,
        cursor: 'pointer',
        fontFamily: MONO,
        transition: 'all 0.15s',
    }),
    searchInput: {
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: '4px',
        padding: '4px 10px',
        fontSize: '11px',
        color: colors.text,
        fontFamily: MONO,
        width: '160px',
        outline: 'none',
        marginLeft: 'auto',
    },
    mainArea: {
        display: 'flex',
        flex: 1,
        position: 'relative',
        overflow: 'hidden',
    },
    graphContainer: {
        flex: 1,
        position: 'relative',
        overflow: 'hidden',
    },
    detailPanel: {
        width: '320px',
        minWidth: '280px',
        background: colors.card,
        borderLeft: `1px solid ${colors.border}`,
        overflowY: 'auto',
        padding: '16px',
        display: 'flex',
        flexDirection: 'column',
        gap: '12px',
        zIndex: 5,
    },
    alertBanner: {
        display: 'flex',
        alignItems: 'center',
        gap: '24px',
        padding: '8px 16px',
        borderTop: `1px solid ${colors.border}`,
        background: '#0A0E14',
        overflow: 'hidden',
        whiteSpace: 'nowrap',
        fontSize: '11px',
        fontFamily: MONO,
        color: colors.textMuted,
        minHeight: '32px',
    },
    badge: (color) => ({
        display: 'inline-block',
        padding: '2px 8px',
        borderRadius: '4px',
        fontSize: '9px',
        fontWeight: 700,
        fontFamily: MONO,
        letterSpacing: '0.5px',
        background: `${color}20`,
        color: color,
        border: `1px solid ${color}40`,
    }),
    metricRow: {
        display: 'flex',
        justifyContent: 'space-between',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        fontSize: '11px',
    },
    sectionTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: MONO,
        marginTop: '8px',
    },
};

function formatMoney(val) {
    if (!val && val !== 0) return '--';
    const abs = Math.abs(val);
    if (abs >= 1e12) return `$${(val / 1e12).toFixed(1)}T`;
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(0)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

// ── Component ──
export default function ActorNetwork() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const simulationRef = useRef(null);
    const tooltipRef = useRef(null);

    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [dimensions, setDimensions] = useState({ width: 900, height: 600 });

    // Filters
    const [tierFilter, setTierFilter] = useState('all');
    const [categoryFilter, setCategoryFilter] = useState('all');
    const [activityFilter, setActivityFilter] = useState('all');
    const [searchQuery, setSearchQuery] = useState('');

    // Selection
    const [selectedNode, setSelectedNode] = useState(null);
    const [actorDetail, setActorDetail] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);

    // ── Load data ──
    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const d = await api.getActorNetwork();
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load actor network');
        }
        setLoading(false);
    };

    // ── Load actor detail on selection ──
    useEffect(() => {
        if (!selectedNode) { setActorDetail(null); return; }
        let cancelled = false;
        setDetailLoading(true);
        api.getActorDetail(selectedNode.id).then(d => {
            if (!cancelled) { setActorDetail(d); setDetailLoading(false); }
        }).catch(() => {
            if (!cancelled) setDetailLoading(false);
        });
        return () => { cancelled = true; };
    }, [selectedNode?.id]);

    // ── Responsive sizing ──
    useEffect(() => {
        if (!containerRef.current) return;
        const observer = new ResizeObserver(entries => {
            for (const entry of entries) {
                const { width, height } = entry.contentRect;
                if (width > 0 && height > 0) setDimensions({ width, height });
            }
        });
        observer.observe(containerRef.current);
        return () => observer.disconnect();
    }, []);

    // ── Filtered data ──
    const filteredData = useMemo(() => {
        if (!data) return { nodes: [], links: [], wealth_flows: [], pocket_lining_alerts: [] };

        const q = searchQuery.toLowerCase().trim();
        let nodes = data.nodes || [];

        if (tierFilter !== 'all') nodes = nodes.filter(n => n.tier === tierFilter);
        if (categoryFilter !== 'all') nodes = nodes.filter(n => n.category === categoryFilter);
        if (activityFilter === 'active') {
            const activeActors = new Set();
            (data.wealth_flows || []).forEach(f => {
                activeActors.add(f.from_actor);
                activeActors.add(f.to_actor);
            });
            nodes = nodes.filter(n => activeActors.has(n.id));
        }
        if (q) {
            nodes = nodes.filter(n =>
                n.label.toLowerCase().includes(q)
                || n.title.toLowerCase().includes(q)
                || n.id.toLowerCase().includes(q)
            );
        }

        const nodeIds = new Set(nodes.map(n => n.id));
        const links = (data.links || []).filter(
            l => nodeIds.has(l.source?.id || l.source) && nodeIds.has(l.target?.id || l.target)
        );

        // Wealth flows between visible nodes
        const wealth_flows = (data.wealth_flows || []).filter(
            f => nodeIds.has(f.from_actor) || nodeIds.has(f.to_actor)
        );

        return {
            nodes: nodes.map(n => ({ ...n })),
            links: links.map(l => ({ ...l })),
            wealth_flows,
            pocket_lining_alerts: data.pocket_lining_alerts || [],
        };
    }, [data, tierFilter, categoryFilter, activityFilter, searchQuery]);

    // ── Build flow lookup for link coloring ──
    const activeFlowPairs = useMemo(() => {
        const pairs = new Set();
        (filteredData.wealth_flows || []).forEach(f => {
            pairs.add(`${f.from_actor}|${f.to_actor}`);
            pairs.add(`${f.to_actor}|${f.from_actor}`);
        });
        return pairs;
    }, [filteredData.wealth_flows]);

    // ── D3 Force Simulation ──
    useEffect(() => {
        if (!svgRef.current || filteredData.nodes.length === 0) return;

        const { width, height } = dimensions;
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const defs = svg.append('defs');

        // Glow filter for active nodes
        const glow = defs.append('filter').attr('id', 'glow');
        glow.append('feGaussianBlur').attr('stdDeviation', '3').attr('result', 'blur');
        glow.append('feMerge')
            .selectAll('feMergeNode')
            .data(['blur', 'SourceGraphic'])
            .enter().append('feMergeNode')
            .attr('in', d => d);

        // Arrow marker for directed links
        defs.append('marker')
            .attr('id', 'arrow-green')
            .attr('viewBox', '0 0 10 10')
            .attr('refX', 20).attr('refY', 5)
            .attr('markerWidth', 6).attr('markerHeight', 6)
            .attr('orient', 'auto')
            .append('path').attr('d', 'M0,0 L10,5 L0,10 Z').attr('fill', '#22C55E80');

        defs.append('marker')
            .attr('id', 'arrow-gray')
            .attr('viewBox', '0 0 10 10')
            .attr('refX', 20).attr('refY', 5)
            .attr('markerWidth', 5).attr('markerHeight', 5)
            .attr('orient', 'auto')
            .append('path').attr('d', 'M0,0 L10,5 L0,10 Z').attr('fill', '#3A4A5A');

        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 6])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        // ── Simulation ──
        const nodes = filteredData.nodes;
        const links = filteredData.links;

        const simulation = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(links)
                .id(d => d.id)
                .distance(d => {
                    const relType = d.relationship || '';
                    if (relType === 'reports_to') return 60;
                    if (relType === 'board_member') return 80;
                    return 120;
                })
                .strength(d => Math.min(1, d.strength || 0.3))
            )
            .force('charge', d3.forceManyBody()
                .strength(d => -100 - d.influence * 200)
            )
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => d.size + 8))
            .force('x', d3.forceX(width / 2).strength(0.03))
            .force('y', d3.forceY(height / 2).strength(0.03))
            .alphaDecay(0.02)
            .velocityDecay(0.4);

        simulationRef.current = simulation;

        // ── Links ──
        const linkG = g.append('g').attr('class', 'links');
        const link = linkG.selectAll('line')
            .data(links)
            .join('line')
            .attr('stroke', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? '#22C55E' : '#2A3A4A';
            })
            .attr('stroke-width', d => Math.max(0.5, (d.strength || 0.3) * 4))
            .attr('stroke-opacity', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? 0.6 : 0.2;
            })
            .attr('marker-end', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? 'url(#arrow-green)' : '';
            });

        // ── Particle layer for wealth flow animation ──
        const particleG = g.append('g').attr('class', 'particles');

        // ── Nodes ──
        const nodeG = g.append('g').attr('class', 'nodes');
        const node = nodeG.selectAll('g')
            .data(nodes)
            .join('g')
            .style('cursor', 'pointer')
            .call(d3.drag()
                .on('start', (event, d) => {
                    if (!event.active) simulation.alphaTarget(0.3).restart();
                    d.fx = d.x;
                    d.fy = d.y;
                })
                .on('drag', (event, d) => {
                    d.fx = event.x;
                    d.fy = event.y;
                })
                .on('end', (event, d) => {
                    if (!event.active) simulation.alphaTarget(0);
                    d.fx = null;
                    d.fy = null;
                })
            );

        // Determine if actor is person or institution
        const isInstitution = (d) => {
            const cat = d.category;
            return cat === 'fund' || cat === 'corporation' || cat === 'swf'
                || cat === 'central_bank';
        };

        // Draw node shapes
        node.each(function(d) {
            const el = d3.select(this);
            const tierColor = TIER_COLORS[d.tier] || '#5A7080';

            if (isInstitution(d)) {
                // Rounded rect for institutions
                const w = d.size * 2;
                const h = d.size * 1.4;
                el.append('rect')
                    .attr('x', -w / 2)
                    .attr('y', -h / 2)
                    .attr('width', w)
                    .attr('height', h)
                    .attr('rx', 5)
                    .attr('fill', `${tierColor}25`)
                    .attr('stroke', tierColor)
                    .attr('stroke-width', 1.5);
            } else {
                // Circle for people
                el.append('circle')
                    .attr('r', d.size)
                    .attr('fill', `${tierColor}25`)
                    .attr('stroke', tierColor)
                    .attr('stroke-width', 1.5);
            }

            // Label
            el.append('text')
                .attr('dy', d.size + 12)
                .attr('text-anchor', 'middle')
                .attr('font-size', d.influence > 0.8 ? '10px' : '8px')
                .attr('font-family', MONO)
                .attr('fill', colors.textDim)
                .attr('pointer-events', 'none')
                .text(() => {
                    const name = d.label;
                    return name.length > 18 ? name.substring(0, 16) + '..' : name;
                });
        });

        // ── Pulse effect for high-influence nodes ──
        node.filter(d => d.influence > 0.85).each(function(d) {
            const el = d3.select(this);
            const tierColor = TIER_COLORS[d.tier] || '#5A7080';
            const pulse = el.insert('circle', ':first-child')
                .attr('r', d.size)
                .attr('fill', 'none')
                .attr('stroke', tierColor)
                .attr('stroke-width', 1);
            (function animatePulse() {
                pulse.attr('r', d.size).attr('opacity', 0.6)
                    .transition().duration(2000).ease(d3.easeSinInOut)
                    .attr('r', d.size * 2.5).attr('opacity', 0)
                    .on('end', animatePulse);
            })();
        });

        // ── Hover tooltip ──
        node.on('mouseenter', function(event, d) {
            d3.select(this).select('circle, rect')
                .attr('filter', 'url(#glow)');
            if (tooltipRef.current) {
                const tip = tooltipRef.current;
                tip.style.display = 'block';
                tip.style.left = `${event.clientX + 12}px`;
                tip.style.top = `${event.clientY - 20}px`;
                const tierColor = TIER_COLORS[d.tier] || '#5A7080';
                tip.innerHTML = `
                    <div style="font-weight:700;color:${tierColor};font-size:12px">${d.label}</div>
                    <div style="color:${colors.textDim};font-size:10px;margin-top:2px">${d.title || ''}</div>
                    <div style="color:${colors.textMuted};font-size:9px;margin-top:4px">
                        Trust: ${(d.trust_score * 100).toFixed(0)}% | Influence: ${(d.influence * 100).toFixed(0)}%
                    </div>
                `;
            }
        }).on('mousemove', function(event) {
            if (tooltipRef.current) {
                tooltipRef.current.style.left = `${event.clientX + 12}px`;
                tooltipRef.current.style.top = `${event.clientY - 20}px`;
            }
        }).on('mouseleave', function() {
            d3.select(this).select('circle, rect')
                .attr('filter', null);
            if (tooltipRef.current) tooltipRef.current.style.display = 'none';
        });

        // ── Click to select ──
        node.on('click', (event, d) => {
            event.stopPropagation();
            setSelectedNode(prev => prev?.id === d.id ? null : d);
        });

        // Click background to deselect
        svg.on('click', () => setSelectedNode(null));

        // ── Highlight selected node connections ──
        const updateHighlights = () => {
            if (!selectedNode) {
                node.attr('opacity', 1);
                link.attr('opacity', d => {
                    const srcId = d.source?.id || d.source;
                    const tgtId = d.target?.id || d.target;
                    return activeFlowPairs.has(`${srcId}|${tgtId}`) ? 0.6 : 0.2;
                });
                return;
            }
            const connectedIds = new Set([selectedNode.id]);
            links.forEach(l => {
                const sid = l.source?.id || l.source;
                const tid = l.target?.id || l.target;
                if (sid === selectedNode.id) connectedIds.add(tid);
                if (tid === selectedNode.id) connectedIds.add(sid);
            });
            node.attr('opacity', d => connectedIds.has(d.id) ? 1 : 0.15);
            link.attr('opacity', d => {
                const sid = d.source?.id || d.source;
                const tid = d.target?.id || d.target;
                return (sid === selectedNode.id || tid === selectedNode.id) ? 0.8 : 0.05;
            });
        };

        // ── Wealth flow particles ──
        const flowData = filteredData.wealth_flows.slice(0, 60);
        const linkIndex = {};
        links.forEach(l => {
            const sid = l.source?.id || l.source;
            const tid = l.target?.id || l.target;
            linkIndex[`${sid}|${tid}`] = l;
            linkIndex[`${tid}|${sid}`] = l;
        });

        let particleTimer;
        if (flowData.length > 0) {
            particleTimer = d3.interval(() => {
                const flow = flowData[Math.floor(Math.random() * flowData.length)];
                const l = linkIndex[`${flow.from_actor}|${flow.to_actor}`];
                if (!l || !l.source || !l.target) return;

                const sx = l.source.x || 0, sy = l.source.y || 0;
                const tx = l.target.x || 0, ty = l.target.y || 0;

                const particle = particleG.append('circle')
                    .attr('cx', sx).attr('cy', sy)
                    .attr('r', Math.min(4, 1 + (flow.amount || 0) / 1e9))
                    .attr('fill', flow.amount >= 0 ? '#22C55E' : '#EF4444')
                    .attr('opacity', 0.8);

                particle.transition()
                    .duration(1500 + Math.random() * 1000)
                    .ease(d3.easeLinear)
                    .attr('cx', tx).attr('cy', ty)
                    .attr('opacity', 0)
                    .remove();
            }, 200);
        }

        // ── Tick ──
        simulation.on('tick', () => {
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            node.attr('transform', d => `translate(${d.x},${d.y})`);

            updateHighlights();
        });

        return () => {
            simulation.stop();
            if (particleTimer) particleTimer.stop();
        };
    }, [filteredData.nodes.length, filteredData.links.length, dimensions, activeFlowPairs]);

    // Update highlights when selection changes without full re-render
    useEffect(() => {
        if (!svgRef.current || !data) return;
        const svg = d3.select(svgRef.current);
        const nodes = svg.selectAll('.nodes g');
        const links = svg.selectAll('.links line');

        if (!selectedNode) {
            nodes.attr('opacity', 1);
            links.attr('stroke-opacity', null);
            return;
        }

        const connectedIds = new Set([selectedNode.id]);
        (data.links || []).forEach(l => {
            const sid = typeof l.source === 'object' ? l.source.id : l.source;
            const tid = typeof l.target === 'object' ? l.target.id : l.target;
            if (sid === selectedNode.id) connectedIds.add(tid);
            if (tid === selectedNode.id) connectedIds.add(sid);
        });

        nodes.attr('opacity', function(d) { return connectedIds.has(d.id) ? 1 : 0.15; });
        links.attr('stroke-opacity', function(d) {
            const sid = d.source?.id || d.source;
            const tid = d.target?.id || d.target;
            return (sid === selectedNode.id || tid === selectedNode.id) ? 0.8 : 0.03;
        });
    }, [selectedNode?.id]);

    // ── Unique categories in data ──
    const categories = useMemo(() => {
        if (!data?.nodes) return [];
        const cats = [...new Set(data.nodes.map(n => n.category))];
        return cats.sort();
    }, [data?.nodes]);

    // ── Alert banner scrolling text ──
    const alertText = useMemo(() => {
        const alerts = data?.pocket_lining_alerts || [];
        if (alerts.length === 0) return 'No pocket-lining alerts detected.';
        return alerts.map(a => a.implication || a.what || JSON.stringify(a)).join('     ///     ');
    }, [data?.pocket_lining_alerts]);

    // ── Render ──
    if (loading) {
        return (
            <div style={{ ...S.page, justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ color: colors.textMuted, fontFamily: MONO, fontSize: '13px' }}>
                    Loading actor network...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ ...S.page, justifyContent: 'center', alignItems: 'center' }}>
                <div style={{ color: colors.red, fontFamily: MONO, fontSize: '13px' }}>{error}</div>
                <button onClick={loadData} style={{ ...shared.buttonSmall, marginTop: '12px' }}>Retry</button>
            </div>
        );
    }

    return (
        <div style={S.page}>
            {/* ── Tooltip (portal-like, fixed position) ── */}
            <div
                ref={tooltipRef}
                style={{
                    display: 'none',
                    position: 'fixed',
                    zIndex: 100,
                    background: colors.card,
                    border: `1px solid ${colors.border}`,
                    borderRadius: '6px',
                    padding: '8px 12px',
                    fontFamily: MONO,
                    pointerEvents: 'none',
                    boxShadow: colors.shadow.md,
                    maxWidth: '260px',
                }}
            />

            {/* ── Filter bar ── */}
            <div style={S.filterBar}>
                {/* Tier filter */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>TIER</span>
                    {['all', 'sovereign', 'regional', 'institutional', 'individual'].map(t => (
                        <button key={t} onClick={() => setTierFilter(t)} style={{
                            ...S.filterBtn(tierFilter === t),
                            ...(t !== 'all' && tierFilter === t ? { borderColor: TIER_COLORS[t], color: TIER_COLORS[t] } : {}),
                        }}>
                            {t === 'all' ? 'All' : TIER_LABELS[t]}
                        </button>
                    ))}
                </div>

                <div style={{ width: '1px', height: '20px', background: colors.border }} />

                {/* Category filter */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>TYPE</span>
                    <button onClick={() => setCategoryFilter('all')} style={S.filterBtn(categoryFilter === 'all')}>All</button>
                    {categories.map(c => (
                        <button key={c} onClick={() => setCategoryFilter(c)} style={S.filterBtn(categoryFilter === c)}>
                            {CATEGORY_LABELS[c] || c}
                        </button>
                    ))}
                </div>

                <div style={{ width: '1px', height: '20px', background: colors.border }} />

                {/* Activity filter */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>ACTIVITY</span>
                    <button onClick={() => setActivityFilter('all')} style={S.filterBtn(activityFilter === 'all')}>All</button>
                    <button onClick={() => setActivityFilter('active')} style={S.filterBtn(activityFilter === 'active')}>Active</button>
                </div>

                {/* Search */}
                <input
                    type="text"
                    placeholder="Search actors..."
                    value={searchQuery}
                    onChange={e => setSearchQuery(e.target.value)}
                    style={S.searchInput}
                />

                {/* Stats */}
                <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, whiteSpace: 'nowrap' }}>
                    {filteredData.nodes.length} nodes / {filteredData.links.length} links
                </div>
            </div>

            {/* ── Main area: graph + detail panel ── */}
            <div style={S.mainArea}>
                {/* Graph */}
                <div ref={containerRef} style={S.graphContainer}>
                    <svg
                        ref={svgRef}
                        width={dimensions.width}
                        height={dimensions.height}
                        style={{ display: 'block', background: 'transparent' }}
                    />

                    {/* Legend overlay */}
                    <div style={{
                        position: 'absolute', bottom: '12px', left: '12px',
                        background: `${colors.card}DD`,
                        border: `1px solid ${colors.border}`,
                        borderRadius: '8px',
                        padding: '10px 14px',
                        fontFamily: MONO,
                        fontSize: '9px',
                        display: 'flex',
                        flexDirection: 'column',
                        gap: '4px',
                    }}>
                        {Object.entries(TIER_COLORS).map(([tier, color]) => (
                            <div key={tier} style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: `${color}40`, border: `1.5px solid ${color}` }} />
                                <span style={{ color: colors.textDim }}>{TIER_LABELS[tier]}</span>
                            </div>
                        ))}
                        <div style={{ marginTop: '4px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '4px' }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '16px', height: '2px', background: '#22C55E' }} />
                                <span style={{ color: colors.textDim }}>Active flow</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '16px', height: '1px', background: '#2A3A4A' }} />
                                <span style={{ color: colors.textDim }}>Dormant link</span>
                            </div>
                        </div>
                    </div>
                </div>

                {/* ── Detail panel (right sidebar) ── */}
                {selectedNode && (
                    <div style={S.detailPanel}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                            <div>
                                <div style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: SANS }}>
                                    {selectedNode.label}
                                </div>
                                <div style={{ fontSize: '11px', color: colors.textDim, marginTop: '2px' }}>
                                    {selectedNode.title}
                                </div>
                            </div>
                            <span style={S.badge(TIER_COLORS[selectedNode.tier] || '#5A7080')}>
                                {(selectedNode.tier || '').toUpperCase()}
                            </span>
                        </div>

                        {/* Key metrics */}
                        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                            <div style={{ ...shared.metric, flex: 1, minWidth: '80px' }}>
                                <div style={{ ...shared.metricValue, fontSize: '15px' }}>
                                    {(selectedNode.influence * 100).toFixed(0)}%
                                </div>
                                <div style={shared.metricLabel}>Influence</div>
                            </div>
                            <div style={{ ...shared.metric, flex: 1, minWidth: '80px' }}>
                                <div style={{ ...shared.metricValue, fontSize: '15px' }}>
                                    {(selectedNode.trust_score * 100).toFixed(0)}%
                                </div>
                                <div style={shared.metricLabel}>Trust</div>
                            </div>
                        </div>

                        {/* Trust score bar */}
                        <div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '4px' }}>
                                <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>TRUST SCORE</span>
                                <span style={{ fontSize: '10px', color: colors.text, fontFamily: MONO }}>
                                    {(selectedNode.trust_score * 100).toFixed(0)}%
                                </span>
                            </div>
                            <div style={{ height: '4px', background: colors.bg, borderRadius: '2px', overflow: 'hidden' }}>
                                <div style={{
                                    height: '100%',
                                    width: `${selectedNode.trust_score * 100}%`,
                                    background: selectedNode.trust_score > 0.7 ? colors.green
                                        : selectedNode.trust_score > 0.4 ? colors.yellow : colors.red,
                                    borderRadius: '2px',
                                    transition: 'width 0.3s ease',
                                }} />
                            </div>
                        </div>

                        {/* Net worth / AUM */}
                        {(selectedNode.net_worth || selectedNode.aum) && (
                            <div>
                                {selectedNode.net_worth && (
                                    <div style={S.metricRow}>
                                        <span style={{ color: colors.textMuted }}>Net Worth</span>
                                        <span style={{ color: colors.text, fontFamily: MONO, fontWeight: 600 }}>
                                            {formatMoney(selectedNode.net_worth)}
                                            <span style={{ fontSize: '8px', color: colors.textMuted, marginLeft: '4px' }}>
                                                {selectedNode.credibility}
                                            </span>
                                        </span>
                                    </div>
                                )}
                                {selectedNode.aum && (
                                    <div style={S.metricRow}>
                                        <span style={{ color: colors.textMuted }}>AUM</span>
                                        <span style={{ color: colors.text, fontFamily: MONO, fontWeight: 600 }}>
                                            {formatMoney(selectedNode.aum)}
                                        </span>
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Connections */}
                        {actorDetail?.connections?.length > 0 && (
                            <div>
                                <div style={S.sectionTitle}>CONNECTIONS ({actorDetail.connections.length})</div>
                                <div style={{ maxHeight: '140px', overflowY: 'auto', marginTop: '6px' }}>
                                    {actorDetail.connections.map((c, i) => {
                                        const linkedNode = (data?.nodes || []).find(n => n.id === c.actor_id);
                                        return (
                                            <div key={i} style={{
                                                ...S.metricRow,
                                                cursor: 'pointer',
                                                padding: '4px 0',
                                            }} onClick={() => {
                                                const target = filteredData.nodes.find(n => n.id === c.actor_id);
                                                if (target) setSelectedNode(target);
                                            }}>
                                                <span style={{ color: colors.textDim, fontSize: '10px' }}>
                                                    {linkedNode?.label || c.actor_id}
                                                </span>
                                                <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO }}>
                                                    {c.relationship} ({(c.strength * 100).toFixed(0)}%)
                                                </span>
                                            </div>
                                        );
                                    })}
                                </div>
                            </div>
                        )}

                        {/* Connected actions */}
                        {actorDetail?.connected_actions?.length > 0 && (
                            <div>
                                <div style={S.sectionTitle}>RECENT COORDINATED ACTIONS</div>
                                <div style={{ maxHeight: '140px', overflowY: 'auto', marginTop: '6px' }}>
                                    {actorDetail.connected_actions.slice(0, 8).map((a, i) => (
                                        <div key={i} style={{
                                            background: colors.bg,
                                            borderRadius: '6px',
                                            padding: '8px',
                                            marginBottom: '4px',
                                            fontSize: '10px',
                                        }}>
                                            <div style={{ display: 'flex', justifyContent: 'space-between' }}>
                                                <span style={{ color: colors.text, fontWeight: 600, fontFamily: MONO }}>
                                                    {a.ticker}
                                                </span>
                                                <span style={{
                                                    color: a.alignment === 'aligned' ? colors.green : colors.yellow,
                                                    fontFamily: MONO,
                                                    fontSize: '9px',
                                                }}>
                                                    {a.alignment} ({a.total_actors} actors)
                                                </span>
                                            </div>
                                            <div style={{ color: colors.textMuted, marginTop: '2px', fontSize: '9px' }}>
                                                Direction: {a.dominant_direction} | Conviction: {a.conviction}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Pocket-lining alerts */}
                        {actorDetail?.pocket_lining_alerts?.length > 0 && (
                            <div>
                                <div style={{ ...S.sectionTitle, color: colors.red }}>POCKET-LINING ALERTS</div>
                                <div style={{ maxHeight: '140px', overflowY: 'auto', marginTop: '6px' }}>
                                    {actorDetail.pocket_lining_alerts.map((a, i) => (
                                        <div key={i} style={{
                                            background: `${colors.red}10`,
                                            border: `1px solid ${colors.red}30`,
                                            borderRadius: '6px',
                                            padding: '8px',
                                            marginBottom: '4px',
                                            fontSize: '10px',
                                            color: colors.text,
                                        }}>
                                            <div style={{ fontWeight: 600, color: colors.red, marginBottom: '2px' }}>
                                                {a.detection || 'Suspicious Pattern'}
                                            </div>
                                            <div style={{ color: colors.textDim, lineHeight: 1.4 }}>
                                                {a.implication}
                                            </div>
                                        </div>
                                    ))}
                                </div>
                            </div>
                        )}

                        {/* Motivation / interpretation */}
                        <div>
                            <div style={S.sectionTitle}>INTERPRETATION</div>
                            <div style={{
                                fontSize: '11px',
                                color: colors.textDim,
                                lineHeight: 1.5,
                                marginTop: '4px',
                                padding: '8px',
                                background: colors.bg,
                                borderRadius: '6px',
                            }}>
                                {selectedNode.motivation === 'institutional_mandate'
                                    ? `${selectedNode.label} operates under institutional mandate. Actions reflect policy objectives, not personal positioning. Track official statements vs actual policy moves.`
                                    : selectedNode.motivation === 'profit_maximizing'
                                    ? `${selectedNode.label} is a profit-maximizing actor. Disclosed positions indicate directional conviction. Watch for position sizing changes and timing relative to catalysts.`
                                    : selectedNode.motivation === 'informed'
                                    ? `${selectedNode.label} has privileged access to information through their role. Trading activity may signal material non-public knowledge. Monitor filing patterns.`
                                    : `${selectedNode.label} -- motivation model unknown. Track behavior pattern for classification.`
                                }
                            </div>
                        </div>

                        {detailLoading && (
                            <div style={{ textAlign: 'center', padding: '12px', color: colors.textMuted, fontSize: '10px', fontFamily: MONO }}>
                                Loading details...
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* ── Alert banner (bottom) ── */}
            <div style={S.alertBanner}>
                <span style={{
                    color: colors.red,
                    fontWeight: 700,
                    fontSize: '9px',
                    letterSpacing: '1.5px',
                    flexShrink: 0,
                }}>ALERTS</span>
                <div style={{ overflow: 'hidden', flex: 1 }}>
                    <div style={{
                        display: 'inline-block',
                        whiteSpace: 'nowrap',
                        animation: 'scrollBanner 60s linear infinite',
                        paddingLeft: '100%',
                    }}>
                        {alertText}
                    </div>
                </div>
                <style>{`
                    @keyframes scrollBanner {
                        0% { transform: translateX(0); }
                        100% { transform: translateX(-100%); }
                    }
                `}</style>
            </div>
        </div>
    );
}
