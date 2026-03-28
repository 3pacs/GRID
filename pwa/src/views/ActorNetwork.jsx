/**
 * ActorNetwork -- Full-page D3 force-directed graph of the financial power structure
 * with ANIMATED MONEY FLOWS between actors.
 *
 * Shows:
 *   - Green particles = money flowing (contributions, contracts, investments)
 *   - Red particles = money leaving (stock sales, withdrawals)
 *   - Gold particles = influence (votes, policy, regulation)
 *   - Particle speed proportional to recency
 *   - Particle size proportional to dollar amount (log scale)
 *   - Flow timeline slider (30/60/90 days)
 *   - Flow labels on links (dollar amounts)
 *   - Flow aggregation panel (inflows/outflows per actor)
 *   - Circular flow highlighting from influence_network
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';

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

// Flow type → particle color
const FLOW_COLORS = {
    campaign: '#22C55E',
    contribution: '#22C55E',
    contract: '#22C55E',
    lobbying: '#22C55E',
    investment: '#22C55E',
    stock_trade: '#EF4444',
    stock_sale: '#EF4444',
    sell: '#EF4444',
    outflow: '#EF4444',
    influence: '#FFD700',
    vote: '#FFD700',
    policy: '#FFD700',
    regulation: '#FFD700',
};

const FLOW_TYPE_CATEGORY = {
    campaign: 'money',
    contribution: 'money',
    contract: 'money',
    lobbying: 'money',
    investment: 'money',
    stock_trade: 'outflow',
    stock_sale: 'outflow',
    sell: 'outflow',
    outflow: 'outflow',
    influence: 'influence',
    vote: 'influence',
    policy: 'influence',
    regulation: 'influence',
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
        width: '340px',
        minWidth: '300px',
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
        padding: '4px 8px',
        borderRadius: '999px',
        fontSize: '9px',
        fontWeight: 700,
        fontFamily: MONO,
        letterSpacing: '0.5px',
        background: `${color}20`,
        color: color,
        border: `1px solid ${color}40`,
        whiteSpace: 'nowrap',
        minWidth: '32px',
        textAlign: 'center',
    }),
    metricRow: {
        display: 'flex',
        justifyContent: 'space-between',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        fontSize: '11px',
        gap: '8px',
        alignItems: 'center',
    },
    sectionTitle: {
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1.5px',
        color: colors.accent,
        fontFamily: MONO,
        marginTop: '8px',
    },
    timelineBar: {
        display: 'flex',
        alignItems: 'center',
        gap: '10px',
        padding: '8px 16px',
        borderTop: `1px solid ${colors.border}`,
        background: colors.card,
        zIndex: 10,
    },
    circularBanner: {
        display: 'flex',
        alignItems: 'center',
        gap: '12px',
        padding: '6px 16px',
        background: '#FFD70015',
        borderBottom: `1px solid #FFD70040`,
        fontSize: '11px',
        fontFamily: MONO,
        color: '#FFD700',
        overflowX: 'auto',
        overflowY: 'hidden',
        whiteSpace: 'nowrap',
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

function getFlowColor(type) {
    return FLOW_COLORS[type] || '#22C55E';
}

function getParticleSize(amount) {
    if (!amount || amount <= 0) return 2;
    // Log scale: $1K=2px, $1M=4px, $1B=6px, $1T=8px
    return Math.max(2, Math.min(8, 1.5 + Math.log10(Math.max(amount, 1000)) * 0.7));
}

function getParticleSpeed(dateStr) {
    if (!dateStr) return 2000;
    const now = Date.now();
    const flowDate = new Date(dateStr).getTime();
    const daysAgo = (now - flowDate) / (1000 * 60 * 60 * 24);
    // Recent = fast (800ms), old = slow (3000ms)
    return Math.max(800, Math.min(3000, 800 + daysAgo * 25));
}

function parseFlowDate(dateStr) {
    if (!dateStr) return null;
    const d = new Date(dateStr);
    return isNaN(d.getTime()) ? null : d;
}

// ── Component ──
export default function ActorNetwork() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const simulationRef = useRef(null);
    const tooltipRef = useRef(null);
    const fullScreenRef = useRef(null);
    const zoomRef = useRef(null);
    const miniMapRef = useRef(null);
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

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
    const [selectedFlow, setSelectedFlow] = useState(null);
    const [actorDetail, setActorDetail] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);

    // Flow controls
    const [timelineDays, setTimelineDays] = useState(90);
    const [isPlaying, setIsPlaying] = useState(true);
    const [showFlowLabels, setShowFlowLabels] = useState(false);
    const [showPanel, setShowPanel] = useState('detail'); // 'detail' | 'flows'
    const [highlightedLoop, setHighlightedLoop] = useState(null);

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

    // ── Filter flows by timeline ──
    const timelineFilteredFlows = useMemo(() => {
        const allFlows = data?.flows || [];
        if (!allFlows.length) return [];
        const cutoff = new Date();
        cutoff.setDate(cutoff.getDate() - timelineDays);
        return allFlows.filter(f => {
            const d = parseFlowDate(f.date);
            return !d || d >= cutoff;
        });
    }, [data?.flows, timelineDays]);

    // ── Filtered data ──
    const filteredData = useMemo(() => {
        if (!data) return { nodes: [], links: [], wealth_flows: [], pocket_lining_alerts: [], flows: [], circular_flows: [] };

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
            timelineFilteredFlows.forEach(f => {
                activeActors.add(f.from);
                activeActors.add(f.to);
            });
            nodes = nodes.filter(n => activeActors.has(n.id));
        }
        if (q) {
            nodes = nodes.filter(n =>
                n.label.toLowerCase().includes(q)
                || (n.title || '').toLowerCase().includes(q)
                || n.id.toLowerCase().includes(q)
            );
        }

        const nodeIds = new Set(nodes.map(n => n.id));
        const links = (data.links || []).filter(
            l => nodeIds.has(l.source?.id || l.source) && nodeIds.has(l.target?.id || l.target)
        );

        const wealth_flows = (data.wealth_flows || []).filter(
            f => nodeIds.has(f.from_actor) || nodeIds.has(f.to_actor)
        );

        return {
            nodes: nodes.map(n => ({ ...n })),
            links: links.map(l => ({ ...l })),
            wealth_flows,
            pocket_lining_alerts: data.pocket_lining_alerts || [],
            flows: timelineFilteredFlows,
            circular_flows: data.circular_flows || [],
        };
    }, [data, tierFilter, categoryFilter, activityFilter, searchQuery, timelineFilteredFlows]);

    // ── Build flow lookup for link coloring ──
    const activeFlowPairs = useMemo(() => {
        const pairs = new Set();
        (filteredData.wealth_flows || []).forEach(f => {
            pairs.add(`${f.from_actor}|${f.to_actor}`);
            pairs.add(`${f.to_actor}|${f.from_actor}`);
        });
        (filteredData.flows || []).forEach(f => {
            pairs.add(`${f.from}|${f.to}`);
            pairs.add(`${f.to}|${f.from}`);
        });
        return pairs;
    }, [filteredData.wealth_flows, filteredData.flows]);

    // ── Flow aggregation per actor ──
    const flowAggregation = useMemo(() => {
        const actors = {};
        (filteredData.flows || []).forEach(f => {
            const amt = Math.abs(f.amount || 0);
            // Outflow from sender
            if (f.from) {
                if (!actors[f.from]) actors[f.from] = { inflow: 0, outflow: 0, flows: [] };
                actors[f.from].outflow += amt;
                actors[f.from].flows.push(f);
            }
            // Inflow to receiver
            if (f.to) {
                if (!actors[f.to]) actors[f.to] = { inflow: 0, outflow: 0, flows: [] };
                actors[f.to].inflow += amt;
                actors[f.to].flows.push(f);
            }
        });

        const sorted = Object.entries(actors)
            .map(([id, agg]) => ({
                id,
                label: (data?.nodes || []).find(n => n.id === id)?.label || id,
                ...agg,
                net: agg.inflow - agg.outflow,
            }))
            .sort((a, b) => Math.abs(b.net) - Math.abs(a.net));

        const topFlows = [...(filteredData.flows || [])]
            .sort((a, b) => Math.abs(b.amount || 0) - Math.abs(a.amount || 0))
            .slice(0, 10);

        return { actors: sorted.slice(0, 20), topFlows };
    }, [filteredData.flows, data?.nodes]);

    // ── Circular flow nodes for highlighting ──
    const circularLoopNodes = useMemo(() => {
        if (!highlightedLoop) return new Set();
        const nodes = new Set();
        // Add company node
        if (highlightedLoop.ticker) nodes.add(`company:${highlightedLoop.ticker}`);
        // Add recipient members
        (highlightedLoop.recipients || []).forEach(r => {
            if (r.member) nodes.add(`member:${r.member.toLowerCase().replace(/ /g, '_')}`);
        });
        // Add trading members
        (highlightedLoop.member_trades || []).forEach(t => {
            if (t.member) nodes.add(`member:${t.member.toLowerCase().replace(/ /g, '_')}`);
        });
        return nodes;
    }, [highlightedLoop]);

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

        // Brighter glow for circular flow highlighting
        const loopGlow = defs.append('filter').attr('id', 'loopGlow');
        loopGlow.append('feGaussianBlur').attr('stdDeviation', '6').attr('result', 'blur');
        loopGlow.append('feMerge')
            .selectAll('feMergeNode')
            .data(['blur', 'SourceGraphic'])
            .enter().append('feMergeNode')
            .attr('in', d => d);

        // Arrow markers for each flow type
        ['green', 'red', 'gold', 'gray'].forEach(color => {
            const fill = color === 'green' ? '#22C55E80' : color === 'red' ? '#EF444480' : color === 'gold' ? '#FFD70080' : '#3A4A5A';
            defs.append('marker')
                .attr('id', `arrow-${color}`)
                .attr('viewBox', '0 0 10 10')
                .attr('refX', 20).attr('refY', 5)
                .attr('markerWidth', 6).attr('markerHeight', 6)
                .attr('orient', 'auto')
                .append('path').attr('d', 'M0,0 L10,5 L0,10 Z').attr('fill', fill);
        });

        const g = svg.append('g');

        // Zoom
        const zoom = d3.zoom()
            .scaleExtent([0.1, 6])
            .on('zoom', (event) => {
                g.attr('transform', event.transform);
                // Update mini-map viewport indicator
                if (miniMapRef.current) {
                    const t = event.transform;
                    const mmW = 150, mmH = 100;
                    const scaleX = mmW / dimensions.width;
                    const scaleY = mmH / dimensions.height;
                    const vx = (-t.x / t.k) * scaleX;
                    const vy = (-t.y / t.k) * scaleY;
                    const vw = (dimensions.width / t.k) * scaleX;
                    const vh = (dimensions.height / t.k) * scaleY;
                    const vp = miniMapRef.current.querySelector('.mm-viewport');
                    if (vp) {
                        vp.setAttribute('x', vx);
                        vp.setAttribute('y', vy);
                        vp.setAttribute('width', Math.min(vw, mmW));
                        vp.setAttribute('height', Math.min(vh, mmH));
                    }
                }
            });
        svg.call(zoom);
        zoomRef.current = { zoom, svg };

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
                if (circularLoopNodes.size > 0 && circularLoopNodes.has(srcId) && circularLoopNodes.has(tgtId)) {
                    return '#FFD700';
                }
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? '#22C55E' : '#2A3A4A';
            })
            .attr('stroke-width', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                if (circularLoopNodes.size > 0 && circularLoopNodes.has(srcId) && circularLoopNodes.has(tgtId)) {
                    return 3;
                }
                return Math.max(0.5, (d.strength || 0.3) * 4);
            })
            .attr('stroke-opacity', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                if (circularLoopNodes.size > 0 && circularLoopNodes.has(srcId) && circularLoopNodes.has(tgtId)) {
                    return 0.9;
                }
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? 0.6 : 0.2;
            })
            .attr('marker-end', d => {
                const srcId = d.source?.id || d.source;
                const tgtId = d.target?.id || d.target;
                if (circularLoopNodes.size > 0 && circularLoopNodes.has(srcId) && circularLoopNodes.has(tgtId)) {
                    return 'url(#arrow-gold)';
                }
                return activeFlowPairs.has(`${srcId}|${tgtId}`) ? 'url(#arrow-green)' : '';
            });

        // ── Flow labels on links ──
        const flowLabelG = g.append('g').attr('class', 'flow-labels');
        if (showFlowLabels) {
            const flowsForLabels = filteredData.flows.slice(0, 100);
            const linkIndex = {};
            links.forEach(l => {
                const sid = l.source?.id || l.source;
                const tid = l.target?.id || l.target;
                linkIndex[`${sid}|${tid}`] = l;
                linkIndex[`${tid}|${sid}`] = l;
            });

            flowsForLabels.forEach(f => {
                const l = linkIndex[`${f.from}|${f.to}`];
                if (!l) return;
                flowLabelG.append('text')
                    .attr('class', 'flow-label-text')
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '8px')
                    .attr('font-family', MONO)
                    .attr('fill', '#8899AA')
                    .attr('pointer-events', 'none')
                    .attr('dy', -6)
                    .text(f.label || formatMoney(f.amount))
                    .datum({ link: l, flow: f });
            });
        }

        // ── Particle layer for animated money flow ──
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

        const isInstitution = (d) => {
            const cat = d.category;
            return cat === 'fund' || cat === 'corporation' || cat === 'swf'
                || cat === 'central_bank';
        };

        // Draw node shapes
        node.each(function(d) {
            const el = d3.select(this);
            const tierColor = TIER_COLORS[d.tier] || '#5A7080';
            const isInLoop = circularLoopNodes.has(d.id);

            if (isInstitution(d)) {
                const w = d.size * 2;
                const h = d.size * 1.4;
                el.append('rect')
                    .attr('x', -w / 2)
                    .attr('y', -h / 2)
                    .attr('width', w)
                    .attr('height', h)
                    .attr('rx', 5)
                    .attr('fill', isInLoop ? '#FFD70025' : `${tierColor}25`)
                    .attr('stroke', isInLoop ? '#FFD700' : tierColor)
                    .attr('stroke-width', isInLoop ? 2.5 : 1.5);
            } else {
                el.append('circle')
                    .attr('r', d.size)
                    .attr('fill', isInLoop ? '#FFD70025' : `${tierColor}25`)
                    .attr('stroke', isInLoop ? '#FFD700' : tierColor)
                    .attr('stroke-width', isInLoop ? 2.5 : 1.5);
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

        // ── Pulse effect for high-influence nodes and circular loop nodes ──
        node.filter(d => d.influence > 0.85 || circularLoopNodes.has(d.id)).each(function(d) {
            const el = d3.select(this);
            const isInLoop = circularLoopNodes.has(d.id);
            const pulseColor = isInLoop ? '#FFD700' : (TIER_COLORS[d.tier] || '#5A7080');
            const pulse = el.insert('circle', ':first-child')
                .attr('r', d.size)
                .attr('fill', 'none')
                .attr('stroke', pulseColor)
                .attr('stroke-width', isInLoop ? 2 : 1);
            (function animatePulse() {
                pulse.attr('r', d.size).attr('opacity', isInLoop ? 0.8 : 0.6)
                    .transition().duration(isInLoop ? 1200 : 2000).ease(d3.easeSinInOut)
                    .attr('r', d.size * (isInLoop ? 3 : 2.5)).attr('opacity', 0)
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

                // Compute flow summary for this node
                const nodeAgg = flowAggregation.actors.find(a => a.id === d.id);
                const flowInfo = nodeAgg
                    ? `<div style="margin-top:4px;font-size:9px;color:#8899AA">` +
                      `In: <span style="color:#22C55E">${formatMoney(nodeAgg.inflow)}</span> | ` +
                      `Out: <span style="color:#EF4444">${formatMoney(nodeAgg.outflow)}</span> | ` +
                      `Net: <span style="color:${nodeAgg.net >= 0 ? '#22C55E' : '#EF4444'}">${formatMoney(nodeAgg.net)}</span>` +
                      `</div>`
                    : '';

                tip.innerHTML = `
                    <div style="font-weight:700;color:${tierColor};font-size:12px">${d.label}</div>
                    <div style="color:${colors.textDim};font-size:10px;margin-top:2px">${d.title || ''}</div>
                    <div style="color:${colors.textMuted};font-size:9px;margin-top:4px">
                        Trust: ${(d.trust_score * 100).toFixed(0)}% | Influence: ${(d.influence * 100).toFixed(0)}%
                    </div>
                    ${flowInfo}
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
            setShowPanel('detail');
        });

        // Click background to deselect
        svg.on('click', () => { setSelectedNode(null); setSelectedFlow(null); });

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

        // ── Animated money flow particles ──
        const flowData = filteredData.flows.slice(0, 100);
        const wealthFlowData = filteredData.wealth_flows.slice(0, 60);
        const linkIndex = {};
        links.forEach(l => {
            const sid = l.source?.id || l.source;
            const tid = l.target?.id || l.target;
            linkIndex[`${sid}|${tid}`] = l;
            linkIndex[`${tid}|${sid}`] = l;
        });

        let particleTimer;
        if (isPlaying && (flowData.length > 0 || wealthFlowData.length > 0)) {
            particleTimer = d3.interval(() => {
                // Randomly pick from money flows or wealth flows
                const useMoneyFlow = flowData.length > 0 && (wealthFlowData.length === 0 || Math.random() < 0.7);

                if (useMoneyFlow && flowData.length > 0) {
                    const flow = flowData[Math.floor(Math.random() * flowData.length)];
                    const l = linkIndex[`${flow.from}|${flow.to}`] || linkIndex[`${flow.to}|${flow.from}`];
                    if (!l || !l.source || !l.target) return;

                    // Determine direction: from→to on the link
                    const fromId = flow.from;
                    const srcIsFrom = (l.source.id || l.source) === fromId;
                    const sx = srcIsFrom ? (l.source.x || 0) : (l.target.x || 0);
                    const sy = srcIsFrom ? (l.source.y || 0) : (l.target.y || 0);
                    const tx = srcIsFrom ? (l.target.x || 0) : (l.source.x || 0);
                    const ty = srcIsFrom ? (l.target.y || 0) : (l.source.y || 0);

                    const color = getFlowColor(flow.type);
                    const size = getParticleSize(flow.amount);
                    const speed = getParticleSpeed(flow.date);

                    const particle = particleG.append('circle')
                        .attr('cx', sx).attr('cy', sy)
                        .attr('r', size)
                        .attr('fill', color)
                        .attr('opacity', 0.85)
                        .style('cursor', 'pointer')
                        .style('filter', `drop-shadow(0 0 ${size}px ${color})`);

                    // Click particle to show flow details
                    particle.on('click', (event) => {
                        event.stopPropagation();
                        setSelectedFlow(flow);
                        setShowPanel('flows');
                    });

                    particle.transition()
                        .duration(speed)
                        .ease(d3.easeLinear)
                        .attr('cx', tx).attr('cy', ty)
                        .attr('opacity', 0)
                        .remove();
                } else if (wealthFlowData.length > 0) {
                    // Legacy wealth flow particles
                    const flow = wealthFlowData[Math.floor(Math.random() * wealthFlowData.length)];
                    const l = linkIndex[`${flow.from_actor}|${flow.to_actor}`] || linkIndex[`${flow.to_actor}|${flow.from_actor}`];
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
                }
            }, 150);
        }

        // ── Tick ──
        simulation.on('tick', () => {
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            node.attr('transform', d => `translate(${d.x},${d.y})`);

            // Update flow labels position
            if (showFlowLabels) {
                flowLabelG.selectAll('.flow-label-text').each(function(d) {
                    if (!d || !d.link) return;
                    const l = d.link;
                    const mx = ((l.source.x || 0) + (l.target.x || 0)) / 2;
                    const my = ((l.source.y || 0) + (l.target.y || 0)) / 2;
                    d3.select(this).attr('x', mx).attr('y', my);
                });
            }

            updateHighlights();
        });

        return () => {
            simulation.stop();
            if (particleTimer) particleTimer.stop();
        };
    }, [filteredData.nodes.length, filteredData.links.length, dimensions, activeFlowPairs, isPlaying, showFlowLabels, circularLoopNodes]);

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

    // ── Circular flow banner text ──
    const circularFlowBanner = useMemo(() => {
        const loops = (data?.circular_flows || []).filter(c => c.circular_flow_detected);
        if (!loops.length) return null;
        return loops.slice(0, 3);
    }, [data?.circular_flows]);

    // ── Zoom control handlers ──
    const handleZoomIn = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(300).call(zoom.scaleBy, 1.4);
    }, []);

    const handleZoomOut = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(300).call(zoom.scaleBy, 0.7);
    }, []);

    const handleFitScreen = useCallback(() => {
        if (!zoomRef.current) return;
        const { zoom, svg } = zoomRef.current;
        svg.transition().duration(500).call(zoom.transform, d3.zoomIdentity);
    }, []);

    const handleGraphSearch = useCallback((query) => {
        setSearchQuery(query);
    }, []);

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

    const flowSummary = data?.flow_summary || {};

    return (
        <div ref={fullScreenRef} style={S.page}>
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
                    maxWidth: '280px',
                }}
            />

            {/* ── Circular flow banner ── */}
            {circularFlowBanner && circularFlowBanner.length > 0 && (
                <div style={S.circularBanner}>
                    <span style={{ fontWeight: 700, fontSize: '9px', letterSpacing: '1.5px', flexShrink: 0 }}>
                        CIRCULAR FLOWS ({flowSummary.active_loops || circularFlowBanner.length})
                    </span>
                    <div style={{ display: 'flex', gap: '16px', overflow: 'hidden' }}>
                        {circularFlowBanner.map((loop, i) => (
                            <span
                                key={i}
                                style={{
                                    cursor: 'pointer',
                                    whiteSpace: 'nowrap',
                                    fontSize: '10px',
                                    textDecoration: highlightedLoop?.ticker === loop.ticker ? 'underline' : 'none',
                                }}
                                onClick={() => setHighlightedLoop(
                                    highlightedLoop?.ticker === loop.ticker ? null : loop
                                )}
                            >
                                {loop.company}: {formatMoney(loop.lobbying_spend)} lobby
                                {' -> '}{formatMoney(loop.pac_contributions)} PAC
                                {' -> '}{formatMoney(loop.contracts_received)} contracts
                                {loop.suspicion_score > 0 ? ` (${(loop.suspicion_score * 100).toFixed(0)}% sus)` : ''}
                            </span>
                        ))}
                    </div>
                </div>
            )}

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

                <div style={{ width: '1px', height: '20px', background: colors.border }} />

                {/* Flow controls */}
                <div style={S.filterGroup}>
                    <span style={S.filterLabel}>FLOWS</span>
                    <button
                        onClick={() => setShowFlowLabels(!showFlowLabels)}
                        style={S.filterBtn(showFlowLabels)}
                    >
                        Labels
                    </button>
                    <button
                        onClick={() => setShowPanel(showPanel === 'flows' ? 'detail' : 'flows')}
                        style={S.filterBtn(showPanel === 'flows')}
                    >
                        Aggregation
                    </button>
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
                    {filteredData.flows.length > 0 && ` / ${filteredData.flows.length} flows`}
                    {flowSummary.total_tracked && flowSummary.total_tracked !== '$0' && ` / ${flowSummary.total_tracked} tracked`}
                </div>
            </div>

            {/* ── Main area: graph + detail/flow panel ── */}
            <div style={S.mainArea}>
                {/* Graph */}
                <div ref={containerRef} style={S.graphContainer}>
                    <ChartControls
                        onZoomIn={handleZoomIn}
                        onZoomOut={handleZoomOut}
                        onFitScreen={handleFitScreen}
                        onFullScreen={toggleFullScreen}
                        isFullScreen={isFullScreen}
                        onSearch={handleGraphSearch}
                        searchPlaceholder="Search actors..."
                        showSearch={false}
                    />
                    <svg
                        ref={svgRef}
                        width={dimensions.width}
                        height={dimensions.height}
                        style={{ display: 'block', background: 'transparent' }}
                    />

                    {/* Mini-map */}
                    <svg
                        ref={miniMapRef}
                        width={150}
                        height={100}
                        style={{
                            position: 'absolute',
                            bottom: '12px',
                            right: '12px',
                            background: `${colors.card}CC`,
                            border: `1px solid ${colors.border}`,
                            borderRadius: '6px',
                            overflow: 'hidden',
                            pointerEvents: 'none',
                        }}
                    >
                        <rect width={150} height={100} fill={`${colors.bg}80`} />
                        {filteredData.nodes.map(n => (
                            <circle
                                key={n.id}
                                cx={(n.x || dimensions.width / 2) / dimensions.width * 150}
                                cy={(n.y || dimensions.height / 2) / dimensions.height * 100}
                                r={1.5}
                                fill={TIER_COLORS[n.tier] || '#5A7080'}
                                opacity={0.7}
                            />
                        ))}
                        <rect className="mm-viewport" x={0} y={0} width={150} height={100}
                            fill="none" stroke={colors.accent} strokeWidth={1} opacity={0.5} />
                    </svg>

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
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#22C55E' }} />
                                <span style={{ color: colors.textDim }}>Money in (campaign, contract)</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#EF4444' }} />
                                <span style={{ color: colors.textDim }}>Money out (stock sale)</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                <div style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#FFD700' }} />
                                <span style={{ color: colors.textDim }}>Influence (vote, policy)</span>
                            </div>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px', marginTop: '2px' }}>
                                <div style={{ width: '16px', height: '2px', background: '#FFD700' }} />
                                <span style={{ color: colors.textDim }}>Circular flow loop</span>
                            </div>
                        </div>
                    </div>
                </div>

                {/* ── Right sidebar: Detail panel OR Flow aggregation panel ── */}
                {showPanel === 'flows' ? (
                    /* ── Flow Aggregation Panel ── */
                    <div style={S.detailPanel}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <div style={{ fontSize: '14px', fontWeight: 700, color: '#E8F0F8', fontFamily: SANS }}>
                                Flow Aggregation
                            </div>
                            <button
                                onClick={() => setShowPanel('detail')}
                                style={{ ...S.filterBtn(false), fontSize: '9px', padding: '2px 8px' }}
                            >
                                X
                            </button>
                        </div>

                        {/* Flow summary */}
                        <div style={{ background: colors.bg, borderRadius: '6px', padding: '10px' }}>
                            <div style={S.metricRow}>
                                <span style={{ color: colors.textMuted }}>Total Tracked</span>
                                <span style={{ color: colors.text, fontFamily: MONO, fontWeight: 600 }}>
                                    {flowSummary.total_tracked || '--'}
                                </span>
                            </div>
                            <div style={S.metricRow}>
                                <span style={{ color: colors.textMuted }}>Active Loops</span>
                                <span style={{ color: '#FFD700', fontFamily: MONO, fontWeight: 600 }}>
                                    {flowSummary.active_loops || 0}
                                </span>
                            </div>
                            <div style={S.metricRow}>
                                <span style={{ color: colors.textMuted }}>Flows in View</span>
                                <span style={{ color: colors.text, fontFamily: MONO }}>
                                    {filteredData.flows.length}
                                </span>
                            </div>
                        </div>

                        {/* Selected flow detail */}
                        {selectedFlow && (
                            <div style={{
                                background: `${getFlowColor(selectedFlow.type)}10`,
                                border: `1px solid ${getFlowColor(selectedFlow.type)}40`,
                                borderRadius: '6px',
                                padding: '10px',
                            }}>
                                <div style={{ ...S.sectionTitle, color: getFlowColor(selectedFlow.type), marginTop: 0 }}>
                                    SELECTED FLOW
                                </div>
                                <div style={{ fontSize: '11px', color: colors.text, marginTop: '4px' }}>
                                    {selectedFlow.label || `${formatMoney(selectedFlow.amount)} ${selectedFlow.type}`}
                                </div>
                                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '4px' }}>
                                    From: {selectedFlow.from}
                                </div>
                                <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                    To: {selectedFlow.to}
                                </div>
                                {selectedFlow.date && (
                                    <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                        Date: {selectedFlow.date}
                                    </div>
                                )}
                            </div>
                        )}

                        {/* Per-actor inflow/outflow */}
                        <div>
                            <div style={S.sectionTitle}>NET FLOWS BY ACTOR</div>
                            <div style={{ maxHeight: '250px', overflowY: 'auto', marginTop: '6px' }}>
                                {flowAggregation.actors.map((a, i) => (
                                    <div key={i} style={{
                                        background: colors.bg,
                                        borderRadius: '6px',
                                        padding: '8px',
                                        marginBottom: '4px',
                                        fontSize: '10px',
                                    }}>
                                        <div title={a.label} style={{ fontWeight: 600, color: colors.text, fontFamily: MONO, marginBottom: '2px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', lineHeight: '1.3' }}>
                                            {a.label}
                                        </div>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '2px' }}>
                                            <span style={{ color: '#22C55E', whiteSpace: 'nowrap', fontFamily: MONO }}>
                                                {formatMoney(a.outflow)} out
                                            </span>
                                            <span style={{ color: colors.textMuted }}> -&gt; </span>
                                            <span style={{ color: '#22C55E', whiteSpace: 'nowrap', fontFamily: MONO }}>
                                                {formatMoney(a.inflow)} in
                                            </span>
                                            <span style={{ color: colors.textMuted }}> = </span>
                                            <span style={{
                                                color: a.net >= 0 ? '#22C55E' : '#EF4444',
                                                fontWeight: 700, whiteSpace: 'nowrap', fontFamily: MONO,
                                            }}>
                                                net {a.net >= 0 ? '+' : ''}{formatMoney(a.net)}
                                            </span>
                                        </div>
                                    </div>
                                ))}
                                {flowAggregation.actors.length === 0 && (
                                    <div style={{ color: colors.textMuted, fontSize: '10px', padding: '8px' }}>
                                        No flow data available for current view.
                                    </div>
                                )}
                            </div>
                        </div>

                        {/* Top 10 largest flows */}
                        <div>
                            <div style={S.sectionTitle}>TOP 10 LARGEST FLOWS</div>
                            <div style={{ maxHeight: '200px', overflowY: 'auto', marginTop: '6px' }}>
                                {flowAggregation.topFlows.map((f, i) => (
                                    <div
                                        key={i}
                                        style={{
                                            ...S.metricRow,
                                            cursor: 'pointer',
                                            padding: '4px 0',
                                        }}
                                        onClick={() => setSelectedFlow(f)}
                                    >
                                        <span title={f.label || `${f.from} -> ${f.to}`} style={{ color: colors.textDim, fontSize: '10px', flex: 1, minWidth: 0, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                            {f.label || `${f.from} -> ${f.to}`}
                                        </span>
                                        <span style={{
                                            color: getFlowColor(f.type),
                                            fontFamily: MONO,
                                            fontSize: '10px',
                                            fontWeight: 600,
                                            flexShrink: 0,
                                        }}>
                                            {formatMoney(f.amount)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>
                ) : selectedNode ? (
                    /* ── Detail panel (right sidebar) ── */
                    <div style={S.detailPanel}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: '8px' }}>
                            <div style={{ minWidth: 0, flex: 1 }}>
                                <div title={selectedNode.label} style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: SANS, lineHeight: '1.2', wordBreak: 'break-word' }}>
                                    {selectedNode.label}
                                </div>
                                <div title={selectedNode.title} style={{ fontSize: '11px', color: colors.textDim, marginTop: '2px', lineHeight: '1.5', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                    {selectedNode.title}
                                </div>
                            </div>
                            <span style={{ ...S.badge(TIER_COLORS[selectedNode.tier] || '#5A7080'), flexShrink: 0 }}>
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

                        {/* Flow summary for selected actor */}
                        {(() => {
                            const nodeAgg = flowAggregation.actors.find(a => a.id === selectedNode.id);
                            if (!nodeAgg) return null;
                            return (
                                <div>
                                    <div style={{ ...S.sectionTitle, color: '#22C55E' }}>MONEY FLOWS</div>
                                    <div style={{ background: colors.bg, borderRadius: '6px', padding: '8px', marginTop: '4px' }}>
                                        <div style={S.metricRow}>
                                            <span style={{ color: colors.textMuted }}>Total Out</span>
                                            <span style={{ color: '#EF4444', fontFamily: MONO, fontWeight: 600 }}>
                                                {formatMoney(nodeAgg.outflow)}
                                            </span>
                                        </div>
                                        <div style={S.metricRow}>
                                            <span style={{ color: colors.textMuted }}>Total In</span>
                                            <span style={{ color: '#22C55E', fontFamily: MONO, fontWeight: 600 }}>
                                                {formatMoney(nodeAgg.inflow)}
                                            </span>
                                        </div>
                                        <div style={{ ...S.metricRow, borderBottom: 'none' }}>
                                            <span style={{ color: colors.textMuted }}>Net</span>
                                            <span style={{
                                                color: nodeAgg.net >= 0 ? '#22C55E' : '#EF4444',
                                                fontFamily: MONO,
                                                fontWeight: 700,
                                            }}>
                                                {nodeAgg.net >= 0 ? '+' : ''}{formatMoney(nodeAgg.net)}
                                            </span>
                                        </div>
                                    </div>
                                </div>
                            );
                        })()}

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
                ) : null}
            </div>

            {/* ── Flow timeline bar (bottom) ── */}
            <div style={S.timelineBar}>
                <button
                    onClick={() => setIsPlaying(!isPlaying)}
                    style={{
                        ...S.filterBtn(isPlaying),
                        padding: '4px 12px',
                        fontSize: '11px',
                        minWidth: '36px',
                    }}
                    title={isPlaying ? 'Pause flow animation' : 'Play flow animation'}
                >
                    {isPlaying ? '||' : '>'}
                </button>

                <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, minWidth: '55px' }}>
                    {timelineDays}d window
                </span>

                <input
                    type="range"
                    min="7"
                    max="365"
                    step="1"
                    value={timelineDays}
                    onChange={e => setTimelineDays(Number(e.target.value))}
                    style={{ flex: 1, accentColor: colors.accent, cursor: 'pointer' }}
                    title="Flow timeline window"
                />

                <div style={{ display: 'flex', gap: '4px' }}>
                    {[30, 60, 90, 180, 365].map(d => (
                        <button
                            key={d}
                            onClick={() => setTimelineDays(d)}
                            style={{
                                ...S.filterBtn(timelineDays === d),
                                padding: '2px 6px',
                                fontSize: '9px',
                            }}
                        >
                            {d}d
                        </button>
                    ))}
                </div>

                <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO, marginLeft: '8px' }}>
                    {filteredData.flows.length} flows
                </div>
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
