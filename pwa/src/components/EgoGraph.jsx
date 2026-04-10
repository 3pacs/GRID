/**
 * EgoGraph — Search-first actor ego-graph with concentric ring layout.
 *
 * Type a name or ticker → get a 2-3 degree connection graph centered
 * on that actor. Rings represent hop distance from the center.
 * Click any node to re-center the graph on that actor.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const CATEGORY_COLORS = {
    company: '#3B82F6',
    corporation: '#3B82F6',
    billionaire: '#F59E0B',
    insider: '#EC4899',
    politician: '#EF4444',
    government: '#EF4444',
    central_bank: '#FFD700',
    fund: '#22C55E',
    regulator: '#8B5CF6',
    policy: '#A855F7',
    macro: '#06B6D4',
    indicator: '#14B8A6',
    data: '#6366F1',
    sovereign: '#FFD700',
    infra: '#64748B',
    physical: '#84CC16',
};

const RING_RADII = [0, 140, 280, 420];

function escapeHtml(str) {
    if (str == null) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

export default function EgoGraph() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const tooltipRef = useRef(null);
    const simRef = useRef(null);

    const [query, setQuery] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [searching, setSearching] = useState(false);
    const [centerId, setCenterId] = useState(null);
    const [centerName, setCenterName] = useState('');
    const [depth, setDepth] = useState(2);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [dims, setDims] = useState({ width: 900, height: 600 });
    const [history, setHistory] = useState([]);

    // Resize observer
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const obs = new ResizeObserver(entries => {
            const { width, height } = entries[0].contentRect;
            if (width > 0 && height > 0) setDims({ width, height });
        });
        obs.observe(el);
        return () => obs.disconnect();
    }, []);

    // Debounced search
    useEffect(() => {
        if (query.length < 2) { setSearchResults([]); return; }
        const timer = setTimeout(async () => {
            setSearching(true);
            try {
                const res = await api.getEgoGraphSearch(query);
                setSearchResults(res.results || []);
            } catch { setSearchResults([]); }
            setSearching(false);
        }, 300);
        return () => clearTimeout(timer);
    }, [query]);

    // Load ego-graph when center changes
    useEffect(() => {
        if (!centerId) return;
        let cancelled = false;
        setLoading(true);
        setError(null);
        api.getEgoGraph(centerId, depth).then(d => {
            if (!cancelled) { setData(d); setLoading(false); }
        }).catch(err => {
            if (!cancelled) { setError(err.message); setLoading(false); }
        });
        return () => { cancelled = true; };
    }, [centerId, depth]);

    const selectActor = useCallback((id, name) => {
        if (centerId) {
            setHistory(h => [...h, { id: centerId, name: centerName }]);
        }
        setCenterId(id);
        setCenterName(name);
        setQuery('');
        setSearchResults([]);
    }, [centerId, centerName]);

    const goBack = useCallback(() => {
        if (history.length === 0) return;
        const prev = history[history.length - 1];
        setHistory(h => h.slice(0, -1));
        setCenterId(prev.id);
        setCenterName(prev.name);
    }, [history]);

    // D3 render — concentric ring layout
    useEffect(() => {
        if (!data || !svgRef.current) return;
        const { nodes: rawNodes, edges: rawEdges } = data;
        if (!rawNodes?.length) return;

        const { width, height } = dims;
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const nodes = rawNodes.map(n => ({ ...n }));
        const edges = rawEdges.filter(e => {
            const ids = new Set(nodes.map(n => n.id));
            return ids.has(e.source) && ids.has(e.target);
        }).map(e => ({ ...e }));

        // Scale radii to viewport
        const minDim = Math.min(width, height);
        const scale = minDim / 900;
        const ringR = RING_RADII.map(r => r * scale);

        // Position nodes in concentric rings
        const ringGroups = {};
        nodes.forEach(n => {
            const ring = n.ring || 0;
            if (!ringGroups[ring]) ringGroups[ring] = [];
            ringGroups[ring].push(n);
        });

        const cx = width / 2, cy = height / 2;
        Object.entries(ringGroups).forEach(([ring, group]) => {
            const r = ringR[ring] || ringR[ringR.length - 1];
            if (r === 0) {
                // Center node
                group[0].x = cx;
                group[0].y = cy;
                group[0].fx = cx;
                group[0].fy = cy;
            } else {
                group.forEach((n, i) => {
                    const angle = (2 * Math.PI * i) / group.length - Math.PI / 2;
                    n.x = cx + r * Math.cos(angle);
                    n.y = cy + r * Math.sin(angle);
                });
            }
        });

        const maxInfluence = Math.max(...nodes.map(n => n.influence || 0.1), 0.1);
        const rScale = d3.scaleSqrt().domain([0, maxInfluence]).range([5, 24]);

        const g = svg.append('g');
        const zoom = d3.zoom().scaleExtent([0.3, 5])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        // Ring guides (subtle circles)
        ringR.slice(1).forEach(r => {
            g.append('circle')
                .attr('cx', cx).attr('cy', cy).attr('r', r)
                .attr('fill', 'none')
                .attr('stroke', '#1A2332')
                .attr('stroke-width', 1)
                .attr('stroke-dasharray', '4,4');
        });

        // Ring labels
        ['center', '1st degree', '2nd degree', '3rd degree'].forEach((label, i) => {
            if (i === 0 || !ringR[i]) return;
            g.append('text')
                .attr('x', cx).attr('y', cy - ringR[i] - 8)
                .attr('text-anchor', 'middle')
                .attr('fill', '#3A4A5A')
                .attr('font-size', '9px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .text(label);
        });

        // Edges
        const link = g.append('g').selectAll('line').data(edges).join('line')
            .attr('stroke', d => d.color || '#334155')
            .attr('stroke-width', d => Math.max(1, (d.strength || 0.5) * 2.5))
            .attr('stroke-opacity', 0.35);

        // Edge labels
        const edgeLabel = g.append('g').selectAll('text').data(edges).join('text')
            .attr('text-anchor', 'middle')
            .attr('fill', d => d.color || '#5A7A90')
            .attr('font-size', '6px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('opacity', 0.5)
            .text(d => d.relationship?.replace(/_/g, ' '));

        // Node groups
        const node = g.append('g').selectAll('g').data(nodes).join('g')
            .style('cursor', 'pointer')
            .call(d3.drag()
                .on('start', (event, d) => {
                    if (!event.active && simRef.current) simRef.current.alphaTarget(0.3).restart();
                    d.fx = d.x; d.fy = d.y;
                })
                .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
                .on('end', (event, d) => {
                    if (!event.active && simRef.current) simRef.current.alphaTarget(0);
                    if (d.ring !== 0) { d.fx = null; d.fy = null; }
                })
            );

        // Click to re-center
        node.on('click', (event, d) => {
            if (d.id !== centerId) {
                selectActor(d.id, d.name);
            }
        });

        // Glow for center
        node.filter(d => d.ring === 0).append('circle')
            .attr('r', d => rScale(d.influence || 0.1) + 8)
            .attr('fill', 'none')
            .attr('stroke', '#FFD700')
            .attr('stroke-width', 2)
            .attr('stroke-opacity', 0.4);

        // Main circle
        node.append('circle')
            .attr('r', d => d.ring === 0 ? rScale(d.influence || 0.1) + 4 : rScale(d.influence || 0.1))
            .attr('fill', d => CATEGORY_COLORS[d.category] || '#6B7280')
            .attr('fill-opacity', d => d.ring === 0 ? 1 : 0.8)
            .attr('stroke', d => d.ring === 0 ? '#FFD700' : '#0F172A')
            .attr('stroke-width', d => d.ring === 0 ? 2 : 1.5);

        // Name label
        node.append('text')
            .attr('dy', d => (d.ring === 0 ? rScale(d.influence || 0.1) + 18 : rScale(d.influence || 0.1) + 12))
            .attr('text-anchor', 'middle')
            .attr('fill', d => d.ring === 0 ? '#E2E8F0' : '#94A3B8')
            .attr('font-size', d => d.ring === 0 ? '10px' : '8px')
            .attr('font-weight', d => d.ring === 0 ? 700 : 400)
            .attr('font-family', "'JetBrains Mono', monospace")
            .text(d => {
                const name = d.name || '';
                return name.length > 20 ? name.slice(0, 18) + '…' : name;
            });

        // Tooltip
        const tooltip = d3.select(tooltipRef.current);
        node.on('mouseenter', (event, d) => {
            const cat = d.category || 'unknown';
            const inf = ((d.influence || 0) * 100).toFixed(0);
            const nw = d.net_worth ? `$${(d.net_worth / 1e9).toFixed(1)}B` : '';
            tooltip.style('opacity', 1)
                .style('left', `${event.clientX + 12}px`)
                .style('top', `${event.clientY - 12}px`)
                .html(`
                    <div style="font-weight:700;font-size:12px;margin-bottom:4px">${escapeHtml(d.name)}</div>
                    <div style="font-size:10px;color:#94A3B8">${escapeHtml(cat)} · influence ${inf}% · ring ${d.ring}</div>
                    ${d.title ? `<div style="font-size:9px;color:#64748B;margin-top:2px">${escapeHtml(d.title)}</div>` : ''}
                    ${nw ? `<div style="font-size:9px;color:#22C55E;margin-top:2px">Net worth: ${nw}</div>` : ''}
                    <div style="font-size:8px;color:#5A7080;margin-top:4px">Click to re-center</div>
                `);
        })
        .on('mousemove', (event) => {
            tooltip.style('left', `${event.clientX + 12}px`).style('top', `${event.clientY - 12}px`);
        })
        .on('mouseleave', () => tooltip.style('opacity', 0));

        // Gentle force to keep ring structure but allow spreading
        const sim = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(edges).id(d => d.id).distance(80).strength(0.15))
            .force('charge', d3.forceManyBody().strength(-120))
            .force('collision', d3.forceCollide().radius(d => rScale(d.influence || 0.1) + 12))
            .force('radial', d3.forceRadial(d => ringR[d.ring] || ringR[ringR.length - 1], cx, cy).strength(0.8))
            .alphaDecay(0.04);

        simRef.current = sim;

        sim.on('tick', () => {
            link.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
            edgeLabel.attr('x', d => (d.source.x + d.target.x) / 2)
                .attr('y', d => (d.source.y + d.target.y) / 2);
            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });

        return () => { sim.stop(); };
    }, [data, dims, centerId, selectActor]);

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: '100%' }}>
            {/* Search bar + controls */}
            <div style={{
                display: 'flex', gap: '8px', padding: '8px 12px',
                borderBottom: `1px solid ${colors.border}`,
                alignItems: 'center', flexShrink: 0,
            }}>
                <div style={{ position: 'relative', flex: 1, maxWidth: '400px' }}>
                    <input
                        type="text"
                        value={query}
                        onChange={e => setQuery(e.target.value)}
                        placeholder="Search actor or ticker..."
                        style={{
                            width: '100%', padding: '6px 12px',
                            background: '#0D1520', border: `1px solid ${colors.border}`,
                            borderRadius: '6px', color: colors.text,
                            fontSize: '12px', fontFamily: "'JetBrains Mono', monospace",
                            outline: 'none',
                        }}
                    />
                    {/* Search dropdown */}
                    {searchResults.length > 0 && (
                        <div style={{
                            position: 'absolute', top: '100%', left: 0, right: 0,
                            background: '#1E293B', border: `1px solid ${colors.border}`,
                            borderRadius: '0 0 6px 6px', zIndex: 100,
                            maxHeight: '240px', overflowY: 'auto',
                        }}>
                            {searchResults.map(r => (
                                <div key={r.id}
                                    onClick={() => selectActor(r.id, r.name)}
                                    style={{
                                        padding: '6px 12px', cursor: 'pointer',
                                        borderBottom: `1px solid ${colors.borderSubtle}`,
                                        fontSize: '11px', fontFamily: "'JetBrains Mono', monospace",
                                        color: colors.text,
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.background = '#2D3748'}
                                    onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                                >
                                    <span style={{ fontWeight: 700 }}>{r.name}</span>
                                    <span style={{ color: colors.textMuted, marginLeft: '8px' }}>
                                        {r.category} · {((r.influence || 0) * 100).toFixed(0)}%
                                    </span>
                                    {r.title && (
                                        <div style={{ fontSize: '9px', color: colors.textMuted }}>
                                            {r.title}
                                        </div>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                {/* Depth selector */}
                <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                    <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>
                        depth:
                    </span>
                    {[1, 2, 3].map(d => (
                        <button key={d} onClick={() => setDepth(d)}
                            style={{
                                background: d === depth ? `${colors.accent}20` : 'transparent',
                                border: `1px solid ${d === depth ? colors.accent : colors.border}`,
                                borderRadius: '4px', padding: '2px 8px', fontSize: '10px',
                                color: d === depth ? colors.accent : colors.textMuted,
                                cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                fontWeight: d === depth ? 700 : 400,
                            }}
                        >{d}</button>
                    ))}
                </div>

                {/* Back button */}
                {history.length > 0 && (
                    <button onClick={goBack}
                        style={{
                            background: 'transparent', border: `1px solid ${colors.border}`,
                            borderRadius: '4px', padding: '2px 10px', fontSize: '10px',
                            color: colors.textMuted, cursor: 'pointer',
                            fontFamily: "'JetBrains Mono', monospace",
                        }}
                    >← back</button>
                )}
            </div>

            {/* Stats bar */}
            {data && !loading && centerId && (
                <div style={{
                    display: 'flex', gap: '16px', padding: '6px 12px',
                    fontSize: '10px', color: colors.textMuted,
                    fontFamily: "'JetBrains Mono', monospace",
                    borderBottom: `1px solid ${colors.borderSubtle}`,
                    flexShrink: 0,
                }}>
                    <span style={{ color: '#FFD700', fontWeight: 700 }}>{centerName}</span>
                    <span>{data.nodes?.length || 0} actors</span>
                    <span>{data.edges?.length || 0} connections</span>
                    <span>depth {depth}</span>
                </div>
            )}

            {/* Graph area */}
            <div ref={containerRef} style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
                {!centerId && !loading && (
                    <div style={{
                        position: 'absolute', inset: 0, display: 'flex',
                        flexDirection: 'column', alignItems: 'center', justifyContent: 'center',
                        color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace",
                        gap: '12px',
                    }}>
                        <div style={{ fontSize: '14px', color: colors.textDim }}>
                            Search for any actor, company, or ticker
                        </div>
                        <div style={{ fontSize: '10px' }}>
                            Type a name above to see their connection network
                        </div>
                    </div>
                )}
                {loading && (
                    <div style={{
                        position: 'absolute', inset: 0, display: 'flex',
                        alignItems: 'center', justifyContent: 'center',
                        color: colors.textMuted, fontSize: '12px',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>Loading ego-graph for {centerName}...</div>
                )}
                {error && (
                    <div style={{
                        position: 'absolute', inset: 0, display: 'flex',
                        alignItems: 'center', justifyContent: 'center',
                        color: '#EF4444', fontSize: '12px',
                    }}>{error}</div>
                )}
                <svg
                    ref={svgRef}
                    width={dims.width}
                    height={dims.height}
                    style={{ display: 'block', background: '#0A0E14' }}
                />
                <div ref={tooltipRef} style={{
                    position: 'fixed', pointerEvents: 'none',
                    background: '#1E293B', border: `1px solid ${colors.border}`,
                    borderRadius: '6px', padding: '8px 12px',
                    opacity: 0, transition: 'opacity 0.15s',
                    zIndex: 1000, maxWidth: '280px',
                    fontFamily: "'JetBrains Mono', monospace",
                    color: colors.text, fontSize: '11px',
                }} />
            </div>

            {/* Legend */}
            <div style={{
                display: 'flex', gap: '12px', padding: '6px 12px',
                fontSize: '8px', color: colors.textMuted,
                fontFamily: "'JetBrains Mono', monospace",
                borderTop: `1px solid ${colors.borderSubtle}`,
                flexWrap: 'wrap', flexShrink: 0,
            }}>
                {data?.relationship_colors && Object.entries(data.relationship_colors).map(([rel, col]) => (
                    <span key={rel} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <span style={{ width: '12px', height: '2px', background: col, display: 'inline-block' }} />
                        {rel.replace(/_/g, ' ')}
                    </span>
                ))}
                {!data?.relationship_colors && (
                    <>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#3B82F6', display: 'inline-block' }} />
                            company
                        </span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#F59E0B', display: 'inline-block' }} />
                            billionaire
                        </span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#EF4444', display: 'inline-block' }} />
                            politician
                        </span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: '#22C55E', display: 'inline-block' }} />
                            fund
                        </span>
                    </>
                )}
            </div>
        </div>
    );
}
