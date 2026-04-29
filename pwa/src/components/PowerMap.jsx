/**
 * PowerMap — Sector-focused actor power map with force-directed layout.
 *
 * Shows top 15-30 actors per sector with real connections from the DB.
 * Nodes sized by influence, colored by category.
 * Edges colored by relationship type (competitor=red, co_investor=green, etc.)
 * Click a sector tab to switch. Search to find actors.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

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
    subsector: '#6B7280',
};

const SECTORS = [
    'Technology', 'Healthcare', 'Energy', 'Financials',
    'Consumer Discretionary', 'Industrials', 'Communication Services',
    'Consumer Staples', 'Materials', 'Real Estate', 'Utilities',
    'Crypto', 'Defense & Aerospace', 'Commodities',
];
const POWER_MAP_SHELL = {
    display: 'flex',
    flexDirection: 'column',
    height: '100%',
    minHeight: 360,
    maxHeight: 'min(720px, calc(100vh - 160px))',
    overflow: 'hidden',
};

function escapeHtml(str) {
    if (str == null) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

// Flow implication → particle color
const FLOW_COLORS = {
    contribution: '#22C55E',
    investment: '#22C55E',
    contract: '#22C55E',
    lobbying: '#22C55E',
    insider_buy: '#22C55E',
    stock_sale: '#EF4444',
    insider_sell: '#EF4444',
    sell: '#EF4444',
    outflow: '#EF4444',
    influence: '#FFD700',
    policy: '#FFD700',
    regulation: '#FFD700',
    congressional_trade: '#F59E0B',
};

function getFlowColor(impl) {
    if (!impl) return '#22C55E';
    const lower = impl.toLowerCase();
    for (const [key, col] of Object.entries(FLOW_COLORS)) {
        if (lower.includes(key)) return col;
    }
    return lower.includes('sell') || lower.includes('negative') ? '#EF4444' : '#22C55E';
}

function getParticleSize(amount) {
    if (!amount || amount <= 0) return 2;
    return Math.max(2, Math.min(7, 1.5 + Math.log10(Math.max(amount, 1000)) * 0.6));
}

export default function PowerMap({ initialSector = 'Technology', grand = false }) {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const tooltipRef = useRef(null);
    const simRef = useRef(null);
    const particleTimerRef = useRef(null);

    const [sector, setSector] = useState(initialSector);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [dims, setDims] = useState({ width: 900, height: 600 });
    const [selectedNode, setSelectedNode] = useState(null);

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

    // Load data when sector changes (or grand mode)
    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);
        setSelectedNode(null);
        const promise = grand
            ? api.getGrandPowerMap(50)
            : api.getPowerMap(sector);
        promise.then(d => {
            if (!cancelled) { setData(d); setLoading(false); }
        }).catch(err => {
            if (!cancelled) { setError(err.message); setLoading(false); }
        });
        return () => { cancelled = true; };
    }, [sector, grand]);

    // D3 render
    useEffect(() => {
        if (!data || !svgRef.current) return;
        const { nodes: rawNodes, edges: rawEdges } = data;
        if (!rawNodes?.length) return;

        const width = Math.max(360, Math.min(dims.width, 1400));
        const height = Math.max(320, Math.min(dims.height, 660));
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        // Deep copy for D3 mutation
        const nodes = rawNodes.map(n => ({ ...n }));
        const edges = rawEdges.filter(e => {
            const nodeIds = new Set(nodes.map(n => n.id));
            return nodeIds.has(e.source) && nodeIds.has(e.target);
        }).map(e => ({ ...e }));

        // Scale node size by influence
        // In grand mode, size by degree (connection count); else by influence
        const sizeAccessor = (n) => {
            if (grand && n.degree != null) return Math.max(n.degree, 1);
            return n.influence || 0.1;
        };
        const maxSize = Math.max(...nodes.map(sizeAccessor), 0.1);
        const rScale = d3.scaleSqrt().domain([0, maxSize]).range([6, 28]);

        // Container group for zoom
        const g = svg.append('g');

        // Zoom behavior
        const zoom = d3.zoom()
            .scaleExtent([0.3, 5])
            .on('zoom', (event) => g.attr('transform', event.transform));
        svg.call(zoom);

        // Edges
        const link = g.append('g')
            .selectAll('line')
            .data(edges)
            .join('line')
            .attr('stroke', d => d.color || '#334155')
            .attr('stroke-width', d => Math.max(1, (d.strength || 0.5) * 3))
            .attr('stroke-opacity', 0.4);

        // Edge labels (relationship type)
        const edgeLabel = g.append('g')
            .selectAll('text')
            .data(edges)
            .join('text')
            .attr('text-anchor', 'middle')
            .attr('fill', d => d.color || '#5A7A90')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('opacity', 0.6)
            .text(d => d.relationship?.replace(/_/g, ' '));

        // Node groups
        const node = g.append('g')
            .selectAll('g')
            .data(nodes)
            .join('g')
            .style('cursor', 'pointer')
            .call(d3.drag()
                .on('start', (event, d) => {
                    if (!event.active && simRef.current) simRef.current.alphaTarget(0.3).restart();
                    d.fx = d.x; d.fy = d.y;
                })
                .on('drag', (event, d) => { d.fx = event.x; d.fy = event.y; })
                .on('end', (event, d) => {
                    if (!event.active && simRef.current) simRef.current.alphaTarget(0);
                    d.fx = null; d.fy = null;
                })
            );

        // Glow ring
        node.append('circle')
            .attr('r', d => rScale(sizeAccessor(d)) + 3)
            .attr('fill', 'none')
            .attr('stroke', d => CATEGORY_COLORS[d.category] || '#6B7280')
            .attr('stroke-width', 1)
            .attr('stroke-opacity', 0.2);

        // Main circle
        node.append('circle')
            .attr('r', d => rScale(sizeAccessor(d)))
            .attr('fill', d => CATEGORY_COLORS[d.category] || '#6B7280')
            .attr('fill-opacity', 0.8)
            .attr('stroke', '#0F172A')
            .attr('stroke-width', 1.5);

        // Ticker badge
        node.filter(d => d.ticker)
            .append('text')
            .attr('dy', '0.35em')
            .attr('text-anchor', 'middle')
            .attr('fill', '#fff')
            .attr('font-size', d => rScale(sizeAccessor(d)) > 14 ? '8px' : '6px')
            .attr('font-weight', 700)
            .attr('font-family', "'JetBrains Mono', monospace")
            .text(d => d.ticker);

        // Name label below
        node.append('text')
            .attr('dy', d => rScale(sizeAccessor(d)) + 12)
            .attr('text-anchor', 'middle')
            .attr('fill', '#94A3B8')
            .attr('font-size', '8px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .text(d => {
                const name = d.name || '';
                return name.length > 18 ? name.slice(0, 16) + '…' : name;
            });

        // Tooltip
        const tooltip = d3.select(tooltipRef.current);
        node.on('mouseenter', (event, d) => {
            const cat = d.category || 'unknown';
            const inf = ((d.influence || 0) * 100).toFixed(0);
            const trust = ((d.trust || 0) * 100).toFixed(0);
            const nw = d.net_worth ? `$${(d.net_worth / 1e9).toFixed(1)}B` : '';
            const ticker = d.ticker ? `<span style="color:${CATEGORY_COLORS[cat]}">${escapeHtml(d.ticker)}</span> · ` : '';
            tooltip
                .style('opacity', 1)
                .style('left', `${event.clientX + 12}px`)
                .style('top', `${event.clientY - 12}px`)
                .html(`
                    <div style="font-weight:700;font-size:12px;margin-bottom:4px">${escapeHtml(d.name)}</div>
                    <div style="font-size:10px;color:#94A3B8">${ticker}${escapeHtml(cat)} · influence ${inf}% · trust ${trust}%</div>
                    ${d.title ? `<div style="font-size:9px;color:#64748B;margin-top:2px">${escapeHtml(d.title)}</div>` : ''}
                    ${nw ? `<div style="font-size:9px;color:#22C55E;margin-top:2px">Net worth: ${nw}</div>` : ''}
                    ${d.subsector ? `<div style="font-size:9px;color:#5A7A90;margin-top:2px">${escapeHtml(d.subsector)}</div>` : ''}
                `);
        })
        .on('mousemove', (event) => {
            tooltip.style('left', `${event.clientX + 12}px`).style('top', `${event.clientY - 12}px`);
        })
        .on('mouseleave', () => tooltip.style('opacity', 0));

        // Particle layer — above edges, below nodes
        const particleG = g.append('g').attr('class', 'particles');

        // Force simulation
        const sim = d3.forceSimulation(nodes)
            .force('link', d3.forceLink(edges).id(d => d.id).distance(100).strength(d => (d.strength || 0.3) * 0.5))
            .force('charge', d3.forceManyBody().strength(-200))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collision', d3.forceCollide().radius(d => rScale(sizeAccessor(d)) + 15))
            .force('x', d3.forceX(width / 2).strength(0.05))
            .force('y', d3.forceY(height / 2).strength(0.05));

        simRef.current = sim;

        sim.on('tick', () => {
            link
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);

            edgeLabel
                .attr('x', d => (d.source.x + d.target.x) / 2)
                .attr('y', d => (d.source.y + d.target.y) / 2);

            node.attr('transform', d => `translate(${d.x},${d.y})`);
        });

        // ── Animated wealth flow particles ──
        // Build edge lookup for matching flows to rendered edges
        const linkIndex = {};
        edges.forEach(e => {
            const sid = e.source?.id || e.source;
            const tid = e.target?.id || e.target;
            linkIndex[`${sid}|${tid}`] = e;
            linkIndex[`${tid}|${sid}`] = e;
        });

        const flows = data.flows || [];
        if (flows.length > 0) {
            if (particleTimerRef.current) particleTimerRef.current.stop();
            particleTimerRef.current = d3.interval(() => {
                const flow = flows[Math.floor(Math.random() * flows.length)];
                const e = linkIndex[`${flow.from}|${flow.to}`] || linkIndex[`${flow.to}|${flow.from}`];
                if (!e || !e.source || !e.target) return;

                // Direction: from → to
                const fromId = flow.from;
                const srcIsFrom = (e.source.id || e.source) === fromId;
                const sx = srcIsFrom ? (e.source.x || 0) : (e.target.x || 0);
                const sy = srcIsFrom ? (e.source.y || 0) : (e.target.y || 0);
                const tx = srcIsFrom ? (e.target.x || 0) : (e.source.x || 0);
                const ty = srcIsFrom ? (e.target.y || 0) : (e.source.y || 0);

                const color = getFlowColor(flow.implication);
                const size = getParticleSize(flow.amount);

                particleG.append('circle')
                    .attr('cx', sx).attr('cy', sy)
                    .attr('r', size)
                    .attr('fill', color)
                    .attr('opacity', 0.85)
                    .style('filter', `drop-shadow(0 0 ${size}px ${color})`)
                    .transition()
                    .duration(1200 + Math.random() * 800)
                    .ease(d3.easeLinear)
                    .attr('cx', tx).attr('cy', ty)
                    .attr('opacity', 0)
                    .remove();
            }, 120);
        }

        // Initial zoom to fit
        sim.on('end', () => {
            const xs = nodes.map(n => n.x);
            const ys = nodes.map(n => n.y);
            const x0 = Math.min(...xs) - 60, x1 = Math.max(...xs) + 60;
            const y0 = Math.min(...ys) - 60, y1 = Math.max(...ys) + 60;
            const bw = x1 - x0, bh = y1 - y0;
            const scale = Math.min(width / bw, height / bh, 2) * 0.85;
            const tx = (width - bw * scale) / 2 - x0 * scale;
            const ty = (height - bh * scale) / 2 - y0 * scale;
            svg.transition().duration(500).call(
                zoom.transform,
                d3.zoomIdentity.translate(tx, ty).scale(scale)
            );
        });

        return () => {
            sim.stop();
            if (particleTimerRef.current) particleTimerRef.current.stop();
        };
    }, [data, dims]);

    return (
        <div style={POWER_MAP_SHELL}>
            {/* Sector tabs (hidden in grand mode) */}
            {!grand && (
                <div style={{
                    display: 'flex', gap: '4px', padding: '8px 12px',
                    overflowX: 'auto', flexShrink: 0,
                    borderBottom: `1px solid ${colors.border}`,
                }}>
                    {SECTORS.map(s => (
                        <button key={s} onClick={() => setSector(s)}
                            style={{
                                background: s === sector ? `${colors.accent}20` : 'transparent',
                                border: `1px solid ${s === sector ? colors.accent : colors.border}`,
                                borderRadius: '4px', padding: '4px 10px', fontSize: '10px',
                                color: s === sector ? colors.accent : colors.textMuted,
                                cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                fontWeight: s === sector ? 700 : 400, whiteSpace: 'nowrap',
                            }}
                        >{s}</button>
                    ))}
                </div>
            )}

            {/* Stats bar */}
            {data && !loading && (
                <div style={{
                    display: 'flex', gap: '10px 16px', padding: '6px 12px',
                    fontSize: '10px', color: colors.textMuted,
                    fontFamily: "'JetBrains Mono', monospace",
                    borderBottom: `1px solid ${colors.borderSubtle}`,
                    flexShrink: 0, flexWrap: 'wrap',
                }}>
                    {grand && <span style={{ color: '#FFD700', fontWeight: 700 }}>GRAND POWER MAP</span>}
                    <span>{data.nodes?.length || 0} actors</span>
                    <span>{data.edges?.length || 0} connections</span>
                    {data.flows?.length > 0 && <span>{data.flows.length} flows</span>}
                    {data.etf && <span>ETF: {data.etf}</span>}
                    {data.subsectors?.length > 0 && (
                        <span>{data.subsectors.slice(0, 4).join(' · ')}{data.subsectors.length > 4 ? ` +${data.subsectors.length - 4}` : ''}</span>
                    )}
                </div>
            )}

            {/* Graph area */}
            <div ref={containerRef} style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
                {loading && (
                    <div style={{
                        position: 'absolute', inset: 0, display: 'flex',
                        alignItems: 'center', justifyContent: 'center',
                        color: colors.textMuted, fontSize: '12px',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>Loading {grand ? 'grand' : sector} power map...</div>
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
                    width={Math.max(360, Math.min(dims.width, 1400))}
                    height={Math.max(320, Math.min(dims.height, 660))}
                    style={{ display: 'block', width: '100%', height: '100%', background: '#0A0E14' }}
                />
                {/* Tooltip */}
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
                {data?.flows?.length > 0 && (
                    <>
                        <span style={{ margin: '0 4px', color: colors.border }}>|</span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#22C55E', display: 'inline-block', boxShadow: '0 0 4px #22C55E' }} />
                            inflow
                        </span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#EF4444', display: 'inline-block', boxShadow: '0 0 4px #EF4444' }} />
                            outflow
                        </span>
                        <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                            <span style={{ width: 6, height: 6, borderRadius: '50%', background: '#FFD700', display: 'inline-block', boxShadow: '0 0 4px #FFD700' }} />
                            influence
                        </span>
                    </>
                )}
            </div>
        </div>
    );
}
