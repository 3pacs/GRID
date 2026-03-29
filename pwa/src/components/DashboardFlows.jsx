/**
 * DashboardFlows — compact capital flow visualizations for the dashboard.
 *
 * Three view modes toggled by small icons:
 *   A) Mini Sankey — 3-column flow: Liquidity → Markets → Top 5 sectors
 *   B) Bubble Map  — D3 force simulation, sized by abs(net_flow)
 *   C) Actor Flow Bars — horizontal diverging bar chart
 *
 * Props: { data, onNavigate, defaultView }
 *   data from /api/v1/flows/aggregated — has by_sector, by_actor_tier, rotation_matrix
 */
import React, { useEffect, useRef, useState, useMemo, useCallback } from 'react';
import * as d3 from 'd3';
import { sankey as d3Sankey, sankeyLinkHorizontal } from 'd3-sankey';
import { GitBranch, Circle, BarChart3 } from 'lucide-react';
import { colors, tokens } from '../styles/shared.js';

// ── Constants ────────────────────────────────────────────────────
const MAX_HEIGHT = 250;
const FLOW_GREEN = '#22C55E';
const FLOW_RED = '#EF4444';
const FONT = "'JetBrains Mono', monospace";
const LS_KEY = 'dashboard_flow_view';

const VIEWS = [
  { key: 'sankey', Icon: GitBranch, label: 'Flow' },
  { key: 'bubble', Icon: Circle, label: 'Bubbles' },
  { key: 'bars',   Icon: BarChart3, label: 'Bars' },
];

// ── Helpers ──────────────────────────────────────────────────────

function fmtDollar(val) {
  const abs = Math.abs(val);
  if (abs >= 1e12) return `$${(val / 1e12).toFixed(1)}T`;
  if (abs >= 1e9)  return `$${(val / 1e9).toFixed(1)}B`;
  if (abs >= 1e6)  return `$${(val / 1e6).toFixed(1)}M`;
  if (abs >= 1e3)  return `$${(val / 1e3).toFixed(0)}K`;
  return `$${val.toFixed(0)}`;
}

function confidenceOpacity(confidence) {
  if (!confidence) return 0.85;
  const c = String(confidence).toLowerCase();
  if (c === 'confirmed') return 1.0;
  if (c === 'derived')   return 0.85;
  if (c === 'estimated') return 0.7;
  if (c === 'rumored')   return 0.5;
  return 0.7;
}

function savedView() {
  try { return localStorage.getItem(LS_KEY) || null; } catch { return null; }
}
function saveView(v) {
  try { localStorage.setItem(LS_KEY, v); } catch { /* noop */ }
}

/** Extract sorted sector entries from by_sector object. */
function sectorEntries(bySector) {
  if (!bySector || typeof bySector !== 'object') return [];
  return Object.entries(bySector)
    .filter(([name]) => name !== 'Unknown')
    .sort((a, b) => Math.abs(b[1].net_flow) - Math.abs(a[1].net_flow));
}

// ═════════════════════════════════════════════════════════════════
// View A: Mini Sankey
// ═════════════════════════════════════════════════════════════════

function MiniSankey({ data, onNavigate, width }) {
  const svgRef = useRef(null);

  useEffect(() => {
    if (!svgRef.current || !data?.by_sector) return;

    const entries = sectorEntries(data.by_sector).slice(0, 5);
    if (entries.length === 0) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const margin = { top: 12, right: 8, bottom: 12, left: 8 };
    const w = width - margin.left - margin.right;
    const h = MAX_HEIGHT - margin.top - margin.bottom;

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Build nodes: Liquidity(0), Markets(1), sector_0..sector_4
    const nodes = [
      { id: 0, name: 'Liquidity', column: 0 },
      { id: 1, name: 'Markets',   column: 1 },
    ];
    entries.forEach(([name], i) => {
      nodes.push({ id: i + 2, name, column: 2 });
    });

    // Links: Liquidity→Markets (total), Markets→each sector
    const totalFlow = entries.reduce((s, [, d]) => s + Math.abs(d.net_flow), 0) || 1;
    const links = [
      { source: 0, target: 1, value: Math.log10(totalFlow + 1), rawValue: totalFlow, direction: 'neutral' },
    ];
    entries.forEach(([, d], i) => {
      const absFlow = Math.abs(d.net_flow) || 1;
      links.push({
        source: 1,
        target: i + 2,
        value: Math.log10(absFlow + 1),
        rawValue: d.net_flow,
        direction: d.direction,
        confidence: d.confidence,
      });
    });

    // Layout
    let graph;
    try {
      const layout = d3Sankey()
        .nodeId(d => d.id)
        .nodeWidth(14)
        .nodePadding(6)
        .nodeAlign((node) => node.column !== undefined ? node.column : node.depth)
        .extent([[0, 0], [w, h]]);
      graph = layout({ nodes: nodes.map(d => ({ ...d })), links: links.map(d => ({ ...d })) });
    } catch { return; }

    // Links
    g.append('g')
      .attr('fill', 'none')
      .selectAll('path')
      .data(graph.links)
      .join('path')
      .attr('d', sankeyLinkHorizontal())
      .attr('stroke', d => d.direction === 'inflow' ? FLOW_GREEN : d.direction === 'outflow' ? FLOW_RED : colors.accent)
      .attr('stroke-width', d => Math.max(2, d.width))
      .attr('stroke-opacity', d => confidenceOpacity(d.confidence) * 0.4)
      .style('cursor', 'pointer')
      .on('mouseenter', function () { d3.select(this).attr('stroke-opacity', 0.75); })
      .on('mouseleave', function (_, d) { d3.select(this).attr('stroke-opacity', confidenceOpacity(d.confidence) * 0.4); });

    // Dollar labels on links
    g.append('g')
      .selectAll('text')
      .data(graph.links.filter(d => d.target.id >= 2))
      .join('text')
      .attr('x', d => (d.source.x1 + d.target.x0) / 2)
      .attr('y', d => {
        const sy = d.y0 !== undefined ? d.y0 : (d.source.y0 + d.source.y1) / 2;
        const ty = d.y1 !== undefined ? d.y1 : (d.target.y0 + d.target.y1) / 2;
        return (sy + ty) / 2;
      })
      .attr('text-anchor', 'middle')
      .attr('dy', '-3')
      .attr('font-size', '8px')
      .attr('font-family', FONT)
      .attr('fill', d => d.direction === 'inflow' ? FLOW_GREEN : FLOW_RED)
      .attr('opacity', 0.85)
      .text(d => fmtDollar(Math.abs(d.rawValue)));

    // Nodes
    const node = g.append('g')
      .selectAll('g')
      .data(graph.nodes)
      .join('g')
      .style('cursor', d => d.column === 2 ? 'pointer' : 'default')
      .on('click', (_, d) => {
        if (d.column === 2 && onNavigate) onNavigate('money-flow');
      });

    node.append('rect')
      .attr('x', d => d.x0)
      .attr('y', d => d.y0)
      .attr('width', d => d.x1 - d.x0)
      .attr('height', d => Math.max(1, d.y1 - d.y0))
      .attr('fill', d => d.column === 0 ? colors.accent : d.column === 1 ? '#8B5CF6' : '#06B6D4')
      .attr('rx', 2)
      .attr('opacity', 0.9);

    node.append('text')
      .attr('x', d => d.column < 2 ? d.x1 + 5 : d.x0 - 5)
      .attr('y', d => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', d => d.column < 2 ? 'start' : 'end')
      .attr('font-size', '9px')
      .attr('font-family', FONT)
      .attr('fill', colors.textDim)
      .text(d => d.name.length > 14 ? d.name.slice(0, 12) + '..' : d.name);

  }, [data, width, onNavigate]);

  return (
    <svg ref={svgRef} width={width} height={MAX_HEIGHT} style={{ display: 'block' }} />
  );
}

// ═════════════════════════════════════════════════════════════════
// View B: Bubble Map
// ═════════════════════════════════════════════════════════════════

function BubbleMap({ data, onNavigate, width }) {
  const svgRef = useRef(null);
  const simRef = useRef(null);

  useEffect(() => {
    if (!svgRef.current || !data?.by_sector) return;

    const entries = sectorEntries(data.by_sector).slice(0, 12);
    if (entries.length === 0) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const cx = width / 2;
    const cy = MAX_HEIGHT / 2;

    // Scale: radius from abs(net_flow), log scaled
    const maxFlow = d3.max(entries, ([, d]) => Math.abs(d.net_flow)) || 1;
    const rScale = d3.scaleSqrt()
      .domain([0, Math.log10(maxFlow + 1)])
      .range([14, Math.min(55, width / 7)]);

    const bubbles = entries.map(([name, d]) => ({
      name,
      net_flow: d.net_flow,
      direction: d.direction,
      confidence: d.confidence,
      acceleration: d.acceleration,
      r: rScale(Math.log10(Math.abs(d.net_flow) + 1)),
    }));

    // Optional: actor tier sub-bubbles
    let actorBubbles = [];
    if (data.by_actor_tier) {
      const tiers = Object.entries(data.by_actor_tier)
        .filter(([, d]) => Math.abs(d.net_flow) > 0)
        .sort((a, b) => Math.abs(b[1].net_flow) - Math.abs(a[1].net_flow))
        .slice(0, 4);
      actorBubbles = tiers.map(([tier, d]) => ({
        name: tier.slice(0, 5),
        net_flow: d.net_flow,
        direction: d.direction,
        r: rScale(Math.log10(Math.abs(d.net_flow) + 1)) * 0.45,
        isTier: true,
      }));
    }

    const allNodes = [...bubbles, ...actorBubbles];

    // Force simulation
    if (simRef.current) simRef.current.stop();

    const sim = d3.forceSimulation(allNodes)
      .force('center', d3.forceCenter(cx, cy))
      .force('charge', d3.forceManyBody().strength(-2))
      .force('collision', d3.forceCollide(d => d.r + 3).strength(0.8))
      .force('x', d3.forceX(cx).strength(0.04))
      .force('y', d3.forceY(cy).strength(0.04));

    simRef.current = sim;

    // Defs for pulse animation
    const defs = svg.append('defs');
    bubbles.forEach((b, i) => {
      const dur = Math.max(1.5, 4 - (b.r / 55) * 2.5); // bigger = slower pulse
      defs.append('animate')
        .attr('id', `pulse-${i}`)
        .attr('attributeName', 'r')
        .attr('values', `${b.r};${b.r * 1.06};${b.r}`)
        .attr('dur', `${dur}s`)
        .attr('repeatCount', 'indefinite');
    });

    const gBubbles = svg.append('g');

    sim.on('tick', () => {
      gBubbles.selectAll('*').remove();

      allNodes.forEach((b, i) => {
        // Clamp positions
        b.x = Math.max(b.r, Math.min(width - b.r, b.x || cx));
        b.y = Math.max(b.r, Math.min(MAX_HEIGHT - b.r, b.y || cy));

        const isInflow = b.net_flow >= 0;
        const baseColor = isInflow ? FLOW_GREEN : FLOW_RED;
        const opacity = confidenceOpacity(b.confidence);

        const group = gBubbles.append('g')
          .style('cursor', b.isTier ? 'default' : 'pointer')
          .on('click', () => {
            if (!b.isTier && onNavigate) onNavigate('sector-dive', b.name);
          });

        // Circle with pulse
        const circle = group.append('circle')
          .attr('cx', b.x)
          .attr('cy', b.y)
          .attr('r', b.r)
          .attr('fill', baseColor)
          .attr('fill-opacity', b.isTier ? opacity * 0.25 : opacity * 0.2)
          .attr('stroke', baseColor)
          .attr('stroke-width', b.isTier ? 0.5 : 1)
          .attr('stroke-opacity', opacity * 0.6);

        // Pulse animation on the main sector bubbles
        if (!b.isTier) {
          const dur = Math.max(1.5, 4 - (b.r / 55) * 2.5);
          circle.append('animate')
            .attr('attributeName', 'r')
            .attr('values', `${b.r};${b.r * 1.06};${b.r}`)
            .attr('dur', `${dur}s`)
            .attr('repeatCount', 'indefinite');
        }

        // Label inside bubble
        if (b.r > 16) {
          const truncName = b.name.length > 8 ? b.name.slice(0, 7) + '..' : b.name;
          group.append('text')
            .attr('x', b.x)
            .attr('y', b.y - (b.isTier ? 0 : 4))
            .attr('text-anchor', 'middle')
            .attr('dy', '0.35em')
            .attr('font-size', b.isTier ? '7px' : Math.min(10, b.r / 3.5) + 'px')
            .attr('font-family', FONT)
            .attr('fill', colors.text)
            .attr('opacity', 0.9)
            .text(truncName);

          if (!b.isTier) {
            group.append('text')
              .attr('x', b.x)
              .attr('y', b.y + 8)
              .attr('text-anchor', 'middle')
              .attr('dy', '0.35em')
              .attr('font-size', Math.min(9, b.r / 4) + 'px')
              .attr('font-family', FONT)
              .attr('fill', isInflow ? FLOW_GREEN : FLOW_RED)
              .attr('opacity', 0.85)
              .text(fmtDollar(Math.abs(b.net_flow)));
          }
        }
      });
    });

    // Let sim settle then stop
    sim.alpha(0.6).restart();
    const timer = setTimeout(() => sim.stop(), 3000);

    return () => {
      clearTimeout(timer);
      if (simRef.current) simRef.current.stop();
    };
  }, [data, width, onNavigate]);

  return (
    <svg ref={svgRef} width={width} height={MAX_HEIGHT} style={{ display: 'block' }} />
  );
}

// ═════════════════════════════════════════════════════════════════
// View C: Actor Flow Bars
// ═════════════════════════════════════════════════════════════════

function FlowBars({ data, onNavigate, width }) {
  const entries = useMemo(() => {
    if (!data?.by_sector) return [];
    return sectorEntries(data.by_sector).slice(0, 10);
  }, [data]);

  if (entries.length === 0) return null;

  const maxAbs = Math.max(...entries.map(([, d]) => Math.abs(d.net_flow)), 1);
  const labelLeft = width * 0.22;  // labels take ~22% left side
  const dollarRight = width * 0.85; // dollar amounts start here
  const barAreaWidth = (dollarRight - labelLeft - 10) / 2;
  const midX = labelLeft + barAreaWidth + 5;

  return (
    <svg width={width} height={MAX_HEIGHT} style={{ display: 'block' }}>
      {entries.map(([name, d], i) => {
        const rowH = Math.min(22, (MAX_HEIGHT - 10) / entries.length);
        const y = 5 + i * rowH;
        const barH = Math.max(4, rowH - 6);
        const isInflow = d.net_flow >= 0;
        const ratio = Math.abs(d.net_flow) / maxAbs;
        const barW = ratio * barAreaWidth;
        const opacity = confidenceOpacity(d.confidence);

        // Trust score dot — look in top_actors for avg trust
        const trustColor = d.acceleration === 'accelerating' ? FLOW_GREEN
          : d.acceleration === 'decelerating' ? FLOW_RED
          : colors.yellow;

        return (
          <g
            key={name}
            style={{ cursor: 'pointer' }}
            onClick={() => {
              if (onNavigate) onNavigate('sector-dive', name);
            }}
          >
            {/* Sector name on left */}
            <text
              x={16}
              y={y + barH / 2}
              dy="0.35em"
              textAnchor="start"
              fontSize="9px"
              fontFamily={FONT}
              fill={colors.textDim}
            >
              {name.length > 14 ? name.slice(0, 12) + '..' : name}
            </text>

            {/* Trust/acceleration dot */}
            <circle
              cx={8}
              cy={y + barH / 2}
              r={3}
              fill={trustColor}
              opacity={0.8}
            />

            {/* Bar — green right or red left from center */}
            <rect
              x={isInflow ? midX : midX - barW}
              y={y}
              width={barW}
              height={barH}
              rx={2}
              fill={isInflow ? FLOW_GREEN : FLOW_RED}
              opacity={opacity * 0.65}
            />

            {/* Dollar amount on right */}
            <text
              x={dollarRight}
              y={y + barH / 2}
              dy="0.35em"
              textAnchor="start"
              fontSize="9px"
              fontFamily={FONT}
              fill={isInflow ? FLOW_GREEN : FLOW_RED}
              opacity={0.85}
            >
              {isInflow ? '+' : '-'}{fmtDollar(Math.abs(d.net_flow))}
            </text>
          </g>
        );
      })}

      {/* Center axis line */}
      <line
        x1={midX} y1={2} x2={midX} y2={MAX_HEIGHT - 2}
        stroke={colors.border} strokeWidth={1} strokeDasharray="2,2"
      />
    </svg>
  );
}

// ═════════════════════════════════════════════════════════════════
// Main Component
// ═════════════════════════════════════════════════════════════════

export default function DashboardFlows({ data, onNavigate, defaultView }) {
  const containerRef = useRef(null);
  const [view, setView] = useState(() => savedView() || defaultView || 'sankey');
  const [containerWidth, setContainerWidth] = useState(400);

  // Measure container width
  useEffect(() => {
    if (!containerRef.current) return;
    const measure = () => {
      const w = containerRef.current?.clientWidth;
      if (w && w > 0) setContainerWidth(w);
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(containerRef.current);
    return () => ro.disconnect();
  }, []);

  const handleViewChange = useCallback((v) => {
    setView(v);
    saveView(v);
  }, []);

  const hasData = data?.by_sector && Object.keys(data.by_sector).length > 0;

  return (
    <div
      ref={containerRef}
      style={{
        background: colors.card,
        borderRadius: tokens.radius.md,
        border: `1px solid ${colors.border}`,
        overflow: 'hidden',
        width: '100%',
      }}
    >
      {/* Header with toggle */}
      <div style={{
        padding: '8px 12px',
        borderBottom: `1px solid ${colors.border}`,
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
      }}>
        <div style={{
          fontSize: '10px',
          fontWeight: 700,
          letterSpacing: '1.5px',
          color: colors.accent,
          fontFamily: FONT,
        }}>
          CAPITAL FLOWS
        </div>

        <div style={{ display: 'flex', gap: '3px' }}>
          {VIEWS.map(({ key, Icon }) => {
            const active = view === key;
            return (
              <button
                key={key}
                onClick={() => handleViewChange(key)}
                title={key}
                style={{
                  background: active ? `${colors.accent}20` : 'transparent',
                  border: active ? `1px solid ${colors.accent}` : `1px solid ${colors.border}`,
                  borderRadius: '4px',
                  padding: '3px 6px',
                  cursor: 'pointer',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  transition: `all ${tokens.transition.fast}`,
                }}
              >
                <Icon
                  size={12}
                  color={active ? colors.accent : colors.textMuted}
                  strokeWidth={active ? 2.5 : 1.5}
                />
              </button>
            );
          })}
        </div>
      </div>

      {/* Visualization area */}
      <div style={{ minHeight: MAX_HEIGHT }}>
        {!hasData ? (
          <div style={{
            height: MAX_HEIGHT,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'center',
            color: colors.textMuted,
            fontSize: '11px',
            fontFamily: FONT,
          }}>
            No flow data
          </div>
        ) : (
          <>
            {view === 'sankey' && (
              <MiniSankey data={data} onNavigate={onNavigate} width={containerWidth} />
            )}
            {view === 'bubble' && (
              <BubbleMap data={data} onNavigate={onNavigate} width={containerWidth} />
            )}
            {view === 'bars' && (
              <FlowBars data={data} onNavigate={onNavigate} width={containerWidth} />
            )}
          </>
        )}
      </div>
    </div>
  );
}
