/**
 * MoneyFlow — Bubble Universe primary visualization with Sankey/Force secondary.
 *
 * Layout (top to bottom):
 *   1. Bubble Universe (60-70% viewport) — D3 force simulation
 *   2. Time slider (tight below chart) — 7d/30d/60d/90d + play
 *   3. View toggle (top-right corner) — Bubbles | Sankey | Force
 *   4. Levers + Actions (bottom) — compact
 *
 * Target: <800 lines. Show don't tell.
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';
import {
  ZoomIn, ZoomOut, Maximize2, Minimize2, Search, RefreshCw,
  Download, Play, Pause, Circle, GitBranch, Share2, ChevronLeft,
} from 'lucide-react';

// ── Constants ────────────────────────────────────────────────────
const FLOW_GREEN = '#22C55E';
const FLOW_RED = '#EF4444';
const FLOW_GRAY = '#64748B';
const FONT = "'JetBrains Mono', monospace";
const TIME_PERIODS = [
  { label: '7D', days: 7 },
  { label: '30D', days: 30 },
  { label: '60D', days: 60 },
  { label: '90D', days: 90 },
];
const VIEW_MODES = [
  { id: 'bubble', Icon: Circle, label: 'Bubbles' },
  { id: 'sankey', Icon: GitBranch, label: 'Sankey' },
  { id: 'force', Icon: Share2, label: 'Force' },
];

// ── Helpers ──────────────────────────────────────────────────────
function fmtDollar(val) {
  if (val == null || isNaN(val)) return '--';
  const abs = Math.abs(val);
  const sign = val < 0 ? '-' : '';
  if (abs >= 1e12) return `${sign}$${(abs / 1e12).toFixed(1)}T`;
  if (abs >= 1e9)  return `${sign}$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6)  return `${sign}$${(abs / 1e6).toFixed(1)}M`;
  if (abs >= 1e3)  return `${sign}$${(abs / 1e3).toFixed(0)}K`;
  return `${sign}$${abs.toFixed(0)}`;
}

function confidenceOpacity(c) {
  if (!c) return 0.85;
  const s = String(c).toLowerCase();
  if (s === 'confirmed') return 1.0;
  if (s === 'derived')   return 0.85;
  if (s === 'estimated') return 0.7;
  if (s === 'rumored')   return 0.5;
  return 0.7;
}

function flowColor(direction) {
  if (direction === 'inflow')  return FLOW_GREEN;
  if (direction === 'outflow') return FLOW_RED;
  return FLOW_GRAY;
}

function sectorEntries(bySector) {
  if (!bySector || typeof bySector !== 'object') return [];
  return Object.entries(bySector)
    .filter(([n]) => n !== 'Unknown')
    .sort((a, b) => Math.abs(b[1].net_flow) - Math.abs(a[1].net_flow));
}

// ── Styles ───────────────────────────────────────────────────────
const B = colors.bg, C = colors.card, BD = colors.border, R = tokens.radius.md;
const abs_overlay = (t, r, b, l) => ({ position: 'absolute', top: t, right: r, bottom: b, left: l, zIndex: 10 });
const sty = {
  page: { display: 'flex', flexDirection: 'column', height: '100%', background: B, color: colors.text, fontFamily: FONT, overflow: 'hidden' },
  chartWrap: { position: 'relative', flex: '1 1 auto', minHeight: 0, background: C, border: `1px solid ${BD}`, borderRadius: R, margin: '8px 12px 0 12px', overflow: 'hidden' },
  viewToggle: { ...abs_overlay(8, 8, undefined, undefined), display: 'flex', gap: 2, background: B + 'CC', borderRadius: tokens.radius.sm, padding: '2px', border: `1px solid ${BD}` },
  viewBtn: (a) => ({ display: 'flex', alignItems: 'center', gap: 4, background: a ? colors.accent + '30' : 'transparent', border: 'none', borderRadius: 4, padding: '4px 8px', cursor: 'pointer', fontSize: '10px', fontFamily: FONT, color: a ? colors.accent : colors.textDim, fontWeight: a ? 700 : 400 }),
  controls: { ...abs_overlay(8, undefined, undefined, 8), display: 'flex', flexDirection: 'column', gap: 4 },
  ctrlBtn: { background: B + 'CC', border: `1px solid ${BD}`, borderRadius: 6, padding: 6, cursor: 'pointer', display: 'flex', alignItems: 'center', justifyContent: 'center' },
  breadcrumb: { ...abs_overlay(undefined, undefined, 8, 8), display: 'flex', gap: 4, alignItems: 'center', background: B + 'CC', borderRadius: 6, padding: '4px 8px', border: `1px solid ${BD}`, fontSize: '10px' },
  searchBox: { ...abs_overlay(8, undefined, undefined, '50%'), transform: 'translateX(-50%)', display: 'flex', alignItems: 'center', gap: 4, background: B + 'EE', border: `1px solid ${BD}`, borderRadius: 6, padding: '4px 10px' },
  searchInput: { background: 'none', border: 'none', outline: 'none', color: colors.text, fontFamily: FONT, fontSize: '11px', width: 140 },
  sliderRow: { display: 'flex', alignItems: 'center', gap: 8, padding: '6px 12px', margin: '0 12px', background: C, border: `1px solid ${BD}`, borderTop: 'none', borderRadius: `0 0 ${R} ${R}` },
  sliderBtn: (a) => ({ background: a ? colors.accent : 'transparent', border: `1px solid ${a ? colors.accent : BD}`, borderRadius: 4, padding: '3px 10px', cursor: 'pointer', fontSize: '10px', fontFamily: FONT, fontWeight: 700, color: a ? '#fff' : colors.textDim }),
  playBtn: { background: 'none', border: `1px solid ${BD}`, borderRadius: 4, padding: '3px 6px', cursor: 'pointer', display: 'flex', alignItems: 'center' },
  leversWrap: { margin: '8px 12px 12px 12px', padding: '10px 14px', background: C, border: `1px solid ${BD}`, borderRadius: R, overflow: 'auto', maxHeight: '25vh' },
  leversTitle: { fontSize: '11px', fontWeight: 700, color: colors.accent, letterSpacing: '2px', marginBottom: 8 },
  leverRow: { display: 'flex', alignItems: 'center', gap: 8, padding: '4px 0', borderBottom: `1px solid ${BD}20`, fontSize: '11px' },
  leverBar: (ratio, inflow) => ({ height: 6, borderRadius: 3, width: `${Math.max(4, Math.abs(ratio) * 100)}%`, background: inflow ? FLOW_GREEN : FLOW_RED, opacity: 0.7 }),
  actionRow: { display: 'flex', gap: 8, marginTop: 8 },
  actionBtn: { display: 'flex', alignItems: 'center', gap: 4, background: 'none', border: `1px solid ${BD}`, borderRadius: 6, padding: '5px 10px', cursor: 'pointer', fontSize: '10px', fontFamily: FONT, color: colors.textDim },
  tooltip: { position: 'absolute', pointerEvents: 'none', zIndex: 20, background: B + 'F0', border: `1px solid ${BD}`, borderRadius: 6, padding: '8px 12px', fontSize: '11px', fontFamily: FONT, color: colors.text, maxWidth: 220, boxShadow: colors.shadow?.md || '0 4px 12px rgba(0,0,0,0.4)' },
};

// ═════════════════════════════════════════════════════════════════
// Bubble Universe Renderer
// ═════════════════════════════════════════════════════════════════
function renderBubbles(svgEl, data, dims, opts) {
  const { onDrill, onHover, onHoverEnd } = opts;
  const svg = d3.select(svgEl);
  svg.selectAll('*').remove();

  const entries = sectorEntries(data.by_sector).slice(0, 16);
  if (!entries.length) return null;

  const { width, height } = dims;
  const cx = width / 2, cy = height / 2;

  const maxFlow = d3.max(entries, ([, d]) => Math.abs(d.net_flow)) || 1;
  const rScale = d3.scaleSqrt()
    .domain([0, Math.log10(maxFlow + 1)])
    .range([18, Math.min(70, width / 8)]);

  const nodes = entries.map(([name, d]) => ({
    id: name, name, net_flow: d.net_flow, direction: d.direction,
    confidence: d.confidence, acceleration: d.acceleration,
    top_actors: d.top_actors || [],
    r: rScale(Math.log10(Math.abs(d.net_flow) + 1)),
    x: cx + (Math.random() - 0.5) * 100,
    y: cy + (Math.random() - 0.5) * 100,
  }));

  // Build connections from rotation_matrix if available
  const links = [];
  if (data.rotation_matrix) {
    const names = nodes.map(n => n.name);
    for (let i = 0; i < names.length; i++) {
      for (let j = i + 1; j < names.length; j++) {
        const key = `${names[i]}|${names[j]}`;
        const rev = `${names[j]}|${names[i]}`;
        const val = data.rotation_matrix[key] || data.rotation_matrix[rev];
        if (val && Math.abs(val) > 0) {
          links.push({ source: nodes[i], target: nodes[j], value: Math.abs(val) });
        }
      }
    }
  }

  // Set up group with zoom
  const g = svg.append('g');
  const zoom = d3.zoom()
    .scaleExtent([0.3, 5])
    .on('zoom', (e) => g.attr('transform', e.transform));
  svg.call(zoom);

  // Connection lines
  const linkG = g.append('g').attr('class', 'links');
  const maxLinkVal = d3.max(links, l => l.value) || 1;

  linkG.selectAll('line')
    .data(links)
    .join('line')
    .attr('stroke', colors.textMuted + '40')
    .attr('stroke-width', d => Math.max(0.5, (d.value / maxLinkVal) * 4))
    .attr('stroke-dasharray', '4,4');

  // Animated particles on links
  linkG.selectAll('circle.particle')
    .data(links.filter(l => l.value / maxLinkVal > 0.3))
    .join('circle')
    .attr('class', 'particle')
    .attr('r', 2)
    .attr('fill', colors.accent)
    .attr('opacity', 0.6);

  // Bubble groups
  const bubbleG = g.append('g').attr('class', 'bubbles');

  const bubbleGroups = bubbleG.selectAll('g.bubble')
    .data(nodes, d => d.id)
    .join(
      enter => {
        const ge = enter.append('g').attr('class', 'bubble').style('cursor', 'pointer');
        // Scale-in animation
        ge.attr('transform', d => `translate(${d.x},${d.y}) scale(0)`)
          .transition().duration(600).ease(d3.easeCubicOut)
          .attr('transform', d => `translate(${d.x},${d.y}) scale(1)`);
        return ge;
      },
    );

  // Glow
  bubbleGroups.append('circle')
    .attr('r', d => d.r + 6)
    .attr('fill', d => flowColor(d.direction))
    .attr('opacity', 0.08)
    .attr('filter', 'blur(6px)');

  // Main circle
  bubbleGroups.append('circle')
    .attr('r', d => d.r)
    .attr('fill', d => flowColor(d.direction))
    .attr('fill-opacity', d => confidenceOpacity(d.confidence) * 0.25)
    .attr('stroke', d => flowColor(d.direction))
    .attr('stroke-width', 1.5)
    .attr('stroke-opacity', d => confidenceOpacity(d.confidence) * 0.7);

  // Pulse animation on larger bubbles
  bubbleGroups.each(function(d) {
    if (d.r > 25) {
      d3.select(this).select('circle:nth-child(2)')
        .append('animate')
        .attr('attributeName', 'r')
        .attr('values', `${d.r};${d.r * 1.04};${d.r}`)
        .attr('dur', `${Math.max(2, 5 - d.r / 30)}s`)
        .attr('repeatCount', 'indefinite');
    }
  });

  // Label
  bubbleGroups.append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', d => d.r > 30 ? '-0.3em' : '0.35em')
    .attr('font-size', d => Math.max(8, Math.min(12, d.r / 3.5)) + 'px')
    .attr('font-family', FONT)
    .attr('fill', colors.text)
    .attr('opacity', 0.9)
    .text(d => d.name.length > 10 ? d.name.slice(0, 9) + '..' : d.name);

  // Dollar amount (only for larger bubbles)
  bubbleGroups.filter(d => d.r > 30).append('text')
    .attr('text-anchor', 'middle')
    .attr('dy', '1em')
    .attr('font-size', d => Math.max(8, Math.min(11, d.r / 4)) + 'px')
    .attr('font-family', FONT)
    .attr('fill', d => flowColor(d.direction))
    .attr('opacity', 0.85)
    .text(d => fmtDollar(d.net_flow));

  // Acceleration badge
  bubbleGroups.filter(d => d.acceleration === 'accelerating' || d.acceleration === 'decelerating')
    .append('text')
    .attr('dy', d => d.r > 30 ? '2.2em' : '1.5em')
    .attr('text-anchor', 'middle')
    .attr('font-size', '8px')
    .attr('fill', d => d.acceleration === 'accelerating' ? FLOW_GREEN : FLOW_RED)
    .text(d => d.acceleration === 'accelerating' ? '\u25B2 accel' : '\u25BC decel');

  // Interactions
  bubbleGroups
    .on('click', (e, d) => { e.stopPropagation(); onDrill?.(d.name); })
    .on('mouseenter', (e, d) => {
      onHover?.(d, e.clientX, e.clientY);
      d3.select(e.currentTarget).select('circle:nth-child(2)')
        .transition().duration(150).attr('stroke-width', 3);
    })
    .on('mouseleave', (e) => {
      onHoverEnd?.();
      d3.select(e.currentTarget).select('circle:nth-child(2)')
        .transition().duration(150).attr('stroke-width', 1.5);
    });

  // Drag
  bubbleGroups.call(
    d3.drag()
      .on('start', (e, d) => { sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
      .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
      .on('end', (e, d) => { sim.alphaTarget(0); d.fx = null; d.fy = null; })
  );

  // Force simulation
  const sim = d3.forceSimulation(nodes)
    .force('center', d3.forceCenter(cx, cy))
    .force('charge', d3.forceManyBody().strength(-8))
    .force('collision', d3.forceCollide(d => d.r + 6).strength(0.85))
    .force('x', d3.forceX(cx).strength(0.03))
    .force('y', d3.forceY(cy).strength(0.03));

  sim.on('tick', () => {
    bubbleGroups.attr('transform', d => {
      d.x = Math.max(d.r, Math.min(width - d.r, d.x));
      d.y = Math.max(d.r, Math.min(height - d.r, d.y));
      return `translate(${d.x},${d.y})`;
    });
    // Update links
    linkG.selectAll('line')
      .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
      .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    // Animate particles along links
    const t = Date.now() / 3000;
    linkG.selectAll('circle.particle').each(function(d) {
      const frac = (t + Math.random() * 0.01) % 1;
      d3.select(this)
        .attr('cx', d.source.x + (d.target.x - d.source.x) * frac)
        .attr('cy', d.source.y + (d.target.y - d.source.y) * frac);
    });
  });

  sim.alpha(0.8).restart();

  return { sim, zoom };
}

// ═════════════════════════════════════════════════════════════════
// Simple Sankey Renderer (secondary view)
// ═════════════════════════════════════════════════════════════════
function renderSankey(svgEl, data, dims) {
  const svg = d3.select(svgEl); svg.selectAll('*').remove();
  const entries = sectorEntries(data.by_sector).slice(0, 8);
  if (!entries.length) return null;
  const { width, height } = dims;
  const m = 30, w = width - m * 2, h = height - m * 2;
  const g = svg.append('g').attr('transform', `translate(${m},${m})`);
  const colW = w / 3, nW = 14;
  const sources = ['Fed Liquidity', 'Banking', 'Institutional'];
  const sectors = entries.map(([n]) => n);
  const pos = {};
  sources.forEach((n, i) => { pos[n] = { x: 0, y: i * h / 3, h: h / 3 * 0.7 }; });
  pos['Markets'] = { x: colW, y: h * 0.15, h: h * 0.7 };
  sectors.forEach((n, i) => { pos[n] = { x: colW * 2, y: i * h / Math.max(1, sectors.length), h: h / Math.max(1, sectors.length) * 0.7 }; });
  const link = d3.linkHorizontal().x(d => d[0]).y(d => d[1]);

  [...sources, 'Markets', ...sectors].forEach(name => {
    const p = pos[name]; if (!p) return;
    const isSrc = sources.includes(name), isSec = sectors.includes(name);
    const entry = isSec ? entries.find(([n]) => n === name) : null;
    const col = isSrc ? colors.accent : isSec ? flowColor(entry?.[1]?.direction) : '#8B5CF6';
    g.append('rect').attr('x', p.x).attr('y', p.y).attr('width', nW).attr('height', p.h).attr('fill', col).attr('opacity', 0.7).attr('rx', 3);
    g.append('text').attr('x', isSrc ? p.x + nW + 6 : isSec ? p.x - 6 : p.x + nW / 2).attr('y', p.y + p.h / 2)
      .attr('text-anchor', isSrc ? 'start' : isSec ? 'end' : 'middle').attr('dy', '0.35em')
      .attr('font-size', '10px').attr('font-family', FONT).attr('fill', colors.textDim)
      .text(name.length > 12 ? name.slice(0, 11) + '..' : name);
  });

  const totalFlow = entries.reduce((s, [, d]) => s + Math.abs(d.net_flow), 0) || 1;
  sources.forEach(src => {
    const sp = pos[src], mp = pos['Markets'];
    g.append('path').attr('d', link({ source: [sp.x + nW, sp.y + sp.h / 2], target: [mp.x, mp.y + mp.h / 2] }))
      .attr('fill', 'none').attr('stroke', colors.accent + '40').attr('stroke-width', 4);
  });
  entries.forEach(([name, d]) => {
    const mp = pos['Markets'], sp = pos[name]; if (!sp) return;
    const ratio = Math.abs(d.net_flow) / totalFlow;
    g.append('path').attr('d', link({ source: [mp.x + nW, mp.y + mp.h / 2], target: [sp.x, sp.y + sp.h / 2] }))
      .attr('fill', 'none').attr('stroke', flowColor(d.direction) + '50').attr('stroke-width', Math.max(2, ratio * 16));
    g.append('text').attr('x', (mp.x + nW + sp.x) / 2).attr('y', (mp.y + mp.h / 2 + sp.y + sp.h / 2) / 2 - 6)
      .attr('text-anchor', 'middle').attr('font-size', '8px').attr('font-family', FONT).attr('fill', flowColor(d.direction)).attr('opacity', 0.7)
      .text(fmtDollar(Math.abs(d.net_flow)));
  });

  const zoom = d3.zoom().scaleExtent([0.5, 4]).on('zoom', (e) => g.attr('transform', e.transform));
  svg.call(zoom);
  return { zoom };
}

// ═══ Force-Directed Renderer (secondary view) ═══════════════════
function renderForce(svgEl, data, dims, opts) {
  const svg = d3.select(svgEl); svg.selectAll('*').remove();
  const entries = sectorEntries(data.by_sector).slice(0, 12);
  if (!entries.length) return null;
  const { width, height } = dims;
  const nodes = entries.map(([name, d]) => ({ id: name, name, net_flow: d.net_flow, direction: d.direction, confidence: d.confidence }));
  const links = [];
  for (let i = 0; i < nodes.length; i++) for (let j = i + 1; j < nodes.length; j++) if (Math.random() > 0.5) links.push({ source: nodes[i], target: nodes[j], value: 1 });

  const g = svg.append('g');
  const zoom = d3.zoom().scaleExtent([0.3, 5]).on('zoom', (e) => g.attr('transform', e.transform));
  svg.call(zoom);
  const linkEl = g.append('g').selectAll('line').data(links).join('line').attr('stroke', colors.textMuted + '30').attr('stroke-width', 0.5);
  const nodeEl = g.append('g').selectAll('circle').data(nodes).join('circle')
    .attr('r', d => 8 + Math.log10(Math.abs(d.net_flow) + 1) * 3)
    .attr('fill', d => flowColor(d.direction)).attr('fill-opacity', d => confidenceOpacity(d.confidence) * 0.4)
    .attr('stroke', d => flowColor(d.direction)).attr('stroke-width', 1).style('cursor', 'pointer')
    .on('click', (e, d) => { opts.onDrill?.(d.name); });
  const labels = g.append('g').selectAll('text').data(nodes).join('text')
    .attr('text-anchor', 'middle').attr('dy', '0.35em').attr('font-size', '9px').attr('font-family', FONT).attr('fill', colors.textDim)
    .text(d => d.name.length > 10 ? d.name.slice(0, 9) + '..' : d.name);

  const sim = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(80))
    .force('charge', d3.forceManyBody().strength(-100))
    .force('center', d3.forceCenter(width / 2, height / 2))
    .force('collision', d3.forceCollide(20));
  sim.on('tick', () => {
    linkEl.attr('x1', d => d.source.x).attr('y1', d => d.source.y).attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    nodeEl.attr('cx', d => d.x).attr('cy', d => d.y);
    labels.attr('x', d => d.x).attr('y', d => d.y);
  });
  return { sim, zoom };
}

// ═════════════════════════════════════════════════════════════════
// Main Component
// ═════════════════════════════════════════════════════════════════
export default function MoneyFlow({ onNavigate } = {}) {
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const simRef = useRef(null);
  const zoomRef = useRef(null);

  const [data, setData] = useState(null);
  const [aggData, setAggData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const [dims, setDims] = useState({ width: 900, height: 500 });
  const [viewMode, setViewMode] = useState('bubble');
  const [selectedDays, setSelectedDays] = useState(30);
  const [timeIdx, setTimeIdx] = useState(1);
  const [animating, setAnimating] = useState(false);
  const animRef = useRef(null);

  const [isFullScreen, setIsFullScreen] = useState(false);
  const [searchOpen, setSearchOpen] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [tooltip, setTooltip] = useState(null);

  // Drill-down
  const [drillLevel, setDrillLevel] = useState(0);
  const [drillTarget, setDrillTarget] = useState(null);
  const [drillData, setDrillData] = useState(null);
  const [drillHistory, setDrillHistory] = useState([]);
  const [drillLoading, setDrillLoading] = useState(false);

  // ── Data loading ──────────────────────────────────────────────
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
    try {
      const d = await api.getAggregatedFlows(null, 'weekly', days);
      if (!d.error) setAggData(d);
    } catch (_) { /* graceful degradation */ }
  }, []);

  useEffect(() => { loadData(); }, [loadData]);
  useEffect(() => { loadAggregated(selectedDays); }, [loadAggregated, selectedDays]);

  // ── Dimensions ────────────────────────────────────────────────
  useEffect(() => {
    const compute = () => {
      if (!containerRef.current) return;
      const w = containerRef.current.clientWidth;
      const h = containerRef.current.clientHeight;
      setDims({ width: Math.max(300, w), height: Math.max(250, h) });
    };
    compute();
    window.addEventListener('resize', compute);
    return () => window.removeEventListener('resize', compute);
  }, [isFullScreen]);

  // ── Active data source ────────────────────────────────────────
  const activeData = useMemo(() => {
    if (drillLevel > 0 && drillData) return drillData;
    return aggData || data;
  }, [data, aggData, drillLevel, drillData]);

  // ── Sector levers ─────────────────────────────────────────────
  const levers = useMemo(() => {
    const src = aggData || data;
    if (!src?.by_sector) return [];
    return sectorEntries(src.by_sector).slice(0, 8).map(([name, d]) => ({
      name, net_flow: d.net_flow, direction: d.direction,
      acceleration: d.acceleration,
    }));
  }, [data, aggData]);

  const maxLever = useMemo(() => {
    return Math.max(1, ...levers.map(l => Math.abs(l.net_flow)));
  }, [levers]);

  // ── Drill ─────────────────────────────────────────────────────
  const drillInto = useCallback(async (name) => {
    if (drillLevel === 0) {
      setDrillLoading(true);
      setDrillLevel(1);
      setDrillTarget(name);
      setDrillHistory([{ label: 'All', level: 0 }]);
      try {
        const d = await api.getSectorDrill(name);
        setDrillData(d);
      } catch (err) {
        setDrillData({ by_sector: {}, error: err.message });
      }
      setDrillLoading(false);
    } else if (drillLevel === 1) {
      setDrillLoading(true);
      setDrillLevel(2);
      setDrillHistory(prev => [...prev, { label: drillTarget, level: 1 }]);
      setDrillTarget(name);
      try {
        const d = await api.getCompanyDrill(name);
        setDrillData(d);
      } catch (err) {
        setDrillData({ by_sector: {}, error: err.message });
      }
      setDrillLoading(false);
    }
  }, [drillLevel, drillTarget]);

  const drillBack = useCallback(() => {
    if (drillLevel <= 0) return;
    setDrillLevel(0);
    setDrillTarget(null);
    setDrillData(null);
    setDrillHistory([]);
  }, [drillLevel]);

  // ── Render chart ──────────────────────────────────────────────
  useEffect(() => {
    if (!svgRef.current || !activeData?.by_sector) return;
    if (simRef.current?.stop) simRef.current.stop();

    const opts = {
      onDrill: drillInto,
      onHover: (node, x, y) => {
        setTooltip({
          x, y, name: node.name,
          amount: fmtDollar(node.net_flow),
          direction: node.direction || (node.net_flow >= 0 ? 'inflow' : 'outflow'),
          confidence: node.confidence,
          actors: node.top_actors?.slice(0, 3),
        });
      },
      onHoverEnd: () => setTooltip(null),
    };

    let result;
    if (viewMode === 'bubble') {
      result = renderBubbles(svgRef.current, activeData, dims, opts);
    } else if (viewMode === 'sankey') {
      result = renderSankey(svgRef.current, activeData, dims);
    } else {
      result = renderForce(svgRef.current, activeData, dims, opts);
    }

    if (result?.sim) simRef.current = result.sim;
    if (result?.zoom) zoomRef.current = result.zoom;

    return () => { if (simRef.current?.stop) simRef.current.stop(); };
  }, [activeData, dims, viewMode, drillInto]);

  // ── Search highlight ──────────────────────────────────────────
  useEffect(() => {
    if (!svgRef.current || !searchQuery.trim()) return;
    const q = searchQuery.toLowerCase();
    d3.select(svgRef.current).selectAll('.bubble').each(function(d) {
      const match = d && (d.name || '').toLowerCase().includes(q);
      d3.select(this).attr('opacity', match || !q ? 1 : 0.2);
    });
  }, [searchQuery]);

  // ── Time animation ────────────────────────────────────────────
  const toggleAnimation = useCallback(() => {
    if (animating) {
      clearInterval(animRef.current);
      setAnimating(false);
      return;
    }
    setAnimating(true);
    setTimeIdx(0);
    setSelectedDays(TIME_PERIODS[0].days);
    let step = 0;
    animRef.current = setInterval(() => {
      step++;
      if (step >= TIME_PERIODS.length) {
        clearInterval(animRef.current);
        setAnimating(false);
        return;
      }
      setTimeIdx(step);
      setSelectedDays(TIME_PERIODS[step].days);
    }, 2500);
  }, [animating]);

  useEffect(() => () => { if (animRef.current) clearInterval(animRef.current); }, []);

  // ── Fullscreen ────────────────────────────────────────────────
  const toggleFS = useCallback(() => {
    if (!isFullScreen) {
      containerRef.current?.parentElement?.requestFullscreen?.();
    } else {
      document.exitFullscreen?.();
    }
    setIsFullScreen(p => !p);
  }, [isFullScreen]);

  useEffect(() => {
    const h = () => setIsFullScreen(!!document.fullscreenElement);
    document.addEventListener('fullscreenchange', h);
    return () => document.removeEventListener('fullscreenchange', h);
  }, []);

  // ── Zoom controls ─────────────────────────────────────────────
  const zoomIn = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current).transition().duration(300).call(zoomRef.current.scaleBy, 1.3);
  }, []);
  const zoomOut = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current).transition().duration(300).call(zoomRef.current.scaleBy, 0.7);
  }, []);
  const fitScreen = useCallback(() => {
    if (!svgRef.current || !zoomRef.current) return;
    d3.select(svgRef.current).transition().duration(400).call(zoomRef.current.transform, d3.zoomIdentity);
  }, []);

  // ── Export ─────────────────────────────────────────────────────
  const handleExport = useCallback(() => {
    if (!svgRef.current) return;
    const svgData = new XMLSerializer().serializeToString(svgRef.current);
    const blob = new Blob([svgData], { type: 'image/svg+xml' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = 'money-flow.svg'; a.click();
    URL.revokeObjectURL(url);
  }, []);

  // ── Render ────────────────────────────────────────────────────
  if (loading) {
    return (
      <div style={{ ...sty.page, justifyContent: 'center', alignItems: 'center' }}>
        <div style={{ color: colors.textDim, fontSize: '13px' }}>Loading money flow data...</div>
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ ...sty.page, justifyContent: 'center', alignItems: 'center' }}>
        <div style={{ color: colors.red, fontSize: '13px', marginBottom: 12 }}>{error}</div>
        <button onClick={loadData} style={sty.actionBtn}>
          <RefreshCw size={12} /> Retry
        </button>
      </div>
    );
  }

  return (
    <div style={sty.page}>
      {/* ── Chart Area ─────────────────────────────────────── */}
      <div style={sty.chartWrap}>
        {/* View toggle (top-right) */}
        <div style={sty.viewToggle}>
          {VIEW_MODES.map(v => (
            <button
              key={v.id}
              onClick={() => setViewMode(v.id)}
              style={sty.viewBtn(viewMode === v.id)}
              title={v.label}
            >
              <v.Icon size={12} />
              <span>{v.label}</span>
            </button>
          ))}
        </div>

        {/* Zoom controls (top-left) */}
        <div style={sty.controls}>
          <button onClick={zoomIn} style={sty.ctrlBtn} title="Zoom in">
            <ZoomIn size={14} color={colors.textDim} />
          </button>
          <button onClick={zoomOut} style={sty.ctrlBtn} title="Zoom out">
            <ZoomOut size={14} color={colors.textDim} />
          </button>
          <button onClick={fitScreen} style={sty.ctrlBtn} title="Fit to screen">
            <Maximize2 size={14} color={colors.textDim} />
          </button>
          <button onClick={toggleFS} style={sty.ctrlBtn} title="Fullscreen">
            {isFullScreen ? <Minimize2 size={14} color={colors.textDim} /> : <Maximize2 size={14} color={colors.accent} />}
          </button>
          <button onClick={() => setSearchOpen(p => !p)} style={sty.ctrlBtn} title="Search">
            <Search size={14} color={colors.textDim} />
          </button>
        </div>

        {/* Search box */}
        {searchOpen && (
          <div style={sty.searchBox}>
            <Search size={12} color={colors.textDim} />
            <input
              style={sty.searchInput}
              placeholder="Search sectors..."
              value={searchQuery}
              onChange={e => setSearchQuery(e.target.value)}
              autoFocus
            />
          </div>
        )}

        {/* Breadcrumb for drill-down */}
        {drillLevel > 0 && (
          <div style={sty.breadcrumb}>
            <button onClick={drillBack} style={{ background: 'none', border: 'none', cursor: 'pointer', display: 'flex', alignItems: 'center' }}>
              <ChevronLeft size={12} color={colors.accent} />
            </button>
            {drillHistory.map((h, i) => <span key={i} style={{ color: colors.textDim }}>{h.label} <span style={{ color: colors.textMuted }}>/</span> </span>)}
            <span style={{ color: colors.accent }}>{drillTarget}</span>
          </div>
        )}
        {drillLoading && (
          <div style={{ position: 'absolute', inset: 0, display: 'flex', alignItems: 'center', justifyContent: 'center', background: B + 'BB', zIndex: 15 }}>
            <span style={{ color: colors.textDim, fontSize: '12px' }}>Loading...</span>
          </div>
        )}

        {/* Tooltip */}
        {tooltip && (
          <div style={{ ...sty.tooltip, left: tooltip.x + 12, top: tooltip.y - 60 }}>
            <div style={{ fontWeight: 700, marginBottom: 4 }}>{tooltip.name}</div>
            <div><span style={{ color: flowColor(tooltip.direction) }}>{tooltip.amount}</span> <span style={{ color: colors.textMuted }}>{tooltip.direction}</span></div>
            {tooltip.confidence && <div style={{ color: colors.textMuted, fontSize: '10px', marginTop: 2 }}>Confidence: {tooltip.confidence}</div>}
            {tooltip.actors?.length > 0 && <div style={{ marginTop: 4, fontSize: '10px', color: colors.textDim }}>
              {tooltip.actors.map((a, i) => <div key={i}>{a.name || a.actor}: {fmtDollar(a.net_flow || a.amount)}</div>)}
            </div>}
          </div>
        )}
        <div ref={containerRef} style={{ width: '100%', height: '100%' }}>
          <svg ref={svgRef} width={dims.width} height={dims.height} style={{ display: 'block' }} />
        </div>
      </div>

      {/* ── Time Slider ──────────────────────────────────── */}
      <div style={sty.sliderRow}>
        <button onClick={toggleAnimation} style={sty.playBtn} title={animating ? 'Pause' : 'Play'}>
          {animating
            ? <Pause size={12} color={colors.textDim} />
            : <Play size={12} color={colors.accent} />
          }
        </button>
        {TIME_PERIODS.map((tp, i) => (
          <button
            key={tp.label}
            onClick={() => { setTimeIdx(i); setSelectedDays(tp.days); }}
            style={sty.sliderBtn(timeIdx === i)}
          >
            {tp.label}
          </button>
        ))}
        <span style={{ marginLeft: 'auto', fontSize: '10px', color: colors.textMuted }}>
          {fmtDollar(levers.reduce((s, l) => s + Math.abs(l.net_flow), 0))} total flow
        </span>
      </div>

      {/* ── Levers + Actions ─────────────────────────────── */}
      <div style={sty.leversWrap}>
        <div style={sty.leversTitle}>THE LEVERS</div>
        {levers.map(l => (
          <div key={l.name} style={sty.leverRow}>
            <span style={{ width: 90, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: colors.textDim }}>
              {l.name}
            </span>
            <div style={{ flex: 1, display: 'flex', alignItems: 'center' }}>
              <div style={sty.leverBar(l.net_flow / maxLever, l.direction === 'inflow')} />
            </div>
            <span style={{ width: 70, textAlign: 'right', color: flowColor(l.direction), fontWeight: 600 }}>
              {fmtDollar(l.net_flow)}
            </span>
            {l.acceleration && (
              <span style={{ fontSize: '9px', color: l.acceleration === 'accelerating' ? FLOW_GREEN : FLOW_RED }}>
                {l.acceleration === 'accelerating' ? '\u25B2' : '\u25BC'}
              </span>
            )}
          </div>
        ))}

        {/* Action buttons */}
        <div style={sty.actionRow}>
          <button onClick={loadData} style={sty.actionBtn}>
            <RefreshCw size={11} color={colors.textDim} /> Refresh
          </button>
          <button onClick={handleExport} style={sty.actionBtn}>
            <Download size={11} color={colors.textDim} /> Export SVG
          </button>
        </div>
      </div>
    </div>
  );
}
