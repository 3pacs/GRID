/**
 * LivingGraph — Universal renderer for VizSpec objects.
 *
 * Takes a visualization specification from the backend and renders the
 * optimal chart type with proper animations, weight schedules, and
 * real-time updates.
 *
 * The key insight: the SYSTEM chooses the chart type, not the developer.
 * Capital flows → Sankey + time scrubber. Regime → phase space.
 * Correlations → force network. The VizSpec encodes this knowledge.
 *
 * Weight schedules make data sources "breathe" at their natural cadence:
 * real-time equity data pulses fast, monthly macro is a slow heartbeat.
 */

import { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { useStore } from '../store';

// ── Shared Styles ──────────────────────────────────────────────────────────

const COLORS = {
  bg: '#0a0e14',
  surface: '#111820',
  border: '#1e2a38',
  text: '#c8d6e5',
  textMuted: '#5a7080',
  accent: '#4fc3f7',
  positive: '#22c55e',
  negative: '#ef4444',
  warning: '#f59e0b',
  regime: {
    GROWTH: '#22c55e',
    NEUTRAL: '#f59e0b',
    FRAGILE: '#f97316',
    CRISIS: '#ef4444',
  },
};

// ── Time Scrubber ──────────────────────────────────────────────────────────

function TimeScrubber({ dates, currentIndex, onChange, autoPlay, playSpeed, onTogglePlay, isPlaying }) {
  const pct = dates.length > 1 ? (currentIndex / (dates.length - 1)) * 100 : 0;

  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 12, padding: '8px 0',
      borderTop: `1px solid ${COLORS.border}`, marginTop: 8,
    }}>
      <button
        onClick={onTogglePlay}
        style={{
          background: 'none', border: `1px solid ${COLORS.border}`,
          color: COLORS.text, borderRadius: 4, padding: '4px 12px',
          cursor: 'pointer', fontSize: 12, minWidth: 48,
        }}
      >
        {isPlaying ? '⏸' : '▶'}
      </button>
      <input
        type="range"
        min={0}
        max={Math.max(0, dates.length - 1)}
        value={currentIndex}
        onChange={e => onChange(parseInt(e.target.value))}
        style={{ flex: 1, accentColor: COLORS.accent }}
      />
      <span style={{ fontSize: 11, color: COLORS.textMuted, minWidth: 80, textAlign: 'right' }}>
        {dates[currentIndex] || '—'}
      </span>
    </div>
  );
}

// ── Weight Pulse Indicator ─────────────────────────────────────────────────

function WeightIndicator({ schedules, weights }) {
  if (!schedules || schedules.length === 0) return null;

  return (
    <div style={{
      display: 'flex', gap: 8, flexWrap: 'wrap', padding: '4px 0',
      borderBottom: `1px solid ${COLORS.border}`, marginBottom: 8,
    }}>
      {schedules.map((s, i) => {
        const w = weights?.[s.source] ?? s.peak_weight;
        const opacity = 0.3 + w * 0.7;
        return (
          <div key={i} style={{
            display: 'flex', alignItems: 'center', gap: 4, fontSize: 10,
            color: COLORS.textMuted, opacity,
            transition: 'opacity 0.5s ease',
          }}>
            <div style={{
              width: 6, height: 6, borderRadius: '50%',
              background: s.pulse_on_update ? COLORS.accent : COLORS.textMuted,
              animation: s.pulse_on_update && w > 0.7 ? 'pulse 2s infinite' : 'none',
            }} />
            <span>{s.source}</span>
            <span style={{ color: COLORS.text }}>{s.cadence}</span>
          </div>
        );
      })}
      <style>{`
        @keyframes pulse {
          0%, 100% { opacity: 0.4; transform: scale(1); }
          50% { opacity: 1; transform: scale(1.5); }
        }
      `}</style>
    </div>
  );
}

// ── Phase Space Renderer ───────────────────────────────────────────────────

function PhaseSpace({ spec, data, width, height }) {
  const svgRef = useRef(null);
  const [timeIdx, setTimeIdx] = useState(0);
  const [isPlaying, setIsPlaying] = useState(spec.animation?.auto_play ?? false);

  const points = data?.trajectory || [];
  const dates = points.map(p => p.date);

  useEffect(() => {
    if (!isPlaying || points.length === 0) return;
    const iv = setInterval(() => {
      setTimeIdx(prev => {
        if (prev >= points.length - 1) { setIsPlaying(false); return prev; }
        return prev + 1;
      });
    }, spec.animation?.play_speed_ms || 200);
    return () => clearInterval(iv);
  }, [isPlaying, points.length]);

  useEffect(() => {
    if (!svgRef.current || points.length === 0) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const margin = { top: 20, right: 20, bottom: 30, left: 40 };
    const w = width - margin.left - margin.right;
    const h = height - margin.top - margin.bottom;
    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    const xExtent = d3.extent(points, p => p.pc1);
    const yExtent = d3.extent(points, p => p.pc2);
    const x = d3.scaleLinear().domain(xExtent).range([0, w]).nice();
    const y = d3.scaleLinear().domain(yExtent).range([h, 0]).nice();

    // Axes
    g.append('g').attr('transform', `translate(0,${h})`).call(d3.axisBottom(x).ticks(5))
      .selectAll('text').style('fill', COLORS.textMuted).style('font-size', '10px');
    g.append('g').call(d3.axisLeft(y).ticks(5))
      .selectAll('text').style('fill', COLORS.textMuted).style('font-size', '10px');

    // Trail
    const trailLen = spec.animation?.trail_length || 20;
    const trailStart = Math.max(0, timeIdx - trailLen);
    const trailPoints = points.slice(trailStart, timeIdx + 1);

    if (trailPoints.length > 1) {
      const line = d3.line().x(p => x(p.pc1)).y(p => y(p.pc2)).curve(d3.curveCatmullRom);
      g.append('path')
        .datum(trailPoints)
        .attr('d', line)
        .attr('fill', 'none')
        .attr('stroke', COLORS.accent)
        .attr('stroke-width', 2)
        .attr('stroke-opacity', spec.animation?.trail_opacity || 0.2);
    }

    // Trail dots (fading)
    trailPoints.forEach((p, i) => {
      const opacity = (i / trailPoints.length) * 0.6;
      const color = COLORS.regime[p.regime_state] || COLORS.accent;
      g.append('circle')
        .attr('cx', x(p.pc1)).attr('cy', y(p.pc2))
        .attr('r', 3).attr('fill', color).attr('opacity', opacity);
    });

    // Current position (large, bright)
    const current = points[timeIdx];
    if (current) {
      const color = COLORS.regime[current.regime_state] || COLORS.accent;
      g.append('circle')
        .attr('cx', x(current.pc1)).attr('cy', y(current.pc2))
        .attr('r', 8).attr('fill', color).attr('opacity', 0.9)
        .attr('stroke', '#fff').attr('stroke-width', 2);

      // Label
      g.append('text')
        .attr('x', x(current.pc1) + 12).attr('y', y(current.pc2) + 4)
        .text(current.regime_state)
        .style('fill', color).style('font-size', '11px').style('font-weight', 'bold');
    }
  }, [timeIdx, points, width, height]);

  return (
    <div>
      <svg ref={svgRef} width={width} height={height}
        style={{ background: COLORS.bg, borderRadius: 8 }} />
      {spec.time_scrubber && dates.length > 0 && (
        <TimeScrubber
          dates={dates} currentIndex={timeIdx} onChange={setTimeIdx}
          isPlaying={isPlaying} onTogglePlay={() => setIsPlaying(!isPlaying)}
          playSpeed={spec.animation?.play_speed_ms}
        />
      )}
    </div>
  );
}

// ── Force Network Renderer ─────────────────────────────────────────────────

function ForceNetwork({ spec, data, width, height }) {
  const svgRef = useRef(null);
  const simulationRef = useRef(null);

  useEffect(() => {
    if (!svgRef.current || !data?.nodes) return;
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    const nodes = data.nodes.map(n => ({ ...n }));
    const links = data.links.map(l => ({ ...l }));

    const colorScale = d3.scaleOrdinal(d3.schemeTableau10);

    // Force simulation — weights become spring constants
    const sim = d3.forceSimulation(nodes)
      .force('link', d3.forceLink(links).id(d => d.id)
        .distance(d => 80 / (Math.abs(d.weight || 0.1) + 0.1))
        .strength(d => Math.abs(d.weight || 0.1)))
      .force('charge', d3.forceManyBody()
        .strength(d => -(d.importance || 1) * 100))
      .force('center', d3.forceCenter(width / 2, height / 2))
      .force('collision', d3.forceCollide().radius(d => (d.importance || 1) * 15 + 5));

    simulationRef.current = sim;

    // Links
    const link = svg.append('g')
      .selectAll('line').data(links).join('line')
      .attr('stroke', COLORS.border)
      .attr('stroke-opacity', d => Math.abs(d.weight || 0.3) * 0.8)
      .attr('stroke-width', d => Math.abs(d.weight || 0.5) * 3);

    // Nodes
    const node = svg.append('g')
      .selectAll('circle').data(nodes).join('circle')
      .attr('r', d => Math.max(4, (d.importance || 1) * 12))
      .attr('fill', d => colorScale(d.family || d.group || 0))
      .attr('stroke', '#fff').attr('stroke-width', 1)
      .attr('opacity', 0.85)
      .call(d3.drag()
        .on('start', (e, d) => { if (!e.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (e, d) => { d.fx = e.x; d.fy = e.y; })
        .on('end', (e, d) => { if (!e.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));

    // Labels
    const label = svg.append('g')
      .selectAll('text').data(nodes).join('text')
      .text(d => d.name || d.id)
      .style('fill', COLORS.textMuted).style('font-size', '9px')
      .attr('dx', 12).attr('dy', 4);

    sim.on('tick', () => {
      link.attr('x1', d => d.source.x).attr('y1', d => d.source.y)
          .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      node.attr('cx', d => d.x).attr('cy', d => d.y);
      label.attr('x', d => d.x).attr('y', d => d.y);
    });

    return () => sim.stop();
  }, [data, width, height]);

  return (
    <svg ref={svgRef} width={width} height={height}
      style={{ background: COLORS.bg, borderRadius: 8 }} />
  );
}

// ── Orbital Renderer (Sector Rotation) ─────────────────────────────────────

function Orbital({ spec, data, width, height }) {
  const canvasRef = useRef(null);
  const [timeIdx, setTimeIdx] = useState(0);
  const [isPlaying, setIsPlaying] = useState(spec.animation?.auto_play ?? false);

  const snapshots = data?.snapshots || [];
  const dates = snapshots.map(s => s.date);

  useEffect(() => {
    if (!isPlaying || snapshots.length === 0) return;
    const iv = setInterval(() => {
      setTimeIdx(prev => prev >= snapshots.length - 1 ? 0 : prev + 1);
    }, spec.animation?.play_speed_ms || 200);
    return () => clearInterval(iv);
  }, [isPlaying, snapshots.length]);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || snapshots.length === 0) return;
    const ctx = canvas.getContext('2d');
    const dpr = window.devicePixelRatio || 1;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);

    const cx = width / 2;
    const cy = height / 2;
    const maxR = Math.min(cx, cy) - 40;

    // Clear
    ctx.fillStyle = COLORS.bg;
    ctx.fillRect(0, 0, width, height);

    // Center (SPY)
    ctx.beginPath();
    ctx.arc(cx, cy, 6, 0, Math.PI * 2);
    ctx.fillStyle = COLORS.accent;
    ctx.fill();
    ctx.fillStyle = COLORS.textMuted;
    ctx.font = '10px monospace';
    ctx.fillText('SPY', cx + 10, cy + 4);

    // Orbit rings
    for (let ring of [0.25, 0.5, 0.75, 1.0]) {
      ctx.beginPath();
      ctx.arc(cx, cy, maxR * ring, 0, Math.PI * 2);
      ctx.strokeStyle = COLORS.border;
      ctx.lineWidth = 0.5;
      ctx.stroke();
    }

    // Current snapshot sectors
    const snap = snapshots[timeIdx];
    if (!snap?.sectors) return;

    const sectors = Object.entries(snap.sectors);
    const angleStep = (Math.PI * 2) / sectors.length;

    sectors.forEach(([name, sector], i) => {
      const angle = angleStep * i - Math.PI / 2;
      const perf = sector.relative_strength || 0;
      const dist = Math.min(1, Math.max(0.05, Math.abs(perf) / 10)) * maxR;

      const sx = cx + Math.cos(angle) * dist;
      const sy = cy + Math.sin(angle) * dist;
      const color = perf > 0 ? COLORS.positive : perf < 0 ? COLORS.negative : COLORS.textMuted;
      const r = Math.max(8, Math.min(20, (sector.volume || 1) / 1e8));

      // Trail (previous positions)
      const trailLen = Math.min(timeIdx, spec.animation?.trail_length || 30);
      for (let t = Math.max(0, timeIdx - trailLen); t < timeIdx; t++) {
        const prevSnap = snapshots[t];
        if (!prevSnap?.sectors?.[name]) continue;
        const prevPerf = prevSnap.sectors[name].relative_strength || 0;
        const prevDist = Math.min(1, Math.max(0.05, Math.abs(prevPerf) / 10)) * maxR;
        const px = cx + Math.cos(angle) * prevDist;
        const py = cy + Math.sin(angle) * prevDist;
        const alpha = ((t - (timeIdx - trailLen)) / trailLen) * (spec.animation?.trail_opacity || 0.2);
        ctx.beginPath();
        ctx.arc(px, py, 3, 0, Math.PI * 2);
        ctx.fillStyle = color;
        ctx.globalAlpha = alpha;
        ctx.fill();
      }

      // Current position
      ctx.globalAlpha = 0.85;
      ctx.beginPath();
      ctx.arc(sx, sy, r, 0, Math.PI * 2);
      ctx.fillStyle = color;
      ctx.fill();
      ctx.strokeStyle = '#fff';
      ctx.lineWidth = 1.5;
      ctx.stroke();

      // Label
      ctx.globalAlpha = 1;
      ctx.fillStyle = COLORS.text;
      ctx.font = 'bold 10px monospace';
      ctx.fillText(sector.etf || name.slice(0, 4), sx + r + 4, sy + 4);
    });

    ctx.globalAlpha = 1;
  }, [timeIdx, snapshots, width, height]);

  return (
    <div>
      <canvas ref={canvasRef} style={{ width, height, borderRadius: 8 }} />
      {spec.time_scrubber && dates.length > 0 && (
        <TimeScrubber
          dates={dates} currentIndex={timeIdx} onChange={setTimeIdx}
          isPlaying={isPlaying} onTogglePlay={() => setIsPlaying(!isPlaying)}
        />
      )}
    </div>
  );
}

// ── Main LivingGraph Component ─────────────────────────────────────────────

export default function LivingGraph({ spec, data, width = 600, height = 400 }) {
  const [sourceWeights, setSourceWeights] = useState({});

  // Fetch source weights on mount
  useEffect(() => {
    if (!spec?.weight_schedules?.length) return;
    const families = spec.weight_schedules.map(s => s.source).join(',');
    fetch(`/api/v1/viz/weights?families=${families}`)
      .then(r => r.json())
      .then(d => setSourceWeights(d.weights || {}))
      .catch(() => {});
  }, [spec]);

  if (!spec) return null;

  const chartType = spec.chart_type;

  return (
    <div style={{
      background: COLORS.surface,
      border: `1px solid ${COLORS.border}`,
      borderRadius: 12,
      padding: 16,
      position: 'relative',
    }}>
      {/* Title */}
      <div style={{ marginBottom: 8 }}>
        <div style={{ fontSize: 16, fontWeight: 700, color: COLORS.text }}>{spec.title}</div>
        {spec.subtitle && (
          <div style={{ fontSize: 11, color: COLORS.textMuted, marginTop: 2 }}>{spec.subtitle}</div>
        )}
      </div>

      {/* Weight indicators */}
      <WeightIndicator schedules={spec.weight_schedules} weights={sourceWeights} />

      {/* Chart renderer by type */}
      {chartType === 'phase_space' && (
        <PhaseSpace spec={spec} data={data} width={width} height={height} />
      )}
      {chartType === 'force_network' && (
        <ForceNetwork spec={spec} data={data} width={width} height={height} />
      )}
      {chartType === 'orbital' && (
        <Orbital spec={spec} data={data} width={width} height={height} />
      )}
      {(chartType === 'sankey_temporal') && (
        <div style={{ color: COLORS.textMuted, padding: 40, textAlign: 'center' }}>
          Use CapitalFlowSankey component with time scrubber
        </div>
      )}
      {!['phase_space', 'force_network', 'orbital', 'sankey_temporal'].includes(chartType) && (
        <div style={{ color: COLORS.textMuted, padding: 40, textAlign: 'center', fontSize: 12 }}>
          Renderer: <strong>{chartType}</strong> — spec ready, renderer loading
        </div>
      )}

      {/* Narrative overlay */}
      {spec.narrative_overlay && (
        <div style={{
          position: 'absolute', bottom: 16, left: 16, right: 16,
          background: 'rgba(10, 14, 20, 0.85)', borderRadius: 8,
          padding: '8px 12px', fontSize: 11, color: COLORS.text,
          backdropFilter: 'blur(8px)',
        }}>
          {spec.narrative_overlay}
        </div>
      )}
    </div>
  );
}

// Export sub-components for direct use
export { PhaseSpace, ForceNetwork, Orbital, TimeScrubber, WeightIndicator };
