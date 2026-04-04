/**
 * FlowWaterfall — Trace money from a source node through all 8 layers.
 * Shows cascade with attenuation at each step.
 */
import React, { useState, useRef, useEffect } from 'react';
import * as d3 from 'd3';
import { colors } from '../../styles/shared.js';
import { useFlowWaterfall, fmtDollar } from './useFlowData.js';
import FreshnessIndicator from './FreshnessIndicator.jsx';

const LAYER_COLORS = {
  monetary: '#6366F1', credit: '#3B82F6', institutional: '#14B8A6',
  market: '#22C55E', corporate: '#F59E0B', sovereign: '#EF4444',
  retail: '#EC4899', crypto: '#F97316',
};

const SOURCE_OPTIONS = [
  { id: 'fed', label: 'Federal Reserve' },
  { id: 'reverse_repo', label: 'Reverse Repo' },
  { id: 'tga_balance', label: 'Treasury General Account' },
  { id: 'global_m2', label: 'Global M2' },
];

export default function FlowWaterfall() {
  const [source, setSource] = useState('fed');
  const { data, loading, error, refetch } = useFlowWaterfall(source);
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const [dims, setDims] = useState({ width: 800, height: 400 });

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 50) setDims({ width, height: Math.max(height - 60, 200) });
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, []);

  useEffect(() => {
    if (!data?.chain || !svgRef.current) return;
    const { chain } = data;
    if (chain.length < 2) return;

    const { width, height } = dims;
    const margin = { top: 40, right: 30, bottom: 30, left: 30 };
    const iw = width - margin.left - margin.right;
    const ih = height - margin.top - margin.bottom;

    const maxVal = d3.max(chain, d => d.value) || 1;

    const xScale = d3.scaleBand()
      .domain(chain.map((_, i) => i))
      .range([0, iw])
      .padding(0.15);

    const yScale = d3.scaleLinear()
      .domain([0, maxVal * 1.1])
      .range([ih, 0]);

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Waterfall bars
    const barW = xScale.bandwidth();

    chain.forEach((step, i) => {
      const layerColor = LAYER_COLORS[step.layer] || colors.accent;
      const barH = ih - yScale(step.value);
      const x = xScale(i);
      const y = yScale(step.value);

      // Bar
      g.append('rect')
        .attr('x', x).attr('y', y)
        .attr('width', barW).attr('height', barH)
        .attr('fill', layerColor)
        .attr('fill-opacity', 0.7)
        .attr('rx', 4);

      // Glow
      g.append('rect')
        .attr('x', x - 2).attr('y', y - 2)
        .attr('width', barW + 4).attr('height', barH + 4)
        .attr('fill', 'none')
        .attr('stroke', layerColor)
        .attr('stroke-opacity', 0.2)
        .attr('rx', 6)
        .attr('filter', 'blur(4px)');

      // Value label
      g.append('text')
        .attr('x', x + barW / 2).attr('y', y - 6)
        .attr('text-anchor', 'middle')
        .attr('fill', colors.text)
        .attr('font-size', '10px').attr('font-weight', 700)
        .attr('font-family', colors.mono)
        .text(fmtDollar(step.value));

      // Layer label
      g.append('text')
        .attr('x', x + barW / 2).attr('y', ih + 14)
        .attr('text-anchor', 'middle')
        .attr('fill', layerColor)
        .attr('font-size', '9px').attr('font-weight', 600)
        .attr('font-family', colors.mono)
        .text((step.label || step.layer).slice(0, 12));

      // Attenuation arrow between bars
      if (i > 0) {
        const prevX = xScale(i - 1) + barW;
        const curX = x;
        const midY = yScale(step.value) + barH / 2;
        const att = step.attenuation;
        g.append('line')
          .attr('x1', prevX + 2).attr('y1', midY)
          .attr('x2', curX - 2).attr('y2', midY)
          .attr('stroke', colors.textDim)
          .attr('stroke-width', 1)
          .attr('stroke-dasharray', '3,3')
          .attr('marker-end', 'url(#arrow)');

        // Attenuation label
        if (att > 0) {
          g.append('text')
            .attr('x', (prevX + curX) / 2).attr('y', midY - 6)
            .attr('text-anchor', 'middle')
            .attr('fill', att > 0.5 ? colors.red : colors.yellow)
            .attr('font-size', '8px').attr('font-weight', 600)
            .attr('font-family', colors.mono)
            .text(`-${(att * 100).toFixed(0)}%`);
        }
      }
    });

    // Arrow marker def
    svg.append('defs').append('marker')
      .attr('id', 'arrow').attr('viewBox', '0 0 10 10')
      .attr('refX', 8).attr('refY', 5)
      .attr('markerWidth', 6).attr('markerHeight', 6)
      .attr('orient', 'auto-start-reverse')
      .append('path')
      .attr('d', 'M 0 0 L 10 5 L 0 10 z')
      .attr('fill', colors.textDim);

  }, [data, dims]);

  return (
    <div ref={containerRef} style={{ height: '100%', display: 'flex', flexDirection: 'column' }}>
      {/* Source selector */}
      <div style={{ display: 'flex', gap: 6, padding: '8px 12px', flexWrap: 'wrap' }}>
        {SOURCE_OPTIONS.map(opt => (
          <button key={opt.id} onClick={() => setSource(opt.id)} style={{
            background: source === opt.id ? colors.accent + '30' : 'transparent',
            border: `1px solid ${source === opt.id ? colors.accent : colors.border}`,
            borderRadius: 6, padding: '4px 10px', cursor: 'pointer',
            fontSize: '10px', fontWeight: 600, fontFamily: colors.mono,
            color: source === opt.id ? colors.accent : colors.textDim,
          }}>
            {opt.label}
          </button>
        ))}
      </div>

      {loading && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
          Tracing flow from {source}...
        </div>
      )}

      {error && (
        <div style={{ flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.red, fontFamily: colors.mono, fontSize: '12px' }}>
          {error}
        </div>
      )}

      {!loading && !error && (
        <div style={{ flex: 1, minHeight: 0 }}>
          <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />
        </div>
      )}

      {data && (
        <div style={{ padding: '4px 12px 8px', fontSize: '9px', fontFamily: colors.mono, color: colors.textDim, display: 'flex', gap: 16 }}>
          <span>Source: {data.source}</span>
          <span>Starting: {fmtDollar(data.starting_value)}</span>
          <span>Steps: {data.chain?.length || 0}</span>
        </div>
      )}
    </div>
  );
}
