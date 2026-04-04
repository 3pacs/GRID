/**
 * OrthogonalityView — PCA scatter with K-means clustering.
 * Shows which junction points are redundant vs orthogonal.
 */
import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { colors } from '../../styles/shared.js';
import { useFlowOrthogonality } from './useFlowData.js';
import FlowTooltip from './FlowTooltip.jsx';

const CLUSTER_COLORS = ['#6366F1', '#22C55E', '#F59E0B', '#EF4444', '#EC4899', '#14B8A6', '#F97316', '#3B82F6'];

export default function OrthogonalityView() {
  const { data, loading, error, refetch } = useFlowOrthogonality();
  const svgRef = useRef(null);
  const containerRef = useRef(null);
  const [dims, setDims] = useState({ width: 800, height: 400 });
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, node: null, edge: null });

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

  useEffect(() => {
    if (!data?.components || !svgRef.current) return;
    const { components, clusters, explained_variance, correlation_matrix } = data;
    if (components.length < 2) return;

    const { width, height } = dims;
    const margin = { top: 30, right: 30, bottom: 40, left: 50 };
    const iw = width - margin.left - margin.right;
    const ih = height - margin.top - margin.bottom;

    // PCA scatter — use first two components
    const pc1 = components.map(c => c.pc1 ?? c[0] ?? 0);
    const pc2 = components.map(c => c.pc2 ?? c[1] ?? 0);
    const labels = components.map(c => c.label || c.id || '');
    const clusterIds = clusters || components.map(() => 0);

    const xExtent = d3.extent(pc1);
    const yExtent = d3.extent(pc2);
    const xPad = (xExtent[1] - xExtent[0]) * 0.15 || 1;
    const yPad = (yExtent[1] - yExtent[0]) * 0.15 || 1;

    const xScale = d3.scaleLinear()
      .domain([xExtent[0] - xPad, xExtent[1] + xPad])
      .range([0, iw]);
    const yScale = d3.scaleLinear()
      .domain([yExtent[0] - yPad, yExtent[1] + yPad])
      .range([ih, 0]);

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

    // Grid lines
    g.append('g').attr('class', 'grid-x')
      .selectAll('line')
      .data(xScale.ticks(6))
      .join('line')
      .attr('x1', d => xScale(d)).attr('x2', d => xScale(d))
      .attr('y1', 0).attr('y2', ih)
      .attr('stroke', colors.border).attr('stroke-opacity', 0.3);
    g.append('g').attr('class', 'grid-y')
      .selectAll('line')
      .data(yScale.ticks(6))
      .join('line')
      .attr('x1', 0).attr('x2', iw)
      .attr('y1', d => yScale(d)).attr('y2', d => yScale(d))
      .attr('stroke', colors.border).attr('stroke-opacity', 0.3);

    // Axes
    g.append('g').attr('transform', `translate(0,${ih})`)
      .call(d3.axisBottom(xScale).ticks(5).tickSize(0).tickPadding(8))
      .call(g => g.select('.domain').attr('stroke', colors.border))
      .call(g => g.selectAll('.tick text').attr('fill', colors.textDim).attr('font-size', '9px').attr('font-family', colors.mono));
    g.append('g')
      .call(d3.axisLeft(yScale).ticks(5).tickSize(0).tickPadding(8))
      .call(g => g.select('.domain').attr('stroke', colors.border))
      .call(g => g.selectAll('.tick text').attr('fill', colors.textDim).attr('font-size', '9px').attr('font-family', colors.mono));

    // Axis labels
    g.append('text')
      .attr('x', iw / 2).attr('y', ih + 32)
      .attr('text-anchor', 'middle')
      .attr('fill', colors.textDim).attr('font-size', '10px').attr('font-family', colors.mono)
      .text(`PC1 (${explained_variance?.[0] ? (explained_variance[0] * 100).toFixed(0) + '%' : '?'})`);
    g.append('text')
      .attr('transform', `translate(-36,${ih / 2}) rotate(-90)`)
      .attr('text-anchor', 'middle')
      .attr('fill', colors.textDim).attr('font-size', '10px').attr('font-family', colors.mono)
      .text(`PC2 (${explained_variance?.[1] ? (explained_variance[1] * 100).toFixed(0) + '%' : '?'})`);

    // Points
    const points = g.append('g').attr('class', 'points')
      .selectAll('g')
      .data(pc1.map((_, i) => i))
      .join('g')
      .attr('transform', d => `translate(${xScale(pc1[d])},${yScale(pc2[d])})`);

    // Glow
    points.append('circle')
      .attr('r', 10)
      .attr('fill', d => CLUSTER_COLORS[clusterIds[d] % CLUSTER_COLORS.length])
      .attr('opacity', 0.15)
      .attr('filter', 'blur(4px)');

    // Dot
    points.append('circle')
      .attr('r', 5)
      .attr('fill', d => CLUSTER_COLORS[clusterIds[d] % CLUSTER_COLORS.length])
      .attr('stroke', '#fff').attr('stroke-width', 1)
      .style('cursor', 'pointer')
      .on('mouseenter', (e, d) => {
        setTooltip({ visible: true, x: e.clientX, y: e.clientY, node: { id: labels[d], label: labels[d], value: null, confidence: 'derived' }, edge: null });
      })
      .on('mousemove', (e) => setTooltip(t => ({ ...t, x: e.clientX, y: e.clientY })))
      .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

    // Labels
    points.append('text')
      .attr('x', 8).attr('dy', '0.35em')
      .attr('fill', colors.textDim)
      .attr('font-size', '8px').attr('font-family', colors.mono)
      .text(d => labels[d]?.slice(0, 16) || '');

  }, [data, dims]);

  if (loading) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
        Computing orthogonality...
      </div>
    );
  }

  if (error || data?.warning) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.yellow, fontFamily: colors.mono, fontSize: '12px' }}>
        {error || data?.warning}
      </div>
    );
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }}>
      <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />
      <FlowTooltip {...tooltip} />
      {data?.explained_variance && (
        <div style={{
          position: 'absolute', top: 8, right: 8,
          fontSize: '9px', fontFamily: colors.mono, color: colors.textDim,
          background: colors.bg + 'CC', padding: '4px 8px', borderRadius: 4,
        }}>
          Total variance explained: {((data.explained_variance[0] || 0) + (data.explained_variance[1] || 0) * 100).toFixed(0)}%
        </div>
      )}
    </div>
  );
}
