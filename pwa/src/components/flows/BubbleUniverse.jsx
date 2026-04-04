/**
 * BubbleUniverse — Conviction-colored sector bubbles with force simulation.
 * Uses v2 layer data. Replaces Math.random garbage with real rotation-matrix links.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { colors } from '../../styles/shared.js';
import { useFlowLayers, fmtDollar } from './useFlowData.js';
import { confidenceOpacity } from './FreshnessIndicator.jsx';
import FlowTooltip from './FlowTooltip.jsx';

const LAYER_COLORS = {
  monetary: '#6366F1', credit: '#3B82F6', institutional: '#14B8A6',
  market: '#22C55E', corporate: '#F59E0B', sovereign: '#EF4444',
  retail: '#EC4899', crypto: '#F97316',
};

function flowColor(change) {
  if (change == null) return colors.textDim;
  return change >= 0 ? colors.green : colors.red;
}

export default function BubbleUniverse() {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const simRef = useRef(null);
  const [dims, setDims] = useState({ width: 800, height: 500 });
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, node: null, edge: null });
  const { data, loading, error } = useFlowLayers();

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
    if (!data?.layers || !svgRef.current) return;

    const { width, height } = dims;
    const cx = width / 2, cy = height / 2;

    // Build bubble nodes from layers
    const bubbles = [];
    for (const layer of data.layers) {
      const layerColor = LAYER_COLORS[layer.id] || colors.accent;
      bubbles.push({
        id: layer.id,
        label: layer.label,
        value: Math.abs(layer.total_value_usd || 0),
        change: layer.net_flow_1m,
        confidence: layer.confidence,
        regime: layer.regime,
        stress: layer.stress_score,
        layerColor,
        nodeCount: (layer.nodes || []).length,
        nodes: layer.nodes || [],
      });
    }

    // Scale bubble radius by log of value
    const maxVal = d3.max(bubbles, d => d.value) || 1;
    const rScale = d3.scaleSqrt()
      .domain([0, Math.log10(maxVal + 1)])
      .range([20, Math.min(80, width / 10)]);

    const nodes = bubbles.map(d => ({
      ...d,
      r: rScale(Math.log10(d.value + 1)),
      x: cx + (Math.random() - 0.5) * 80,
      y: cy + (Math.random() - 0.5) * 80,
    }));

    // Links from edges
    const links = [];
    for (const edge of (data.edges || [])) {
      const src = nodes.find(n => n.id === edge.source_layer);
      const tgt = nodes.find(n => n.id === edge.target_layer);
      if (src && tgt && src !== tgt) {
        links.push({ source: src, target: tgt, value: edge.value_usd || 1 });
      }
    }

    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const g = svg.append('g');

    // Zoom
    svg.call(d3.zoom().scaleExtent([0.3, 5]).on('zoom', e => g.attr('transform', e.transform)));

    // Connection lines
    const maxLinkVal = d3.max(links, l => l.value) || 1;
    const linkG = g.append('g');
    const linkEls = linkG.selectAll('line').data(links).join('line')
      .attr('stroke', colors.textMuted + '40')
      .attr('stroke-width', d => Math.max(0.5, (d.value / maxLinkVal) * 4))
      .attr('stroke-dasharray', '4,4');

    // Bubble groups
    const bubbleG = g.append('g');
    const bubbleGroups = bubbleG.selectAll('g').data(nodes).join('g')
      .style('cursor', 'pointer');

    // Glow
    bubbleGroups.append('circle')
      .attr('r', d => d.r + 6)
      .attr('fill', d => flowColor(d.change))
      .attr('opacity', 0.08)
      .style('filter', 'blur(6px)');

    // Main circle
    bubbleGroups.append('circle')
      .attr('r', d => d.r)
      .attr('fill', d => flowColor(d.change))
      .attr('fill-opacity', d => confidenceOpacity(d.confidence) * 0.25)
      .attr('stroke', d => d.layerColor)
      .attr('stroke-width', 2)
      .attr('stroke-opacity', d => confidenceOpacity(d.confidence) * 0.8);

    // Label
    bubbleGroups.append('text')
      .attr('text-anchor', 'middle').attr('dy', '-0.3em')
      .attr('fill', colors.text).attr('font-size', '11px')
      .attr('font-weight', 700).attr('font-family', colors.mono)
      .text(d => d.label);

    // Value
    bubbleGroups.append('text')
      .attr('text-anchor', 'middle').attr('dy', '1em')
      .attr('fill', d => flowColor(d.change))
      .attr('font-size', '10px').attr('font-weight', 600).attr('font-family', colors.mono)
      .text(d => fmtDollar(d.value));

    // Regime badge
    bubbleGroups.append('text')
      .attr('text-anchor', 'middle').attr('dy', '2.2em')
      .attr('fill', colors.textDim).attr('font-size', '8px').attr('font-family', colors.mono)
      .text(d => d.regime || '');

    // Tooltip events
    bubbleGroups
      .on('mouseenter', (e, d) => {
        setTooltip({
          visible: true, x: e.clientX, y: e.clientY,
          node: { id: d.id, label: d.label, value: d.value, change_1m: d.change, confidence: d.confidence, layer: d.id },
          edge: null,
        });
      })
      .on('mousemove', e => setTooltip(t => ({ ...t, x: e.clientX, y: e.clientY })))
      .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

    // Force simulation
    if (simRef.current) simRef.current.stop();
    const sim = d3.forceSimulation(nodes)
      .force('charge', d3.forceManyBody().strength(-150))
      .force('center', d3.forceCenter(cx, cy))
      .force('collision', d3.forceCollide(d => d.r + 8))
      .force('link', d3.forceLink(links).distance(d => 100 + d.source.r + d.target.r).strength(0.3))
      .on('tick', () => {
        bubbleGroups.attr('transform', d => `translate(${d.x},${d.y})`);
        linkEls
          .attr('x1', d => d.source.x).attr('y1', d => d.source.y)
          .attr('x2', d => d.target.x).attr('y2', d => d.target.y);
      });
    simRef.current = sim;

    return () => sim.stop();
  }, [data, dims]);

  if (loading) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono }}>
        Loading bubble universe...
      </div>
    );
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }}>
      <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />
      <FlowTooltip {...tooltip} />
    </div>
  );
}
