/**
 * FlowSankey8 — 8-layer D3 Sankey with real flow data.
 *
 * Columns: Monetary → Credit → Institutional → Market → Corporate → Sovereign → Retail → Crypto
 * Nodes sized by value, edges by flow amount. Confidence → opacity.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { sankey, sankeyLinkHorizontal } from 'd3-sankey';
import { colors } from '../../styles/shared.js';
import { useFlowLayers, fmtDollar } from './useFlowData.js';
import { confidenceOpacity } from './FreshnessIndicator.jsx';
import FlowTooltip from './FlowTooltip.jsx';

const LAYER_COLORS = {
  monetary: '#6366F1',
  credit: '#3B82F6',
  institutional: '#14B8A6',
  market: '#22C55E',
  corporate: '#F59E0B',
  sovereign: '#EF4444',
  retail: '#EC4899',
  crypto: '#F97316',
};

const MARGIN = { top: 24, right: 20, bottom: 24, left: 20 };

export default function FlowSankey8({ width: propWidth, height: propHeight }) {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, node: null, edge: null });
  const [dims, setDims] = useState({ width: propWidth || 900, height: propHeight || 500 });
  const { data, loading, error, refetch } = useFlowLayers();

  // Auto-resize
  useEffect(() => {
    if (propWidth && propHeight) {
      setDims({ width: propWidth, height: propHeight });
      return;
    }
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      const { width, height } = entries[0].contentRect;
      if (width > 0 && height > 0) setDims({ width, height });
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, [propWidth, propHeight]);

  // Render Sankey
  useEffect(() => {
    if (!data || !svgRef.current) return;
    const { layers = [], edges = [] } = data;
    if (!layers.length) return;

    const { width, height } = dims;
    const iw = width - MARGIN.left - MARGIN.right;
    const ih = height - MARGIN.top - MARGIN.bottom;

    // Build sankey node/link structure
    const nodeMap = new Map();
    const sankeyNodes = [];
    const sankeyLinks = [];

    // Sort layers by order
    const sortedLayers = [...layers].sort((a, b) => a.order - b.order);

    for (const layer of sortedLayers) {
      for (const node of (layer.nodes || [])) {
        const nid = `${layer.id}:${node.id}`;
        const idx = sankeyNodes.length;
        nodeMap.set(nid, idx);
        sankeyNodes.push({
          name: nid,
          label: node.label || node.id,
          layerId: layer.id,
          layerLabel: layer.label,
          value: Math.abs(node.value || 0),
          confidence: node.confidence,
          change_1m: node.change_1m,
          change_1w: node.change_1w,
          z_score: node.z_score,
          rawNode: node,
        });
      }
    }

    // Build links from edges
    for (const edge of edges) {
      const src = nodeMap.get(`${edge.source_layer}:${edge.source_node}`);
      const tgt = nodeMap.get(`${edge.target_layer}:${edge.target_node}`);
      if (src != null && tgt != null && src !== tgt) {
        sankeyLinks.push({
          source: src,
          target: tgt,
          value: Math.max(edge.value_usd || 1, 1),
          confidence: edge.confidence,
          channel: edge.channel,
          rawEdge: edge,
        });
      }
    }

    // If we have no real edges, create synthetic connections between adjacent layers
    if (sankeyLinks.length === 0) {
      for (let i = 0; i < sortedLayers.length - 1; i++) {
        const srcLayer = sortedLayers[i];
        const tgtLayer = sortedLayers[i + 1];
        const srcNodes = (srcLayer.nodes || []);
        const tgtNodes = (tgtLayer.nodes || []);
        if (!srcNodes.length || !tgtNodes.length) continue;

        // Connect largest node in each layer
        const srcBest = srcNodes.reduce((a, b) => (Math.abs(a.value || 0) > Math.abs(b.value || 0) ? a : b));
        const tgtBest = tgtNodes.reduce((a, b) => (Math.abs(a.value || 0) > Math.abs(b.value || 0) ? a : b));
        const srcIdx = nodeMap.get(`${srcLayer.id}:${srcBest.id}`);
        const tgtIdx = nodeMap.get(`${tgtLayer.id}:${tgtBest.id}`);
        if (srcIdx != null && tgtIdx != null) {
          const flowVal = Math.min(Math.abs(srcBest.value || 1e9), Math.abs(tgtBest.value || 1e9)) * 0.3;
          sankeyLinks.push({
            source: srcIdx, target: tgtIdx,
            value: Math.max(flowVal, 1),
            confidence: 'estimated', channel: 'inferred',
            rawEdge: { source_layer: srcLayer.id, target_layer: tgtLayer.id, confidence: 'estimated' },
          });
        }
      }
    }

    // Ensure every node participates in at least one link
    const linked = new Set();
    for (const l of sankeyLinks) { linked.add(l.source); linked.add(l.target); }
    // Remove orphan nodes by filtering (sankey requires all nodes linked)
    const activeNodes = sankeyNodes.filter((_, i) => linked.has(i));
    const idxRemap = new Map();
    activeNodes.forEach((n, i) => {
      const oldIdx = sankeyNodes.indexOf(n);
      idxRemap.set(oldIdx, i);
    });
    const activeLinks = sankeyLinks
      .filter(l => idxRemap.has(l.source) && idxRemap.has(l.target))
      .map(l => ({ ...l, source: idxRemap.get(l.source), target: idxRemap.get(l.target) }));

    if (!activeNodes.length || !activeLinks.length) return;

    // Build sankey layout
    const sankeyGen = sankey()
      .nodeId(d => d.index)
      .nodeWidth(16)
      .nodePadding(8)
      .nodeSort(null)
      .extent([[0, 0], [iw, ih]]);

    const graph = sankeyGen({
      nodes: activeNodes.map(d => ({ ...d })),
      links: activeLinks.map(d => ({ ...d })),
    });

    // Render
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const g = svg.append('g').attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

    // Layer labels at top
    const layerPositions = new Map();
    for (const node of graph.nodes) {
      const lid = node.layerId;
      if (!layerPositions.has(lid)) layerPositions.set(lid, []);
      layerPositions.get(lid).push(node.x0);
    }
    for (const [lid, xs] of layerPositions) {
      const x = d3.mean(xs) + 8;
      const layer = sortedLayers.find(l => l.id === lid);
      g.append('text')
        .attr('x', x).attr('y', -8)
        .attr('text-anchor', 'middle')
        .attr('fill', LAYER_COLORS[lid] || colors.textDim)
        .attr('font-size', '10px').attr('font-weight', 700)
        .attr('font-family', colors.mono)
        .text((layer?.label || lid).toUpperCase());
    }

    // Links
    const linkPath = sankeyLinkHorizontal();
    g.append('g').attr('class', 'links')
      .selectAll('path')
      .data(graph.links)
      .join('path')
      .attr('d', linkPath)
      .attr('fill', 'none')
      .attr('stroke', d => LAYER_COLORS[d.source.layerId] || colors.accent)
      .attr('stroke-width', d => Math.max(1, d.width))
      .attr('stroke-opacity', d => confidenceOpacity(d.confidence) * 0.35)
      .on('mouseenter', (e, d) => {
        setTooltip({ visible: true, x: e.clientX, y: e.clientY, edge: d.rawEdge, node: null });
      })
      .on('mousemove', (e) => setTooltip(t => ({ ...t, x: e.clientX, y: e.clientY })))
      .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

    // Nodes
    const nodeG = g.append('g').attr('class', 'nodes')
      .selectAll('g')
      .data(graph.nodes)
      .join('g');

    // Node rect
    nodeG.append('rect')
      .attr('x', d => d.x0).attr('y', d => d.y0)
      .attr('width', d => d.x1 - d.x0)
      .attr('height', d => Math.max(d.y1 - d.y0, 2))
      .attr('fill', d => LAYER_COLORS[d.layerId] || colors.accent)
      .attr('fill-opacity', d => confidenceOpacity(d.confidence) * 0.85)
      .attr('rx', 3)
      .style('cursor', 'pointer')
      .on('mouseenter', (e, d) => {
        setTooltip({ visible: true, x: e.clientX, y: e.clientY, node: d.rawNode, edge: null });
      })
      .on('mousemove', (e) => setTooltip(t => ({ ...t, x: e.clientX, y: e.clientY })))
      .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

    // Node labels
    nodeG.append('text')
      .attr('x', d => d.x0 < iw / 2 ? d.x1 + 6 : d.x0 - 6)
      .attr('y', d => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', d => d.x0 < iw / 2 ? 'start' : 'end')
      .attr('fill', colors.textDim)
      .attr('font-size', '9px')
      .attr('font-family', colors.mono)
      .text(d => {
        const maxLen = 18;
        const lbl = d.label || d.name;
        return lbl.length > maxLen ? lbl.slice(0, maxLen) + '...' : lbl;
      });

    // Value labels on nodes
    nodeG.filter(d => (d.y1 - d.y0) > 20)
      .append('text')
      .attr('x', d => (d.x0 + d.x1) / 2)
      .attr('y', d => (d.y0 + d.y1) / 2)
      .attr('dy', '0.35em')
      .attr('text-anchor', 'middle')
      .attr('fill', '#fff')
      .attr('font-size', '8px')
      .attr('font-weight', 700)
      .attr('font-family', colors.mono)
      .text(d => fmtDollar(d.value));

  }, [data, dims]);

  if (loading) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
        Loading flow map...
      </div>
    );
  }

  if (error) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.red, fontFamily: colors.mono, fontSize: '12px' }}>
        {error}
      </div>
    );
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: '100%', position: 'relative' }}>
      <svg ref={svgRef} style={{ width: '100%', height: '100%' }} />
      <FlowTooltip {...tooltip} />
      {data && (
        <div style={{
          position: 'absolute', bottom: 8, right: 8,
          fontSize: '9px', fontFamily: colors.mono, color: colors.textDim,
          display: 'flex', gap: 12,
        }}>
          <span>Liquidity: {fmtDollar(data.global_liquidity_total)}</span>
          {data.global_policy_score != null && <span>Policy: {data.global_policy_score?.toFixed(2)}</span>}
        </div>
      )}
    </div>
  );
}
