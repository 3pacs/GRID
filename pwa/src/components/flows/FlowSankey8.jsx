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

const MARGIN = { top: 28, right: 24, bottom: 16, left: 24 };

/**
 * Log-scale a value for Sankey sizing.
 * Raw values span $0 to $12T — impossible to visualize linearly.
 * Log10 compresses the range so all nodes/edges are visible.
 */
function logScale(v) {
  const abs = Math.abs(v || 0);
  if (abs < 1) return 1;
  return Math.log10(abs + 1);
}

export default function FlowSankey8({ width: propWidth, height: propHeight }) {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, node: null, edge: null });
  const [dims, setDims] = useState({ width: propWidth || 900, height: propHeight || 500 });
  const { data, loading, error, refetch } = useFlowLayers();

  // Auto-resize — only track width, height is fixed via prop
  useEffect(() => {
    const fixedH = propHeight || 500;
    if (propWidth) {
      setDims({ width: propWidth, height: fixedH });
      return;
    }
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      const { width } = entries[0].contentRect;
      if (width > 0) setDims(prev => ({ width, height: fixedH }));
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

    // Build layer → column index mapping
    const layerColIndex = new Map();
    sortedLayers.forEach((l, i) => layerColIndex.set(l.id, i));
    const nCols = sortedLayers.length;

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
          layerCol: layerColIndex.get(layer.id),
          value: logScale(node.value),
          rawValue: node.value,
          confidence: node.confidence,
          change_1m: node.change_1m,
          change_1w: node.change_1w,
          z_score: node.z_score,
          rawNode: node,
        });
      }
    }

    // Build layer order lookup for cycle prevention
    const layerOrder = new Map();
    for (const layer of sortedLayers) {
      layerOrder.set(layer.id, layer.order);
    }

    // Build links from edges, filtering out back-edges (DAG required).
    for (const edge of edges) {
      const srcOrder = layerOrder.get(edge.source_layer);
      const tgtOrder = layerOrder.get(edge.target_layer);
      if (srcOrder != null && tgtOrder != null && srcOrder >= tgtOrder) continue;
      const src = nodeMap.get(`${edge.source_layer}:${edge.source_node}`);
      const tgt = nodeMap.get(`${edge.target_layer}:${edge.target_node}`);
      if (src != null && tgt != null && src !== tgt) {
        sankeyLinks.push({
          source: src, target: tgt,
          value: Math.max(logScale(edge.value_usd), 2),
          rawValue: edge.value_usd,
          confidence: edge.confidence, channel: edge.channel, rawEdge: edge,
        });
      }
    }

    // Ensure every layer is connected: add synthetic edges between
    // adjacent layers that have no real edges
    const layerPairsWithEdges = new Set(sankeyLinks.map(l => {
      const sn = sankeyNodes[l.source], tn = sankeyNodes[l.target];
      return `${sn?.layerCol}→${tn?.layerCol}`;
    }));
    for (let i = 0; i < nCols - 1; i++) {
      if (layerPairsWithEdges.has(`${i}→${i + 1}`)) continue;
      const srcLayer = sortedLayers[i], tgtLayer = sortedLayers[i + 1];
      const srcNodes = (srcLayer.nodes || []), tgtNodes = (tgtLayer.nodes || []);
      if (!srcNodes.length || !tgtNodes.length) continue;
      const srcBest = srcNodes.reduce((a, b) => (Math.abs(a.value || 0) > Math.abs(b.value || 0) ? a : b));
      const tgtBest = tgtNodes.reduce((a, b) => (Math.abs(a.value || 0) > Math.abs(b.value || 0) ? a : b));
      const srcIdx = nodeMap.get(`${srcLayer.id}:${srcBest.id}`);
      const tgtIdx = nodeMap.get(`${tgtLayer.id}:${tgtBest.id}`);
      if (srcIdx != null && tgtIdx != null) {
        sankeyLinks.push({
          source: srcIdx, target: tgtIdx, value: 3,
          confidence: 'estimated', channel: 'inferred',
          rawEdge: { source_layer: srcLayer.id, target_layer: tgtLayer.id, confidence: 'estimated', label: 'inferred flow' },
        });
      }
    }

    // Remove orphan nodes (sankey requires all nodes linked)
    const linked = new Set();
    for (const l of sankeyLinks) { linked.add(l.source); linked.add(l.target); }
    const activeNodes = sankeyNodes.filter((_, i) => linked.has(i));
    const idxRemap = new Map();
    activeNodes.forEach((n, i) => { idxRemap.set(sankeyNodes.indexOf(n), i); });
    const activeLinks = sankeyLinks
      .filter(l => idxRemap.has(l.source) && idxRemap.has(l.target))
      .map(l => ({ ...l, source: idxRemap.get(l.source), target: idxRemap.get(l.target) }));

    if (!activeNodes.length || !activeLinks.length) return;

    // Build sankey layout — force nodes into their layer column
    const colWidth = iw / nCols;
    const sankeyGen = sankey()
      .nodeId(d => d.index)
      .nodeWidth(14)
      .nodePadding(12)
      .nodeSort(null)
      .nodeAlign((node) => node.layerCol ?? 0)
      .extent([[0, 0], [iw, ih]]);

    let graph;
    try {
      graph = sankeyGen({
        nodes: activeNodes.map(d => ({ ...d })),
        links: activeLinks.map(d => ({ ...d })),
      });
    } catch (err) {
      // d3-sankey throws "circular link" if any cycles remain.
      // Fall back to rendering nothing rather than crashing the view.
      console.error('Sankey layout failed:', err.message);
      return;
    }

    // Render
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();
    svg.attr('viewBox', `0 0 ${width} ${height}`);

    const g = svg.append('g').attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

    // Layer labels at top — positioned by actual node column
    sortedLayers.forEach((layer) => {
      const col = layerColIndex.get(layer.id);
      const x = col * colWidth + colWidth / 2;
      g.append('text')
        .attr('x', x).attr('y', -8)
        .attr('text-anchor', 'middle')
        .attr('fill', LAYER_COLORS[layer.id] || colors.textDim)
        .attr('font-size', '7px').attr('font-weight', 700)
        .attr('font-family', colors.mono)
        .attr('letter-spacing', '0.5px')
        .text((layer?.label || layer.id).toUpperCase().slice(0, 12));
    });

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

    // Node labels — name + value, positioned outside the node rect
    // Only show for nodes tall enough to avoid overlap
    const labelG = nodeG.filter(d => (d.y1 - d.y0) > 8);

    // Name label
    labelG.append('text')
      .attr('x', d => {
        const isLeft = (d.layerCol ?? 0) < nCols / 2;
        return isLeft ? d.x1 + 5 : d.x0 - 5;
      })
      .attr('y', d => (d.y0 + d.y1) / 2 - 4)
      .attr('text-anchor', d => (d.layerCol ?? 0) < nCols / 2 ? 'start' : 'end')
      .attr('fill', colors.textDim)
      .attr('font-size', '7px')
      .attr('font-family', colors.mono)
      .text(d => {
        const lbl = d.label || d.name;
        return lbl.length > 16 ? lbl.slice(0, 15) + '…' : lbl;
      });

    // Value label (below name)
    labelG.append('text')
      .attr('x', d => {
        const isLeft = (d.layerCol ?? 0) < nCols / 2;
        return isLeft ? d.x1 + 5 : d.x0 - 5;
      })
      .attr('y', d => (d.y0 + d.y1) / 2 + 6)
      .attr('text-anchor', d => (d.layerCol ?? 0) < nCols / 2 ? 'start' : 'end')
      .attr('fill', d => LAYER_COLORS[d.layerId] || colors.accent)
      .attr('font-size', '7px')
      .attr('font-weight', 700)
      .attr('font-family', colors.mono)
      .text(d => fmtDollar(d.rawValue));

  }, [data, dims]);

  if (loading) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: dims.height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
        Loading flow map...
      </div>
    );
  }

  if (error) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: dims.height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.red, fontFamily: colors.mono, fontSize: '12px' }}>
        {error}
      </div>
    );
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: dims.height, position: 'relative', overflow: 'hidden' }}>
      <svg ref={svgRef} width={dims.width} height={dims.height} />
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
