/**
 * FlowSankey8 — Circular force diagram with all junction point nodes.
 *
 * 46 nodes from 8 layers, arranged in concentric arcs by layer.
 * Inner ring: monetary (cause). Outer ring: crypto/retail (effect).
 * Edges: curved arcs colored by flow direction.
 * Same visual language as GrandLoop but with full granularity.
 */
import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { colors } from '../../styles/shared.js';
import { useFlowLayers, fmtDollar } from './useFlowData.js';
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

// Layer display order — inner to outer
const LAYER_ORDER = ['monetary', 'credit', 'institutional', 'market', 'corporate', 'sovereign', 'retail', 'crypto'];

const MARGIN = { top: 20, right: 20, bottom: 40, left: 20 };

export default function FlowSankey8({ width: propWidth, height: propHeight }) {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, content: null });
  const [dims, setDims] = useState({ width: propWidth || 900, height: propHeight || 500 });
  const { data, loading, error } = useFlowLayers();

  useEffect(() => {
    const fixedH = propHeight || 500;
    const el = containerRef.current;
    if (!el) return;
    const obs = new ResizeObserver(entries => {
      const { width } = entries[0].contentRect;
      if (width > 0) setDims({ width, height: fixedH });
    });
    obs.observe(el);
    return () => obs.disconnect();
  }, [propHeight]);

  useEffect(() => {
    if (!data || !svgRef.current) return;
    const { layers = [], edges: flowEdges = [] } = data;
    if (!layers.length) return;

    const { width, height } = dims;
    const cx = width / 2;
    const cy = (height - MARGIN.bottom) / 2 + MARGIN.top / 2;
    const maxRadius = Math.min(cx - MARGIN.left - 80, cy - MARGIN.top - 30);

    // Group nodes by layer, sorted by layer order
    const sortedLayers = [...layers].sort((a, b) => {
      return LAYER_ORDER.indexOf(a.id) - LAYER_ORDER.indexOf(b.id);
    });

    // Assign positions: each layer gets an arc segment of the circle
    // All layers share the same ring but get different angular sectors
    const allResolved = [];
    const nodeById = {};
    const totalNodes = layers.reduce((s, l) => s + (l.nodes || []).length, 0);
    let globalIdx = 0;

    for (let li = 0; li < sortedLayers.length; li++) {
      const layer = sortedLayers[li];
      const layerNodes = layer.nodes || [];
      if (!layerNodes.length) continue;

      const layerColor = LAYER_COLORS[layer.id] || '#6B7280';

      for (let ni = 0; ni < layerNodes.length; ni++) {
        const node = layerNodes[ni];
        const poolValue = node.value || 0;
        const changePct = node.change_1m || 0;
        const monthlyDelta = poolValue * Math.abs(changePct);
        const isUSD = (node.unit || 'USD') === 'USD';

        // Position around the circle — evenly spaced across all nodes
        const angle = (2 * Math.PI * globalIdx / totalNodes) - Math.PI / 2;

        // Radius: slightly varied by layer to create depth
        const layerRing = 0.85 + (li / (sortedLayers.length - 1)) * 0.15;
        const r = maxRadius * layerRing;
        const x = cx + r * Math.cos(angle);
        const y = cy + r * Math.sin(angle);

        // Node size: log-scaled pool value
        const nodeR = isUSD && poolValue > 0
          ? Math.max(4, Math.min(18, Math.log10(poolValue) * 2 - 14))
          : 5;

        const resolved = {
          id: `${layer.id}:${node.id}`,
          nodeId: node.id,
          layerId: layer.id,
          label: node.label || node.id,
          color: layerColor,
          x, y, angle, nodeR,
          poolValue, changePct, monthlyDelta, isUSD,
          rawNode: node,
        };
        allResolved.push(resolved);
        nodeById[resolved.id] = resolved;
        globalIdx++;
      }
    }

    // Resolve edges from flow data
    const resolvedEdges = [];
    for (const fe of flowEdges) {
      const srcKey = `${fe.source_layer}:${fe.source_node}`;
      const tgtKey = `${fe.target_layer}:${fe.target_node}`;
      const src = nodeById[srcKey];
      const tgt = nodeById[tgtKey];
      if (!src || !tgt) continue;

      const srcChg = src.changePct || 0;
      const tgtChg = tgt.changePct || 0;
      const diff = tgtChg - srcChg;

      let direction;
      if (diff > 0.005) direction = 'inflow';
      else if (diff < -0.005) direction = 'outflow';
      else if (tgtChg > 0 && srcChg > 0) direction = 'inflow';
      else if (tgtChg < 0 && srcChg < 0) direction = 'outflow';
      else direction = 'neutral';

      const flowMag = (src.monthlyDelta + tgt.monthlyDelta) / 2;
      const strokeW = Math.max(1, Math.min(6, Math.log10(Math.max(flowMag, 1e6)) - 4));

      resolvedEdges.push({
        src, tgt, direction, flowMag, strokeW,
        channel: fe.channel || fe.label || '',
        label: fe.label || '',
      });
    }

    // Render
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    // Layer arc labels around the outside
    for (let li = 0; li < sortedLayers.length; li++) {
      const layer = sortedLayers[li];
      const layerNodes = allResolved.filter(n => n.layerId === layer.id);
      if (!layerNodes.length) continue;

      // Find the angular midpoint of this layer's nodes
      const midAngle = layerNodes.reduce((s, n) => s + n.angle, 0) / layerNodes.length;
      const labelR = maxRadius + 30;
      const lx = cx + labelR * Math.cos(midAngle);
      const ly = cy + labelR * Math.sin(midAngle);
      const anchor = Math.cos(midAngle) > 0.3 ? 'start' : Math.cos(midAngle) < -0.3 ? 'end' : 'middle';

      svg.append('text')
        .attr('x', lx).attr('y', ly)
        .attr('text-anchor', anchor)
        .attr('dominant-baseline', 'central')
        .attr('fill', LAYER_COLORS[layer.id] || '#5A7A90')
        .attr('font-size', '8px')
        .attr('font-weight', 700)
        .attr('font-family', colors.mono)
        .attr('letter-spacing', '1px')
        .text(layer.label?.toUpperCase() || layer.id.toUpperCase());
    }

    // Edges
    const edgeG = svg.append('g').attr('class', 'edges');
    for (const e of resolvedEdges) {
      // Curved arc through center
      const midX = (e.src.x + e.tgt.x) / 2;
      const midY = (e.src.y + e.tgt.y) / 2;
      const toCenterX = cx - midX;
      const toCenterY = cy - midY;
      const dist = Math.sqrt(toCenterX * toCenterX + toCenterY * toCenterY) || 1;
      const dx = e.tgt.x - e.src.x;
      const dy = e.tgt.y - e.src.y;
      const edgeDist = Math.sqrt(dx * dx + dy * dy);
      const curvature = edgeDist < maxRadius ? maxRadius * 0.25 : maxRadius * 0.1;
      const ctrlX = midX + (toCenterX / dist) * curvature;
      const ctrlY = midY + (toCenterY / dist) * curvature;

      const path = `M ${e.src.x} ${e.src.y} Q ${ctrlX} ${ctrlY} ${e.tgt.x} ${e.tgt.y}`;
      const edgeColor = e.direction === 'inflow' ? '#10B981'
        : e.direction === 'outflow' ? '#EF4444' : '#3B82F6';

      edgeG.append('path')
        .attr('d', path)
        .attr('fill', 'none')
        .attr('stroke', edgeColor)
        .attr('stroke-width', e.strokeW)
        .attr('stroke-opacity', 0.3)
        .style('cursor', 'pointer')
        .on('mouseenter', (ev) => {
          setTooltip({
            visible: true, x: ev.clientX, y: ev.clientY,
            content: { type: 'edge', channel: e.label || e.channel, direction: e.direction,
              src: e.src.label, tgt: e.tgt.label, flow: e.flowMag },
          });
        })
        .on('mousemove', (ev) => setTooltip(t => ({ ...t, x: ev.clientX, y: ev.clientY })))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

      // Arrow
      const t = 0.82;
      const ax = (1-t)*(1-t)*e.src.x + 2*(1-t)*t*ctrlX + t*t*e.tgt.x;
      const ay = (1-t)*(1-t)*e.src.y + 2*(1-t)*t*ctrlY + t*t*e.tgt.y;
      const tx = 2*(1-t)*(ctrlX-e.src.x) + 2*t*(e.tgt.x-ctrlX);
      const ty = 2*(1-t)*(ctrlY-e.src.y) + 2*t*(e.tgt.y-ctrlY);
      const ang = Math.atan2(ty, tx);
      edgeG.append('polygon')
        .attr('points', '0,-3 5,0 0,3')
        .attr('transform', `translate(${ax},${ay}) rotate(${ang * 180 / Math.PI})`)
        .attr('fill', edgeColor)
        .attr('fill-opacity', 0.5);
    }

    // Nodes
    const nodeG = svg.append('g').attr('class', 'nodes');
    for (const n of allResolved) {
      const g = nodeG.append('g')
        .style('cursor', 'pointer')
        .on('mouseenter', (ev) => {
          setTooltip({
            visible: true, x: ev.clientX, y: ev.clientY,
            content: { type: 'node', label: n.label, pool: n.poolValue,
              change: n.changePct, delta: n.monthlyDelta, isUSD: n.isUSD,
              layer: n.layerId, rawNode: n.rawNode },
          });
        })
        .on('mousemove', (ev) => setTooltip(t => ({ ...t, x: ev.clientX, y: ev.clientY })))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

      // Glow ring
      g.append('circle')
        .attr('cx', n.x).attr('cy', n.y).attr('r', n.nodeR + 2)
        .attr('fill', 'none')
        .attr('stroke', n.color)
        .attr('stroke-width', 0.5)
        .attr('stroke-opacity', 0.3);

      // Main dot
      g.append('circle')
        .attr('cx', n.x).attr('cy', n.y).attr('r', n.nodeR)
        .attr('fill', n.color)
        .attr('fill-opacity', 0.75)
        .attr('stroke', n.changePct > 0 ? '#10B981' : n.changePct < 0 ? '#EF4444' : '#1E2A3A')
        .attr('stroke-width', 1.5);

      // Label — only for nodes big enough
      if (n.nodeR >= 6) {
        const isRight = Math.cos(n.angle) > 0.1;
        const isLeft = Math.cos(n.angle) < -0.1;
        const labelDist = n.nodeR + 6;
        const lx = n.x + labelDist * Math.cos(n.angle);
        const ly = n.y + labelDist * Math.sin(n.angle);
        const anchor = isRight ? 'start' : isLeft ? 'end' : 'middle';

        // Name
        const maxLen = 14;
        const lbl = n.label.length > maxLen ? n.label.slice(0, maxLen - 1) + '…' : n.label;
        g.append('text')
          .attr('x', lx).attr('y', ly - 3)
          .attr('text-anchor', anchor)
          .attr('fill', '#A0B0C0')
          .attr('font-size', '6px')
          .attr('font-family', colors.mono)
          .text(lbl);

        // Value + change
        if (n.isUSD && n.poolValue > 0) {
          const valStr = fmtDollar(n.poolValue);
          const chgStr = n.changePct !== 0
            ? ` ${n.changePct > 0 ? '▲' : '▼'}${Math.abs(n.changePct * 100).toFixed(1)}%`
            : '';
          g.append('text')
            .attr('x', lx).attr('y', ly + 5)
            .attr('text-anchor', anchor)
            .attr('fill', n.changePct > 0 ? '#10B981' : n.changePct < 0 ? '#EF4444' : '#5A7A90')
            .attr('font-size', '6px')
            .attr('font-weight', 600)
            .attr('font-family', colors.mono)
            .text(`${valStr}${chgStr}`);
        } else if (!n.isUSD) {
          // Index nodes — show label value
          g.append('text')
            .attr('x', lx).attr('y', ly + 5)
            .attr('text-anchor', anchor)
            .attr('fill', '#5A7A90')
            .attr('font-size', '6px')
            .attr('font-family', colors.mono)
            .text(n.label);
        }
      }
    }

    // Legend
    const legendY = height - 16;
    const legendItems = LAYER_ORDER.map((id, i) => {
      const layer = sortedLayers.find(l => l.id === id);
      return { id, label: layer?.label || id, color: LAYER_COLORS[id] };
    });
    const legendWidth = legendItems.length * 85;
    const legendX = (width - legendWidth) / 2;
    legendItems.forEach((item, i) => {
      const x = legendX + i * 85;
      svg.append('circle').attr('cx', x).attr('cy', legendY).attr('r', 3).attr('fill', item.color);
      svg.append('text')
        .attr('x', x + 6).attr('y', legendY)
        .attr('dominant-baseline', 'central')
        .attr('fill', '#5A7A90')
        .attr('font-size', '7px')
        .attr('font-family', colors.mono)
        .text(item.label);
    });

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
      <div ref={containerRef} style={{ width: '100%', height: dims.height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: '#EF4444', fontFamily: colors.mono, fontSize: '12px' }}>
        {error}
      </div>
    );
  }

  return (
    <div ref={containerRef} style={{ width: '100%', height: dims.height, position: 'relative', overflow: 'hidden' }}>
      <svg ref={svgRef} width={dims.width} height={dims.height} />
      {tooltip.visible && tooltip.content && (
        <div style={{
          position: 'fixed', left: Math.min(tooltip.x + 12, window.innerWidth - 280), top: Math.min(tooltip.y - 10, window.innerHeight - 180),
          background: 'rgba(13, 17, 23, 0.95)', border: '1px solid #1E2A3A',
          borderRadius: 8, padding: '8px 12px', fontSize: 11,
          color: '#C8D8E8', fontFamily: colors.mono, zIndex: 100,
          maxWidth: 280, pointerEvents: 'none',
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
        }}>
          {tooltip.content.type === 'node' ? (
            <>
              <div style={{ fontWeight: 700, marginBottom: 4 }}>{tooltip.content.label}</div>
              <div style={{ fontSize: 9, color: '#5A7A90', marginBottom: 4 }}>{tooltip.content.layer}</div>
              {tooltip.content.isUSD && <div>Pool: {fmtDollar(tooltip.content.pool)}</div>}
              {tooltip.content.change !== 0 && (
                <div style={{ color: tooltip.content.change > 0 ? '#10B981' : '#EF4444' }}>
                  Monthly: {tooltip.content.change > 0 ? '+' : ''}{(tooltip.content.change * 100).toFixed(2)}%
                  {tooltip.content.isUSD && tooltip.content.delta > 0 && ` (${fmtDollar(tooltip.content.delta)})`}
                </div>
              )}
            </>
          ) : (
            <>
              <div style={{ fontWeight: 700, marginBottom: 4 }}>{tooltip.content.src} → {tooltip.content.tgt}</div>
              <div style={{ color: '#8A9AB0', lineHeight: 1.4 }}>{tooltip.content.channel}</div>
              <div style={{ marginTop: 4, color: tooltip.content.direction === 'inflow' ? '#10B981' : tooltip.content.direction === 'outflow' ? '#EF4444' : '#3B82F6' }}>
                {tooltip.content.direction === 'inflow' ? '▲ Capital flowing' : tooltip.content.direction === 'outflow' ? '▼ Pulling back' : '● Neutral'}
                {tooltip.content.flow > 0 && ` · ${fmtDollar(tooltip.content.flow)}/mo`}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
