/**
 * GrandLoop — Circular capital flow diagram.
 *
 * Money flows in a loop: Fed → Banks → Markets → Economy → Treasury → Fed.
 * This shows the complete cycle, not a left-to-right waterfall.
 *
 * Nodes arranged in a circle. Edges are curved arcs showing flow direction.
 * Node size = pool size. Edge width = monthly flow magnitude.
 * Edge color: green = expanding flow, red = contracting flow.
 * Change indicators on each node show the monthly delta.
 */
import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { colors } from '../../styles/shared.js';
import { useFlowLayers, fmtDollar } from './useFlowData.js';
import FlowTooltip from './FlowTooltip.jsx';

const MARGIN = { top: 20, right: 20, bottom: 50, left: 20 };

// The grand loop: ~12 pools arranged clockwise as capital flows through the system
const LOOP_NODES = [
  // Top: monetary authority
  { id: 'fed',           layer: 'monetary',  nodeId: 'fed_balance_sheet',   label: 'Federal Reserve',     color: '#6366F1', shortLabel: 'FED' },
  { id: 'global_m2',     layer: 'monetary',  nodeId: 'global_m2',          label: 'Global M2 Supply',    color: '#818CF8', shortLabel: 'M2' },
  // Right: credit + institutional
  { id: 'credit',        layer: 'credit',    nodeId: 'bank_credit',        label: 'Bank Credit',         color: '#3B82F6', shortLabel: 'CREDIT' },
  { id: 'fx_reserves',   layer: 'sovereign', nodeId: 'fx_reserves',        label: 'FX Reserves',         color: '#EF4444', shortLabel: 'FX RES' },
  // Bottom: markets
  { id: 'bonds',         layer: 'market',    nodeId: 'bonds',              label: 'Bond Market',         color: '#14B8A6', shortLabel: 'BONDS' },
  { id: 'equities',      layer: 'market',    nodeId: 'equities',           label: 'Equity Market',       color: '#22C55E', shortLabel: 'EQUITIES' },
  { id: 'commodities',   layer: 'market',    nodeId: 'commodities',        label: 'Commodities',         color: '#84CC16', shortLabel: 'COMMOD' },
  { id: 'btc',           layer: 'crypto',    nodeId: 'btc_flows',          label: 'Bitcoin',             color: '#F97316', shortLabel: 'BTC' },
  // Left: downstream effects that loop back
  { id: 'margin_debt',   layer: 'retail',    nodeId: 'margin_debt',        label: 'Margin Debt',         color: '#EC4899', shortLabel: 'MARGIN' },
  { id: 'stablecoins',   layer: 'crypto',    nodeId: 'stablecoin_supply',  label: 'Stablecoins',         color: '#FB923C', shortLabel: 'STABLE' },
  { id: 'tga',           layer: 'monetary',  nodeId: 'tga_balance',        label: 'Treasury Account',    color: '#A78BFA', shortLabel: 'TGA' },
];

// The loop: each edge is a real capital transmission channel.
// Flows clockwise AND counter-clockwise — it's a cycle.
const LOOP_EDGES = [
  // Forward flow: Fed creates money → flows downstream
  { src: 'fed',         tgt: 'credit',      channel: 'Fed rate policy → bank lending capacity' },
  { src: 'fed',         tgt: 'bonds',       channel: 'QE/QT → direct bond market intervention' },
  { src: 'global_m2',   tgt: 'credit',      channel: 'M2 expansion → bank reserves → lending' },
  { src: 'global_m2',   tgt: 'fx_reserves', channel: 'Global liquidity → foreign reserve accumulation' },
  { src: 'credit',      tgt: 'equities',    channel: 'Bank lending → corporate borrowing → buybacks/investment' },
  { src: 'credit',      tgt: 'bonds',       channel: 'Credit conditions set bond demand' },
  { src: 'credit',      tgt: 'commodities', channel: 'Credit expansion → commodity demand' },
  { src: 'credit',      tgt: 'margin_debt', channel: 'Bank lending → broker margin → retail leverage' },
  { src: 'fx_reserves', tgt: 'bonds',       channel: 'Foreign CBs buy/sell US Treasuries' },
  { src: 'equities',    tgt: 'btc',         channel: 'Risk sentiment correlation' },
  { src: 'equities',    tgt: 'margin_debt', channel: 'Rising market → more leverage' },
  { src: 'global_m2',   tgt: 'stablecoins', channel: 'Fiat liquidity overflow → stablecoin minting' },
  { src: 'commodities', tgt: 'btc',         channel: 'Inflation hedge rotation' },

  // Return flow: money loops back
  { src: 'bonds',       tgt: 'tga',         channel: 'Treasury bond auctions → fund government' },
  { src: 'tga',         tgt: 'fed',         channel: 'Treasury spending/draining → Fed balance sheet' },
  { src: 'equities',    tgt: 'tga',         channel: 'Capital gains tax → Treasury revenue' },
  { src: 'stablecoins', tgt: 'bonds',       channel: 'Stablecoin reserves held in T-bills' },
  { src: 'margin_debt', tgt: 'credit',      channel: 'Margin repayment → bank balance sheets' },
];

export default function GrandLoop({ height: propHeight }) {
  const containerRef = useRef(null);
  const svgRef = useRef(null);
  const [tooltip, setTooltip] = useState({ visible: false, x: 0, y: 0, content: null });
  const [dims, setDims] = useState({ width: 900, height: propHeight || 520 });
  const { data, loading, error } = useFlowLayers();

  useEffect(() => {
    const fixedH = propHeight || 520;
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
    const { layers = [] } = data;
    if (!layers.length) return;

    const { width, height } = dims;
    const cx = width / 2;
    const cy = (height - MARGIN.bottom) / 2 + MARGIN.top / 2;
    const radius = Math.min(cx - MARGIN.left - 90, cy - MARGIN.top - 40);

    // Resolve nodes from flow data
    const nodeLookup = {};
    for (const layer of layers) {
      for (const node of (layer.nodes || [])) {
        nodeLookup[`${layer.id}:${node.id}`] = node;
      }
    }

    const resolved = [];
    for (let i = 0; i < LOOP_NODES.length; i++) {
      const gn = LOOP_NODES[i];
      const fullNode = nodeLookup[`${gn.layer}:${gn.nodeId}`];
      if (!fullNode) continue;

      const poolValue = fullNode.value || 0;
      const changePct = fullNode.change_1m || 0;
      const monthlyDelta = poolValue * Math.abs(changePct);
      const isUSD = (fullNode.unit || 'USD') === 'USD';

      // Position around circle
      const angle = (2 * Math.PI * i / LOOP_NODES.length) - Math.PI / 2; // start at top
      const x = cx + radius * Math.cos(angle);
      const y = cy + radius * Math.sin(angle);

      // Node circle radius: log-scaled pool size, clamped
      const nodeR = isUSD && poolValue > 0
        ? Math.max(8, Math.min(30, Math.log10(poolValue) * 3 - 20))
        : 10;

      resolved.push({
        ...gn, x, y, angle, nodeR,
        poolValue, changePct, monthlyDelta, isUSD,
        rawNode: fullNode,
      });
    }

    const nodeById = {};
    for (const n of resolved) nodeById[n.id] = n;

    // Resolve edges
    const resolvedEdges = [];
    for (const ge of LOOP_EDGES) {
      const src = nodeById[ge.src];
      const tgt = nodeById[ge.tgt];
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
      const strokeW = Math.max(1.5, Math.min(8, Math.log10(Math.max(flowMag, 1e6)) - 4));

      resolvedEdges.push({ ...ge, src, tgt, direction, flowMag, strokeW });
    }

    // Render
    const svg = d3.select(svgRef.current);
    svg.selectAll('*').remove();

    // Subtle radial grid
    svg.append('circle')
      .attr('cx', cx).attr('cy', cy).attr('r', radius)
      .attr('fill', 'none').attr('stroke', '#1A2332').attr('stroke-width', 1);

    // Edges: curved arcs between nodes
    const edgeG = svg.append('g').attr('class', 'edges');
    for (const e of resolvedEdges) {
      const dx = e.tgt.x - e.src.x;
      const dy = e.tgt.y - e.src.y;
      const dist = Math.sqrt(dx * dx + dy * dy);
      // Arc curvature: pull toward center for short arcs, away for long
      const curvature = dist < radius ? radius * 0.3 : radius * 0.15;

      // Midpoint pulled toward center for the arc
      const midX = (e.src.x + e.tgt.x) / 2;
      const midY = (e.src.y + e.tgt.y) / 2;
      const toCenterX = cx - midX;
      const toCenterY = cy - midY;
      const toCenterDist = Math.sqrt(toCenterX * toCenterX + toCenterY * toCenterY) || 1;
      const ctrlX = midX + (toCenterX / toCenterDist) * curvature;
      const ctrlY = midY + (toCenterY / toCenterDist) * curvature;

      const path = `M ${e.src.x} ${e.src.y} Q ${ctrlX} ${ctrlY} ${e.tgt.x} ${e.tgt.y}`;
      const edgeColor = e.direction === 'inflow' ? '#10B981'
        : e.direction === 'outflow' ? '#EF4444' : '#3B82F6';

      edgeG.append('path')
        .attr('d', path)
        .attr('fill', 'none')
        .attr('stroke', edgeColor)
        .attr('stroke-width', e.strokeW)
        .attr('stroke-opacity', 0.35)
        .style('cursor', 'pointer')
        .on('mouseenter', (ev) => {
          setTooltip({
            visible: true, x: ev.clientX, y: ev.clientY,
            content: { type: 'edge', channel: e.channel, direction: e.direction,
              src: e.src.label, tgt: e.tgt.label, flow: e.flowMag },
          });
        })
        .on('mousemove', (ev) => setTooltip(t => ({ ...t, x: ev.clientX, y: ev.clientY })))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

      // Arrowhead at target
      const t = 0.85; // position along the curve
      const ax = (1-t)*(1-t)*e.src.x + 2*(1-t)*t*ctrlX + t*t*e.tgt.x;
      const ay = (1-t)*(1-t)*e.src.y + 2*(1-t)*t*ctrlY + t*t*e.tgt.y;
      const tx = 2*(1-t)*(ctrlX-e.src.x) + 2*t*(e.tgt.x-ctrlX);
      const ty = 2*(1-t)*(ctrlY-e.src.y) + 2*t*(e.tgt.y-ctrlY);
      const ang = Math.atan2(ty, tx);
      const arrSize = 5;
      edgeG.append('polygon')
        .attr('points', `0,${-arrSize} ${arrSize*1.8},0 0,${arrSize}`)
        .attr('transform', `translate(${ax},${ay}) rotate(${ang * 180 / Math.PI})`)
        .attr('fill', edgeColor)
        .attr('fill-opacity', 0.6);
    }

    // Nodes: circles
    const nodeG = svg.append('g').attr('class', 'nodes');
    for (const n of resolved) {
      const g = nodeG.append('g')
        .style('cursor', 'pointer')
        .on('mouseenter', (ev) => {
          setTooltip({
            visible: true, x: ev.clientX, y: ev.clientY,
            content: { type: 'node', label: n.label, pool: n.poolValue,
              change: n.changePct, delta: n.monthlyDelta, isUSD: n.isUSD,
              rawNode: n.rawNode },
          });
        })
        .on('mousemove', (ev) => setTooltip(t => ({ ...t, x: ev.clientX, y: ev.clientY })))
        .on('mouseleave', () => setTooltip(t => ({ ...t, visible: false })));

      // Glow
      g.append('circle')
        .attr('cx', n.x).attr('cy', n.y).attr('r', n.nodeR + 4)
        .attr('fill', 'none')
        .attr('stroke', n.color)
        .attr('stroke-width', 1)
        .attr('stroke-opacity', 0.2);

      // Main circle
      g.append('circle')
        .attr('cx', n.x).attr('cy', n.y).attr('r', n.nodeR)
        .attr('fill', n.color)
        .attr('fill-opacity', 0.8)
        .attr('stroke', n.changePct > 0 ? '#10B981' : n.changePct < 0 ? '#EF4444' : '#1E2A3A')
        .attr('stroke-width', 2);

      // Short label inside circle
      if (n.nodeR >= 12) {
        g.append('text')
          .attr('x', n.x).attr('y', n.y)
          .attr('text-anchor', 'middle')
          .attr('dominant-baseline', 'central')
          .attr('fill', '#fff')
          .attr('font-size', `${Math.max(6, n.nodeR * 0.5)}px`)
          .attr('font-weight', 700)
          .attr('font-family', colors.mono)
          .text(n.shortLabel);
      }

      // External label: name + value + change
      const labelAngle = n.angle;
      const isRight = Math.cos(labelAngle) > 0.1;
      const isLeft = Math.cos(labelAngle) < -0.1;
      const labelDist = n.nodeR + 14;
      const lx = n.x + labelDist * Math.cos(labelAngle);
      const ly = n.y + labelDist * Math.sin(labelAngle);
      const anchor = isRight ? 'start' : isLeft ? 'end' : 'middle';

      // Name
      g.append('text')
        .attr('x', lx).attr('y', ly - 6)
        .attr('text-anchor', anchor)
        .attr('fill', '#C8D8E8')
        .attr('font-size', '8px')
        .attr('font-weight', 600)
        .attr('font-family', colors.mono)
        .text(n.label);

      // Pool size
      if (n.isUSD && n.poolValue > 0) {
        g.append('text')
          .attr('x', lx).attr('y', ly + 4)
          .attr('text-anchor', anchor)
          .attr('fill', '#5A7A90')
          .attr('font-size', '7px')
          .attr('font-family', colors.mono)
          .text(fmtDollar(n.poolValue));
      }

      // Monthly change
      if (n.changePct !== 0) {
        const arrow = n.changePct > 0 ? '▲' : '▼';
        const pct = Math.abs(n.changePct * 100).toFixed(1);
        const deltaStr = n.isUSD && n.monthlyDelta > 0 ? ` ${fmtDollar(n.monthlyDelta)}/mo` : '';

        g.append('text')
          .attr('x', lx).attr('y', ly + 14)
          .attr('text-anchor', anchor)
          .attr('fill', n.changePct > 0 ? '#10B981' : '#EF4444')
          .attr('font-size', '8px')
          .attr('font-weight', 700)
          .attr('font-family', colors.mono)
          .text(`${arrow} ${pct}%${deltaStr}`);
      }
    }

    // Narrative
    const fed = nodeById.fed;
    const credit = nodeById.credit;
    const equities = nodeById.equities;
    const btc = nodeById.btc;

    const parts = [];
    if (fed) {
      const dir = fed.changePct > 0.002 ? 'expanding' : fed.changePct < -0.002 ? 'contracting' : 'holding';
      parts.push(`Fed ${dir}`);
    }
    if (credit) {
      const dir = credit.changePct > 0.002 ? 'loosening' : credit.changePct < -0.002 ? 'tightening' : 'steady';
      parts.push(`credit ${dir}`);
    }
    if (equities && equities.changePct !== 0) {
      parts.push(`equities ${equities.changePct > 0 ? '↑' : '↓'}${Math.abs(equities.changePct * 100).toFixed(1)}%`);
    }
    if (btc && equities && btc.changePct !== 0) {
      const diverging = (btc.changePct > 0) !== (equities.changePct > 0);
      if (diverging) {
        parts.push(`⚡ BTC diverging ${btc.changePct > 0 ? '↑' : '↓'}${Math.abs(btc.changePct * 100).toFixed(1)}%`);
      } else {
        parts.push(`BTC ${btc.changePct > 0 ? '↑' : '↓'}${Math.abs(btc.changePct * 100).toFixed(1)}%`);
      }
    }

    svg.append('text')
      .attr('x', cx).attr('y', height - 12)
      .attr('text-anchor', 'middle')
      .attr('fill', '#8A9AB0')
      .attr('font-size', '10px')
      .attr('font-family', colors.mono)
      .text(parts.join(' → '));

  }, [data, dims]);

  if (loading) {
    return (
      <div ref={containerRef} style={{ width: '100%', height: dims.height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
        Loading capital loop...
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
          position: 'fixed', left: tooltip.x + 12, top: tooltip.y - 10,
          background: 'rgba(13, 17, 23, 0.95)', border: '1px solid #1E2A3A',
          borderRadius: 8, padding: '8px 12px', fontSize: 11,
          color: '#C8D8E8', fontFamily: colors.mono, zIndex: 100,
          maxWidth: 280, pointerEvents: 'none',
          boxShadow: '0 4px 16px rgba(0,0,0,0.4)',
        }}>
          {tooltip.content.type === 'node' ? (
            <>
              <div style={{ fontWeight: 700, marginBottom: 4 }}>{tooltip.content.label}</div>
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
                {tooltip.content.direction === 'inflow' ? '▲ Capital flowing' : tooltip.content.direction === 'outflow' ? '▼ Capital pulling back' : '● Neutral'}
                {tooltip.content.flow > 0 && ` · ${fmtDollar(tooltip.content.flow)}/mo`}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  );
}
