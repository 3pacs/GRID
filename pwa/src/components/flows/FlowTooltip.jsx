/**
 * FlowTooltip — Shared tooltip for flow visualizations.
 * Shows node/edge details with upstream/downstream context.
 */
import React from 'react';
import { colors } from '../../styles/shared.js';
import FreshnessIndicator from './FreshnessIndicator.jsx';
import { fmtDollar, fmtPct, trendArrow } from './useFlowData.js';

const sty = {
  wrap: {
    position: 'absolute', pointerEvents: 'none', zIndex: 50,
    background: colors.bg + 'F5', border: `1px solid ${colors.border}`,
    borderRadius: 8, padding: '10px 14px', maxWidth: 260,
    fontFamily: colors.mono, fontSize: '11px', color: colors.text,
    boxShadow: colors.shadow?.lg || '0 8px 24px rgba(0,0,0,0.5)',
    backdropFilter: 'blur(8px)',
  },
  label: { fontWeight: 700, fontSize: '12px', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 6 },
  row: { display: 'flex', justifyContent: 'space-between', padding: '2px 0', gap: 12 },
  dim: { color: colors.textDim },
  val: { fontWeight: 600, textAlign: 'right' },
  divider: { height: 1, background: colors.border, margin: '4px 0' },
};

export default function FlowTooltip({ node, edge, x, y, visible }) {
  if (!visible || (!node && !edge)) return null;

  const left = Math.min(x + 12, window.innerWidth - 280);
  const top = Math.min(y + 12, window.innerHeight - 200);

  if (edge) {
    return (
      <div style={{ ...sty.wrap, left, top }}>
        <div style={sty.label}>{edge.source_layer} → {edge.target_layer}</div>
        <div style={sty.row}><span style={sty.dim}>Flow</span><span style={sty.val}>{fmtDollar(edge.value_usd)}</span></div>
        <div style={sty.row}><span style={sty.dim}>Channel</span><span style={sty.val}>{edge.channel || '--'}</span></div>
        <div style={sty.row}><span style={sty.dim}>Confidence</span><FreshnessIndicator confidence={edge.confidence} /></div>
      </div>
    );
  }

  return (
    <div style={{ ...sty.wrap, left, top }}>
      <div style={sty.label}>
        {node.label || node.id}
        <FreshnessIndicator confidence={node.confidence} />
      </div>
      <div style={sty.divider} />
      <div style={sty.row}><span style={sty.dim}>Value</span><span style={sty.val}>{fmtDollar(node.value)}</span></div>
      {node.change_1w != null && (
        <div style={sty.row}><span style={sty.dim}>1W Chg</span><span style={{ ...sty.val, color: node.change_1w >= 0 ? colors.green : colors.red }}>{fmtPct(node.change_1w)}</span></div>
      )}
      {node.change_1m != null && (
        <div style={sty.row}><span style={sty.dim}>1M Chg</span><span style={{ ...sty.val, color: node.change_1m >= 0 ? colors.green : colors.red }}>{fmtPct(node.change_1m)}</span></div>
      )}
      {node.stress_z != null && (
        <div style={sty.row}><span style={sty.dim}>Z-Score</span><span style={sty.val}>{node.stress_z?.toFixed(2)}</span></div>
      )}
      {node.trend && (
        <div style={sty.row}><span style={sty.dim}>Trend</span><span style={sty.val}>{trendArrow(node.trend)} {node.trend}</span></div>
      )}
      {node.layer && (
        <div style={sty.row}><span style={sty.dim}>Layer</span><span style={sty.val}>{node.layer}</span></div>
      )}
    </div>
  );
}
