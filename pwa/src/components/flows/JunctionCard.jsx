/**
 * JunctionCard — Single junction point card.
 * Shows value, change, trend, freshness, stress z-score.
 */
import React from 'react';
import { colors } from '../../styles/shared.js';
import FreshnessIndicator from './FreshnessIndicator.jsx';
import { fmtDollar, fmtPct, trendArrow } from './useFlowData.js';

const LAYER_COLORS = {
  monetary: '#6366F1', credit: '#3B82F6', institutional: '#14B8A6',
  market: '#22C55E', corporate: '#F59E0B', sovereign: '#EF4444',
  retail: '#EC4899', crypto: '#F97316',
};

function stressColor(z) {
  if (z == null) return colors.textDim;
  const abs = Math.abs(z);
  if (abs > 2) return colors.red;
  if (abs > 1) return colors.yellow;
  return colors.green;
}

export default function JunctionCard({ point, onClick }) {
  const layerColor = LAYER_COLORS[point.layer] || colors.accent;
  const changeColor = (point.change_1m || 0) >= 0 ? colors.green : colors.red;

  return (
    <div
      onClick={() => onClick?.(point)}
      style={{
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: 10, padding: '12px 14px',
        borderLeft: `3px solid ${layerColor}`,
        cursor: onClick ? 'pointer' : 'default',
        transition: 'border-color 0.2s, background 0.2s',
        minWidth: 0,
      }}
      onMouseEnter={e => { e.currentTarget.style.background = colors.cardHover; }}
      onMouseLeave={e => { e.currentTarget.style.background = colors.card; }}
    >
      {/* Header */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: 6 }}>
        <div style={{ fontSize: '11px', fontWeight: 700, color: colors.text, fontFamily: colors.mono, lineHeight: '1.3', flex: 1, minWidth: 0 }}>
          {point.label || point.id}
        </div>
        <FreshnessIndicator confidence={point.confidence} />
      </div>

      {/* Value */}
      <div style={{ fontSize: '16px', fontWeight: 700, color: colors.text, fontFamily: colors.mono, marginBottom: 4 }}>
        {fmtDollar(point.value)}
      </div>

      {/* Changes row */}
      <div style={{ display: 'flex', gap: 10, fontSize: '10px', fontFamily: colors.mono, marginBottom: 4 }}>
        {point.change_1w != null && (
          <span style={{ color: (point.change_1w || 0) >= 0 ? colors.green : colors.red }}>
            1W {fmtPct(point.change_1w)}
          </span>
        )}
        {point.change_1m != null && (
          <span style={{ color: changeColor }}>
            1M {fmtPct(point.change_1m)}
          </span>
        )}
      </div>

      {/* Bottom row: trend + z-score */}
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '10px', fontFamily: colors.mono }}>
        <span style={{ color: changeColor }}>
          {trendArrow(point.trend)} {point.trend || '--'}
        </span>
        {point.stress_z != null && (
          <span style={{ color: stressColor(point.stress_z), fontWeight: 600 }}>
            z={point.stress_z?.toFixed(2)}
          </span>
        )}
      </div>

      {/* Layer tag */}
      <div style={{
        marginTop: 6, fontSize: '8px', fontWeight: 700, letterSpacing: '1px',
        color: layerColor, textTransform: 'uppercase',
      }}>
        {point.layer}
      </div>
    </div>
  );
}
