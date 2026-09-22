/**
 * FreshnessIndicator — LIVE / ESTIMATED / STALE badge.
 * Used across all flow components to show data confidence.
 */
import React from 'react';
import { colors } from '../../styles/shared.js';

const CONFIDENCE_MAP = {
  confirmed: { label: 'LIVE', bg: colors.greenBg, color: colors.green, dot: colors.green },
  derived:   { label: 'DERIVED', bg: colors.yellowBg, color: colors.yellow, dot: colors.yellow },
  estimated: { label: 'EST', bg: `${colors.accent}20`, color: colors.accent, dot: colors.accent },
  stale:     { label: 'STALE', bg: colors.redBg, color: colors.red, dot: colors.red },
  rumored:   { label: 'RUMOR', bg: colors.redBg, color: colors.red, dot: colors.red },
};

export default function FreshnessIndicator({ confidence, compact = false, style = {} }) {
  const key = String(confidence || 'estimated').toLowerCase();
  const cfg = CONFIDENCE_MAP[key] || CONFIDENCE_MAP.estimated;

  if (compact) {
    return (
      <span style={{
        display: 'inline-block', width: 6, height: 6, borderRadius: '50%',
        background: cfg.dot, boxShadow: `0 0 4px ${cfg.dot}60`,
        ...style,
      }} title={cfg.label} />
    );
  }

  return (
    <span style={{
      display: 'inline-flex', alignItems: 'center', gap: 4,
      background: cfg.bg, color: cfg.color, borderRadius: 4,
      padding: '1px 6px', fontSize: '9px', fontWeight: 700,
      letterSpacing: '0.5px', fontFamily: colors.mono,
      ...style,
    }}>
      <span style={{
        width: 5, height: 5, borderRadius: '50%',
        background: cfg.dot, boxShadow: `0 0 4px ${cfg.dot}60`,
      }} />
      {cfg.label}
    </span>
  );
}

/** Returns 0-1 opacity based on confidence level */
export function confidenceOpacity(c) {
  const s = String(c || '').toLowerCase();
  if (s === 'confirmed') return 1.0;
  if (s === 'derived') return 0.85;
  if (s === 'estimated') return 0.7;
  if (s === 'rumored') return 0.5;
  return 0.7;
}
