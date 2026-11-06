/**
 * FreshnessIndicator — provenance badge for flow data.
 *
 * Renders the states docs/reference/AVAILABILITY_CONTRACT.md defines, derived
 * from the row's own `confidence` / `as_of` / `unavailable` fields:
 *
 *   OBSERVED <as-of>  confidence "confirmed" (an observation); the as-of is the
 *                     data's own date when the payload reports one
 *   DERIVED           computed from observations
 *   ESTIMATED         model / assumption output
 *   STALE             the source is older than its freshness threshold
 *   RUMOR             unverified report
 *   UNAVAILABLE       `unavailable: true` (or confidence "unavailable"); the
 *                     reason is shown in the tooltip
 *   UNKNOWN           confidence missing or unrecognised — never shown as an
 *                     estimate
 *
 * The badge never says "LIVE": nothing in a flow payload proves the value is a
 * real-time quote, and the contract lists literal "LIVE" as a defect.
 */
import React from 'react';
import { colors } from '../../styles/shared.js';

const STATES = {
  observed:    { label: 'OBSERVED', bg: colors.greenBg, color: colors.green, dot: colors.green },
  derived:     { label: 'DERIVED', bg: colors.yellowBg, color: colors.yellow, dot: colors.yellow },
  estimated:   { label: 'ESTIMATED', bg: `${colors.accent}20`, color: colors.accent, dot: colors.accent },
  stale:       { label: 'STALE', bg: colors.redBg, color: colors.red, dot: colors.red },
  rumored:     { label: 'RUMOR', bg: colors.redBg, color: colors.red, dot: colors.red },
  unavailable: { label: 'UNAVAILABLE', bg: 'transparent', color: colors.textDim, dot: 'transparent', border: colors.textDim },
  unknown:     { label: 'UNKNOWN', bg: 'transparent', color: colors.textDim, dot: 'transparent', border: colors.textDim },
};

const CONFIDENCE_TO_STATE = {
  confirmed: 'observed',
  observed: 'observed',
  derived: 'derived',
  estimated: 'estimated',
  stale: 'stale',
  rumored: 'rumored',
  unavailable: 'unavailable',
};

/** YYYY-MM-DD for ISO-ish strings; the raw string otherwise; null when absent. */
export function formatAsOf(asOf) {
  if (asOf === null || asOf === undefined || asOf === '') return null;
  const s = String(asOf);
  return /^\d{4}-\d{2}-\d{2}/.test(s) ? s.slice(0, 10) : s;
}

/**
 * Pure mapping from a row's provenance fields to the badge state.
 * Returns { state, label, title, ...style }.
 */
export function describeFreshness({ confidence, asOf, stale = false, unavailable = false, reason } = {}) {
  const raw = confidence === null || confidence === undefined ? '' : String(confidence).trim().toLowerCase();
  const date = formatAsOf(asOf);
  const why = reason ? String(reason) : null;

  let state;
  if (unavailable || raw === 'unavailable') state = 'unavailable';
  else if (stale || raw === 'stale') state = 'stale';
  else state = CONFIDENCE_TO_STATE[raw] || 'unknown';

  const cfg = STATES[state];
  let label = cfg.label;
  let title;
  switch (state) {
    case 'unavailable':
      title = `Unavailable: ${why || 'no reason reported'}`;
      break;
    case 'stale':
      title = `Stale${date ? ` (as of ${date})` : ''}${why ? `: ${why}` : ''}`;
      break;
    case 'observed':
      if (date) label = `${cfg.label} ${date}`;
      title = date ? `Observed; as of ${date}` : 'Observed; as-of date not reported';
      break;
    case 'derived':
      title = `Derived from observations${date ? `; as of ${date}` : ''}`;
      break;
    case 'estimated':
      title = 'Estimated: model or assumption output, not an observation';
      break;
    case 'rumored':
      title = 'Unverified report';
      break;
    default:
      title = raw ? `Confidence unknown (unrecognised value "${raw}")` : 'Confidence unknown';
  }
  return { state, ...cfg, label, title };
}

export default function FreshnessIndicator({
  confidence, asOf, stale = false, unavailable = false, reason, compact = false, style = {},
}) {
  const cfg = describeFreshness({ confidence, asOf, stale, unavailable, reason });
  const ring = cfg.border ? `1px solid ${cfg.border}` : 'none';
  const glow = cfg.border ? 'none' : `0 0 4px ${cfg.dot}60`;

  if (compact) {
    return (
      <span
        data-freshness={cfg.state}
        role="img"
        aria-label={`${cfg.label}. ${cfg.title}`}
        title={`${cfg.label}: ${cfg.title}`}
        style={{
          display: 'inline-block', width: 6, height: 6, borderRadius: '50%',
          boxSizing: 'border-box', border: ring, background: cfg.dot, boxShadow: glow,
          ...style,
        }}
      />
    );
  }

  return (
    <span
      data-freshness={cfg.state}
      title={cfg.title}
      style={{
        display: 'inline-flex', alignItems: 'center', gap: 4,
        background: cfg.bg, color: cfg.color, borderRadius: 4, border: ring,
        padding: '1px 6px', fontSize: '9px', fontWeight: 700,
        letterSpacing: '0.5px', fontFamily: colors.mono, whiteSpace: 'nowrap',
        ...style,
      }}
    >
      <span style={{
        width: 5, height: 5, borderRadius: '50%', boxSizing: 'border-box',
        border: ring, background: cfg.dot, boxShadow: glow,
      }} />
      {cfg.label}
    </span>
  );
}

/**
 * Returns 0-1 opacity based on confidence level. Unknown / unavailable are
 * drawn fainter than an estimate so a missing confidence never looks like one.
 */
export function confidenceOpacity(c) {
  const s = String(c || '').toLowerCase();
  if (s === 'confirmed') return 1.0;
  if (s === 'derived') return 0.85;
  if (s === 'estimated') return 0.7;
  if (s === 'rumored' || s === 'stale') return 0.5;
  return 0.35;
}
