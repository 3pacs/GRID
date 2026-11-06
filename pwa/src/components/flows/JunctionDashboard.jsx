/**
 * JunctionDashboard — Card grid for all junction points.
 * Groups by layer, shows layer summaries, filterable.
 */
import React, { useState, useMemo } from 'react';
import { colors, tokens } from '../../styles/shared.js';
import { useJunctionPoints, fmtDollar } from './useFlowData.js';
import FreshnessIndicator from './FreshnessIndicator.jsx';
import JunctionCard from './JunctionCard.jsx';

const LAYER_ORDER = ['monetary', 'credit', 'institutional', 'market', 'corporate', 'sovereign', 'retail', 'crypto'];
const LAYER_COLORS = {
  monetary: '#6366F1', credit: '#3B82F6', institutional: '#14B8A6',
  market: '#22C55E', corporate: '#F59E0B', sovereign: '#EF4444',
  retail: '#EC4899', crypto: '#F97316',
};

/**
 * Table-level freshness of junction_point_readings from the flows provenance
 * envelope (docs/flows_provenance_contract.md). It describes the readings
 * table, not each card, so it is shown once above the grid.
 */
export function ReadingsProvenance({ provenance }) {
  const p = provenance?.junction_point_readings;
  const textStyle = { fontSize: '9px', fontFamily: colors.mono, color: colors.textDim };
  if (!p) {
    return (
      <div data-testid="readings-provenance" style={{ ...textStyle, display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10 }}>
        <FreshnessIndicator confidence={null} />
        <span>junction_point_readings freshness not reported</span>
      </div>
    );
  }
  const ageParts = [];
  if (p.age_days != null) ageParts.push(`${p.age_days}d old`);
  if (p.stale_after_days != null) ageParts.push(`threshold ${p.stale_after_days}d`);
  const newest = p.max_obs_date
    ? `newest obs ${p.max_obs_date}${ageParts.length ? ` (${ageParts.join(', ')})` : ''}`
    : 'newest obs date unknown';
  const isStale = p.unavailable === true && p.reason === 'stale';
  return (
    <div data-testid="readings-provenance" style={{ ...textStyle, display: 'flex', alignItems: 'center', gap: 6, marginBottom: 10, flexWrap: 'wrap' }}>
      <FreshnessIndicator
        confidence={p.unavailable === true ? null : 'confirmed'}
        asOf={p.max_obs_date}
        stale={isStale}
        unavailable={p.unavailable === true && !isStale}
        reason={p.reason}
      />
      <span>junction_point_readings: {newest}{p.unavailable === true && p.reason ? ` — ${p.reason}` : ''}</span>
    </div>
  );
}

export default function JunctionDashboard() {
  const { data, loading, error, refetch } = useJunctionPoints();
  const [filterLayer, setFilterLayer] = useState(null);
  const [search, setSearch] = useState('');

  const points = useMemo(() => {
    if (!data?.junction_points) return [];
    let pts = data.junction_points;
    if (filterLayer) pts = pts.filter(p => p.layer === filterLayer);
    if (search) {
      const q = search.toLowerCase();
      pts = pts.filter(p => (p.label || p.id).toLowerCase().includes(q));
    }
    return pts;
  }, [data, filterLayer, search]);

  const summaries = useMemo(() => {
    if (!data?.layer_summaries) return [];
    return [...data.layer_summaries].sort((a, b) => a.order - b.order);
  }, [data]);

  if (loading) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px' }}>
        Loading junction points...
      </div>
    );
  }

  if (error) {
    return (
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: colors.red, fontFamily: colors.mono, fontSize: '12px' }}>
        {error}
      </div>
    );
  }

  return (
    <div style={{ height: '100%', overflow: 'auto', padding: '12px' }}>
      {/* Layer summary bar */}
      <div style={{ display: 'flex', gap: 6, marginBottom: 12, flexWrap: 'wrap' }}>
        <button
          onClick={() => setFilterLayer(null)}
          style={{
            background: !filterLayer ? colors.accent + '30' : 'transparent',
            border: `1px solid ${!filterLayer ? colors.accent : colors.border}`,
            borderRadius: 6, padding: '4px 10px', cursor: 'pointer',
            fontSize: '10px', fontWeight: 700, fontFamily: colors.mono,
            color: !filterLayer ? colors.accent : colors.textDim,
          }}
        >
          ALL ({data?.total_junction_points || 0})
        </button>
        {summaries.map(s => (
          <button
            key={s.id}
            onClick={() => setFilterLayer(filterLayer === s.id ? null : s.id)}
            style={{
              background: filterLayer === s.id ? (LAYER_COLORS[s.id] || colors.accent) + '20' : 'transparent',
              border: `1px solid ${filterLayer === s.id ? LAYER_COLORS[s.id] || colors.accent : colors.border}`,
              borderRadius: 6, padding: '4px 10px', cursor: 'pointer',
              fontSize: '10px', fontWeight: 600, fontFamily: colors.mono,
              color: LAYER_COLORS[s.id] || colors.textDim,
              display: 'flex', alignItems: 'center', gap: 4,
            }}
          >
            {s.label} ({s.node_count})
            <FreshnessIndicator confidence={s.dominant_confidence} compact />
          </button>
        ))}
      </div>

      <ReadingsProvenance provenance={data?.provenance} />

      {/* Search */}
      <input
        type="text" placeholder="Search junctions..." value={search}
        onChange={e => setSearch(e.target.value)}
        style={{
          width: '100%', maxWidth: 300, marginBottom: 12,
          background: colors.bg, border: `1px solid ${colors.border}`,
          borderRadius: 6, padding: '6px 10px',
          color: colors.text, fontFamily: colors.mono, fontSize: '11px',
          outline: 'none',
        }}
      />

      {/* Layer aggregate cards */}
      {!filterLayer && (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 8, marginBottom: 16 }}>
          {summaries.map(s => (
            <div key={s.id} style={{
              background: colors.cardElevated, border: `1px solid ${colors.border}`,
              borderRadius: 10, padding: '10px 14px',
              borderTop: `3px solid ${LAYER_COLORS[s.id] || colors.accent}`,
            }}>
              <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <span style={{ fontSize: '11px', fontWeight: 700, color: LAYER_COLORS[s.id], fontFamily: colors.mono }}>{s.label}</span>
                <FreshnessIndicator confidence={s.dominant_confidence} />
              </div>
              <div style={{ fontSize: '14px', fontWeight: 700, color: colors.text, fontFamily: colors.mono }}>
                {fmtDollar(s.aggregate_value)}
              </div>
              <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '9px', fontFamily: colors.mono, marginTop: 4 }}>
                <span style={{ color: s.aggregate_change_1m == null ? colors.textDim : s.aggregate_change_1m >= 0 ? colors.green : colors.red }}>
                  1M: {fmtDollar(s.aggregate_change_1m)}
                </span>
                <span style={{ color: colors.textDim, textTransform: 'uppercase' }}>
                  {s.regime || 'regime unknown'}
                </span>
              </div>
              {s.stress_z != null && (
                <div style={{ fontSize: '9px', color: Math.abs(s.stress_z) > 1 ? colors.yellow : colors.textDim, fontFamily: colors.mono, marginTop: 2 }}>
                  stress: {s.stress_z?.toFixed(2)}
                </div>
              )}
            </div>
          ))}
        </div>
      )}

      {/* Junction point cards grid */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 8 }}>
        {points.map(p => (
          <JunctionCard key={p.id} point={p} />
        ))}
      </div>

      {points.length === 0 && (
        <div style={{ textAlign: 'center', color: colors.textDim, fontFamily: colors.mono, fontSize: '12px', padding: 40 }}>
          No junction points found
        </div>
      )}
    </div>
  );
}
