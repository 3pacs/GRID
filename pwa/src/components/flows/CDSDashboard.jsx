/**
 * CDSDashboard — Credit Default Swap proxy dashboard.
 * Shows spread levels, term structure, compression ratio, ETF signals.
 */
import React, { useState, useEffect, useCallback } from 'react';
import { colors } from '../../styles/shared.js';
import { api } from '../../api.js';
import FreshnessIndicator from './FreshnessIndicator.jsx';

const FONT = colors.mono || "'IBM Plex Mono', monospace";

function fmtSpread(val) {
  if (val == null) return '--';
  return `${val.toFixed(2)}%`;
}

function fmtChange(val) {
  if (val == null) return '--';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}`;
}

function changeColor(val) {
  if (val == null) return colors.textDim;
  // For spreads: widening (positive) = bad = red, tightening (negative) = good = green
  return val > 0 ? colors.red : val < 0 ? colors.green : colors.textDim;
}

function zColor(z) {
  if (z == null) return colors.textDim;
  if (z > 2) return colors.red;
  if (z > 1) return colors.yellow;
  return colors.green;
}

function regimeColor(regime) {
  if (regime === 'risk_on') return colors.green;
  if (regime === 'risk_off' || regime === 'stress') return colors.red;
  if (regime === 'transitioning') return colors.yellow;
  return colors.textDim;
}

function pctBar(pct) {
  if (pct == null) return null;
  const width = Math.max(4, pct * 100);
  const bg = pct > 0.8 ? colors.red : pct > 0.5 ? colors.yellow : colors.green;
  return (
    <div style={{ width: '100%', height: 4, background: colors.border, borderRadius: 2, overflow: 'hidden' }}>
      <div style={{ width: `${width}%`, height: '100%', background: bg, borderRadius: 2 }} />
    </div>
  );
}

export default function CDSDashboard() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  const fetch = useCallback(async () => {
    setLoading(true);
    const res = await api.getCdsDashboard();
    if (res.error) {
      setError(res.message);
    } else {
      setData(res);
    }
    setLoading(false);
  }, []);

  useEffect(() => { fetch(); }, [fetch]);

  if (loading) {
    return <div style={sty.center}>Loading CDS dashboard...</div>;
  }
  if (error) {
    return <div style={{ ...sty.center, color: colors.red }}>{error}</div>;
  }
  if (!data) return null;

  const { spreads = [], regime, hy_ig_compression, term_slope, spread_momentum, narrative, etf_signals = {} } = data;

  return (
    <div style={{ height: '100%', overflow: 'auto', padding: 12 }}>
      {/* Regime + narrative header */}
      <div style={sty.header}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
          <span style={{ ...sty.regime, color: regimeColor(regime), borderColor: regimeColor(regime) }}>
            {(regime || 'unknown').toUpperCase().replace('_', ' ')}
          </span>
          <span style={{ ...sty.momentum, color: spread_momentum === 'tightening' ? colors.green : spread_momentum === 'widening' ? colors.red : colors.textDim }}>
            {spread_momentum || '--'}
          </span>
        </div>
        <p style={sty.narrative}>{narrative}</p>
      </div>

      {/* Key metrics */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(160px, 1fr))', gap: 8, marginBottom: 16 }}>
        <MetricCard label="HY/IG Compression" value={hy_ig_compression?.toFixed(2) + 'x'} sub={hy_ig_compression < 2 ? 'Risk appetite HIGH' : hy_ig_compression > 3 ? 'Discrimination HIGH' : 'Moderate'} color={hy_ig_compression < 2 ? colors.yellow : hy_ig_compression > 3 ? colors.red : colors.green} />
        <MetricCard label="Term Slope (CCC/BB)" value={term_slope?.toFixed(2) + 'x'} sub={term_slope > 5 ? 'Distress gradient steep' : 'Normal gradient'} color={term_slope > 5 ? colors.yellow : colors.green} />
        <MetricCard label="Spread Momentum" value={spread_momentum} sub="5-day direction" color={spread_momentum === 'tightening' ? colors.green : spread_momentum === 'widening' ? colors.red : colors.textDim} />
      </div>

      {/* Spread table */}
      <div style={sty.section}>CREDIT SPREADS (OAS)</div>
      <div style={{ overflowX: 'auto' }}>
        <table style={sty.table}>
          <thead>
            <tr>
              <th style={sty.th}>Series</th>
              <th style={sty.th}>Level</th>
              <th style={sty.th}>1D</th>
              <th style={sty.th}>5D</th>
              <th style={sty.th}>20D</th>
              <th style={sty.th}>Z-Score</th>
              <th style={sty.th}>Percentile</th>
              <th style={sty.th}>Status</th>
            </tr>
          </thead>
          <tbody>
            {spreads.map(s => (
              <tr key={s.key}>
                <td style={sty.td}><span style={{ fontWeight: 600 }}>{s.label}</span></td>
                <td style={{ ...sty.td, fontWeight: 700 }}>{fmtSpread(s.value)}</td>
                <td style={{ ...sty.td, color: changeColor(s.change_1d) }}>{fmtChange(s.change_1d)}</td>
                <td style={{ ...sty.td, color: changeColor(s.change_5d) }}>{fmtChange(s.change_5d)}</td>
                <td style={{ ...sty.td, color: changeColor(s.change_20d) }}>{fmtChange(s.change_20d)}</td>
                <td style={{ ...sty.td, color: zColor(s.z_score_2y), fontWeight: 600 }}>{s.z_score_2y?.toFixed(2) ?? '--'}</td>
                <td style={sty.td}>
                  <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
                    {pctBar(s.percentile_2y)}
                    <span style={{ fontSize: '9px', minWidth: 30 }}>{s.percentile_2y != null ? `${(s.percentile_2y * 100).toFixed(0)}%` : '--'}</span>
                  </div>
                </td>
                <td style={sty.td}>
                  {s.is_stressed
                    ? <span style={{ color: colors.red, fontWeight: 700 }}>STRESS</span>
                    : <FreshnessIndicator confidence={s.confidence} />
                  }
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* ETF signals */}
      <div style={{ ...sty.section, marginTop: 16 }}>ETF SPREAD PROXIES</div>
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))', gap: 8 }}>
        {Object.entries(etf_signals).map(([key, sig]) => (
          <div key={key} style={sty.etfCard}>
            <div style={{ fontSize: '11px', fontWeight: 700, marginBottom: 4 }}>{sig.label}</div>
            <div style={{ display: 'flex', justifyContent: 'space-between', fontSize: '10px' }}>
              <span>Ratio: {sig.ratio?.toFixed(4) ?? '--'}</span>
              <span style={{ color: sig.signal === 'tightening' ? colors.green : sig.signal === 'widening' ? colors.red : colors.textDim, fontWeight: 600 }}>
                {sig.signal?.toUpperCase()}
              </span>
            </div>
            {sig.change_5d != null && (
              <div style={{ fontSize: '9px', color: colors.textDim, marginTop: 2 }}>
                5D change: <span style={{ color: sig.change_5d > 0 ? colors.green : colors.red }}>{sig.change_5d > 0 ? '+' : ''}{sig.change_5d.toFixed(4)}</span>
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  );
}

function MetricCard({ label, value, sub, color }) {
  return (
    <div style={{ background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: '10px 14px' }}>
      <div style={{ fontSize: '9px', color: colors.textDim, fontFamily: FONT, letterSpacing: '0.5px', marginBottom: 4 }}>{label}</div>
      <div style={{ fontSize: '18px', fontWeight: 700, color: color || colors.text, fontFamily: FONT }}>{value || '--'}</div>
      <div style={{ fontSize: '9px', color: colors.textDim, fontFamily: FONT, marginTop: 2 }}>{sub}</div>
    </div>
  );
}

const sty = {
  center: { display: 'flex', alignItems: 'center', justifyContent: 'center', height: '100%', color: colors.textDim, fontFamily: FONT, fontSize: '12px' },
  header: { marginBottom: 16 },
  regime: { fontSize: '12px', fontWeight: 700, fontFamily: FONT, letterSpacing: '1px', border: '1px solid', borderRadius: 6, padding: '4px 10px' },
  momentum: { fontSize: '11px', fontWeight: 600, fontFamily: FONT },
  narrative: { fontSize: '11px', color: colors.textDim, fontFamily: FONT, marginTop: 8, lineHeight: '1.5' },
  section: { fontSize: '10px', fontWeight: 700, color: colors.accent, letterSpacing: '1.5px', fontFamily: FONT, marginBottom: 8 },
  table: { width: '100%', borderCollapse: 'collapse', fontFamily: FONT, fontSize: '11px' },
  th: { textAlign: 'left', padding: '6px 8px', borderBottom: `1px solid ${colors.border}`, color: colors.textDim, fontSize: '9px', fontWeight: 700, letterSpacing: '0.5px' },
  td: { padding: '6px 8px', borderBottom: `1px solid ${colors.border}20`, color: colors.text },
  etfCard: { background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 8, padding: '10px 12px', fontFamily: FONT },
};
