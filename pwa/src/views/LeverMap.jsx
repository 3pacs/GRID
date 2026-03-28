/**
 * LeverMap -- Global Lever Map: Hierarchical visualization of world economic power.
 *
 * Shows all 8 lever domains in a radial/hierarchical layout:
 *   - Click a domain to expand tier 1/2/3/4 actors
 *   - Click an actor to see what they control + who influences them
 *   - Animated pulses show which levers are currently being pulled
 *   - Cross-domain arcs connect actors who appear in multiple domains
 *
 * API: GET /api/v1/intelligence/levers
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens } from '../styles/shared.js';

// ── Domain colors ───────────────────────────────────────────────────────
const DOMAIN_COLORS = {
  monetary_policy:    '#EF4444',
  fiscal_policy:      '#F59E0B',
  regulation:         '#8B5CF6',
  capital_allocation: '#3B82F6',
  information:        '#06B6D4',
  technology:         '#10B981',
  energy:             '#F97316',
  trade:              '#EC4899',
};

const DOMAIN_ICONS = {
  monetary_policy:    '\u{1F3E6}',
  fiscal_policy:      '\u{1F4B5}',
  regulation:         '\u2696\uFE0F',
  capital_allocation: '\u{1F4CA}',
  information:        '\u{1F4F0}',
  technology:         '\u{1F4BB}',
  energy:             '\u26A1',
  trade:              '\u{1F6A2}',
};

const TIER_OPACITY = {
  tier_1: 1.0,
  tier_2: 0.75,
  tier_3: 0.55,
  tier_4: 0.40,
};

const TIER_LABELS = {
  tier_1: 'Direct Control',
  tier_2: 'Influence',
  tier_3: 'React & Amplify',
  tier_4: 'Price Takers',
};

// ── Formatting helpers ──────────────────────────────────────────────────

function _fmtDollar(val) {
  if (val == null || isNaN(val)) return '--';
  const abs = Math.abs(val);
  if (abs >= 1e12) return `$${(abs / 1e12).toFixed(1)}T`;
  if (abs >= 1e9) return `$${(abs / 1e9).toFixed(1)}B`;
  if (abs >= 1e6) return `$${(abs / 1e6).toFixed(0)}M`;
  return `$${abs.toLocaleString()}`;
}

function _influenceBar(val) {
  const pct = Math.round((val || 0) * 100);
  return `${pct}%`;
}

// ── Styles ──────────────────────────────────────────────────────────────

const S = {
  container: {
    width: '100%',
    minHeight: '100vh',
    background: '#080C10',
    color: '#E2E8F0',
    fontFamily: "'IBM Plex Sans', sans-serif",
    display: 'flex',
    flexDirection: 'column',
    overflow: 'hidden',
  },
  header: {
    padding: '16px 24px',
    borderBottom: '1px solid #1A2332',
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    flexWrap: 'wrap',
    gap: '12px',
  },
  title: {
    fontSize: '22px',
    fontWeight: 700,
    letterSpacing: '-0.02em',
    margin: 0,
  },
  subtitle: {
    fontSize: '13px',
    color: '#8AA0B8',
    margin: 0,
  },
  main: {
    display: 'flex',
    flex: 1,
    overflow: 'hidden',
  },
  domainGrid: {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))',
    gap: '16px',
    padding: '20px',
    flex: 1,
    overflowY: 'auto',
  },
  domainCard: (color, expanded) => ({
    background: expanded ? '#111B2A' : '#0D1520',
    border: `1px solid ${expanded ? color : '#1A2332'}`,
    borderRadius: '10px',
    padding: '16px',
    cursor: 'pointer',
    transition: 'all 0.25s ease',
    boxShadow: expanded ? `0 0 20px ${color}22` : 'none',
    gridColumn: expanded ? '1 / -1' : 'auto',
  }),
  domainHeader: {
    display: 'flex',
    alignItems: 'center',
    gap: '10px',
    marginBottom: '8px',
  },
  domainIcon: {
    fontSize: '24px',
  },
  domainLabel: {
    fontSize: '15px',
    fontWeight: 600,
    flex: 1,
  },
  domainStats: {
    fontSize: '12px',
    color: '#8AA0B8',
    fontFamily: "'IBM Plex Mono', monospace",
  },
  tierSection: {
    marginTop: '12px',
    paddingTop: '12px',
    borderTop: '1px solid #1A233266',
  },
  tierLabel: {
    fontSize: '11px',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.08em',
    marginBottom: '8px',
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
  },
  actorRow: (color, selected) => ({
    display: 'flex',
    alignItems: 'center',
    gap: '10px',
    padding: '8px 10px',
    borderRadius: '6px',
    cursor: 'pointer',
    background: selected ? `${color}15` : 'transparent',
    border: selected ? `1px solid ${color}44` : '1px solid transparent',
    transition: 'all 0.15s ease',
    marginBottom: '4px',
  }),
  actorDot: (color, influence) => ({
    width: `${8 + influence * 12}px`,
    height: `${8 + influence * 12}px`,
    borderRadius: '50%',
    background: color,
    opacity: influence,
    flexShrink: 0,
  }),
  actorName: {
    fontSize: '13px',
    fontWeight: 500,
    flex: 1,
  },
  actorInfluence: {
    fontSize: '11px',
    fontFamily: "'IBM Plex Mono', monospace",
    color: '#8AA0B8',
  },
  sidebar: {
    width: '380px',
    background: '#0D1520',
    borderLeft: '1px solid #1A2332',
    overflowY: 'auto',
    padding: '20px',
    flexShrink: 0,
  },
  sidebarTitle: {
    fontSize: '16px',
    fontWeight: 700,
    marginBottom: '4px',
  },
  sidebarEntity: {
    fontSize: '12px',
    color: '#8AA0B8',
    marginBottom: '16px',
  },
  sectionLabel: {
    fontSize: '11px',
    fontWeight: 600,
    textTransform: 'uppercase',
    letterSpacing: '0.06em',
    color: '#8AA0B8',
    marginBottom: '6px',
    marginTop: '14px',
  },
  tag: (color) => ({
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: '4px',
    fontSize: '11px',
    fontFamily: "'IBM Plex Mono', monospace",
    background: `${color}20`,
    color: color,
    marginRight: '4px',
    marginBottom: '4px',
  }),
  personRow: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'space-between',
    padding: '4px 0',
    fontSize: '12px',
  },
  crossArc: {
    position: 'relative',
  },
  crossLink: (color) => ({
    display: 'flex',
    alignItems: 'center',
    gap: '6px',
    padding: '4px 8px',
    borderRadius: '4px',
    fontSize: '11px',
    background: `${color}10`,
    border: `1px solid ${color}22`,
    marginBottom: '4px',
  }),
  transmission: {
    fontSize: '11px',
    fontFamily: "'IBM Plex Mono', monospace",
    color: '#8AA0B8',
    lineHeight: '1.6',
    padding: '8px 10px',
    background: '#080C10',
    borderRadius: '6px',
    marginTop: '8px',
  },
  pill: (color) => ({
    display: 'inline-flex',
    alignItems: 'center',
    gap: '4px',
    padding: '2px 8px',
    borderRadius: '12px',
    fontSize: '11px',
    fontWeight: 600,
    background: `${color}20`,
    color: color,
  }),
  arcCanvas: {
    position: 'absolute',
    top: 0,
    left: 0,
    pointerEvents: 'none',
    zIndex: 10,
  },
  loading: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100vh',
    fontSize: '16px',
    color: '#8AA0B8',
  },
  error: {
    display: 'flex',
    alignItems: 'center',
    justifyContent: 'center',
    height: '100vh',
    fontSize: '14px',
    color: '#EF4444',
  },
  topBar: {
    display: 'flex',
    gap: '8px',
    alignItems: 'center',
  },
  btn: (active) => ({
    padding: '6px 12px',
    borderRadius: '6px',
    fontSize: '12px',
    fontWeight: 500,
    border: active ? '1px solid #3B82F6' : '1px solid #1A2332',
    background: active ? '#3B82F620' : 'transparent',
    color: active ? '#3B82F6' : '#8AA0B8',
    cursor: 'pointer',
    transition: 'all 0.15s ease',
  }),
};

// ── Component ───────────────────────────────────────────────────────────

export default function LeverMap() {
  const [data, setData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [expandedDomain, setExpandedDomain] = useState(null);
  const [selectedActor, setSelectedActor] = useState(null);
  const [selectedActorDomain, setSelectedActorDomain] = useState(null);
  const [showCrossArcs, setShowCrossArcs] = useState(true);
  const [crossDomainActors, setCrossDomainActors] = useState([]);
  const [chainData, setChainData] = useState(null);
  const [viewMode, setViewMode] = useState('grid'); // 'grid' | 'report'
  const [report, setReport] = useState(null);
  const arcCanvasRef = useRef(null);
  const domainRefs = useRef({});

  // ── Data fetch ──────────────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        setLoading(true);
        const res = await api._fetch('/api/v1/intelligence/levers');
        const json = await res;
        if (!cancelled) {
          setData(json);
          setCrossDomainActors(json.cross_domain_actors || []);
        }
      } catch (err) {
        if (!cancelled) setError(err.message || 'Failed to load lever map');
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => { cancelled = true; };
  }, []);

  // ── Fetch lever chain when an event is clicked ──────────────────────
  const fetchChain = useCallback(async (event) => {
    try {
      const res = await api._fetch(`/api/v1/intelligence/levers/chain/${encodeURIComponent(event)}`);
      setChainData(res);
    } catch (err) {
      console.warn('Chain fetch failed:', err);
    }
  }, []);

  // ── Fetch narrative report ──────────────────────────────────────────
  const fetchReport = useCallback(async () => {
    try {
      const res = await api._fetch('/api/v1/intelligence/levers/report');
      setReport(res.report || 'No report available.');
    } catch (err) {
      setReport('Failed to load report.');
    }
  }, []);

  useEffect(() => {
    if (viewMode === 'report' && !report) fetchReport();
  }, [viewMode, report, fetchReport]);

  // ── Draw cross-domain arcs ──────────────────────────────────────────
  useEffect(() => {
    if (!showCrossArcs || !crossDomainActors.length || !arcCanvasRef.current) return;
    const canvas = arcCanvasRef.current;
    const ctx = canvas.getContext('2d');
    const rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width;
    canvas.height = rect.height;
    ctx.clearRect(0, 0, canvas.width, canvas.height);

    // Draw arcs between domains that share actors
    const domainPositions = {};
    Object.keys(DOMAIN_COLORS).forEach((dk) => {
      const el = domainRefs.current[dk];
      if (el) {
        const r = el.getBoundingClientRect();
        const parentR = canvas.parentElement.getBoundingClientRect();
        domainPositions[dk] = {
          x: r.left - parentR.left + r.width / 2,
          y: r.top - parentR.top + r.height / 2,
        };
      }
    });

    ctx.globalAlpha = 0.25;
    ctx.lineWidth = 1.5;

    crossDomainActors.forEach((actor) => {
      const domains = actor.domains || [];
      for (let i = 0; i < domains.length; i++) {
        for (let j = i + 1; j < domains.length; j++) {
          const a = domainPositions[domains[i]];
          const b = domainPositions[domains[j]];
          if (!a || !b) continue;
          const color = DOMAIN_COLORS[domains[i]] || '#3B82F6';
          ctx.strokeStyle = color;
          ctx.beginPath();
          const midX = (a.x + b.x) / 2;
          const midY = (a.y + b.y) / 2 - 40;
          ctx.moveTo(a.x, a.y);
          ctx.quadraticCurveTo(midX, midY, b.x, b.y);
          ctx.stroke();
        }
      }
    });
  }, [showCrossArcs, crossDomainActors, expandedDomain, data]);

  // ── Actor click handler ─────────────────────────────────────────────
  const handleActorClick = useCallback((actorId, actorData, domain) => {
    setSelectedActor({ id: actorId, ...actorData });
    setSelectedActorDomain(domain);
  }, []);

  // ── Render helpers ──────────────────────────────────────────────────

  const renderActorRow = (actorId, actorData, domain, tierKey, color) => {
    const isSelected = selectedActor && selectedActor.id === actorId && selectedActorDomain === domain;
    const influence = actorData.influence || 0;
    const hasSignals = actorData.recent_signals && actorData.recent_signals.length > 0;

    return (
      <div
        key={`${domain}-${tierKey}-${actorId}`}
        style={S.actorRow(color, isSelected)}
        onClick={(e) => { e.stopPropagation(); handleActorClick(actorId, actorData, domain); }}
        onMouseEnter={(e) => { e.currentTarget.style.background = `${color}10`; }}
        onMouseLeave={(e) => {
          if (!isSelected) e.currentTarget.style.background = 'transparent';
        }}
      >
        <div style={S.actorDot(color, influence)} />
        <span style={S.actorName}>{actorData.name || actorId}</span>
        {hasSignals && (
          <span style={S.pill('#22C55E')}>ACTIVE</span>
        )}
        <span style={S.actorInfluence}>{_influenceBar(influence)}</span>
      </div>
    );
  };

  const renderTier = (tierKey, tierActors, domain, color) => {
    const label = TIER_LABELS[tierKey] || tierKey;
    return (
      <div key={tierKey} style={S.tierSection}>
        <div style={{ ...S.tierLabel, color }}>
          <span style={{ opacity: TIER_OPACITY[tierKey] || 0.5 }}>{'\u25CF'}</span>
          {label} ({Object.keys(tierActors).length})
        </div>
        {Object.entries(tierActors).map(([actorId, actorData]) =>
          renderActorRow(actorId, actorData, domain, tierKey, color)
        )}
      </div>
    );
  };

  const renderDomainCard = (domainKey, domainData) => {
    const color = DOMAIN_COLORS[domainKey] || '#3B82F6';
    const icon = DOMAIN_ICONS[domainKey] || '';
    const expanded = expandedDomain === domainKey;
    const actors = domainData.actors || {};
    const actorCount = Object.values(actors).reduce(
      (sum, tier) => sum + Object.keys(tier).length, 0
    );
    const tier1Count = Object.keys(actors.tier_1 || {}).length;

    return (
      <div
        key={domainKey}
        ref={(el) => { domainRefs.current[domainKey] = el; }}
        style={S.domainCard(color, expanded)}
        onClick={() => setExpandedDomain(expanded ? null : domainKey)}
      >
        <div style={S.domainHeader}>
          <span style={S.domainIcon}>{icon}</span>
          <span style={{ ...S.domainLabel, color }}>{domainData.label}</span>
          <span style={S.domainStats}>
            {actorCount} actors / {tier1Count} tier-1
          </span>
        </div>

        {!expanded && domainData.transmission && (
          <div style={{ fontSize: '11px', color: '#5A7080', marginTop: '4px' }}>
            {domainData.transmission.split(' -> ').slice(0, 4).join(' \u2192 ')}...
          </div>
        )}

        {expanded && (
          <div>
            {Object.entries(actors).map(([tierKey, tierActors]) =>
              renderTier(tierKey, tierActors, domainKey, color)
            )}
            {domainData.transmission && (
              <div style={S.transmission}>
                <strong>Transmission:</strong>{' '}
                {domainData.transmission.replace(/ -> /g, ' \u2192 ')}
              </div>
            )}
          </div>
        )}
      </div>
    );
  };

  // ── Sidebar: selected actor detail ──────────────────────────────────

  const renderSidebar = () => {
    if (!selectedActor) {
      return (
        <div style={S.sidebar}>
          <div style={{ ...S.sidebarTitle, marginBottom: '16px' }}>
            Global Lever Map
          </div>
          <div style={S.sectionLabel}>Cross-Domain Power Brokers</div>
          {crossDomainActors.slice(0, 12).map((actor) => (
            <div
              key={actor.actor_id}
              style={{
                display: 'flex', justifyContent: 'space-between',
                padding: '6px 0', borderBottom: '1px solid #1A233233',
                fontSize: '12px',
              }}
            >
              <span>{actor.name}</span>
              <span style={{ color: '#8AA0B8', fontFamily: "'IBM Plex Mono', monospace" }}>
                {actor.domain_count}d / {_influenceBar(actor.max_influence)}
              </span>
            </div>
          ))}

          <div style={S.sectionLabel}>Event Chains</div>
          {[
            'interest_rate_hike', 'interest_rate_cut', 'tariff_war',
            'oil_supply_cut', 'tech_antitrust', 'bank_stress',
            'ai_capex_boom', 'quantitative_tightening',
          ].map((evt) => (
            <div
              key={evt}
              style={{
                padding: '6px 8px', borderRadius: '4px', fontSize: '12px',
                cursor: 'pointer', marginBottom: '4px',
                background: chainData && chainData.event === evt ? '#3B82F615' : 'transparent',
                border: chainData && chainData.event === evt ? '1px solid #3B82F644' : '1px solid transparent',
              }}
              onClick={() => fetchChain(evt)}
            >
              {evt.replace(/_/g, ' ')}
            </div>
          ))}

          {chainData && chainData.chain && (
            <div style={{ marginTop: '12px' }}>
              <div style={S.sectionLabel}>
                Chain: {(chainData.event || '').replace(/_/g, ' ')}
              </div>
              {chainData.chain
                .filter((s) => s.actor)
                .map((step, i) => (
                  <div key={i} style={{
                    display: 'flex', alignItems: 'center', gap: '8px',
                    fontSize: '11px', padding: '4px 0',
                  }}>
                    <span style={{
                      color: DOMAIN_COLORS[step.domain] || '#8AA0B8',
                      fontWeight: 600, minWidth: '80px',
                    }}>
                      {step.actor}
                    </span>
                    <span style={{ color: '#5A7080' }}>{'\u2192'}</span>
                    <span style={{ color: '#8AA0B8' }}>{step.action}</span>
                  </div>
                ))}
            </div>
          )}
        </div>
      );
    }

    const a = selectedActor;
    const domainColor = DOMAIN_COLORS[selectedActorDomain] || '#3B82F6';

    return (
      <div style={S.sidebar}>
        <div style={{ ...S.sidebarTitle, color: domainColor }}>{a.name || a.id}</div>
        <div style={S.sidebarEntity}>{a.entity || ''}</div>

        {/* Influence meter */}
        <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
          <div style={{
            flex: 1, height: '6px', background: '#1A2332', borderRadius: '3px', overflow: 'hidden',
          }}>
            <div style={{
              width: `${(a.influence || 0) * 100}%`,
              height: '100%',
              background: domainColor,
              borderRadius: '3px',
              transition: 'width 0.3s ease',
            }} />
          </div>
          <span style={S.actorInfluence}>{_influenceBar(a.influence)}</span>
        </div>

        {a.aum && (
          <div style={{ fontSize: '12px', marginBottom: '8px' }}>
            AUM: <strong>{_fmtDollar(a.aum)}</strong>
          </div>
        )}

        {/* Controls */}
        {a.controls && a.controls.length > 0 && (
          <>
            <div style={S.sectionLabel}>Controls</div>
            <div style={{ marginBottom: '8px' }}>
              {a.controls.map((c) => (
                <span key={c} style={S.tag(domainColor)}>
                  {c.replace(/_/g, ' ')}
                </span>
              ))}
            </div>
          </>
        )}

        {/* Reports to */}
        {a.reports_to && a.reports_to.length > 0 && (
          <>
            <div style={S.sectionLabel}>Reports To</div>
            <div style={{ marginBottom: '8px' }}>
              {a.reports_to.map((r) => (
                <span key={r} style={S.tag('#F59E0B')}>
                  {r.replace(/_/g, ' ')}
                </span>
              ))}
            </div>
          </>
        )}

        {/* Influenced by */}
        {a.influenced_by && a.influenced_by.length > 0 && (
          <>
            <div style={S.sectionLabel}>Influenced By</div>
            <div style={{ marginBottom: '8px' }}>
              {a.influenced_by.map((ib) => (
                <span key={ib} style={S.tag('#8B5CF6')}>
                  {ib.replace(/_/g, ' ')}
                </span>
              ))}
            </div>
          </>
        )}

        {/* Cross-domain links */}
        {a.cross_domain && Object.keys(a.cross_domain).length > 0 && (
          <>
            <div style={S.sectionLabel}>Cross-Domain Links</div>
            {Object.entries(a.cross_domain).map(([targetDomain, desc]) => (
              <div
                key={targetDomain}
                style={S.crossLink(DOMAIN_COLORS[targetDomain] || '#3B82F6')}
              >
                <span style={{ fontSize: '14px' }}>{DOMAIN_ICONS[targetDomain] || ''}</span>
                <span style={{ flex: 1 }}>{desc}</span>
              </div>
            ))}
          </>
        )}

        {/* Key personnel */}
        {a.key_personnel && a.key_personnel.length > 0 && (
          <>
            <div style={S.sectionLabel}>Key Personnel</div>
            {a.key_personnel.map((p, i) => (
              <div key={i} style={S.personRow}>
                <span>{p.name}</span>
                <span style={{ color: '#8AA0B8', fontSize: '11px' }}>
                  {p.title}{p.influence ? ` (${_influenceBar(p.influence)})` : ''}
                </span>
              </div>
            ))}
          </>
        )}

        {/* Also appears in */}
        {a.also_appears_in && a.also_appears_in.length > 0 && (
          <>
            <div style={S.sectionLabel}>Also Appears In</div>
            {a.also_appears_in.map((app, i) => (
              <div key={i} style={{
                display: 'flex', alignItems: 'center', gap: '6px',
                fontSize: '12px', padding: '3px 0',
              }}>
                <span style={{ fontSize: '14px' }}>{DOMAIN_ICONS[app.domain] || ''}</span>
                <span style={{ color: DOMAIN_COLORS[app.domain] || '#8AA0B8' }}>
                  {app.domain.replace(/_/g, ' ')}
                </span>
                <span style={{ color: '#5A7080' }}>({app.tier})</span>
              </div>
            ))}
          </>
        )}

        {/* Confidence */}
        {a.confidence && (
          <div style={{ marginTop: '16px', fontSize: '11px', color: '#5A7080' }}>
            Data confidence: <strong style={{ color: '#8AA0B8' }}>{a.confidence}</strong>
          </div>
        )}

        {/* Close button */}
        <button
          style={{
            marginTop: '16px', padding: '8px 16px', borderRadius: '6px',
            background: 'transparent', border: '1px solid #1A2332',
            color: '#8AA0B8', cursor: 'pointer', fontSize: '12px', width: '100%',
          }}
          onClick={() => { setSelectedActor(null); setSelectedActorDomain(null); }}
        >
          Close
        </button>
      </div>
    );
  };

  // ── Report view ─────────────────────────────────────────────────────

  const renderReport = () => (
    <div style={{
      flex: 1, padding: '24px', overflowY: 'auto',
      fontFamily: "'IBM Plex Mono', monospace",
      fontSize: '13px', lineHeight: '1.8',
      whiteSpace: 'pre-wrap', color: '#C9D1D9',
    }}>
      {report || 'Loading report...'}
    </div>
  );

  // ── Main render ─────────────────────────────────────────────────────

  if (loading) return <div style={S.loading}>Loading global lever map...</div>;
  if (error) return <div style={S.error}>{error}</div>;
  if (!data || !data.hierarchy) return <div style={S.error}>No lever data available.</div>;

  const hierarchy = data.hierarchy;

  return (
    <div style={S.container}>
      <div style={S.header}>
        <div>
          <h1 style={S.title}>Global Lever Map</h1>
          <p style={S.subtitle}>
            {data.total_domains} domains / {data.total_actors} actors / {crossDomainActors.length} cross-domain
          </p>
        </div>
        <div style={S.topBar}>
          <button style={S.btn(viewMode === 'grid')} onClick={() => setViewMode('grid')}>
            Grid
          </button>
          <button style={S.btn(viewMode === 'report')} onClick={() => setViewMode('report')}>
            Report
          </button>
          <button
            style={S.btn(showCrossArcs)}
            onClick={() => setShowCrossArcs(!showCrossArcs)}
          >
            Arcs {showCrossArcs ? 'ON' : 'OFF'}
          </button>
        </div>
      </div>

      <div style={S.main}>
        {viewMode === 'grid' ? (
          <div style={{ ...S.domainGrid, position: 'relative' }}>
            <canvas ref={arcCanvasRef} style={S.arcCanvas} />
            {Object.entries(hierarchy).map(([domainKey, domainData]) =>
              renderDomainCard(domainKey, domainData)
            )}
          </div>
        ) : (
          renderReport()
        )}
        {renderSidebar()}
      </div>
    </div>
  );
}
