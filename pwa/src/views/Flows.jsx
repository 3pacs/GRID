/**
 * Flows — Interactive sector flow visualization.
 *
 * Tappable sector cards that expand to show subsectors, actors,
 * their influence weights, live z-scores, and options data.
 * Shows who moves markets and which data points matter.
 *
 * Detail view drills into per-subsector actor breakdowns with
 * relative performance vs the sector ETF.
 */
import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import { shared, colors, tokens } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';
import FlowSankey8 from '../components/flows/FlowSankey8.jsx';
import TimeframeComparison from '../components/TimeframeComparison.jsx';

const HELP_ID = 'flows';

const TYPE_COLORS = {
    company: '#3B82F6',
    sovereign: '#F59E0B',
    central_bank: '#EF4444',
    policy: '#8B5CF6',
    regulator: '#EC4899',
    macro: '#06B6D4',
    indicator: '#22C55E',
    data: '#6366F1',
    sentiment: '#F97316',
    flow: '#14B8A6',
    structural: '#A855F7',
    infra: '#64748B',
    asset: '#F59E0B',
    physical: '#84CC16',
    subsector: '#6B7280',
    fx: '#06B6D4',
};

function stressColor(z) {
    if (z == null) return colors.textMuted;
    if (z > 1.5) return colors.red;
    if (z > 0.5) return '#F97316';
    if (z < -1.5) return colors.green;
    if (z < -0.5) return '#4ADE80';
    return colors.textMuted;
}

function stressLabel(z) {
    if (z == null) return '';
    if (z > 1.5) return 'HIGH STRESS';
    if (z > 0.5) return 'ELEVATED';
    if (z < -1.5) return 'VERY CALM';
    if (z < -0.5) return 'CALM';
    return 'NEUTRAL';
}

function perfColor(pct) {
    if (pct == null) return colors.textMuted;
    if (pct > 0.05) return colors.green;
    if (pct > 0) return '#4ADE80';
    if (pct < -0.05) return colors.red;
    if (pct < 0) return '#F97316';
    return colors.textMuted;
}

function formatPct(pct) {
    if (pct == null) return '--';
    return `${pct >= 0 ? '+' : ''}${(pct * 100).toFixed(1)}%`;
}

function InfluenceBar({ value, color }) {
    return (
        <div style={{ width: '100%', height: '4px', background: colors.bg, borderRadius: '2px', overflow: 'hidden' }}>
            <div style={{ width: `${Math.min(100, value * 100 * 3)}%`, height: '100%', background: color, borderRadius: '2px' }} />
        </div>
    );
}

function ActorCard({ actor, isExpanded, onToggle }) {
    const tc = TYPE_COLORS[actor.type] || colors.textMuted;
    const zc = stressColor(actor.avg_z);

    return (
        <div style={{ marginBottom: '4px' }}>
            <div
                onClick={onToggle}
                style={{
                    display: 'flex', alignItems: 'center', gap: '8px',
                    padding: '10px 12px', cursor: 'pointer',
                    background: isExpanded ? colors.bg : 'transparent',
                    borderRadius: isExpanded ? '6px 6px 0 0' : '6px',
                    border: isExpanded ? `1px solid ${colors.border}` : 'none',
                    borderBottom: isExpanded ? 'none' : `1px solid ${colors.borderSubtle}`,
                }}
            >
                {/* Type dot */}
                <div style={{
                    width: '8px', height: '8px', borderRadius: '50%',
                    background: tc, flexShrink: 0,
                }} />

                {/* Name & type */}
                <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                        <span style={{ fontSize: '12px', fontWeight: 600, color: colors.text }}>
                            {actor.name}
                        </span>
                        {actor.ticker && (
                            <span style={{
                                fontSize: '9px', padding: '1px 5px', borderRadius: '3px',
                                background: `${tc}20`, color: tc,
                                fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
                            }}>{actor.ticker}</span>
                        )}
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textMuted }}>
                        {actor.type} · influence {(actor.influence * 100).toFixed(0)}%
                    </div>
                </div>

                {/* Z-score */}
                {actor.avg_z != null && (
                    <div style={{ textAlign: 'right', flexShrink: 0 }}>
                        <div style={{
                            fontSize: '13px', fontWeight: 700, color: zc,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {actor.avg_z >= 0 ? '+' : ''}{actor.avg_z.toFixed(2)}
                        </div>
                        <div style={{ fontSize: '9px', color: zc }}>{stressLabel(actor.avg_z)}</div>
                    </div>
                )}

                <span style={{ fontSize: '12px', color: colors.textMuted }}>{isExpanded ? '▾' : '›'}</span>
            </div>

            {/* Expanded detail */}
            {isExpanded && (
                <div style={{
                    background: colors.bg, padding: '10px 12px',
                    borderRadius: '0 0 6px 6px',
                    border: `1px solid ${colors.border}`, borderTop: 'none',
                }}>
                    {/* Description */}
                    {actor.description && (
                        <div style={{ fontSize: '11px', color: colors.textDim, lineHeight: '1.5', marginBottom: '8px' }}>
                            {actor.description}
                        </div>
                    )}

                    {/* Influence bar */}
                    <div style={{ marginBottom: '8px' }}>
                        <div style={{ fontSize: '9px', color: colors.textMuted, marginBottom: '3px' }}>INFLUENCE</div>
                        <InfluenceBar value={actor.influence} color={tc} />
                    </div>

                    {/* Live features */}
                    {actor.live?.length > 0 && (
                        <div style={{ marginBottom: '8px' }}>
                            <div style={{ fontSize: '9px', color: colors.textMuted, marginBottom: '4px' }}>CONNECTED DATA</div>
                            {actor.live.map(f => (
                                <div key={f.feature} style={{
                                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                    padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
                                }}>
                                    <span style={{ fontSize: '11px', color: colors.text }}>{f.feature}</span>
                                    <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                                        <span style={{ fontSize: '11px', color: colors.textDim, fontFamily: "'JetBrains Mono', monospace" }}>
                                            {f.value != null ? (Math.abs(f.value) >= 100 ? f.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : f.value.toFixed(4)) : '--'}
                                        </span>
                                        <span style={{
                                            fontSize: '11px', fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                                            color: stressColor(f.z),
                                        }}>
                                            z={f.z >= 0 ? '+' : ''}{f.z.toFixed(2)}
                                        </span>
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}

                    {/* Options */}
                    {actor.options && (
                        <div>
                            <div style={{ fontSize: '9px', color: colors.textMuted, marginBottom: '4px' }}>OPTIONS</div>
                            <div style={{ display: 'flex', gap: '12px', fontSize: '11px' }}>
                                <span style={{ color: colors.textDim }}>
                                    P/C: <span style={{ color: actor.options.pcr > 1.2 ? colors.red : actor.options.pcr < 0.7 ? colors.green : colors.text, fontWeight: 600 }}>
                                        {actor.options.pcr?.toFixed(2)}
                                    </span>
                                </span>
                                <span style={{ color: colors.textDim }}>
                                    IV: <span style={{ color: colors.text, fontWeight: 600 }}>
                                        {actor.options.iv != null ? `${(actor.options.iv * 100).toFixed(1)}%` : '--'}
                                    </span>
                                </span>
                                <span style={{ color: colors.textDim }}>
                                    Pain: <span style={{ color: colors.text, fontWeight: 600 }}>
                                        ${actor.options.max_pain?.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                                    </span>
                                </span>
                            </div>
                        </div>
                    )}

                    {/* No data */}
                    {(!actor.live || actor.live.length === 0) && !actor.options && (
                        <div style={{ fontSize: '10px', color: colors.textMuted, fontStyle: 'italic' }}>
                            No live data connected — influence based on structural role
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

/* ── Sector Detail Panel ────────────────────────────────────── */

function DetailActorRow({ actor, etfTicker }) {
    const tc = TYPE_COLORS[actor.type] || colors.textMuted;
    const rc = perfColor(actor.rel_perf_vs_etf);
    const pc = perfColor(actor.pct_30d);

    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: '8px',
            padding: '8px 10px',
            borderBottom: `1px solid ${colors.borderSubtle}`,
        }}>
            {/* Rank indicator / type dot */}
            <div style={{
                width: '8px', height: '8px', borderRadius: '50%',
                background: tc, flexShrink: 0,
            }} />

            {/* Name + ticker */}
            <div style={{ flex: 1, minWidth: 0 }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '5px' }}>
                    <span style={{ fontSize: '11px', fontWeight: 600, color: colors.text }}>
                        {actor.name}
                    </span>
                    {actor.ticker && (
                        <span style={{
                            fontSize: '9px', padding: '1px 4px', borderRadius: '3px',
                            background: `${tc}20`, color: tc,
                            fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
                        }}>{actor.ticker}</span>
                    )}
                </div>
                {actor.description && (
                    <div title={actor.description} style={{ fontSize: '9px', color: colors.textMuted, marginTop: '1px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                        {actor.description}
                    </div>
                )}
            </div>

            {/* Price */}
            <div style={{ textAlign: 'right', flexShrink: 0, minWidth: '50px' }}>
                {actor.latest_price != null ? (
                    <div style={{ fontSize: '11px', fontWeight: 600, color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                        ${Math.abs(actor.latest_price) >= 1000
                            ? actor.latest_price.toLocaleString(undefined, { maximumFractionDigits: 0 })
                            : actor.latest_price.toFixed(2)}
                    </div>
                ) : (
                    <div style={{ fontSize: '11px', color: colors.textMuted }}>--</div>
                )}
            </div>

            {/* 30d % */}
            <div style={{ textAlign: 'right', flexShrink: 0, minWidth: '48px' }}>
                <div style={{ fontSize: '11px', fontWeight: 600, color: pc, fontFamily: "'JetBrains Mono', monospace" }}>
                    {formatPct(actor.pct_30d)}
                </div>
                <div style={{ fontSize: '8px', color: colors.textMuted }}>30d</div>
            </div>

            {/* Relative perf vs ETF */}
            <div style={{ textAlign: 'right', flexShrink: 0, minWidth: '52px' }}>
                <div style={{ fontSize: '11px', fontWeight: 700, color: rc, fontFamily: "'JetBrains Mono', monospace" }}>
                    {formatPct(actor.rel_perf_vs_etf)}
                </div>
                <div style={{ fontSize: '8px', color: colors.textMuted }}>vs {etfTicker}</div>
            </div>

            {/* Z-score */}
            <div style={{ textAlign: 'right', flexShrink: 0, minWidth: '36px' }}>
                {actor.avg_z != null ? (
                    <div style={{ fontSize: '11px', fontWeight: 600, color: stressColor(actor.avg_z), fontFamily: "'JetBrains Mono', monospace" }}>
                        {actor.avg_z >= 0 ? '+' : ''}{actor.avg_z.toFixed(1)}
                    </div>
                ) : (
                    <div style={{ fontSize: '11px', color: colors.textMuted }}>--</div>
                )}
            </div>

            {/* Options indicator */}
            <div style={{ flexShrink: 0, minWidth: '28px', textAlign: 'center' }}>
                {actor.options ? (
                    <span style={{
                        fontSize: '8px', padding: '1px 4px', borderRadius: '3px',
                        background: actor.options.pcr > 1.2 ? colors.redBg : actor.options.pcr < 0.7 ? colors.greenBg : colors.bg,
                        color: actor.options.pcr > 1.2 ? colors.red : actor.options.pcr < 0.7 ? colors.green : colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
                    }}>
                        {actor.options.pcr?.toFixed(1)}
                    </span>
                ) : null}
            </div>
        </div>
    );
}

function SectorDetailPanel({ detail, onClose }) {
    if (!detail) return null;

    const subsectorNames = Object.keys(detail.subsectors || {});

    return (
        <div style={{
            background: colors.card,
            border: `1px solid ${colors.border}`,
            borderRadius: '10px',
            marginBottom: '8px',
            overflow: 'hidden',
        }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '12px 16px',
                background: colors.cardElevated,
                borderBottom: `1px solid ${colors.border}`,
            }}>
                <div>
                    <div style={{ fontSize: '13px', fontWeight: 700, color: colors.text }}>
                        {detail.sector} Detail
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>
                        {detail.etf} 30d: <span style={{
                            fontWeight: 600, color: perfColor(detail.etf_change_30d),
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>{formatPct(detail.etf_change_30d)}</span>
                        {detail.etf_options && (
                            <span style={{ marginLeft: '8px' }}>
                                P/C: <span style={{
                                    fontWeight: 600,
                                    color: detail.etf_options.pcr > 1.2 ? colors.red : colors.text,
                                }}>{detail.etf_options.pcr?.toFixed(2)}</span>
                            </span>
                        )}
                    </div>
                </div>
                <button
                    onClick={onClose}
                    style={{
                        ...shared.buttonSmall,
                        fontSize: '10px',
                        padding: '4px 10px',
                    }}
                >
                    Close
                </button>
            </div>

            {/* Column headers */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: '8px',
                padding: '6px 10px',
                fontSize: '8px', fontWeight: 700, letterSpacing: '1px',
                color: colors.textMuted,
                fontFamily: "'JetBrains Mono', monospace",
                borderBottom: `1px solid ${colors.borderSubtle}`,
            }}>
                <div style={{ width: '8px' }} />
                <div style={{ flex: 1 }}>ACTOR</div>
                <div style={{ minWidth: '50px', textAlign: 'right' }}>PRICE</div>
                <div style={{ minWidth: '48px', textAlign: 'right' }}>30D</div>
                <div style={{ minWidth: '52px', textAlign: 'right' }}>REL</div>
                <div style={{ minWidth: '36px', textAlign: 'right' }}>Z</div>
                <div style={{ minWidth: '28px', textAlign: 'center' }}>P/C</div>
            </div>

            {/* Subsectors */}
            {subsectorNames.map(subName => {
                const sub = detail.subsectors[subName];
                return (
                    <div key={subName}>
                        <div style={{
                            fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                            color: colors.accent, padding: '8px 10px 4px',
                            fontFamily: "'JetBrains Mono', monospace",
                            borderTop: `1px solid ${colors.borderSubtle}`,
                        }}>
                            {subName.toUpperCase()}
                            <span style={{ color: colors.textMuted, fontWeight: 400, marginLeft: '8px' }}>
                                wt {(sub.weight * 100).toFixed(0)}%
                            </span>
                        </div>
                        {(sub.actors || []).map((actor, i) => (
                            <DetailActorRow
                                key={`${actor.name}-${i}`}
                                actor={actor}
                                etfTicker={detail.etf}
                            />
                        ))}
                    </div>
                );
            })}
        </div>
    );
}

/* ── Flow Summary ──────────────────────────────────────── */

function TimeframeComparisonPicker({ sectors }) {
    const [selectedFeature, setSelectedFeature] = useState(null);
    const sectorNames = Object.keys(sectors || {});

    // Build ETF list from sector data
    const etfs = sectorNames
        .map(name => ({ name, etf: sectors[name]?.etf, stress: sectors[name]?.sector_stress }))
        .filter(s => s.etf)
        .sort((a, b) => Math.abs(b.stress || 0) - Math.abs(a.stress || 0));

    if (!etfs.length) return null;

    // Default to the most extreme sector
    const active = selectedFeature || `${(etfs[0]?.etf || 'spy').toLowerCase()}_full`;

    return (
        <div style={{
            background: colors.card, borderRadius: '10px',
            border: `1px solid ${colors.border}`, overflow: 'hidden',
        }}>
            <div style={{
                padding: '10px 14px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', alignItems: 'center', gap: '6px', flexWrap: 'wrap',
            }}>
                <span style={{
                    fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                    color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    marginRight: '4px',
                }}>TIMEFRAME</span>
                {etfs.map(s => {
                    const feat = `${s.etf.toLowerCase()}_full`;
                    const isActive = active === feat;
                    const sc = (s.stress || 0) < -0.3 ? colors.green : (s.stress || 0) > 0.3 ? colors.red : colors.textMuted;
                    return (
                        <button key={s.etf} onClick={() => setSelectedFeature(feat)}
                            style={{
                                background: isActive ? `${sc}20` : 'transparent',
                                border: `1px solid ${isActive ? sc : colors.border}`,
                                borderRadius: '4px', padding: '3px 8px', fontSize: '10px',
                                color: isActive ? sc : colors.textMuted,
                                cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                fontWeight: isActive ? 700 : 400,
                            }}>
                            {s.etf}
                        </button>
                    );
                })}
                <button onClick={() => setSelectedFeature('spy_full')}
                    style={{
                        background: active === 'spy_full' ? `${colors.accent}20` : 'transparent',
                        border: `1px solid ${active === 'spy_full' ? colors.accent : colors.border}`,
                        borderRadius: '4px', padding: '3px 8px', fontSize: '10px',
                        color: active === 'spy_full' ? colors.accent : colors.textMuted,
                        cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                        fontWeight: active === 'spy_full' ? 700 : 400,
                    }}>
                    SPY
                </button>
            </div>
            <TimeframeComparison feature={active} compact={false} />
        </div>
    );
}

function FlowSummary({ sectors }) {
    const names = Object.keys(sectors);
    if (!names.length) return null;

    const inflows = [];
    const neutral = [];
    const outflows = [];

    names.forEach(name => {
        const s = sectors[name];
        const z = s.sector_stress;
        if (z == null) neutral.push(name);
        else if (z < -0.3) inflows.push({ name, z, etf: s.etf });
        else if (z > 0.3) outflows.push({ name, z, etf: s.etf });
        else neutral.push(name);
    });

    inflows.sort((a, b) => a.z - b.z); // most negative = strongest inflow
    outflows.sort((a, b) => b.z - a.z); // most positive = strongest outflow

    const topIn = inflows[0];
    const topOut = outflows[0];

    let narrative = '';
    if (topIn && topOut) {
        const inStr = `${topIn.name} is attracting capital (stress ${topIn.z.toFixed(2)})`;
        const outStr = `${topOut.name} faces outflow pressure (stress ${topOut.z >= 0 ? '+' : ''}${topOut.z.toFixed(2)})`;
        narrative = `${inStr}, while ${outStr}. `;
    }
    narrative += `${inflows.length} sector${inflows.length !== 1 ? 's' : ''} seeing inflows, ${outflows.length} under pressure, ${neutral.length} neutral.`;

    // Cross-sector flow arrows: if top inflow and top outflow are very different, suggest rotation
    let rotation = null;
    if (topIn && topOut && Math.abs(topOut.z - topIn.z) > 1.0) {
        rotation = { from: topOut.name, to: topIn.name, delta: (topOut.z - topIn.z).toFixed(2) };
    }

    return (
        <div style={{
            background: colors.card, borderRadius: '10px',
            border: `1px solid ${colors.border}`, padding: '14px 16px',
            marginBottom: '12px', borderLeft: `3px solid ${colors.accent}`,
        }}>
            <div style={{
                fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', marginBottom: '8px',
                color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
            }}>MARKET FLOW SUMMARY</div>
            <div style={{ fontSize: '12px', color: colors.textDim, lineHeight: '1.6' }}>
                {narrative}
            </div>
            {rotation && (
                <div style={{
                    marginTop: '10px', padding: '8px 12px', borderRadius: '6px',
                    background: colors.bg, border: `1px solid ${colors.borderSubtle}`,
                    fontSize: '11px', display: 'flex', alignItems: 'center', gap: '8px',
                }}>
                    <span style={{ color: colors.red, fontWeight: 600 }}>{rotation.from}</span>
                    <span style={{ color: colors.textMuted }}>→</span>
                    <span style={{ color: colors.green, fontWeight: 600 }}>{rotation.to}</span>
                    <span style={{ color: colors.textMuted, fontSize: '10px', marginLeft: 'auto', fontFamily: "'JetBrains Mono', monospace" }}>
                        spread {rotation.delta}σ
                    </span>
                </div>
            )}
        </div>
    );
}

/* ── Relative Performance Bar ─────────────────────────── */

function RelPerfBar({ actors, etfTicker }) {
    if (!actors || actors.length === 0) return null;
    const sorted = [...actors]
        .filter(a => a.rel_perf_vs_etf != null)
        .sort((a, b) => (b.rel_perf_vs_etf || 0) - (a.rel_perf_vs_etf || 0));
    if (!sorted.length) return null;

    const maxAbs = Math.max(...sorted.map(a => Math.abs(a.rel_perf_vs_etf || 0)), 0.01);

    return (
        <div style={{ marginTop: '8px' }}>
            <div style={{ fontSize: '9px', color: colors.textMuted, marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                RELATIVE PERFORMANCE vs {etfTicker}
            </div>
            {sorted.slice(0, 10).map((a, i) => {
                const pct = a.rel_perf_vs_etf || 0;
                const width = Math.min(100, (Math.abs(pct) / maxAbs) * 100);
                const isPos = pct >= 0;
                return (
                    <div key={`${a.name}-${i}`} style={{
                        display: 'flex', alignItems: 'center', gap: '6px',
                        padding: '3px 0', fontSize: '11px',
                    }}>
                        <span title={a.ticker || a.name} style={{ width: '70px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', color: colors.text, fontWeight: 500, fontFamily: "'JetBrains Mono', monospace", textTransform: a.ticker ? 'uppercase' : 'none' }}>
                            {a.ticker || a.name}
                        </span>
                        <div style={{ flex: 1, display: 'flex', justifyContent: isPos ? 'flex-start' : 'flex-end' }}>
                            <div style={{
                                height: '10px', borderRadius: '2px',
                                width: `${width}%`, minWidth: '2px',
                                background: isPos ? colors.green : colors.red,
                                opacity: 0.7,
                            }} />
                        </div>
                        <span style={{
                            width: '52px', textAlign: 'right', fontFamily: "'JetBrains Mono', monospace",
                            fontSize: '10px', fontWeight: 600,
                            color: isPos ? colors.green : colors.red,
                        }}>
                            {formatPct(pct)}
                        </span>
                    </div>
                );
            })}
        </div>
    );
}

/* ── Sector Insight Text ──────────────────────────────── */

function sectorInsight(name, sector) {
    const z = sector.sector_stress;
    if (z == null) return `${name} — no stress data available`;
    const dir = z < -0.3 ? 'attracting capital' : z > 0.3 ? 'under outflow pressure' : 'neutral flow';
    const mag = Math.abs(z) > 1.5 ? 'strongly' : Math.abs(z) > 0.5 ? 'moderately' : 'mildly';
    const actors = sector.actors?.length || 0;
    return `${name} is ${mag} ${dir} — ${actors} actors tracked`;
}

export default function Flows() {
    const { setLoading, addNotification } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLocalLoading] = useState(true);
    const [error, setError] = useState(null);
    const [expandedSector, setExpandedSector] = useState(null);
    const [expandedActor, setExpandedActor] = useState(null);
    const [detailSector, setDetailSector] = useState(null);
    const [detailData, setDetailData] = useState(null);
    const [detailLoading, setDetailLoading] = useState(false);

    useEffect(() => { load(); }, []);

    const load = async () => {
        setLocalLoading(true);
        setLoading('flows', true);
        try {
            const d = await api.getSectorFlows();
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load');
        }
        setLocalLoading(false);
        setLoading('flows', false);
    };

    const loadDetail = async (sectorName) => {
        if (detailSector === sectorName) {
            setDetailSector(null);
            setDetailData(null);
            return;
        }
        setDetailSector(sectorName);
        setDetailLoading(true);
        try {
            const d = await api.getSectorDetail(sectorName);
            setDetailData(d);
        } catch (err) {
            addNotification(`Detail load failed: ${err.message}`, 'error');
            setDetailSector(null);
            setDetailData(null);
        }
        setDetailLoading(false);
    };

    const sectors = data?.sectors || {};
    const sectorNames = Object.keys(sectors);

    // Group by flow direction: inflows (negative stress), neutral, outflows (positive stress)
    const inflows = [];
    const neutral = [];
    const outflows = [];
    sectorNames.forEach(name => {
        const z = sectors[name].sector_stress;
        if (z == null) neutral.push(name);
        else if (z < -0.3) inflows.push(name);
        else if (z > 0.3) outflows.push(name);
        else neutral.push(name);
    });
    inflows.sort((a, b) => (sectors[a].sector_stress || 0) - (sectors[b].sector_stress || 0));
    outflows.sort((a, b) => (sectors[b].sector_stress || 0) - (sectors[a].sector_stress || 0));
    const orderedSectors = [...inflows, ...neutral, ...outflows];

    const groupLabel = (name) => {
        if (inflows.includes(name) && inflows[0] === name) return 'INFLOWS';
        if (neutral.includes(name) && (inflows.length === 0 ? neutral[0] === name : neutral[0] === name)) return 'NEUTRAL';
        if (outflows.includes(name) && outflows[0] === name) return 'OUTFLOWS';
        return null;
    };

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.md }}>
                <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: tokens.fontSize.lg, color: colors.textMuted, letterSpacing: '2px' }}>
                    FLOWS
                </div>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                    <ViewHelp id="flows" />
                    <button onClick={load} style={shared.buttonSmall}>Refresh</button>
                </div>
            </div>

            {loading ? (
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '40px' }}>Loading sector data...</div>
            ) : error ? (
                <div style={shared.error}>{error}</div>
            ) : (
                <>
                    {/* Sankey flow map */}
                    <div style={{ marginBottom: '12px' }}>
                        <FlowSankey8 height={350} />
                    </div>

                    {/* Timeframe comparison for key ETFs */}
                    <div style={{ marginBottom: '12px' }}>
                        <TimeframeComparisonPicker sectors={sectors} />
                    </div>

                    {/* Narrative summary */}
                    <FlowSummary sectors={sectors} />

                    <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                        {orderedSectors.map(name => {
                            const sector = sectors[name];
                            const isOpen = expandedSector === name;
                            const sc = stressColor(sector.sector_stress);
                            const isDetailOpen = detailSector === name;
                            const label = groupLabel(name);
                            const flowDir = inflows.includes(name) ? colors.green : outflows.includes(name) ? colors.red : colors.textMuted;

                            return (
                                <React.Fragment key={name}>
                                    {/* Group divider */}
                                    {label && (
                                        <div style={{
                                            fontSize: '9px', fontWeight: 700, letterSpacing: '2px',
                                            color: label === 'INFLOWS' ? colors.green : label === 'OUTFLOWS' ? colors.red : colors.textMuted,
                                            padding: '8px 0 2px', marginTop: '4px',
                                            fontFamily: "'JetBrains Mono', monospace",
                                            borderTop: label !== 'INFLOWS' ? `1px solid ${colors.borderSubtle}` : 'none',
                                        }}>
                                            {label === 'INFLOWS' ? '▲ INFLOWS' : label === 'OUTFLOWS' ? '▼ OUTFLOWS' : '● NEUTRAL'}
                                        </div>
                                    )}

                                    <div>
                                        {/* Sector header — insight-led */}
                                        <div
                                            onClick={() => setExpandedSector(isOpen ? null : name)}
                                            style={{
                                                background: isOpen ? colors.cardElevated : colors.card,
                                                border: `1px solid ${colors.border}`,
                                                borderLeft: `3px solid ${flowDir}`,
                                                borderRadius: isOpen ? '10px 10px 0 0' : '10px',
                                                padding: '14px 16px', cursor: 'pointer',
                                            }}
                                        >
                                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                                                <div style={{ flex: 1 }}>
                                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '4px' }}>
                                                        <span style={{ fontSize: '14px', fontWeight: 700, color: colors.text }}>{name}</span>
                                                        <span style={{
                                                            fontSize: '10px', padding: '1px 6px', borderRadius: '3px',
                                                            background: colors.bg, color: colors.textMuted,
                                                            fontFamily: "'JetBrains Mono', monospace",
                                                        }}>{sector.etf}</span>
                                                        {sector.etf_options && (
                                                            <span style={{
                                                                fontSize: '9px', padding: '2px 6px', borderRadius: '3px',
                                                                background: sector.etf_options.pcr > 1.2 ? colors.redBg : colors.bg,
                                                                color: sector.etf_options.pcr > 1.2 ? colors.red : colors.textMuted,
                                                                fontFamily: "'JetBrains Mono', monospace",
                                                            }}>
                                                                P/C {sector.etf_options.pcr?.toFixed(1)}
                                                            </span>
                                                        )}
                                                    </div>
                                                    {/* Insight line instead of just subsector list */}
                                                    <div style={{ fontSize: '11px', color: colors.textDim, lineHeight: '1.4' }}>
                                                        {sectorInsight(name, sector)}
                                                    </div>
                                                </div>
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexShrink: 0 }}>
                                                    {sector.sector_stress != null && (
                                                        <div style={{ textAlign: 'right' }}>
                                                            <div style={{ fontSize: '14px', fontWeight: 700, color: sc, fontFamily: "'JetBrains Mono', monospace" }}>
                                                                {sector.sector_stress >= 0 ? '+' : ''}{sector.sector_stress.toFixed(2)}
                                                            </div>
                                                            <div style={{ fontSize: '9px', color: sc }}>{stressLabel(sector.sector_stress)}</div>
                                                        </div>
                                                    )}
                                                    <span style={{ fontSize: '14px', color: colors.textMuted }}>{isOpen ? '▾' : '›'}</span>
                                                </div>
                                            </div>
                                        </div>

                                        {/* Expanded: actor list */}
                                        {isOpen && (
                                            <div style={{
                                                background: colors.card,
                                                border: `1px solid ${colors.border}`, borderTop: 'none',
                                                borderRadius: '0 0 10px 10px',
                                                padding: '8px 12px',
                                            }}>
                                                {/* Detail button */}
                                                <div style={{ display: 'flex', justifyContent: 'flex-end', marginBottom: '6px' }}>
                                                    <button
                                                        onClick={(e) => { e.stopPropagation(); loadDetail(name); }}
                                                        style={{
                                                            ...shared.buttonSmall,
                                                            fontSize: '10px',
                                                            padding: '4px 12px',
                                                            background: isDetailOpen ? colors.accent : colors.bg,
                                                            color: isDetailOpen ? colors.bg : colors.textMuted,
                                                            border: `1px solid ${isDetailOpen ? colors.accent : colors.border}`,
                                                        }}
                                                    >
                                                        {detailLoading && isDetailOpen ? 'Loading...' : isDetailOpen ? 'Hide Detail' : 'Detail'}
                                                    </button>
                                                </div>

                                                {/* Detail panel with ranked bar chart */}
                                                {isDetailOpen && detailData && !detailLoading && (
                                                    <>
                                                        <RelPerfBar
                                                            actors={Object.values(detailData.subsectors || {}).flatMap(s => s.actors || [])}
                                                            etfTicker={detailData.etf}
                                                        />
                                                        <SectorDetailPanel
                                                            detail={detailData}
                                                            onClose={() => { setDetailSector(null); setDetailData(null); }}
                                                        />
                                                    </>
                                                )}
                                                {isDetailOpen && detailLoading && (
                                                    <div style={{ color: colors.textMuted, textAlign: 'center', padding: '16px', fontSize: '11px' }}>
                                                        Loading detail...
                                                    </div>
                                                )}

                                                {/* Subsector headers within the actor list */}
                                                {(() => {
                                                    let currentSub = '';
                                                    return (sector.actors || []).map((actor, i) => {
                                                        const showSubHeader = actor.subsector !== currentSub;
                                                        if (showSubHeader) currentSub = actor.subsector;
                                                        return (
                                                            <React.Fragment key={`${actor.name}-${i}`}>
                                                                {showSubHeader && (
                                                                    <div style={{
                                                                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                                                        color: colors.accent, padding: '8px 0 4px',
                                                                        fontFamily: "'JetBrains Mono', monospace",
                                                                        borderTop: i > 0 ? `1px solid ${colors.borderSubtle}` : 'none',
                                                                        marginTop: i > 0 ? '4px' : 0,
                                                                    }}>
                                                                        {actor.subsector?.toUpperCase()}
                                                                    </div>
                                                                )}
                                                                <ActorCard
                                                                    actor={actor}
                                                                    isExpanded={expandedActor === `${name}-${actor.name}`}
                                                                    onToggle={() => setExpandedActor(
                                                                        expandedActor === `${name}-${actor.name}` ? null : `${name}-${actor.name}`
                                                                    )}
                                                                />
                                                            </React.Fragment>
                                                        );
                                                    });
                                                })()}
                                            </div>
                                        )}
                                    </div>
                                </React.Fragment>
                            );
                        })}
                    </div>
                </>
            )}
            <div style={{ height: '80px' }} />
        </div>
    );
}
