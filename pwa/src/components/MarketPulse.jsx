/**
 * Dashboard Market Overview — insight-driven signal summary.
 *
 * Instead of a grid of numbers, shows per-family analysis with
 * net bias, top movers, and plain-English interpretation.
 */
import React, { useMemo, useState } from 'react';
import { colors, tokens } from '../styles/shared.js';

const INVERSE_SIGNALS = new Set([
    'vix', 'vvix', 'vxn', 'move_index', 'skew_index',
    'hy_spread', 'ted_spread', 'ofr_financial_stress',
    'dollar_index', 'dxy_etf',
    'unemployment', 'unrate', 'continued_claims', 'initial_claims',
    'cpi', 'core_pce', 'pce_yoy', 'cpi_yoy',
    'put_call_ratio',
]);

const FAMILY_ORDER = ['rates', 'credit', 'breadth', 'vol', 'commodity', 'fx', 'sentiment', 'macro', 'earnings'];
const FAMILY_LABELS = {
    rates: 'Rates', credit: 'Credit', breadth: 'Equities', vol: 'Volatility',
    commodity: 'Commodities', fx: 'FX', sentiment: 'Sentiment', macro: 'Macro',
    earnings: 'Earnings', crypto: 'Crypto', alternative: 'Alt Data',
};

function biasColor(avg) {
    if (avg > 0.8) return colors.green;
    if (avg > 0.3) return '#4ADE80';
    if (avg > -0.3) return colors.textMuted;
    if (avg > -0.8) return '#F97316';
    return colors.red;
}

function biasLabel(avg) {
    if (avg > 0.8) return 'STRONG';
    if (avg > 0.3) return 'BULLISH';
    if (avg > -0.3) return 'MIXED';
    if (avg > -0.8) return 'BEARISH';
    return 'STRESSED';
}

function shortName(name) {
    return (name || '')
        .replace(/^(fred_|yf_|crucix_)/i, '')
        .replace(/_full$/, '')
        .replace(/_/g, ' ')
        .toUpperCase()
        .substring(0, 14);
}

export default function DashboardHeatmap({ signals }) {
    const [expandedFamily, setExpandedFamily] = useState(null);
    const features = signals?.features || (Array.isArray(signals) ? signals : []);

    const { grouped, familyStats, orderedFamilies } = useMemo(() => {
        const groups = {};
        for (const f of features) {
            const z = f.z_score ?? f.zscore ?? null;
            if (z == null) continue;
            const family = (f.family || 'other').toLowerCase();
            if (!groups[family]) groups[family] = [];
            const inverse = INVERSE_SIGNALS.has((f.feature_name || f.name || '').toLowerCase());
            groups[family].push({
                name: f.feature_name || f.name || '?',
                z, value: f.value ?? f.latest_value,
                inverse, effectiveZ: inverse ? -z : z,
            });
        }

        const stats = {};
        for (const [fam, tiles] of Object.entries(groups)) {
            tiles.sort((a, b) => Math.abs(b.z) - Math.abs(a.z));
            const effs = tiles.map(t => t.effectiveZ);
            const avg = effs.reduce((a, b) => a + b, 0) / effs.length;
            const extreme = effs.filter(z => Math.abs(z) > 2).length;
            const topBull = tiles.filter(t => t.effectiveZ > 0.5).sort((a, b) => b.effectiveZ - a.effectiveZ).slice(0, 2);
            const topBear = tiles.filter(t => t.effectiveZ < -0.5).sort((a, b) => a.effectiveZ - b.effectiveZ).slice(0, 2);
            stats[fam] = { avg, extreme, count: tiles.length, topBull, topBear };
        }

        const ordered = [];
        for (const fam of FAMILY_ORDER) { if (groups[fam]?.length) ordered.push(fam); }
        for (const fam of Object.keys(groups)) { if (!ordered.includes(fam) && groups[fam]?.length) ordered.push(fam); }

        return { grouped: groups, familyStats: stats, orderedFamilies: ordered };
    }, [features]);

    // Overall summary
    const overall = useMemo(() => {
        const allStats = Object.values(familyStats);
        if (!allStats.length) return null;
        const avgs = allStats.map(s => s.avg);
        const net = avgs.reduce((a, b) => a + b, 0) / avgs.length;
        const totalExtreme = allStats.reduce((s, x) => s + x.extreme, 0);
        const totalFeatures = allStats.reduce((s, x) => s + x.count, 0);

        const stressed = Object.entries(familyStats)
            .filter(([, s]) => s.avg < -0.5)
            .sort((a, b) => a[1].avg - b[1].avg)
            .map(([f]) => FAMILY_LABELS[f] || f);
        const strong = Object.entries(familyStats)
            .filter(([, s]) => s.avg > 0.5)
            .sort((a, b) => b[1].avg - a[1].avg)
            .map(([f]) => FAMILY_LABELS[f] || f);

        let narrative = '';
        if (strong.length && stressed.length) {
            narrative = `${strong.join(', ')} showing strength while ${stressed.join(', ')} under pressure. `;
        } else if (stressed.length) {
            narrative = `${stressed.join(' and ')} under pressure. `;
        } else if (strong.length) {
            narrative = `${strong.join(' and ')} showing strength. `;
        }
        if (totalExtreme > 0) {
            narrative += `${totalExtreme} extreme reading${totalExtreme > 1 ? 's' : ''} across ${totalFeatures} signals.`;
        } else {
            narrative += `${totalFeatures} signals tracked, no extreme readings.`;
        }

        return { net, label: biasLabel(net), color: biasColor(net), narrative };
    }, [familyStats]);

    if (!overall) {
        return (
            <div style={{ background: colors.card, borderRadius: tokens.radius.md, padding: '16px', border: `1px solid ${colors.border}`, color: colors.textMuted, fontSize: '12px', textAlign: 'center' }}>
                No signal data available
            </div>
        );
    }

    return (
        <div style={{ background: colors.card, borderRadius: tokens.radius.md, padding: '12px', border: `1px solid ${colors.border}` }}>
            {/* Overall summary */}
            <div style={{
                background: `${overall.color}11`, border: `1px solid ${overall.color}33`,
                borderRadius: '6px', padding: '10px 12px', marginBottom: '10px',
            }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                    <span style={{ fontSize: '13px', fontWeight: 700, color: overall.color, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px' }}>
                        {overall.label}
                    </span>
                    <span style={{ fontSize: '12px', color: overall.color, fontFamily: "'JetBrains Mono', monospace", fontWeight: 600 }}>
                        {overall.net >= 0 ? '+' : ''}{overall.net.toFixed(2)}
                    </span>
                </div>
                <div style={{ fontSize: '12px', color: colors.textDim, lineHeight: '1.5' }}>
                    {overall.narrative}
                </div>
            </div>

            {/* Per-family strips */}
            <div style={{ display: 'flex', flexDirection: 'column', gap: '2px' }}>
                {orderedFamilies.map(fam => {
                    const s = familyStats[fam];
                    const label = FAMILY_LABELS[fam] || fam;
                    const bc = biasColor(s.avg);
                    const isExpanded = expandedFamily === fam;
                    const tiles = grouped[fam] || [];

                    // Bias bar: percentage position from -3 to +3
                    const barPos = Math.max(0, Math.min(100, ((s.avg + 3) / 6) * 100));

                    return (
                        <div key={fam}>
                            <div
                                onClick={() => setExpandedFamily(isExpanded ? null : fam)}
                                style={{
                                    display: 'flex', alignItems: 'center', gap: '8px',
                                    padding: '8px 6px', cursor: 'pointer',
                                    borderRadius: isExpanded ? '4px 4px 0 0' : '4px',
                                    background: isExpanded ? colors.bg : 'transparent',
                                }}
                            >
                                <span style={{
                                    fontSize: '11px', fontWeight: 600, color: colors.textDim,
                                    width: '72px', flexShrink: 0,
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>{label}</span>

                                {/* Bias bar */}
                                <div style={{
                                    flex: 1, height: '8px', borderRadius: '4px',
                                    background: colors.bg, position: 'relative', overflow: 'hidden',
                                }}>
                                    <div style={{
                                        position: 'absolute', left: '50%', top: 0, bottom: 0,
                                        width: '1px', background: colors.border,
                                    }} />
                                    <div style={{
                                        position: 'absolute',
                                        left: s.avg >= 0 ? '50%' : undefined,
                                        right: s.avg < 0 ? '50%' : undefined,
                                        top: '1px', bottom: '1px',
                                        width: `${Math.min(50, Math.abs(s.avg) / 3 * 50)}%`,
                                        background: bc, borderRadius: '3px',
                                    }} />
                                </div>

                                <span style={{
                                    fontSize: '10px', fontWeight: 700, color: bc,
                                    width: '44px', textAlign: 'right', flexShrink: 0,
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>
                                    {s.avg >= 0 ? '+' : ''}{s.avg.toFixed(1)}
                                </span>

                                <span style={{
                                    fontSize: '10px', color: colors.textMuted,
                                    width: '14px', textAlign: 'center',
                                }}>{isExpanded ? '▾' : '›'}</span>
                            </div>

                            {/* Expanded detail */}
                            {isExpanded && (
                                <div style={{
                                    background: colors.bg, borderRadius: '0 0 4px 4px',
                                    padding: '8px 10px', marginBottom: '4px',
                                    border: `1px solid ${colors.borderSubtle}`, borderTop: 'none',
                                }}>
                                    {/* Top movers */}
                                    {(s.topBull.length > 0 || s.topBear.length > 0) && (
                                        <div style={{ marginBottom: '8px' }}>
                                            {s.topBull.length > 0 && (
                                                <div style={{ fontSize: '11px', color: colors.textDim, marginBottom: '2px' }}>
                                                    <span style={{ color: colors.green }}>Strong: </span>
                                                    {s.topBull.map(t => `${shortName(t.name)} (${t.z >= 0 ? '+' : ''}${t.z.toFixed(1)})`).join(', ')}
                                                </div>
                                            )}
                                            {s.topBear.length > 0 && (
                                                <div style={{ fontSize: '11px', color: colors.textDim, marginBottom: '2px' }}>
                                                    <span style={{ color: colors.red }}>Weak: </span>
                                                    {s.topBear.map(t => `${shortName(t.name)} (${t.z >= 0 ? '+' : ''}${t.z.toFixed(1)})`).join(', ')}
                                                </div>
                                            )}
                                        </div>
                                    )}

                                    {/* All features in this family */}
                                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '3px' }}>
                                        {tiles.slice(0, 12).map(tile => {
                                            const c = biasColor(tile.effectiveZ);
                                            return (
                                                <div key={tile.name} title={`${tile.name}: z=${tile.z.toFixed(3)}, val=${tile.value}`}
                                                    style={{
                                                        fontSize: '9px', padding: '3px 6px',
                                                        borderRadius: '3px', background: `${c}18`,
                                                        color: Math.abs(tile.z) > 1 ? c : colors.textMuted,
                                                        fontFamily: "'JetBrains Mono', monospace",
                                                        fontWeight: Math.abs(tile.z) > 1.5 ? 700 : 400,
                                                    }}>
                                                    {shortName(tile.name)} {tile.z >= 0 ? '+' : ''}{tile.z.toFixed(1)}
                                                </div>
                                            );
                                        })}
                                        {tiles.length > 12 && (
                                            <span style={{ fontSize: '9px', color: colors.textMuted, padding: '3px 6px' }}>
                                                +{tiles.length - 12} more
                                            </span>
                                        )}
                                    </div>
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
