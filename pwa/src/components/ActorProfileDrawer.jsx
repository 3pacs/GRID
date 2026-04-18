/**
 * ActorProfileDrawer — slide-in profile for SectorDive network nodes.
 *
 * Opens on double-click / shift-click in the OWNERSHIP & POWER NETWORK.
 * Loads /api/v1/actors/:id/detail and renders snapshot, description,
 * connections, signals, top holders.
 *
 * Tabs: Overview | Explain | Supply | Capital
 *   - Overview: fetches actor detail
 *   - Explain:  "why did this move?" — ranked evidence across every
 *               intelligence lens for a date + ±window slider
 *   - Supply:   lazy-fetches supply chain (depth 2), compact column stack
 *   - Capital:  lazy-fetches capital flow (4 periods), KPI grid + latest mini sankey
 *   - Each tab has "Open in Canvas" deep link → #/canvas/{id}/{lens}
 *
 * Props:
 *   actor   {id, sector?}  null = closed
 *   onClose ()
 *   onNavigate (nextId)    swap drawer target
 */
import React, { useEffect, useState, useCallback, useMemo } from 'react';
import { ExternalLink, Newspaper, Search, Building2, Users, Landmark, BookOpen, ChevronDown, ChevronRight } from 'lucide-react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const fmtPct = (v) => v == null ? '--' :
    `${(Number(v) * 100) >= 0 ? '+' : ''}${(Number(v) * 100).toFixed(2)}%`;
const fmtUSD = (v) => {
    if (v == null) return '--';
    const abs = Math.abs(v);
    if (abs >= 1e12) return `$${(v / 1e12).toFixed(2)}T`;
    if (abs >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
    return `$${Number(v).toFixed(2)}`;
};
const fmtUsdShort = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n) || n === 0) return '—';
    const a = Math.abs(n);
    if (a >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
    if (a >= 1e9)  return `$${(n / 1e9).toFixed(1)}B`;
    if (a >= 1e6)  return `$${(n / 1e6).toFixed(1)}M`;
    if (a >= 1e3)  return `$${(n / 1e3).toFixed(1)}K`;
    return `$${n.toFixed(0)}`;
};
const fmtPctPlain = (v) => {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return `${(n * 100).toFixed(1)}%`;
};
const fmtInt = (v) => v == null ? '--' : Number(v).toLocaleString();

const TYPE_COLOR = {
    company: colors.accent, person: colors.yellow, family_office: colors.yellow,
    regulator: colors.red, commodity: '#8B4513', macro: '#06B6D4',
    event: colors.green, trade_org: '#A855F7', concept: colors.textDim, unknown: colors.textMuted,
};
const SIGNAL_COLOR = (val) => {
    if (!val) return colors.textMuted;
    const s = String(val).toLowerCase();
    if (s.includes('bull') || s.includes('accumul')) return colors.green;
    if (s.includes('bear') || s.includes('distrib')) return colors.red;
    if (s.includes('balanc') || s.includes('neutral')) return colors.textDim;
    return colors.yellow;
};

// Supply chain palette
const SUPPLY_TYPE_FILL = {
    commodity: '#8B5A2B',
    private:   '#445',
    ticker:    colors.accent,
    country:   '#4A5568',
    actor:     colors.accent,
};

// Capital flow palette
const INFLOW_COLOR = '#10B981';
const OUTFLOW_COLORS = {
    cogs: '#EF4444', opex: '#F97316', capex: '#F59E0B',
    r_and_d: '#A3A3A3', rnd: '#A3A3A3', research: '#A3A3A3',
    dividends: '#FBBF24', buybacks: '#FACC15', taxes: '#94A3B8',
    interest: '#64748B', other: '#71717A',
};
const pickOutflow = (type) => OUTFLOW_COLORS[(type || 'other').toLowerCase()] || OUTFLOW_COLORS.other;

// ── Sub-components ────────────────────────────────────────────────
const Section = ({ title, children }) => (
    <div style={{ marginBottom: tokens.space.md }}>
        <div style={{ ...shared.sectionTitle, marginBottom: tokens.space.xs }}>{title}</div>
        {children}
    </div>
);

// SWEEP: Oracle confidence-stack panel — shows the 7-multiplier breakdown
// for a ticker's live ensemble prediction. Each multiplier gets a horizontal
// bar showing how much it shrunk or amplified the confidence scalar.
const ConfidenceStackPanel = ({ ticker }) => {
    const [data, setData] = React.useState(null);
    const [loading, setLoading] = React.useState(false);
    const [error, setError] = React.useState(null);

    React.useEffect(() => {
        if (!ticker) return;
        let cancelled = false;
        setLoading(true);
        setError(null);
        api.getOraclePredictLive(ticker, 7)
            .then((res) => { if (!cancelled) setData(res); })
            .catch((err) => { if (!cancelled) setError(String(err)); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [ticker]);

    if (loading) return null;
    if (error || !data || data.model_count === 0) return null;

    // Direction color
    const dirColor = data.direction === "bullish" ? "#10B981"
        : data.direction === "bearish" ? "#EF4444"
        : colors.textMuted;

    // Per-multiplier rows to render
    const multipliers = [
        {
            label: "catalyst",
            value: data.catalyst_proximity,
            factor: 1 - 0.5 * (data.catalyst_proximity || 0),
            note: data.catalyst_type || "none",
            maxDampen: 0.50,
        },
        {
            label: "disagreement",
            value: data.disagreement_score,
            factor: 1 - 0.4 * (data.disagreement_score || 0),
            note: `entropy ${(data.directional_entropy || 0).toFixed(2)}`,
            maxDampen: 0.40,
        },
        {
            label: "liquidity",
            value: data.liquidity_level_percentile / 100,
            factor: data.liquidity_state === "EXPANSION_STRONG" ? 1.20
                : data.liquidity_state === "EXPANSION" ? 1.10
                : data.liquidity_state === "TIGHTENING" ? 0.85
                : data.liquidity_state === "CRISIS" ? 0.60
                : 1.00,
            note: data.liquidity_state || "UNKNOWN",
            maxAmplify: 0.20,
        },
        {
            label: "FCI",
            value: data.fci_score,
            factor: 1 + 0.05 * Math.max(-3, Math.min(3, data.fci_score || 0)),
            note: data.fci_regime || "NEUTRAL",
            maxAmplify: 0.15,
        },
        {
            label: "fragility",
            value: 1 - (data.fragility_multiplier || 1),
            factor: data.fragility_multiplier || 1,
            note: data.shapley_top_contributor
                ? `top ${((data.shapley_top_share || 0) * 100).toFixed(0)}%`
                : "balanced",
            maxDampen: 0.50,
        },
        {
            label: "crowded",
            value: data.crowdedness_score,
            factor: data.crowd_aligned ? 0.80 : 1.00,
            note: data.crowd_aligned ? `with ${data.crowd_direction || "crowd"}` : "contrarian",
            maxDampen: 0.20,
        },
        {
            label: "mkt-implied",
            value: data.market_implied_prob,
            factor: data.market_divergence_severity === "extreme" ? 0.85
                : data.market_divergence_severity === "moderate" ? 1.10
                : data.market_divergence_severity === "mild" ? 1.05
                : 1.00,
            note: data.market_divergence_severity || "aligned",
            maxAmplify: 0.10,
        },
    ];

    return (
        <Section title={`CONFIDENCE STACK (7d, ${ticker.toUpperCase()})`}>
            <div style={{
                background: colors.bg, border: `1px solid ${colors.borderSubtle}`,
                borderRadius: tokens.radius.sm, padding: '10px 12px',
            }}>
                {/* Header: direction + final confidence */}
                <div style={{
                    display: 'flex', justifyContent: 'space-between',
                    alignItems: 'center', marginBottom: '10px',
                }}>
                    <div>
                        <div style={{
                            fontSize: '11px', fontWeight: 700, fontFamily: mono,
                            letterSpacing: '1px', textTransform: 'uppercase', color: dirColor,
                        }}>
                            {data.direction} · score {data.score}
                        </div>
                        <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '2px' }}>
                            regime: {data.regime || "NEUTRAL"} · horizon {data.horizon}d
                        </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                        <div style={{ fontSize: '14px', fontFamily: mono, color: dirColor, fontWeight: 700 }}>
                            {(data.confidence * 100).toFixed(0)}%
                        </div>
                        <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: mono }}>
                            CI [{(data.confidence_lower * 100).toFixed(0)}–{(data.confidence_upper * 100).toFixed(0)}]
                        </div>
                    </div>
                </div>

                {/* Per-multiplier bars */}
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                    {multipliers.map((m) => {
                        const factor = m.factor ?? 1;
                        const isAmp = factor > 1;
                        const delta = Math.abs(factor - 1);
                        // Bar width scaled to max dampen/amplify
                        const cap = isAmp ? (m.maxAmplify || 0.20) : (m.maxDampen || 0.50);
                        const widthPct = Math.min(100, (delta / cap) * 50);
                        const barColor = isAmp ? "#10B981" : (factor < 1 ? "#F59E0B" : colors.borderSubtle);
                        return (
                            <div key={m.label} style={{
                                display: 'grid', gridTemplateColumns: '70px 1fr 50px',
                                alignItems: 'center', gap: '6px',
                                fontFamily: mono, fontSize: '9px',
                            }}>
                                <div style={{
                                    color: colors.textMuted, textTransform: 'uppercase',
                                    letterSpacing: '0.5px',
                                }}>
                                    {m.label}
                                </div>
                                <div style={{
                                    height: '6px', background: 'rgba(255,255,255,0.04)',
                                    borderRadius: '3px', position: 'relative',
                                }}>
                                    {/* Center line (×1.0) */}
                                    <div style={{
                                        position: 'absolute', left: '50%', top: 0, bottom: 0,
                                        width: '1px', background: colors.borderSubtle,
                                    }} />
                                    {/* Bar — grows right for amplify, left for dampen */}
                                    <div style={{
                                        position: 'absolute', top: 0, bottom: 0,
                                        ...(isAmp
                                            ? { left: '50%', width: `${widthPct}%` }
                                            : { right: '50%', width: `${widthPct}%` }),
                                        background: barColor,
                                        borderRadius: '2px',
                                    }} />
                                </div>
                                <div style={{
                                    color: isAmp ? "#10B981" : (factor < 1 ? "#F59E0B" : colors.textMuted),
                                    textAlign: 'right', fontWeight: 700,
                                }}>
                                    ×{factor.toFixed(2)}
                                </div>
                            </div>
                        );
                    })}
                </div>

                {/* Notes row */}
                <div style={{
                    marginTop: '8px', paddingTop: '8px',
                    borderTop: `1px solid ${colors.borderSubtle}`,
                    fontSize: '9px', color: colors.textMuted, fontFamily: mono,
                    lineHeight: 1.5,
                }}>
                    {multipliers.filter(m => (m.factor ?? 1) !== 1.00).map((m) => (
                        <div key={m.label}>
                            <span style={{ color: colors.textDim }}>{m.label}:</span> {m.note}
                        </div>
                    ))}
                </div>
            </div>
        </Section>
    );
};

// INTEL-2: trust-or-cog classifier badge + per-component breakdown.
// Score is in [-1, +1]; classification is 'trust' / 'cog' / 'mixed' / 'unknown'.
const TrustCogBadge = ({ actorId }) => {
    const [data, setData] = React.useState(null);
    const [loading, setLoading] = React.useState(false);

    React.useEffect(() => {
        if (!actorId) return;
        let cancelled = false;
        setLoading(true);
        api.getActorTrustCog(actorId)
            .then((res) => { if (!cancelled) setData(res); })
            .catch(() => { if (!cancelled) setData(null); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [actorId]);

    if (loading) return null;
    if (!data || data.found === false || data.error) return null;

    const cls = (data.classification || 'unknown').toLowerCase();
    const score = typeof data.score === 'number' ? data.score : 0;
    const palette = {
        trust: { fg: '#10B981', bg: 'rgba(16,185,129,0.12)', border: '#10B981' },
        cog: { fg: '#EF4444', bg: 'rgba(239,68,68,0.12)', border: '#EF4444' },
        mixed: { fg: '#F59E0B', bg: 'rgba(245,158,11,0.12)', border: '#F59E0B' },
        unknown: { fg: colors.textMuted, bg: 'rgba(148,163,184,0.10)', border: colors.borderSubtle },
    };
    const p = palette[cls] || palette.unknown;
    const pct = Math.round(((score + 1) / 2) * 100); // -1..+1 → 0..100

    const components = data.components || {};
    const inputs = data.inputs || {};

    return (
        <Section title="TRUST / COG">
            <div style={{
                background: p.bg, border: `1px solid ${p.border}`,
                borderRadius: tokens.radius.sm, padding: '10px 12px',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: '6px' }}>
                    <div style={{
                        fontSize: '11px', fontWeight: 700, fontFamily: mono,
                        letterSpacing: '1px', textTransform: 'uppercase', color: p.fg,
                    }}>
                        {cls}
                    </div>
                    <div style={{ fontSize: '13px', fontFamily: mono, color: p.fg, fontWeight: 700 }}>
                        {score >= 0 ? '+' : ''}{score.toFixed(2)}
                    </div>
                </div>
                <div style={{ height: '4px', background: 'rgba(255,255,255,0.06)', borderRadius: '2px', overflow: 'hidden', marginBottom: '8px' }}>
                    <div style={{ width: `${pct}%`, height: '100%', background: p.fg }} />
                </div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '6px', fontFamily: mono, fontSize: '9px' }}>
                    <div>
                        <div style={{ color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.5px' }}>precision</div>
                        <div style={{ color: colors.text, fontWeight: 700, marginTop: '2px' }}>
                            {(components.precision ?? 0).toFixed(2)}
                        </div>
                    </div>
                    <div>
                        <div style={{ color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.5px' }}>centrality</div>
                        <div style={{ color: colors.text, fontWeight: 700, marginTop: '2px' }}>
                            {(components.centrality ?? 0).toFixed(2)}
                        </div>
                    </div>
                    <div>
                        <div style={{ color: colors.textMuted, textTransform: 'uppercase', letterSpacing: '0.5px' }}>credibility</div>
                        <div style={{ color: colors.text, fontWeight: 700, marginTop: '2px' }}>
                            {(components.credibility ?? 0).toFixed(2)}
                        </div>
                    </div>
                </div>
                {inputs.precision?.total != null && (
                    <div style={{ marginTop: '8px', fontSize: '9px', color: colors.textMuted, fontFamily: mono }}>
                        {inputs.precision.correct}/{inputs.precision.total} signals correct, {inputs.precision.lead_days?.toFixed?.(1) ?? '0.0'}d lead
                    </div>
                )}
            </div>
        </Section>
    );
};

const SigTile = ({ label, value, color }) => (
    <div style={{
        background: colors.bg, border: `1px solid ${colors.borderSubtle}`,
        borderRadius: tokens.radius.sm, padding: '8px 10px',
    }}>
        <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: mono, letterSpacing: '1px', textTransform: 'uppercase' }}>
            {label}
        </div>
        <div style={{ fontSize: '12px', fontWeight: 700, color: color || colors.text, fontFamily: mono, marginTop: '2px' }}>
            {value || 'n/a'}
        </div>
    </div>
);

const ConnRow = ({ c, onNavigate }) => (
    <div onClick={() => onNavigate?.(c.target)} style={{
        display: 'flex', flexDirection: 'column', gap: '2px',
        padding: '6px 8px', marginBottom: '4px',
        background: colors.bg, border: `1px solid ${colors.borderSubtle}`,
        borderLeft: `2px solid ${colors.accent}`,
        borderRadius: tokens.radius.sm, cursor: 'pointer',
    }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
            <span style={{ fontSize: '11px', fontWeight: 600, color: colors.text, fontFamily: mono }}>
                {c.target_label || c.target}
            </span>
            <div style={{ width: '48px', height: '3px', borderRadius: '2px', background: colors.border, overflow: 'hidden' }}>
                <div style={{ width: `${Math.min(100, (c.strength || 0) * 100)}%`, height: '100%', background: colors.accent }} />
            </div>
        </div>
        {c.evidence && (
            <div style={{ fontSize: '10px', color: colors.textDim, lineHeight: 1.4 }}>{c.evidence}</div>
        )}
    </div>
);

// Quintile colors for a 0-100 percentile score. Bottom=red, middle=gray,
// top=green — matches the CapitalLens canvas view so users get the same
// semantic cue in both places.
function percentileColors(p) {
    if (p == null || !Number.isFinite(Number(p))) {
        return { bg: 'rgba(148,163,184,0.18)', fg: '#94A3B8' };
    }
    const n = Number(p);
    if (n <= 20) return { bg: 'rgba(239,68,68,0.18)',  fg: '#F87171' };
    if (n <= 40) return { bg: 'rgba(249,115,22,0.18)', fg: '#FB923C' };
    if (n <= 60) return { bg: 'rgba(148,163,184,0.18)', fg: '#CBD5E1' };
    if (n <= 80) return { bg: 'rgba(132,204,22,0.18)', fg: '#A3E635' };
    return { bg: 'rgba(16,185,129,0.22)', fg: '#34D399' };
}

// Shared small building blocks for lens tabs
const MiniKpi = ({ label, value, color, badge }) => (
    <div style={{
        background: colors.bg,
        border: `1px solid ${colors.borderSubtle}`,
        borderRadius: tokens.radius.sm,
        padding: '8px 10px',
        display: 'flex', flexDirection: 'column', gap: '3px', minWidth: 0,
    }}>
        <div style={{
            fontSize: '9px', color: colors.textMuted, fontFamily: mono,
            letterSpacing: '1px', textTransform: 'uppercase',
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
        }}>{label}</div>
        <div style={{
            fontSize: '13px', fontWeight: 700, fontFamily: mono,
            color: color || colors.text,
            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
        }}>{value}</div>
        {badge != null && (
            <div style={{
                display: 'inline-flex', alignItems: 'center',
                alignSelf: 'flex-start',
                padding: '1px 5px', borderRadius: '3px',
                fontSize: '8px', fontWeight: 700, fontFamily: mono,
                letterSpacing: '0.3px',
                background: badge.bg, color: badge.fg,
                maxWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis',
                whiteSpace: 'nowrap',
            }}>{badge.text}</div>
        )}
    </div>
);

const OpenInCanvasButton = ({ href, label }) => (
    <button
        onClick={() => { window.location.hash = href; }}
        style={{
            width: '100%',
            background: `${colors.accent}15`,
            color: colors.accentLight,
            border: `1px solid ${colors.accent}66`,
            borderRadius: tokens.radius.sm,
            padding: '9px 10px',
            fontSize: '11px', fontWeight: 700,
            fontFamily: mono, letterSpacing: '1px',
            textTransform: 'uppercase', cursor: 'pointer',
            marginTop: tokens.space.md,
        }}
    >
        {label} →
    </button>
);

// ── Community Intel ─────────────────────────────────────────────────
// Cooperative "tentacle" system: any logged-in user can submit facts,
// connections, loyalties, or rumors about an actor. Other users upvote.
// Admins verify. Verified intel is shown with a green badge.

const INTEL_TYPE_COLOR = {
    biography: '#06B6D4',
    connection: colors.accent,
    loyalty: '#A855F7',
    stance: '#F59E0B',
    rumor: colors.yellow,
    tip: colors.green,
    fact: '#10B981',
};

const CommunityIntelSection = ({ actorId }) => {
    const [items, setItems] = React.useState(null);
    const [loading, setLoading] = React.useState(false);
    const [formOpen, setFormOpen] = React.useState(false);
    const [form, setForm] = React.useState({
        intel_type: 'fact', note: '', source_url: '', confidence: 'medium',
    });
    const [submitting, setSubmitting] = React.useState(false);
    const [toast, setToast] = React.useState(null);

    const load = React.useCallback(async () => {
        if (!actorId) return;
        setLoading(true);
        const res = await api.getActorIntel(actorId, 50);
        setLoading(false);
        if (!res || res.error) {
            setItems([]);
            return;
        }
        setItems(Array.isArray(res) ? res : (res.data || []));
    }, [actorId]);

    React.useEffect(() => { load(); }, [load]);

    const onVote = async (id, vote) => {
        const res = await api.voteIntel(id, vote);
        if (res && !res.error) {
            setItems(prev => (prev || []).map(it =>
                it.id === id ? {
                    ...it,
                    upvotes: res.upvotes,
                    downvotes: res.downvotes,
                    score: res.score,
                } : it
            ));
        }
    };

    const onFlag = async (id) => {
        const res = await api.flagIntel(id);
        if (res && !res.error) {
            setToast({ type: 'success', msg: 'Flagged for review' });
            setTimeout(() => setToast(null), 2500);
        }
    };

    const onSubmit = async () => {
        if (!form.note.trim()) {
            setToast({ type: 'error', msg: 'Note is required' });
            setTimeout(() => setToast(null), 2500);
            return;
        }
        setSubmitting(true);
        const res = await api.submitIntel(actorId, form);
        setSubmitting(false);
        if (res && !res.error) {
            setToast({ type: 'success', msg: 'Intel submitted. Thanks for contributing.' });
            setForm({ intel_type: 'fact', note: '', source_url: '', confidence: 'medium' });
            setFormOpen(false);
            await load();
        } else {
            setToast({
                type: 'error',
                msg: res?.message || 'Submission failed',
            });
        }
        setTimeout(() => setToast(null), 3000);
    };

    const sorted = React.useMemo(() => {
        const list = items || [];
        return [...list].sort((a, b) => (b.score ?? 0) - (a.score ?? 0));
    }, [items]);

    return (
        <Section title="COMMUNITY INTEL">
            {toast && (
                <div style={{
                    fontSize: '10px', fontFamily: mono, marginBottom: '6px',
                    padding: '5px 8px', borderRadius: tokens.radius.sm,
                    background: toast.type === 'error' ? `${colors.red}20` : `${colors.green}20`,
                    color: toast.type === 'error' ? colors.red : colors.green,
                    border: `1px solid ${toast.type === 'error' ? colors.red : colors.green}44`,
                }}>{toast.msg}</div>
            )}

            {!formOpen ? (
                <button
                    onClick={() => setFormOpen(true)}
                    style={{
                        width: '100%', padding: '8px',
                        background: `${colors.accent}15`, color: colors.accentLight,
                        border: `1px dashed ${colors.accent}66`,
                        borderRadius: tokens.radius.sm,
                        fontSize: '11px', fontFamily: mono, fontWeight: 700,
                        letterSpacing: '1px', textTransform: 'uppercase',
                        cursor: 'pointer', marginBottom: '8px',
                    }}
                >+ ADD INTEL</button>
            ) : (
                <div style={{
                    border: `1px solid ${colors.borderSubtle}`,
                    borderRadius: tokens.radius.sm,
                    padding: '8px', marginBottom: '8px',
                    background: colors.bg,
                }}>
                    <div style={{ display: 'flex', gap: '6px', marginBottom: '6px' }}>
                        <select
                            value={form.intel_type}
                            onChange={e => setForm(f => ({ ...f, intel_type: e.target.value }))}
                            style={{
                                flex: 1, padding: '5px', fontSize: '10px',
                                fontFamily: mono, background: colors.bg,
                                color: colors.text, border: `1px solid ${colors.borderSubtle}`,
                            }}
                        >
                            {Object.keys(INTEL_TYPE_COLOR).map(t => (
                                <option key={t} value={t}>{t}</option>
                            ))}
                        </select>
                        <select
                            value={form.confidence}
                            onChange={e => setForm(f => ({ ...f, confidence: e.target.value }))}
                            style={{
                                flex: 1, padding: '5px', fontSize: '10px',
                                fontFamily: mono, background: colors.bg,
                                color: colors.text, border: `1px solid ${colors.borderSubtle}`,
                            }}
                        >
                            <option value="high">high</option>
                            <option value="medium">medium</option>
                            <option value="low">low</option>
                        </select>
                    </div>
                    <textarea
                        placeholder="Share what you know (fact, connection, rumor, tip)…"
                        value={form.note}
                        onChange={e => setForm(f => ({ ...f, note: e.target.value }))}
                        rows={3}
                        style={{
                            width: '100%', padding: '6px', fontSize: '11px',
                            fontFamily: mono, background: colors.bg, color: colors.text,
                            border: `1px solid ${colors.borderSubtle}`,
                            borderRadius: '3px', resize: 'vertical',
                            marginBottom: '6px', boxSizing: 'border-box',
                        }}
                    />
                    <input
                        type="url"
                        placeholder="Source URL (optional)"
                        value={form.source_url}
                        onChange={e => setForm(f => ({ ...f, source_url: e.target.value }))}
                        style={{
                            width: '100%', padding: '6px', fontSize: '10px',
                            fontFamily: mono, background: colors.bg, color: colors.text,
                            border: `1px solid ${colors.borderSubtle}`,
                            borderRadius: '3px', marginBottom: '6px', boxSizing: 'border-box',
                        }}
                    />
                    <div style={{ display: 'flex', gap: '6px' }}>
                        <button
                            disabled={submitting}
                            onClick={onSubmit}
                            style={{
                                flex: 1, padding: '6px',
                                background: colors.accent, color: '#fff',
                                border: 'none', borderRadius: '3px',
                                fontSize: '10px', fontFamily: mono, fontWeight: 700,
                                letterSpacing: '1px', textTransform: 'uppercase',
                                cursor: submitting ? 'wait' : 'pointer',
                                opacity: submitting ? 0.6 : 1,
                            }}
                        >{submitting ? 'Sending…' : 'Submit'}</button>
                        <button
                            onClick={() => setFormOpen(false)}
                            style={{
                                flex: 1, padding: '6px',
                                background: 'transparent', color: colors.textMuted,
                                border: `1px solid ${colors.borderSubtle}`,
                                borderRadius: '3px', fontSize: '10px',
                                fontFamily: mono, cursor: 'pointer',
                                textTransform: 'uppercase', letterSpacing: '1px',
                            }}
                        >Cancel</button>
                    </div>
                </div>
            )}

            {loading && !items && (
                <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, textAlign: 'center', padding: '10px' }}>
                    Loading community intel…
                </div>
            )}

            {items && sorted.length === 0 && !loading && (
                <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, textAlign: 'center', padding: '10px' }}>
                    No community intel yet. Be the first to contribute.
                </div>
            )}

            {sorted.map(item => (
                <div key={item.id} style={{
                    border: `1px solid ${colors.borderSubtle}`,
                    borderLeft: `3px solid ${INTEL_TYPE_COLOR[item.intel_type] || colors.accent}`,
                    borderRadius: tokens.radius.sm,
                    padding: '7px 8px', marginBottom: '5px',
                    background: colors.bg,
                }}>
                    <div style={{
                        display: 'flex', alignItems: 'center', gap: '6px',
                        marginBottom: '4px', flexWrap: 'wrap',
                    }}>
                        <span style={{
                            padding: '1px 5px', borderRadius: '3px',
                            fontSize: '8px', fontFamily: mono, fontWeight: 700,
                            letterSpacing: '0.5px', textTransform: 'uppercase',
                            background: `${INTEL_TYPE_COLOR[item.intel_type] || colors.accent}25`,
                            color: INTEL_TYPE_COLOR[item.intel_type] || colors.accent,
                        }}>{item.intel_type}</span>
                        {item.verification_status === 'verified' && (
                            <span style={{
                                padding: '1px 5px', borderRadius: '3px',
                                fontSize: '8px', fontFamily: mono, fontWeight: 700,
                                background: `${colors.green}25`, color: colors.green,
                                letterSpacing: '0.5px',
                            }}>✓ VERIFIED</span>
                        )}
                        {item.confidence && (
                            <span style={{
                                fontSize: '8px', fontFamily: mono, color: colors.textMuted,
                                textTransform: 'uppercase', letterSpacing: '0.5px',
                            }}>conf: {item.confidence}</span>
                        )}
                    </div>
                    <div style={{ fontSize: '11px', color: colors.text, lineHeight: 1.4, marginBottom: '4px' }}>
                        {item.note}
                    </div>
                    {item.source_url && (
                        <a
                            href={item.source_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            style={{
                                fontSize: '9px', fontFamily: mono,
                                color: colors.accent, textDecoration: 'none',
                                display: 'block', marginBottom: '4px',
                                overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                            }}
                        >↗ {item.source_url}</a>
                    )}
                    <div style={{
                        display: 'flex', alignItems: 'center', gap: '8px',
                        fontSize: '9px', fontFamily: mono, color: colors.textMuted,
                    }}>
                        <span>by {item.submitted_by}</span>
                        <span>•</span>
                        <span>{item.submitted_at ? String(item.submitted_at).slice(0, 10) : ''}</span>
                        <div style={{ flex: 1 }} />
                        <button
                            onClick={() => onVote(item.id, 1)}
                            title="Upvote"
                            style={{
                                background: 'transparent', border: 'none',
                                color: colors.green, cursor: 'pointer',
                                fontSize: '11px', padding: '2px 4px',
                            }}
                        >▲ {item.upvotes ?? 0}</button>
                        <button
                            onClick={() => onVote(item.id, -1)}
                            title="Downvote"
                            style={{
                                background: 'transparent', border: 'none',
                                color: colors.red, cursor: 'pointer',
                                fontSize: '11px', padding: '2px 4px',
                            }}
                        >▼ {item.downvotes ?? 0}</button>
                        <button
                            onClick={() => onFlag(item.id)}
                            title="Flag as inappropriate"
                            style={{
                                background: 'transparent', border: 'none',
                                color: colors.textMuted, cursor: 'pointer',
                                fontSize: '10px', padding: '2px 4px',
                            }}
                        >⚑</button>
                    </div>
                </div>
            ))}
        </Section>
    );
};

const LensEmpty = ({ title, msg }) => (
    <div style={{
        padding: '28px 14px', textAlign: 'center',
        color: colors.textMuted, fontFamily: mono,
    }}>
        <div style={{ fontSize: '13px', color: colors.yellow, fontWeight: 700, marginBottom: '6px' }}>
            {title}
        </div>
        <div style={{ fontSize: '11px', lineHeight: 1.5 }}>{msg}</div>
    </div>
);

// ── Supply tab content ────────────────────────────────────────────
function SupplyTab({ actorId }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!actorId) return;
        let cancelled = false;
        setLoading(true); setError(null);
        api.getActorSupplyChain(actorId, 'both', 2)
            .then((d) => {
                if (cancelled) return;
                if (d && !d.error) setData(d);
                else setError(d?.error || 'Failed to load supply chain');
            })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Network error'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [actorId]);

    const summary = data?.summary || {};
    const chokepoints = (data?.chokepoints || []).slice(0, 3);
    const nodes = data?.nodes || [];
    const isFallback = data?.provenance?.source === 'fallback' || nodes.length === 0;

    // Group nodes by tier for compact column stack
    const tiers = useMemo(() => {
        const by = new Map();
        for (const n of nodes) {
            const t = n.tier ?? 0;
            if (!by.has(t)) by.set(t, []);
            by.get(t).push(n);
        }
        return [...by.entries()].sort((a, b) => a[0] - b[0]);
    }, [nodes]);

    if (loading) {
        return <div style={{ padding: '24px 0', fontSize: '11px', fontFamily: mono, color: colors.textMuted }}>
            Loading supply chain…
        </div>;
    }
    if (error || isFallback) {
        return (
            <>
                <LensEmpty
                    title="SUPPLY CHAIN PENDING"
                    msg={error || data?.narrative || 'No provenance edges yet for this actor.'} />
                <OpenInCanvasButton href={`#/canvas/${actorId}/supply`} label="Open Canvas Supply Lens" />
            </>
        );
    }

    return (
        <>
            {/* 3-KPI row */}
            <div style={{
                display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
                gap: '6px', marginBottom: tokens.space.md,
            }}>
                <MiniKpi label="Upstream"
                    value={`${summary.upstream_count || 0}`}
                    color={colors.green} />
                <MiniKpi label="Downstream"
                    value={`${summary.downstream_count || 0}`}
                    color={colors.accent} />
                <MiniKpi label="Chokepts"
                    value={`${chokepoints.length}`}
                    color={colors.red} />
            </div>

            {data?.narrative && (
                <div style={{
                    fontSize: '11px', color: colors.textDim, lineHeight: 1.5,
                    marginBottom: tokens.space.md,
                }}>{data.narrative}</div>
            )}

            {/* Tier stack — compact pills */}
            <div style={{ marginBottom: tokens.space.md }}>
                {tiers.map(([tier, list]) => {
                    const label = tier < 0 ? `UPSTREAM T${tier}` : tier === 0 ? 'FOCAL' : `DOWNSTREAM T+${tier}`;
                    return (
                        <div key={`tier-${tier}`} style={{ marginBottom: '10px' }}>
                            <div style={{
                                fontSize: '9px', letterSpacing: '1px', color: colors.textMuted,
                                fontFamily: mono, marginBottom: '4px',
                                borderTop: `1px dashed ${colors.borderSubtle}`, paddingTop: '6px',
                            }}>{label} ({list.length})</div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                {list.slice(0, 12).map((n) => {
                                    const fill = SUPPLY_TYPE_FILL[n.type] || SUPPLY_TYPE_FILL.actor;
                                    const isChoke = (n.chokepoint || 0) > 0 || (Number(n.chokepoint_score) || 0) > 0.5;
                                    const isFocal = tier === 0;
                                    return (
                                        <span key={n.id} title={`${n.label || n.id} · ${n.type || ''}${n.country ? ' · ' + n.country : ''}`} style={{
                                            background: `${fill}33`,
                                            border: `1px solid ${isChoke ? colors.red : (isFocal ? colors.accentLight : fill + '88')}`,
                                            color: isFocal ? colors.accentLight : colors.text,
                                            fontFamily: mono, fontSize: '10px', fontWeight: 600,
                                            padding: '3px 7px', borderRadius: '10px',
                                            whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                                            maxWidth: '180px',
                                        }}>
                                            {(n.label || n.id || '').slice(0, 22)}
                                        </span>
                                    );
                                })}
                                {list.length > 12 && (
                                    <span style={{
                                        fontFamily: mono, fontSize: '10px',
                                        color: colors.textMuted, padding: '3px 6px',
                                    }}>+{list.length - 12}</span>
                                )}
                            </div>
                        </div>
                    );
                })}
            </div>

            {/* Top chokepoints */}
            {chokepoints.length > 0 && (
                <div style={{ marginBottom: tokens.space.md }}>
                    <div style={{ ...shared.sectionTitle, marginBottom: tokens.space.xs }}>TOP CHOKEPOINTS</div>
                    {chokepoints.map((c, i) => {
                        const score = Math.max(0, Math.min(1, Number(c.score) || 0));
                        return (
                            <div key={`ch-${i}`} style={{
                                background: colors.bg,
                                border: `1px solid ${colors.red}33`,
                                borderLeft: `3px solid ${colors.red}`,
                                borderRadius: tokens.radius.sm,
                                padding: '6px 9px',
                                marginBottom: '6px',
                            }}>
                                <div style={{
                                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                    fontSize: '11px', fontWeight: 600, color: colors.text,
                                }}>
                                    <span style={{
                                        overflow: 'hidden', textOverflow: 'ellipsis',
                                        whiteSpace: 'nowrap', flex: 1,
                                    }}>{c.label || c.id}</span>
                                    <span style={{ fontFamily: mono, fontSize: '10px', color: colors.red, marginLeft: '6px' }}>
                                        {score.toFixed(2)}
                                    </span>
                                </div>
                                <div style={{
                                    height: '3px', background: colors.border,
                                    borderRadius: '2px', marginTop: '5px', overflow: 'hidden',
                                }}>
                                    <div style={{ width: `${score * 100}%`, height: '100%', background: colors.red }} />
                                </div>
                                {c.reason && (
                                    <div style={{ fontSize: '10px', color: colors.textDim, marginTop: '4px', lineHeight: 1.4 }}>
                                        {c.reason}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}

            <OpenInCanvasButton href={`#/canvas/${actorId}/supply`} label="Open Canvas Supply Lens" />
        </>
    );
}

// ── Capital tab content ───────────────────────────────────────────
function CapitalTab({ actorId }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!actorId) return;
        let cancelled = false;
        setLoading(true); setError(null);
        api.getActorCapitalFlow(actorId, 4, 'annual')
            .then((d) => {
                if (cancelled) return;
                if (d && !d.error) setData(d);
                else setError(d?.error || 'Failed to load capital flow');
            })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Network error'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [actorId]);

    const summary = data?.summary || {};
    const periods = Array.isArray(data?.periods) ? data.periods : [];
    const latest = periods.length > 0 ? periods[periods.length - 1] : null;
    const isFallback = data?.provenance?.source === 'fallback' || !latest;

    // Build the "mini sankey" — a horizontal 3-column stacked bar.
    // Inflows left, actor middle, outflows right. Bar heights ∝ share of total.
    const miniBars = useMemo(() => {
        if (!latest) return null;
        const inflows = (latest.inflows || [])
            .map((f) => ({ ...f, amt: Math.max(0, Number(f.amount_usd) || 0) }))
            .filter((f) => f.amt > 0);
        const outflows = (latest.outflows || [])
            .map((f) => ({ ...f, amt: Math.max(0, Number(f.amount_usd) || 0) }))
            .filter((f) => f.amt > 0);
        if (inflows.length === 0 && outflows.length === 0) return null;
        const totalIn = inflows.reduce((a, f) => a + f.amt, 0) || 1;
        const totalOut = outflows.reduce((a, f) => a + f.amt, 0) || 1;
        return { inflows, outflows, totalIn, totalOut };
    }, [latest]);

    if (loading) {
        return <div style={{ padding: '24px 0', fontSize: '11px', fontFamily: mono, color: colors.textMuted }}>
            Loading capital flow…
        </div>;
    }
    if (error || isFallback) {
        return (
            <>
                <LensEmpty
                    title="CAPITAL FLOW PENDING"
                    msg={error || data?.narrative || 'No filings aggregated for this actor yet.'} />
                <OpenInCanvasButton href={`#/canvas/${actorId}/capital`} label="Open Canvas Capital Lens" />
            </>
        );
    }

    // KPI grid — 4 up, using data from summary
    const topOutflow = miniBars && miniBars.outflows.length
        ? [...miniBars.outflows].sort((a, b) => b.amt - a.amt)[0]
        : null;

    // Percentile context — pulled from the latest period's _percentiles block
    // so each KPI gets a per-sector ranking badge next to the value.
    const latestRatios = latest?.ratios || {};
    const pcts = latestRatios._percentiles || {};
    const sectorName = data?.actor?.sector || '';
    const sectorShort = sectorName ? sectorName.slice(0, 10).toUpperCase() : '';
    const makeBadge = (ratioKey) => {
        const p = pcts[ratioKey];
        if (p == null) return null;
        const rounded = Math.round(Number(p));
        return {
            ...percentileColors(p),
            text: sectorShort ? `P${rounded} · ${sectorShort}` : `P${rounded}`,
        };
    };

    return (
        <>
            <div style={{
                display: 'grid', gridTemplateColumns: '1fr 1fr',
                gap: '6px', marginBottom: tokens.space.md,
            }}>
                <MiniKpi label="Gross Margin"
                    value={fmtPctPlain(latestRatios.gross_margin)}
                    color={colors.accentLight}
                    badge={makeBadge('gross_margin')} />
                <MiniKpi label="3y CAGR"
                    value={fmtPctPlain(summary.revenue_3y_cagr)}
                    color={(Number(summary.revenue_3y_cagr) || 0) >= 0 ? colors.green : colors.red} />
                <MiniKpi label="Capex Int."
                    value={fmtPctPlain(latestRatios.capex_intensity ?? summary.capex_3y_avg_intensity)}
                    color={colors.yellow}
                    badge={makeBadge('capex_intensity')} />
                <MiniKpi label="FCF Conv."
                    value={fmtPctPlain(latestRatios.fcf_conversion)}
                    color={colors.green}
                    badge={makeBadge('fcf_conversion')} />
                <MiniKpi label="Opex Int."
                    value={fmtPctPlain(latestRatios.opex_intensity)}
                    color={colors.yellow}
                    badge={makeBadge('opex_intensity')} />
                <MiniKpi label="Shareholder Yld"
                    value={fmtPctPlain(latestRatios.shareholder_yield)}
                    color={colors.accentLight}
                    badge={makeBadge('shareholder_yield')} />
                <MiniKpi label="Revenue"
                    value={fmtUsdShort(summary.latest_revenue_usd)}
                    color={colors.accentLight} />
                <MiniKpi label="Top Use"
                    value={topOutflow ? (topOutflow.flow_type || '—') : '—'}
                    color={topOutflow ? pickOutflow(topOutflow.flow_type) : colors.text} />
            </div>

            {/* Mini sankey — latest period only */}
            {miniBars && (
                <div style={{ marginBottom: tokens.space.md }}>
                    <div style={{
                        ...shared.sectionTitle, marginBottom: tokens.space.xs,
                        display: 'flex', justifyContent: 'space-between', alignItems: 'baseline',
                    }}>
                        <span>LATEST PERIOD</span>
                        <span style={{ fontSize: '9px', color: colors.textMuted }}>
                            {latest.label || latest.fiscal_period || ''}
                        </span>
                    </div>
                    <div style={{
                        background: colors.bg,
                        border: `1px solid ${colors.borderSubtle}`,
                        borderRadius: tokens.radius.sm,
                        padding: '10px',
                    }}>
                        {/* Inflows */}
                        <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: mono, marginBottom: '4px', letterSpacing: '1px' }}>
                            INFLOWS
                        </div>
                        {miniBars.inflows.map((f, i) => {
                            const pct = f.amt / miniBars.totalIn;
                            return (
                                <div key={`in-${i}`} style={{ marginBottom: '5px' }}>
                                    <div style={{
                                        display: 'flex', justifyContent: 'space-between',
                                        fontSize: '10px', fontFamily: mono,
                                    }}>
                                        <span style={{ color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }}>
                                            {f.flow_type}
                                        </span>
                                        <span style={{ color: INFLOW_COLOR }}>
                                            {fmtUsdShort(f.amt)}
                                        </span>
                                    </div>
                                    <div style={{ height: '4px', background: colors.border, borderRadius: '2px', marginTop: '2px', overflow: 'hidden' }}>
                                        <div style={{
                                            width: `${Math.max(4, pct * 100)}%`, height: '100%',
                                            background: INFLOW_COLOR, opacity: 0.85,
                                        }} />
                                    </div>
                                </div>
                            );
                        })}

                        {/* Actor pivot strip */}
                        <div style={{
                            background: `${colors.accent}22`,
                            border: `1px solid ${colors.accent}66`,
                            borderRadius: tokens.radius.sm,
                            padding: '4px 8px',
                            margin: '8px 0',
                            fontSize: '10px', fontFamily: mono, fontWeight: 700,
                            color: colors.accentLight, textAlign: 'center',
                            letterSpacing: '1px',
                        }}>
                            {(data?.actor?.label || actorId || '').slice(0, 32).toUpperCase()}
                        </div>

                        {/* Outflows */}
                        <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: mono, marginBottom: '4px', letterSpacing: '1px' }}>
                            OUTFLOWS
                        </div>
                        {miniBars.outflows.map((f, i) => {
                            const pct = f.amt / miniBars.totalOut;
                            const c = pickOutflow(f.flow_type);
                            return (
                                <div key={`out-${i}`} style={{ marginBottom: '5px' }}>
                                    <div style={{
                                        display: 'flex', justifyContent: 'space-between',
                                        fontSize: '10px', fontFamily: mono,
                                    }}>
                                        <span style={{ color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', maxWidth: '60%' }}>
                                            {f.flow_type}
                                        </span>
                                        <span style={{ color: c }}>{fmtUsdShort(f.amt)}</span>
                                    </div>
                                    <div style={{ height: '4px', background: colors.border, borderRadius: '2px', marginTop: '2px', overflow: 'hidden' }}>
                                        <div style={{
                                            width: `${Math.max(4, pct * 100)}%`, height: '100%',
                                            background: c, opacity: 0.85,
                                        }} />
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {data?.narrative && (
                <div style={{
                    fontSize: '11px', color: colors.textDim,
                    lineHeight: 1.5, marginBottom: tokens.space.md,
                }}>{data.narrative}</div>
            )}

            <OpenInCanvasButton href={`#/canvas/${actorId}/capital`} label="Open Canvas Capital Lens" />
        </>
    );
}

// ── Explain tab content ───────────────────────────────────────────
// Hero "why did this move?" lens — ranked evidence from every
// intelligence source (insider/congress/dark pool/options/capital flows/
// supply shock attributions/chain contagion predictions/corporate actions/
// news). User controls a date pivot + a ±window slider.
const EVIDENCE_TYPE_COLORS = {
    contagion_prediction:   '#A855F7',
    supply_shock_attribution: '#F97316',
    announcement:           '#FBBF24',
    corporate_action:       '#FACC15',
    insider_trade:          '#10B981',
    contagion_backtest:     '#8B5CF6',
    congressional_trade:    '#3B82F6',
    options_signal:         '#EC4899',
    dark_pool:              '#64748B',
    news:                   '#94A3B8',
};

const todayIso = () => {
    const d = new Date();
    return `${d.getFullYear()}-${String(d.getMonth() + 1).padStart(2, '0')}-${String(d.getDate()).padStart(2, '0')}`;
};

function ExplainTab({ actorId }) {
    const [pivotDate, setPivotDate] = useState(todayIso());
    const [windowDays, setWindowDays] = useState(5);
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => {
        if (!actorId) return;
        let cancelled = false;
        setLoading(true); setError(null);
        api.getActorExplain(actorId, pivotDate, windowDays)
            .then((d) => {
                if (cancelled) return;
                if (d && !d.error) setData(d);
                else setError(d?.error || 'Failed to load explain');
            })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Network error'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [actorId, pivotDate, windowDays]);

    const evidence = Array.isArray(data?.evidence) ? data.evidence : [];
    const grouped = useMemo(() => {
        const g = {};
        for (const ev of evidence) {
            (g[ev.type] ||= []).push(ev);
        }
        return g;
    }, [evidence]);

    const move = data?.actual_move || {};
    const movePct = move.pct;

    return (
        <>
            {/* Controls row */}
            <div style={{
                display: 'flex', gap: '8px', marginBottom: tokens.space.md,
                alignItems: 'flex-end', flexWrap: 'wrap',
            }}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '3px' }}>
                    <label style={{
                        fontSize: '9px', color: colors.textMuted, fontFamily: mono,
                        letterSpacing: '1px', textTransform: 'uppercase',
                    }}>PIVOT DATE</label>
                    <input
                        type="date"
                        value={pivotDate}
                        onChange={(e) => setPivotDate(e.target.value)}
                        style={{
                            background: colors.bg,
                            color: colors.text,
                            border: `1px solid ${colors.border}`,
                            borderRadius: tokens.radius.sm,
                            padding: '6px 8px',
                            fontFamily: mono, fontSize: '11px',
                        }}
                    />
                </div>
                <div style={{ flex: 1, minWidth: '140px', display: 'flex', flexDirection: 'column', gap: '3px' }}>
                    <label style={{
                        fontSize: '9px', color: colors.textMuted, fontFamily: mono,
                        letterSpacing: '1px', textTransform: 'uppercase',
                    }}>WINDOW ± {windowDays}d</label>
                    <input
                        type="range"
                        min={1} max={14} step={1}
                        value={windowDays}
                        onChange={(e) => setWindowDays(Number(e.target.value))}
                        style={{ width: '100%', accentColor: colors.accent }}
                    />
                </div>
            </div>

            {/* Actual move card */}
            <div style={{
                ...shared.card, marginBottom: tokens.space.md, background: colors.bg,
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                gap: '12px',
            }}>
                <div>
                    <div style={{
                        fontSize: '9px', color: colors.textMuted, fontFamily: mono,
                        letterSpacing: '1px', textTransform: 'uppercase',
                    }}>ACTUAL MOVE</div>
                    <div style={{
                        fontSize: '18px', fontFamily: mono, fontWeight: 700,
                        color: movePct == null
                            ? colors.textDim
                            : (movePct >= 0 ? colors.green : colors.red),
                    }}>
                        {movePct == null ? '—' : fmtPct(movePct)}
                    </div>
                </div>
                <div style={{
                    fontSize: '10px', color: colors.textDim, fontFamily: mono,
                    textAlign: 'right',
                }}>
                    <div>{move.start_price != null ? `$${Number(move.start_price).toFixed(2)}` : '--'}</div>
                    <div>→ {move.end_price != null ? `$${Number(move.end_price).toFixed(2)}` : '--'}</div>
                </div>
            </div>

            {loading && (
                <div style={{
                    padding: '18px 0', fontSize: '11px', fontFamily: mono,
                    color: colors.textMuted, textAlign: 'center',
                }}>
                    <div style={{
                        display: 'inline-block', width: '14px', height: '14px',
                        border: `2px solid ${colors.border}`, borderTop: `2px solid ${colors.accent}`,
                        borderRadius: '50%', animation: 'spin 1s linear infinite',
                        verticalAlign: 'middle', marginRight: '8px',
                    }} />
                    Scanning evidence…
                </div>
            )}

            {error && (
                <div style={{
                    padding: '14px', fontSize: '11px', fontFamily: mono,
                    color: colors.red,
                }}>{error}</div>
            )}

            {!loading && !error && data && (
                <>
                    {/* Narrative */}
                    <div style={{
                        ...shared.card, marginBottom: tokens.space.md,
                        fontSize: '12px', lineHeight: 1.55, color: colors.textDim,
                    }}>{data.summary}</div>

                    {/* Evidence list grouped by type, sorted by strength */}
                    {Object.entries(grouped).map(([type, rows]) => (
                        <div key={type} style={{ marginBottom: tokens.space.md }}>
                            <div style={{
                                fontSize: '9px', letterSpacing: '1px',
                                color: EVIDENCE_TYPE_COLORS[type] || colors.textMuted,
                                fontFamily: mono, textTransform: 'uppercase',
                                marginBottom: '6px', fontWeight: 700,
                            }}>
                                {type.replace(/_/g, ' ')} · {rows.length}
                            </div>
                            {rows.map((ev, i) => (
                                <div key={i} style={{
                                    background: colors.bg,
                                    border: `1px solid ${colors.borderSubtle}`,
                                    borderLeft: `3px solid ${EVIDENCE_TYPE_COLORS[type] || colors.accent}`,
                                    borderRadius: tokens.radius.sm,
                                    padding: '8px 10px', marginBottom: '4px',
                                    display: 'flex', flexDirection: 'column', gap: '4px',
                                }}>
                                    <div style={{
                                        display: 'flex', justifyContent: 'space-between',
                                        alignItems: 'center', gap: '8px',
                                    }}>
                                        <div style={{
                                            flex: 1, fontSize: '11px', color: colors.text,
                                            lineHeight: 1.4,
                                        }}>{ev.summary}</div>
                                        <div style={{
                                            fontSize: '10px', fontFamily: mono, fontWeight: 700,
                                            color: EVIDENCE_TYPE_COLORS[type] || colors.accent,
                                            minWidth: '36px', textAlign: 'right',
                                        }}>{Math.round((ev.strength || 0) * 100)}%</div>
                                    </div>
                                    <div style={{
                                        width: '100%', height: '3px', borderRadius: '2px',
                                        background: colors.border, overflow: 'hidden',
                                    }}>
                                        <div style={{
                                            width: `${Math.min(100, (ev.strength || 0) * 100)}%`,
                                            height: '100%',
                                            background: EVIDENCE_TYPE_COLORS[type] || colors.accent,
                                        }} />
                                    </div>
                                </div>
                            ))}
                        </div>
                    ))}

                    {evidence.length === 0 && (
                        <LensEmpty
                            title="NO EVIDENCE"
                            msg={`No signals surfaced from ${data.provenance?.sources_checked || 0} sources in this window.`} />
                    )}

                    {/* Provenance footer */}
                    <div style={{
                        fontSize: '9px', fontFamily: mono, color: colors.textMuted,
                        letterSpacing: '0.5px', marginTop: tokens.space.md,
                        padding: '8px 0', borderTop: `1px solid ${colors.borderSubtle}`,
                    }}>
                        {data.provenance?.sources_checked || 0} sources scanned ·{' '}
                        {data.provenance?.evidence_rows || 0} rows found ·{' '}
                        window {data.window?.start} → {data.window?.end}
                    </div>
                </>
            )}

            <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
        </>
    );
}

// ── External Intel section ────────────────────────────────────────
// "Make it easy to look them up, like a Google News search or Wikipedia."
// Renders quick-lookup pill buttons + inline Wikipedia intro pull + internal
// actor_news count/expand. CORS-friendly Wikipedia API — no proxy needed.

const PillLink = ({ href, icon: Icon, label, color }) => (
    <a href={href} target="_blank" rel="noopener noreferrer" style={{
        display: 'inline-flex', alignItems: 'center', gap: '6px',
        background: `${color}15`, border: `1px solid ${color}40`,
        color, padding: '6px 10px', borderRadius: tokens.radius.sm,
        fontSize: '11px', fontFamily: mono, fontWeight: 600,
        textDecoration: 'none', cursor: 'pointer',
        transition: 'all 150ms',
    }}
    onMouseEnter={(e) => { e.currentTarget.style.background = `${color}25`; }}
    onMouseLeave={(e) => { e.currentTarget.style.background = `${color}15`; }}
    >
        {Icon && <Icon size={12} strokeWidth={2.2} />}
        {label}
        <ExternalLink size={10} strokeWidth={2.2} style={{ opacity: 0.6 }} />
    </a>
);

function ExternalIntelSection({ actor, data, onNavigate }) {
    const name = data?.label || actor?.id || '';
    const type = data?.type || 'unknown';
    const actorId = actor?.id;

    const [wiki, setWiki] = useState({ loading: true, intro: null, thumb: null, url: null, error: null });
    const [news, setNews] = useState({ loading: true, count: 0, items: [], available: false });
    const [newsExpanded, setNewsExpanded] = useState(false);

    // Wikipedia inline pull — CORS-enabled via origin=*.
    useEffect(() => {
        if (!name) return;
        let cancelled = false;
        setWiki({ loading: true, intro: null, thumb: null, url: null, error: null });

        // Clean the display name: drop parentheticals, strip ticker suffixes.
        const cleanName = name.replace(/\s*\([^)]*\)\s*/g, '').trim();
        const title = encodeURIComponent(cleanName);
        const url = `https://en.wikipedia.org/w/api.php?action=query&prop=extracts|pageimages&titles=${title}&exintro=1&explaintext=1&piprop=thumbnail&pithumbsize=120&format=json&origin=*&redirects=1`;

        fetch(url)
            .then(r => r.ok ? r.json() : Promise.reject(new Error(`HTTP ${r.status}`)))
            .then(j => {
                if (cancelled) return;
                const pages = j?.query?.pages || {};
                const pageIds = Object.keys(pages);
                const firstId = pageIds[0];
                if (!firstId || firstId === '-1') {
                    setWiki({ loading: false, intro: null, thumb: null, url: null, error: 'not_found' });
                    return;
                }
                const page = pages[firstId];
                const extract = (page?.extract || '').trim();
                const thumb = page?.thumbnail?.source || null;
                const pageUrl = `https://en.wikipedia.org/wiki/${encodeURIComponent((page?.title || cleanName).replace(/ /g, '_'))}`;
                setWiki({
                    loading: false,
                    intro: extract ? extract.slice(0, 480) + (extract.length > 480 ? '…' : '') : null,
                    thumb,
                    url: pageUrl,
                    error: extract ? null : 'empty',
                });
            })
            .catch((err) => {
                if (cancelled) return;
                setWiki({ loading: false, intro: null, thumb: null, url: null, error: err.message || 'fetch_failed' });
            });
        return () => { cancelled = true; };
    }, [name]);

    // Internal actor_news pull.
    useEffect(() => {
        if (!actorId) return;
        let cancelled = false;
        setNews({ loading: true, count: 0, items: [], available: false });
        api.getActorNews(actorId, 20)
            .then((d) => {
                if (cancelled) return;
                setNews({
                    loading: false,
                    count: d?.count || 0,
                    items: d?.items || [],
                    available: !!d?.available,
                });
            })
            .catch(() => {
                if (cancelled) return;
                setNews({ loading: false, count: 0, items: [], available: false });
            });
        return () => { cancelled = true; };
    }, [actorId]);

    // Build the link set per actor type.
    const links = useMemo(() => {
        const q = encodeURIComponent(name);
        const out = [];
        // Google News — always
        out.push({
            href: `https://news.google.com/search?q=${q}`,
            icon: Newspaper, label: 'Google News', color: '#60A5FA',
        });
        // Wikipedia link — always (fallback if inline pull fails)
        out.push({
            href: wiki.url || `https://en.wikipedia.org/wiki/Special:Search?search=${q}`,
            icon: BookOpen, label: 'Wikipedia', color: '#E2E8F0',
        });
        // SEC EDGAR — companies + tickers + filer-like actors
        if (type === 'company' || type === 'family_office' || type === 'trade_org' || type === 'unknown') {
            out.push({
                href: `https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&company=${q}&type=&dateb=&owner=include&count=10`,
                icon: Building2, label: 'SEC EDGAR', color: '#10B981',
            });
        }
        // LinkedIn — people
        if (type === 'person' || type === 'family_office') {
            out.push({
                href: `https://www.linkedin.com/search/results/people/?keywords=${q}`,
                icon: Users, label: 'LinkedIn', color: '#38BDF8',
            });
        }
        // OpenSecrets — political / regulator / person
        if (type === 'person' || type === 'regulator') {
            out.push({
                href: `https://www.opensecrets.org/search?q=${q}`,
                icon: Landmark, label: 'OpenSecrets', color: '#F59E0B',
            });
        }
        return out;
    }, [name, type, wiki.url]);

    const fmtDate = (iso) => {
        if (!iso) return '';
        try {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return String(iso).slice(0, 10);
            return d.toISOString().slice(0, 10);
        } catch { return String(iso).slice(0, 10); }
    };

    return (
        <Section title="EXTERNAL INTEL">
            {/* Link pills */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginBottom: tokens.space.sm }}>
                {links.map((l, i) => <PillLink key={i} {...l} />)}
            </div>

            {/* Wikipedia inline card */}
            <div style={{
                background: colors.bg,
                border: `1px solid ${colors.borderSubtle}`,
                borderRadius: tokens.radius.sm,
                padding: '10px 12px',
                marginBottom: tokens.space.sm,
                display: 'flex', gap: '10px', alignItems: 'flex-start',
                minHeight: '60px',
            }}>
                {wiki.loading && (
                    <div style={{ color: colors.textMuted, fontFamily: mono, fontSize: '10px' }}>
                        loading wikipedia…
                    </div>
                )}
                {!wiki.loading && wiki.intro && (
                    <>
                        {wiki.thumb && (
                            <img src={wiki.thumb} alt="" style={{
                                width: '56px', height: '56px', objectFit: 'cover',
                                borderRadius: tokens.radius.sm, flexShrink: 0,
                                border: `1px solid ${colors.borderSubtle}`,
                            }} />
                        )}
                        <div style={{ flex: 1, minWidth: 0 }}>
                            <div style={{
                                fontSize: '9px', fontFamily: mono, color: colors.textMuted,
                                letterSpacing: '1px', marginBottom: '3px',
                            }}>WIKIPEDIA</div>
                            <div style={{ fontSize: '11px', color: colors.textDim, lineHeight: 1.5 }}>
                                {wiki.intro}
                            </div>
                            {wiki.url && (
                                <a href={wiki.url} target="_blank" rel="noopener noreferrer" style={{
                                    display: 'inline-block', marginTop: '6px',
                                    fontSize: '10px', fontFamily: mono, color: colors.accent,
                                    textDecoration: 'none',
                                }}>read more →</a>
                            )}
                        </div>
                    </>
                )}
                {!wiki.loading && !wiki.intro && (
                    <div style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                        no wikipedia match —{' '}
                        <a href={`https://en.wikipedia.org/wiki/Special:Search?search=${encodeURIComponent(name)}`}
                           target="_blank" rel="noopener noreferrer"
                           style={{ color: colors.accent, textDecoration: 'none' }}>
                            search manually →
                        </a>
                    </div>
                )}
            </div>

            {/* Internal news */}
            <div style={{
                background: colors.bg,
                border: `1px solid ${colors.borderSubtle}`,
                borderRadius: tokens.radius.sm,
                padding: '8px 12px',
            }}>
                {news.loading && (
                    <div style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                        loading internal news…
                    </div>
                )}
                {!news.loading && news.count > 0 && (
                    <>
                        <button
                            onClick={() => setNewsExpanded((v) => !v)}
                            style={{
                                display: 'flex', alignItems: 'center', gap: '6px',
                                background: 'transparent', border: 'none',
                                color: colors.accent, fontFamily: mono, fontSize: '11px',
                                fontWeight: 600, cursor: 'pointer', padding: 0, width: '100%',
                                textAlign: 'left',
                            }}>
                            {newsExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                            View {news.count} internal news item{news.count === 1 ? '' : 's'}
                        </button>
                        {newsExpanded && (
                            <div style={{ marginTop: '8px', display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                {news.items.map((item, i) => {
                                    const content = (
                                        <>
                                            <div style={{
                                                fontSize: '11px', color: colors.text, lineHeight: 1.4,
                                                marginBottom: '2px',
                                            }}>{item.title || '(untitled)'}</div>
                                            <div style={{
                                                fontSize: '9px', fontFamily: mono, color: colors.textMuted,
                                                display: 'flex', gap: '8px',
                                            }}>
                                                {item.source && <span>{item.source}</span>}
                                                {item.published_at && <span>{fmtDate(item.published_at)}</span>}
                                            </div>
                                        </>
                                    );

                                    const rowStyle = {
                                        display: 'block',
                                        padding: '6px 8px',
                                        borderTop: `1px solid ${colors.borderSubtle}`,
                                        textDecoration: 'none',
                                    };

                                    if (!item.url) {
                                        return (
                                            <div key={i} style={rowStyle}>
                                                {content}
                                            </div>
                                        );
                                    }

                                    return (
                                        <a
                                            key={i}
                                            href={item.url}
                                            target="_blank"
                                            rel="noopener noreferrer"
                                            style={rowStyle}
                                        >
                                            {content}
                                        </a>
                                    );
                                })}
                            </div>
                        )}
                    </>
                )}
                {!news.loading && news.count === 0 && (
                    <div style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                        {news.available ? 'no internal news rows for this actor' : 'internal news table not yet available'}
                    </div>
                )}
            </div>
        </Section>
    );
}

// ── Tab bar ───────────────────────────────────────────────────────
const TabBar = ({ active, onChange }) => {
    const tabs = [
        { id: 'overview', label: 'Overview' },
        { id: 'explain',  label: 'Explain' },
        { id: 'supply',   label: 'Supply' },
        { id: 'capital',  label: 'Capital' },
    ];
    return (
        <div style={{
            display: 'flex', gap: '0',
            borderBottom: `1px solid ${colors.border}`,
            background: colors.card,
            position: 'sticky', top: '56px', zIndex: 2,
        }}>
            {tabs.map((t) => {
                const isActive = t.id === active;
                return (
                    <button key={t.id}
                        onClick={() => onChange(t.id)}
                        style={{
                            flex: 1,
                            background: 'transparent',
                            border: 'none',
                            borderBottom: `2px solid ${isActive ? colors.accentLight : 'transparent'}`,
                            color: isActive ? colors.accentLight : colors.textDim,
                            fontFamily: mono, fontSize: '11px',
                            fontWeight: 700, letterSpacing: '1.5px',
                            textTransform: 'uppercase',
                            padding: '11px 6px',
                            cursor: 'pointer',
                            transition: 'all 150ms',
                        }}>
                        {t.label}
                    </button>
                );
            })}
        </div>
    );
};

// ── Main component ────────────────────────────────────────────────
export default function ActorProfileDrawer({ actor, onClose, onNavigate }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [activeTab, setActiveTab] = useState('overview');
    // Lazy-fetch cache: once a tab mounts, its content component holds the
    // data in its own state. We use a mount latch so we don't unmount/remount
    // the tab (and thus refetch) every time the user toggles tabs.
    const [mounted, setMounted] = useState({ overview: true, explain: false, supply: false, capital: false });

    const load = useCallback(async (target) => {
        if (!target?.id) return;
        setLoading(true); setError(null);
        try {
            const d = await api.getActorProfileDetail(target.id, target.sector || null);
            if (d?.error) throw new Error(d.error);
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load actor');
        }
        setLoading(false);
    }, []);

    useEffect(() => {
        if (actor?.id) {
            load(actor);
            // Reset tab + mount latches on new actor so stale sub-tab data is dropped.
            setActiveTab('overview');
            setMounted({ overview: true, explain: false, supply: false, capital: false });
        } else {
            setData(null);
        }
    }, [actor, load]);

    useEffect(() => {
        if (!actor) return;
        const onKey = (e) => { if (e.key === 'Escape') onClose?.(); };
        window.addEventListener('keydown', onKey);
        return () => window.removeEventListener('keydown', onKey);
    }, [actor, onClose]);

    // Latch a tab as mounted the first time it's activated — keeps its state/cache alive
    // until a new actor is loaded.
    useEffect(() => {
        setMounted((m) => (m[activeTab] ? m : { ...m, [activeTab]: true }));
    }, [activeTab]);

    if (!actor) return null;

    const typeColor = TYPE_COLOR[data?.type] || TYPE_COLOR.unknown;
    const isCompany = data?.type === 'company';
    const sig = data?.signals || {};
    const grouped = {};
    for (const c of data?.connections || []) {
        (grouped[c.type || 'other'] ||= []).push(c);
    }

    return (
        <>
            <style>{`@keyframes actorDrawerIn { from { transform: translateX(100%); } to { transform: translateX(0); } }`}</style>
            <div onClick={onClose} style={{
                position: 'fixed', inset: 0, background: 'rgba(2,6,12,0.55)',
                backdropFilter: 'blur(2px)', zIndex: 900,
            }} />
            <aside onClick={(e) => e.stopPropagation()} style={{
                position: 'fixed', top: 0, right: 0, bottom: 0,
                width: '480px', maxWidth: '100vw',
                background: colors.card, borderLeft: `1px solid ${colors.border}`,
                boxShadow: '-12px 0 32px rgba(0,0,0,0.6)',
                overflowY: 'auto', zIndex: 901,
                fontFamily: colors.sans, color: colors.text,
                animation: 'actorDrawerIn 0.25s ease-out',
            }}>
                {/* Header */}
                <div style={{
                    padding: '14px 18px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', alignItems: 'center', gap: '10px',
                    position: 'sticky', top: 0, background: colors.card, zIndex: 3,
                    height: '56px', boxSizing: 'border-box',
                }}>
                    <div style={{ width: '10px', height: '10px', borderRadius: '50%', background: typeColor, flexShrink: 0 }} />
                    <div style={{ flex: 1, minWidth: 0 }}>
                        <div style={{
                            fontSize: '15px', fontWeight: 700, color: '#E8F0F8',
                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                        }}>{data?.label || actor.id}</div>
                        <div style={{
                            display: 'inline-block', marginTop: '2px',
                            fontSize: '9px', fontWeight: 700, letterSpacing: '1px',
                            color: typeColor, fontFamily: mono,
                            background: `${typeColor}15`, padding: '2px 8px',
                            borderRadius: tokens.radius.sm, textTransform: 'uppercase',
                        }}>
                            {data?.type || '...'}
                            {data?.sector && <span style={{ color: colors.textMuted, marginLeft: '6px' }}>· {data.sector}</span>}
                        </div>
                    </div>
                    <button onClick={onClose} aria-label="Close" style={{
                        background: 'transparent', border: `1px solid ${colors.border}`,
                        color: colors.textDim, cursor: 'pointer', borderRadius: tokens.radius.sm,
                        width: '28px', height: '28px', fontSize: '16px',
                        display: 'flex', alignItems: 'center', justifyContent: 'center',
                    }}>x</button>
                </div>

                {/* Tab bar */}
                <TabBar active={activeTab} onChange={setActiveTab} />

                <div style={{ padding: '14px 18px' }}>
                    {activeTab === 'overview' && (
                        <>
                            {loading && <div style={{ color: colors.textMuted, fontFamily: mono, fontSize: '11px', padding: '24px 0' }}>Loading actor profile...</div>}
                            {error && <div style={{ color: colors.red, fontFamily: mono, fontSize: '11px', padding: '24px 0' }}>{error}</div>}

                            {data && !loading && !error && (
                                <>
                                    <div style={{ ...shared.card, marginBottom: tokens.space.md, background: colors.bg }}>
                                        {isCompany && data.price != null ? (
                                            <div style={{ display: 'flex', alignItems: 'baseline', gap: '12px', flexWrap: 'wrap' }}>
                                                <span style={{ fontSize: '22px', fontWeight: 700, color: '#E8F0F8', fontFamily: mono }}>
                                                    ${Number(data.price).toFixed(2)}
                                                </span>
                                                <span style={{ fontFamily: mono, fontSize: '12px', fontWeight: 600,
                                                    color: (data.change_1d ?? 0) >= 0 ? colors.green : colors.red }}>
                                                    {fmtPct(data.change_1d)} (1d)
                                                </span>
                                                <span style={{ fontFamily: mono, fontSize: '12px', fontWeight: 600,
                                                    color: (data.change_30d ?? 0) >= 0 ? colors.green : colors.red }}>
                                                    {fmtPct(data.change_30d)} (30d)
                                                </span>
                                                {data.market_cap && (
                                                    <span style={{ fontFamily: mono, fontSize: '11px', color: colors.textDim, marginLeft: 'auto' }}>
                                                        mcap {fmtUSD(data.market_cap)}
                                                    </span>
                                                )}
                                            </div>
                                        ) : (
                                            <div style={{ fontSize: '12px', color: colors.textDim, fontStyle: 'italic', lineHeight: 1.5 }}>
                                                {data.description || 'No description available.'}
                                            </div>
                                        )}
                                    </div>

                                    {data.description && isCompany && (
                                        <Section title="DESCRIPTION">
                                            <div style={{ fontSize: '12px', color: colors.textDim, lineHeight: 1.55 }}>{data.description}</div>
                                        </Section>
                                    )}

                                    <TrustCogBadge actorId={actor.id} />

                                    {/* SWEEP: only render for equity tickers — skip for
                                        actors without a ticker (people, family offices, etc.) */}
                                    {actor.ticker && (
                                        <ConfidenceStackPanel ticker={actor.ticker} />
                                    )}

                                    {isCompany && data.signals && (
                                        <Section title="SIGNALS">
                                            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                                                <SigTile label="Options" value={sig.options_signal} color={SIGNAL_COLOR(sig.options_signal)} />
                                                <SigTile label="Dark Pool" value={sig.dark_pool_signal} color={SIGNAL_COLOR(sig.dark_pool_signal)} />
                                                <SigTile label="Insider 30d" value={`${(sig.insider_trades_30d || []).length} trades`} color={colors.text} />
                                                <SigTile label="Congress 60d" value={`${(sig.congressional_trades_60d || []).length} trades`} color={colors.text} />
                                            </div>
                                            {(sig.insider_trades_30d || []).slice(0, 3).map((t, i) => (
                                                <div key={i} style={{
                                                    display: 'flex', justifyContent: 'space-between',
                                                    fontSize: '10px', fontFamily: mono, color: colors.textDim,
                                                    padding: '3px 0', borderTop: `1px solid ${colors.borderSubtle}`,
                                                }}>
                                                    <span>{t.name || '--'}</span>
                                                    <span style={{ color: (t.type === 'P' || t.type === 'Purchase') ? colors.green : colors.red }}>
                                                        {t.type} {t.value ? fmtUSD(t.value) : ''}
                                                    </span>
                                                </div>
                                            ))}
                                        </Section>
                                    )}

                                    {isCompany && data.holders_top10?.length > 0 && (
                                        <Section title="TOP HOLDERS">
                                            <div style={{ fontSize: '10px', fontFamily: mono }}>
                                                {data.holders_top10.map((h, i) => (
                                                    <div key={i} style={{
                                                        display: 'flex', justifyContent: 'space-between', gap: '8px',
                                                        padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
                                                    }}>
                                                        <span style={{ color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap', flex: 1 }}>{h.filer}</span>
                                                        <span style={{ color: colors.textDim }}>{fmtInt(h.shares)}</span>
                                                        <span style={{ color: colors.accent, minWidth: '60px', textAlign: 'right' }}>{fmtUSD(h.value_usd)}</span>
                                                    </div>
                                                ))}
                                            </div>
                                        </Section>
                                    )}

                                    {!isCompany && data.known_holdings?.length > 0 && (
                                        <Section title="KNOWN HOLDINGS / TARGETS">
                                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                                                {data.known_holdings.map(t => (
                                                    <button key={t} onClick={() => onNavigate?.(t)} style={{
                                                        background: `${colors.accent}15`, color: colors.accent,
                                                        border: `1px solid ${colors.accent}30`,
                                                        padding: '3px 8px', borderRadius: '3px',
                                                        fontSize: '10px', fontFamily: mono, fontWeight: 600, cursor: 'pointer',
                                                    }}>{t}</button>
                                                ))}
                                            </div>
                                        </Section>
                                    )}

                                    {Object.keys(grouped).length > 0 && (
                                        <Section title={`CONNECTIONS (${(data.connections || []).length})`}>
                                            {Object.entries(grouped).map(([type, rows]) => (
                                                <div key={type} style={{ marginBottom: '10px' }}>
                                                    <div style={{
                                                        fontSize: '9px', letterSpacing: '1px', color: colors.textMuted,
                                                        fontFamily: mono, textTransform: 'uppercase', marginBottom: '4px',
                                                    }}>{type.replace(/_/g, ' ')}</div>
                                                    {rows.slice(0, 10).map((c, i) => <ConnRow key={i} c={c} onNavigate={onNavigate} />)}
                                                </div>
                                            ))}
                                        </Section>
                                    )}

                                    {(data.connections || []).length === 0 && (data.holders_top10 || []).length === 0 && !data.description && (
                                        <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono, textAlign: 'center', padding: '16px 0' }}>
                                            No detail data available for this node.
                                        </div>
                                    )}

                                    <ExternalIntelSection actor={actor} data={data} onNavigate={onNavigate} />

                                    {/* Cooperative "tentacle" intel — users contribute facts, rumors, loyalties */}
                                    <CommunityIntelSection actorId={actor.id} />
                                </>
                            )}
                        </>
                    )}

                    {/* Supply + Capital tabs — each lazily mounts on first visit and
                        stays mounted (state cached) while the drawer is open. We
                        toggle display:none to hide inactive tabs so their fetched
                        data isn't re-requested every time the user switches tabs. */}
                    {mounted.explain && (
                        <div style={{ display: activeTab === 'explain' ? 'block' : 'none' }}>
                            <ExplainTab actorId={actor.id} />
                        </div>
                    )}
                    {mounted.supply && (
                        <div style={{ display: activeTab === 'supply' ? 'block' : 'none' }}>
                            <SupplyTab actorId={actor.id} />
                        </div>
                    )}
                    {mounted.capital && (
                        <div style={{ display: activeTab === 'capital' ? 'block' : 'none' }}>
                            <CapitalTab actorId={actor.id} />
                        </div>
                    )}
                </div>
            </aside>
        </>
    );
}
