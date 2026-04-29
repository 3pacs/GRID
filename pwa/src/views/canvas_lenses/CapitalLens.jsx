/**
 * CanvasCapitalLens — D3 Sankey of a single fiscal period's cash flow.
 *
 * Left column:   inflows (revenue, financing, asset_sales)
 * Middle column: the actor
 * Right column:  outflows (cogs, opex, capex, r&d, dividends, buybacks)
 *
 * Animated particles travel along each edge at a rate proportional to flow
 * amount. Period selector switches which fiscal period is rendered. KPI row
 * and narrative render under the sankey.
 */
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { sankey as d3Sankey, sankeyLinkHorizontal } from 'd3-sankey';
import { AlertTriangle } from 'lucide-react';
import { colors, tokens } from '../../styles/shared.js';
import { api } from '../../api.js';

const MONO = colors.mono;
const SANS = colors.sans;

// ── Colors ────────────────────────────────────────────────────
const INFLOW_COLOR  = '#10B981';
const ACTOR_COLOR   = colors.accent;
const OUTFLOW_COLORS = {
    cogs:           '#EF4444',
    opex:           '#F97316',
    capex:          '#F59E0B',
    r_and_d:        '#A3A3A3',
    rnd:            '#A3A3A3',
    research:       '#A3A3A3',
    dividends:      '#FBBF24',
    buybacks:       '#FACC15',
    taxes:          '#94A3B8',
    interest:       '#64748B',
    other:          '#71717A',
};
const pickOutflow = (type) => OUTFLOW_COLORS[(type || 'other').toLowerCase()] || OUTFLOW_COLORS.other;

function fmtUsd(v) {
    const n = Number(v);
    if (!Number.isFinite(n) || n === 0) return '—';
    const a = Math.abs(n);
    if (a >= 1e12) return `$${(n / 1e12).toFixed(2)}T`;
    if (a >= 1e9)  return `$${(n / 1e9).toFixed(2)}B`;
    if (a >= 1e6)  return `$${(n / 1e6).toFixed(2)}M`;
    if (a >= 1e3)  return `$${(n / 1e3).toFixed(1)}K`;
    return `$${n.toFixed(0)}`;
}
function fmtPct(v) {
    const n = Number(v);
    if (!Number.isFinite(n)) return '—';
    return `${(n * 100).toFixed(1)}%`;
}

// Map a 0-100 percentile to a quintile color pair (background, foreground).
// Null/undefined percentiles return the "neutral" gray pair so the badge
// still renders but visually says "no peer data".
function percentileColors(p) {
    if (p == null || !Number.isFinite(Number(p))) {
        return { bg: 'rgba(148,163,184,0.18)', fg: '#94A3B8' };
    }
    const n = Number(p);
    if (n <= 20) return { bg: 'rgba(239,68,68,0.18)',  fg: '#F87171' };  // bottom → red
    if (n <= 40) return { bg: 'rgba(249,115,22,0.18)', fg: '#FB923C' };  // low    → orange
    if (n <= 60) return { bg: 'rgba(148,163,184,0.18)', fg: '#CBD5E1' }; // middle → gray
    if (n <= 80) return { bg: 'rgba(132,204,22,0.18)', fg: '#A3E635' };  // high   → lime
    return { bg: 'rgba(16,185,129,0.22)', fg: '#34D399' };               // top    → green
}

// ── Parse LEVER / CONDITION / THESIS / INVALIDATION thesis string ────
function parseThesis(thesis) {
    if (!thesis || typeof thesis !== 'string') return null;
    const out = { lever: '', condition: '', thesis: '', invalidation: '' };
    const re = /(LEVER|CONDITION|THESIS|INVALIDATION):\s*([\s\S]*?)(?=(?:LEVER|CONDITION|THESIS|INVALIDATION):|$)/g;
    let m;
    while ((m = re.exec(thesis)) !== null) {
        const key = m[1].toLowerCase();
        const val = m[2].trim().replace(/\s+$/, '');
        if (key in out) out[key] = val;
    }
    return out;
}

// ── Styles ────────────────────────────────────────────────────
const S = {
    root: { position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', background: colors.bg, fontFamily: SANS, overflow: 'auto' },
    header: { padding: '14px 20px', borderBottom: `1px solid ${colors.border}`, background: colors.card, display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' },
    title: { fontSize: '18px', fontWeight: 700, color: colors.text, fontFamily: SANS },
    periodRow: { display: 'flex', gap: '4px', marginLeft: 'auto', flexShrink: 0 },
    periodPill: (active) => ({ padding: '6px 12px', borderRadius: tokens.radius.sm, fontSize: '11px', fontWeight: 600, fontFamily: MONO, cursor: 'pointer', border: `1px solid ${active ? colors.accent : colors.border}`, background: active ? `${colors.accent}22` : 'transparent', color: active ? colors.accentLight : colors.textDim, transition: 'all 150ms' }),
    sankeyWrap: { position: 'relative', padding: '16px 20px', minHeight: '400px', flex: '1 0 auto' },
    kpiRow: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '10px', padding: '14px 20px', borderTop: `1px solid ${colors.border}`, background: colors.card },
    kpiCard: { padding: '12px 14px', borderRadius: tokens.radius.md, background: colors.bg, border: `1px solid ${colors.borderSubtle}`, display: 'flex', flexDirection: 'column', gap: '4px' },
    kpiLabel: { fontSize: '10px', color: colors.textMuted, fontFamily: MONO, letterSpacing: '0.5px' },
    kpiValue: { fontSize: '18px', fontWeight: 700, color: colors.text, fontFamily: MONO },
    ratioStrip: { display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: '10px', padding: '10px 20px 14px', background: colors.card, borderTop: `1px solid ${colors.borderSubtle}` },
    ratioCard: { padding: '10px 12px', borderRadius: tokens.radius.md, background: colors.bg, border: `1px solid ${colors.borderSubtle}`, display: 'flex', flexDirection: 'column', gap: '6px' },
    ratioLabel: { fontSize: '9px', color: colors.textMuted, fontFamily: MONO, letterSpacing: '0.5px' },
    ratioValue: { fontSize: '15px', fontWeight: 700, color: colors.text, fontFamily: MONO },
    pctBadge: (bg, fg) => ({ display: 'inline-flex', alignItems: 'center', gap: '4px', padding: '2px 6px', borderRadius: '4px', background: bg, color: fg, fontSize: '9px', fontWeight: 700, fontFamily: MONO, letterSpacing: '0.3px', alignSelf: 'flex-start' }),
    narrative: { padding: '10px 20px 18px', fontSize: '12px', color: colors.textDim, lineHeight: 1.55, fontFamily: SANS },
    tooltip: { position: 'absolute', pointerEvents: 'none', background: 'rgba(8,12,16,0.96)', border: `1px solid ${colors.accent}`, borderRadius: tokens.radius.sm, padding: '8px 10px', fontSize: '11px', color: colors.text, fontFamily: MONO, zIndex: 50, maxWidth: '260px', boxShadow: colors.shadow.lg },
    dealPopover: { position: 'absolute', pointerEvents: 'auto', background: 'rgba(8,12,16,0.98)', border: `1px solid ${colors.accent}`, borderRadius: tokens.radius.md, padding: '10px 12px', fontSize: '11px', color: colors.text, fontFamily: MONO, zIndex: 60, maxWidth: '360px', minWidth: '280px', boxShadow: colors.shadow.lg, display: 'flex', flexDirection: 'column', gap: '6px' },
    dealPopHeader: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px', borderBottom: `1px solid ${colors.borderSubtle}`, paddingBottom: '6px', marginBottom: '2px' },
    dealPopTitle: { color: colors.accentLight, fontWeight: 700, textTransform: 'uppercase', letterSpacing: '0.5px' },
    dealPopClose: { cursor: 'pointer', color: colors.textMuted, border: 'none', background: 'transparent', fontSize: '14px', lineHeight: 1, padding: '0 4px' },
    dealRow: { display: 'flex', flexDirection: 'column', gap: '2px', padding: '4px 0', borderBottom: `1px dashed ${colors.borderSubtle}` },
    dealRowTop: { display: 'flex', justifyContent: 'space-between', gap: '8px' },
    dealTarget: { color: colors.text, fontWeight: 700 },
    dealAmount: { color: '#FBBF24', fontWeight: 700 },
    dealMeta: { color: colors.textMuted, fontSize: '10px' },
    empty: { minHeight: '300px', display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '10px', color: colors.textMuted, fontFamily: SANS, textAlign: 'center', padding: '20px' },
    // Trade ticket section styles
    ticketWrap: { padding: '14px 20px 24px', borderTop: `1px solid ${colors.border}`, background: colors.bg },
    ticketHeader: { display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '10px' },
    ticketTitle: { fontSize: '13px', fontWeight: 700, color: colors.text, fontFamily: MONO, letterSpacing: '0.5px' },
    ticketSub: { fontSize: '10px', color: colors.textMuted, fontFamily: MONO },
    ticketGrid: { display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(320px, 1fr))', gap: '10px' },
    ticketCard: (dir) => ({
        padding: '12px 14px', borderRadius: tokens.radius.md, background: colors.card,
        border: `1px solid ${dir === 'short' ? 'rgba(239,68,68,0.45)' : 'rgba(16,185,129,0.45)'}`,
        display: 'flex', flexDirection: 'column', gap: '8px',
        boxShadow: '0 1px 2px rgba(0,0,0,0.35)',
    }),
    ticketTop: { display: 'flex', alignItems: 'center', justifyContent: 'space-between', gap: '8px' },
    ticketTicker: { fontSize: '14px', fontWeight: 800, color: colors.text, fontFamily: MONO, letterSpacing: '0.5px' },
    ticketDirBadge: (dir) => ({
        padding: '2px 8px', borderRadius: '4px',
        background: dir === 'short' ? 'rgba(239,68,68,0.18)' : 'rgba(16,185,129,0.18)',
        color: dir === 'short' ? '#F87171' : '#34D399',
        fontSize: '10px', fontWeight: 700, fontFamily: MONO, letterSpacing: '0.5px',
    }),
    ticketMetaRow: { display: 'flex', flexWrap: 'wrap', gap: '10px', fontSize: '10px', color: colors.textDim, fontFamily: MONO },
    ticketMeta: { display: 'flex', gap: '4px' },
    ticketMetaK: { color: colors.textMuted },
    ticketMetaV: { color: colors.text },
    lcti: { display: 'flex', flexDirection: 'column', gap: '4px' },
    lctiRow: { fontSize: '10px', color: colors.textDim, fontFamily: SANS, lineHeight: 1.4 },
    lctiLabel: { fontWeight: 700, color: colors.accentLight, fontFamily: MONO, marginRight: '4px' },
    kellyStrip: { display: 'flex', gap: '4px', alignItems: 'center', marginTop: '4px' },
    kellyBar: (pct) => ({
        height: '6px', width: '100%', background: 'rgba(148,163,184,0.15)',
        borderRadius: '3px', overflow: 'hidden', position: 'relative',
    }),
    kellyFill: (pct) => ({
        position: 'absolute', left: 0, top: 0, bottom: 0,
        width: `${Math.min(100, pct * 100 / 0.05).toFixed(0)}%`,
        background: pct >= 0.04 ? '#34D399' : (pct >= 0.02 ? '#FBBF24' : '#94A3B8'),
    }),
    kellyLabel: { fontSize: '10px', color: colors.textMuted, fontFamily: MONO, whiteSpace: 'nowrap' },
};

// ── Sankey builder for a single period ────────────────────────
function buildSankey(period, actorLabel, width, height) {
    if (!period) return null;
    const inflows = Array.isArray(period.inflows) ? period.inflows : [];
    const outflows = Array.isArray(period.outflows) ? period.outflows : [];
    if (inflows.length === 0 && outflows.length === 0) return null;

    const ACTOR_KEY = '__actor__';
    const nodes = [];
    const nodeIdx = new Map();
    const pushNode = (key, obj) => {
        if (nodeIdx.has(key)) return nodeIdx.get(key);
        nodeIdx.set(key, nodes.length);
        nodes.push({ key, ...obj });
        return nodes.length - 1;
    };

    inflows.forEach((f) => pushNode(`in:${f.flow_type}`, {
        side: 'in', label: f.flow_type, color: INFLOW_COLOR, raw: f,
    }));
    pushNode(ACTOR_KEY, { side: 'actor', label: actorLabel || 'Actor', color: ACTOR_COLOR });
    outflows.forEach((f) => pushNode(`out:${f.flow_type}`, {
        side: 'out', label: f.flow_type, color: pickOutflow(f.flow_type), raw: f,
    }));

    const links = [];
    inflows.forEach((f) => {
        const amt = Math.max(0, Number(f.amount_usd) || 0);
        if (amt <= 0) return;
        links.push({ source: nodeIdx.get(`in:${f.flow_type}`), target: nodeIdx.get(ACTOR_KEY), value: amt, raw: f, side: 'in' });
    });
    outflows.forEach((f) => {
        const amt = Math.max(0, Number(f.amount_usd) || 0);
        if (amt <= 0) return;
        links.push({ source: nodeIdx.get(ACTOR_KEY), target: nodeIdx.get(`out:${f.flow_type}`), value: amt, raw: f, side: 'out' });
    });
    if (links.length === 0) return null;

    const layout = d3Sankey()
        .nodeWidth(18)
        .nodePadding(14)
        .extent([[10, 10], [Math.max(width - 10, 200), Math.max(height - 10, 200)]]);
    try {
        const res = layout({ nodes: nodes.map((n) => ({ ...n })), links: links.map((l) => ({ ...l })) });
        return res;
    } catch (e) {
        return null;
    }
}

// ── Component ─────────────────────────────────────────────────
// ── Ticket card: LEVER/CONDITION/THESIS/INVALIDATION trade ticket ────
function TicketCard({ ticket }) {
    const parsed = parseThesis(ticket.thesis) || {};
    const dir = ticket.direction || 'short';
    const kelly = Number(ticket.kelly_size) || 0;
    const conf = Number(ticket.confidence) || 0;
    const instrument = (ticket.instrument || '').toUpperCase();
    return (
        <div style={S.ticketCard(dir)}>
            <div style={S.ticketTop}>
                <div style={S.ticketTicker}>
                    {String(ticket.ticker || '').toUpperCase()} {instrument} ${ticket.strike}
                </div>
                <div style={S.ticketDirBadge(dir)}>{dir.toUpperCase()}</div>
            </div>
            <div style={S.ticketMetaRow}>
                <div style={S.ticketMeta}>
                    <span style={S.ticketMetaK}>EXP</span>
                    <span style={S.ticketMetaV}>{ticket.expiry}</span>
                </div>
                <div style={S.ticketMeta}>
                    <span style={S.ticketMetaK}>ENTRY</span>
                    <span style={S.ticketMetaV}>${Number(ticket.entry_premium).toFixed(2)}</span>
                </div>
                <div style={S.ticketMeta}>
                    <span style={S.ticketMetaK}>TGT</span>
                    <span style={S.ticketMetaV}>${Number(ticket.target_premium).toFixed(2)}</span>
                </div>
                <div style={S.ticketMeta}>
                    <span style={S.ticketMetaK}>STOP</span>
                    <span style={S.ticketMetaV}>${Number(ticket.stop_premium).toFixed(2)}</span>
                </div>
                <div style={S.ticketMeta}>
                    <span style={S.ticketMetaK}>INV</span>
                    <span style={S.ticketMetaV}>${Number(ticket.invalidation_price).toFixed(2)}</span>
                </div>
            </div>
            <div style={S.lcti}>
                {parsed.lever && (
                    <div style={S.lctiRow}><span style={S.lctiLabel}>LEVER</span>{parsed.lever}</div>
                )}
                {parsed.condition && (
                    <div style={S.lctiRow}><span style={S.lctiLabel}>CONDITION</span>{parsed.condition}</div>
                )}
                {parsed.thesis && (
                    <div style={S.lctiRow}><span style={S.lctiLabel}>THESIS</span>{parsed.thesis}</div>
                )}
                {parsed.invalidation && (
                    <div style={S.lctiRow}><span style={S.lctiLabel}>INVALIDATION</span>{parsed.invalidation}</div>
                )}
            </div>
            <div style={S.kellyStrip}>
                <div style={S.kellyBar(kelly)}>
                    <div style={S.kellyFill(kelly)} />
                </div>
                <div style={S.kellyLabel}>
                    Kelly {(kelly * 100).toFixed(1)}% · conf {(conf * 100).toFixed(0)}%
                </div>
            </div>
        </div>
    );
}

export default function CanvasCapitalLens({ actor }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [periodIdx, setPeriodIdx] = useState(0);
    const [size, setSize] = useState({ w: 800, h: 420 });
    const [hover, setHover] = useState(null);
    const [pinnedDeals, setPinnedDeals] = useState(null);
    const [tickets, setTickets] = useState([]);
    const [ticketsLoading, setTicketsLoading] = useState(false);
    const wrapRef = useRef(null);
    const canvasRef = useRef(null);
    const particlesRef = useRef([]);
    const rafRef = useRef(0);

    const focalId = actor?.id;

    // ── Fetch generated trade tickets (contagion → dealer gamma → ticket) ──
    useEffect(() => {
        let cancelled = false;
        setTicketsLoading(true);
        api.getRecentTradeTickets(168)
            .then((d) => {
                if (cancelled) return;
                const list = Array.isArray(d?.tickets) ? d.tickets : [];
                setTickets(list);
            })
            .catch(() => { if (!cancelled) setTickets([]); })
            .finally(() => { if (!cancelled) setTicketsLoading(false); });
        return () => { cancelled = true; };
    }, [focalId]);

    // ── Fetch ──
    useEffect(() => {
        if (!focalId) return;
        let cancelled = false;
        setLoading(true);
        setError(null);
        api.getActorCapitalFlow(focalId, 4, 'annual')
            .then((d) => {
                if (cancelled) return;
                if (d && !d.error) {
                    setData(d);
                    const len = Array.isArray(d.periods) ? d.periods.length : 0;
                    setPeriodIdx(Math.max(0, len - 1));
                } else {
                    setError(d?.error || 'Failed to load capital flow');
                }
            })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Network error'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [focalId]);

    // ── Resize observer ──
    useEffect(() => {
        if (!wrapRef.current) return;
        const el = wrapRef.current;
        const ro = new ResizeObserver(() => {
            const r = el.getBoundingClientRect();
            if (r.width > 0) setSize({ w: r.width - 40, h: Math.max(360, r.height - 32) });
        });
        ro.observe(el);
        const r0 = el.getBoundingClientRect();
        if (r0.width > 0) setSize({ w: r0.width - 40, h: Math.max(360, r0.height - 32) });
        return () => ro.disconnect();
    }, []);

    const periods = Array.isArray(data?.periods) ? data.periods : [];
    const period = periods[periodIdx] || null;
    const actorLabel = data?.actor?.label || actor?.label || focalId || 'Actor';

    const sankey = useMemo(
        () => buildSankey(period, actorLabel, size.w, size.h),
        [period, actorLabel, size.w, size.h],
    );

    // ── Particles — shared RAF loop ──
    useEffect(() => {
        const cvs = canvasRef.current;
        if (!cvs || !sankey?.links?.length) {
            particlesRef.current = [];
            cancelAnimationFrame(rafRef.current);
            return;
        }
        const dpr = window.devicePixelRatio || 1;
        cvs.width = size.w * dpr;
        cvs.height = size.h * dpr;
        cvs.style.width = `${size.w}px`;
        cvs.style.height = `${size.h}px`;
        const ctx = cvs.getContext('2d');
        ctx.scale(dpr, dpr);

        // Build particles: proportional to value share, ~30 total
        const links = sankey.links;
        const totalValue = links.reduce((a, l) => a + (l.value || 0), 0) || 1;
        const targetTotal = 30;
        const particles = [];
        links.forEach((l, li) => {
            const share = (l.value || 0) / totalValue;
            const count = Math.max(1, Math.round(share * targetTotal));
            for (let i = 0; i < count; i++) {
                particles.push({
                    linkIdx: li,
                    t: Math.random(),
                    speed: 0.0015 + share * 0.004,
                    color: l.side === 'in' ? INFLOW_COLOR : pickOutflow(l.raw?.flow_type),
                });
            }
        });
        particlesRef.current = particles;

        let running = true;
        const tick = () => {
            if (!running) return;
            if (document.hidden) {
                rafRef.current = requestAnimationFrame(tick);
                return;
            }
            ctx.clearRect(0, 0, size.w, size.h);
            const svgPathEls = svgPathCacheRef.current;
            for (const p of particlesRef.current) {
                p.t += p.speed;
                if (p.t > 1) p.t -= 1;
                const pathEl = svgPathEls[p.linkIdx];
                if (!pathEl) continue;
                const len = pathEl.getTotalLength?.() || 0;
                if (!len) continue;
                const pt = pathEl.getPointAtLength(len * p.t);
                ctx.beginPath();
                ctx.arc(pt.x, pt.y, 2.2, 0, Math.PI * 2);
                ctx.fillStyle = p.color;
                ctx.globalAlpha = 0.85;
                ctx.fill();
            }
            ctx.globalAlpha = 1;
            rafRef.current = requestAnimationFrame(tick);
        };
        rafRef.current = requestAnimationFrame(tick);
        return () => { running = false; cancelAnimationFrame(rafRef.current); };
    }, [sankey, size.w, size.h]);

    // Cache svg path refs to measure lengths via getPointAtLength.
    const svgPathCacheRef = useRef([]);
    // Reset cache whenever the sankey topology changes — stale refs are useless.
    useEffect(() => { svgPathCacheRef.current = []; }, [sankey]);
    const registerPath = (i) => (el) => {
        if (el) svgPathCacheRef.current[i] = el;
    };

    const summary = data?.summary || {};
    const isFallback = data?.provenance?.source === 'fallback' || !period;

    const showTooltip = (evt, content) => {
        const r = wrapRef.current?.getBoundingClientRect();
        if (!r) return;
        setHover({ x: evt.clientX - r.left + 12, y: evt.clientY - r.top + 12, content });
    };

    return (
        <div style={S.root}>
            <div style={S.header}>
                <div style={S.title}>Capital Flow · {actorLabel}</div>
                <div style={S.periodRow}>
                    {periods.map((p, i) => (
                        <button key={`p-${i}`}
                            style={S.periodPill(i === periodIdx)}
                            onClick={() => { setPeriodIdx(i); setPinnedDeals(null); }}>
                            {p.label || p.fiscal_period || `P${i + 1}`}
                        </button>
                    ))}
                </div>
            </div>

            <div style={S.sankeyWrap} ref={wrapRef}>
                {loading && <div style={S.empty}><div>Loading capital flow…</div></div>}
                {!loading && (isFallback || error || !sankey) && (
                    <div style={S.empty}>
                        <AlertTriangle size={28} color={colors.yellow} />
                        <div style={{ fontSize: '14px', color: colors.text, fontWeight: 600 }}>
                            Capital flow data pending
                        </div>
                        <div style={{ fontSize: '12px', color: colors.textMuted, maxWidth: '420px' }}>
                            {error || data?.narrative || 'No flows returned for this period.'}
                        </div>
                    </div>
                )}

                {!loading && sankey && (
                    <>
                        <svg width={size.w} height={size.h} style={{ display: 'block' }}>
                            <defs>
                                {sankey.links.map((l, i) => (
                                    <linearGradient key={`lg-${i}`} id={`lg-${i}`}
                                        gradientUnits="userSpaceOnUse"
                                        x1={l.source.x1} x2={l.target.x0}>
                                        <stop offset="0%" stopColor={l.source.color || INFLOW_COLOR} stopOpacity="0.5" />
                                        <stop offset="100%" stopColor={l.target.color || ACTOR_COLOR} stopOpacity="0.5" />
                                    </linearGradient>
                                ))}
                            </defs>
                            {sankey.links.map((l, i) => {
                                const deals = Array.isArray(l.raw?.deals) ? l.raw.deals : [];
                                const hasDeals = deals.length > 0;
                                return (
                                    <path key={`ln-${i}`}
                                        ref={registerPath(i)}
                                        d={sankeyLinkHorizontal()(l) || ''}
                                        fill="none"
                                        stroke={`url(#lg-${i})`}
                                        strokeWidth={Math.max(1, l.width || 1)}
                                        strokeOpacity={0.6}
                                        style={{ cursor: hasDeals ? 'zoom-in' : 'pointer' }}
                                        onMouseMove={(ev) => showTooltip(ev, (
                                            <>
                                                <div style={{ color: colors.accentLight, fontWeight: 700 }}>{l.raw?.flow_type || 'flow'}</div>
                                                <div>{fmtUsd(l.value)}</div>
                                                {l.raw?.confidence != null && (
                                                    <div style={{ color: colors.textMuted }}>conf {Number(l.raw.confidence).toFixed(2)}</div>
                                                )}
                                                {l.raw?.source_filing && (
                                                    <div style={{ color: colors.textMuted }}>{l.raw.source_filing}</div>
                                                )}
                                                {hasDeals && (
                                                    <div style={{ color: colors.accentLight, marginTop: 4 }}>
                                                        click to view {deals.length} deal{deals.length === 1 ? '' : 's'}
                                                    </div>
                                                )}
                                            </>
                                        ))}
                                        onMouseLeave={() => setHover(null)}
                                        onClick={(ev) => {
                                            if (!hasDeals) return;
                                            ev.stopPropagation();
                                            const r = wrapRef.current?.getBoundingClientRect();
                                            if (!r) return;
                                            setPinnedDeals({
                                                flow_type: l.raw?.flow_type || 'flow',
                                                total: l.value,
                                                deals,
                                                x: ev.clientX - r.left + 12,
                                                y: ev.clientY - r.top + 12,
                                            });
                                        }} />
                                );
                            })}
                            {sankey.nodes.map((n, i) => (
                                <g key={`nd-${i}`}
                                    onMouseMove={(ev) => showTooltip(ev, (
                                        <>
                                            <div style={{ color: n.color, fontWeight: 700 }}>{n.label}</div>
                                            <div>{n.side}</div>
                                            {n.raw?.amount_usd != null && <div>{fmtUsd(n.raw.amount_usd)}</div>}
                                            {n.raw?.confidence != null && (
                                                <div style={{ color: colors.textMuted }}>conf {Number(n.raw.confidence).toFixed(2)}</div>
                                            )}
                                        </>
                                    ))}
                                    onMouseLeave={() => setHover(null)}>
                                    <rect x={n.x0} y={n.y0}
                                        width={Math.max(2, (n.x1 || 0) - (n.x0 || 0))}
                                        height={Math.max(2, (n.y1 || 0) - (n.y0 || 0))}
                                        fill={n.color} fillOpacity={0.9}
                                        stroke={colors.border} strokeWidth={0.5} rx={2} />
                                    <text x={n.side === 'out' ? n.x0 - 6 : n.x1 + 6}
                                        y={(n.y0 + n.y1) / 2}
                                        dy="0.35em"
                                        textAnchor={n.side === 'out' ? 'end' : 'start'}
                                        fontSize="10" fontFamily={MONO}
                                        fill={colors.textDim}>
                                        {n.label}
                                    </text>
                                </g>
                            ))}
                        </svg>
                        <canvas ref={canvasRef}
                            style={{ position: 'absolute', left: 20, top: 16, pointerEvents: 'none' }} />
                        {hover && !pinnedDeals && (
                            <div style={{ ...S.tooltip, left: hover.x, top: hover.y }}>
                                {hover.content}
                            </div>
                        )}
                        {pinnedDeals && (
                            <div style={{ ...S.dealPopover, left: pinnedDeals.x, top: pinnedDeals.y }}
                                onClick={(ev) => ev.stopPropagation()}>
                                <div style={S.dealPopHeader}>
                                    <div style={S.dealPopTitle}>
                                        {pinnedDeals.flow_type} · {fmtUsd(pinnedDeals.total)}
                                    </div>
                                    <button style={S.dealPopClose}
                                        onClick={() => setPinnedDeals(null)}>×</button>
                                </div>
                                <div style={{ color: colors.textMuted, fontSize: '10px' }}>
                                    {pinnedDeals.deals.length} deal{pinnedDeals.deals.length === 1 ? '' : 's'}
                                </div>
                                {pinnedDeals.deals.map((d, i) => (
                                    <div key={`deal-${i}`} style={S.dealRow}>
                                        <div style={S.dealRowTop}>
                                            <span style={S.dealTarget}>
                                                {d.target_label || d.target || 'unknown target'}
                                            </span>
                                            <span style={S.dealAmount}>{fmtUsd(d.amount_usd)}</span>
                                        </div>
                                        <div style={S.dealMeta}>
                                            {d.announcement_date || '—'}
                                            {d.source_filing ? ` · ${d.source_filing}` : ''}
                                            {d.confidence ? ` · ${d.confidence}` : ''}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </>
                )}
            </div>

            {/* KPI row */}
            <div style={S.kpiRow}>
                <div style={S.kpiCard}>
                    <div style={S.kpiLabel}>LATEST REVENUE</div>
                    <div style={S.kpiValue}>{fmtUsd(summary.latest_revenue_usd)}</div>
                </div>
                <div style={S.kpiCard}>
                    <div style={S.kpiLabel}>3Y REV CAGR</div>
                    <div style={S.kpiValue}>{fmtPct(summary.revenue_3y_cagr)}</div>
                </div>
                <div style={S.kpiCard}>
                    <div style={S.kpiLabel}>3Y CAPEX INTENSITY</div>
                    <div style={S.kpiValue}>{fmtPct(summary.capex_3y_avg_intensity)}</div>
                </div>
                <div style={S.kpiCard}>
                    <div style={S.kpiLabel}>3Y SHAREHOLDER RETURN</div>
                    <div style={S.kpiValue}>{fmtUsd(summary.shareholder_return_3y_total_usd)}</div>
                </div>
            </div>

            {/* Ratio strip — per-period ratios with per-sector percentile badges. */}
            {period?.ratios && (() => {
                const r = period.ratios;
                const pcts = r._percentiles || {};
                const sectorName = data?.actor?.sector || '';
                const ratioDefs = [
                    { key: 'gross_margin',     label: 'GROSS MARGIN',     fmt: fmtPct },
                    { key: 'opex_intensity',   label: 'OPEX INTENSITY',   fmt: fmtPct },
                    { key: 'capex_intensity',  label: 'CAPEX INTENSITY',  fmt: fmtPct },
                    { key: 'fcf_conversion',   label: 'FCF CONVERSION',   fmt: fmtPct },
                    { key: 'shareholder_yield',label: 'SHAREHOLDER YIELD',fmt: fmtPct },
                    { key: 'reinvestment_ratio',label: 'REINVESTMENT',    fmt: fmtPct },
                ];
                return (
                    <div style={S.ratioStrip}>
                        {ratioDefs.map(({ key, label, fmt }) => {
                            const val = r[key];
                            const pct = pcts[key];
                            const { bg, fg } = percentileColors(pct);
                            return (
                                <div key={key} style={S.ratioCard}>
                                    <div style={S.ratioLabel}>{label}</div>
                                    <div style={S.ratioValue}>{fmt(val)}</div>
                                    <div style={S.pctBadge(bg, fg)}>
                                        {pct == null ? 'NO PEERS' : `P${Math.round(Number(pct))}${sectorName ? ` · ${sectorName}` : ''}`}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                );
            })()}

            {data?.narrative && <div style={S.narrative}>{data.narrative}</div>}

            {/* Generated trade tickets — contagion → dealer gamma → options */}
            {(ticketsLoading || tickets.length > 0) && (() => {
                const focalTicker = String(focalId || '').toLowerCase();
                const matched = tickets.filter((t) => t.ticker?.toLowerCase() === focalTicker);
                const shown = matched.length > 0 ? matched : tickets.slice(0, 6);
                return (
                    <div style={S.ticketWrap}>
                        <div style={S.ticketHeader}>
                            <div style={S.ticketTitle}>GENERATED TICKETS</div>
                            <div style={S.ticketSub}>
                                {ticketsLoading
                                    ? 'loading…'
                                    : `${shown.length} of ${tickets.length} · last 7d · contagion-derived`}
                            </div>
                        </div>
                        {!ticketsLoading && shown.length === 0 && (
                            <div style={{ ...S.ticketSub, padding: '8px 0' }}>
                                No tickets yet — waiting for contagion predictions with options data.
                            </div>
                        )}
                        <div style={S.ticketGrid}>
                            {shown.map((t, i) => (
                                <TicketCard key={`tk-${t.prediction_id}-${t.ticker}-${i}`} ticket={t} />
                            ))}
                        </div>
                    </div>
                );
            })()}
        </div>
    );
}
