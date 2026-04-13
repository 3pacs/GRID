/**
 * CanvasSupplyLens — hierarchical supply chain view for a focal actor.
 *
 * Layout: Left-to-right columns by tier.
 *   upstream (tier < 0)  →  focal (tier = 0)  →  downstream (tier > 0)
 *
 * Nodes are rectangles colored by type. Edges are curved SVG paths, stroke
 * width proportional to log(annual_usd), color by relationship, dashed for
 * low-confidence. Hover tooltips, click-to-refocus, chokepoints pulse red.
 */
import React, { useEffect, useMemo, useRef, useState } from 'react';
import { AlertTriangle, ArrowLeft, ArrowRight, Zap, X } from 'lucide-react';
import { colors, tokens, shared } from '../../styles/shared.js';
import { api } from '../../api.js';

const MONO = colors.mono;
const SANS = colors.sans;

// ── Type / relationship palette ────────────────────────────────
const TYPE_FILL = {
    commodity: '#8B5A2B',
    private:   '#445',
    ticker:    colors.accent,
    country:   '#4A5568',
    actor:     colors.accent,
};
const REL_COLOR = {
    raw_material:  '#A0522D',
    component:     '#3B82F6',
    contract_mfg:  '#8B5CF6',
    distribution:  '#10B981',
    customer:      colors.accent,
    service:       '#F59E0B',
    other:         '#5A7080',
};

// Currency formatter — short notation
function fmtUsd(v) {
    const n = Number(v);
    if (!Number.isFinite(n) || n === 0) return '—';
    const a = Math.abs(n);
    if (a >= 1e12) return `$${(n / 1e12).toFixed(1)}T`;
    if (a >= 1e9)  return `$${(n / 1e9).toFixed(1)}B`;
    if (a >= 1e6)  return `$${(n / 1e6).toFixed(1)}M`;
    if (a >= 1e3)  return `$${(n / 1e3).toFixed(1)}K`;
    return `$${n.toFixed(0)}`;
}

// ── Styles ────────────────────────────────────────────────────
const S = {
    root: { position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', background: colors.bg, fontFamily: SANS },
    header: { padding: '14px 20px', borderBottom: `1px solid ${colors.border}`, background: colors.card, display: 'flex', alignItems: 'center', gap: '16px', flexWrap: 'wrap' },
    title: { fontSize: '18px', fontWeight: 700, color: colors.text, fontFamily: SANS },
    narrative: { flex: '1 1 340px', fontSize: '12px', color: colors.textDim, fontFamily: SANS, lineHeight: 1.5 },
    kpiRow: { display: 'flex', gap: '8px', flexShrink: 0 },
    kpi: (accent) => ({ padding: '6px 12px', borderRadius: tokens.radius.sm, background: colors.bg, border: `1px solid ${accent || colors.borderSubtle}`, fontFamily: MONO, display: 'flex', flexDirection: 'column', gap: '2px', minWidth: '92px' }),
    kpiLabel: { fontSize: '9px', color: colors.textMuted, letterSpacing: '0.5px' },
    kpiValue: (c) => ({ fontSize: '14px', fontWeight: 700, color: c || colors.text }),
    body: { flex: 1, display: 'flex', minHeight: 0 },
    graphWrap: { flex: 1, position: 'relative', overflow: 'hidden' },
    sidePanel: { width: '280px', flexShrink: 0, borderLeft: `1px solid ${colors.border}`, background: colors.card, padding: '14px', overflowY: 'auto' },
    panelHead: { fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', color: colors.accent, fontFamily: MONO, marginBottom: '10px' },
    chokeCard: { background: colors.bg, border: `1px solid ${colors.red}33`, borderLeft: `3px solid ${colors.red}`, borderRadius: tokens.radius.sm, padding: '8px 10px', marginBottom: '8px', fontSize: '11px', color: colors.textDim },
    chokeLabel: { color: colors.text, fontWeight: 600, marginBottom: '2px', fontSize: '12px' },
    scoreBar: { height: '4px', background: colors.border, borderRadius: '2px', margin: '6px 0', overflow: 'hidden' },
    scoreFill: (v) => ({ height: '100%', width: `${Math.max(0, Math.min(1, v)) * 100}%`, background: colors.red }),
    tooltip: { position: 'absolute', pointerEvents: 'none', background: 'rgba(8,12,16,0.96)', border: `1px solid ${colors.accent}`, borderRadius: tokens.radius.sm, padding: '8px 10px', fontSize: '11px', color: colors.text, fontFamily: MONO, zIndex: 50, maxWidth: '260px', boxShadow: colors.shadow.lg },
    empty: { position: 'absolute', inset: 0, display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', gap: '10px', color: colors.textMuted, fontFamily: SANS, textAlign: 'center', padding: '20px' },
};

// ── Component ─────────────────────────────────────────────────
export default function CanvasSupplyLens({ actor, onFocus }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [size, setSize] = useState({ w: 800, h: 500 });
    const [hover, setHover] = useState(null); // {x,y,content}
    const [focalOverride, setFocalOverride] = useState(null);
    const containerRef = useRef(null);

    const focalId = focalOverride || actor?.id;

    // ── Fetch on focal change ──
    useEffect(() => {
        if (!focalId) return;
        let cancelled = false;
        setLoading(true);
        setError(null);
        api.getActorSupplyChain(focalId, 'both', 3)
            .then((d) => {
                if (cancelled) return;
                if (d && !d.error) setData(d);
                else setError(d?.error || 'Failed to load supply chain');
            })
            .catch((e) => { if (!cancelled) setError(e?.message || 'Network error'); })
            .finally(() => { if (!cancelled) setLoading(false); });
        return () => { cancelled = true; };
    }, [focalId]);

    // ── ResizeObserver ──
    useEffect(() => {
        if (!containerRef.current) return;
        const el = containerRef.current;
        const ro = new ResizeObserver(() => {
            const r = el.getBoundingClientRect();
            if (r.width > 0 && r.height > 0) setSize({ w: r.width, h: r.height });
        });
        ro.observe(el);
        const r0 = el.getBoundingClientRect();
        if (r0.width > 0) setSize({ w: r0.width, h: r0.height });
        return () => ro.disconnect();
    }, []);

    // ── Layout: columns per tier ──
    const layout = useMemo(() => {
        if (!data?.nodes?.length) return null;
        const nodes = data.nodes || [];
        const edges = data.edges || [];

        // Group by tier
        const byTier = new Map();
        for (const n of nodes) {
            const t = n.tier ?? 0;
            if (!byTier.has(t)) byTier.set(t, []);
            byTier.get(t).push(n);
        }
        const tiers = [...byTier.keys()].sort((a, b) => a - b);
        if (tiers.length === 0) return null;

        const padX = 60, padY = 40;
        const W = Math.max(600, size.w - padX * 2);
        const H = Math.max(360, size.h - padY * 2);
        const colW = W / Math.max(tiers.length, 1);
        const nodeW = Math.min(160, colW - 30);
        const nodeH = 34;

        // Position nodes
        const pos = new Map();
        tiers.forEach((t, ti) => {
            const col = byTier.get(t) || [];
            const cx = padX + ti * colW + colW / 2;
            const totalH = col.length * (nodeH + 10) - 10;
            const startY = padY + Math.max(0, (H - totalH) / 2);
            col.forEach((n, ni) => {
                pos.set(n.id, {
                    x: cx - nodeW / 2,
                    y: startY + ni * (nodeH + 10),
                    w: nodeW, h: nodeH,
                    node: n,
                });
            });
        });

        // Edges with log-scale width
        const amounts = edges.map((e) => Number(e.annual_usd) || 0).filter((a) => a > 0);
        const maxLog = amounts.length ? Math.log10(Math.max(...amounts) + 1) : 1;
        const laidEdges = edges
            .map((e) => {
                const s = pos.get(e.source);
                const t = pos.get(e.target);
                if (!s || !t) return null;
                const amt = Number(e.annual_usd) || 0;
                const width = amt > 0
                    ? 1 + (Math.log10(amt + 1) / maxLog) * 6
                    : 1;
                return { e, s, t, width };
            })
            .filter(Boolean);

        return { tiers, byTier, pos, laidEdges, padX, padY, nodeW, nodeH };
    }, [data, size]);

    const summary = data?.summary || {};
    const chokepoints = (data?.chokepoints || []).slice(0, 5);
    const isFallback = data?.provenance?.source === 'fallback' || (data && (data.nodes?.length ?? 0) === 0);

    const showTooltip = (evt, content) => {
        const rect = containerRef.current?.getBoundingClientRect();
        if (!rect) return;
        setHover({ x: evt.clientX - rect.left + 12, y: evt.clientY - rect.top + 12, content });
    };

    const onNodeClick = (n) => {
        if (!n?.id || n.id === focalId) return;
        setFocalOverride(n.id);
        onFocus?.(n.id);
    };

    // ── Render ──
    return (
        <div style={S.root}>
            {/* Header */}
            <div style={S.header}>
                <div style={S.title}>
                    Supply Chain · {data?.actor?.label || actor?.label || focalId || 'Select an actor'}
                </div>
                {data?.narrative && (
                    <div style={S.narrative}>{data.narrative}</div>
                )}
                <div style={S.kpiRow}>
                    <div style={S.kpi(colors.green)}>
                        <span style={S.kpiLabel}>UPSTREAM ({summary.upstream_count || 0})</span>
                        <span style={S.kpiValue(colors.green)}>{fmtUsd(summary.upstream_annual_usd_total)}</span>
                    </div>
                    <div style={S.kpi(colors.accent)}>
                        <span style={S.kpiLabel}>DOWNSTREAM ({summary.downstream_count || 0})</span>
                        <span style={S.kpiValue(colors.accent)}>{fmtUsd(summary.downstream_annual_usd_total)}</span>
                    </div>
                    <div style={S.kpi(colors.red)}>
                        <span style={S.kpiLabel}>CHOKEPOINTS</span>
                        <span style={S.kpiValue(colors.red)}>{chokepoints.length}</span>
                    </div>
                </div>
            </div>

            {/* Body */}
            <div style={S.body}>
                <div style={S.graphWrap} ref={containerRef}>
                    {loading && (
                        <div style={S.empty}><div>Loading supply chain…</div></div>
                    )}
                    {!loading && (isFallback || error) && (
                        <div style={S.empty}>
                            <AlertTriangle size={28} color={colors.yellow} />
                            <div style={{ fontSize: '14px', color: colors.text, fontWeight: 600 }}>
                                Supply chain data pending
                            </div>
                            <div style={{ fontSize: '12px', color: colors.textMuted, maxWidth: '420px' }}>
                                {error || data?.narrative || 'No edges returned — endpoint is in fallback mode.'}
                            </div>
                        </div>
                    )}
                    {!loading && !isFallback && !error && layout && (
                        <svg width="100%" height="100%" viewBox={`0 0 ${size.w} ${size.h}`}>
                            <defs>
                                <filter id="chokeGlow" x="-50%" y="-50%" width="200%" height="200%">
                                    <feGaussianBlur stdDeviation="3" result="blur" />
                                    <feMerge>
                                        <feMergeNode in="blur" />
                                        <feMergeNode in="SourceGraphic" />
                                    </feMerge>
                                </filter>
                            </defs>

                            {/* Tier labels */}
                            {layout.tiers.map((t) => {
                                const first = layout.byTier.get(t)?.[0];
                                const p = first ? layout.pos.get(first.id) : null;
                                if (!p) return null;
                                const label = t < 0 ? `Upstream T${t}` : t === 0 ? 'Focal' : `Downstream T+${t}`;
                                return (
                                    <text key={`tl-${t}`}
                                        x={p.x + p.w / 2} y={20}
                                        textAnchor="middle"
                                        fontSize="10" fontFamily={MONO}
                                        fill={colors.textMuted} letterSpacing="1">
                                        {label.toUpperCase()}
                                    </text>
                                );
                            })}

                            {/* Edges */}
                            {layout.laidEdges.map((le, i) => {
                                const { e, s, t, width } = le;
                                const sx = s.x + s.w, sy = s.y + s.h / 2;
                                const tx = t.x,      ty = t.y + t.h / 2;
                                const mx = (sx + tx) / 2;
                                const path = `M ${sx},${sy} C ${mx},${sy} ${mx},${ty} ${tx},${ty}`;
                                const rel = e.relationship || 'other';
                                const stroke = REL_COLOR[rel] || REL_COLOR.other;
                                const lowConf = (Number(e.confidence) || 1) < 0.5;
                                return (
                                    <path key={`e-${i}`}
                                        d={path} fill="none"
                                        stroke={stroke}
                                        strokeWidth={width}
                                        strokeOpacity={0.55}
                                        strokeDasharray={lowConf ? '4 4' : undefined}
                                        style={{ cursor: 'pointer' }}
                                        onMouseMove={(ev) => showTooltip(ev, (
                                            <>
                                                <div style={{ color: stroke, fontWeight: 700 }}>{rel}</div>
                                                <div>{e.input_type || ''}</div>
                                                <div>{fmtUsd(e.annual_usd)} /yr</div>
                                                {e.pct_upstream_revenue != null && (
                                                    <div>upstream rev: {(e.pct_upstream_revenue * 100).toFixed(1)}%</div>
                                                )}
                                                {e.pct_downstream_cogs != null && (
                                                    <div>downstream cogs: {(e.pct_downstream_cogs * 100).toFixed(1)}%</div>
                                                )}
                                                <div style={{ color: colors.textMuted }}>conf {(Number(e.confidence) || 0).toFixed(2)}</div>
                                            </>
                                        ))}
                                        onMouseLeave={() => setHover(null)}
                                    />
                                );
                            })}

                            {/* Nodes */}
                            {[...layout.pos.values()].map(({ x, y, w, h, node: n }) => {
                                const fill = TYPE_FILL[n.type] || TYPE_FILL.actor;
                                const isFocal = n.tier === 0 || n.id === focalId;
                                const isChoke = (n.chokepoint || 0) > 0 || (Number(n.chokepoint_score) || 0) > 0.5;
                                return (
                                    <g key={`n-${n.id}`}
                                        style={{ cursor: 'pointer' }}
                                        onMouseMove={(ev) => showTooltip(ev, (
                                            <>
                                                <div style={{ color: colors.accentLight, fontWeight: 700 }}>{n.label || n.id}</div>
                                                <div>{n.type || 'actor'}{n.country ? ` · ${n.country}` : ''}</div>
                                                {isChoke && (
                                                    <div style={{ color: colors.red }}>CHOKEPOINT score {(Number(n.chokepoint_score) || 0).toFixed(2)}</div>
                                                )}
                                                <div style={{ color: colors.textMuted }}>tier {n.tier ?? 0}</div>
                                            </>
                                        ))}
                                        onMouseLeave={() => setHover(null)}
                                        onClick={() => onNodeClick(n)}>
                                        <rect
                                            x={x} y={y} width={w} height={h}
                                            rx="4"
                                            fill={fill}
                                            fillOpacity={isFocal ? 0.95 : 0.75}
                                            stroke={isChoke ? colors.red : (isFocal ? colors.accentLight : colors.border)}
                                            strokeWidth={isChoke ? 2 : (isFocal ? 2 : 1)}
                                            filter={isChoke ? 'url(#chokeGlow)' : undefined}
                                        >
                                            {isChoke && (
                                                <animate attributeName="stroke-opacity"
                                                    values="0.4;1;0.4" dur="1.8s" repeatCount="indefinite" />
                                            )}
                                        </rect>
                                        <text
                                            x={x + w / 2} y={y + h / 2 + 4}
                                            textAnchor="middle"
                                            fontSize="11" fontWeight="600"
                                            fontFamily={MONO}
                                            fill={colors.text}
                                            style={{ pointerEvents: 'none' }}>
                                            {(n.label || n.id || '').slice(0, 18)}
                                        </text>
                                    </g>
                                );
                            })}
                        </svg>
                    )}

                    {hover && (
                        <div style={{ ...S.tooltip, left: hover.x, top: hover.y }}>
                            {hover.content}
                        </div>
                    )}
                </div>

                {/* Side panel — top chokepoints */}
                <div style={S.sidePanel}>
                    <div style={S.panelHead}>TOP CHOKEPOINTS</div>
                    {chokepoints.length === 0 && (
                        <div style={{ fontSize: '11px', color: colors.textMuted }}>
                            No chokepoints flagged.
                        </div>
                    )}
                    {chokepoints.map((c, i) => (
                        <div key={`ch-${i}`} style={S.chokeCard}>
                            <div style={S.chokeLabel}>{c.label || c.id}</div>
                            <div style={{ fontSize: '10px', fontFamily: MONO, color: colors.textMuted }}>
                                score {(Number(c.score) || 0).toFixed(2)}
                            </div>
                            <div style={S.scoreBar}><div style={S.scoreFill(Number(c.score) || 0)} /></div>
                            {c.reason && <div style={{ marginTop: '4px' }}>{c.reason}</div>}
                            {Array.isArray(c.downstream_impact) && c.downstream_impact.length > 0 && (
                                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '4px' }}>
                                    impacts: {c.downstream_impact.slice(0, 3).join(', ')}
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
