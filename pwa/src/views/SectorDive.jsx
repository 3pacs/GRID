/**
 * SectorDive -- full sector analysis deep dive.
 *
 * Navigated to from MoneyFlow / Globe by clicking a sector node.
 * Shows: header with ETF, subsector treemap, intelligence panel,
 * sector vs market relative chart, and top movers table.
 */
import React, { useEffect, useRef, useState, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ActorProfileDrawer from '../components/ActorProfileDrawer.jsx';

// ── Helpers ──────────────────────────────────────────────────────────
const fmt = (v, decimals = 2) => {
    if (v == null) return '--';
    return Number(v).toFixed(decimals);
};
const fmtPct = (v) => {
    if (v == null) return '--';
    const n = (Number(v) * 100).toFixed(2);
    return `${n >= 0 ? '+' : ''}${n}%`;
};
const fmtUSD = (v) => {
    if (v == null) return '--';
    const abs = Math.abs(v);
    if (abs >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
    return `$${v.toFixed(0)}`;
};

const PERF_COLOR = (v) => {
    if (v == null) return colors.textMuted;
    return v >= 0 ? colors.green : colors.red;
};

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

// ── Canvas lens deep-link helpers ────────────────────────────────────
// Lens = 'graph' | 'supply' | 'capital'. Routes to Gotham Canvas at
// `#/canvas/{actorId}/{lens}`. Used everywhere we render a ticker so any
// actor in SectorDive is a jump-off point into the intelligence canvas.
const navCanvas = (actorId, lens = 'graph') => {
    if (typeof window === 'undefined' || !actorId) return;
    const id = encodeURIComponent(String(actorId));
    const l = (lens === 'supply' || lens === 'capital') ? `/${lens}` : '';
    window.location.hash = `#/canvas/${id}${l}`;
};

// Tiny icon button used inline next to tickers. Stops propagation so the
// row's primary click (open drawer / focus) still works.
const LensBtn = ({ label, lens, actorId, title }) => {
    const onClick = (e) => {
        e.stopPropagation();
        e.preventDefault();
        // Guard against text-selection drags: only fire on clean click.
        const sel = typeof window !== 'undefined' && window.getSelection?.();
        if (sel && sel.toString().length > 0) return;
        navCanvas(actorId, lens);
    };
    return (
        <span
            role="button"
            onClick={onClick}
            title={title || `Open ${actorId} in Canvas (${lens})`}
            style={{
                display: 'inline-flex',
                alignItems: 'center',
                justifyContent: 'center',
                minWidth: '16px',
                height: '16px',
                padding: '0 4px',
                background: 'transparent',
                border: `1px solid ${colors.border}`,
                borderRadius: '3px',
                color: colors.textDim,
                fontSize: '9px',
                fontWeight: 700,
                fontFamily: mono,
                cursor: 'pointer',
                userSelect: 'none',
                letterSpacing: '0.3px',
            }}
            onMouseEnter={(e) => {
                e.currentTarget.style.color = colors.accent;
                e.currentTarget.style.borderColor = colors.accent;
            }}
            onMouseLeave={(e) => {
                e.currentTarget.style.color = colors.textDim;
                e.currentTarget.style.borderColor = colors.border;
            }}
        >
            {label}
        </span>
    );
};

// G/S/F group — Graph / Supply / Capital lens shortcuts.
const LensLinks = ({ actorId }) => {
    if (!actorId) return null;
    return (
        <span style={{ display: 'inline-flex', gap: '3px', marginLeft: '6px' }} onClick={(e) => e.stopPropagation()}>
            <LensBtn label="G" lens="graph" actorId={actorId} title={`Open ${actorId} in Canvas graph lens`} />
            <LensBtn label="S" lens="supply" actorId={actorId} title={`Open ${actorId} in Canvas supply-chain lens`} />
            <LensBtn label="F" lens="capital" actorId={actorId} title={`Open ${actorId} in Canvas capital-flow lens`} />
        </span>
    );
};

// Clickable ticker badge that deep-links to the graph lens.
const TickerBadge = ({ ticker, lens = 'graph' }) => {
    if (!ticker) return null;
    return (
        <span
            role="button"
            onClick={(e) => {
                e.stopPropagation();
                e.preventDefault();
                navCanvas(ticker, lens);
            }}
            title={`Open ${ticker} in Canvas`}
            style={{
                fontWeight: 700,
                color: colors.accent,
                cursor: 'pointer',
                textDecoration: 'none',
                borderBottom: `1px dotted ${colors.accent}55`,
            }}
        >
            {ticker}
        </span>
    );
};

// ── Sector Health Ring Gauge ─────────────────────────────────────────
// 0-100 score with color band (red <40, amber 40-60, green >60). Trend
// arrow + component mini-bars render to the right of the ring.
const HEALTH_COLOR = (s) => {
    if (s == null) return '#6b7380';
    if (s < 40) return '#ef4444';
    if (s < 60) return '#f59e0b';
    return '#22c55e';
};
const TREND_ARROW = {
    improving: '\u2191',
    deteriorating: '\u2193',
    stable: '\u2192',
};
const COMPONENT_LABELS = {
    margin: 'Margin',
    chokepoints: 'Chokepoints',
    capital_allocation: 'Capital Alloc',
    insider: 'Insider',
    congress: 'Congress',
    dark_pool: 'Dark Pool',
};

function SectorHealthGauge({ health }) {
    const ringRef = useRef(null);
    const [showTip, setShowTip] = useState(false);
    const score = health?.score ?? null;
    const trend = health?.trend_30d || 'stable';
    const components = health?.components || {};
    const color = HEALTH_COLOR(score);

    useEffect(() => {
        if (!ringRef.current) return;
        const svg = d3.select(ringRef.current);
        svg.selectAll('*').remove();

        const size = 110;
        const radius = 44;
        const thickness = 10;
        const cx = size / 2;
        const cy = size / 2;

        svg.attr('width', size).attr('height', size).attr('viewBox', `0 0 ${size} ${size}`);

        const bgArc = d3.arc()
            .innerRadius(radius - thickness)
            .outerRadius(radius)
            .startAngle(0)
            .endAngle(2 * Math.PI);
        svg.append('path')
            .attr('d', bgArc())
            .attr('transform', `translate(${cx},${cy})`)
            .attr('fill', '#1A2332')
            .attr('stroke', '#243248')
            .attr('stroke-width', 0.5);

        if (score != null) {
            const pct = Math.max(0, Math.min(1, score / 100));
            const fgArc = d3.arc()
                .innerRadius(radius - thickness)
                .outerRadius(radius)
                .startAngle(0)
                .endAngle(pct * 2 * Math.PI);
            svg.append('path')
                .attr('d', fgArc())
                .attr('transform', `translate(${cx},${cy})`)
                .attr('fill', color)
                .attr('fill-opacity', 0.9);
        }

        svg.append('text')
            .attr('x', cx)
            .attr('y', cy + 2)
            .attr('text-anchor', 'middle')
            .attr('dominant-baseline', 'middle')
            .attr('font-family', mono)
            .attr('font-weight', 700)
            .attr('font-size', '22px')
            .attr('fill', color)
            .text(score == null ? '--' : Math.round(score));

        svg.append('text')
            .attr('x', cx)
            .attr('y', cy + 22)
            .attr('text-anchor', 'middle')
            .attr('font-family', mono)
            .attr('font-size', '9px')
            .attr('fill', '#6b7380')
            .attr('letter-spacing', '0.5')
            .text('HEALTH');
    }, [score, color]);

    if (!health) return null;

    const orderedKeys = ['margin', 'chokepoints', 'capital_allocation', 'insider', 'congress', 'dark_pool'];

    return (
        <div
            style={{
                display: 'flex',
                alignItems: 'center',
                gap: '16px',
                padding: '14px 18px',
                marginBottom: tokens.space.lg,
                background: 'linear-gradient(180deg, #0f1622 0%, #0a1018 100%)',
                border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md,
                position: 'relative',
            }}
            onMouseEnter={() => setShowTip(true)}
            onMouseLeave={() => setShowTip(false)}
        >
            <div style={{ flexShrink: 0, display: 'flex', alignItems: 'center', gap: '10px' }}>
                <svg ref={ringRef} style={{ display: 'block' }} />
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                    <div style={{
                        fontFamily: mono, fontSize: '10px', color: colors.textMuted,
                        letterSpacing: '0.5px', textTransform: 'uppercase',
                    }}>
                        30d Trend
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                        <span style={{
                            fontFamily: mono, fontSize: '20px', fontWeight: 700,
                            color: trend === 'improving' ? colors.green
                                 : trend === 'deteriorating' ? colors.red
                                 : colors.textDim,
                        }}>
                            {TREND_ARROW[trend] || '\u2192'}
                        </span>
                        <span style={{ fontFamily: mono, fontSize: '11px', color: colors.text }}>
                            {trend}
                        </span>
                    </div>
                </div>
            </div>

            <div style={{
                flex: 1, display: 'grid',
                gridTemplateColumns: 'repeat(2, 1fr)',
                gap: '6px 16px', minWidth: 0,
            }}>
                {orderedKeys.map((k) => {
                    const v = components[k];
                    const w = v == null ? 0 : Math.max(0, Math.min(1, v)) * 100;
                    const barColor = v == null ? '#3a4454'
                        : v >= 0.6 ? colors.green
                        : v >= 0.4 ? '#f59e0b'
                        : colors.red;
                    return (
                        <div key={k} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                            <div style={{
                                fontFamily: mono, fontSize: '9px', color: colors.textMuted,
                                width: '78px', flexShrink: 0,
                                textTransform: 'uppercase', letterSpacing: '0.3px',
                            }}>
                                {COMPONENT_LABELS[k]}
                            </div>
                            <div style={{
                                flex: 1, height: '6px',
                                background: '#1A2332', borderRadius: '3px', overflow: 'hidden',
                            }}>
                                <div style={{
                                    width: `${w}%`, height: '100%', background: barColor,
                                    transition: 'width 240ms ease-out',
                                }} />
                            </div>
                            <div style={{
                                fontFamily: mono, fontSize: '9px',
                                color: colors.textDim, width: '28px', textAlign: 'right',
                            }}>
                                {v == null ? '--' : v.toFixed(2)}
                            </div>
                        </div>
                    );
                })}
            </div>

            {showTip && health?.narrative && (
                <div style={{
                    position: 'absolute',
                    top: '100%',
                    left: '18px',
                    marginTop: '6px',
                    padding: '8px 12px',
                    background: '#0a0f18',
                    border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.sm,
                    fontFamily: mono, fontSize: '11px',
                    color: colors.text,
                    maxWidth: '520px',
                    zIndex: 20,
                    boxShadow: '0 6px 20px rgba(0,0,0,0.5)',
                }}>
                    {health.narrative}
                </div>
            )}
        </div>
    );
}

// ── Component ────────────────────────────────────────────────────────
export default function SectorDive({ sector: sectorProp, onBack }) {
    const [data, setData] = useState(null);
    const [health, setHealth] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const treemapRef = useRef(null);
    const chartRef = useRef(null);
    const [selectedSub, setSelectedSub] = useState(null);
    const [selectedActor, setSelectedActor] = useState(null);  // {id, sector} for profile drawer
    const [tradeTickets, setTradeTickets] = useState([]);

    // Derive sector name from prop or URL hash. No silent Technology default —
    // render an explicit error below if the caller didn't supply one.
    const sectorName = useMemo(() => {
        if (sectorProp) return sectorProp;
        const hash = typeof window !== 'undefined' ? window.location.hash : '';
        const m = hash.match(/sector-dive\/(.+)/);
        return m ? decodeURIComponent(m[1]) : null;
    }, [sectorProp]);

    useEffect(() => {
        loadData();
    }, [sectorName]);

    // Fetch recent trade tickets (contagion-derived) for the intel feed.
    useEffect(() => {
        let cancelled = false;
        api.getRecentTradeTickets(168)
            .then((d) => {
                if (cancelled) return;
                setTradeTickets(Array.isArray(d?.tickets) ? d.tickets : []);
            })
            .catch(() => { if (!cancelled) setTradeTickets([]); });
        return () => { cancelled = true; };
    }, [sectorName]);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const d = await api.getSectorDetail(sectorName);
            if (d.error) throw new Error(d.message || 'Failed to load sector');
            setData(d);
        } catch (err) {
            setError(err.message || 'Failed to load sector data');
        }
        setLoading(false);

        // Health score loads in parallel; a failure here must not block the
        // main sector dive render, so errors are swallowed with a warning.
        try {
            const h = await api.getSectorHealth(sectorName);
            if (h && !h.error) setHealth(h);
        } catch (err) {
            // eslint-disable-next-line no-console
            console.warn('sector health load failed', err);
        }
    };

    // ── All actors flat list for top movers ──────────────────────
    const allActors = useMemo(() => {
        if (!data?.subsectors) return [];
        const actors = [];
        for (const [subName, sub] of Object.entries(data.subsectors)) {
            for (const a of sub.actors || []) {
                actors.push({ ...a, subsector: subName });
            }
        }
        return actors;
    }, [data]);

    const topGainers = useMemo(() =>
        [...allActors].filter(a => a.pct_30d != null).sort((a, b) => (b.pct_30d || 0) - (a.pct_30d || 0)).slice(0, 5),
        [allActors]
    );
    const topLosers = useMemo(() =>
        [...allActors].filter(a => a.pct_30d != null).sort((a, b) => (a.pct_30d || 0) - (b.pct_30d || 0)).slice(0, 5),
        [allActors]
    );

    // ── Treemap rendering ────────────────────────────────────────
    useEffect(() => {
        if (!data?.subsectors || !treemapRef.current) return;

        const container = treemapRef.current;
        const width = container.clientWidth || 600;
        const height = 320;

        const svg = d3.select(container);
        svg.selectAll('*').remove();

        // Build hierarchy: root -> subsectors -> actors
        const children = Object.entries(data.subsectors).map(([name, sub]) => ({
            name,
            children: (sub.actors || []).map(a => ({
                name: a.ticker || a.name,
                fullName: a.name,
                value: Math.max(0.01, a.weight || 0.05),
                pct_30d: a.pct_30d,
                latest_price: a.latest_price,
                ticker: a.ticker,
                insider_signal: a.insider_signal,
                options_signal: a.options_signal,
            })),
        }));

        const root = d3.hierarchy({ name: 'root', children })
            .sum(d => d.value || 0)
            .sort((a, b) => (b.value || 0) - (a.value || 0));

        d3.treemap()
            .size([width, height])
            .paddingTop(18)
            .paddingInner(2)
            .paddingOuter(3)
            .round(true)(root);

        // Subsector group labels
        const subsectorNodes = root.children || [];
        subsectorNodes.forEach(sub => {
            svg.append('rect')
                .attr('x', sub.x0)
                .attr('y', sub.y0)
                .attr('width', sub.x1 - sub.x0)
                .attr('height', sub.y1 - sub.y0)
                .attr('fill', colors.bg)
                .attr('stroke', colors.border)
                .attr('stroke-width', 1)
                .attr('rx', 4);

            svg.append('text')
                .attr('x', sub.x0 + 4)
                .attr('y', sub.y0 + 12)
                .attr('font-size', '9px')
                .attr('font-family', mono)
                .attr('font-weight', 700)
                .attr('fill', colors.accent)
                .text(sub.data.name);
        });

        // Actor tiles
        const leaves = root.leaves();
        const perfExtent = d3.extent(leaves, d => d.data.pct_30d);
        const perfScale = d3.scaleLinear()
            .domain([perfExtent[0] || -0.1, 0, perfExtent[1] || 0.1])
            .range([colors.red, '#1A2840', colors.green])
            .clamp(true);

        const groups = svg.selectAll('.leaf')
            .data(leaves)
            .join('g')
            .attr('class', 'leaf')
            .style('cursor', 'pointer')
            .on('dblclick', (event, d) => {
                // Double-click → open Canvas supply-chain lens for the ticker.
                event.stopPropagation();
                event.preventDefault();
                const tk = d?.data?.ticker || d?.data?.name;
                if (tk) navCanvas(tk, 'supply');
            });

        groups.append('rect')
            .attr('x', d => d.x0)
            .attr('y', d => d.y0)
            .attr('width', d => Math.max(0, d.x1 - d.x0))
            .attr('height', d => Math.max(0, d.y1 - d.y0))
            .attr('fill', d => {
                if (d.data.pct_30d == null) return '#1A2840';
                return perfScale(d.data.pct_30d);
            })
            .attr('fill-opacity', 0.7)
            .attr('rx', 3)
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.5);

        // Ticker label
        groups.filter(d => (d.x1 - d.x0) > 30 && (d.y1 - d.y0) > 20)
            .append('text')
            .attr('x', d => d.x0 + 3)
            .attr('y', d => d.y0 + 12)
            .attr('font-size', d => (d.x1 - d.x0) > 60 ? '10px' : '8px')
            .attr('font-family', mono)
            .attr('font-weight', 600)
            .attr('fill', '#E8F0F8')
            .text(d => d.data.name);

        // Price + change
        groups.filter(d => (d.x1 - d.x0) > 50 && (d.y1 - d.y0) > 34)
            .append('text')
            .attr('x', d => d.x0 + 3)
            .attr('y', d => d.y0 + 24)
            .attr('font-size', '8px')
            .attr('font-family', mono)
            .attr('fill', d => PERF_COLOR(d.data.pct_30d))
            .text(d => {
                const p = d.data.latest_price != null ? `$${fmt(d.data.latest_price, 0)}` : '';
                const c = d.data.pct_30d != null ? ` ${fmtPct(d.data.pct_30d)}` : '';
                return `${p}${c}`;
            });

        // Signal indicators
        groups.filter(d => (d.x1 - d.x0) > 40 && (d.y1 - d.y0) > 44 && (d.data.insider_signal || d.data.options_signal))
            .append('text')
            .attr('x', d => d.x0 + 3)
            .attr('y', d => d.y0 + 36)
            .attr('font-size', '7px')
            .attr('font-family', mono)
            .attr('fill', colors.textMuted)
            .text(d => {
                const parts = [];
                if (d.data.insider_signal) parts.push(`INS:${d.data.insider_signal}`);
                if (d.data.options_signal) parts.push(`OPT:${d.data.options_signal}`);
                return parts.join(' ');
            });

    }, [data]);

    // ── Relative performance chart (sector ETF vs SPY) ───────────
    useEffect(() => {
        if (!data || !chartRef.current) return;
        // We show a simple bar comparison since we don't have full timeseries here
        const container = chartRef.current;
        const width = container.clientWidth || 400;
        const height = 140;

        const svg = d3.select(container);
        svg.selectAll('*').remove();

        const etfChange = data.change_1m || 0;
        const relStr = data.sector_metrics?.relative_strength_1m || 0;
        const spyChange = etfChange - relStr;

        const bars = [
            { label: data.etf || 'Sector', value: etfChange },
            { label: 'SPY', value: spyChange },
        ];

        const maxAbs = Math.max(0.01, d3.max(bars, d => Math.abs(d.value)));
        const xScale = d3.scaleLinear()
            .domain([-maxAbs, maxAbs])
            .range([80, width - 20]);
        const yScale = d3.scaleBand()
            .domain(bars.map(d => d.label))
            .range([20, height - 10])
            .padding(0.4);

        // Zero line
        svg.append('line')
            .attr('x1', xScale(0)).attr('x2', xScale(0))
            .attr('y1', 10).attr('y2', height - 5)
            .attr('stroke', colors.border).attr('stroke-width', 1);

        bars.forEach(bar => {
            const x = bar.value >= 0 ? xScale(0) : xScale(bar.value);
            const w = Math.abs(xScale(bar.value) - xScale(0));

            svg.append('rect')
                .attr('x', x)
                .attr('y', yScale(bar.label))
                .attr('width', w)
                .attr('height', yScale.bandwidth())
                .attr('fill', bar.value >= 0 ? colors.green : colors.red)
                .attr('fill-opacity', 0.7)
                .attr('rx', 3);

            svg.append('text')
                .attr('x', 4)
                .attr('y', yScale(bar.label) + yScale.bandwidth() / 2)
                .attr('dy', '0.35em')
                .attr('font-size', '11px')
                .attr('font-family', mono)
                .attr('font-weight', 600)
                .attr('fill', colors.text)
                .text(bar.label);

            svg.append('text')
                .attr('x', bar.value >= 0 ? xScale(bar.value) + 4 : xScale(bar.value) - 4)
                .attr('y', yScale(bar.label) + yScale.bandwidth() / 2)
                .attr('dy', '0.35em')
                .attr('text-anchor', bar.value >= 0 ? 'start' : 'end')
                .attr('font-size', '10px')
                .attr('font-family', mono)
                .attr('fill', PERF_COLOR(bar.value))
                .text(fmtPct(bar.value));
        });
    }, [data]);

    // ── Render ───────────────────────────────────────────────────
    if (loading) {
        return (
            <div style={{ ...shared.container, textAlign: 'center', padding: '80px 20px' }}>
                <div style={{ color: colors.textMuted, fontFamily: mono, fontSize: '13px' }}>
                    Loading sector analysis...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={shared.container}>
                <div style={{ color: colors.red, fontFamily: mono, fontSize: '13px', marginBottom: '12px' }}>
                    {error}
                </div>
                <button onClick={onBack} style={shared.buttonSmall}>Back</button>
            </div>
        );
    }

    const metrics = data?.sector_metrics || {};
    const intel = data?.intelligence || {};

    return (
        <div style={{ ...shared.container, maxWidth: '1100px' }}>

            {/* ═══ HEADER ═══ */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: tokens.space.lg, flexWrap: 'wrap' }}>
                {onBack && (
                    <button onClick={onBack} style={{
                        background: 'none', border: `1px solid ${colors.border}`, borderRadius: tokens.radius.sm,
                        color: colors.textDim, cursor: 'pointer', padding: '6px 12px', fontSize: '12px', fontFamily: mono,
                    }}>
                        Back
                    </button>
                )}
                <div>
                    <h1 style={{ ...shared.header, marginBottom: '2px', fontSize: '24px' }}>
                        {data?.sector}
                    </h1>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flexWrap: 'wrap' }}>
                        <span style={{
                            fontFamily: mono, fontSize: '13px', fontWeight: 700, color: colors.accent,
                            background: `${colors.accent}15`, padding: '2px 8px', borderRadius: '4px',
                        }}>
                            {data?.etf}
                        </span>
                        {data?.etf && (
                            <span
                                role="button"
                                onClick={() => navCanvas(data.etf, 'graph')}
                                title={`Open ${data.etf} in Canvas (graph lens)`}
                                style={{
                                    display: 'inline-flex', alignItems: 'center', gap: '4px',
                                    fontFamily: mono, fontSize: '10px', fontWeight: 700,
                                    color: colors.accent,
                                    background: 'transparent',
                                    border: `1px solid ${colors.accent}55`,
                                    padding: '2px 8px',
                                    borderRadius: tokens.radius.pill,
                                    cursor: 'pointer',
                                    letterSpacing: '0.5px',
                                    textTransform: 'uppercase',
                                }}
                            >
                                Canvas →
                            </span>
                        )}
                        {data?.price != null && (
                            <span style={{ fontFamily: mono, fontSize: '14px', color: colors.text, fontWeight: 600 }}>
                                ${fmt(data.price)}
                            </span>
                        )}
                        <span style={{
                            ...shared.badge(PERF_COLOR(data?.change_1m)),
                            background: `${PERF_COLOR(data?.change_1m)}20`,
                            color: PERF_COLOR(data?.change_1m),
                            fontSize: '12px', fontFamily: mono,
                        }}>
                            {fmtPct(data?.change_1m)} (30d)
                        </span>
                        {metrics.dark_pool_signal && metrics.dark_pool_signal !== 'neutral' && (
                            <span style={{
                                fontSize: '10px', padding: '2px 8px', borderRadius: '4px', fontFamily: mono,
                                background: metrics.dark_pool_signal === 'accumulation' ? `${colors.green}15` : `${colors.red}15`,
                                color: metrics.dark_pool_signal === 'accumulation' ? colors.green : colors.red,
                            }}>
                                Dark Pool: {metrics.dark_pool_signal}
                            </span>
                        )}
                    </div>
                </div>
            </div>

            {/* ═══ SECTOR HEALTH GAUGE ═══ */}
            {health && <SectorHealthGauge health={health} />}

            {/* ═══ TOP METRICS ROW ═══ */}
            <div style={{ ...shared.metricGrid, gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', marginBottom: tokens.space.lg }}>
                <MetricCard label="Relative Strength (1M)" value={fmtPct(metrics.relative_strength_1m)} color={PERF_COLOR(metrics.relative_strength_1m)} />
                <MetricCard
                    label="ETF Flow (5D)"
                    value={fmtUSD(metrics.etf_flow_5d)}
                    color={metrics.etf_flow_5d == null ? colors.textMuted : (metrics.etf_flow_5d >= 0 ? colors.green : colors.red)}
                />
                <MetricCard label="Dark Pool Signal" value={metrics.dark_pool_signal || 'neutral'} color={
                    metrics.dark_pool_signal === 'accumulation' ? colors.green
                    : metrics.dark_pool_signal === 'distribution' ? colors.red
                    : colors.textDim
                } />
                <MetricCard label="Insider Trades (30d)" value={String((metrics.insider_activity || []).length)} color={colors.text} />
                <MetricCard label="Congressional (60d)" value={String((metrics.congressional_activity || []).length)} color={colors.text} />
            </div>

            {/* ═══ SUBSECTOR TREEMAP ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>SUBSECTOR BREAKDOWN</div>
                <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, marginBottom: '6px' }}>
                    Double-click any tile to open its supply-chain lens in Canvas.
                </div>
                <svg
                    ref={treemapRef}
                    width="100%"
                    height={320}
                    style={{ display: 'block', overflow: 'visible' }}
                />
            </div>

            {/* ═══ TWO-COLUMN: CHART + INTEL ═══ */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tokens.space.lg, marginBottom: tokens.space.lg }}>

                {/* Sector vs Market chart */}
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>SECTOR vs MARKET (30D)</div>
                    <svg
                        ref={chartRef}
                        width="100%"
                        height={140}
                        style={{ display: 'block' }}
                    />
                </div>

                {/* Intelligence panel */}
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>INTELLIGENCE</div>
                    {intel.narrative && (
                        <div style={{
                            fontSize: '12px', color: colors.textDim, lineHeight: '1.5', marginBottom: '10px',
                            fontStyle: 'italic', fontFamily: colors.sans,
                        }}>
                            {intel.narrative}
                        </div>
                    )}

                    {/* Lever pullers */}
                    {intel.lever_pullers?.length > 0 && (
                        <div style={{ marginBottom: '8px' }}>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.yellow, fontFamily: mono, letterSpacing: '1px', marginBottom: '4px' }}>
                                LEVER PULLERS
                            </div>
                            {intel.lever_pullers.slice(0, 5).map((lp, i) => (
                                <div key={i} style={{ fontSize: '11px', color: colors.text, fontFamily: mono, marginBottom: '2px' }}>
                                    {lp.ticker || lp.name} - {lp.description || lp.signal || lp.type || ''}
                                </div>
                            ))}
                        </div>
                    )}

                    {/* Convergence */}
                    {intel.convergence?.length > 0 && (
                        <div>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: '#06B6D4', fontFamily: mono, letterSpacing: '1px', marginBottom: '4px' }}>
                                CONVERGENCE ALERTS
                            </div>
                            {intel.convergence.slice(0, 5).map((c, i) => (
                                <div key={i} style={{ fontSize: '11px', color: colors.text, fontFamily: mono, marginBottom: '2px' }}>
                                    {c.ticker || c.name}: {c.message || c.alert || JSON.stringify(c).slice(0, 80)}
                                </div>
                            ))}
                        </div>
                    )}

                    {!intel.narrative && !(intel.lever_pullers?.length) && !(intel.convergence?.length) && (
                        <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                            No intelligence signals available for this sector.
                        </div>
                    )}
                </div>
            </div>

            {/* ═══ WHO'S BUYING / SELLING ═══ */}
            {((metrics.insider_activity || []).length > 0 || (metrics.congressional_activity || []).length > 0) && (
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tokens.space.lg, marginBottom: tokens.space.lg }}>
                    {/* Insider activity */}
                    <div style={shared.card}>
                        <div style={shared.sectionTitle}>INSIDER ACTIVITY</div>
                        {(metrics.insider_activity || []).length === 0 ? (
                            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>No recent insider trades</div>
                        ) : (
                            <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                                {metrics.insider_activity.slice(0, 10).map((t, i) => (
                                    <div key={i} style={{
                                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                        padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
                                        fontSize: '11px', fontFamily: mono,
                                    }}>
                                        <div>
                                            <TickerBadge ticker={t.ticker} lens="graph" />
                                            <span style={{ color: colors.textMuted, marginLeft: '6px' }}>{t.name}</span>
                                        </div>
                                        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                            <span style={{
                                                color: (t.type === 'P' || t.type === 'Purchase' || t.type === 'Buy') ? colors.green : colors.red,
                                                fontWeight: 600,
                                            }}>
                                                {t.type}
                                            </span>
                                            {t.value != null && <span style={{ color: colors.textDim }}>{fmtUSD(t.value)}</span>}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>

                    {/* Congressional activity */}
                    <div style={shared.card}>
                        <div style={shared.sectionTitle}>CONGRESSIONAL TRADES</div>
                        {(metrics.congressional_activity || []).length === 0 ? (
                            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>No recent congressional trades</div>
                        ) : (
                            <div style={{ maxHeight: '200px', overflowY: 'auto' }}>
                                {metrics.congressional_activity.slice(0, 10).map((t, i) => (
                                    <div key={i} style={{
                                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                        padding: '4px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
                                        fontSize: '11px', fontFamily: mono,
                                    }}>
                                        <div>
                                            <TickerBadge ticker={t.ticker} lens="graph" />
                                            <span style={{ color: colors.textMuted, marginLeft: '6px' }}>{t.representative}</span>
                                        </div>
                                        <div>
                                            <span style={{ color: colors.textDim }}>{t.type} {t.amount || ''}</span>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            )}

            {/* ═══ TOP MOVERS ═══ */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: tokens.space.lg, marginBottom: tokens.space.lg }}>
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, color: colors.green }}>TOP GAINERS (30D)</div>
                    {topGainers.map((a, i) => (
                        <MoverRow key={a.ticker || i} actor={a} rank={i + 1} />
                    ))}
                    {topGainers.length === 0 && (
                        <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>No data</div>
                    )}
                </div>
                <div style={shared.card}>
                    <div style={{ ...shared.sectionTitle, color: colors.red }}>TOP LOSERS (30D)</div>
                    {topLosers.map((a, i) => (
                        <MoverRow key={a.ticker || i} actor={a} rank={i + 1} />
                    ))}
                    {topLosers.length === 0 && (
                        <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>No data</div>
                    )}
                </div>
            </div>

            {/* ═══ OWNERSHIP & POWER NETWORK ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>OWNERSHIP & POWER NETWORK</div>
                <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, marginBottom: '6px' }}>
                    Click = filter neighborhood (then use "Canvas →" button). Double-click or shift-click = open actor profile.
                </div>
                <PowerNetwork
                    connections={data?.connections}
                    onActorOpen={(nodeId) => setSelectedActor({ id: nodeId, sector: data?.sector || sectorName })}
                />
            </div>

            {/* ═══ CROSS-REFERENCES ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>CROSS-REFERENCES</div>
                <CrossRefPills edges={data?.connections?.edges || []} />
            </div>

            {/* ═══ CLUSTERS ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>CLUSTERS</div>
                <ClusterGrid clusters={data?.connections?.clusters || []} />
            </div>

            {/* ═══ LINEAGE CHAINS ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>LINEAGE CHAINS — CONNECT THE DOTS</div>
                <LineageFlows lineage={data?.connections?.lineage || []} />
            </div>

            {/* ═══ INTELLIGENCE FEED ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>INTELLIGENCE FEED</div>
                <IntelligenceFeed
                    insider={metrics.insider_activity || []}
                    congress={metrics.congressional_activity || []}
                    convergence={intel.convergence || []}
                    levers={intel.lever_pullers || []}
                    tradeTickets={tradeTickets}
                    sectorTickers={new Set(
                        allActors
                            .map((a) => String(a.ticker || a.id || '').toLowerCase())
                            .filter(Boolean),
                    )}
                />
            </div>

            {/* ═══ ETF + OPTIONS DEEP DIVE ═══ */}
            <div style={{ ...shared.card, marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>ETF + OPTIONS DEEP DIVE</div>
                <OptionsGauges
                    etfOptions={data?.etf_options}
                    dealerGamma={metrics.dealer_gamma}
                />
            </div>

            {/* ═══ ACTOR PROFILE DRAWER ═══ */}
            <ActorProfileDrawer
                actor={selectedActor}
                onClose={() => setSelectedActor(null)}
                onNavigate={(nextId) => setSelectedActor({ id: nextId, sector: data?.sector || sectorName })}
            />
        </div>
    );
}

// ── Sub-components ───────────────────────────────────────────────────

function MetricCard({ label, value, color }) {
    return (
        <div style={shared.metric}>
            <div style={{ ...shared.metricValue, fontSize: '15px', color: color || '#E8F0F8' }}>{value}</div>
            <div style={shared.metricLabel}>{label}</div>
        </div>
    );
}

// ── Edge type metadata (colors + labels) ─────────────────────────────
const EDGE_META = {
    common_13f_holder:        { color: '#6366F1', label: 'common 13F holder',       group: 'ticker' },
    co_insider_activity:      { color: '#10B981', label: 'co-insider activity',     group: 'people' },
    co_congress_trade:        { color: '#F59E0B', label: 'co-congress trade',       group: 'people' },
    co_dark_pool_accumulation:{ color: '#8B5CF6', label: 'co-dark-pool accumulation', group: 'ticker' },
    activist_holder:          { color: '#EF4444', label: 'activist holder',         group: 'ticker' },
    supply_chain:             { color: '#8B4513', label: 'supply chain',            group: 'supply' },
    regulatory_threat:        { color: '#DC2626', label: 'regulatory threat',       group: 'regulatory' },
    demand_destruction:       { color: '#F97316', label: 'demand destruction',      group: 'supply' },
    private_control:          { color: '#A855F7', label: 'private control',         group: 'people' },
    lever_puller:             { color: '#FBBF24', label: 'lever puller',            group: 'people' },
    convergence:              { color: '#06B6D4', label: 'convergence',             group: 'ticker' },
};

const NODE_COLOR = (type) => {
    switch (type) {
        case 'company':       return colors.accent;
        case 'person':
        case 'family_office': return colors.yellow;
        case 'regulator':     return colors.red;
        case 'commodity':     return '#8B4513';
        case 'macro':         return '#06B6D4';
        case 'event':         return colors.green;
        case 'trade_org':     return '#A855F7';
        case 'private':       return colors.textDim;
        default:              return colors.textMuted;
    }
};

// ── Power Network (D3 force-directed graph) ──────────────────────────
function PowerNetwork({ connections, onActorOpen }) {
    const wrapRef = useRef(null);
    const svgRef = useRef(null);
    const tooltipRef = useRef(null);
    const [width, setWidth] = useState(800);
    const [filter, setFilter] = useState('all');
    const [enabledTypes, setEnabledTypes] = useState(() => {
        const init = {};
        Object.keys(EDGE_META).forEach(k => { init[k] = true; });
        return init;
    });
    const [focusNode, setFocusNode] = useState(null);
    const height = 520;

    // Resize observer for accurate SVG width
    useEffect(() => {
        if (!wrapRef.current) return;
        const update = () => {
            const rect = wrapRef.current.getBoundingClientRect();
            if (rect.width > 0) setWidth(Math.floor(rect.width));
        };
        update();
        const ro = new ResizeObserver(update);
        ro.observe(wrapRef.current);
        return () => ro.disconnect();
    }, []);

    // Compute filtered node + edge sets
    const { nodes, edges, neighbors } = useMemo(() => {
        const rawNodes = connections?.nodes || [];
        const rawEdges = connections?.edges || [];
        if (!rawNodes.length) return { nodes: [], edges: [], neighbors: {} };

        // Deduplicate nodes by id
        const nodeMap = new Map();
        for (const n of rawNodes) {
            if (!n || n.id == null) continue;
            if (!nodeMap.has(n.id)) nodeMap.set(n.id, { ...n });
        }

        // Dedupe edges and drop ones with missing endpoints
        const edgeKey = (e) => `${e.source}::${e.target}::${e.type}`;
        const seen = new Set();
        const validEdges = [];
        for (const e of rawEdges) {
            if (!e || e.source == null || e.target == null) continue;
            if (!nodeMap.has(e.source) || !nodeMap.has(e.target)) continue;
            if (!enabledTypes[e.type] && e.type != null) continue;
            if (filter !== 'all') {
                const meta = EDGE_META[e.type];
                if (!meta || meta.group !== filter) continue;
            }
            const k = edgeKey(e);
            if (seen.has(k)) continue;
            seen.add(k);
            validEdges.push({ ...e });
        }

        // Build neighbor map from visible edges
        const nbrs = {};
        for (const e of validEdges) {
            if (!nbrs[e.source]) nbrs[e.source] = new Set();
            if (!nbrs[e.target]) nbrs[e.target] = new Set();
            nbrs[e.source].add(e.target);
            nbrs[e.target].add(e.source);
        }

        return { nodes: Array.from(nodeMap.values()), edges: validEdges, neighbors: nbrs };
    }, [connections, enabledTypes, filter]);

    // D3 simulation
    useEffect(() => {
        if (!svgRef.current || !nodes.length) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const simNodes = nodes.map(n => ({ ...n }));
        const simEdges = edges.map(e => ({ ...e }));

        const sim = d3.forceSimulation(simNodes)
            .force('link', d3.forceLink(simEdges).id(d => d.id).distance(90).strength(0.4))
            .force('charge', d3.forceManyBody().strength(-220))
            .force('center', d3.forceCenter(width / 2, height / 2))
            .force('collide', d3.forceCollide().radius(d => Math.max(8, Math.sqrt(d.size || 40) * 2) + 3))
            .alpha(1)
            .alphaDecay(0.03);

        // Edges (under nodes)
        const linkSel = svg.append('g')
            .attr('class', 'edges')
            .selectAll('line')
            .data(simEdges)
            .join('line')
            .attr('stroke', d => EDGE_META[d.type]?.color || colors.textMuted)
            .attr('stroke-opacity', 0.5)
            .attr('stroke-width', d => Math.max(0.5, (d.strength || 0.3) * 3));

        const nodeSel = svg.append('g')
            .attr('class', 'nodes')
            .selectAll('g')
            .data(simNodes)
            .join('g')
            .style('cursor', 'pointer')
            .on('mouseenter', (event, d) => {
                const tip = tooltipRef.current;
                if (!tip) return;
                const evCount = simEdges.filter(e =>
                    (e.source.id || e.source) === d.id || (e.target.id || e.target) === d.id
                ).reduce((acc, e) => acc + (e.evidence || 0), 0);
                tip.style.display = 'block';
                tip.innerHTML = `
                    <div style="font-weight:700;color:${NODE_COLOR(d.type)};">${d.label || d.id}</div>
                    <div style="color:${colors.textDim};font-size:10px;">${d.type || 'unknown'}</div>
                    ${d.price != null ? `<div style="color:${colors.text};">$${Number(d.price).toFixed(2)}</div>` : ''}
                    ${d.pct_30d != null ? `<div style="color:${d.pct_30d >= 0 ? colors.green : colors.red};">${(d.pct_30d * 100).toFixed(2)}% (30d)</div>` : ''}
                    <div style="color:${colors.textMuted};font-size:10px;">evidence: ${evCount}</div>
                `;
            })
            .on('mousemove', (event) => {
                const tip = tooltipRef.current;
                if (!tip) return;
                const rect = wrapRef.current.getBoundingClientRect();
                tip.style.left = `${event.clientX - rect.left + 10}px`;
                tip.style.top = `${event.clientY - rect.top + 10}px`;
            })
            .on('mouseleave', () => {
                if (tooltipRef.current) tooltipRef.current.style.display = 'none';
            })
            .on('click', (event, d) => {
                // Shift-click opens actor drawer; plain click filters neighborhood.
                if (event.shiftKey && onActorOpen) {
                    event.stopPropagation();
                    onActorOpen(d.id);
                    return;
                }
                setFocusNode(prev => prev === d.id ? null : d.id);
            })
            .on('dblclick', (event, d) => {
                event.stopPropagation();
                if (onActorOpen) onActorOpen(d.id);
            });

        nodeSel.append('circle')
            .attr('r', d => Math.max(6, Math.sqrt(d.size || 40) * 2))
            .attr('fill', d => NODE_COLOR(d.type))
            .attr('fill-opacity', 0.85)
            .attr('stroke', colors.bg)
            .attr('stroke-width', 1.5);

        nodeSel.append('text')
            .text(d => d.label || d.id)
            .attr('font-size', '9px')
            .attr('font-family', mono)
            .attr('fill', colors.text)
            .attr('text-anchor', 'middle')
            .attr('dy', d => Math.max(6, Math.sqrt(d.size || 40) * 2) + 10)
            .attr('pointer-events', 'none');

        // Apply focus dimming
        const applyFocus = () => {
            if (!focusNode) {
                nodeSel.attr('opacity', 1);
                linkSel.attr('stroke-opacity', 0.5);
                return;
            }
            const nbrs = neighbors[focusNode] || new Set();
            nodeSel.attr('opacity', d => (d.id === focusNode || nbrs.has(d.id)) ? 1 : 0.15);
            linkSel.attr('stroke-opacity', d => {
                const s = d.source.id || d.source;
                const t = d.target.id || d.target;
                return (s === focusNode || t === focusNode) ? 0.9 : 0.05;
            });
        };
        applyFocus();

        let tick = 0;
        sim.on('tick', () => {
            tick++;
            // Clamp to bounds
            simNodes.forEach(n => {
                const r = Math.max(6, Math.sqrt(n.size || 40) * 2) + 2;
                n.x = Math.max(r, Math.min(width - r, n.x));
                n.y = Math.max(r, Math.min(height - r, n.y));
            });
            linkSel
                .attr('x1', d => d.source.x)
                .attr('y1', d => d.source.y)
                .attr('x2', d => d.target.x)
                .attr('y2', d => d.target.y);
            nodeSel.attr('transform', d => `translate(${d.x},${d.y})`);

            if (tick > 300 || sim.alpha() < 0.01) sim.stop();
        });

        return () => sim.stop();
    }, [nodes, edges, width, focusNode, neighbors, onActorOpen]);

    const toggleType = (t) => {
        setEnabledTypes(prev => ({ ...prev, [t]: !prev[t] }));
    };

    if (!connections || !(connections.nodes || []).length) {
        return (
            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono, padding: '40px 0', textAlign: 'center' }}>
                No connections detected yet.
            </div>
        );
    }

    return (
        <div ref={wrapRef} style={{ position: 'relative', width: '100%' }}>
            {/* Filter bar */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px', marginBottom: '8px', alignItems: 'center' }}>
                <div style={{ display: 'flex', gap: '4px' }}>
                    {['all', 'ticker', 'people', 'regulatory', 'supply'].map(g => (
                        <button
                            key={g}
                            onClick={() => setFilter(g)}
                            style={{
                                background: filter === g ? colors.accent : 'transparent',
                                color: filter === g ? '#fff' : colors.textDim,
                                border: `1px solid ${colors.border}`,
                                borderRadius: tokens.radius.sm,
                                padding: '3px 10px',
                                fontSize: '10px',
                                fontFamily: mono,
                                cursor: 'pointer',
                                textTransform: 'uppercase',
                                letterSpacing: '0.5px',
                            }}
                        >
                            {g}
                        </button>
                    ))}
                </div>
                {focusNode && (
                    <div style={{ display: 'flex', gap: '6px', marginLeft: 'auto', alignItems: 'center' }}>
                        <button
                            onClick={() => navCanvas(focusNode, 'graph')}
                            title={`Open ${focusNode} in Canvas graph lens`}
                            style={{
                                background: `${colors.accent}20`,
                                color: colors.accent,
                                border: `1px solid ${colors.accent}`,
                                borderRadius: tokens.radius.sm,
                                padding: '3px 10px',
                                fontSize: '10px',
                                fontFamily: mono,
                                cursor: 'pointer',
                                fontWeight: 600,
                            }}
                        >
                            Canvas {focusNode} →
                        </button>
                        <button
                            onClick={() => setFocusNode(null)}
                            style={{
                                background: `${colors.yellow}20`,
                                color: colors.yellow,
                                border: `1px solid ${colors.yellow}`,
                                borderRadius: tokens.radius.sm,
                                padding: '3px 10px',
                                fontSize: '10px',
                                fontFamily: mono,
                                cursor: 'pointer',
                            }}
                        >
                            clear focus
                        </button>
                    </div>
                )}
            </div>

            {/* Edge type legend with checkbox toggles */}
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px', marginBottom: '10px' }}>
                {Object.entries(EDGE_META).map(([key, meta]) => (
                    <label
                        key={key}
                        style={{
                            display: 'inline-flex', alignItems: 'center', gap: '4px',
                            fontSize: '9px', fontFamily: mono, cursor: 'pointer',
                            color: enabledTypes[key] ? colors.text : colors.textMuted,
                        }}
                    >
                        <input
                            type="checkbox"
                            checked={!!enabledTypes[key]}
                            onChange={() => toggleType(key)}
                            style={{ margin: 0, cursor: 'pointer' }}
                        />
                        <span style={{
                            display: 'inline-block', width: '10px', height: '2px',
                            background: meta.color, verticalAlign: 'middle',
                        }} />
                        {meta.label}
                    </label>
                ))}
            </div>

            <svg
                ref={svgRef}
                width={width}
                height={height}
                style={{
                    display: 'block',
                    background: colors.bg,
                    borderRadius: tokens.radius.sm,
                    border: `1px solid ${colors.borderSubtle}`,
                }}
            />

            {/* Tooltip overlay */}
            <div
                ref={tooltipRef}
                style={{
                    display: 'none',
                    position: 'absolute',
                    pointerEvents: 'none',
                    background: colors.card,
                    border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.sm,
                    padding: '6px 8px',
                    fontSize: '11px',
                    fontFamily: mono,
                    color: colors.text,
                    zIndex: 10,
                    boxShadow: colors.shadow?.md || '0 4px 12px rgba(0,0,0,0.4)',
                }}
            />
        </div>
    );
}

// ── Cluster Grid ─────────────────────────────────────────────────────
function ClusterGrid({ clusters }) {
    if (!clusters || clusters.length === 0) {
        return (
            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                No clusters detected yet.
            </div>
        );
    }
    const signalColor = (signal) => {
        const s = (signal || '').toLowerCase();
        if (s.includes('bullish') || s.includes('accumulation') || s.includes('buy')) return colors.green;
        if (s.includes('bearish') || s.includes('distribution') || s.includes('sell')) return colors.red;
        if (s.includes('rotation') || s.includes('pivot')) return colors.yellow;
        return colors.accent;
    };
    return (
        <div style={{
            display: 'grid',
            gridTemplateColumns: 'repeat(auto-fill, minmax(220px, 1fr))',
            gap: '10px',
        }}>
            {clusters.map((c, i) => {
                const col = signalColor(c.signal);
                return (
                    <div key={i} style={{
                        background: colors.bg,
                        border: `1px solid ${colors.border}`,
                        borderLeft: `3px solid ${col}`,
                        borderRadius: tokens.radius.sm,
                        padding: '10px 12px',
                    }}>
                        <div style={{
                            fontSize: '11px', fontWeight: 700, fontFamily: mono,
                            color: colors.text, marginBottom: '6px',
                        }}>
                            {c.name || 'Unnamed cluster'}
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px', marginBottom: '6px' }}>
                            {(c.tickers || []).slice(0, 8).map(t => (
                                <span key={t} style={{
                                    background: `${colors.accent}15`,
                                    color: colors.accent,
                                    padding: '1px 6px',
                                    borderRadius: '3px',
                                    fontSize: '9px',
                                    fontFamily: mono,
                                    fontWeight: 600,
                                }}>{t}</span>
                            ))}
                        </div>
                        {c.signal && (
                            <div style={{
                                fontSize: '10px',
                                color: col,
                                fontFamily: mono,
                                lineHeight: '1.4',
                            }}>
                                {c.signal}
                            </div>
                        )}
                    </div>
                );
            })}
        </div>
    );
}

// ── Lineage Flows ────────────────────────────────────────────────────
function LineageFlows({ lineage }) {
    if (!lineage || lineage.length === 0) {
        return (
            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                No lineage chains detected yet.
            </div>
        );
    }
    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
            {lineage.slice(0, 5).map((entry, idx) => {
                const path = Array.isArray(entry.path) ? entry.path : [];
                return (
                    <div key={idx} style={{
                        background: colors.bg,
                        border: `1px solid ${colors.borderSubtle}`,
                        borderRadius: tokens.radius.sm,
                        padding: '10px 12px',
                    }}>
                        <div style={{
                            display: 'flex', flexWrap: 'wrap', alignItems: 'center', gap: '6px',
                            marginBottom: '6px',
                        }}>
                            {path.map((node, i) => (
                                <React.Fragment key={i}>
                                    <span style={{
                                        background: `${colors.accent}20`,
                                        color: colors.accent,
                                        border: `1px solid ${colors.accent}40`,
                                        padding: '3px 9px',
                                        borderRadius: tokens.radius.pill,
                                        fontSize: '10px',
                                        fontFamily: mono,
                                        fontWeight: 600,
                                        whiteSpace: 'nowrap',
                                    }}>
                                        {typeof node === 'string' ? node : (node.label || node.id || '?')}
                                    </span>
                                    {i < path.length - 1 && (
                                        <span style={{
                                            color: colors.textDim,
                                            fontSize: '12px',
                                            fontFamily: mono,
                                        }}>→</span>
                                    )}
                                </React.Fragment>
                            ))}
                        </div>
                        {entry.label && (
                            <div style={{
                                fontSize: '11px',
                                color: colors.textDim,
                                fontStyle: 'italic',
                                fontFamily: colors.sans,
                                lineHeight: '1.5',
                            }}>
                                {entry.label}
                            </div>
                        )}
                    </div>
                );
            })}
        </div>
    );
}

// ── Intelligence Feed (unified timeline) ─────────────────────────────
function IntelligenceFeed({ insider, congress, convergence, levers, tradeTickets, sectorTickers }) {
    const feed = useMemo(() => {
        const rows = [];
        const sectorSet = sectorTickers instanceof Set ? sectorTickers : new Set();
        for (const t of tradeTickets || []) {
            const tk = String(t.ticker || '').toLowerCase();
            if (sectorSet.size > 0 && !sectorSet.has(tk)) continue;
            rows.push({
                source: 'ticket',
                ticker: tk.toUpperCase(),
                actor: String(t.shock_node || 'contagion').toUpperCase(),
                action: `${(t.direction || '').toUpperCase()} ${(t.instrument || '').toUpperCase()} $${t.strike} exp ${t.expiry} · kelly ${((t.kelly_size || 0) * 100).toFixed(1)}%`,
                date: t.generated_at,
                confidence: (Number(t.confidence) || 0) >= 0.6 ? 'derived' : 'inferred',
            });
        }
        for (const t of insider || []) {
            rows.push({
                source: 'insider',
                ticker: t.ticker,
                actor: t.name || t.insider || 'Insider',
                action: `${t.type || 'trade'}${t.value != null ? ' ' + fmtUSD(t.value) : ''}`,
                date: t.date || t.filing_date || t.transaction_date,
                confidence: t.confidence || 'confirmed',
            });
        }
        for (const t of congress || []) {
            rows.push({
                source: 'congress',
                ticker: t.ticker,
                actor: t.representative || t.name || 'Member',
                action: `${t.type || ''} ${t.amount || ''}`.trim(),
                date: t.date || t.transaction_date || t.disclosure_date,
                confidence: t.confidence || 'confirmed',
            });
        }
        for (const c of convergence || []) {
            rows.push({
                source: 'convergence',
                ticker: c.ticker || c.name,
                actor: c.source || 'convergence',
                action: c.message || c.alert || '',
                date: c.date || c.detected_at,
                confidence: c.confidence || 'derived',
            });
        }
        for (const l of levers || []) {
            rows.push({
                source: 'lever',
                ticker: l.ticker || l.name,
                actor: l.name || l.actor || 'Lever puller',
                action: l.description || l.signal || l.type || '',
                date: l.date || l.detected_at,
                confidence: l.confidence || 'inferred',
            });
        }
        rows.sort((a, b) => {
            const da = a.date ? Date.parse(a.date) : 0;
            const db = b.date ? Date.parse(b.date) : 0;
            return db - da;
        });
        return rows.slice(0, 30);
    }, [insider, congress, convergence, levers, tradeTickets, sectorTickers]);

    if (feed.length === 0) {
        return (
            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                No intelligence events yet.
            </div>
        );
    }
    return (
        <div style={{ maxHeight: '340px', overflowY: 'auto' }}>
            {feed.map((row, i) => <FeedRow key={i} row={row} />)}
        </div>
    );
}

function FeedRow({ row }) {
    const srcMeta = {
        insider:     { icon: 'INS', color: colors.green },
        congress:    { icon: 'CON', color: colors.yellow },
        convergence: { icon: 'CVG', color: '#06B6D4' },
        lever:       { icon: 'LVR', color: '#A855F7' },
        ticket:      { icon: 'TKT', color: colors.accent },
    }[row.source] || { icon: 'INT', color: colors.textDim };

    const confColor = {
        confirmed: '#E2E8F0',
        derived:   colors.accent,
        estimated: colors.yellow,
        rumored:   '#A855F7',
        inferred:  '#F97316',
    }[row.confidence] || colors.textMuted;

    const rel = (() => {
        if (!row.date) return '';
        const t = Date.parse(row.date);
        if (!t) return '';
        const diff = (Date.now() - t) / 1000;
        if (diff < 3600) return `${Math.floor(diff / 60)}m ago`;
        if (diff < 86400) return `${Math.floor(diff / 3600)}h ago`;
        if (diff < 2592000) return `${Math.floor(diff / 86400)}d ago`;
        return `${Math.floor(diff / 2592000)}mo ago`;
    })();

    return (
        <div style={{
            display: 'flex', alignItems: 'center', gap: '10px',
            padding: '6px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
            fontSize: '11px', fontFamily: mono,
        }}>
            <span style={{
                background: `${srcMeta.color}20`,
                color: srcMeta.color,
                padding: '2px 6px',
                borderRadius: '3px',
                fontSize: '9px',
                fontWeight: 700,
                letterSpacing: '0.5px',
                minWidth: '30px',
                textAlign: 'center',
            }}>{srcMeta.icon}</span>
            {row.ticker && (
                <span style={{
                    background: `${colors.accent}15`,
                    color: colors.accent,
                    padding: '1px 6px',
                    borderRadius: '3px',
                    fontSize: '10px',
                    fontWeight: 700,
                    minWidth: '42px',
                    textAlign: 'center',
                }}>{row.ticker}</span>
            )}
            <span style={{ color: colors.text, flex: '0 0 auto', maxWidth: '140px', overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {row.actor}
            </span>
            <span style={{ color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {row.action}
            </span>
            <span style={{ color: colors.textMuted, fontSize: '10px', flex: '0 0 auto' }}>{rel}</span>
            <span style={{
                width: '6px', height: '6px', borderRadius: '50%',
                background: confColor, display: 'inline-block',
            }} title={row.confidence} />
        </div>
    );
}

// ── Options Gauges ───────────────────────────────────────────────────
function OptionsGauges({ etfOptions, dealerGamma }) {
    const o = etfOptions || {};
    const hasAny = Object.keys(o).length > 0;

    const Gauge = ({ label, value, color, sub }) => (
        <div style={{
            background: colors.bg,
            border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.sm,
            padding: '12px 14px',
            textAlign: 'center',
        }}>
            <div style={{
                fontSize: '9px', fontWeight: 700, letterSpacing: '1px',
                color: colors.textMuted, fontFamily: mono, marginBottom: '6px',
            }}>{label}</div>
            <div style={{
                fontSize: '18px', fontWeight: 700, color: color || colors.text,
                fontFamily: mono,
            }}>{value}</div>
            {sub && (
                <div style={{
                    fontSize: '9px', color: colors.textDim, fontFamily: mono, marginTop: '4px',
                }}>{sub}</div>
            )}
        </div>
    );

    return (
        <div>
            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '10px',
            }}>
                <Gauge
                    label="MAX PAIN"
                    value={o.max_pain != null ? `$${fmt(o.max_pain)}` : '--'}
                    color={colors.yellow}
                />
                <Gauge
                    label="IV"
                    value={o.iv != null ? `${(o.iv * 100).toFixed(1)}%` : '--'}
                    color={colors.accent}
                />
                <Gauge
                    label="PUT/CALL"
                    value={o.pcr != null ? fmt(o.pcr) : '--'}
                    color={o.pcr != null ? (o.pcr > 1 ? colors.red : colors.green) : colors.text}
                    sub={o.pcr != null ? (o.pcr > 1 ? 'bearish tilt' : 'bullish tilt') : null}
                />
                <Gauge
                    label="TOTAL OI"
                    value={o.total_oi != null ? fmtUSD(o.total_oi).replace('$', '') : '--'}
                    color={colors.text}
                />
                <Gauge
                    label="DEALER GAMMA"
                    value={dealerGamma != null ? fmtUSD(dealerGamma) : 'n/a'}
                    color={dealerGamma == null ? colors.textMuted : (dealerGamma >= 0 ? colors.green : colors.red)}
                    sub={dealerGamma == null ? 'not available' : null}
                />
            </div>
            {!hasAny && dealerGamma == null && (
                <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono, marginTop: '8px' }}>
                    No ETF options data available.
                </div>
            )}
        </div>
    );
}

// ── Cross-Reference Pills ────────────────────────────────────────────
function CrossRefPills({ edges }) {
    const counts = useMemo(() => {
        const c = {};
        for (const e of edges || []) {
            if (!e || !e.type) continue;
            c[e.type] = (c[e.type] || 0) + 1;
        }
        return c;
    }, [edges]);

    const entries = Object.entries(counts)
        .filter(([, v]) => v > 0)
        .sort((a, b) => b[1] - a[1]);

    if (entries.length === 0) {
        return (
            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                No cross-reference events yet.
            </div>
        );
    }
    return (
        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
            {entries.map(([type, count]) => {
                const meta = EDGE_META[type] || { color: colors.textDim, label: type };
                return (
                    <span
                        key={type}
                        style={{
                            display: 'inline-flex', alignItems: 'center', gap: '6px',
                            background: `${meta.color}18`,
                            border: `1px solid ${meta.color}55`,
                            color: meta.color,
                            padding: '4px 10px',
                            borderRadius: tokens.radius.pill,
                            fontSize: '10px',
                            fontFamily: mono,
                            fontWeight: 600,
                        }}
                    >
                        <span style={{ color: colors.text, fontWeight: 700 }}>{count}</span>
                        {meta.label}
                    </span>
                );
            })}
        </div>
    );
}

function MoverRow({ actor, rank }) {
    return (
        <div style={{
            display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            padding: '6px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, width: '14px' }}>
                    {rank}
                </span>
                <div>
                    <span style={{ fontSize: '12px', fontWeight: 700, color: '#E8F0F8', fontFamily: mono }}>
                        {actor.ticker || actor.name}
                    </span>
                    <span style={{ fontSize: '10px', color: colors.textMuted, marginLeft: '6px' }}>
                        {actor.subsector}
                    </span>
                </div>
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                {actor.latest_price != null && (
                    <span style={{ fontSize: '11px', color: colors.textDim, fontFamily: mono }}>
                        ${fmt(actor.latest_price, 0)}
                    </span>
                )}
                <span style={{
                    fontSize: '12px', fontWeight: 600, fontFamily: mono,
                    color: PERF_COLOR(actor.pct_30d),
                }}>
                    {fmtPct(actor.pct_30d)}
                </span>
                <LensLinks actorId={actor.ticker || actor.name} />
            </div>
        </div>
    );
}
