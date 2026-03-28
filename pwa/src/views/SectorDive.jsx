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

// ── Component ────────────────────────────────────────────────────────
export default function SectorDive({ sector: sectorProp, onBack }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const treemapRef = useRef(null);
    const chartRef = useRef(null);
    const [selectedSub, setSelectedSub] = useState(null);

    // Derive sector name from prop or URL hash
    const sectorName = useMemo(() => {
        if (sectorProp) return sectorProp;
        const hash = window.location.hash;
        const m = hash.match(/sector-dive\/(.+)/);
        return m ? decodeURIComponent(m[1]) : 'Technology';
    }, [sectorProp]);

    useEffect(() => {
        loadData();
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
            .style('cursor', 'pointer');

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

            {/* ═══ TOP METRICS ROW ═══ */}
            <div style={{ ...shared.metricGrid, gridTemplateColumns: 'repeat(auto-fit, minmax(130px, 1fr))', marginBottom: tokens.space.lg }}>
                <MetricCard label="Relative Strength (1M)" value={fmtPct(metrics.relative_strength_1m)} color={PERF_COLOR(metrics.relative_strength_1m)} />
                <MetricCard label="ETF Flow (5D)" value={fmtUSD(metrics.etf_flow_5d)} color={metrics.etf_flow_5d >= 0 ? colors.green : colors.red} />
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
                                            <span style={{ fontWeight: 600, color: colors.text }}>{t.ticker}</span>
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
                                            <span style={{ fontWeight: 600, color: colors.text }}>{t.ticker}</span>
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
            </div>
        </div>
    );
}
