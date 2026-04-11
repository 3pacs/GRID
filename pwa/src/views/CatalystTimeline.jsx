/**
 * CatalystTimeline — Forward-looking event timeline with invalidation gates.
 *
 * Shows milestones, catalysts, predictions, and hypotheses on a horizontal
 * timeline. Each event has an invalidation condition and value impact
 * linked to intrinsic/extrinsic value.
 *
 * Color-coded by type:
 *   - Green: milestones (earnings, revenue, product launches)
 *   - Cyan: catalysts (trials, FDA decisions)
 *   - Blue: predictions (oracle model calls)
 *   - Purple: hypotheses (active theses)
 *
 * Vertical position = value impact (above = positive, below = negative)
 * Size = probability/confidence
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";

const TYPE_COLORS = {
    milestone: '#10B981',
    catalyst: '#06B6D4',
    prediction: '#3B82F6',
    hypothesis: '#A855F7',
};

const STATUS_COLORS = {
    PENDING: '#F59E0B',
    ON_TRACK: '#10B981',
    AHEAD: '#22D3EE',
    BEHIND: '#F97316',
    ACHIEVED: '#10B981',
    MISSED: '#EF4444',
    CANCELLED: '#6B7280',
    hit: '#10B981',
    miss: '#EF4444',
    partial: '#F59E0B',
    pending: '#6B7280',
};

const SUBTYPE_ICONS = {
    EARNINGS_GUIDANCE: '$',
    REVENUE_GUIDANCE: 'R',
    PRODUCT_LAUNCH: '🚀',
    M_AND_A: 'M&A',
    REGULATORY: 'FDA',
    COST_TARGET: '✂',
    BUYBACK: '↩',
    DIVIDEND: 'D',
    READOUT: 'Rx',
    FDA_DECISION: 'FDA',
    ENROLLMENT_COMPLETE: 'E',
    hypothesis: 'H',
    antithesis: '¬H',
};

function escapeHtml(str) {
    if (str == null) return '';
    return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;')
        .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function formatPrice(val) {
    if (!val && val !== 0) return '--';
    return `$${Number(val).toFixed(2)}`;
}

export default function CatalystTimeline() {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const tooltipRef = useRef(null);

    const [ticker, setTicker] = useState('');
    const [searchInput, setSearchInput] = useState('');
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [dims, setDims] = useState({ width: 1200, height: 500 });
    const [selectedEvent, setSelectedEvent] = useState(null);
    const [typeFilter, setTypeFilter] = useState(new Set(['milestone', 'catalyst', 'prediction', 'hypothesis']));

    // Resize
    useEffect(() => {
        const el = containerRef.current;
        if (!el) return;
        const obs = new ResizeObserver(entries => {
            const { width, height } = entries[0].contentRect;
            if (width > 0 && height > 0) setDims({ width, height });
        });
        obs.observe(el);
        return () => obs.disconnect();
    }, []);

    // Fetch data
    const loadTimeline = useCallback(async (t) => {
        if (!t) return;
        setLoading(true);
        setError(null);
        setSelectedEvent(null);
        try {
            const res = await api.getCatalystTimeline(t);
            if (res.status === 'ok') {
                setData(res);
                setTicker(res.ticker);
            } else {
                setError(res.error || 'Failed to load');
            }
        } catch (err) {
            setError(err.message);
        }
        setLoading(false);
    }, []);

    const handleSearch = (e) => {
        e.preventDefault();
        if (searchInput.trim()) loadTimeline(searchInput.trim().toUpperCase());
    };

    // D3 render
    useEffect(() => {
        if (!data || !svgRef.current) return;
        const { events, today, valuation } = data;
        const filtered = events.filter(e => typeFilter.has(e.type));
        if (!filtered.length) return;

        const { width, height } = dims;
        const margin = { top: 60, right: 40, bottom: 80, left: 60 };
        const plotW = width - margin.left - margin.right;
        const plotH = height - margin.top - margin.bottom;
        const midY = plotH / 2;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        // Parse dates
        const todayDate = new Date(today);
        const dates = filtered.map(e => new Date(e.date)).filter(d => !isNaN(d));
        if (!dates.length) return;

        const xMin = d3.min(dates);
        const xMax = d3.max(dates);
        // Pad 10% on each side
        const pad = (xMax - xMin) * 0.1 || 30 * 86400000;
        const xScale = d3.scaleTime()
            .domain([new Date(xMin - pad), new Date(xMax.getTime() + pad)])
            .range([0, plotW]);

        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        // ── Background ──
        g.append('rect').attr('width', plotW).attr('height', plotH)
            .attr('fill', '#080C10').attr('rx', 4);

        // ── Today line ──
        const todayX = xScale(todayDate);
        g.append('line')
            .attr('x1', todayX).attr('y1', 0)
            .attr('x2', todayX).attr('y2', plotH)
            .attr('stroke', '#FFD700').attr('stroke-width', 1.5)
            .attr('stroke-dasharray', '4,4').attr('opacity', 0.6);
        g.append('text')
            .attr('x', todayX).attr('y', -6)
            .attr('text-anchor', 'middle')
            .attr('fill', '#FFD700').attr('font-size', '9px')
            .attr('font-family', MONO).text('TODAY');

        // ── Valuation band (if available) ──
        if (valuation?.fair_value_low && valuation?.fair_value_high) {
            const bandY = midY - 20;
            g.append('rect')
                .attr('x', 0).attr('y', bandY)
                .attr('width', plotW).attr('height', 40)
                .attr('fill', '#10B98110').attr('rx', 2);
            g.append('text')
                .attr('x', 8).attr('y', bandY + 14)
                .attr('fill', '#10B981').attr('font-size', '8px')
                .attr('font-family', MONO).attr('opacity', 0.8)
                .text(`Fair Value: ${formatPrice(valuation.fair_value_low)} – ${formatPrice(valuation.fair_value_mid)} – ${formatPrice(valuation.fair_value_high)}`);
            if (valuation.margin_of_safety) {
                const mosColor = valuation.margin_of_safety > 0 ? '#10B981' : '#EF4444';
                g.append('text')
                    .attr('x', 8).attr('y', bandY + 28)
                    .attr('fill', mosColor).attr('font-size', '8px')
                    .attr('font-family', MONO).attr('opacity', 0.8)
                    .text(`MOS: ${(valuation.margin_of_safety * 100).toFixed(0)}% | Live: ${formatPrice(valuation.live_price)}`);
            }
        }

        // ── X axis ──
        const xAxis = d3.axisBottom(xScale)
            .ticks(d3.timeMonth.every(1))
            .tickFormat(d3.timeFormat('%b %y'));
        g.append('g')
            .attr('transform', `translate(0,${plotH})`)
            .call(xAxis)
            .selectAll('text')
            .attr('fill', colors.textMuted).attr('font-family', MONO).attr('font-size', '8px');
        g.selectAll('.domain, .tick line').attr('stroke', colors.border);

        // ── Center line ──
        g.append('line')
            .attr('x1', 0).attr('y1', midY)
            .attr('x2', plotW).attr('y2', midY)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);

        // ── Event markers ──
        const tooltip = d3.select(tooltipRef.current);

        // Spread overlapping events vertically
        const eventData = filtered.map((e, i) => {
            const d = new Date(e.date);
            const x = xScale(d);
            const impact = e.value_impact_pct || 0;
            const conf = e.confidence || e.probability || 0.5;
            // Positive impact = above center, negative = below
            const yOffset = impact > 0 ? -Math.abs(impact) * plotH * 0.3
                : impact < 0 ? Math.abs(impact) * plotH * 0.3
                : (i % 2 === 0 ? -1 : 1) * (30 + (i % 5) * 20);
            return { ...e, x, y: midY + yOffset, r: 6 + conf * 10, dateObj: d };
        });

        // Past/future split
        const pastG = g.append('g').attr('class', 'past');
        const futureG = g.append('g').attr('class', 'future');

        eventData.forEach(ev => {
            const isPast = ev.dateObj < todayDate;
            const parent = isPast ? pastG : futureG;
            const color = TYPE_COLORS[ev.type] || '#6B7280';
            const statusColor = STATUS_COLORS[ev.status || ev.verdict] || color;

            // Connection line to center
            parent.append('line')
                .attr('x1', ev.x).attr('y1', midY)
                .attr('x2', ev.x).attr('y2', ev.y)
                .attr('stroke', color).attr('stroke-width', 0.5)
                .attr('stroke-dasharray', isPast ? 'none' : '2,2')
                .attr('opacity', isPast ? 0.3 : 0.5);

            // Main dot
            const dot = parent.append('circle')
                .attr('cx', ev.x).attr('cy', ev.y)
                .attr('r', ev.r)
                .attr('fill', isPast ? statusColor : color)
                .attr('fill-opacity', isPast ? 0.6 : 0.85)
                .attr('stroke', isPast ? statusColor : '#fff')
                .attr('stroke-width', isPast ? 1 : 1.5)
                .attr('stroke-opacity', 0.5)
                .style('cursor', 'pointer');

            if (!isPast) {
                dot.style('filter', `drop-shadow(0 0 ${ev.r}px ${color}40)`);
            }

            // Icon/label inside dot
            const icon = SUBTYPE_ICONS[ev.subtype] || ev.type[0].toUpperCase();
            if (ev.r > 8) {
                parent.append('text')
                    .attr('x', ev.x).attr('y', ev.y + 3)
                    .attr('text-anchor', 'middle')
                    .attr('fill', '#fff').attr('font-size', `${Math.min(ev.r, 10)}px`)
                    .attr('font-family', MONO).attr('font-weight', 700)
                    .style('pointer-events', 'none')
                    .text(icon.length <= 3 ? icon : icon[0]);
            }

            // Short label below
            parent.append('text')
                .attr('x', ev.x).attr('y', ev.y + ev.r + 10)
                .attr('text-anchor', 'middle')
                .attr('fill', colors.textMuted).attr('font-size', '7px')
                .attr('font-family', MONO)
                .text((ev.label || '').slice(0, 20));

            // Tooltip
            dot.on('mouseenter', (event) => {
                setSelectedEvent(ev);
                const confPct = ev.confidence || ev.probability;
                const confStr = confPct ? `${(confPct * 100).toFixed(0)}%` : '--';
                tooltip.style('opacity', 1)
                    .style('left', `${event.clientX + 16}px`)
                    .style('top', `${event.clientY - 16}px`)
                    .html(`
                        <div style="font-weight:700;font-size:12px;color:${color};margin-bottom:4px">
                            ${escapeHtml(ev.type.toUpperCase())} · ${escapeHtml(ev.subtype || '')}
                        </div>
                        <div style="font-size:11px;margin-bottom:6px">${escapeHtml(ev.label)}</div>
                        <div style="font-size:9px;color:${colors.textMuted}">
                            Date: ${ev.date} · Confidence: ${confStr}
                        </div>
                        ${ev.value_impact_ps ? `<div style="font-size:9px;color:#10B981;margin-top:2px">Value Impact: ${ev.value_impact_ps > 0 ? '+' : ''}$${ev.value_impact_ps.toFixed(2)}/sh (${ev.value_impact_pct ? (ev.value_impact_pct * 100).toFixed(1) + '%' : '--'})</div>` : ''}
                        ${ev.status ? `<div style="font-size:9px;color:${statusColor};margin-top:2px">Status: ${ev.status}${ev.verdict ? ' · Verdict: ' + ev.verdict : ''}</div>` : ''}
                        <div style="font-size:9px;color:#F59E0B;margin-top:6px;border-top:1px solid ${colors.border};padding-top:4px">
                            ⚠ INVALIDATION: ${escapeHtml(ev.invalidation || 'None defined')}
                        </div>
                    `);
            })
            .on('mousemove', (event) => {
                tooltip.style('left', `${event.clientX + 16}px`).style('top', `${event.clientY - 16}px`);
            })
            .on('mouseleave', () => {
                tooltip.style('opacity', 0);
            })
            .on('click', () => setSelectedEvent(ev));

            // Invalidation marker (small red triangle below dot)
            if (ev.invalidation && !isPast) {
                parent.append('polygon')
                    .attr('points', `${ev.x},${ev.y + ev.r + 2} ${ev.x - 4},${ev.y + ev.r + 8} ${ev.x + 4},${ev.y + ev.r + 8}`)
                    .attr('fill', '#EF4444').attr('opacity', 0.6)
                    .style('pointer-events', 'none');
            }
        });

    }, [data, dims, typeFilter]);

    const toggleFilter = (type) => {
        setTypeFilter(prev => {
            const next = new Set(prev);
            if (next.has(type)) next.delete(type); else next.add(type);
            return next;
        });
    };

    return (
        <div style={{ display: 'flex', flexDirection: 'column', height: 'calc(100vh - 70px)', background: colors.bg }}>
            {/* Header */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: '12px',
                padding: '10px 16px', borderBottom: `1px solid ${colors.border}`,
                background: colors.card,
            }}>
                <span style={{ fontWeight: 700, fontSize: '13px', fontFamily: MONO, color: colors.text }}>
                    CATALYST TIMELINE
                </span>

                {/* Search */}
                <form onSubmit={handleSearch} style={{ display: 'flex', gap: '4px', marginLeft: '16px' }}>
                    <input
                        value={searchInput}
                        onChange={e => setSearchInput(e.target.value.toUpperCase())}
                        placeholder="Ticker..."
                        style={{
                            background: colors.bg, border: `1px solid ${colors.border}`,
                            borderRadius: '4px', padding: '4px 10px', fontSize: '11px',
                            color: colors.text, fontFamily: MONO, width: '100px', outline: 'none',
                        }}
                    />
                    <button type="submit" style={{
                        background: '#3B82F6', border: 'none', borderRadius: '4px',
                        padding: '4px 12px', fontSize: '10px', color: '#fff',
                        cursor: 'pointer', fontFamily: MONO, fontWeight: 700,
                    }}>GO</button>
                </form>

                {/* Type filters */}
                <div style={{ display: 'flex', gap: '4px', marginLeft: '16px' }}>
                    {Object.entries(TYPE_COLORS).map(([type, col]) => (
                        <button key={type} onClick={() => toggleFilter(type)} style={{
                            background: typeFilter.has(type) ? `${col}25` : 'transparent',
                            border: `1px solid ${typeFilter.has(type) ? col : colors.border}`,
                            borderRadius: '4px', padding: '3px 8px', fontSize: '9px',
                            color: typeFilter.has(type) ? col : colors.textMuted,
                            cursor: 'pointer', fontFamily: MONO, fontWeight: typeFilter.has(type) ? 700 : 400,
                        }}>{type}</button>
                    ))}
                </div>

                {/* Stats */}
                {data && (
                    <div style={{ marginLeft: 'auto', display: 'flex', gap: '12px', fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                        <span>{data.ticker}</span>
                        <span>{data.event_count} events</span>
                        {data.valuation?.live_price && <span>${data.valuation.live_price.toFixed(2)}</span>}
                        {data.valuation?.margin_of_safety != null && (
                            <span style={{ color: data.valuation.margin_of_safety > 0 ? '#10B981' : '#EF4444' }}>
                                MOS: {(data.valuation.margin_of_safety * 100).toFixed(0)}%
                            </span>
                        )}
                    </div>
                )}
            </div>

            {/* Main area: timeline + detail panel */}
            <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
                {/* Timeline */}
                <div ref={containerRef} style={{ flex: 1, position: 'relative', overflow: 'hidden' }}>
                    {loading && (
                        <div style={{
                            position: 'absolute', inset: 0, display: 'flex',
                            alignItems: 'center', justifyContent: 'center',
                            color: colors.textMuted, fontSize: '12px', fontFamily: MONO,
                        }}>Loading catalyst timeline for {searchInput}...</div>
                    )}
                    {error && (
                        <div style={{
                            position: 'absolute', inset: 0, display: 'flex',
                            alignItems: 'center', justifyContent: 'center',
                            color: '#EF4444', fontSize: '12px', fontFamily: MONO,
                        }}>{error}</div>
                    )}
                    {!data && !loading && !error && (
                        <div style={{
                            position: 'absolute', inset: 0, display: 'flex',
                            alignItems: 'center', justifyContent: 'center',
                            flexDirection: 'column', gap: '12px',
                            color: colors.textMuted, fontSize: '13px', fontFamily: MONO,
                        }}>
                            <div style={{ fontSize: '24px', opacity: 0.3 }}>⏱</div>
                            <div>Enter a ticker to see the catalyst timeline</div>
                            <div style={{ fontSize: '10px', color: colors.textDim }}>
                                Milestones · Catalysts · Predictions · Hypotheses
                            </div>
                        </div>
                    )}
                    <svg ref={svgRef} width={dims.width} height={dims.height}
                        style={{ display: 'block', background: colors.bg }} />
                    {/* Tooltip */}
                    <div ref={tooltipRef} style={{
                        position: 'fixed', pointerEvents: 'none',
                        background: '#1E293B', border: `1px solid ${colors.border}`,
                        borderRadius: '6px', padding: '10px 14px',
                        opacity: 0, transition: 'opacity 0.15s',
                        zIndex: 1000, maxWidth: '340px',
                        fontFamily: MONO, color: colors.text, fontSize: '11px',
                    }} />
                </div>

                {/* Detail panel (when event selected) */}
                {selectedEvent && (
                    <div style={{
                        width: '320px', minWidth: '280px',
                        background: colors.card, borderLeft: `1px solid ${colors.border}`,
                        overflowY: 'auto', padding: '16px',
                        display: 'flex', flexDirection: 'column', gap: '12px',
                    }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <span style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                color: TYPE_COLORS[selectedEvent.type], fontFamily: MONO,
                            }}>{selectedEvent.type.toUpperCase()}</span>
                            <button onClick={() => setSelectedEvent(null)} style={{
                                background: 'none', border: 'none', color: colors.textMuted,
                                cursor: 'pointer', fontSize: '14px',
                            }}>×</button>
                        </div>

                        <div style={{ fontSize: '13px', fontWeight: 700, color: colors.text }}>
                            {selectedEvent.label}
                        </div>

                        <div style={{ fontSize: '10px', color: colors.textMuted }}>
                            {selectedEvent.date} · {selectedEvent.subtype}
                        </div>

                        {/* Key metrics */}
                        <div style={{ borderTop: `1px solid ${colors.border}`, paddingTop: '8px' }}>
                            {selectedEvent.confidence != null && (
                                <Row label="Confidence" value={`${(selectedEvent.confidence * 100).toFixed(0)}%`}
                                    color={selectedEvent.confidence > 0.7 ? '#10B981' : selectedEvent.confidence > 0.4 ? '#F59E0B' : '#EF4444'} />
                            )}
                            {selectedEvent.probability != null && (
                                <Row label="Probability" value={`${(selectedEvent.probability * 100).toFixed(0)}%`}
                                    color={selectedEvent.probability > 0.7 ? '#10B981' : '#F59E0B'} />
                            )}
                            {selectedEvent.value_impact_ps != null && (
                                <Row label="Value Impact" value={`${selectedEvent.value_impact_ps > 0 ? '+' : ''}$${selectedEvent.value_impact_ps.toFixed(2)}/sh`}
                                    color={selectedEvent.value_impact_ps > 0 ? '#10B981' : '#EF4444'} />
                            )}
                            {selectedEvent.target_price != null && (
                                <Row label="Target Price" value={formatPrice(selectedEvent.target_price)} color="#3B82F6" />
                            )}
                            {selectedEvent.entry_price != null && (
                                <Row label="Entry Price" value={formatPrice(selectedEvent.entry_price)} color={colors.textMuted} />
                            )}
                            {selectedEvent.status && (
                                <Row label="Status" value={selectedEvent.status}
                                    color={STATUS_COLORS[selectedEvent.status] || colors.textMuted} />
                            )}
                            {selectedEvent.verdict && (
                                <Row label="Verdict" value={selectedEvent.verdict}
                                    color={STATUS_COLORS[selectedEvent.verdict] || colors.textMuted} />
                            )}
                            {selectedEvent.accuracy != null && (
                                <Row label="Accuracy" value={`${(selectedEvent.accuracy * 100).toFixed(0)}% (${selectedEvent.correct}/${selectedEvent.tested})`}
                                    color={selectedEvent.accuracy > 0.6 ? '#10B981' : '#EF4444'} />
                            )}
                        </div>

                        {/* Invalidation */}
                        <div style={{
                            background: '#EF444415', border: '1px solid #EF444440',
                            borderRadius: '6px', padding: '10px',
                        }}>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1px',
                                color: '#EF4444', fontFamily: MONO, marginBottom: '4px',
                            }}>INVALIDATION</div>
                            <div style={{ fontSize: '11px', color: '#FCA5A5', lineHeight: 1.4 }}>
                                {selectedEvent.invalidation || 'No invalidation defined'}
                            </div>
                        </div>

                        {/* Notes */}
                        {selectedEvent.notes && (
                            <div style={{ fontSize: '10px', color: colors.textDim, lineHeight: 1.4 }}>
                                {selectedEvent.notes}
                            </div>
                        )}
                    </div>
                )}
            </div>

            {/* Legend */}
            <div style={{
                display: 'flex', gap: '16px', padding: '6px 16px',
                fontSize: '8px', color: colors.textMuted, fontFamily: MONO,
                borderTop: `1px solid ${colors.border}`,
                flexWrap: 'wrap', flexShrink: 0, alignItems: 'center',
            }}>
                {Object.entries(TYPE_COLORS).map(([type, col]) => (
                    <span key={type} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                        <span style={{ width: 8, height: 8, borderRadius: '50%', background: col, display: 'inline-block' }} />
                        {type}
                    </span>
                ))}
                <span style={{ margin: '0 4px', color: colors.border }}>|</span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <span style={{ width: 0, height: 0, borderLeft: '4px solid transparent', borderRight: '4px solid transparent', borderBottom: '6px solid #EF4444', display: 'inline-block' }} />
                    invalidation gate
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <span style={{ width: 12, height: 1, background: '#FFD700', display: 'inline-block' }} />
                    today
                </span>
                <span>above center = positive value impact | below = negative</span>
            </div>
        </div>
    );
}

function Row({ label, value, color }) {
    return (
        <div style={{
            display: 'flex', justifyContent: 'space-between', padding: '4px 0',
            borderBottom: `1px solid ${colors.borderSubtle}`, fontSize: '10px',
        }}>
            <span style={{ color: colors.textMuted }}>{label}</span>
            <span style={{ color: color || colors.text, fontWeight: 600, fontFamily: "'JetBrains Mono', monospace" }}>{value}</span>
        </div>
    );
}
