/**
 * WhyView -- "Why did this move?"
 *
 * The capstone forensic reconstruction view. Given a ticker and a significant
 * price move, shows WHO moved the money, HOW MUCH, WHEN, and WHY.
 *
 * Layout:
 *   1. Header: "WHY DID [TICKER] [MOVE]?" with price change badge
 *   2. The Move: price chart with highlighted move period
 *   3. The Timeline: D3 zoomed timeline with causal connection lines
 *   4. The Actors: who was active, dollar amounts, motivation
 *   5. The Causes: what events explain why actors traded
 *   6. The Dollar Story: total flow breakdown pie chart
 *   7. The Narrative: LLM-generated forensic story
 *   8. Pattern Match: recurring pattern badge
 *
 * API:
 *   GET  /api/v1/intelligence/forensics/{ticker}?days=90
 *   POST /api/v1/intelligence/forensics/{ticker}/analyze?date=YYYY-MM-DD
 *   GET  /api/v1/intelligence/causation?ticker=TICKER
 *   GET  /api/v1/intelligence/events?ticker=TICKER&days=30
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

// ── Constants ─────────────────────────────────────────────────────────────────

const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

const EVENT_TYPE_CONFIG = {
    congressional: { color: '#FFD700', shape: 'diamond', label: 'Congress' },
    insider:       { color: '#3B82F6', shape: 'circle',  label: 'Insider' },
    dark_pool:     { color: '#A855F7', shape: 'square',  label: 'Dark Pool' },
    whale:         { color: '#10B981', shape: 'triangle', label: 'Whale Opts' },
    news:          { color: '#6B7280', shape: 'dot',     label: 'News' },
    earnings:      { color: '#F97316', shape: 'star',    label: 'Earnings' },
    macro:         { color: '#EF4444', shape: 'diamond', label: 'FOMC/CPI' },
    crossref:      { color: '#EF4444', shape: 'diamond', label: 'Cross-Ref' },
    regime:        { color: '#8B5CF6', shape: 'band',    label: 'Regime' },
    prediction:    { color: '#06B6D4', shape: 'dot',     label: 'Prediction' },
    contract:      { color: '#F59E0B', shape: 'star',    label: 'Contract' },
    legislation:   { color: '#EC4899', shape: 'diamond', label: 'Legislation' },
};

const SOURCE_TYPE_COLORS = {
    congressional: '#FFD700',
    insider: '#3B82F6',
    darkpool: '#A855F7',
    dark_pool: '#A855F7',
    options_flow: '#10B981',
    prediction_market: '#06B6D4',
    '13f': '#F97316',
    etf_flow: '#EC4899',
    unknown: '#6B7280',
};

const TIMELINE_HEIGHT = 300;
const TIMELINE_MARGIN = { top: 24, right: 60, bottom: 40, left: 64 };
const EVENT_LANE_H = 44;
const PIE_SIZE = 160;

// ── Helpers ───────────────────────────────────────────────────────────────────

function formatUSD(val) {
    if (val == null || val === 0) return '--';
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

function formatPct(pct) {
    if (pct == null) return '--';
    const sign = pct >= 0 ? '+' : '';
    return `${sign}${pct.toFixed(2)}%`;
}

function formatLeadTime(hours) {
    if (hours == null) return '--';
    if (hours < 1) return `${Math.round(hours * 60)}m before`;
    if (hours < 24) return `${hours.toFixed(0)}h before`;
    const days = hours / 24;
    return `${days.toFixed(0)}d before`;
}

function formatLeadDays(hours) {
    if (hours == null) return null;
    const days = Math.round(hours / 24);
    if (days === 0) return 'same day';
    return `${days} day${days !== 1 ? 's' : ''} before`;
}

function dirColor(dir) {
    if (dir === 'bullish' || dir === 'up' || dir === 'inflow') return colors.green;
    if (dir === 'bearish' || dir === 'down' || dir === 'outflow') return colors.red;
    return colors.textMuted;
}

function confidenceBadgeBg(c) {
    const map = {
        confirmed: { bg: '#1A7A4A', text: '#10B981' },
        derived:   { bg: '#1A3A6A', text: '#3B82F6' },
        estimated: { bg: '#3A3A1A', text: '#F59E0B' },
        rumored:   { bg: '#3B1111', text: '#EF4444' },
        inferred:  { bg: '#2A1A3A', text: '#A855F7' },
    };
    return map[c] || map.estimated;
}

// ── Main Component ────────────────────────────────────────────────────────────

export default function WhyView({ onNavigate }) {
    const timelineRef = useRef(null);
    const timelineSvgRef = useRef(null);
    const tooltipRef = useRef(null);
    const pieRef = useRef(null);

    const [width, setWidth] = useState(900);
    const [tickerInput, setTickerInput] = useState('');
    const [ticker, setTicker] = useState('');
    const [watchlist, setWatchlist] = useState([]);

    // Data
    const [significantMoves, setSignificantMoves] = useState([]);
    const [selectedMove, setSelectedMove] = useState(null);
    const [forensicReport, setForensicReport] = useState(null);
    const [causalLinks, setCausalLinks] = useState([]);
    const [timelineEvents, setTimelineEvents] = useState([]);

    const [loading, setLoading] = useState(false);
    const [analyzing, setAnalyzing] = useState(false);
    const [error, setError] = useState(null);
    const [animPhase, setAnimPhase] = useState(0); // 0=idle, 1=animating

    // Load watchlist
    useEffect(() => {
        api.getWatchlist().then(res => {
            if (res && !res.error && Array.isArray(res)) {
                setWatchlist(res.map(w => w.ticker).filter(Boolean));
            } else if (res && res.watchlist) {
                setWatchlist(res.watchlist.map(w => w.ticker).filter(Boolean));
            }
        });
    }, []);

    // Responsive width
    useEffect(() => {
        if (!timelineRef.current) return;
        const obs = new ResizeObserver(entries => {
            for (const e of entries) {
                if (e.contentRect.width > 0) setWidth(e.contentRect.width);
            }
        });
        obs.observe(timelineRef.current);
        setWidth(timelineRef.current.clientWidth || 900);
        return () => obs.disconnect();
    }, []);

    // ── Step 1: Load forensic reports (significant moves) ───────────────
    const loadForensics = useCallback(async (t) => {
        if (!t) return;
        setLoading(true);
        setError(null);
        setSelectedMove(null);
        setForensicReport(null);
        setCausalLinks([]);
        setTimelineEvents([]);
        setAnimPhase(0);

        try {
            const res = await api.getForensicReports(t, 90);
            if (res && !res.error) {
                const reports = res.reports || res || [];
                setSignificantMoves(Array.isArray(reports) ? reports : []);
            } else {
                setError(res?.message || 'Failed to load forensic data');
            }
        } catch (err) {
            setError(err.message);
        }
        setLoading(false);
    }, []);

    const handleSearch = () => {
        const t = tickerInput.trim().toUpperCase();
        if (!t) return;
        setTicker(t);
        loadForensics(t);
    };

    // ── Step 2: When user selects a move, load full analysis ────────────
    const loadMoveAnalysis = useCallback(async (move) => {
        setSelectedMove(move);
        setAnalyzing(true);
        setAnimPhase(0);

        try {
            // If we already have a full forensic report from the list, use it
            let report = move;
            if (!move.preceding_events || move.preceding_events.length === 0) {
                // Trigger on-demand analysis
                const res = await api.analyzeForensicMove(ticker, move.move_date);
                if (res && !res.error) {
                    report = res.report || res;
                }
            }
            setForensicReport(report);

            // Load causal links and timeline in parallel
            const [causalRes, eventsRes] = await Promise.all([
                api.getCausalLinks(ticker),
                api.getEventTimeline(ticker, 30),
            ]);

            if (causalRes && !causalRes.error) {
                const links = causalRes.causes || causalRes.links || causalRes || [];
                setCausalLinks(Array.isArray(links) ? links : []);
            }
            if (eventsRes && !eventsRes.error) {
                const evts = eventsRes.events || [];
                setTimelineEvents(evts);
            }

            // Trigger animation
            setTimeout(() => setAnimPhase(1), 100);
        } catch (err) {
            setError(err.message);
        }
        setAnalyzing(false);
    }, [ticker]);

    // ── Derived data ────────────────────────────────────────────────────

    const moveDate = selectedMove ? new Date(selectedMove.move_date) : null;
    const movePct = selectedMove?.move_pct || 0;
    const moveDir = selectedMove?.move_direction || (movePct >= 0 ? 'up' : 'down');
    const isUp = moveDir === 'up';

    // Events preceding the move (from forensic report)
    const precedingEvents = useMemo(() => {
        if (!forensicReport?.preceding_events) return [];
        return [...forensicReport.preceding_events].sort((a, b) =>
            new Date(a.timestamp) - new Date(b.timestamp)
        );
    }, [forensicReport]);

    // Actors sorted by dollar amount
    const actors = useMemo(() => {
        if (!precedingEvents.length) return [];
        const actorMap = {};
        precedingEvents.forEach(ev => {
            const name = ev.actor || 'Unknown';
            if (!actorMap[name]) {
                actorMap[name] = {
                    name,
                    actions: [],
                    totalAmount: 0,
                    earliestLeadTime: null,
                    direction: ev.direction,
                    confidence: ev.confidence,
                };
            }
            actorMap[name].actions.push(ev);
            actorMap[name].totalAmount += (ev.amount_usd || 0);
            const lt = ev.lead_time_hours;
            if (lt != null && (actorMap[name].earliestLeadTime == null || lt > actorMap[name].earliestLeadTime)) {
                actorMap[name].earliestLeadTime = lt;
            }
        });
        return Object.values(actorMap).sort((a, b) => b.totalAmount - a.totalAmount);
    }, [precedingEvents]);

    // Causes from causal links, filtered to this ticker
    const causes = useMemo(() => {
        if (!causalLinks.length) return [];
        return causalLinks
            .filter(c => c.ticker === ticker)
            .sort((a, b) => (b.probability || 0) - (a.probability || 0));
    }, [causalLinks, ticker]);

    // Dollar flow breakdown by source type
    const flowBreakdown = useMemo(() => {
        if (!precedingEvents.length) return [];
        const bySource = {};
        precedingEvents.forEach(ev => {
            const src = ev.source || ev.event_type || 'unknown';
            if (!bySource[src]) bySource[src] = 0;
            bySource[src] += (ev.amount_usd || 0);
        });
        return Object.entries(bySource)
            .filter(([, v]) => v > 0)
            .map(([source, amount]) => ({ source, amount }))
            .sort((a, b) => b.amount - a.amount);
    }, [precedingEvents]);

    const totalFlow = forensicReport?.total_dollar_flow || 0;

    // ── Price data from timeline events ─────────────────────────────────
    const priceData = useMemo(() => {
        return timelineEvents
            .filter(e => e.event_type === 'price_move')
            .map(e => {
                const match = e.description?.match(/\$[\d.]+\s*->\s*\$([\d.]+)/);
                return {
                    date: new Date(e.timestamp),
                    price: match ? parseFloat(match[1]) : null,
                };
            })
            .filter(d => d.price != null)
            .sort((a, b) => a.date - b.date);
    }, [timelineEvents]);

    // ── D3 Timeline Render ──────────────────────────────────────────────
    useEffect(() => {
        if (!timelineSvgRef.current || !moveDate || precedingEvents.length === 0) return;

        const svg = d3.select(timelineSvgRef.current);
        svg.selectAll('*').remove();

        const totalH = TIMELINE_HEIGHT + EVENT_LANE_H;
        svg.attr('width', width).attr('height', totalH);

        const chartW = width - TIMELINE_MARGIN.left - TIMELINE_MARGIN.right;
        const chartH = TIMELINE_HEIGHT - TIMELINE_MARGIN.top - TIMELINE_MARGIN.bottom;

        const g = svg.append('g')
            .attr('transform', `translate(${TIMELINE_MARGIN.left},${TIMELINE_MARGIN.top})`);

        // Time domain: 14 days before move to move date
        const windowStart = new Date(moveDate);
        windowStart.setDate(windowStart.getDate() - 14);
        const xScale = d3.scaleTime()
            .domain([windowStart, moveDate])
            .range([0, chartW]);

        // If we have price data, draw the price line
        const relevantPrices = priceData.filter(d => d.date >= windowStart && d.date <= moveDate);
        let yScale;
        if (relevantPrices.length >= 2) {
            const priceExtent = d3.extent(relevantPrices, d => d.price);
            const pad = (priceExtent[1] - priceExtent[0]) * 0.15 || 1;
            yScale = d3.scaleLinear()
                .domain([priceExtent[0] - pad, priceExtent[1] + pad])
                .range([chartH, 0]);

            // Grid lines
            const yTicks = yScale.ticks(5);
            g.selectAll('.grid-h').data(yTicks).enter()
                .append('line')
                .attr('x1', 0).attr('x2', chartW)
                .attr('y1', d => yScale(d)).attr('y2', d => yScale(d))
                .attr('stroke', colors.border).attr('stroke-width', 0.4).attr('opacity', 0.5);

            // Price line
            const priceLine = d3.line()
                .x(d => xScale(d.date))
                .y(d => yScale(d.price))
                .curve(d3.curveMonotoneX)
                .defined(d => d.price != null);

            const path = g.append('path')
                .datum(relevantPrices)
                .attr('fill', 'none')
                .attr('stroke', '#C8D8E8')
                .attr('stroke-width', 2)
                .attr('d', priceLine);

            // Animate path drawing
            const pathLen = path.node().getTotalLength();
            path.attr('stroke-dasharray', `${pathLen} ${pathLen}`)
                .attr('stroke-dashoffset', pathLen)
                .transition().duration(1200).ease(d3.easeCubicOut)
                .attr('stroke-dashoffset', 0);

            // Area fill
            const area = d3.area()
                .x(d => xScale(d.date))
                .y0(chartH)
                .y1(d => yScale(d.price))
                .curve(d3.curveMonotoneX)
                .defined(d => d.price != null);

            g.append('path')
                .datum(relevantPrices)
                .attr('d', area)
                .attr('fill', isUp ? `${colors.green}08` : `${colors.red}08`);

            // Y axis
            g.append('g')
                .attr('transform', `translate(${chartW},0)`)
                .call(d3.axisRight(yScale).ticks(5).tickFormat(d => `$${d.toFixed(0)}`))
                .call(g2 => g2.select('.domain').remove())
                .call(g2 => g2.selectAll('text').attr('fill', colors.textMuted).attr('font-size', '9px').attr('font-family', MONO))
                .call(g2 => g2.selectAll('line').attr('stroke', colors.border).attr('opacity', 0.3));
        }

        // ── Move date vertical line (THE MOVE) ──
        const moveX = xScale(moveDate);
        g.append('line')
            .attr('x1', moveX).attr('x2', moveX)
            .attr('y1', 0).attr('y2', chartH)
            .attr('stroke', isUp ? colors.green : colors.red)
            .attr('stroke-width', 2)
            .attr('stroke-dasharray', '6,3')
            .attr('opacity', 0.8);

        // Move label
        g.append('text')
            .attr('x', moveX).attr('y', -8)
            .attr('text-anchor', 'middle')
            .attr('fill', isUp ? colors.green : colors.red)
            .attr('font-size', '11px')
            .attr('font-weight', 700)
            .attr('font-family', MONO)
            .text(`THE MOVE ${formatPct(movePct)}`);

        // "Before" and "After" labels
        g.append('text')
            .attr('x', moveX - 20).attr('y', chartH + 4)
            .attr('text-anchor', 'end')
            .attr('fill', colors.textMuted)
            .attr('font-size', '9px')
            .attr('font-family', MONO)
            .text('BEFORE');

        g.append('text')
            .attr('x', moveX + 20).attr('y', chartH + 4)
            .attr('text-anchor', 'start')
            .attr('fill', colors.textMuted)
            .attr('font-size', '9px')
            .attr('font-family', MONO)
            .text('AFTER');

        // ── Event markers in the lane below ──
        const eventG = g.append('g')
            .attr('transform', `translate(0,${chartH + 14})`);

        eventG.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);

        const markerEvents = precedingEvents.filter(e =>
            e.event_type !== 'price_move' && e.event_type !== 'regime'
        );

        const maxAmt = d3.max(markerEvents, e => e.amount_usd || 0) || 1;
        const sizeScale = d3.scaleSqrt().domain([0, maxAmt]).range([4, 12]);

        // Draw events sequentially with animation
        markerEvents.forEach((ev, idx) => {
            const cfg = EVENT_TYPE_CONFIG[ev.event_type] || EVENT_TYPE_CONFIG.news;
            const evDate = new Date(ev.timestamp);
            const ex = xScale(evDate);
            if (ex < 0 || ex > chartW) return;

            const sz = ev.amount_usd ? sizeScale(ev.amount_usd) : 5;
            const cy = EVENT_LANE_H / 2;
            const fill = cfg.color;
            const delay = animPhase === 1 ? idx * 120 : 0;

            // Marker shape
            let marker;
            if (cfg.shape === 'diamond') {
                marker = eventG.append('polygon')
                    .attr('points', `${ex},${cy - sz} ${ex + sz},${cy} ${ex},${cy + sz} ${ex - sz},${cy}`)
                    .attr('fill', fill);
            } else if (cfg.shape === 'circle') {
                marker = eventG.append('circle')
                    .attr('cx', ex).attr('cy', cy).attr('r', sz)
                    .attr('fill', fill);
            } else if (cfg.shape === 'square') {
                marker = eventG.append('rect')
                    .attr('x', ex - sz).attr('y', cy - sz)
                    .attr('width', sz * 2).attr('height', sz * 2)
                    .attr('fill', fill);
            } else if (cfg.shape === 'triangle') {
                marker = eventG.append('polygon')
                    .attr('points', `${ex},${cy - sz} ${ex - sz},${cy + sz} ${ex + sz},${cy + sz}`)
                    .attr('fill', fill);
            } else if (cfg.shape === 'star') {
                const starPath = d3.symbol().type(d3.symbolStar).size(sz * sz * 3);
                marker = eventG.append('path')
                    .attr('d', starPath())
                    .attr('transform', `translate(${ex},${cy})`)
                    .attr('fill', fill);
            } else {
                marker = eventG.append('circle')
                    .attr('cx', ex).attr('cy', cy).attr('r', Math.max(sz * 0.6, 3))
                    .attr('fill', fill);
            }

            // Animate: fade in with delay
            if (marker) {
                marker.attr('opacity', 0)
                    .transition().delay(delay).duration(300)
                    .attr('opacity', 0.85);

                // Tooltip
                marker.style('cursor', 'pointer')
                    .on('mouseenter', function (event) {
                        d3.select(this).attr('opacity', 1).attr('stroke', '#fff').attr('stroke-width', 1.5);
                        if (tooltipRef.current) {
                            const tt = tooltipRef.current;
                            tt.style.display = 'block';
                            const dateStr = evDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                            const leadDays = formatLeadDays(ev.lead_time_hours);
                            tt.innerHTML = `
                                <div style="display:flex;align-items:center;gap:6px;margin-bottom:4px">
                                    <span style="width:8px;height:8px;border-radius:2px;background:${fill};display:inline-block"></span>
                                    <span style="color:${colors.text};font-weight:600;font-size:11px">${cfg.label}</span>
                                    <span style="color:${colors.textMuted};font-size:10px">${dateStr}</span>
                                </div>
                                ${ev.actor ? `<div style="color:${colors.accentLight || '#2A8EDF'};font-size:10px;font-weight:600">${ev.actor}</div>` : ''}
                                <div style="color:${colors.textDim};font-size:10px;line-height:1.4;max-width:350px;margin-top:2px">${ev.description || ''}</div>
                                ${ev.amount_usd ? `<div style="color:${colors.yellow};font-size:11px;margin-top:4px;font-weight:600">${formatUSD(ev.amount_usd)}</div>` : ''}
                                ${leadDays ? `<div style="color:${colors.textMuted};font-size:9px;margin-top:2px">${leadDays}</div>` : ''}
                            `;
                            const rect = event.target.getBoundingClientRect();
                            const containerRect = timelineRef.current.getBoundingClientRect();
                            tt.style.left = `${Math.min(rect.left - containerRect.left, containerRect.width - 280)}px`;
                            tt.style.top = `${rect.top - containerRect.top - tt.offsetHeight - 8}px`;
                        }
                    })
                    .on('mouseleave', function () {
                        d3.select(this).attr('opacity', 0.85).attr('stroke', 'none');
                        if (tooltipRef.current) tooltipRef.current.style.display = 'none';
                    });
            }

            // ── Causal connection line from event to the move ──
            const connectionLine = g.append('line')
                .attr('x1', ex).attr('y1', chartH + 14 + cy)
                .attr('x2', moveX).attr('y2', chartH / 2)
                .attr('stroke', fill)
                .attr('stroke-width', 0.8)
                .attr('stroke-dasharray', '3,3')
                .attr('opacity', 0);

            connectionLine.transition()
                .delay(delay + 200).duration(500)
                .attr('opacity', 0.25);

            // Dollar amount label on event marker
            if (ev.amount_usd && ev.amount_usd > 0) {
                const label = eventG.append('text')
                    .attr('x', ex).attr('y', cy - sz - 4)
                    .attr('text-anchor', 'middle')
                    .attr('fill', fill)
                    .attr('font-size', '8px')
                    .attr('font-weight', 600)
                    .attr('font-family', MONO)
                    .attr('opacity', 0)
                    .text(formatUSD(ev.amount_usd));

                label.transition().delay(delay + 100).duration(300).attr('opacity', 0.7);
            }

            // Lead time label
            const leadDays = formatLeadDays(ev.lead_time_hours);
            if (leadDays) {
                const ltLabel = eventG.append('text')
                    .attr('x', ex).attr('y', cy + sz + 12)
                    .attr('text-anchor', 'middle')
                    .attr('fill', colors.textMuted)
                    .attr('font-size', '7px')
                    .attr('font-family', MONO)
                    .attr('opacity', 0)
                    .text(leadDays);

                ltLabel.transition().delay(delay + 150).duration(300).attr('opacity', 0.5);
            }
        });

        // ── X Axis ──
        const xAxis = g.append('g')
            .attr('transform', `translate(0,${chartH})`)
            .call(d3.axisBottom(xScale).ticks(7).tickFormat(d3.timeFormat('%b %d')))
            .call(g2 => g2.select('.domain').attr('stroke', colors.border))
            .call(g2 => g2.selectAll('text').attr('fill', colors.textMuted).attr('font-size', '9px').attr('font-family', MONO))
            .call(g2 => g2.selectAll('line').attr('stroke', colors.border).attr('opacity', 0.5));

    }, [width, moveDate, precedingEvents, priceData, animPhase, movePct, isUp]);

    // ── Pie Chart ───────────────────────────────────────────────────────
    useEffect(() => {
        if (!pieRef.current || flowBreakdown.length === 0) return;

        const svg = d3.select(pieRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', PIE_SIZE).attr('height', PIE_SIZE);

        const radius = PIE_SIZE / 2 - 8;
        const g = svg.append('g')
            .attr('transform', `translate(${PIE_SIZE / 2},${PIE_SIZE / 2})`);

        const pie = d3.pie().value(d => d.amount).sort(null);
        const arc = d3.arc().innerRadius(radius * 0.55).outerRadius(radius);

        const arcs = g.selectAll('path')
            .data(pie(flowBreakdown))
            .enter().append('path')
            .attr('d', arc)
            .attr('fill', (d) => SOURCE_TYPE_COLORS[d.data.source] || '#6B7280')
            .attr('stroke', colors.bg)
            .attr('stroke-width', 2)
            .attr('opacity', 0)
            .transition().delay((_, i) => i * 100).duration(400)
            .attr('opacity', 0.85);

        // Center text
        g.append('text')
            .attr('text-anchor', 'middle').attr('dy', '-0.2em')
            .attr('fill', colors.text).attr('font-size', '14px')
            .attr('font-weight', 700).attr('font-family', MONO)
            .text(formatUSD(totalFlow));

        g.append('text')
            .attr('text-anchor', 'middle').attr('dy', '1.2em')
            .attr('fill', colors.textMuted).attr('font-size', '9px')
            .attr('font-family', MONO)
            .text('TOTAL FLOW');

    }, [flowBreakdown, totalFlow]);

    // ── Render ──────────────────────────────────────────────────────────

    const causeTypeIcon = (type) => {
        const icons = {
            contract: '\u{1F4DC}',
            legislation: '\u{1F3DB}',
            earnings: '\u{1F4CA}',
            hearing: '\u{1F3DB}',
            insider_knowledge: '\u{1F50D}',
            rebalancing: '\u{1F504}',
            unknown: '\u{2753}',
        };
        return icons[type] || '';
    };

    return (
        <div style={{ ...shared.container, maxWidth: '1400px' }}>
            {/* ─── Header + Search ──────────────────────────────── */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '16px', marginBottom: '16px', flexWrap: 'wrap' }}>
                <div style={{ ...shared.header, marginBottom: 0, fontSize: '20px' }}>
                    {selectedMove
                        ? <span>
                            WHY DID{' '}
                            <span style={{ color: colors.accent }}>{ticker}</span>
                            {' '}
                            <span style={{ color: isUp ? colors.green : colors.red }}>
                                {isUp ? 'RISE' : 'FALL'}
                            </span>
                            ?
                          </span>
                        : 'WHY DID THIS MOVE?'
                    }
                </div>
                {selectedMove && (
                    <span style={{
                        ...shared.badge(isUp ? colors.greenBg : colors.redBg),
                        color: isUp ? colors.green : colors.red,
                        fontSize: '16px',
                        fontWeight: 700,
                        fontFamily: MONO,
                        padding: '6px 14px',
                        border: `1px solid ${isUp ? colors.green : colors.red}33`,
                    }}>
                        {formatPct(movePct)}
                    </span>
                )}
            </div>

            {/* ─── Search Bar ─────────────────────────────────── */}
            <div style={{ display: 'flex', gap: '8px', marginBottom: '20px', flexWrap: 'wrap' }}>
                <input
                    style={{ ...shared.input, width: '160px', flex: 'none' }}
                    placeholder="Ticker (e.g. NVDA)"
                    value={tickerInput}
                    onChange={e => setTickerInput(e.target.value.toUpperCase())}
                    onKeyDown={e => e.key === 'Enter' && handleSearch()}
                />
                <button style={shared.button} onClick={handleSearch} disabled={loading}>
                    {loading ? 'Scanning...' : 'Investigate'}
                </button>
                {/* Watchlist quick picks */}
                {watchlist.length > 0 && (
                    <div style={{ display: 'flex', gap: '4px', flexWrap: 'wrap', alignItems: 'center' }}>
                        {watchlist.slice(0, 8).map(t => (
                            <button
                                key={t}
                                style={{
                                    ...shared.buttonSmall,
                                    background: t === ticker ? colors.accent : colors.card,
                                    color: t === ticker ? '#fff' : colors.textDim,
                                    padding: '6px 12px',
                                    fontSize: '11px',
                                }}
                                onClick={() => { setTickerInput(t); setTicker(t); loadForensics(t); }}
                            >
                                {t}
                            </button>
                        ))}
                    </div>
                )}
            </div>

            {error && <div style={shared.error}>{error}</div>}

            {/* ─── Move Selector ──────────────────────────────── */}
            {significantMoves.length > 0 && !selectedMove && (
                <div style={{ ...shared.card, marginBottom: '16px' }}>
                    <div style={shared.sectionTitle}>SIGNIFICANT MOVES DETECTED</div>
                    <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '12px' }}>
                        Select a move to investigate. Each row represents a day where {ticker} made a significant price move.
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                        {significantMoves.slice(0, 15).map((m, i) => {
                            const mUp = m.move_direction === 'up' || m.move_pct > 0;
                            return (
                                <div
                                    key={i}
                                    onClick={() => loadMoveAnalysis(m)}
                                    style={{
                                        ...shared.row,
                                        cursor: 'pointer',
                                        borderRadius: tokens.radius.sm,
                                        padding: '10px 12px',
                                        background: 'transparent',
                                        transition: `background ${tokens.transition.fast}`,
                                    }}
                                    onMouseEnter={e => e.currentTarget.style.background = colors.cardHover}
                                    onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                                >
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                                        <span style={{ color: colors.textDim, fontFamily: MONO, fontSize: '12px', width: '80px' }}>
                                            {m.move_date}
                                        </span>
                                        <span style={{
                                            color: mUp ? colors.green : colors.red,
                                            fontFamily: MONO,
                                            fontSize: '14px',
                                            fontWeight: 700,
                                            width: '70px',
                                        }}>
                                            {formatPct(m.move_pct)}
                                        </span>
                                        <span style={{ color: colors.textMuted, fontSize: '11px' }}>
                                            {m.warning_signals || 0} signals
                                        </span>
                                    </div>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                        {m.confidence != null && (
                                            <span style={{
                                                fontFamily: MONO,
                                                fontSize: '10px',
                                                color: m.confidence > 0.5 ? colors.green : colors.yellow,
                                            }}>
                                                {(m.confidence * 100).toFixed(0)}% conf
                                            </span>
                                        )}
                                        <span style={{ color: colors.accent, fontSize: '11px' }}>Investigate &rarr;</span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* Back button when a move is selected */}
            {selectedMove && (
                <button
                    style={{ ...shared.buttonSmall, background: colors.card, color: colors.textDim, marginBottom: '16px' }}
                    onClick={() => { setSelectedMove(null); setForensicReport(null); setCausalLinks([]); setTimelineEvents([]); setAnimPhase(0); }}
                >
                    &larr; Back to moves
                </button>
            )}

            {analyzing && (
                <div style={{
                    textAlign: 'center', padding: '60px 20px',
                    color: colors.textMuted, fontFamily: MONO, fontSize: '13px',
                }}>
                    Reconstructing what happened... Analyzing signals, actors, and causes...
                </div>
            )}

            {/* ─── THE RECONSTRUCTION ─────────────────────────── */}
            {forensicReport && !analyzing && (
                <>
                    {/* ── Section 3: The Timeline (D3) ─────────────── */}
                    <div ref={timelineRef} style={{ ...shared.cardGradient, position: 'relative', marginBottom: '16px', overflow: 'hidden' }}>
                        <div style={shared.sectionTitle}>THE TIMELINE</div>
                        <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '8px' }}>
                            14 days before the move. Events appear in sequence with causal connection lines drawn to the move.
                        </div>
                        <svg ref={timelineSvgRef} style={{ display: 'block', width: '100%' }} />
                        {/* Tooltip */}
                        <div ref={tooltipRef} style={{
                            position: 'absolute', display: 'none', zIndex: 10,
                            background: colors.glassOverlay,
                            backdropFilter: 'blur(12px)', WebkitBackdropFilter: 'blur(12px)',
                            border: `1px solid ${colors.border}`,
                            borderRadius: tokens.radius.sm,
                            padding: '10px 12px',
                            pointerEvents: 'none',
                            maxWidth: '320px',
                            boxShadow: colors.shadow?.md || '0 4px 12px rgba(0,0,0,0.4)',
                        }} />
                    </div>

                    {/* ── Two-column: Actors + Causes ──────────────── */}
                    <div style={{ display: 'grid', gridTemplateColumns: 'minmax(0,1fr) minmax(0,1fr)', gap: '12px', marginBottom: '16px' }}>

                        {/* ── Section 4: The Actors ─────────────────── */}
                        <div style={{ ...shared.card, overflow: 'hidden' }}>
                            <div style={shared.sectionTitle}>THE ACTORS</div>
                            <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '10px' }}>
                                Who was active before the move? Sorted by dollar amount.
                            </div>
                            {actors.length === 0 && (
                                <div style={{ color: colors.textMuted, fontSize: '12px', padding: '12px 0' }}>No named actors identified.</div>
                            )}
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                {actors.slice(0, 12).map((actor, i) => {
                                    // Find matching causal link for motivation
                                    const motivation = causes.find(c => c.actor === actor.name);
                                    return (
                                        <div key={i} style={{
                                            background: colors.bg,
                                            borderRadius: tokens.radius.sm,
                                            padding: '10px 12px',
                                            borderLeft: `3px solid ${dirColor(actor.direction)}`,
                                        }}>
                                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '4px' }}>
                                                <div>
                                                    <span title={actor.name} style={{ color: colors.text, fontWeight: 600, fontSize: '12px', lineHeight: '1.3' }}>
                                                        {actor.name}
                                                    </span>
                                                    {actor.actions[0]?.confidence && (
                                                        <span style={{
                                                            marginLeft: '6px',
                                                            fontSize: '9px',
                                                            padding: '4px 8px',
                                                            borderRadius: '999px',
                                                            background: confidenceBadgeBg(actor.actions[0].confidence).bg,
                                                            color: confidenceBadgeBg(actor.actions[0].confidence).text,
                                                            whiteSpace: 'nowrap',
                                                        }}>
                                                            {actor.actions[0].confidence}
                                                        </span>
                                                    )}
                                                </div>
                                                <span style={{ color: colors.yellow, fontFamily: MONO, fontSize: '12px', fontWeight: 700 }}>
                                                    {formatUSD(actor.totalAmount)}
                                                </span>
                                            </div>
                                            <div style={{ fontSize: '10px', color: colors.textDim }}>
                                                {actor.actions.length} action{actor.actions.length !== 1 ? 's' : ''}
                                                {actor.earliestLeadTime != null && (
                                                    <span style={{ marginLeft: '8px', color: colors.textMuted }}>
                                                        {formatLeadTime(actor.earliestLeadTime)}
                                                    </span>
                                                )}
                                                <span style={{ marginLeft: '8px', color: dirColor(actor.direction), fontWeight: 600 }}>
                                                    {(actor.direction || '').toUpperCase()}
                                                </span>
                                            </div>
                                            {motivation && (
                                                <div style={{
                                                    marginTop: '6px', fontSize: '10px', color: colors.textDim,
                                                    lineHeight: '1.4', fontStyle: 'italic',
                                                    borderTop: `1px solid ${colors.borderSubtle}`,
                                                    paddingTop: '6px',
                                                }}>
                                                    Probable cause: {motivation.probable_cause}
                                                    {motivation.probability != null && (
                                                        <span style={{ color: colors.yellow, marginLeft: '6px' }}>
                                                            ({(motivation.probability * 100).toFixed(0)}%)
                                                        </span>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    );
                                })}
                            </div>
                        </div>

                        {/* ── Section 5: The Causes ─────────────────── */}
                        <div style={{ ...shared.card, overflow: 'hidden' }}>
                            <div style={shared.sectionTitle}>THE CAUSES</div>
                            <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '10px' }}>
                                What events explain why actors traded?
                            </div>
                            {causes.length === 0 && (
                                <div style={{ color: colors.textMuted, fontSize: '12px', padding: '12px 0' }}>
                                    No causal links identified yet. The causation engine runs periodically.
                                </div>
                            )}
                            <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                                {causes.slice(0, 12).map((cause, i) => (
                                    <div key={i} style={{
                                        background: colors.bg,
                                        borderRadius: tokens.radius.sm,
                                        padding: '10px 12px',
                                    }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '4px' }}>
                                            <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                                <span style={{ fontSize: '14px' }}>{causeTypeIcon(cause.cause_type)}</span>
                                                <span style={{
                                                    fontSize: '10px', fontWeight: 600,
                                                    color: colors.accentLight || '#2A8EDF',
                                                    textTransform: 'uppercase',
                                                    letterSpacing: '0.5px',
                                                }}>
                                                    {(cause.cause_type || 'unknown').replace(/_/g, ' ')}
                                                </span>
                                            </div>
                                            {cause.probability != null && (
                                                <span style={{
                                                    fontFamily: MONO, fontSize: '11px', fontWeight: 700,
                                                    color: cause.probability > 0.7 ? colors.green : cause.probability > 0.4 ? colors.yellow : colors.textMuted,
                                                }}>
                                                    {(cause.probability * 100).toFixed(0)}%
                                                </span>
                                            )}
                                        </div>
                                        <div style={{ color: colors.text, fontSize: '11px', lineHeight: '1.4', marginBottom: '4px' }}>
                                            {cause.probable_cause}
                                        </div>
                                        {cause.actor && (
                                            <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                                Actor: <span style={{ color: colors.textDim }}>{cause.actor}</span>
                                                {cause.lead_time_days != null && (
                                                    <span style={{ marginLeft: '8px' }}>{cause.lead_time_days.toFixed(0)}d lead</span>
                                                )}
                                            </div>
                                        )}
                                        {cause.evidence && cause.evidence.length > 0 && (
                                            <div style={{ marginTop: '4px', fontSize: '9px', color: colors.textMuted }}>
                                                {cause.evidence.slice(0, 2).map((e, j) => (
                                                    <div key={j} style={{ marginTop: '2px' }}>
                                                        {typeof e === 'string' ? e : (e.description || e.title || JSON.stringify(e).slice(0, 100))}
                                                    </div>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>

                    {/* ── Section 6: The Dollar Story ───────────────── */}
                    <div style={{ ...shared.card, display: 'flex', alignItems: 'center', gap: '20px', marginBottom: '16px', flexWrap: 'wrap' }}>
                        <div style={{ flex: 'none' }}>
                            <svg ref={pieRef} />
                        </div>
                        <div style={{ flex: 1, minWidth: '200px' }}>
                            <div style={shared.sectionTitle}>THE DOLLAR STORY</div>
                            <div style={{
                                color: colors.text, fontSize: '14px', lineHeight: '1.6',
                                fontFamily: SANS, marginBottom: '10px',
                            }}>
                                Approximately <span style={{ color: colors.yellow, fontWeight: 700, fontFamily: MONO }}>
                                    {formatUSD(totalFlow)}
                                </span> of informed capital entered <span style={{ color: colors.accent, fontWeight: 600 }}>{ticker}</span> in
                                the 14 days preceding the <span style={{ color: isUp ? colors.green : colors.red, fontWeight: 600 }}>
                                    {formatPct(movePct)}
                                </span> move.
                            </div>
                            {/* Legend */}
                            <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
                                {flowBreakdown.map((fb, i) => (
                                    <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                        <div style={{
                                            width: '8px', height: '8px', borderRadius: '2px',
                                            background: SOURCE_TYPE_COLORS[fb.source] || '#6B7280',
                                        }} />
                                        <span style={{ fontSize: '10px', color: colors.textDim, textTransform: 'capitalize' }}>
                                            {fb.source.replace(/_/g, ' ')}
                                        </span>
                                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                                            {formatUSD(fb.amount)}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </div>
                    </div>

                    {/* ── Section 7: The Narrative ──────────────────── */}
                    {forensicReport.narrative && (
                        <div style={{ ...shared.cardGradient, marginBottom: '16px' }}>
                            <div style={shared.sectionTitle}>THE NARRATIVE</div>
                            <div style={{
                                color: colors.text, fontSize: '13px', lineHeight: '1.8',
                                fontFamily: SANS, whiteSpace: 'pre-wrap',
                            }}>
                                {forensicReport.narrative}
                            </div>
                        </div>
                    )}

                    {/* ── Section 8: Pattern Match ─────────────────── */}
                    {forensicReport.pattern_match && (
                        <div style={{
                            ...shared.card,
                            borderLeft: `3px solid ${colors.yellow}`,
                            marginBottom: '16px',
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                                <span style={{
                                    ...shared.badge(colors.yellowBg),
                                    color: colors.yellow,
                                    fontSize: '10px',
                                    fontWeight: 700,
                                    letterSpacing: '1px',
                                }}>
                                    PATTERN MATCH
                                </span>
                            </div>
                            <div style={{ color: colors.text, fontSize: '12px', lineHeight: '1.6', fontFamily: SANS }}>
                                This pattern
                                {forensicReport.pattern_match.pattern && (
                                    <span style={{ fontFamily: MONO, color: colors.yellow, margin: '0 4px' }}>
                                        ({Array.isArray(forensicReport.pattern_match.pattern)
                                            ? forensicReport.pattern_match.pattern.join(' -> ')
                                            : String(forensicReport.pattern_match.pattern)
                                        })
                                    </span>
                                )}
                                has occurred
                                {forensicReport.pattern_match.occurrences && (
                                    <span style={{ color: colors.yellow, fontWeight: 700, fontFamily: MONO, margin: '0 4px' }}>
                                        {forensicReport.pattern_match.occurrences} times
                                    </span>
                                )}
                                before
                                {forensicReport.pattern_match.accuracy && (
                                    <span> with <span style={{ color: colors.green, fontWeight: 700, fontFamily: MONO }}>
                                        {(forensicReport.pattern_match.accuracy * 100).toFixed(0)}% accuracy
                                    </span></span>
                                )}
                                .
                            </div>
                        </div>
                    )}

                    {/* ── Confidence + Metadata ────────────────────── */}
                    <div style={{ ...shared.metricGrid, marginBottom: '20px' }}>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{forensicReport.warning_signals || 0}</div>
                            <div style={shared.metricLabel}>Warning Signals</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>
                                {forensicReport.avg_lead_time_hours
                                    ? `${(forensicReport.avg_lead_time_hours / 24).toFixed(1)}d`
                                    : '--'}
                            </div>
                            <div style={shared.metricLabel}>Avg Lead Time</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={{
                                ...shared.metricValue,
                                color: (forensicReport.confidence || 0) > 0.5 ? colors.green : colors.yellow,
                            }}>
                                {forensicReport.confidence != null
                                    ? `${(forensicReport.confidence * 100).toFixed(0)}%`
                                    : '--'}
                            </div>
                            <div style={shared.metricLabel}>Confidence</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{precedingEvents.length}</div>
                            <div style={shared.metricLabel}>Events Detected</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{actors.length}</div>
                            <div style={shared.metricLabel}>Actors</div>
                        </div>
                    </div>
                </>
            )}

            {/* ─── Empty state ────────────────────────────────── */}
            {!loading && !selectedMove && significantMoves.length === 0 && ticker && (
                <div style={{
                    textAlign: 'center', padding: '80px 20px',
                    color: colors.textMuted, fontFamily: MONO, fontSize: '12px',
                }}>
                    No significant moves found for {ticker} in the last 90 days.
                </div>
            )}
            {!ticker && (
                <div style={{
                    textAlign: 'center', padding: '100px 20px',
                }}>
                    <div style={{ fontSize: '40px', marginBottom: '16px', opacity: 0.2 }}>?</div>
                    <div style={{
                        color: colors.textDim, fontFamily: SANS, fontSize: '15px',
                        maxWidth: '480px', margin: '0 auto', lineHeight: '1.7',
                    }}>
                        Enter a ticker to investigate. GRID will find every significant price move,
                        reconstruct who moved the money, how much, when, and why.
                    </div>
                    <div style={{ color: colors.textMuted, fontFamily: MONO, fontSize: '11px', marginTop: '12px' }}>
                        This is forensic market intelligence.
                    </div>
                </div>
            )}
        </div>
    );
}
