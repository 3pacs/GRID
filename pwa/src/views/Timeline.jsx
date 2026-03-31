/**
 * Timeline -- Forensic event timeline visualization.
 *
 * Horizontal D3 timeline showing price action overlaid with every intelligence
 * event (congressional trades, insider filings, dark pool spikes, whale options,
 * news, earnings, FOMC/CPI, regime changes).  Click any marker to expand a
 * detail panel.  Pattern sidebar shows recurring sequences detected by the
 * backend.
 *
 * API: GET /api/v1/intelligence/events?ticker=&days=90
 */
import React, { useEffect, useRef, useState, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';
import { formatDate, formatShortDate, formatDateTime } from '../utils/formatTime.js';

// ── Constants ────────────────────────────────────────────────────────────────

const PERIODS = [
    { label: '30D', days: 30 },
    { label: '90D', days: 90 },
    { label: '180D', days: 180 },
    { label: '1Y', days: 365 },
];

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
    price_move:    { color: '#94A3B8', shape: 'none',    label: 'Price Move' },
    prediction:    { color: '#06B6D4', shape: 'dot',     label: 'Prediction' },
};

const CHART_HEIGHT = 320;
const MARGIN = { top: 24, right: 60, bottom: 50, left: 64 };
const EVENT_LANE_H = 36;
const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

// ── Helpers ──────────────────────────────────────────────────────────────────

function formatUSD(val) {
    if (val == null) return '--';
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

function formatLeadTime(hours) {
    if (hours == null) return '--';
    if (hours < 1) return `${Math.round(hours * 60)}m`;
    if (hours < 24) return `${hours.toFixed(1)}h`;
    return `${(hours / 24).toFixed(1)}d`;
}

function dirColor(dir) {
    if (dir === 'bullish') return colors.green;
    if (dir === 'bearish') return colors.red;
    return colors.textMuted;
}

function confidenceBadge(c) {
    const map = {
        confirmed: { bg: '#1A7A4A', text: '#10B981' },
        derived:   { bg: '#1A3A6A', text: '#3B82F6' },
        estimated: { bg: '#3A3A1A', text: '#F59E0B' },
        rumored:   { bg: '#3B1111', text: '#EF4444' },
    };
    return map[c] || map.estimated;
}

// ── Main Component ───────────────────────────────────────────────────────────

export default function Timeline({ onNavigate }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const tooltipRef = useRef(null);
    const fullScreenRef = useRef(null);
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

    const [width, setWidth] = useState(900);
    const [ticker, setTicker] = useState('');
    const [tickerInput, setTickerInput] = useState('');
    const [period, setPeriod] = useState(90);
    const [events, setEvents] = useState([]);
    const [priceData, setPriceData] = useState([]);
    const [patterns, setPatterns] = useState([]);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [selectedEvent, setSelectedEvent] = useState(null);
    const [watchlist, setWatchlist] = useState([]);
    const [visibleTypes, setVisibleTypes] = useState(() => {
        const initial = {};
        Object.keys(EVENT_TYPE_CONFIG).forEach(k => { initial[k] = true; });
        return initial;
    });
    const [isPlaying, setIsPlaying] = useState(false);
    const [playIndex, setPlayIndex] = useState(0);
    const [forensicMode, setForensicMode] = useState(false);
    const [forensicRange, setForensicRange] = useState(null);
    const playTimerRef = useRef(null);

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

    // Load patterns once
    useEffect(() => {
        api.getRecurringPatterns(3).then(res => {
            if (res && !res.error) {
                setPatterns(res.patterns || res || []);
            }
        });
    }, []);

    // Responsive width
    useEffect(() => {
        if (!containerRef.current) return;
        const obs = new ResizeObserver(entries => {
            for (const e of entries) {
                if (e.contentRect.width > 0) setWidth(e.contentRect.width);
            }
        });
        obs.observe(containerRef.current);
        setWidth(containerRef.current.clientWidth || 900);
        return () => obs.disconnect();
    }, []);

    // Fetch events when ticker/period changes
    const loadData = useCallback(async () => {
        if (!ticker) return;
        setLoading(true);
        setError(null);
        setSelectedEvent(null);
        setForensicRange(null);
        setIsPlaying(false);
        try {
            const res = await api.getEventTimeline(ticker, period);
            if (res && !res.error) {
                const evts = res.events || [];
                setEvents(evts);
                // Extract price data from price_move events
                const prices = evts
                    .filter(e => e.event_type === 'price_move')
                    .map(e => {
                        const match = e.description?.match(/\$[\d.]+\s*->\s*\$([\d.]+)/);
                        return {
                            date: new Date(e.timestamp),
                            price: match ? parseFloat(match[1]) : null,
                        };
                    })
                    .filter(d => d.price != null);
                setPriceData(prices);
            } else {
                setError(res?.message || 'Failed to load timeline');
            }
        } catch (err) {
            setError(err.message);
        }
        setLoading(false);
    }, [ticker, period]);

    useEffect(() => { loadData(); }, [loadData]);

    // Filter events by visible types
    const filteredEvents = useMemo(() =>
        events.filter(e => visibleTypes[e.event_type] !== false),
        [events, visibleTypes]
    );

    // Play animation
    useEffect(() => {
        if (!isPlaying) {
            if (playTimerRef.current) clearInterval(playTimerRef.current);
            return;
        }
        setPlayIndex(0);
        playTimerRef.current = setInterval(() => {
            setPlayIndex(prev => {
                if (prev >= filteredEvents.length - 1) {
                    setIsPlaying(false);
                    return prev;
                }
                return prev + 1;
            });
        }, 300);
        return () => clearInterval(playTimerRef.current);
    }, [isPlaying, filteredEvents.length]);

    const playableEvents = isPlaying
        ? filteredEvents.slice(0, playIndex + 1)
        : filteredEvents;

    // ── D3 Render ────────────────────────────────────────────────────────

    useEffect(() => {
        if (!svgRef.current || priceData.length < 2) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const totalH = CHART_HEIGHT + EVENT_LANE_H;
        svg.attr('width', width).attr('height', totalH);

        const chartW = width - MARGIN.left - MARGIN.right;
        const chartH = CHART_HEIGHT - MARGIN.top - MARGIN.bottom;

        const g = svg.append('g')
            .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

        // X scale
        const xDomain = d3.extent(priceData, d => d.date);
        const xScale = d3.scaleTime().domain(xDomain).range([0, chartW]);

        // Y scale: price
        const priceExtent = d3.extent(priceData, d => d.price);
        const pricePad = (priceExtent[1] - priceExtent[0]) * 0.1 || 1;
        const yScale = d3.scaleLinear()
            .domain([priceExtent[0] - pricePad, priceExtent[1] + pricePad])
            .range([chartH, 0]);

        // ── Background grid ──
        const yTicks = yScale.ticks(6);
        g.selectAll('.grid-h')
            .data(yTicks).enter()
            .append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', d => yScale(d)).attr('y2', d => yScale(d))
            .attr('stroke', colors.border).attr('stroke-width', 0.4).attr('opacity', 0.5);

        // ── Regime bands (full-width colored backgrounds) ──
        const regimeEvents = playableEvents.filter(e => e.event_type === 'regime');
        regimeEvents.forEach((re, i) => {
            const reDate = new Date(re.timestamp);
            const nextDate = i < regimeEvents.length - 1
                ? new Date(regimeEvents[i + 1].timestamp)
                : xDomain[1];
            const x0 = Math.max(0, xScale(reDate));
            const x1 = Math.min(chartW, xScale(nextDate));
            if (x1 <= x0) return;
            const isShort = re.direction === 'bearish';
            g.append('rect')
                .attr('x', x0).attr('y', 0)
                .attr('width', x1 - x0).attr('height', chartH)
                .attr('fill', isShort ? colors.red : colors.green)
                .attr('opacity', 0.04);
        });

        // ── Forensic mode highlight ──
        if (forensicRange) {
            const fx0 = xScale(forensicRange[0]);
            const fx1 = xScale(forensicRange[1]);
            g.append('rect')
                .attr('x', fx0).attr('y', 0)
                .attr('width', fx1 - fx0).attr('height', chartH)
                .attr('fill', '#F59E0B').attr('opacity', 0.08)
                .attr('stroke', '#F59E0B').attr('stroke-width', 1)
                .attr('stroke-dasharray', '4,2');
        }

        // ── Price line ──
        const priceLine = d3.line()
            .x(d => xScale(d.date))
            .y(d => yScale(d.price))
            .curve(d3.curveMonotoneX)
            .defined(d => d.price != null);

        const pricePath = g.append('path')
            .datum(priceData)
            .attr('fill', 'none')
            .attr('stroke', '#C8D8E8')
            .attr('stroke-width', 1.8)
            .attr('d', priceLine);

        // Animate price line
        const pathLen = pricePath.node().getTotalLength();
        pricePath
            .attr('stroke-dasharray', `${pathLen} ${pathLen}`)
            .attr('stroke-dashoffset', pathLen)
            .transition().duration(800).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // Price area fill
        const priceArea = d3.area()
            .x(d => xScale(d.date))
            .y0(chartH)
            .y1(d => yScale(d.price))
            .curve(d3.curveMonotoneX)
            .defined(d => d.price != null);

        g.append('path')
            .datum(priceData)
            .attr('d', priceArea)
            .attr('fill', `${colors.accent}08`);

        // ── Event markers (bottom lane) ──
        const eventG = g.append('g')
            .attr('transform', `translate(0,${chartH + 6})`);

        eventG.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);

        const markerEvents = playableEvents.filter(e =>
            e.event_type !== 'price_move' && e.event_type !== 'regime'
        );

        // Size scale for dollar amounts
        const maxAmt = d3.max(markerEvents, e => e.amount_usd || 0) || 1;
        const sizeScale = d3.scaleSqrt().domain([0, maxAmt]).range([3, 10]);

        markerEvents.forEach(ev => {
            const cfg = EVENT_TYPE_CONFIG[ev.event_type] || EVENT_TYPE_CONFIG.news;
            const evDate = new Date(ev.timestamp);
            const ex = xScale(evDate);
            if (ex < 0 || ex > chartW) return;

            const sz = ev.amount_usd ? sizeScale(ev.amount_usd) : 4;
            const cy = EVENT_LANE_H / 2;
            const fill = cfg.color;
            let marker;

            if (cfg.shape === 'diamond') {
                marker = eventG.append('polygon')
                    .attr('points', `${ex},${cy - sz} ${ex + sz},${cy} ${ex},${cy + sz} ${ex - sz},${cy}`)
                    .attr('fill', fill).attr('opacity', 0.85);
            } else if (cfg.shape === 'circle') {
                marker = eventG.append('circle')
                    .attr('cx', ex).attr('cy', cy).attr('r', sz)
                    .attr('fill', fill).attr('opacity', 0.8);
            } else if (cfg.shape === 'square') {
                marker = eventG.append('rect')
                    .attr('x', ex - sz).attr('y', cy - sz)
                    .attr('width', sz * 2).attr('height', sz * 2)
                    .attr('fill', fill).attr('opacity', 0.8);
            } else if (cfg.shape === 'triangle') {
                marker = eventG.append('polygon')
                    .attr('points', `${ex},${cy - sz} ${ex - sz},${cy + sz} ${ex + sz},${cy + sz}`)
                    .attr('fill', fill).attr('opacity', 0.8);
            } else if (cfg.shape === 'star') {
                const starPath = d3.symbol().type(d3.symbolStar).size(sz * sz * 3);
                marker = eventG.append('path')
                    .attr('d', starPath())
                    .attr('transform', `translate(${ex},${cy})`)
                    .attr('fill', fill).attr('opacity', 0.9);
            } else {
                // dot
                marker = eventG.append('circle')
                    .attr('cx', ex).attr('cy', cy).attr('r', Math.max(sz * 0.6, 2))
                    .attr('fill', fill).attr('opacity', 0.5);
            }

            // Also draw a vertical whisker line up into the chart area
            g.append('line')
                .attr('x1', ex).attr('x2', ex)
                .attr('y1', chartH).attr('y2', chartH + 6)
                .attr('stroke', fill).attr('stroke-width', 0.5).attr('opacity', 0.4);

            if (marker) {
                marker.style('cursor', 'pointer')
                    .on('mouseenter', function (event) {
                        d3.select(this).attr('opacity', 1).attr('stroke', '#fff').attr('stroke-width', 1.5);
                        if (tooltipRef.current) {
                            const tt = tooltipRef.current;
                            tt.style.display = 'block';
                            const dateStr = formatDate(evDate);
                            tt.innerHTML = `
                                <div style="display:flex;align-items:center;gap:8px;margin-bottom:4px">
                                    <span style="width:8px;height:8px;border-radius:2px;background:${fill};display:inline-block"></span>
                                    <span style="color:${colors.text};font-weight:600;font-size:11px">${cfg.label}</span>
                                    <span style="color:${colors.textMuted};font-size:10px">${dateStr}</span>
                                    <span style="color:${dirColor(ev.direction)};font-size:10px;font-weight:600">${ev.direction.toUpperCase()}</span>
                                </div>
                                <div style="color:${colors.textDim};font-size:10px;line-height:1.4;max-width:400px">${ev.description || ''}</div>
                                ${ev.amount_usd ? `<div style="color:${colors.yellow};font-size:10px;margin-top:2px">${formatUSD(ev.amount_usd)}</div>` : ''}
                                ${ev.lead_time_to_next_move != null ? `<div style="color:${colors.textMuted};font-size:9px;margin-top:2px">Lead time: ${formatLeadTime(ev.lead_time_to_next_move)}</div>` : ''}
                            `;
                            const rect = event.target.getBoundingClientRect();
                            const containerRect = containerRef.current.getBoundingClientRect();
                            tt.style.left = `${rect.left - containerRect.left}px`;
                            tt.style.top = `${rect.top - containerRect.top - tt.offsetHeight - 8}px`;
                        }
                    })
                    .on('mouseleave', function () {
                        const cfg2 = EVENT_TYPE_CONFIG[ev.event_type] || EVENT_TYPE_CONFIG.news;
                        d3.select(this).attr('opacity', cfg2.shape === 'dot' ? 0.5 : 0.8).attr('stroke', 'none');
                        if (tooltipRef.current) tooltipRef.current.style.display = 'none';
                    })
                    .on('click', () => {
                        setSelectedEvent(ev);
                    });
            }
        });

        // ── Axes ──
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(8, priceData.length))
            .tickSize(0)
            .tickFormat(d3.timeFormat('%b %d'));

        g.append('g')
            .attr('transform', `translate(0,${chartH + EVENT_LANE_H})`)
            .call(xAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px').attr('font-family', MONO).attr('fill', colors.textMuted));

        const yAxis = d3.axisLeft(yScale)
            .ticks(6).tickSize(0)
            .tickFormat(v => `$${v.toFixed(0)}`);

        g.append('g')
            .call(yAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px').attr('font-family', MONO).attr('fill', colors.textMuted));

        // ── Crosshair ──
        const crossG = g.append('g').style('display', 'none');
        crossG.append('line').attr('class', 'cv')
            .attr('y1', 0).attr('y2', chartH)
            .attr('stroke', colors.textMuted).attr('stroke-width', 0.5).attr('stroke-dasharray', '3,2');
        crossG.append('line').attr('class', 'ch')
            .attr('x1', 0).attr('x2', chartW)
            .attr('stroke', colors.textMuted).attr('stroke-width', 0.5).attr('stroke-dasharray', '3,2');
        crossG.append('circle').attr('r', 3).attr('fill', '#C8D8E8').attr('stroke', colors.bg).attr('stroke-width', 1.5);

        const bisect = d3.bisector(d => d.date).left;

        // Forensic mode brush
        if (forensicMode) {
            const brush = d3.brushX()
                .extent([[0, 0], [chartW, chartH]])
                .on('end', (event) => {
                    if (!event.selection) { setForensicRange(null); return; }
                    const [x0, x1] = event.selection;
                    setForensicRange([xScale.invert(x0), xScale.invert(x1)]);
                });
            g.append('g').attr('class', 'brush').call(brush);
        } else {
            g.append('rect')
                .attr('width', chartW).attr('height', chartH + EVENT_LANE_H)
                .attr('fill', 'transparent').style('cursor', 'crosshair')
                .on('mouseenter', () => crossG.style('display', null))
                .on('mouseleave', () => crossG.style('display', 'none'))
                .on('mousemove', function (event) {
                    const [mx] = d3.pointer(event, this);
                    const x0 = xScale.invert(mx);
                    const idx = bisect(priceData, x0, 1);
                    const d0 = priceData[idx - 1];
                    const d1 = priceData[idx];
                    if (!d0) return;
                    const d = d1 && (x0 - d0.date > d1.date - x0) ? d1 : d0;
                    const cx = xScale(d.date);
                    const cy = yScale(d.price);
                    crossG.select('.cv').attr('x1', cx).attr('x2', cx);
                    crossG.select('.ch').attr('y1', cy).attr('y2', cy);
                    crossG.select('circle').attr('cx', cx).attr('cy', cy);
                });
        }

    }, [priceData, playableEvents, width, forensicMode, forensicRange]);

    // ── Related events for detail panel ──
    const relatedEvents = useMemo(() => {
        if (!selectedEvent) return [];
        const selDate = new Date(selectedEvent.timestamp).getTime();
        return events.filter(e => {
            if (e === selectedEvent) return false;
            const d = new Date(e.timestamp).getTime();
            return Math.abs(d - selDate) < 7 * 86400000;
        }).slice(0, 10);
    }, [selectedEvent, events]);

    // ── Forensic preceding events ──
    const forensicEvents = useMemo(() => {
        if (!forensicRange) return [];
        const [start, end] = forensicRange;
        return events.filter(e => {
            const d = new Date(e.timestamp);
            return d >= new Date(start.getTime() - 14 * 86400000) && d <= end;
        });
    }, [forensicRange, events]);

    // ── Render ───────────────────────────────────────────────────────────

    const handleTickerSubmit = (t) => {
        const val = (t || tickerInput).trim().toUpperCase();
        if (val) {
            setTicker(val);
            setTickerInput(val);
        }
    };

    // Zoom handlers -- scale the SVG viewport
    const [timelineZoom, setTimelineZoom] = useState(1);

    const handleTimelineZoomIn = useCallback(() => {
        setTimelineZoom(prev => Math.min(prev * 1.3, 4));
    }, []);

    const handleTimelineZoomOut = useCallback(() => {
        setTimelineZoom(prev => Math.max(prev * 0.7, 0.5));
    }, []);

    const handleTimelineFit = useCallback(() => {
        setTimelineZoom(1);
    }, []);

    return (
        <div ref={fullScreenRef} style={{
            background: isFullScreen ? colors.bg : undefined,
        }}>
        <div ref={containerRef} style={{
            padding: tokens.space.lg,
            maxWidth: '1600px',
            margin: '0 auto',
            fontFamily: SANS,
            position: 'relative',
        }}>
            {/* ── Header ── */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: '12px',
                marginBottom: tokens.space.lg,
            }}>
                <span style={{
                    fontSize: '10px', fontWeight: 700, letterSpacing: '2px',
                    color: colors.accent, fontFamily: MONO,
                }}>FORENSIC TIMELINE</span>
                <span style={{
                    fontSize: '9px', color: colors.textMuted, fontFamily: MONO,
                    background: `${colors.red}15`, padding: '2px 8px',
                    borderRadius: tokens.radius.sm, border: `1px solid ${colors.red}30`,
                }}>INVESTIGATIVE</span>
            </div>

            {/* ── Controls ── */}
            <div style={{
                ...shared.card,
                display: 'flex', flexWrap: 'wrap', gap: '12px',
                alignItems: 'center', marginBottom: tokens.space.md,
            }}>
                {/* Ticker */}
                <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                    <input
                        type="text"
                        value={tickerInput}
                        onChange={e => setTickerInput(e.target.value.toUpperCase())}
                        onKeyDown={e => e.key === 'Enter' && handleTickerSubmit()}
                        placeholder="TICKER"
                        style={{
                            ...shared.input,
                            width: '90px', padding: '6px 10px',
                            fontSize: '12px', fontFamily: MONO,
                            minHeight: '32px',
                        }}
                    />
                    <button
                        onClick={() => handleTickerSubmit()}
                        style={{
                            ...shared.buttonSmall,
                            padding: '6px 12px', minHeight: '32px',
                            fontSize: '11px',
                        }}
                    >GO</button>
                </div>

                {/* Watchlist quick picks */}
                {watchlist.length > 0 && (
                    <div style={{
                        display: 'flex', gap: '4px', flexWrap: 'wrap',
                        maxWidth: '300px',
                    }}>
                        {watchlist.slice(0, 8).map(t => (
                            <button
                                key={t}
                                onClick={() => handleTickerSubmit(t)}
                                style={{
                                    background: ticker === t ? `${colors.accent}30` : colors.bg,
                                    border: `1px solid ${ticker === t ? colors.accent : colors.border}`,
                                    color: ticker === t ? colors.accent : colors.textMuted,
                                    borderRadius: tokens.radius.sm,
                                    padding: '3px 8px', fontSize: '10px',
                                    fontFamily: MONO, cursor: 'pointer',
                                    fontWeight: ticker === t ? 700 : 400,
                                }}
                            >{t}</button>
                        ))}
                    </div>
                )}

                {/* Separator */}
                <div style={{ width: '1px', height: '24px', background: colors.border }} />

                {/* Period */}
                <div style={{ display: 'flex', gap: '2px' }}>
                    {PERIODS.map(p => (
                        <button
                            key={p.days}
                            onClick={() => setPeriod(p.days)}
                            style={{
                                background: period === p.days ? colors.accent : 'none',
                                color: period === p.days ? '#fff' : colors.textMuted,
                                border: 'none', borderRadius: tokens.radius.sm,
                                padding: '4px 10px', fontSize: '10px',
                                fontFamily: MONO, fontWeight: 600,
                                cursor: 'pointer',
                            }}
                        >{p.label}</button>
                    ))}
                </div>

                {/* Separator */}
                <div style={{ width: '1px', height: '24px', background: colors.border }} />

                {/* Play */}
                <button
                    onClick={() => setIsPlaying(!isPlaying)}
                    style={{
                        background: isPlaying ? `${colors.green}20` : 'none',
                        border: `1px solid ${isPlaying ? colors.green : colors.border}`,
                        color: isPlaying ? colors.green : colors.textMuted,
                        borderRadius: tokens.radius.sm,
                        padding: '4px 10px', fontSize: '10px',
                        fontFamily: MONO, fontWeight: 600,
                        cursor: 'pointer',
                    }}
                >{isPlaying ? 'STOP' : 'PLAY'}</button>

                {/* Forensic mode */}
                <button
                    onClick={() => { setForensicMode(!forensicMode); setForensicRange(null); }}
                    style={{
                        background: forensicMode ? `${colors.red}20` : 'none',
                        border: `1px solid ${forensicMode ? colors.red : colors.border}`,
                        color: forensicMode ? colors.red : colors.textMuted,
                        borderRadius: tokens.radius.sm,
                        padding: '4px 10px', fontSize: '10px',
                        fontFamily: MONO, fontWeight: 600,
                        cursor: 'pointer',
                    }}
                >FORENSIC{forensicMode ? ' ON' : ''}</button>
            </div>

            {/* ── Filter checkboxes ── */}
            <div style={{
                display: 'flex', flexWrap: 'wrap', gap: '8px',
                marginBottom: tokens.space.md, padding: '0 4px',
            }}>
                {Object.entries(EVENT_TYPE_CONFIG).filter(([k]) => k !== 'price_move').map(([key, cfg]) => (
                    <label
                        key={key}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '4px',
                            fontSize: '10px', fontFamily: MONO, color: colors.textMuted,
                            cursor: 'pointer', userSelect: 'none',
                            opacity: visibleTypes[key] ? 1 : 0.4,
                        }}
                    >
                        <span style={{
                            width: '8px', height: '8px', borderRadius: '2px',
                            background: cfg.color, display: 'inline-block',
                        }} />
                        <input
                            type="checkbox"
                            checked={visibleTypes[key] !== false}
                            onChange={() => setVisibleTypes(prev => ({
                                ...prev, [key]: !prev[key],
                            }))}
                            style={{ display: 'none' }}
                        />
                        {cfg.label}
                    </label>
                ))}
            </div>

            {/* ── Main layout: chart + sidebar ── */}
            <div style={{ display: 'flex', gap: '16px' }}>
                {/* Chart area */}
                <div style={{ flex: 1, minWidth: 0 }}>
                    {/* Status */}
                    {loading && (
                        <div style={{
                            ...shared.card, textAlign: 'center',
                            color: colors.textMuted, fontSize: '12px',
                            fontFamily: MONO, padding: '60px 16px',
                        }}>Reconstructing timeline...</div>
                    )}
                    {error && (
                        <div style={{
                            ...shared.card, textAlign: 'center',
                            color: colors.red, fontSize: '12px',
                            fontFamily: MONO, padding: '40px 16px',
                        }}>{error}</div>
                    )}
                    {!ticker && !loading && (
                        <div style={{
                            ...shared.card, textAlign: 'center',
                            color: colors.textMuted, fontSize: '12px',
                            fontFamily: MONO, padding: '80px 16px',
                        }}>Enter a ticker to begin forensic analysis</div>
                    )}
                    {ticker && !loading && !error && priceData.length < 2 && events.length === 0 && (
                        <div style={{
                            ...shared.card, textAlign: 'center',
                            color: colors.textMuted, fontSize: '12px',
                            fontFamily: MONO, padding: '60px 16px',
                        }}>No events found for {ticker} in the last {period} days</div>
                    )}

                    {/* Chart */}
                    {priceData.length >= 2 && (
                        <div style={{
                            ...shared.card,
                            padding: '8px 4px 4px 4px',
                            position: 'relative',
                            overflow: 'hidden',
                        }}>
                            <ChartControls
                                onZoomIn={handleTimelineZoomIn}
                                onZoomOut={handleTimelineZoomOut}
                                onFitScreen={handleTimelineFit}
                                onFullScreen={toggleFullScreen}
                                isFullScreen={isFullScreen}
                                showSearch={false}
                                compact
                            />
                            {/* Header stats */}
                            <div style={{
                                display: 'flex', justifyContent: 'space-between',
                                alignItems: 'center', padding: '0 12px 6px 12px',
                            }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                                    <span style={{
                                        fontSize: '14px', fontWeight: 700,
                                        color: colors.text, fontFamily: MONO,
                                    }}>{ticker}</span>
                                    <span style={{
                                        fontSize: '10px', color: colors.textMuted, fontFamily: MONO,
                                    }}>{period}d | {filteredEvents.length} events</span>
                                    {isPlaying && (
                                        <span style={{
                                            fontSize: '10px', color: colors.green, fontFamily: MONO,
                                        }}>{playIndex + 1}/{filteredEvents.length}</span>
                                    )}
                                </div>
                                {priceData.length > 0 && (
                                    <span style={{
                                        fontSize: '13px', fontWeight: 600,
                                        color: colors.text, fontFamily: MONO,
                                    }}>${priceData[priceData.length - 1]?.price?.toFixed(2)}</span>
                                )}
                            </div>

                            <div style={{
                                transform: `scaleX(${timelineZoom})`,
                                transformOrigin: 'center top',
                                transition: 'transform 0.3s ease',
                                overflow: 'hidden',
                            }}>
                                <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />
                            </div>

                            {/* Tooltip */}
                            <div
                                ref={tooltipRef}
                                style={{
                                    display: 'none',
                                    position: 'absolute',
                                    background: colors.card,
                                    border: `1px solid ${colors.border}`,
                                    borderRadius: tokens.radius.sm,
                                    padding: '8px 12px',
                                    fontFamily: MONO,
                                    fontSize: '10px',
                                    zIndex: 50,
                                    pointerEvents: 'none',
                                    maxWidth: '450px',
                                    boxShadow: '0 4px 16px rgba(0,0,0,0.5)',
                                }}
                            />
                        </div>
                    )}

                    {/* ── Forensic analysis results ── */}
                    {forensicRange && forensicEvents.length > 0 && (
                        <div style={{
                            ...shared.card,
                            marginTop: tokens.space.sm,
                            borderLeft: `3px solid ${colors.red}`,
                        }}>
                            <div style={{
                                ...shared.sectionTitle,
                                color: colors.red, marginBottom: '8px',
                            }}>FORENSIC ANALYSIS</div>
                            <div style={{
                                fontSize: '11px', color: colors.textMuted,
                                fontFamily: MONO, marginBottom: '8px',
                            }}>
                                {forensicEvents.length} events in selected window
                                ({formatDate(forensicRange[0])} - {formatDate(forensicRange[1])})
                            </div>
                            {forensicEvents.filter(e => e.event_type !== 'price_move').slice(0, 15).map((e, i) => (
                                <div
                                    key={i}
                                    onClick={() => setSelectedEvent(e)}
                                    style={{
                                        display: 'flex', alignItems: 'center', gap: '8px',
                                        padding: '6px 0',
                                        borderBottom: `1px solid ${colors.borderSubtle}`,
                                        cursor: 'pointer',
                                        fontSize: '10px', fontFamily: MONO,
                                    }}
                                >
                                    <span style={{
                                        width: '8px', height: '8px', borderRadius: '2px',
                                        background: EVENT_TYPE_CONFIG[e.event_type]?.color || '#666',
                                        flexShrink: 0,
                                    }} />
                                    <span style={{ color: colors.textMuted, flexShrink: 0, width: '58px' }}>
                                        {formatShortDate(e.timestamp)}
                                    </span>
                                    <span style={{ color: dirColor(e.direction), flexShrink: 0, width: '50px' }}>
                                        {e.direction}
                                    </span>
                                    <span style={{ color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                        {e.description?.slice(0, 80)}
                                    </span>
                                    {e.amount_usd && (
                                        <span style={{ color: colors.yellow, flexShrink: 0 }}>{formatUSD(e.amount_usd)}</span>
                                    )}
                                </div>
                            ))}
                        </div>
                    )}

                    {/* ── Detail panel (on event click) ── */}
                    {selectedEvent && (
                        <div style={{
                            ...shared.card,
                            marginTop: tokens.space.sm,
                            borderLeft: `3px solid ${EVENT_TYPE_CONFIG[selectedEvent.event_type]?.color || colors.accent}`,
                        }}>
                            <div style={{
                                display: 'flex', justifyContent: 'space-between',
                                alignItems: 'flex-start', marginBottom: '10px',
                            }}>
                                <div>
                                    <div style={{
                                        display: 'flex', alignItems: 'center', gap: '8px',
                                        marginBottom: '4px',
                                    }}>
                                        <span style={{
                                            width: '10px', height: '10px', borderRadius: '2px',
                                            background: EVENT_TYPE_CONFIG[selectedEvent.event_type]?.color,
                                            display: 'inline-block',
                                        }} />
                                        <span style={{
                                            fontSize: '12px', fontWeight: 700, color: colors.text,
                                            fontFamily: MONO, textTransform: 'uppercase',
                                        }}>
                                            {EVENT_TYPE_CONFIG[selectedEvent.event_type]?.label || selectedEvent.event_type}
                                        </span>
                                        <span style={{
                                            fontSize: '11px', color: dirColor(selectedEvent.direction),
                                            fontWeight: 600, fontFamily: MONO,
                                        }}>
                                            {selectedEvent.direction?.toUpperCase()}
                                        </span>
                                    </div>
                                    <div style={{
                                        fontSize: '10px', color: colors.textMuted, fontFamily: MONO,
                                    }}>
                                        {formatDateTime(selectedEvent.timestamp)}
                                    </div>
                                </div>
                                <button
                                    onClick={() => setSelectedEvent(null)}
                                    style={{
                                        background: 'none', border: 'none',
                                        color: colors.textMuted, cursor: 'pointer',
                                        fontSize: '14px', fontFamily: MONO,
                                    }}
                                >X</button>
                            </div>

                            {/* Detail grid */}
                            <div style={{
                                display: 'grid',
                                gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))',
                                gap: '10px', marginBottom: '10px',
                            }}>
                                {selectedEvent.actor && (
                                    <div style={metricStyle}>
                                        <div style={metricLabel}>ACTOR</div>
                                        <div style={metricValue}>{selectedEvent.actor}</div>
                                    </div>
                                )}
                                {selectedEvent.amount_usd && (
                                    <div style={metricStyle}>
                                        <div style={metricLabel}>AMOUNT</div>
                                        <div style={{ ...metricValue, color: colors.yellow }}>
                                            {formatUSD(selectedEvent.amount_usd)}
                                        </div>
                                    </div>
                                )}
                                <div style={metricStyle}>
                                    <div style={metricLabel}>SOURCE</div>
                                    <div style={metricValue}>{selectedEvent.source}</div>
                                </div>
                                <div style={metricStyle}>
                                    <div style={metricLabel}>CONFIDENCE</div>
                                    <div style={{
                                        ...metricValue,
                                        color: confidenceBadge(selectedEvent.confidence).text,
                                    }}>
                                        {selectedEvent.confidence?.toUpperCase()}
                                    </div>
                                </div>
                                {selectedEvent.lead_time_to_next_move != null && (
                                    <div style={metricStyle}>
                                        <div style={metricLabel}>LEAD TIME</div>
                                        <div style={metricValue}>
                                            {formatLeadTime(selectedEvent.lead_time_to_next_move)}
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Description */}
                            <div style={{
                                fontSize: '11px', color: colors.textDim,
                                fontFamily: MONO, lineHeight: '1.5',
                                padding: '8px 0',
                                borderTop: `1px solid ${colors.borderSubtle}`,
                            }}>
                                {selectedEvent.description}
                            </div>

                            {/* Causal connection hint */}
                            {selectedEvent.lead_time_to_next_move != null && (
                                <div style={{
                                    fontSize: '10px', color: colors.accent,
                                    fontFamily: MONO, padding: '6px 0',
                                    borderTop: `1px solid ${colors.borderSubtle}`,
                                }}>
                                    This {selectedEvent.event_type.replace('_', ' ')} ({selectedEvent.direction}) preceded the next significant price move by {formatLeadTime(selectedEvent.lead_time_to_next_move)}.
                                </div>
                            )}

                            {/* Related events */}
                            {relatedEvents.length > 0 && (
                                <div style={{
                                    borderTop: `1px solid ${colors.borderSubtle}`,
                                    paddingTop: '8px', marginTop: '4px',
                                }}>
                                    <div style={{
                                        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                        color: colors.textMuted, fontFamily: MONO,
                                        marginBottom: '6px',
                                    }}>RELATED EVENTS (7-DAY WINDOW)</div>
                                    {relatedEvents.slice(0, 5).map((re, i) => (
                                        <div
                                            key={i}
                                            onClick={() => setSelectedEvent(re)}
                                            style={{
                                                display: 'flex', alignItems: 'center', gap: '6px',
                                                padding: '4px 0', cursor: 'pointer',
                                                fontSize: '10px', fontFamily: MONO,
                                            }}
                                        >
                                            <span style={{
                                                width: '6px', height: '6px', borderRadius: '1px',
                                                background: EVENT_TYPE_CONFIG[re.event_type]?.color || '#666',
                                            }} />
                                            <span style={{ color: colors.textMuted, width: '56px' }}>
                                                {formatShortDate(re.timestamp)}
                                            </span>
                                            <span style={{ color: dirColor(re.direction), width: '44px' }}>
                                                {re.direction}
                                            </span>
                                            <span style={{ color: colors.textDim, flex: 1, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                                {re.description?.slice(0, 60)}
                                            </span>
                                        </div>
                                    ))}
                                </div>
                            )}
                        </div>
                    )}
                </div>

                {/* ── Pattern Sidebar ── */}
                <div style={{
                    width: '280px', flexShrink: 0,
                    display: 'flex', flexDirection: 'column', gap: tokens.space.sm,
                }}>
                    <div style={{
                        ...shared.card,
                        borderLeft: `3px solid ${colors.accent}`,
                    }}>
                        <div style={{
                            ...shared.sectionTitle,
                            marginBottom: '8px',
                        }}>RECURRING PATTERNS</div>
                        {patterns.length === 0 && (
                            <div style={{
                                fontSize: '10px', color: colors.textMuted,
                                fontFamily: MONO, padding: '12px 0',
                            }}>No patterns detected yet. Patterns are computed across all tracked tickers.</div>
                        )}
                        {(Array.isArray(patterns) ? patterns : []).slice(0, 12).map((p, i) => (
                            <div key={i} style={{
                                padding: '8px 0',
                                borderBottom: `1px solid ${colors.borderSubtle}`,
                            }}>
                                <div style={{
                                    fontSize: '10px', color: colors.text,
                                    fontFamily: MONO, lineHeight: '1.4',
                                    wordBreak: 'break-word',
                                }}>
                                    {p.pattern?.split(' -> ').map((step, si, arr) => (
                                        <React.Fragment key={si}>
                                            <span style={{
                                                color: EVENT_TYPE_CONFIG[step.split(':')[0]]?.color || colors.textDim,
                                            }}>
                                                {step.replace(':', ' ')}
                                            </span>
                                            {si < arr.length - 1 && (
                                                <span style={{ color: colors.textMuted }}> {'->'} </span>
                                            )}
                                        </React.Fragment>
                                    ))}
                                </div>
                                <div style={{
                                    display: 'flex', gap: '12px', marginTop: '4px',
                                    fontSize: '9px', fontFamily: MONO,
                                }}>
                                    <span style={{ color: colors.green }}>
                                        {p.occurrences}x seen
                                    </span>
                                    <span style={{ color: colors.textMuted }}>
                                        avg {formatLeadTime(p.avg_total_gap_hours)}
                                    </span>
                                    {p.tickers && (
                                        <span style={{ color: colors.textMuted }}>
                                            {p.tickers.slice(0, 3).join(', ')}
                                        </span>
                                    )}
                                </div>
                            </div>
                        ))}
                    </div>

                    {/* Legend */}
                    <div style={{
                        ...shared.card,
                        padding: '10px 12px',
                    }}>
                        <div style={{
                            ...shared.sectionTitle,
                            marginBottom: '6px',
                        }}>EVENT LEGEND</div>
                        {Object.entries(EVENT_TYPE_CONFIG).filter(([k]) => k !== 'price_move').map(([key, cfg]) => (
                            <div key={key} style={{
                                display: 'flex', alignItems: 'center', gap: '8px',
                                padding: '3px 0', fontSize: '10px', fontFamily: MONO,
                            }}>
                                <span style={{
                                    width: '8px', height: '8px', borderRadius: '2px',
                                    background: cfg.color, display: 'inline-block',
                                    flexShrink: 0,
                                }} />
                                <span style={{
                                    color: visibleTypes[key] ? colors.textDim : colors.textMuted,
                                    textDecoration: visibleTypes[key] ? 'none' : 'line-through',
                                }}>
                                    {cfg.label}
                                </span>
                                <span style={{ color: colors.textMuted, marginLeft: 'auto', fontSize: '9px' }}>
                                    {cfg.shape}
                                </span>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
        </div>
    );
}

// ── Style fragments ──────────────────────────────────────────────────────────

const metricStyle = {
    background: colors.bg,
    borderRadius: tokens.radius.sm,
    padding: '8px 10px',
};

const metricLabel = {
    fontSize: '9px', fontWeight: 700, letterSpacing: '1px',
    color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace",
    marginBottom: '2px',
};

const metricValue = {
    fontSize: '11px', color: colors.text,
    fontFamily: "'JetBrains Mono', monospace",
    wordBreak: 'break-word',
};
