/**
 * FlowTimeline -- Horizontal D3 timeline showing GEX history, OpEx calendar,
 * catalyst markers, price overlay, regime bands, and gamma flip crossings.
 *
 * Props:
 *   ticker       - string
 *   timelineData - object from /api/v1/derivatives/flow-timeline/{ticker}
 */
import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { colors, tokens } from '../styles/shared.js';

const CHART_HEIGHT = 250;
const MARGIN = { top: 18, right: 52, bottom: 42, left: 58 };
const EVENT_TRACK_H = 24;

function formatGEX(val) {
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

const CATALYST_COLORS = {
    fomc: '#EF4444',
    cpi: '#F97316',
    earnings: '#A855F7',
};
const OPEX_COLOR = '#3B82F6';

export default function FlowTimeline({ ticker, timelineData }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const tooltipRef = useRef(null);
    const [width, setWidth] = useState(600);

    // Responsive width
    useEffect(() => {
        if (!containerRef.current) return;
        const observer = new ResizeObserver(entries => {
            for (const entry of entries) {
                const w = entry.contentRect.width;
                if (w > 0) setWidth(w);
            }
        });
        observer.observe(containerRef.current);
        setWidth(containerRef.current.clientWidth || 600);
        return () => observer.disconnect();
    }, []);

    // Main D3 render
    useEffect(() => {
        if (!svgRef.current || !timelineData) return;

        const history = timelineData.history || [];
        const opexCal = timelineData.opex_calendar || [];
        const catalysts = timelineData.catalysts || [];
        const flipCrossings = timelineData.gamma_flip_crossings || [];

        if (history.length === 0) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', width).attr('height', CHART_HEIGHT);

        const chartW = width - MARGIN.left - MARGIN.right;
        const chartH = CHART_HEIGHT - MARGIN.top - MARGIN.bottom - EVENT_TRACK_H;

        const g = svg.append('g')
            .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

        // Parse dates
        const parsed = history.map(d => ({
            date: new Date(d.date),
            gex: d.net_gex || 0,
            spot: d.spot || 0,
            regime: d.regime || 'neutral',
        }));

        // X scale: dates from history
        const xDomain = d3.extent(parsed, d => d.date);
        const xScale = d3.scaleTime().domain(xDomain).range([0, chartW]);

        // Y scale: GEX
        const gexMax = d3.max(parsed, d => Math.abs(d.gex)) || 1;
        const yScale = d3.scaleLinear()
            .domain([-gexMax * 1.15, gexMax * 1.15])
            .range([chartH, 0]);

        const zeroY = yScale(0);

        // Y2 scale: spot price (secondary axis)
        const spotMin = d3.min(parsed, d => d.spot) || 0;
        const spotMax = d3.max(parsed, d => d.spot) || 1;
        const spotPad = (spotMax - spotMin) * 0.08 || 1;
        const y2Scale = d3.scaleLinear()
            .domain([spotMin - spotPad, spotMax + spotPad])
            .range([chartH, 0]);

        // ── Defs ──
        const defs = svg.append('defs');

        // Green gradient (GEX > 0)
        const greenGrad = defs.append('linearGradient')
            .attr('id', `ft-green-${ticker}`)
            .attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        greenGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.green).attr('stop-opacity', 0.35);
        greenGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.green).attr('stop-opacity', 0.02);

        // Red gradient (GEX < 0)
        const redGrad = defs.append('linearGradient')
            .attr('id', `ft-red-${ticker}`)
            .attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        redGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.red).attr('stop-opacity', 0.02);
        redGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.red).attr('stop-opacity', 0.35);

        // ── Regime bands (background) ──
        for (let i = 0; i < parsed.length - 1; i++) {
            const d = parsed[i];
            const next = parsed[i + 1];
            const x0 = xScale(d.date);
            const x1 = xScale(next.date);
            const isShort = d.regime === 'short_gamma';
            const magnitude = Math.min(Math.abs(d.gex) / gexMax, 1);
            const opacity = 0.03 + magnitude * 0.08;

            g.append('rect')
                .attr('x', x0).attr('y', 0)
                .attr('width', Math.max(x1 - x0, 1))
                .attr('height', chartH)
                .attr('fill', isShort ? colors.red : colors.green)
                .attr('opacity', opacity);
        }

        // ── Grid lines ──
        const yTicks = yScale.ticks(5);
        g.selectAll('.grid-h')
            .data(yTicks)
            .enter().append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', d => yScale(d)).attr('y2', d => yScale(d))
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.4)
            .attr('opacity', 0.5);

        // ── Zero line ──
        g.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', zeroY).attr('y2', zeroY)
            .attr('stroke', colors.yellow)
            .attr('stroke-width', 1)
            .attr('opacity', 0.6);

        g.append('text')
            .attr('x', chartW - 4).attr('y', zeroY - 4)
            .attr('text-anchor', 'end')
            .attr('font-size', '8px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.yellow)
            .attr('opacity', 0.7)
            .text('GEX = 0');

        // ── GEX area: positive (green) ──
        const areaPos = d3.area()
            .x(d => xScale(d.date))
            .y0(zeroY)
            .y1(d => d.gex > 0 ? yScale(d.gex) : zeroY)
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(parsed)
            .attr('d', areaPos)
            .attr('fill', `url(#ft-green-${ticker})`)
            .attr('opacity', 0)
            .transition().duration(500).attr('opacity', 1);

        // ── GEX area: negative (red) ──
        const areaNeg = d3.area()
            .x(d => xScale(d.date))
            .y0(zeroY)
            .y1(d => d.gex < 0 ? yScale(d.gex) : zeroY)
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(parsed)
            .attr('d', areaNeg)
            .attr('fill', `url(#ft-red-${ticker})`)
            .attr('opacity', 0)
            .transition().duration(500).attr('opacity', 1);

        // ── GEX line ──
        const gexLine = d3.line()
            .x(d => xScale(d.date))
            .y(d => yScale(d.gex))
            .curve(d3.curveMonotoneX);

        const gexPath = g.append('path')
            .datum(parsed)
            .attr('fill', 'none')
            .attr('stroke', '#C8D8E8')
            .attr('stroke-width', 1.5)
            .attr('d', gexLine);

        // Animate
        const totalLen = gexPath.node().getTotalLength();
        gexPath.attr('stroke-dasharray', `${totalLen} ${totalLen}`)
            .attr('stroke-dashoffset', totalLen)
            .transition().duration(700).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // ── Price overlay (faint secondary) ──
        if (parsed.some(d => d.spot > 0)) {
            const priceLine = d3.line()
                .x(d => xScale(d.date))
                .y(d => y2Scale(d.spot))
                .curve(d3.curveMonotoneX)
                .defined(d => d.spot > 0);

            g.append('path')
                .datum(parsed)
                .attr('fill', 'none')
                .attr('stroke', colors.accent)
                .attr('stroke-width', 1)
                .attr('stroke-dasharray', '4,2')
                .attr('opacity', 0.4)
                .attr('d', priceLine);
        }

        // ── Gamma flip crossings (vertical dashed) ──
        flipCrossings.forEach(fc => {
            const fcDate = new Date(fc.date);
            const fx = xScale(fcDate);
            if (fx >= 0 && fx <= chartW) {
                g.append('line')
                    .attr('x1', fx).attr('x2', fx)
                    .attr('y1', 0).attr('y2', chartH)
                    .attr('stroke', colors.yellow)
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '5,3')
                    .attr('opacity', 0.6);

                // Small label
                g.append('text')
                    .attr('x', fx + 3).attr('y', 10)
                    .attr('font-size', '7px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.yellow)
                    .attr('opacity', 0.7)
                    .text(fc.direction === 'below' ? 'FLIP -' : 'FLIP +');
            }
        });

        // ── Event track (bottom) ──
        const eventG = g.append('g')
            .attr('transform', `translate(0,${chartH + 4})`);

        // Separator line
        eventG.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.5);

        // OpEx markers (triangles)
        opexCal.forEach(opex => {
            const opDate = new Date(opex.date);
            const ox = xScale(opDate);
            if (ox < 0 || ox > chartW) return;

            let size, yOff;
            if (opex.type === 'quarterly') {
                // Star marker for quarterly
                size = 6;
                yOff = 12;
                const starPath = d3.symbol().type(d3.symbolStar).size(size * 12);
                eventG.append('path')
                    .attr('d', starPath())
                    .attr('transform', `translate(${ox},${yOff})`)
                    .attr('fill', OPEX_COLOR)
                    .attr('opacity', 0.9);
            } else if (opex.type === 'monthly') {
                size = 5;
                yOff = 12;
                eventG.append('polygon')
                    .attr('points', `${ox},${yOff - size} ${ox - size},${yOff + size} ${ox + size},${yOff + size}`)
                    .attr('fill', OPEX_COLOR)
                    .attr('opacity', 0.7);
            } else {
                // Weekly: small triangle
                size = 3;
                yOff = 12;
                eventG.append('polygon')
                    .attr('points', `${ox},${yOff - size} ${ox - size},${yOff + size} ${ox + size},${yOff + size}`)
                    .attr('fill', OPEX_COLOR)
                    .attr('opacity', 0.3);
            }
        });

        // Catalyst markers (diamonds)
        catalysts.forEach(cat => {
            const cDate = new Date(cat.date);
            const cx = xScale(cDate);
            if (cx < 0 || cx > chartW) return;

            const catColor = CATALYST_COLORS[cat.type] || colors.textMuted;
            const cy = 12;
            const s = 5;

            // Diamond shape
            eventG.append('polygon')
                .attr('points', `${cx},${cy - s} ${cx + s},${cy} ${cx},${cy + s} ${cx - s},${cy}`)
                .attr('fill', catColor)
                .attr('opacity', 0.85)
                .attr('class', 'catalyst-marker')
                .style('cursor', 'pointer');
        });

        // ── Y axis (left, GEX) ──
        const yAxis = d3.axisLeft(yScale)
            .ticks(5)
            .tickSize(0)
            .tickFormat(v => {
                const abs = Math.abs(v);
                if (abs >= 1e9) return `${(v / 1e9).toFixed(1)}B`;
                if (abs >= 1e6) return `${(v / 1e6).toFixed(0)}M`;
                if (abs >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
                return `${v.toFixed(0)}`;
            });

        g.append('g')
            .call(yAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

        g.append('text')
            .attr('transform', 'rotate(-90)')
            .attr('x', -chartH / 2).attr('y', -44)
            .attr('text-anchor', 'middle')
            .attr('font-size', '8px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('Net GEX ($)');

        // ── Y2 axis (right, spot price) ──
        if (parsed.some(d => d.spot > 0)) {
            const y2Axis = d3.axisRight(y2Scale)
                .ticks(4)
                .tickSize(0)
                .tickFormat(v => `$${v.toFixed(0)}`);

            g.append('g')
                .attr('transform', `translate(${chartW},0)`)
                .call(y2Axis)
                .call(g => g.select('.domain').remove())
                .call(g => g.selectAll('.tick text')
                    .attr('font-size', '9px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', `${colors.accent}80`));
        }

        // ── X axis ──
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(7, parsed.length))
            .tickSize(0)
            .tickFormat(d3.timeFormat('%b %d'));

        g.append('g')
            .attr('transform', `translate(0,${chartH + EVENT_TRACK_H})`)
            .call(xAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

        // ── Crosshair overlay ──
        const crosshairG = g.append('g').style('display', 'none');

        crosshairG.append('line')
            .attr('class', 'ch-v')
            .attr('y1', 0).attr('y2', chartH)
            .attr('stroke', colors.textMuted)
            .attr('stroke-width', 0.5)
            .attr('stroke-dasharray', '3,2');

        crosshairG.append('line')
            .attr('class', 'ch-h')
            .attr('x1', 0).attr('x2', chartW)
            .attr('stroke', colors.textMuted)
            .attr('stroke-width', 0.5)
            .attr('stroke-dasharray', '3,2');

        crosshairG.append('circle')
            .attr('r', 3.5)
            .attr('fill', '#C8D8E8')
            .attr('stroke', colors.bg)
            .attr('stroke-width', 1.5);

        // Bisector
        const bisect = d3.bisector(d => d.date).left;

        // Build event lookup for tooltip
        const allEvents = [
            ...opexCal.map(e => ({ ...e, _category: 'opex' })),
            ...catalysts.map(e => ({ ...e, _category: 'catalyst' })),
        ];

        g.append('rect')
            .attr('width', chartW).attr('height', chartH + EVENT_TRACK_H)
            .attr('fill', 'transparent')
            .style('cursor', 'crosshair')
            .on('mouseenter', () => crosshairG.style('display', null))
            .on('mouseleave', () => {
                crosshairG.style('display', 'none');
                if (tooltipRef.current) tooltipRef.current.style.display = 'none';
            })
            .on('mousemove touchmove', function (event) {
                event.preventDefault();
                const coords = d3.pointer(event, this);
                const x0 = xScale.invert(coords[0]);
                const idx = bisect(parsed, x0, 1);
                const d0 = parsed[idx - 1];
                const d1 = parsed[idx];
                if (!d0) return;
                const d = d1 && (x0 - d0.date > d1.date - x0) ? d1 : d0;

                const cx = xScale(d.date);
                const cy = yScale(d.gex);

                crosshairG.select('.ch-v').attr('x1', cx).attr('x2', cx);
                crosshairG.select('.ch-h').attr('y1', cy).attr('y2', cy);
                crosshairG.select('circle').attr('cx', cx).attr('cy', cy);

                if (tooltipRef.current) {
                    const tooltip = tooltipRef.current;
                    tooltip.style.display = 'flex';

                    const dateStr = d.date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
                    const gexColor = d.gex >= 0 ? colors.green : colors.red;
                    const regimeLabel = d.regime === 'short_gamma' ? 'SHORT' :
                                        d.regime === 'long_gamma' ? 'LONG' : 'NEUTRAL';

                    // Find nearest events within 3 days
                    const dMs = d.date.getTime();
                    const nearEvents = allEvents
                        .filter(e => Math.abs(new Date(e.date).getTime() - dMs) < 3 * 86400000)
                        .map(e => e.label)
                        .slice(0, 2);
                    const eventStr = nearEvents.length > 0 ? ` | ${nearEvents.join(', ')}` : '';

                    tooltip.innerHTML =
                        `<span style="color:${colors.text};font-weight:600">${dateStr}</span>` +
                        `<span style="color:${gexColor};margin-left:8px;font-weight:600">${formatGEX(d.gex)}</span>` +
                        `<span style="color:${colors.accent}80;margin-left:8px">$${d.spot.toFixed(1)}</span>` +
                        `<span style="color:${colors.textMuted};margin-left:8px;font-size:10px">${regimeLabel}${eventStr}</span>`;
                }
            });

    }, [timelineData, width, ticker]);

    if (!timelineData || timelineData.error) {
        return (
            <div style={{
                background: colors.bg,
                border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md,
                padding: '40px 16px',
                textAlign: 'center',
                color: colors.textMuted,
                fontSize: '11px',
            }}>
                {timelineData?.error || 'No flow timeline data available'}
            </div>
        );
    }

    const history = timelineData.history || [];
    const latestGex = history.length > 0 ? history[history.length - 1].net_gex : 0;
    const flipCount = (timelineData.gamma_flip_crossings || []).length;

    return (
        <div ref={containerRef} style={{
            background: colors.bg,
            border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.md,
            overflow: 'hidden',
        }}>
            {/* Header */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '10px 12px 0 12px',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    }}>FLOW TIMELINE</span>
                    <span style={{
                        fontSize: '10px', color: colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>{timelineData.days}d</span>
                </div>
                <div style={{
                    display: 'flex', gap: '10px', fontSize: '10px',
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    <span style={{ color: colors.textMuted }}>
                        GEX: <span style={{ color: latestGex >= 0 ? colors.green : colors.red, fontWeight: 600 }}>
                            {formatGEX(latestGex)}
                        </span>
                    </span>
                    {flipCount > 0 && (
                        <span style={{ color: colors.yellow }}>
                            {flipCount} flip{flipCount > 1 ? 's' : ''}
                        </span>
                    )}
                </div>
            </div>

            {/* Tooltip bar */}
            <div
                ref={tooltipRef}
                style={{
                    display: 'none',
                    alignItems: 'center',
                    padding: '4px 12px',
                    fontSize: '11px',
                    fontFamily: "'JetBrains Mono', monospace",
                    minHeight: '20px',
                }}
            />

            {/* Chart */}
            <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />

            {/* Legend */}
            <div style={{
                display: 'flex', gap: '14px', padding: '4px 12px 8px 12px',
                fontSize: '9px', fontFamily: "'JetBrains Mono', monospace",
                color: colors.textMuted, borderTop: `1px solid ${colors.border}`,
                flexWrap: 'wrap', alignItems: 'center',
            }}>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <svg width="10" height="10"><polygon points="5,1 1,9 9,9" fill={OPEX_COLOR} opacity="0.7" /></svg>
                    OpEx
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <svg width="10" height="10"><polygon points="5,0 10,5 5,10 0,5" fill={CATALYST_COLORS.fomc} opacity="0.85" /></svg>
                    FOMC
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <svg width="10" height="10"><polygon points="5,0 10,5 5,10 0,5" fill={CATALYST_COLORS.cpi} opacity="0.85" /></svg>
                    CPI
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <svg width="10" height="10"><polygon points="5,0 10,5 5,10 0,5" fill={CATALYST_COLORS.earnings} opacity="0.85" /></svg>
                    Earnings
                </span>
                <span style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                    <span style={{ width: '10px', height: '2px', background: colors.accent, opacity: 0.4, display: 'inline-block', borderTop: '1px dashed ' + colors.accent }}></span>
                    Spot
                </span>
            </div>
        </div>
    );
}
