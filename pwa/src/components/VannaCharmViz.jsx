/**
 * VannaCharmViz -- Standalone vanna/charm visualization showing how
 * dealer hedging flows evolve with time.
 *
 * Two-part layout:
 *   1. Compass (top) -- circular gauge showing combined vanna+charm
 *      dealer flow direction with animated arrow and quadrant labels.
 *   2. Decay Timeline (bottom) -- horizontal chart showing projected
 *      cumulative dealer delta change from charm decay until OpEx.
 *
 * Props:
 *   ticker         - string
 *   vannaCharmData - object from /api/v1/derivatives/vanna-charm/{ticker}
 */
import React, { useEffect, useRef, useState, useMemo } from 'react';
import * as d3 from 'd3';
import { colors, tokens } from '../styles/shared.js';
import { formatShortDate } from '../utils/formatTime.js';

const TOTAL_HEIGHT = 300;
const COMPASS_DIAMETER = 200;
const COMPASS_R = COMPASS_DIAMETER / 2;
const TIMELINE_HEIGHT = 100;

function formatDelta(val) {
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(1)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

export default function VannaCharmViz({ ticker, vannaCharmData }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const [width, setWidth] = useState(500);

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
        setWidth(containerRef.current.clientWidth || 500);
        return () => observer.disconnect();
    }, []);

    const d = vannaCharmData;

    // Compute weekly OpEx markers (Fridays between now and monthly opex)
    const weeklyMarkers = useMemo(() => {
        if (!d || !d.days_to_opex) return [];
        const markers = [];
        const today = new Date();
        for (let i = 1; i <= d.days_to_opex; i++) {
            const dt = new Date(today);
            dt.setDate(dt.getDate() + i);
            if (dt.getDay() === 5) { // Friday
                markers.push({ day: i, label: `Fri ${formatShortDate(dt)}` });
            }
        }
        return markers;
    }, [d]);

    // Main D3 render
    useEffect(() => {
        if (!svgRef.current || !d || d.error) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', width).attr('height', TOTAL_HEIGHT);

        const defs = svg.append('defs');

        // ================================================================
        // PART 1: COMPASS
        // ================================================================
        const compassCX = width / 2;
        const compassCY = COMPASS_R + 12;
        const cg = svg.append('g')
            .attr('transform', `translate(${compassCX},${compassCY})`);

        // Quadrant background fills
        const quadrants = [
            { startAngle: -Math.PI,     endAngle: -Math.PI / 2, fill: `${colors.red}08`,    label: 'Headwind',  lx: -COMPASS_R * 0.58, ly: COMPASS_R * 0.52, color: colors.red },
            { startAngle: -Math.PI / 2, endAngle: 0,            fill: `${colors.yellow}08`, label: 'Drag',      lx: COMPASS_R * 0.58,  ly: COMPASS_R * 0.52, color: colors.yellow },
            { startAngle: 0,            endAngle: Math.PI / 2,  fill: `${colors.green}08`,  label: 'Tailwind',  lx: COMPASS_R * 0.58,  ly: -COMPASS_R * 0.52, color: colors.green },
            { startAngle: Math.PI / 2,  endAngle: Math.PI,      fill: `${colors.yellow}08`, label: 'Chop',      lx: -COMPASS_R * 0.58, ly: -COMPASS_R * 0.52, color: colors.yellow },
        ];

        const arc = d3.arc()
            .innerRadius(0)
            .outerRadius(COMPASS_R - 2);

        quadrants.forEach(q => {
            cg.append('path')
                .attr('d', arc({ startAngle: q.startAngle, endAngle: q.endAngle }))
                .attr('fill', q.fill)
                .attr('stroke', 'none');

            cg.append('text')
                .attr('x', q.lx).attr('y', q.ly)
                .attr('text-anchor', 'middle')
                .attr('font-size', '8px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', q.color)
                .attr('opacity', 0.7)
                .text(q.label);
        });

        // Outer ring
        cg.append('circle')
            .attr('r', COMPASS_R)
            .attr('fill', 'none')
            .attr('stroke', colors.border)
            .attr('stroke-width', 1.5);

        // Inner reference circles
        [0.33, 0.66].forEach(pct => {
            cg.append('circle')
                .attr('r', COMPASS_R * pct)
                .attr('fill', 'none')
                .attr('stroke', colors.border)
                .attr('stroke-width', 0.3)
                .attr('opacity', 0.5);
        });

        // Crosshairs
        cg.append('line')
            .attr('x1', -(COMPASS_R - 6)).attr('x2', COMPASS_R - 6)
            .attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.border).attr('stroke-width', 0.6);
        cg.append('line')
            .attr('x1', 0).attr('x2', 0)
            .attr('y1', -(COMPASS_R - 6)).attr('y2', COMPASS_R - 6)
            .attr('stroke', colors.border).attr('stroke-width', 0.6);

        // Axis labels
        const axisLabels = [
            { x: COMPASS_R - 4,  y: -8, anchor: 'end',    text: 'IV rise', sub: 'dealers sell' },
            { x: -(COMPASS_R - 4), y: -8, anchor: 'start', text: 'IV drop',  sub: 'dealers buy' },
            { x: 0, y: -(COMPASS_R - 8), anchor: 'middle', text: 'Time helps bulls', sub: '' },
            { x: 0, y: COMPASS_R - 4, anchor: 'middle',    text: 'Time helps bears', sub: '' },
        ];

        axisLabels.forEach(al => {
            cg.append('text')
                .attr('x', al.x).attr('y', al.y)
                .attr('text-anchor', al.anchor)
                .attr('font-size', '7px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted)
                .text(al.text);
            if (al.sub) {
                cg.append('text')
                    .attr('x', al.x).attr('y', al.y + 9)
                    .attr('text-anchor', al.anchor)
                    .attr('font-size', '6px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.textMuted)
                    .attr('opacity', 0.6)
                    .text(al.sub);
            }
        });

        // Normalize vanna/charm to compass radius
        const vannaExp = d.vanna_exposure || 0;
        const charmExp = d.charm_exposure || 0;
        const maxMag = Math.max(Math.abs(vannaExp), Math.abs(charmExp), 1);
        const arrowX = (vannaExp / maxMag) * (COMPASS_R - 20);
        const arrowY = -(charmExp / maxMag) * (COMPASS_R - 20); // SVG Y inverted

        // Arrow glow gradient
        const glowId = `vc-glow-${ticker}`;
        const grad = defs.append('radialGradient').attr('id', glowId);
        grad.append('stop').attr('offset', '0%').attr('stop-color', colors.accent).attr('stop-opacity', 0.5);
        grad.append('stop').attr('offset', '100%').attr('stop-color', colors.accent).attr('stop-opacity', 0);

        // Arrow glow
        cg.append('circle')
            .attr('cx', arrowX).attr('cy', arrowY)
            .attr('r', 16)
            .attr('fill', `url(#${glowId})`);

        // Animated arrow line
        const arrowLine = cg.append('line')
            .attr('x1', 0).attr('y1', 0)
            .attr('x2', 0).attr('y2', 0)
            .attr('stroke', colors.accent)
            .attr('stroke-width', 2.5)
            .attr('stroke-linecap', 'round');

        arrowLine.transition()
            .duration(800)
            .ease(d3.easeCubicOut)
            .attr('x2', arrowX)
            .attr('y2', arrowY);

        // Arrowhead (triangle at tip)
        const angle = Math.atan2(arrowY, arrowX);
        const tipLen = 8;
        const arrowhead = cg.append('polygon')
            .attr('fill', colors.accent)
            .attr('opacity', 0);

        arrowhead.transition()
            .delay(600)
            .duration(300)
            .attr('opacity', 1)
            .attrTween('points', function () {
                return function (t) {
                    const cx = arrowX * t;
                    const cy = arrowY * t;
                    const a = Math.atan2(cy, cx);
                    const p1x = cx + tipLen * Math.cos(a);
                    const p1y = cy + tipLen * Math.sin(a);
                    const p2x = cx + tipLen * 0.5 * Math.cos(a + 2.3);
                    const p2y = cy + tipLen * 0.5 * Math.sin(a + 2.3);
                    const p3x = cx + tipLen * 0.5 * Math.cos(a - 2.3);
                    const p3y = cy + tipLen * 0.5 * Math.sin(a - 2.3);
                    return `${p1x},${p1y} ${p2x},${p2y} ${p3x},${p3y}`;
                };
            });

        // Animated tip dot
        const tipDot = cg.append('circle')
            .attr('cx', 0).attr('cy', 0)
            .attr('r', 4)
            .attr('fill', colors.accent);

        tipDot.transition()
            .duration(800)
            .ease(d3.easeCubicOut)
            .attr('cx', arrowX)
            .attr('cy', arrowY);

        // Center dot
        cg.append('circle')
            .attr('r', 3)
            .attr('fill', colors.border);

        // Center label: net delta change
        const netDelta = d.net_dealer_delta_change || 0;
        const centerColor = netDelta >= 0 ? colors.green : colors.red;
        cg.append('text')
            .attr('x', 0).attr('y', COMPASS_R * 0.18)
            .attr('text-anchor', 'middle')
            .attr('font-size', '13px')
            .attr('font-weight', 700)
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', centerColor)
            .text(formatDelta(netDelta));

        cg.append('text')
            .attr('x', 0).attr('y', COMPASS_R * 0.18 + 12)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('net delta by OpEx');

        // ================================================================
        // PART 2: DECAY TIMELINE
        // ================================================================
        const tlMargin = { top: 8, right: 20, bottom: 22, left: 50 };
        const tlY0 = COMPASS_DIAMETER + 28;
        const tlW = width - tlMargin.left - tlMargin.right;
        const tlH = TIMELINE_HEIGHT - tlMargin.top - tlMargin.bottom;

        const tg = svg.append('g')
            .attr('transform', `translate(${tlMargin.left},${tlY0 + tlMargin.top})`);

        const daysToOpex = d.days_to_opex || 1;
        const dailyCharm = d.charm_exposure || 0;

        // Build cumulative data
        const cumData = [];
        for (let i = 0; i <= daysToOpex; i++) {
            cumData.push({ day: i, cumDelta: dailyCharm * i });
        }

        const xScale = d3.scaleLinear()
            .domain([0, daysToOpex])
            .range([0, tlW]);

        const yExtent = d3.extent(cumData, dd => dd.cumDelta);
        const yPad = Math.max(Math.abs(yExtent[0] || 0), Math.abs(yExtent[1] || 0), 1) * 0.15;
        const yScale = d3.scaleLinear()
            .domain([(yExtent[0] || 0) - yPad, (yExtent[1] || 0) + yPad])
            .range([tlH, 0]);

        const zeroY = yScale(0);

        // Shaded area (gravitational pull)
        const areaGen = d3.area()
            .x(dd => xScale(dd.day))
            .y0(zeroY)
            .y1(dd => yScale(dd.cumDelta))
            .curve(d3.curveMonotoneX);

        const areaColor = dailyCharm >= 0 ? colors.green : colors.red;

        const shadedGrad = defs.append('linearGradient')
            .attr('id', `vc-shade-${ticker}`)
            .attr('x1', '0').attr('y1', dailyCharm >= 0 ? '0' : '1')
            .attr('x2', '0').attr('y2', dailyCharm >= 0 ? '1' : '0');
        shadedGrad.append('stop').attr('offset', '0%').attr('stop-color', areaColor).attr('stop-opacity', 0.25);
        shadedGrad.append('stop').attr('offset', '100%').attr('stop-color', areaColor).attr('stop-opacity', 0.02);

        tg.append('path')
            .datum(cumData)
            .attr('d', areaGen)
            .attr('fill', `url(#vc-shade-${ticker})`)
            .attr('opacity', 0)
            .transition().duration(600).attr('opacity', 1);

        // Zero line
        tg.append('line')
            .attr('x1', 0).attr('x2', tlW)
            .attr('y1', zeroY).attr('y2', zeroY)
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.8)
            .attr('stroke-dasharray', '4,3');

        // Main line
        const lineGen = d3.line()
            .x(dd => xScale(dd.day))
            .y(dd => yScale(dd.cumDelta))
            .curve(d3.curveMonotoneX);

        const linePath = tg.append('path')
            .datum(cumData)
            .attr('fill', 'none')
            .attr('stroke', areaColor)
            .attr('stroke-width', 1.8)
            .attr('d', lineGen);

        const lineLen = linePath.node().getTotalLength();
        linePath.attr('stroke-dasharray', `${lineLen} ${lineLen}`)
            .attr('stroke-dashoffset', lineLen)
            .transition().duration(800).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // Today marker (day 0)
        tg.append('circle')
            .attr('cx', xScale(0))
            .attr('cy', yScale(0))
            .attr('r', 3)
            .attr('fill', colors.accent);
        tg.append('text')
            .attr('x', xScale(0) + 4).attr('y', yScale(0) - 6)
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.accent)
            .text('Today');

        // Weekly OpEx markers (Fridays)
        const monthlyDay = daysToOpex;
        weeklyMarkers.forEach(wm => {
            const isMonthly = wm.day === monthlyDay;
            const markerColor = isMonthly ? colors.yellow : colors.textMuted;
            const mx = xScale(wm.day);
            const my = yScale(dailyCharm * wm.day);

            tg.append('line')
                .attr('x1', mx).attr('x2', mx)
                .attr('y1', 0).attr('y2', tlH)
                .attr('stroke', markerColor)
                .attr('stroke-width', 0.5)
                .attr('stroke-dasharray', '2,2')
                .attr('opacity', 0.5);

            tg.append('circle')
                .attr('cx', mx).attr('cy', my)
                .attr('r', 2.5)
                .attr('fill', markerColor);

            if (isMonthly || weeklyMarkers.length <= 4) {
                tg.append('text')
                    .attr('x', mx).attr('y', tlH + 12)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '6px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', markerColor)
                    .text(wm.label);
            }
        });

        // Monthly OpEx marker (always shown)
        const opexX = xScale(daysToOpex);
        const opexY = yScale(dailyCharm * daysToOpex);
        tg.append('line')
            .attr('x1', opexX).attr('x2', opexX)
            .attr('y1', 0).attr('y2', tlH)
            .attr('stroke', colors.yellow)
            .attr('stroke-width', 1)
            .attr('stroke-dasharray', '3,2')
            .attr('opacity', 0.7);

        tg.append('circle')
            .attr('cx', opexX).attr('cy', opexY)
            .attr('r', 4)
            .attr('fill', colors.yellow)
            .attr('opacity', 0.9);

        tg.append('text')
            .attr('x', opexX).attr('y', -2)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-weight', 600)
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.yellow)
            .text('OpEx');

        // End annotation
        const endDelta = dailyCharm * daysToOpex;
        const action = endDelta < 0 ? 'SELL' : 'BUY';
        const annotColor = endDelta < 0 ? colors.red : colors.green;
        tg.append('text')
            .attr('x', opexX - 4).attr('y', opexY + (endDelta < 0 ? 14 : -8))
            .attr('text-anchor', 'end')
            .attr('font-size', '8px')
            .attr('font-weight', 600)
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', annotColor)
            .text(`${action} ${formatDelta(Math.abs(endDelta))}`);

        // Y axis
        const yAxis = d3.axisLeft(yScale)
            .ticks(4)
            .tickSize(0)
            .tickFormat(v => {
                const abs = Math.abs(v);
                if (abs >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
                if (abs >= 1e3) return `${(v / 1e3).toFixed(0)}K`;
                return `${v.toFixed(0)}`;
            });

        tg.append('g')
            .call(yAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '8px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

        // X axis
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(6, daysToOpex))
            .tickSize(0)
            .tickFormat(v => `${Math.round(v)}d`);

        tg.append('g')
            .attr('transform', `translate(0,${tlH})`)
            .call(xAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '8px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

    }, [d, width, ticker, weeklyMarkers]);

    if (!d || d.error) {
        return (
            <div style={{
                background: colors.bg,
                border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md,
                padding: '30px 16px',
                textAlign: 'center',
                color: colors.textMuted,
                fontSize: '11px',
            }}>
                {d?.error || 'No vanna/charm data available'}
            </div>
        );
    }

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
                    }}>VANNA / CHARM COMPASS</span>
                    <span style={{
                        fontSize: '10px', padding: '1px 6px', borderRadius: '3px',
                        fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                        background: `${colors.yellow}18`, color: colors.yellow,
                    }}>
                        {d.days_to_opex}d to OpEx
                    </span>
                </div>
                <div style={{
                    fontSize: '10px', color: colors.textMuted,
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    {d.ticker}
                </div>
            </div>

            {/* Chart */}
            <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />

            {/* Interpretation footer */}
            <div style={{
                padding: '6px 12px 10px 12px',
                fontSize: '10px', fontFamily: "'JetBrains Mono', monospace",
                color: colors.textDim, borderTop: `1px solid ${colors.border}`,
                lineHeight: '1.5',
            }}>
                {d.interpretation}
            </div>
        </div>
    );
}
