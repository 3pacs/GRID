/**
 * GEXProfile -- Interactive D3 dealer gamma exposure landscape chart.
 *
 * Visualizes the GEX curve (gamma at each strike), gamma flip point,
 * call wall (resistance), put wall (support), current spot, vanna/charm
 * compass, and regime annotation.
 *
 * Props:
 *   ticker      - string
 *   gexData     - object from /api/v1/derivatives/gex/{ticker}
 *   spotPrice   - number (current spot, falls back to gexData.spot)
 *   onStrikeClick - (strike: number) => void (optional)
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { colors, tokens } from '../styles/shared.js';

const CHART_HEIGHT = 350;
const MARGIN = { top: 28, right: 54, bottom: 36, left: 60 };

function formatGEX(val) {
    const abs = Math.abs(val);
    if (abs >= 1e9) return `$${(val / 1e9).toFixed(2)}B`;
    if (abs >= 1e6) return `$${(val / 1e6).toFixed(1)}M`;
    if (abs >= 1e3) return `$${(val / 1e3).toFixed(0)}K`;
    return `$${val.toFixed(0)}`;
}

export default function GEXProfile({ ticker, gexData, spotPrice, onStrikeClick }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const tooltipRef = useRef(null);
    const [width, setWidth] = useState(600);

    const spot = spotPrice || gexData?.spot || 0;

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
        if (!svgRef.current || !gexData) return;

        const perStrike = gexData.per_strike || [];
        const profile = gexData.profile || [];
        if (perStrike.length === 0 && profile.length === 0) return;

        const gammaFlip = gexData.gamma_flip;
        const callWall = gexData.call_wall;
        const putWall = gexData.put_wall;
        const regime = gexData.regime;
        const netGEX = gexData.gex_aggregate || 0;
        const vannaExp = gexData.vanna_exposure || 0;
        const charmExp = gexData.charm_exposure || 0;

        // Use per_strike for the bar/area data
        const chartData = perStrike.length > 0
            ? perStrike.map(s => ({ strike: s.strike, gex: s.net_gex }))
            : profile.map(p => ({ strike: p.spot, gex: p.gex }));

        // Filter to spot +/- 15%
        const lo = spot * 0.85;
        const hi = spot * 1.15;
        const filtered = chartData.filter(d => d.strike >= lo && d.strike <= hi);
        if (filtered.length < 2) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', width).attr('height', CHART_HEIGHT);

        const chartW = width - MARGIN.left - MARGIN.right;
        const chartH = CHART_HEIGHT - MARGIN.top - MARGIN.bottom;

        const g = svg.append('g')
            .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

        // Scales
        const xScale = d3.scaleLinear()
            .domain(d3.extent(filtered, d => d.strike))
            .range([0, chartW]);

        const yMax = d3.max(filtered, d => Math.abs(d.gex)) || 1;
        const yScale = d3.scaleLinear()
            .domain([-yMax * 1.15, yMax * 1.15])
            .range([chartH, 0]);

        const zeroY = yScale(0);

        // ── Grid lines ──
        const yTicks = yScale.ticks(6);
        g.selectAll('.grid-h')
            .data(yTicks)
            .enter().append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', d => yScale(d)).attr('y2', d => yScale(d))
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.4)
            .attr('opacity', 0.5);

        // ── Zero line (bright) ──
        g.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', zeroY).attr('y2', zeroY)
            .attr('stroke', colors.yellow)
            .attr('stroke-width', 1.5)
            .attr('opacity', 0.8);

        g.append('text')
            .attr('x', chartW - 4).attr('y', zeroY - 5)
            .attr('text-anchor', 'end')
            .attr('font-size', '9px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.yellow)
            .attr('opacity', 0.9)
            .text('Gamma Flip');

        // ── Gradient definitions ──
        const defs = svg.append('defs');

        // Green gradient (above zero)
        const greenGrad = defs.append('linearGradient')
            .attr('id', `gex-green-${ticker}`)
            .attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        greenGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.green).attr('stop-opacity', 0.4);
        greenGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.green).attr('stop-opacity', 0.02);

        // Red gradient (below zero)
        const redGrad = defs.append('linearGradient')
            .attr('id', `gex-red-${ticker}`)
            .attr('x1', '0').attr('y1', '0').attr('x2', '0').attr('y2', '1');
        redGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.red).attr('stop-opacity', 0.02);
        redGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.red).attr('stop-opacity', 0.4);

        // Pulsing spot marker
        const pulseGrad = defs.append('radialGradient')
            .attr('id', `gex-pulse-${ticker}`);
        pulseGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.accent).attr('stop-opacity', 0.6);
        pulseGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.accent).attr('stop-opacity', 0);

        // ── GEX area: positive (green) ──
        const areaPos = d3.area()
            .x(d => xScale(d.strike))
            .y0(zeroY)
            .y1(d => d.gex > 0 ? yScale(d.gex) : zeroY)
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(filtered)
            .attr('d', areaPos)
            .attr('fill', `url(#gex-green-${ticker})`)
            .attr('opacity', 0)
            .transition().duration(500).attr('opacity', 1);

        // ── GEX area: negative (red) ──
        const areaNeg = d3.area()
            .x(d => xScale(d.strike))
            .y0(zeroY)
            .y1(d => d.gex < 0 ? yScale(d.gex) : zeroY)
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(filtered)
            .attr('d', areaNeg)
            .attr('fill', `url(#gex-red-${ticker})`)
            .attr('opacity', 0)
            .transition().duration(500).attr('opacity', 1);

        // ── GEX line ──
        const line = d3.line()
            .x(d => xScale(d.strike))
            .y(d => yScale(d.gex))
            .curve(d3.curveMonotoneX);

        const path = g.append('path')
            .datum(filtered)
            .attr('fill', 'none')
            .attr('stroke', '#C8D8E8')
            .attr('stroke-width', 1.8)
            .attr('d', line);

        // Animate line
        const totalLength = path.node().getTotalLength();
        path.attr('stroke-dasharray', `${totalLength} ${totalLength}`)
            .attr('stroke-dashoffset', totalLength)
            .transition().duration(700).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // ── Current spot marker (pulsing) ──
        if (spot > 0) {
            const spotX = xScale(spot);
            if (spotX >= 0 && spotX <= chartW) {
                // Vertical line
                g.append('line')
                    .attr('x1', spotX).attr('x2', spotX)
                    .attr('y1', 0).attr('y2', chartH)
                    .attr('stroke', colors.accent)
                    .attr('stroke-width', 1.5)
                    .attr('opacity', 0.7);

                // Pulsing circle
                const pulse = g.append('circle')
                    .attr('cx', spotX).attr('cy', 14)
                    .attr('r', 8)
                    .attr('fill', `url(#gex-pulse-${ticker})`);

                // Animate pulse
                (function animatePulse() {
                    pulse.attr('r', 6).attr('opacity', 0.8)
                        .transition().duration(1200).ease(d3.easeSinInOut)
                        .attr('r', 14).attr('opacity', 0)
                        .on('end', animatePulse);
                })();

                // Inner dot
                g.append('circle')
                    .attr('cx', spotX).attr('cy', 14)
                    .attr('r', 3)
                    .attr('fill', colors.accent);

                // Label
                g.append('text')
                    .attr('x', spotX).attr('y', -4)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '9px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.accent)
                    .attr('font-weight', 700)
                    .text(`Spot $${spot.toFixed(0)}`);
            }
        }

        // ── Gamma flip point (dashed vertical) ──
        if (gammaFlip != null) {
            const flipX = xScale(gammaFlip);
            if (flipX >= 0 && flipX <= chartW) {
                g.append('line')
                    .attr('x1', flipX).attr('x2', flipX)
                    .attr('y1', 0).attr('y2', chartH)
                    .attr('stroke', colors.yellow)
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '6,4')
                    .attr('opacity', 0.7);

                g.append('text')
                    .attr('x', flipX).attr('y', chartH + 14)
                    .attr('text-anchor', 'middle')
                    .attr('font-size', '8px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.yellow)
                    .text(`Flip $${gammaFlip.toFixed(0)}`);
            }
        }

        // ── Call wall marker (green, "Resistance") ──
        if (callWall) {
            const cwX = xScale(callWall);
            if (cwX >= 0 && cwX <= chartW) {
                g.append('line')
                    .attr('x1', cwX).attr('x2', cwX)
                    .attr('y1', 0).attr('y2', chartH)
                    .attr('stroke', colors.green)
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '3,3')
                    .attr('opacity', 0.6);

                // Triangle marker
                g.append('polygon')
                    .attr('points', `${cwX - 5},${0} ${cwX + 5},${0} ${cwX},${8}`)
                    .attr('fill', colors.green)
                    .attr('opacity', 0.8);

                g.append('text')
                    .attr('x', cwX + 6).attr('y', 18)
                    .attr('font-size', '8px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.green)
                    .text(`Resistance $${callWall.toFixed(0)}`);
            }
        }

        // ── Put wall marker (red, "Support") ──
        if (putWall) {
            const pwX = xScale(putWall);
            if (pwX >= 0 && pwX <= chartW) {
                g.append('line')
                    .attr('x1', pwX).attr('x2', pwX)
                    .attr('y1', 0).attr('y2', chartH)
                    .attr('stroke', colors.red)
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '3,3')
                    .attr('opacity', 0.6);

                // Inverted triangle marker
                g.append('polygon')
                    .attr('points', `${pwX - 5},${chartH} ${pwX + 5},${chartH} ${pwX},${chartH - 8}`)
                    .attr('fill', colors.red)
                    .attr('opacity', 0.8);

                g.append('text')
                    .attr('x', pwX + 6).attr('y', chartH - 6)
                    .attr('font-size', '8px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', colors.red)
                    .text(`Support $${putWall.toFixed(0)}`);
            }
        }

        // ── Regime annotation (top-left) ──
        const isShortGamma = regime === 'SHORT_GAMMA' || netGEX < 0;
        const regimeColor = isShortGamma ? colors.red : colors.green;
        const regimeText = isShortGamma
            ? 'Dealers SHORT gamma -- moves amplified'
            : 'Dealers LONG gamma -- moves dampened';

        const annotG = g.append('g')
            .attr('transform', `translate(8, 8)`);

        annotG.append('rect')
            .attr('x', -4).attr('y', -10)
            .attr('width', 260).attr('height', 22)
            .attr('rx', 4)
            .attr('fill', `${regimeColor}12`)
            .attr('stroke', `${regimeColor}30`)
            .attr('stroke-width', 0.5);

        annotG.append('text')
            .attr('x', 2).attr('y', 4)
            .attr('font-size', '10px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', regimeColor)
            .attr('font-weight', 600)
            .text(regimeText);

        // ── Y axis (left) ──
        const yAxis = d3.axisLeft(yScale)
            .ticks(6)
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

        // Y-axis label
        g.append('text')
            .attr('transform', 'rotate(-90)')
            .attr('x', -chartH / 2).attr('y', -46)
            .attr('text-anchor', 'middle')
            .attr('font-size', '9px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('Net GEX ($)');

        // ── X axis (bottom) ──
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(8, filtered.length))
            .tickSize(0)
            .tickFormat(v => `$${v.toFixed(0)}`);

        g.append('g')
            .attr('transform', `translate(0,${chartH})`)
            .call(xAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

        // ── Vanna/Charm compass (bottom-right corner) ──
        const compassR = 28;
        const compassCX = chartW - compassR - 8;
        const compassCY = chartH - compassR - 30;
        const compassG = g.append('g')
            .attr('transform', `translate(${compassCX},${compassCY})`);

        // Background circle
        compassG.append('circle')
            .attr('r', compassR)
            .attr('fill', `${colors.card}CC`)
            .attr('stroke', colors.border)
            .attr('stroke-width', 0.5);

        // Crosshairs
        compassG.append('line')
            .attr('x1', -compassR + 4).attr('x2', compassR - 4)
            .attr('y1', 0).attr('y2', 0)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);
        compassG.append('line')
            .attr('x1', 0).attr('x2', 0)
            .attr('y1', -compassR + 4).attr('y2', compassR - 4)
            .attr('stroke', colors.border).attr('stroke-width', 0.5);

        // Axis labels
        compassG.append('text')
            .attr('x', compassR - 2).attr('y', -3)
            .attr('text-anchor', 'end')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('V+');

        compassG.append('text')
            .attr('x', -compassR + 2).attr('y', -3)
            .attr('text-anchor', 'start')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('V-');

        compassG.append('text')
            .attr('x', 0).attr('y', -compassR + 10)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('C+');

        compassG.append('text')
            .attr('x', 0).attr('y', compassR - 4)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('C-');

        // Normalize vanna/charm to fit within compass
        const maxMag = Math.max(Math.abs(vannaExp), Math.abs(charmExp), 1);
        const arrowX = (vannaExp / maxMag) * (compassR - 6);
        const arrowY = -(charmExp / maxMag) * (compassR - 6); // negative because SVG Y is inverted

        // Arrow
        compassG.append('line')
            .attr('x1', 0).attr('y1', 0)
            .attr('x2', arrowX).attr('y2', arrowY)
            .attr('stroke', colors.accent)
            .attr('stroke-width', 2)
            .attr('stroke-linecap', 'round');

        // Arrowhead dot
        compassG.append('circle')
            .attr('cx', arrowX).attr('cy', arrowY)
            .attr('r', 3)
            .attr('fill', colors.accent);

        // Label below compass
        compassG.append('text')
            .attr('x', 0).attr('y', compassR + 12)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('Dealer flow direction');

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
            .attr('r', 4)
            .attr('fill', '#E8F0F8')
            .attr('stroke', colors.bg)
            .attr('stroke-width', 1.5);

        // Hover overlay
        const bisect = d3.bisector(d => d.strike).left;

        g.append('rect')
            .attr('width', chartW).attr('height', chartH)
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
                const idx = bisect(filtered, x0, 1);
                const d0 = filtered[idx - 1];
                const d1 = filtered[idx];
                if (!d0) return;
                const d = d1 && (x0 - d0.strike > d1.strike - x0) ? d1 : d0;

                const cx = xScale(d.strike);
                const cy = yScale(d.gex);

                crosshairG.select('.ch-v').attr('x1', cx).attr('x2', cx);
                crosshairG.select('.ch-h').attr('y1', cy).attr('y2', cy);
                crosshairG.select('circle').attr('cx', cx).attr('cy', cy);

                if (tooltipRef.current) {
                    const tooltip = tooltipRef.current;
                    tooltip.style.display = 'flex';
                    const gexColor = d.gex >= 0 ? colors.green : colors.red;
                    const interp = d.gex >= 0
                        ? 'Dealers long gamma here (stabilizing)'
                        : 'Dealers short gamma here (amplifying)';
                    tooltip.innerHTML =
                        `<span style="color:${colors.text};font-weight:600">$${d.strike.toFixed(0)}</span>` +
                        `<span style="color:${gexColor};margin-left:10px;font-weight:600">${formatGEX(d.gex)}</span>` +
                        `<span style="color:${colors.textMuted};margin-left:10px;font-size:10px">${interp}</span>`;
                }
            })
            .on('click', function (event) {
                if (!onStrikeClick) return;
                const coords = d3.pointer(event, this);
                const x0 = xScale.invert(coords[0]);
                const idx = bisect(filtered, x0, 1);
                const d0 = filtered[idx - 1];
                const d1 = filtered[idx];
                if (!d0) return;
                const d = d1 && (x0 - d0.strike > d1.strike - x0) ? d1 : d0;
                onStrikeClick(d.strike);
            });

    }, [gexData, width, ticker, spot, onStrikeClick]);

    if (!gexData || gexData.error) {
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
                {gexData?.error || 'No GEX data available'}
            </div>
        );
    }

    const regime = gexData.regime;
    const netGEX = gexData.gex_aggregate || 0;

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
                    }}>DEALER GEX PROFILE</span>
                    <span style={{
                        fontSize: '10px',
                        padding: '1px 6px',
                        borderRadius: '3px',
                        fontWeight: 600,
                        fontFamily: "'JetBrains Mono', monospace",
                        background: regime === 'SHORT_GAMMA' ? `${colors.red}18` : regime === 'LONG_GAMMA' ? `${colors.green}18` : `${colors.yellow}18`,
                        color: regime === 'SHORT_GAMMA' ? colors.red : regime === 'LONG_GAMMA' ? colors.green : colors.yellow,
                    }}>
                        {regime || 'UNKNOWN'}
                    </span>
                </div>
                <div style={{
                    fontSize: '10px', color: colors.textMuted,
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    Net: <span style={{
                        color: netGEX >= 0 ? colors.green : colors.red,
                        fontWeight: 600,
                    }}>{formatGEX(netGEX)}</span>
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

            {/* Footer metrics */}
            <div style={{
                display: 'flex', gap: '16px', padding: '6px 12px 10px 12px',
                fontSize: '10px', fontFamily: "'JetBrains Mono', monospace",
                color: colors.textMuted, borderTop: `1px solid ${colors.border}`,
                flexWrap: 'wrap',
            }}>
                {gexData.gamma_flip != null && (
                    <span>Flip: <span style={{ color: colors.yellow }}>${gexData.gamma_flip.toFixed(0)}</span></span>
                )}
                {gexData.call_wall != null && (
                    <span>Call Wall: <span style={{ color: colors.green }}>${gexData.call_wall.toFixed(0)}</span></span>
                )}
                {gexData.put_wall != null && (
                    <span>Put Wall: <span style={{ color: colors.red }}>${gexData.put_wall.toFixed(0)}</span></span>
                )}
                {gexData.vanna_exposure != null && (
                    <span>Vanna: <span style={{ color: colors.text }}>{formatGEX(gexData.vanna_exposure)}</span></span>
                )}
                {gexData.charm_exposure != null && (
                    <span>Charm: <span style={{ color: colors.text }}>{formatGEX(gexData.charm_exposure)}</span></span>
                )}
            </div>
        </div>
    );
}
