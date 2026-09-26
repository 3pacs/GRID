/** Modeled vanna/charm sensitivities. No inferred trading or OpEx forecast. */
import React, { useEffect, useRef, useState } from 'react';
import * as d3 from 'd3';
import { colors, tokens } from '../styles/shared.js';
import GEXProvenance from './GEXProvenance.jsx';

const TOTAL_HEIGHT = 225;
const COMPASS_DIAMETER = 200;
const COMPASS_R = COMPASS_DIAMETER / 2;

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

    // Main D3 render
    useEffect(() => {
        if (!svgRef.current || !d || d.error || d.stale) return;

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
            { x: COMPASS_R - 4,  y: -8, anchor: 'end',    text: 'IV rise', sub: 'modeled sensitivity' },
            { x: -(COMPASS_R - 4), y: -8, anchor: 'start', text: 'IV drop',  sub: 'modeled sensitivity' },
            { x: 0, y: -(COMPASS_R - 8), anchor: 'middle', text: 'Positive charm', sub: '' },
            { x: 0, y: COMPASS_R - 4, anchor: 'middle',    text: 'Negative charm', sub: '' },
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

        if (Number.isFinite(d.vanna_exposure) && Number.isFinite(d.charm_exposure)) {
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

        }

        // Center label: net delta change
        const netDelta = Number.isFinite(d.net_dealer_delta_change) ? d.net_dealer_delta_change : null;
        const centerColor = netDelta == null || netDelta === 0 ? colors.textMuted : netDelta > 0 ? colors.green : colors.red;
        cg.append('text')
            .attr('x', 0).attr('y', COMPASS_R * 0.18)
            .attr('text-anchor', 'middle')
            .attr('font-size', '13px')
            .attr('font-weight', 700)
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', centerColor)
            .text(netDelta == null ? 'Unavailable' : formatDelta(netDelta));

        cg.append('text')
            .attr('x', 0).attr('y', COMPASS_R * 0.18 + 12)
            .attr('text-anchor', 'middle')
            .attr('font-size', '7px')
            .attr('font-family', "'JetBrains Mono', monospace")
            .attr('fill', colors.textMuted)
            .text('net delta by OpEx');

    }, [d, width, ticker]);

    if (!d || d.error || d.stale) {
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
                {d?.stale ? 'Stale vanna/charm data unavailable' : 'No vanna/charm data available'}
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
                    }}>MODELED VANNA / CHARM</span>
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

            <GEXProvenance data={d} />
            {/* Chart */}
            <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />

            {/* Interpretation footer */}
            <div style={{
                padding: '6px 12px 10px 12px',
                fontSize: '10px', fontFamily: "'JetBrains Mono', monospace",
                color: colors.textDim, borderTop: `1px solid ${colors.border}`,
                lineHeight: '1.5',
            }}>
                Sensitivities under assumed dealer positions; not observed trades. A net delta forecast and an OpEx trading projection are unavailable.
            </div>
        </div>
    );
}
