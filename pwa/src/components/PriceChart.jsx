/**
 * PriceChart — Interactive D3 line chart for watchlist detail pages.
 *
 * Features:
 *   - Timeframe buttons: 1W | 1M | 3M | 6M | 1Y
 *   - Crosshair on hover showing date + price
 *   - Volume bars at bottom (when data includes volume)
 *   - Key levels as horizontal dashed lines
 *   - Regime bands as subtle colored background regions
 *   - Responsive width, fixed ~300px height
 *   - Smooth transitions when switching timeframes
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { colors, tokens } from '../styles/shared.js';

const TIMEFRAMES = ['1W', '1M', '3M', '6M', '1Y'];
const CHART_HEIGHT = 300;
const MARGIN = { top: 12, right: 50, bottom: 28, left: 0 };
const VOLUME_HEIGHT_RATIO = 0.18; // volume bars take up 18% of chart area

const REGIME_COLORS = {
    GROWTH: `${colors.green}08`,
    EXPANSION: `${colors.green}08`,
    FRAGILE: `${colors.yellow}08`,
    CRISIS: `${colors.red}08`,
    CONTRACTION: `${colors.red}08`,
};

const KEY_LEVEL_COLORS = {
    'Max Pain': '#8B5CF6',
    'Support': colors.green,
    'Resistance': colors.red,
    'Spot': colors.yellow,
    'Last': colors.accent,
};

export default function PriceChart({ data, ticker, period, onPeriodChange, keyLevels, regime }) {
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

    // Render chart
    useEffect(() => {
        if (!svgRef.current || !data || data.length < 2) return;

        const svg = d3.select(svgRef.current);
        const chartW = width - MARGIN.left - MARGIN.right;
        const chartH = CHART_HEIGHT - MARGIN.top - MARGIN.bottom;
        const hasVolume = data.some(d => d.volume != null && d.volume > 0);
        const priceH = hasVolume ? chartH * (1 - VOLUME_HEIGHT_RATIO) : chartH;
        const volumeH = hasVolume ? chartH * VOLUME_HEIGHT_RATIO : 0;

        // Parse data
        const parsed = data.map(d => ({
            date: new Date(d.date),
            value: d.value,
            volume: d.volume || 0,
        }));

        const isUp = parsed[parsed.length - 1].value >= parsed[0].value;
        const lineColor = isUp ? colors.green : colors.red;

        // Scales
        const xScale = d3.scaleTime()
            .domain(d3.extent(parsed, d => d.date))
            .range([0, chartW]);

        const yMin = d3.min(parsed, d => d.value);
        const yMax = d3.max(parsed, d => d.value);
        const yPad = (yMax - yMin) * 0.08 || 1;
        const yScale = d3.scaleLinear()
            .domain([yMin - yPad, yMax + yPad])
            .range([priceH, 0]);

        const vScale = hasVolume
            ? d3.scaleLinear()
                .domain([0, d3.max(parsed, d => d.volume)])
                .range([0, volumeH - 2])
            : null;

        // Clear and set up
        svg.selectAll('*').remove();
        svg.attr('width', width).attr('height', CHART_HEIGHT);

        const g = svg.append('g')
            .attr('transform', `translate(${MARGIN.left},${MARGIN.top})`);

        // ── Regime bands ──
        if (regime && regime.state) {
            const bandColor = REGIME_COLORS[regime.state.toUpperCase()] || REGIME_COLORS.FRAGILE;
            g.append('rect')
                .attr('x', 0).attr('y', 0)
                .attr('width', chartW).attr('height', priceH)
                .attr('fill', bandColor);
        }

        // ── Key levels ──
        if (keyLevels && keyLevels.length > 0) {
            const levelsG = g.append('g').attr('class', 'key-levels');
            keyLevels.forEach(level => {
                const yPos = yScale(level.value);
                if (yPos < 0 || yPos > priceH) return; // out of range

                const levelColor = KEY_LEVEL_COLORS[level.label] || colors.textMuted;
                levelsG.append('line')
                    .attr('x1', 0).attr('x2', chartW)
                    .attr('y1', yPos).attr('y2', yPos)
                    .attr('stroke', levelColor)
                    .attr('stroke-width', 1)
                    .attr('stroke-dasharray', '4,3')
                    .attr('opacity', 0.45);

                levelsG.append('text')
                    .attr('x', chartW - 2)
                    .attr('y', yPos - 3)
                    .attr('text-anchor', 'end')
                    .attr('font-size', '9px')
                    .attr('font-family', "'JetBrains Mono', monospace")
                    .attr('fill', levelColor)
                    .attr('opacity', 0.7)
                    .text(level.label);
            });
        }

        // ── Area gradient ──
        const gradientId = `price-area-grad-${ticker}`;
        const defs = svg.append('defs');
        const gradient = defs.append('linearGradient')
            .attr('id', gradientId)
            .attr('x1', '0').attr('y1', '0')
            .attr('x2', '0').attr('y2', '1');
        gradient.append('stop').attr('offset', '0%').attr('stop-color', lineColor).attr('stop-opacity', 0.15);
        gradient.append('stop').attr('offset', '100%').attr('stop-color', lineColor).attr('stop-opacity', 0);

        // ── Area fill ──
        const area = d3.area()
            .x(d => xScale(d.date))
            .y0(priceH)
            .y1(d => yScale(d.value))
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(parsed)
            .attr('fill', `url(#${gradientId})`)
            .attr('d', area)
            .attr('opacity', 0)
            .transition().duration(600).attr('opacity', 1);

        // ── Price line ──
        const line = d3.line()
            .x(d => xScale(d.date))
            .y(d => yScale(d.value))
            .curve(d3.curveMonotoneX);

        const path = g.append('path')
            .datum(parsed)
            .attr('fill', 'none')
            .attr('stroke', lineColor)
            .attr('stroke-width', 1.5)
            .attr('d', line);

        // Animate line drawing
        const totalLength = path.node().getTotalLength();
        path.attr('stroke-dasharray', `${totalLength} ${totalLength}`)
            .attr('stroke-dashoffset', totalLength)
            .transition().duration(800).ease(d3.easeCubicOut)
            .attr('stroke-dashoffset', 0);

        // ── Volume bars ──
        if (hasVolume && vScale) {
            const volumeG = g.append('g')
                .attr('transform', `translate(0,${priceH + 2})`);

            const barWidth = Math.max(1, (chartW / parsed.length) * 0.7);

            volumeG.selectAll('rect')
                .data(parsed)
                .join('rect')
                .attr('x', d => xScale(d.date) - barWidth / 2)
                .attr('y', d => volumeH - vScale(d.volume))
                .attr('width', barWidth)
                .attr('height', d => vScale(d.volume))
                .attr('fill', (d, i) => {
                    if (i === 0) return colors.textMuted;
                    return d.value >= parsed[i - 1].value ? `${colors.green}40` : `${colors.red}40`;
                })
                .attr('rx', 0.5);
        }

        // ── Y axis (right side) ──
        const yAxis = d3.axisRight(yScale)
            .ticks(5)
            .tickSize(0)
            .tickFormat(v => {
                if (v >= 1000) return `$${(v / 1000).toFixed(v >= 10000 ? 0 : 1)}k`;
                return `$${v.toFixed(v < 10 ? 2 : 0)}`;
            });

        g.append('g')
            .attr('transform', `translate(${chartW},0)`)
            .call(yAxis)
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '9px')
                .attr('font-family', "'JetBrains Mono', monospace")
                .attr('fill', colors.textMuted));

        // ── X axis ──
        const xAxis = d3.axisBottom(xScale)
            .ticks(Math.min(6, parsed.length))
            .tickSize(0)
            .tickFormat(d3.timeFormat('%b %d'));

        g.append('g')
            .attr('transform', `translate(0,${chartH})`)
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
            .attr('y1', 0).attr('y2', priceH)
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
            .attr('fill', lineColor)
            .attr('stroke', colors.bg)
            .attr('stroke-width', 1.5);

        // Tooltip overlay rect for mouse events
        const bisect = d3.bisector(d => d.date).left;

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
                const idx = bisect(parsed, x0, 1);
                const d0 = parsed[idx - 1];
                const d1 = parsed[idx];
                if (!d0) return;
                const d = d1 && (x0 - d0.date > d1.date - x0) ? d1 : d0;

                const cx = xScale(d.date);
                const cy = yScale(d.value);

                crosshairG.select('.ch-v').attr('x1', cx).attr('x2', cx);
                crosshairG.select('.ch-h').attr('y1', cy).attr('y2', cy);
                crosshairG.select('circle').attr('cx', cx).attr('cy', cy);

                // Update tooltip
                if (tooltipRef.current) {
                    const tooltip = tooltipRef.current;
                    tooltip.style.display = 'flex';
                    const dateStr = d.date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: '2-digit' });
                    const priceStr = `$${d.value.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
                    const volStr = d.volume ? `Vol: ${d.volume >= 1e6 ? (d.volume / 1e6).toFixed(1) + 'M' : (d.volume / 1e3).toFixed(0) + 'K'}` : '';
                    tooltip.innerHTML = `<span style="color:${colors.text};font-weight:600">${priceStr}</span>` +
                        `<span style="color:${colors.textMuted};margin-left:8px">${dateStr}</span>` +
                        (volStr ? `<span style="color:${colors.textMuted};margin-left:8px">${volStr}</span>` : '');
                }
            });

    }, [data, width, ticker, keyLevels, regime]);

    const prices = data || [];
    const isUp = prices.length >= 2 && prices[prices.length - 1].value >= prices[0].value;
    const lineColor = isUp ? colors.green : colors.red;
    const high = prices.length ? Math.max(...prices.map(p => p.value)) : null;
    const low = prices.length ? Math.min(...prices.map(p => p.value)) : null;

    return (
        <div ref={containerRef} style={{
            background: colors.bg,
            border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.md,
            overflow: 'hidden',
        }}>
            {/* Header row */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                padding: '10px 12px 0 12px',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                        color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                    }}>PRICE</span>
                    {high != null && low != null && (
                        <span style={{
                            fontSize: '10px', color: colors.textMuted,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            H: ${high.toLocaleString(undefined, { maximumFractionDigits: 2 })} · L: ${low.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                        </span>
                    )}
                </div>
                {/* Timeframe buttons */}
                <div style={{ display: 'flex', gap: '2px' }}>
                    {TIMEFRAMES.map(tf => (
                        <button
                            key={tf}
                            onClick={() => onPeriodChange && onPeriodChange(tf)}
                            style={{
                                background: tf === period ? `${colors.accent}25` : 'transparent',
                                border: `1px solid ${tf === period ? colors.accent : colors.border}`,
                                borderRadius: '4px',
                                padding: '3px 8px',
                                fontSize: '9px',
                                fontWeight: tf === period ? 700 : 500,
                                color: tf === period ? colors.accent : colors.textMuted,
                                cursor: 'pointer',
                                fontFamily: "'JetBrains Mono', monospace",
                                transition: `all ${tokens.transition.fast}`,
                            }}
                        >
                            {tf}
                        </button>
                    ))}
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
            {prices.length >= 2 ? (
                <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />
            ) : (
                <div style={{
                    height: CHART_HEIGHT,
                    display: 'flex', alignItems: 'center', justifyContent: 'center',
                    color: colors.textMuted, fontSize: '11px',
                }}>
                    No price data for this period
                </div>
            )}
        </div>
    );
}
