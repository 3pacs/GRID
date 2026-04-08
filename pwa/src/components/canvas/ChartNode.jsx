import React, { useEffect, useRef } from 'react';
import { Handle, Position } from '@xyflow/react';
import * as d3 from 'd3';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const color = NODE_COLORS.chart;
const CHART_W = 200;
const CHART_H = 80;

function ChartNode({ data }) {
    const svgRef = useRef(null);

    useEffect(() => {
        if (!svgRef.current || !data.prices?.length) return;

        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const prices = data.prices; // [{date, close}]
        const margin = { top: 4, right: 4, bottom: 16, left: 32 };
        const w = CHART_W - margin.left - margin.right;
        const h = CHART_H - margin.top - margin.bottom;

        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        const x = d3.scaleTime()
            .domain(d3.extent(prices, d => new Date(d.date)))
            .range([0, w]);

        const y = d3.scaleLinear()
            .domain(d3.extent(prices, d => d.close))
            .nice()
            .range([h, 0]);

        // Area fill
        const area = d3.area()
            .x(d => x(new Date(d.date)))
            .y0(h)
            .y1(d => y(d.close))
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(prices)
            .attr('d', area)
            .attr('fill', color)
            .attr('fill-opacity', 0.15);

        // Line
        const line = d3.line()
            .x(d => x(new Date(d.date)))
            .y(d => y(d.close))
            .curve(d3.curveMonotoneX);

        g.append('path')
            .datum(prices)
            .attr('d', line)
            .attr('fill', 'none')
            .attr('stroke', color)
            .attr('stroke-width', 1.5);

        // Current price dot
        const last = prices[prices.length - 1];
        g.append('circle')
            .attr('cx', x(new Date(last.date)))
            .attr('cy', y(last.close))
            .attr('r', 3)
            .attr('fill', color);

        // Y axis (price)
        g.append('g')
            .call(d3.axisLeft(y).ticks(3).tickFormat(d3.format('$.0f')))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick line').attr('stroke', '#1E2A3A'))
            .call(g => g.selectAll('.tick text').attr('fill', '#5A7080').attr('font-size', 9));

        // X axis (date)
        g.append('g')
            .attr('transform', `translate(0,${h})`)
            .call(d3.axisBottom(x).ticks(3).tickFormat(d3.timeFormat('%b %d')))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick line').attr('stroke', '#1E2A3A'))
            .call(g => g.selectAll('.tick text').attr('fill', '#5A7080').attr('font-size', 9));

    }, [data.prices]);

    // Price change calculation
    const prices = data.prices || [];
    const first = prices[0]?.close;
    const last = prices[prices.length - 1]?.close;
    const change = first && last ? ((last - first) / first * 100).toFixed(1) : null;
    const changeColor = change > 0 ? '#10B981' : change < 0 ? '#EF4444' : '#6B7280';

    return (
        <div style={{ ...baseNodeStyle, borderColor: color, padding: '8px 8px 4px', minWidth: CHART_W + 16 }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />

            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <span style={{ ...labelStyle, fontSize: 12 }}>{data.label || data.ticker || 'Chart'}</span>
                {change != null && (
                    <span style={{ fontSize: 11, fontWeight: 600, color: changeColor }}>
                        {change > 0 ? '+' : ''}{change}%
                    </span>
                )}
            </div>

            {prices.length > 0 ? (
                <svg ref={svgRef} width={CHART_W} height={CHART_H} />
            ) : (
                <div style={{ ...metaStyle, height: CHART_H, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    No price data
                </div>
            )}

            {last && (
                <div style={{ ...metaStyle, textAlign: 'right', fontSize: 10, fontFamily: "'JetBrains Mono', monospace" }}>
                    ${last.toFixed(2)}
                </div>
            )}

            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(ChartNode);
