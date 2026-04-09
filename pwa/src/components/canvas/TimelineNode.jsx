import React, { useEffect, useRef } from 'react';
import { Handle, Position } from '@xyflow/react';
import * as d3 from 'd3';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.timeline;
const TL_W = 240;
const TL_H = 60;

const EVENT_COLORS = {
    congressional: '#FFD700', insider: '#3B82F6', dark_pool: '#A855F7',
    whale: '#10B981', news: '#EF4444', earnings: '#F97316',
    macro: '#EF4444', regime: '#8B5CF6', prediction: '#06B6D4', default: '#94A3B8',
};

function TimelineNode({ data, selected }) {
    const svgRef = useRef(null);

    useEffect(() => {
        if (!svgRef.current || !data.events?.length) return;
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const events = data.events;
        const margin = { top: 8, right: 8, bottom: 16, left: 8 };
        const w = TL_W - margin.left - margin.right;
        const h = TL_H - margin.top - margin.bottom;
        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        const x = d3.scaleTime().domain(d3.extent(events, d => new Date(d.date))).range([0, w]);

        g.append('line').attr('x1', 0).attr('x2', w).attr('y1', h / 2).attr('y2', h / 2)
            .attr('stroke', '#1E2A3A').attr('stroke-width', 1);

        g.selectAll('circle').data(events).enter().append('circle')
            .attr('cx', d => x(new Date(d.date))).attr('cy', h / 2).attr('r', 4)
            .attr('fill', d => EVENT_COLORS[d.type] || EVENT_COLORS.default)
            .attr('stroke', '#0D1117').attr('stroke-width', 1)
            .attr('filter', d => `drop-shadow(0 0 3px ${EVENT_COLORS[d.type] || EVENT_COLORS.default})`);

        const fmt = d3.timeFormat('%b %d');
        const dates = events.map(d => new Date(d.date)).sort((a, b) => a - b);
        g.append('text').attr('x', 0).attr('y', h + 10).attr('fill', '#5A7080').attr('font-size', 9).text(fmt(dates[0]));
        g.append('text').attr('x', w).attr('y', h + 10).attr('fill', '#5A7080').attr('font-size', 9).attr('text-anchor', 'end').text(fmt(dates[dates.length - 1]));
    }, [data.events]);

    return (
        <div style={{ ...glowNodeStyle('timeline', selected), padding: '8px 8px 4px', minWidth: TL_W + 16 }}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 4 }}>
                <span style={{ ...labelStyle, fontSize: 12 }}>{data.label || 'Timeline'}</span>
                <span style={metaStyle}>{data.events?.length || 0} events</span>
            </div>
            {data.events?.length > 0 ? (
                <svg ref={svgRef} width={TL_W} height={TL_H} />
            ) : (
                <div style={{ ...metaStyle, height: TL_H, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                    No events
                </div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(TimelineNode);
