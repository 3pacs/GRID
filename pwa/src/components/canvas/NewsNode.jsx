import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.news;

const URGENCY_COLORS = {
    breaking: '#EF4444',
    high: '#F59E0B',
    normal: '#3B82F6',
    low: '#6B7280',
};

const SENTIMENT_COLORS = {
    bullish: '#10B981',
    bearish: '#EF4444',
    neutral: '#6B7280',
};

function NewsNode({ data, selected }) {
    const urgency = data.urgency || 'normal';
    const urgencyColor = URGENCY_COLORS[urgency] || URGENCY_COLORS.normal;
    const sentiment = data.sentiment || 'neutral';
    const sentimentColor = SENTIMENT_COLORS[sentiment] || SENTIMENT_COLORS.neutral;

    const isBreaking = urgency === 'breaking';

    return (
        <div style={{
            ...glowNodeStyle('news', selected),
            maxWidth: 280,
            borderColor: urgencyColor,
            ...(isBreaking ? { animation: 'nodePulse 2s ease-in-out infinite' } : {}),
        }}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />

            <div style={{ display: 'flex', gap: 4, flexWrap: 'wrap', marginBottom: 4 }}>
                <span style={badgeStyle(urgencyColor)}>
                    {isBreaking ? '⚡ BREAKING' : urgency.toUpperCase()}
                </span>
                <span style={badgeStyle(sentimentColor)}>{sentiment}</span>
                {data.source && (
                    <span style={badgeStyle('#1E2A3A')}>{data.source}</span>
                )}
            </div>

            <div style={{ ...labelStyle, whiteSpace: 'normal', wordWrap: 'break-word', lineHeight: '1.3' }}>
                {data.label || data.headline || 'News'}
            </div>

            {data.summary && (
                <div style={{ ...metaStyle, maxHeight: 48, overflow: 'hidden', lineHeight: '1.4', marginTop: 4 }}>
                    {data.summary.length > 140 ? data.summary.slice(0, 140) + '...' : data.summary}
                </div>
            )}

            {/* Entity tags */}
            {data.entities?.length > 0 && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 3, marginTop: 6 }}>
                    {data.entities.slice(0, 5).map((ent, i) => (
                        <span key={i} style={{
                            fontSize: 9,
                            padding: '1px 5px',
                            borderRadius: 3,
                            background: 'rgba(59, 130, 246, 0.15)',
                            color: '#7CB3F0',
                            border: '1px solid rgba(59, 130, 246, 0.2)',
                        }}>
                            {ent}
                        </span>
                    ))}
                    {data.entities.length > 5 && (
                        <span style={{ fontSize: 9, color: '#5A7080' }}>+{data.entities.length - 5}</span>
                    )}
                </div>
            )}

            {data.published_at && (
                <div style={{ ...metaStyle, fontSize: 10, marginTop: 4 }}>
                    {timeAgo(data.published_at)}
                </div>
            )}

            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

function timeAgo(dateStr) {
    const now = Date.now();
    const then = new Date(dateStr).getTime();
    const mins = Math.floor((now - then) / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    return `${Math.floor(hrs / 24)}d ago`;
}

export default React.memo(NewsNode);
