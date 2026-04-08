import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const color = NODE_COLORS.signal;

function SignalNode({ data }) {
    const dirColor = data.direction === 'bullish' ? '#10B981'
        : data.direction === 'bearish' ? '#EF4444'
        : '#5A7080';

    return (
        <div style={{ ...baseNodeStyle, borderColor: color }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />
            {data.signal_type && (
                <span style={badgeStyle(color)}>{data.signal_type}</span>
            )}
            {data.direction && (
                <span style={badgeStyle(dirColor)}>{data.direction}</span>
            )}
            <div style={{ ...labelStyle, marginTop: 4 }}>
                {data.label || 'Signal'}
            </div>
            {data.ticker && (
                <div style={metaStyle}>{data.ticker}</div>
            )}
            {data.magnitude != null && (
                <div style={metaStyle}>Magnitude: {data.magnitude}</div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(SignalNode);
