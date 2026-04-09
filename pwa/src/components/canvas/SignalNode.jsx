import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.signal;

function SignalNode({ data, selected }) {
    const dirColor = data.direction === 'bullish' || data.direction === 'buy' ? '#10B981'
        : data.direction === 'bearish' || data.direction === 'sell' ? '#EF4444'
        : '#5A7080';

    return (
        <div style={glowNodeStyle('signal', selected)}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
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
                <div style={metaStyle}>Mag {Number(data.magnitude).toFixed(1)}</div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(SignalNode);
