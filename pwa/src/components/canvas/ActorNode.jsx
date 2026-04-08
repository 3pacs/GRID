import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const color = NODE_COLORS.actor;

function ActorNode({ data }) {
    return (
        <div style={{ ...baseNodeStyle, borderColor: color }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />
            <div style={labelStyle}>{data.label || 'Actor'}</div>
            {data.category && (
                <span style={badgeStyle(color)}>{data.category}</span>
            )}
            {data.trust_score != null && (
                <div style={metaStyle}>Trust: {(data.trust_score * 100).toFixed(0)}%</div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(ActorNode);
