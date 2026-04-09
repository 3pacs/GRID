import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.actor;

function ActorNode({ data, selected }) {
    const influence = data.influence_score ?? data.trust_score ?? 0;
    const scale = 1 + Math.min(influence, 1) * 0.15;

    return (
        <div style={{
            ...glowNodeStyle('actor', selected),
            transform: `scale(${scale})`,
            transformOrigin: 'center center',
        }}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            <div style={labelStyle}>{data.label || 'Actor'}</div>
            {data.category && (
                <span style={badgeStyle(color)}>{data.category}</span>
            )}
            {data.tier && (
                <span style={badgeStyle('#1E2A3A')}>{data.tier}</span>
            )}
            {data.trust_score != null && (
                <div style={metaStyle}>Trust {(data.trust_score * 100).toFixed(0)}%</div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(ActorNode);
