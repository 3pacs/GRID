import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

function HypothesisNode({ data, selected }) {
    const isAnti = data.role === 'antithesis';
    const color = isAnti ? '#EF4444' : NODE_COLORS.hypothesis;
    const type = isAnti ? 'signal' : 'hypothesis'; // reuse signal glow for anti

    return (
        <div style={{ ...glowNodeStyle(isAnti ? undefined : 'hypothesis', selected), borderColor: color, maxWidth: 260 }}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            <span style={badgeStyle(color)}>
                {isAnti ? 'ANTI' : 'THESIS'}
            </span>
            {data.status && (
                <span style={badgeStyle('#1E2A3A')}>{data.status}</span>
            )}
            <div style={{ ...labelStyle, marginTop: 4, whiteSpace: 'normal', wordWrap: 'break-word' }}>
                {data.label || 'Hypothesis'}
            </div>
            {data.confidence != null && (
                <div style={metaStyle}>
                    Confidence {typeof data.confidence === 'number'
                        ? (data.confidence * 100).toFixed(0) + '%'
                        : data.confidence}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(HypothesisNode);
