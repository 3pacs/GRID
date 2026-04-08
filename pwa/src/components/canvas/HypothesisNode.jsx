import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

function HypothesisNode({ data }) {
    const isAnti = data.role === 'antithesis';
    const color = isAnti ? '#EF4444' : NODE_COLORS.hypothesis;

    return (
        <div style={{ ...baseNodeStyle, borderColor: color, maxWidth: 260 }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />
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
                    Confidence: {typeof data.confidence === 'number'
                        ? (data.confidence * 100).toFixed(0) + '%'
                        : data.confidence}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(HypothesisNode);
