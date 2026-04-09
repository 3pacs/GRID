import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.company;

function CompanyNode({ data, selected }) {
    return (
        <div style={glowNodeStyle('company', selected)}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            <div style={labelStyle}>{data.label || 'Company'}</div>
            {data.ticker && (
                <span style={badgeStyle(color)}>{data.ticker}</span>
            )}
            {data.sector && (
                <div style={metaStyle}>{data.sector}</div>
            )}
            {data.suspicion_score != null && data.suspicion_score > 0.5 && (
                <div style={{ ...metaStyle, color: '#F59E0B' }}>
                    Suspicion {(data.suspicion_score * 100).toFixed(0)}%
                </div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(CompanyNode);
