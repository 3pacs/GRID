import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const color = NODE_COLORS.company;

function CompanyNode({ data }) {
    return (
        <div style={{ ...baseNodeStyle, borderColor: color }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />
            <div style={labelStyle}>{data.label || 'Company'}</div>
            {data.ticker && (
                <span style={badgeStyle(color)}>{data.ticker}</span>
            )}
            {data.sector && (
                <div style={metaStyle}>{data.sector}</div>
            )}
            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(CompanyNode);
