import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.evidence;

const confidenceColors = {
    confirmed: '#10B981',
    derived: '#3B82F6',
    estimated: '#F59E0B',
    rumored: '#EF4444',
    inferred: '#6B7280',
};

function EvidenceNode({ data, selected }) {
    const confidence = data.confidence || 'derived';
    const confidenceColor = confidenceColors[confidence] || '#6B7280';

    return (
        <div style={{
            ...glowNodeStyle('evidence', selected),
            borderColor: confidenceColor,
            borderWidth: 2,
            maxWidth: 240,
        }}>
            <Handle type="target" position={Position.Left} style={handleStyle(color)} />
            <span style={badgeStyle(color)}>{data.evidence_type || 'evidence'}</span>
            <span style={badgeStyle(confidenceColor)}>{confidence}</span>
            <div style={labelStyle}>{data.label || 'Evidence'}</div>
            {data.content && (
                <div style={{ ...metaStyle, maxHeight: 60, overflow: 'hidden', lineHeight: '1.4' }}>
                    {data.content.length > 120 ? data.content.slice(0, 120) + '...' : data.content}
                </div>
            )}
            {data.source_url && (
                <div style={{ ...metaStyle, color: '#3B82F6', cursor: 'pointer', marginTop: 4 }}>
                    Source ↗
                </div>
            )}
            {data.captured_at && (
                <div style={{ ...metaStyle, fontSize: 10 }}>
                    {new Date(data.captured_at).toLocaleDateString()}
                </div>
            )}
            <Handle type="source" position={Position.Right} style={handleStyle(color)} />
        </div>
    );
}

export default React.memo(EvidenceNode);
