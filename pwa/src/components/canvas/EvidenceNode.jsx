import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, baseNodeStyle, labelStyle, metaStyle, badgeStyle } from './nodeStyles.js';

const color = NODE_COLORS.evidence;

const confidenceColors = {
    confirmed: '#10B981',
    derived: '#3B82F6',
    estimated: '#F59E0B',
    rumored: '#EF4444',
    inferred: '#6B7280',
};

function EvidenceNode({ data }) {
    const confidence = data.confidence || 'derived';
    const confidenceColor = confidenceColors[confidence] || '#6B7280';

    return (
        <div style={{ ...baseNodeStyle, borderColor: confidenceColor, borderWidth: 2, maxWidth: 240 }}>
            <Handle type="target" position={Position.Left} style={{ background: color }} />

            {/* Evidence type badge */}
            <span style={badgeStyle(color)}>{data.evidence_type || 'evidence'}</span>

            {/* Confidence badge */}
            <span style={badgeStyle(confidenceColor)}>{confidence}</span>

            {/* Title / label */}
            <div style={labelStyle}>{data.label || 'Evidence'}</div>

            {/* Content preview (truncated) */}
            {data.content && (
                <div style={{ ...metaStyle, maxHeight: 60, overflow: 'hidden', lineHeight: '1.4' }}>
                    {data.content.length > 120 ? data.content.slice(0, 120) + '...' : data.content}
                </div>
            )}

            {/* Source link */}
            {data.source_url && (
                <div style={{ ...metaStyle, color: '#3B82F6', cursor: 'pointer', marginTop: 4 }}>
                    Source →
                </div>
            )}

            {/* Captured date */}
            {data.captured_at && (
                <div style={{ ...metaStyle, fontSize: 10 }}>
                    {new Date(data.captured_at).toLocaleDateString()}
                </div>
            )}

            <Handle type="source" position={Position.Right} style={{ background: color }} />
        </div>
    );
}

export default React.memo(EvidenceNode);
