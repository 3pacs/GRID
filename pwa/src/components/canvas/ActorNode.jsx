import React from 'react';
import { Handle, Position } from '@xyflow/react';
import { NODE_COLORS, glowNodeStyle, nodeGlowActive, labelStyle, metaStyle, badgeStyle, handleStyle } from './nodeStyles.js';

const color = NODE_COLORS.actor;
const LEVER_COLOR = '#FFD700'; // gold for lever pullers

function ActorNode({ data, selected }) {
    const influence = data.influence_rank ?? data.influence_score ?? data.trust_score ?? 0;
    const scale = 1 + Math.min(influence, 1) * 0.15;
    const isLever = data.is_lever_puller;

    return (
        <div style={{
            ...glowNodeStyle('actor', selected),
            transform: `scale(${scale})`,
            transformOrigin: 'center center',
            ...(isLever ? {
                borderColor: LEVER_COLOR,
                boxShadow: selected ? nodeGlowActive(LEVER_COLOR) : `0 0 14px rgba(255, 215, 0, 0.35), inset 0 1px 0 rgba(255,255,255,0.04)`,
            } : {}),
        }}>
            <Handle type="target" position={Position.Left} style={handleStyle(isLever ? LEVER_COLOR : color)} />

            {isLever && (
                <div style={{ display: 'flex', gap: 4, marginBottom: 4, flexWrap: 'wrap' }}>
                    <span style={badgeStyle(LEVER_COLOR)}>LEVER</span>
                    {data.lever_category && (
                        <span style={badgeStyle('#1E2A3A')}>{data.lever_category}</span>
                    )}
                </div>
            )}

            <div style={labelStyle}>{data.label || 'Actor'}</div>

            {!isLever && data.category && (
                <span style={badgeStyle(color)}>{data.category}</span>
            )}
            {data.tier && (
                <span style={badgeStyle('#1E2A3A')}>{data.tier}</span>
            )}

            {isLever && (
                <div style={{ marginTop: 4 }}>
                    {data.lever_position && (
                        <div style={{ ...metaStyle, color: '#C8D8E8', fontWeight: 500 }}>{data.lever_position}</div>
                    )}
                    <div style={{ display: 'flex', gap: 8, marginTop: 2 }}>
                        {data.trust_score != null && (
                            <span style={metaStyle}>Trust {(data.trust_score * 100).toFixed(0)}%</span>
                        )}
                        {data.accuracy_pct != null && (
                            <span style={{ ...metaStyle, color: data.accuracy_pct > 60 ? '#10B981' : data.accuracy_pct > 40 ? '#F59E0B' : '#EF4444' }}>
                                Acc {data.accuracy_pct}%
                            </span>
                        )}
                    </div>
                    {data.total_signals > 0 && (
                        <div style={metaStyle}>
                            {data.correct_signals}/{data.total_signals} calls
                            {data.avg_lead_time_days ? ` · ${data.avg_lead_time_days.toFixed(0)}d lead` : ''}
                        </div>
                    )}
                    {data.motivation_model && data.motivation_model !== 'unknown' && (
                        <div style={{ ...metaStyle, color: data.motivation_model === 'self_serving' ? '#F59E0B' : '#5A7A90' }}>
                            {data.motivation_model.replace('_', ' ')}
                        </div>
                    )}
                </div>
            )}

            {!isLever && data.trust_score != null && (
                <div style={metaStyle}>Trust {(data.trust_score * 100).toFixed(0)}%</div>
            )}

            <Handle type="source" position={Position.Right} style={handleStyle(isLever ? LEVER_COLOR : color)} />
        </div>
    );
}

export default React.memo(ActorNode);
