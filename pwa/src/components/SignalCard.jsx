import React from 'react';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';
import { interpretZScore, getFeatureLabel } from '../utils/interpret.js';

export default function SignalCard({ name, value, direction, magnitude, z_score, family }) {
    const dirIcon = direction === 'up'
        ? <TrendingUp size={14} color="#1A7A4A" />
        : direction === 'down'
            ? <TrendingDown size={14} color="#8B1F1F" />
            : <Minus size={14} color="#5A7080" />;

    const zColor = z_score != null
        ? (Math.abs(z_score) > 2 ? '#EF4444' : Math.abs(z_score) > 1 ? '#F59E0B' : '#5A7080')
        : '#5A7080';

    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '12px 16px',
            border: '1px solid #1A2840', minWidth: '140px', flexShrink: 0,
        }}>
            <div title={getFeatureLabel(name)} style={{
                fontSize: '11px', color: '#8AA0B8', marginBottom: '2px',
                fontFamily: "'IBM Plex Sans', sans-serif", fontWeight: 600,
                whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                lineHeight: '1.3',
            }}>
                {getFeatureLabel(name)}
            </div>
            <div style={{
                fontSize: '9px', color: '#5A7080', marginBottom: '6px',
                fontFamily: "'JetBrains Mono', monospace",
            }}>
                {name}{family ? ` · ${family}` : ''}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '16px', fontWeight: 500, color: '#C8D8E8',
                }}>
                    {typeof value === 'number' ? (Math.abs(value) >= 100 ? value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : value.toFixed(2)) : value}
                </span>
                {dirIcon}
                {z_score != null && (
                    <span style={{
                        fontSize: '10px', fontWeight: 600, padding: '1px 5px',
                        borderRadius: '3px', background: `${zColor}20`, color: zColor,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {z_score >= 0 ? '+' : ''}{z_score.toFixed(1)}σ
                    </span>
                )}
            </div>
            {/* Interpretation */}
            {z_score != null && (
                <div style={{
                    fontSize: '9px', color: '#5A708099', marginTop: '4px',
                    lineHeight: '1.4',
                }}>
                    {interpretZScore(z_score, name)}
                </div>
            )}
        </div>
    );
}
