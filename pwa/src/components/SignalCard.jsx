import React from 'react';
import { TrendingUp, TrendingDown, Minus } from 'lucide-react';

export default function SignalCard({ name, value, direction, magnitude }) {
    const dirIcon = direction === 'up'
        ? <TrendingUp size={14} color="#1A7A4A" />
        : direction === 'down'
            ? <TrendingDown size={14} color="#8B1F1F" />
            : <Minus size={14} color="#5A7080" />;

    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '12px 16px',
            border: '1px solid #1A2840', minWidth: '140px', flexShrink: 0,
        }}>
            <div style={{
                fontSize: '11px', color: '#5A7080', marginBottom: '6px',
                fontFamily: "'IBM Plex Sans', sans-serif",
                whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
            }}>
                {name}
            </div>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '16px', fontWeight: 500, color: '#C8D8E8',
                }}>
                    {typeof value === 'number' ? value.toFixed(2) : value}
                </span>
                {dirIcon}
            </div>
            {magnitude !== undefined && (
                <div style={{
                    fontSize: '10px', color: '#5A7080', marginTop: '4px',
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    mag: {magnitude.toFixed(2)}
                </div>
            )}
        </div>
    );
}
