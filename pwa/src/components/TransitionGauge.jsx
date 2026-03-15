import React from 'react';

export default function TransitionGauge({ probability = 0, horizon = 'next quarter' }) {
    const pct = Math.round(probability * 100);
    const color = pct > 60 ? '#8B1F1F' : pct > 30 ? '#8A6000' : '#1A7A4A';

    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '14px 16px',
            border: '1px solid #1A2840',
        }}>
            <div style={{
                fontSize: '11px', color: '#5A7080', marginBottom: '8px',
                fontFamily: "'IBM Plex Sans', sans-serif",
            }}>
                TRANSITION PROBABILITY
            </div>
            <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '24px', fontWeight: 700, color,
                }}>
                    {pct}%
                </span>
                <span style={{ fontSize: '12px', color: '#5A7080' }}>
                    {horizon}
                </span>
            </div>
            <div style={{
                marginTop: '8px', height: '4px', borderRadius: '2px',
                background: '#1A2840', overflow: 'hidden',
            }}>
                <div style={{
                    height: '100%', width: `${pct}%`, background: color,
                    borderRadius: '2px', transition: 'width 0.6s ease',
                }} />
            </div>
        </div>
    );
}
