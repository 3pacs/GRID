import React from 'react';

export default function ConfidenceMeter({ value = 0, label = '', color }) {
    const pct = Math.round(value * 100);
    const barColor = color || (value < 0.4 ? '#5A7080' : value < 0.7 ? '#1A6EBF' : '#B8922A');

    return (
        <div style={{ width: '100%' }}>
            {label && (
                <div style={{
                    display: 'flex', justifyContent: 'space-between', marginBottom: '4px',
                    fontSize: '12px', color: '#5A7080', fontFamily: "'IBM Plex Sans', sans-serif",
                }}>
                    <span>{label}</span>
                    <span style={{ fontFamily: "'JetBrains Mono', monospace", color: barColor }}>
                        {pct}%
                    </span>
                </div>
            )}
            <div style={{
                height: '6px', borderRadius: '3px', background: '#1A2840',
                overflow: 'hidden',
            }}>
                <div style={{
                    height: '100%', borderRadius: '3px',
                    background: barColor,
                    width: `${pct}%`,
                    transition: 'width 0.6s ease',
                }} />
            </div>
        </div>
    );
}
