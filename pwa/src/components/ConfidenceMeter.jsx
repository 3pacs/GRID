import React from 'react';

export default function ConfidenceMeter({ value = 0, label = '', color, unscoredNote }) {
    // value == null means UNSCORED: no confidence was ever measured.
    // `null * 100` is 0 in JS, so without this guard an unmeasured decision
    // renders as a confident-looking 0% bar. Show it as unscored instead.
    if (value == null || Number.isNaN(Number(value))) {
        return (
            <div style={{ width: '100%' }}>
                {label && (
                    <div style={{
                        display: 'flex', justifyContent: 'space-between', marginBottom: '4px',
                        fontSize: '12px', color: '#5A7080', fontFamily: "'IBM Plex Sans', sans-serif",
                    }}>
                        <span>{label}</span>
                        <span style={{ fontFamily: "'JetBrains Mono', monospace", color: '#5A7080' }}>
                            unscored
                        </span>
                    </div>
                )}
                <div
                    title={unscoredNote || 'No confidence was measured for this decision'}
                    style={{
                        height: '6px', borderRadius: '3px',
                        background: 'repeating-linear-gradient(45deg, #1A2840 0 4px, #0D1520 4px 8px)',
                    }}
                />
            </div>
        );
    }

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
