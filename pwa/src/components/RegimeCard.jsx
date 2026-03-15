import React from 'react';
import ConfidenceMeter from './ConfidenceMeter.jsx';

const stateColors = {
    'Expansion': '#1A7A4A',
    'Late Cycle': '#8A6000',
    'Contraction': '#8B1F1F',
    'Recovery': '#1A6EBF',
    'Mixed': '#5A7080',
    'UNCALIBRATED': '#5A7080',
};

export default function RegimeCard({ regime, compact = false, onClick }) {
    if (!regime) return null;

    const color = stateColors[regime.state] || '#5A7080';

    if (compact) {
        return (
            <span onClick={onClick} style={{
                display: 'inline-flex', alignItems: 'center', gap: '8px',
                padding: '4px 12px', borderRadius: '6px', cursor: onClick ? 'pointer' : 'default',
                background: `${color}22`, border: `1px solid ${color}44`,
            }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace",
                    fontSize: '13px', fontWeight: 500, color,
                }}>
                    {regime.state}
                </span>
                <span style={{ fontSize: '12px', color: '#5A7080' }}>
                    {Math.round((regime.confidence || 0) * 100)}%
                </span>
            </span>
        );
    }

    return (
        <div onClick={onClick} style={{
            background: '#0D1520', borderRadius: '12px', padding: '20px',
            border: `1px solid ${color}33`, cursor: onClick ? 'pointer' : 'default',
        }}>
            <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '8px', fontFamily: "'IBM Plex Sans', sans-serif" }}>
                CURRENT REGIME
            </div>
            <div style={{
                fontFamily: "'JetBrains Mono', monospace",
                fontSize: '28px', fontWeight: 700, color,
                marginBottom: '16px',
            }}>
                {regime.state}
            </div>
            <ConfidenceMeter value={regime.confidence || 0} label="Confidence" color={color} />
            {regime.transition_probability > 0 && (
                <div style={{
                    marginTop: '12px', fontSize: '13px', color: '#5A7080',
                    fontFamily: "'IBM Plex Sans', sans-serif",
                }}>
                    {Math.round(regime.transition_probability * 100)}% chance of shift
                </div>
            )}
            {regime.top_drivers?.length > 0 && (
                <div style={{ marginTop: '12px', display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                    {regime.top_drivers.slice(0, 3).map((d, i) => (
                        <span key={i} style={{
                            fontSize: '11px', padding: '2px 8px', borderRadius: '4px',
                            background: '#1A284022', color: '#C8D8E8',
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {d.feature}
                        </span>
                    ))}
                </div>
            )}
        </div>
    );
}
