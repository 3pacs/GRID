import React, { useMemo } from 'react';

const regimeStages = [
    { label: 'CRISIS', color: '#EF4444', position: 0 },
    { label: 'FRAGILE', color: '#F59E0B', position: 0.25 },
    { label: 'NEUTRAL', color: '#3B82F6', position: 0.5 },
    { label: 'GROWTH', color: '#22C55E', position: 1.0 },
];

/**
 * Map regime state string to a 0-1 position on the thermometer.
 */
function regimeToPosition(regime) {
    if (!regime?.state) return 0.5;
    const state = regime.state.toLowerCase();
    const confidence = regime.confidence ?? 0.5;

    if (state.includes('contraction') || state.includes('crisis')) {
        return 0.05 + confidence * 0.15;
    }
    if (state.includes('late') || state.includes('fragile')) {
        return 0.25 + (1 - confidence) * 0.1;
    }
    if (state.includes('mixed') || state.includes('uncalibrated') || state.includes('neutral')) {
        return 0.5;
    }
    if (state.includes('recovery')) {
        return 0.6 + confidence * 0.15;
    }
    if (state.includes('expansion') || state.includes('growth')) {
        return 0.8 + confidence * 0.18;
    }
    return 0.5;
}

function positionToColor(pos) {
    if (pos < 0.25) {
        const t = pos / 0.25;
        return lerpColor('#EF4444', '#F59E0B', t);
    }
    if (pos < 0.5) {
        const t = (pos - 0.25) / 0.25;
        return lerpColor('#F59E0B', '#3B82F6', t);
    }
    const t = (pos - 0.5) / 0.5;
    return lerpColor('#3B82F6', '#22C55E', t);
}

function lerpColor(a, b, t) {
    const parseHex = (c) => [
        parseInt(c.slice(1, 3), 16),
        parseInt(c.slice(3, 5), 16),
        parseInt(c.slice(5, 7), 16),
    ];
    const [ar, ag, ab] = parseHex(a);
    const [br, bg, bb] = parseHex(b);
    const r = Math.round(ar + (br - ar) * t);
    const g = Math.round(ag + (bg - ag) * t);
    const bl = Math.round(ab + (bb - ab) * t);
    return `rgb(${r},${g},${bl})`;
}

export default function RegimeThermometer({ regime }) {
    const position = useMemo(() => regimeToPosition(regime), [regime]);
    const dotColor = positionToColor(position);
    const transProb = regime?.transition_probability;

    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '10px 14px',
            border: '1px solid #1A2840',
        }}>
            {/* Labels */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', marginBottom: '6px',
            }}>
                {regimeStages.map(stage => (
                    <span
                        key={stage.label}
                        style={{
                            fontSize: '11px', fontWeight: 700, color: stage.color,
                            fontFamily: "'JetBrains Mono', monospace",
                            letterSpacing: '0.5px', opacity: 0.8,
                        }}
                    >
                        {stage.label}
                    </span>
                ))}
            </div>

            {/* Bar */}
            <div style={{
                position: 'relative', height: '12px', borderRadius: '4px',
                background: 'linear-gradient(to right, #EF4444, #F59E0B 25%, #3B82F6 50%, #22C55E)',
                boxShadow: 'inset 0 1px 3px rgba(0,0,0,0.3)',
                overflow: 'visible',
            }}>
                {/* Current position dot */}
                <div style={{
                    position: 'absolute',
                    left: `${Math.max(2, Math.min(98, position * 100))}%`,
                    top: '50%',
                    transform: 'translate(-50%, -50%)',
                    width: '18px', height: '18px',
                    borderRadius: '50%',
                    background: dotColor,
                    border: '2px solid #0D1520',
                    boxShadow: `0 0 8px ${dotColor}88`,
                    transition: 'left 0.6s ease-out, background 0.6s ease-out',
                    zIndex: 1,
                }} />
            </div>

            {/* Bottom info */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginTop: '6px',
            }}>
                <span style={{
                    fontSize: '12px', color: dotColor,
                    fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
                }}>
                    {regime?.state || '--'}
                    {regime?.confidence != null && (
                        <span style={{ color: '#5A7080', fontWeight: 400, marginLeft: '6px' }}>
                            {Math.round(regime.confidence * 100)}%
                        </span>
                    )}
                </span>
                {transProb != null && transProb > 0 && (
                    <span style={{
                        fontSize: '11px', color: '#F59E0B',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {transProb > 0.3 ? '\u21C9' : '\u2192'} {Math.round(transProb * 100)}% shift
                    </span>
                )}
            </div>
        </div>
    );
}
