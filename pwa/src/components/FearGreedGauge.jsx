import React, { useMemo } from 'react';

const zones = [
    { label: 'Extreme Fear', min: 0, max: 20, color: '#EF4444' },
    { label: 'Fear', min: 20, max: 40, color: '#F97316' },
    { label: 'Neutral', min: 40, max: 60, color: '#F59E0B' },
    { label: 'Greed', min: 60, max: 80, color: '#84CC16' },
    { label: 'Extreme Greed', min: 80, max: 100, color: '#22C55E' },
];

function getZone(score) {
    if (score == null || isNaN(score)) return zones[2]; // neutral fallback
    return zones.find(z => score >= z.min && score < z.max) || zones[4];
}

/**
 * Compute a composite fear/greed score from available signals.
 * Returns 0-100 where 0 = extreme fear, 100 = extreme greed.
 */
function computeScore(signals, regime) {
    const items = Array.isArray(signals) ? signals
        : signals?.features || signals?.signals || [];

    // Try to find VIX as primary fear indicator
    const vix = items.find(s =>
        (s.ticker || s.feature || s.name || '').toUpperCase().includes('VIX')
    );
    // Try put/call ratio
    const putCall = items.find(s => {
        const n = (s.ticker || s.feature || s.name || '').toUpperCase();
        return n.includes('PUT_CALL') || n.includes('PCALL');
    });
    // Regime confidence contributes
    const regimeConf = regime?.confidence ?? 0.5;
    const regimeState = (regime?.state || '').toLowerCase();

    let score = 50; // start neutral
    let factors = 0;

    // VIX inversion: high VIX = fear, low VIX = greed
    if (vix?.value != null) {
        const vixVal = vix.value;
        // VIX typically 10-40+; map to 0-100 inverted
        const vixScore = Math.max(0, Math.min(100, 100 - ((vixVal - 10) / 30) * 100));
        score += vixScore;
        factors++;
    }

    // Put/call: high = fear, low = greed
    if (putCall?.value != null) {
        const pcScore = Math.max(0, Math.min(100, 100 - ((putCall.value - 0.5) / 1.0) * 100));
        score += pcScore;
        factors++;
    }

    // Regime state
    if (regimeState.includes('expansion') || regimeState.includes('growth')) {
        score += 70 + regimeConf * 20;
        factors++;
    } else if (regimeState.includes('recovery')) {
        score += 60 + regimeConf * 10;
        factors++;
    } else if (regimeState.includes('contraction') || regimeState.includes('crisis')) {
        score += 15 - regimeConf * 10;
        factors++;
    } else if (regimeState.includes('late') || regimeState.includes('fragile')) {
        score += 35;
        factors++;
    } else {
        score += 50;
        factors++;
    }

    return factors > 0 ? Math.max(0, Math.min(100, score / (factors + 1))) : null;
}

export default function FearGreedGauge({ signals, regime }) {
    const score = useMemo(() => computeScore(signals, regime), [signals, regime]);
    const zone = getZone(score);
    const displayScore = score != null ? Math.round(score) : null;

    // SVG semicircle gauge
    const cx = 80;
    const cy = 70;
    const r = 55;
    const startAngle = Math.PI; // left (180deg)
    const endAngle = 0; // right (0deg)

    // Needle angle: score 0 = left, 100 = right
    const needleAngle = displayScore != null
        ? Math.PI - (displayScore / 100) * Math.PI
        : Math.PI / 2; // center if null

    const needleX = cx + r * 0.85 * Math.cos(needleAngle);
    const needleY = cy - r * 0.85 * Math.sin(needleAngle);

    return (
        <div style={{
            background: '#080C10', borderRadius: '8px', padding: '8px',
            textAlign: 'center', minWidth: '120px',
        }}>
            <div style={{ maxWidth: '180px', width: '100%', margin: '0 auto' }}>
            <svg width="100%" height="85" viewBox="0 0 160 85" style={{ display: 'block', margin: '0 auto' }}>
                {/* Background arc segments */}
                {zones.map((z, i) => {
                    const a1 = Math.PI - (z.min / 100) * Math.PI;
                    const a2 = Math.PI - (z.max / 100) * Math.PI;
                    const x1 = cx + r * Math.cos(a1);
                    const y1 = cy - r * Math.sin(a1);
                    const x2 = cx + r * Math.cos(a2);
                    const y2 = cy - r * Math.sin(a2);
                    return (
                        <path
                            key={i}
                            d={`M ${x1} ${y1} A ${r} ${r} 0 0 1 ${x2} ${y2}`}
                            fill="none"
                            stroke={z.color}
                            strokeWidth="6"
                            opacity="0.35"
                        />
                    );
                })}

                {/* Active arc up to current score */}
                {displayScore != null && (() => {
                    const activeEnd = Math.PI - (displayScore / 100) * Math.PI;
                    const x1 = cx + r * Math.cos(Math.PI);
                    const y1 = cy - r * Math.sin(Math.PI);
                    const x2 = cx + r * Math.cos(activeEnd);
                    const y2 = cy - r * Math.sin(activeEnd);
                    const largeArc = displayScore > 50 ? 1 : 0;
                    return (
                        <path
                            d={`M ${x1} ${y1} A ${r} ${r} 0 ${largeArc} 1 ${x2} ${y2}`}
                            fill="none"
                            stroke={zone.color}
                            strokeWidth="6"
                            strokeLinecap="round"
                        />
                    );
                })()}

                {/* Needle */}
                <line
                    x1={cx}
                    y1={cy}
                    x2={needleX}
                    y2={needleY}
                    stroke={zone.color}
                    strokeWidth="2.5"
                    strokeLinecap="round"
                />
                <circle cx={cx} cy={cy} r="3" fill={zone.color} />

                {/* Score text */}
                <text
                    x={cx}
                    y={cy - 12}
                    textAnchor="middle"
                    fill="#E8F0F8"
                    fontSize="18"
                    fontWeight="700"
                    fontFamily="'JetBrains Mono', monospace"
                >
                    {displayScore != null ? displayScore : '--'}
                </text>
            </svg>
            </div>
            <div style={{
                fontSize: '11px', fontWeight: 700, color: zone.color,
                fontFamily: "'JetBrains Mono', monospace",
                letterSpacing: '0.5px', marginTop: '-4px',
            }}>
                {displayScore != null ? zone.label.toUpperCase() : 'NO DATA'}
            </div>
        </div>
    );
}
