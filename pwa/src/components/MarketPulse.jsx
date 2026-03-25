import React from 'react';

const groups = {
    'Equities': ['SPX', 'NDX', 'RUT', 'VTI', 'EEM'],
    'Rates': ['US10Y', 'US2Y', 'US30Y', 'TIPS', 'HY'],
    'Credit': ['IG_OAS', 'HY_OAS', 'TED', 'LIBOR'],
    'Volatility': ['VIX', 'VVIX', 'MOVE', 'SKEW'],
    'Commodities': ['CL', 'GC', 'SI', 'HG', 'NG'],
    'FX': ['DXY', 'EURUSD', 'USDJPY', 'GBPUSD'],
    'Sentiment': ['AAII', 'PUT_CALL', 'MARGIN', 'FLOW'],
};

const groupColors = {
    'Equities': '#3B82F6',
    'Rates': '#8B5CF6',
    'Credit': '#EC4899',
    'Volatility': '#EF4444',
    'Commodities': '#F59E0B',
    'FX': '#06B6D4',
    'Sentiment': '#22C55E',
};

/**
 * Map a normalized value (-1 to 1) to a color.
 * -1 = bearish (red), 0 = neutral (amber), 1 = bullish (green)
 */
function valueToColor(val) {
    if (val == null || isNaN(val)) return '#1A2840';
    const v = Math.max(-1, Math.min(1, val));
    if (v > 0) {
        const t = v;
        const r = Math.round(245 * (1 - t) + 34 * t);
        const g = Math.round(158 * (1 - t) + 197 * t);
        const b = Math.round(11 * (1 - t) + 94 * t);
        return `rgb(${r},${g},${b})`;
    } else {
        const t = -v;
        const r = Math.round(245 * (1 - t) + 239 * t);
        const g = Math.round(158 * (1 - t) + 68 * t);
        const b = Math.round(11 * (1 - t) + 68 * t);
        return `rgb(${r},${g},${b})`;
    }
}

function dirArrow(val) {
    if (val == null || isNaN(val)) return '\u2014';
    if (val > 0.05) return '\u25B2';
    if (val < -0.05) return '\u25BC';
    return '\u2014';
}

function matchSignal(signalData, ticker) {
    if (!signalData) return null;
    // signalData could be an array of signal objects or an object with features
    if (Array.isArray(signalData)) {
        return signalData.find(s =>
            (s.ticker || s.feature || s.name || '').toUpperCase().includes(ticker)
        ) || null;
    }
    if (signalData.features && Array.isArray(signalData.features)) {
        return signalData.features.find(s =>
            (s.ticker || s.feature || s.name || '').toUpperCase().includes(ticker)
        ) || null;
    }
    if (signalData.signals && Array.isArray(signalData.signals)) {
        return signalData.signals.find(s =>
            (s.ticker || s.feature || s.name || '').toUpperCase().includes(ticker)
        ) || null;
    }
    return null;
}

export default function MarketPulse({ signals }) {
    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '12px',
            border: '1px solid #1A2840',
        }}>
            <div style={{
                fontSize: '12px', color: '#5A7080', letterSpacing: '1px',
                fontFamily: "'JetBrains Mono', monospace", marginBottom: '10px',
            }}>
                MARKET PULSE
            </div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {Object.entries(groups).map(([group, tickers]) => (
                    <div key={group}>
                        <div style={{
                            fontSize: '11px', color: groupColors[group] || '#5A7080',
                            fontFamily: "'JetBrains Mono', monospace",
                            letterSpacing: '0.5px', marginBottom: '4px',
                            fontWeight: 600,
                        }}>
                            {group.toUpperCase()}
                        </div>
                        <div style={{
                            display: 'flex', gap: '4px', flexWrap: 'wrap',
                        }}>
                            {tickers.map(ticker => {
                                const sig = matchSignal(signals, ticker);
                                const normVal = sig?.normalized_value ?? sig?.z_score ?? sig?.value ?? null;
                                const rawVal = sig?.value ?? sig?.latest_value ?? null;
                                const change = sig?.change ?? sig?.pct_change ?? null;
                                const bg = valueToColor(normVal);

                                return (
                                    <div
                                        key={ticker}
                                        style={{
                                            background: normVal != null ? `${bg}22` : '#0A0F16',
                                            border: `1px solid ${normVal != null ? `${bg}55` : '#1A2840'}`,
                                            borderRadius: '6px',
                                            padding: '6px 8px',
                                            minWidth: '64px',
                                            textAlign: 'center',
                                            cursor: 'default',
                                        }}
                                        title={`${ticker}: ${rawVal != null ? rawVal : '--'}`}
                                    >
                                        <div style={{
                                            fontSize: '10px', fontWeight: 700,
                                            color: normVal != null ? bg : '#5A7080',
                                            fontFamily: "'JetBrains Mono', monospace",
                                            lineHeight: '1.2',
                                        }}>
                                            {ticker}
                                        </div>
                                        <div style={{
                                            fontSize: '10px', color: '#C8D8E8',
                                            fontFamily: "'JetBrains Mono', monospace",
                                            lineHeight: '1.3',
                                        }}>
                                            {rawVal != null ? (typeof rawVal === 'number' ? rawVal.toFixed(1) : rawVal) : '--'}
                                        </div>
                                        <div style={{
                                            fontSize: '10px',
                                            color: change > 0 ? '#22C55E' : change < 0 ? '#EF4444' : '#5A7080',
                                            lineHeight: '1',
                                        }}>
                                            {dirArrow(change)}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
