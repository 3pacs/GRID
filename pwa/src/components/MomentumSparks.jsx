import React from 'react';

const indicators = [
    { key: 'SPX', label: 'S&P 500', aliases: ['SP500', 'SPX', 'SPY', 'ES'] },
    { key: 'VIX', label: 'VIX', aliases: ['VIX', 'CBOE_VIX'] },
    { key: 'US10Y', label: '10Y Yield', aliases: ['US10Y', 'TNX', 'DGS10', 'TREASURY_10Y'] },
    { key: 'DXY', label: 'Dollar', aliases: ['DXY', 'DOLLAR', 'USD_INDEX'] },
    { key: 'GC', label: 'Gold', aliases: ['GC', 'GOLD', 'XAUUSD'] },
    { key: 'BTC', label: 'Bitcoin', aliases: ['BTC', 'BITCOIN', 'BTCUSD'] },
];

function matchIndicator(signalData, aliases) {
    if (!signalData) return null;
    const items = Array.isArray(signalData) ? signalData
        : signalData?.features || signalData?.signals || [];

    for (const alias of aliases) {
        const found = items.find(s => {
            const name = (s.ticker || s.feature || s.name || '').toUpperCase();
            return name === alias || name.includes(alias);
        });
        if (found) return found;
    }
    return null;
}

/**
 * Render a tiny sparkline bar chart from an array of values.
 * Pure inline divs, no canvas/SVG needed.
 */
function Sparkline({ values, positive }) {
    if (!values || values.length === 0) {
        return (
            <div style={{
                display: 'flex', alignItems: 'flex-end', gap: '1px',
                height: '24px', width: '52px',
            }}>
                {Array.from({ length: 7 }).map((_, i) => (
                    <div key={i} style={{
                        flex: 1, height: '3px', borderRadius: '1px',
                        background: '#1A2840',
                    }} />
                ))}
            </div>
        );
    }

    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;

    return (
        <div style={{
            display: 'flex', alignItems: 'flex-end', gap: '1px',
            height: '24px', width: '52px',
        }}>
            {values.slice(-7).map((v, i) => {
                const h = Math.max(3, ((v - min) / range) * 18);
                const isLast = i === values.slice(-7).length - 1;
                return (
                    <div
                        key={i}
                        style={{
                            flex: 1,
                            height: `${h}px`,
                            borderRadius: '1px',
                            background: isLast
                                ? (positive ? '#22C55E' : '#EF4444')
                                : (positive ? '#22C55E44' : '#EF444444'),
                        }}
                    />
                );
            })}
        </div>
    );
}

export default function MomentumSparks({ signals, physics }) {
    return (
        <div style={{
            background: '#0D1520', borderRadius: '10px', padding: '10px 14px',
            border: '1px solid #1A2840',
        }}>
            <div style={{
                fontSize: '12px', color: '#5A7080', letterSpacing: '1px',
                fontFamily: "'JetBrains Mono', monospace", marginBottom: '8px',
            }}>
                MOMENTUM
            </div>
            <div style={{
                display: 'flex', gap: '8px', overflowX: 'auto',
                paddingBottom: '4px',
                scrollbarWidth: 'thin',
                scrollbarColor: '#1A2840 transparent',
            }}>
                {indicators.map(ind => {
                    const sig = matchIndicator(signals, ind.aliases);
                    const phys = matchIndicator(physics?.features || physics?.indicators, ind.aliases);

                    const value = sig?.value ?? sig?.latest_value ?? phys?.value ?? null;
                    const change = sig?.change ?? sig?.pct_change ?? phys?.momentum ?? null;
                    const history = sig?.history ?? sig?.recent_values ?? phys?.history ?? [];
                    const positive = change != null ? change >= 0 : null;

                    return (
                        <div
                            key={ind.key}
                            style={{
                                minWidth: '88px', flex: '0 0 auto',
                                background: '#080C10', borderRadius: '6px',
                                padding: '8px', display: 'flex',
                                flexDirection: 'column', gap: '4px',
                            }}
                        >
                            <div style={{
                                display: 'flex', justifyContent: 'space-between',
                                alignItems: 'center',
                            }}>
                                <span style={{
                                    fontSize: '11px', fontWeight: 700, color: '#8AA0B8',
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>
                                    {ind.label}
                                </span>
                                {change != null && (
                                    <span style={{
                                        fontSize: '10px', fontWeight: 600,
                                        color: positive ? '#22C55E' : '#EF4444',
                                        fontFamily: "'JetBrains Mono', monospace",
                                    }}>
                                        {positive ? '+' : ''}{typeof change === 'number' ? change.toFixed(2) : change}%
                                    </span>
                                )}
                            </div>
                            <div style={{
                                fontSize: '13px', fontWeight: 700, color: '#E8F0F8',
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>
                                {value != null
                                    ? (typeof value === 'number'
                                        ? (Math.abs(value) >= 1000 ? value.toLocaleString(undefined, { maximumFractionDigits: 0 })
                                            : value.toFixed(2))
                                        : value)
                                    : '--'}
                            </div>
                            <Sparkline values={history} positive={positive} />
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
