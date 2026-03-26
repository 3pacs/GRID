/**
 * Momentum indicators — key asset cards with price, z-score bar, and signal.
 */
import React from 'react';
import { colors } from '../styles/shared.js';

const indicators = [
    { key: 'SPX', label: 'S&P 500', aliases: ['SP500', 'SP500_FULL', 'SPX', 'SPY'] },
    { key: 'VIX', label: 'VIX', aliases: ['VIX', 'CBOE_VIX', 'CRUCIX_VIX'], inverse: true },
    { key: 'US10Y', label: '10Y Yield', aliases: ['TREASURY_10Y', 'US10Y', 'DGS10'] },
    { key: 'DXY', label: 'Dollar', aliases: ['DOLLAR_INDEX', 'DXY', 'DXY_ETF'], inverse: true },
    { key: 'GC', label: 'Gold', aliases: ['GOLD', 'GOLD_FULL', 'GC'] },
    { key: 'BTC', label: 'Bitcoin', aliases: ['BTC', 'BITCOIN'] },
];

function matchIndicator(items, aliases) {
    if (!items || !Array.isArray(items)) return null;
    for (const alias of aliases) {
        const found = items.find(s => {
            const name = (s.feature_name || s.name || '').toUpperCase();
            return name === alias || name === alias + '_FULL';
        });
        if (found) return found;
    }
    return null;
}

function ZBar({ z, inverse }) {
    if (z == null) return <div style={{ height: '6px', background: colors.bg, borderRadius: '3px' }} />;
    const effective = inverse ? -z : z;
    const pct = Math.min(50, Math.abs(effective) / 3 * 50);
    const c = effective > 0.5 ? colors.green : effective < -0.5 ? colors.red : colors.textMuted;
    return (
        <div style={{ height: '6px', background: colors.bg, borderRadius: '3px', position: 'relative', overflow: 'hidden' }}>
            <div style={{ position: 'absolute', left: '50%', top: 0, bottom: 0, width: '1px', background: colors.border }} />
            <div style={{
                position: 'absolute',
                left: effective >= 0 ? '50%' : undefined,
                right: effective < 0 ? '50%' : undefined,
                top: 0, bottom: 0, width: `${pct}%`,
                background: c, borderRadius: '3px',
            }} />
        </div>
    );
}

export default function MomentumSparks({ signals }) {
    const items = signals?.features || (Array.isArray(signals) ? signals : []);

    return (
        <div style={{
            background: colors.card, borderRadius: '10px', padding: '10px 14px',
            border: `1px solid ${colors.border}`,
        }}>
            <div style={{
                fontSize: '11px', color: colors.textMuted, letterSpacing: '1.5px',
                fontFamily: "'JetBrains Mono', monospace", marginBottom: '8px',
            }}>
                KEY ASSETS
            </div>
            <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
                gap: '6px',
            }}>
                {indicators.map(ind => {
                    const sig = matchIndicator(items, ind.aliases);
                    const value = sig?.value ?? sig?.latest_value ?? null;
                    const z = sig?.z_score ?? sig?.zscore ?? null;
                    const effective = z != null ? (ind.inverse ? -z : z) : null;
                    const signalLabel = effective != null
                        ? (effective > 1.5 ? 'STRONG' : effective > 0.5 ? 'BULL' : effective < -1.5 ? 'WEAK' : effective < -0.5 ? 'BEAR' : 'FLAT')
                        : null;
                    const signalColor = effective != null
                        ? (effective > 0.5 ? colors.green : effective < -0.5 ? colors.red : colors.textMuted)
                        : null;

                    return (
                        <div key={ind.key} style={{
                            background: colors.bg, borderRadius: '6px',
                            padding: '8px', display: 'flex', flexDirection: 'column', gap: '4px',
                        }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <span style={{
                                    fontSize: '10px', fontWeight: 700, color: colors.textDim,
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>{ind.label}</span>
                                {signalLabel && (
                                    <span style={{
                                        fontSize: '8px', fontWeight: 700, color: signalColor,
                                        fontFamily: "'JetBrains Mono', monospace",
                                    }}>{signalLabel}</span>
                                )}
                            </div>
                            <div style={{
                                fontSize: '14px', fontWeight: 700, color: '#E8F0F8',
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>
                                {value != null
                                    ? (typeof value === 'number'
                                        ? (Math.abs(value) >= 1000 ? value.toLocaleString(undefined, { maximumFractionDigits: 0 }) : value.toFixed(2))
                                        : value)
                                    : '--'}
                            </div>
                            <ZBar z={z} inverse={ind.inverse} />
                            {z != null && (
                                <div style={{
                                    fontSize: '9px', color: signalColor || colors.textMuted,
                                    fontFamily: "'JetBrains Mono', monospace",
                                    textAlign: 'center',
                                }}>
                                    z={z >= 0 ? '+' : ''}{z.toFixed(1)}
                                </div>
                            )}
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
