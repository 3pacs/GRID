import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

const stateColors = {
    'GROWTH': '#22C55E', 'NEUTRAL': '#3B82F6', 'FRAGILE': '#F59E0B', 'CRISIS': '#EF4444',
    'EQUITY_VALUE': '#10B981', 'BUYOUT_ARBITRAGE': '#8B5CF6',
    'DISTRESSED_TURNAROUND': '#F97316', 'CRYPTO_CORE': '#06B6D4', 'CRYPTO_AI': '#EC4899',
};

const riskColors = {
    'Low': '#22C55E',
    'Medium': '#F59E0B',
    'High': '#EF4444',
    'Extreme': '#DC2626',
};

const s = {
    page: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' },
    headerTitle: { fontFamily: "'JetBrains Mono', monospace", fontSize: '14px', color: '#5A7080', letterSpacing: '2px' },
    sectionLabel: { fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '10px', marginTop: '16px' },
};

function StrategyCard({ strategy }) {
    const regimeColor = stateColors[strategy.regime_state] || '#5A7080';
    const riskColor = riskColors[strategy.risk_level] || '#5A7080';

    return (
        <div style={{
            background: '#0D1520', borderRadius: '12px', padding: '16px',
            border: `1px solid ${regimeColor}33`, marginBottom: '10px',
            position: 'relative', overflow: 'hidden',
        }}>
            {/* Color accent bar */}
            <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '3px',
                background: `linear-gradient(90deg, ${regimeColor}, transparent)` }} />

            {/* Header row */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '10px' }}>
                <div>
                    <div style={{ fontSize: '15px', fontWeight: 700, color: '#C8D8E8',
                        fontFamily: "'IBM Plex Sans', sans-serif", marginBottom: '4px' }}>
                        {strategy.name}
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        <span style={{ fontSize: '11px', fontWeight: 600, color: regimeColor,
                            fontFamily: "'JetBrains Mono', monospace" }}>
                            {strategy.regime_state}
                        </span>
                        <span style={{ fontSize: '10px', padding: '1px 6px', borderRadius: '4px',
                            background: `${regimeColor}22`, color: regimeColor, fontWeight: 600 }}>
                            {strategy.posture}
                        </span>
                        {strategy.source === 'default' && (
                            <span style={{ fontSize: '9px', padding: '1px 5px', borderRadius: '3px',
                                background: '#5A708022', color: '#5A7080' }}>DEFAULT</span>
                        )}
                    </div>
                </div>
                <div style={{
                    fontSize: '10px', fontWeight: 600, padding: '2px 8px', borderRadius: '6px',
                    background: `${riskColor}22`, color: riskColor,
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    {strategy.risk_level} RISK
                </div>
            </div>

            {/* Action */}
            {strategy.action && (
                <div style={{ fontSize: '13px', color: '#C8D8E8', fontWeight: 600, marginBottom: '10px' }}>
                    {strategy.action}
                </div>
            )}

            {/* Allocation breakdown */}
            {strategy.allocation && (
                <div style={{
                    background: '#080C1088', borderRadius: '8px', padding: '10px', marginBottom: '8px',
                }}>
                    <div style={{ fontSize: '9px', color: '#5A7080', letterSpacing: '0.5px', marginBottom: '4px' }}>
                        TARGET ALLOCATION
                    </div>
                    <div style={{ fontSize: '12px', color: '#8AA0B8', lineHeight: '1.5' }}>
                        {strategy.allocation}
                    </div>
                </div>
            )}

            {/* Rationale */}
            {strategy.rationale && (
                <div style={{ fontSize: '11px', color: '#5A7080', fontStyle: 'italic' }}>
                    {strategy.rationale}
                </div>
            )}

            {/* Assigned timestamp */}
            {strategy.assigned_at && (
                <div style={{ fontSize: '10px', color: '#3A4A5A', marginTop: '8px',
                    fontFamily: "'JetBrains Mono', monospace" }}>
                    assigned {new Date(strategy.assigned_at).toLocaleString()}
                </div>
            )}
        </div>
    );
}

export default function Strategy() {
    const [strategies, setStrategies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => { loadStrategies(); }, []);

    const loadStrategies = async () => {
        setLoading(true);
        setError(null);
        try {
            const data = await api.getActiveStrategies();
            setStrategies(data || []);
        } catch (err) {
            setError(err.message || 'Failed to load strategies');
        } finally {
            setLoading(false);
        }
    };

    // Group strategies by regime type (macro vs other)
    const macroRegimes = ['GROWTH', 'NEUTRAL', 'FRAGILE', 'CRISIS'];
    const macroStrategies = strategies.filter(s => macroRegimes.includes(s.regime_state));
    const otherStrategies = strategies.filter(s => !macroRegimes.includes(s.regime_state));

    return (
        <div style={s.page}>
            {/* Header */}
            <div style={s.header}>
                <span style={s.headerTitle}>STRATEGY</span>
                <button onClick={loadStrategies} style={{
                    ...shared.buttonSmall, background: 'transparent', border: `1px solid ${colors.border}`,
                    color: colors.textMuted, fontSize: '11px',
                }}>REFRESH</button>
            </div>

            {/* Description */}
            <div style={{
                fontSize: '12px', color: '#5A7080', marginBottom: '16px', lineHeight: '1.6',
            }}>
                Strategies are action plans assigned to each detected regime. They can be updated
                independently of regime detection. Default strategies serve as fallbacks until overridden.
            </div>

            {loading && (
                <div style={{ textAlign: 'center', padding: '40px', color: colors.textMuted }}>
                    Loading strategies...
                </div>
            )}

            {error && (
                <div style={{
                    background: '#3B111188', borderRadius: '8px', padding: '14px',
                    border: '1px solid #8B1F1F44', marginBottom: '12px',
                    fontSize: '13px', color: '#EF4444',
                }}>
                    {error}
                </div>
            )}

            {!loading && !error && (
                <>
                    {/* Macro regime strategies */}
                    <div style={s.sectionLabel}>MACRO REGIME STRATEGIES</div>
                    {macroStrategies.length > 0 ? (
                        macroStrategies.map((strat, i) => (
                            <StrategyCard key={strat.id || `macro-${i}`} strategy={strat} />
                        ))
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                            No macro strategies configured
                        </div>
                    )}

                    {/* Other regime strategies */}
                    {otherStrategies.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>SUBTYPE STRATEGIES</div>
                            {otherStrategies.map((strat, i) => (
                                <StrategyCard key={strat.id || `other-${i}`} strategy={strat} />
                            ))}
                        </>
                    )}
                </>
            )}
        </div>
    );
}
