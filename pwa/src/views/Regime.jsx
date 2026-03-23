import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ConfidenceMeter from '../components/ConfidenceMeter.jsx';
import TransitionGauge from '../components/TransitionGauge.jsx';
import { shared, colors } from '../styles/shared.js';

const stateColors = {
    // Macro regimes
    'GROWTH': '#22C55E', 'NEUTRAL': '#3B82F6', 'FRAGILE': '#F59E0B', 'CRISIS': '#EF4444',
    // Strategy regimes
    'EQUITY_VALUE': '#10B981', 'BUYOUT_ARBITRAGE': '#8B5CF6',
    'DISTRESSED_TURNAROUND': '#F97316', 'CRYPTO_CORE': '#06B6D4', 'CRYPTO_AI': '#EC4899',
    // Fallback
    'UNCALIBRATED': '#5A7080',
};

const regimeMeta = {
    'GROWTH':                  { posture: 'Aggressive',           icon: '\u25B2', desc: 'Broad expansion — risk-on equities, credit, commodities' },
    'NEUTRAL':                 { posture: 'Balanced',             icon: '\u25C6', desc: 'Mixed signals — diversify, no strong directional bias' },
    'FRAGILE':                 { posture: 'Defensive',            icon: '\u25BC', desc: 'Deteriorating — reduce risk, favor quality and duration' },
    'CRISIS':                  { posture: 'Capital Preservation', icon: '\u26A0', desc: 'Active stress — cash, treasuries, tail hedges' },
    'EQUITY_VALUE':            { posture: 'Value Tilt',           icon: '\u2193', desc: 'Deep value detected — cheap equities vs fundamentals' },
    'BUYOUT_ARBITRAGE':        { posture: 'Event-Driven',         icon: '\u21C4', desc: 'M&A/arb spreads elevated — catalyst opportunities' },
    'DISTRESSED_TURNAROUND':   { posture: 'Contrarian',           icon: '\u21BB', desc: 'Distressed assets pricing recovery — high risk/reward' },
    'CRYPTO_CORE':             { posture: 'Crypto Allocation',    icon: '\u20BF', desc: 'On-chain + macro favor core crypto (BTC/ETH)' },
    'CRYPTO_AI':               { posture: 'AI + Crypto',          icon: '\u2605', desc: 'AI/compute tokens showing relative strength' },
};

const macroStates = new Set(['GROWTH', 'NEUTRAL', 'FRAGILE', 'CRISIS']);

export default function Regime() {
    const { currentRegime, setCurrentRegime } = useStore();
    const [allRegimes, setAllRegimes] = useState(null);
    const [history, setHistory] = useState([]);
    const [transitions, setTransitions] = useState([]);
    const [activeTab, setActiveTab] = useState('overview');

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        const [current, all, hist, trans] = await Promise.all([
            api.getCurrent().catch(() => null),
            api.getAllActiveRegimes().catch(() => null),
            api.getHistory(90).catch(() => ({ history: [] })),
            api.getTransitions().catch(() => ({ transitions: [] })),
        ]);
        if (current) setCurrentRegime(current);
        if (all) setAllRegimes(all);
        setHistory(hist.history || []);
        setTransitions(trans.transitions || []);
    };

    const regime = currentRegime || { state: 'UNCALIBRATED' };
    const primaryColor = stateColors[regime.state] || '#5A7080';
    const meta = regimeMeta[regime.state];
    const tabs = ['overview', 'strategy', 'features', 'history'];

    return (
        <div style={{ padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
                    color: '#5A7080', letterSpacing: '2px' }}>REGIME</span>
                <button onClick={loadData} style={{
                    ...shared.buttonSmall, background: 'transparent', border: `1px solid ${colors.border}`,
                    color: colors.textMuted, fontSize: '11px',
                }}>REFRESH</button>
            </div>

            {/* Primary Regime State */}
            <div style={{
                background: '#0D1520', borderRadius: '14px', padding: '20px',
                border: `1px solid ${primaryColor}44`, marginBottom: '12px',
                textAlign: 'center', position: 'relative', overflow: 'hidden',
            }}>
                <div style={{
                    position: 'absolute', top: 0, left: 0, right: 0, height: '3px',
                    background: `linear-gradient(90deg, transparent, ${primaryColor}, transparent)`,
                }} />
                <div style={{ fontSize: '11px', color: '#5A7080', letterSpacing: '1px',
                    fontFamily: "'JetBrains Mono', monospace", marginBottom: '8px' }}>
                    CURRENT STATE
                </div>
                <div style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: '28px',
                    fontWeight: 700, color: primaryColor, letterSpacing: '2px',
                }}>
                    {meta?.icon || ''} {regime.state}
                </div>
                {meta && (
                    <div style={{ marginTop: '8px' }}>
                        <span style={{
                            display: 'inline-block', padding: '3px 12px', borderRadius: '12px',
                            background: `${primaryColor}22`, color: primaryColor,
                            fontSize: '12px', fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {meta.posture}
                        </span>
                        <div style={{ fontSize: '12px', color: '#8AA0B8', marginTop: '8px', lineHeight: '1.5' }}>
                            {meta.desc}
                        </div>
                    </div>
                )}
                {regime.as_of && regime.state !== 'UNCALIBRATED' && (
                    <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '8px',
                        fontFamily: "'JetBrains Mono', monospace" }}>
                        as of {new Date(regime.as_of).toLocaleString()}
                    </div>
                )}
            </div>

            {/* Confidence + Transition side by side */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '10px', marginBottom: '12px' }}>
                <div style={{ background: '#0D1520', borderRadius: '10px', padding: '14px', border: '1px solid #1A2840' }}>
                    <ConfidenceMeter value={regime.confidence || 0} label="Confidence" color={primaryColor} />
                </div>
                <div>
                    <TransitionGauge probability={regime.transition_probability || 0} horizon="shift risk" />
                </div>
            </div>

            {/* Tab Bar */}
            <div style={{ display: 'flex', gap: '4px', marginBottom: '14px', overflowX: 'auto' }}>
                {tabs.map(t => (
                    <button key={t} onClick={() => setActiveTab(t)} style={{
                        padding: '7px 14px', borderRadius: '8px', fontSize: '12px', fontWeight: 600,
                        cursor: 'pointer', border: 'none', fontFamily: "'IBM Plex Sans', sans-serif",
                        background: activeTab === t ? colors.accent : colors.card,
                        color: activeTab === t ? '#fff' : colors.textMuted,
                        textTransform: 'uppercase', letterSpacing: '0.5px',
                    }}>
                        {t}
                    </button>
                ))}
            </div>

            {/* Overview Tab */}
            {activeTab === 'overview' && (
                <>
                    {/* Macro Regime Map */}
                    <div style={{ marginBottom: '16px' }}>
                        <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                            letterSpacing: '1px', marginBottom: '10px' }}>MACRO REGIMES</div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                            {['GROWTH', 'NEUTRAL', 'FRAGILE', 'CRISIS'].map(state => {
                                const active = allRegimes?.macro?.find(r => r.state === state);
                                const sc = stateColors[state];
                                const rm = regimeMeta[state];
                                const conf = active?.confidence || 0;
                                const isCurrent = regime.state === state;
                                return (
                                    <div key={state} style={{
                                        background: isCurrent ? `${sc}15` : '#0D1520',
                                        borderRadius: '10px', padding: '12px',
                                        border: `1px solid ${isCurrent ? sc + '66' : '#1A2840'}`,
                                    }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                            <span style={{ fontSize: '12px', fontWeight: 700, color: sc,
                                                fontFamily: "'JetBrains Mono', monospace" }}>
                                                {rm?.icon} {state}
                                            </span>
                                            {isCurrent && (
                                                <span style={{
                                                    fontSize: '9px', padding: '1px 6px', borderRadius: '4px',
                                                    background: sc, color: '#fff', fontWeight: 700,
                                                }}>ACTIVE</span>
                                            )}
                                        </div>
                                        <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '4px' }}>
                                            {rm?.posture}
                                        </div>
                                        <div style={{ marginTop: '6px', height: '3px', borderRadius: '2px',
                                            background: '#1A2840', overflow: 'hidden' }}>
                                            <div style={{ height: '100%', width: `${conf * 100}%`,
                                                background: sc, borderRadius: '2px', transition: 'width 0.6s ease' }} />
                                        </div>
                                        <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '2px',
                                            fontFamily: "'JetBrains Mono', monospace", textAlign: 'right' }}>
                                            {conf > 0 ? `${Math.round(conf * 100)}%` : '--'}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    </div>

                    {/* Contradiction Flags */}
                    {regime.contradiction_flags?.length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                                letterSpacing: '1px', marginBottom: '8px' }}>CONTRADICTIONS</div>
                            {regime.contradiction_flags.map((f, i) => (
                                <div key={i} style={{
                                    background: '#8B1F1F11', borderRadius: '8px', padding: '10px 14px',
                                    border: '1px solid #8B1F1F33', marginBottom: '6px',
                                    fontSize: '13px', color: '#C8D8E8',
                                }}>{f}</div>
                            ))}
                        </div>
                    )}

                    {/* Drivers */}
                    {regime.top_drivers?.length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                                letterSpacing: '1px', marginBottom: '8px' }}>TOP DRIVERS</div>
                            {regime.top_drivers.map((d, i) => (
                                <div key={i} style={{
                                    display: 'flex', alignItems: 'center', gap: '8px',
                                    padding: '8px 0', borderBottom: '1px solid #1A284044',
                                }}>
                                    <span style={{ flex: 1, fontSize: '13px', fontFamily: "'JetBrains Mono', monospace" }}>
                                        {d.feature}
                                    </span>
                                    <span style={{ fontSize: '12px', color: d.direction === 'up' ? '#22C55E' : '#EF4444' }}>
                                        {d.direction === 'up' ? '\u25B2' : '\u25BC'} {d.magnitude?.toFixed(2)}
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}
                </>
            )}

            {/* Strategy Tab */}
            {activeTab === 'strategy' && (
                <div>
                    <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                        letterSpacing: '1px', marginBottom: '10px' }}>STRATEGY REGIMES</div>
                    {allRegimes?.strategy?.length > 0 ? (
                        allRegimes.strategy.map((s, i) => {
                            const sc = stateColors[s.state] || '#5A7080';
                            const rm = regimeMeta[s.state];
                            const isCurrent = regime.state === s.state;
                            return (
                                <div key={i} style={{
                                    background: isCurrent ? `${sc}15` : '#0D1520',
                                    borderRadius: '10px', padding: '14px', marginBottom: '8px',
                                    border: `1px solid ${isCurrent ? sc + '66' : '#1A2840'}`,
                                }}>
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <div>
                                            <span style={{ fontSize: '14px', fontWeight: 700, color: sc,
                                                fontFamily: "'JetBrains Mono', monospace" }}>
                                                {rm?.icon || '\u25CF'} {s.state}
                                            </span>
                                            {isCurrent && (
                                                <span style={{
                                                    marginLeft: '8px', fontSize: '9px', padding: '1px 6px',
                                                    borderRadius: '4px', background: sc, color: '#fff', fontWeight: 700,
                                                }}>ACTIVE</span>
                                            )}
                                        </div>
                                        <span style={{ fontSize: '16px', fontWeight: 700, color: sc,
                                            fontFamily: "'JetBrains Mono', monospace" }}>
                                            {Math.round(s.confidence * 100)}%
                                        </span>
                                    </div>
                                    {rm && (
                                        <div style={{ marginTop: '6px' }}>
                                            <span style={{
                                                display: 'inline-block', padding: '2px 8px', borderRadius: '6px',
                                                background: `${sc}22`, color: sc, fontSize: '11px', fontWeight: 600,
                                            }}>{rm.posture}</span>
                                            <div style={{ fontSize: '12px', color: '#8AA0B8', marginTop: '4px' }}>
                                                {rm.desc}
                                            </div>
                                        </div>
                                    )}
                                    {s.recommendation && (
                                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '6px',
                                            fontFamily: "'JetBrains Mono', monospace", fontStyle: 'italic' }}>
                                            {s.recommendation}
                                        </div>
                                    )}
                                    <div style={{ marginTop: '8px', height: '3px', borderRadius: '2px',
                                        background: '#1A2840', overflow: 'hidden' }}>
                                        <div style={{ height: '100%', width: `${s.confidence * 100}%`,
                                            background: sc, borderRadius: '2px' }} />
                                    </div>
                                </div>
                            );
                        })
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                            No strategy regimes detected yet
                        </div>
                    )}

                    {/* Legend */}
                    <div style={{ marginTop: '16px' }}>
                        <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                            letterSpacing: '1px', marginBottom: '10px' }}>REGIME LEGEND</div>
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px' }}>
                            {Object.entries(regimeMeta).filter(([k]) => !macroStates.has(k)).map(([key, info]) => (
                                <div key={key} style={{
                                    background: '#0D1520', borderRadius: '8px', padding: '8px 10px',
                                    border: `1px solid #1A2840`,
                                }}>
                                    <div style={{ fontSize: '10px', fontWeight: 600, color: stateColors[key] || '#5A7080',
                                        fontFamily: "'JetBrains Mono', monospace" }}>
                                        {info.icon} {key}
                                    </div>
                                    <div style={{ fontSize: '9px', color: '#5A7080', marginTop: '2px' }}>
                                        {info.posture}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            )}

            {/* Features Tab */}
            {activeTab === 'features' && (
                <div>
                    <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                        letterSpacing: '1px', marginBottom: '10px' }}>FEATURE CONTRIBUTIONS</div>
                    {allRegimes?.feature_contributions?.length > 0 ? (
                        allRegimes.feature_contributions.map((f, i) => {
                            const maxImp = allRegimes.feature_contributions[0]?.importance || 1;
                            const pct = Math.abs(f.importance) / maxImp * 100;
                            const barColor = f.importance > 0 ? '#22C55E' : '#EF4444';
                            return (
                                <div key={i} style={{
                                    display: 'flex', alignItems: 'center', gap: '10px',
                                    padding: '8px 0', borderBottom: '1px solid #1A284044',
                                }}>
                                    <span style={{ flex: '0 0 140px', fontSize: '12px',
                                        fontFamily: "'JetBrains Mono', monospace", color: colors.text,
                                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                        {f.feature}
                                    </span>
                                    <div style={{ flex: 1, height: '6px', borderRadius: '3px',
                                        background: '#1A2840', overflow: 'hidden' }}>
                                        <div style={{ height: '100%', width: `${pct}%`,
                                            background: barColor, borderRadius: '3px',
                                            transition: 'width 0.4s ease' }} />
                                    </div>
                                    <span style={{ fontSize: '11px', fontFamily: "'JetBrains Mono', monospace",
                                        color: barColor, minWidth: '50px', textAlign: 'right' }}>
                                        {f.importance > 0 ? '+' : ''}{f.importance.toFixed(3)}
                                    </span>
                                </div>
                            );
                        })
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '30px' }}>
                            Run clustering from the Discovery page to see feature contributions.
                            <br /><br />
                            Feature importance shows which data series are driving regime classification.
                        </div>
                    )}
                </div>
            )}

            {/* History Tab */}
            {activeTab === 'history' && (
                <div>
                    {/* Regime Timeline */}
                    {history.length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                                letterSpacing: '1px', marginBottom: '10px' }}>90-DAY TIMELINE</div>
                            <div style={{
                                display: 'flex', height: '24px', borderRadius: '6px', overflow: 'hidden',
                                border: '1px solid #1A2840',
                            }}>
                                {history.map((h, i) => (
                                    <div key={i} style={{
                                        flex: 1, background: stateColors[h.state] || '#5A7080',
                                        opacity: 0.6 + h.confidence * 0.4,
                                    }} title={`${h.date}: ${h.state} (${Math.round(h.confidence * 100)}%)`} />
                                ))}
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '4px',
                                fontSize: '10px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace" }}>
                                <span>{history[0]?.date}</span>
                                <span>{history[history.length - 1]?.date}</span>
                            </div>
                        </div>
                    )}

                    {/* Transitions */}
                    <div style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                        letterSpacing: '1px', marginBottom: '10px' }}>
                        TRANSITIONS ({transitions.length})
                    </div>
                    {transitions.length > 0 ? (
                        transitions.slice(-15).reverse().map((t, i) => (
                            <div key={i} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '10px 12px', marginBottom: '6px', borderRadius: '8px',
                                background: '#0D1520', border: '1px solid #1A2840',
                            }}>
                                <span style={{ fontSize: '11px', color: '#5A7080',
                                    fontFamily: "'JetBrains Mono', monospace", minWidth: '80px' }}>
                                    {t.date?.split('T')[0] || t.date?.substring(0, 10)}
                                </span>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '6px' }}>
                                    <span style={{
                                        fontSize: '11px', fontWeight: 600,
                                        color: stateColors[t.from_state] || '#5A7080',
                                        fontFamily: "'JetBrains Mono', monospace",
                                    }}>{t.from_state}</span>
                                    <span style={{ color: '#5A7080' }}>{'\u2192'}</span>
                                    <span style={{
                                        fontSize: '11px', fontWeight: 600,
                                        color: stateColors[t.to_state] || '#5A7080',
                                        fontFamily: "'JetBrains Mono', monospace",
                                    }}>{t.to_state}</span>
                                </div>
                                <span style={{ fontSize: '11px', color: '#5A7080',
                                    fontFamily: "'JetBrains Mono', monospace" }}>
                                    {Math.round(t.confidence * 100)}%
                                </span>
                            </div>
                        ))
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                            No transitions detected yet
                        </div>
                    )}
                </div>
            )}

            {/* Uncalibrated info */}
            {regime.state === 'UNCALIBRATED' && (
                <div style={{
                    background: '#1A284033', borderRadius: '8px', padding: '14px',
                    border: '1px solid #1A2840', marginTop: '16px',
                    fontSize: '13px', color: '#5A7080', lineHeight: '1.6',
                }}>
                    Regime detection runs daily at 6:00 PM ET after data ingestion completes.
                    The system needs data in the decision journal to display regime state.
                    Check the System Logs page for ingestion status.
                </div>
            )}
        </div>
    );
}
