import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ConfidenceMeter from '../components/ConfidenceMeter.jsx';
import TransitionGauge from '../components/TransitionGauge.jsx';
import { shared, colors } from '../styles/shared.js';

const stateColors = {
    'GROWTH': '#22C55E', 'NEUTRAL': '#3B82F6', 'FRAGILE': '#F59E0B', 'CRISIS': '#EF4444',
    'EQUITY_VALUE': '#10B981', 'BUYOUT_ARBITRAGE': '#8B5CF6',
    'DISTRESSED_TURNAROUND': '#F97316', 'CRYPTO_CORE': '#06B6D4', 'CRYPTO_AI': '#EC4899',
    'UNCALIBRATED': '#5A7080',
};

const regimeMeta = {
    'GROWTH':                  { posture: 'Aggressive',           icon: '\u25B2', desc: 'Broad expansion — risk-on equities, credit, commodities' },
    'NEUTRAL':                 { posture: 'Balanced',             icon: '\u25C6', desc: 'Mixed signals — diversify, no strong directional bias' },
    'FRAGILE':                 { posture: 'Defensive',            icon: '\u25BC', desc: 'Deteriorating — reduce risk, favor quality and duration' },
    'CRISIS':                  { posture: 'Capital Preservation', icon: '\u26A0', desc: 'Active stress — cash, treasuries, tail hedges' },
    'EQUITY_VALUE':            { posture: 'Value Tilt',           icon: '\u2193', desc: 'Deep value — cheap equities vs fundamentals' },
    'BUYOUT_ARBITRAGE':        { posture: 'Event-Driven',         icon: '\u21C4', desc: 'M&A spreads elevated — catalyst plays' },
    'DISTRESSED_TURNAROUND':   { posture: 'Contrarian',           icon: '\u21BB', desc: 'Distressed recovery — high risk/reward' },
    'CRYPTO_CORE':             { posture: 'Crypto Allocation',    icon: '\u20BF', desc: 'On-chain + macro favor BTC/ETH' },
    'CRYPTO_AI':               { posture: 'AI + Crypto',          icon: '\u2605', desc: 'AI/compute tokens outperforming' },
};

const actionGuide = {
    'GROWTH':   { action: 'Stay long equities, add on dips', allocation: '70% equities, 15% commodities, 10% crypto, 5% cash', risk: 'Low — momentum is your friend' },
    'NEUTRAL':  { action: 'Diversify broadly, reduce conviction bets', allocation: '40% equities, 25% bonds, 15% alternatives, 20% cash', risk: 'Medium — no clear edge, stay nimble' },
    'FRAGILE':  { action: 'Reduce risk, move to quality', allocation: '25% equities (quality), 35% bonds, 20% gold, 20% cash', risk: 'High — protect capital, hedge tail risk' },
    'CRISIS':   { action: 'Preserve capital, buy tail hedges', allocation: '10% equities, 40% treasuries, 25% gold, 25% cash', risk: 'Extreme — survival mode, wait for opportunity' },
};

const s = {
    page: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    header: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' },
    headerTitle: { fontFamily: "'JetBrains Mono', monospace", fontSize: '14px', color: '#5A7080', letterSpacing: '2px' },
    sectionLabel: { fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '10px', marginTop: '16px' },
    card: { background: '#0D1520', borderRadius: '10px', padding: '14px', border: '1px solid #1A2840', marginBottom: '8px' },
};

function RegimeBar({ state, confidence, recommendation, isCurrent }) {
    const sc = stateColors[state] || '#5A7080';
    const meta = regimeMeta[state];
    return (
        <div style={{
            ...s.card, borderColor: isCurrent ? `${sc}88` : '#1A2840',
            background: isCurrent ? `${sc}0D` : '#0D1520',
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ fontSize: '14px', fontWeight: 700, color: sc,
                        fontFamily: "'JetBrains Mono', monospace" }}>
                        {meta?.icon || '\u25CF'} {state}
                    </span>
                    {isCurrent && (
                        <span style={{ fontSize: '9px', padding: '1px 6px', borderRadius: '4px',
                            background: sc, color: '#fff', fontWeight: 700 }}>NOW</span>
                    )}
                </div>
                <span style={{ fontSize: '18px', fontWeight: 700, color: sc,
                    fontFamily: "'JetBrains Mono', monospace" }}>
                    {Math.round(confidence * 100)}%
                </span>
            </div>
            <div style={{ marginTop: '6px', height: '4px', borderRadius: '2px',
                background: '#1A2840', overflow: 'hidden' }}>
                <div style={{ height: '100%', width: `${confidence * 100}%`,
                    background: sc, borderRadius: '2px', transition: 'width 0.6s ease' }} />
            </div>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '6px' }}>
                <span style={{ fontSize: '11px', color: '#5A7080' }}>{meta?.posture}</span>
                {recommendation && (
                    <span style={{ fontSize: '10px', color: colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace" }}>{recommendation}</span>
                )}
            </div>
        </div>
    );
}

function MoverCard({ feature, family, change_pct, latest }) {
    const isUp = change_pct > 0;
    const color = isUp ? '#22C55E' : '#EF4444';
    const abs_pct = Math.abs(change_pct);
    return (
        <div style={{
            background: '#0D1520', borderRadius: '8px', padding: '10px',
            border: `1px solid ${abs_pct > 10 ? color + '44' : '#1A2840'}`,
        }}>
            <div style={{ fontSize: '11px', fontWeight: 600, color: colors.text,
                fontFamily: "'JetBrains Mono', monospace", overflow: 'hidden',
                textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                {feature}
            </div>
            <div style={{ fontSize: '9px', color: '#5A7080', marginTop: '2px' }}>{family}</div>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'baseline', marginTop: '4px' }}>
                <span style={{ fontSize: '13px', fontWeight: 700, color,
                    fontFamily: "'JetBrains Mono', monospace" }}>
                    {isUp ? '+' : ''}{change_pct.toFixed(1)}%
                </span>
                <span style={{ fontSize: '10px', color: '#5A7080',
                    fontFamily: "'JetBrains Mono', monospace" }}>
                    {latest?.toFixed(2)}
                </span>
            </div>
        </div>
    );
}

export default function Regime() {
    const { currentRegime, setCurrentRegime } = useStore();
    const [allRegimes, setAllRegimes] = useState(null);
    const [synthesis, setSynthesis] = useState(null);
    const [synthLoading, setSynthLoading] = useState(false);
    const [history, setHistory] = useState([]);
    const [transitions, setTransitions] = useState([]);
    const [activeTab, setActiveTab] = useState('action');

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

    const loadSynthesis = async () => {
        setSynthLoading(true);
        const result = await api.getRegimeSynthesis().catch(() => null);
        setSynthesis(result);
        setSynthLoading(false);
    };

    const regime = currentRegime || { state: 'UNCALIBRATED' };
    const primaryColor = stateColors[regime.state] || '#5A7080';
    const meta = regimeMeta[regime.state];
    const tabs = ['action', 'macro', 'strategy', 'analysis', 'movers', 'history'];

    const macroStates = ['GROWTH', 'NEUTRAL', 'FRAGILE', 'CRISIS'];

    return (
        <div style={s.page}>
            {/* Header */}
            <div style={s.header}>
                <span style={s.headerTitle}>REGIME</span>
                <button onClick={loadData} style={{
                    ...shared.buttonSmall, background: 'transparent', border: `1px solid ${colors.border}`,
                    color: colors.textMuted, fontSize: '11px',
                }}>REFRESH</button>
            </div>

            {/* Primary State Banner */}
            <div style={{
                background: '#0D1520', borderRadius: '14px', padding: '16px 20px',
                border: `1px solid ${primaryColor}44`, marginBottom: '14px',
                position: 'relative', overflow: 'hidden',
            }}>
                <div style={{ position: 'absolute', top: 0, left: 0, right: 0, height: '3px',
                    background: `linear-gradient(90deg, transparent, ${primaryColor}, transparent)` }} />
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                        <div style={{ fontSize: '10px', color: '#5A7080', letterSpacing: '1px',
                            fontFamily: "'JetBrains Mono', monospace", marginBottom: '4px' }}>CURRENT</div>
                        <div style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '22px',
                            fontWeight: 700, color: primaryColor }}>
                            {meta?.icon || ''} {regime.state}
                        </div>
                    </div>
                    <div style={{ textAlign: 'right' }}>
                        <div style={{ fontSize: '24px', fontWeight: 700, color: primaryColor,
                            fontFamily: "'JetBrains Mono', monospace" }}>
                            {regime.confidence ? `${Math.round(regime.confidence * 100)}%` : '--'}
                        </div>
                        {meta && (
                            <span style={{ fontSize: '10px', padding: '2px 8px', borderRadius: '6px',
                                background: `${primaryColor}22`, color: primaryColor, fontWeight: 600 }}>
                                {meta.posture}
                            </span>
                        )}
                    </div>
                </div>
                {meta && (
                    <div style={{ fontSize: '12px', color: '#8AA0B8', marginTop: '8px' }}>{meta.desc}</div>
                )}
                {regime.as_of && regime.state !== 'UNCALIBRATED' && (
                    <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '6px',
                        fontFamily: "'JetBrains Mono', monospace" }}>
                        as of {new Date(regime.as_of).toLocaleString()}
                    </div>
                )}
            </div>

            {/* Action Guide — plain English "what to do" */}
            {regime.state !== 'UNCALIBRATED' && actionGuide[regime.state] && (
                <div style={{
                    background: `${primaryColor}08`, borderRadius: '12px', padding: '14px 16px',
                    border: `1px solid ${primaryColor}22`, marginBottom: '12px',
                }}>
                    <div style={{ fontSize: '10px', color: primaryColor, letterSpacing: '1px',
                        fontFamily: "'JetBrains Mono', monospace", marginBottom: '8px' }}>
                        WHAT TO DO
                    </div>
                    <div style={{ fontSize: '14px', color: '#C8D8E8', fontWeight: 600, marginBottom: '10px' }}>
                        {actionGuide[regime.state].action}
                    </div>
                    <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                        <div style={{ background: '#0D152088', borderRadius: '8px', padding: '10px' }}>
                            <div style={{ fontSize: '9px', color: '#5A7080', letterSpacing: '0.5px', marginBottom: '4px' }}>
                                TARGET ALLOCATION
                            </div>
                            <div style={{ fontSize: '11px', color: '#8AA0B8', lineHeight: '1.5' }}>
                                {actionGuide[regime.state].allocation}
                            </div>
                        </div>
                        <div style={{ background: '#0D152088', borderRadius: '8px', padding: '10px' }}>
                            <div style={{ fontSize: '9px', color: '#5A7080', letterSpacing: '0.5px', marginBottom: '4px' }}>
                                RISK LEVEL
                            </div>
                            <div style={{ fontSize: '11px', color: '#8AA0B8', lineHeight: '1.5' }}>
                                {actionGuide[regime.state].risk}
                            </div>
                        </div>
                    </div>
                </div>
            )}

            {/* Transition risk */}
            <div style={{ marginBottom: '14px' }}>
                <TransitionGauge probability={regime.transition_probability || 0} horizon="shift risk" />
            </div>

            {/* Tabs */}
            <div style={{ display: 'flex', gap: '3px', marginBottom: '14px', overflowX: 'auto' }}>
                {tabs.map(t => (
                    <button key={t} onClick={() => {
                        setActiveTab(t);
                        if (t === 'analysis' && !synthesis) loadSynthesis();
                    }} style={{
                        padding: '7px 12px', borderRadius: '8px', fontSize: '11px', fontWeight: 600,
                        cursor: 'pointer', border: 'none', textTransform: 'uppercase', letterSpacing: '0.5px',
                        fontFamily: "'IBM Plex Sans', sans-serif",
                        background: activeTab === t ? colors.accent : colors.card,
                        color: activeTab === t ? '#fff' : colors.textMuted,
                    }}>
                        {t}
                    </button>
                ))}
            </div>

            {/* ═══ ACTION TAB ═══ */}
            {activeTab === 'action' && (
                <div>
                    <div style={s.sectionLabel}>REGIME EXPLAINED</div>
                    <div style={{ ...s.card, lineHeight: '1.7', fontSize: '13px', color: '#8AA0B8' }}>
                        <p style={{ marginBottom: '12px' }}>
                            GRID analyzes <strong style={{ color: '#C8D8E8' }}>37+ data sources</strong> across
                            economics, markets, sentiment, and alternative data to classify the current market
                            environment into one of 4 macro regimes.
                        </p>
                        <p style={{ marginBottom: '12px' }}>
                            The <strong style={{ color: primaryColor }}>{regime.state}</strong> regime means{' '}
                            {meta?.desc?.toLowerCase() || 'conditions are still being evaluated'}.
                            Confidence of <strong style={{ color: primaryColor }}>
                            {regime.confidence ? `${Math.round(regime.confidence * 100)}%` : '--'}</strong> indicates
                            how strongly the data supports this classification.
                        </p>
                        {regime.transition_probability > 0.3 && (
                            <p style={{ color: '#F59E0B' }}>
                                ⚠ Transition probability is elevated ({Math.round(regime.transition_probability * 100)}%)
                                — the regime may be shifting. Monitor closely.
                            </p>
                        )}
                    </div>

                    {regime.top_drivers?.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>KEY DRIVERS</div>
                            <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '8px' }}>
                                The features most responsible for the current regime classification:
                            </div>
                            {regime.top_drivers.slice(0, 5).map((d, i) => (
                                <div key={i} style={{
                                    display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                                    padding: '10px 14px', marginBottom: '4px', borderRadius: '8px',
                                    background: '#0D1520', border: '1px solid #1A2840',
                                }}>
                                    <span style={{ fontSize: '13px', fontFamily: "'JetBrains Mono', monospace", color: '#C8D8E8' }}>
                                        {d.feature}
                                    </span>
                                    <span style={{
                                        fontSize: '13px', fontWeight: 700,
                                        fontFamily: "'JetBrains Mono', monospace",
                                        color: d.direction === 'up' ? '#22C55E' : '#EF4444',
                                    }}>
                                        {d.direction === 'up' ? '▲' : '▼'} {d.magnitude?.toFixed(2)}σ
                                    </span>
                                </div>
                            ))}
                        </>
                    )}
                </div>
            )}

            {/* ═══ MACRO TAB ═══ */}
            {activeTab === 'macro' && (
                <div>
                    <div style={s.sectionLabel}>MACRO REGIME READINGS</div>
                    <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '12px' }}>
                        How the economy is positioned across 4 macro states. Multiple can fire simultaneously
                        — contradictions reveal regime transitions in progress.
                    </div>
                    {macroStates.map(state => {
                        const active = allRegimes?.macro?.find(r => r.state === state);
                        return (
                            <RegimeBar
                                key={state}
                                state={state}
                                confidence={active?.confidence || 0}
                                recommendation={active?.recommendation}
                                isCurrent={regime.state === state}
                            />
                        );
                    })}

                    {/* Contradictions */}
                    {regime.contradiction_flags?.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>CONTRADICTIONS</div>
                            {regime.contradiction_flags.map((f, i) => (
                                <div key={i} style={{
                                    background: '#8B1F1F11', borderRadius: '8px', padding: '10px 14px',
                                    border: '1px solid #8B1F1F33', marginBottom: '6px',
                                    fontSize: '13px', color: '#C8D8E8',
                                }}>{f}</div>
                            ))}
                        </>
                    )}

                    {regime.top_drivers?.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>TOP DRIVERS</div>
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
                        </>
                    )}
                </div>
            )}

            {/* ═══ STRATEGY TAB ═══ */}
            {activeTab === 'strategy' && (
                <div>
                    <div style={s.sectionLabel}>STRATEGY REGIMES</div>
                    <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '12px' }}>
                        Granular strategy archetypes detected by clustering. These show WHERE opportunities
                        concentrate, not just the macro direction.
                    </div>
                    {allRegimes?.strategy?.length > 0 ? (
                        allRegimes.strategy.map((r, i) => (
                            <RegimeBar
                                key={i}
                                state={r.state}
                                confidence={r.confidence}
                                recommendation={r.recommendation}
                                isCurrent={regime.state === r.state}
                            />
                        ))
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                            No strategy regimes detected yet
                        </div>
                    )}
                </div>
            )}

            {/* ═══ ANALYSIS TAB ═══ */}
            {activeTab === 'analysis' && (
                <div>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                        <div style={s.sectionLabel}>LLM SYNTHESIS</div>
                        <button onClick={loadSynthesis} style={{
                            ...shared.buttonSmall, fontSize: '10px',
                            background: synthLoading ? colors.border : colors.accent,
                        }} disabled={synthLoading}>
                            {synthLoading ? 'ANALYZING...' : 'REGENERATE'}
                        </button>
                    </div>
                    <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '12px' }}>
                        AI interprets combined regime signals, identifies drivers, momentum, and mispricings.
                    </div>

                    {synthLoading && (
                        <div style={{ ...s.card, textAlign: 'center', padding: '40px' }}>
                            <div style={{ fontSize: '14px', color: colors.accent }}>Synthesizing regime signals...</div>
                            <div style={{ fontSize: '12px', color: '#5A7080', marginTop: '8px' }}>
                                Querying LLM with {allRegimes?.macro?.length || 0} macro + {allRegimes?.strategy?.length || 0} strategy readings
                            </div>
                        </div>
                    )}

                    {!synthLoading && synthesis?.synthesis && (
                        <div style={{
                            background: '#0D1520', borderRadius: '12px', padding: '16px',
                            border: '1px solid #1A284088', whiteSpace: 'pre-wrap',
                            fontSize: '13px', color: '#C8D8E8', lineHeight: '1.7',
                            fontFamily: "'IBM Plex Sans', sans-serif",
                        }}>
                            {synthesis.synthesis}
                        </div>
                    )}

                    {!synthLoading && synthesis && !synthesis.synthesis && (
                        <div style={{ ...s.card, textAlign: 'center', padding: '30px' }}>
                            <div style={{ fontSize: '13px', color: colors.textMuted }}>
                                {synthesis.error || 'LLM not available — start Ollama or llama.cpp to enable synthesis'}
                            </div>
                            {synthesis.regime_summary && (
                                <div style={{ marginTop: '12px', fontSize: '12px', color: '#5A7080',
                                    fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'pre-wrap', textAlign: 'left' }}>
                                    Raw readings:\n{synthesis.regime_summary}
                                </div>
                            )}
                        </div>
                    )}

                    {!synthLoading && !synthesis && (
                        <div style={{ ...s.card, textAlign: 'center', padding: '30px' }}>
                            <button onClick={loadSynthesis} style={shared.button}>
                                Generate Synthesis
                            </button>
                            <div style={{ fontSize: '12px', color: '#5A7080', marginTop: '10px' }}>
                                Uses LLM to interpret all regime signals as unified market intelligence
                            </div>
                        </div>
                    )}
                </div>
            )}

            {/* ═══ MOVERS TAB ═══ */}
            {activeTab === 'movers' && (
                <div>
                    <div style={s.sectionLabel}>TOP MOVERS</div>
                    <div style={{ fontSize: '12px', color: '#5A7080', marginBottom: '12px' }}>
                        Features with the biggest recent changes — these are the forces pushing regime readings.
                    </div>
                    {allRegimes?.top_movers?.length > 0 ? (
                        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px' }}>
                            {allRegimes.top_movers.map((m, i) => (
                                <MoverCard key={i} {...m} />
                            ))}
                        </div>
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                            No significant movers detected
                        </div>
                    )}

                    {/* Feature contributions */}
                    {allRegimes?.feature_contributions?.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>CLUSTERING FEATURE WEIGHTS</div>
                            {allRegimes.feature_contributions.map((f, i) => {
                                const maxImp = allRegimes.feature_contributions[0]?.importance || 1;
                                const pct = Math.abs(f.importance) / maxImp * 100;
                                const barColor = f.importance > 0 ? '#22C55E' : '#EF4444';
                                return (
                                    <div key={i} style={{
                                        display: 'flex', alignItems: 'center', gap: '10px',
                                        padding: '6px 0', borderBottom: '1px solid #1A284044',
                                    }}>
                                        <span style={{ flex: '0 0 120px', fontSize: '11px',
                                            fontFamily: "'JetBrains Mono', monospace", color: colors.text,
                                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                                            {f.feature}
                                        </span>
                                        <div style={{ flex: 1, height: '5px', borderRadius: '3px',
                                            background: '#1A2840', overflow: 'hidden' }}>
                                            <div style={{ height: '100%', width: `${pct}%`,
                                                background: barColor, borderRadius: '3px' }} />
                                        </div>
                                        <span style={{ fontSize: '10px', fontFamily: "'JetBrains Mono', monospace",
                                            color: barColor, minWidth: '45px', textAlign: 'right' }}>
                                            {f.importance > 0 ? '+' : ''}{f.importance.toFixed(3)}
                                        </span>
                                    </div>
                                );
                            })}
                        </>
                    )}
                </div>
            )}

            {/* ═══ HISTORY TAB ═══ */}
            {activeTab === 'history' && (
                <div>
                    {history.length > 0 && (
                        <>
                            <div style={s.sectionLabel}>90-DAY TIMELINE</div>
                            <div style={{
                                display: 'flex', height: '28px', borderRadius: '6px', overflow: 'hidden',
                                border: '1px solid #1A2840', marginBottom: '4px',
                            }}>
                                {history.map((h, i) => (
                                    <div key={i} style={{
                                        flex: 1, background: stateColors[h.state] || '#5A7080',
                                        opacity: 0.5 + h.confidence * 0.5,
                                    }} title={`${h.date}: ${h.state} (${Math.round(h.confidence * 100)}%)`} />
                                ))}
                            </div>
                            <div style={{ display: 'flex', justifyContent: 'space-between',
                                fontSize: '10px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
                                marginBottom: '16px' }}>
                                <span>{history[0]?.date}</span>
                                <span>{history[history.length - 1]?.date}</span>
                            </div>
                        </>
                    )}

                    <div style={s.sectionLabel}>TRANSITIONS ({transitions.length})</div>
                    {transitions.length > 0 ? (
                        transitions.slice(-15).reverse().map((t, i) => (
                            <div key={i} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '10px 12px', marginBottom: '6px', borderRadius: '8px',
                                background: '#0D1520', border: '1px solid #1A2840',
                            }}>
                                <span style={{ fontSize: '11px', color: '#5A7080',
                                    fontFamily: "'JetBrains Mono', monospace", minWidth: '75px' }}>
                                    {(t.date || '').split('T')[0]?.substring(5) || t.date}
                                </span>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                    <span style={{ fontSize: '11px', fontWeight: 600,
                                        color: stateColors[t.from_state] || '#5A7080',
                                        fontFamily: "'JetBrains Mono', monospace" }}>{t.from_state}</span>
                                    <span style={{ color: '#5A7080', fontSize: '10px' }}>{'\u2192'}</span>
                                    <span style={{ fontSize: '11px', fontWeight: 600,
                                        color: stateColors[t.to_state] || '#5A7080',
                                        fontFamily: "'JetBrains Mono', monospace" }}>{t.to_state}</span>
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

            {/* Uncalibrated */}
            {regime.state === 'UNCALIBRATED' && (
                <div style={{
                    background: '#1A284033', borderRadius: '8px', padding: '14px',
                    border: '1px solid #1A2840', marginTop: '16px',
                    fontSize: '13px', color: '#5A7080', lineHeight: '1.6',
                }}>
                    Regime detection runs daily at 6:00 PM ET after data ingestion.
                    Check System Logs for ingestion status.
                </div>
            )}
        </div>
    );
}
