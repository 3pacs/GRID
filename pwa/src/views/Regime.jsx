import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ConfidenceMeter from '../components/ConfidenceMeter.jsx';
import TransitionGauge from '../components/TransitionGauge.jsx';

const stateColors = {
    // Macro regimes
    'GROWTH': '#1A7A4A', 'Expansion': '#1A7A4A',
    'NEUTRAL': '#1A6EBF', 'Recovery': '#1A6EBF',
    'FRAGILE': '#8A6000', 'Late Cycle': '#8A6000',
    'CRISIS': '#8B1F1F', 'Contraction': '#8B1F1F',
    // Strategy regimes
    'EQUITY_VALUE': '#22C55E',
    'BUYOUT_ARBITRAGE': '#8B5CF6',
    'DISTRESSED_TURNAROUND': '#F97316',
    'CRYPTO_CORE': '#3B82F6',
    'CRYPTO_AI': '#06B6D4',
    // Fallback
    'Mixed': '#5A7080', 'UNCALIBRATED': '#5A7080',
};

const regimeLegend = {
    // Macro regimes
    'GROWTH': { posture: 'Aggressive', desc: 'Broad economic expansion — risk-on across equities, credit, commodities' },
    'NEUTRAL': { posture: 'Balanced', desc: 'Mixed signals — no strong directional bias, diversify across asset classes' },
    'FRAGILE': { posture: 'Defensive', desc: 'Deteriorating conditions — reduce risk, favor quality and duration' },
    'CRISIS': { posture: 'Capital Preservation', desc: 'Active stress — maximize cash, treasuries, and tail hedges' },
    // Strategy regimes
    'EQUITY_VALUE': { posture: 'Value Tilt', desc: 'Deep value opportunities detected — cheap equities relative to fundamentals' },
    'BUYOUT_ARBITRAGE': { posture: 'Event-Driven', desc: 'M&A/arbitrage spreads elevated — catalyst-driven opportunities' },
    'DISTRESSED_TURNAROUND': { posture: 'Contrarian', desc: 'Distressed assets pricing recovery — high risk/reward turnaround plays' },
    'CRYPTO_CORE': { posture: 'Crypto Allocation', desc: 'On-chain and macro data favor core crypto exposure (BTC/ETH)' },
    'CRYPTO_AI': { posture: 'AI + Crypto', desc: 'AI/compute token sector showing strength relative to broader crypto' },
};

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '16px',
    },
    stateCard: {
        background: '#0D1520', borderRadius: '12px', padding: '24px',
        border: '1px solid #1A2840', marginBottom: '16px', textAlign: 'center',
    },
    stateLabel: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '32px', fontWeight: 700,
    },
    section: { marginBottom: '16px' },
    sectionTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    driverRow: {
        display: 'flex', alignItems: 'center', gap: '8px',
        padding: '8px 0', borderBottom: '1px solid #1A284044',
    },
    flagCard: {
        background: '#8B1F1F11', borderRadius: '8px', padding: '10px 14px',
        border: '1px solid #8B1F1F33', marginBottom: '6px', fontSize: '13px', color: '#C8D8E8',
    },
    transitionRow: {
        display: 'flex', justifyContent: 'space-between', padding: '8px 0',
        borderBottom: '1px solid #1A284044', fontSize: '13px',
    },
};

export default function Regime() {
    const { currentRegime, setCurrentRegime } = useStore();
    const [history, setHistory] = useState([]);
    const [transitions, setTransitions] = useState([]);

    useEffect(() => {
        api.getCurrent().then(setCurrentRegime).catch(() => {});
        api.getHistory(90).then(d => setHistory(d.history || [])).catch(() => {});
        api.getTransitions().then(d => setTransitions(d.transitions || [])).catch(() => {});
    }, []);

    const regime = currentRegime || { state: 'UNCALIBRATED' };
    const color = stateColors[regime.state] || '#5A7080';

    return (
        <div style={styles.container}>
            <div style={styles.title}>REGIME</div>

            <div style={{ ...styles.stateCard, borderColor: `${color}44` }}>
                <div style={{ ...styles.stateLabel, color }}>{regime.state}</div>
            </div>

            {regime.as_of && regime.state !== 'UNCALIBRATED' && (
                <div style={{ textAlign: 'center', fontSize: '11px', color: '#5A7080', marginTop: '-8px', marginBottom: '16px',
                    fontFamily: "'JetBrains Mono', monospace" }}>
                    as of {new Date(regime.as_of).toLocaleString()}
                </div>
            )}

            <div style={styles.section}>
                <ConfidenceMeter value={regime.confidence || 0} label="Confidence" color={color} />
            </div>

            <div style={styles.section}>
                <TransitionGauge probability={regime.transition_probability || 0} />
            </div>

            {/* Regime Legend */}
            {regime.state !== 'UNCALIBRATED' && regimeLegend[regime.state] && (
                <div style={{
                    background: `${color}11`, borderRadius: '10px', padding: '14px',
                    border: `1px solid ${color}33`, marginBottom: '16px',
                }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                        <span style={{ fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px' }}>
                            POSTURE
                        </span>
                        <span style={{ fontSize: '13px', fontWeight: 600, color, fontFamily: "'JetBrains Mono', monospace" }}>
                            {regimeLegend[regime.state].posture}
                        </span>
                    </div>
                    <div style={{ fontSize: '13px', color: '#8AA0B8', lineHeight: '1.5' }}>
                        {regimeLegend[regime.state].desc}
                    </div>
                </div>
            )}

            {/* All Regimes Legend */}
            <div style={{ marginBottom: '16px' }}>
                <div style={styles.sectionTitle}>REGIME LEGEND</div>
                <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '6px' }}>
                    {Object.entries(regimeLegend).map(([key, info]) => (
                        <div key={key} style={{
                            background: regime.state === key ? `${stateColors[key]}22` : '#0D1520',
                            borderRadius: '8px', padding: '8px 10px',
                            border: `1px solid ${regime.state === key ? stateColors[key] + '66' : '#1A2840'}`,
                        }}>
                            <div style={{
                                fontSize: '11px', fontWeight: 600, color: stateColors[key] || '#5A7080',
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>
                                {key}
                            </div>
                            <div style={{ fontSize: '10px', color: '#5A7080', marginTop: '2px' }}>
                                {info.posture}
                            </div>
                        </div>
                    ))}
                </div>
            </div>

            {regime.top_drivers?.length > 0 && (
                <div style={styles.section}>
                    <div style={styles.sectionTitle}>DRIVERS</div>
                    {regime.top_drivers.map((d, i) => (
                        <div key={i} style={styles.driverRow}>
                            <span style={{ flex: 1, fontSize: '13px', fontFamily: "'JetBrains Mono', monospace" }}>
                                {d.feature}
                            </span>
                            <span style={{ fontSize: '12px', color: d.direction === 'up' ? '#1A7A4A' : '#8B1F1F' }}>
                                {d.direction} {d.magnitude?.toFixed(2)}
                            </span>
                        </div>
                    ))}
                </div>
            )}

            {regime.contradiction_flags?.length > 0 && (
                <div style={styles.section}>
                    <div style={styles.sectionTitle}>CONTRADICTIONS</div>
                    {regime.contradiction_flags.map((f, i) => (
                        <div key={i} style={styles.flagCard}>{f}</div>
                    ))}
                </div>
            )}

            {transitions.length > 0 && (
                <div style={styles.section}>
                    <div style={styles.sectionTitle}>RECENT TRANSITIONS</div>
                    {transitions.slice(-10).reverse().map((t, i) => (
                        <div key={i} style={styles.transitionRow}>
                            <span style={{ color: '#5A7080' }}>{t.date}</span>
                            <span>
                                <span style={{ color: stateColors[t.from_state] || '#5A7080' }}>{t.from_state}</span>
                                {' → '}
                                <span style={{ color: stateColors[t.to_state] || '#5A7080' }}>{t.to_state}</span>
                            </span>
                            <span style={{ color: '#5A7080', fontFamily: "'JetBrains Mono', monospace" }}>
                                {Math.round(t.confidence * 100)}%
                            </span>
                        </div>
                    ))}
                </div>
            )}

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
