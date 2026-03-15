import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ConfidenceMeter from '../components/ConfidenceMeter.jsx';
import TransitionGauge from '../components/TransitionGauge.jsx';

const stateColors = {
    'Expansion': '#1A7A4A', 'Late Cycle': '#8A6000',
    'Contraction': '#8B1F1F', 'Recovery': '#1A6EBF',
    'Mixed': '#5A7080', 'UNCALIBRATED': '#5A7080',
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

            <div style={styles.section}>
                <ConfidenceMeter value={regime.confidence || 0} label="Confidence" color={color} />
            </div>

            <div style={styles.section}>
                <TransitionGauge probability={regime.transition_probability || 0} />
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
        </div>
    );
}
