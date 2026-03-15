import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ConfidenceMeter from '../components/ConfidenceMeter.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    backBtn: {
        background: 'none', border: 'none', color: '#1A6EBF', fontSize: '14px',
        cursor: 'pointer', fontFamily: "'IBM Plex Sans', sans-serif",
        marginBottom: '16px', padding: '8px 0', minHeight: '44px',
    },
    section: {
        background: '#0D1520', borderRadius: '10px', padding: '16px',
        border: '1px solid #1A2840', marginBottom: '12px',
    },
    sectionTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    row: {
        display: 'flex', justifyContent: 'space-between', padding: '4px 0',
        fontSize: '13px',
    },
    label: { color: '#5A7080' },
    value: { color: '#C8D8E8', fontFamily: "'JetBrains Mono', monospace" },
    compare: {
        display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '12px',
    },
    compareCol: {
        background: '#080C10', borderRadius: '8px', padding: '12px',
    },
    colTitle: {
        fontSize: '10px', color: '#5A7080', marginBottom: '6px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    input: {
        width: '100%', padding: '10px 12px', borderRadius: '6px',
        border: '1px solid #1A2840', background: '#080C10', color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '14px',
        outline: 'none', marginBottom: '10px',
    },
    textarea: {
        width: '100%', padding: '10px 12px', borderRadius: '6px',
        border: '1px solid #1A2840', background: '#080C10', color: '#C8D8E8',
        fontFamily: "'IBM Plex Sans', sans-serif", fontSize: '14px',
        outline: 'none', marginBottom: '10px', resize: 'vertical', minHeight: '80px',
    },
    verdictBtns: {
        display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '8px', marginBottom: '12px',
    },
    verdictBtn: {
        padding: '10px', borderRadius: '8px', border: '1px solid #1A2840',
        background: 'transparent', fontSize: '12px',
        fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
        cursor: 'pointer', minHeight: '44px',
    },
    submitBtn: {
        width: '100%', padding: '14px', borderRadius: '8px', border: 'none',
        background: '#1A6EBF', color: '#fff', fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px', fontWeight: 600, cursor: 'pointer', minHeight: '44px',
    },
    locked: {
        textAlign: 'center', padding: '12px', color: '#5A7080', fontSize: '13px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
};

const verdictOptions = ['HELPED', 'HARMED', 'NEUTRAL', 'INSUFFICIENT_DATA'];
const verdictColors = {
    HELPED: '#1A7A4A', HARMED: '#8B1F1F', NEUTRAL: '#5A7080', INSUFFICIENT_DATA: '#8A6000',
};

export default function JournalEntry({ entryId, onBack }) {
    const [entry, setEntry] = useState(null);
    const [outcomeValue, setOutcomeValue] = useState('');
    const [verdict, setVerdict] = useState('');
    const [annotation, setAnnotation] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const { addNotification } = useStore();

    useEffect(() => {
        if (entryId) {
            api.getJournalEntry(entryId).then(setEntry).catch(() => {});
        }
    }, [entryId]);

    const handleSubmit = async () => {
        if (!verdict || submitting) return;
        setSubmitting(true);
        try {
            await api.recordOutcome(entryId, {
                outcome_value: parseFloat(outcomeValue) || 0,
                verdict,
                annotation: annotation || null,
            });
            addNotification('success', 'Outcome recorded');
            const updated = await api.getJournalEntry(entryId);
            setEntry(updated);
        } catch (err) {
            addNotification('error', err.message || 'Failed to record outcome');
        }
        setSubmitting(false);
    };

    if (!entry) return <div style={styles.container}>Loading...</div>;

    const hasOutcome = entry.outcome_recorded_at != null;

    return (
        <div style={styles.container}>
            <button style={styles.backBtn} onClick={onBack}>← Journal</button>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>DECISION CONTEXT</div>
                <div style={styles.row}>
                    <span style={styles.label}>Time</span>
                    <span style={styles.value}>{entry.decision_timestamp?.substring(0, 16)}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Regime</span>
                    <span style={styles.value}>{entry.inferred_state}</span>
                </div>
                <div style={{ margin: '8px 0' }}>
                    <ConfidenceMeter value={entry.state_confidence || 0} label="Confidence" />
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>GRID</span>
                    <span style={styles.value}>{entry.grid_recommendation}</span>
                </div>
                <div style={styles.row}>
                    <span style={styles.label}>Baseline</span>
                    <span style={styles.value}>{entry.baseline_recommendation}</span>
                </div>
            </div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>ACTION vs COUNTERFACTUAL</div>
                <div style={styles.compare}>
                    <div style={styles.compareCol}>
                        <div style={styles.colTitle}>ACTION TAKEN</div>
                        <div style={{ fontSize: '13px', color: '#C8D8E8' }}>{entry.action_taken}</div>
                    </div>
                    <div style={styles.compareCol}>
                        <div style={styles.colTitle}>COUNTERFACTUAL</div>
                        <div style={{ fontSize: '13px', color: '#C8D8E8' }}>{entry.counterfactual}</div>
                    </div>
                </div>
            </div>

            <div style={styles.section}>
                <div style={styles.sectionTitle}>OUTCOME</div>
                {hasOutcome ? (
                    <>
                        <div style={styles.row}>
                            <span style={styles.label}>Value</span>
                            <span style={styles.value}>{entry.outcome_value}</span>
                        </div>
                        <div style={styles.row}>
                            <span style={styles.label}>Verdict</span>
                            <span style={{
                                ...styles.value,
                                color: verdictColors[entry.verdict] || '#C8D8E8',
                            }}>
                                {entry.verdict}
                            </span>
                        </div>
                        {entry.annotation && (
                            <div style={{ marginTop: '8px', fontSize: '13px', color: '#C8D8E8' }}>
                                {entry.annotation}
                            </div>
                        )}
                        <div style={styles.locked}>Outcome locked — immutable</div>
                    </>
                ) : (
                    <>
                        <input
                            type="number" step="any"
                            placeholder="Outcome value (P&L delta)"
                            value={outcomeValue}
                            onChange={e => setOutcomeValue(e.target.value)}
                            style={styles.input}
                        />
                        <div style={styles.verdictBtns}>
                            {verdictOptions.map(v => (
                                <button key={v}
                                    onClick={() => setVerdict(v)}
                                    style={{
                                        ...styles.verdictBtn,
                                        borderColor: verdict === v ? verdictColors[v] : '#1A2840',
                                        color: verdict === v ? verdictColors[v] : '#5A7080',
                                        background: verdict === v ? `${verdictColors[v]}22` : 'transparent',
                                    }}>
                                    {v}
                                </button>
                            ))}
                        </div>
                        <textarea
                            placeholder="Annotation (optional)"
                            value={annotation}
                            onChange={e => setAnnotation(e.target.value)}
                            style={styles.textarea}
                        />
                        <button onClick={handleSubmit} disabled={!verdict || submitting}
                            style={{ ...styles.submitBtn, opacity: (!verdict || submitting) ? 0.5 : 1 }}>
                            {submitting ? '...' : 'RECORD OUTCOME'}
                        </button>
                    </>
                )}
            </div>
        </div>
    );
}
