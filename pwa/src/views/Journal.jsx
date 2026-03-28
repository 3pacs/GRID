import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ViewHelp from '../components/ViewHelp.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    title: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '14px',
        color: '#5A7080', letterSpacing: '2px', marginBottom: '12px',
    },
    statsBar: {
        display: 'flex', gap: '12px', marginBottom: '16px', overflowX: 'auto',
    },
    statBox: {
        background: '#0D1520', borderRadius: '8px', padding: '10px 14px',
        border: '1px solid #1A2840', minWidth: '80px', textAlign: 'center',
    },
    statValue: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '18px', fontWeight: 700,
    },
    statLabel: {
        fontSize: '10px', color: '#5A7080', marginTop: '2px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    tabs: {
        display: 'flex', gap: '4px', marginBottom: '16px', overflowX: 'auto',
    },
    tab: {
        padding: '6px 14px', borderRadius: '6px', border: '1px solid #1A2840',
        background: 'transparent', color: '#5A7080', fontSize: '12px',
        fontFamily: "'JetBrains Mono', monospace", cursor: 'pointer',
        whiteSpace: 'nowrap', minHeight: '36px',
    },
    tabActive: {
        background: '#1A6EBF22', borderColor: '#1A6EBF', color: '#1A6EBF',
    },
    entryRow: {
        background: '#0D1520', borderRadius: '8px', padding: '12px 14px',
        border: '1px solid #1A2840', marginBottom: '8px', cursor: 'pointer',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        minHeight: '44px',
    },
    verdictChip: {
        fontSize: '10px', fontWeight: 600, padding: '2px 8px', borderRadius: '4px',
        fontFamily: "'JetBrains Mono', monospace",
    },
};

const verdictColors = {
    HELPED: { bg: '#1A7A4A33', color: '#1A7A4A' },
    HARMED: { bg: '#8B1F1F33', color: '#8B1F1F' },
    NEUTRAL: { bg: '#5A708033', color: '#5A7080' },
    PENDING: { bg: '#1A284033', color: '#5A7080' },
};

const FILTERS = ['ALL', 'PENDING', 'HELPED', 'HARMED', 'NEUTRAL'];

export default function Journal({ onNavigate }) {
    const { journalEntries, journalStats, setJournalEntries, setJournalStats } = useStore();
    const [filter, setFilter] = useState('ALL');

    useEffect(() => {
        loadEntries();
        api.getJournalStats().then(setJournalStats).catch(() => {});
    }, [filter]);

    const loadEntries = async () => {
        const params = { limit: 50 };
        if (filter !== 'ALL') params.verdict = filter;
        try {
            const data = await api.getJournal(params);
            setJournalEntries(data.entries || []);
        } catch (e) { console.warn('[GRID] Journal:', e.message); }
    };

    const stats = journalStats || {};

    return (
        <div style={styles.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={styles.title}>DECISION JOURNAL</div>
                <ViewHelp id="journal" />
            </div>

            <div style={styles.statsBar}>
                <div style={styles.statBox}>
                    <div style={{ ...styles.statValue, color: '#1A7A4A' }}>
                        {stats.helped_rate ? `${Math.round(stats.helped_rate * 100)}%` : '—'}
                    </div>
                    <div style={styles.statLabel}>Helped</div>
                </div>
                <div style={styles.statBox}>
                    <div style={{ ...styles.statValue, color: '#C8D8E8' }}>
                        {stats.total_decisions || 0}
                    </div>
                    <div style={styles.statLabel}>Total</div>
                </div>
                <div style={styles.statBox}>
                    <div style={{ ...styles.statValue, color: '#B8922A' }}>
                        {stats.avg_outcome_value?.toFixed(2) || '—'}
                    </div>
                    <div style={styles.statLabel}>Avg Value</div>
                </div>
            </div>

            <div style={styles.tabs}>
                {FILTERS.map(f => (
                    <button key={f} onClick={() => setFilter(f)}
                        style={{ ...styles.tab, ...(filter === f ? styles.tabActive : {}) }}>
                        {f}
                    </button>
                ))}
            </div>

            {journalEntries.map((entry, i) => {
                const verdict = entry.verdict || 'PENDING';
                const vc = verdictColors[verdict] || verdictColors.PENDING;
                return (
                    <div key={entry.id || i} style={styles.entryRow}
                        onClick={() => onNavigate('journal-entry', entry.id)}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px', flex: 1, overflow: 'hidden' }}>
                            <span style={{ ...styles.verdictChip, background: vc.bg, color: vc.color }}>
                                {verdict}
                            </span>
                            <div style={{ overflow: 'hidden', minWidth: 0 }}>
                                <div style={{
                                    fontSize: '12px', color: '#5A7080',
                                    fontFamily: "'JetBrains Mono', monospace",
                                    whiteSpace: 'nowrap',
                                }}>
                                    {entry.decision_timestamp?.substring(0, 16)}
                                </div>
                                <div title={entry.action_taken} style={{
                                    fontSize: '13px', color: '#C8D8E8',
                                    whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                                    lineHeight: '1.5',
                                }}>
                                    {entry.action_taken}
                                </div>
                            </div>
                        </div>
                        <span style={{
                            fontFamily: "'JetBrains Mono', monospace",
                            fontSize: '14px', color: '#C8D8E8', flexShrink: 0,
                        }}>
                            {entry.outcome_value != null ? entry.outcome_value.toFixed(2) : '→'}
                        </span>
                    </div>
                );
            })}

            {journalEntries.length === 0 && (
                <div style={{ color: '#5A7080', textAlign: 'center', padding: '40px', fontSize: '14px' }}>
                    No entries match filter
                </div>
            )}
        </div>
    );
}
