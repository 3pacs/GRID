import React, { useEffect } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import RegimeCard from '../components/RegimeCard.jsx';
import StatusDot from '../components/StatusDot.jsx';

const styles = {
    container: { padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' },
    header: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '20px',
    },
    wordmark: {
        fontFamily: "'JetBrains Mono', monospace", fontSize: '18px',
        fontWeight: 700, color: '#1A6EBF', letterSpacing: '3px',
    },
    dots: { display: 'flex', gap: '12px', alignItems: 'center' },
    section: { marginBottom: '20px' },
    sectionTitle: {
        fontSize: '11px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px', marginBottom: '10px',
    },
    signalRow: {
        display: 'flex', gap: '10px', overflowX: 'auto', paddingBottom: '8px',
        WebkitOverflowScrolling: 'touch',
    },
    journalRow: {
        background: '#0D1520', borderRadius: '8px', padding: '12px',
        border: '1px solid #1A2840', marginBottom: '8px',
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        cursor: 'pointer', minHeight: '44px',
    },
    verdictChip: {
        fontSize: '10px', fontWeight: 600, padding: '2px 8px', borderRadius: '4px',
        fontFamily: "'JetBrains Mono', monospace",
    },
    strip: {
        background: '#0D1520', borderRadius: '10px', padding: '12px 16px',
        border: '1px solid #1A2840', display: 'flex', alignItems: 'center',
        gap: '16px', cursor: 'pointer', minHeight: '44px',
    },
    viewAll: {
        color: '#1A6EBF', fontSize: '13px', cursor: 'pointer',
        fontFamily: "'IBM Plex Sans', sans-serif", background: 'none', border: 'none',
    },
};

const verdictColors = {
    HELPED: { bg: '#1A7A4A33', color: '#1A7A4A' },
    HARMED: { bg: '#8B1F1F33', color: '#8B1F1F' },
    NEUTRAL: { bg: '#5A708033', color: '#5A7080' },
    PENDING: { bg: '#1A284033', color: '#5A7080' },
};

export default function Dashboard({ onNavigate }) {
    const {
        currentRegime, journalEntries, systemStatus,
        setCurrentRegime, setJournalEntries, setSystemStatus,
        setLoading, addNotification,
    } = useStore();

    useEffect(() => {
        loadData();
    }, []);

    const loadData = async () => {
        setLoading('dashboard', true);
        try {
            const [regime, journal, status] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getJournal({ limit: 3 }).catch(() => ({ entries: [] })),
                api.getStatus().catch(() => null),
            ]);
            if (regime) setCurrentRegime(regime);
            if (journal?.entries) setJournalEntries(journal.entries);
            if (status) setSystemStatus(status);
        } catch (err) {
            addNotification('error', 'Failed to load dashboard');
        }
        setLoading('dashboard', false);
    };

    const dbOnline = systemStatus?.database?.connected;
    const hsOnline = systemStatus?.hyperspace?.node_online;

    return (
        <div style={styles.container}>
            <div style={styles.header}>
                <span style={styles.wordmark}>GRID</span>
                <div style={styles.dots}>
                    <StatusDot status={dbOnline ? 'online' : 'offline'} label="DB" />
                    <StatusDot status={hsOnline ? 'online' : 'offline'} label="HS" />
                </div>
            </div>

            <div style={styles.section}>
                <RegimeCard regime={currentRegime} onClick={() => onNavigate('regime')} />
            </div>

            <div style={styles.section}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div style={styles.sectionTitle}>RECENT JOURNAL</div>
                    <button style={styles.viewAll} onClick={() => onNavigate('journal')}>View All</button>
                </div>
                {journalEntries.slice(0, 3).map((entry, i) => {
                    const verdict = entry.verdict || 'PENDING';
                    const vc = verdictColors[verdict] || verdictColors.PENDING;
                    return (
                        <div key={entry.id || i} style={styles.journalRow}
                            onClick={() => onNavigate('journal-entry', entry.id)}>
                            <div>
                                <span style={{ ...styles.verdictChip, background: vc.bg, color: vc.color }}>
                                    {verdict}
                                </span>
                                <span style={{ marginLeft: '10px', fontSize: '13px', color: '#C8D8E8' }}>
                                    {entry.action_taken?.substring(0, 40)}
                                </span>
                            </div>
                            <span style={{ fontSize: '12px', color: '#5A7080', fontFamily: "'JetBrains Mono', monospace" }}>
                                {entry.outcome_value != null ? entry.outcome_value.toFixed(2) : '→'}
                            </span>
                        </div>
                    );
                })}
                {journalEntries.length === 0 && (
                    <div style={{ color: '#5A7080', fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                        No journal entries yet
                    </div>
                )}
            </div>

            <div style={styles.strip} onClick={() => onNavigate('hyperspace')}>
                <StatusDot status={dbOnline ? 'online' : 'offline'} label="Database" />
                <StatusDot status={hsOnline ? 'online' : 'offline'} label="Hyperspace" />
                {systemStatus?.hyperspace?.points != null && (
                    <span style={{
                        marginLeft: 'auto', fontFamily: "'JetBrains Mono', monospace",
                        fontSize: '12px', color: '#B8922A',
                    }}>
                        {systemStatus.hyperspace.points.toFixed(0)} pts
                    </span>
                )}
            </div>
        </div>
    );
}
