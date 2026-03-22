import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import RegimeCard from '../components/RegimeCard.jsx';
import StatusDot from '../components/StatusDot.jsx';
import { shared, colors } from '../styles/shared.js';

const verdictColors = {
    HELPED: { bg: '#1A7A4A33', color: '#1A7A4A' },
    HARMED: { bg: '#8B1F1F33', color: '#8B1F1F' },
    NEUTRAL: { bg: '#5A708033', color: '#5A7080' },
    PENDING: { bg: '#1A284033', color: '#5A7080' },
};

const quickActions = [
    { id: 'backtest', label: 'Backtest', desc: 'Performance & track record', color: '#EC4899' },
    { id: 'briefings', label: 'Briefings', desc: 'Market analysis reports', color: '#8B5CF6' },
    { id: 'agents', label: 'Agents', desc: 'LLM trading deliberation', color: '#1A6EBF' },
    { id: 'workflows', label: 'Workflows', desc: '16 data & compute pipelines', color: '#22C55E' },
    { id: 'physics', label: 'Physics', desc: 'Market physics verification', color: '#F59E0B' },
    { id: 'discovery', label: 'Discovery', desc: 'Hypothesis generation', color: '#EF4444' },
];

export default function Dashboard({ onNavigate }) {
    const {
        currentRegime, journalEntries, systemStatus,
        setCurrentRegime, setJournalEntries, setSystemStatus,
        setLoading, addNotification, agentProgress,
    } = useStore();

    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [agentStatus, setAgentStatus] = useState(null);
    const [latestBriefing, setLatestBriefing] = useState(null);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading('dashboard', true);
        try {
            const [regime, journal, status, ollama, agents, briefing] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getJournal({ limit: 3 }).catch(() => ({ entries: [] })),
                api.getStatus().catch(() => null),
                api.getOllamaStatus().catch(() => null),
                api.getAgentStatus().catch(() => null),
                api.getLatestBriefing('hourly').catch(() => null),
            ]);
            if (regime) setCurrentRegime(regime);
            if (journal?.entries) setJournalEntries(journal.entries);
            if (status) setSystemStatus(status);
            setOllamaStatus(ollama);
            setAgentStatus(agents);
            setLatestBriefing(briefing);
        } catch {
            addNotification('error', 'Failed to load dashboard');
        }
        setLoading('dashboard', false);
    };

    const dbOnline = systemStatus?.database?.connected;
    const hsOnline = systemStatus?.hyperspace?.node_online;
    const ollamaOnline = ollamaStatus?.available;
    const agentsEnabled = agentStatus?.enabled;

    return (
        <div style={{ padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '20px' }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: '18px',
                    fontWeight: 700, color: '#1A6EBF', letterSpacing: '3px',
                }}>GRID</span>
                <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                    <StatusDot status={dbOnline ? 'online' : 'offline'} label="DB" />
                    <StatusDot status={hsOnline ? 'online' : 'offline'} label="HS" />
                    <StatusDot status={ollamaOnline ? 'online' : 'offline'} label="LLM" />
                </div>
            </div>

            {/* Regime */}
            <div style={{ marginBottom: '16px' }}>
                <RegimeCard regime={currentRegime} onClick={() => onNavigate('regime')} />
            </div>

            {/* Live Agent Progress */}
            {agentProgress && (
                <div style={{
                    ...shared.card, borderColor: colors.accent,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                }}>
                    <div>
                        <span style={{ fontSize: '12px', color: colors.accent, fontWeight: 600 }}>
                            AGENT RUNNING
                        </span>
                        <div style={{ fontSize: '12px', color: colors.textMuted, marginTop: '2px' }}>
                            {agentProgress.stage}: {agentProgress.detail}
                        </div>
                    </div>
                    <span style={shared.badge('#1A6EBF')}>{agentProgress.ticker}</span>
                </div>
            )}

            {/* Quick Actions Grid */}
            <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
                gap: '10px', marginBottom: '16px',
            }}>
                {quickActions.map(action => (
                    <div
                        key={action.id}
                        onClick={() => onNavigate(action.id)}
                        style={{
                            background: colors.card, border: `1px solid ${colors.border}`,
                            borderRadius: '10px', padding: '14px 12px', cursor: 'pointer',
                            borderTop: `3px solid ${action.color}`,
                        }}
                    >
                        <div style={{ fontSize: '13px', fontWeight: 600, color: colors.text }}>
                            {action.label}
                        </div>
                        <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '4px' }}>
                            {action.desc}
                        </div>
                    </div>
                ))}
            </div>

            {/* Status Strip */}
            <div style={shared.metricGrid}>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, fontSize: '14px' }}>
                        {systemStatus?.grid?.total_features || '--'}
                    </div>
                    <div style={shared.metricLabel}>Features</div>
                </div>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, fontSize: '14px' }}>
                        {systemStatus?.grid?.hypotheses_total || '--'}
                    </div>
                    <div style={shared.metricLabel}>Hypotheses</div>
                </div>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, fontSize: '14px' }}>
                        {systemStatus?.grid?.journal_entries || '--'}
                    </div>
                    <div style={shared.metricLabel}>Journal</div>
                </div>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, fontSize: '14px', color: agentsEnabled ? colors.green : colors.textMuted }}>
                        {agentsEnabled ? 'ON' : 'OFF'}
                    </div>
                    <div style={shared.metricLabel}>Agents</div>
                </div>
            </div>

            {/* Latest Briefing Preview */}
            {latestBriefing?.content && (
                <div
                    style={{ ...shared.card, marginTop: '12px', cursor: 'pointer' }}
                    onClick={() => onNavigate('briefings')}
                >
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                        <span style={{ fontSize: '11px', color: colors.textMuted, letterSpacing: '1px',
                            fontFamily: "'JetBrains Mono', monospace" }}>
                            LATEST BRIEFING
                        </span>
                        <span style={{ fontSize: '11px', color: colors.accent, cursor: 'pointer' }}>View All</span>
                    </div>
                    <div style={{
                        fontSize: '12px', color: colors.textDim, lineHeight: '1.6',
                        maxHeight: '80px', overflow: 'hidden', fontFamily: colors.mono,
                    }}>
                        {latestBriefing.content.substring(0, 300)}...
                    </div>
                </div>
            )}

            {/* Recent Journal */}
            <div style={{ marginTop: '12px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <span style={{
                        fontSize: '11px', color: colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px',
                    }}>RECENT JOURNAL</span>
                    <button
                        style={{ color: colors.accent, fontSize: '13px', cursor: 'pointer',
                            background: 'none', border: 'none', fontFamily: colors.sans }}
                        onClick={() => onNavigate('journal')}
                    >View All</button>
                </div>
                {journalEntries.slice(0, 3).map((entry, i) => {
                    const verdict = entry.verdict || 'PENDING';
                    const vc = verdictColors[verdict] || verdictColors.PENDING;
                    return (
                        <div key={entry.id || i}
                            style={{
                                background: colors.card, borderRadius: '8px', padding: '12px',
                                border: `1px solid ${colors.border}`, marginTop: '8px',
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                cursor: 'pointer', minHeight: '44px',
                            }}
                            onClick={() => onNavigate('journal-entry', entry.id)}
                        >
                            <div>
                                <span style={{
                                    fontSize: '10px', fontWeight: 600, padding: '2px 8px',
                                    borderRadius: '4px', background: vc.bg, color: vc.color,
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>{verdict}</span>
                                <span style={{ marginLeft: '10px', fontSize: '13px', color: colors.text }}>
                                    {entry.action_taken?.substring(0, 40)}
                                </span>
                            </div>
                            <span style={{ fontSize: '12px', color: colors.textMuted, fontFamily: colors.mono }}>
                                {entry.outcome_value != null ? entry.outcome_value.toFixed(2) : ''}
                            </span>
                        </div>
                    );
                })}
                {journalEntries.length === 0 && (
                    <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                        No journal entries yet
                    </div>
                )}
            </div>
        </div>
    );
}
