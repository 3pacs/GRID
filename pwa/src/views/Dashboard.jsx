import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import RegimeCard from '../components/RegimeCard.jsx';
import StatusDot from '../components/StatusDot.jsx';
import RegimeThermometer from '../components/RegimeThermometer.jsx';
import MarketPulse from '../components/MarketPulse.jsx';
import MomentumSparks from '../components/MomentumSparks.jsx';
import FearGreedGauge from '../components/FearGreedGauge.jsx';
import CapitalFlowAnalysis from '../components/CapitalFlowAnalysis.jsx';
import WidgetManager, { loadWidgetPrefs, isWidgetVisible } from '../components/WidgetManager.jsx';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

const assetTypeColors = {
    stock: '#1A6EBF',
    crypto: '#F59E0B',
    commodity: '#22C55E',
    etf: '#8B5CF6',
    index: '#EC4899',
    forex: '#06B6D4',
};

const verdictColors = {
    HELPED: { bg: '#1A7A4A33', color: '#1A7A4A' },
    HARMED: { bg: '#8B1F1F33', color: '#8B1F1F' },
    NEUTRAL: { bg: '#5A708033', color: '#5A7080' },
    PENDING: { bg: '#1A284033', color: '#5A7080' },
};

const quickActions = [
    { id: 'briefings', label: 'Briefings', desc: 'Market analysis reports', color: '#8B5CF6' },
    { id: 'options', label: 'Options', desc: 'Signals, scanner, 100x plays', color: '#EC4899' },
    { id: 'heatmap', label: 'Heatmap', desc: 'Signal strength across sectors', color: '#06B6D4' },
    { id: 'regime', label: 'Regime', desc: 'Market state & what to do', color: '#22C55E' },
    { id: 'agents', label: 'Agents', desc: 'LLM trading deliberation', color: '#1A6EBF' },
    { id: 'backtest', label: 'Backtest', desc: 'Performance & track record', color: '#F59E0B' },
    { id: 'physics', label: 'Physics', desc: 'Momentum & energy analysis', color: '#EF4444' },
    { id: 'discovery', label: 'Discovery', desc: 'Hypothesis generation', color: '#8B5CF6' },
];

export default function Dashboard({ onNavigate }) {
    const {
        currentRegime, journalEntries, systemStatus,
        setCurrentRegime, setJournalEntries, setSystemStatus,
        setLoading, addNotification, agentProgress,
    } = useStore();

    const { isMobile, isTablet } = useDevice();
    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [agentStatus, setAgentStatus] = useState(null);
    const [latestBriefing, setLatestBriefing] = useState(null);
    const [watchlist, setWatchlist] = useState([]);
    const [addingTicker, setAddingTicker] = useState(false);
    const [newTicker, setNewTicker] = useState('');
    const [liveSignals, setLiveSignals] = useState(null);
    const [physicsDash, setPhysicsDash] = useState(null);

    // Widget manager state
    const [widgetPrefs, setWidgetPrefs] = useState(loadWidgetPrefs);
    const [widgetPanelOpen, setWidgetPanelOpen] = useState(false);
    const w = (id) => isWidgetVisible(widgetPrefs, id);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading('dashboard', true);
        try {
            const [regime, journal, status, ollama, agents, briefing, wl, signals, physics] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getJournal({ limit: 3 }).catch(() => ({ entries: [] })),
                api.getStatus().catch(() => null),
                api.getOllamaStatus().catch(() => null),
                api.getAgentStatus().catch(() => null),
                api.getLatestBriefing('hourly').catch(() => null),
                api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                api.getSignalSnapshot().catch(() => null),
                api.getPhysicsDashboard().catch(() => null),
            ]);
            if (regime) setCurrentRegime(regime);
            if (journal?.entries) setJournalEntries(journal.entries);
            if (status) setSystemStatus(status);
            setOllamaStatus(ollama);
            setAgentStatus(agents);
            setLatestBriefing(briefing);
            if (wl?.items) setWatchlist(wl.items);
            setLiveSignals(signals);
            setPhysicsDash(physics);
        } catch {
            addNotification('error', 'Failed to load dashboard');
        }
        setLoading('dashboard', false);
    };

    const handleAddTicker = async () => {
        if (!newTicker.trim()) return;
        setAddingTicker(true);
        try {
            await api.addToWatchlist({ ticker: newTicker.trim() });
            setNewTicker('');
            addNotification('success', `Added ${newTicker.trim().toUpperCase()} to watchlist`);
            const wl = await api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] }));
            if (wl?.items) setWatchlist(wl.items);
        } catch (err) {
            addNotification('error', err.message || 'Failed to add ticker');
        }
        setAddingTicker(false);
    };

    const handleRemoveTicker = async (ticker) => {
        try {
            await api.removeFromWatchlist(ticker);
            setWatchlist(prev => prev.filter(item => item.ticker !== ticker));
            addNotification('info', `Removed ${ticker} from watchlist`);
        } catch (err) {
            addNotification('error', err.message || 'Failed to remove ticker');
        }
    };

    const dbOnline = systemStatus?.database?.connected;
    const hsOnline = systemStatus?.hyperspace?.node_online;
    const ollamaOnline = ollamaStatus?.available;
    const agentsEnabled = agentStatus?.enabled;

    const activeWidgets = Object.values(widgetPrefs).filter(Boolean).length;
    const totalWidgets = Object.keys(widgetPrefs).length;

    return (
        <div style={{ padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <span style={{
                    fontFamily: "'JetBrains Mono', monospace", fontSize: '20px',
                    fontWeight: 700, color: '#1A6EBF', letterSpacing: '3px',
                }}>GRID</span>
                <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                    <StatusDot status={dbOnline ? 'online' : 'offline'} label="DB" />
                    <StatusDot status={hsOnline ? 'online' : 'offline'} label="HS" />
                    <StatusDot status={ollamaOnline ? 'online' : 'offline'} label="LLM" />
                    <button
                        onClick={() => setWidgetPanelOpen(true)}
                        style={{
                            background: colors.card, border: `1px solid ${colors.border}`,
                            borderRadius: '6px', padding: '8px 12px', cursor: 'pointer',
                            fontSize: '11px', color: colors.textMuted, minHeight: '36px',
                            fontFamily: "'JetBrains Mono', monospace",
                        }}
                        title="Configure dashboard widgets"
                    >
                        {activeWidgets}/{totalWidgets}
                    </button>
                </div>
            </div>

            {/* Regime Thermometer */}
            {w('regime-thermo') && (
                <div style={{ marginBottom: '12px' }}>
                    <RegimeThermometer regime={currentRegime} />
                </div>
            )}

            {/* Market Pulse Heatmap */}
            {w('market-pulse') && (
                <div style={{ marginBottom: '12px' }}>
                    <MarketPulse signals={liveSignals} />
                </div>
            )}

            {/* Momentum Sparklines */}
            {w('momentum-sparks') && (
                <div style={{ marginBottom: '12px' }}>
                    <MomentumSparks signals={liveSignals} physics={physicsDash} />
                </div>
            )}

            {/* Capital Flow Analysis */}
            {w('capital-flow') && (
                <div style={{ marginBottom: '16px' }}>
                    <CapitalFlowAnalysis />
                </div>
            )}

            {/* Watchlist */}
            {w('watchlist') && (
                <div style={{ marginBottom: '16px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                        <span style={{
                            fontSize: '11px', color: colors.textMuted,
                            fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px',
                        }}>WATCHLIST</span>
                        <span style={{ fontSize: '11px', color: colors.textMuted }}>
                            {watchlist.length} ticker{watchlist.length !== 1 ? 's' : ''}
                        </span>
                    </div>

                    {/* Add Ticker Input */}
                    <div style={{
                        display: 'flex', gap: '8px', marginBottom: '10px',
                    }}>
                        <input
                            type="text"
                            value={newTicker}
                            onChange={(e) => setNewTicker(e.target.value)}
                            onKeyDown={(e) => e.key === 'Enter' && handleAddTicker()}
                            placeholder="Add ticker (e.g. AAPL)"
                            style={{
                                ...shared.input, flex: 1, padding: '8px 12px',
                                fontSize: '13px',
                            }}
                        />
                        <button
                            onClick={handleAddTicker}
                            disabled={addingTicker || !newTicker.trim()}
                            style={{
                                ...shared.buttonSmall,
                                ...(addingTicker || !newTicker.trim() ? shared.buttonDisabled : {}),
                                whiteSpace: 'nowrap',
                            }}
                        >
                            {addingTicker ? '...' : 'Add'}
                        </button>
                    </div>

                    {/* Watchlist Items */}
                    {watchlist.length > 0 ? (
                        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                            {watchlist.map((item) => {
                                const typeColor = assetTypeColors[item.asset_type] || colors.accent;
                                return (
                                    <div
                                        key={item.id}
                                        style={{
                                            background: colors.card, borderRadius: '10px',
                                            padding: '12px 14px',
                                            border: `1px solid ${colors.border}`,
                                            borderLeft: `3px solid ${typeColor}`,
                                            display: 'flex', justifyContent: 'space-between',
                                            alignItems: 'center', cursor: 'pointer',
                                            minHeight: '44px',
                                        }}
                                        onClick={() => onNavigate('watchlist-analysis', item.ticker)}
                                    >
                                        <div style={{ flex: 1 }}>
                                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                                <span style={{
                                                    fontSize: '14px', fontWeight: 700, color: '#E8F0F8',
                                                    fontFamily: "'JetBrains Mono', monospace",
                                                }}>
                                                    {item.ticker}
                                                </span>
                                                <span style={{
                                                    fontSize: '10px', padding: '1px 6px', borderRadius: '4px',
                                                    background: typeColor + '25', color: typeColor,
                                                    fontFamily: colors.mono, fontWeight: 600,
                                                }}>
                                                    {item.asset_type}
                                                </span>
                                            </div>
                                            {item.display_name && item.display_name !== item.ticker && (
                                                <div style={{
                                                    fontSize: '11px', color: colors.textMuted, marginTop: '2px',
                                                }}>
                                                    {item.display_name}
                                                </div>
                                            )}
                                            {item.notes && (
                                                <div style={{
                                                    fontSize: '11px', color: colors.textDim, marginTop: '2px',
                                                    maxWidth: '220px', overflow: 'hidden', textOverflow: 'ellipsis',
                                                    whiteSpace: 'nowrap',
                                                }}>
                                                    {item.notes}
                                                </div>
                                            )}
                                        </div>
                                        <button
                                            onClick={(e) => { e.stopPropagation(); handleRemoveTicker(item.ticker); }}
                                            style={{
                                                background: 'none', border: 'none', cursor: 'pointer',
                                                color: colors.textMuted, fontSize: '16px', padding: '10px',
                                                minWidth: tokens.minTouch, minHeight: tokens.minTouch,
                                            }}
                                            title="Remove from watchlist"
                                        >
                                            x
                                        </button>
                                    </div>
                                );
                            })}
                        </div>
                    ) : (
                        <div style={{
                            ...shared.card, color: colors.textMuted, fontSize: '13px',
                            textAlign: 'center', padding: '16px',
                        }}>
                            No tickers on watchlist. Add one above.
                        </div>
                    )}
                </div>
            )}

            {/* Regime */}
            {w('regime-card') && (
                <div style={{ marginBottom: '16px' }}>
                    <RegimeCard regime={currentRegime} onClick={() => onNavigate('regime')} />
                </div>
            )}

            {/* Live Agent Progress */}
            {w('agent-progress') && agentProgress && (
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
            {w('quick-actions') && (
                <div style={{
                    display: 'grid',
                    gridTemplateColumns: isMobile ? 'repeat(2, 1fr)' : isTablet ? 'repeat(3, 1fr)' : 'repeat(4, 1fr)',
                    gap: '10px', marginBottom: '16px',
                }}>
                    {quickActions.map(action => (
                        <div
                            key={action.id}
                            onClick={() => onNavigate(action.id)}
                            style={{
                                background: colors.card, border: `1px solid ${colors.border}`,
                                borderRadius: tokens.radius.md, padding: '16px', cursor: 'pointer',
                                borderTop: `3px solid ${action.color}`,
                                boxShadow: colors.shadow.sm,
                                transition: `all ${tokens.transition.fast}`,
                            }}
                        >
                            <div style={{ fontSize: tokens.fontSize.md, fontWeight: 600, color: colors.text }}>
                                {action.label}
                            </div>
                            <div style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, marginTop: '4px' }}>
                                {action.desc}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Status Strip with Fear/Greed Gauge */}
            {(w('fear-greed') || w('status-metrics')) && (
                <div style={{
                    display: 'flex', flexDirection: isMobile ? 'column' : 'row',
                    gap: '10px', alignItems: isMobile ? 'center' : 'stretch',
                    marginTop: '8px', marginBottom: '12px',
                }}>
                    {/* Fear/Greed Gauge */}
                    {w('fear-greed') && (
                        <FearGreedGauge signals={liveSignals} regime={currentRegime} />
                    )}

                    {/* Metrics */}
                    {w('status-metrics') && (
                        <div style={{
                            flex: 1, display: 'grid',
                            gridTemplateColumns: 'repeat(2, 1fr)',
                            gap: '8px',
                        }}>
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
                    )}
                </div>
            )}

            {/* Latest Briefing Preview */}
            {w('briefing-preview') && latestBriefing?.content && (
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
            {w('journal') && (
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
            )}

            {/* Widget Manager Panel */}
            <WidgetManager
                prefs={widgetPrefs}
                onPrefsChange={setWidgetPrefs}
                open={widgetPanelOpen}
                onClose={() => setWidgetPanelOpen(false)}
            />
        </div>
    );
}
