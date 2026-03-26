import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import StatusDot from '../components/StatusDot.jsx';
import RegimeThermometer from '../components/RegimeThermometer.jsx';
import CapitalFlowAnalysis from '../components/CapitalFlowAnalysis.jsx';
import WidgetManager, { loadWidgetPrefs, isWidgetVisible } from '../components/WidgetManager.jsx';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';
import ViewHelp from '../components/ViewHelp.jsx';

const flowColor = (pct) => {
    if (pct == null) return colors.textMuted;
    if (pct > 0.03) return colors.green;
    if (pct > 0) return '#4ADE80';
    if (pct < -0.03) return colors.red;
    if (pct < 0) return '#F97316';
    return colors.textMuted;
};

export default function Dashboard({ onNavigate }) {
    const {
        currentRegime, journalEntries, systemStatus,
        setCurrentRegime, setJournalEntries, setSystemStatus,
        setLoading, addNotification, agentProgress,
    } = useStore();

    const { isMobile } = useDevice();
    const [ollamaStatus, setOllamaStatus] = useState(null);
    const [latestBriefing, setLatestBriefing] = useState(null);
    const [watchlist, setWatchlist] = useState([]);
    const [addingTicker, setAddingTicker] = useState(false);
    const [newTicker, setNewTicker] = useState('');
    const [liveSignals, setLiveSignals] = useState(null);
    const [askOpen, setAskOpen] = useState(false);
    const [askQuery, setAskQuery] = useState('');
    const [askResult, setAskResult] = useState(null);
    const [actionResult, setActionResult] = useState(null);

    const [widgetPrefs, setWidgetPrefs] = useState(loadWidgetPrefs);
    const [widgetPanelOpen, setWidgetPanelOpen] = useState(false);

    const [enrichedWatchlist, setEnrichedWatchlist] = useState([]);
    const [suggestions, setSuggestions] = useState([]);

    useEffect(() => { loadData(); }, []);

    const loadData = async () => {
        setLoading('dashboard', true);
        try {
            const [regime, journal, status, ollama, briefing, wl, signals, enrichedWl] = await Promise.all([
                api.getCurrent().catch(() => null),
                api.getJournal({ limit: 3 }).catch(() => ({ entries: [] })),
                api.getStatus().catch(() => null),
                api.getOllamaStatus().catch(() => null),
                api.getLatestBriefing('hourly').catch(() => null),
                api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] })),
                api.getSignalSnapshot().catch(() => null),
                api.getWatchlistEnriched(10).catch(() => ({ items: [], suggestions: [] })),
            ]);
            if (regime) setCurrentRegime(regime);
            if (journal?.entries) setJournalEntries(journal.entries);
            if (status) setSystemStatus(status);
            setOllamaStatus(ollama);
            setLatestBriefing(briefing);
            if (wl?.items) setWatchlist(wl.items);
            setLiveSignals(signals);
            if (enrichedWl?.items) setEnrichedWatchlist(enrichedWl.items);
            if (enrichedWl?.suggestions) setSuggestions(enrichedWl.suggestions);
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
            addNotification('success', `Added ${newTicker.trim().toUpperCase()}`);
            const wl = await api.getWatchlist({ limit: 10 }).catch(() => ({ items: [] }));
            if (wl?.items) setWatchlist(wl.items);
        } catch (err) {
            addNotification('error', err.message || 'Failed');
        }
        setAddingTicker(false);
    };

    const _runAction = async (type) => {
        setActionResult(`Running ${type}...`);
        try {
            if (type === 'briefing') {
                const r = await api.generateBriefing('daily');
                setActionResult(`Briefing: ${r?.content?.length || 0} chars`);
                loadData();
            } else if (type === 'regime') {
                await api.runWorkflow('auto-regime');
                setActionResult('Regime updated');
                loadData();
            } else if (type === 'pull') {
                await api.runWorkflow('daily-pulls');
                setActionResult('Data pull started');
            } else if (type === 'orthogonality') {
                await api.runOrthogonality();
                setActionResult('Orthogonality started');
            }
        } catch (err) {
            setActionResult(`Error: ${err.message || 'failed'}`);
        }
        setTimeout(() => setActionResult(null), 5000);
    };

    const _timeAgo = (ts) => {
        const diff = Date.now() - new Date(ts).getTime();
        const mins = Math.floor(diff / 60000);
        if (mins < 60) return `${mins}m ago`;
        const hrs = Math.floor(mins / 60);
        if (hrs < 24) return `${hrs}h ago`;
        return `${Math.floor(hrs / 24)}d ago`;
    };

    const _formatBriefing = (md) => {
        if (!md) return '';
        return md
            .replace(/### (.*)/g, `<div style="font-size:12px;font-weight:700;color:${colors.text};margin:8px 0 4px;font-family:'JetBrains Mono',monospace">$1</div>`)
            .replace(/## (.*)/g, `<div style="font-size:13px;font-weight:700;color:#E8F0F8;margin:10px 0 4px;font-family:'JetBrains Mono',monospace">$1</div>`)
            .replace(/# (.*)/g, `<div style="font-size:14px;font-weight:700;color:#E8F0F8;margin:0 0 6px;font-family:'JetBrains Mono',monospace">$1</div>`)
            .replace(/\*\*(.*?)\*\*/g, `<strong style="color:${colors.text}">$1</strong>`)
            .replace(/- (.*)/g, `<div style="font-size:12px;color:${colors.textDim};line-height:1.6;padding-left:10px">• $1</div>`)
            .replace(/\n\n/g, '<div style="height:6px"></div>')
            .replace(/\n/g, '');
    };

    const dbOnline = systemStatus?.database?.connected;
    const hsOnline = systemStatus?.hyperspace?.node_online;
    const ollamaOnline = ollamaStatus?.available;

    // Build situation report from live data
    const regime = currentRegime;
    const regimeState = regime?.state || '?';
    const regimeConf = regime?.confidence;
    const regimeColor = regimeState === 'GROWTH' ? colors.green : regimeState === 'CRISIS' ? colors.red : regimeState === 'FRAGILE' ? colors.yellow : colors.accent;

    // Signal summary from snapshot
    const feats = liveSignals?.features || [];
    const withZ = feats.filter(f => f.z_score != null);
    const bullish = withZ.filter(f => f.z_score > 0.5).length;
    const bearish = withZ.filter(f => f.z_score < -0.5).length;
    const extreme = withZ.filter(f => Math.abs(f.z_score) > 2.5).length;

    return (
        <div style={{ padding: '16px', paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <span style={{ fontFamily: "'JetBrains Mono', monospace", fontSize: '20px', fontWeight: 700, color: '#1A6EBF', letterSpacing: '3px' }}>GRID</span>
                <div style={{ display: 'flex', gap: '10px', alignItems: 'center' }}>
                    <ViewHelp id="dashboard" />
                    <StatusDot status={dbOnline ? 'online' : 'offline'} label="DB" />
                    <StatusDot status={hsOnline ? 'online' : 'offline'} label="HS" />
                    <StatusDot status={ollamaOnline ? 'online' : 'offline'} label="LLM" />
                    <button onClick={() => setWidgetPanelOpen(true)} style={{
                        background: colors.card, border: `1px solid ${colors.border}`,
                        borderRadius: '6px', padding: '8px 12px', cursor: 'pointer',
                        fontSize: '11px', color: colors.textMuted, minHeight: '36px',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>cfg</button>
                </div>
            </div>

            {/* ═══ SITUATION REPORT ═══ */}
            <div style={{
                ...shared.card, borderLeft: `3px solid ${regimeColor}`,
            }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                        <span style={{
                            fontSize: '14px', fontWeight: 700, padding: '3px 10px',
                            borderRadius: '6px', background: `${regimeColor}20`, color: regimeColor,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>{regimeState}</span>
                        <span style={{ fontSize: '12px', color: colors.textDim }}>
                            {regimeConf != null ? `${(regimeConf * 100).toFixed(0)}% confidence` : ''}
                        </span>
                    </div>
                    <span style={{ fontSize: '10px', color: colors.textMuted }}>
                        {regime?.as_of ? _timeAgo(regime.as_of) : ''}
                    </span>
                </div>

                {/* Stress bar */}
                <RegimeThermometer regime={regime} />

                {/* Signal summary */}
                <div style={{
                    display: 'flex', gap: '16px', marginTop: '10px',
                    fontSize: '12px', fontFamily: "'JetBrains Mono', monospace",
                }}>
                    <span style={{ color: colors.textDim }}>
                        <span style={{ color: colors.green, fontWeight: 600 }}>{bullish}</span> bullish
                    </span>
                    <span style={{ color: colors.textDim }}>
                        <span style={{ color: colors.red, fontWeight: 600 }}>{bearish}</span> bearish
                    </span>
                    {extreme > 0 && (
                        <span style={{ color: colors.yellow, fontWeight: 600 }}>
                            {extreme} extreme
                        </span>
                    )}
                    <span style={{ color: colors.textMuted }}>
                        {withZ.length} signals
                    </span>
                </div>

                {/* Latest decision */}
                {journalEntries.length > 0 && (() => {
                    const e = journalEntries[0];
                    const posture = e.grid_recommendation || e.action_taken || '';
                    return (
                        <div style={{
                            marginTop: '10px', paddingTop: '10px',
                            borderTop: `1px solid ${colors.borderSubtle}`,
                            fontSize: '12px', color: colors.textDim,
                        }}>
                            <span style={{ fontWeight: 600, color: colors.text }}>Posture: {posture}</span>
                            {e.counterfactual && (
                                <span style={{ marginLeft: '12px', fontStyle: 'italic', color: colors.textMuted }}>
                                    {e.counterfactual}
                                </span>
                            )}
                        </div>
                    );
                })()}
            </div>

            {/* ═══ CAPITAL FLOW ANALYSIS (the good stuff) ═══ */}
            <div style={{ marginBottom: '12px' }}>
                <CapitalFlowAnalysis />
            </div>

            {/* ═══ LATEST BRIEFING — actionable summary ═══ */}
            {latestBriefing?.content && (() => {
                // Extract the actionable parts from the briefing
                const content = latestBriefing.content || '';
                // Find "Bottom Line" or "What's Happening" section
                const bottomLineMatch = content.match(/## (?:Bottom Line|What'?s Happening[^\n]*)\n+([\s\S]*?)(?=\n## |\n---|\n\*Generated|$)/i);
                const actionMatch = content.match(/## (?:Action|Tomorrow|Playbook|Opportunities)[^\n]*\n+([\s\S]*?)(?=\n## |\n---|\n\*Generated|$)/i);
                const regimeMatch = content.match(/## Regime[^\n]*\n+([\s\S]*?)(?=\n## |\n---|\n\*Generated|$)/i);

                const bottomLine = bottomLineMatch ? bottomLineMatch[1].trim().replace(/\*\*/g, '').substring(0, 300) : null;
                const action = actionMatch ? actionMatch[1].trim().replace(/\*\*/g, '').substring(0, 200) : null;
                const regime = regimeMatch ? regimeMatch[1].trim().replace(/\*\*/g, '').substring(0, 150) : null;

                // Fallback: just show first meaningful paragraph
                const fallback = content.replace(/^#[^\n]*\n+/gm, '').trim().substring(0, 300);

                return (
                    <div
                        style={{
                            ...shared.card, cursor: 'pointer',
                            borderLeft: `3px solid ${colors.accent}`,
                        }}
                        onClick={() => onNavigate('briefings')}
                    >
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                            <span style={{ ...shared.sectionTitle, marginBottom: 0 }}>
                                AI BRIEFING
                            </span>
                            <span style={{ fontSize: '10px', color: colors.textMuted }}>
                                {latestBriefing.generated_at ? _timeAgo(latestBriefing.generated_at) : ''} · Full ›
                            </span>
                        </div>

                        {/* Bottom line — the key insight */}
                        <div style={{ fontSize: '13px', lineHeight: '1.6', color: colors.text, fontWeight: 500, marginBottom: bottomLine ? '10px' : 0 }}>
                            {bottomLine || fallback}
                        </div>

                        {/* Regime one-liner */}
                        {regime && (
                            <div style={{
                                fontSize: '11px', lineHeight: '1.5', color: colors.textDim,
                                padding: '8px 10px', background: colors.bg, borderRadius: '6px',
                                marginBottom: action ? '8px' : 0,
                            }}>
                                {regime}
                            </div>
                        )}

                        {/* Action item */}
                        {action && (
                            <div style={{
                                fontSize: '11px', lineHeight: '1.5', color: colors.accent,
                                padding: '8px 10px', borderRadius: '6px',
                                background: `${colors.accent}10`,
                                border: `1px solid ${colors.accent}30`,
                            }}>
                                {action}
                            </div>
                        )}
                    </div>
                );
            })()}

            {/* ═══ WATCHLIST — enriched with context ═══ */}
            <div style={{ marginBottom: '12px' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '8px' }}>
                    <span style={shared.sectionTitle}>WATCHLIST</span>
                    <button onClick={() => setAskOpen(true)} style={{
                        background: 'none', border: `1px solid ${colors.border}`, borderRadius: '6px',
                        padding: '4px 10px', fontSize: '10px', color: colors.accent, cursor: 'pointer',
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>Ask GRID</button>
                </div>
                <div style={{ display: 'flex', gap: '8px', marginBottom: '8px' }}>
                    <input type="text" value={newTicker}
                        onChange={(e) => setNewTicker(e.target.value)}
                        onKeyDown={(e) => e.key === 'Enter' && handleAddTicker()}
                        placeholder="Add ticker"
                        style={{ ...shared.input, flex: 1, padding: '8px 12px', fontSize: '13px' }}
                    />
                    <button onClick={handleAddTicker} disabled={addingTicker || !newTicker.trim()}
                        style={{ ...shared.buttonSmall, ...(addingTicker || !newTicker.trim() ? shared.buttonDisabled : {}) }}>
                        {addingTicker ? '...' : 'Add'}
                    </button>
                </div>
                {enrichedWatchlist.length > 0 ? (
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                        {enrichedWatchlist.map((item) => {
                            const pc = flowColor(item.pct_1m);
                            const sectorColor = item.sector === 'Technology' ? '#3B82F6' :
                                item.sector === 'Energy' ? '#22C55E' :
                                item.sector === 'Financials' ? '#F59E0B' :
                                item.sector === 'Healthcare' ? '#EC4899' : colors.accent;
                            return (
                                <div key={item.id || item.ticker} onClick={() => onNavigate('watchlist-analysis', item.ticker)}
                                    style={{
                                        ...shared.card, borderLeft: `3px solid ${sectorColor}`,
                                        cursor: 'pointer',
                                    }}>
                                    {/* Top row: ticker + price + change */}
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                                        <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                            <span style={{ fontSize: '14px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                                                {item.ticker}
                                            </span>
                                            {item.price != null && (
                                                <span style={{ fontSize: '12px', color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                                    ${typeof item.price === 'number' ? item.price.toLocaleString(undefined, { maximumFractionDigits: 2 }) : item.price}
                                                </span>
                                            )}
                                            {item.pct_1m != null && (
                                                <span style={{ fontSize: '11px', fontWeight: 600, color: pc, fontFamily: "'JetBrains Mono', monospace" }}>
                                                    {item.pct_1m >= 0 ? '+' : ''}{(item.pct_1m * 100).toFixed(1)}%
                                                </span>
                                            )}
                                        </div>
                                        {item.options && (
                                            <span style={{
                                                fontSize: '9px', padding: '2px 6px', borderRadius: '3px',
                                                background: item.options.pcr > 1.2 ? colors.redBg : item.options.pcr < 0.7 ? colors.greenBg : colors.bg,
                                                color: item.options.pcr > 1.2 ? colors.red : item.options.pcr < 0.7 ? colors.green : colors.textMuted,
                                                fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
                                            }}>P/C {item.options.pcr?.toFixed(2)}</span>
                                        )}
                                    </div>
                                    {/* Insight line — the "why this matters" */}
                                    <div style={{ fontSize: '11px', color: colors.textDim, lineHeight: '1.4' }}>
                                        {item.insight}
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                ) : watchlist.length > 0 ? (
                    <div style={{ color: colors.textMuted, fontSize: '11px', textAlign: 'center', padding: '12px' }}>
                        Loading enriched data...
                    </div>
                ) : (
                    <div style={{ color: colors.textMuted, fontSize: '12px', textAlign: 'center', padding: '12px' }}>
                        Add tickers to track
                    </div>
                )}
                {/* Auto-suggestions */}
                {suggestions.length > 0 && (
                    <div style={{ marginTop: '8px', padding: '8px 0' }}>
                        <div style={{ fontSize: '9px', color: colors.textMuted, letterSpacing: '1px', marginBottom: '4px', fontFamily: "'JetBrains Mono', monospace" }}>
                            SUGGESTED
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                            {suggestions.map(s => (
                                <button key={s.ticker} onClick={async (e) => {
                                    e.stopPropagation();
                                    try { await api.addToWatchlist({ ticker: s.ticker }); loadData(); }
                                    catch (err) { addNotification('error', err.message); }
                                }} style={{
                                    background: 'transparent', border: `1px dashed ${colors.border}`,
                                    borderRadius: '4px', padding: '3px 8px', fontSize: '10px',
                                    color: colors.textMuted, cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                }}>+ {s.ticker}</button>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            {/* Agent Progress */}
            {agentProgress && (
                <div style={{ ...shared.card, borderColor: colors.accent, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                    <div>
                        <span style={{ fontSize: '12px', color: colors.accent, fontWeight: 600 }}>AGENT RUNNING</span>
                        <div style={{ fontSize: '12px', color: colors.textMuted, marginTop: '2px' }}>{agentProgress.stage}: {agentProgress.detail}</div>
                    </div>
                    <span style={shared.badge('#1A6EBF')}>{agentProgress.ticker}</span>
                </div>
            )}

            {/* Ask GRID modal */}
            {askOpen && (
                <div style={{ position: 'fixed', inset: 0, zIndex: 1000, background: 'rgba(0,0,0,0.6)', backdropFilter: 'blur(4px)', display: 'flex', alignItems: 'flex-end', justifyContent: 'center' }} onClick={() => setAskOpen(false)}>
                    <div style={{ width: '100%', maxWidth: '600px', background: colors.bg, borderTop: `2px solid ${colors.accent}`, borderRadius: '16px 16px 0 0', padding: '20px', paddingBottom: 'calc(20px + env(safe-area-inset-bottom, 0px))' }} onClick={e => e.stopPropagation()}>
                        <div style={{ fontSize: '14px', fontWeight: 700, color: colors.text, marginBottom: '12px' }}>Ask GRID</div>
                        <textarea value={askQuery} onChange={e => setAskQuery(e.target.value)}
                            placeholder="e.g., What's the current risk to equities from credit spreads?"
                            style={{ ...shared.textarea, minHeight: '80px', marginBottom: '10px' }} />
                        {askResult && <div style={{ ...shared.prose, marginBottom: '10px', maxHeight: '300px' }}>{askResult}</div>}
                        <div style={{ display: 'flex', gap: '8px' }}>
                            <button onClick={async () => {
                                if (!askQuery.trim()) return;
                                setAskResult('Thinking...');
                                try { const r = await api.askOllama(askQuery); setAskResult(r?.response || r?.content || 'No response'); }
                                catch (err) { setAskResult('Error: ' + (err.message || 'failed')); }
                            }} style={{ ...shared.button, flex: 1 }}>Ask</button>
                            <button onClick={() => { setAskOpen(false); setAskResult(null); }} style={{ ...shared.buttonSmall, background: colors.card }}>Close</button>
                        </div>
                    </div>
                </div>
            )}

            {/* Toast */}
            {actionResult && (
                <div style={{
                    position: 'fixed', bottom: '80px', left: '16px', right: '16px',
                    background: colors.card, border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.md, padding: '12px 16px', zIndex: 500, boxShadow: colors.shadow.md,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                }}>
                    <span style={{ fontSize: '12px', color: colors.text }}>{actionResult}</span>
                    <button onClick={() => setActionResult(null)} style={{ background: 'none', border: 'none', color: colors.textMuted, cursor: 'pointer', fontSize: '16px' }}>×</button>
                </div>
            )}

            <div style={{ height: '80px' }} />
            <WidgetManager prefs={widgetPrefs} onPrefsChange={setWidgetPrefs} open={widgetPanelOpen} onClose={() => setWidgetPanelOpen(false)} />
        </div>
    );
}
