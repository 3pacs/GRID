import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import useStore from '../store.js';
import ViewHelp from '../components/ViewHelp.jsx';

const styles = {
    container: { padding: '16px', maxWidth: '800px', margin: '0 auto' },
    header: {
        fontSize: '22px', fontWeight: 600, color: '#E8F0F8',
        marginBottom: '16px', fontFamily: "'IBM Plex Sans', sans-serif",
    },
    card: {
        background: '#0D1520', border: '1px solid #1A2840', borderRadius: '12px',
        padding: '16px', marginBottom: '12px',
    },
    statusRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '12px',
    },
    badge: (color) => ({
        display: 'inline-block', padding: '2px 10px', borderRadius: '6px',
        fontSize: '12px', fontWeight: 600, background: color, color: '#fff',
    }),
    button: {
        background: '#1A6EBF', color: '#fff', border: 'none', borderRadius: '8px',
        padding: '10px 20px', fontSize: '14px', fontWeight: 600, cursor: 'pointer',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    buttonSmall: {
        background: '#1A6EBF', color: '#fff', border: 'none', borderRadius: '6px',
        padding: '6px 14px', fontSize: '12px', fontWeight: 600, cursor: 'pointer',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    buttonDanger: { background: '#8B1F1F' },
    buttonDisabled: { background: '#1A2840', color: '#5A7080', cursor: 'not-allowed' },
    input: {
        background: '#080C10', border: '1px solid #1A2840', borderRadius: '6px',
        color: '#C8D8E8', padding: '8px 12px', fontSize: '14px', width: '100px',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    label: { fontSize: '12px', color: '#5A7080', marginBottom: '4px', display: 'block' },
    value: { fontSize: '14px', color: '#C8D8E8', fontFamily: "'IBM Plex Mono', monospace" },
    sectionTitle: {
        fontSize: '14px', fontWeight: 600, color: '#8AA0B8',
        marginTop: '16px', marginBottom: '8px',
    },
    runRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        padding: '10px 0', borderBottom: '1px solid #1A2840', cursor: 'pointer',
    },
    decision: (d) => ({
        fontWeight: 700, fontSize: '13px',
        color: d === 'BUY' ? '#22C55E' : d === 'SELL' ? '#EF4444' : '#8AA0B8',
    }),
    detail: {
        background: '#080C10', borderRadius: '8px', padding: '12px',
        marginTop: '8px', fontSize: '13px', color: '#8AA0B8',
        whiteSpace: 'pre-wrap', maxHeight: '300px', overflowY: 'auto',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    error: { color: '#EF4444', fontSize: '13px', marginTop: '8px' },
    progressBar: {
        height: '4px', borderRadius: '2px', background: '#1A2840',
        overflow: 'hidden', marginTop: '8px',
    },
    progressFill: (pct) => ({
        height: '100%', width: `${pct * 100}%`, background: '#1A6EBF',
        borderRadius: '2px', transition: 'width 0.5s ease',
    }),
    metricGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))',
        gap: '12px', marginTop: '8px',
    },
    metric: {
        background: '#080C10', borderRadius: '8px', padding: '12px', textAlign: 'center',
    },
    metricValue: {
        fontSize: '20px', fontWeight: 700, color: '#E8F0F8',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    metricLabel: { fontSize: '11px', color: '#5A7080', marginTop: '4px' },
};

const decisionColor = (d) =>
    d === 'BUY' ? '#0D3320' : d === 'SELL' ? '#3B1111' : '#1A2840';

export default function Agents() {
    const [status, setStatus] = useState(null);
    const [runs, setRuns] = useState([]);
    const [ticker, setTicker] = useState('');
    const [loading, setLoading] = useState(false);
    const [expanded, setExpanded] = useState(null);
    const [detail, setDetail] = useState(null);
    const [error, setError] = useState(null);
    const [backtest, setBacktest] = useState(null);
    const [btLoading, setBtLoading] = useState(false);

    const agentProgress = useStore(s => s.agentProgress);
    const agentLastComplete = useStore(s => s.agentLastComplete);

    const addNotification = useStore(s => s.addNotification);

    useEffect(() => {
        api.getAgentStatus().then(setStatus).catch(() => addNotification('error', 'Failed to load agent status'));
        api.getAgentRuns().then(setRuns).catch(() => addNotification('error', 'Failed to load agent runs'));
        api.getBacktestSummary().then(setBacktest).catch(() => addNotification('error', 'Failed to load backtest summary'));
    }, []);

    // Refresh runs when a run completes via WebSocket
    useEffect(() => {
        if (agentLastComplete) {
            api.getAgentRuns().then(setRuns).catch(() => {});
            setLoading(false);
        }
    }, [agentLastComplete]);

    const triggerRun = async () => {
        setLoading(true);
        setError(null);
        try {
            await api.triggerAgentRun({ ticker: ticker || undefined });
        } catch (e) {
            setError(e.message || 'Run failed');
            setLoading(false);
        }
    };

    const runBacktest = async () => {
        setBtLoading(true);
        try {
            const result = await api.runAgentBacktest({ days_back: 90 });
            setBacktest(result);
        } catch (e) {
            addNotification('error', e.message || 'Backtest failed');
            setBacktest(null);
        }
        setBtLoading(false);
    };

    const toggleSchedule = async () => {
        try {
            if (status?.schedule?.running) {
                await api.stopAgentSchedule();
            } else {
                await api.startAgentSchedule();
            }
            const updated = await api.getAgentStatus();
            setStatus(updated);
        } catch (e) {
            addNotification('error', e.message || 'Schedule toggle failed');
        }
    };

    const toggleExpand = async (id) => {
        if (expanded === id) {
            setExpanded(null);
            setDetail(null);
            return;
        }
        setExpanded(id);
        try {
            const d = await api.getAgentRun(id);
            setDetail(d);
        } catch {
            setDetail(null);
        }
    };

    const enabled = status?.enabled;
    const scheduleRunning = status?.schedule?.running;

    return (
        <div style={styles.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={styles.header}>Trading Agents</div>
                <ViewHelp id="agents" />
            </div>

            {/* Status Card */}
            <div style={styles.card}>
                <div style={styles.statusRow}>
                    <span style={styles.label}>Status</span>
                    <span style={styles.badge(enabled ? '#1A7A4A' : '#5A3A00')}>
                        {enabled ? 'ENABLED' : 'DISABLED'}
                    </span>
                </div>
                {status && (
                    <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                        <div>
                            <span style={styles.label}>LLM Provider</span>
                            <div style={styles.value}>{status.llm_provider}</div>
                        </div>
                        <div>
                            <span style={styles.label}>Model</span>
                            <div style={styles.value}>{status.llm_model}</div>
                        </div>
                        <div>
                            <span style={styles.label}>Debate Rounds</span>
                            <div style={styles.value}>{status.debate_rounds}</div>
                        </div>
                        <div>
                            <span style={styles.label}>Package</span>
                            <div style={styles.value}>
                                {status.tradingagents_installed ? 'installed' : 'not installed'}
                            </div>
                        </div>
                    </div>
                )}
            </div>

            {/* Live Progress */}
            {(agentProgress || loading) && (
                <div style={{ ...styles.card, borderColor: '#1A6EBF' }}>
                    <div style={styles.statusRow}>
                        <span style={{ fontSize: '13px', color: '#E8F0F8', fontWeight: 600 }}>
                            {agentProgress
                                ? `${agentProgress.stage}: ${agentProgress.detail}`
                                : 'Starting agent run...'}
                        </span>
                        <span style={styles.badge('#1A6EBF')}>RUNNING</span>
                    </div>
                    <div style={styles.progressBar}>
                        <div style={styles.progressFill(agentProgress?.progress_pct || 0.05)} />
                    </div>
                    {agentProgress?.ticker && (
                        <div style={{ fontSize: '11px', color: '#5A7080', marginTop: '6px' }}>
                            Ticker: {agentProgress.ticker}
                        </div>
                    )}
                </div>
            )}

            {/* Trigger Run */}
            <div style={styles.card}>
                <div style={{ display: 'flex', gap: '12px', alignItems: 'flex-end' }}>
                    <div>
                        <span style={styles.label}>Ticker</span>
                        <input
                            style={styles.input}
                            value={ticker}
                            onChange={(e) => setTicker(e.target.value.toUpperCase())}
                            placeholder={status?.default_ticker || 'SPY'}
                        />
                    </div>
                    <button
                        style={{
                            ...styles.button,
                            ...((!enabled || loading) ? styles.buttonDisabled : {}),
                        }}
                        onClick={triggerRun}
                        disabled={!enabled || loading}
                    >
                        {loading ? 'Running...' : 'Run Agents'}
                    </button>
                </div>
                {error && <div style={styles.error}>{error}</div>}
            </div>

            {/* Schedule Card */}
            <div style={styles.card}>
                <div style={styles.statusRow}>
                    <span style={{ fontSize: '14px', fontWeight: 600, color: '#8AA0B8' }}>
                        Schedule
                    </span>
                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                        <span style={styles.badge(scheduleRunning ? '#1A7A4A' : '#1A2840')}>
                            {scheduleRunning ? 'ACTIVE' : 'STOPPED'}
                        </span>
                        <button
                            style={{
                                ...styles.buttonSmall,
                                ...(scheduleRunning ? styles.buttonDanger : {}),
                                ...(!enabled ? styles.buttonDisabled : {}),
                            }}
                            onClick={toggleSchedule}
                            disabled={!enabled}
                        >
                            {scheduleRunning ? 'Stop' : 'Start'}
                        </button>
                    </div>
                </div>
                {status?.schedule && (
                    <div style={{ display: 'flex', gap: '24px', flexWrap: 'wrap' }}>
                        <div>
                            <span style={styles.label}>Cron</span>
                            <div style={styles.value}>{status.schedule.cron || 'N/A'}</div>
                        </div>
                        <div>
                            <span style={styles.label}>Next Run</span>
                            <div style={styles.value}>
                                {status.schedule.next_run
                                    ? new Date(status.schedule.next_run).toLocaleString()
                                    : 'N/A'}
                            </div>
                        </div>
                        <div>
                            <span style={styles.label}>Scheduled Jobs</span>
                            <div style={styles.value}>{status.schedule.scheduled_jobs || 0}</div>
                        </div>
                    </div>
                )}
            </div>

            {/* Backtest Card */}
            <div style={styles.card}>
                <div style={styles.statusRow}>
                    <span style={{ fontSize: '14px', fontWeight: 600, color: '#8AA0B8' }}>
                        Agent vs GRID Backtest
                    </span>
                    <button
                        style={{ ...styles.buttonSmall, ...(btLoading ? styles.buttonDisabled : {}) }}
                        onClick={runBacktest}
                        disabled={btLoading}
                    >
                        {btLoading ? 'Running...' : 'Run Backtest'}
                    </button>
                </div>
                {backtest?.has_data === false && (
                    <div style={{ color: '#5A7080', fontSize: '13px' }}>
                        {backtest.message || 'No data yet — run some agents first'}
                    </div>
                )}
                {backtest?.metrics && (
                    <div style={styles.metricGrid}>
                        <div style={styles.metric}>
                            <div style={styles.metricValue}>{backtest.total_runs || backtest.total_compared || 0}</div>
                            <div style={styles.metricLabel}>Total Runs</div>
                        </div>
                        <div style={styles.metric}>
                            <div style={styles.metricValue}>
                                {((backtest.metrics.agreement_rate || 0) * 100).toFixed(0)}%
                            </div>
                            <div style={styles.metricLabel}>Agreement</div>
                        </div>
                        <div style={styles.metric}>
                            <div style={{ ...styles.metricValue, color: '#22C55E' }}>
                                {backtest.metrics.agent_helped || 0}
                            </div>
                            <div style={styles.metricLabel}>Helped</div>
                        </div>
                        <div style={styles.metric}>
                            <div style={{ ...styles.metricValue, color: '#EF4444' }}>
                                {backtest.metrics.agent_harmed || 0}
                            </div>
                            <div style={styles.metricLabel}>Harmed</div>
                        </div>
                        <div style={styles.metric}>
                            <div style={styles.metricValue}>
                                {((backtest.metrics.helped_rate || 0) * 100).toFixed(0)}%
                            </div>
                            <div style={styles.metricLabel}>Win Rate</div>
                        </div>
                        <div style={styles.metric}>
                            <div style={styles.metricValue}>
                                {backtest.metrics.avg_outcome?.toFixed(4) || '0'}
                            </div>
                            <div style={styles.metricLabel}>Avg Outcome</div>
                        </div>
                    </div>
                )}
                {backtest?.by_regime && Object.keys(backtest.by_regime).length > 0 && (
                    <>
                        <div style={styles.sectionTitle}>By Regime</div>
                        <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap' }}>
                            {Object.entries(backtest.by_regime).map(([regime, data]) => (
                                <div key={regime} style={{ ...styles.metric, minWidth: '120px' }}>
                                    <div style={{ fontSize: '12px', fontWeight: 600, color: '#8AA0B8' }}>{regime}</div>
                                    <div style={{ fontSize: '11px', color: '#5A7080', marginTop: '4px' }}>
                                        {data.runs} runs
                                        {data.avg_outcome != null && ` / avg ${data.avg_outcome.toFixed(4)}`}
                                    </div>
                                </div>
                            ))}
                        </div>
                    </>
                )}
            </div>

            {/* Run History */}
            <div style={styles.sectionTitle}>Recent Runs</div>
            <div style={styles.card}>
                {runs.length === 0 && (
                    <div style={{ color: '#5A7080', fontSize: '13px' }}>
                        No agent runs yet
                    </div>
                )}
                {runs.map((run) => (
                    <div key={run.id}>
                        <div
                            style={styles.runRow}
                            onClick={() => toggleExpand(run.id)}
                        >
                            <div>
                                <span style={{ fontSize: '13px', color: '#8AA0B8' }}>
                                    {run.ticker}
                                </span>
                                <span style={{ fontSize: '11px', color: '#5A7080', marginLeft: '8px' }}>
                                    {run.as_of_date}
                                </span>
                            </div>
                            <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                                <span style={styles.badge(decisionColor(run.final_decision))}>
                                    <span style={styles.decision(run.final_decision)}>
                                        {run.final_decision}
                                    </span>
                                </span>
                                <span style={{ fontSize: '11px', color: '#5A7080' }}>
                                    {run.duration_seconds?.toFixed(1)}s
                                </span>
                            </div>
                        </div>
                        {expanded === run.id && detail && (
                            <div>
                                <div style={styles.sectionTitle}>Reasoning</div>
                                <div style={styles.detail}>
                                    {detail.decision_reasoning}
                                </div>
                                {detail.analyst_reports && Object.keys(detail.analyst_reports).length > 0 && (
                                    <>
                                        <div style={styles.sectionTitle}>Analyst Reports</div>
                                        <div style={styles.detail}>
                                            {JSON.stringify(detail.analyst_reports, null, 2)}
                                        </div>
                                    </>
                                )}
                                {detail.bull_bear_debate && Object.keys(detail.bull_bear_debate).length > 0 && (
                                    <>
                                        <div style={styles.sectionTitle}>Bull/Bear Debate</div>
                                        <div style={styles.detail}>
                                            {JSON.stringify(detail.bull_bear_debate, null, 2)}
                                        </div>
                                    </>
                                )}
                                {detail.risk_assessment && Object.keys(detail.risk_assessment).length > 0 && (
                                    <>
                                        <div style={styles.sectionTitle}>Risk Assessment</div>
                                        <div style={styles.detail}>
                                            {JSON.stringify(detail.risk_assessment, null, 2)}
                                        </div>
                                    </>
                                )}
                                <div style={{ display: 'flex', gap: '16px', marginTop: '12px', flexWrap: 'wrap' }}>
                                    <div>
                                        <span style={styles.label}>Regime</span>
                                        <div style={styles.value}>{detail.grid_regime_state}</div>
                                    </div>
                                    <div>
                                        <span style={styles.label}>Confidence</span>
                                        <div style={styles.value}>
                                            {detail.grid_confidence != null
                                                ? (detail.grid_confidence * 100).toFixed(1) + '%'
                                                : 'N/A'}
                                        </div>
                                    </div>
                                    <div>
                                        <span style={styles.label}>LLM</span>
                                        <div style={styles.value}>
                                            {detail.llm_provider} / {detail.llm_model}
                                        </div>
                                    </div>
                                </div>
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}
