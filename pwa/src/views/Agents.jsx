import React, { useState, useEffect } from 'react';
import { api } from '../api.js';

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
    buttonDisabled: {
        background: '#1A2840', color: '#5A7080', cursor: 'not-allowed',
    },
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

    useEffect(() => {
        api.getAgentStatus().then(setStatus).catch(() => {});
        api.getAgentRuns().then(setRuns).catch(() => {});
    }, []);

    const triggerRun = async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.triggerAgentRun({
                ticker: ticker || undefined,
            });
            if (result.error) setError(result.error);
            // Refresh runs list
            const updated = await api.getAgentRuns();
            setRuns(updated);
        } catch (e) {
            setError(e.message || 'Run failed');
        }
        setLoading(false);
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

    return (
        <div style={styles.container}>
            <div style={styles.header}>Trading Agents</div>

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
