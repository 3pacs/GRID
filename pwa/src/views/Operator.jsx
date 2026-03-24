import React, { useState, useEffect } from 'react';
import { api } from '../api.js';
import { colors, shared } from '../styles/shared.js';

const SEVERITY_COLORS = {
    CRITICAL: colors.red,
    ERROR: '#F87171',
    WARNING: colors.yellow,
    INFO: colors.accent,
};

const FIX_COLORS = {
    SUCCESS: colors.green,
    FAILED: colors.red,
    PENDING: colors.yellow,
    SKIPPED: colors.textMuted,
};

const SEVERITY_OPTIONS = ['ALL', 'CRITICAL', 'ERROR', 'WARNING', 'INFO'];
const CATEGORY_OPTIONS = ['ALL', 'ingestion', 'normalization', 'discovery', 'inference', 'system'];

const fmtDate = (d) => d ? d.substring(0, 19).replace('T', ' ') : '-';

export default function Operator() {
    const [status, setStatus] = useState(null);
    const [issues, setIssues] = useState([]);
    const [recentCycles, setRecentCycles] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [severityFilter, setSeverityFilter] = useState('ALL');
    const [categoryFilter, setCategoryFilter] = useState('ALL');
    const [expandedIssue, setExpandedIssue] = useState(null);
    const [daysBack, setDaysBack] = useState(30);

    useEffect(() => {
        loadAll();
    }, []);

    useEffect(() => {
        loadIssues();
    }, [severityFilter, categoryFilter, daysBack]);

    const loadAll = async () => {
        setLoading(true);
        setError(null);
        try {
            const [statusRes, issuesRes, cyclesRes] = await Promise.all([
                api._fetch('/api/v1/system/status').catch(() => null),
                api._fetch('/api/v1/snapshots/issues?days_back=30').catch(() => null),
                api._fetch('/api/v1/snapshots/latest/pipeline_summary?n=10').catch(() => null),
            ]);
            setStatus(statusRes);
            setIssues(issuesRes?.issues || issuesRes || []);
            setRecentCycles(cyclesRes?.snapshots || cyclesRes || []);
        } catch (e) {
            setError(e.message || 'Failed to load operator data');
        }
        setLoading(false);
    };

    const loadIssues = async () => {
        try {
            let url = '/api/v1/snapshots/issues?days_back=' + daysBack;
            if (categoryFilter !== 'ALL') url += '&category=' + categoryFilter;
            if (severityFilter !== 'ALL') url += '&severity=' + severityFilter;
            const res = await api._fetch(url);
            setIssues(res?.issues || res || []);
        } catch (e) {
            console.warn('[GRID] Operator:', e.message);
        }
    };

    const hermes = status?.hermes || status?.operator || {};
    const isOnline = hermes.online || hermes.status === 'running' || hermes.active || false;

    const stats = {
        pulls_retried: 0, fixes_applied: 0, hypotheses_tested: 0, errors_diagnosed: 0,
    };
    if (Array.isArray(issues)) {
        issues.forEach(iss => {
            if (iss.fix_result === 'SUCCESS') stats.fixes_applied++;
            if (iss.severity === 'ERROR' || iss.severity === 'CRITICAL') stats.errors_diagnosed++;
        });
    }
    if (hermes.stats) Object.assign(stats, hermes.stats);

    return (
        <div style={shared.container}>
            <div style={shared.header}>Operator</div>

            {error && <div style={shared.error}>{error}</div>}
            {loading && <div style={{ color: colors.textMuted, fontSize: '13px', padding: '12px' }}>Loading...</div>}

            {/* Hermes Status */}
            {status && !loading && (
                <div style={shared.card}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
                        <div style={shared.sectionTitle}>HERMES STATUS</div>
                        <span style={shared.badge(isOnline ? colors.green : colors.red)}>
                            {isOnline ? 'ONLINE' : 'OFFLINE'}
                        </span>
                    </div>
                    <div style={shared.metricGrid}>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>
                                {hermes.last_cycle_time || hermes.last_run || '-'}
                            </div>
                            <div style={shared.metricLabel}>last cycle</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={{
                                ...shared.metricValue,
                                color: (hermes.consecutive_failures || 0) > 0 ? colors.red : colors.green,
                            }}>
                                {hermes.consecutive_failures ?? 0}
                            </div>
                            <div style={shared.metricLabel}>consec. failures</div>
                        </div>
                        <div style={shared.metric}>
                            <div style={shared.metricValue}>{hermes.total_cycles ?? '-'}</div>
                            <div style={shared.metricLabel}>total cycles</div>
                        </div>
                    </div>
                </div>
            )}

            {/* Current Cycle */}
            {hermes.current_step && (
                <div style={{
                    ...shared.card,
                    borderLeft: `3px solid ${colors.accent}`,
                }}>
                    <div style={shared.sectionTitle}>CURRENT CYCLE</div>
                    <div style={{ fontSize: '14px', color: colors.text, fontFamily: colors.mono }}>
                        {hermes.current_step}
                    </div>
                    {hermes.cycle_started && (
                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '4px' }}>
                            Started: {fmtDate(hermes.cycle_started)}
                        </div>
                    )}
                </div>
            )}

            {/* Stats Summary */}
            {!loading && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>STATS SUMMARY</div>
                    <div style={shared.metricGrid}>
                        {Object.entries(stats).map(([k, v]) => (
                            <div key={k} style={shared.metric}>
                                <div style={shared.metricValue}>{v}</div>
                                <div style={shared.metricLabel}>{k.replace(/_/g, ' ')}</div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Issue Tracker */}
            {!loading && (
                <div>
                    <div style={shared.sectionTitle}>ISSUE TRACKER</div>
                    <div style={{ display: 'flex', gap: '8px', marginBottom: '8px', flexWrap: 'wrap' }}>
                        <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>Severity:</span>
                            {SEVERITY_OPTIONS.map(s => (
                                <button key={s} style={shared.tab(severityFilter === s)}
                                    onClick={() => setSeverityFilter(s)}>
                                    {s}
                                </button>
                            ))}
                        </div>
                        <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }}>
                            <span style={{ fontSize: '11px', color: colors.textMuted }}>Category:</span>
                            {CATEGORY_OPTIONS.map(c => (
                                <button key={c} style={shared.tab(categoryFilter === c)}
                                    onClick={() => setCategoryFilter(c)}>
                                    {c}
                                </button>
                            ))}
                        </div>
                    </div>

                    {issues.length === 0 && (
                        <div style={{ color: colors.textMuted, fontSize: '13px', padding: '16px', textAlign: 'center' }}>
                            No issues found
                        </div>
                    )}

                    {issues.map((iss, i) => {
                        const id = iss.id || i;
                        const expanded = expandedIssue === id;
                        const sevColor = SEVERITY_COLORS[iss.severity] || colors.textMuted;
                        const fixColor = FIX_COLORS[iss.fix_result] || colors.textMuted;
                        return (
                            <div key={id} style={{
                                ...shared.card, marginBottom: '6px', cursor: 'pointer',
                            }} onClick={() => setExpandedIssue(expanded ? null : id)}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flex: 1, minWidth: 0 }}>
                                        <span style={shared.badge(sevColor)}>{iss.severity}</span>
                                        <span style={{
                                            fontSize: '13px', color: colors.text,
                                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                        }}>
                                            {iss.title || iss.message || 'Untitled issue'}
                                        </span>
                                    </div>
                                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexShrink: 0 }}>
                                        {iss.fix_result && (
                                            <span style={shared.badge(fixColor)}>{iss.fix_result}</span>
                                        )}
                                        <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                            {fmtDate(iss.created_at || iss.timestamp)}
                                        </span>
                                    </div>
                                </div>
                                {iss.source && (
                                    <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '4px' }}>
                                        Source: {iss.source}
                                    </div>
                                )}

                                {expanded && (
                                    <div style={{ marginTop: '10px' }}>
                                        {iss.detail && (
                                            <div style={{ fontSize: '12px', color: colors.textDim, marginBottom: '8px' }}>
                                                {iss.detail}
                                            </div>
                                        )}
                                        {iss.stack_trace && (
                                            <div style={{
                                                ...shared.prose, fontSize: '11px', maxHeight: '200px', marginBottom: '8px',
                                            }}>
                                                {iss.stack_trace}
                                            </div>
                                        )}
                                        {iss.hermes_diagnosis && (
                                            <div style={{
                                                background: colors.bg, borderRadius: '8px', padding: '12px',
                                                borderLeft: `3px solid ${colors.accent}`,
                                                fontSize: '12px', color: colors.textDim, lineHeight: '1.6',
                                                fontStyle: 'italic',
                                            }}>
                                                {iss.hermes_diagnosis}
                                            </div>
                                        )}
                                    </div>
                                )}
                            </div>
                        );
                    })}
                </div>
            )}

            {/* Recent Cycles */}
            {!loading && recentCycles.length > 0 && (
                <div style={{ marginTop: '8px' }}>
                    <div style={shared.sectionTitle}>RECENT CYCLES</div>
                    {recentCycles.map((cycle, i) => {
                        const payload = cycle.payload || cycle;
                        const metrics = {};
                        if (payload && typeof payload === 'object') {
                            for (const [k, v] of Object.entries(payload)) {
                                if (typeof v === 'number') metrics[k] = v;
                            }
                        }
                        return (
                            <div key={cycle.id || i} style={{ ...shared.card, marginBottom: '6px' }}>
                                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                    <span style={{ fontSize: '12px', color: colors.textDim, fontFamily: colors.mono }}>
                                        {fmtDate(cycle.created_at || cycle.snapshot_date)}
                                    </span>
                                    <div style={{ display: 'flex', gap: '10px' }}>
                                        {Object.entries(metrics).slice(0, 4).map(([k, v]) => (
                                            <span key={k} style={{ fontSize: '11px', color: colors.textMuted }}>
                                                {k.replace(/_/g, ' ')}: <span style={{ color: colors.text }}>
                                                    {Number.isInteger(v) ? v : v.toFixed(2)}
                                                </span>
                                            </span>
                                        ))}
                                    </div>
                                </div>
                            </div>
                        );
                    })}
                </div>
            )}
        </div>
    );
}
