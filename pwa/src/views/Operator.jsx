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

    const [health, setHealth] = useState(null);
    const [freshness, setFreshness] = useState(null);

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
            const [statusRes, issuesRes, cyclesRes, healthRes, freshnessRes] = await Promise.all([
                api._fetch('/api/v1/system/status').catch(() => null),
                api._fetch('/api/v1/snapshots/issues?days_back=30').catch(() => null),
                api._fetch('/api/v1/snapshots/latest/pipeline_summary?n=10').catch(() => null),
                api._fetch('/api/v1/system/health').catch(() => null),
                api._fetch('/api/v1/system/freshness').catch(() => null),
            ]);
            setStatus(statusRes);
            setIssues(issuesRes?.issues || issuesRes || []);
            setRecentCycles(cyclesRes?.snapshots || cyclesRes || []);
            setHealth(healthRes);
            setFreshness(freshnessRes);
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

            {/* Subsystem Health */}
            {health && !loading && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>SUBSYSTEM HEALTH</div>
                    <div style={{
                        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
                        gap: '8px', marginTop: '8px',
                    }}>
                        {[
                            { key: 'database', label: 'Database', ok: health.checks?.database },
                            { key: 'recent_data', label: 'Data Fresh', ok: health.checks?.recent_data },
                            { key: 'llm_available', label: 'LLM', ok: health.checks?.llm_available },
                            { key: 'features_registered', label: 'Features', ok: health.checks?.features_registered },
                            { key: 'pool_healthy', label: 'DB Pool', ok: health.checks?.pool_healthy },
                            { key: 'thread_ingestion', label: 'Ingestion', ok: health.checks?.thread_ingestion },
                        ].map(sub => (
                            <div key={sub.key} style={{
                                display: 'flex', alignItems: 'center', gap: '6px',
                                padding: '8px 10px', borderRadius: '6px',
                                background: colors.bg,
                                border: `1px solid ${sub.ok ? colors.green + '30' : colors.red + '30'}`,
                            }}>
                                <div style={{
                                    width: '8px', height: '8px', borderRadius: '50%',
                                    background: sub.ok ? colors.green : colors.red,
                                    boxShadow: sub.ok ? `0 0 4px ${colors.green}` : `0 0 4px ${colors.red}`,
                                }} />
                                <span style={{ fontSize: '11px', color: colors.text }}>{sub.label}</span>
                            </div>
                        ))}
                    </div>
                    {health.checks && (
                        <div style={{
                            display: 'flex', gap: '16px', marginTop: '10px', flexWrap: 'wrap',
                            fontSize: '11px', color: colors.textMuted,
                        }}>
                            <span>Disk: <span style={{
                                color: health.checks.disk_percent > 85 ? colors.red : health.checks.disk_percent > 70 ? colors.yellow : colors.green,
                                fontWeight: 600, fontFamily: "'JetBrains Mono', monospace",
                            }}>{health.checks.disk_percent}%</span> ({health.checks.disk_free_gb}GB free)</span>
                            <span>API Keys: <span style={{
                                color: health.checks.api_keys_configured < health.checks.api_keys_total ? colors.yellow : colors.green,
                                fontWeight: 600,
                            }}>{health.checks.api_keys_configured}/{health.checks.api_keys_total}</span></span>
                            {health.checks.ws_clients != null && (
                                <span>WS Clients: <span style={{ fontWeight: 600 }}>{health.checks.ws_clients}</span></span>
                            )}
                        </div>
                    )}
                    {health.degraded_reasons?.length > 0 && (
                        <div style={{
                            marginTop: '8px', padding: '8px 10px', borderRadius: '6px',
                            background: colors.redBg || `${colors.red}10`,
                            border: `1px solid ${colors.red}30`,
                            fontSize: '11px', color: colors.red,
                        }}>
                            {health.degraded_reasons.join(' · ')}
                        </div>
                    )}
                </div>
            )}

            {/* Data Freshness */}
            {freshness && !loading && freshness.families && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>DATA PIPELINE FRESHNESS</div>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', marginTop: '8px' }}>
                        {(freshness.families || []).slice(0, 15).map(fam => {
                            const pct = fam.total > 0 ? fam.fresh_today / fam.total : 0;
                            const barColor = pct >= 0.8 ? colors.green : pct >= 0.5 ? colors.yellow : colors.red;
                            const statusLabel = pct >= 0.8 ? 'GREEN' : pct >= 0.5 ? 'YELLOW' : 'RED';
                            return (
                                <div key={fam.family} style={{
                                    display: 'flex', alignItems: 'center', gap: '8px',
                                    padding: '4px 0',
                                }}>
                                    <span style={{
                                        width: '120px', fontSize: '11px', color: colors.text,
                                        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                    }}>{fam.family}</span>
                                    <div style={{
                                        flex: 1, height: '6px', background: colors.bg,
                                        borderRadius: '3px', overflow: 'hidden',
                                    }}>
                                        <div style={{
                                            width: `${pct * 100}%`, height: '100%',
                                            background: barColor, borderRadius: '3px',
                                        }} />
                                    </div>
                                    <span style={{
                                        fontSize: '10px', fontWeight: 600, color: barColor,
                                        fontFamily: "'JetBrains Mono', monospace", width: '50px', textAlign: 'right',
                                    }}>{statusLabel}</span>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* Server Resources */}
            {status?.server && !loading && (
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>SERVER RESOURCES</div>
                    <div style={shared.metricGrid}>
                        {status.server.cpu_percent != null && (
                            <div style={shared.metric}>
                                <div style={{
                                    ...shared.metricValue,
                                    color: status.server.cpu_percent > 80 ? colors.red : status.server.cpu_percent > 60 ? colors.yellow : colors.green,
                                }}>{status.server.cpu_percent}%</div>
                                <div style={shared.metricLabel}>CPU</div>
                            </div>
                        )}
                        {status.server.memory_percent != null && (
                            <div style={shared.metric}>
                                <div style={{
                                    ...shared.metricValue,
                                    color: status.server.memory_percent > 85 ? colors.red : status.server.memory_percent > 70 ? colors.yellow : colors.green,
                                }}>{status.server.memory_percent}%</div>
                                <div style={shared.metricLabel}>RAM ({status.server.memory_used_gb}/{status.server.memory_total_gb}GB)</div>
                            </div>
                        )}
                        {status.server.gpu_temp_c != null && (
                            <div style={shared.metric}>
                                <div style={{
                                    ...shared.metricValue,
                                    color: status.server.gpu_temp_c > 85 ? colors.red : status.server.gpu_temp_c > 70 ? colors.yellow : colors.green,
                                }}>{status.server.gpu_temp_c}°C</div>
                                <div style={shared.metricLabel}>GPU Temp</div>
                            </div>
                        )}
                        {status.server.load_avg_1m != null && (
                            <div style={shared.metric}>
                                <div style={shared.metricValue}>{status.server.load_avg_1m}</div>
                                <div style={shared.metricLabel}>Load (1m)</div>
                            </div>
                        )}
                    </div>
                    {status.database && (
                        <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '8px' }}>
                            DB: {status.database.connected ? 'Connected' : 'Disconnected'}
                            {status.database.size_mb && ` · ${(status.database.size_mb / 1024).toFixed(1)}GB`}
                        </div>
                    )}
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
                                        <span title={iss.title || iss.message || 'Untitled issue'} style={{
                                            fontSize: '13px', color: colors.text,
                                            overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
                                            lineHeight: '1.5',
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
