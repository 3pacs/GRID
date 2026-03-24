/**
 * MobileDashboard - iOS-optimized granular dashboard for GRID PWA.
 * Shows more data than desktop, organized into swipeable sections:
 * Status Grid, Source Freshness, Recent Activity, Quick Actions.
 */
import React, { useEffect, useState, useCallback } from 'react';
import { api } from '../../api.js';
import { colors } from '../../styles/shared.js';

/* ---------- freshness helpers ---------- */
function freshnessColor(lastPull) {
    if (!lastPull) return colors.textMuted;
    const hoursAgo = (Date.now() - new Date(lastPull).getTime()) / 3600000;
    if (hoursAgo < 12) return colors.green;
    if (hoursAgo < 24) return colors.yellow;
    return colors.red;
}

function freshnessLabel(lastPull) {
    if (!lastPull) return 'never';
    const hoursAgo = (Date.now() - new Date(lastPull).getTime()) / 3600000;
    if (hoursAgo < 1) return `${Math.round(hoursAgo * 60)}m ago`;
    if (hoursAgo < 24) return `${Math.round(hoursAgo)}h ago`;
    return `${Math.round(hoursAgo / 24)}d ago`;
}

function timeAgo(ts) {
    if (!ts) return '';
    const sec = (Date.now() - new Date(ts).getTime()) / 1000;
    if (sec < 60) return `${Math.round(sec)}s ago`;
    if (sec < 3600) return `${Math.round(sec / 60)}m ago`;
    if (sec < 86400) return `${Math.round(sec / 3600)}h ago`;
    return `${Math.round(sec / 86400)}d ago`;
}

/* ---------- event icons ---------- */
const EVENT_ICONS = {
    pull: '\u2193',       // down arrow
    regime: '\u25C6',     // diamond
    journal: '\u270E',    // pencil
    issue: '\u26A0',      // warning
    default: '\u2022',    // bullet
};

/* ---------- styles ---------- */
const s = {
    container: {
        padding: '10px',
        paddingBottom: 'calc(env(safe-area-inset-bottom, 0px) + 10px)',
    },
    sectionLabel: {
        fontSize: '10px',
        fontWeight: 700,
        color: colors.textMuted,
        letterSpacing: '1.5px',
        fontFamily: "'JetBrains Mono', monospace",
        marginTop: '14px',
        marginBottom: '6px',
        textTransform: 'uppercase',
    },
    statusGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(2, 1fr)',
        gap: '6px',
    },
    metricCard: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '8px',
        padding: '10px',
        minHeight: '48px',
    },
    metricTop: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
    },
    metricValue: {
        fontSize: '16px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: "'IBM Plex Mono', monospace",
    },
    metricLabel: {
        fontSize: '10px',
        color: colors.textMuted,
        marginTop: '2px',
    },
    metricSub: {
        fontSize: '10px',
        color: colors.textDim,
        fontFamily: "'IBM Plex Mono', monospace",
    },
    sourceRow: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '8px 10px',
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '6px',
        marginBottom: '4px',
        minHeight: '40px',
    },
    sourceName: {
        fontSize: '11px',
        fontWeight: 600,
        color: colors.text,
    },
    sourceTime: (color) => ({
        fontSize: '10px',
        fontWeight: 600,
        color: color,
        fontFamily: "'IBM Plex Mono', monospace",
    }),
    eventRow: {
        display: 'flex',
        alignItems: 'flex-start',
        gap: '8px',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.border}`,
    },
    eventIcon: (color) => ({
        fontSize: '12px',
        color: color,
        flexShrink: 0,
        width: '16px',
        textAlign: 'center',
        marginTop: '1px',
    }),
    eventText: {
        fontSize: '11px',
        color: colors.text,
        lineHeight: '1.4',
        flex: 1,
    },
    eventTime: {
        fontSize: '9px',
        color: colors.textMuted,
        fontFamily: "'IBM Plex Mono', monospace",
        flexShrink: 0,
    },
    actionGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(2, 1fr)',
        gap: '6px',
    },
    actionBtn: (accentColor) => ({
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '8px',
        borderTop: `2px solid ${accentColor}`,
        padding: '12px 10px',
        cursor: 'pointer',
        textAlign: 'left',
        minHeight: '48px',
    }),
    actionLabel: {
        fontSize: '12px',
        fontWeight: 600,
        color: colors.text,
    },
    actionDesc: {
        fontSize: '9px',
        color: colors.textMuted,
        marginTop: '2px',
    },
    dot: (color) => ({
        display: 'inline-block',
        width: 6,
        height: 6,
        borderRadius: '50%',
        background: color,
        marginRight: 4,
    }),
    badge: (bg, fg) => ({
        display: 'inline-block',
        padding: '1px 6px',
        borderRadius: '4px',
        fontSize: '9px',
        fontWeight: 700,
        background: bg,
        color: fg,
        fontFamily: "'IBM Plex Mono', monospace",
    }),
    loading: {
        textAlign: 'center',
        padding: '40px 0',
        color: colors.textMuted,
        fontSize: '12px',
    },
    error: {
        textAlign: 'center',
        padding: '20px 0',
        color: colors.red,
        fontSize: '11px',
    },
};

/* ---------- quick actions ---------- */
const QUICK_ACTIONS = [
    { id: 'pipeline',  label: 'Run Pipeline',     desc: 'Full data pipeline',     color: colors.green,   action: 'pipeline' },
    { id: 'scan',      label: 'Trigger Scan',     desc: 'Options mispricing',     color: '#EC4899',      action: 'scan' },
    { id: 'briefing',  label: 'Gen Briefing',     desc: 'Market analysis',        color: '#8B5CF6',      action: 'briefing' },
    { id: 'backtest',  label: 'Run Backtest',     desc: 'Walk-forward test',      color: colors.yellow,  action: 'backtest' },
    { id: 'cluster',   label: 'Clustering',       desc: 'Regime discovery',       color: colors.accent,  action: 'cluster' },
    { id: 'ortho',     label: 'Orthogonality',    desc: 'Feature audit',          color: colors.red,     action: 'ortho' },
];

export default function MobileDashboard({ subTab, onNavigate }) {
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    /* data state */
    const [status, setStatus] = useState(null);
    const [regime, setRegime] = useState(null);
    const [sources, setSources] = useState([]);
    const [journal, setJournal] = useState([]);
    const [issues, setIssues] = useState([]);
    const [snapshots, setSnapshots] = useState([]);

    /* load all data */
    const loadData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const [sysStatus, regimeCurrent, srcList, journalData, issueData, snapData] =
                await Promise.all([
                    api._fetch('/api/v1/system/status').catch(() => null),
                    api._fetch('/api/v1/regime/current').catch(() => null),
                    api._fetch('/api/v1/config/sources').catch(() => ({ sources: [] })),
                    api._fetch('/api/v1/journal?limit=5').catch(() => ({ entries: [] })),
                    api._fetch('/api/v1/snapshots/issues?days_back=7&severity=ERROR').catch(() => ({ issues: [] })),
                    api._fetch('/api/v1/snapshots/latest/pipeline_summary?n=5').catch(() => ({ snapshots: [] })),
                ]);
            setStatus(sysStatus);
            setRegime(regimeCurrent);
            setSources(srcList?.sources || srcList || []);
            setJournal(journalData?.entries || []);
            setIssues(issueData?.issues || []);
            setSnapshots(snapData?.snapshots || snapData || []);
        } catch (err) {
            setError(err.message || 'Failed to load dashboard data');
        }
        setLoading(false);
    }, []);

    useEffect(() => { loadData(); }, [loadData]);

    /* derived metrics */
    const grid = status?.grid || {};
    const db = status?.database || {};
    const hs = status?.hyperspace || {};
    const pipeline = status?.pipeline || {};

    const regimeState = regime?.state || regime?.regime || '--';
    const regimeConf = regime?.confidence != null
        ? `${(regime.confidence * 100).toFixed(0)}%`
        : '--';

    const staleCount = Array.isArray(sources)
        ? sources.filter((src) => {
            if (!src.last_pull) return true;
            return (Date.now() - new Date(src.last_pull).getTime()) > 86400000;
        }).length
        : 0;

    const latestPull = Array.isArray(sources) && sources.length > 0
        ? sources.reduce((latest, src) => {
            if (!src.last_pull) return latest;
            const t = new Date(src.last_pull).getTime();
            return t > latest ? t : latest;
        }, 0)
        : 0;

    /* build recent activity timeline from multiple sources */
    const recentActivity = buildTimeline(journal, issues, snapshots);

    /* quick action handlers */
    const handleAction = useCallback(async (actionId) => {
        try {
            if (navigator.vibrate) navigator.vibrate(8);
            switch (actionId) {
                case 'pipeline':
                    await api._fetch('/api/v1/workflows/run-all', { method: 'POST' }).catch(() => null);
                    break;
                case 'scan':
                    await api._fetch('/api/v1/options/scan?min_score=5.0').catch(() => null);
                    break;
                case 'briefing':
                    await api._fetch('/api/v1/ollama/briefing', {
                        method: 'POST',
                        body: JSON.stringify({ briefing_type: 'hourly' }),
                    }).catch(() => null);
                    break;
                case 'backtest':
                    await api._fetch('/api/v1/backtest/run', {
                        method: 'POST',
                        body: JSON.stringify({ start_date: '2015-01-01', initial_capital: 100000, cost_bps: 10 }),
                    }).catch(() => null);
                    break;
                case 'cluster':
                    await api._fetch('/api/v1/discovery/clustering?n_components=3', { method: 'POST' }).catch(() => null);
                    break;
                case 'ortho':
                    await api._fetch('/api/v1/discovery/orthogonality', { method: 'POST' }).catch(() => null);
                    break;
                default:
                    break;
            }
        } catch (_) {
            /* action errors handled silently on mobile */
        }
    }, []);

    /* ---------- sub-tab routing ---------- */
    const activeTab = subTab || 'Overview';

    if (loading) {
        return <div style={s.loading}>Loading dashboard...</div>;
    }

    if (error) {
        return (
            <div style={s.error}>
                {error}
                <div style={{ marginTop: '8px' }}>
                    <button
                        style={{ ...s.actionLabel, color: colors.accent, background: 'none', border: 'none', cursor: 'pointer' }}
                        onClick={loadData}
                    >
                        Retry
                    </button>
                </div>
            </div>
        );
    }

    return (
        <div style={s.container}>
            {/* Overview tab: Status Grid + Quick Actions */}
            {activeTab === 'Overview' && (
                <>
                    <div style={s.sectionLabel}>STATUS</div>
                    <div style={s.statusGrid}>
                        {/* Regime */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={{ ...s.metricValue, color: regimeColor(regimeState) }}>
                                    {regimeState}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Regime State</div>
                            <div style={s.metricSub}>conf {regimeConf}</div>
                        </div>

                        {/* Pipeline Health */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={s.metricValue}>
                                    {pipeline.success_rate != null
                                        ? `${(pipeline.success_rate * 100).toFixed(0)}%`
                                        : (db.connected ? 'OK' : '--')}
                                </span>
                                <span style={s.dot(db.connected ? colors.green : colors.red)} />
                            </div>
                            <div style={s.metricLabel}>Pipeline</div>
                            <div style={s.metricSub}>
                                {pipeline.last_run ? timeAgo(pipeline.last_run) : 'no runs'}
                            </div>
                        </div>

                        {/* Data Freshness */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={{
                                    ...s.metricValue,
                                    color: staleCount > 5 ? colors.red : staleCount > 0 ? colors.yellow : colors.green,
                                }}>
                                    {staleCount}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Stale Sources</div>
                            <div style={s.metricSub}>
                                {latestPull ? `latest ${timeAgo(latestPull)}` : 'no data'}
                            </div>
                        </div>

                        {/* Hyperspace */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={{
                                    ...s.metricValue,
                                    color: hs.node_online ? colors.green : colors.textMuted,
                                    fontSize: '14px',
                                }}>
                                    {hs.node_online ? 'ONLINE' : 'OFFLINE'}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Hyperspace</div>
                            <div style={s.metricSub}>
                                {hs.cycles != null ? `${hs.cycles} cycles` : '--'}
                            </div>
                        </div>

                        {/* Features */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={s.metricValue}>
                                    {grid.total_features || '--'}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Features</div>
                            <div style={s.metricSub}>
                                {grid.model_eligible || '--'} eligible
                            </div>
                        </div>

                        {/* Hypotheses */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={s.metricValue}>
                                    {grid.hypotheses_total || '--'}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Hypotheses</div>
                            <div style={s.metricSub}>
                                {grid.hypotheses_passed || '0'} passed / {grid.hypotheses_failed || '0'} failed
                            </div>
                        </div>

                        {/* Journal */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={s.metricValue}>
                                    {grid.journal_entries || '--'}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Journal</div>
                            <div style={s.metricSub}>
                                {grid.pending_outcomes || '0'} pending
                            </div>
                        </div>

                        {/* Options */}
                        <div style={s.metricCard}>
                            <div style={s.metricTop}>
                                <span style={s.metricValue}>
                                    {grid.options_opportunities || '--'}
                                </span>
                            </div>
                            <div style={s.metricLabel}>Options</div>
                            <div style={s.metricSub}>
                                {grid.options_100x || '0'} 100x
                            </div>
                        </div>
                    </div>

                    {/* Quick Actions */}
                    <div style={s.sectionLabel}>ACTIONS</div>
                    <div style={s.actionGrid}>
                        {QUICK_ACTIONS.map((act) => (
                            <div
                                key={act.id}
                                style={s.actionBtn(act.color)}
                                onClick={() => handleAction(act.action)}
                            >
                                <div style={s.actionLabel}>{act.label}</div>
                                <div style={s.actionDesc}>{act.desc}</div>
                            </div>
                        ))}
                    </div>
                </>
            )}

            {/* Metrics tab: detailed system metrics */}
            {activeTab === 'Metrics' && (
                <>
                    <div style={s.sectionLabel}>SYSTEM METRICS</div>
                    <div style={s.statusGrid}>
                        <MetricDetail label="Database" value={db.connected ? 'Connected' : 'Disconnected'} color={db.connected ? colors.green : colors.red} sub={db.pool_size ? `pool: ${db.pool_size}` : ''} />
                        <MetricDetail label="Hyperspace" value={hs.node_online ? 'Online' : 'Offline'} color={hs.node_online ? colors.green : colors.textMuted} sub={hs.peer_count != null ? `${hs.peer_count} peers` : ''} />
                        <MetricDetail label="Total Features" value={grid.total_features || '--'} color={colors.text} sub={`${grid.model_eligible || 0} model-eligible`} />
                        <MetricDetail label="Hypotheses" value={grid.hypotheses_total || '--'} color={colors.text} sub={`${grid.hypotheses_active || 0} active`} />
                        <MetricDetail label="Journal" value={grid.journal_entries || '--'} color={colors.text} sub={`${grid.pending_outcomes || 0} pending`} />
                        <MetricDetail label="Models" value={grid.production_models || '--'} color={colors.accent} sub="in production" />
                        <MetricDetail label="Sources" value={Array.isArray(sources) ? sources.length : '--'} color={colors.text} sub={`${staleCount} stale`} />
                        <MetricDetail label="Issues (7d)" value={issues.length || '0'} color={issues.length > 0 ? colors.red : colors.green} sub="errors" />
                    </div>
                </>
            )}

            {/* Sources tab: source freshness list */}
            {activeTab === 'Sources' && (
                <>
                    <div style={s.sectionLabel}>
                        DATA SOURCES ({Array.isArray(sources) ? sources.length : 0})
                    </div>
                    {Array.isArray(sources) && sources.length > 0 ? (
                        sources
                            .sort((a, b) => {
                                if (!a.last_pull && !b.last_pull) return 0;
                                if (!a.last_pull) return 1;
                                if (!b.last_pull) return -1;
                                return new Date(b.last_pull) - new Date(a.last_pull);
                            })
                            .map((src, i) => {
                                const fc = freshnessColor(src.last_pull);
                                return (
                                    <div key={src.id || src.name || i} style={s.sourceRow}>
                                        <div>
                                            <span style={s.dot(fc)} />
                                            <span style={s.sourceName}>
                                                {src.name || src.source_name || `Source ${i + 1}`}
                                            </span>
                                            {src.category && (
                                                <span style={{ fontSize: '9px', color: colors.textMuted, marginLeft: '6px' }}>
                                                    {src.category}
                                                </span>
                                            )}
                                        </div>
                                        <span style={s.sourceTime(fc)}>
                                            {freshnessLabel(src.last_pull)}
                                        </span>
                                    </div>
                                );
                            })
                    ) : (
                        <div style={s.loading}>No sources configured</div>
                    )}
                </>
            )}

            {/* Freshness tab: recent activity timeline */}
            {activeTab === 'Freshness' && (
                <>
                    <div style={s.sectionLabel}>RECENT ACTIVITY</div>
                    {recentActivity.length > 0 ? (
                        recentActivity.map((evt, i) => (
                            <div key={i} style={s.eventRow}>
                                <span style={s.eventIcon(evt.color)}>
                                    {EVENT_ICONS[evt.type] || EVENT_ICONS.default}
                                </span>
                                <span style={s.eventText}>{evt.description}</span>
                                <span style={s.eventTime}>{timeAgo(evt.timestamp)}</span>
                            </div>
                        ))
                    ) : (
                        <div style={s.loading}>No recent activity</div>
                    )}
                </>
            )}
        </div>
    );
}

/* ---------- helper components ---------- */

function MetricDetail({ label, value, color, sub }) {
    return (
        <div style={s.metricCard}>
            <div style={{ ...s.metricValue, color: color || '#E8F0F8', fontSize: '15px' }}>
                {value}
            </div>
            <div style={s.metricLabel}>{label}</div>
            {sub && <div style={s.metricSub}>{sub}</div>}
        </div>
    );
}

/* ---------- timeline builder ---------- */

function buildTimeline(journal, issues, snapshots) {
    const events = [];

    /* journal entries */
    if (Array.isArray(journal)) {
        journal.forEach((entry) => {
            events.push({
                type: 'journal',
                description: entry.action_taken
                    ? entry.action_taken.substring(0, 80)
                    : `Journal #${entry.id}`,
                timestamp: entry.created_at || entry.timestamp,
                color: colors.accent,
            });
        });
    }

    /* issues */
    if (Array.isArray(issues)) {
        issues.forEach((issue) => {
            events.push({
                type: 'issue',
                description: issue.message || issue.title || 'System issue',
                timestamp: issue.created_at || issue.timestamp,
                color: colors.red,
            });
        });
    }

    /* snapshots as pipeline events */
    if (Array.isArray(snapshots)) {
        snapshots.forEach((snap) => {
            events.push({
                type: 'pull',
                description: snap.category
                    ? `Pipeline: ${snap.category}`
                    : 'Pipeline snapshot',
                timestamp: snap.created_at || snap.snapshot_date,
                color: colors.green,
            });
        });
    }

    /* sort by timestamp descending, limit to 20 */
    events.sort((a, b) => {
        const ta = a.timestamp ? new Date(a.timestamp).getTime() : 0;
        const tb = b.timestamp ? new Date(b.timestamp).getTime() : 0;
        return tb - ta;
    });

    return events.slice(0, 20);
}

/* ---------- regime color helper ---------- */

function regimeColor(state) {
    if (!state || state === '--') return colors.textMuted;
    const s = String(state).toLowerCase();
    if (s.includes('risk_on') || s.includes('expansion') || s.includes('bull')) return colors.green;
    if (s.includes('risk_off') || s.includes('contraction') || s.includes('bear')) return colors.red;
    if (s.includes('transition') || s.includes('volatile')) return colors.yellow;
    return colors.accent;
}
