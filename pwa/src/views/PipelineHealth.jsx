import React, { useEffect, useState, useMemo } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

// ── Helpers ────────────────────────────────────────────────────────

function timeAgo(iso) {
    if (!iso) return 'never';
    const diff = Date.now() - new Date(iso).getTime();
    const mins = Math.floor(diff / 60000);
    if (mins < 1) return 'just now';
    if (mins < 60) return `${mins}m ago`;
    const hrs = Math.floor(mins / 60);
    if (hrs < 24) return `${hrs}h ago`;
    const days = Math.floor(hrs / 24);
    return `${days}d ago`;
}

const STATUS_COLORS = {
    healthy: colors.green,
    stale: colors.yellow,
    broken: colors.red,
};

const FRESHNESS_BG = {
    green: colors.greenBg,
    yellow: colors.yellowBg,
    red: colors.redBg,
};

// ── Styles ─────────────────────────────────────────────────────────

const s = {
    container: {
        padding: '16px',
        paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)',
        maxWidth: '1100px',
        margin: '0 auto',
    },
    title: {
        fontFamily: "'JetBrains Mono', monospace",
        fontSize: '14px',
        color: colors.textMuted,
        letterSpacing: '2px',
        marginBottom: '16px',
    },
    section: { marginBottom: '24px' },
    sectionTitle: {
        ...shared.sectionTitle,
        textTransform: 'uppercase',
    },
    card: {
        background: colors.card,
        borderRadius: tokens.radius.md,
        padding: '16px',
        border: `1px solid ${colors.border}`,
        marginBottom: '12px',
    },
    // Summary bar
    summaryBar: {
        display: 'flex',
        gap: '12px',
        flexWrap: 'wrap',
        marginBottom: '20px',
    },
    summaryTile: (color) => ({
        flex: '1 1 120px',
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: '14px 16px',
        textAlign: 'center',
        borderLeft: `3px solid ${color}`,
    }),
    summaryNum: {
        fontSize: '26px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: colors.mono,
    },
    summaryLabel: {
        fontSize: tokens.fontSize.xs,
        color: colors.textMuted,
        marginTop: '4px',
        textTransform: 'uppercase',
        letterSpacing: '1px',
    },
    // Table
    tableWrap: {
        overflowX: 'auto',
        WebkitOverflowScrolling: 'touch',
    },
    table: {
        width: '100%',
        borderCollapse: 'collapse',
        fontSize: tokens.fontSize.sm,
        fontFamily: colors.mono,
    },
    th: {
        textAlign: 'left',
        padding: '8px 10px',
        color: colors.textMuted,
        fontSize: '10px',
        fontWeight: 700,
        letterSpacing: '1px',
        textTransform: 'uppercase',
        borderBottom: `1px solid ${colors.border}`,
        cursor: 'pointer',
        userSelect: 'none',
        whiteSpace: 'nowrap',
    },
    td: {
        padding: '8px 10px',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        verticalAlign: 'middle',
        whiteSpace: 'nowrap',
    },
    dot: (color) => ({
        display: 'inline-block',
        width: '8px',
        height: '8px',
        borderRadius: '50%',
        background: color,
        marginRight: '6px',
        flexShrink: 0,
    }),
    badge: (bg, fg) => ({
        display: 'inline-block',
        padding: '2px 8px',
        borderRadius: tokens.radius.sm,
        fontSize: '10px',
        fontWeight: 600,
        background: bg,
        color: fg,
        textTransform: 'uppercase',
    }),
    freshnessBarOuter: {
        width: '60px',
        height: '6px',
        background: colors.bg,
        borderRadius: '3px',
        overflow: 'hidden',
        display: 'inline-block',
        verticalAlign: 'middle',
    },
    freshnessBarInner: (color, pct) => ({
        width: `${pct}%`,
        height: '100%',
        background: color,
        borderRadius: '3px',
        transition: `width ${tokens.transition.normal}`,
    }),
    errorText: {
        fontSize: '11px',
        color: colors.red,
        maxWidth: '220px',
        overflow: 'hidden',
        textOverflow: 'ellipsis',
        whiteSpace: 'nowrap',
        lineHeight: '1.5',
    },
    // Coverage heatmap
    heatmapGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(140px, 1fr))',
        gap: '8px',
    },
    heatmapCell: (pct) => {
        const bg = pct >= 90 ? colors.greenBg
            : pct >= 60 ? colors.yellowBg
            : colors.redBg;
        const borderColor = pct >= 90 ? '#1A5A3A'
            : pct >= 60 ? '#5A4A00'
            : '#5A1A1A';
        return {
            background: bg,
            border: `1px solid ${borderColor}`,
            borderRadius: tokens.radius.sm,
            padding: '10px',
            textAlign: 'center',
        };
    },
    heatmapFamily: {
        fontSize: '11px',
        fontWeight: 600,
        color: colors.text,
        textTransform: 'uppercase',
        marginBottom: '4px',
    },
    heatmapPct: {
        fontSize: '18px',
        fontWeight: 700,
        fontFamily: colors.mono,
    },
    heatmapDetail: {
        fontSize: '10px',
        color: colors.textMuted,
        marginTop: '2px',
    },
    // Error log
    errorRow: {
        display: 'flex',
        gap: '10px',
        padding: '6px 0',
        borderBottom: `1px solid ${colors.borderSubtle}`,
        fontSize: '12px',
        alignItems: 'flex-start',
    },
    errorTs: {
        color: colors.textMuted,
        fontFamily: colors.mono,
        fontSize: '11px',
        flexShrink: 0,
        minWidth: '70px',
    },
    errorSource: {
        color: colors.accent,
        fontWeight: 600,
        flexShrink: 0,
        minWidth: '100px',
    },
    errorMsg: {
        color: colors.text,
        fontFamily: colors.mono,
        fontSize: '11px',
        wordBreak: 'break-word',
    },
    // Resolver
    resolverGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
        gap: '10px',
    },
    resolverItem: {
        background: colors.bg,
        borderRadius: tokens.radius.md,
        padding: '12px',
        textAlign: 'center',
    },
    resolverValue: {
        fontSize: '20px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: colors.mono,
    },
    resolverLabel: {
        fontSize: tokens.fontSize.xs,
        color: colors.textMuted,
        marginTop: '4px',
    },
    loading: {
        textAlign: 'center',
        padding: '40px',
        color: colors.textMuted,
        fontFamily: colors.mono,
    },
    sortArrow: {
        fontSize: '10px',
        marginLeft: '3px',
    },
    filterRow: {
        display: 'flex',
        gap: '6px',
        marginBottom: '12px',
        flexWrap: 'wrap',
    },
    filterBtn: (active) => ({
        padding: '6px 14px',
        borderRadius: tokens.radius.pill,
        fontSize: '11px',
        fontWeight: 600,
        cursor: 'pointer',
        border: 'none',
        fontFamily: "'IBM Plex Sans', sans-serif",
        background: active ? colors.accent : colors.card,
        color: active ? '#fff' : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
    }),
};

// ── Sort logic ─────────────────────────────────────────────────────

const STATUS_ORDER = { broken: 0, stale: 1, healthy: 2 };

function sortSources(sources, sortKey, sortDir) {
    const sorted = [...sources];
    sorted.sort((a, b) => {
        let cmp = 0;
        if (sortKey === 'status') {
            cmp = (STATUS_ORDER[a.status] ?? 3) - (STATUS_ORDER[b.status] ?? 3);
        } else if (sortKey === 'name') {
            cmp = a.name.localeCompare(b.name);
        } else if (sortKey === 'freshness') {
            const f = { red: 0, yellow: 1, green: 2 };
            cmp = (f[a.freshness] ?? 0) - (f[b.freshness] ?? 0);
        } else if (sortKey === 'last_pull') {
            const ta = a.last_pull ? new Date(a.last_pull).getTime() : 0;
            const tb = b.last_pull ? new Date(b.last_pull).getTime() : 0;
            cmp = ta - tb;
        } else if (sortKey === 'rows') {
            cmp = (a.rows_last_pull || 0) - (b.rows_last_pull || 0);
        }
        return sortDir === 'asc' ? cmp : -cmp;
    });
    return sorted;
}

// ── Component ──────────────────────────────────────────────────────

export default function PipelineHealth() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [sortKey, setSortKey] = useState('status');
    const [sortDir, setSortDir] = useState('asc');
    const [statusFilter, setStatusFilter] = useState('all');

    useEffect(() => {
        let cancelled = false;
        async function load() {
            setLoading(true);
            const res = await api.getPipelineHealth();
            if (cancelled) return;
            if (res.error) {
                setError(res.message || 'Failed to load pipeline health');
            } else {
                setData(res);
            }
            setLoading(false);
        }
        load();
        const interval = setInterval(load, 60000); // refresh every minute
        return () => { cancelled = true; clearInterval(interval); };
    }, []);

    const handleSort = (key) => {
        if (sortKey === key) {
            setSortDir(d => d === 'asc' ? 'desc' : 'asc');
        } else {
            setSortKey(key);
            setSortDir('asc');
        }
    };

    const sortArrow = (key) => {
        if (sortKey !== key) return '';
        return sortDir === 'asc' ? ' \u25B2' : ' \u25BC';
    };

    const filteredSources = useMemo(() => {
        if (!data) return [];
        let srcs = data.sources || [];
        if (statusFilter !== 'all') {
            srcs = srcs.filter(s => s.status === statusFilter);
        }
        return sortSources(srcs, sortKey, sortDir);
    }, [data, sortKey, sortDir, statusFilter]);

    if (loading && !data) {
        return <div style={s.loading}>Loading pipeline health...</div>;
    }

    if (error && !data) {
        return (
            <div style={s.container}>
                <div style={s.title}>PIPELINE HEALTH</div>
                <div style={{ ...s.card, color: colors.red }}>{error}</div>
            </div>
        );
    }

    const { summary = {}, coverage = {}, recent_errors = [], resolver_status = {} } = data || {};
    const byFamily = coverage.by_family || {};

    const freshnessPct = (src) => {
        if (src.freshness === 'green') return 100;
        if (src.freshness === 'yellow') return 55;
        return 15;
    };

    return (
        <div style={s.container}>
            <div style={s.title}>PIPELINE HEALTH</div>

            {/* ── Summary Bar ────────────────────────────────────── */}
            <div style={s.summaryBar}>
                <div style={s.summaryTile(colors.text)}>
                    <div style={s.summaryNum}>{summary.total_sources || 0}</div>
                    <div style={s.summaryLabel}>Total Sources</div>
                </div>
                <div style={s.summaryTile(colors.green)}>
                    <div style={{ ...s.summaryNum, color: colors.green }}>{summary.healthy || 0}</div>
                    <div style={s.summaryLabel}>Healthy</div>
                </div>
                <div style={s.summaryTile(colors.yellow)}>
                    <div style={{ ...s.summaryNum, color: colors.yellow }}>{summary.stale || 0}</div>
                    <div style={s.summaryLabel}>Stale</div>
                </div>
                <div style={s.summaryTile(colors.red)}>
                    <div style={{ ...s.summaryNum, color: colors.red }}>{summary.broken || 0}</div>
                    <div style={s.summaryLabel}>Broken</div>
                </div>
            </div>

            {/* ── Source Table ────────────────────────────────────── */}
            <div style={s.section}>
                <div style={s.sectionTitle}>SOURCE STATUS</div>
                <div style={s.filterRow}>
                    {['all', 'healthy', 'stale', 'broken'].map(f => (
                        <button
                            key={f}
                            style={s.filterBtn(statusFilter === f)}
                            onClick={() => setStatusFilter(f)}
                        >
                            {f === 'all' ? `All (${summary.total_sources || 0})` :
                             f === 'healthy' ? `Healthy (${summary.healthy || 0})` :
                             f === 'stale' ? `Stale (${summary.stale || 0})` :
                             `Broken (${summary.broken || 0})`}
                        </button>
                    ))}
                </div>
                <div style={s.card}>
                    <div style={s.tableWrap}>
                        <table style={s.table}>
                            <thead>
                                <tr>
                                    <th style={s.th} onClick={() => handleSort('status')}>
                                        Status{sortArrow('status')}
                                    </th>
                                    <th style={s.th} onClick={() => handleSort('name')}>
                                        Source{sortArrow('name')}
                                    </th>
                                    <th style={s.th}>Type</th>
                                    <th style={s.th} onClick={() => handleSort('last_pull')}>
                                        Last Pull{sortArrow('last_pull')}
                                    </th>
                                    <th style={s.th} onClick={() => handleSort('rows')}>
                                        Rows{sortArrow('rows')}
                                    </th>
                                    <th style={s.th} onClick={() => handleSort('freshness')}>
                                        Freshness{sortArrow('freshness')}
                                    </th>
                                    <th style={s.th}>Error</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredSources.map((src) => (
                                    <tr key={src.name}>
                                        <td style={s.td}>
                                            <span style={s.dot(STATUS_COLORS[src.status] || colors.textMuted)} />
                                            {src.status}
                                        </td>
                                        <td style={{ ...s.td, color: colors.text, fontWeight: 600 }}>
                                            {src.name}
                                        </td>
                                        <td style={s.td}>
                                            <span style={s.badge(
                                                FRESHNESS_BG[src.freshness] || colors.bg,
                                                STATUS_COLORS[src.status] || colors.textMuted,
                                            )}>
                                                {src.type}
                                            </span>
                                        </td>
                                        <td style={{ ...s.td, color: colors.textDim }}>
                                            {timeAgo(src.last_pull)}
                                        </td>
                                        <td style={{ ...s.td, color: colors.textDim }}>
                                            {src.rows_last_pull != null ? src.rows_last_pull.toLocaleString() : '-'}
                                        </td>
                                        <td style={s.td}>
                                            <div style={s.freshnessBarOuter}>
                                                <div style={s.freshnessBarInner(
                                                    STATUS_COLORS[src.status] || colors.textMuted,
                                                    freshnessPct(src),
                                                )} />
                                            </div>
                                        </td>
                                        <td style={s.td}>
                                            {src.error ? (
                                                <span style={s.errorText} title={src.error}>
                                                    {src.error}
                                                </span>
                                            ) : null}
                                        </td>
                                    </tr>
                                ))}
                                {filteredSources.length === 0 && (
                                    <tr>
                                        <td colSpan={7} style={{ ...s.td, textAlign: 'center', color: colors.textMuted }}>
                                            No sources match filter
                                        </td>
                                    </tr>
                                )}
                            </tbody>
                        </table>
                    </div>
                </div>
            </div>

            {/* ── Coverage Heatmap ───────────────────────────────── */}
            {Object.keys(byFamily).length > 0 && (
                <div style={s.section}>
                    <div style={s.sectionTitle}>COVERAGE BY FAMILY</div>
                    <div style={s.heatmapGrid}>
                        {Object.entries(byFamily).map(([family, info]) => (
                            <div key={family} style={s.heatmapCell(info.pct)}>
                                <div style={s.heatmapFamily}>{family}</div>
                                <div style={{
                                    ...s.heatmapPct,
                                    color: info.pct >= 90 ? colors.green
                                        : info.pct >= 60 ? colors.yellow
                                        : colors.red,
                                }}>
                                    {info.pct}%
                                </div>
                                <div style={s.heatmapDetail}>
                                    {info.with_data}/{info.total} series
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* ── Resolver Status ────────────────────────────────── */}
            <div style={s.section}>
                <div style={s.sectionTitle}>RESOLVER STATUS</div>
                <div style={s.card}>
                    <div style={s.resolverGrid}>
                        <div style={s.resolverItem}>
                            <div style={{
                                ...s.resolverValue,
                                color: (resolver_status.pending || 0) > 500 ? colors.yellow : colors.green,
                            }}>
                                {(resolver_status.pending || 0).toLocaleString()}
                            </div>
                            <div style={s.resolverLabel}>Pending</div>
                        </div>
                        <div style={s.resolverItem}>
                            <div style={s.resolverValue}>
                                {resolver_status.last_resolved || 0}
                            </div>
                            <div style={s.resolverLabel}>Resolved (24h)</div>
                        </div>
                        <div style={s.resolverItem}>
                            <div style={{ ...s.resolverValue, fontSize: '14px' }}>
                                {resolver_status.last_run ? timeAgo(resolver_status.last_run) : 'never'}
                            </div>
                            <div style={s.resolverLabel}>Last Run</div>
                        </div>
                    </div>
                </div>
            </div>

            {/* ── Error Log ──────────────────────────────────────── */}
            {recent_errors.length > 0 && (
                <div style={s.section}>
                    <div style={s.sectionTitle}>RECENT ERRORS ({recent_errors.length})</div>
                    <div style={s.card}>
                        {recent_errors.map((err, i) => (
                            <div key={i} style={s.errorRow}>
                                <span style={s.errorTs}>{timeAgo(err.timestamp)}</span>
                                <span style={s.errorSource}>{err.source}</span>
                                <span style={s.errorMsg}>{err.message}</span>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
}
