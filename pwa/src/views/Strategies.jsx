import React, { useEffect, useState, useCallback } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

// ── Styles ──────────────────────────────────────────────────────

const MONO = "'IBM Plex Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

const s = {
    page: {
        padding: '16px', maxWidth: '1100px', margin: '0 auto',
        paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)',
    },
    header: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '16px',
    },
    headerTitle: {
        fontFamily: MONO, fontSize: '14px', color: '#5A7080',
        letterSpacing: '2px',
    },
    sectionLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.accent, fontFamily: MONO,
        marginBottom: tokens.space.sm, marginTop: '20px',
    },
    stratCard: {
        background: colors.card, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md, padding: '14px 16px',
        marginBottom: '10px', cursor: 'pointer',
        transition: `all ${tokens.transition.fast}`,
    },
    stratCardExpanded: {
        borderColor: colors.accent,
        boxShadow: '0 2px 12px rgba(26,110,191,0.15)',
    },
    stratRow: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        gap: '12px', flexWrap: 'wrap',
    },
    stratName: {
        fontSize: '14px', fontWeight: 600, color: '#E8F0F8',
        fontFamily: SANS,
    },
    stratSub: {
        fontSize: '11px', color: colors.textMuted, fontFamily: MONO,
        marginTop: '2px',
    },
    badge: (bg) => ({
        display: 'inline-flex', alignItems: 'center', padding: '2px 8px',
        borderRadius: tokens.radius.sm, fontSize: '11px', fontWeight: 700,
        fontFamily: MONO, background: bg, color: '#fff',
    }),
    pnlBadge: (pnl) => ({
        display: 'inline-flex', alignItems: 'center', padding: '3px 10px',
        borderRadius: tokens.radius.sm, fontSize: '13px', fontWeight: 700,
        fontFamily: MONO, minWidth: '70px', justifyContent: 'center',
        background: pnl >= 0 ? colors.greenBg : colors.redBg,
        color: pnl >= 0 ? colors.green : colors.red,
    }),
    metricRow: {
        display: 'flex', gap: '16px', marginTop: '8px', flexWrap: 'wrap',
    },
    metric: {
        fontSize: '11px', color: colors.textDim, fontFamily: MONO,
    },
    metricVal: {
        fontWeight: 700, color: colors.text,
    },
    tradeTable: {
        width: '100%', borderCollapse: 'collapse', marginTop: '10px',
        fontSize: '12px', fontFamily: MONO,
    },
    th: {
        textAlign: 'left', padding: '6px 8px', color: colors.textMuted,
        borderBottom: `1px solid ${colors.border}`, fontSize: '10px',
        letterSpacing: '0.5px', fontWeight: 700,
    },
    td: {
        padding: '6px 8px', color: colors.text,
        borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    filterRow: {
        display: 'flex', gap: '10px', marginBottom: '12px', flexWrap: 'wrap',
        alignItems: 'center',
    },
    filterInput: {
        background: colors.bg, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm, color: colors.text,
        padding: '8px 10px', fontSize: '12px', fontFamily: MONO,
        minHeight: '36px', width: '120px',
    },
    filterSelect: {
        background: colors.bg, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm, color: colors.text,
        padding: '8px 10px', fontSize: '12px', fontFamily: MONO,
        minHeight: '36px',
    },
    promoteBtn: {
        background: colors.accent, color: '#fff', border: 'none',
        borderRadius: tokens.radius.sm, padding: '4px 12px',
        fontSize: '11px', fontWeight: 600, cursor: 'pointer',
        fontFamily: MONO, minHeight: '28px',
        transition: `all ${tokens.transition.fast}`,
    },
    dashCard: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: tokens.space.md, textAlign: 'center', flex: '1 1 120px',
    },
    dashVal: {
        fontSize: tokens.fontSize.xl, fontWeight: 700, color: '#E8F0F8',
        fontFamily: MONO,
    },
    dashLabel: {
        fontSize: tokens.fontSize.xs, color: colors.textMuted,
        marginTop: tokens.space.xs,
    },
    killedCard: {
        background: colors.redBg, border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm, padding: '8px 12px',
        marginBottom: '6px',
    },
    empty: {
        textAlign: 'center', color: colors.textMuted, padding: '40px 20px',
        fontFamily: MONO, fontSize: '13px',
    },
    loading: {
        textAlign: 'center', color: colors.textMuted, padding: '60px 20px',
        fontFamily: MONO, fontSize: '13px',
    },
};

const statusColors = {
    ACTIVE: colors.green,
    PAUSED: colors.yellow,
    KILLED: colors.red,
};

// ── Helpers ─────────────────────────────────────────────────────

function fmtPnl(v) {
    if (v == null) return '--';
    const sign = v >= 0 ? '+' : '';
    return `${sign}$${v.toFixed(2)}`;
}

function fmtPct(v) {
    if (v == null) return '--';
    return `${(v * 100).toFixed(1)}%`;
}

function fmtSharpe(v) {
    if (v == null) return '--';
    return v.toFixed(2);
}

// ── Strategy Card ───────────────────────────────────────────────

function StrategyCard({ strat }) {
    const [expanded, setExpanded] = useState(false);
    const [trades, setTrades] = useState(null);
    const [loadingTrades, setLoadingTrades] = useState(false);

    const toggle = useCallback(async () => {
        if (!expanded && trades === null) {
            setLoadingTrades(true);
            const res = await api.getStrategyHistory(strat.id);
            if (!res.error) setTrades(res.trades || []);
            setLoadingTrades(false);
        }
        setExpanded(e => !e);
    }, [expanded, trades, strat.id]);

    const pnl = strat.total_pnl || 0;
    const status = strat.status || 'ACTIVE';
    const winRate = strat.win_rate || 0;

    return (
        <div
            style={{ ...s.stratCard, ...(expanded ? s.stratCardExpanded : {}) }}
            onClick={toggle}
        >
            <div style={s.stratRow}>
                <div style={{ flex: 1 }}>
                    <div style={s.stratName}>
                        {strat.leader_display || strat.leader} &rarr; {strat.follower_display || strat.follower}
                    </div>
                    <div style={s.stratSub}>
                        {strat.description || strat.id}
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '8px', alignItems: 'center', flexWrap: 'wrap' }}>
                    <span style={s.pnlBadge(pnl)}>{fmtPnl(pnl)}</span>
                    <span style={s.badge(statusColors[status] || '#5A7080')}>{status}</span>
                </div>
            </div>

            <div style={s.metricRow}>
                <span style={s.metric}>Sharpe <span style={s.metricVal}>{fmtSharpe(strat.sharpe)}</span></span>
                <span style={s.metric}>WR <span style={s.metricVal}>{fmtPct(winRate)}</span></span>
                <span style={s.metric}>Trades <span style={s.metricVal}>{strat.total_trades || 0}</span></span>
                <span style={s.metric}>Open <span style={s.metricVal}>{strat.open_positions || 0}</span></span>
                <span style={s.metric}>DD <span style={s.metricVal}>{fmtPct(strat.max_drawdown)}</span></span>
            </div>

            {status === 'KILLED' && strat.kill_reason && (
                <div style={{ marginTop: '6px', fontSize: '11px', color: colors.red, fontFamily: MONO }}>
                    Killed: {strat.kill_reason}
                </div>
            )}

            {expanded && (
                <div style={{ marginTop: '12px' }} onClick={e => e.stopPropagation()}>
                    <div style={{ ...s.sectionLabel, marginTop: '4px', marginBottom: '6px' }}>TRADE HISTORY</div>
                    {loadingTrades ? (
                        <div style={{ color: colors.textMuted, fontSize: '12px', fontFamily: MONO }}>Loading...</div>
                    ) : trades && trades.length > 0 ? (
                        <div style={{ overflowX: 'auto' }}>
                            <table style={s.tradeTable}>
                                <thead>
                                    <tr>
                                        <th style={s.th}>DIR</th>
                                        <th style={s.th}>TICKER</th>
                                        <th style={s.th}>ENTRY</th>
                                        <th style={s.th}>EXIT</th>
                                        <th style={s.th}>P&L</th>
                                        <th style={s.th}>STATUS</th>
                                        <th style={s.th}>DATE</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {trades.map(t => (
                                        <tr key={t.id}>
                                            <td style={{ ...s.td, color: t.direction === 'LONG' ? colors.green : colors.red }}>
                                                {t.direction}
                                            </td>
                                            <td style={s.td}>{t.ticker}</td>
                                            <td style={s.td}>${Number(t.entry_price).toFixed(2)}</td>
                                            <td style={s.td}>{t.exit_price != null ? `$${Number(t.exit_price).toFixed(2)}` : '--'}</td>
                                            <td style={{ ...s.td, color: (t.pnl || 0) >= 0 ? colors.green : colors.red }}>
                                                {t.pnl != null ? fmtPnl(t.pnl) : '--'}
                                            </td>
                                            <td style={s.td}>{t.status}</td>
                                            <td style={s.td}>{(t.entry_date || '').slice(0, 10)}</td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '12px', fontFamily: MONO }}>No trades yet</div>
                    )}
                </div>
            )}
        </div>
    );
}

// ── Backtest Winners Table ──────────────────────────────────────

function BacktestWinners() {
    const [results, setResults] = useState([]);
    const [loading, setLoading] = useState(true);
    const [promoting, setPromoting] = useState(null);
    const [filters, setFilters] = useState({ family: '', minSharpe: '1.0', minWinRate: '0.55' });

    const load = useCallback(async () => {
        setLoading(true);
        const params = {};
        if (filters.minSharpe) params.min_sharpe = filters.minSharpe;
        if (filters.minWinRate) params.min_win_rate = filters.minWinRate;
        if (filters.family) params.family = filters.family;
        const res = await api.getBacktestWinners(params);
        if (!res.error) setResults(res.results || []);
        setLoading(false);
    }, [filters]);

    useEffect(() => { load(); }, [load]);

    const handlePromote = async (w) => {
        setPromoting(w.leader + w.follower);
        await api.promoteToStrategy({
            leader: w.leader,
            follower: w.follower,
            sharpe: w.sharpe,
            win_rate: w.win_rate,
            total_return: w.total_return,
        });
        setPromoting(null);
    };

    const families = [...new Set(results.flatMap(r => [r.leader_family, r.follower_family]).filter(Boolean))].sort();

    return (
        <div>
            <div style={s.sectionLabel}>BACKTEST WINNERS</div>
            <div style={s.filterRow}>
                <select
                    style={s.filterSelect}
                    value={filters.family}
                    onChange={e => setFilters(f => ({ ...f, family: e.target.value }))}
                >
                    <option value="">All Families</option>
                    {families.map(f => <option key={f} value={f}>{f}</option>)}
                </select>
                <label style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO }}>
                    Min Sharpe
                    <input
                        type="number" step="0.1" min="0"
                        style={{ ...s.filterInput, width: '70px', marginLeft: '4px' }}
                        value={filters.minSharpe}
                        onChange={e => setFilters(f => ({ ...f, minSharpe: e.target.value }))}
                    />
                </label>
                <label style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO }}>
                    Min WR
                    <input
                        type="number" step="0.01" min="0" max="1"
                        style={{ ...s.filterInput, width: '70px', marginLeft: '4px' }}
                        value={filters.minWinRate}
                        onChange={e => setFilters(f => ({ ...f, minWinRate: e.target.value }))}
                    />
                </label>
                <button style={shared.buttonSmall} onClick={load}>Scan</button>
            </div>

            {loading ? (
                <div style={s.loading}>Scanning pairs...</div>
            ) : results.length === 0 ? (
                <div style={s.empty}>No backtest winners found with current filters.</div>
            ) : (
                <div style={{ overflowX: 'auto' }}>
                    <table style={s.tradeTable}>
                        <thead>
                            <tr>
                                <th style={s.th}>LEADER</th>
                                <th style={s.th}>FOLLOWER</th>
                                <th style={s.th}>SHARPE</th>
                                <th style={s.th}>WIN RATE</th>
                                <th style={s.th}>RETURN</th>
                                <th style={s.th}>TRADES</th>
                                <th style={s.th}></th>
                            </tr>
                        </thead>
                        <tbody>
                            {results.map((w, i) => (
                                <tr key={i}>
                                    <td style={s.td}>
                                        <div>{w.leader_display || w.leader}</div>
                                        <div style={{ fontSize: '10px', color: colors.textMuted }}>{w.leader_family}</div>
                                    </td>
                                    <td style={s.td}>
                                        <div>{w.follower_display || w.follower}</div>
                                        <div style={{ fontSize: '10px', color: colors.textMuted }}>{w.follower_family}</div>
                                    </td>
                                    <td style={{ ...s.td, color: w.sharpe >= 2 ? colors.green : colors.text }}>
                                        {fmtSharpe(w.sharpe)}
                                    </td>
                                    <td style={{ ...s.td, color: w.win_rate >= 0.6 ? colors.green : colors.text }}>
                                        {fmtPct(w.win_rate)}
                                    </td>
                                    <td style={{ ...s.td, color: (w.total_return || 0) >= 0 ? colors.green : colors.red }}>
                                        {fmtPct(w.total_return)}
                                    </td>
                                    <td style={s.td}>{w.trades}</td>
                                    <td style={s.td}>
                                        <button
                                            style={{
                                                ...s.promoteBtn,
                                                ...(promoting === w.leader + w.follower ? shared.buttonDisabled : {}),
                                            }}
                                            disabled={promoting === w.leader + w.follower}
                                            onClick={(e) => { e.stopPropagation(); handlePromote(w); }}
                                        >
                                            {promoting === w.leader + w.follower ? 'Creating...' : 'Promote'}
                                        </button>
                                    </td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}

// ── Performance Dashboard ───────────────────────────────────────

function PerformanceDashboard({ strategies }) {
    const active = strategies.filter(s => s.status === 'ACTIVE');
    const killed = strategies.filter(s => s.status === 'KILLED');
    const paused = strategies.filter(s => s.status === 'PAUSED');

    const totalPnl = strategies.reduce((a, s) => a + (s.total_pnl || 0), 0);
    const totalTrades = strategies.reduce((a, s) => a + (s.total_trades || 0), 0);
    const totalWins = strategies.reduce((a, s) => a + (s.wins || 0), 0);
    const aggWinRate = totalTrades > 0 ? totalWins / totalTrades : 0;

    const sorted = [...strategies].sort((a, b) => (b.total_pnl || 0) - (a.total_pnl || 0));
    const best = sorted[0];
    const worst = sorted[sorted.length - 1];

    return (
        <div>
            <div style={s.sectionLabel}>PERFORMANCE DASHBOARD</div>

            {/* Summary cards */}
            <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', marginBottom: '16px' }}>
                <div style={s.dashCard}>
                    <div style={{ ...s.dashVal, color: totalPnl >= 0 ? colors.green : colors.red }}>
                        {fmtPnl(totalPnl)}
                    </div>
                    <div style={s.dashLabel}>Total P&L</div>
                </div>
                <div style={s.dashCard}>
                    <div style={s.dashVal}>{active.length}</div>
                    <div style={s.dashLabel}>Active</div>
                </div>
                <div style={s.dashCard}>
                    <div style={s.dashVal}>{killed.length}</div>
                    <div style={s.dashLabel}>Killed</div>
                </div>
                <div style={s.dashCard}>
                    <div style={s.dashVal}>{totalTrades}</div>
                    <div style={s.dashLabel}>Total Trades</div>
                </div>
                <div style={s.dashCard}>
                    <div style={{ ...s.dashVal, color: aggWinRate >= 0.5 ? colors.green : colors.red }}>
                        {fmtPct(aggWinRate)}
                    </div>
                    <div style={s.dashLabel}>Agg Win Rate</div>
                </div>
            </div>

            {/* Best / Worst */}
            {best && (
                <div style={{ display: 'flex', gap: '10px', flexWrap: 'wrap', marginBottom: '16px' }}>
                    <div style={{ ...s.dashCard, border: `1px solid ${colors.border}`, flex: '1 1 200px', textAlign: 'left' }}>
                        <div style={{ fontSize: '10px', color: colors.green, fontFamily: MONO, fontWeight: 700, letterSpacing: '1px', marginBottom: '4px' }}>
                            BEST STRATEGY
                        </div>
                        <div style={{ fontSize: '13px', color: '#E8F0F8', fontFamily: SANS, fontWeight: 600 }}>
                            {best.leader_display || best.leader} &rarr; {best.follower_display || best.follower}
                        </div>
                        <div style={{ fontSize: '12px', color: colors.green, fontFamily: MONO, marginTop: '2px' }}>
                            {fmtPnl(best.total_pnl)} | WR {fmtPct(best.win_rate)}
                        </div>
                    </div>
                    {worst && worst.id !== best.id && (
                        <div style={{ ...s.dashCard, border: `1px solid ${colors.border}`, flex: '1 1 200px', textAlign: 'left' }}>
                            <div style={{ fontSize: '10px', color: colors.red, fontFamily: MONO, fontWeight: 700, letterSpacing: '1px', marginBottom: '4px' }}>
                                WORST STRATEGY
                            </div>
                            <div style={{ fontSize: '13px', color: '#E8F0F8', fontFamily: SANS, fontWeight: 600 }}>
                                {worst.leader_display || worst.leader} &rarr; {worst.follower_display || worst.follower}
                            </div>
                            <div style={{ fontSize: '12px', color: colors.red, fontFamily: MONO, marginTop: '2px' }}>
                                {fmtPnl(worst.total_pnl)} | WR {fmtPct(worst.win_rate)}
                            </div>
                        </div>
                    )}
                </div>
            )}

            {/* Killed strategies with reasons */}
            {killed.length > 0 && (
                <div>
                    <div style={{ ...s.sectionLabel, marginTop: '12px' }}>KILLED STRATEGIES</div>
                    {killed.map(k => (
                        <div key={k.id} style={s.killedCard}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                <div>
                                    <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, fontWeight: 600 }}>
                                        {k.leader_display || k.leader} &rarr; {k.follower_display || k.follower}
                                    </span>
                                    <span style={{ fontSize: '11px', color: colors.red, fontFamily: MONO, marginLeft: '10px' }}>
                                        {fmtPnl(k.total_pnl)}
                                    </span>
                                </div>
                            </div>
                            {k.kill_reason && (
                                <div style={{ fontSize: '11px', color: colors.textDim, fontFamily: MONO, marginTop: '4px' }}>
                                    {k.kill_reason}
                                </div>
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// ── Main View ───────────────────────────────────────────────────

export default function Strategies() {
    const [strategies, setStrategies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [tab, setTab] = useState('active');

    useEffect(() => {
        (async () => {
            const res = await api.getPaperStrategies();
            if (!res.error) setStrategies(res.strategies || []);
            setLoading(false);
        })();
    }, []);

    const active = strategies.filter(s => s.status === 'ACTIVE');
    const paused = strategies.filter(s => s.status === 'PAUSED');
    const killed = strategies.filter(s => s.status === 'KILLED');
    const liveStrategies = [...active, ...paused];

    if (loading) {
        return <div style={s.loading}>Loading strategies...</div>;
    }

    return (
        <div style={s.page}>
            <div style={s.header}>
                <div style={s.headerTitle}>PAPER STRATEGIES</div>
                <div style={{ fontSize: '12px', color: colors.textMuted, fontFamily: MONO }}>
                    {active.length} active / {strategies.length} total
                </div>
            </div>

            {/* Tabs */}
            <div style={shared.tabs}>
                {[
                    { id: 'active', label: `Active (${liveStrategies.length})` },
                    { id: 'backtest', label: 'Backtest Winners' },
                    { id: 'performance', label: 'Performance' },
                ].map(t => (
                    <button
                        key={t.id}
                        style={shared.tab(tab === t.id)}
                        onClick={() => setTab(t.id)}
                    >
                        {t.label}
                    </button>
                ))}
            </div>

            {/* Active strategies */}
            {tab === 'active' && (
                <div>
                    <div style={s.sectionLabel}>ACTIVE STRATEGIES</div>
                    {liveStrategies.length === 0 ? (
                        <div style={s.empty}>
                            No active paper strategies. Promote a backtest winner to get started.
                        </div>
                    ) : (
                        liveStrategies.map(st => <StrategyCard key={st.id} strat={st} />)
                    )}

                    {killed.length > 0 && (
                        <>
                            <div style={{ ...s.sectionLabel, marginTop: '24px' }}>KILLED ({killed.length})</div>
                            {killed.map(st => <StrategyCard key={st.id} strat={st} />)}
                        </>
                    )}
                </div>
            )}

            {/* Backtest winners */}
            {tab === 'backtest' && <BacktestWinners />}

            {/* Performance dashboard */}
            {tab === 'performance' && <PerformanceDashboard strategies={strategies} />}
        </div>
    );
}
