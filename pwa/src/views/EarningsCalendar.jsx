import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

/* ─────────────── Helpers ─────────────── */

const fmt = (v, dec = 2) => v != null ? Number(v).toFixed(dec) : '--';
const fmtPct = (v) => v != null ? `${v >= 0 ? '+' : ''}${Number(v).toFixed(2)}%` : '--';
const fmtDate = (d) => {
    if (!d) return '--';
    const dt = new Date(d + 'T00:00:00');
    return dt.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
};
const daysLabel = (n) => {
    if (n == null) return '';
    if (n === 0) return 'Today';
    if (n === 1) return 'Tomorrow';
    return `${n}d`;
};

/* ─────────────── Sub-components ─────────────── */

function ClassBadge({ cls }) {
    const map = {
        beat: { bg: colors.greenBg, color: colors.green, label: 'BEAT' },
        miss: { bg: colors.redBg, color: colors.red, label: 'MISS' },
        inline: { bg: colors.yellowBg, color: colors.yellow, label: 'INLINE' },
        pending: { bg: colors.card, color: colors.textMuted, label: 'PENDING' },
    };
    const c = map[cls] || map.pending;
    return (
        <span style={{
            ...shared.badge(c.bg), color: c.color,
            fontSize: '10px', letterSpacing: '1px', fontWeight: 700,
        }}>
            {c.label}
        </span>
    );
}

function VerdictBadge({ verdict }) {
    const map = {
        hit: { bg: colors.greenBg, color: colors.green, label: 'HIT' },
        miss: { bg: colors.redBg, color: colors.red, label: 'MISS' },
        partial: { bg: colors.yellowBg, color: colors.yellow, label: 'PARTIAL' },
        pending: { bg: colors.card, color: colors.textMuted, label: 'PENDING' },
    };
    const c = map[verdict] || map.pending;
    return (
        <span style={{
            ...shared.badge(c.bg), color: c.color,
            fontSize: '10px', letterSpacing: '1px', fontWeight: 700,
        }}>
            {c.label}
        </span>
    );
}

function TickerBadge({ ticker, daysUntil }) {
    return (
        <div style={{
            display: 'inline-flex', alignItems: 'center', gap: '6px',
            padding: '4px 10px', borderRadius: tokens.radius.sm,
            background: colors.bg, border: `1px solid ${colors.border}`,
        }}>
            <span style={{ fontFamily: colors.mono, fontSize: '13px', fontWeight: 700, color: '#E8F0F8' }}>
                {ticker}
            </span>
            {daysUntil != null && (
                <span style={{
                    fontSize: '10px', color: daysUntil <= 2 ? colors.yellow : colors.textMuted,
                    fontWeight: 600,
                }}>
                    {daysLabel(daysUntil)}
                </span>
            )}
        </div>
    );
}

/* ─────────────── Calendar Timeline ─────────────── */

function CalendarTimeline({ entries }) {
    if (!entries || entries.length === 0) {
        return <div style={{ color: colors.textMuted, padding: tokens.space.lg }}>No upcoming earnings</div>;
    }

    // Group by date
    const grouped = {};
    entries.forEach(e => {
        const d = e.earnings_date || 'Unknown';
        if (!grouped[d]) grouped[d] = [];
        grouped[d].push(e);
    });

    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: tokens.space.sm }}>
            {Object.entries(grouped).map(([dateStr, tickers]) => (
                <div key={dateStr} style={{
                    ...shared.card, display: 'flex', alignItems: 'center', gap: tokens.space.md,
                    flexWrap: 'wrap',
                }}>
                    <div style={{
                        minWidth: '80px', textAlign: 'center',
                        padding: '6px 10px', borderRadius: tokens.radius.sm,
                        background: colors.bg,
                    }}>
                        <div style={{ fontSize: '13px', fontWeight: 700, color: '#E8F0F8', fontFamily: colors.mono }}>
                            {fmtDate(dateStr)}
                        </div>
                        <div style={{ fontSize: '10px', color: colors.textMuted }}>
                            {tickers[0]?.days_until != null ? daysLabel(tickers[0].days_until) : ''}
                        </div>
                    </div>
                    <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap', flex: 1 }}>
                        {tickers.map(t => (
                            <TickerBadge key={t.ticker} ticker={t.ticker} daysUntil={t.days_until} />
                        ))}
                    </div>
                    <div style={{ fontSize: '11px', color: colors.textMuted }}>
                        {tickers.length} ticker{tickers.length > 1 ? 's' : ''}
                    </div>
                </div>
            ))}
        </div>
    );
}

/* ─────────────── Pre-Earnings Card ─────────────── */

function PreEarningsCard({ entry, onPredict }) {
    return (
        <div style={shared.card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.sm }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: tokens.space.sm }}>
                    <span style={{ fontFamily: colors.mono, fontSize: '15px', fontWeight: 700, color: '#E8F0F8' }}>
                        {entry.ticker}
                    </span>
                    <span style={{ fontSize: '11px', color: colors.textMuted }}>
                        {fmtDate(entry.earnings_date)}
                    </span>
                    <span style={{
                        fontSize: '10px', fontWeight: 600,
                        color: (entry.days_until || 99) <= 3 ? colors.yellow : colors.textDim,
                    }}>
                        {daysLabel(entry.days_until)}
                    </span>
                </div>
                {!entry.prediction && (
                    <button
                        onClick={() => onPredict(entry.ticker)}
                        style={{ ...shared.buttonSmall, padding: '6px 12px', fontSize: '11px' }}
                    >
                        Predict
                    </button>
                )}
            </div>

            <div style={shared.metricGrid}>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{fmt(entry.eps_estimate)}</div>
                    <div style={shared.metricLabel}>EPS Est</div>
                </div>
                {entry.iv_atm != null && (
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{(entry.iv_atm * 100).toFixed(1)}%</div>
                        <div style={shared.metricLabel}>IV</div>
                    </div>
                )}
                {entry.iv_rank != null && (
                    <div style={shared.metric}>
                        <div style={{
                            ...shared.metricValue,
                            color: entry.iv_rank > 70 ? colors.red : entry.iv_rank < 30 ? colors.green : '#E8F0F8',
                        }}>{entry.iv_rank.toFixed(0)}</div>
                        <div style={shared.metricLabel}>IV Rank</div>
                    </div>
                )}
                {entry.expected_move_options != null && (
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{entry.expected_move_options.toFixed(1)}%</div>
                        <div style={shared.metricLabel}>Exp Move</div>
                    </div>
                )}
            </div>

            {entry.prediction && (
                <div style={{
                    marginTop: tokens.space.sm, padding: tokens.space.sm,
                    background: colors.bg, borderRadius: tokens.radius.sm,
                    display: 'flex', alignItems: 'center', gap: tokens.space.md,
                }}>
                    <span style={{
                        fontSize: '12px', fontWeight: 700, fontFamily: colors.mono,
                        color: entry.prediction.direction === 'up' ? colors.green
                            : entry.prediction.direction === 'down' ? colors.red
                            : colors.textMuted,
                    }}>
                        {entry.prediction.direction?.toUpperCase()} {fmtPct(entry.prediction.move_pct)}
                    </span>
                    <span style={{ fontSize: '11px', color: colors.textMuted }}>
                        Confidence: {((entry.prediction.confidence || 0) * 100).toFixed(0)}%
                    </span>
                    <VerdictBadge verdict={entry.prediction.verdict} />
                </div>
            )}
        </div>
    );
}

/* ─────────────── Post-Earnings Card ─────────────── */

function PostEarningsCard({ entry }) {
    const surpriseColor = (entry.eps_surprise_pct || 0) > 0 ? colors.green
        : (entry.eps_surprise_pct || 0) < 0 ? colors.red : colors.textMuted;

    return (
        <div style={shared.card}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.sm }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: tokens.space.sm }}>
                    <span style={{ fontFamily: colors.mono, fontSize: '15px', fontWeight: 700, color: '#E8F0F8' }}>
                        {entry.ticker}
                    </span>
                    <ClassBadge cls={entry.classification} />
                </div>
                <span style={{ fontSize: '11px', color: colors.textMuted }}>
                    {fmtDate(entry.earnings_date)}
                </span>
            </div>

            <div style={shared.metricGrid}>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{fmt(entry.eps_estimate)}</div>
                    <div style={shared.metricLabel}>EPS Est</div>
                </div>
                <div style={shared.metric}>
                    <div style={shared.metricValue}>{fmt(entry.eps_actual)}</div>
                    <div style={shared.metricLabel}>EPS Act</div>
                </div>
                <div style={shared.metric}>
                    <div style={{ ...shared.metricValue, color: surpriseColor }}>
                        {fmtPct(entry.eps_surprise_pct)}
                    </div>
                    <div style={shared.metricLabel}>Surprise</div>
                </div>
                {entry.revenue_estimate != null && (
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{(entry.revenue_estimate / 1e9).toFixed(1)}B</div>
                        <div style={shared.metricLabel}>Rev Est</div>
                    </div>
                )}
                {entry.revenue_actual != null && (
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{(entry.revenue_actual / 1e9).toFixed(1)}B</div>
                        <div style={shared.metricLabel}>Rev Act</div>
                    </div>
                )}
            </div>
        </div>
    );
}

/* ─────────────── Scorecard Section ─────────────── */

function Scorecard({ data }) {
    if (!data || !data.overall) return null;
    const o = data.overall;

    return (
        <div>
            <div style={shared.sectionTitle}>PREDICTION TRACK RECORD</div>
            <div style={shared.cardGradient}>
                <div style={shared.metricGrid}>
                    <div style={shared.metric}>
                        <div style={{
                            ...shared.metricValue,
                            color: o.accuracy_pct >= 60 ? colors.green
                                : o.accuracy_pct >= 40 ? colors.yellow : colors.red,
                            fontSize: '22px',
                        }}>
                            {o.accuracy_pct}%
                        </div>
                        <div style={shared.metricLabel}>Accuracy</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{o.total_scored}</div>
                        <div style={shared.metricLabel}>Scored</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.green }}>{o.hits}</div>
                        <div style={shared.metricLabel}>Hits</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.red }}>{o.misses}</div>
                        <div style={shared.metricLabel}>Misses</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={{ ...shared.metricValue, color: colors.yellow }}>{o.partials}</div>
                        <div style={shared.metricLabel}>Partial</div>
                    </div>
                    <div style={shared.metric}>
                        <div style={shared.metricValue}>{o.pending}</div>
                        <div style={shared.metricLabel}>Pending</div>
                    </div>
                </div>

                {/* Calibration */}
                {data.calibration && data.calibration.length > 0 && (
                    <div style={{ marginTop: tokens.space.lg }}>
                        <div style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '1px', color: colors.textMuted, marginBottom: tokens.space.sm }}>
                            CONFIDENCE CALIBRATION
                        </div>
                        <div style={{ display: 'flex', gap: tokens.space.sm }}>
                            {data.calibration.map(c => (
                                <div key={c.bucket} style={{
                                    flex: 1, textAlign: 'center', padding: tokens.space.sm,
                                    background: colors.bg, borderRadius: tokens.radius.sm,
                                }}>
                                    <div style={{ fontSize: '14px', fontWeight: 700, color: '#E8F0F8', fontFamily: colors.mono }}>
                                        {c.accuracy_pct}%
                                    </div>
                                    <div style={{ fontSize: '10px', color: colors.textMuted, textTransform: 'uppercase' }}>
                                        {c.bucket} ({c.total})
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                )}
            </div>

            {/* Recent predictions with outcomes */}
            {data.recent && data.recent.length > 0 && (
                <div style={{ marginTop: tokens.space.md }}>
                    <div style={shared.sectionTitle}>RECENT PREDICTIONS</div>
                    {data.recent.slice(0, 10).map((p, i) => (
                        <div key={i} style={{
                            ...shared.row, gap: tokens.space.sm, fontSize: '12px',
                        }}>
                            <span style={{ fontFamily: colors.mono, fontWeight: 700, color: '#E8F0F8', minWidth: '50px' }}>
                                {p.ticker}
                            </span>
                            <span style={{ color: colors.textMuted, minWidth: '60px' }}>{fmtDate(p.earnings_date)}</span>
                            <span style={{
                                fontWeight: 600, minWidth: '50px',
                                color: p.predicted_direction === 'up' ? colors.green
                                    : p.predicted_direction === 'down' ? colors.red : colors.textMuted,
                            }}>
                                {p.predicted_direction?.toUpperCase()}
                            </span>
                            <span style={{ color: colors.textMuted, minWidth: '60px' }}>
                                {fmtPct(p.predicted_move_pct)}
                            </span>
                            {p.actual_move_pct != null && (
                                <span style={{
                                    fontFamily: colors.mono, minWidth: '60px',
                                    color: p.actual_move_pct >= 0 ? colors.green : colors.red,
                                }}>
                                    {fmtPct(p.actual_move_pct)}
                                </span>
                            )}
                            <VerdictBadge verdict={p.verdict} />
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

/* ─────────────── Main View ─────────────── */

const TABS = ['Calendar', 'Upcoming', 'Reported', 'Scorecard'];

export default function EarningsCalendar() {
    const [tab, setTab] = useState('Calendar');
    const [calendar, setCalendar] = useState([]);
    const [recent, setRecent] = useState([]);
    const [scorecard, setScorecard] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    const fetchData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const [calRes, recRes, scRes] = await Promise.all([
                api.getEarningsCalendar(60),
                api.getRecentEarnings(30),
                api.getEarningsScorecard(),
            ]);
            if (!calRes?.error) setCalendar(calRes?.entries || []);
            if (!recRes?.error) setRecent(recRes?.entries || []);
            if (!scRes?.error) setScorecard(scRes);
        } catch (e) {
            setError(e.message || 'Failed to load earnings data');
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => { fetchData(); }, [fetchData]);

    const handlePredict = async (ticker) => {
        try {
            await api.predictEarnings(ticker);
            fetchData();
        } catch (e) {
            console.error('Prediction failed:', e);
        }
    };

    return (
        <div style={shared.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: tokens.space.lg }}>
                <h1 style={shared.header}>Earnings Calendar</h1>
                <button onClick={fetchData} style={shared.buttonSmall} disabled={loading}>
                    {loading ? 'Loading...' : 'Refresh'}
                </button>
            </div>

            {error && <div style={shared.error}>{error}</div>}

            {/* Tabs */}
            <div style={shared.tabs}>
                {TABS.map(t => (
                    <button key={t} onClick={() => setTab(t)} style={shared.tab(tab === t)}>
                        {t}
                    </button>
                ))}
            </div>

            {/* Tab content */}
            {tab === 'Calendar' && (
                <div>
                    <div style={shared.sectionTitle}>UPCOMING EARNINGS TIMELINE</div>
                    <CalendarTimeline entries={calendar} />
                </div>
            )}

            {tab === 'Upcoming' && (
                <div>
                    <div style={shared.sectionTitle}>PRE-EARNINGS ANALYSIS</div>
                    {calendar.length === 0 && !loading && (
                        <div style={{ color: colors.textMuted, padding: tokens.space.lg }}>
                            No upcoming earnings found. Run the earnings data pull first.
                        </div>
                    )}
                    {calendar.map(e => (
                        <PreEarningsCard key={`${e.ticker}-${e.earnings_date}`} entry={e} onPredict={handlePredict} />
                    ))}
                </div>
            )}

            {tab === 'Reported' && (
                <div>
                    <div style={shared.sectionTitle}>RECENT EARNINGS RESULTS</div>
                    {recent.length === 0 && !loading && (
                        <div style={{ color: colors.textMuted, padding: tokens.space.lg }}>
                            No recent earnings data. Run the earnings data pull first.
                        </div>
                    )}
                    {recent.map(e => (
                        <PostEarningsCard key={`${e.ticker}-${e.earnings_date}`} entry={e} />
                    ))}
                </div>
            )}

            {tab === 'Scorecard' && (
                <Scorecard data={scorecard} />
            )}
        </div>
    );
}
