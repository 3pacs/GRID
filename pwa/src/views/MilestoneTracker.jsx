/**
 * MilestoneTracker -- Execution scorecard for 118+ companies.
 * Two modes: Scorecard table (default) and Timeline drill-down per ticker.
 */
import React, { useState, useEffect, useCallback, useRef } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

/* ── Grade color mapping ─────────────────────────────────────── */
const GRADE_COLORS = {
    'A+': '#10B981', A: '#10B981',
    'A-': '#34D399',
    'B+': '#3B82F6', B: '#3B82F6',
    'B-': '#60A5FA',
    'C+': '#F59E0B', C: '#F59E0B',
    'C-': '#FBBF24',
    'D+': '#F97316', D: '#F97316',
    'D-': '#FB923C',
    F: '#EF4444',
};

const gradeColor = (g) => GRADE_COLORS[g] || colors.textDim;

/* ── Beat/Miss/Met dot colors ────────────────────────────────── */
const OUTCOME_COLORS = {
    BEAT: '#10B981',
    MISS: '#EF4444',
    MET: '#4B5563',
};

/* ── Trend arrow helper ──────────────────────────────────────── */
const trendArrow = (trend) => {
    if (trend === 'improving' || trend > 0) return { symbol: '\u25B2', color: '#10B981' };
    if (trend === 'declining' || trend < 0) return { symbol: '\u25BC', color: '#EF4444' };
    return { symbol: '\u25C6', color: colors.textMuted };
};

/* ── Streak badge ────────────────────────────────────────────── */
const streakBadge = (streak, streakType) => {
    if (!streak || streak === 0) return null;
    const isBeat = streakType === 'BEAT' || streakType === 'beat';
    const bg = isBeat ? 'rgba(16,185,129,0.15)' : 'rgba(239,68,68,0.15)';
    const fg = isBeat ? '#10B981' : '#EF4444';
    return { bg, fg, label: `${streak}${isBeat ? 'B' : 'M'}` };
};

/* ── Hover effect ────────────────────────────────────────────── */
const hoverRow = {
    onMouseEnter: (e) => { e.currentTarget.style.background = colors.cardHover; },
    onMouseLeave: (e) => { e.currentTarget.style.background = 'transparent'; },
};

/* ── Sort helpers ────────────────────────────────────────────── */
const GRADE_ORDER = ['A+', 'A', 'A-', 'B+', 'B', 'B-', 'C+', 'C', 'C-', 'D+', 'D', 'D-', 'F'];
const gradeRank = (g) => { const i = GRADE_ORDER.indexOf(g); return i >= 0 ? i : 99; };

/* ================================================================
   SCORECARD VIEW
   ================================================================ */
function ScorecardView({ companies, onSelect, sortCol, sortDir, onSort }) {
    const cols = [
        { key: 'ticker', label: 'TICKER', width: '80px' },
        { key: 'grade', label: 'GRADE', width: '64px' },
        { key: 'beat_rate', label: 'BEAT%', width: '72px' },
        { key: 'beats', label: 'BEATS', width: '56px' },
        { key: 'misses', label: 'MISS', width: '56px' },
        { key: 'total', label: 'TOTAL', width: '56px' },
        { key: 'trend', label: 'TREND', width: '64px' },
        { key: 'streak', label: 'STREAK', width: '72px' },
        { key: 'avg_magnitude', label: 'AVG MAG', width: '80px' },
    ];

    const sortIndicator = (key) => {
        if (sortCol !== key) return '';
        return sortDir === 'asc' ? ' \u25B2' : ' \u25BC';
    };

    return (
        <div style={{ overflowX: 'auto' }}>
            {/* Header row */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: 0,
                borderBottom: `1px solid ${colors.border}`,
                padding: '10px 12px',
                position: 'sticky', top: 0, zIndex: 2,
                background: colors.bg,
            }}>
                {cols.map((c) => (
                    <div
                        key={c.key}
                        onClick={() => onSort(c.key)}
                        style={{
                            width: c.width, minWidth: c.width, flexShrink: 0,
                            fontSize: '10px', fontWeight: 700, letterSpacing: '1.2px',
                            color: sortCol === c.key ? colors.accent : colors.textMuted,
                            fontFamily: MONO, cursor: 'pointer',
                            userSelect: 'none', whiteSpace: 'nowrap',
                        }}
                    >
                        {c.label}{sortIndicator(c.key)}
                    </div>
                ))}
            </div>

            {/* Data rows */}
            {companies.map((co) => {
                const t = trendArrow(co.trend);
                const s = streakBadge(co.streak, co.streak_type);
                return (
                    <div
                        key={co.ticker}
                        onClick={() => onSelect(co.ticker)}
                        style={{
                            display: 'flex', alignItems: 'center', gap: 0,
                            padding: '10px 12px',
                            borderBottom: `1px solid ${colors.borderSubtle}`,
                            cursor: 'pointer',
                            transition: `background ${tokens.transition.fast}`,
                        }}
                        {...hoverRow}
                    >
                        {/* Ticker */}
                        <div style={{
                            width: '80px', minWidth: '80px', flexShrink: 0,
                            fontSize: '13px', fontWeight: 700, color: colors.text,
                            fontFamily: MONO,
                        }}>
                            {co.ticker}
                        </div>
                        {/* Grade */}
                        <div style={{ width: '64px', minWidth: '64px', flexShrink: 0 }}>
                            <span style={{
                                display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                                width: '32px', height: '24px', borderRadius: tokens.radius.sm,
                                fontSize: '12px', fontWeight: 800, fontFamily: MONO,
                                background: `${gradeColor(co.grade)}22`,
                                color: gradeColor(co.grade),
                            }}>
                                {co.grade}
                            </span>
                        </div>
                        {/* Beat% */}
                        <div style={{
                            width: '72px', minWidth: '72px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO,
                            color: co.beat_rate >= 0.7 ? '#10B981' : co.beat_rate >= 0.5 ? colors.text : '#EF4444',
                        }}>
                            {(co.beat_rate * 100).toFixed(0)}%
                        </div>
                        {/* Beats */}
                        <div style={{
                            width: '56px', minWidth: '56px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO, color: '#10B981',
                        }}>
                            {co.beats}
                        </div>
                        {/* Misses */}
                        <div style={{
                            width: '56px', minWidth: '56px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO, color: '#EF4444',
                        }}>
                            {co.misses}
                        </div>
                        {/* Total */}
                        <div style={{
                            width: '56px', minWidth: '56px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO, color: colors.textDim,
                        }}>
                            {co.total}
                        </div>
                        {/* Trend */}
                        <div style={{
                            width: '64px', minWidth: '64px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO, color: t.color,
                        }}>
                            {t.symbol}
                        </div>
                        {/* Streak */}
                        <div style={{ width: '72px', minWidth: '72px', flexShrink: 0 }}>
                            {s ? (
                                <span style={{
                                    display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                                    padding: '2px 8px', borderRadius: tokens.radius.pill,
                                    fontSize: '11px', fontWeight: 700, fontFamily: MONO,
                                    background: s.bg, color: s.fg,
                                }}>
                                    {s.label}
                                </span>
                            ) : (
                                <span style={{ color: colors.textMuted, fontSize: '11px', fontFamily: MONO }}>--</span>
                            )}
                        </div>
                        {/* Avg Magnitude */}
                        <div style={{
                            width: '80px', minWidth: '80px', flexShrink: 0,
                            fontSize: '13px', fontFamily: MONO,
                            color: co.avg_magnitude >= 5 ? '#10B981' : co.avg_magnitude >= 2 ? colors.text : colors.textDim,
                        }}>
                            {co.avg_magnitude != null ? co.avg_magnitude.toFixed(1) + '%' : '--'}
                        </div>
                    </div>
                );
            })}
        </div>
    );
}

/* ================================================================
   TIMELINE VIEW (per-ticker drill-down)
   ================================================================ */
function TimelineView({ ticker, milestones, score, onBack }) {
    const canvasRef = useRef(null);
    const [tooltip, setTooltip] = useState(null);

    const sorted = [...(milestones || [])].sort((a, b) => new Date(a.date) - new Date(b.date));

    /* Draw the timeline + magnitude line on canvas */
    useEffect(() => {
        const canvas = canvasRef.current;
        if (!canvas || sorted.length === 0) return;

        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 1;
        const W = canvas.clientWidth;
        const H = canvas.clientHeight;
        canvas.width = W * dpr;
        canvas.height = H * dpr;
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, W, H);

        const PAD_L = 48, PAD_R = 24, PAD_T = 32, PAD_B = 40;
        const plotW = W - PAD_L - PAD_R;
        const plotH = H - PAD_T - PAD_B;

        const dates = sorted.map(m => new Date(m.date).getTime());
        const minDate = Math.min(...dates);
        const maxDate = Math.max(...dates);
        const dateSpan = maxDate - minDate || 1;

        const mags = sorted.map(m => Math.abs(m.magnitude || 0));
        const maxMag = Math.max(...mags, 1);

        const xOf = (d) => PAD_L + ((d - minDate) / dateSpan) * plotW;
        const yOf = (mag) => PAD_T + plotH - (mag / maxMag) * plotH;

        /* Grid lines */
        ctx.strokeStyle = colors.borderSubtle;
        ctx.lineWidth = 0.5;
        for (let i = 0; i <= 4; i++) {
            const y = PAD_T + (plotH / 4) * i;
            ctx.beginPath();
            ctx.moveTo(PAD_L, y);
            ctx.lineTo(W - PAD_R, y);
            ctx.stroke();
        }

        /* Y-axis labels */
        ctx.fillStyle = colors.textMuted;
        ctx.font = `10px ${MONO}`;
        ctx.textAlign = 'right';
        for (let i = 0; i <= 4; i++) {
            const y = PAD_T + (plotH / 4) * i;
            const val = (maxMag * (4 - i) / 4).toFixed(1);
            ctx.fillText(val + '%', PAD_L - 6, y + 3);
        }

        /* Magnitude line */
        ctx.strokeStyle = `${colors.accent}88`;
        ctx.lineWidth = 1.5;
        ctx.beginPath();
        sorted.forEach((m, i) => {
            const x = xOf(dates[i]);
            const y = yOf(Math.abs(m.magnitude || 0));
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.stroke();

        /* Area fill under magnitude line */
        ctx.fillStyle = `${colors.accent}12`;
        ctx.beginPath();
        sorted.forEach((m, i) => {
            const x = xOf(dates[i]);
            const y = yOf(Math.abs(m.magnitude || 0));
            if (i === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
        });
        ctx.lineTo(xOf(dates[dates.length - 1]), PAD_T + plotH);
        ctx.lineTo(xOf(dates[0]), PAD_T + plotH);
        ctx.closePath();
        ctx.fill();

        /* Dots */
        sorted.forEach((m, i) => {
            const x = xOf(dates[i]);
            const y = yOf(Math.abs(m.magnitude || 0));
            const outcome = (m.beat_miss || '').toUpperCase();
            const dotColor = OUTCOME_COLORS[outcome] || OUTCOME_COLORS.MET;

            ctx.beginPath();
            ctx.arc(x, y, 5, 0, Math.PI * 2);
            ctx.fillStyle = dotColor;
            ctx.fill();
            ctx.strokeStyle = '#0D1520';
            ctx.lineWidth = 2;
            ctx.stroke();
        });

        /* X-axis date labels (first, mid, last) */
        ctx.fillStyle = colors.textMuted;
        ctx.font = `10px ${MONO}`;
        ctx.textAlign = 'center';
        const fmt = (ts) => {
            const d = new Date(ts);
            return `${d.getMonth() + 1}/${d.getDate()}/${String(d.getFullYear()).slice(2)}`;
        };
        if (dates.length > 0) {
            ctx.fillText(fmt(minDate), xOf(minDate), H - 8);
            if (dates.length > 2) {
                const mid = dates[Math.floor(dates.length / 2)];
                ctx.fillText(fmt(mid), xOf(mid), H - 8);
            }
            ctx.fillText(fmt(maxDate), xOf(maxDate), H - 8);
        }
    }, [sorted]);

    /* Canvas hover handler */
    const handleCanvasMove = useCallback((e) => {
        if (sorted.length === 0) return;
        const canvas = canvasRef.current;
        if (!canvas) return;
        const rect = canvas.getBoundingClientRect();
        const mx = e.clientX - rect.left;
        const my = e.clientY - rect.top;

        const W = canvas.clientWidth;
        const H = canvas.clientHeight;
        const PAD_L = 48, PAD_R = 24, PAD_T = 32, PAD_B = 40;
        const plotW = W - PAD_L - PAD_R;
        const plotH = H - PAD_T - PAD_B;
        const dates = sorted.map(m => new Date(m.date).getTime());
        const minDate = Math.min(...dates);
        const maxDate = Math.max(...dates);
        const dateSpan = maxDate - minDate || 1;
        const mags = sorted.map(m => Math.abs(m.magnitude || 0));
        const maxMag = Math.max(...mags, 1);

        const xOf = (d) => PAD_L + ((d - minDate) / dateSpan) * plotW;
        const yOf = (mag) => PAD_T + plotH - (mag / maxMag) * plotH;

        let closest = null;
        let closestDist = Infinity;
        sorted.forEach((m, i) => {
            const x = xOf(dates[i]);
            const y = yOf(Math.abs(m.magnitude || 0));
            const dist = Math.sqrt((mx - x) ** 2 + (my - y) ** 2);
            if (dist < closestDist) {
                closestDist = dist;
                closest = { ...m, x, y, idx: i };
            }
        });

        if (closest && closestDist < 20) {
            setTooltip({
                x: closest.x, y: closest.y,
                date: closest.date,
                category: closest.category,
                description: closest.description,
                beat_miss: closest.beat_miss,
                magnitude: closest.magnitude,
            });
        } else {
            setTooltip(null);
        }
    }, [sorted]);

    const handleCanvasLeave = useCallback(() => setTooltip(null), []);

    return (
        <div>
            {/* Back button + ticker header */}
            <div style={{
                display: 'flex', alignItems: 'center', gap: tokens.space.md,
                marginBottom: tokens.space.lg,
            }}>
                <button
                    onClick={onBack}
                    style={{
                        ...shared.buttonSmall,
                        background: colors.card,
                        border: `1px solid ${colors.border}`,
                        color: colors.textDim,
                    }}
                >
                    \u2190 Back
                </button>
                <div style={{
                    fontSize: tokens.fontSize.xxl, fontWeight: 700,
                    fontFamily: MONO, color: colors.text,
                }}>
                    {ticker}
                </div>
                {score?.grade && (
                    <span style={{
                        display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                        padding: '4px 12px', borderRadius: tokens.radius.sm,
                        fontSize: '14px', fontWeight: 800, fontFamily: MONO,
                        background: `${gradeColor(score.grade)}22`,
                        color: gradeColor(score.grade),
                    }}>
                        {score.grade}
                    </span>
                )}
            </div>

            {/* Score summary cards */}
            {score && (
                <div style={{
                    display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(100px, 1fr))',
                    gap: tokens.space.sm, marginBottom: tokens.space.lg,
                }}>
                    {[
                        { label: 'BEAT RATE', value: `${(score.beat_rate * 100).toFixed(0)}%`, color: score.beat_rate >= 0.7 ? '#10B981' : colors.text },
                        { label: 'BEATS', value: score.beats, color: '#10B981' },
                        { label: 'MISSES', value: score.misses, color: '#EF4444' },
                        { label: 'METS', value: score.mets, color: colors.textDim },
                        { label: 'TOTAL', value: score.total, color: colors.text },
                        { label: 'AVG MAG', value: score.avg_magnitude != null ? score.avg_magnitude.toFixed(1) + '%' : '--', color: colors.accent },
                    ].map((m) => (
                        <div key={m.label} style={{
                            ...shared.card, textAlign: 'center', padding: tokens.space.md,
                        }}>
                            <div style={{ fontSize: '20px', fontWeight: 700, fontFamily: MONO, color: m.color }}>
                                {m.value}
                            </div>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.2px',
                                color: colors.textMuted, fontFamily: MONO, marginTop: '4px',
                            }}>
                                {m.label}
                            </div>
                        </div>
                    ))}
                </div>
            )}

            {/* Timeline canvas */}
            <div style={{
                ...shared.card, padding: tokens.space.lg,
                position: 'relative', marginBottom: tokens.space.lg,
            }}>
                <div style={{
                    ...shared.sectionTitle, marginBottom: tokens.space.md,
                }}>
                    MILESTONE TIMELINE
                </div>
                <div style={{ position: 'relative' }}>
                    <canvas
                        ref={canvasRef}
                        style={{ width: '100%', height: '220px', display: 'block' }}
                        onMouseMove={handleCanvasMove}
                        onMouseLeave={handleCanvasLeave}
                    />
                    {/* Tooltip overlay */}
                    {tooltip && (
                        <div style={{
                            position: 'absolute',
                            left: tooltip.x + 12, top: tooltip.y - 60,
                            background: colors.glassOverlay,
                            backdropFilter: 'blur(12px)',
                            WebkitBackdropFilter: 'blur(12px)',
                            border: `1px solid ${colors.border}`,
                            borderRadius: tokens.radius.sm,
                            padding: '10px 14px',
                            pointerEvents: 'none',
                            zIndex: 10, maxWidth: '260px',
                            boxShadow: colors.shadow.md,
                        }}>
                            <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO }}>
                                {tooltip.date}
                            </div>
                            <div style={{
                                fontSize: '12px', color: colors.text, fontFamily: SANS,
                                marginTop: '4px', lineHeight: '1.4',
                            }}>
                                {tooltip.description || tooltip.category}
                            </div>
                            <div style={{
                                display: 'flex', gap: tokens.space.sm, marginTop: '6px',
                                alignItems: 'center',
                            }}>
                                <span style={{
                                    fontSize: '11px', fontWeight: 700, fontFamily: MONO,
                                    color: OUTCOME_COLORS[(tooltip.beat_miss || '').toUpperCase()] || colors.textDim,
                                }}>
                                    {(tooltip.beat_miss || '').toUpperCase()}
                                </span>
                                {tooltip.magnitude != null && (
                                    <span style={{
                                        fontSize: '11px', fontFamily: MONO, color: colors.accent,
                                    }}>
                                        {tooltip.magnitude > 0 ? '+' : ''}{tooltip.magnitude.toFixed(1)}%
                                    </span>
                                )}
                            </div>
                        </div>
                    )}
                </div>
                {/* Legend */}
                <div style={{
                    display: 'flex', gap: tokens.space.lg, marginTop: tokens.space.md,
                    justifyContent: 'center',
                }}>
                    {[
                        { label: 'BEAT', color: OUTCOME_COLORS.BEAT },
                        { label: 'MISS', color: OUTCOME_COLORS.MISS },
                        { label: 'MET', color: OUTCOME_COLORS.MET },
                    ].map((l) => (
                        <div key={l.label} style={{
                            display: 'flex', alignItems: 'center', gap: '6px',
                        }}>
                            <div style={{
                                width: 10, height: 10, borderRadius: '50%',
                                background: l.color,
                            }} />
                            <span style={{
                                fontSize: '10px', fontFamily: MONO, color: colors.textMuted,
                                letterSpacing: '0.5px',
                            }}>
                                {l.label}
                            </span>
                        </div>
                    ))}
                </div>
            </div>

            {/* Milestone list */}
            <div style={shared.card}>
                <div style={shared.sectionTitle}>MILESTONES</div>
                {sorted.map((m, i) => {
                    const outcome = (m.beat_miss || '').toUpperCase();
                    return (
                        <div key={i} style={{
                            display: 'flex', alignItems: 'flex-start', gap: tokens.space.md,
                            padding: '10px 0',
                            borderBottom: i < sorted.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                        }}>
                            {/* Dot */}
                            <div style={{
                                width: 10, height: 10, borderRadius: '50%', flexShrink: 0,
                                marginTop: '4px',
                                background: OUTCOME_COLORS[outcome] || OUTCOME_COLORS.MET,
                            }} />
                            {/* Content */}
                            <div style={{ flex: 1, minWidth: 0 }}>
                                <div style={{
                                    display: 'flex', justifyContent: 'space-between',
                                    alignItems: 'center', gap: tokens.space.sm,
                                }}>
                                    <span style={{
                                        fontSize: '12px', fontFamily: MONO, color: colors.textDim,
                                    }}>
                                        {m.date}
                                    </span>
                                    <div style={{ display: 'flex', gap: tokens.space.sm, alignItems: 'center' }}>
                                        <span style={{
                                            fontSize: '11px', fontWeight: 700, fontFamily: MONO,
                                            color: OUTCOME_COLORS[outcome] || colors.textDim,
                                        }}>
                                            {outcome}
                                        </span>
                                        {m.magnitude != null && (
                                            <span style={{
                                                fontSize: '11px', fontFamily: MONO, color: colors.accent,
                                            }}>
                                                {m.magnitude > 0 ? '+' : ''}{m.magnitude.toFixed(1)}%
                                            </span>
                                        )}
                                    </div>
                                </div>
                                {m.category && (
                                    <span style={{
                                        fontSize: '10px', fontWeight: 600, fontFamily: MONO,
                                        color: colors.accent, letterSpacing: '0.8px',
                                        marginTop: '2px', display: 'block',
                                    }}>
                                        {m.category.toUpperCase()}
                                    </span>
                                )}
                                {m.description && (
                                    <div style={{
                                        fontSize: '12px', color: colors.textDim,
                                        fontFamily: SANS, lineHeight: '1.5', marginTop: '4px',
                                    }}>
                                        {m.description}
                                    </div>
                                )}
                            </div>
                        </div>
                    );
                })}
                {sorted.length === 0 && (
                    <div style={{ color: colors.textMuted, fontSize: '13px', fontFamily: MONO, padding: '20px 0', textAlign: 'center' }}>
                        No milestones found for {ticker}.
                    </div>
                )}
            </div>
        </div>
    );
}

/* ================================================================
   MAIN COMPONENT
   ================================================================ */
export default function MilestoneTracker() {
    const [companies, setCompanies] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [search, setSearch] = useState('');
    const [sortCol, setSortCol] = useState('grade');
    const [sortDir, setSortDir] = useState('asc');

    /* Drill-down state */
    const [selectedTicker, setSelectedTicker] = useState(null);
    const [tickerData, setTickerData] = useState(null);
    const [tickerLoading, setTickerLoading] = useState(false);

    /* ── Load scorecard ──────────────────────────────────────── */
    const loadScorecard = useCallback(async () => {
        setLoading(true);
        setError(null);
        const res = await api.getMilestoneScorecard();
        if (res?.error) {
            setError(res.message || 'Failed to load scorecard');
            setCompanies([]);
        } else {
            setCompanies(res?.companies || []);
        }
        setLoading(false);
    }, []);

    useEffect(() => { loadScorecard(); }, [loadScorecard]);

    /* ── Load ticker timeline ────────────────────────────────── */
    const loadTicker = useCallback(async (ticker) => {
        setSelectedTicker(ticker);
        setTickerLoading(true);
        setTickerData(null);
        const res = await api.getTickerMilestones(ticker);
        if (res?.error) {
            setTickerData({ milestones: [], score: null });
        } else {
            setTickerData({
                milestones: res?.milestones || [],
                score: res?.score || null,
            });
        }
        setTickerLoading(false);
    }, []);

    /* ── Sort logic ──────────────────────────────────────────── */
    const handleSort = useCallback((col) => {
        setSortDir((prev) => (sortCol === col ? (prev === 'asc' ? 'desc' : 'asc') : 'asc'));
        setSortCol(col);
    }, [sortCol]);

    const sorted = [...companies]
        .filter((c) => !search || c.ticker.toLowerCase().includes(search.toLowerCase()))
        .sort((a, b) => {
            const dir = sortDir === 'asc' ? 1 : -1;
            if (sortCol === 'grade') return dir * (gradeRank(a.grade) - gradeRank(b.grade));
            if (sortCol === 'ticker') return dir * a.ticker.localeCompare(b.ticker);
            if (sortCol === 'trend') {
                const tv = (t) => t === 'improving' ? 0 : t === 'declining' ? 2 : 1;
                return dir * (tv(a.trend) - tv(b.trend));
            }
            const av = a[sortCol] ?? 0;
            const bv = b[sortCol] ?? 0;
            return dir * (av - bv);
        });

    /* ── Render ───────────────────────────────────────────────── */
    return (
        <div style={{ ...shared.container, maxWidth: '1100px' }}>
            {/* Header */}
            <div style={{ marginBottom: tokens.space.lg }}>
                <div style={shared.sectionTitle}>EXECUTION INTELLIGENCE</div>
                <div style={{
                    fontSize: tokens.fontSize.xxl, fontWeight: 600, color: '#E8F0F8',
                    fontFamily: SANS,
                }}>
                    Milestone Tracker
                </div>
                <div style={{
                    fontSize: tokens.fontSize.sm, color: colors.textDim,
                    fontFamily: SANS, marginTop: '4px',
                }}>
                    {companies.length} companies tracked &middot; Execution scorecard &amp; timeline analysis
                </div>
            </div>

            {/* Drill-down: ticker timeline */}
            {selectedTicker ? (
                tickerLoading ? (
                    <div style={{
                        textAlign: 'center', padding: '60px 0',
                        color: colors.textDim, fontFamily: MONO, fontSize: '13px',
                    }}>
                        Loading {selectedTicker} timeline...
                    </div>
                ) : (
                    <TimelineView
                        ticker={selectedTicker}
                        milestones={tickerData?.milestones || []}
                        score={tickerData?.score}
                        onBack={() => { setSelectedTicker(null); setTickerData(null); }}
                    />
                )
            ) : (
                <>
                    {/* Search bar */}
                    <div style={{ marginBottom: tokens.space.md }}>
                        <input
                            type="text"
                            placeholder="Search ticker..."
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                            style={{
                                ...shared.input,
                                maxWidth: '280px',
                                fontSize: '13px',
                            }}
                        />
                    </div>

                    {/* Scorecard */}
                    <div style={{
                        ...shared.card, padding: 0, overflow: 'hidden',
                    }}>
                        {loading ? (
                            <div style={{
                                textAlign: 'center', padding: '60px 0',
                                color: colors.textDim, fontFamily: MONO, fontSize: '13px',
                            }}>
                                Loading scorecard...
                            </div>
                        ) : error ? (
                            <div style={{
                                textAlign: 'center', padding: '40px 0',
                                color: colors.red, fontFamily: MONO, fontSize: '13px',
                            }}>
                                {error}
                            </div>
                        ) : sorted.length === 0 ? (
                            <div style={{
                                textAlign: 'center', padding: '40px 0',
                                color: colors.textMuted, fontFamily: MONO, fontSize: '13px',
                            }}>
                                {search ? `No companies matching "${search}"` : 'No scorecard data available.'}
                            </div>
                        ) : (
                            <ScorecardView
                                companies={sorted}
                                onSelect={loadTicker}
                                sortCol={sortCol}
                                sortDir={sortDir}
                                onSort={handleSort}
                            />
                        )}
                    </div>

                    {/* Summary bar */}
                    {!loading && companies.length > 0 && (
                        <div style={{
                            display: 'flex', gap: tokens.space.lg, marginTop: tokens.space.md,
                            justifyContent: 'center', flexWrap: 'wrap',
                        }}>
                            {['A', 'B', 'C', 'D', 'F'].map((g) => {
                                const count = companies.filter((c) => (c.grade || '').startsWith(g)).length;
                                return (
                                    <div key={g} style={{
                                        display: 'flex', alignItems: 'center', gap: '6px',
                                    }}>
                                        <span style={{
                                            display: 'inline-flex', alignItems: 'center', justifyContent: 'center',
                                            width: '24px', height: '20px', borderRadius: tokens.radius.sm,
                                            fontSize: '11px', fontWeight: 800, fontFamily: MONO,
                                            background: `${gradeColor(g)}22`, color: gradeColor(g),
                                        }}>
                                            {g}
                                        </span>
                                        <span style={{
                                            fontSize: '12px', fontFamily: MONO, color: colors.textDim,
                                        }}>
                                            {count}
                                        </span>
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
