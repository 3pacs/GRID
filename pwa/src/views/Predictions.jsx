import React, { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

// ── Styles ────────────────────────────────────────────────────────────────

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const s = {
    container: { padding: tokens.space.lg, maxWidth: '960px', margin: '0 auto' },
    header: {
        fontSize: '20px', fontWeight: 700, color: '#E8F0F8',
        fontFamily: colors.sans, marginBottom: tokens.space.lg,
        display: 'flex', alignItems: 'center', gap: '10px',
    },
    headerAccent: { color: colors.accent, fontFamily: mono, fontSize: '11px', letterSpacing: '2px' },

    // Scoreboard header
    scoreboardRow: {
        display: 'grid', gridTemplateColumns: '1fr 1fr 1fr',
        gap: '10px', marginBottom: tokens.space.lg,
    },
    scoreCard: {
        ...shared.cardGradient,
        display: 'flex', flexDirection: 'column', alignItems: 'center',
        justifyContent: 'center', padding: '16px 12px', textAlign: 'center',
    },
    bigNumber: {
        fontSize: '32px', fontWeight: 800, fontFamily: mono,
        lineHeight: 1.1,
    },
    bigLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        color: colors.textMuted, marginTop: '4px', fontFamily: mono,
    },
    subStat: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono, marginTop: '4px',
    },

    // Ring chart
    ringWrap: { position: 'relative', width: '80px', height: '80px', margin: '0 auto 6px' },
    ringLabel: {
        position: 'absolute', top: '50%', left: '50%',
        transform: 'translate(-50%, -50%)',
        fontSize: '18px', fontWeight: 800, fontFamily: mono, color: '#E8F0F8',
    },

    // Model bars
    modelBar: {
        display: 'flex', alignItems: 'center', gap: '10px',
        padding: '8px 0', borderBottom: `1px solid ${colors.borderSubtle}`,
    },
    modelName: {
        fontSize: '12px', fontWeight: 600, color: colors.text,
        width: '120px', flexShrink: 0, whiteSpace: 'nowrap',
        overflow: 'hidden', textOverflow: 'ellipsis',
    },
    modelBarOuter: {
        flex: 1, height: '16px', background: colors.bg,
        borderRadius: '8px', overflow: 'hidden', position: 'relative',
    },
    modelBarInner: (pct, color) => ({
        height: '100%', borderRadius: '8px',
        background: `linear-gradient(90deg, ${color}CC, ${color})`,
        width: `${Math.max(2, pct)}%`,
        transition: `width 0.6s cubic-bezier(0.4, 0, 0.2, 1)`,
    }),
    modelStat: { fontSize: '11px', fontFamily: mono, color: colors.textDim, width: '50px', textAlign: 'right' },

    // Tabs
    tabs: { ...shared.tabs, marginBottom: '12px' },
    tab: (active) => shared.tab(active),

    // Prediction card
    predCard: {
        ...shared.card,
        padding: '14px 16px', marginBottom: '8px',
        transition: `all ${tokens.transition.normal}`,
    },
    predHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: '8px',
    },
    tickerBadge: (isCall) => ({
        display: 'inline-flex', alignItems: 'center', gap: '6px',
        padding: '4px 12px', borderRadius: '20px',
        fontSize: '13px', fontWeight: 700, fontFamily: mono,
        background: isCall ? colors.greenBg : colors.redBg,
        color: isCall ? colors.green : colors.red,
    }),
    confBar: {
        height: '4px', background: colors.bg, borderRadius: '2px',
        overflow: 'hidden', margin: '6px 0',
    },
    confFill: (pct) => ({
        height: '100%', borderRadius: '2px',
        width: `${pct}%`,
        background: pct > 60 ? colors.green : pct > 30 ? colors.yellow : colors.textMuted,
        transition: 'width 0.4s ease',
    }),
    predMeta: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(80px, 1fr))',
        gap: '6px', marginTop: '8px',
    },
    predMetric: {
        background: colors.bg, borderRadius: '6px', padding: '6px 8px', textAlign: 'center',
    },
    predMetricVal: { fontSize: '13px', fontWeight: 700, fontFamily: mono, color: '#E8F0F8' },
    predMetricLbl: { fontSize: '9px', color: colors.textMuted, letterSpacing: '0.5px', marginTop: '1px' },

    // Signal chips
    signalRow: { display: 'flex', flexWrap: 'wrap', gap: '4px', marginTop: '8px' },
    signalChip: (type) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '2px 8px', borderRadius: '4px',
        fontSize: '10px', fontFamily: mono, fontWeight: 600,
        background: type === 'bull' ? `${colors.green}18` :
                    type === 'bear' ? `${colors.red}18` : `${colors.textMuted}18`,
        color: type === 'bull' ? colors.green : type === 'bear' ? colors.red : colors.textMuted,
    }),
    antiLabel: {
        fontSize: '9px', fontWeight: 700, color: colors.red,
        letterSpacing: '1px', marginTop: '8px', marginBottom: '4px',
    },

    // Verdict badges
    verdictBadge: (verdict) => ({
        display: 'inline-flex', alignItems: 'center',
        padding: '3px 10px', borderRadius: '4px',
        fontSize: '11px', fontWeight: 700, fontFamily: mono,
        background: verdict === 'hit' ? colors.greenBg :
                    verdict === 'partial' ? colors.yellowBg : colors.redBg,
        color: verdict === 'hit' ? colors.green :
               verdict === 'partial' ? colors.yellow : colors.red,
    }),

    // Track record
    streakBanner: (type) => ({
        ...shared.card,
        display: 'flex', alignItems: 'center', justifyContent: 'center', gap: '8px',
        padding: '10px 16px', marginBottom: '10px',
        borderLeft: `3px solid ${type === 'win' ? colors.green : colors.red}`,
    }),
    streakText: { fontSize: '13px', fontWeight: 600, fontFamily: mono, color: '#E8F0F8' },

    // Heatmap
    heatmapGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(70px, 1fr))',
        gap: '4px', marginTop: '8px',
    },
    heatmapCell: (accuracy) => {
        const h = accuracy > 0.6 ? 142 : accuracy > 0.4 ? 45 : 0;
        const s = accuracy > 0 ? 60 : 0;
        const l = 15 + accuracy * 25;
        return {
            padding: '8px 4px', borderRadius: '6px', textAlign: 'center',
            background: `hsl(${h}, ${s}%, ${l}%)`,
            border: `1px solid ${colors.borderSubtle}`,
        };
    },
    heatmapTicker: { fontSize: '11px', fontWeight: 700, fontFamily: mono, color: '#E8F0F8' },
    heatmapAcc: { fontSize: '10px', fontFamily: mono, color: colors.textDim, marginTop: '2px' },

    // Calibration chart
    calibChart: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: '16px', marginTop: '10px',
    },

    // Section titles
    sectionTitle: { ...shared.sectionTitle, marginTop: '20px', marginBottom: '10px' },

    // Loading / empty
    emptyState: {
        textAlign: 'center', padding: '40px 20px', color: colors.textMuted,
        fontSize: '14px',
    },
    loadingBar: {
        height: '2px', background: colors.bg, borderRadius: '1px',
        overflow: 'hidden', marginBottom: '12px',
    },
    loadingFill: {
        height: '100%', width: '30%', background: colors.accent,
        borderRadius: '1px',
        animation: 'loadSlide 1.2s ease-in-out infinite',
    },

    // Calibration badge
    calBadge: (label) => ({
        display: 'inline-flex', alignItems: 'center', gap: '4px',
        padding: '4px 10px', borderRadius: '12px',
        fontSize: '11px', fontWeight: 700, fontFamily: mono,
        background: label === 'well_calibrated' ? colors.greenBg :
                    label === 'overconfident' ? colors.redBg : colors.yellowBg,
        color: label === 'well_calibrated' ? colors.green :
               label === 'overconfident' ? colors.red : colors.yellow,
    }),

    // Filter row
    filterRow: {
        display: 'flex', gap: '8px', flexWrap: 'wrap',
        marginBottom: '12px', alignItems: 'center',
    },
    filterSelect: {
        ...shared.input,
        width: 'auto', minWidth: '120px', padding: '8px 10px',
        fontSize: '12px', minHeight: '36px',
    },
};


// ── Ring Chart (SVG) ──────────────────────────────────────────────────────

function RingChart({ value, size = 80, strokeWidth = 6, color = colors.green }) {
    const radius = (size - strokeWidth) / 2;
    const circumference = 2 * Math.PI * radius;
    const offset = circumference * (1 - Math.min(1, Math.max(0, value)));

    return (
        <div style={s.ringWrap}>
            <svg width={size} height={size} style={{ transform: 'rotate(-90deg)' }}>
                <circle cx={size/2} cy={size/2} r={radius}
                    fill="none" stroke={colors.bg} strokeWidth={strokeWidth} />
                <circle cx={size/2} cy={size/2} r={radius}
                    fill="none" stroke={color} strokeWidth={strokeWidth}
                    strokeDasharray={circumference}
                    strokeDashoffset={offset}
                    strokeLinecap="round"
                    style={{ transition: 'stroke-dashoffset 0.8s cubic-bezier(0.4, 0, 0.2, 1)' }} />
            </svg>
            <span style={s.ringLabel}>{Math.round(value * 100)}%</span>
        </div>
    );
}


// ── Calibration Chart (Canvas) ────────────────────────────────────────────

function CalibrationChart({ buckets }) {
    const canvasRef = useRef(null);

    useEffect(() => {
        if (!canvasRef.current || !buckets?.length) return;
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');
        const dpr = window.devicePixelRatio || 2;
        const w = canvas.offsetWidth;
        const h = canvas.offsetHeight;
        canvas.width = w * dpr;
        canvas.height = h * dpr;
        ctx.scale(dpr, dpr);
        ctx.clearRect(0, 0, w, h);

        const pad = { top: 20, right: 20, bottom: 35, left: 45 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;

        // Grid lines
        ctx.strokeStyle = colors.borderSubtle;
        ctx.lineWidth = 0.5;
        for (let i = 0; i <= 4; i++) {
            const y = pad.top + ch * (1 - i / 4);
            ctx.beginPath(); ctx.moveTo(pad.left, y); ctx.lineTo(w - pad.right, y); ctx.stroke();
        }
        for (let i = 0; i <= 4; i++) {
            const x = pad.left + cw * i / 4;
            ctx.beginPath(); ctx.moveTo(x, pad.top); ctx.lineTo(x, pad.top + ch); ctx.stroke();
        }

        // Axis labels
        ctx.fillStyle = colors.textMuted;
        ctx.font = `10px ${mono}`;
        ctx.textAlign = 'center';
        for (let i = 0; i <= 4; i++) {
            ctx.fillText(`${i * 25}%`, pad.left + cw * i / 4, h - 8);
        }
        ctx.textAlign = 'right';
        for (let i = 0; i <= 4; i++) {
            ctx.fillText(`${i * 25}%`, pad.left - 6, pad.top + ch * (1 - i / 4) + 3);
        }

        // Axis titles
        ctx.fillStyle = colors.textMuted;
        ctx.font = `9px ${mono}`;
        ctx.textAlign = 'center';
        ctx.fillText('PREDICTED CONFIDENCE', pad.left + cw / 2, h - 0);
        ctx.save();
        ctx.translate(10, pad.top + ch / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.fillText('ACTUAL RATE', 0, 0);
        ctx.restore();

        // Perfect calibration diagonal
        ctx.strokeStyle = `${colors.accent}60`;
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.beginPath();
        ctx.moveTo(pad.left, pad.top + ch);
        ctx.lineTo(pad.left + cw, pad.top);
        ctx.stroke();
        ctx.setLineDash([]);

        // Plot buckets
        const populated = buckets.filter(b => b.count > 0);
        if (populated.length > 0) {
            // Scatter dots
            populated.forEach(b => {
                const x = pad.left + b.predicted_mean * cw;
                const y = pad.top + ch * (1 - b.actual_rate);
                const r = Math.max(4, Math.min(12, Math.sqrt(b.count) * 2));

                ctx.beginPath();
                ctx.arc(x, y, r, 0, Math.PI * 2);
                ctx.fillStyle = `${colors.accent}40`;
                ctx.fill();
                ctx.strokeStyle = colors.accent;
                ctx.lineWidth = 1.5;
                ctx.stroke();
            });

            // Trend line through populated buckets
            if (populated.length >= 2) {
                ctx.strokeStyle = colors.accent;
                ctx.lineWidth = 2;
                ctx.beginPath();
                populated.forEach((b, i) => {
                    const x = pad.left + b.predicted_mean * cw;
                    const y = pad.top + ch * (1 - b.actual_rate);
                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);
                });
                ctx.stroke();
            }
        }
    }, [buckets]);

    return (
        <div style={s.calibChart}>
            <div style={s.sectionTitle}>CALIBRATION CURVE</div>
            <canvas
                ref={canvasRef}
                style={{ width: '100%', height: '220px', display: 'block' }}
            />
        </div>
    );
}


// ── Prediction Card ───────────────────────────────────────────────────────

function PredictionCard({ pred }) {
    const [expanded, setExpanded] = useState(false);
    const isCall = pred.direction === 'CALL' || pred.direction === 'LONG';
    const confPct = Math.round((pred.confidence || 0) * 100);
    const signals = pred.signals || [];
    const antiSignals = pred.anti_signals || [];
    const bullSignals = signals.filter(s => s.direction === 'bullish');
    const bearSignals = signals.filter(s => s.direction === 'bearish');

    const expiryStr = pred.days_left != null
        ? pred.days_left > 0 ? `${pred.days_left}d left` : 'expired'
        : pred.expiry ? pred.expiry : '';

    const trackingPnl = pred.tracking_pnl;
    const pnlColor = trackingPnl > 0 ? colors.green : trackingPnl < 0 ? colors.red : colors.textDim;

    return (
        <div style={s.predCard} onClick={() => setExpanded(!expanded)}>
            <div style={s.predHeader}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={s.tickerBadge(isCall)}>
                        {isCall ? '\u25B2' : '\u25BC'} {pred.ticker}
                    </span>
                    <span style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                        {(pred.model_name || '').replace(/_/g, ' ')}
                    </span>
                    {pred.verdict && pred.verdict !== 'pending' && (
                        <span style={s.verdictBadge(pred.verdict)}>
                            {pred.verdict.toUpperCase()}
                        </span>
                    )}
                </div>
                <div style={{ textAlign: 'right' }}>
                    <div style={{
                        fontSize: '16px', fontWeight: 800, fontFamily: mono,
                        color: confPct > 60 ? colors.green : confPct > 30 ? colors.yellow : colors.textMuted,
                    }}>
                        {confPct}%
                    </div>
                    <div style={{ fontSize: '9px', color: colors.textMuted, letterSpacing: '1px' }}>CONF</div>
                </div>
            </div>

            <div style={s.confBar}>
                <div style={s.confFill(confPct)} />
            </div>

            <div style={s.predMeta}>
                <div style={s.predMetric}>
                    <div style={s.predMetricVal}>${pred.entry_price?.toFixed(2) || '---'}</div>
                    <div style={s.predMetricLbl}>ENTRY</div>
                </div>
                <div style={s.predMetric}>
                    <div style={s.predMetricVal}>${pred.target_price?.toFixed(2) || '---'}</div>
                    <div style={s.predMetricLbl}>TARGET</div>
                </div>
                <div style={s.predMetric}>
                    <div style={{
                        ...s.predMetricVal,
                        color: isCall ? colors.green : colors.red,
                    }}>
                        {pred.expected_move_pct ? `${pred.expected_move_pct > 0 ? '+' : ''}${pred.expected_move_pct.toFixed(1)}%` : '---'}
                    </div>
                    <div style={s.predMetricLbl}>EXPECTED</div>
                </div>
                <div style={s.predMetric}>
                    <div style={s.predMetricVal}>{expiryStr}</div>
                    <div style={s.predMetricLbl}>EXPIRY</div>
                </div>
                {trackingPnl != null && (
                    <div style={s.predMetric}>
                        <div style={{ ...s.predMetricVal, color: pnlColor }}>
                            {trackingPnl > 0 ? '+' : ''}{trackingPnl.toFixed(1)}%
                        </div>
                        <div style={s.predMetricLbl}>P&L</div>
                    </div>
                )}
                {pred.pnl_pct != null && (
                    <div style={s.predMetric}>
                        <div style={{
                            ...s.predMetricVal,
                            color: pred.pnl_pct > 0 ? colors.green : pred.pnl_pct < 0 ? colors.red : colors.textDim,
                        }}>
                            {pred.pnl_pct > 0 ? '+' : ''}{pred.pnl_pct.toFixed(1)}%
                        </div>
                        <div style={s.predMetricLbl}>RESULT</div>
                    </div>
                )}
            </div>

            {expanded && (
                <div style={{ marginTop: '10px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '8px' }}>
                    {bullSignals.length > 0 && (
                        <>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.green, letterSpacing: '1px', marginBottom: '4px' }}>
                                CONFIRMING SIGNALS ({bullSignals.length})
                            </div>
                            <div style={s.signalRow}>
                                {bullSignals.slice(0, 6).map((sig, i) => (
                                    <span key={i} style={s.signalChip('bull')}>
                                        {sig.name} z={sig.z_score?.toFixed(1)}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}
                    {bearSignals.length > 0 && (
                        <>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.red, letterSpacing: '1px', marginTop: '8px', marginBottom: '4px' }}>
                                OPPOSING SIGNALS ({bearSignals.length})
                            </div>
                            <div style={s.signalRow}>
                                {bearSignals.slice(0, 6).map((sig, i) => (
                                    <span key={i} style={s.signalChip('bear')}>
                                        {sig.name} z={sig.z_score?.toFixed(1)}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}
                    {antiSignals.length > 0 && (
                        <>
                            <div style={s.antiLabel}>ANTI-SIGNALS ({antiSignals.length})</div>
                            <div style={s.signalRow}>
                                {antiSignals.slice(0, 4).map((a, i) => (
                                    <span key={i} style={s.signalChip('bear')}>
                                        {a.name} sev={a.severity?.toFixed(2)}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}
                    <div style={{ marginTop: '8px', fontSize: '10px', color: colors.textMuted, fontFamily: mono }}>
                        Coherence: {((pred.coherence || 0) * 100).toFixed(0)}%
                        {' | '}Signal: {(pred.signal_strength || 0).toFixed(2)}
                        {pred.score_notes ? ` | ${pred.score_notes}` : ''}
                    </div>
                </div>
            )}
        </div>
    );
}


// ── Main View ─────────────────────────────────────────────────────────────

export default function Predictions() {
    const [tab, setTab] = useState('active');
    const [scoreboard, setScoreboard] = useState(null);
    const [predictions, setPredictions] = useState([]);
    const [latest, setLatest] = useState(null);
    const [loading, setLoading] = useState(true);
    const [filterTicker, setFilterTicker] = useState('');
    const [filterModel, setFilterModel] = useState('');
    const [predTotal, setPredTotal] = useState(0);

    const fetchScoreboard = useCallback(async () => {
        try {
            const data = await api._fetch('/api/v1/oracle/scoreboard');
            setScoreboard(data);
        } catch (e) {
            console.warn('Scoreboard fetch failed:', e);
        }
    }, []);

    const fetchPredictions = useCallback(async (status) => {
        try {
            const params = new URLSearchParams({ status, limit: '100' });
            if (filterTicker) params.set('ticker', filterTicker);
            if (filterModel) params.set('model', filterModel);
            const data = await api._fetch(`/api/v1/oracle/predictions?${params}`);
            setPredictions(data.predictions || []);
            setPredTotal(data.total || 0);
        } catch (e) {
            console.warn('Predictions fetch failed:', e);
            setPredictions([]);
        }
    }, [filterTicker, filterModel]);

    const fetchLatest = useCallback(async () => {
        try {
            const data = await api._fetch('/api/v1/oracle/latest');
            setLatest(data);
        } catch (e) {
            console.warn('Latest fetch failed:', e);
        }
    }, []);

    useEffect(() => {
        setLoading(true);
        Promise.all([fetchScoreboard(), fetchLatest()]).finally(() => setLoading(false));
    }, []);

    useEffect(() => {
        const statusMap = { active: 'active', scored: 'scored', expired: 'expired' };
        if (statusMap[tab]) {
            fetchPredictions(statusMap[tab]);
        }
    }, [tab, filterTicker, filterModel]);

    const overall = scoreboard?.overall || {};
    const models = scoreboard?.models || [];
    const byTicker = scoreboard?.by_ticker || [];
    const calibration = scoreboard?.calibration || {};
    const streak = latest?.streak || {};
    const recentScored = latest?.recent_scored || [];

    const accuracy = overall.accuracy || 0;
    const scored = overall.scored || 0;
    const accColor = accuracy > 0.55 ? colors.green : accuracy > 0.45 ? colors.yellow : colors.red;

    // Sort models by accuracy for tournament
    const sortedModels = [...models].sort((a, b) => (b.accuracy || 0) - (a.accuracy || 0));

    return (
        <div style={s.container}>
            {/* Inject animation keyframes */}
            <style>{`
                @keyframes loadSlide {
                    0% { transform: translateX(-100%); }
                    50% { transform: translateX(250%); }
                    100% { transform: translateX(-100%); }
                }
                @keyframes fadeIn {
                    from { opacity: 0; transform: translateY(6px); }
                    to { opacity: 1; transform: translateY(0); }
                }
                .pred-card-anim { animation: fadeIn 0.3s ease both; }
            `}</style>

            {loading && (
                <div style={s.loadingBar}><div style={s.loadingFill} /></div>
            )}

            <div style={s.header}>
                <span>Oracle Predictions</span>
                <span style={s.headerAccent}>TRACK RECORD</span>
            </div>

            {/* ── Scoreboard Header ─────────────────────────────── */}
            <div style={s.scoreboardRow}>
                <div style={s.scoreCard}>
                    <RingChart value={accuracy} color={accColor} />
                    <div style={{ ...s.bigLabel, marginTop: '4px' }}>ACCURACY</div>
                    <div style={s.subStat}>{scored} scored predictions</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: overall.total_pnl > 0 ? colors.green : overall.total_pnl < 0 ? colors.red : colors.text }}>
                        {overall.total_pnl > 0 ? '+' : ''}{(overall.total_pnl || 0).toFixed(1)}%
                    </div>
                    <div style={s.bigLabel}>CUMULATIVE P&L</div>
                    <div style={s.subStat}>avg {(overall.avg_pnl || 0).toFixed(2)}% per trade</div>
                </div>
                <div style={s.scoreCard}>
                    {calibration?.label ? (
                        <span style={s.calBadge(calibration.label)}>
                            {(calibration.label || '').replace(/_/g, ' ').toUpperCase()}
                        </span>
                    ) : (
                        <span style={{ fontSize: '12px', color: colors.textMuted }}>---</span>
                    )}
                    <div style={{ ...s.bigLabel, marginTop: '8px' }}>CALIBRATION</div>
                    {calibration?.brier_score != null && (
                        <div style={s.subStat}>Brier: {calibration.brier_score.toFixed(3)}</div>
                    )}
                </div>
            </div>

            {/* ── Quick stats row ──────────────────────────────── */}
            <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap' }}>
                {[
                    { label: 'TOTAL', value: overall.total_predictions, color: colors.text },
                    { label: 'HITS', value: overall.hits, color: colors.green },
                    { label: 'PARTIAL', value: overall.partials, color: colors.yellow },
                    { label: 'MISS', value: overall.misses, color: colors.red },
                    { label: 'PENDING', value: overall.pending, color: colors.accent },
                ].map(st => (
                    <div key={st.label} style={{ ...shared.card, padding: '6px 12px', flex: '1 1 60px', textAlign: 'center', marginBottom: 0 }}>
                        <div style={{ fontSize: '16px', fontWeight: 700, fontFamily: mono, color: st.color }}>{st.value || 0}</div>
                        <div style={{ fontSize: '8px', fontWeight: 700, letterSpacing: '1px', color: colors.textMuted }}>{st.label}</div>
                    </div>
                ))}
            </div>

            {/* ── Model Tournament ─────────────────────────────── */}
            {sortedModels.length > 0 && (
                <div style={{ ...shared.card, padding: '14px 16px', marginBottom: '12px' }}>
                    <div style={s.sectionTitle}>MODEL TOURNAMENT</div>
                    {sortedModels.map((m, i) => {
                        const pct = (m.accuracy || 0) * 100;
                        const barColor = i === 0 ? colors.green : i === sortedModels.length - 1 ? colors.red : colors.accent;
                        return (
                            <div key={m.name} style={s.modelBar}>
                                <span style={{ fontSize: '12px', fontFamily: mono, color: colors.textMuted, width: '18px' }}>
                                    {i === 0 ? '\u{1F947}' : i === 1 ? '\u{1F948}' : i === 2 ? '\u{1F949}' : `#${i + 1}`}
                                </span>
                                <span style={s.modelName} title={m.description}>
                                    {m.name.replace(/_/g, ' ')}
                                </span>
                                <div style={s.modelBarOuter}>
                                    <div style={s.modelBarInner(pct, barColor)} />
                                </div>
                                <span style={s.modelStat}>{pct.toFixed(0)}%</span>
                                <span style={{ ...s.modelStat, color: m.cumulative_pnl > 0 ? colors.green : colors.red }}>
                                    {m.cumulative_pnl > 0 ? '+' : ''}{m.cumulative_pnl?.toFixed(1)}%
                                </span>
                            </div>
                        );
                    })}
                </div>
            )}

            {/* ── Tabs ─────────────────────────────────────────── */}
            <div style={s.tabs}>
                {['active', 'scored', 'track_record', 'calibration'].map(t => (
                    <button key={t} style={s.tab(tab === t)} onClick={() => setTab(t)}>
                        {t === 'active' ? 'Active' : t === 'scored' ? 'Scored' : t === 'track_record' ? 'Track Record' : 'Calibration'}
                    </button>
                ))}
            </div>

            {/* ── Active / Scored Predictions ──────────────────── */}
            {(tab === 'active' || tab === 'scored') && (
                <>
                    <div style={s.filterRow}>
                        <input
                            style={s.filterSelect}
                            placeholder="Ticker..."
                            value={filterTicker}
                            onChange={e => setFilterTicker(e.target.value.toUpperCase())}
                        />
                        <select
                            style={s.filterSelect}
                            value={filterModel}
                            onChange={e => setFilterModel(e.target.value)}
                        >
                            <option value="">All Models</option>
                            {models.map(m => (
                                <option key={m.name} value={m.name}>
                                    {m.name.replace(/_/g, ' ')}
                                </option>
                            ))}
                        </select>
                        <span style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                            {predTotal} result{predTotal !== 1 ? 's' : ''}
                        </span>
                    </div>
                    {predictions.length === 0 ? (
                        <div style={s.emptyState}>
                            No {tab} predictions found.
                        </div>
                    ) : (
                        predictions.map((pred, i) => (
                            <div key={pred.id} className="pred-card-anim" style={{ animationDelay: `${i * 0.04}s` }}>
                                <PredictionCard pred={pred} />
                            </div>
                        ))
                    )}
                </>
            )}

            {/* ── Track Record ─────────────────────────────────── */}
            {tab === 'track_record' && (
                <>
                    {/* Streak banner */}
                    {streak.type && streak.type !== 'none' && (
                        <div style={s.streakBanner(streak.type)}>
                            <span style={{ fontSize: '18px' }}>
                                {streak.type === 'win' ? '\u{1F525}' : '\u{1F9CA}'}
                            </span>
                            <span style={s.streakText}>{streak.label}</span>
                        </div>
                    )}

                    {/* Recent scored */}
                    <div style={s.sectionTitle}>RECENT RESULTS</div>
                    {recentScored.length === 0 ? (
                        <div style={s.emptyState}>No scored predictions yet.</div>
                    ) : (
                        recentScored.map((pred, i) => (
                            <div key={pred.id} className="pred-card-anim" style={{ animationDelay: `${i * 0.04}s` }}>
                                <PredictionCard pred={pred} />
                            </div>
                        ))
                    )}

                    {/* Ticker heatmap */}
                    {byTicker.length > 0 && (
                        <>
                            <div style={s.sectionTitle}>TICKER ACCURACY HEATMAP</div>
                            <div style={s.heatmapGrid}>
                                {byTicker.map(t => (
                                    <div key={t.ticker} style={s.heatmapCell(t.accuracy)}>
                                        <div style={s.heatmapTicker}>{t.ticker}</div>
                                        <div style={s.heatmapAcc}>{(t.accuracy * 100).toFixed(0)}%</div>
                                        <div style={{ fontSize: '9px', color: colors.textMuted }}>{t.total} pred</div>
                                    </div>
                                ))}
                            </div>
                        </>
                    )}
                </>
            )}

            {/* ── Calibration ──────────────────────────────────── */}
            {tab === 'calibration' && (
                <>
                    {calibration?.buckets?.length > 0 ? (
                        <>
                            <CalibrationChart buckets={calibration.buckets} />
                            <div style={{ display: 'flex', gap: '12px', marginTop: '10px', flexWrap: 'wrap' }}>
                                {[
                                    { label: 'BRIER SCORE', value: calibration.brier_score?.toFixed(3), desc: '0 = perfect' },
                                    { label: 'CAL ERROR (ECE)', value: calibration.calibration_error?.toFixed(3), desc: 'Lower = better' },
                                    { label: 'SHARPNESS', value: calibration.sharpness?.toFixed(3), desc: 'Confidence spread' },
                                    { label: 'OVERALL ACC', value: `${(calibration.overall_accuracy * 100).toFixed(1)}%`, desc: `${calibration.total_predictions} predictions` },
                                ].map(m => (
                                    <div key={m.label} style={{ ...shared.card, flex: '1 1 120px', textAlign: 'center', padding: '10px', marginBottom: 0 }}>
                                        <div style={{ fontSize: '18px', fontWeight: 700, fontFamily: mono, color: '#E8F0F8' }}>{m.value}</div>
                                        <div style={{ fontSize: '9px', fontWeight: 700, color: colors.textMuted, letterSpacing: '1px', marginTop: '2px' }}>{m.label}</div>
                                        <div style={{ fontSize: '9px', color: colors.textMuted, marginTop: '2px' }}>{m.desc}</div>
                                    </div>
                                ))}
                            </div>

                            {/* Bucket breakdown table */}
                            <div style={{ ...shared.card, marginTop: '10px', padding: '12px', overflowX: 'auto' }}>
                                <div style={s.sectionTitle}>BUCKET BREAKDOWN</div>
                                <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '11px', fontFamily: mono }}>
                                    <thead>
                                        <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                                            <th style={{ padding: '4px 8px', textAlign: 'left', color: colors.textMuted }}>Range</th>
                                            <th style={{ padding: '4px 8px', textAlign: 'right', color: colors.textMuted }}>Predicted</th>
                                            <th style={{ padding: '4px 8px', textAlign: 'right', color: colors.textMuted }}>Actual</th>
                                            <th style={{ padding: '4px 8px', textAlign: 'right', color: colors.textMuted }}>Count</th>
                                            <th style={{ padding: '4px 8px', textAlign: 'right', color: colors.textMuted }}>Gap</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {calibration.buckets.filter(b => b.count > 0).map((b, i) => {
                                            const gap = b.predicted_mean - b.actual_rate;
                                            const gapColor = Math.abs(gap) < 0.05 ? colors.green : Math.abs(gap) < 0.15 ? colors.yellow : colors.red;
                                            return (
                                                <tr key={i} style={{ borderBottom: `1px solid ${colors.borderSubtle}` }}>
                                                    <td style={{ padding: '4px 8px', color: colors.textDim }}>
                                                        {(b.bin_start * 100).toFixed(0)}-{(b.bin_end * 100).toFixed(0)}%
                                                    </td>
                                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: colors.text }}>
                                                        {(b.predicted_mean * 100).toFixed(1)}%
                                                    </td>
                                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: colors.text }}>
                                                        {(b.actual_rate * 100).toFixed(1)}%
                                                    </td>
                                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: colors.textDim }}>
                                                        {b.count}
                                                    </td>
                                                    <td style={{ padding: '4px 8px', textAlign: 'right', color: gapColor }}>
                                                        {gap > 0 ? '+' : ''}{(gap * 100).toFixed(1)}%
                                                    </td>
                                                </tr>
                                            );
                                        })}
                                    </tbody>
                                </table>
                            </div>
                        </>
                    ) : (
                        <div style={s.emptyState}>
                            Calibration data requires scored predictions.
                            <br />Run oracle cycles and score expired predictions to see the calibration curve.
                        </div>
                    )}
                </>
            )}
        </div>
    );
}
