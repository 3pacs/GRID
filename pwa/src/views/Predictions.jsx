import React, { useState, useEffect, useRef, useCallback } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { useAsyncData } from '../hooks/useAsyncData.js';
import LoadingSkeleton from '../components/LoadingSkeleton.jsx';
import ErrorState from '../components/ErrorState.jsx';
import { humanizeOr } from '../utils/text.js';

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
        lineHeight: '1.3',
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
                    verdict === 'partial' ? colors.yellowBg :
                    verdict === 'miss' ? colors.redBg : colors.bg,
        color: verdict === 'hit' ? colors.green :
               verdict === 'partial' ? colors.yellow :
               verdict === 'miss' ? colors.red : colors.textMuted,
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

export function PredictionCard({ pred }) {
    const [expanded, setExpanded] = useState(false);
    const isCall = pred.direction === 'CALL' || pred.direction === 'LONG';
    // D-H11: a prediction that stated no confidence has none to render. `|| 0`
    // printed "0%" and drew an empty bar, which reads as a measured
    // no-confidence call. It reads "unscored" — the same word
    // components/ConfidenceMeter.jsx uses for an unmeasured confidence — and a
    // genuine measured 0 still renders "0%". The check is `== null`, never
    // falsiness, because 0 is a measurement.
    const confPct = pred.confidence == null
        ? null
        : Math.round(pred.confidence * 100);
    const signals = pred.signals || [];
    const antiSignals = pred.anti_signals || [];
    const isDirectional = isCall || pred.direction === 'PUT' || pred.direction === 'SHORT';
    const confirmingSignals = isDirectional
        ? signals.filter(s => s.direction === (isCall ? 'bullish' : 'bearish')) : [];
    const opposingSignals = isDirectional
        ? signals.filter(s => s.direction === (isCall ? 'bearish' : 'bullish')) : [];

    const expiryStr = pred.days_left != null
        ? pred.days_left > 0 ? `${pred.days_left}d left` : 'expired'
        : pred.expiry ? pred.expiry : '';

    const trackingPnl = pred.tracking_pnl;
    const pnlColor = trackingPnl > 0 ? colors.green : trackingPnl < 0 ? colors.red : colors.textDim;

    return (
        <div style={s.predCard} onClick={() => setExpanded(!expanded)}>
            <div style={s.predHeader}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={isDirectional ? s.tickerBadge(isCall) : { ...s.tickerBadge(false), color: colors.textMuted, background: colors.bg }}>
                        {isDirectional ? (isCall ? '\u25B2' : '\u25BC') : '—'} {pred.ticker}
                    </span>
                    <span style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                        {(pred.model_name || '').replace(/_/g, ' ')}
                    </span>
                    {pred.verdict && pred.verdict !== 'pending' && (
                        <span style={s.verdictBadge(pred.verdict)}>
                            {pred.verdict === 'no_data' ? 'UNSCORABLE' : pred.verdict.toUpperCase()}
                        </span>
                    )}
                </div>
                <div style={{ textAlign: 'right' }}>
                    <div style={{
                        fontSize: confPct == null ? '12px' : '16px',
                        fontWeight: 800, fontFamily: mono,
                        color: confPct == null ? colors.textMuted
                            : confPct > 60 ? colors.green
                            : confPct > 30 ? colors.yellow : colors.textMuted,
                    }} title={confPct == null ? 'No confidence was stated for this prediction' : undefined}>
                        {confPct == null ? 'unscored' : `${confPct}%`}
                    </div>
                    <div style={{ fontSize: '9px', color: colors.textMuted, letterSpacing: '1px' }}>STATED CONF</div>
                </div>
            </div>

            {/* An unscored prediction gets a hatched track, not a 0%-wide fill:
                an empty bar is how a measured 0% confidence looks. */}
            {confPct == null ? (
                <div style={{ ...s.confBar, background: 'repeating-linear-gradient(45deg, #1A2840 0 4px, #0D1520 4px 8px)' }}
                    title="No confidence was stated for this prediction" />
            ) : (
                <div style={s.confBar}>
                    <div style={s.confFill(confPct)} />
                </div>
            )}

            <div style={s.predMeta}>
                <div style={s.predMetric}>
                    {/* D-M32: entry_price is null when no spot was measured at
                        publish time. `x?.toFixed(2) || '---'` never fired for the
                        old 0.0 (not nullish) and printed "$0.00"; an explicit
                        null check also keeps the "$" off the empty case. */}
                    <div style={s.predMetricVal}>
                        {pred.entry_price == null ? '---' : `$${pred.entry_price.toFixed(2)}`}
                    </div>
                    <div style={s.predMetricLbl}>REFERENCE ENTRY</div>
                </div>
                <div style={s.predMetric}>
                    <div style={s.predMetricVal}>${pred.target_price?.toFixed(2) || '---'}</div>
                    <div style={s.predMetricLbl}>TARGET</div>
                </div>
                <div style={s.predMetric}>
                    <div style={{
                        ...s.predMetricVal,
                        color: isDirectional ? (isCall ? colors.green : colors.red) : colors.textMuted,
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
                        <div style={s.predMetricLbl}>REFERENCE MOVE</div>
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

            <div style={{ marginTop: '8px', fontSize: '11px', color: colors.textDim }}>
                <strong>{pred.recording_policy === 'oracle_forward_v2' ? 'PROSPECTIVE OBSERVED REFERENCE'
                    : pred.recording_policy === 'oracle_external_v2' ? 'EXTERNAL RESEARCH' : 'LEGACY / UNQUALIFIED'}</strong>
                <div>Recorded decision: {pred.created_at || 'unknown'}</div>
                {pred.entry_price_basis?.status === 'measured' && pred.entry_price_basis?.market_at ? (
                    <div>Entry quote: {pred.entry_price_basis.provider} · observed {pred.entry_price_basis.market_at}
                        {' · registered '}{pred.entry_price_basis.registered_at || pred.entry_price_basis.available_at}
                        {' · read by '}{pred.entry_price_basis.read_observed_at || 'unknown'}. Delayed reference; not an executable fill.</div>
                ) : <div>{pred.entry_price_basis?.reason || 'Entry quote source and exact observation time are unqualified.'}</div>}
                {pred.verdict === 'pending' && <div>{pred.tracking_pnl_basis || 'Tracking return is unavailable.'}</div>}
                {pred.tracking_quote?.market_at && <div>Tracking quote: ${pred.tracking_quote.price?.toFixed(2)}
                    {' · '}{pred.tracking_quote.provider} · observed {pred.tracking_quote.market_at}
                    {' · registered '}{pred.tracking_quote.registered_at || pred.tracking_quote.available_at}
                    {' · read by '}{pred.tracking_quote.read_observed_at || 'unknown'}</div>}
            </div>

            {(pred.verdict === 'no_data' || (pred.verdict === 'pending' && pred.days_left <= 0)) && (
                <div style={{ marginTop: '8px', fontSize: '12px', color: colors.textDim }}>
                    {pred.verdict === 'no_data' ? 'Unscorable: ' : 'Awaiting score: '}
                    {pred.score_notes || 'No reason has been recorded.'}
                </div>
            )}

            {expanded && (
                <div style={{ marginTop: '10px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '8px' }}>
                    {confirmingSignals.length > 0 && (
                        <>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.green, letterSpacing: '1px', marginBottom: '4px' }}>
                                CONFIRMING SIGNALS ({confirmingSignals.length})
                            </div>
                            <div style={s.signalRow}>
                                {confirmingSignals.slice(0, 6).map((sig, i) => (
                                    <span key={i} style={s.signalChip(isCall ? 'bull' : 'bear')}>
                                        {sig.name} z={sig.z_score?.toFixed(1)}
                                    </span>
                                ))}
                            </div>
                        </>
                    )}
                    {opposingSignals.length > 0 && (
                        <>
                            <div style={{ fontSize: '9px', fontWeight: 700, color: colors.red, letterSpacing: '1px', marginTop: '8px', marginBottom: '4px' }}>
                                OPPOSING SIGNALS ({opposingSignals.length})
                            </div>
                            <div style={s.signalRow}>
                                {opposingSignals.slice(0, 6).map((sig, i) => (
                                    <span key={i} style={s.signalChip(isCall ? 'bear' : 'bull')}>
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


// ── Evidence by model ─────────────────────────────────────────────────────
// A historical relative comparison, not a forward trading-edge qualification.

const VERDICT_LABEL = {
    relative_advantage: { text: 'RELATIVE ADVANTAGE', color: colors.text },
    no_relative_advantage: { text: 'NO RELATIVE ADVANTAGE', color: colors.textMuted },
    insufficient_windows: { text: 'TOO FEW CLUSTERS', color: colors.textMuted },
};

const pct = (v, digits = 2) => (v == null ? 'N/A' : `${v > 0 ? '+' : ''}${v.toFixed(digits)}%`);

export function EvidencePanel({ evidence }) {
    if (!evidence?.summary) return null;
    const sm = evidence.summary;
    const method = evidence.method || {};
    const models = evidence.models || [];
    const relativeCount = sm.models_with_relative_advantage;
    const headline = Number.isInteger(relativeCount) && Number.isInteger(sm.models_tested)
        ? `${relativeCount} of ${sm.models_tested} testable models show a relative advantage. Financial edge is not demonstrated.`
        : 'Relative comparison count unavailable.';
    return (
        <div style={{ ...shared.card, padding: '14px 16px', marginBottom: '12px' }}>
            <div style={s.sectionTitle}>EXPLORATORY MODEL COMPARISON</div>
            <div data-testid="evidence-headline" style={{ fontSize: '13px', fontWeight: 700, color: colors.text, marginBottom: '6px' }}>
                {headline}
            </div>
            <div style={{ fontSize: '11px', color: colors.textDim, fontFamily: mono, lineHeight: 1.5, marginBottom: '10px' }}>
                {sm.outcomes} outcomes over {sm.windows} expiry-date clusters. Official verdicts end {sm.official_last_expiry || 'N/A'};
                {' '}{sm.corrected_outcomes} more are corrected rescores (hindsight, through {sm.corrected_last_expiry || 'N/A'}).
                {' '}Gross excess = return minus same-direction calls on the same expiry. Lower bound: expiry-clustered bootstrap.
                {' '}Overlapping horizons may remain dependent. This mixes historical and hindsight outcomes; relative advantage can still lose money and does not establish prospective trading performance.
                {sm.excluded_implausible > 0 && ` ${sm.excluded_implausible} implausible outcomes (price/split errors) excluded.`}
                {sm.models_hidden_small > 0 && ` ${sm.models_hidden_small} models with <${method.min_outcomes_shown} outcomes not listed.`}
            </div>
            <details>
                <summary style={{ cursor: 'pointer', fontSize: '12px', color: colors.textDim, padding: '6px 0' }}>
                    View {models.length} model comparisons
                </summary>
            <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', borderCollapse: 'collapse', fontFamily: mono, fontSize: '11px' }}>
                    <thead>
                        <tr style={{ color: colors.textMuted, textAlign: 'right' }}>
                            <th style={{ textAlign: 'left', padding: '4px' }}>MODEL</th>
                            <th style={{ padding: '4px' }}>CALLS</th>
                            <th style={{ padding: '4px' }}>CLUSTERS</th>
                            <th style={{ padding: '4px' }}>GROSS</th>
                            <th style={{ padding: '4px' }}>RELATIVE</th>
                            <th style={{ padding: '4px' }}>LOWER BOUND</th>
                            <th style={{ padding: '4px' }}>VERDICT</th>
                        </tr>
                    </thead>
                    <tbody>
                        {models.map(m => {
                            const v = VERDICT_LABEL[m.verdict] || { text: 'UNCLASSIFIED', color: colors.textMuted };
                            return (
                                <tr key={m.name} style={{ borderTop: `1px solid ${colors.borderSubtle}`, textAlign: 'right', color: colors.textDim }}>
                                    <td style={{ textAlign: 'left', padding: '4px', color: colors.text }} title={`${m.first_created?.slice(0, 10)} to ${m.last_created?.slice(0, 10)}`}>
                                        {humanizeOr(m.name, 'unnamed model')}
                                    </td>
                                    <td style={{ padding: '4px' }}>{m.outcomes}</td>
                                    <td style={{ padding: '4px', color: m.windows >= method.min_windows ? colors.text : colors.textMuted }}>{m.windows}</td>
                                    <td style={{ padding: '4px' }}>{pct(m.raw_mean_pct)}</td>
                                    <td style={{ padding: '4px' }}>{pct(m.excess_mean_pct)}</td>
                                    <td style={{ padding: '4px' }}>{pct(m.lower_bound_pct)}</td>
                                    <td style={{ padding: '4px', color: v.color, fontWeight: 700 }}>{v.text}</td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
            </details>
        </div>
    );
}


// ── Main View ─────────────────────────────────────────────────────────────

const PREDICTION_TABS = {
    active: { status: 'active', label: 'Active' },
    awaiting: { status: 'expired', label: 'Awaiting score' },
    unscorable: { status: 'unscorable', label: 'Unscorable' },
    scored: { status: 'scored', label: 'Scored' },
    other: { status: 'other', label: 'Void / other' },
};
const PAGE_SIZE = 100;

export default function Predictions() {
    const [tab, setTab] = useState('active');
    const [predictions, setPredictions] = useState([]);
    const [filterTicker, setFilterTicker] = useState('');
    const [filterModel, setFilterModel] = useState('');
    const [cohort, setCohort] = useState('prospective');
    const [predTotal, setPredTotal] = useState(0);
    const [predLoading, setPredLoading] = useState(true);
    const [predError, setPredError] = useState(null);
    const [predOffset, setPredOffset] = useState(0);
    const [hasMore, setHasMore] = useState(false);
    const predictionRequest = useRef(0);

    const { data: scoreData, loading, error, refetch: loadScoreboard } = useAsyncData(async () => {
        const [scoreboardRes, latestRes] = await Promise.all([
            api.getOracleScoreboard(),
            api.getOracleLatest(),
        ]);
        if (scoreboardRes?.error) throw new Error(scoreboardRes.message || 'Failed to load oracle scoreboard');
        if (latestRes?.error) throw new Error(latestRes.message || 'Failed to load oracle latest results');
        return { scoreboard: scoreboardRes, latest: latestRes };
    }, { fallback: { scoreboard: null, latest: null } });
    const scoreboard = scoreData.scoreboard;
    const latest = scoreData.latest;
    const { data: evidenceRes, error: evidenceError, loading: evidenceLoading, refetch: loadEvidence } = useAsyncData(async () => {
        const res = await api.getOracleEvidence();
        if (res?.error) throw new Error(res.message || 'Model evidence unavailable');
        if (!res?.summary) throw new Error('Model evidence response is incomplete');
        return res;
    }, { fallback: null });

    const fetchPredictions = useCallback(async (status) => {
        const request = ++predictionRequest.current;
        setPredLoading(true);
        setPredError(null);
        setPredictions([]);
        setPredTotal(0);
        setHasMore(false);
        try {
            const params = { status, cohort, limit: String(PAGE_SIZE), offset: String(predOffset) };
            if (filterTicker) params.ticker = filterTicker;
            if (filterModel) params.model = filterModel;
            const data = await api.getOraclePredictions(params);
            if (request !== predictionRequest.current) return;
            if (data?.error) throw new Error(data.message || 'Predictions unavailable');
            if (!Array.isArray(data?.predictions)) throw new Error('Prediction response is incomplete');
            setPredictions(data.predictions || []);
            setPredTotal(data.total || 0);
            setHasMore(data.has_more === true);
        } catch (e) {
            if (request === predictionRequest.current) setPredError(e);
        } finally {
            if (request === predictionRequest.current) setPredLoading(false);
        }
    }, [filterTicker, filterModel, predOffset, cohort]);

    useEffect(() => {
        const selected = PREDICTION_TABS[tab];
        if (selected) fetchPredictions(selected.status);
        return () => { predictionRequest.current += 1; };
    }, [tab, fetchPredictions]);

    const overall = scoreboard?.overall || {};
    const models = scoreboard?.models || [];
    const byTicker = scoreboard?.by_ticker || [];
    const calibration = scoreboard?.calibration || {};
    const prospective = scoreboard?.recording_cohorts?.oracle_forward_v2;
    const legacy = scoreboard?.recording_cohorts?.legacy;
    const streak = latest?.streak || {};
    const recentScored = latest?.recent_scored || [];

    const accuracy = overall.accuracy || 0;
    const scored = overall.scored || 0;
    const meanReturn = overall.mean_signed_return_pct;
    const accColor = accuracy > 0.55 ? colors.green : accuracy > 0.45 ? colors.yellow : colors.red;

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

            <div style={s.header}>
                <span>Oracle Predictions</span>
                <span style={s.headerAccent}>TRACK RECORD</span>
            </div>

            {loading && !scoreboard ? (
                <LoadingSkeleton variant="card" count={3} />
            ) : error ? (
                <ErrorState error={error} onRetry={loadScoreboard} title="Oracle scoreboard unavailable" />
            ) : (
            <>
            {/* ── Scoreboard Header ─────────────────────────────── */}
            <div style={{ ...shared.card, padding: '12px', marginBottom: '12px' }}>
                <div style={s.sectionTitle}>PROSPECTIVE OBSERVED REFERENCES</div>
                <div>{prospective?.total ?? '—'} recorded · {prospective?.pending ?? '—'} pending · {prospective?.unscorable ?? '—'} abstained / unscorable · {prospective?.scored ?? '—'} scored</div>
                <div style={s.subStat}>Immutable forecasts with a stated quote basis. Cboe quotes can be delayed; these are not executable fills or financial-edge evidence.</div>
                <div style={s.subStat}>{legacy?.total ?? '—'} legacy records remain separate. The historical aggregate below includes all recording policies.</div>
            </div>
            <div style={s.scoreboardRow}>
                <div style={s.scoreCard}>
                    {scored > 0 ? <RingChart value={accuracy} color={accColor} /> : <div style={s.bigNumber}>—</div>}
                    <div style={{ ...s.bigLabel, marginTop: '4px' }}>WEIGHTED OUTCOME SCORE</div>
                    <div style={s.subStat}>{scored} official verdicts; hit=1, partial=0.5, miss=0</div>
                    <div style={s.subStat}>Forecasts are not independent</div>
                </div>
                <div style={s.scoreCard}>
                    <div style={{ ...s.bigNumber, color: meanReturn > 0 ? colors.green : meanReturn < 0 ? colors.red : colors.text }}>
                        {meanReturn == null ? '—' : `${meanReturn > 0 ? '+' : ''}${meanReturn.toFixed(2)}%`}
                    </div>
                    <div style={s.bigLabel}>MEAN SIGNED FORECAST RETURN</div>
                    <div style={s.subStat}>Gross official outcomes; costs excluded</div>
                    <div style={s.subStat}>Not portfolio P&L or executed trades</div>
                </div>
                <div style={s.scoreCard}>
                    {calibration?.label ? (
                        <span style={s.calBadge(calibration.label)}>
                            {(calibration.label || '').replace(/_/g, ' ').toUpperCase()}
                        </span>
                    ) : (
                        <span style={{ fontSize: '12px', color: colors.textMuted }}>---</span>
                    )}
                    <div style={{ ...s.bigLabel, marginTop: '8px' }}>HISTORICAL STRICT-HIT CALIBRATION</div>
                    <div style={s.subStat}>Diagnostic of stated confidence; not validated probabilities</div>
                    <div style={s.subStat}>Strict hit: correct direction and at least half the stated expected move. Partial and miss count as 0.</div>
                    {calibration?.total_predictions > 0 && calibration?.brier_score != null && (
                        <div style={s.subStat}>Brier: {calibration.brier_score.toFixed(3)}</div>
                    )}
                    {calibration?.total_predictions === 0 && <div style={s.subStat}>No measured strict-hit outcomes for calibration.</div>}
                </div>
            </div>

            {/* ── Quick stats row ──────────────────────────────── */}
            <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap' }}>
                {[
                    { label: 'TOTAL', value: overall.total_predictions, color: colors.text },
                    { label: 'ACTIVE', value: overall.active, color: colors.accent },
                    { label: 'AWAITING SCORE', value: overall.awaiting_score, color: colors.yellow },
                    { label: 'UNSCORABLE', value: overall.unscorable, color: colors.textMuted },
                    { label: 'SCORED', value: overall.scored, color: colors.text },
                    { label: 'VOID / OTHER', key: 'other', value: overall.other, color: colors.textMuted },
                ].map(st => (
                    <div key={st.label} data-testid={`lifecycle-${st.key || st.label.toLowerCase().replaceAll(' ', '-')}`} style={{ ...shared.card, padding: '6px 12px', flex: '1 1 60px', textAlign: 'center', marginBottom: 0 }}>
                        <div style={{ fontSize: '16px', fontWeight: 700, fontFamily: mono, color: st.color }}>{st.value ?? '—'}</div>
                        <div style={{ fontSize: '8px', fontWeight: 700, letterSpacing: '1px', color: colors.textMuted }}>{st.label}</div>
                    </div>
                ))}
            </div>

            <div style={s.subStat}>{overall.hits ?? '—'} hits / {overall.partials ?? '—'} partial / {overall.misses ?? '—'} misses. Awaiting score includes expiry dates through {overall.lifecycle_as_of || 'the current date'}; the session may not have closed yet.</div>

            {/* Evidence failures remain visible beside the historic headline. */}
            {evidenceError ? <ErrorState error={evidenceError} onRetry={loadEvidence} title="Model evidence unavailable" />
                : evidenceLoading ? <LoadingSkeleton variant="card" count={1} />
                    : <EvidencePanel evidence={evidenceRes} />}

            {/* ── Tabs ─────────────────────────────────────────── */}
            <div style={s.tabs}>
                {[...Object.keys(PREDICTION_TABS), 'track_record', 'calibration'].map(t => (
                    <button key={t} style={s.tab(tab === t)} onClick={() => { setPredOffset(0); setTab(t); }}>
                        {PREDICTION_TABS[t]?.label || (t === 'track_record' ? 'Track Record' : 'Calibration')}
                    </button>
                ))}
            </div>

            {/* ── Active / Scored Predictions ──────────────────── */}
            {PREDICTION_TABS[tab] && (
                <>
                    <div style={s.filterRow}>
                        <select aria-label="Recording cohort" style={s.filterSelect} value={cohort}
                            onChange={e => { setPredOffset(0); setCohort(e.target.value); }}>
                            <option value="prospective">Prospective references</option>
                            <option value="legacy">Legacy / unqualified</option>
                            <option value="external">External research</option>
                            <option value="all">All recording policies</option>
                        </select>
                        <input
                            style={s.filterSelect}
                            placeholder="Ticker..."
                            value={filterTicker}
                            onChange={e => { setPredOffset(0); setFilterTicker(e.target.value.toUpperCase()); }}
                        />
                        <select
                            style={s.filterSelect}
                            value={filterModel}
                            onChange={e => { setPredOffset(0); setFilterModel(e.target.value); }}
                        >
                            <option value="">All Models</option>
                            {models.map(m => (
                                <option key={m.name} value={m.name}>
                                    {humanizeOr(m.name, 'unnamed model')}
                                </option>
                            ))}
                        </select>
                        <span style={{ fontSize: '11px', color: colors.textMuted, fontFamily: mono }}>
                            {!predLoading && !predError && `${predTotal} result${predTotal !== 1 ? 's' : ''}`}
                        </span>
                    </div>
                    {predLoading ? <LoadingSkeleton variant="card" count={2} />
                        : predError ? <ErrorState error={predError} onRetry={() => fetchPredictions(PREDICTION_TABS[tab].status)} title="Predictions unavailable" />
                        : predictions.length === 0 ? (
                        <div style={s.emptyState}>
                            No {PREDICTION_TABS[tab].label.toLowerCase()} predictions found.
                        </div>
                    ) : (
                        predictions.map((pred, i) => (
                            <div key={pred.id} className="pred-card-anim" style={{ animationDelay: `${i * 0.04}s` }}>
                                <PredictionCard pred={pred} />
                            </div>
                        ))
                    )}
                    {!predLoading && !predError && (predOffset > 0 || hasMore) && (
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                            <button style={shared.buttonSmall} disabled={predOffset === 0} onClick={() => setPredOffset(Math.max(0, predOffset - PAGE_SIZE))}>Previous</button>
                            <span style={s.subStat}>{predOffset + 1}–{predOffset + predictions.length} of {predTotal}</span>
                            <button style={shared.buttonSmall} disabled={!hasMore} onClick={() => setPredOffset(predOffset + PAGE_SIZE)}>Next</button>
                        </div>
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
                                    { label: 'STRICT-HIT RATE', value: `${(calibration.overall_accuracy * 100).toFixed(1)}%`, desc: `${calibration.total_predictions} predictions; partial = 0` },
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
            </>
            )}
        </div>
    );
}
