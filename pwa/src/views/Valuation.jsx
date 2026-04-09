import React, { useState, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import {
    DollarSign, Search, Copy, Check, Send, Clock,
    BarChart3, AlertTriangle, FileText, Target, Activity,
    Shield, TrendingUp, ChevronDown, ChevronUp,
} from 'lucide-react';

/* ====== Constants ====== */

const QUICK_TICKERS = ['SPY', 'AAPL', 'NVDA', 'TSLA', 'MSFT', 'GOOGL', 'AMZN', 'META'];

const REGIME_COLORS = {
    STRONG_SUPPORT: '#10B981', MILD_SUPPORT: '#3B82F6', NEUTRAL: '#8AA0B8',
    MILD_PRESSURE: '#F59E0B', STRONG_PRESSURE: '#EF4444',
};
const VALUE_COLORS = {
    SIGNIFICANTLY_UNDERVALUED: '#10B981', UNDERVALUED: '#34D399',
    FAIR_VALUE: '#3B82F6', OVERVALUED: '#F59E0B', SIGNIFICANTLY_OVERVALUED: '#EF4444',
};

const scoreColor = (s) => s >= 70 ? '#10B981' : s >= 50 ? '#3B82F6' : s >= 30 ? '#F59E0B' : '#EF4444';
const fmtDollar = (v) => v != null ? `$${Number(v).toFixed(2)}` : '--';
const fmtPct = (v) => v != null ? `${(Number(v) * 100).toFixed(1)}%` : '--';
const fmt = (v, d = 2) => v != null ? Number(v).toFixed(d) : '--';

/* ====== Shimmer Loading ====== */

const shimmerKeyframes = `
@keyframes shimmer {
  0% { background-position: -400px 0; }
  100% { background-position: 400px 0; }
}`;

function ShimmerBlock({ width = '100%', height = '20px', radius = tokens.radius.md }) {
    return (
        <div style={{
            width, height, borderRadius: radius,
            background: `linear-gradient(90deg, ${colors.card} 25%, ${colors.border} 50%, ${colors.card} 75%)`,
            backgroundSize: '800px 100%',
            animation: 'shimmer 1.5s ease-in-out infinite',
        }} />
    );
}

function LoadingSkeleton() {
    return (
        <>
            <style>{shimmerKeyframes}</style>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginBottom: '20px' }}>
                {[1, 2, 3, 4].map(i => (
                    <div key={i} style={{
                        ...shared.card, flex: '1 1 160px', minWidth: '160px', padding: '20px',
                    }}>
                        <ShimmerBlock width="60px" height="10px" />
                        <div style={{ height: '8px' }} />
                        <ShimmerBlock width="100px" height="28px" />
                    </div>
                ))}
            </div>
            <div style={{ ...shared.card, padding: '20px', marginBottom: '20px' }}>
                <ShimmerBlock width="200px" height="14px" />
                <div style={{ height: '16px' }} />
                <ShimmerBlock height="200px" />
            </div>
            <div style={{ display: 'flex', gap: '16px', justifyContent: 'center', marginBottom: '20px' }}>
                {[1, 2, 3].map(i => (
                    <ShimmerBlock key={i} width="100px" height="100px" radius="50%" />
                ))}
            </div>
        </>
    );
}

/* ====== Badge Components ====== */

function SupportBadge({ regime }) {
    const c = REGIME_COLORS[regime] || colors.textMuted;
    return (
        <span style={{
            display: 'inline-flex', alignItems: 'center', gap: '6px',
            padding: '4px 12px', borderRadius: tokens.radius.pill,
            background: `${c}18`, border: `1px solid ${c}40`,
            fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
            color: c, fontFamily: colors.mono,
        }}>
            <span style={{
                width: '6px', height: '6px', borderRadius: '50%',
                background: c, boxShadow: `0 0 6px ${c}`,
            }} />
            {(regime || 'UNKNOWN').replace(/_/g, ' ')}
        </span>
    );
}

function ValueBadge({ value }) {
    const c = VALUE_COLORS[value] || colors.textMuted;
    return (
        <span style={{
            padding: '4px 10px', borderRadius: tokens.radius.pill,
            background: `${c}18`, border: `1px solid ${c}40`,
            fontSize: '10px', fontWeight: 700, letterSpacing: '1px',
            color: c, fontFamily: colors.mono,
        }}>
            {(value || 'UNKNOWN').replace(/_/g, ' ')}
        </span>
    );
}

/* ====== Ring Chart (SVG) ====== */

function RingChart({ score, label, sublabel, size = 90 }) {
    const r = (size / 2) - 8;
    const circumference = 2 * Math.PI * r;
    const offset = circumference * (1 - (score || 0) / 100);
    const c = scoreColor(score || 0);
    return (
        <div style={{ textAlign: 'center', flex: '1 1 120px' }}>
            <svg width={size} height={size}>
                <circle cx={size / 2} cy={size / 2} r={r} fill="none"
                    stroke={colors.border} strokeWidth="6" />
                <circle cx={size / 2} cy={size / 2} r={r} fill="none"
                    stroke={c} strokeWidth="6"
                    strokeDasharray={circumference} strokeDashoffset={offset}
                    transform={`rotate(-90 ${size / 2} ${size / 2})`}
                    strokeLinecap="round"
                    style={{ transition: 'stroke-dashoffset 0.8s ease' }} />
                <text x={size / 2} y={size / 2 + 5} textAnchor="middle"
                    fill={colors.text} fontSize="18" fontWeight="700"
                    fontFamily={colors.mono}>
                    {score != null ? Math.round(score) : '--'}
                </text>
            </svg>
            <div style={{
                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                color: colors.textMuted, marginTop: '6px', fontFamily: colors.mono,
            }}>{label}</div>
            {sublabel && (
                <div style={{ fontSize: '11px', color: colors.textDim, marginTop: '2px' }}>
                    {sublabel}
                </div>
            )}
        </div>
    );
}

/* ====== Price Ruler — where price sits vs intrinsic range ====== */

function PriceRuler({ price, low, mid, high }) {
    if (!price || !low || !high || high <= low) return null;
    const pad = 0.15;
    const rangeMin = Math.min(low, price) * (1 - pad);
    const rangeMax = Math.max(high, price) * (1 + pad);
    const span = rangeMax - rangeMin;
    const pct = (v) => ((v - rangeMin) / span) * 100;

    const pricePos = Math.max(0, Math.min(100, pct(price)));
    const lowPos = Math.max(0, Math.min(100, pct(low)));
    const midPos = mid ? Math.max(0, Math.min(100, pct(mid))) : null;
    const highPos = Math.max(0, Math.min(100, pct(high)));
    const priceInRange = price >= low && price <= high;

    return (
        <div style={{ ...shared.card, padding: '20px', marginTop: '12px' }}>
            <div style={{ ...shared.sectionTitle, marginBottom: '16px' }}>PRICE vs INTRINSIC RANGE</div>
            <div style={{ position: 'relative', height: '48px', margin: '0 20px' }}>
                {/* Background track */}
                <div style={{
                    position: 'absolute', top: '18px', left: 0, right: 0, height: '12px',
                    background: colors.bg, borderRadius: '6px', border: `1px solid ${colors.border}`,
                }} />
                {/* Intrinsic value range bar */}
                <div style={{
                    position: 'absolute', top: '18px', height: '12px',
                    left: `${lowPos}%`, width: `${highPos - lowPos}%`,
                    background: `linear-gradient(90deg, #10B98140, ${colors.accent}40, #10B98140)`,
                    borderRadius: '6px', border: '1px solid #10B98150',
                }} />
                {/* Low marker */}
                <div style={{
                    position: 'absolute', left: `${lowPos}%`, top: '6px',
                    transform: 'translateX(-50%)', textAlign: 'center',
                }}>
                    <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: colors.mono }}>LOW</div>
                </div>
                {/* Mid marker */}
                {midPos != null && (
                    <div style={{
                        position: 'absolute', left: `${midPos}%`, top: '32px',
                        transform: 'translateX(-50%)', textAlign: 'center',
                    }}>
                        <div style={{
                            width: '2px', height: '10px', background: '#10B981',
                            margin: '0 auto',
                        }} />
                        <div style={{
                            fontSize: '10px', color: '#10B981', fontWeight: 700,
                            fontFamily: colors.mono, marginTop: '2px',
                        }}>{fmtDollar(mid)}</div>
                    </div>
                )}
                {/* High marker */}
                <div style={{
                    position: 'absolute', left: `${highPos}%`, top: '6px',
                    transform: 'translateX(-50%)', textAlign: 'center',
                }}>
                    <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: colors.mono }}>HIGH</div>
                </div>
                {/* Price marker — the key indicator */}
                <div style={{
                    position: 'absolute', left: `${pricePos}%`, top: '10px',
                    transform: 'translateX(-50%)', textAlign: 'center', zIndex: 2,
                }}>
                    <div style={{
                        width: '14px', height: '14px', borderRadius: '50%',
                        background: priceInRange ? colors.accent : '#EF4444',
                        border: '2px solid #fff', margin: '0 auto',
                        boxShadow: `0 0 8px ${priceInRange ? colors.accent : '#EF4444'}`,
                    }} />
                    <div style={{
                        fontSize: '11px', fontWeight: 700, marginTop: '6px',
                        color: priceInRange ? colors.text : '#EF4444',
                        fontFamily: colors.mono,
                    }}>
                        {fmtDollar(price)}
                    </div>
                    <div style={{
                        fontSize: '8px', letterSpacing: '1px', color: colors.textMuted,
                        fontFamily: colors.mono,
                    }}>PRICE</div>
                </div>
            </div>
        </div>
    );
}

/* ====== Intrinsic Value Breakdown Table ====== */

function IntrinsicBreakdown({ valuation }) {
    if (!valuation) return null;
    const v = valuation;
    const price = v.market_price;
    const methods = [
        ['Book Value', v.book_value_ps, 'Total equity / shares'],
        ['Tangible Book', v.tangible_book_ps, 'Equity - intangibles'],
        ['NCAV (Graham)', v.ncav_ps, 'Current assets - liabilities'],
        ['Net Cash', v.net_cash_ps, 'Cash - total debt'],
        ['Liquidation', v.liquidation_ps, 'Conservative haircuts'],
        ['Earnings Power', v.epv_ps, 'Norm. earnings / 10% CoC'],
        ['Owner Earnings', v.owner_earnings_ps, 'Buffett method'],
        ['DCF (10yr)', v.dcf_ps, 'FCF discounted at 10%'],
    ];
    return (
        <div style={{ ...shared.card, padding: '20px' }}>
            <div style={{ ...shared.sectionTitle, marginBottom: '14px' }}>
                INTRINSIC VALUE BREAKDOWN
            </div>
            <div style={{ overflowX: 'auto' }}>
                {methods.map(([name, val, basis]) => {
                    const diff = (price && val) ? ((val - price) / price) * 100 : null;
                    const barColor = diff != null && diff > 0 ? '#10B981' : '#EF4444';
                    const barWidth = diff != null ? Math.min(Math.abs(diff), 60) : 0;
                    return (
                        <div key={name} style={{
                            display: 'flex', alignItems: 'center', gap: '12px',
                            padding: '10px 0',
                            borderBottom: `1px solid ${colors.border}22`,
                        }}>
                            <div style={{ width: '120px', fontSize: '12px', color: colors.textDim, flexShrink: 0 }}>
                                {name}
                            </div>
                            <div style={{
                                width: '80px', fontSize: '14px', fontWeight: 700,
                                fontFamily: colors.mono, textAlign: 'right', flexShrink: 0,
                                color: val != null && val > 0 ? '#10B981' : val != null && val < 0 ? '#EF4444' : colors.textMuted,
                            }}>
                                {fmtDollar(val)}
                            </div>
                            <div style={{ flex: 1, display: 'flex', alignItems: 'center', gap: '8px' }}>
                                <div style={{
                                    flex: 1, height: '4px', background: colors.bg,
                                    borderRadius: '2px', overflow: 'hidden',
                                }}>
                                    <div style={{
                                        width: `${barWidth}%`, height: '100%',
                                        background: barColor, borderRadius: '2px',
                                        transition: 'width 0.6s ease',
                                    }} />
                                </div>
                                <div style={{
                                    width: '50px', fontSize: '10px', fontFamily: colors.mono,
                                    color: diff != null && diff > 0 ? '#10B981' : diff != null ? '#EF4444' : colors.textMuted,
                                    textAlign: 'right', flexShrink: 0,
                                }}>
                                    {diff != null ? `${diff > 0 ? '+' : ''}${diff.toFixed(0)}%` : '--'}
                                </div>
                            </div>
                            <div style={{
                                width: '110px', fontSize: '10px', color: colors.textMuted,
                                flexShrink: 0, display: window.innerWidth > 640 ? 'block' : 'none',
                            }}>
                                {basis}
                            </div>
                        </div>
                    );
                })}
            </div>
            {/* Ratio row */}
            <div style={{ ...shared.metricGrid, marginTop: '16px' }}>
                {[
                    ['P/E', v.pe_ratio],
                    ['P/B', v.pb_ratio],
                    ['P/S', v.ps_ratio],
                    ['EV/EBITDA', v.ev_ebitda],
                ].map(([label, val]) => (
                    <div key={label} style={{ ...shared.metric }}>
                        <div style={shared.metricValue}>{fmt(val, 1)}</div>
                        <div style={shared.metricLabel}>{label}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}

/* ====== Derivatives Support Gauge ====== */

function DerivativesGauge({ derivatives }) {
    if (!derivatives) return null;
    const d = derivatives;
    const cs = d.derivatives_support_score;
    const csColor = scoreColor(cs || 50);

    return (
        <div style={{ ...shared.card, padding: '20px' }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '10px', marginBottom: '20px' }}>
                <Activity size={16} color={colors.accent} />
                <span style={{ ...shared.sectionTitle, margin: 0 }}>DERIVATIVES SUPPORT</span>
                <SupportBadge regime={d.support_regime} />
            </div>

            {/* Three ring charts */}
            <div style={{
                display: 'flex', justifyContent: 'center', gap: '24px',
                flexWrap: 'wrap', marginBottom: '24px',
            }}>
                <RingChart score={d.short_pressure_score} label="SHORT PRESSURE"
                    sublabel={d.short_float_pct != null ? `${fmt(d.short_float_pct, 1)}% float` : null} />
                <RingChart score={d.gamma_support_score} label="GAMMA SUPPORT"
                    sublabel={d.gex_regime ? d.gex_regime.replace('_', ' ') : null} />
                <RingChart score={d.options_sentiment_score} label="OPTIONS SENT."
                    sublabel={d.put_call_ratio != null ? `PCR ${fmt(d.put_call_ratio)}` : null} />
            </div>

            {/* Composite bar */}
            <div style={{ marginBottom: '16px' }}>
                <div style={{
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                    marginBottom: '6px',
                }}>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, letterSpacing: '1px',
                        color: colors.textMuted, fontFamily: colors.mono,
                    }}>COMPOSITE SCORE</span>
                    <span style={{
                        fontSize: '18px', fontWeight: 800, color: csColor,
                        fontFamily: colors.mono,
                    }}>{cs != null ? Math.round(cs) : '--'}</span>
                </div>
                <div style={{
                    height: '8px', background: colors.bg, borderRadius: '4px',
                    overflow: 'hidden', border: `1px solid ${colors.border}`,
                }}>
                    <div style={{
                        height: '100%', width: `${cs || 0}%`,
                        background: `linear-gradient(90deg, ${csColor}88, ${csColor})`,
                        borderRadius: '4px', transition: 'width 0.8s ease',
                    }} />
                </div>
                <div style={{
                    display: 'flex', justifyContent: 'space-between',
                    fontSize: '9px', color: colors.textMuted, marginTop: '4px',
                    fontFamily: colors.mono,
                }}>
                    <span>PRESSURE</span>
                    <span>NEUTRAL</span>
                    <span>SUPPORT</span>
                </div>
            </div>

            {/* Narrative */}
            {d.narrative && (
                <div style={{
                    fontSize: '12px', lineHeight: '1.7', color: colors.textDim,
                    padding: '14px', background: colors.bg, borderRadius: tokens.radius.md,
                    border: `1px solid ${colors.border}`, marginBottom: '16px',
                }}>
                    {d.narrative}
                </div>
            )}

            {/* Key levels */}
            <div style={{ ...shared.metricGrid }}>
                {[
                    ['PUT WALL', d.put_wall, '#10B981'],
                    ['GAMMA WALL', d.gamma_wall, '#F59E0B'],
                    ['GAMMA FLIP', d.gamma_flip, colors.textDim],
                    ['MAX PAIN', d.max_pain, colors.accent],
                ].filter(([, v]) => v != null).map(([label, val, c]) => (
                    <div key={label} style={{ ...shared.metric }}>
                        <div style={{ ...shared.metricValue, color: c, fontSize: '15px' }}>
                            {fmtDollar(val)}
                        </div>
                        <div style={shared.metricLabel}>{label}</div>
                    </div>
                ))}
            </div>
        </div>
    );
}

/* ====== Tabs: Prompt / Response / History ====== */

function TabBar({ active, onChange }) {
    const tabs = [
        { id: 'prompt', label: 'PROMPT', icon: FileText },
        { id: 'response', label: 'RESPONSE', icon: Send },
        { id: 'history', label: 'HISTORY', icon: Clock },
    ];
    return (
        <div style={{
            display: 'flex', gap: '2px', background: colors.bg,
            borderRadius: tokens.radius.md, padding: '3px',
            border: `1px solid ${colors.border}`, marginBottom: '16px',
        }}>
            {tabs.map(t => {
                const Icon = t.icon;
                const isActive = active === t.id;
                return (
                    <button key={t.id} onClick={() => onChange(t.id)} style={{
                        flex: 1, display: 'flex', alignItems: 'center', justifyContent: 'center',
                        gap: '6px', padding: '10px', border: 'none', cursor: 'pointer',
                        borderRadius: tokens.radius.sm, fontSize: '10px', fontWeight: 700,
                        letterSpacing: '1.5px', fontFamily: colors.mono,
                        background: isActive ? colors.card : 'transparent',
                        color: isActive ? colors.text : colors.textMuted,
                        transition: 'all 0.15s ease',
                    }}>
                        <Icon size={13} />
                        {t.label}
                    </button>
                );
            })}
        </div>
    );
}

function PromptTab({ prompt, analysisId }) {
    const [copied, setCopied] = useState(false);
    const handleCopy = useCallback(() => {
        if (!prompt) return;
        navigator.clipboard.writeText(prompt).then(() => {
            setCopied(true);
            setTimeout(() => setCopied(false), 2500);
        });
    }, [prompt]);

    if (!prompt) return (
        <div style={{ textAlign: 'center', padding: '40px', color: colors.textMuted }}>
            Run an analysis to generate a Claude Max prompt.
        </div>
    );

    return (
        <div>
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginBottom: '12px',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{
                        fontSize: '10px', color: colors.textMuted, fontFamily: colors.mono,
                        padding: '2px 8px', background: colors.bg, borderRadius: tokens.radius.sm,
                        border: `1px solid ${colors.border}`,
                    }}>ID: {analysisId}</span>
                </div>
                <button onClick={handleCopy} style={{
                    ...shared.button, padding: '8px 16px', fontSize: '12px',
                    display: 'flex', alignItems: 'center', gap: '6px',
                    background: copied ? '#10B98130' : colors.accent,
                    color: copied ? '#10B981' : '#fff',
                }}>
                    {copied ? <Check size={13} /> : <Copy size={13} />}
                    {copied ? 'Copied!' : 'Copy to Clipboard'}
                </button>
            </div>
            <pre style={{
                background: colors.bg, border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md, padding: '16px',
                maxHeight: '500px', overflow: 'auto',
                fontSize: '11px', lineHeight: '1.65', color: colors.textDim,
                fontFamily: colors.mono, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
            }}>
                {prompt}
            </pre>
        </div>
    );
}

function ResponseTab({ analysisId, ticker, onSubmitted }) {
    const [response, setResponse] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [result, setResult] = useState(null);

    const handleSubmit = useCallback(async () => {
        if (!response.trim() || !analysisId) return;
        setSubmitting(true);
        try {
            let predictions = [];
            const jsonMatch = response.match(/```json\s*([\s\S]*?)```/);
            if (jsonMatch) {
                try { predictions = JSON.parse(jsonMatch[1]).predictions || []; }
                catch { /* ignore parse errors */ }
            }
            const res = await api.post('/api/v1/valuation/response', {
                analysis_id: analysisId, ticker, response_text: response, predictions,
            });
            if (res.status === 'ok') {
                setResult(res);
                onSubmitted?.(res);
            }
        } catch (err) {
            console.error('Log response failed:', err);
        } finally {
            setSubmitting(false);
        }
    }, [response, analysisId, ticker, onSubmitted]);

    if (!analysisId) return (
        <div style={{ textAlign: 'center', padding: '40px', color: colors.textMuted }}>
            Generate a prompt first, then paste the Claude Max response here.
        </div>
    );

    return (
        <div>
            <p style={{ fontSize: '12px', color: colors.textDim, marginBottom: '12px' }}>
                Paste the full Claude Max response below. JSON prediction blocks will be auto-extracted
                and tracked for accuracy scoring.
            </p>
            <textarea
                value={response}
                onChange={e => setResponse(e.target.value)}
                placeholder="Paste Claude Max analysis here..."
                disabled={!!result}
                style={{
                    ...shared.textarea, width: '100%', minHeight: '200px',
                    fontFamily: colors.mono, fontSize: '12px',
                }}
            />
            <div style={{
                display: 'flex', justifyContent: 'flex-end', marginTop: '12px',
                alignItems: 'center', gap: '12px',
            }}>
                {result ? (
                    <div style={{
                        display: 'flex', alignItems: 'center', gap: '8px',
                        color: '#10B981', fontSize: '13px', fontWeight: 600,
                    }}>
                        <Check size={16} />
                        Logged ({result.predictions_logged || 0} predictions tracked)
                    </div>
                ) : (
                    <button onClick={handleSubmit}
                        disabled={!response.trim() || submitting}
                        style={{
                            ...shared.button, padding: '10px 20px', fontSize: '13px',
                            display: 'flex', alignItems: 'center', gap: '6px',
                            opacity: response.trim() ? 1 : 0.4,
                        }}>
                        <Send size={14} />
                        {submitting ? 'Logging...' : 'Log Response'}
                    </button>
                )}
            </div>
        </div>
    );
}

function HistoryTab({ predictions }) {
    const [expanded, setExpanded] = useState(false);
    if (!predictions || predictions.length === 0) return (
        <div style={{ textAlign: 'center', padding: '40px', color: colors.textMuted }}>
            No past analyses yet. Run an analysis and log a response to start tracking.
        </div>
    );

    const shown = expanded ? predictions : predictions.slice(0, 8);
    return (
        <div>
            {shown.map((p, i) => {
                const preds = Array.isArray(p.predictions) ? p.predictions : [];
                const acc = p.accuracy_score;
                return (
                    <div key={p.id || i} style={{
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        padding: '12px 0', borderBottom: `1px solid ${colors.border}22`,
                    }}>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
                            <span style={{
                                fontSize: '12px', fontFamily: colors.mono, color: colors.text,
                            }}>{p.analysis_date}</span>
                            <span style={{
                                fontSize: '10px', color: colors.textMuted, fontFamily: colors.mono,
                                padding: '2px 6px', background: colors.bg, borderRadius: tokens.radius.sm,
                            }}>{p.analysis_id}</span>
                        </div>
                        <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                            {preds.length > 0 && (
                                <span style={{
                                    fontSize: '10px', color: colors.textMuted, fontFamily: colors.mono,
                                }}>{preds.length} pred.</span>
                            )}
                            {acc != null && (
                                <span style={{
                                    fontSize: '11px', fontWeight: 700, fontFamily: colors.mono,
                                    color: acc > 0.6 ? '#10B981' : acc > 0.4 ? '#F59E0B' : '#EF4444',
                                }}>{(acc * 100).toFixed(0)}%</span>
                            )}
                        </div>
                    </div>
                );
            })}
            {predictions.length > 8 && (
                <button onClick={() => setExpanded(!expanded)} style={{
                    background: 'none', border: 'none', color: colors.accent,
                    cursor: 'pointer', fontSize: '12px', marginTop: '8px',
                    display: 'flex', alignItems: 'center', gap: '4px',
                }}>
                    {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                    {expanded ? 'Show less' : `Show all ${predictions.length}`}
                </button>
            )}
        </div>
    );
}

/* ====== Main Component ====== */

export default function Valuation() {
    const [ticker, setTicker] = useState('');
    const [loading, setLoading] = useState(false);
    const [promptData, setPromptData] = useState(null);
    const [predictions, setPredictions] = useState([]);
    const [error, setError] = useState(null);
    const [activeTab, setActiveTab] = useState('prompt');

    const handleAnalyze = useCallback(async (t) => {
        const target = (t || ticker).trim().toUpperCase();
        if (!target) return;
        setTicker(target);
        setLoading(true);
        setError(null);
        setPromptData(null);
        setPredictions([]);
        setActiveTab('prompt');

        try {
            const [promptResult, predResult] = await Promise.all([
                api.get(`/api/v1/valuation/prompt/${target}`),
                api.get(`/api/v1/valuation/predictions/${target}`).catch(() => ({ predictions: [] })),
            ]);
            if (promptResult.status === 'error') {
                setError(promptResult.error);
            } else {
                setPromptData(promptResult);
                setPredictions(predResult.predictions || []);
            }
        } catch (err) {
            setError(err.message || 'Analysis failed');
        } finally {
            setLoading(false);
        }
    }, [ticker]);

    const handleKeyDown = useCallback((e) => {
        if (e.key === 'Enter') handleAnalyze();
    }, [handleAnalyze]);

    const data = promptData?.data;
    const valuation = data?.valuation;
    const derivatives = data?.derivatives;

    return (
        <div style={{ padding: tokens.space.xl, maxWidth: '960px', margin: '0 auto' }}>

            {/* ── Hero Header ── */}
            <div style={{ marginBottom: '28px' }}>
                <div style={{ ...shared.sectionTitle, fontSize: '10px', marginBottom: '8px' }}>
                    VALUATION MODEL
                </div>
                <h1 style={{
                    fontSize: '22px', fontWeight: 700, color: colors.text,
                    margin: '0 0 6px 0', display: 'flex', alignItems: 'center', gap: '10px',
                }}>
                    <DollarSign size={22} color={colors.accent} />
                    Intrinsic Value + Derivatives
                </h1>
                <p style={{ fontSize: '13px', color: colors.textDim, margin: 0 }}>
                    Balance sheet valuations, company milestones, and derivatives positioning
                    — merged into a Claude Max analysis prompt.
                </p>
            </div>

            {/* ── Ticker Input + Quick Pills ── */}
            <div style={{ marginBottom: '24px' }}>
                <div style={{ display: 'flex', gap: '10px', marginBottom: '12px' }}>
                    <div style={{
                        flex: 1, maxWidth: '340px', display: 'flex', alignItems: 'center',
                        background: colors.card, border: `1px solid ${colors.border}`,
                        borderRadius: tokens.radius.md, padding: '0 14px',
                    }}>
                        <Search size={16} color={colors.textMuted} />
                        <input
                            type="text" value={ticker}
                            onChange={e => setTicker(e.target.value.toUpperCase())}
                            onKeyDown={handleKeyDown}
                            placeholder="Enter ticker..."
                            style={{
                                flex: 1, background: 'transparent', border: 'none',
                                color: colors.text, fontFamily: colors.mono,
                                fontSize: '18px', fontWeight: 700, letterSpacing: '2px',
                                padding: '12px 10px', outline: 'none',
                            }}
                        />
                    </div>
                    <button onClick={() => handleAnalyze()} disabled={!ticker.trim() || loading}
                        style={{
                            ...shared.button, padding: '12px 28px', fontSize: '14px',
                            display: 'flex', alignItems: 'center', gap: '8px',
                            opacity: ticker.trim() ? 1 : 0.4,
                        }}>
                        <Target size={16} />
                        {loading ? 'Analyzing...' : 'Analyze'}
                    </button>
                </div>
                {/* Quick ticker pills */}
                <div style={{ display: 'flex', gap: '6px', flexWrap: 'wrap' }}>
                    {QUICK_TICKERS.map(t => (
                        <button key={t} onClick={() => handleAnalyze(t)}
                            style={{
                                padding: '5px 12px', borderRadius: tokens.radius.pill,
                                background: ticker === t ? `${colors.accent}30` : colors.bg,
                                border: `1px solid ${ticker === t ? colors.accent : colors.border}`,
                                color: ticker === t ? colors.accent : colors.textDim,
                                fontSize: '11px', fontWeight: 600, fontFamily: colors.mono,
                                cursor: 'pointer', letterSpacing: '1px',
                                transition: 'all 0.15s ease',
                            }}>
                            {t}
                        </button>
                    ))}
                </div>
            </div>

            {/* ── Error ── */}
            {error && (
                <div style={{
                    ...shared.card, padding: '14px 18px', marginBottom: '20px',
                    background: '#3B111115', borderColor: '#EF444440',
                    display: 'flex', alignItems: 'center', gap: '10px', color: '#EF4444',
                }}>
                    <AlertTriangle size={16} />
                    <span style={{ fontSize: '13px' }}>{error}</span>
                </div>
            )}

            {/* ── Loading ── */}
            {loading && <LoadingSkeleton />}

            {/* ── Empty State ── */}
            {!data && !loading && !error && (
                <div style={{
                    textAlign: 'center', padding: '60px 20px',
                    color: colors.textMuted,
                }}>
                    <DollarSign size={40} color={colors.border} style={{ marginBottom: '16px' }} />
                    <div style={{ fontSize: '14px', marginBottom: '6px', color: colors.textDim }}>
                        Enter a ticker to analyze
                    </div>
                    <div style={{ fontSize: '12px' }}>
                        Intrinsic value, milestone execution, and derivatives support
                    </div>
                </div>
            )}

            {/* ====== Results ====== */}
            {data && !loading && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>

                    {/* ── Metric Cards ── */}
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px' }}>
                        <div style={{
                            ...shared.card, flex: '1 1 160px', minWidth: '160px', padding: '16px',
                        }}>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                color: colors.textMuted, fontFamily: colors.mono, marginBottom: '6px',
                            }}>MARKET PRICE</div>
                            <div style={{
                                fontSize: '22px', fontWeight: 800, fontFamily: colors.mono,
                                color: colors.text,
                            }}>{fmtDollar(valuation?.market_price)}</div>
                        </div>

                        <div style={{
                            ...shared.card, flex: '1 1 160px', minWidth: '160px', padding: '16px',
                            borderColor: '#10B98140',
                        }}>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                color: colors.textMuted, fontFamily: colors.mono, marginBottom: '6px',
                            }}>INTRINSIC VALUE</div>
                            <div style={{
                                fontSize: '22px', fontWeight: 800, fontFamily: colors.mono,
                                color: '#10B981',
                            }}>{fmtDollar(data.adjusted_intrinsic_mid || valuation?.intrinsic_mid)}</div>
                            <div style={{ fontSize: '10px', color: colors.textDim, marginTop: '4px' }}>
                                {fmtDollar(data.adjusted_intrinsic_low || valuation?.intrinsic_low)} - {fmtDollar(data.adjusted_intrinsic_high || valuation?.intrinsic_high)}
                            </div>
                        </div>

                        <div style={{
                            ...shared.card, flex: '1 1 130px', minWidth: '130px', padding: '16px',
                        }}>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                color: colors.textMuted, fontFamily: colors.mono, marginBottom: '6px',
                            }}>MARGIN OF SAFETY</div>
                            <div style={{
                                fontSize: '22px', fontWeight: 800, fontFamily: colors.mono,
                                color: valuation?.margin_of_safety > 0 ? '#10B981' : '#EF4444',
                            }}>{fmtPct(valuation?.margin_of_safety)}</div>
                        </div>

                        <div style={{
                            ...shared.card, flex: '1 1 160px', minWidth: '160px', padding: '16px',
                            display: 'flex', flexDirection: 'column', justifyContent: 'center',
                        }}>
                            <div style={{
                                fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
                                color: colors.textMuted, fontFamily: colors.mono, marginBottom: '8px',
                            }}>ASSESSMENT</div>
                            <ValueBadge value={data.price_vs_value} />
                        </div>
                    </div>

                    {/* ── Price Ruler ── */}
                    <PriceRuler
                        price={valuation?.market_price}
                        low={data.adjusted_intrinsic_low || valuation?.intrinsic_low}
                        mid={data.adjusted_intrinsic_mid || valuation?.intrinsic_mid}
                        high={data.adjusted_intrinsic_high || valuation?.intrinsic_high}
                    />

                    {/* ── Intrinsic Breakdown ── */}
                    <IntrinsicBreakdown valuation={valuation} />

                    {/* ── Milestone adjustment ── */}
                    {data.milestone_value_adjustment !== 0 && (
                        <div style={{
                            ...shared.card, padding: '14px 18px',
                            background: data.milestone_value_adjustment > 0 ? '#10B98112' : '#EF444412',
                            borderColor: data.milestone_value_adjustment > 0 ? '#10B98130' : '#EF444430',
                            display: 'flex', alignItems: 'center', gap: '10px',
                        }}>
                            <TrendingUp size={16} color={data.milestone_value_adjustment > 0 ? '#10B981' : '#EF4444'} />
                            <span style={{ fontSize: '13px', color: colors.text }}>
                                Milestone adjustment: <strong style={{ fontFamily: colors.mono }}>
                                    {data.milestone_value_adjustment > 0 ? '+' : ''}{fmtDollar(data.milestone_value_adjustment)}
                                </strong>/share from pending goals
                            </span>
                        </div>
                    )}

                    {/* ── Derivatives Gauge ── */}
                    <DerivativesGauge derivatives={derivatives} />

                    {/* ── Assessment summary ── */}
                    {data.overall_assessment && (
                        <div style={{
                            ...shared.cardGradient, padding: '18px 20px',
                            borderLeft: `3px solid ${colors.accent}`,
                        }}>
                            <div style={{
                                display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px',
                            }}>
                                <Shield size={15} color={colors.accent} />
                                <span style={{ ...shared.sectionTitle, margin: 0 }}>ASSESSMENT</span>
                            </div>
                            <p style={{
                                fontSize: '13px', lineHeight: '1.7', color: colors.textDim, margin: 0,
                            }}>{data.overall_assessment}</p>
                        </div>
                    )}

                    {/* ── Tabs: Prompt / Response / History ── */}
                    <div style={{ ...shared.card, padding: '20px' }}>
                        <TabBar active={activeTab} onChange={setActiveTab} />
                        {activeTab === 'prompt' && (
                            <PromptTab prompt={promptData?.prompt} analysisId={promptData?.analysis_id} />
                        )}
                        {activeTab === 'response' && (
                            <ResponseTab
                                analysisId={promptData?.analysis_id}
                                ticker={promptData?.ticker}
                                onSubmitted={(res) => {
                                    setPredictions(prev => [{
                                        analysis_id: promptData.analysis_id,
                                        analysis_date: promptData.analysis_date,
                                        predictions: res.predictions_logged || 0,
                                    }, ...prev]);
                                }}
                            />
                        )}
                        {activeTab === 'history' && (
                            <HistoryTab predictions={predictions} />
                        )}
                    </div>
                </div>
            )}
        </div>
    );
}
