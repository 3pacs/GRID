import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import {
    DollarSign, TrendingUp, TrendingDown, Shield, Copy, Check,
    Send, Clock, BarChart3, AlertTriangle, ChevronDown, ChevronUp,
    Clipboard, FileText, Target, Activity,
} from 'lucide-react';

/* ─────────────── Helpers ─────────────── */

const fmt = (v, dec = 2) => v != null ? Number(v).toFixed(dec) : '--';
const fmtDollar = (v) => v != null ? `$${Number(v).toFixed(2)}` : '--';
const fmtPct = (v) => v != null ? `${(Number(v) * 100).toFixed(1)}%` : '--';
const fmtScore = (v) => v != null ? `${Number(v).toFixed(0)}/100` : '--';

const REGIME_COLORS = {
    STRONG_SUPPORT: '#10B981',
    MILD_SUPPORT: '#3B82F6',
    NEUTRAL: '#8AA0B8',
    MILD_PRESSURE: '#F59E0B',
    STRONG_PRESSURE: '#EF4444',
};

const VALUE_COLORS = {
    SIGNIFICANTLY_UNDERVALUED: '#10B981',
    UNDERVALUED: '#34D399',
    FAIR_VALUE: '#3B82F6',
    OVERVALUED: '#F59E0B',
    SIGNIFICANTLY_OVERVALUED: '#EF4444',
};

/* ─────────────── Sub-components ─────────────── */

function SupportBadge({ regime }) {
    const c = REGIME_COLORS[regime] || colors.textMuted;
    return (
        <span style={{
            display: 'inline-flex', alignItems: 'center', gap: '6px',
            padding: '4px 12px', borderRadius: tokens.radius.pill,
            background: `${c}18`, border: `1px solid ${c}40`,
            fontSize: '11px', fontWeight: 700, letterSpacing: '1.5px',
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
            padding: '3px 10px', borderRadius: tokens.radius.pill,
            background: `${c}18`, border: `1px solid ${c}40`,
            fontSize: '10px', fontWeight: 700, letterSpacing: '1px',
            color: c, fontFamily: colors.mono,
        }}>
            {(value || 'UNKNOWN').replace(/_/g, ' ')}
        </span>
    );
}

function MetricCard({ label, value, sub, color }) {
    return (
        <div style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.lg, padding: '16px',
            flex: '1 1 140px', minWidth: '140px',
        }}>
            <div style={{ fontSize: '11px', color: colors.textMuted, marginBottom: '6px', letterSpacing: '0.5px' }}>
                {label}
            </div>
            <div style={{
                fontSize: '20px', fontWeight: 700, color: color || colors.text,
                fontFamily: colors.mono,
            }}>
                {value}
            </div>
            {sub && (
                <div style={{ fontSize: '11px', color: colors.textDim, marginTop: '4px' }}>
                    {sub}
                </div>
            )}
        </div>
    );
}

function ValuationTable({ valuation }) {
    if (!valuation) return null;
    const v = valuation;
    const methods = [
        ['Book Value', v.book_value_ps, 'Total equity / shares'],
        ['Tangible Book', v.tangible_book_ps, 'Equity - intangibles - goodwill'],
        ['NCAV (Graham)', v.ncav_ps, 'Current assets - total liabilities'],
        ['Net Cash', v.net_cash_ps, 'Cash - total debt'],
        ['Liquidation', v.liquidation_ps, 'Conservative asset haircuts'],
        ['Earnings Power', v.epv_ps, 'Normalized earnings / 10% CoC'],
        ['Owner Earnings', v.owner_earnings_ps, 'NI + D&A - maintenance capex'],
        ['DCF (10yr)', v.dcf_ps, 'Free CF discounted at 10%'],
    ];

    return (
        <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
                <thead>
                    <tr style={{ borderBottom: `1px solid ${colors.border}` }}>
                        <th style={{ textAlign: 'left', padding: '8px', color: colors.textMuted, fontWeight: 600 }}>Method</th>
                        <th style={{ textAlign: 'right', padding: '8px', color: colors.textMuted, fontWeight: 600 }}>Value/Share</th>
                        <th style={{ textAlign: 'left', padding: '8px', color: colors.textMuted, fontWeight: 600 }}>Basis</th>
                    </tr>
                </thead>
                <tbody>
                    {methods.map(([name, val, basis]) => (
                        <tr key={name} style={{ borderBottom: `1px solid ${colors.border}22` }}>
                            <td style={{ padding: '8px', color: colors.text }}>{name}</td>
                            <td style={{
                                padding: '8px', textAlign: 'right',
                                fontFamily: colors.mono, fontWeight: 600,
                                color: val != null && val > 0 ? '#10B981' : val != null && val < 0 ? '#EF4444' : colors.textDim,
                            }}>
                                {fmtDollar(val)}
                            </td>
                            <td style={{ padding: '8px', color: colors.textDim, fontSize: '11px' }}>{basis}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function PromptSection({ prompt, analysisId, onCopied }) {
    const [copied, setCopied] = useState(false);

    const handleCopy = useCallback(() => {
        navigator.clipboard.writeText(prompt).then(() => {
            setCopied(true);
            onCopied?.();
            setTimeout(() => setCopied(false), 2000);
        });
    }, [prompt, onCopied]);

    return (
        <div style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.lg, padding: '20px',
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <FileText size={16} color={colors.accent} />
                    <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                        Claude Max Prompt
                    </span>
                    <span style={{
                        fontSize: '10px', color: colors.textMuted, fontFamily: colors.mono,
                        padding: '2px 8px', background: colors.border, borderRadius: tokens.radius.sm,
                    }}>
                        {analysisId}
                    </span>
                </div>
                <button
                    onClick={handleCopy}
                    style={{
                        display: 'flex', alignItems: 'center', gap: '6px',
                        padding: '8px 16px', borderRadius: tokens.radius.md,
                        background: copied ? '#10B98122' : colors.accent,
                        color: copied ? '#10B981' : '#fff',
                        border: 'none', cursor: 'pointer', fontSize: '13px', fontWeight: 600,
                    }}
                >
                    {copied ? <Check size={14} /> : <Copy size={14} />}
                    {copied ? 'Copied!' : 'Copy Prompt'}
                </button>
            </div>
            <pre style={{
                background: '#080C10', border: `1px solid ${colors.border}`,
                borderRadius: tokens.radius.md, padding: '16px',
                maxHeight: '400px', overflow: 'auto',
                fontSize: '12px', lineHeight: '1.6', color: colors.textDim,
                fontFamily: colors.mono, whiteSpace: 'pre-wrap', wordBreak: 'break-word',
            }}>
                {prompt}
            </pre>
        </div>
    );
}

function ResponseInput({ analysisId, ticker, onSubmitted }) {
    const [response, setResponse] = useState('');
    const [submitting, setSubmitting] = useState(false);
    const [submitted, setSubmitted] = useState(false);

    const handleSubmit = useCallback(async () => {
        if (!response.trim() || !analysisId) return;
        setSubmitting(true);
        try {
            // Try to extract JSON predictions from response
            let predictions = [];
            const jsonMatch = response.match(/```json\s*([\s\S]*?)```/);
            if (jsonMatch) {
                try {
                    const parsed = JSON.parse(jsonMatch[1]);
                    predictions = parsed.predictions || [];
                } catch { /* not valid JSON, that's fine */ }
            }

            const result = await api.post('/api/v1/valuation/response', {
                analysis_id: analysisId,
                ticker: ticker,
                response_text: response,
                predictions: predictions,
            });

            if (result.status === 'ok') {
                setSubmitted(true);
                onSubmitted?.(result);
            }
        } catch (err) {
            console.error('Failed to log response:', err);
        } finally {
            setSubmitting(false);
        }
    }, [response, analysisId, ticker, onSubmitted]);

    return (
        <div style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.lg, padding: '20px',
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '12px' }}>
                <Clipboard size={16} color={colors.accent} />
                <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                    Paste Claude Max Response
                </span>
            </div>
            <p style={{ fontSize: '12px', color: colors.textDim, marginBottom: '12px' }}>
                Paste the full Claude Max response here. If it contains a JSON predictions block,
                those predictions will be automatically extracted and tracked for accuracy.
            </p>
            <textarea
                value={response}
                onChange={e => setResponse(e.target.value)}
                placeholder="Paste Claude Max analysis response here..."
                disabled={submitted}
                style={{
                    width: '100%', minHeight: '200px', padding: '14px',
                    background: '#080C10', border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.md, color: colors.text,
                    fontFamily: colors.mono, fontSize: '12px', lineHeight: '1.6',
                    resize: 'vertical', outline: 'none',
                }}
            />
            <div style={{ display: 'flex', justifyContent: 'flex-end', marginTop: '12px', gap: '12px' }}>
                {submitted ? (
                    <div style={{
                        display: 'flex', alignItems: 'center', gap: '8px',
                        color: '#10B981', fontSize: '13px', fontWeight: 600,
                    }}>
                        <Check size={16} />
                        Response logged & predictions tracked
                    </div>
                ) : (
                    <button
                        onClick={handleSubmit}
                        disabled={!response.trim() || submitting}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '6px',
                            padding: '10px 20px', borderRadius: tokens.radius.md,
                            background: response.trim() ? colors.accent : colors.border,
                            color: response.trim() ? '#fff' : colors.textMuted,
                            border: 'none', cursor: response.trim() ? 'pointer' : 'default',
                            fontSize: '13px', fontWeight: 600,
                        }}
                    >
                        <Send size={14} />
                        {submitting ? 'Logging...' : 'Log Response & Track Predictions'}
                    </button>
                )}
            </div>
        </div>
    );
}

function DerivativesPanel({ derivatives }) {
    if (!derivatives) return null;
    const d = derivatives;

    return (
        <div style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.lg, padding: '20px',
        }}>
            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
                <Activity size={16} color={colors.accent} />
                <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                    Derivatives Support
                </span>
                <SupportBadge regime={d.support_regime} />
            </div>

            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginBottom: '16px' }}>
                <MetricCard
                    label="SHORT PRESSURE"
                    value={fmtScore(d.short_pressure_score)}
                    sub={d.short_float_pct != null ? `${fmt(d.short_float_pct, 1)}% float short` : null}
                    color={d.short_pressure_score > 60 ? '#10B981' : d.short_pressure_score < 40 ? '#EF4444' : colors.text}
                />
                <MetricCard
                    label="GAMMA SUPPORT"
                    value={fmtScore(d.gamma_support_score)}
                    sub={d.gex_regime ? `Dealer ${d.gex_regime.replace('_', ' ')}` : null}
                    color={d.gamma_support_score > 60 ? '#10B981' : d.gamma_support_score < 40 ? '#EF4444' : colors.text}
                />
                <MetricCard
                    label="OPTIONS SENTIMENT"
                    value={fmtScore(d.options_sentiment_score)}
                    sub={d.put_call_ratio != null ? `PCR ${fmt(d.put_call_ratio)}` : null}
                    color={d.options_sentiment_score > 60 ? '#10B981' : d.options_sentiment_score < 40 ? '#EF4444' : colors.text}
                />
                <MetricCard
                    label="COMPOSITE"
                    value={fmtScore(d.derivatives_support_score)}
                    color={d.derivatives_support_score > 60 ? '#10B981' : d.derivatives_support_score < 40 ? '#EF4444' : '#3B82F6'}
                />
            </div>

            {d.narrative && (
                <p style={{
                    fontSize: '12px', lineHeight: '1.7', color: colors.textDim,
                    padding: '12px', background: '#080C10', borderRadius: tokens.radius.md,
                    border: `1px solid ${colors.border}`,
                }}>
                    {d.narrative}
                </p>
            )}

            {(d.gamma_wall || d.put_wall) && (
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px', marginTop: '12px' }}>
                    {d.put_wall && (
                        <MetricCard label="PUT WALL (SUPPORT)" value={fmtDollar(d.put_wall)} color="#10B981" />
                    )}
                    {d.gamma_wall && (
                        <MetricCard label="GAMMA WALL (RESIST)" value={fmtDollar(d.gamma_wall)} color="#F59E0B" />
                    )}
                    {d.gamma_flip && (
                        <MetricCard label="GAMMA FLIP" value={fmtDollar(d.gamma_flip)} color="#8AA0B8" />
                    )}
                </div>
            )}
        </div>
    );
}

function PredictionHistory({ predictions }) {
    const [expanded, setExpanded] = useState(false);
    if (!predictions || predictions.length === 0) return null;

    const shown = expanded ? predictions : predictions.slice(0, 5);

    return (
        <div style={{
            background: colors.card, border: `1px solid ${colors.border}`,
            borderRadius: tokens.radius.lg, padding: '20px',
        }}>
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginBottom: '12px',
            }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <Clock size={16} color={colors.accent} />
                    <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                        Past Analyses ({predictions.length})
                    </span>
                </div>
                {predictions.length > 5 && (
                    <button
                        onClick={() => setExpanded(!expanded)}
                        style={{
                            display: 'flex', alignItems: 'center', gap: '4px',
                            background: 'none', border: 'none', color: colors.accent,
                            cursor: 'pointer', fontSize: '12px',
                        }}
                    >
                        {expanded ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
                        {expanded ? 'Show less' : `Show all ${predictions.length}`}
                    </button>
                )}
            </div>
            {shown.map((p, i) => (
                <div key={p.id || i} style={{
                    padding: '10px 12px', borderBottom: `1px solid ${colors.border}22`,
                    display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                }}>
                    <div>
                        <span style={{ fontSize: '12px', fontFamily: colors.mono, color: colors.textDim }}>
                            {p.analysis_date}
                        </span>
                        <span style={{
                            marginLeft: '12px', fontSize: '11px', color: colors.textMuted,
                        }}>
                            {p.analysis_id}
                        </span>
                    </div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                        {p.predictions && Array.isArray(p.predictions) && (
                            <span style={{
                                fontSize: '10px', color: colors.textMuted, fontFamily: colors.mono,
                                padding: '2px 6px', background: colors.border, borderRadius: tokens.radius.sm,
                            }}>
                                {p.predictions.length} predictions
                            </span>
                        )}
                        {p.accuracy_score != null && (
                            <span style={{
                                fontSize: '11px', fontWeight: 700, fontFamily: colors.mono,
                                color: p.accuracy_score > 0.6 ? '#10B981' : p.accuracy_score > 0.4 ? '#F59E0B' : '#EF4444',
                            }}>
                                {(p.accuracy_score * 100).toFixed(0)}% accurate
                            </span>
                        )}
                    </div>
                </div>
            ))}
        </div>
    );
}

/* ─────────────── Main Component ─────────────── */

export default function Valuation() {
    const [ticker, setTicker] = useState('');
    const [loading, setLoading] = useState(false);
    const [promptData, setPromptData] = useState(null);
    const [predictions, setPredictions] = useState([]);
    const [error, setError] = useState(null);

    const handleAnalyze = useCallback(async () => {
        if (!ticker.trim()) return;
        setLoading(true);
        setError(null);
        setPromptData(null);
        setPredictions([]);

        try {
            const [promptResult, predResult] = await Promise.all([
                api.get(`/api/v1/valuation/prompt/${ticker.trim().toUpperCase()}`),
                api.get(`/api/v1/valuation/predictions/${ticker.trim().toUpperCase()}`).catch(() => ({ predictions: [] })),
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
        <div style={{ padding: '24px', maxWidth: '1200px', margin: '0 auto' }}>
            {/* Header */}
            <div style={{ marginBottom: '24px' }}>
                <h1 style={{
                    fontSize: '22px', fontWeight: 700, color: colors.text,
                    margin: 0, display: 'flex', alignItems: 'center', gap: '10px',
                }}>
                    <DollarSign size={22} color={colors.accent} />
                    Valuation & Derivatives Model
                </h1>
                <p style={{ fontSize: '13px', color: colors.textDim, marginTop: '6px' }}>
                    Balance sheet intrinsic value + company goals + derivatives positioning.
                    Enter a ticker to generate a Claude Max analysis prompt.
                </p>
            </div>

            {/* Ticker input */}
            <div style={{
                display: 'flex', gap: '12px', marginBottom: '24px',
                alignItems: 'center',
            }}>
                <input
                    type="text"
                    value={ticker}
                    onChange={e => setTicker(e.target.value.toUpperCase())}
                    onKeyDown={handleKeyDown}
                    placeholder="Enter ticker (e.g. AAPL, NVDA, TSLA)"
                    style={{
                        flex: 1, maxWidth: '300px', padding: '12px 16px',
                        background: colors.card, border: `1px solid ${colors.border}`,
                        borderRadius: tokens.radius.md, color: colors.text,
                        fontFamily: colors.mono, fontSize: '16px', fontWeight: 700,
                        letterSpacing: '2px', outline: 'none',
                    }}
                />
                <button
                    onClick={handleAnalyze}
                    disabled={!ticker.trim() || loading}
                    style={{
                        padding: '12px 24px', borderRadius: tokens.radius.md,
                        background: ticker.trim() ? colors.accent : colors.border,
                        color: ticker.trim() ? '#fff' : colors.textMuted,
                        border: 'none', cursor: ticker.trim() ? 'pointer' : 'default',
                        fontSize: '14px', fontWeight: 600,
                        display: 'flex', alignItems: 'center', gap: '8px',
                    }}
                >
                    <Target size={16} />
                    {loading ? 'Analyzing...' : 'Analyze'}
                </button>
            </div>

            {error && (
                <div style={{
                    padding: '12px 16px', borderRadius: tokens.radius.md,
                    background: '#3B111122', border: '1px solid #EF444440',
                    color: '#EF4444', fontSize: '13px', marginBottom: '20px',
                    display: 'flex', alignItems: 'center', gap: '8px',
                }}>
                    <AlertTriangle size={16} />
                    {error}
                </div>
            )}

            {/* Results */}
            {data && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>

                    {/* Summary cards */}
                    <div style={{ display: 'flex', flexWrap: 'wrap', gap: '12px' }}>
                        <MetricCard
                            label="MARKET PRICE"
                            value={fmtDollar(valuation?.market_price)}
                            color={colors.text}
                        />
                        <MetricCard
                            label="INTRINSIC (MID)"
                            value={fmtDollar(data.adjusted_intrinsic_mid || valuation?.intrinsic_mid)}
                            sub={`Range: ${fmtDollar(data.adjusted_intrinsic_low || valuation?.intrinsic_low)} - ${fmtDollar(data.adjusted_intrinsic_high || valuation?.intrinsic_high)}`}
                            color="#10B981"
                        />
                        <MetricCard
                            label="MARGIN OF SAFETY"
                            value={fmtPct(valuation?.margin_of_safety)}
                            color={valuation?.margin_of_safety > 0 ? '#10B981' : '#EF4444'}
                        />
                        <MetricCard
                            label="ASSESSMENT"
                            value={<ValueBadge value={data.price_vs_value} />}
                        />
                    </div>

                    {/* Valuation table */}
                    {valuation && (
                        <div style={{
                            background: colors.card, border: `1px solid ${colors.border}`,
                            borderRadius: tokens.radius.lg, padding: '20px',
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '16px' }}>
                                <BarChart3 size={16} color={colors.accent} />
                                <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>
                                    Intrinsic Value Breakdown
                                </span>
                            </div>
                            <ValuationTable valuation={valuation} />
                        </div>
                    )}

                    {/* Milestone impact */}
                    {data.milestone_value_adjustment !== 0 && (
                        <div style={{
                            padding: '14px 18px', borderRadius: tokens.radius.md,
                            background: data.milestone_value_adjustment > 0 ? '#10B98115' : '#EF444415',
                            border: `1px solid ${data.milestone_value_adjustment > 0 ? '#10B98140' : '#EF444440'}`,
                            display: 'flex', alignItems: 'center', gap: '10px',
                        }}>
                            {data.milestone_value_adjustment > 0
                                ? <TrendingUp size={16} color="#10B981" />
                                : <TrendingDown size={16} color="#EF4444" />}
                            <span style={{ fontSize: '13px', color: colors.text }}>
                                Milestone value adjustment: <strong style={{ fontFamily: colors.mono }}>
                                    {data.milestone_value_adjustment > 0 ? '+' : ''}{fmtDollar(data.milestone_value_adjustment)}
                                </strong>/share from pending goals & rumors
                            </span>
                        </div>
                    )}

                    {/* Derivatives panel */}
                    <DerivativesPanel derivatives={derivatives} />

                    {/* Overall assessment */}
                    {data.overall_assessment && (
                        <div style={{
                            padding: '16px 20px', borderRadius: tokens.radius.lg,
                            background: colors.card, border: `1px solid ${colors.accent}40`,
                        }}>
                            <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '8px' }}>
                                <Shield size={16} color={colors.accent} />
                                <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text }}>Assessment</span>
                            </div>
                            <p style={{ fontSize: '13px', lineHeight: '1.7', color: colors.textDim, margin: 0 }}>
                                {data.overall_assessment}
                            </p>
                        </div>
                    )}

                    {/* Claude Max prompt */}
                    {promptData?.prompt && (
                        <PromptSection
                            prompt={promptData.prompt}
                            analysisId={promptData.analysis_id}
                        />
                    )}

                    {/* Response input */}
                    {promptData?.analysis_id && (
                        <ResponseInput
                            analysisId={promptData.analysis_id}
                            ticker={promptData.ticker}
                            onSubmitted={(result) => {
                                setPredictions(prev => [{
                                    analysis_id: promptData.analysis_id,
                                    analysis_date: promptData.analysis_date,
                                    predictions: result.predictions_logged,
                                }, ...prev]);
                            }}
                        />
                    )}

                    {/* Prediction history */}
                    <PredictionHistory predictions={predictions} />
                </div>
            )}
        </div>
    );
}
