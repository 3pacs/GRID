import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';
import { interpretPCR, interpretIV, interpretMaxPain } from '../utils/interpret.js';
import GEXProfile from '../components/GEXProfile.jsx';

const tabs = ['Signals', 'Scanner', '100x', 'Dealer Flow', 'Trades'];

const scoreColor = (score) => {
    if (score >= 7) return '#EF4444';
    if (score >= 5) return '#F97316';
    if (score >= 3) return '#F59E0B';
    return '#5A7080';
};

const pcrColor = (pcr) => {
    if (pcr > 1.2) return colors.red;
    if (pcr < 0.7) return colors.green;
    return colors.textDim;
};

const directionStyle = (dir) => ({
    color: dir === 'CALL' ? colors.green : colors.red,
    fontWeight: 700,
    fontFamily: "'JetBrains Mono', monospace",
    fontSize: '12px',
});

const styles = {
    container: {
        padding: '16px',
        paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)',
        maxWidth: '900px',
        margin: '0 auto',
    },
    header: {
        fontSize: '22px',
        fontWeight: 600,
        color: '#E8F0F8',
        marginBottom: '16px',
        fontFamily: "'IBM Plex Sans', sans-serif",
    },
    signalCard: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '10px',
        padding: '14px 16px',
        marginBottom: '8px',
    },
    ticker: {
        fontSize: '16px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px',
    },
    metricRow: {
        display: 'grid',
        gridTemplateColumns: 'repeat(3, 1fr)',
        gap: '8px',
        marginTop: '12px',
    },
    metricBox: {
        background: colors.bg,
        borderRadius: '8px',
        padding: '8px',
        textAlign: 'center',
    },
    metricLabel: {
        fontSize: '10px',
        color: colors.textMuted,
        textTransform: 'uppercase',
        letterSpacing: '0.5px',
    },
    metricValue: {
        fontSize: '14px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: "'JetBrains Mono', monospace",
        marginTop: '2px',
    },
    scannerCard: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '10px',
        padding: '14px 16px',
        marginBottom: '8px',
    },
    scoreBar: (score) => ({
        height: '4px',
        borderRadius: '2px',
        background: colors.border,
        marginTop: '8px',
        overflow: 'hidden',
    }),
    scoreFill: (score) => ({
        height: '100%',
        width: `${(score / 10) * 100}%`,
        background: scoreColor(score),
        borderRadius: '2px',
        transition: 'width 0.3s ease',
    }),
    confidenceBar: (conf) => ({
        height: '3px',
        borderRadius: '2px',
        background: colors.border,
        marginTop: '6px',
        overflow: 'hidden',
    }),
    confidenceFill: (conf) => ({
        height: '100%',
        width: `${(conf || 0) * 100}%`,
        background: colors.accent,
        borderRadius: '2px',
    }),
    hundredXCard: {
        background: colors.card,
        border: `2px solid #EF4444`,
        borderRadius: '10px',
        padding: '14px 16px',
        marginBottom: '8px',
        boxShadow: '0 0 20px rgba(239, 68, 68, 0.3)',
    },
    hundredXTicker: {
        fontSize: '24px',
        fontWeight: 700,
        color: '#E8F0F8',
        fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '2px',
    },
    hundredXPayoff: {
        fontSize: '32px',
        fontWeight: 700,
        color: '#EF4444',
        fontFamily: "'JetBrains Mono', monospace",
    },
    badge100x: {
        display: 'inline-block',
        padding: '2px 8px',
        borderRadius: '4px',
        fontSize: '10px',
        fontWeight: 700,
        background: '#EF4444',
        color: '#fff',
        fontFamily: "'JetBrains Mono', monospace",
        letterSpacing: '1px',
    },
    emptyState: {
        textAlign: 'center',
        padding: '40px 20px',
        color: colors.textMuted,
        fontSize: '14px',
    },
    loadingState: {
        textAlign: 'center',
        padding: '60px 20px',
        color: colors.textMuted,
        fontSize: '14px',
    },
};

function SignalCard({ signal }) {
    const pcr = signal.put_call_ratio;
    return (
        <div style={styles.signalCard}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <span style={styles.ticker}>{signal.ticker}</span>
                <span style={{
                    fontSize: '13px',
                    color: colors.textMuted,
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    Spot: ${signal.spot_price?.toFixed(2) || '--'}
                </span>
            </div>
            <div style={styles.metricRow}>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>P/C Ratio</div>
                    <div style={{ ...styles.metricValue, color: pcrColor(pcr) }}>
                        {pcr?.toFixed(2) || '--'}
                    </div>
                </div>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>Max Pain</div>
                    <div style={styles.metricValue}>
                        ${signal.max_pain?.toFixed(0) || '--'}
                    </div>
                </div>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>IV Skew</div>
                    <div style={styles.metricValue}>
                        {signal.iv_skew?.toFixed(3) || '--'}
                    </div>
                </div>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>IV ATM</div>
                    <div style={styles.metricValue}>
                        {signal.iv_atm?.toFixed(1) || '--'}%
                    </div>
                </div>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>Total OI</div>
                    <div style={styles.metricValue}>
                        {signal.total_oi ? (signal.total_oi / 1000).toFixed(0) + 'K' : '--'}
                    </div>
                </div>
                <div style={styles.metricBox}>
                    <div style={styles.metricLabel}>Sentiment</div>
                    <div style={{
                        ...styles.metricValue,
                        fontSize: '12px',
                        color: pcr > 1.2 ? colors.red : pcr < 0.7 ? colors.green : colors.textDim,
                    }}>
                        {pcr > 1.2 ? 'BEARISH' : pcr < 0.7 ? 'BULLISH' : 'NEUTRAL'}
                    </div>
                </div>
            </div>
            {/* Interpretation */}
            <div style={{ marginTop: '10px', fontSize: '11px', lineHeight: '1.5', color: '#6A8098' }}>
                {interpretPCR(pcr)}
                {signal.iv_atm != null && <span style={{ display: 'block', marginTop: '3px' }}>{interpretIV(signal.iv_atm)}</span>}
                {signal.max_pain != null && signal.spot_price != null && (
                    <span style={{ display: 'block', marginTop: '3px' }}>{interpretMaxPain(signal.max_pain, signal.spot_price)}</span>
                )}
            </div>
        </div>
    );
}

function ScannerCard({ item }) {
    return (
        <div style={styles.scannerCard}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '10px' }}>
                    <span style={styles.ticker}>{item.ticker}</span>
                    <span style={directionStyle(item.direction)}>{item.direction}</span>
                    {item.is_100x && <span style={styles.badge100x}>100x</span>}
                </div>
                <div style={{ textAlign: 'right' }}>
                    <div style={{
                        fontSize: '20px',
                        fontWeight: 700,
                        color: scoreColor(item.score),
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        {item.score?.toFixed(1)}
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textMuted }}>SCORE</div>
                </div>
            </div>
            <div style={styles.scoreBar(item.score)}>
                <div style={styles.scoreFill(item.score)} />
            </div>
            {item.estimated_payoff_multiple && (
                <div style={{
                    marginTop: '10px',
                    fontSize: '13px',
                    color: colors.text,
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    Est. Payoff: <span style={{ color: colors.green, fontWeight: 700 }}>
                        {item.estimated_payoff_multiple?.toFixed(1)}x
                    </span>
                </div>
            )}
            {item.thesis && (
                <div style={{
                    marginTop: '8px',
                    fontSize: '12px',
                    color: colors.textDim,
                    lineHeight: '1.5',
                }}>
                    {item.thesis}
                </div>
            )}
            {item.confidence != null && (
                <div style={{ marginTop: '8px' }}>
                    <div style={{
                        fontSize: '10px',
                        color: colors.textMuted,
                        marginBottom: '4px',
                    }}>
                        Confidence: {(item.confidence * 100).toFixed(0)}%
                    </div>
                    <div style={styles.confidenceBar(item.confidence)}>
                        <div style={styles.confidenceFill(item.confidence)} />
                    </div>
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Trade Recommendation Card
   ═══════════════════════════════════════════════════════════════════ */

const sanityLayerNames = ['Volatility', 'Liquidity', 'Greeks', 'Regime', 'Risk'];

function SanityDots({ checks }) {
    // checks: array of { layer, status } or null
    const layers = checks || sanityLayerNames.map(() => null);
    return (
        <div style={{ display: 'flex', gap: '4px', alignItems: 'center' }} title="Sanity checks">
            {sanityLayerNames.map((name, i) => {
                const check = Array.isArray(layers) ? layers[i] : null;
                const status = check?.status || check;
                const bg = status === 'PASS' ? colors.green
                    : status === 'FAIL' ? colors.red
                    : '#3A4A5A';
                return (
                    <div key={name} title={`${name}: ${status || 'N/A'}`} style={{
                        width: '8px', height: '8px', borderRadius: '50%',
                        background: bg, flexShrink: 0,
                    }} />
                );
            })}
        </div>
    );
}

function ConfidenceCircle({ value }) {
    const pct = Math.round((value || 0) * 100);
    const r = 16;
    const circ = 2 * Math.PI * r;
    const offset = circ - (circ * (value || 0));
    const col = pct >= 70 ? colors.green : pct >= 40 ? colors.yellow : colors.red;
    return (
        <svg width="40" height="40" viewBox="0 0 40 40" style={{ flexShrink: 0 }}>
            <circle cx="20" cy="20" r={r} fill="none" stroke={colors.border} strokeWidth="3" />
            <circle cx="20" cy="20" r={r} fill="none" stroke={col} strokeWidth="3"
                strokeDasharray={circ} strokeDashoffset={offset}
                strokeLinecap="round" transform="rotate(-90 20 20)"
                style={{ transition: 'stroke-dashoffset 0.4s ease' }}
            />
            <text x="20" y="21" textAnchor="middle" dominantBaseline="middle"
                fontSize="10" fontWeight="700" fill={col}
                fontFamily="'JetBrains Mono', monospace"
            >
                {pct}
            </text>
        </svg>
    );
}

function RiskRewardBar({ stop, entry, target }) {
    if (stop == null || entry == null || target == null) return null;
    const min = Math.min(stop, entry, target);
    const max = Math.max(stop, entry, target);
    const range = max - min || 1;
    const stopPct = ((stop - min) / range) * 100;
    const entryPct = ((entry - min) / range) * 100;
    const targetPct = ((target - min) / range) * 100;

    return (
        <div style={{ position: 'relative', height: '16px', marginTop: '8px' }}>
            {/* Track */}
            <div style={{
                position: 'absolute', top: '6px', left: 0, right: 0, height: '4px',
                background: colors.border, borderRadius: '2px',
            }} />
            {/* Stop to Entry (red zone) */}
            <div style={{
                position: 'absolute', top: '6px', height: '4px', borderRadius: '2px',
                left: `${Math.min(stopPct, entryPct)}%`,
                width: `${Math.abs(entryPct - stopPct)}%`,
                background: `${colors.red}60`,
            }} />
            {/* Entry to Target (green zone) */}
            <div style={{
                position: 'absolute', top: '6px', height: '4px', borderRadius: '2px',
                left: `${Math.min(entryPct, targetPct)}%`,
                width: `${Math.abs(targetPct - entryPct)}%`,
                background: `${colors.green}60`,
            }} />
            {/* Stop marker */}
            <div style={{
                position: 'absolute', top: '2px', left: `${stopPct}%`,
                width: '2px', height: '12px', background: colors.red,
                transform: 'translateX(-1px)', borderRadius: '1px',
            }} />
            {/* Entry marker */}
            <div style={{
                position: 'absolute', top: '0px', left: `${entryPct}%`,
                width: '6px', height: '16px', background: colors.accent,
                transform: 'translateX(-3px)', borderRadius: '2px',
            }} />
            {/* Target marker */}
            <div style={{
                position: 'absolute', top: '2px', left: `${targetPct}%`,
                width: '2px', height: '12px', background: colors.green,
                transform: 'translateX(-1px)', borderRadius: '1px',
            }} />
            {/* Labels */}
            <div style={{
                position: 'absolute', top: '16px', left: `${stopPct}%`,
                transform: 'translateX(-50%)', fontSize: '8px', color: colors.red,
                fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap',
            }}>STOP</div>
            <div style={{
                position: 'absolute', top: '16px', left: `${entryPct}%`,
                transform: 'translateX(-50%)', fontSize: '8px', color: colors.accent,
                fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap',
            }}>ENTRY</div>
            <div style={{
                position: 'absolute', top: '16px', left: `${targetPct}%`,
                transform: 'translateX(-50%)', fontSize: '8px', color: colors.green,
                fontFamily: "'JetBrains Mono', monospace", whiteSpace: 'nowrap',
            }}>TARGET</div>
        </div>
    );
}

const outcomeColors = {
    WIN: colors.green,
    LOSS: colors.red,
    EXPIRED: '#5A7080',
    OPEN: colors.accent,
};

function TradeRecommendationCard({ rec }) {
    const dir = rec.direction || 'CALL';
    const dirColor = dir === 'CALL' ? colors.green : colors.red;
    const expReturn = rec.expected_return != null ? (rec.expected_return * 100).toFixed(1) : null;
    const kelly = rec.kelly_fraction != null ? (rec.kelly_fraction * 100).toFixed(1) : null;

    return (
        <div style={{
            ...styles.signalCard,
            borderLeft: `3px solid ${dirColor}`,
        }}>
            {/* Header: Ticker + Direction + Confidence */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={styles.ticker}>{rec.ticker}</span>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, padding: '2px 8px',
                        borderRadius: '4px', color: '#fff',
                        background: dirColor,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>{dir}</span>
                </div>
                <ConfidenceCircle value={rec.confidence} />
            </div>

            {/* Key numbers row */}
            <div style={{
                display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)',
                gap: '6px', marginTop: '10px',
            }}>
                {[
                    { label: 'Strike', value: rec.strike != null ? `$${rec.strike.toFixed(0)}` : '--' },
                    { label: 'Expiry', value: rec.expiry || '--' },
                    { label: 'Entry', value: rec.entry != null ? `$${rec.entry.toFixed(2)}` : '--' },
                    { label: 'Target', value: rec.target != null ? `$${rec.target.toFixed(2)}` : '--' },
                    { label: 'Stop', value: rec.stop != null ? `$${rec.stop.toFixed(2)}` : '--' },
                ].map(m => (
                    <div key={m.label} style={styles.metricBox}>
                        <div style={{ ...styles.metricLabel, fontSize: '9px' }}>{m.label}</div>
                        <div style={{ ...styles.metricValue, fontSize: '12px' }}>{m.value}</div>
                    </div>
                ))}
            </div>

            {/* Risk/Reward bar */}
            <RiskRewardBar stop={rec.stop} entry={rec.entry} target={rec.target} />

            {/* Expected return + Kelly + Sanity */}
            <div style={{
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                marginTop: '20px', paddingTop: '8px',
                borderTop: `1px solid ${colors.borderSubtle}`,
            }}>
                <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
                    {expReturn != null && (
                        <span style={{
                            fontSize: '13px', fontWeight: 700,
                            color: parseFloat(expReturn) >= 0 ? colors.green : colors.red,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {parseFloat(expReturn) >= 0 ? '+' : ''}{expReturn}% exp
                        </span>
                    )}
                    {kelly != null && (
                        <span style={{
                            fontSize: '10px', color: colors.textMuted,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            Kelly: {kelly}%
                        </span>
                    )}
                </div>
                <SanityDots checks={rec.sanity_checks} />
            </div>

            {/* Thesis */}
            {rec.thesis && (
                <div style={{
                    marginTop: '8px', fontSize: '12px', color: colors.textDim,
                    lineHeight: '1.5', fontFamily: colors.sans,
                }}>
                    {rec.thesis}
                </div>
            )}

            {/* Dealer context */}
            {rec.dealer_context && (
                <div style={{
                    marginTop: '6px', fontSize: '11px', color: colors.textMuted,
                    lineHeight: '1.4', fontStyle: 'italic',
                }}>
                    {rec.dealer_context}
                </div>
            )}
        </div>
    );
}

function TradeHistoryCard({ rec }) {
    const outcome = rec.outcome || 'OPEN';
    const badgeColor = outcomeColors[outcome] || colors.textMuted;
    const actualRet = rec.actual_return != null ? (rec.actual_return * 100).toFixed(1) : null;
    const expRet = rec.expected_return != null ? (rec.expected_return * 100).toFixed(1) : null;

    return (
        <div style={{
            ...styles.signalCard,
            opacity: outcome === 'EXPIRED' ? 0.7 : 1,
        }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <span style={{ ...styles.ticker, fontSize: '14px' }}>{rec.ticker}</span>
                    <span style={directionStyle(rec.direction)}>{rec.direction}</span>
                    <span style={{
                        fontSize: '10px', fontWeight: 700, padding: '2px 8px',
                        borderRadius: '4px', color: '#fff', background: badgeColor,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>{outcome}</span>
                </div>
                <div style={{ textAlign: 'right' }}>
                    {actualRet != null && (
                        <div style={{
                            fontSize: '14px', fontWeight: 700,
                            color: parseFloat(actualRet) >= 0 ? colors.green : colors.red,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            {parseFloat(actualRet) >= 0 ? '+' : ''}{actualRet}%
                        </div>
                    )}
                    {expRet != null && (
                        <div style={{
                            fontSize: '10px', color: colors.textMuted,
                            fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            exp: {expRet}%
                        </div>
                    )}
                </div>
            </div>
            {rec.expiry && (
                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '4px' }}>
                    Strike ${rec.strike?.toFixed(0)} | Exp {rec.expiry}
                </div>
            )}
        </div>
    );
}

function TradesTab() {
    const [recs, setRecs] = useState([]);
    const [history, setHistory] = useState([]);
    const [loading, setLoading] = useState(true);
    const [refreshing, setRefreshing] = useState(false);
    const [error, setError] = useState(null);

    useEffect(() => { loadTrades(); }, []);

    const loadTrades = async () => {
        setLoading(true);
        setError(null);
        try {
            const [active, hist] = await Promise.all([
                api.getOptionsRecommendations().catch(() => ({ recommendations: [] })),
                api.getOptionsRecommendationHistory().catch(() => ({ recommendations: [], summary: null })),
            ]);
            setRecs(active.recommendations || []);
            setHistory(hist.recommendations || []);
        } catch (e) {
            setError('Failed to load trade recommendations');
        }
        setLoading(false);
    };

    const handleRefresh = async () => {
        setRefreshing(true);
        try {
            await api.refreshOptionsRecommendations();
            await loadTrades();
        } catch (e) {
            setError('Refresh failed: ' + (e.message || 'unknown error'));
        }
        setRefreshing(false);
    };

    // Calculate running P&L from history
    const runningPnL = history.reduce((sum, r) => {
        if (r.actual_return != null) return sum + r.actual_return;
        return sum;
    }, 0);
    const wins = history.filter(r => r.outcome === 'WIN').length;
    const losses = history.filter(r => r.outcome === 'LOSS').length;

    if (loading) {
        return <div style={styles.loadingState}>Loading trade recommendations...</div>;
    }

    if (error) {
        return (
            <div style={styles.emptyState}>
                <div style={{ color: colors.red, marginBottom: '12px' }}>{error}</div>
                <button style={shared.buttonSmall} onClick={loadTrades}>Retry</button>
            </div>
        );
    }

    return (
        <div>
            {/* Refresh button */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                <div style={{
                    fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                    color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                }}>
                    ACTIVE RECOMMENDATIONS {recs.length > 0 && `\u00b7 ${recs.length}`}
                </div>
                <button
                    style={{
                        ...shared.buttonSmall,
                        fontSize: '11px',
                        padding: '6px 14px',
                        minHeight: '32px',
                        opacity: refreshing ? 0.6 : 1,
                    }}
                    onClick={handleRefresh}
                    disabled={refreshing}
                >
                    {refreshing ? 'Refreshing...' : 'Refresh Recs'}
                </button>
            </div>

            {/* Active Recommendations */}
            {recs.length === 0 ? (
                <div style={styles.emptyState}>No active trade recommendations</div>
            ) : (
                recs.map((r, i) => (
                    <TradeRecommendationCard key={`${r.ticker}-${r.strike}-${i}`} rec={r} />
                ))
            )}

            {/* History Section */}
            {history.length > 0 && (
                <div style={{ marginTop: '20px' }}>
                    <div style={{
                        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                        marginBottom: '10px',
                    }}>
                        <div style={{
                            fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                            color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                        }}>
                            HISTORY {`\u00b7 ${history.length}`}
                        </div>
                        <div style={{ display: 'flex', gap: '12px', fontSize: '11px', fontFamily: "'JetBrains Mono', monospace" }}>
                            <span style={{ color: colors.green }}>{wins}W</span>
                            <span style={{ color: colors.red }}>{losses}L</span>
                            <span style={{
                                color: runningPnL >= 0 ? colors.green : colors.red,
                                fontWeight: 700,
                            }}>
                                P&L: {runningPnL >= 0 ? '+' : ''}{(runningPnL * 100).toFixed(1)}%
                            </span>
                        </div>
                    </div>
                    {history.map((r, i) => (
                        <TradeHistoryCard key={`hist-${r.ticker}-${i}`} rec={r} />
                    ))}
                </div>
            )}
        </div>
    );
}

/* ═══════════════════════════════════════════════════════════════════
   Exported TradeRecommendationCards for reuse in WatchlistAnalysis
   ═══════════════════════════════════════════════════════════════════ */

export function TickerRecommendations({ ticker }) {
    const [recs, setRecs] = useState([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        if (!ticker) return;
        setLoading(true);
        api.getOptionsRecommendations(ticker)
            .then(data => setRecs(data.recommendations || []))
            .catch(() => setRecs([]))
            .finally(() => setLoading(false));
    }, [ticker]);

    if (loading) return null;
    if (recs.length === 0) return null;

    return (
        <div>
            <div style={{
                fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
                color: colors.accent, fontFamily: "'JetBrains Mono', monospace",
                marginBottom: '8px',
            }}>
                TRADE RECOMMENDATIONS {`\u00b7 ${recs.length}`}
            </div>
            {recs.map((r, i) => (
                <TradeRecommendationCard key={`${r.ticker}-${r.strike}-${i}`} rec={r} />
            ))}
        </div>
    );
}

function HundredXCard({ item }) {
    return (
        <div style={styles.hundredXCard}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
                <div>
                    <div style={styles.hundredXTicker}>{item.ticker}</div>
                    <span style={directionStyle(item.direction)}>{item.direction}</span>
                </div>
                <div style={{ textAlign: 'right' }}>
                    <div style={styles.hundredXPayoff}>
                        {item.estimated_payoff_multiple?.toFixed(0)}x
                    </div>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>
                        EST. PAYOFF
                    </div>
                </div>
            </div>
            <div style={{
                display: 'flex',
                gap: '12px',
                marginTop: '16px',
                alignItems: 'center',
            }}>
                <div style={{
                    fontSize: '20px',
                    fontWeight: 700,
                    color: scoreColor(item.score),
                    fontFamily: "'JetBrains Mono', monospace",
                }}>
                    {item.score?.toFixed(1)}
                </div>
                <div style={{
                    flex: 1,
                    height: '6px',
                    borderRadius: '3px',
                    background: colors.border,
                    overflow: 'hidden',
                }}>
                    <div style={{
                        height: '100%',
                        width: `${(item.score / 10) * 100}%`,
                        background: scoreColor(item.score),
                        borderRadius: '3px',
                    }} />
                </div>
            </div>
            {item.thesis && (
                <div style={{
                    marginTop: '12px',
                    fontSize: '13px',
                    color: colors.textDim,
                    lineHeight: '1.6',
                    borderTop: `1px solid ${colors.border}`,
                    paddingTop: '12px',
                }}>
                    {item.thesis}
                </div>
            )}
            {item.confidence != null && (
                <div style={{
                    marginTop: '8px',
                    fontSize: '11px',
                    color: colors.textMuted,
                }}>
                    Confidence: {(item.confidence * 100).toFixed(0)}%
                </div>
            )}
        </div>
    );
}

export default function Options() {
    const [activeTab, setActiveTab] = useState('Signals');
    const [signals, setSignals] = useState([]);
    const [scanner, setScanner] = useState([]);
    const [opportunities, setOpportunities] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [gexTicker, setGexTicker] = useState('SPY');
    const [gexData, setGexData] = useState(null);
    const [gexLoading, setGexLoading] = useState(false);
    const [gexError, setGexError] = useState(null);

    useEffect(() => { loadData(); }, []);

    const loadGEX = async (t) => {
        setGexLoading(true);
        setGexError(null);
        try {
            const d = await api.getGEXProfile(t || gexTicker);
            if (d.error) {
                setGexError(d.error);
                setGexData(null);
            } else {
                setGexData(d);
            }
        } catch (e) {
            setGexError(e.message || 'Failed to load GEX data');
            setGexData(null);
        }
        setGexLoading(false);
    };

    useEffect(() => {
        if (activeTab === 'Dealer Flow' && !gexData && !gexLoading) {
            loadGEX();
        }
    }, [activeTab]);

    const loadData = async () => {
        setLoading(true);
        setError(null);
        try {
            const [sig, scan, opps] = await Promise.all([
                api.getOptionsSignals().catch(() => ({ signals: [] })),
                api.scanMispricing(5.0).catch(() => ({ opportunities: [] })),
                api.get100xOpportunities().catch(() => ({ opportunities: [] })),
            ]);
            setSignals(sig.signals || []);
            setScanner((scan.opportunities || []).sort((a, b) => (b.score || 0) - (a.score || 0)));
            setOpportunities(opps.opportunities || []);
        } catch (e) {
            setError('Failed to load options data');
        }
        setLoading(false);
    };

    const renderContent = () => {
        if (loading) {
            return <div style={styles.loadingState}>Loading options data...</div>;
        }
        if (error) {
            return (
                <div style={styles.emptyState}>
                    <div style={{ color: colors.red, marginBottom: '12px' }}>{error}</div>
                    <button style={shared.buttonSmall} onClick={loadData}>Retry</button>
                </div>
            );
        }

        switch (activeTab) {
            case 'Signals':
                return signals.length === 0
                    ? <div style={styles.emptyState}>No options signals available</div>
                    : signals.map((s, i) => <SignalCard key={s.ticker || i} signal={s} />);

            case 'Scanner':
                return scanner.length === 0
                    ? <div style={styles.emptyState}>No mispricing opportunities found</div>
                    : scanner.map((s, i) => <ScannerCard key={`${s.ticker}-${i}`} item={s} />);

            case '100x':
                return opportunities.length === 0
                    ? <div style={styles.emptyState}>No 100x opportunities detected</div>
                    : opportunities.map((o, i) => <HundredXCard key={`${o.ticker}-${i}`} item={o} />);

            case 'Trades':
                return <TradesTab />;

            case 'Dealer Flow':
                return (
                    <div>
                        {/* Ticker selector */}
                        <div style={{ display: 'flex', gap: '6px', marginBottom: '12px', flexWrap: 'wrap', alignItems: 'center' }}>
                            {['SPY', 'QQQ', 'IWM', 'AAPL', 'TSLA', 'NVDA', 'MSFT', 'META', 'AMZN', 'GOOG'].map(t => (
                                <button
                                    key={t}
                                    onClick={() => { setGexTicker(t); loadGEX(t); }}
                                    style={{
                                        background: t === gexTicker ? `${colors.accent}25` : 'transparent',
                                        border: `1px solid ${t === gexTicker ? colors.accent : colors.border}`,
                                        borderRadius: '4px',
                                        padding: '5px 10px',
                                        fontSize: '11px',
                                        fontWeight: t === gexTicker ? 700 : 500,
                                        color: t === gexTicker ? colors.accent : colors.textMuted,
                                        cursor: 'pointer',
                                        fontFamily: "'JetBrains Mono', monospace",
                                        transition: `all 0.15s ease`,
                                    }}
                                >
                                    {t}
                                </button>
                            ))}
                        </div>
                        {gexLoading ? (
                            <div style={styles.loadingState}>Loading GEX profile for {gexTicker}...</div>
                        ) : gexError ? (
                            <div style={styles.emptyState}>
                                <div style={{ color: colors.red, marginBottom: '12px' }}>{gexError}</div>
                                <button style={shared.buttonSmall} onClick={() => loadGEX()}>Retry</button>
                            </div>
                        ) : (
                            <GEXProfile
                                ticker={gexTicker}
                                gexData={gexData}
                                spotPrice={gexData?.spot}
                            />
                        )}
                    </div>
                );

            default:
                return null;
        }
    };

    return (
        <div style={styles.container}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                <div style={styles.header}>Options & Watchlist</div>
                <ViewHelp id="options" />
            </div>

            {/* Tab Bar */}
            <div style={shared.tabs}>
                {tabs.map(tab => (
                    <button
                        key={tab}
                        style={shared.tab(activeTab === tab)}
                        onClick={() => setActiveTab(tab)}
                    >
                        {tab}
                    </button>
                ))}
                <button
                    style={{
                        ...shared.buttonSmall,
                        marginLeft: 'auto',
                        background: 'transparent',
                        border: `1px solid ${colors.border}`,
                        color: colors.textMuted,
                        fontSize: '12px',
                    }}
                    onClick={loadData}
                >
                    Refresh
                </button>
            </div>

            {renderContent()}
        </div>
    );
}
