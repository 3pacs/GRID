/**
 * Watchlist Ticker Analysis — full-page deep dive on a single ticker.
 *
 * Shows price chart, options signals, related features, regime context,
 * and TradingView webhook history.
 */
import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';

function MiniChart({ data, height = 80 }) {
    if (!data || data.length < 2) {
        return <div style={{ height, display: 'flex', alignItems: 'center', justifyContent: 'center', color: colors.textMuted, fontSize: '11px' }}>No price data</div>;
    }

    const values = data.map(d => d.value);
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = max - min || 1;
    const w = 100;
    const isUp = values[values.length - 1] >= values[0];
    const lineColor = isUp ? colors.green : colors.red;

    const points = values.map((v, i) => {
        const x = (i / (values.length - 1)) * w;
        const y = height - ((v - min) / range) * (height - 8) - 4;
        return `${x},${y}`;
    }).join(' ');

    return (
        <svg viewBox={`0 0 ${w} ${height}`} width="100%" height={height} preserveAspectRatio="none" style={{ display: 'block' }}>
            <polyline points={points} fill="none" stroke={lineColor} strokeWidth="1.5" vectorEffect="non-scaling-stroke" />
            <linearGradient id="cg" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stopColor={lineColor} stopOpacity="0.15" />
                <stop offset="100%" stopColor={lineColor} stopOpacity="0" />
            </linearGradient>
            <polygon points={`0,${height} ${points} ${w},${height}`} fill="url(#cg)" />
        </svg>
    );
}

function StatCard({ label, value, sub, color }) {
    return (
        <div style={shared.metric}>
            <div style={{ ...shared.metricValue, fontSize: '15px', color: color || '#E8F0F8' }}>{value}</div>
            <div style={shared.metricLabel}>{label}</div>
            {sub && <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>{sub}</div>}
        </div>
    );
}

function SignalDot({ value }) {
    if (value == null) return null;
    const color = value > 0 ? colors.green : value < 0 ? colors.red : colors.yellow;
    const label = value > 0 ? 'BUY' : value < 0 ? 'SELL' : 'ALERT';
    return (
        <span style={{
            fontSize: '10px', fontWeight: 700, padding: '2px 8px',
            borderRadius: '4px', background: `${color}20`, color,
            fontFamily: "'JetBrains Mono', monospace",
        }}>{label}</span>
    );
}

export default function WatchlistAnalysis({ ticker, onBack }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => { if (ticker) load(); }, [ticker]);

    const load = async () => {
        setLoading(true);
        setError(null);
        try {
            setData(await api.getTickerAnalysis(ticker));
        } catch (err) {
            setError(err.message || 'Failed to load');
        }
        setLoading(false);
    };

    if (loading) {
        return (
            <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
                <div style={{ color: colors.textMuted, textAlign: 'center', padding: '60px 0', fontSize: '13px' }}>
                    Loading {ticker}...
                </div>
            </div>
        );
    }

    if (error || !data) {
        return (
            <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
                <button onClick={onBack} style={{ ...shared.buttonSmall, background: colors.card, marginBottom: '16px' }}>Back</button>
                <div style={shared.error}>{error || 'No data'}</div>
            </div>
        );
    }

    const item = data.watchlist_item;
    const prices = data.price_history || [];
    const opts = (data.options || [])[0]; // latest
    const optsHistory = data.options || [];
    const regime = data.regime;
    const related = data.related_features || [];
    const tvSignals = data.tradingview_signals || [];

    const lastPrice = prices.length ? prices[prices.length - 1].value : null;
    const prevPrice = prices.length > 1 ? prices[prices.length - 2].value : null;
    const change = lastPrice && prevPrice ? ((lastPrice - prevPrice) / prevPrice * 100) : null;
    const high90 = prices.length ? Math.max(...prices.map(p => p.value)) : null;
    const low90 = prices.length ? Math.min(...prices.map(p => p.value)) : null;

    return (
        <div style={{ ...shared.container, paddingTop: 'calc(env(safe-area-inset-top, 0px) + 16px)' }}>
            {/* Header */}
            <div style={{ display: 'flex', alignItems: 'center', gap: '12px', marginBottom: '16px' }}>
                <button onClick={onBack} style={{
                    background: colors.card, border: `1px solid ${colors.border}`,
                    borderRadius: '8px', color: colors.textDim, padding: '8px 14px',
                    fontSize: '13px', cursor: 'pointer', minHeight: '36px',
                }}>Back</button>
                <div style={{ flex: 1 }}>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '10px' }}>
                        <span style={{ fontSize: '22px', fontWeight: 700, color: '#E8F0F8', fontFamily: "'JetBrains Mono', monospace" }}>
                            {ticker}
                        </span>
                        {lastPrice != null && (
                            <span style={{ fontSize: '16px', fontWeight: 600, color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                ${typeof lastPrice === 'number' ? lastPrice.toLocaleString(undefined, { maximumFractionDigits: 2 }) : lastPrice}
                            </span>
                        )}
                        {change != null && (
                            <span style={{
                                fontSize: '12px', fontWeight: 600,
                                color: change >= 0 ? colors.green : colors.red,
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>
                                {change >= 0 ? '+' : ''}{change.toFixed(2)}%
                            </span>
                        )}
                    </div>
                    <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '2px' }}>
                        {item?.display_name || ticker} · {(item?.asset_type || 'stock').toUpperCase()}
                        {regime && <> · Regime: <span style={{ color: colors.accent }}>{regime.state}</span></>}
                    </div>
                </div>
            </div>

            {/* Price Chart */}
            {prices.length > 0 && (
                <div style={{ ...shared.card, padding: '12px' }}>
                    <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '8px' }}>
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px' }}>90-DAY PRICE</span>
                        <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace" }}>
                            H: ${high90?.toLocaleString(undefined, { maximumFractionDigits: 2 })} · L: ${low90?.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                        </span>
                    </div>
                    <MiniChart data={prices} height={100} />
                </div>
            )}

            {/* Options Signals */}
            {opts && (
                <div style={{ marginTop: '12px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                        OPTIONS · {opts.date}
                    </div>
                    <div style={{ ...shared.metricGrid }}>
                        <StatCard label="Spot" value={opts.spot_price != null ? `$${opts.spot_price.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '--'} />
                        <StatCard label="P/C Ratio"
                            value={opts.put_call_ratio != null ? opts.put_call_ratio.toFixed(2) : '--'}
                            color={opts.put_call_ratio > 1.5 ? colors.red : opts.put_call_ratio < 0.7 ? colors.green : colors.text}
                            sub={opts.put_call_ratio > 1.5 ? 'bearish' : opts.put_call_ratio < 0.7 ? 'bullish' : 'neutral'}
                        />
                        <StatCard label="Max Pain" value={opts.max_pain != null ? `$${opts.max_pain.toLocaleString(undefined, { maximumFractionDigits: 0 })}` : '--'}
                            sub={opts.max_pain && opts.spot_price ? `${((opts.max_pain / opts.spot_price - 1) * 100).toFixed(1)}% from spot` : null}
                        />
                        <StatCard label="IV ATM" value={opts.iv_atm != null ? `${(opts.iv_atm * 100).toFixed(1)}%` : '--'}
                            color={opts.iv_atm > 0.4 ? colors.red : opts.iv_atm > 0.25 ? colors.yellow : colors.text}
                        />
                        <StatCard label="IV Skew" value={opts.iv_skew != null ? opts.iv_skew.toFixed(2) : '--'}
                            sub={opts.iv_skew > 1.3 ? 'put demand high' : opts.iv_skew < 0.9 ? 'complacent' : 'normal'}
                        />
                        <StatCard label="Total OI" value={opts.total_oi != null ? (opts.total_oi > 1e6 ? `${(opts.total_oi / 1e6).toFixed(1)}M` : `${(opts.total_oi / 1e3).toFixed(0)}K`) : '--'} />
                    </div>
                </div>
            )}

            {/* Related Features */}
            {related.length > 0 && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                        RELATED FEATURES · {related.length}
                    </div>
                    <div style={shared.card}>
                        {related.map((f, i) => (
                            <div key={f.name} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '8px 0',
                                borderBottom: i < related.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                            }}>
                                <div>
                                    <div style={{ fontSize: '12px', color: colors.text }}>{f.name}</div>
                                    <div style={{ fontSize: '10px', color: colors.textMuted }}>{f.family} · {f.obs_date}</div>
                                </div>
                                <div style={{ fontSize: '13px', fontWeight: 600, color: colors.text, fontFamily: "'JetBrains Mono', monospace" }}>
                                    {f.value != null ? (typeof f.value === 'number' && Math.abs(f.value) > 100 ? f.value.toLocaleString(undefined, { maximumFractionDigits: 2 }) : f.value.toFixed(4)) : '--'}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* TradingView Signals */}
            {tvSignals.length > 0 && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                        TRADINGVIEW ALERTS · {tvSignals.length}
                    </div>
                    <div style={shared.card}>
                        {tvSignals.map((s, i) => (
                            <div key={i} style={{
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                padding: '8px 0',
                                borderBottom: i < tvSignals.length - 1 ? `1px solid ${colors.borderSubtle}` : 'none',
                            }}>
                                <div>
                                    <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                                        <SignalDot value={s.signal_value} />
                                        <span style={{ fontSize: '12px', color: colors.text }}>
                                            {s.strategy || s.action || 'alert'}
                                        </span>
                                    </div>
                                    {s.message && <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>{s.message}</div>}
                                </div>
                                <div style={{ textAlign: 'right' }}>
                                    {s.price != null && <div style={{ fontSize: '12px', fontFamily: "'JetBrains Mono', monospace", color: colors.text }}>${s.price}</div>}
                                    <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                        {s.timestamp ? new Date(s.timestamp).toLocaleDateString() : ''}
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}

            {/* Regime Context */}
            {regime && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>
                        REGIME CONTEXT
                    </div>
                    <div style={{ ...shared.metricGrid }}>
                        <StatCard label="Regime" value={regime.state}
                            color={regime.state === 'GROWTH' ? colors.green : regime.state === 'CRISIS' ? colors.red : regime.state === 'FRAGILE' ? colors.yellow : colors.text}
                        />
                        <StatCard label="Confidence" value={regime.confidence != null ? `${(regime.confidence * 100).toFixed(0)}%` : '--'} />
                        <StatCard label="Posture" value={regime.posture || '--'} />
                    </div>
                </div>
            )}

            {/* Notes */}
            {item?.notes && (
                <div style={{ marginTop: '16px' }}>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: "'JetBrains Mono', monospace", letterSpacing: '1px', marginBottom: '6px' }}>NOTES</div>
                    <div style={{ ...shared.card, fontSize: '12px', color: colors.textDim, lineHeight: '1.5' }}>{item.notes}</div>
                </div>
            )}

            <div style={{ height: '80px' }} />
        </div>
    );
}
