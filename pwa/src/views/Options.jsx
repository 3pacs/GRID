import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';
import ViewHelp from '../components/ViewHelp.jsx';
import { interpretPCR, interpretIV, interpretMaxPain } from '../utils/interpret.js';
import GEXProfile from '../components/GEXProfile.jsx';

const tabs = ['Signals', 'Scanner', '100x', 'Dealer Flow'];

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
