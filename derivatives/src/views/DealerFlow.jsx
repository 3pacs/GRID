import React, { useEffect, useState } from 'react';
import useStore from '../store.js';
import api from '../api.js';
import TickerSelector from '../components/TickerSelector.jsx';
import { tokens } from '../styles/tokens.js';

const styles = {
    container: { padding: tokens.spacing.lg },
    header: {
        fontSize: '12px',
        fontWeight: 600,
        color: tokens.textMuted,
        letterSpacing: '3px',
        textTransform: 'uppercase',
        marginBottom: tokens.spacing.md,
        fontFamily: tokens.fontMono,
    },
    regimeBanner: {
        padding: `${tokens.spacing.md} ${tokens.spacing.lg}`,
        borderRadius: tokens.radius.md,
        marginBottom: tokens.spacing.lg,
        fontFamily: tokens.fontMono,
        fontSize: '14px',
        fontWeight: 700,
        letterSpacing: '2px',
        textAlign: 'center',
    },
    metricsRow: {
        display: 'grid',
        gridTemplateColumns: 'repeat(3, 1fr)',
        gap: tokens.spacing.sm,
        marginBottom: tokens.spacing.lg,
    },
    metricCard: {
        background: tokens.card,
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        padding: tokens.spacing.md,
    },
    metricLabel: {
        fontSize: '9px',
        fontWeight: 500,
        color: tokens.textMuted,
        letterSpacing: '1px',
        textTransform: 'uppercase',
        fontFamily: tokens.fontMono,
        marginBottom: tokens.spacing.xs,
    },
    metricValue: {
        fontSize: '16px',
        fontWeight: 700,
        fontFamily: tokens.fontMono,
        color: tokens.textBright,
    },
    profilePlaceholder: {
        background: tokens.card,
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.lg,
        height: '240px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        marginBottom: tokens.spacing.lg,
        color: tokens.textMuted,
        fontSize: '12px',
        fontFamily: tokens.fontMono,
    },
    interpretation: {
        background: tokens.card,
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md,
        padding: tokens.spacing.lg,
        fontSize: '12px',
        lineHeight: '1.7',
        color: tokens.text,
        fontFamily: tokens.fontMono,
    },
    loading: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '200px',
        color: tokens.textMuted,
        fontSize: '12px',
        fontFamily: tokens.fontMono,
    },
    error: {
        color: tokens.danger,
        fontSize: '12px',
        fontFamily: tokens.fontMono,
        padding: tokens.spacing.lg,
    },
};

function formatNum(n) {
    if (n == null) return '--';
    if (Math.abs(n) >= 1e9) return `${(n / 1e9).toFixed(1)}B`;
    if (Math.abs(n) >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
    if (Math.abs(n) >= 1e3) return `${(n / 1e3).toFixed(1)}K`;
    return n.toFixed(1);
}

function getRegimeStyle(regime) {
    if (regime === 'LONG_GAMMA') return { background: 'rgba(46, 213, 115, 0.12)', color: tokens.safe, borderLeft: `3px solid ${tokens.safe}` };
    if (regime === 'SHORT_GAMMA') return { background: 'rgba(255, 71, 87, 0.12)', color: tokens.danger, borderLeft: `3px solid ${tokens.danger}` };
    return { background: 'rgba(255, 165, 2, 0.12)', color: tokens.caution, borderLeft: `3px solid ${tokens.caution}` };
}

function getRegimeText(regime) {
    if (regime === 'LONG_GAMMA') return 'Dealers are LONG GAMMA — expect dampened moves, mean-reversion, sell-the-rip/buy-the-dip flows.';
    if (regime === 'SHORT_GAMMA') return 'Dealers are SHORT GAMMA — expect amplified moves, trend-following, breakout risk. Hedging flows reinforce direction.';
    return 'NEUTRAL regime — no dominant dealer positioning. Gamma flip is near spot. Watch for regime transitions.';
}

function DealerFlow() {
    const { selectedTicker } = useStore();
    const [data, setData] = useState(null);
    const [regime, setRegime] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        setError(null);

        Promise.all([
            api.getGEX(selectedTicker),
            api.getRegime(),
        ])
            .then(([gexRes, regimeRes]) => {
                if (!cancelled) {
                    setData(gexRes);
                    setRegime(regimeRes);
                    setLoading(false);
                }
            })
            .catch((err) => {
                if (!cancelled) {
                    setError(err.message || 'Failed to load data');
                    setLoading(false);
                }
            });

        return () => { cancelled = true; };
    }, [selectedTicker]);

    const r = data?.regime || regime?.regime || 'NEUTRAL';

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Dealer Flow Dashboard</div>

                {loading && <div style={styles.loading}>Loading dealer positioning...</div>}
                {error && <div style={styles.error}>{error}</div>}

                {!loading && !error && data && (
                    <>
                        <div style={{ ...styles.regimeBanner, ...getRegimeStyle(r) }}>
                            {r.replace('_', ' ')}
                        </div>

                        <div style={styles.metricsRow}>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>GEX Aggregate</div>
                                <div style={{ ...styles.metricValue, color: data.gex_aggregate >= 0 ? tokens.safe : tokens.danger }}>
                                    ${formatNum(data.gex_aggregate)}
                                </div>
                            </div>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>Gamma Flip</div>
                                <div style={styles.metricValue}>
                                    {data.gamma_flip ? `$${data.gamma_flip.toFixed(0)}` : '--'}
                                </div>
                            </div>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>Vanna Exp</div>
                                <div style={{ ...styles.metricValue, color: tokens.purple }}>
                                    ${formatNum(data.vanna_exposure)}
                                </div>
                            </div>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>Charm Exp</div>
                                <div style={{ ...styles.metricValue, color: tokens.caution }}>
                                    ${formatNum(data.charm_exposure)}
                                </div>
                            </div>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>Net Delta</div>
                                <div style={{ ...styles.metricValue, color: data.dealer_delta >= 0 ? tokens.safe : tokens.danger }}>
                                    {formatNum(data.dealer_delta)}
                                </div>
                            </div>
                            <div style={styles.metricCard}>
                                <div style={styles.metricLabel}>Spot</div>
                                <div style={styles.metricValue}>
                                    ${data.spot?.toFixed(2) || '--'}
                                </div>
                            </div>
                        </div>

                        <div style={styles.profilePlaceholder}>
                            GEX Profile Chart — {data.profile?.length || 0} data points
                        </div>

                        <div style={styles.interpretation}>
                            <div style={{ ...styles.metricLabel, marginBottom: tokens.spacing.sm }}>Interpretation</div>
                            {getRegimeText(r)}
                            {data.gamma_flip && data.spot && (
                                <div style={{ marginTop: tokens.spacing.sm, color: tokens.textMuted }}>
                                    Gamma flip at ${data.gamma_flip.toFixed(0)} (spot ${data.spot.toFixed(0)}) —{' '}
                                    {data.spot > data.gamma_flip
                                        ? 'spot is ABOVE flip, dealers stabilizing'
                                        : 'spot is BELOW flip, dealers amplifying'}
                                </div>
                            )}
                            {data.put_wall && data.call_wall && (
                                <div style={{ marginTop: tokens.spacing.xs, color: tokens.textMuted }}>
                                    Put wall ${data.put_wall.toFixed(0)} | Call wall ${data.call_wall.toFixed(0)}
                                </div>
                            )}
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}

export default DealerFlow;
