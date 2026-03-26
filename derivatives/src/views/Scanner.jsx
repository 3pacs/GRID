import React, { useEffect, useState } from 'react';
import api from '../api.js';
import TickerSelector from '../components/TickerSelector.jsx';
import { tokens } from '../styles/tokens.js';

const styles = {
    container: { padding: tokens.spacing.lg },
    header: {
        fontSize: '12px', fontWeight: 600, color: tokens.textMuted,
        letterSpacing: '3px', textTransform: 'uppercase',
        marginBottom: tokens.spacing.md, fontFamily: tokens.fontMono,
    },
    summary: {
        display: 'flex', gap: tokens.spacing.lg, marginBottom: tokens.spacing.lg,
        fontFamily: tokens.fontMono, fontSize: '12px',
    },
    summaryItem: {
        display: 'flex', flexDirection: 'column', gap: tokens.spacing.xs,
    },
    summaryLabel: { fontSize: '9px', color: tokens.textMuted, letterSpacing: '1px', textTransform: 'uppercase' },
    summaryValue: { fontSize: '18px', fontWeight: 700, color: tokens.textBright },
    card: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md, padding: tokens.spacing.lg,
        marginBottom: tokens.spacing.sm,
    },
    cardHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginBottom: tokens.spacing.sm,
    },
    ticker: {
        fontSize: '14px', fontWeight: 700, color: tokens.accent,
        fontFamily: tokens.fontMono, letterSpacing: '1px',
    },
    score: {
        fontSize: '12px', fontWeight: 700, fontFamily: tokens.fontMono,
        padding: `${tokens.spacing.xs} ${tokens.spacing.sm}`,
        borderRadius: tokens.radius.sm,
    },
    thesis: {
        fontSize: '11px', lineHeight: '1.6', color: tokens.text,
        fontFamily: tokens.fontMono,
    },
    meta: {
        display: 'flex', gap: tokens.spacing.md, marginTop: tokens.spacing.sm,
        fontSize: '10px', color: tokens.textMuted, fontFamily: tokens.fontMono,
    },
    loading: {
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '200px', color: tokens.textMuted, fontSize: '12px', fontFamily: tokens.fontMono,
    },
    error: { color: tokens.danger, fontSize: '12px', fontFamily: tokens.fontMono, padding: tokens.spacing.lg },
    empty: {
        textAlign: 'center', padding: tokens.spacing.xxl,
        color: tokens.textMuted, fontSize: '12px', fontFamily: tokens.fontMono,
    },
};

function Scanner() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getScan()
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, []);

    const opps = data?.opportunities || [];

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Mispricing Scanner</div>
                {loading && <div style={styles.loading}>Running scanner...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && (
                    <>
                        <div style={styles.summary}>
                            <div style={styles.summaryItem}>
                                <span style={styles.summaryLabel}>Opportunities</span>
                                <span style={styles.summaryValue}>{data?.count || 0}</span>
                            </div>
                            <div style={styles.summaryItem}>
                                <span style={styles.summaryLabel}>100x Flags</span>
                                <span style={{ ...styles.summaryValue, color: tokens.caution }}>
                                    {data?.count_100x || 0}
                                </span>
                            </div>
                        </div>
                        {opps.length === 0 && (
                            <div style={styles.empty}>No mispricing opportunities detected.</div>
                        )}
                        {opps.map((o, i) => (
                            <div key={i} style={styles.card}>
                                <div style={styles.cardHeader}>
                                    <span style={styles.ticker}>{o.ticker}</span>
                                    <span style={{
                                        ...styles.score,
                                        background: o.score >= 7 ? 'rgba(46,213,115,0.15)' : 'rgba(255,165,2,0.15)',
                                        color: o.score >= 7 ? tokens.safe : tokens.caution,
                                    }}>
                                        {o.score?.toFixed(1)}/10
                                    </span>
                                </div>
                                <div style={styles.thesis}>{o.thesis || 'No thesis available.'}</div>
                                <div style={styles.meta}>
                                    <span>{o.direction}</span>
                                    <span>{o.expiry}</span>
                                    {o.estimated_payoff_multiple && (
                                        <span style={{ color: tokens.accent }}>
                                            {o.estimated_payoff_multiple.toFixed(0)}x payoff
                                        </span>
                                    )}
                                    {o.is_100x && (
                                        <span style={{ color: tokens.caution, fontWeight: 700 }}>100x</span>
                                    )}
                                </div>
                            </div>
                        ))}
                    </>
                )}
            </div>
        </div>
    );
}

export default Scanner;
