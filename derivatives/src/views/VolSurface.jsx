import React, { useEffect, useState } from 'react';
import useStore from '../store.js';
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
    surfacePlaceholder: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.lg, height: '400px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        marginBottom: tokens.spacing.lg, color: tokens.textMuted,
        fontSize: '12px', fontFamily: tokens.fontMono,
        flexDirection: 'column', gap: tokens.spacing.sm,
    },
    statsRow: {
        display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
        gap: tokens.spacing.sm, marginBottom: tokens.spacing.lg,
    },
    statCard: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md, padding: tokens.spacing.md,
    },
    statLabel: {
        fontSize: '9px', fontWeight: 500, color: tokens.textMuted,
        letterSpacing: '1px', textTransform: 'uppercase', fontFamily: tokens.fontMono,
        marginBottom: tokens.spacing.xs,
    },
    statValue: { fontSize: '14px', fontWeight: 700, fontFamily: tokens.fontMono, color: tokens.textBright },
    loading: {
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '200px', color: tokens.textMuted, fontSize: '12px', fontFamily: tokens.fontMono,
    },
    error: { color: tokens.danger, fontSize: '12px', fontFamily: tokens.fontMono, padding: tokens.spacing.lg },
};

function VolSurface() {
    const { selectedTicker } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getVolSurface(selectedTicker)
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, [selectedTicker]);

    const nPoints = data?.surface?.length || 0;
    const expiries = data?.surface ? [...new Set(data.surface.map(d => d.expiry))].length : 0;
    const strikes = data?.surface ? [...new Set(data.surface.map(d => d.strike))].length : 0;

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Volatility Surface</div>
                {loading && <div style={styles.loading}>Loading vol surface...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && (
                    <>
                        <div style={styles.surfacePlaceholder}>
                            <div>3D Volatility Surface</div>
                            <div style={{ fontSize: '10px' }}>
                                {nPoints} grid points | {expiries} expiries | {strikes} strikes
                            </div>
                        </div>
                        <div style={styles.statsRow}>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Grid Points</div>
                                <div style={styles.statValue}>{nPoints}</div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Expiries</div>
                                <div style={styles.statValue}>{expiries}</div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Strikes</div>
                                <div style={styles.statValue}>{strikes}</div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Ticker</div>
                                <div style={{ ...styles.statValue, color: tokens.accent }}>{selectedTicker}</div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}

export default VolSurface;
