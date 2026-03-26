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
    heatmapPlaceholder: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.lg, height: '400px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        marginBottom: tokens.spacing.lg, color: tokens.textMuted,
        fontSize: '12px', fontFamily: tokens.fontMono, flexDirection: 'column',
        gap: tokens.spacing.sm,
    },
    statsRow: {
        display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)',
        gap: tokens.spacing.sm,
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

function PositionHeatmap() {
    const { selectedTicker } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getOIHeatmap(selectedTicker)
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, [selectedTicker]);

    const nCells = data?.heatmap?.length || 0;
    const totalCallOI = data?.heatmap?.reduce((s, c) => s + (c.call_oi || 0), 0) || 0;
    const totalPutOI = data?.heatmap?.reduce((s, c) => s + (c.put_oi || 0), 0) || 0;

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Open Interest Heatmap</div>
                {loading && <div style={styles.loading}>Loading OI heatmap...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && (
                    <>
                        <div style={styles.heatmapPlaceholder}>
                            <div>OI Heatmap — Strike x Expiry</div>
                            <div style={{ fontSize: '10px' }}>{nCells} data points</div>
                        </div>
                        <div style={styles.statsRow}>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Total Call OI</div>
                                <div style={{ ...styles.statValue, color: tokens.safe }}>
                                    {totalCallOI.toLocaleString()}
                                </div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Total Put OI</div>
                                <div style={{ ...styles.statValue, color: tokens.danger }}>
                                    {totalPutOI.toLocaleString()}
                                </div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>P/C Ratio</div>
                                <div style={styles.statValue}>
                                    {totalCallOI > 0 ? (totalPutOI / totalCallOI).toFixed(2) : '--'}
                                </div>
                            </div>
                            <div style={styles.statCard}>
                                <div style={styles.statLabel}>Grid Cells</div>
                                <div style={styles.statValue}>{nCells}</div>
                            </div>
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}

export default PositionHeatmap;
