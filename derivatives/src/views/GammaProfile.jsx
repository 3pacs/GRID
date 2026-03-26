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
    chartArea: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.lg, height: '300px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        marginBottom: tokens.spacing.lg, color: tokens.textMuted,
        fontSize: '12px', fontFamily: tokens.fontMono,
    },
    wallsRow: {
        display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)',
        gap: tokens.spacing.sm, marginBottom: tokens.spacing.lg,
    },
    wallCard: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md, padding: tokens.spacing.md, textAlign: 'center',
    },
    wallLabel: {
        fontSize: '9px', fontWeight: 500, color: tokens.textMuted,
        letterSpacing: '1px', textTransform: 'uppercase', fontFamily: tokens.fontMono,
        marginBottom: tokens.spacing.xs,
    },
    wallValue: { fontSize: '18px', fontWeight: 700, fontFamily: tokens.fontMono },
    strikeTable: {
        width: '100%', borderCollapse: 'collapse', fontFamily: tokens.fontMono, fontSize: '11px',
    },
    th: {
        padding: `${tokens.spacing.sm} ${tokens.spacing.xs}`, textAlign: 'right',
        color: tokens.textMuted, fontWeight: 500, fontSize: '9px', letterSpacing: '1px',
        borderBottom: `1px solid ${tokens.cardBorder}`, textTransform: 'uppercase',
    },
    td: {
        padding: `${tokens.spacing.xs}`, textAlign: 'right', color: tokens.text,
        borderBottom: `1px solid rgba(0,212,170,0.05)`,
    },
    loading: {
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '200px', color: tokens.textMuted, fontSize: '12px', fontFamily: tokens.fontMono,
    },
    error: { color: tokens.danger, fontSize: '12px', fontFamily: tokens.fontMono, padding: tokens.spacing.lg },
};

function GammaProfile() {
    const { selectedTicker } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getGEX(selectedTicker)
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, [selectedTicker]);

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Gamma Exposure Profile</div>
                {loading && <div style={styles.loading}>Computing gamma profile...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && data && (
                    <>
                        <div style={styles.chartArea}>
                            GEX vs Strike — {data.per_strike?.length || 0} strikes
                        </div>

                        <div style={styles.wallsRow}>
                            <div style={styles.wallCard}>
                                <div style={styles.wallLabel}>Put Wall</div>
                                <div style={{ ...styles.wallValue, color: tokens.safe }}>
                                    {data.put_wall ? `$${data.put_wall.toFixed(0)}` : '--'}
                                </div>
                            </div>
                            <div style={styles.wallCard}>
                                <div style={styles.wallLabel}>Gamma Wall</div>
                                <div style={{ ...styles.wallValue, color: tokens.accent }}>
                                    {data.gamma_wall ? `$${data.gamma_wall.toFixed(0)}` : '--'}
                                </div>
                            </div>
                            <div style={styles.wallCard}>
                                <div style={styles.wallLabel}>Call Wall</div>
                                <div style={{ ...styles.wallValue, color: tokens.danger }}>
                                    {data.call_wall ? `$${data.call_wall.toFixed(0)}` : '--'}
                                </div>
                            </div>
                        </div>

                        {data.per_strike && data.per_strike.length > 0 && (
                            <div style={{ overflowX: 'auto' }}>
                                <table style={styles.strikeTable}>
                                    <thead>
                                        <tr>
                                            <th style={{ ...styles.th, textAlign: 'left' }}>Strike</th>
                                            <th style={styles.th}>Call GEX</th>
                                            <th style={styles.th}>Put GEX</th>
                                            <th style={styles.th}>Net GEX</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {data.per_strike.slice(0, 20).map((s, i) => (
                                            <tr key={i}>
                                                <td style={{ ...styles.td, textAlign: 'left', color: tokens.textBright }}>
                                                    ${s.strike}
                                                </td>
                                                <td style={{ ...styles.td, color: tokens.danger }}>
                                                    {s.call_gex?.toFixed(0)}
                                                </td>
                                                <td style={{ ...styles.td, color: tokens.safe }}>
                                                    {s.put_gex?.toFixed(0)}
                                                </td>
                                                <td style={{
                                                    ...styles.td,
                                                    color: s.net_gex >= 0 ? tokens.safe : tokens.danger,
                                                    fontWeight: 600,
                                                }}>
                                                    {s.net_gex?.toFixed(0)}
                                                </td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        )}
                    </>
                )}
            </div>
        </div>
    );
}

export default GammaProfile;
