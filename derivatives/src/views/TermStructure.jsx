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
    chartPlaceholder: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.lg, height: '280px',
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        marginBottom: tokens.spacing.lg, color: tokens.textMuted,
        fontSize: '12px', fontFamily: tokens.fontMono,
    },
    table: {
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

function TermStructure() {
    const { selectedTicker } = useStore();
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getTermStructure(selectedTicker)
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, [selectedTicker]);

    const points = data?.term_structure || [];

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>IV Term Structure</div>
                {loading && <div style={styles.loading}>Loading term structure...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && (
                    <>
                        <div style={styles.chartPlaceholder}>
                            Term Structure Curve — {points.length} expiries
                        </div>
                        {points.length > 0 && (
                            <div style={{ overflowX: 'auto' }}>
                                <table style={styles.table}>
                                    <thead>
                                        <tr>
                                            <th style={{ ...styles.th, textAlign: 'left' }}>Expiry</th>
                                            <th style={styles.th}>DTE</th>
                                            <th style={styles.th}>IV ATM</th>
                                            <th style={styles.th}>25d Put</th>
                                            <th style={styles.th}>25d Call</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {points.map((p, i) => (
                                            <tr key={i}>
                                                <td style={{ ...styles.td, textAlign: 'left', color: tokens.textBright }}>
                                                    {p.expiry}
                                                </td>
                                                <td style={styles.td}>{p.dte}</td>
                                                <td style={{ ...styles.td, color: tokens.accent, fontWeight: 600 }}>
                                                    {p.iv_atm != null ? (p.iv_atm * 100).toFixed(1) + '%' : '--'}
                                                </td>
                                                <td style={styles.td}>
                                                    {p.iv_25d_put != null ? (p.iv_25d_put * 100).toFixed(1) + '%' : '--'}
                                                </td>
                                                <td style={styles.td}>
                                                    {p.iv_25d_call != null ? (p.iv_25d_call * 100).toFixed(1) + '%' : '--'}
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

export default TermStructure;
