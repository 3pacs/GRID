import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import useStore from '../store.js';
import api from '../api.js';

const corrStyles = {
    bar: (value) => ({
        height: '6px',
        borderRadius: '3px',
        background: value > 0
            ? `linear-gradient(90deg, transparent, ${tokens.green})`
            : `linear-gradient(90deg, ${tokens.red}, transparent)`,
        width: `${Math.min(Math.abs(value) * 100, 100)}%`,
        marginLeft: value < 0 ? 'auto' : 0,
    }),
    corrValue: (value) => ({
        fontSize: '14px',
        fontWeight: 700,
        fontFamily: tokens.fontMono,
        color: value > 0.3 ? tokens.green : value < -0.3 ? tokens.red : tokens.textMuted,
    }),
};

export default function Correlations() {
    const { correlations, setCorrelations } = useStore();
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        setLoading(true);
        api.getCorrelations()
            .then(data => {
                setCorrelations(Array.isArray(data) ? data : data.correlations || []);
            })
            .catch(e => setError(e.message))
            .finally(() => setLoading(false));
    }, []);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Correlations</div>
            <div style={styles.subheader}>Market-Astro Patterns</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Analyzing correlations...</div>}

            {correlations.length > 0 ? (
                correlations.slice(0, 20).map((c, i) => (
                    <div key={i} style={styles.card}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: tokens.spacing.sm }}>
                            <div style={{ fontSize: '13px', fontWeight: 600, color: tokens.textBright }}>
                                {c.event || c.name || c.label || `Pattern ${i + 1}`}
                            </div>
                            <div style={corrStyles.corrValue(c.correlation || c.value || 0)}>
                                {((c.correlation || c.value || 0) > 0 ? '+' : '')}{((c.correlation || c.value || 0) * 100).toFixed(1)}%
                            </div>
                        </div>
                        <div style={corrStyles.bar(c.correlation || c.value || 0)} />
                        {c.description && (
                            <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                                {c.description}
                            </div>
                        )}
                    </div>
                ))
            ) : !loading && !error ? (
                <div style={styles.card}>
                    <div style={{ textAlign: 'center', color: tokens.textMuted, padding: tokens.spacing.xl }}>
                        No correlation data available yet.
                        Correlations will appear once celestial-market analysis runs.
                    </div>
                </div>
            ) : null}
        </div>
    );
}
