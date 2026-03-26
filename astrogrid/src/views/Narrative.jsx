import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import useStore from '../store.js';
import api from '../api.js';

const narrStyles = {
    briefingCard: {
        ...styles.card,
        padding: tokens.spacing.xl,
        lineHeight: '1.8',
        fontSize: '14px',
        color: tokens.text,
        whiteSpace: 'pre-wrap',
        fontFamily: tokens.fontSans,
    },
    timestamp: {
        fontSize: '11px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        marginBottom: tokens.spacing.md,
    },
    solar: {
        ...styles.card,
        borderLeft: `3px solid ${tokens.gold}`,
    },
};

export default function Narrative() {
    const { briefing, setBriefing } = useStore();
    const [solar, setSolar] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        setLoading(true);
        api.getBriefing()
            .then(data => {
                setBriefing(data.briefing || data.text || data.content || JSON.stringify(data, null, 2));
            })
            .catch(e => setError(e.message))
            .finally(() => setLoading(false));

        api.getSolarActivity()
            .then(setSolar)
            .catch(() => {});
    }, []);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Narrative</div>
            <div style={styles.subheader}>Intelligence Briefing</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Generating celestial briefing...</div>}

            {briefing && !loading && (
                <div style={narrStyles.briefingCard}>
                    {briefing}
                </div>
            )}

            {!briefing && !loading && !error && (
                <div style={styles.card}>
                    <div style={{ textAlign: 'center', color: tokens.textMuted, padding: tokens.spacing.xl }}>
                        No briefing available. The celestial narrative engine will generate one on the next cycle.
                    </div>
                </div>
            )}

            {solar && (
                <>
                    <div style={{ ...styles.subheader, marginTop: tokens.spacing.xl }}>
                        Solar Activity
                    </div>
                    <div style={narrStyles.solar}>
                        <div style={styles.metricGrid}>
                            {Object.entries(solar).slice(0, 6).map(([key, val]) => (
                                <div key={key} style={styles.metric}>
                                    <div style={{ ...styles.metricValue, color: tokens.gold }}>
                                        {typeof val === 'number' ? val.toFixed(1) : typeof val === 'string' ? val : '--'}
                                    </div>
                                    <div style={styles.metricLabel}>{key.replace(/_/g, ' ')}</div>
                                </div>
                            ))}
                        </div>
                    </div>
                </>
            )}
        </div>
    );
}
