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
    narrativeCard: {
        background: tokens.card, border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.md, padding: tokens.spacing.xl,
        marginBottom: tokens.spacing.lg,
    },
    narrativeTitle: {
        fontSize: '14px', fontWeight: 700, color: tokens.accent,
        fontFamily: tokens.fontMono, marginBottom: tokens.spacing.md,
        letterSpacing: '1px',
    },
    narrativeText: {
        fontSize: '12px', lineHeight: '1.8', color: tokens.text,
        fontFamily: tokens.fontMono, whiteSpace: 'pre-wrap',
    },
    regimeTag: {
        display: 'inline-block', padding: `${tokens.spacing.xs} ${tokens.spacing.md}`,
        borderRadius: tokens.radius.sm, fontSize: '10px', fontWeight: 700,
        fontFamily: tokens.fontMono, letterSpacing: '1px', marginBottom: tokens.spacing.lg,
    },
    loading: {
        display: 'flex', alignItems: 'center', justifyContent: 'center',
        height: '200px', color: tokens.textMuted, fontSize: '12px', fontFamily: tokens.fontMono,
    },
    error: { color: tokens.danger, fontSize: '12px', fontFamily: tokens.fontMono, padding: tokens.spacing.lg },
};

function FlowNarrative() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);

    useEffect(() => {
        let cancelled = false;
        setLoading(true); setError(null);
        api.getFlowNarrative()
            .then((res) => { if (!cancelled) { setData(res); setLoading(false); } })
            .catch((err) => { if (!cancelled) { setError(err.message); setLoading(false); } });
        return () => { cancelled = true; };
    }, []);

    const regimeColor = data?.regime === 'LONG_GAMMA' ? tokens.safe
        : data?.regime === 'SHORT_GAMMA' ? tokens.danger : tokens.caution;

    return (
        <div>
            <TickerSelector />
            <div style={styles.container}>
                <div style={styles.header}>Flow Narrative</div>
                {loading && <div style={styles.loading}>Generating narrative...</div>}
                {error && <div style={styles.error}>{error}</div>}
                {!loading && !error && data && (
                    <div style={styles.narrativeCard}>
                        {data.regime && (
                            <div style={{
                                ...styles.regimeTag,
                                background: `${regimeColor}22`,
                                color: regimeColor,
                            }}>
                                {data.regime.replace('_', ' ')}
                            </div>
                        )}
                        <div style={styles.narrativeTitle}>Market Flow Briefing</div>
                        <div style={styles.narrativeText}>
                            {data.narrative || data.briefing || 'No narrative available.'}
                        </div>
                    </div>
                )}
            </div>
        </div>
    );
}

export default FlowNarrative;
