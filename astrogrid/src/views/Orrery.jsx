import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import api from '../api.js';

const viewStyles = {
    hero: {
        textAlign: 'center',
        padding: `${tokens.spacing.xxl} ${tokens.spacing.lg}`,
    },
    title: {
        fontSize: '26px',
        fontWeight: 700,
        color: tokens.textBright,
        fontFamily: tokens.fontSans,
        marginBottom: tokens.spacing.xs,
    },
    subtitle: {
        fontSize: '12px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        letterSpacing: '2px',
        textTransform: 'uppercase',
    },
    placeholder: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: '280px',
        background: 'radial-gradient(circle at 50% 50%, rgba(74, 158, 255, 0.08) 0%, transparent 70%)',
        borderRadius: tokens.radius.xl,
        border: `1px dashed ${tokens.cardBorder}`,
        margin: `0 ${tokens.spacing.lg} ${tokens.spacing.lg}`,
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        fontSize: '13px',
    },
    retroCard: {
        ...styles.card,
        display: 'flex',
        alignItems: 'center',
        gap: tokens.spacing.md,
    },
    retroDot: (active) => ({
        width: '10px',
        height: '10px',
        borderRadius: '50%',
        background: active ? tokens.red : tokens.green,
        boxShadow: active ? `0 0 8px ${tokens.red}` : `0 0 8px ${tokens.green}`,
        flexShrink: 0,
    }),
    planetName: {
        fontSize: '14px',
        fontWeight: 600,
        color: tokens.textBright,
    },
    planetDetail: {
        fontSize: '12px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
    },
};

export default function Orrery() {
    const [data, setData] = useState(null);
    const [retrogrades, setRetrogrades] = useState(null);
    const [error, setError] = useState(null);

    useEffect(() => {
        api.getCelestialOverview()
            .then(setData)
            .catch(e => setError(e.message));
        api.getRetrogrades()
            .then(setRetrogrades)
            .catch(() => {});
    }, []);

    return (
        <div>
            <div style={viewStyles.hero}>
                <div style={viewStyles.title}>Planetary Orrery</div>
                <div style={viewStyles.subtitle}>3D Celestial Mechanics</div>
            </div>

            <div style={viewStyles.placeholder}>
                3D Three.js Orrery — Coming Soon
            </div>

            <div style={styles.container}>
                <div style={styles.subheader}>Retrograde Status</div>

                {error && <div style={styles.error}>{error}</div>}

                {retrogrades && Array.isArray(retrogrades.planets) ? (
                    retrogrades.planets.map((p, i) => (
                        <div key={i} style={viewStyles.retroCard}>
                            <div style={viewStyles.retroDot(p.is_retrograde)} />
                            <div>
                                <div style={viewStyles.planetName}>{p.name}</div>
                                <div style={viewStyles.planetDetail}>
                                    {p.is_retrograde ? `Retrograde until ${p.direct_date || 'TBD'}` : 'Direct'}
                                    {p.sign ? ` in ${p.sign}` : ''}
                                </div>
                            </div>
                        </div>
                    ))
                ) : retrogrades ? (
                    <div style={styles.card}>
                        <div style={styles.value}>
                            {JSON.stringify(retrogrades, null, 2).slice(0, 300)}
                        </div>
                    </div>
                ) : !error ? (
                    <div style={styles.loading}>Loading retrogrades...</div>
                ) : null}

                {data && (
                    <>
                        <div style={{ ...styles.subheader, marginTop: tokens.spacing.xl }}>
                            Celestial Overview
                        </div>
                        <div style={styles.metricGrid}>
                            {Object.entries(data).slice(0, 8).map(([key, val]) => (
                                <div key={key} style={styles.metric}>
                                    <div style={styles.metricValue}>
                                        {typeof val === 'number' ? val.toFixed(1) : typeof val === 'string' ? val.slice(0, 6) : '--'}
                                    </div>
                                    <div style={styles.metricLabel}>{key.replace(/_/g, ' ')}</div>
                                </div>
                            ))}
                        </div>
                    </>
                )}
            </div>
        </div>
    );
}
