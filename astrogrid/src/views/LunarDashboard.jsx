import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import api from '../api.js';

const moonStyles = {
    phaseCircle: {
        width: '120px',
        height: '120px',
        borderRadius: '50%',
        background: 'radial-gradient(circle at 40% 40%, #E8F0F8 0%, #5A7080 50%, #0A1628 100%)',
        margin: '0 auto',
        boxShadow: '0 0 40px rgba(74, 158, 255, 0.15), inset 0 0 20px rgba(0,0,0,0.3)',
    },
    phaseLabel: {
        textAlign: 'center',
        fontSize: '18px',
        fontWeight: 600,
        color: tokens.textBright,
        marginTop: tokens.spacing.lg,
    },
    phaseDetail: {
        textAlign: 'center',
        fontSize: '13px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
        marginTop: tokens.spacing.xs,
    },
};

export default function LunarDashboard() {
    const [lunar, setLunar] = useState(null);
    const [nakshatra, setNakshatra] = useState(null);
    const [error, setError] = useState(null);

    useEffect(() => {
        api.getLunarCalendar()
            .then(setLunar)
            .catch(e => setError(e.message));
        api.getNakshatra()
            .then(setNakshatra)
            .catch(() => {});
    }, []);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Lunar Dashboard</div>
            <div style={styles.subheader}>Current Moon Phase</div>

            {error && <div style={styles.error}>{error}</div>}

            <div style={{ ...styles.card, textAlign: 'center', padding: tokens.spacing.xxl }}>
                <div style={moonStyles.phaseCircle} />
                {lunar ? (
                    <>
                        <div style={moonStyles.phaseLabel}>
                            {lunar.phase || lunar.moon_phase || 'Loading...'}
                        </div>
                        <div style={moonStyles.phaseDetail}>
                            {lunar.illumination != null
                                ? `${(lunar.illumination * 100).toFixed(1)}% illumination`
                                : lunar.percent ? `${lunar.percent}% illumination` : ''}
                        </div>
                        {lunar.sign && (
                            <div style={moonStyles.phaseDetail}>Moon in {lunar.sign}</div>
                        )}
                    </>
                ) : !error ? (
                    <div style={{ ...styles.loading, marginTop: tokens.spacing.lg }}>
                        Loading lunar data...
                    </div>
                ) : null}
            </div>

            {nakshatra && (
                <>
                    <div style={styles.subheader}>Nakshatra</div>
                    <div style={styles.card}>
                        <div style={{ fontSize: '16px', fontWeight: 600, color: tokens.gold }}>
                            {nakshatra.name || nakshatra.nakshatra || '--'}
                        </div>
                        {nakshatra.deity && (
                            <div style={{ ...styles.value, marginTop: tokens.spacing.xs }}>
                                Deity: {nakshatra.deity}
                            </div>
                        )}
                        {nakshatra.ruler && (
                            <div style={{ ...styles.value, marginTop: tokens.spacing.xs }}>
                                Ruler: {nakshatra.ruler}
                            </div>
                        )}
                    </div>
                </>
            )}

            {lunar && lunar.upcoming && (
                <>
                    <div style={styles.subheader}>Upcoming Phases</div>
                    {lunar.upcoming.map((event, i) => (
                        <div key={i} style={styles.card}>
                            <div style={{ fontSize: '14px', fontWeight: 600, color: tokens.textBright }}>
                                {event.phase || event.name}
                            </div>
                            <div style={styles.value}>{event.date || event.datetime}</div>
                        </div>
                    ))}
                </>
            )}
        </div>
    );
}
