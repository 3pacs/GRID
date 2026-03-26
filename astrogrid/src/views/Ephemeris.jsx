import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import api from '../api.js';

const ephStyles = {
    dateInput: {
        background: 'rgba(10, 18, 35, 0.6)',
        border: `1px solid ${tokens.cardBorder}`,
        borderRadius: tokens.radius.sm,
        color: tokens.text,
        padding: '10px 14px',
        fontSize: '14px',
        fontFamily: tokens.fontMono,
        width: '100%',
        boxSizing: 'border-box',
        colorScheme: 'dark',
    },
    row: {
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '10px 0',
        borderBottom: `1px solid rgba(74, 158, 255, 0.08)`,
    },
    planetName: {
        fontSize: '14px',
        fontWeight: 600,
        color: tokens.textBright,
    },
    planetSign: {
        fontSize: '13px',
        color: tokens.purple,
        fontFamily: tokens.fontMono,
    },
    planetDegree: {
        fontSize: '12px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
    },
};

export default function Ephemeris() {
    const [date, setDate] = useState(() => new Date().toISOString().slice(0, 10));
    const [data, setData] = useState(null);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    const fetchEphemeris = (d) => {
        setLoading(true);
        setError(null);
        api.getEphemeris(d)
            .then(setData)
            .catch(e => setError(e.message))
            .finally(() => setLoading(false));
    };

    useEffect(() => {
        fetchEphemeris(date);
    }, []);

    const handleDateChange = (e) => {
        const newDate = e.target.value;
        setDate(newDate);
        fetchEphemeris(newDate);
    };

    return (
        <div style={styles.container}>
            <div style={styles.header}>Ephemeris</div>
            <div style={styles.subheader}>Planetary Positions</div>

            <div style={{ marginBottom: tokens.spacing.lg }}>
                <input
                    type="date"
                    value={date}
                    onChange={handleDateChange}
                    style={ephStyles.dateInput}
                />
            </div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Calculating positions...</div>}

            {data && !loading && (
                <div style={styles.card}>
                    {data.planets ? (
                        data.planets.map((p, i) => (
                            <div key={i} style={ephStyles.row}>
                                <div>
                                    <div style={ephStyles.planetName}>{p.name}</div>
                                    {p.degree != null && (
                                        <div style={ephStyles.planetDegree}>{p.degree.toFixed(2)}</div>
                                    )}
                                </div>
                                <div style={ephStyles.planetSign}>
                                    {p.sign || '--'}
                                    {p.retrograde ? ' Rx' : ''}
                                </div>
                            </div>
                        ))
                    ) : (
                        <div style={styles.value}>
                            <pre style={{ whiteSpace: 'pre-wrap', fontSize: '12px' }}>
                                {JSON.stringify(data, null, 2).slice(0, 800)}
                            </pre>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}
