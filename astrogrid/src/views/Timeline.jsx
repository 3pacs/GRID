import React, { useEffect, useState } from 'react';
import { tokens, styles } from '../styles/tokens.js';
import api from '../api.js';

const tlStyles = {
    event: {
        ...styles.card,
        display: 'flex',
        gap: tokens.spacing.md,
    },
    line: {
        display: 'flex',
        flexDirection: 'column',
        alignItems: 'center',
        width: '24px',
        flexShrink: 0,
    },
    dot: (color) => ({
        width: '10px',
        height: '10px',
        borderRadius: '50%',
        background: color || tokens.accent,
        boxShadow: `0 0 8px ${color || tokens.accent}`,
    }),
    connector: {
        width: '2px',
        flex: 1,
        background: tokens.cardBorder,
        marginTop: '4px',
    },
    date: {
        fontSize: '11px',
        color: tokens.accent,
        fontFamily: tokens.fontMono,
        fontWeight: 600,
    },
    eventName: {
        fontSize: '14px',
        fontWeight: 600,
        color: tokens.textBright,
        marginTop: '2px',
    },
    eventDetail: {
        fontSize: '12px',
        color: tokens.textMuted,
        marginTop: tokens.spacing.xs,
        lineHeight: '1.5',
    },
};

const eventColors = {
    retrograde: tokens.red,
    eclipse: tokens.purple,
    conjunction: tokens.gold,
    ingress: tokens.accent,
    default: tokens.accent,
};

export default function Timeline() {
    const [events, setEvents] = useState([]);
    const [eclipses, setEclipses] = useState([]);
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        setLoading(true);
        Promise.all([
            api.getTimeline().catch(() => ({ events: [] })),
            api.getEclipses().catch(() => ({ eclipses: [] })),
        ]).then(([tl, ecl]) => {
            const tlEvents = Array.isArray(tl) ? tl : tl.events || [];
            const eclEvents = (Array.isArray(ecl) ? ecl : ecl.eclipses || []).map(e => ({
                ...e,
                type: 'eclipse',
                name: e.name || `${e.type || ''} Eclipse`,
            }));
            setEvents(tlEvents);
            setEclipses(eclEvents);
        })
        .catch(e => setError(e.message))
        .finally(() => setLoading(false));
    }, []);

    const allEvents = [...events, ...eclipses].sort((a, b) => {
        const da = a.date || a.datetime || '';
        const db = b.date || b.datetime || '';
        return da.localeCompare(db);
    });

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Timeline</div>
            <div style={styles.subheader}>Upcoming Events</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Loading timeline...</div>}

            {allEvents.length > 0 ? (
                allEvents.slice(0, 30).map((ev, i) => (
                    <div key={i} style={tlStyles.event}>
                        <div style={tlStyles.line}>
                            <div style={tlStyles.dot(eventColors[ev.type] || eventColors.default)} />
                            {i < allEvents.length - 1 && <div style={tlStyles.connector} />}
                        </div>
                        <div style={{ flex: 1 }}>
                            <div style={tlStyles.date}>
                                {ev.date || ev.datetime || 'TBD'}
                            </div>
                            <div style={tlStyles.eventName}>
                                {ev.name || ev.event || ev.label || 'Celestial Event'}
                            </div>
                            {(ev.description || ev.detail) && (
                                <div style={tlStyles.eventDetail}>
                                    {ev.description || ev.detail}
                                </div>
                            )}
                        </div>
                    </div>
                ))
            ) : !loading && !error ? (
                <div style={styles.card}>
                    <div style={{ textAlign: 'center', color: tokens.textMuted, padding: tokens.spacing.xl }}>
                        No upcoming events loaded. Timeline will populate from the celestial engine.
                    </div>
                </div>
            ) : null}
        </div>
    );
}
