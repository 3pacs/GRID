import React, { useEffect, useMemo, useState } from 'react';
import api from '../api.js';
import CelestialTimeline from '../components/CelestialTimeline.jsx';
import EclipseCountdown from '../components/EclipseCountdown.jsx';
import { buildEclipseFallback, buildTimelineFallback } from '../lib/mockData.js';
import useStore from '../store.js';
import { tokens, styles } from '../styles/tokens.js';

export default function Timeline() {
    const { selectedDate } = useStore();
    const [events, setEvents] = useState([]);
    const [eclipses, setEclipses] = useState(null);
    const [error, setError] = useState(null);
    const [timelineSource, setTimelineSource] = useState('loading');
    const [eclipseSource, setEclipseSource] = useState('loading');
    const [statusNote, setStatusNote] = useState('Waiting for live timeline and eclipse payloads.');
    const [loading, setLoading] = useState(false);

    const date = useMemo(() => new Date(`${selectedDate}T12:00:00Z`), [selectedDate]);
    const fallbackEvents = useMemo(() => buildTimelineFallback(date), [date]);
    const fallbackEclipses = useMemo(() => buildEclipseFallback(date), [date]);

    useEffect(() => {
        let cancelled = false;

        setError(null);
        setLoading(true);
        Promise.allSettled([
            api.getTimeline({
                start: selectedDate,
                end: new Date(date.getTime() + 1000 * 60 * 60 * 24 * 60).toISOString().slice(0, 10),
            }),
            api.getEclipses(),
        ]).then(([timelineResult, eclipseResult]) => {
            if (cancelled) return;

            let nextTimelineSource = 'demo';
            let nextEclipseSource = 'demo';
            const notes = [];

            if (timelineResult.status === 'fulfilled') {
                const timelinePayload = timelineResult.value;
                const timelineEvents = Array.isArray(timelinePayload)
                    ? timelinePayload
                    : timelinePayload?.events || timelinePayload?.items || [];

                if (timelineEvents.length) {
                    setEvents(timelineEvents);
                    nextTimelineSource = 'live';
                } else {
                    setEvents(fallbackEvents);
                    notes.push('Timeline payload was empty, so the fallback event ribbon is shown.');
                }
            } else {
                setEvents(fallbackEvents);
                notes.push('Timeline endpoint unavailable, so the fallback event ribbon is shown.');
                if (timelineResult.reason?.message) {
                    setError(timelineResult.reason.message);
                }
            }

            if (eclipseResult.status === 'fulfilled') {
                const eclipsePayload = eclipseResult.value;
                if (eclipsePayload?.next_lunar || eclipsePayload?.next_solar) {
                    setEclipses(eclipsePayload);
                    nextEclipseSource = 'live';
                } else {
                    setEclipses(fallbackEclipses);
                    notes.push('Eclipse payload was empty, so the fallback countdown is shown.');
                }
            } else {
                setEclipses(fallbackEclipses);
                notes.push('Eclipse endpoint unavailable, so the fallback countdown is shown.');
                if (!error && eclipseResult.reason?.message) {
                    setError(eclipseResult.reason.message);
                }
            }

            setTimelineSource(nextTimelineSource);
            setEclipseSource(nextEclipseSource);

            if (nextTimelineSource === 'live' && nextEclipseSource === 'live') {
                setStatusNote('Live timeline and eclipse data loaded.');
            } else if (nextTimelineSource === 'live' || nextEclipseSource === 'live') {
                setStatusNote('Partial live data loaded. Some fallback content remains visible.');
            } else {
                setStatusNote('Demo timeline mode is active because the live payloads were unavailable or empty.');
            }

            if (notes.length) {
                setStatusNote((current) => `${current} ${notes.join(' ')}`);
            }
        }).finally(() => {
            if (!cancelled) setLoading(false);
        });

        return () => {
            cancelled = true;
        };
    }, [date, fallbackEclipses, fallbackEvents, selectedDate]);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Timeline</div>
            <div style={styles.subheader}>Upcoming Events</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Loading timeline...</div>}
            <div style={styles.card}>
                <div style={styles.subheader}>Data Source</div>
                <div style={styles.value}>
                    {timelineSource === 'loading' || eclipseSource === 'loading'
                        ? 'Checking live timeline data...'
                        : timelineSource === 'live' && eclipseSource === 'live'
                        ? 'Live timeline and eclipse data'
                        : timelineSource === 'live' || eclipseSource === 'live'
                            ? 'Partial live data'
                            : 'Generated demo timeline'}
                </div>
                <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                    {statusNote}
                </div>
            </div>

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
                gap: tokens.spacing.md,
                marginBottom: tokens.spacing.lg,
            }}>
                <EclipseCountdown eclipse={eclipses?.next_lunar || fallbackEclipses.next_lunar} title="Next Lunar Eclipse" />
                <EclipseCountdown eclipse={eclipses?.next_solar || fallbackEclipses.next_solar} title="Next Solar Eclipse" accent={tokens.gold} />
            </div>

            <CelestialTimeline
                events={events.length ? events : fallbackEvents}
                title="Event Ribbon"
                subtitle="Forecast windows, aspects, and eclipse checkpoints"
            />
        </div>
    );
}
