import React, { useEffect, useMemo, useState } from 'react';
import api from '../api.js';
import ChineseCalendar from '../components/ChineseCalendar.jsx';
import EclipseCountdown from '../components/EclipseCountdown.jsx';
import MoonPhaseWheel from '../components/MoonPhaseWheel.jsx';
import NakshatraWheel from '../components/NakshatraWheel.jsx';
import SolarActivityGauge from '../components/SolarActivityGauge.jsx';
import { computeLunarPhase, computeNakshatra } from '../lib/ephemeris.js';
import { buildEclipseFallback, extractChineseMetrics, extractSolarMetrics } from '../lib/fallbacks.js';
import { normalizeAstrogridLunar, normalizeAstrogridNakshatra } from '../lib/snapshot.js';
import useAstrogridSnapshot from '../hooks/useAstrogridSnapshot.js';
import useStore from '../store.js';
import { tokens, styles } from '../styles/tokens.js';

export default function LunarDashboard() {
    const { apiMode, celestialData, preferences, selectedDate, setCelestialData } = useStore();
    const [error, setError] = useState(null);
    const [eclipses, setEclipses] = useState(null);
    const [telemetryMode, setTelemetryMode] = useState('loading');
    const [telemetryNote, setTelemetryNote] = useState('Waiting for celestial telemetry.');
    const [eclipseMode, setEclipseMode] = useState('loading');
    const [eclipseNote, setEclipseNote] = useState('Waiting for eclipse data.');
    const snapshotEnabled = preferences.useLiveTelemetry && apiMode === 'live';
    const { snapshot, error: snapshotError } = useAstrogridSnapshot(selectedDate, snapshotEnabled);

    const date = useMemo(() => new Date(`${selectedDate}T12:00:00Z`), [selectedDate]);
    const localLunar = useMemo(() => computeLunarPhase(date), [date]);
    const localNakshatra = useMemo(() => computeNakshatra(date), [date]);
    const liveLunar = useMemo(() => normalizeAstrogridLunar(snapshot), [snapshot]);
    const liveNakshatra = useMemo(() => normalizeAstrogridNakshatra(snapshot), [snapshot]);
    const lunar = liveLunar?.phase_name ? liveLunar : localLunar;
    const nakshatra = liveNakshatra?.nakshatra_name ? liveNakshatra : localNakshatra;
    const solar = extractSolarMetrics(celestialData);
    const chinese = extractChineseMetrics(celestialData);
    const hasTelemetry = Boolean(celestialData?.categories);
    const displayError = error || snapshotError;

    useEffect(() => {
        let cancelled = false;

        if (hasTelemetry) {
            setTelemetryMode('cached-live');
            setTelemetryNote('Celestial signal feed is already cached in session.');
            setError(null);
        } else if (preferences.useLiveTelemetry) {
            setTelemetryMode('loading');
            setTelemetryNote('Fetching live celestial signals for the dashboard.');
            api.getCelestialSignals()
                .then((payload) => {
                    if (cancelled) return;
                    setCelestialData(payload);
                    setTelemetryMode('live');
                    setTelemetryNote('Live celestial signal feed loaded successfully.');
                    setError(null);
                })
                .catch((e) => {
                    if (cancelled) return;
                    setError(e.message);
                    setTelemetryMode('demo');
                    setTelemetryNote('Live celestial signal feed is unavailable, so the dashboard is using deterministic fallback values.');
                });
        } else {
            setTelemetryMode('demo');
            setTelemetryNote('Live telemetry is disabled, so this dashboard is using deterministic fallback values.');
            setError(null);
        }

        api.getEclipses()
            .then((payload) => {
                if (cancelled) return;
                setEclipses(payload);
                setEclipseMode('live');
                setEclipseNote('Live eclipse timing loaded from the backend.');
            })
            .catch(() => {
                if (cancelled) return;
                setEclipses(buildEclipseFallback(date));
                setEclipseMode('demo');
                setEclipseNote('Live eclipse timing is unavailable, so the dashboard is using a deterministic fallback countdown.');
            });

        return () => {
            cancelled = true;
        };
    }, [date, hasTelemetry, preferences.useLiveTelemetry, setCelestialData]);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Lunar Dashboard</div>
            <div style={styles.subheader}>Moon Phase and Regime Overlay</div>

            {displayError && <div style={styles.error}>{displayError}</div>}
            <div style={styles.card}>
                <div style={styles.subheader}>Telemetry Source</div>
                <div style={styles.value}>
                    {telemetryMode === 'loading'
                        ? 'Checking live feed...'
                        : telemetryMode === 'live'
                        ? 'Live feed'
                        : telemetryMode === 'cached-live'
                            ? 'Cached live feed'
                            : 'Demo mode'}
                </div>
                <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                    {telemetryNote}
                </div>
            </div>

            <MoonPhaseWheel
                phase={lunar.phase}
                illumination={Math.round(lunar.illumination)}
                label={lunar.phase_name}
                regime={lunar.phase < 0.5 ? 'Expansion' : 'Distribution'}
                subtitle={`${lunar.days_to_full.toFixed(1)} days to full moon, ${lunar.days_to_new.toFixed(1)} days to new moon.`}
            />

            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
                gap: tokens.spacing.md,
                marginTop: tokens.spacing.lg,
            }}>
                <NakshatraWheel
                    index={nakshatra.nakshatra_index}
                    name={nakshatra.nakshatra_name}
                    quality={nakshatra.quality}
                    rulingPlanet={nakshatra.ruling_planet}
                    deity={nakshatra.deity}
                />
                <div style={{ display: 'flex', flexDirection: 'column', gap: tokens.spacing.md }}>
                    <EclipseCountdown eclipse={eclipses?.next_lunar || buildEclipseFallback(date).next_lunar} title="Next Lunar Eclipse" />
                    <div style={styles.card}>
                        <div style={styles.subheader}>Eclipse Source</div>
                        <div style={styles.value}>
                            {eclipseMode === 'loading'
                                ? 'Checking eclipse data...'
                                : eclipseMode === 'live'
                                    ? 'Live eclipse timing'
                                    : 'Fallback eclipse timing'}
                        </div>
                        <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                            {eclipseNote}
                        </div>
                    </div>
                    {preferences.showSolarLayer && (
                        <SolarActivityGauge
                            kpIndex={solar.kpIndex}
                            sunspotNumber={solar.sunspotNumber}
                            solarWindSpeed={solar.solarWindSpeed}
                            flareClass={solar.flareClass}
                        />
                    )}
                </div>
            </div>

            {preferences.showChineseLayer && (
                <div style={{ marginTop: tokens.spacing.lg }}>
                    <ChineseCalendar {...chinese} />
                </div>
            )}
        </div>
    );
}
