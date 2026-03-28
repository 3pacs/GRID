import React, { useEffect, useMemo, useState } from 'react';
import api from '../api.js';
import PlanetaryOrrery from '../components/PlanetaryOrrery.jsx';
import RetrogradeBanner from '../components/RetrogradeBanner.jsx';
import {
    buildRetrogradeSummary,
    describeAspectTone,
    getCategoryHighlights,
    normalizeCelestialCategories,
    summarizeCategories,
} from '../lib/interpret.js';
import { computeAllPositions, computeAspects, computeLunarPhase } from '../lib/ephemeris.js';
import { normalizeAstrogridAspects, normalizeAstrogridBodies, normalizeAstrogridLunar } from '../lib/snapshot.js';
import useAstrogridSnapshot from '../hooks/useAstrogridSnapshot.js';
import useStore from '../store.js';
import { tokens, styles } from '../styles/tokens.js';

const viewStyles = {
    hero: {
        padding: `clamp(20px, 4vw, 32px) clamp(16px, 4vw, 24px) ${tokens.spacing.lg}`,
    },
    heroGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(280px, 1fr))',
        gap: tokens.spacing.lg,
        alignItems: 'start',
    },
    heroCopy: {
        paddingTop: tokens.spacing.md,
    },
    eyebrow: {
        fontSize: '11px',
        color: tokens.accent,
        fontFamily: tokens.fontMono,
        letterSpacing: '2px',
        textTransform: 'uppercase',
        marginBottom: tokens.spacing.sm,
    },
    title: {
        fontSize: 'clamp(28px, 5vw, 48px)',
        fontWeight: 700,
        color: tokens.textBright,
        fontFamily: tokens.fontSans,
        lineHeight: 1.05,
        maxWidth: '11ch',
    },
    subtitle: {
        marginTop: tokens.spacing.md,
        fontSize: '14px',
        color: tokens.text,
        maxWidth: '54ch',
        lineHeight: 1.7,
    },
    statRail: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(120px, 1fr))',
        gap: tokens.spacing.sm,
        marginTop: tokens.spacing.lg,
    },
    statChip: {
        padding: '10px 12px',
        borderRadius: tokens.radius.pill,
        background: 'rgba(10, 18, 35, 0.7)',
        border: `1px solid ${tokens.cardBorder}`,
        minWidth: '120px',
    },
    highlights: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))',
        gap: tokens.spacing.md,
    },
    highlightCard: {
        ...styles.card,
        minHeight: '112px',
    },
    smallLabel: {
        fontSize: '10px',
        textTransform: 'uppercase',
        letterSpacing: '1.2px',
        color: tokens.textMuted,
        fontFamily: tokens.fontMono,
    },
    emphasis: {
        fontSize: '20px',
        color: tokens.textBright,
        fontWeight: 700,
        marginTop: tokens.spacing.sm,
    },
    responsiveInfo: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        gap: tokens.spacing.md,
        marginTop: tokens.spacing.lg,
    },
};

export default function Orrery() {
    const { apiMode, celestialData, celestialStatus, preferences, selectedDate, setCelestialData } = useStore();
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);
    const snapshotEnabled = preferences.useLiveTelemetry && apiMode === 'live';
    const { snapshot, status: snapshotStatus, error: snapshotError } = useAstrogridSnapshot(selectedDate, snapshotEnabled);

    const referenceDate = useMemo(() => {
        const parsed = new Date(`${selectedDate}T12:00:00Z`);
        return Number.isNaN(parsed.getTime()) ? new Date() : parsed;
    }, [selectedDate]);
    const localPositions = useMemo(() => Object.values(computeAllPositions(referenceDate)), [referenceDate]);
    const localAspects = useMemo(() => computeAspects(referenceDate), [referenceDate]);
    const localLunar = useMemo(() => computeLunarPhase(referenceDate), [referenceDate]);
    const livePositions = useMemo(() => normalizeAstrogridBodies(snapshot).map((body) => ({
        planet: body.planet || body.name,
        geocentric_longitude: body.geocentric_longitude ?? body.longitude,
        right_ascension: body.right_ascension ?? body.rightAscension ?? 0,
        zodiac_sign: body.sign || 'Unknown',
        zodiac_degree: body.zodiac_degree ?? body.degree ?? 0,
        is_retrograde: Boolean(body.is_retrograde ?? body.retrograde),
    })), [snapshot]);
    const liveAspects = useMemo(() => normalizeAstrogridAspects(snapshot).map((aspect) => ({
        planet1: aspect.planet1,
        planet2: aspect.planet2,
        aspect_type: aspect.aspect_type,
        nature: aspect.nature || 'variable',
        applying: Boolean(aspect.applying),
        orb_used: aspect.orb_used ?? aspect.orb ?? 0,
    })), [snapshot]);
    const liveLunar = useMemo(() => normalizeAstrogridLunar(snapshot), [snapshot]);
    const positions = livePositions.length ? livePositions : localPositions;
    const aspects = liveAspects.length ? liveAspects : localAspects;
    const lunar = liveLunar?.phase_name ? liveLunar : localLunar;

    const retrogrades = useMemo(
        () => positions.filter((body) => body.is_retrograde && body.planet !== 'Rahu' && body.planet !== 'Ketu'),
        [positions]
    );
    const categories = normalizeCelestialCategories(celestialData);
    const categorySummary = summarizeCategories(celestialData);
    const highlights = getCategoryHighlights(celestialData);
    const hasLiveTelemetry = Boolean(celestialData?.categories);
    const hasLiveSnapshot = Boolean(livePositions.length || liveAspects.length);
    const sourceLabel = hasLiveSnapshot
        ? hasLiveTelemetry
            ? 'Live snapshot + signal feed'
            : 'Live snapshot geometry'
        : preferences.useLiveTelemetry && hasLiveTelemetry
            ? 'Live signal feed'
            : 'Local ephemeris';
    const displayError = error || snapshotError;

    useEffect(() => {
        let cancelled = false;

        if (!preferences.useLiveTelemetry) {
            setError(null);
            setLoading(false);
            return () => {
                cancelled = true;
            };
        }

        if (hasLiveTelemetry) {
            setLoading(false);
            return () => {
                cancelled = true;
            };
        }

        if (celestialStatus === 'loading') {
            setLoading(true);
            return () => {
                cancelled = true;
            };
        }

        setLoading(true);
        setError(null);
        api.getCelestialSignals()
            .then((payload) => {
                if (!cancelled) setCelestialData(payload);
            })
            .catch((e) => {
                if (!cancelled) setError(e.message);
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });

        return () => {
            cancelled = true;
        };
    }, [celestialStatus, hasLiveTelemetry, preferences.useLiveTelemetry, setCelestialData]);

    return (
        <div>
            <div style={viewStyles.hero}>
                <div style={viewStyles.heroGrid}>
                    <div>
                        <PlanetaryOrrery
                            positions={positions}
                            aspects={aspects}
                            showAspectLines={preferences.showAspectLines}
                            autoRotate={preferences.animateOrbits}
                        />
                    </div>
                    <div style={viewStyles.heroCopy}>
                        <div style={viewStyles.eyebrow}>Celestial Mechanics</div>
                        <div style={viewStyles.title}>AstroGrid Orrery</div>
                        <div style={viewStyles.subtitle}>
                            Live orbital geometry rendered from client-side ephemeris math, with
                            aspect structure and celestial feature telemetry layered on top.
                        </div>
                        <div style={viewStyles.statRail}>
                            <div style={viewStyles.statChip}>
                                <div style={viewStyles.smallLabel}>Session Date</div>
                                <div style={styles.value}>{selectedDate}</div>
                            </div>
                            <div style={viewStyles.statChip}>
                                <div style={viewStyles.smallLabel}>Tracked Bodies</div>
                                <div style={styles.value}>{positions.length}</div>
                            </div>
                            <div style={viewStyles.statChip}>
                                <div style={viewStyles.smallLabel}>Active Aspects</div>
                                <div style={styles.value}>{aspects.length}</div>
                            </div>
                            <div style={viewStyles.statChip}>
                                <div style={viewStyles.smallLabel}>Moon Phase</div>
                                <div style={styles.value}>{lunar.phase_name}</div>
                            </div>
                            <div style={viewStyles.statChip}>
                                <div style={viewStyles.smallLabel}>Signal Source</div>
                                <div style={styles.value}>{sourceLabel}</div>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <div style={styles.container}>
                {displayError && <div style={styles.error}>{displayError}</div>}
                <div style={{ ...styles.label, marginBottom: tokens.spacing.md }}>
                    Reference time is pinned to {selectedDate} so the hero and support panels stay aligned with the shared session state.
                </div>

                <RetrogradeBanner
                    retrogrades={retrogrades.map((body) => body.planet)}
                    summary={buildRetrogradeSummary(Object.fromEntries(positions.map((body) => [body.planet, body])))}
                />

                <div style={viewStyles.responsiveInfo}>
                    <div style={styles.card}>
                        <div style={styles.subheader}>Aspect Climate</div>
                        <div style={viewStyles.emphasis}>{describeAspectTone(aspects)}</div>
                        <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                            {aspects
                                .filter((aspect) => aspect.orb_used <= 4)
                                .slice(0, 3)
                                .map((aspect) => `${aspect.planet1}-${aspect.planet2} ${aspect.aspect_type}`)
                                .join(' | ') || 'No tight major aspects today'}
                        </div>
                    </div>
                    <div style={styles.card}>
                        <div style={styles.subheader}>Signal Categories</div>
                        <div style={styles.metricGrid}>
                            {categorySummary.map((item) => (
                                <div key={item.key} style={styles.metric}>
                                    <div style={styles.metricValue}>{item.count}</div>
                                    <div style={styles.metricLabel}>{item.label}</div>
                                </div>
                            ))}
                        </div>
                    </div>
                    <div style={styles.card}>
                        <div style={styles.subheader}>Lunar Clock</div>
                        <div style={viewStyles.emphasis}>{lunar.illumination.toFixed(1)}%</div>
                        <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                            {lunar.phase_name} | {lunar.days_to_full.toFixed(1)} days to full | {lunar.days_to_new.toFixed(1)} days to new
                        </div>
                    </div>
                </div>

                <div style={{ ...styles.subheader, marginTop: tokens.spacing.xl }}>Celestial Telemetry</div>
                {(loading || snapshotStatus === 'loading') && !highlights.length ? (
                    <div style={styles.loading}>Loading live celestial signals...</div>
                ) : (
                    <div style={viewStyles.highlights}>
                        {highlights.map((item) => (
                            <div key={`${item.category}-${item.feature}`} style={viewStyles.highlightCard}>
                                <div style={viewStyles.smallLabel}>{item.category}</div>
                                <div style={viewStyles.emphasis}>{item.valueLabel}</div>
                                <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                                    {item.label}
                                </div>
                            </div>
                        ))}
                        {!highlights.length && (
                            <div style={styles.card}>
                                <div style={styles.value}>
                                    {sourceLabel} is the current driver for this view. The Orrery uses local ephemeris math,
                                    and this section will enrich automatically when the signal feed populates.
                                </div>
                            </div>
                        )}
                    </div>
                )}

                <div style={{ ...styles.subheader, marginTop: tokens.spacing.xl }}>Category Detail</div>
                <div style={styles.metricGrid}>
                    {Object.entries(categories).map(([key, items]) => (
                        <div key={key} style={styles.metric}>
                            <div style={styles.metricValue}>{items.length}</div>
                            <div style={styles.metricLabel}>{key}</div>
                        </div>
                    ))}
                </div>
            </div>
        </div>
    );
}
