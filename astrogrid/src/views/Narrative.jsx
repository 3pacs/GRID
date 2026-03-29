import React, { useEffect, useMemo, useState } from 'react';
import api from '../api.js';
import ChineseCalendar from '../components/ChineseCalendar.jsx';
import SolarActivityGauge from '../components/SolarActivityGauge.jsx';
import { normalizeAstrogridBriefing } from '../lib/contract.js';
import { buildNarrativeFallback } from '../lib/fallbacks.js';
import { extractChineseMetrics, extractSolarMetrics } from '../lib/fallbacks.js';
import { normalizeCelestialCategories } from '../lib/interpret.js';
import useStore from '../store.js';
import { tokens, styles } from '../styles/tokens.js';

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
    grid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))',
        gap: tokens.spacing.md,
    },
};

export default function Narrative() {
    const { briefing, setBriefing, celestialData, narrativeData, preferences, setNarrativeData, selectedDate } = useStore();
    const [error, setError] = useState(null);
    const [loading, setLoading] = useState(false);
    const [briefingMode, setBriefingMode] = useState('loading');
    const [briefingNote, setBriefingNote] = useState('Waiting for a live narrative response.');

    const categories = normalizeCelestialCategories(celestialData);
    const solar = extractSolarMetrics(celestialData);
    const chinese = extractChineseMetrics(celestialData);
    const fallbackDate = useMemo(() => new Date(`${selectedDate}T12:00:00Z`), [selectedDate]);
    const fallbackBriefing = useMemo(() => buildNarrativeFallback(fallbackDate, celestialData), [celestialData, fallbackDate]);

    useEffect(() => {
        let cancelled = false;

        setError(null);
        setLoading(true);
        setBriefingMode('loading');
        setBriefingNote('Waiting for a live narrative response.');
        api.getBriefing()
            .then((data) => {
                if (cancelled) return;
                const normalized = normalizeAstrogridBriefing(data, fallbackBriefing);
                setNarrativeData(normalized.raw || data);
                setBriefing(normalized.briefing);
                setBriefingMode('live');
                setBriefingNote('Live backend briefing loaded successfully.');
            })
            .catch((e) => {
                if (cancelled) return;
                setError(e.message);
                setBriefing(fallbackBriefing);
                setNarrativeData({
                    generated_at: fallbackDate.toISOString(),
                    stale: true,
                    source: 'frontend-fallback',
                });
                setBriefingMode('demo');
                setBriefingNote('The live narrative endpoint is unavailable, so this screen is showing a generated fallback briefing.');
            })
            .finally(() => {
                if (!cancelled) setLoading(false);
            });

        return () => {
            cancelled = true;
        };
    }, [fallbackBriefing, fallbackDate, setBriefing, setNarrativeData]);

    return (
        <div style={styles.container}>
            <div style={styles.header}>Celestial Narrative</div>
            <div style={styles.subheader}>Intelligence Briefing</div>

            {error && <div style={styles.error}>{error}</div>}
            {loading && <div style={styles.loading}>Generating celestial briefing...</div>}
            <div style={styles.card}>
                <div style={styles.subheader}>Briefing Source</div>
                <div style={styles.value}>
                    {briefingMode === 'loading'
                        ? 'Checking backend narrative...'
                        : briefingMode === 'live'
                            ? 'Live backend narrative'
                            : 'Generated fallback narrative'}
                </div>
                <div style={{ ...styles.label, marginTop: tokens.spacing.sm }}>
                    {briefingNote}
                </div>
            </div>

            <div style={narrStyles.briefingCard}>
                {narrativeData?.created_at || narrativeData?.generated_at || narrativeData?.briefing_date ? (
                    <div style={narrStyles.timestamp}>
                        {narrativeData.created_at || narrativeData.generated_at || narrativeData.briefing_date}
                        {narrativeData.stale ? ' | stale briefing' : ''}
                    </div>
                ) : (
                    <div style={narrStyles.timestamp}>Frontend fallback narrative for the selected date</div>
                )}
                {briefing || fallbackBriefing}
            </div>

            <div style={{ ...styles.subheader, marginTop: tokens.spacing.xl }}>Telemetry Coverage</div>
            <div style={narrStyles.grid}>
                {Object.entries(categories).map(([key, items]) => (
                    <div key={key} style={styles.metric}>
                        <div style={styles.metricValue}>{items.length}</div>
                        <div style={styles.metricLabel}>{key}</div>
                    </div>
                ))}
            </div>

            {preferences.showSolarLayer && (
                <div style={{ marginTop: tokens.spacing.lg }}>
                    <SolarActivityGauge
                        kpIndex={solar.kpIndex}
                        sunspotNumber={solar.sunspotNumber}
                        solarWindSpeed={solar.solarWindSpeed}
                        flareClass={solar.flareClass}
                    />
                </div>
            )}

            {preferences.showChineseLayer && (
                <div style={{ marginTop: tokens.spacing.lg }}>
                    <ChineseCalendar {...chinese} />
                </div>
            )}
        </div>
    );
}
