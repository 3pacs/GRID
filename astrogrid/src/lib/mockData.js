import { computeAspects, computeLunarPhase, computeNakshatra, getFullEphemeris } from './ephemeris.js';
import { normalizeCelestialCategories } from './interpret.js';

const MARKET_FEATURES = [
    { key: 'spy', label: 'SPY' },
    { key: 'qqq', label: 'QQQ' },
    { key: 'btc', label: 'BTC' },
    { key: 'vix', label: 'VIX' },
    { key: 'dxy', label: 'DXY' },
    { key: 'gold', label: 'Gold' },
    { key: 'ust10y', label: 'US 10Y' },
    { key: 'crude', label: 'Crude' },
];

function hashString(input) {
    let hash = 0;
    for (let i = 0; i < input.length; i += 1) {
        hash = (hash << 5) - hash + input.charCodeAt(i);
        hash |= 0;
    }
    return hash;
}

function scorePair(a, b, bias = 0) {
    const raw = hashString(`${a}:${b}:${bias}`);
    return ((raw % 170) / 100) - 0.85;
}

export function buildCorrelationMatrix(celestialData) {
    const categories = normalizeCelestialCategories(celestialData);
    const rows = Object.values(categories)
        .flat()
        .slice(0, 10)
        .map((item, index) => ({
            key: item.name || `feature-${index}`,
            label: item.description || item.name || `Celestial ${index + 1}`,
            category: item.category || 'celestial',
        }));

    if (!rows.length) {
        rows.push(
            { key: 'lunar_phase', label: 'Lunar Phase', category: 'lunar' },
            { key: 'planetary_stress', label: 'Planetary Stress', category: 'planetary' },
            { key: 'solar_flux', label: 'Solar Flux', category: 'solar' },
            { key: 'nakshatra_index', label: 'Nakshatra', category: 'vedic' },
            { key: 'chinese_cycle', label: 'Chinese Cycle', category: 'chinese' }
        );
    }

    const cells = [];
    for (const row of rows) {
        for (const column of MARKET_FEATURES) {
            cells.push({
                rowKey: row.key,
                columnKey: column.key,
                value: Number(scorePair(row.key, column.key, row.category).toFixed(2)),
            });
        }
    }

    return { rows, columns: MARKET_FEATURES, cells };
}

export function buildTimelineFallback(baseDate = new Date()) {
    const lunar = computeLunarPhase(baseDate);
    const nakshatra = computeNakshatra(baseDate);
    const aspects = computeAspects(baseDate).slice(0, 5);

    const daysToNew = Math.max(0, Math.round(lunar.days_to_new));
    const daysToFull = Math.max(0, Math.round(lunar.days_to_full));

    const nextNew = new Date(baseDate);
    nextNew.setDate(baseDate.getDate() + daysToNew);

    const nextFull = new Date(baseDate);
    nextFull.setDate(baseDate.getDate() + daysToFull);

    const events = [
        {
            type: 'lunar',
            date: nextNew.toISOString().slice(0, 10),
            name: 'New Moon Window',
            description: 'Fresh-cycle reset for sentiment and reflexivity monitoring.',
        },
        {
            type: 'lunar',
            date: nextFull.toISOString().slice(0, 10),
            name: 'Full Moon Window',
            description: 'Higher-volatility moon phase watch with stronger emotional amplitude.',
        },
        {
            type: 'vedic',
            date: baseDate.toISOString().slice(0, 10),
            name: `${nakshatra.nakshatra_name} Active`,
            description: `Moon is moving through ${nakshatra.nakshatra_name}, ruled by ${nakshatra.ruling_planet}.`,
        },
        ...aspects.map((aspect, index) => {
            const eventDate = new Date(baseDate);
            eventDate.setDate(baseDate.getDate() + index + 1);
            return {
                type: aspect.aspect_type,
                date: eventDate.toISOString().slice(0, 10),
                name: `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`,
                description: `${aspect.nature} geometry with ${aspect.orb_used.toFixed(1)}° orb.`,
            };
        }),
    ];

    return events.sort((a, b) => a.date.localeCompare(b.date));
}

export function buildNarrativeFallback(baseDate, celestialData) {
    const ephemeris = getFullEphemeris(baseDate);
    const categories = normalizeCelestialCategories(celestialData);
    const activeCategories = Object.entries(categories)
        .filter(([, items]) => items.length > 0)
        .map(([key]) => key);

    return [
        `AstroGrid Briefing | ${ephemeris.date}`,
        '',
        `Moon phase: ${ephemeris.lunar_phase.phase_name} (${ephemeris.lunar_phase.illumination.toFixed(1)}% illuminated).`,
        `Retrogrades: ${ephemeris.retrograde_planets.length ? ephemeris.retrograde_planets.join(', ') : 'none active'}.`,
        `Aspect climate: ${ephemeris.aspects.length} major aspects tracked today.`,
        `Nakshatra: ${ephemeris.nakshatra.nakshatra_name}, ruled by ${ephemeris.nakshatra.ruling_planet}.`,
        '',
        `Live telemetry categories available: ${activeCategories.length ? activeCategories.join(', ') : 'none yet'}.`,
        'Use this narrative as a fallback until the dedicated narrative endpoint is standardized.',
    ].join('\n');
}

export function extractSolarMetrics(celestialData) {
    const categories = normalizeCelestialCategories(celestialData);
    const solar = categories.solar || [];

    const pick = (patterns) => solar.find((item) =>
        patterns.some((pattern) => (item.name || '').toLowerCase().includes(pattern))
    );

    return {
        kpIndex: pick(['kp'])?.value ?? null,
        sunspotNumber: pick(['sunspot'])?.value ?? null,
        solarWindSpeed: pick(['wind'])?.value ?? null,
        flareClass: pick(['flare'])?.value ?? null,
    };
}

export function extractChineseMetrics(celestialData) {
    const categories = normalizeCelestialCategories(celestialData);
    const chinese = categories.chinese || [];

    const pick = (patterns) => chinese.find((item) =>
        patterns.some((pattern) => (item.name || '').toLowerCase().includes(pattern))
    );

    return {
        animal: pick(['animal'])?.value || 'Unknown',
        element: pick(['element'])?.value || 'Unknown',
        yinYang: pick(['yin', 'yang'])?.value || 'Yang',
        flyingStar: pick(['flying'])?.value ?? null,
        lunarMonth: pick(['month'])?.value ?? null,
        hexagram: pick(['hexagram'])?.value ?? null,
    };
}

export function buildEclipseFallback(baseDate = new Date()) {
    const lunarDate = new Date(baseDate);
    lunarDate.setDate(baseDate.getDate() + 17);

    const solarDate = new Date(baseDate);
    solarDate.setDate(baseDate.getDate() + 42);

    return {
        next_lunar: {
            type: 'Lunar Eclipse',
            date: lunarDate.toISOString().slice(0, 10),
            name: 'Mock Lunar Eclipse Window',
            description: 'Fallback eclipse countdown until standardized AstroGrid eclipse payloads are guaranteed.',
        },
        next_solar: {
            type: 'Solar Eclipse',
            date: solarDate.toISOString().slice(0, 10),
            name: 'Mock Solar Eclipse Window',
            description: 'Fallback eclipse countdown until standardized AstroGrid eclipse payloads are guaranteed.',
        },
    };
}
