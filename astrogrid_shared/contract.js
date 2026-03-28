const REMOTE_ASTROGRID_API_BASE = 'https://grid.stepdad.finance';

function isObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function toArray(value) {
    if (Array.isArray(value)) return value;
    if (value == null) return [];
    return [value];
}

function asNumber(value, fallback = 0) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }
    return fallback;
}

export function getAstrogridDefaultApiBaseUrl(locationLike) {
    const hostname = locationLike?.hostname || '';
    const origin = locationLike?.origin || '';

    if (hostname === 'localhost' || hostname === '127.0.0.1') {
        return REMOTE_ASTROGRID_API_BASE;
    }

    return origin || REMOTE_ASTROGRID_API_BASE;
}

export function normalizeAstrogridSignalMap(payload) {
    if (!isObject(payload)) return {};
    if (!isObject(payload.categories)) return payload;

    const flattened = {};
    for (const [category, values] of Object.entries(payload.categories)) {
        if (!isObject(values)) continue;
        for (const [key, value] of Object.entries(values)) {
            flattened[`${category}_${key}`] = typeof value === 'boolean' ? Number(value) : value;
        }
    }
    return flattened;
}

export function normalizeAstrogridTimeline(payload) {
    if (Array.isArray(payload)) return payload;
    return toArray(payload?.events || payload?.items || []);
}

export function normalizeAstrogridCorrelations(payload) {
    if (Array.isArray(payload)) return payload;
    return toArray(payload?.correlations);
}

export function buildAstrogridCorrelationMatrix(payload) {
    const rows = normalizeAstrogridCorrelations(payload);
    if (!rows.length) return null;

    const rowLabels = [...new Set(rows.map((item) => item.celestial_feature || item.feature || item.name).filter(Boolean))];
    const columnLabels = [...new Set(rows.map((item) => item.market_feature || item.market || item.ticker).filter(Boolean))];
    if (!rowLabels.length || !columnLabels.length) return null;

    return {
        rows: rowLabels,
        columns: columnLabels,
        matrix: rowLabels.map((row) => columnLabels.map((column) => {
            const match = rows.find((item) =>
                (item.celestial_feature || item.feature || item.name) === row
                && (item.market_feature || item.market || item.ticker) === column
            );
            return asNumber(match?.correlation ?? match?.value, 0);
        })),
    };
}

export function normalizeAstrogridBriefing(payload, fallback = '') {
    return {
        briefing: payload?.briefing || payload?.text || payload?.content || fallback,
        generatedAt: payload?.generated_at || payload?.created_at || payload?.briefing_date || null,
        source: payload?.source || null,
        raw: payload || null,
    };
}

export function normalizeAstrogridEphemeris(payload) {
    if (!isObject(payload)) return null;

    const root = payload.ephemeris || payload;
    const rawPlanets = Array.isArray(root.planets)
        ? root.planets
        : Array.isArray(root.positions)
            ? root.positions
            : Array.isArray(root.bodies)
                ? root.bodies
                : isObject(root.positions)
                    ? Object.values(root.positions)
                    : [];
    const rawAspects = Array.isArray(root.aspects) ? root.aspects : [];

    const planets = rawPlanets
        .map((planet, index) => ({
            planet: planet.planet || planet.name || planet.body || `Body ${index + 1}`,
            geocentric_longitude: asNumber(
                planet.geocentric_longitude
                ?? planet.longitude
                ?? planet.lon
                ?? planet.position,
                NaN,
            ),
            right_ascension: asNumber(planet.right_ascension ?? planet.ra ?? planet.ascension, 0),
            zodiac_sign: planet.zodiac_sign || planet.sign || 'Unknown',
            zodiac_degree: asNumber(planet.zodiac_degree ?? planet.degree ?? planet.sign_degree, 0),
            is_retrograde: Boolean(
                planet.is_retrograde
                ?? planet.retrograde
                ?? planet.rx,
            ),
        }))
        .filter((planet) => Number.isFinite(planet.geocentric_longitude));

    const aspects = rawAspects
        .map((aspect, index) => ({
            planet1: aspect.planet1 || aspect.from || aspect.body1 || `Body ${index + 1}`,
            planet2: aspect.planet2 || aspect.to || aspect.body2 || 'Body',
            aspect_type: aspect.aspect_type || aspect.type || 'aspect',
            nature: aspect.nature || aspect.tone || 'variable',
            applying: Boolean(aspect.applying ?? false),
            orb_used: asNumber(aspect.orb_used ?? aspect.orb ?? aspect.distance, 0),
        }))
        .filter((aspect) => aspect.planet1 && aspect.planet2);

    if (!planets.length && !aspects.length) {
        return null;
    }

    return {
        date: root.date || payload.date || null,
        planets,
        aspects,
    };
}
