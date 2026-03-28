export const ASTROGRID_ENDPOINTS = {
    overview: '/api/v1/astrogrid/overview',
    snapshot: '/api/v1/astrogrid/snapshot',
    regimeCurrent: '/api/v1/regime/current',
    intelligenceThesis: '/api/v1/intelligence/thesis',
    moneyMap: '/api/v1/flows/money-map',
    flowsAggregated: '/api/v1/flows/aggregated',
    signalsSnapshot: '/api/v1/signals/snapshot',
    activePatterns: '/api/v1/intelligence/patterns/active',
    crossReference: '/api/v1/intelligence/cross-reference',
    signals: '/api/v1/signals/celestial',
    signalsBriefing: '/api/v1/signals/celestial/briefing',
    ephemeris: '/api/v1/astrogrid/ephemeris',
    correlations: '/api/v1/astrogrid/correlations',
    timeline: '/api/v1/astrogrid/timeline',
    briefing: '/api/v1/astrogrid/briefing',
    narrative: '/api/v1/astrogrid/narrative',
    retrograde: '/api/v1/astrogrid/retrograde',
    retrogrades: '/api/v1/astrogrid/retrogrades',
    eclipses: '/api/v1/astrogrid/eclipses',
    nakshatra: '/api/v1/astrogrid/nakshatra',
    lunar: '/api/v1/astrogrid/lunar',
    lunarCalendar: '/api/v1/astrogrid/lunar/calendar',
    solar: '/api/v1/astrogrid/solar',
    solarActivity: '/api/v1/astrogrid/solar/activity',
    compare: '/api/v1/astrogrid/compare',
};

function isEmpty(value) {
    return value == null || value === '';
}

export function buildAstrogridQueryString(params = {}) {
    const query = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
        if (isEmpty(value)) continue;
        query.set(key, String(value));
    }
    return query.toString();
}

export function appendAstrogridQuery(path, params = {}) {
    const qs = typeof params === 'string' ? params : buildAstrogridQueryString(params);
    return qs ? `${path}?${qs}` : path;
}

export function buildAstrogridSnapshotPath(date) {
    return appendAstrogridQuery(ASTROGRID_ENDPOINTS.snapshot, { date });
}

export function buildAstrogridEphemerisPath(date) {
    return appendAstrogridQuery(ASTROGRID_ENDPOINTS.ephemeris, { date });
}

export function buildAstrogridAggregatedFlowsPath(params = {}) {
    return appendAstrogridQuery(ASTROGRID_ENDPOINTS.flowsAggregated, params);
}

export function buildAstrogridCorrelationsCandidates(params = {}) {
    const contractPath = appendAstrogridQuery(ASTROGRID_ENDPOINTS.correlations, params);
    const altPath = appendAstrogridQuery(ASTROGRID_ENDPOINTS.correlations, {
        market_feature: params.market || params.market_feature || 'spy',
        celestial_category: params.feature || params.celestial_category || 'lunar',
        lookback_days: params.lookback_days || 252,
    });

    return [
        { path: contractPath },
        { path: altPath },
    ];
}

export function buildAstrogridTimelineCandidates(params = {}) {
    return [
        { path: appendAstrogridQuery(ASTROGRID_ENDPOINTS.timeline, params) },
    ];
}

export function buildAstrogridBriefingCandidates() {
    return [
        { path: ASTROGRID_ENDPOINTS.signalsBriefing },
        { path: ASTROGRID_ENDPOINTS.narrative },
        { path: ASTROGRID_ENDPOINTS.briefing },
    ];
}

export function buildAstrogridRetrogradeCandidates() {
    return [
        { path: ASTROGRID_ENDPOINTS.retrograde },
        { path: ASTROGRID_ENDPOINTS.retrogrades },
    ];
}

export function buildAstrogridLunarCalendarCandidates(year, month) {
    const params = { year, month };
    return [
        { path: appendAstrogridQuery(ASTROGRID_ENDPOINTS.lunarCalendar, params) },
        { path: appendAstrogridQuery(ASTROGRID_ENDPOINTS.lunar, params) },
    ];
}

export function buildAstrogridSolarActivityCandidates() {
    return [
        { path: ASTROGRID_ENDPOINTS.solarActivity },
        { path: ASTROGRID_ENDPOINTS.solar },
    ];
}

export function buildAstrogridCompareCandidates(date1, date2) {
    return [
        {
            path: ASTROGRID_ENDPOINTS.compare,
            options: {
                method: 'POST',
                body: JSON.stringify({ date1, date2 }),
            },
        },
        {
            path: appendAstrogridQuery(ASTROGRID_ENDPOINTS.compare, { date1, date2 }),
        },
    ];
}

export async function fetchFirstAstrogridCandidate(request, candidates, options = {}) {
    const requestOptions = options.requestOptions || {};
    let lastError = null;

    for (const candidate of candidates) {
        const descriptor = typeof candidate === 'string' ? { path: candidate } : candidate;
        try {
            return await request(descriptor.path, {
                ...requestOptions,
                ...(descriptor.options || {}),
                headers: {
                    ...(requestOptions.headers || {}),
                    ...((descriptor.options && descriptor.options.headers) || {}),
                },
            });
        } catch (error) {
            lastError = error;
            if (error?.status && ![404, 405].includes(error.status)) {
                break;
            }
        }
    }

    throw lastError || options.fallbackError || new Error('No AstroGrid endpoint candidates succeeded');
}
