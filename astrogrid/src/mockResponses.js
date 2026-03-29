export const mockOverview = {
    sun_sign: 'Aries',
    moon_sign: 'Capricorn',
    mercury_sign: 'Pisces',
    venus_sign: 'Aquarius',
    mars_sign: 'Cancer',
    saturn_sign: 'Pisces',
    ascendant: 'Leo',
    dominant_element: 'Fire',
};

export const mockRetrogrades = {
    planets: [
        { name: 'Mercury', is_retrograde: false, sign: 'Aries', direct_date: null },
        { name: 'Venus', is_retrograde: true, sign: 'Pisces', direct_date: '2026-04-12' },
        { name: 'Mars', is_retrograde: false, sign: 'Cancer', direct_date: null },
        { name: 'Saturn', is_retrograde: false, sign: 'Pisces', direct_date: null },
    ],
};

export const mockEphemeris = {
    planets: [
        { name: 'Sun', sign: 'Aries', degree: 5.43, retrograde: false },
        { name: 'Moon', sign: 'Capricorn', degree: 18.07, retrograde: false },
        { name: 'Mercury', sign: 'Pisces', degree: 29.84, retrograde: false },
        { name: 'Venus', sign: 'Pisces', degree: 24.19, retrograde: true },
        { name: 'Mars', sign: 'Cancer', degree: 11.38, retrograde: false },
        { name: 'Jupiter', sign: 'Gemini', degree: 15.02, retrograde: false },
    ],
};

export const mockCorrelations = {
    correlations: [
        { event: 'Mercury ingress clusters', correlation: 0.41, description: 'Volatility compression often resolves within 3 sessions.' },
        { event: 'Full moon breadth washout', correlation: -0.36, description: 'Breadth weakens while defensives outperform risk baskets.' },
        { event: 'Venus retrograde sentiment drift', correlation: -0.22, description: 'Consumer-facing themes soften as revisions rise.' },
        { event: 'Mars trine momentum burst', correlation: 0.33, description: 'Short-term trend continuation improves in energy-linked assets.' },
    ],
};

export const mockTimeline = {
    events: [
        { date: '2026-03-28', type: 'conjunction', name: 'Moon conjunct Jupiter', description: 'Expansion bias with stronger macro headline sensitivity.' },
        { date: '2026-04-01', type: 'ingress', name: 'Mercury enters Aries', description: 'Sharper tone in communication and faster reaction windows.' },
        { date: '2026-04-12', type: 'retrograde', name: 'Venus stations direct', description: 'Relationship and valuation themes begin to normalize.' },
    ],
};

export const mockEclipses = {
    eclipses: [
        { date: '2026-09-12', type: 'Solar', name: 'Virgo Solar Eclipse', description: 'System reset energy around process and discipline.' },
        { date: '2026-09-28', type: 'Lunar', name: 'Aries Lunar Eclipse', description: 'Reactionary inflection point with elevated emotional beta.' },
    ],
};

export const mockBriefing = {
    briefing: `Today favors disciplined positioning over impulsive conviction.\n\nThe Aries Sun accelerates decision-making, but the Capricorn Moon keeps consequences visible. Traders may see cleaner follow-through when waiting for strength confirmation rather than front-running inflections. Venus retrograde continues to distort preference signals, so treat narrative enthusiasm as suspect until price joins the move.`,
};

export const mockNakshatra = {
    name: 'Shravana',
    deity: 'Vishnu',
    ruler: 'Moon',
};

export const mockLunar = {
    phase: 'Waning Gibbous',
    illumination: 0.72,
    sign: 'Capricorn',
    upcoming: [
        { phase: 'Last Quarter', date: '2026-03-31' },
        { phase: 'New Moon', date: '2026-04-07' },
        { phase: 'First Quarter', date: '2026-04-15' },
    ],
};

export const mockSolar = {
    sunspot_count: 148,
    flare_risk: 0.34,
    proton_flux: 1.7,
    kp_index: 3.1,
    solar_wind_speed: 441,
    geomagnetic_bias: 'Stable',
};

export const mockCompare = {
    summary: 'Date comparison mode is running against bundled demo data.',
};
