const STORAGE_PREFIX = 'astrogrid_web';
const MAX_LOG_ENTRIES = 80;

const FAMILY_TO_ENGINES = {
    'greco-occult': ['western', 'hellenistic', 'hermetic'],
    indicative: ['western', 'hellenistic'],
    indic: ['vedic', 'tantric'],
    abrahamic: ['kabbalistic', 'arabic'],
    east_asian: ['iching', 'taoist'],
    ancient_court: ['babylonian', 'egyptian', 'maya'],
};

export const ENGINE_DEFINITIONS = [
    {
        id: 'western',
        name: 'Western',
        family: 'greco-occult',
        focus: 'aspect grammar, solar balance, transit pressure',
        baseConfidence: 0.68,
    },
    {
        id: 'hellenistic',
        name: 'Hellenistic',
        family: 'greco-occult',
        focus: 'sect, timing, fated turns, sharp edges',
        baseConfidence: 0.7,
    },
    {
        id: 'vedic',
        name: 'Vedic',
        family: 'indic',
        focus: 'moon, nakshatra, nodes, tide',
        baseConfidence: 0.73,
    },
    {
        id: 'hermetic',
        name: 'Hermetic',
        family: 'greco-occult',
        focus: 'correspondence, threshold, linked signs',
        baseConfidence: 0.66,
    },
    {
        id: 'iching',
        name: 'I Ching',
        family: 'east_asian',
        focus: 'change, polarity, line tension, flow',
        baseConfidence: 0.64,
    },
    {
        id: 'kabbalistic',
        name: 'Kabbalistic',
        family: 'abrahamic',
        focus: 'structure, ascent, tension, channel',
        baseConfidence: 0.63,
    },
    {
        id: 'babylonian',
        name: 'Babylonian',
        family: 'ancient_court',
        focus: 'omen, eclipse, watch, warning',
        baseConfidence: 0.67,
    },
    {
        id: 'maya',
        name: 'Maya',
        family: 'ancient_court',
        focus: 'count, cycle, threshold, repeat signal',
        baseConfidence: 0.65,
    },
    {
        id: 'arabic',
        name: 'Arabic',
        family: 'abrahamic',
        focus: 'star road, dignity, lunar motion, omen',
        baseConfidence: 0.66,
    },
    {
        id: 'egyptian',
        name: 'Egyptian',
        family: 'ancient_court',
        focus: 'gate, rise, solar threshold, watchfulness',
        baseConfidence: 0.64,
    },
    {
        id: 'taoist',
        name: 'Taoist',
        family: 'east_asian',
        focus: 'flow, balance, season, yielding edge',
        baseConfidence: 0.67,
    },
    {
        id: 'tantric',
        name: 'Tantric',
        family: 'indic',
        focus: 'force, current, seal, inner pressure',
        baseConfidence: 0.66,
    },
];

const ENGINE_MAP = Object.fromEntries(ENGINE_DEFINITIONS.map((def) => [def.id, def]));

const PERSONA_MAP = {
    seer: {
        id: 'seer',
        name: 'Seer',
        tradition: 'merged',
        lens_mode: 'chorus',
        allowed_lenses: ENGINE_DEFINITIONS.map((def) => def.id),
        forbidden_lenses: [],
        tone: 'cryptic',
        verbosity: 'brief',
    },
    qwen: {
        id: 'qwen',
        name: 'Qwen Mask',
        tradition: 'merged',
        lens_mode: 'chorus',
        allowed_lenses: ENGINE_DEFINITIONS.map((def) => def.id),
        forbidden_lenses: [],
        tone: 'cool',
        verbosity: 'brief',
    },
    western: {
        id: 'western',
        name: 'Western Reader',
        tradition: 'western',
        lens_mode: 'solo',
        allowed_lenses: ['western', 'hellenistic', 'hermetic'],
        forbidden_lenses: ['vedic', 'tantric'],
        tone: 'measured',
        verbosity: 'brief',
    },
    vedic: {
        id: 'vedic',
        name: 'Vedic Reader',
        tradition: 'vedic',
        lens_mode: 'solo',
        allowed_lenses: ['vedic', 'tantric'],
        forbidden_lenses: ['western', 'hellenistic'],
        tone: 'incantatory',
        verbosity: 'brief',
    },
    hermetic: {
        id: 'hermetic',
        name: 'Hermetic Witness',
        tradition: 'hermetic',
        lens_mode: 'shadow',
        allowed_lenses: ['hermetic', 'western', 'hellenistic'],
        forbidden_lenses: [],
        tone: 'cryptic',
        verbosity: 'brief',
    },
    taoist: {
        id: 'taoist',
        name: 'Taoist Observer',
        tradition: 'taoist',
        lens_mode: 'shadow',
        allowed_lenses: ['taoist', 'iching'],
        forbidden_lenses: [],
        tone: 'quiet',
        verbosity: 'brief',
    },
    babylonian: {
        id: 'babylonian',
        name: 'Babylonian Keeper',
        tradition: 'babylonian',
        lens_mode: 'solo',
        allowed_lenses: ['babylonian', 'maya', 'egyptian'],
        forbidden_lenses: [],
        tone: 'grave',
        verbosity: 'brief',
    },
};

const SIGN_TO_ELEMENT = {
    Aries: 'fire',
    Leo: 'fire',
    Sagittarius: 'fire',
    Taurus: 'earth',
    Virgo: 'earth',
    Capricorn: 'earth',
    Gemini: 'air',
    Libra: 'air',
    Aquarius: 'air',
    Cancer: 'water',
    Scorpio: 'water',
    Pisces: 'water',
};

const ASPECT_WEIGHTS = {
    conjunction: 1.0,
    opposition: 1.0,
    square: 0.92,
    trine: 0.78,
    sextile: 0.66,
    quincunx: 0.48,
};

const POSITIVE_PHRASES = [
    'the gate opens',
    'the line leans up',
    'the field clears',
    'signal gathers',
    'motion favors the long arc',
];

const NEGATIVE_PHRASES = [
    'the floor thins',
    'pressure gathers',
    'the wall tightens',
    'signal compresses',
    'the safer move is smaller',
];

const NEUTRAL_PHRASES = [
    'the room is split',
    'the sky withholds',
    'no clean edge',
    'hold the line',
    'wait for the next cut',
];

const OMEN_PHRASES = {
    western: ['hard geometry', 'clean friction', 'sector pressure'],
    hellenistic: ['fated edge', 'sharp turn', 'sect tension'],
    vedic: ['moon thread', 'node shadow', 'nakshatra pulse'],
    hermetic: ['linked sign', 'mirror chamber', 'threshold hum'],
    iching: ['line turns', 'change cuts', 'yielding current'],
    kabbalistic: ['channel strain', 'scale tilt', 'seal breaks'],
    babylonian: ['watch sign', 'sky omen', 'eclipse mark'],
    maya: ['count turns', 'cycle knot', 'calendar seam'],
    arabic: ['star road', 'night cut', 'lunar path'],
    egyptian: ['gate watch', 'solar rise', 'threshold watch'],
    taoist: ['flow bends', 'grain of change', 'quiet current'],
    tantric: ['seal pressure', 'inner fire', 'current knot'],
};

function isObject(value) {
    return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function clamp(value, min = 0, max = 1) {
    return Math.min(max, Math.max(min, value));
}

function round(value, digits = 3) {
    const factor = 10 ** digits;
    return Math.round(value * factor) / factor;
}

function asNumber(value, fallback = 0) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }
    return fallback;
}

function asString(value, fallback = '') {
    if (typeof value === 'string' && value.trim() !== '') return value;
    return fallback;
}

function toArray(value) {
    if (Array.isArray(value)) return value;
    if (value == null) return [];
    return [value];
}

function normalizeId(value) {
    return String(value || '')
        .trim()
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/-+/g, '-')
        .replace(/^-|-$/g, '');
}

function hashString(input) {
    const str = String(input ?? '');
    let hash = 0;
    for (let i = 0; i < str.length; i += 1) {
        hash = ((hash << 5) - hash + str.charCodeAt(i)) | 0;
    }
    return Math.abs(hash);
}

function pick(list, seed) {
    if (!list.length) return '';
    return list[seed % list.length];
}

function getStorage() {
    if (typeof window === 'undefined' || !window.localStorage) return null;
    return window.localStorage;
}

function runLogKey(kind) {
    return `${STORAGE_PREFIX}:${kind}:runs`;
}

function readJson(storage, key, fallback) {
    if (!storage) return fallback;
    try {
        const raw = storage.getItem(key);
        return raw ? JSON.parse(raw) : fallback;
    } catch {
        return fallback;
    }
}

function writeJson(storage, key, value) {
    if (!storage) return;
    try {
        storage.setItem(key, JSON.stringify(value));
    } catch {
        // ignore storage pressure
    }
}

export function readRunLog(kind) {
    const storage = getStorage();
    return readJson(storage, runLogKey(kind), []);
}

function appendRunLog(kind, payload) {
    const storage = getStorage();
    if (!storage) {
        return `mem_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`;
    }

    const entry = {
        id: `${kind}_${Date.now()}_${Math.random().toString(16).slice(2, 8)}`,
        ts: new Date().toISOString(),
        kind,
        payload,
    };

    const current = readJson(storage, runLogKey(kind), []);
    current.push(entry);
    writeJson(storage, runLogKey(kind), current.slice(-MAX_LOG_ENTRIES));
    return entry.id;
}

export function logEngineRun(payload) {
    return appendRunLog('engine', payload);
}

export function logSeerRun(payload) {
    return appendRunLog('seer', payload);
}

export function logPersonaRun(payload) {
    return appendRunLog('persona', payload);
}

function normalizeBodies(snapshot) {
    const raw = snapshot?.bodies ?? snapshot?.positions ?? snapshot?.objects ?? {};
    if (Array.isArray(raw)) {
        return raw
            .map((body, index) => ({
                id: normalizeId(body.id || body.name || `body-${index}`),
                name: asString(body.name, `Body ${index + 1}`),
                sign: asString(body.sign || body.zodiac_sign),
                longitude: asNumber(body.longitude ?? body.geocentric_longitude ?? body.ecliptic_longitude),
                latitude: asNumber(body.latitude ?? body.ecliptic_latitude),
                rightAscension: asNumber(body.right_ascension ?? body.ra),
                declination: asNumber(body.declination ?? body.dec),
                distance: asNumber(body.distance ?? body.distance_au),
                speed: asNumber(body.speed ?? body.motion ?? body.daily_motion),
                retrograde: Boolean(body.retrograde ?? body.is_retrograde),
                degree: asNumber(body.degree ?? body.zodiac_degree ?? body.degree_in_sign),
                precision: asString(body.precision, 'medium'),
            }))
            .filter((body) => body.id);
    }

    if (isObject(raw)) {
        return Object.entries(raw).map(([key, body]) => ({
            id: normalizeId(body?.id || key),
            name: asString(body?.name, key),
            sign: asString(body?.sign || body?.zodiac_sign),
            longitude: asNumber(body?.longitude ?? body?.geocentric_longitude ?? body?.ecliptic_longitude),
            latitude: asNumber(body?.latitude ?? body?.ecliptic_latitude),
            rightAscension: asNumber(body?.right_ascension ?? body?.ra),
            declination: asNumber(body?.declination ?? body?.dec),
            distance: asNumber(body?.distance ?? body?.distance_au),
            speed: asNumber(body?.speed ?? body?.motion ?? body?.daily_motion),
            retrograde: Boolean(body?.retrograde ?? body?.is_retrograde),
            degree: asNumber(body?.degree ?? body?.zodiac_degree ?? body?.degree_in_sign),
            precision: asString(body?.precision, 'medium'),
        }));
    }

    return [];
}

function normalizeAspects(snapshot) {
    const raw = snapshot?.aspects ?? snapshot?.relationships ?? [];
    return toArray(raw)
        .map((aspect, index) => {
            const type = normalizeId(aspect?.aspect_type || aspect?.type || aspect?.name);
            const aspectType = type || 'conjunction';
            const orb = asNumber(aspect?.orb_used ?? aspect?.orb ?? aspect?.distance);
            return {
                id: normalizeId(aspect?.id || `${aspect?.planet1 || 'a'}-${aspect?.planet2 || 'b'}-${index}`),
                planet1: asString(aspect?.planet1 || aspect?.from),
                planet2: asString(aspect?.planet2 || aspect?.to),
                aspect_type: aspectType,
                exact_angle: asNumber(aspect?.exact_angle ?? aspect?.angle),
                angle_between: asNumber(aspect?.angle_between ?? aspect?.separation),
                orb_used: orb,
                applying: Boolean(aspect?.applying),
                nature: asString(aspect?.nature),
                strength: clamp(1 - orb / 12, 0, 1),
            };
        })
        .filter((aspect) => aspect.id);
}

function normalizeLunar(snapshot) {
    const raw = snapshot?.lunar ?? snapshot?.lunar_phase ?? {};
    return {
        phase: asNumber(raw.phase ?? raw.phase_fraction ?? raw.phaseAngle ?? raw.phase_angle, 0.5),
        phaseName: asString(raw.phase_name || raw.phase || raw.name || raw.moon_phase, 'Unknown'),
        illumination: asNumber(raw.illumination ?? raw.percent ?? raw.illumination_pct, 50),
        daysToNew: asNumber(raw.days_to_new ?? raw.days_to_new_moon, 0),
        daysToFull: asNumber(raw.days_to_full ?? raw.days_to_full_moon, 0),
        sign: asString(raw.sign),
    };
}

function normalizeNakshatra(snapshot) {
    const raw = snapshot?.nakshatra ?? {};
    return {
        name: asString(raw.nakshatra_name || raw.name, 'Unknown'),
        quality: asString(raw.quality || raw.nakshatra_quality_name, 'Dual'),
        ruler: asString(raw.ruling_planet || raw.ruler),
        deity: asString(raw.deity),
        index: asNumber(raw.nakshatra_index, 0),
        pada: asNumber(raw.pada, 1),
    };
}

function normalizeSignals(rawSignals) {
    if (Array.isArray(rawSignals)) {
        return rawSignals.map((item, index) => ({
            key: normalizeId(item?.key || item?.name || `signal-${index}`),
            name: asString(item?.name || item?.key, `Signal ${index + 1}`),
            value: asNumber(item?.value ?? item?.score ?? item?.strength),
            label: asString(item?.label),
            direction: asString(item?.direction),
        }));
    }

    if (isObject(rawSignals)) {
        return Object.entries(rawSignals).map(([key, value]) => ({
            key: normalizeId(key),
            name: key,
            value: asNumber(value, 0),
            label: asString(value?.label),
            direction: asString(value?.direction),
        }));
    }

    return [];
}

function findBody(bodies, name) {
    const needle = normalizeId(name);
    return bodies.find((body) => body.id === needle || normalizeId(body.name) === needle);
}

function summarizeSignals(signals) {
    const numeric = signals.map((signal) => signal.value).filter((value) => Number.isFinite(value));
    const avg = numeric.length ? numeric.reduce((a, b) => a + b, 0) / numeric.length : 0;
    const positive = signals.filter((signal) => signal.value > 0);
    const negative = signals.filter((signal) => signal.value < 0);
    const regime = signals.find((signal) => /regime|mode|state/i.test(signal.key || signal.name));
    const volatility = signals.find((signal) => /vol|risk|stress|fear/i.test(signal.key || signal.name));
    const trend = signals.find((signal) => /trend|momentum|breadth|flow|pressure/i.test(signal.key || signal.name));

    const bias = avg + (positive.length - negative.length) * 0.08;
    return {
        bias: clamp(bias, -1, 1),
        regime: asString(regime?.label || regime?.name || regime?.direction),
        volatility: asNumber(volatility?.value, 0),
        trend: asNumber(trend?.value, 0),
        keys: signals.map((signal) => signal.key || signal.name).filter(Boolean),
    };
}

function aspectSummary(aspects) {
    const counts = {
        conjunction: 0,
        opposition: 0,
        square: 0,
        trine: 0,
        sextile: 0,
        quincunx: 0,
    };

    let hard = 0;
    let soft = 0;
    let totalStrength = 0;

    for (const aspect of aspects) {
        const type = normalizeId(aspect.aspect_type);
        if (Object.prototype.hasOwnProperty.call(counts, type)) {
            counts[type] += 1;
        }
        const weight = ASPECT_WEIGHTS[type] ?? 0.5;
        totalStrength += weight * clamp(aspect.strength ?? (1 - asNumber(aspect.orb_used, 0) / 12), 0, 1);
        if (type === 'conjunction' || type === 'opposition' || type === 'square') hard += 1;
        if (type === 'trine' || type === 'sextile') soft += 1;
    }

    const tension = hard * 1.15 + counts.opposition * 0.35 + counts.square * 0.2;
    const flow = soft * 0.95 + counts.trine * 0.15 + counts.sextile * 0.12;

    return {
        counts,
        hard,
        soft,
        tension,
        flow,
        clarity: clamp((flow + 1) / (tension + flow + 2), 0, 1),
        intensity: clamp(totalStrength / Math.max(aspects.length, 1), 0, 1),
    };
}

function bodySummary(bodies) {
    const retrograde = bodies.filter((body) => body.retrograde);
    const core = ['sun', 'moon', 'mercury', 'venus', 'mars', 'jupiter', 'saturn'];
    const coreBodies = core.map((name) => findBody(bodies, name)).filter(Boolean);
    const signCounts = new Map();
    const elementCounts = new Map();

    for (const body of bodies) {
        if (body.sign) {
            signCounts.set(body.sign, (signCounts.get(body.sign) || 0) + 1);
            const element = SIGN_TO_ELEMENT[body.sign];
            if (element) {
                elementCounts.set(element, (elementCounts.get(element) || 0) + 1);
            }
        }
    }

    const dominantSign = [...signCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || 'Unknown';
    const dominantElement = [...elementCounts.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || 'unknown';
    const retrogradeCore = coreBodies.filter((body) => body.retrograde).length;

    return {
        retrogradeCount: retrograde.length,
        retrogradeCore,
        dominantSign,
        dominantElement,
        signCounts: Object.fromEntries(signCounts),
        elementCounts: Object.fromEntries(elementCounts),
    };
}

function lunarSummary(lunar) {
    const phase = clamp(asNumber(lunar.phase, 0.5), 0, 1);
    const illumination = clamp(asNumber(lunar.illumination, 50) / 100, 0, 1);
    const waxing = /wax/i.test(lunar.phaseName);
    const waning = /wan/i.test(lunar.phaseName);
    const lead = waxing ? 1 : waning ? -1 : 0;
    const cycleEdge = 1 - Math.abs(phase - 0.5) * 2;

    return {
        phase,
        illumination,
        waxing,
        waning,
        lead,
        cycleEdge: clamp(cycleEdge, 0, 1),
        daysToNew: lunar.daysToNew,
        daysToFull: lunar.daysToFull,
    };
}

function skySnapshot(snapshot) {
    const bodies = normalizeBodies(snapshot);
    const aspects = normalizeAspects(snapshot);
    const lunar = lunarSummary(normalizeLunar(snapshot));
    const nakshatra = normalizeNakshatra(snapshot);
    const signals = summarizeSignals(normalizeSignals(snapshot?.signals ?? snapshot?.gridSignals ?? snapshot?.marketSignals ?? snapshot?.signals_state));
    const bodyStats = bodySummary(bodies);
    const aspectStats = aspectSummary(aspects);
    const eclipseFlag = Boolean(snapshot?.eclipses || snapshot?.eclipse || /eclipse/i.test(nakshatra.name));

    const balance = clamp((aspectStats.flow - aspectStats.tension * 0.72 + lunar.cycleEdge * 0.7 + signals.bias * 0.9) / 3, -1, 1);
    const pressure = clamp((aspectStats.tension + bodyStats.retrogradeCount * 0.8 + (eclipseFlag ? 1.25 : 0) + Math.max(0, -signals.bias) * 0.8) / 6, 0, 1);
    const flow = clamp((aspectStats.flow + lunar.illumination * 0.4 + Math.max(0, signals.bias) * 0.8) / 4, 0, 1);
    const clarity = clamp((aspectStats.clarity + (1 - pressure) + Math.abs(balance)) / 3, 0, 1);
    const coherence = clamp((flow + clarity - pressure + 1) / 2, 0, 1);

    return {
        timestamp: snapshot?.timestamp || snapshot?.date || new Date().toISOString(),
        bodies,
        aspects,
        lunar,
        nakshatra,
        signals,
        bodyStats,
        aspectStats,
        eclipseFlag,
        balance,
        pressure,
        flow,
        clarity,
        coherence,
    };
}

function selectEngines(activeLensIds = []) {
    const ids = toArray(activeLensIds).map(normalizeId).filter(Boolean);
    if (!ids.length) return ENGINE_DEFINITIONS;

    const active = new Set(ids);
    return ENGINE_DEFINITIONS.filter((def) => {
        if (active.has(def.id) || active.has(def.family)) return true;
        const familyMembers = FAMILY_TO_ENGINES[def.family] || [];
        return familyMembers.some((member) => active.has(member));
    });
}

function engineWeightsFor(def) {
    const byEngine = {
        western: { aspect: 0.45, motion: 0.2, lunar: 0.12, signal: 0.23 },
        hellenistic: { aspect: 0.5, motion: 0.18, lunar: 0.14, signal: 0.18 },
        vedic: { lunar: 0.34, nakshatra: 0.3, node: 0.18, signal: 0.18 },
        hermetic: { balance: 0.28, aspect: 0.22, cycle: 0.25, signal: 0.25 },
        iching: { flow: 0.36, polarity: 0.28, cycle: 0.2, signal: 0.16 },
        kabbalistic: { structure: 0.3, tension: 0.3, lunar: 0.18, signal: 0.22 },
        babylonian: { omen: 0.38, eclipse: 0.24, aspect: 0.2, signal: 0.18 },
        maya: { cycle: 0.42, lunar: 0.2, threshold: 0.18, signal: 0.2 },
        arabic: { star: 0.34, lunar: 0.2, aspect: 0.2, signal: 0.26 },
        egyptian: { gate: 0.36, solar: 0.28, lunar: 0.2, signal: 0.16 },
        taoist: { flow: 0.4, balance: 0.26, cycle: 0.16, signal: 0.18 },
        tantric: { force: 0.3, lunar: 0.2, node: 0.2, signal: 0.3 },
    };

    return byEngine[def.id] || { aspect: 0.25, lunar: 0.25, signal: 0.25, cycle: 0.25 };
}

function engineFactors(sky) {
    const { aspectStats, lunar, nakshatra, bodyStats, signals, balance, pressure, flow, clarity, coherence, eclipseFlag } = sky;
    const retrogradeBias = clamp(bodyStats.retrogradeCount / 5, 0, 1);
    const coreRetrogradeBias = clamp(bodyStats.retrogradeCore / 3, 0, 1);
    const lunarPulse = lunar.lead;
    const lunarEdge = lunar.cycleEdge;
    const nodeBias = /rahu|ketu|node/i.test(nakshatra.name) ? 1 : 0.5;
    const nakQuality = /fixed/i.test(nakshatra.quality) ? 0.85 : /dual/i.test(nakshatra.quality) ? 0.65 : 0.45;
    const signalBias = signals.bias;
    const signalRisk = clamp(Math.abs(signals.bias - signals.volatility) / 2 + Math.max(0, -signals.bias) * 0.3, 0, 1);
    const aspectBias = clamp((aspectStats.flow - aspectStats.tension) / (aspectStats.flow + aspectStats.tension + 1), -1, 1);

    return {
        aspect: aspectBias,
        motion: clamp(1 - retrogradeBias * 1.5 - coreRetrogradeBias * 0.2, -1, 1),
        lunar: lunarPulse * lunarEdge,
        nakshatra: (nakQuality - 0.5) * 2,
        node: nodeBias,
        signal: signalBias,
        balance,
        pressure: -pressure,
        flow,
        cycle: lunarEdge,
        threshold: clamp(1 - pressure + clarity * 0.5, -1, 1),
        structure: clamp(bodyStats.dominantElement === 'earth' ? 0.4 : 0.1, -1, 1),
        star: clamp(clarity * 0.8 - signalRisk * 0.2, -1, 1),
        omen: clamp(eclipseFlag ? -0.4 : 0.15, -1, 1),
        solar: clamp((1 - pressure) * 0.6 + lunarEdge * 0.2, -1, 1),
        gate: clamp(coherence - 0.4, -1, 1),
        force: clamp(balance + signalBias * 0.5, -1, 1),
    };
}

function directionFromScore(score) {
    if (score > 0.12) return 1;
    if (score < -0.12) return -1;
    return 0;
}

function horizonFromIntensity(intensity) {
    if (intensity >= 0.72) return 'hours';
    if (intensity >= 0.5) return 'days';
    if (intensity >= 0.32) return 'weeks';
    return 'cycles';
}

function directionLabel(direction) {
    if (direction > 0) return 'bullish';
    if (direction < 0) return 'bearish';
    return 'mixed';
}

function lensModeFactor(mode) {
    switch (normalizeId(mode)) {
        case 'solo':
            return 1.08;
        case 'shadow':
            return 0.88;
        case 'intersection':
            return 1.02;
        case 'chorus':
        default:
            return 1;
    }
}

function phraseForDirection(direction, seed) {
    if (direction > 0) return pick(POSITIVE_PHRASES, seed);
    if (direction < 0) return pick(NEGATIVE_PHRASES, seed);
    return pick(NEUTRAL_PHRASES, seed);
}

function insightLine(def, sky, direction, intensity, seed) {
    const bank = OMEN_PHRASES[def.id] || OMEN_PHRASES.western;
    const omen = pick(bank, seed);
    const directionPhrase = phraseForDirection(direction, seed + 11);
    const balanceTone = sky.balance > 0.15 ? 'open' : sky.balance < -0.15 ? 'tight' : 'split';
    return `${def.name}: ${omen}. ${directionPhrase}. ${balanceTone}.`;
}

function pickTopFactors(factors) {
    return Object.entries(factors)
        .map(([key, value]) => ({ key, value }))
        .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
        .slice(0, 4)
        .map((item) => item.key);
}

export function computeEngineOutputs(snapshot, activeLensIds = [], mode = 'chorus') {
    const sky = skySnapshot(snapshot);
    const selected = selectEngines(activeLensIds);
    const factors = engineFactors(sky);
    const modeFactor = lensModeFactor(mode);

    const outputs = selected.map((def, index) => {
        const weights = engineWeightsFor(def);
        const score =
            (weights.aspect || 0) * factors.aspect +
            (weights.motion || 0) * factors.motion +
            (weights.lunar || 0) * factors.lunar +
            (weights.nakshatra || 0) * factors.nakshatra +
            (weights.node || 0) * factors.node +
            (weights.signal || 0) * factors.signal +
            (weights.balance || 0) * factors.balance +
            (weights.pressure || 0) * factors.pressure +
            (weights.flow || 0) * factors.flow +
            (weights.cycle || 0) * factors.cycle +
            (weights.threshold || 0) * factors.threshold +
            (weights.structure || 0) * factors.structure +
            (weights.star || 0) * factors.star +
            (weights.omen || 0) * factors.omen +
            (weights.solar || 0) * factors.solar +
            (weights.gate || 0) * factors.gate +
            (weights.force || 0) * factors.force;

        const direction = directionFromScore(score);
        const intensity = clamp(Math.abs(score) * 1.15 * modeFactor, 0, 1);
        const clarity = clamp((sky.clarity + intensity + Math.abs(sky.balance)) / 3, 0, 1);
        const confidence = clamp((def.baseConfidence * 0.42) + (sky.coherence * 0.24) + (clarity * 0.2) + (intensity * 0.14), 0.12, 0.96);
        const horizon = horizonFromIntensity(intensity);
        const seed = hashString(`${def.id}:${sky.timestamp}:${mode}:${score.toFixed(3)}`);
        const topFactors = pickTopFactors(weights);

        const output = {
            engine_id: def.id,
            engine_name: def.name,
            family: def.family,
            lens_mode: normalizeId(mode),
            active: true,
            direction,
            direction_label: directionLabel(direction),
            intensity: round(intensity, 3),
            confidence: round(confidence, 3),
            horizon,
            reading: insightLine(def, sky, direction, intensity, seed),
            omen: `${pick(OMEN_PHRASES[def.id] || OMEN_PHRASES.western, seed + 3)}.`,
            prediction:
                direction > 0
                    ? `Lift follows the cut. ${pick(POSITIVE_PHRASES, seed + 5)}.`
                    : direction < 0
                        ? `Pressure holds first. ${pick(NEGATIVE_PHRASES, seed + 5)}.`
                        : `No clean decree. ${pick(NEUTRAL_PHRASES, seed + 5)}.`,
            rationale: [
                `sky:${sky.balance >= 0 ? 'open' : 'tight'}`,
                `aspect:${round(sky.aspectStats.clarity, 2)}`,
                `lunar:${sky.lunar.phaseName}`,
                `signal:${round(sky.signals.bias, 2)}`,
            ],
            feature_trace: {
                score: round(score, 4),
                factors,
                weights,
                top_factors: topFactors,
                dominant_sign: sky.bodyStats.dominantSign,
                dominant_element: sky.bodyStats.dominantElement,
                retrograde_count: sky.bodyStats.retrogradeCount,
                lunar: sky.lunar,
                nakshatra: sky.nakshatra,
                signals: sky.signals,
            },
            source: {
                timestamp: sky.timestamp,
                body_count: sky.bodies.length,
                aspect_count: sky.aspects.length,
            },
        };

        logEngineRun({
            engine_id: output.engine_id,
            mode: output.lens_mode,
            confidence: output.confidence,
            direction: output.direction,
            horizon: output.horizon,
            reading: output.reading,
        });

        return output;
    });

    return outputs;
}

function summarizeGridSignals(gridSignals) {
    if (!gridSignals) {
        return {
            bias: 0,
            note: 'no grid signal',
            factors: [],
        };
    }

    const values = [];
    const factors = [];
    let note = '';

    if (Array.isArray(gridSignals)) {
        for (const item of gridSignals) {
            const val = asNumber(item?.value ?? item?.score ?? item?.strength, 0);
            values.push(val);
            if (item?.name || item?.key) factors.push(asString(item.name || item.key));
        }
    } else if (isObject(gridSignals)) {
        for (const [key, value] of Object.entries(gridSignals)) {
            if (typeof value === 'number' || (typeof value === 'string' && value.trim() !== '')) {
                values.push(asNumber(value, 0));
                factors.push(key);
            }
        }
        note = asString(gridSignals.regime || gridSignals.status || gridSignals.mode || gridSignals.summary);
    }

    const avg = values.length ? values.reduce((a, b) => a + b, 0) / values.length : 0;
    const regimeBias =
        /risk[_ -]?on|bull|expand|green|up/i.test(note) ? 0.25 :
        /risk[_ -]?off|bear|contract|red|down/i.test(note) ? -0.25 :
        0;
    return {
        bias: clamp(avg + regimeBias, -1, 1),
        note: note || 'quiet grid',
        factors: factors.slice(0, 8),
    };
}

function confidenceBand(confidence) {
    if (confidence >= 0.72) return 'high';
    if (confidence >= 0.48) return 'medium';
    if (confidence >= 0.28) return 'low';
    return 'shadow';
}

function contradictionSummary(engineOutputs) {
    const positive = engineOutputs.filter((item) => item.direction > 0);
    const negative = engineOutputs.filter((item) => item.direction < 0);
    const neutral = engineOutputs.filter((item) => item.direction === 0);
    return {
        split: positive.length > 0 && negative.length > 0,
        positive: positive.map((item) => item.engine_id),
        negative: negative.map((item) => item.engine_id),
        neutral: neutral.map((item) => item.engine_id),
    };
}

function combineHorizons(engineOutputs) {
    const buckets = new Map();
    for (const output of engineOutputs) {
        buckets.set(output.horizon, (buckets.get(output.horizon) || 0) + 1);
    }
    return [...buckets.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || 'cycles';
}

export function computeSeer(engineOutputs, gridSignals = {}) {
    const outputs = toArray(engineOutputs).filter(Boolean);
    const signalSummary = summarizeGridSignals(gridSignals);
    const contradictions = contradictionSummary(outputs);

    if (!outputs.length) {
        const fallback = {
            reading: 'No engines. No mouth.',
            prediction: 'Wait for the sky.',
            confidence: 0.12,
            confidence_band: 'shadow',
            supporting_lenses: [],
            conflicts: [],
            key_factors: signalSummary.factors.slice(0, 3),
            horizon: 'cycles',
            log_ref: logSeerRun({
                confidence: 0.12,
                confidence_band: 'shadow',
                reading: 'No engines. No mouth.',
                prediction: 'Wait for the sky.',
            }),
        };
        return fallback;
    }

    const weightedDirectionTotal = outputs.reduce((sum, output) => sum + output.direction * output.confidence * (output.intensity || 1), 0);
    const weightedWeightTotal = outputs.reduce((sum, output) => sum + output.confidence * (output.intensity || 1), 0) || 1;
    const direction = directionFromScore(weightedDirectionTotal / weightedWeightTotal);

    const weightedConfidence =
        outputs.reduce((sum, output) => sum + output.confidence * (1 + (output.direction === direction ? 0.18 : -0.08)), 0) /
        outputs.length;

    const agreement = outputs.filter((output) => output.direction === direction).length / outputs.length;
    const signalAlignment = direction === 0 ? 0 : Math.sign(signalSummary.bias) === direction ? 0.08 : -0.06;
    const confidence = clamp(weightedConfidence + agreement * 0.14 + signalAlignment, 0.12, 0.98);
    const confidenceBandValue = confidenceBand(confidence);
    const horizon = combineHorizons(outputs);
    const seed = hashString(`${JSON.stringify(outputs.map((o) => [o.engine_id, o.direction, o.confidence]))}:${signalSummary.note}`);

    const supporting = outputs
        .filter((output) => output.direction === direction && output.confidence >= 0.45)
        .map((output) => output.engine_id);

    const conflicts = outputs
        .filter((output) => output.direction !== direction && output.direction !== 0)
        .map((output) => ({
            engine_id: output.engine_id,
            direction: output.direction_label,
            confidence: round(output.confidence, 3),
        }));

    const keyFactors = [
        ...new Set([
            ...outputs.flatMap((output) => output.feature_trace?.top_factors || []),
            ...signalSummary.factors,
        ]),
    ].slice(0, 6);

    const balanceWord = signalSummary.bias > 0.18 ? 'open' : signalSummary.bias < -0.18 ? 'tight' : 'split';
    const reading =
        direction > 0
            ? `The room opens. ${pick(POSITIVE_PHRASES, seed)}.`
            : direction < 0
                ? `The room tightens. ${pick(NEGATIVE_PHRASES, seed)}.`
                : `The room splits. ${pick(NEUTRAL_PHRASES, seed)}.`;
    const prediction =
        direction > 0
            ? `Forward bias survives the cut. ${balanceWord} ground.`
            : direction < 0
                ? `Deferral keeps value. ${balanceWord} ground.`
                : `Hold. The signal is mixed.`;

    const result = {
        reading,
        prediction,
        confidence: round(confidence, 3),
        confidence_band: confidenceBandValue,
        supporting_lenses: supporting,
        conflicts,
        key_factors: keyFactors,
        horizon,
        signal_bias: round(signalSummary.bias, 3),
        log_ref: logSeerRun({
            confidence: round(confidence, 3),
            confidence_band: confidenceBandValue,
            direction,
            reading,
            prediction,
            supporting_lenses: supporting,
            conflicts,
            key_factors: keyFactors,
        }),
    };

    return result;
}

function focusFromQuestion(question) {
    const q = String(question || '').toLowerCase();
    if (/\b(trade|market|buy|sell|price|risk|entry|exit|volatility|money|profit)\b/.test(q)) return 'finance';
    if (/\b(when|timing|soon|today|tomorrow|wait|delay|schedule)\b/.test(q)) return 'timing';
    if (/\b(love|relationship|partner|marriage|bond|heart)\b/.test(q)) return 'relationship';
    if (/\b(should|choose|decision|path|move|next)\b/.test(q)) return 'decision';
    if (/\b(why|meaning|purpose|signal|omen|message)\b/.test(q)) return 'meaning';
    return 'general';
}

function personaFor(id) {
    const key = normalizeId(id) || 'seer';
    return PERSONA_MAP[key] || PERSONA_MAP.seer;
}

function personaToneLine(persona, seer, focus, question) {
    const seed = hashString(`${persona.id}:${focus}:${question}:${seer.reading}:${seer.prediction}`);
    const direction = seer.confidence_band === 'shadow' ? 0 : seer.prediction.includes('Forward') || seer.reading.includes('opens') ? 1 : seer.reading.includes('tightens') ? -1 : 0;
    const lead =
        direction > 0
            ? pick(POSITIVE_PHRASES, seed)
            : direction < 0
                ? pick(NEGATIVE_PHRASES, seed)
                : pick(NEUTRAL_PHRASES, seed);
    const personaHooks = {
        finance: ['size small', 'wait for clean spread', 'do not chase the wick'],
        timing: ['too soon burns', 'let the gate open', 'move on the next turn'],
        relationship: ['keep the edge soft', 'do not force the mirror', 'silence helps'],
        decision: ['choose the clean line', 'do not split the knife', 'hold the center'],
        meaning: ['the omen is plain', 'the sign is not loud', 'watch the seam'],
        general: ['keep the blade quiet', 'the sign is enough', 'move with the cut'],
    };
    const hook = pick(personaHooks[focus] || personaHooks.general, seed + 7);
    return `${lead}. ${hook}.`;
}

export function buildPersonaResponse({
    personaId,
    question = '',
    seer = null,
    engineOutputs = [],
    lensIds = [],
    mode = 'chorus',
}) {
    const persona = personaFor(personaId);
    const focus = focusFromQuestion(question);
    const seerState = seer || computeSeer(engineOutputs, {});
    const activeLensIds = toArray(lensIds).map(normalizeId).filter(Boolean);
    const questionLine = String(question || '').trim().slice(0, 180);
    const toneLine = personaToneLine(persona, seerState, focus, questionLine);
    const answer =
        persona.id === 'seer'
            ? `${seerState.reading} ${seerState.prediction} ${toneLine}`
            : `${persona.name}: ${toneLine} ${seerState.prediction}`;

    const response = {
        persona_id: persona.id,
        persona_name: persona.name,
        question: questionLine,
        focus,
        mode: normalizeId(mode),
        lens_ids: activeLensIds,
        answer,
        reading: seerState.reading,
        prediction: seerState.prediction,
        confidence: seerState.confidence,
        confidence_band: seerState.confidence_band,
        horizon: seerState.horizon,
        conflicts: seerState.conflicts,
        log_ref: logPersonaRun({
            persona_id: persona.id,
            focus,
            mode: normalizeId(mode),
            lens_ids: activeLensIds,
            question: questionLine,
            answer,
            seer_log_ref: seerState.log_ref,
        }),
    };

    return response;
}

export default {
    ENGINE_DEFINITIONS,
    computeEngineOutputs,
    computeSeer,
    buildPersonaResponse,
    logEngineRun,
    logSeerRun,
    logPersonaRun,
    readRunLog,
};
