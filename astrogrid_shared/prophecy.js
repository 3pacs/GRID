import {
    normalizeAstrogridAspects,
    normalizeAstrogridBodies,
    normalizeAstrogridLunar,
    normalizeAstrogridNakshatra,
    normalizeAstrogridSignals,
} from './snapshot.js';

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
        name: 'Meridian House',
        family: 'greco-occult',
        focus: 'aspect grammar, solar balance, transit pressure',
        baseConfidence: 0.68,
    },
    {
        id: 'hellenistic',
        name: 'Bronze Hour',
        family: 'greco-occult',
        focus: 'sect, timing, fated turns, sharp edges',
        baseConfidence: 0.7,
    },
    {
        id: 'vedic',
        name: 'Lunar Knot',
        family: 'indic',
        focus: 'moon, nakshatra, nodes, tide',
        baseConfidence: 0.73,
    },
    {
        id: 'hermetic',
        name: 'Mirror Gate',
        family: 'greco-occult',
        focus: 'correspondence, threshold, linked signs',
        baseConfidence: 0.66,
    },
    {
        id: 'iching',
        name: 'Turning Lines',
        family: 'east_asian',
        focus: 'change, polarity, line tension, flow',
        baseConfidence: 0.64,
    },
    {
        id: 'kabbalistic',
        name: 'Ladder Seal',
        family: 'abrahamic',
        focus: 'structure, ascent, tension, channel',
        baseConfidence: 0.63,
    },
    {
        id: 'babylonian',
        name: 'Watchtower',
        family: 'ancient_court',
        focus: 'omen, eclipse, watch, warning',
        baseConfidence: 0.67,
    },
    {
        id: 'maya',
        name: 'Count Wheel',
        family: 'ancient_court',
        focus: 'count, cycle, threshold, repeat signal',
        baseConfidence: 0.65,
    },
    {
        id: 'arabic',
        name: 'Star Road',
        family: 'abrahamic',
        focus: 'star road, dignity, lunar motion, omen',
        baseConfidence: 0.66,
    },
    {
        id: 'egyptian',
        name: 'Solar Gate',
        family: 'ancient_court',
        focus: 'gate, rise, solar threshold, watchfulness',
        baseConfidence: 0.64,
    },
    {
        id: 'taoist',
        name: 'Quiet Current',
        family: 'east_asian',
        focus: 'flow, balance, season, yielding edge',
        baseConfidence: 0.67,
    },
    {
        id: 'tantric',
        name: 'Inner Seal',
        family: 'indic',
        focus: 'force, current, seal, inner pressure',
        baseConfidence: 0.66,
    },
];

const ENGINE_MAP = Object.fromEntries(ENGINE_DEFINITIONS.map((def) => [def.id, def]));

export function labelLens(lensId) {
    const key = normalizeId(lensId);
    return ENGINE_MAP[key]?.name || String(lensId || '').trim() || 'unknown lens';
}

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
        name: 'Meridian Reader',
        tradition: 'western',
        lens_mode: 'solo',
        allowed_lenses: ['western', 'hellenistic', 'hermetic'],
        forbidden_lenses: ['vedic', 'tantric'],
        tone: 'measured',
        verbosity: 'brief',
    },
    vedic: {
        id: 'vedic',
        name: 'Knot Reader',
        tradition: 'vedic',
        lens_mode: 'solo',
        allowed_lenses: ['vedic', 'tantric'],
        forbidden_lenses: ['western', 'hellenistic'],
        tone: 'incantatory',
        verbosity: 'brief',
    },
    hermetic: {
        id: 'hermetic',
        name: 'Mirror Witness',
        tradition: 'hermetic',
        lens_mode: 'shadow',
        allowed_lenses: ['hermetic', 'western', 'hellenistic'],
        forbidden_lenses: [],
        tone: 'cryptic',
        verbosity: 'brief',
    },
    taoist: {
        id: 'taoist',
        name: 'Quiet Observer',
        tradition: 'taoist',
        lens_mode: 'shadow',
        allowed_lenses: ['taoist', 'iching'],
        forbidden_lenses: [],
        tone: 'quiet',
        verbosity: 'brief',
    },
    babylonian: {
        id: 'babylonian',
        name: 'Watchtower Keeper',
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
    'window is open',
    'structure holds',
    'timing supports the move',
    'pressure stays contained',
    'trend can extend',
];

const NEGATIVE_PHRASES = [
    'window is shut',
    'hard geometry leads',
    'timing is early',
    'pressure exceeds confirmation',
    'risk should stay tight',
];

const NEUTRAL_PHRASES = [
    'field is split',
    'timing is unresolved',
    'support and pressure cancel',
    'no lens has control',
    'wait for the turn',
];

const OMEN_PHRASES = {
    western: ['aspect balance', 'angular pressure', 'planet concentration'],
    hellenistic: ['sect tension', 'malefic pressure', 'timing turn'],
    vedic: ['lunar timing', 'node pressure', 'nakshatra emphasis'],
    hermetic: ['correspondence pattern', 'paired signal', 'threshold relation'],
    iching: ['change pattern', 'reversal pressure', 'yielding vs force'],
    kabbalistic: ['ordered sequence', 'channel strain', 'structural imbalance'],
    babylonian: ['omen pattern', 'eclipse pressure', 'public warning'],
    maya: ['cycle count', 'repeat interval', 'calendar threshold'],
    arabic: ['lunar road', 'stellar timing', 'dignity emphasis'],
    egyptian: ['solar threshold', 'watch interval', 'gate condition'],
    taoist: ['flow imbalance', 'seasonal pressure', 'yielding advantage'],
    tantric: ['current pressure', 'seal tension', 'force buildup'],
};

const FAMILY_FRAMES = {
    'greco-occult': {
        theology_domain: 'occult-astrological',
        doctrine: 'This family emphasizes aspect structure, elemental balance, and timing turns.',
        ritual_window: 'near timing window',
        symbolic_axis: ['aspect', 'sect', 'element'],
    },
    indic: {
        theology_domain: 'dharmic-cyclical',
        doctrine: 'This family emphasizes the moon, nodes, mansions, and cycle timing.',
        ritual_window: 'lunar window',
        symbolic_axis: ['moon', 'nakshatra', 'node'],
    },
    abrahamic: {
        theology_domain: 'scriptural-esoteric',
        doctrine: 'This family emphasizes ordered sequences, channel logic, and lunar timing.',
        ritual_window: 'watch window',
        symbolic_axis: ['channel', 'ladder', 'seal'],
    },
    east_asian: {
        theology_domain: 'cosmological-balance',
        doctrine: 'This family emphasizes polarity, flow, and seasonal change.',
        ritual_window: 'seasonal window',
        symbolic_axis: ['flow', 'change', 'balance'],
    },
    ancient_court: {
        theology_domain: 'omen-statecraft',
        doctrine: 'This family emphasizes omen reading, public warnings, and cyclical thresholds.',
        ritual_window: 'watch interval',
        symbolic_axis: ['omen', 'gate', 'calendar'],
    },
};

const ENGINE_FRAME_MAP = {
    western: {
        tradition_frame: 'Modern western astrology',
        sacred_axis: ['solar balance', 'hard vs soft geometry', 'angular pressure'],
        taboos_or_cautions: ['do not overread one clean trine', 'watch hard angles before conviction'],
    },
    hellenistic: {
        tradition_frame: 'Hellenistic timing and sect',
        sacred_axis: ['sect', 'fated turn', 'malefic edge'],
        taboos_or_cautions: ['do not ignore sect tension', 'do not flatten benefic and malefic roles'],
    },
    vedic: {
        tradition_frame: 'Jyotish and lunar mansions',
        sacred_axis: ['nakshatra', 'nodes', 'lunar tide'],
        taboos_or_cautions: ['do not force action under node pressure', 'watch the moon before the headline'],
    },
    hermetic: {
        tradition_frame: 'Hermetic correspondence',
        sacred_axis: ['mirror', 'threshold', 'linked signs'],
        taboos_or_cautions: ['avoid severing linked causes', 'respect crossings and mirrored signals'],
    },
    iching: {
        tradition_frame: 'Book of changes',
        sacred_axis: ['line change', 'yielding', 'reversal'],
        taboos_or_cautions: ['do not mistake movement for progress', 'respect reversal at the seam'],
    },
    kabbalistic: {
        tradition_frame: 'Kabbalistic ascent and channels',
        sacred_axis: ['ladder', 'channel', 'seal'],
        taboos_or_cautions: ['do not force ascent through strain', 'watch broken channels'],
    },
    babylonian: {
        tradition_frame: 'Court omen reading',
        sacred_axis: ['watch sign', 'eclipse warning', 'public decree'],
        taboos_or_cautions: ['heed eclipse pressure', 'do not treat omens as private only'],
    },
    maya: {
        tradition_frame: 'Calendar cycle reading',
        sacred_axis: ['count', 'repeat', 'threshold'],
        taboos_or_cautions: ['do not ignore repeating counts', 'watch cycle closure before expansion'],
    },
    arabic: {
        tradition_frame: 'Arabic star-road practice',
        sacred_axis: ['manzil', 'night path', 'dignity'],
        taboos_or_cautions: ['do not outrun the moon', 'watch the road, not just the destination'],
    },
    egyptian: {
        tradition_frame: 'Gate and solar threshold reading',
        sacred_axis: ['gate', 'rise', 'solar threshold'],
        taboos_or_cautions: ['watch the gate before the march', 'do not confuse dawn with safety'],
    },
    taoist: {
        tradition_frame: 'Seasonal and energetic balance',
        sacred_axis: ['flow', 'grain', 'yielding edge'],
        taboos_or_cautions: ['do not push against the grain', 'yield before forcing structure'],
    },
    tantric: {
        tradition_frame: 'Tantric current and seal',
        sacred_axis: ['current', 'seal', 'inner heat'],
        taboos_or_cautions: ['do not break the seal under pressure', 'channel force before release'],
    },
};

const FACTOR_LABELS = {
    aspect: 'geometry',
    motion: 'motion',
    lunar: 'moon',
    nakshatra: 'mansion',
    node: 'nodes',
    signal: 'tape',
    balance: 'balance',
    pressure: 'pressure',
    flow: 'flow',
    cycle: 'cycle',
    threshold: 'window',
    structure: 'structure',
    star: 'clarity',
    omen: 'omen',
    solar: 'solar',
    gate: 'gate',
    force: 'force',
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
    const phaseName = asString(lunar.phaseName || lunar.phase_name, 'Unknown');
    const waxing = /wax/i.test(phaseName);
    const waning = /wan/i.test(phaseName);
    const lead = waxing ? 1 : waning ? -1 : 0;
    const cycleEdge = 1 - Math.abs(phase - 0.5) * 2;

    return {
        phase,
        phaseName,
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
    const localFeatures = isObject(snapshot?.local_features) ? snapshot.local_features : {};
    const bodies = normalizeAstrogridBodies(snapshot);
    const aspects = normalizeAstrogridAspects(snapshot).map((aspect) => ({
        ...aspect,
        strength: clamp(1 - asNumber(aspect.orb_used, 0) / 12, 0, 1),
    }));
    const lunar = lunarSummary(normalizeAstrogridLunar(snapshot));
    const nakshatra = normalizeAstrogridNakshatra(snapshot);
    const signals = summarizeSignals(
        normalizeAstrogridSignals(snapshot?.signals ?? snapshot?.gridSignals ?? snapshot?.marketSignals ?? snapshot?.signals_state)
    );
    const bodyStats = bodySummary(bodies);
    const aspectStats = aspectSummary(aspects);
    const eclipseDistance = Math.min(
        asNumber(localFeatures.lunar_eclipse_proximity, 999),
        asNumber(localFeatures.solar_eclipse_proximity, 999),
    );
    const eclipseFlag = Boolean(
        snapshot?.eclipses ||
        snapshot?.eclipse ||
        /eclipse/i.test(nakshatra.name) ||
        eclipseDistance <= 30
    );
    const voidFlag = Boolean(snapshot?.void_of_course?.is_void);
    const geomagneticKp = asNumber(localFeatures.geomagnetic_kp_index_recent, 0);
    const solarWind = asNumber(localFeatures.solar_wind_speed_recent, 350);
    const solarPressure = clamp(((geomagneticKp / 7) * 0.65) + (Math.max(0, solarWind - 350) / 300) * 0.35, 0, 1);
    const venusCycle = asNumber(localFeatures.venus_cycle_phase, 0.5);
    const venusBias = clamp((0.5 - Math.abs(venusCycle - 0.5)) * 2 - 0.2, -1, 1);
    const tithi = asNumber(localFeatures.tithi, 15);
    const tithiBias = clamp((15 - Math.abs(tithi - 15)) / 15 - 0.4, -1, 1);

    const balance = clamp(
        (aspectStats.flow - aspectStats.tension * 0.72 + lunar.cycleEdge * 0.7 + signals.bias * 0.9 + venusBias * 0.35) / 3.35,
        -1,
        1,
    );
    const pressure = clamp(
        (aspectStats.tension + bodyStats.retrogradeCount * 0.8 + (eclipseFlag ? 1.25 : 0) + (voidFlag ? 0.9 : 0) + solarPressure * 0.8 + Math.max(0, -signals.bias) * 0.8) / 7.2,
        0,
        1,
    );
    const flow = clamp(
        (aspectStats.flow + lunar.illumination * 0.4 + Math.max(0, signals.bias) * 0.8 + Math.max(0, venusBias) * 0.35 + Math.max(0, tithiBias) * 0.25) / 4.4,
        0,
        1,
    );
    const clarity = clamp((aspectStats.clarity + (1 - pressure) + Math.abs(balance) + (1 - solarPressure) * 0.2) / 3.2, 0, 1);
    const coherence = clamp((flow + clarity - pressure + 1) / 2, 0, 1);

    return {
        timestamp: snapshot?.timestamp || snapshot?.date || new Date().toISOString(),
        bodies,
        aspects,
        lunar,
        nakshatra,
        signals,
        localFeatures,
        bodyStats,
        aspectStats,
        eclipseFlag,
        eclipseDistance,
        voidFlag,
        solarPressure,
        venusBias,
        tithiBias,
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
    const { aspectStats, lunar, nakshatra, bodyStats, signals, balance, pressure, flow, clarity, coherence, eclipseFlag, voidFlag, solarPressure, venusBias, tithiBias, eclipseDistance } = sky;
    const retrogradeBias = clamp(bodyStats.retrogradeCount / 5, 0, 1);
    const coreRetrogradeBias = clamp(bodyStats.retrogradeCore / 3, 0, 1);
    const lunarPulse = lunar.lead;
    const lunarEdge = lunar.cycleEdge;
    const nodeBias = /rahu|ketu|node/i.test(nakshatra.name) ? 1 : 0.5;
    const nakQuality = /fixed/i.test(nakshatra.quality) ? 0.85 : /dual/i.test(nakshatra.quality) ? 0.65 : 0.45;
    const signalBias = signals.bias;
    const signalRisk = clamp(Math.abs(signals.bias - signals.volatility) / 2 + Math.max(0, -signals.bias) * 0.3, 0, 1);
    const aspectBias = clamp((aspectStats.flow - aspectStats.tension) / (aspectStats.flow + aspectStats.tension + 1), -1, 1);
    const eclipseBias = clamp(eclipseDistance <= 14 ? -0.75 : eclipseDistance <= 45 ? -0.35 : 0.1, -1, 1);
    const voidBias = voidFlag ? -0.7 : 0.12;

    return {
        aspect: aspectBias,
        motion: clamp(1 - retrogradeBias * 1.5 - coreRetrogradeBias * 0.2, -1, 1),
        lunar: clamp((lunarPulse * lunarEdge) + tithiBias * 0.2, -1, 1),
        nakshatra: (nakQuality - 0.5) * 2,
        node: nodeBias,
        signal: signalBias,
        balance: clamp(balance + venusBias * 0.18, -1, 1),
        pressure: clamp(-pressure + voidBias * 0.12 + eclipseBias * 0.08, -1, 1),
        flow,
        cycle: lunarEdge,
        threshold: clamp(1 - pressure + clarity * 0.5 + voidBias * 0.25, -1, 1),
        structure: clamp(bodyStats.dominantElement === 'earth' ? 0.4 : 0.1, -1, 1),
        star: clamp(clarity * 0.8 - signalRisk * 0.2, -1, 1),
        omen: clamp((eclipseFlag ? -0.4 : 0.15) + eclipseBias * 0.5, -1, 1),
        solar: clamp((1 - pressure) * 0.6 + lunarEdge * 0.2 - solarPressure * 0.5, -1, 1),
        gate: clamp(coherence - 0.4 + voidBias * 0.18, -1, 1),
        force: clamp(balance + signalBias * 0.5 + venusBias * 0.25, -1, 1),
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
    const primaryFactor = pickTopFactors(engineWeightsFor(def))[0] || 'aspect';
    const witness = factorWitness(primaryFactor, sky);
    const directionPhrase = phraseForDirection(direction, seed + 11);
    return `${def.name} · ${omen} · ${witness} · ${directionPhrase}.`;
}

function pickTopFactors(factors) {
    return Object.entries(factors)
        .map(([key, value]) => ({ key, value }))
        .sort((a, b) => Math.abs(b.value) - Math.abs(a.value))
        .slice(0, 4)
        .map((item) => item.key);
}

function gridSignalTag(signals = {}) {
    const regime = asString(signals.regime).toLowerCase().replace(/_/g, ' ');
    if (regime) return regime;
    if ((signals.bias ?? 0) > 0.22) return 'risk on';
    if ((signals.bias ?? 0) < -0.22) return 'risk off';
    return 'mixed tape';
}

function factorWitness(key, sky) {
    switch (key) {
        case 'aspect':
            return `h${sky.aspectStats.hard} s${sky.aspectStats.soft}`;
        case 'motion':
            return `retro ${sky.bodyStats.retrogradeCount}`;
        case 'lunar':
            return sky.lunar.waxing ? 'waxing moon' : sky.lunar.waning ? 'waning moon' : 'balanced moon';
        case 'nakshatra':
            return `${(sky.nakshatra.name || 'unknown mansion').toLowerCase()} ${asString(sky.nakshatra.quality, 'mixed')}`.trim();
        case 'node':
            return sky.eclipseFlag ? 'node pressure high' : sky.eclipseDistance <= 45 ? `nodes ${Math.round(sky.eclipseDistance)}d` : 'node pressure low';
        case 'signal':
            return gridSignalTag(sky.signals);
        case 'balance':
            return `balance ${round(sky.balance, 2)}`;
        case 'pressure':
            return `pressure ${round(sky.pressure, 2)}`;
        case 'flow':
            return `flow ${round(sky.flow, 2)}`;
        case 'cycle':
            return `cycle ${round(sky.lunar.cycleEdge, 2)}`;
        case 'threshold':
            return sky.voidFlag ? 'void seam' : sky.eclipseFlag ? 'eclipse window' : `window ${round(sky.coherence, 2)}`;
        case 'structure':
            return `${sky.bodyStats.dominantElement} dominance`;
        case 'star':
            return `clarity ${round(sky.clarity, 2)}`;
        case 'omen':
            return sky.eclipseFlag ? 'eclipse active' : `eclipse ${Math.round(sky.eclipseDistance)}d`;
        case 'solar':
            return `solar ${round(sky.solarPressure, 2)}`;
        case 'gate':
            return sky.voidFlag ? 'gate obstructed' : `gate ${round(sky.coherence, 2)}`;
        case 'force':
            return `force ${round(sky.balance + sky.flow - sky.pressure, 2)}`;
        default:
            return FACTOR_LABELS[key] || key;
    }
}

function topAspectThreads(skyState) {
    return skyState.aspects
        .slice()
        .sort((a, b) => (a.orb_used ?? 99) - (b.orb_used ?? 99))
        .slice(0, 5)
        .map((aspect) => ({
            id: `${normalizeId(aspect.planet1)}-${normalizeId(aspect.aspect_type)}-${normalizeId(aspect.planet2)}`,
            kind: 'aspect',
            title: `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`,
            detail: `Orb ${round(aspect.orb_used ?? 0, 2)}°. ${aspect.applying ? 'Applying' : 'Separating'}.`,
            relevance: round(aspect.strength ?? 0.5, 3),
        }));
}

function retrogradeThreads(skyState) {
    return skyState.bodies
        .filter((body) => body.retrograde)
        .map((body) => ({
            id: `${body.id}-retrograde`,
            kind: 'retrograde',
            title: `${body.name} retrograde`,
            detail: `${body.name} moves against the expected line in ${body.sign || 'its current sign'}.`,
            relevance: 0.74,
        }));
}

function lunarThreads(skyState) {
    return [
        {
            id: 'lunar-phase',
            kind: 'lunar',
            title: skyState.lunar.phaseName,
            detail: `${round((skyState.lunar.illumination || 0) * 100, 1)}% illumination. ${skyState.lunar.waxing ? 'Waxing.' : skyState.lunar.waning ? 'Waning.' : 'Balanced.'}`,
            relevance: round(0.5 + skyState.lunar.cycleEdge * 0.4, 3),
        },
        {
            id: 'nakshatra',
            kind: 'nakshatra',
            title: skyState.nakshatra.name || 'Nakshatra',
            detail: `${skyState.nakshatra.quality || 'Unmarked'} quality. Pada ${skyState.nakshatra.pada || '—'}.`,
            relevance: 0.69,
        },
    ];
}

function signalThreads(skyState) {
    const threads = [
        {
            id: 'signal-bias',
            kind: 'signal',
            title: 'Signal bias',
            detail: `Bias ${round(skyState.signals.bias, 3)}. Regime: ${skyState.signals.regime || 'quiet grid'}.`,
            relevance: round(Math.min(1, Math.abs(skyState.signals.bias) + 0.3), 3),
        },
    ];
    if (Number.isFinite(skyState.signals.volatility) && skyState.signals.volatility !== 0) {
        threads.push({
            id: 'signal-volatility',
            kind: 'signal',
            title: 'Volatility pressure',
            detail: `Volatility proxy ${round(skyState.signals.volatility, 3)}. Trend ${round(skyState.signals.trend, 3)}.`,
            relevance: round(Math.min(1, Math.abs(skyState.signals.volatility) + 0.25), 3),
        });
    }
    return threads;
}

function lensThreads(engineOutputs) {
    return engineOutputs.slice(0, 8).map((engine) => ({
        id: `${engine.engine_id}-lens`,
        kind: 'lens',
        title: `${engine.engine_name} thread`,
        detail: engine.claims?.[0]?.statement || engine.prediction,
        relevance: round(engine.confidence || 0.5, 3),
        lenses: [engine.engine_id],
    }));
}

function frameFor(def) {
    const familyFrame = FAMILY_FRAMES[def.family] || {
        theology_domain: 'cosmological',
        doctrine: 'This lens weights moving sky relations.',
        ritual_window: 'turning point',
        symbolic_axis: ['pattern'],
    };
    const engineFrame = ENGINE_FRAME_MAP[def.id] || {
        tradition_frame: def.name,
        sacred_axis: [def.focus],
        taboos_or_cautions: [],
    };
    return {
        ...familyFrame,
        ...engineFrame,
    };
}

function sacredCalendar(sky) {
    if (sky.eclipseFlag) return 'eclipse window';
    if (sky.lunar.waxing) return 'waxing moon';
    if (sky.lunar.waning) return 'waning moon';
    return 'balanced lunar phase';
}

function ritualWindowFor(frame, horizon, sky) {
    const windowBase = frame.ritual_window || 'turning point';
    if (sky.eclipseFlag) return `${windowBase} / eclipse`;
    if (sky.voidFlag) return `${windowBase} / void`;
    if (horizon === 'hours') return `${windowBase} / live`;
    if (horizon === 'days') return `${windowBase} / near`;
    if (horizon === 'weeks') return `${windowBase} / swing`;
    return `${windowBase} / cycle`;
}

function symbolicAxisFor(frame, sky) {
    return [
        ...(frame.symbolic_axis || []),
        sky.bodyStats.dominantElement,
        sky.bodyStats.dominantSign,
    ].filter(Boolean);
}

function contradictionNotes(def, sky, direction) {
    const notes = [];
    if (direction > 0 && sky.pressure > 0.68) {
        notes.push('pressure resists the lift');
    }
    if (direction < 0 && sky.flow > 0.55) {
        notes.push('soft geometry weakens the warning');
    }
    if (sky.eclipseFlag && sky.clarity > 0.58) {
        notes.push('clarity cuts through an eclipse omen');
    }
    if (def.family === 'abrahamic' && sky.signals.bias > 0.24 && sky.aspectStats.tension > sky.aspectStats.flow) {
        notes.push('gain appears before the field is lawful');
    }
    if (def.family === 'indic' && sky.bodyStats.retrogradeCount > 2 && sky.lunar.waxing) {
        notes.push('waxing motion is under node drag');
    }
    if (sky.voidFlag) {
        notes.push('void moon weakens clean follow-through');
    }
    if (sky.eclipseDistance <= 14) {
        notes.push('eclipse perimeter distorts clean price discovery');
    }
    if (sky.solarPressure >= 0.58) {
        notes.push('solar static contaminates the field');
    }
    return notes;
}

function basisLines(def, sky, topFactors) {
    const lines = [
        `${def.name} weighs ${topFactors.join(', ') || 'the present sky'}.`,
        `Moon: ${sky.lunar.phaseName}. Nakshatra: ${sky.nakshatra.name}.`,
        `Dominant element: ${sky.bodyStats.dominantElement}. Dominant sign: ${sky.bodyStats.dominantSign}.`,
        `Hard aspects: ${sky.aspectStats.hard}. Soft aspects: ${sky.aspectStats.soft}.`,
    ];
    if (sky.eclipseFlag) {
        lines.push('Eclipse conditions contaminate clean readings.');
    }
    if (sky.voidFlag) {
        lines.push('Void-of-course conditions reduce follow-through.');
    }
    if (sky.solarPressure >= 0.58) {
        lines.push('Solar weather raises background noise.');
    }
    return lines;
}

function falsifiableBy(topic, sky) {
    if (topic === 'timing') {
        return sky.aspectStats.hard > sky.aspectStats.soft
            ? 'soft aspects overtake hard geometry on the next turn'
            : 'hard geometry spikes before the stated horizon';
    }
    if (topic === 'risk') {
        return sky.bodyStats.retrogradeCount > 1
            ? 'retrograde count collapses or lunar pressure clears'
            : 'retrograde pressure returns and signal bias flips';
    }
    return 'the next logged outcome contradicts the claimed branch';
}

function claimDirection(direction, positive, negative, neutral = 'hold') {
    if (direction > 0) return positive;
    if (direction < 0) return negative;
    return neutral;
}

function buildPredictionClaims(def, sky, direction, confidence, horizon, topFactors, frame) {
    const timingDirection = claimDirection(direction, 'advance', 'delay', 'wait');
    const riskDirection = claimDirection(direction, 'expand', 'hedge', 'reduce');
    const meaningDirection = claimDirection(direction, 'reveal', 'conceal', 'observe');
    const signalWord = sky.signals.bias > 0.2 ? 'risk-on' : sky.signals.bias < -0.2 ? 'risk-off' : 'mixed';
    const ritualWindow = ritualWindowFor(frame, horizon, sky);
    const primaryWitness = factorWitness(topFactors[0] || 'aspect', sky);
    const secondaryWitness = factorWitness(topFactors[1] || 'pressure', sky);

    return [
        {
            topic: 'timing',
            direction: timingDirection,
            timeframe: horizon,
            strength: round(confidence, 3),
            basis: `${primaryWitness} + ${sky.lunar.phaseName.toLowerCase()} + ${signalWord}`,
            falsifiable_by: falsifiableBy('timing', sky),
            statement:
                direction > 0
                    ? `Advance in ${ritualWindow} while ${primaryWitness} holds.`
                    : direction < 0
                        ? `Delay in ${ritualWindow}; ${primaryWitness} still resists.`
                        : `Wait in ${ritualWindow}; timing is unresolved.`,
            bias: direction,
        },
        {
            topic: 'risk',
            direction: riskDirection,
            timeframe: horizon === 'hours' ? 'hours' : 'days',
            strength: round(clamp(confidence - 0.04, 0.12, 0.98), 3),
            basis: `${secondaryWitness} + retrogrades:${sky.bodyStats.retrogradeCount}`,
            falsifiable_by: falsifiableBy('risk', sky),
            statement:
                direction > 0
                    ? `Risk can widen while ${secondaryWitness} stays contained.`
                    : direction < 0
                        ? `Keep risk tight while ${secondaryWitness} and retro drag stay elevated.`
                        : 'Keep size small.',
            bias: direction,
        },
        {
            topic: 'meaning',
            direction: meaningDirection,
            timeframe: horizon,
            strength: round(clamp(confidence - 0.08, 0.12, 0.98), 3),
            basis: `${frame.tradition_frame} / ${primaryWitness} / ${secondaryWitness}`,
            falsifiable_by: falsifiableBy('meaning', sky),
            statement:
                direction > 0
                    ? `${frame.tradition_frame} favors ${primaryWitness} over ${secondaryWitness}.`
                    : direction < 0
                        ? `${frame.tradition_frame} treats ${secondaryWitness} as dominant.`
                        : `${frame.tradition_frame} shows no clean lead.`,
            bias: direction === 0 ? 0 : direction * 0.6,
        },
    ];
}

function buildCorrespondence(def, sky, frame, horizon) {
    const lunarState = sky.lunar.waxing ? 'waxing' : sky.lunar.waning ? 'waning' : 'balanced';
    return {
        calendar: sacredCalendar(sky),
        sacred_time: `${lunarState} moon / ${sky.nakshatra.name}`,
        ritual_window: ritualWindowFor(frame, horizon, sky),
        symbolic_axis: symbolicAxisFor(frame, sky),
        witness_stack: [
            factorWitness('aspect', sky),
            factorWitness('lunar', sky),
            factorWitness('signal', sky),
        ],
        taboos_or_cautions: [
            ...frame.taboos_or_cautions,
            ...(sky.bodyStats.retrogradeCount > 2 ? ['retrograde drag distorts clean signal'] : []),
            ...(sky.pressure > 0.66 ? ['pressure is too high for force'] : []),
            ...(sky.voidFlag ? ['void moon weakens follow-through'] : []),
            ...(sky.eclipseDistance <= 30 ? ['eclipse perimeter distorts clean reads'] : []),
        ],
    };
}

function mergeRationale(def, sky, topFactors, frame) {
    return [
        `Method: ${frame.tradition_frame}.`,
        `Weights: ${(frame.sacred_axis || []).join(', ')}.`,
        `State: ${factorWitness(topFactors[0] || 'aspect', sky)} / ${factorWitness(topFactors[1] || 'pressure', sky)} / ${factorWitness('signal', sky)}.`,
        ...basisLines(def, sky, topFactors),
    ];
}

export function normalizeSkyState(snapshot) {
    return skySnapshot(snapshot);
}

export function extractSkyThreads(snapshot, engineOutputs = []) {
    const skyState = normalizeSkyState(snapshot);
    const threads = [
        ...topAspectThreads(skyState),
        ...retrogradeThreads(skyState),
        ...lunarThreads(skyState),
        ...signalThreads(skyState),
        ...lensThreads(toArray(engineOutputs).filter(Boolean)),
    ];
    return threads
        .sort((a, b) => (b.relevance || 0) - (a.relevance || 0))
        .slice(0, 12);
}

export function deriveTraditionFeatures(skyState) {
    const sharedFactors = engineFactors(skyState);
    return Object.fromEntries(
        ENGINE_DEFINITIONS.map((def) => {
            const weights = engineWeightsFor(def);
            const topFactors = pickTopFactors(weights);
            return [
                def.id,
                {
                    factors: sharedFactors,
                    weights,
                    topFactors,
                    frame: frameFor(def),
                },
            ];
        }),
    );
}

export function runEngine(engineId, skyState, featureMap = {}, context = {}) {
    const def = ENGINE_MAP[normalizeId(engineId)];
    if (!def) return null;

    const featureSet = featureMap[def.id] || {
        factors: engineFactors(skyState),
        weights: engineWeightsFor(def),
        topFactors: pickTopFactors(engineWeightsFor(def)),
        frame: frameFor(def),
    };

    const factors = featureSet.factors;
    const weights = featureSet.weights;
    const topFactors = featureSet.topFactors;
    const frame = featureSet.frame;
    const mode = normalizeId(context.mode || 'chorus');
    const modeFactor = lensModeFactor(mode);

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
    const clarity = clamp((skyState.clarity + intensity + Math.abs(skyState.balance)) / 3, 0, 1);
    const confidence = clamp(
        (def.baseConfidence * 0.42) + (skyState.coherence * 0.24) + (clarity * 0.2) + (intensity * 0.14),
        0.12,
        0.96,
    );
    const horizon = horizonFromIntensity(intensity);
    const seed = hashString(`${def.id}:${skyState.timestamp}:${mode}:${score.toFixed(3)}`);
    const contradictions = contradictionNotes(def, skyState, direction);
    const claims = buildPredictionClaims(def, skyState, direction, confidence, horizon, topFactors, frame);
    const correspondence = buildCorrespondence(def, skyState, frame, horizon);
    const rationale = mergeRationale(def, skyState, topFactors, frame);

    const output = {
        engine_id: def.id,
        engine_name: def.name,
        family: def.family,
        tradition_frame: frame.tradition_frame,
        theology_domain: frame.theology_domain,
        doctrine: frame.doctrine,
        sacred_axis: frame.sacred_axis,
        lens_mode: mode,
        permitted_lenses: [def.id, ...(FAMILY_TO_ENGINES[def.family] || []).filter((id) => id !== def.id)],
        active: true,
        direction,
        direction_label: directionLabel(direction),
        intensity: round(intensity, 3),
        confidence: round(confidence, 3),
        confidence_band: confidenceBand(confidence),
        horizon,
        reading: insightLine(def, skyState, direction, intensity, seed),
        omen: `${pick(OMEN_PHRASES[def.id] || OMEN_PHRASES.western, seed + 3)}.`,
        prediction:
            direction > 0
                ? `Press on confirm. ${pick(POSITIVE_PHRASES, seed + 5)}.`
                : direction < 0
                    ? `Hedge into the turn. ${pick(NEGATIVE_PHRASES, seed + 5)}.`
                    : `Wait for the turn. ${pick(NEUTRAL_PHRASES, seed + 5)}.`,
        claims,
        rationale,
        correspondence,
        contradictions,
        feature_trace: {
            score: round(score, 4),
            factors,
            weights,
            top_factors: topFactors,
            dominant_sign: skyState.bodyStats.dominantSign,
            dominant_element: skyState.bodyStats.dominantElement,
            retrograde_count: skyState.bodyStats.retrogradeCount,
            lunar: skyState.lunar,
            nakshatra: skyState.nakshatra,
            signals: skyState.signals,
        },
        source_snapshot_ref: skyState.timestamp,
        source: {
            timestamp: skyState.timestamp,
            body_count: skyState.bodies.length,
            aspect_count: skyState.aspects.length,
        },
    };

    logEngineRun({
        engine_id: output.engine_id,
        mode: output.lens_mode,
        confidence: output.confidence,
        direction: output.direction,
        horizon: output.horizon,
        reading: output.reading,
        claims,
        contradictions,
        correspondence,
        source_snapshot_ref: output.source_snapshot_ref,
    });

    return output;
}

export function computeEngineOutputs(snapshot, activeLensIds = [], mode = 'chorus') {
    const sky = normalizeSkyState(snapshot);
    const selected = selectEngines(activeLensIds);
    const featureMap = deriveTraditionFeatures(sky);
    return selected
        .map((def) => runEngine(def.id, sky, featureMap, { mode }))
        .filter(Boolean);
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

function aggregateClaimBranches(engineOutputs) {
    const grouped = new Map();
    for (const output of engineOutputs) {
        for (const claim of output.claims || []) {
            const key = claim.topic || 'general';
            if (!grouped.has(key)) grouped.set(key, []);
            grouped.get(key).push({
                ...claim,
                engine_id: output.engine_id,
                engine_name: output.engine_name,
                family: output.family,
                engine_confidence: output.confidence,
            });
        }
    }

    return [...grouped.entries()].map(([topic, claims]) => {
        const weightedBias = claims.reduce((sum, claim) => sum + (claim.bias || 0) * (claim.strength || 0.5), 0);
        const totalWeight = claims.reduce((sum, claim) => sum + (claim.strength || 0.5), 0) || 1;
        const meanBias = weightedBias / totalWeight;
        const primary = claims
            .slice()
            .sort((a, b) => (b.strength || 0) - (a.strength || 0))[0];
        const support = claims
            .filter((claim) => Math.sign(claim.bias || 0) === Math.sign(meanBias || 0) || (claim.bias || 0) === 0)
            .map((claim) => claim.engine_id);
        const conflict = claims
            .filter((claim) => Math.sign(claim.bias || 0) !== Math.sign(meanBias || 0) && (claim.bias || 0) !== 0)
            .map((claim) => claim.engine_id);

        return {
            topic,
            bias: round(meanBias, 3),
            direction: directionLabel(directionFromScore(meanBias)),
            statement: primary?.statement || 'No statement.',
            timeframe: primary?.timeframe || 'cycles',
            basis: primary?.basis || '',
            support,
            conflict,
            primary_engine: primary?.engine_id || null,
        };
    });
}

function familySpread(engineOutputs) {
    return [...new Set(engineOutputs.map((output) => output.family).filter(Boolean))];
}

function seerWitnessLabel(key, sample, signalSummary) {
    switch (key) {
        case 'signal':
            return gridSignalTag(sample?.signals || signalSummary);
        case 'lunar': {
            const phaseName = asString(sample?.lunar?.phaseName || sample?.lunar?.phase_name);
            return phaseName ? phaseName.toLowerCase() : 'moon mixed';
        }
        case 'nakshatra':
            return asString(sample?.nakshatra?.name, 'mansion mixed').toLowerCase();
        case 'motion':
            return `retro ${asNumber(sample?.retrograde_count, 0)}`;
        case 'structure':
            return `${asString(sample?.dominant_element, 'mixed')} dominance`;
        default:
            return FACTOR_LABELS[key] || key;
    }
}

export function mergeSeer(engineOutputs, gridSignals = {}, history = {}) {
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
    const claimBranches = aggregateClaimBranches(outputs);

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

    const families = familySpread(outputs);
    const agreementRatio = round(agreement, 3);
    const sample = outputs[0]?.feature_trace || {};
    const primaryBranch = claimBranches
        .slice()
        .sort((a, b) => Math.abs(b.bias) - Math.abs(a.bias))[0] || null;
    const alternateBranches = claimBranches
        .filter((branch) => branch.topic !== primaryBranch?.topic)
        .slice(0, 2);
    const fracturePoints = [
        ...(contradictions.split ? ['directional split across active lenses'] : []),
        ...claimBranches
            .filter((branch) => branch.conflict?.length)
            .map((branch) => `${branch.topic}: ${branch.conflict.join(', ')}`),
    ].slice(0, 4);
    const primaryWitness = seerWitnessLabel(keyFactors[0] || 'signal', sample, signalSummary);
    const secondaryWitness = seerWitnessLabel(keyFactors[1] || 'lunar', sample, signalSummary);
    const reading =
        direction > 0
            ? `${primaryWitness} leads. ${pick(POSITIVE_PHRASES, seed)}.`
            : direction < 0
                ? `${primaryWitness} leads against the tape. ${pick(NEGATIVE_PHRASES, seed)}.`
                : `${primaryWitness} and ${secondaryWitness} conflict. ${pick(NEUTRAL_PHRASES, seed)}.`;
    const prediction =
        direction > 0
            ? `Press only if tape confirms ${primaryWitness}.`
            : direction < 0
                ? `Hedge while ${primaryWitness} still dominates.`
                : `Wait for a clean lead between ${primaryWitness} and ${secondaryWitness}.`;
    const contradictionNote = contradictions.split
        ? `Fracture remains between ${contradictions.positive.join(', ')} and ${contradictions.negative.join(', ')}.`
        : 'No major directional fracture.';

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
        agreement_ratio: agreementRatio,
        primary_branch: primaryBranch,
        alternate_branches: alternateBranches,
        fracture_points: fracturePoints,
        contradiction_note: contradictionNote,
        verdicts: claimBranches,
        families,
        grid_alignment: signalSummary.note,
        historical_weighting: history?.weights || 'pending',
        outcome_scoring_ref: history?.scoreRef || null,
        log_ref: logSeerRun({
            confidence: round(confidence, 3),
            confidence_band: confidenceBandValue,
            direction,
            reading,
            prediction,
            supporting_lenses: supporting,
            conflicts,
            key_factors: keyFactors,
            agreement_ratio: agreementRatio,
            primary_branch: primaryBranch,
            fracture_points: fracturePoints,
            verdicts: claimBranches,
            families,
        }),
    };

    return result;
}

export function computeSeer(engineOutputs, gridSignals = {}) {
    return mergeSeer(engineOutputs, gridSignals);
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
        finance: ['size small', 'wait for clean spread', 'do not chase the move'],
        timing: ['too soon burns', 'wait for the next event', 'move on confirmation'],
        relationship: ['keep the edge soft', 'do not force it', 'silence helps'],
        decision: ['choose the clean line', 'do not split the move', 'hold the center'],
        meaning: ['the signal is plain', 'the sign is quiet', 'watch the transition'],
        general: ['keep size small', 'the signal is enough', 'move on confirmation'],
    };
    const hook = pick(personaHooks[focus] || personaHooks.general, seed + 7);
    return `${lead}. ${hook}.`;
}

export function answerPersona({
    personaId,
    question = '',
    seer = null,
    engineOutputs = [],
    lensIds = [],
    mode = 'chorus',
}) {
    const persona = personaFor(personaId);
    const focus = focusFromQuestion(question);
    const seerState = seer || mergeSeer(engineOutputs, {});
    const activeLensIds = toArray(lensIds).map(normalizeId).filter(Boolean);
    const questionLine = String(question || '').trim().slice(0, 180);
    const toneLine = personaToneLine(persona, seerState, focus, questionLine);
    const declaredLens = `${persona.name} / ${persona.lens_mode}`;
    const answer =
        persona.id === 'seer'
            ? `${declaredLens}. ${seerState.reading} ${seerState.prediction} ${toneLine}`
            : `${declaredLens}. ${toneLine} ${seerState.prediction}`;

    const response = {
        persona_id: persona.id,
        persona_name: persona.name,
        declared_lens: declaredLens,
        allowed_lenses: persona.allowed_lenses,
        excluded_lenses: persona.forbidden_lenses,
        source_engine_ids: engineOutputs.map((output) => output.engine_id),
        answer_style: `${persona.tone}/${persona.verbosity}`,
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
            declared_lens: declaredLens,
            allowed_lenses: persona.allowed_lenses,
            excluded_lenses: persona.forbidden_lenses,
            question: questionLine,
            answer,
            seer_log_ref: seerState.log_ref,
        }),
    };

    return response;
}

export function buildPersonaResponse(payload) {
    return answerPersona(payload);
}

export default {
    ENGINE_DEFINITIONS,
    normalizeSkyState,
    extractSkyThreads,
    deriveTraditionFeatures,
    runEngine,
    computeEngineOutputs,
    mergeSeer,
    computeSeer,
    answerPersona,
    buildPersonaResponse,
    labelLens,
    logEngineRun,
    logSeerRun,
    logPersonaRun,
    readRunLog,
};
