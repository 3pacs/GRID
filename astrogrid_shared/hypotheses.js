import {
    normalizeAstrogridAspects,
    normalizeAstrogridLunar,
    normalizeAstrogridNakshatra,
} from './snapshot.js';

function asNumber(value, fallback = null) {
    if (typeof value === 'number' && Number.isFinite(value)) return value;
    if (typeof value === 'string' && value.trim() !== '') {
        const parsed = Number(value);
        return Number.isFinite(parsed) ? parsed : fallback;
    }
    return fallback;
}

function asString(value, fallback = '') {
    return typeof value === 'string' && value.trim() !== '' ? value : fallback;
}

function round(value, digits = 1) {
    const number = asNumber(value, null);
    if (number == null) return null;
    const factor = 10 ** digits;
    return Math.round(number * factor) / factor;
}

function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function daysLabel(value) {
    const days = asNumber(value, null);
    if (days == null) return 'now';
    if (days < 1) return '<1d';
    return `${Math.round(days)}d`;
}

function featureSignal(direction) {
    if (direction > 0) return 'bullish';
    if (direction < 0) return 'bearish';
    return 'neutral';
}

function lunarPhaseState(lunar) {
    const phase = asString(lunar?.phaseName).toLowerCase();
    if (phase.includes('full')) return 'full';
    if (phase.includes('new')) return 'new';
    if (phase.includes('first quarter')) return 'first_quarter';
    if (phase.includes('last quarter')) return 'last_quarter';
    return phase.includes('wax') ? 'waxing' : phase.includes('wan') ? 'waning' : 'mixed';
}

const FEATURE_LABELS = {
    planetary_stress_index: 'Planetary Stress',
    days_to_full_moon: 'Days To Full Moon',
    days_to_new_moon: 'Days To New Moon',
    lunar_eclipse_proximity: 'Lunar Eclipse Proximity',
    solar_eclipse_proximity: 'Solar Eclipse Proximity',
    mercury_retrograde: 'Mercury Retrograde',
    jupiter_saturn_angle: 'Jupiter Saturn Angle',
    mars_volatility_index: 'Mars Volatility',
    venus_cycle_phase: 'Venus Cycle',
    nakshatra_index: 'Nakshatra Index',
    nakshatra_quality: 'Nakshatra Quality',
    tithi: 'Tithi',
    rahu_ketu_axis: 'Rahu Ketu Axis',
    dasha_cycle_phase: 'Dasha Cycle',
    iching_hexagram_of_day: 'I Ching Hexagram',
    geomagnetic_kp_index_recent: 'Geomagnetic Kp',
    solar_wind_speed_recent: 'Solar Wind',
    solar_cycle_phase: 'Solar Cycle',
};

function featureLabel(key) {
    return FEATURE_LABELS[key] || key.replace(/_/g, ' ').replace(/\b\w/g, (s) => s.toUpperCase());
}

function phaseBias(phaseName) {
    const phase = asString(phaseName).toLowerCase();
    if (phase.includes('new')) return 0.2;
    if (phase.includes('first quarter')) return 0.35;
    if (phase.includes('full')) return -0.15;
    if (phase.includes('last quarter')) return -0.25;
    if (phase.includes('wax')) return 0.18;
    if (phase.includes('wan')) return -0.18;
    return 0;
}

function aspectLabel(aspect) {
    return `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`;
}

function nearestAspect(snapshot, predicate) {
    return normalizeAstrogridAspects(snapshot)
        .filter((aspect) => predicate(aspect))
        .sort((a, b) => (a.orb_used ?? 99) - (b.orb_used ?? 99))[0] || null;
}

function eventByName(snapshot, pattern) {
    const events = candidateWindowEvents(snapshot);
    return events.find((event) => pattern.test(String(event.name || event.event || ''))) || null;
}

function eventTime(snapshot, event) {
    const raw = event?.date || event?.datetime || event?.timestamp || snapshot?.date;
    const dt = raw ? new Date(raw) : null;
    return dt && !Number.isNaN(dt.getTime()) ? dt.getTime() : Number.POSITIVE_INFINITY;
}

function eventPhase(snapshot, event) {
    const snapshotTime = eventTime(snapshot, { date: snapshot?.date });
    const targetTime = eventTime(snapshot, event);
    if (!Number.isFinite(snapshotTime) || !Number.isFinite(targetTime)) return 'future';
    const deltaHours = (targetTime - snapshotTime) / 3600000;
    if (Math.abs(deltaHours) <= 18) return 'active';
    if (deltaHours < 0) return 'past';
    return 'future';
}

function eventPhaseVerb(snapshot, event) {
    const phase = eventPhase(snapshot, event);
    if (phase === 'active') return 'through';
    if (phase === 'past') return 'after';
    return 'into';
}

function shiftedIsoDate(value, days) {
    const time = eventTime(null, { date: value });
    if (!Number.isFinite(time)) return null;
    return new Date(time + (asNumber(days, 0) * 86400000)).toISOString().slice(0, 16);
}

function syntheticWindowEvents(snapshot) {
    const lunar = normalizeAstrogridLunar(snapshot);
    const phaseState = lunarPhaseState(lunar);
    const events = [];
    if (snapshot?.date && lunar.phaseName) {
        events.push({
            name: lunar.phaseName,
            date: snapshot.date,
            event: lunar.phaseName,
        });
    }
    if (snapshot?.date && phaseState !== 'full' && Number.isFinite(lunar.daysToFull)) {
        events.push({
            name: 'Next Full Moon',
            date: shiftedIsoDate(snapshot.date, lunar.daysToFull),
            event: 'Next Full Moon',
        });
    }
    if (snapshot?.date && phaseState !== 'new' && Number.isFinite(lunar.daysToNew)) {
        events.push({
            name: 'Next New Moon',
            date: shiftedIsoDate(snapshot.date, lunar.daysToNew),
            event: 'Next New Moon',
        });
    }
    if (snapshot?.void_of_course?.is_void) {
        events.push({
            name: 'Void Of Course',
            date: snapshot?.date,
            event: 'Void Of Course',
        });
    }
    if (snapshot?.nakshatra?.nakshatra_name) {
        events.push({
            name: `Nakshatra ${snapshot.nakshatra.nakshatra_name}`,
            date: snapshot?.date,
            event: `Nakshatra ${snapshot.nakshatra.nakshatra_name}`,
        });
    }
    return events.filter((event) => event?.name && event?.date);
}

function candidateWindowEvents(snapshot) {
    const liveEvents = Array.isArray(snapshot?.events) ? snapshot.events.filter(Boolean) : [];
    const synthetic = syntheticWindowEvents(snapshot);
    const deduped = new Map();
    [...liveEvents, ...synthetic].forEach((event) => {
        const key = `${String(event.name || event.event || '').toLowerCase()}::${String(event.date || event.datetime || event.timestamp || '')}`;
        if (!key.trim()) return;
        if (!deduped.has(key)) deduped.set(key, event);
    });
    return [...deduped.values()];
}

function nextWindowEvent(snapshot) {
    const events = candidateWindowEvents(snapshot);
    return events
        .filter((event) => /(eclipse|full moon|new moon|void|nakshatra|quarter)/i.test(String(event.name || event.event || '')))
        .sort((a, b) => {
            const phaseRank = { active: 0, future: 1, past: 2 };
            const aPhase = eventPhase(snapshot, a);
            const bPhase = eventPhase(snapshot, b);
            if (phaseRank[aPhase] !== phaseRank[bPhase]) return phaseRank[aPhase] - phaseRank[bPhase];
            return eventTime(snapshot, a) - eventTime(snapshot, b);
        })[0] || null;
}

function buildFeatureInterpretation(key, value, snapshot) {
    const lunar = normalizeAstrogridLunar(snapshot);
    const phaseState = lunarPhaseState(lunar);

    switch (key) {
        case 'planetary_stress_index': {
            const level = asNumber(value, 0);
            if (level >= 6) return { text: 'hard geometry high', signal: 'bearish' };
            if (level >= 4) return { text: 'pressure elevated', signal: 'bearish' };
            if (level <= 2) return { text: 'geometry quiet', signal: 'bullish' };
            return { text: 'pressure mixed', signal: 'neutral' };
        }
        case 'days_to_full_moon': {
            const days = asNumber(value, null);
            if (days == null) return null;
            if (phaseState === 'full') return { text: 'full moon active', signal: 'neutral' };
            if (days < 1) return { text: 'full moon now', signal: 'neutral' };
            if (days <= 2) return { text: 'full moon imminent', signal: 'neutral' };
            if (days <= 7) return { text: `full moon in ${daysLabel(days)}`, signal: 'bullish' };
            return { text: `full moon in ${daysLabel(days)}`, signal: 'neutral' };
        }
        case 'days_to_new_moon': {
            const days = asNumber(value, null);
            if (days == null) return null;
            if (phaseState === 'new') return { text: 'new moon active', signal: 'neutral' };
            if (days < 1) return { text: 'dark moon now', signal: 'neutral' };
            if (days <= 2) return { text: 'dark moon imminent', signal: 'bearish' };
            if (days <= 7) return { text: `new moon in ${daysLabel(days)}`, signal: 'neutral' };
            return { text: `new moon in ${daysLabel(days)}`, signal: 'neutral' };
        }
        case 'lunar_eclipse_proximity':
        case 'solar_eclipse_proximity': {
            const days = asNumber(value, null);
            if (days == null) return null;
            if (days <= 14) return { text: 'eclipse season active', signal: 'bearish' };
            if (days <= 45) return { text: 'eclipse season near', signal: 'neutral' };
            return { text: `${daysLabel(days)} to eclipse`, signal: 'neutral' };
        }
        case 'mercury_retrograde':
            return asNumber(value, 0) > 0
                ? { text: 'Mercury drag active', signal: 'bearish' }
                : { text: 'Mercury direct', signal: 'bullish' };
        case 'geomagnetic_kp_index_recent': {
            const kp = asNumber(value, null);
            if (kp == null) return null;
            if (kp >= 5) return { text: `storm pressure ${round(kp)}`, signal: 'bearish' };
            if (kp >= 4) return { text: `solar weather active ${round(kp)}`, signal: 'neutral' };
            return { text: `solar weather quiet ${round(kp)}`, signal: 'bullish' };
        }
        case 'solar_wind_speed_recent': {
            const speed = asNumber(value, null);
            if (speed == null) return null;
            if (speed >= 500) return { text: `${Math.round(speed)} km/s fast`, signal: 'bearish' };
            if (speed >= 400) return { text: `${Math.round(speed)} km/s building`, signal: 'neutral' };
            return { text: `${Math.round(speed)} km/s moderate`, signal: 'bullish' };
        }
        case 'venus_cycle_phase': {
            const phase = asNumber(value, null);
            if (phase == null) return null;
            if (phase < 0.18) return { text: 'venus reset', signal: 'neutral' };
            if (phase < 0.5) return { text: 'venus ascent', signal: 'bullish' };
            if (phase < 0.82) return { text: 'venus high', signal: 'bullish' };
            return { text: 'venus decline', signal: 'neutral' };
        }
        case 'jupiter_saturn_angle': {
            const angle = asNumber(value, null);
            if (angle == null) return null;
            return { text: `${round(angle)} deg order/growth`, signal: 'neutral' };
        }
        case 'nakshatra_quality': {
            const quality = asNumber(value, null);
            if (quality === 0) return { text: 'fixed mansion', signal: 'neutral' };
            if (quality === 1) return { text: 'movable mansion', signal: 'bullish' };
            if (quality === 2) return { text: 'dual mansion', signal: 'neutral' };
            return null;
        }
        case 'tithi': {
            const tithi = asNumber(value, null);
            if (tithi == null) return null;
            return { text: `tithi ${Math.round(tithi)}`, signal: featureSignal(phaseBias(lunar.phaseName)) };
        }
        case 'iching_hexagram_of_day': {
            const hex = asNumber(value, null);
            if (hex == null) return null;
            return { text: `hexagram ${Math.round(hex)}`, signal: 'neutral' };
        }
        default:
            return null;
    }
}

function featureValueDisplay(key, value) {
    const numeric = asNumber(value, null);
    if (numeric == null) return asString(value, 'n/a');
    switch (key) {
        case 'venus_cycle_phase':
        case 'solar_cycle_phase':
        case 'lunar_phase':
            return `${round(numeric * 100)}%`;
        case 'geomagnetic_kp_index_recent':
        case 'jupiter_saturn_angle':
        case 'solar_wind_speed_recent':
            return `${round(numeric)}`;
        default:
            return String(round(numeric, Math.abs(numeric) >= 100 ? 0 : 2));
    }
}

function selectedFeatureKeys(featureMap) {
    const preferred = [
        'planetary_stress_index',
        'days_to_full_moon',
        'days_to_new_moon',
        'geomagnetic_kp_index_recent',
        'solar_wind_speed_recent',
        'lunar_eclipse_proximity',
        'solar_eclipse_proximity',
        'venus_cycle_phase',
        'jupiter_saturn_angle',
        'tithi',
        'iching_hexagram_of_day',
    ];
    return preferred.filter((key) => Object.prototype.hasOwnProperty.call(featureMap, key));
}

export function buildCelestialFeatureRows(snapshot) {
    const featureMap = snapshot?.local_features || {};
    const rows = selectedFeatureKeys(featureMap)
        .map((key) => {
            const interpretation = buildFeatureInterpretation(key, featureMap[key], snapshot);
            return {
                key,
                name: key,
                display_name: featureLabel(key),
                value: featureMap[key],
                display: featureValueDisplay(key, featureMap[key]),
                interpretation: interpretation?.text || 'raw value',
                signal: interpretation?.signal || 'neutral',
            };
        });

    if (snapshot?.void_of_course?.is_void) {
        rows.unshift({
            key: 'void_of_course',
            name: 'void_of_course',
            display_name: 'Void Of Course',
            value: 1,
            display: snapshot.void_of_course.current_sign || 'active',
            interpretation: `moon void in ${snapshot.void_of_course.current_sign || 'current sign'}`,
            signal: 'bearish',
        });
    }

    const nakshatra = normalizeAstrogridNakshatra(snapshot);
    if (nakshatra.name && nakshatra.name !== 'Unknown') {
        rows.push({
            key: 'nakshatra',
            name: 'nakshatra',
            display_name: 'Nakshatra',
            value: nakshatra.index,
            display: nakshatra.name,
            interpretation: `${nakshatra.quality} / pada ${nakshatra.pada}`,
            signal: nakshatra.quality === 'movable' ? 'bullish' : 'neutral',
        });
    }

    return rows.slice(0, 8);
}

function pushCard(cards, card) {
    if (!card || !card.title || !card.act) return;
    cards.push(card);
}

function marketPolarity(value) {
    const token = asString(value).toLowerCase();
    if (!token) return 0;
    if (/(growth|bull|risk[_ -]?on|easing|inflow|open|expansion)/.test(token)) return 1;
    if (/(crisis|fragile|bear|risk[_ -]?off|tight|tightening|outflow|defensive)/.test(token)) return -1;
    return 0;
}

function formatCompactUsd(value) {
    const amount = asNumber(value, null);
    if (amount == null) return 'n/a';
    const absolute = Math.abs(amount);
    if (absolute >= 1e12) return `${amount < 0 ? '-' : ''}$${round(absolute / 1e12, 2)}T`;
    if (absolute >= 1e9) return `${amount < 0 ? '-' : ''}$${round(absolute / 1e9, 2)}B`;
    if (absolute >= 1e6) return `${amount < 0 ? '-' : ''}$${round(absolute / 1e6, 2)}M`;
    return `${amount < 0 ? '-' : ''}$${round(absolute, 0)}`;
}

function marketRegimeCard(overlay, snapshot) {
    const regime = overlay?.regime;
    const thesis = overlay?.thesis;
    if (!regime && !thesis) return null;

    const regimeBias = marketPolarity(regime?.state);
    const thesisBias = marketPolarity(thesis?.overallDirection);
    const biasScore = thesisBias || regimeBias;
    const driver = (thesis?.keyDrivers || regime?.drivers || [])[0];
    const stateLabel = asString(regime?.state || thesis?.overallDirection, 'NEUTRAL').toLowerCase().replace(/_/g, ' ');

    return {
        sigil: '⌁',
        title: 'regime gate',
        bias: biasScore > 0 ? 'press' : biasScore < 0 ? 'hedge' : 'wait',
        window: regime?.asOf || thesis?.generatedAt || snapshot?.date || 'now',
        act: biasScore > 0 ? 'press only with tape confirmation' : biasScore < 0 ? 'protect until the state turns' : 'wait for alignment',
        cue: driver?.label ? `${stateLabel} / ${driver.label}` : stateLabel,
        confidence: Math.max(asNumber(regime?.confidence, 0), asNumber(thesis?.conviction, 0), 0.58),
    };
}

function flowCard(overlay, snapshot) {
    const topSector = overlay?.sectorFlows?.bySector?.[0] || null;
    const topLever = overlay?.moneyMap?.levers?.[0] || null;
    const topFlow = overlay?.moneyMap?.flows?.slice().sort((a, b) => Math.abs(b.volume || 0) - Math.abs(a.volume || 0))[0] || null;
    if (!topSector && !topLever && !topFlow) return null;

    const netFlow = asNumber(topSector?.netFlow, topFlow?.volume ?? 0);
    const direction = netFlow > 0 ? 'press' : netFlow < 0 ? 'hedge' : 'probe';
    const cue = topSector
        ? `${topSector.sector} / ${formatCompactUsd(topSector.netFlow)}`
        : topLever
            ? `${topLever.label} / ${topLever.detail || 'impact active'}`
            : `${topFlow.label} / ${formatCompactUsd(topFlow.volume)}`;

    return {
        sigil: '⟠',
        title: 'flow bias',
        bias: direction,
        window: overlay?.moneyMap?.asOf || snapshot?.date || 'now',
        act: direction === 'press' ? 'follow the dominant inflow' : direction === 'hedge' ? 'fade the drain' : 'probe the handoff',
        cue,
        confidence: Math.max(asNumber(topLever?.impactScore, 0), Math.min(Math.abs(netFlow) / 1e9, 0.82), 0.56),
    };
}

function scorecardCard(overlay, snapshot) {
    const scorecard = overlay?.scorecard;
    if (!scorecard?.summary || !scorecard?.leaders?.length) return null;

    const composite = asNumber(scorecard.summary.compositeScore, 0);
    const leader = scorecard.leaders[0] || null;
    const laggard = scorecard.laggards?.[0] || null;
    const macro = (scorecard.groups || []).find((group) => group.id === 'macro') || null;
    const crypto = (scorecard.groups || []).find((group) => group.id === 'crypto') || null;
    const bias = composite >= 0.18 ? 'press' : composite <= -0.18 ? 'hedge' : 'wait';

    return {
        sigil: '⟐',
        title: 'hybrid tape',
        bias,
        window: scorecard.generatedAt || snapshot?.date || 'now',
        act: bias === 'press'
            ? `lean ${leader?.symbol || 'strength'} while macro and crypto stay aligned`
            : bias === 'hedge'
                ? `protect until ${laggard?.symbol || 'the weak tape'} stops bleeding`
                : 'wait for basket alignment',
        cue: [
            leader ? `${leader.symbol} ${leader.trend}` : null,
            laggard ? `weak ${laggard.symbol}` : null,
            macro ? `macro ${macro.bias}` : null,
            crypto ? `crypto ${crypto.bias}` : null,
        ].filter(Boolean).join(' / '),
        confidence: clamp(0.56 + Math.abs(composite) * 0.22 + Math.min(asNumber(scorecard.summary.coverageRatio, 0), 1) * 0.12, 0.55, 0.9),
    };
}

function patternCard(overlay, snapshot) {
    const pattern = (overlay?.activePatterns || []).find((item) => item.actionable) || overlay?.activePatterns?.[0];
    if (!pattern) return null;
    return {
        sigil: '⋔',
        title: 'pattern wake',
        bias: pattern.actionable ? 'probe' : 'watch',
        window: pattern.nextExpected || snapshot?.date || 'now',
        act: pattern.actionable ? 'probe the next step only if it prints' : 'watch for the next step',
        cue: `${pattern.ticker ? `${pattern.ticker} / ` : ''}${pattern.pattern}`,
        confidence: Math.max(asNumber(pattern.confidence, 0), asNumber(pattern.hitRate, 0), 0.54),
    };
}

function truthCard(overlay, snapshot) {
    const redFlag = overlay?.crossReference?.redFlags?.[0] || null;
    if (!redFlag) return null;
    return {
        sigil: '⟁',
        title: 'truth tear',
        bias: 'hedge',
        window: overlay?.crossReference?.generatedAt || snapshot?.date || 'now',
        act: 'respect the contradiction before sizing up',
        cue: redFlag.category ? `${redFlag.category} / ${redFlag.label}` : redFlag.label,
        confidence: 0.72,
    };
}

function lunarCard(snapshot) {
    const lunar = normalizeAstrogridLunar(snapshot);
    const fullEvent = eventByName(snapshot, /full moon/i);
    const newEvent = eventByName(snapshot, /new moon/i);
    const phase = lunar.phaseName.toLowerCase();

    if (phase.includes('first quarter') || (phase.includes('wax') && lunar.daysToFull <= 7)) {
        return {
            sigil: '◔',
            title: 'waxing hinge',
            bias: 'press',
            window: fullEvent?.date || `${daysLabel(lunar.daysToFull)} to full`,
            act: 'press only on follow-through',
            cue: fullEvent?.name || lunar.phaseName,
            confidence: 0.74,
        };
    }

    if (phase.includes('full')) {
        return {
            sigil: '◕',
            title: 'full swell',
            bias: 'trim',
            window: fullEvent?.date || 'now',
            act: 'trim strength into the peak',
            cue: lunar.phaseName,
            confidence: 0.72,
        };
    }

    if (lunar.daysToNew <= 7) {
        return {
            sigil: '●',
            title: 'dark seam',
            bias: 'wait',
            window: newEvent?.date || `${daysLabel(lunar.daysToNew)} to new`,
            act: 'reset into the dark',
            cue: newEvent?.name || 'New Moon',
            confidence: 0.69,
        };
    }

    return {
        sigil: '◑',
        title: phase.includes('wan') ? 'waning cut' : 'lunar drift',
        bias: phase.includes('wan') ? 'hedge' : 'hold',
        window: `${daysLabel(Math.min(lunar.daysToFull || 99, lunar.daysToNew || 99))}`,
        act: phase.includes('wan') ? 'reduce on pops' : 'hold until the next hinge',
        cue: lunar.phaseName,
        confidence: 0.6,
    };
}

function saturnCard(snapshot) {
    const aspect = nearestAspect(snapshot, (item) => /saturn/i.test(`${item.planet1} ${item.planet2}`));
    if (!aspect) return null;
    const supportive = ['sextile', 'trine', 'conjunction'].includes(aspect.aspect_type);
    return {
        sigil: '♄',
        title: supportive ? 'saturn seal' : 'saturn wall',
        bias: supportive ? 'build' : 'hedge',
        window: snapshot?.date || 'now',
        act: supportive ? 'favor structure over chase' : 'respect the block',
        cue: `${aspectLabel(aspect)} / ${round(aspect.orb_used, 2)} deg`,
        confidence: clamp(0.58 + ((1 - Math.min(asNumber(aspect.orb_used, 6), 6) / 6) * 0.22), 0.55, 0.88),
    };
}

function voidCard(snapshot) {
    const voidState = snapshot?.void_of_course;
    if (!voidState?.is_void) return null;
    return {
        sigil: '◌',
        title: 'void seam',
        bias: 'wait',
        window: voidState.next_sign_entry || snapshot?.date || 'now',
        act: 'wait for ingress',
        cue: `moon void in ${voidState.current_sign || 'current sign'}`,
        confidence: 0.81,
    };
}

function nakshatraCard(snapshot) {
    const nakshatra = normalizeAstrogridNakshatra(snapshot);
    if (!nakshatra.name || nakshatra.name === 'Unknown') return null;

    let bias = 'hold';
    let act = 'hold the line';
    if (nakshatra.quality === 'movable') {
        bias = 'probe';
        act = 'trade quick, do not marry';
    } else if (nakshatra.quality === 'dual') {
        bias = 'pair';
        act = 'pair strength with cover';
    }

    return {
        sigil: '☽',
        title: `${nakshatra.name.toLowerCase()} knot`,
        bias,
        window: snapshot?.date || 'now',
        act,
        cue: `${nakshatra.quality} / pada ${nakshatra.pada}`,
        confidence: 0.66,
    };
}

function eclipseCard(snapshot) {
    const featureMap = snapshot?.local_features || {};
    const lunarDays = asNumber(featureMap.lunar_eclipse_proximity, null);
    const solarDays = asNumber(featureMap.solar_eclipse_proximity, null);
    const nearest = [lunarDays, solarDays].filter((value) => value != null).sort((a, b) => a - b)[0];
    if (nearest == null || nearest > 45) return null;
    return {
        sigil: '☍',
        title: 'eclipse perimeter',
        bias: nearest <= 14 ? 'hedge' : 'watch',
        window: `${daysLabel(nearest)}`,
        act: nearest <= 14 ? 'reduce conviction' : 'keep optionality',
        cue: nearest <= 14 ? 'season active' : 'season near',
        confidence: nearest <= 14 ? 0.83 : 0.64,
    };
}

function solarCard(snapshot) {
    const featureMap = snapshot?.local_features || {};
    const kp = asNumber(featureMap.geomagnetic_kp_index_recent, null);
    const wind = asNumber(featureMap.solar_wind_speed_recent, null);
    if (kp == null && wind == null) return null;

    if ((kp != null && kp >= 4) || (wind != null && wind >= 500)) {
        return {
            sigil: '☉',
            title: 'solar static',
            bias: 'hedge',
            window: snapshot?.date || 'now',
            act: 'expect interference',
            cue: kp != null ? `kp ${round(kp)}` : `${Math.round(wind)} km/s`,
            confidence: 0.62,
        };
    }

    return {
        sigil: '☉',
        title: 'solar quiet',
        bias: 'hold',
        window: snapshot?.date || 'now',
        act: 'use cleaner setups',
        cue: kp != null ? `kp ${round(kp)}` : `${Math.round(wind)} km/s`,
        confidence: 0.55,
    };
}

function seerCard(seer, snapshot) {
    if (!seer) return null;
    const event = nextWindowEvent(snapshot);
    const prediction = asString(seer.prediction, 'wait').replace(/\.$/, '');
    const relation = eventPhaseVerb(snapshot, event);
    const eventLabel = String(event?.name || event?.event || 'window').toLowerCase();
    return {
        sigil: '⟡',
        title: 'seer cut',
        bias: seer.signal_bias > 0.22 ? 'press' : seer.signal_bias < -0.22 ? 'hedge' : 'wait',
        window: event?.date || snapshot?.date || 'now',
        act: event ? `${prediction} ${relation} ${eventLabel}` : prediction,
        cue: event ? `${event.name || event.event} / ${(seer.key_factors || []).slice(0, 2).join(' / ')}` : (seer.key_factors || []).slice(0, 2).join(' / ') || 'mixed field',
        confidence: asNumber(seer.confidence, 0.6),
    };
}

export function buildAstrogridHypotheses(snapshot, seer = null, overlay = null) {
    if (!snapshot) return [];
    const cards = [];
    pushCard(cards, marketRegimeCard(overlay, snapshot));
    pushCard(cards, flowCard(overlay, snapshot));
    pushCard(cards, scorecardCard(overlay, snapshot));
    pushCard(cards, patternCard(overlay, snapshot));
    pushCard(cards, truthCard(overlay, snapshot));
    pushCard(cards, seerCard(seer, snapshot));
    pushCard(cards, lunarCard(snapshot));
    pushCard(cards, saturnCard(snapshot));
    pushCard(cards, voidCard(snapshot));
    pushCard(cards, nakshatraCard(snapshot));
    pushCard(cards, eclipseCard(snapshot));
    pushCard(cards, solarCard(snapshot));

    return cards
        .filter(Boolean)
        .sort((a, b) => (b.confidence || 0) - (a.confidence || 0))
        .slice(0, 5);
}
