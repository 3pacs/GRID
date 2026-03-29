export const WORLD_SCALES = {
    heliocentric: 'heliocentric',
    earthSystem: 'earth_system',
    cislunar: 'cislunar',
    martian: 'martian',
    asset: 'asset',
};

export const WORLD_NODE_TYPES = {
    star: 'star',
    planet: 'planet',
    moon: 'moon',
    orbit: 'orbit',
    surface: 'surface',
    corridor: 'corridor',
    satellite: 'satellite',
    constellation: 'constellation',
    groundStation: 'ground_station',
    market: 'market',
    asset: 'asset',
};

export const WORLD_EDGE_TYPES = {
    capital: 'capital',
    mass: 'mass',
    telemetry: 'telemetry',
    compute: 'compute',
    policy: 'policy',
    corridor: 'corridor',
};

export const WORLD_RENDER_ENGINES = {
    globe: 'cesium',
    local3d: 'react-three-fiber',
    earthVector: 'maplibre',
};

export const WORLD_BODY_PRESETS = {
    sun: {
        id: 'sun',
        name: 'Sun',
        type: WORLD_NODE_TYPES.star,
        scale: WORLD_SCALES.heliocentric,
    },
    earth: {
        id: 'earth',
        name: 'Earth',
        type: WORLD_NODE_TYPES.planet,
        scale: WORLD_SCALES.earthSystem,
        ellipsoid: 'WGS84',
        renderEngine: WORLD_RENDER_ENGINES.globe,
        vectorOverlay: WORLD_RENDER_ENGINES.earthVector,
    },
    moon: {
        id: 'moon',
        name: 'Moon',
        type: WORLD_NODE_TYPES.moon,
        scale: WORLD_SCALES.cislunar,
        ellipsoid: 'IAU2015_Moon',
        renderEngine: WORLD_RENDER_ENGINES.globe,
    },
    mars: {
        id: 'mars',
        name: 'Mars',
        type: WORLD_NODE_TYPES.planet,
        scale: WORLD_SCALES.martian,
        ellipsoid: 'IAU2015_Mars',
        renderEngine: WORLD_RENDER_ENGINES.globe,
    },
};

export const WORLD_LAYER_STACK = [
    'astronomy',
    'terrain',
    'imagery',
    'vector',
    'orbits',
    'assets',
    'flows',
    'signals',
    'seer',
];

export function createWorldNode(config) {
    return {
        id: config.id,
        name: config.name,
        type: config.type,
        scale: config.scale,
        parentId: config.parentId || null,
        ellipsoid: config.ellipsoid || null,
        renderEngine: config.renderEngine || WORLD_RENDER_ENGINES.local3d,
        vectorOverlay: config.vectorOverlay || null,
        tags: Array.isArray(config.tags) ? config.tags : [],
        metrics: config.metrics || {},
        meta: config.meta || {},
    };
}

export function createWorldEdge(config) {
    return {
        id: config.id,
        source: config.source,
        target: config.target,
        type: config.type,
        scale: config.scale,
        quantityKind: config.quantityKind || null,
        unit: config.unit || null,
        value: config.value ?? null,
        currency: config.currency || null,
        horizon: config.horizon || null,
        confidence: config.confidence ?? null,
        metrics: config.metrics || {},
        meta: config.meta || {},
    };
}

export function buildSeedWorldModel() {
    // Minimal cathedral. If this turns into a theme park, delete features until it stops blinking.
    const nodes = [
        createWorldNode(WORLD_BODY_PRESETS.sun),
        createWorldNode(WORLD_BODY_PRESETS.earth),
        createWorldNode(WORLD_BODY_PRESETS.moon),
        createWorldNode(WORLD_BODY_PRESETS.mars),
        createWorldNode({
            id: 'earth_surface',
            name: 'Earth Surface',
            type: WORLD_NODE_TYPES.surface,
            scale: WORLD_SCALES.earthSystem,
            parentId: 'earth',
            renderEngine: WORLD_RENDER_ENGINES.earthVector,
            tags: ['ports', 'launch-sites', 'ground-stations', 'capex'],
        }),
        createWorldNode({
            id: 'leo',
            name: 'Low Earth Orbit',
            type: WORLD_NODE_TYPES.orbit,
            scale: WORLD_SCALES.earthSystem,
            parentId: 'earth',
            tags: ['satellites', 'launch', 'defense', 'telemetry'],
        }),
        createWorldNode({
            id: 'geo',
            name: 'Geostationary Orbit',
            type: WORLD_NODE_TYPES.orbit,
            scale: WORLD_SCALES.earthSystem,
            parentId: 'earth',
            tags: ['comms', 'infra'],
        }),
        createWorldNode({
            id: 'cislunar_space',
            name: 'Cislunar Space',
            type: WORLD_NODE_TYPES.corridor,
            scale: WORLD_SCALES.cislunar,
            parentId: 'earth',
            tags: ['transfer', 'lunar-logistics'],
        }),
        createWorldNode({
            id: 'lunar_surface',
            name: 'Lunar Surface',
            type: WORLD_NODE_TYPES.surface,
            scale: WORLD_SCALES.cislunar,
            parentId: 'moon',
            tags: ['bases', 'resources', 'power'],
        }),
        createWorldNode({
            id: 'mars_surface',
            name: 'Mars Surface',
            type: WORLD_NODE_TYPES.surface,
            scale: WORLD_SCALES.martian,
            parentId: 'mars',
            tags: ['research', 'settlement', 'power'],
        }),
    ];

    const edges = [
        createWorldEdge({
            id: 'flow_earth_surface_leo_capital',
            source: 'earth_surface',
            target: 'leo',
            type: WORLD_EDGE_TYPES.capital,
            scale: WORLD_SCALES.earthSystem,
            quantityKind: 'capital',
            currency: 'USD',
            meta: { label: 'launch capex' },
        }),
        createWorldEdge({
            id: 'flow_leo_earth_surface_telemetry',
            source: 'leo',
            target: 'earth_surface',
            type: WORLD_EDGE_TYPES.telemetry,
            scale: WORLD_SCALES.earthSystem,
            quantityKind: 'bandwidth',
            unit: 'Gbps',
            meta: { label: 'satellite downlink' },
        }),
        createWorldEdge({
            id: 'flow_earth_surface_cislunar_capital',
            source: 'earth_surface',
            target: 'cislunar_space',
            type: WORLD_EDGE_TYPES.capital,
            scale: WORLD_SCALES.cislunar,
            quantityKind: 'capital',
            currency: 'USD',
            meta: { label: 'cislunar logistics capex' },
        }),
        createWorldEdge({
            id: 'flow_cislunar_lunar_surface_mass',
            source: 'cislunar_space',
            target: 'lunar_surface',
            type: WORLD_EDGE_TYPES.mass,
            scale: WORLD_SCALES.cislunar,
            quantityKind: 'mass',
            unit: 'kg',
            meta: { label: 'surface payload' },
        }),
        createWorldEdge({
            id: 'flow_earth_surface_mars_surface_capital',
            source: 'earth_surface',
            target: 'mars_surface',
            type: WORLD_EDGE_TYPES.capital,
            scale: WORLD_SCALES.martian,
            quantityKind: 'capital',
            currency: 'USD',
            meta: { label: 'mars program burn' },
        }),
    ];

    return {
        nodes,
        edges,
        layerStack: WORLD_LAYER_STACK,
        engines: WORLD_RENDER_ENGINES,
    };
}

function asNumber(value, fallback = null) {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : fallback;
}

function clamp(value, min, max) {
    return Math.min(max, Math.max(min, value));
}

function featureMap(snapshot) {
    return {
        ...(snapshot?.derived || {}),
        ...(snapshot?.local_features || {}),
    };
}

function objectById(snapshot, objectId) {
    const target = String(objectId || '').toLowerCase();
    const list = Array.isArray(snapshot?.objects)
        ? snapshot.objects
        : Array.isArray(snapshot?.bodies)
            ? snapshot.bodies
            : [];
    return list.find((item) => String(item?.id || item?.name || '').toLowerCase() === target) || null;
}

function firstEvent(snapshot, pattern) {
    const events = Array.isArray(snapshot?.events) ? snapshot.events : [];
    return events.find((event) => pattern.test(String(event.name || event.event || ''))) || null;
}

function flowState(score) {
    if (score >= 0.3) return 'open';
    if (score <= -0.3) return 'tight';
    return 'mixed';
}

function windowLabel(days) {
    const value = asNumber(days, null);
    if (value == null) return 'now';
    if (value < 1) return '<1d';
    return `${Math.round(value)}d`;
}

function metricScore(value) {
    return Math.round((asNumber(value, 0) || 0) * 100) / 100;
}

function marketPolarity(value) {
    const token = String(value || '').toLowerCase();
    if (!token) return 0;
    if (/(growth|bull|risk[_ -]?on|easing|inflow|open|expansion)/.test(token)) return 1;
    if (/(crisis|fragile|bear|risk[_ -]?off|tight|tightening|outflow|defensive)/.test(token)) return -1;
    return 0;
}

function normalizeConfidence(value) {
    const numeric = asNumber(value, 0);
    if (numeric > 1) {
        return clamp(numeric / 100, 0, 1);
    }
    return clamp(numeric, 0, 1);
}

function compactUsd(value) {
    const amount = asNumber(value, null);
    if (amount == null) return 'n/a';
    const absolute = Math.abs(amount);
    if (absolute >= 1e12) return `${amount < 0 ? '-' : ''}$${(absolute / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) return `${amount < 0 ? '-' : ''}$${(absolute / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${amount < 0 ? '-' : ''}$${(absolute / 1e6).toFixed(2)}M`;
    return `${amount < 0 ? '-' : ''}$${absolute.toFixed(0)}`;
}

function resolveSectorProfile(marketOverlay, sectorName) {
    if (!sectorName) return null;
    return marketOverlay?.sectorMap?.byName?.[sectorName] || null;
}

function marketOverlayState(marketOverlay) {
    if (!marketOverlay) {
        return {
            regimeBias: 0,
            thesisBias: 0,
            conviction: 0,
            liquidityBias: 0,
            topSector: null,
            topLever: null,
            topPattern: null,
            topRedFlag: null,
        };
    }

    const regimeBias = marketPolarity(marketOverlay.regime?.state);
    const thesisBias = marketPolarity(marketOverlay.thesis?.overallDirection);
    const liquidityChange = asNumber(marketOverlay.moneyMap?.globalLiquidity?.change_1m_usd, 0);
    const liquidityBias = clamp(liquidityChange / 5e11, -1, 1);
    const topSector = marketOverlay.sectorFlows?.bySector?.[0] || null;
    const topSectorProfile = resolveSectorProfile(marketOverlay, topSector?.sector);
    const topLever = marketOverlay.moneyMap?.levers?.[0] || null;
    const topPattern = (marketOverlay.activePatterns || []).find((item) => item.actionable) || marketOverlay.activePatterns?.[0] || null;
    const topRedFlag = marketOverlay.crossReference?.redFlags?.[0] || null;
    const topLeader = marketOverlay.scorecard?.leaders?.[0] || null;
    const topLaggard = marketOverlay.scorecard?.laggards?.[0] || null;
    const hybridScore = asNumber(marketOverlay.scorecard?.summary?.compositeScore, 0);

    return {
        regimeBias,
        thesisBias,
        conviction: Math.max(
            normalizeConfidence(marketOverlay.regime?.confidence),
            normalizeConfidence(marketOverlay.thesis?.conviction),
        ),
        liquidityBias,
        topSector,
        topSectorProfile,
        topLever,
        topPattern,
        topRedFlag,
        topLeader,
        topLaggard,
        hybridScore,
    };
}

function shiftedIsoDate(value, days) {
    const dt = value ? new Date(value) : null;
    if (!dt || Number.isNaN(dt.getTime())) return null;
    return new Date(dt.getTime() + (asNumber(days, 0) * 86400000)).toISOString().slice(0, 16);
}

export function enrichWorldModel(worldModel, snapshot, seer = null, marketOverlay = null) {
    const world = worldModel || buildSeedWorldModel();
    if (!snapshot) return world;

    const features = featureMap(snapshot);
    const kp = asNumber(features.geomagnetic_kp_index_recent, 0);
    const wind = asNumber(features.solar_wind_speed_recent, 0);
    const solarPressure = clamp((Math.max(0, kp - 2) / 4) + (Math.max(0, wind - 360) / 240), 0, 1);
    const daysToFull = asNumber(features.days_to_full_moon, null);
    const daysToNew = asNumber(features.days_to_new_moon, null);
    const eclipseDistance = [features.lunar_eclipse_proximity, features.solar_eclipse_proximity]
        .map((value) => asNumber(value, null))
        .filter((value) => value != null)
        .sort((a, b) => a - b)[0] ?? null;
    const hardCount = asNumber(features.hard_aspect_count ?? features.planetary_stress_index, 0);
    const softCount = asNumber(features.soft_aspect_count, 0);
    const retrogradeCount = Array.isArray(snapshot?.retrograde_planets)
        ? snapshot.retrograde_planets.length
        : (Array.isArray(snapshot?.objects) ? snapshot.objects.filter((item) => item?.retrograde).length : 0);
    const moonPhase = snapshot?.lunar?.phase_name || snapshot?.lunar?.phaseName || 'moon phase';
    const moonIllumination = asNumber(snapshot?.lunar?.illumination ?? features.lunar_illumination, null);
    const voidState = snapshot?.void_of_course?.is_void ? snapshot.void_of_course : null;
    const dominantElement = snapshot?.derived?.dominant_element || 'mixed';
    const nakshatra = snapshot?.nakshatra?.nakshatra_name || snapshot?.nakshatra?.name || 'unknown mansion';
    const tithi = asNumber(features.tithi, null);
    const signalBias = asNumber(seer?.signal_bias, 0);
    const market = marketOverlayState(marketOverlay);
    const structuralBalance = clamp((softCount * 0.045) - (hardCount * 0.055) - (retrogradeCount * 0.06), -1, 1);
    const marketBias = (((market.regimeBias * 0.55) + (market.thesisBias * 0.45)) * Math.max(market.conviction, 0.35)) + (market.hybridScore * 0.18);
    const sectorBias = clamp(asNumber(market.topSector?.netFlow, 0) / 2e10, -0.45, 0.45);
    const launchScore = signalBias + structuralBalance - solarPressure * 0.55 - (voidState ? 0.22 : 0) + (marketBias * 0.32) + (market.liquidityBias * 0.18);
    const cislunarScore = (daysToFull != null && daysToFull <= 7 ? 0.28 : 0) - (eclipseDistance != null && eclipseDistance <= 14 ? 0.42 : 0) - (voidState ? 0.2 : 0) + (marketBias * 0.18);
    const lunarScore = (moonIllumination != null ? (moonIllumination / 100) - 0.45 : 0) - (voidState ? 0.25 : 0) + (marketBias * 0.1);
    const mars = objectById(snapshot, 'mars');
    const marsScore = (mars?.retrograde ? -0.45 : 0.14) + Math.max(0, signalBias) * 0.2 + (sectorBias * 0.35);
    const nextFull = firstEvent(snapshot, /full moon/i);
    const nextNew = firstEvent(snapshot, /new moon/i);
    const phaseName = String(moonPhase || '').toLowerCase();
    const nextFullDate = nextFull?.date || (phaseName.includes('full') ? snapshot?.date : shiftedIsoDate(snapshot?.date, daysToFull));
    const nextNewDate = nextNew?.date || (phaseName.includes('new') ? snapshot?.date : shiftedIsoDate(snapshot?.date, daysToNew));
    const regimeLabel = marketOverlay?.regime?.state ? String(marketOverlay.regime.state).toLowerCase().replace(/_/g, ' ') : null;
    const topSectorLabel = market.topSector?.sector ? `${market.topSector.sector} ${market.topSector.netFlow >= 0 ? 'bid' : 'drain'}` : null;
    const topSectorFlow = market.topSector ? compactUsd(market.topSector.netFlow) : null;
    const topSectorActor = market.topSectorProfile?.topActor?.name || null;
    const topLeverLabel = market.topLever?.label || null;
    const topPatternLabel = market.topPattern?.pattern || null;
    const redFlagLabel = market.topRedFlag?.label || null;

    const nodeMetrics = {
        sun: {
            headline: solarPressure >= 0.55 ? 'solar static' : 'solar quiet',
            detail: redFlagLabel ? `truth tear / ${redFlagLabel}` : `kp ${kp.toFixed(1)} / ${Math.round(wind)} km/s`,
            signal: flowState(0.22 - solarPressure),
            score: metricScore(0.22 - solarPressure),
            window: 'now',
        },
        earth: {
            headline: regimeLabel || `${dominantElement} field`,
            detail: topLeverLabel || (market.topLeader ? `${market.topLeader.symbol} leads / ${market.topLeader.trend}` : `${hardCount} hard / ${softCount} soft / ${retrogradeCount} retro`),
            signal: flowState(signalBias - hardCount * 0.08 + marketBias * 0.22),
            score: metricScore(signalBias - hardCount * 0.08 + marketBias * 0.22),
            window: snapshot?.date || 'now',
        },
        earth_surface: {
            headline: `launch ${flowState(launchScore)}`,
            detail: topSectorLabel
                ? `${topSectorLabel} / ${topSectorActor || topSectorFlow || 'flow active'}`
                : market.topLeader
                    ? `${market.topLeader.symbol} ${market.topLeader.trend} / ${market.topLaggard?.symbol || 'weak tape'}`
                    : (voidState ? `void in ${voidState.current_sign || 'current sign'}` : `seer bias ${signalBias.toFixed(2)}`),
            signal: flowState(launchScore),
            score: metricScore(launchScore),
            window: nextNewDate || nextFullDate || snapshot?.date || 'now',
        },
        leo: {
            headline: solarPressure >= 0.45 ? 'downlink noisy' : 'downlink clear',
            detail: topLeverLabel || (solarPressure >= 0.45 ? 'weather friction up' : 'telemetry lane stable'),
            signal: flowState(0.15 - solarPressure + marketBias * 0.16),
            score: metricScore(0.15 - solarPressure + marketBias * 0.16),
            window: snapshot?.date || 'now',
        },
        geo: {
            headline: solarPressure >= 0.5 ? 'relay drag' : 'relay stable',
            detail: market.topPattern ? `${market.topPattern.ticker || 'pattern'} / ${topPatternLabel}` : `solar pressure ${solarPressure.toFixed(2)}`,
            signal: flowState(0.12 - solarPressure + marketBias * 0.12),
            score: metricScore(0.12 - solarPressure + marketBias * 0.12),
            window: snapshot?.date || 'now',
        },
        cislunar_space: {
            headline: `transfer ${flowState(cislunarScore)}`,
            detail: topSectorLabel ? `${topSectorLabel} / ${topSectorActor || regimeLabel || 'market field'}` : (eclipseDistance != null ? `eclipse ${windowLabel(eclipseDistance)}` : `full ${windowLabel(daysToFull)}`),
            signal: flowState(cislunarScore),
            score: metricScore(cislunarScore),
            window: nextFullDate || snapshot?.date || 'now',
        },
        moon: {
            headline: moonPhase,
            detail: moonIllumination != null ? `${moonIllumination.toFixed(1)}% lit` : 'illumination n/a',
            signal: flowState(lunarScore),
            score: metricScore(lunarScore),
            window: nextFullDate || nextNewDate || snapshot?.date || 'now',
        },
        lunar_surface: {
            headline: `surface ${flowState(lunarScore)}`,
            detail: `${nakshatra} / tithi ${tithi ?? '—'}${regimeLabel ? ` / ${regimeLabel}` : ''}`,
            signal: flowState(lunarScore),
            score: metricScore(lunarScore),
            window: voidState?.next_sign_entry || nextFullDate || snapshot?.date || 'now',
        },
        mars: {
            headline: mars?.retrograde ? 'mars drag' : 'mars direct',
            detail: mars ? `${asNumber(mars.speed, 0).toFixed(3)}°/d / ${mars.sign || 'sign n/a'}` : 'mars motion n/a',
            signal: flowState(marsScore),
            score: metricScore(marsScore),
            window: snapshot?.date || 'now',
        },
        mars_surface: {
            headline: `program ${flowState(marsScore)}`,
            detail: topSectorLabel ? `${topSectorLabel} / ${topSectorActor || 'flow active'}` : (mars?.retrograde ? 'burn under drag' : 'burn line open'),
            signal: flowState(marsScore),
            score: metricScore(marsScore),
            window: 'long cycle',
        },
    };

    const edgeMetrics = {
        flow_earth_surface_leo_capital: {
            headline: flowState(launchScore),
            detail: topSectorLabel
                ? `${topSectorLabel} / ${topSectorActor || topSectorFlow || 'flow active'}`
                : market.topLeader
                    ? `${market.topLeader.symbol} leads / ${market.topLeader.bias}`
                    : (solarPressure >= 0.45 ? 'weather hedge' : 'launch window cleaner'),
            signal: flowState(launchScore),
            score: metricScore(launchScore),
            window: snapshot?.date || 'now',
        },
        flow_leo_earth_surface_telemetry: {
            headline: solarPressure >= 0.45 ? 'bandwidth drag' : 'bandwidth clear',
            detail: `kp ${kp.toFixed(1)} / wind ${Math.round(wind)}`,
            signal: flowState(0.18 - solarPressure),
            score: metricScore(0.18 - solarPressure),
            window: snapshot?.date || 'now',
        },
        flow_earth_surface_cislunar_capital: {
            headline: flowState(cislunarScore),
            detail: regimeLabel ? `${regimeLabel} / ${topLeverLabel || 'lunar logistics window'}` : (eclipseDistance != null && eclipseDistance <= 14 ? 'eclipse hedge' : 'lunar logistics window'),
            signal: flowState(cislunarScore),
            score: metricScore(cislunarScore),
            window: nextFullDate || snapshot?.date || 'now',
        },
        flow_cislunar_lunar_surface_mass: {
            headline: voidState ? 'hold payload' : flowState(lunarScore),
            detail: voidState ? `void until ${voidState.next_sign_entry || 'ingress'}` : moonPhase,
            signal: voidState ? 'tight' : flowState(lunarScore),
            score: metricScore(voidState ? -0.35 : lunarScore),
            window: voidState?.next_sign_entry || nextFullDate || snapshot?.date || 'now',
        },
        flow_earth_surface_mars_surface_capital: {
            headline: flowState(marsScore),
            detail: topSectorLabel ? `${topSectorLabel} / ${topSectorActor || 'flow active'}` : (mars?.retrograde ? 'defer size' : 'long-cycle build'),
            signal: flowState(marsScore),
            score: metricScore(marsScore),
            window: 'long cycle',
        },
    };

    return {
        ...world,
        nodes: world.nodes.map((node) => ({
            ...node,
            metrics: {
                ...node.metrics,
                ...(nodeMetrics[node.id] || {}),
            },
        })),
        edges: world.edges.map((edge) => ({
            ...edge,
            metrics: {
                ...edge.metrics,
                ...(edgeMetrics[edge.id] || {}),
            },
        })),
    };
}
