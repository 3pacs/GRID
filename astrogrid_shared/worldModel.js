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
