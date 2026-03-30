import { computePosition, getFullEphemeris } from './lib/ephemeris.js';
import {
    getAstrogridDefaultApiBaseUrl,
    normalizeAstrogridActivePatterns,
    normalizeAstrogridAggregatedFlows,
    normalizeAstrogridCorrelations,
    normalizeAstrogridCrossReference,
    normalizeAstrogridMoneyMap,
    normalizeAstrogridRegime,
    normalizeAstrogridScorecard,
    normalizeAstrogridSectorMap,
    normalizeAstrogridSignalMap,
    normalizeAstrogridSignalsSnapshot,
    normalizeAstrogridThesis,
    normalizeAstrogridTimeline,
} from './lib/contract.js';
import {
    ASTROGRID_ENDPOINTS,
    buildAstrogridAggregatedFlowsPath,
    buildAstrogridBriefingCandidates,
    buildAstrogridCorrelationsCandidates,
    buildAstrogridSectorDetailPath,
    buildAstrogridSnapshotPath,
    fetchFirstAstrogridCandidate,
} from './lib/endpoints.js';
import { ENGINE_DEFINITIONS, buildPersonaResponse, computeEngineOutputs, computeSeer, extractSkyThreads, labelLens } from './engines.js';
import { buildAstrogridHypotheses, buildCelestialFeatureRows } from './lib/hypotheses.js';
import {
    createAspectField,
    createObjectTable,
    createRadialSky,
    createSpacetimeField,
    createTrajectoryAtlas,
    createWorldAtlas,
} from './visuals.js';
import { buildSeedWorldModel, enrichWorldModel } from './lib/worldModel.js';

const LOG_KEYS = {
    seer: 'astrogrid_web_seer_logs',
    persona: 'astrogrid_web_persona_logs',
    oracle: 'astrogrid_web_oracle_logs',
    runtime: 'astrogrid_web_runtime_logs',
};
const CONFIG_KEYS = {
    apiBaseUrl: 'astrogrid_web_api_base_url',
    apiToken: 'astrogrid_web_api_token',
};

const LENS_MODES = ['solo', 'chorus', 'intersection', 'shadow'];
const TRAJECTORY_PROJECTIONS = [
    { id: 'radec', label: 'RA/Dec' },
    { id: 'ecliptic', label: 'lon/lat' },
];
const TRAJECTORY_HORIZONS = [7, 14, 30];
const REMOTE_POLL_INTERVAL_MS = 30000;
const REMOTE_POLL_LIVE_WINDOW_MS = 6 * 60 * 60 * 1000;
const MARKET_OVERLAY_TTL_MS = 5 * 60 * 1000;
const SHARED_LOGIN_PATH = '/#/login';
const SAMPLEABLE_TRAJECTORY_BODIES = new Set([
    'mercury',
    'venus',
    'mars',
    'jupiter',
    'saturn',
    'uranus',
    'neptune',
    'pluto',
    'moon',
    'rahu',
    'ketu',
]);
const PERSONAS = [
    { id: 'seer', name: 'Seer' },
    { id: 'qwen', name: 'Qwen Mask' },
    { id: 'western', name: 'Meridian Reader' },
    { id: 'vedic', name: 'Knot Reader' },
    { id: 'hermetic', name: 'Mirror Witness' },
    { id: 'taoist', name: 'Quiet Observer' },
    { id: 'babylonian', name: 'Watchtower Keeper' },
];
const ORACLE_PROMPT_PRESETS = [
    'What should I watch now?',
    'Where is the cleanest edge this week?',
    'What invalidates the current read?',
    'Which sleeve looks weakest right now?',
];
const PAGES = [
    { id: 'oracle', label: 'Oracle' },
    { id: 'observatory', label: 'Observatory' },
    { id: 'atlas', label: 'Atlas' },
    { id: 'chamber', label: 'Chamber' },
];

const state = {
    page: 'oracle',
    mode: 'chorus',
    activeLensIds: ['western', 'vedic', 'hermetic', 'taoist'],
    selectedDateTime: toLocalInput(new Date()),
    apiBaseUrl: loadApiBaseUrl(),
    apiTokenOverride: loadTokenOverride(),
    question: 'What should I watch now?',
    personaId: 'seer',
    personaResponse: null,
    latestPrediction: null,
    snapshot: null,
    archive: null,
    engineOutputs: [],
    threads: [],
    seer: null,
    backend: {
        enabled: true,
        connected: false,
        summary: 'Awaiting sky.',
        snapshot: null,
        overview: null,
        timeline: [],
        correlations: [],
        briefing: null,
        prophecy: null,
        prophecyKey: '',
        marketOverlay: {
            connected: false,
            summary: 'Market overlay idle.',
            updatedAt: null,
            regime: null,
            thesis: null,
            moneyMap: null,
            scorecard: null,
            universe: null,
            sectorFlows: null,
            sectorMap: null,
            featureSnapshot: [],
            activePatterns: [],
            crossReference: null,
        },
        polling: {
            active: false,
            intervalMs: REMOTE_POLL_INTERVAL_MS,
            nextAt: null,
            lastAttemptAt: null,
            lastSuccessAt: null,
        },
    },
    vectorProjection: 'radec',
    vectorHorizonDays: 14,
    vectorStepHours: 12,
    vectorFocusBodyId: 'moon',
    vectorSample: null,
    worldFocusId: 'earth_surface',
};

function safeStorageGet(key) {
    try {
        return window.localStorage.getItem(key);
    } catch {
        return null;
    }
}

function safeStorageSet(key, value) {
    try {
        window.localStorage.setItem(key, value);
    } catch {
        // ignore storage failures in prototype shell
    }
}

function safeStorageRemove(key) {
    try {
        window.localStorage.removeItem(key);
    } catch {
        // ignore storage failures in prototype shell
    }
}

function toLocalInput(dt) {
    const pad = (value) => `${value}`.padStart(2, '0');
    return `${dt.getFullYear()}-${pad(dt.getMonth() + 1)}-${pad(dt.getDate())}T${pad(dt.getHours())}:${pad(dt.getMinutes())}`;
}

function readQueryParam(...names) {
    try {
        const params = new URLSearchParams(window.location.search);
        for (const name of names) {
            const value = params.get(name);
            if (value && value.trim()) {
                return value.trim();
            }
        }
    } catch {
        // ignore URL parsing failures
    }
    return '';
}

function loadApiBaseUrl() {
    const queryValue = readQueryParam('api', 'apiBaseUrl', 'base');
    if (queryValue) return queryValue;
    const stored = safeStorageGet(CONFIG_KEYS.apiBaseUrl);
    if (stored) return stored;
    return getAstrogridDefaultApiBaseUrl(window.location);
}

function loadTokenOverride() {
    return readQueryParam('token', 'api_token', 'jwt') || safeStorageGet(CONFIG_KEYS.apiToken) || '';
}

function readToken() {
    return state.apiTokenOverride || safeStorageGet('grid_token') || '';
}

function sharedSessionToken() {
    return safeStorageGet('grid_token') || '';
}

function usingManualTokenOverride() {
    return Boolean(state.apiTokenOverride);
}

function hasSharedSession() {
    return Boolean(sharedSessionToken());
}

function openSharedLogin() {
    window.location.assign(SHARED_LOGIN_PATH);
}

function clearTokenOverride() {
    state.apiTokenOverride = '';
    safeStorageRemove(CONFIG_KEYS.apiToken);
}

function getBaseUrl() {
    return state.apiBaseUrl.replace(/\/$/, '');
}

function clearRemotePollTimer() {
    if (typeof window === 'undefined') return;
    if (window.__astrogridRemotePollTimer) {
        window.clearTimeout(window.__astrogridRemotePollTimer);
        window.__astrogridRemotePollTimer = null;
    }
}

function sameOriginApiBase() {
    try {
        return new URL(getBaseUrl(), window.location.href).origin === window.location.origin;
    } catch {
        return false;
    }
}

function dateKey(value) {
    return String(value || '').slice(0, 10);
}

function normalizeBodyId(value) {
    return String(value || '').trim().toLowerCase();
}

let archiveSnapshotCache = null;
let archiveSnapshotAttempted = false;
const archiveYearCache = new Map();

function parseJsonlSnapshots(text) {
    const byDate = new Map();
    for (const line of String(text || '').split('\n')) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        try {
            const record = JSON.parse(trimmed);
            if (record?.date) {
                byDate.set(dateKey(record.date), record);
            }
        } catch {
            // ignore malformed archive lines
        }
    }
    return byDate;
}

async function loadArchiveYear(year) {
    if (archiveYearCache.has(year)) {
        return archiveYearCache.get(year);
    }

    const promise = (async () => {
        try {
            const response = await fetch(`./data/years/daily_${year}.jsonl`, { cache: 'force-cache' });
            if (!response.ok) {
                return null;
            }
            const text = await response.text();
            return parseJsonlSnapshots(text);
        } catch {
            return null;
        }
    })();

    archiveYearCache.set(year, promise);
    return promise;
}

function decodeTokenMeta(token) {
    if (!token || typeof token !== 'string' || token.split('.').length < 2) {
        return null;
    }
    try {
        const payload = token.split('.')[1].replace(/-/g, '+').replace(/_/g, '/');
        const decoded = JSON.parse(window.atob(payload));
        const exp = typeof decoded.exp === 'number' ? new Date(decoded.exp * 1000) : null;
        return {
            role: decoded.role || '',
            username: decoded.sub || '',
            expiresAt: exp,
            expired: Boolean(exp && exp.getTime() < Date.now()),
        };
    } catch {
        return null;
    }
}

async function loadLocalArchiveSnapshot(localSnapshot) {
    const targetDate = dateKey(localSnapshot?.date || state.selectedDateTime);
    const targetYear = targetDate.slice(0, 4);

    const yearSnapshots = await loadArchiveYear(targetYear);
    if (yearSnapshots?.has(targetDate)) {
        return yearSnapshots.get(targetDate);
    }

    if (!archiveSnapshotAttempted) {
        archiveSnapshotAttempted = true;
        try {
            const response = await fetch('./data/latest_snapshot.json', { cache: 'no-store' });
            if (response.ok) {
                archiveSnapshotCache = await response.json();
            }
        } catch {
            archiveSnapshotCache = null;
        }
    }

    if (!archiveSnapshotCache) {
        return null;
    }

    return dateKey(archiveSnapshotCache.date) === targetDate ? archiveSnapshotCache : null;
}

function mergeSnapshotSources(localSnapshot, archiveSnapshot, backendSnapshot) {
    const merged = {
        ...localSnapshot,
    };

    const overlays = [archiveSnapshot, backendSnapshot].filter(Boolean);
    for (const overlay of overlays) {
        for (const key of [
            'objects',
            'bodies',
            'positions',
            'aspects',
            'lunar',
            'nakshatra',
            'events',
            'local_features',
            'void_of_course',
            'motions',
            'derived',
            'provenance',
            'signal_field',
            'seer',
            'precision',
            'source',
        ]) {
            if (overlay[key] != null) {
                merged[key] = overlay[key];
            }
        }
    }

    const archiveSignals = archiveSnapshot ? {
        planetaryStress: archiveSnapshot.local_features?.planetary_stress_index ?? localSnapshot.signals?.planetaryStress,
        retrogradeCount: Array.isArray(archiveSnapshot.retrograde_planets)
            ? archiveSnapshot.retrograde_planets.length
            : localSnapshot.signals?.retrogradeCount,
        lunarIllumination: archiveSnapshot.lunar?.illumination ?? localSnapshot.signals?.lunarIllumination,
        lunarPhase: archiveSnapshot.lunar?.phase_name ?? localSnapshot.signals?.lunarPhase,
        nakshatra: archiveSnapshot.nakshatra?.nakshatra_name ?? localSnapshot.signals?.nakshatra,
    } : {};

    merged.signals = {
        ...(localSnapshot.signals || {}),
        ...archiveSignals,
    };

    if (!merged.date) {
        merged.date = localSnapshot.date;
    }

    return merged;
}

async function fetchJson(path, options = {}) {
    const headers = { ...(options.headers || {}) };
    const token = readToken();
    if (token) {
        headers.Authorization = `Bearer ${token}`;
    }
    const response = await fetch(`${getBaseUrl()}${path}`, { ...options, headers });
    if (!response.ok) {
        const bodyText = await response.text().catch(() => '');
        let body = {};
        try {
            body = bodyText ? JSON.parse(bodyText) : {};
        } catch {
            body = { detail: bodyText };
        }
        const error = new Error(body.detail || body.error || response.statusText);
        error.status = response.status;
        error.body = body;
        reportRuntimeEvent(response.status >= 500 ? 'error' : 'warn', 'fetch_json_failed', {
            path,
            status: response.status,
            detail: error.message,
        });
        throw error;
    }
    return response.json();
}

async function fetchFirstJson(candidates) {
    return fetchFirstAstrogridCandidate((path, options) => fetchJson(path, options), candidates);
}

function buildSnapshot() {
    const dt = new Date(state.selectedDateTime);
    const ephemeris = getFullEphemeris(dt);
    const bodies = Object.entries(ephemeris.positions).map(([id, pos]) => ({
        ...pos,
        id,
        name: id,
        longitude: pos.geocentric_longitude,
        latitude: pos.ecliptic_latitude,
        rightAscension: pos.right_ascension,
        declination: pos.declination,
        distance: pos.distance_au,
        speed: computeBodySpeed(id, dt),
        sign: pos.zodiac_sign,
        degree: pos.zodiac_degree,
        retrograde: Boolean(pos.is_retrograde),
        precision: 'computed',
    }));

    return {
        date: state.selectedDateTime,
        bodies,
        positions: ephemeris.positions,
        aspects: ephemeris.aspects,
        lunar: ephemeris.lunar_phase,
        nakshatra: ephemeris.nakshatra,
        summary: ephemeris.summary,
        signals: deriveSignals(ephemeris),
    };
}

function angularDelta(current, future) {
    let diff = future - current;
    if (diff > 180) diff -= 360;
    if (diff < -180) diff += 360;
    return diff;
}

function computeBodySpeed(bodyId, dt) {
    try {
        const tomorrow = new Date(dt.getTime() + 86400000);
        const current = computePosition(bodyId, dt);
        const next = computePosition(bodyId, tomorrow);
        return angularDelta(current.geocentric_longitude, next.geocentric_longitude);
    } catch {
        return null;
    }
}

function deriveSignals(ephemeris) {
    const stress = ephemeris.aspects.filter((aspect) => ['square', 'opposition', 'conjunction'].includes(aspect.aspect_type)).length;
    const retrogradeCount = ephemeris.retrograde_planets.length;
    return {
        planetaryStress: stress,
        retrogradeCount,
        lunarIllumination: ephemeris.lunar_phase.illumination,
        lunarPhase: ephemeris.lunar_phase.phase_name,
        nakshatra: ephemeris.nakshatra.nakshatra_name,
    };
}

function availableTrajectoryBodies() {
    const bodies = Array.isArray(state.snapshot?.bodies) ? state.snapshot.bodies : [];
    const seen = new Set();
    return bodies
        .map((body) => {
            const id = normalizeBodyId(body?.id || body?.name);
            if (!id || seen.has(id) || !SAMPLEABLE_TRAJECTORY_BODIES.has(id)) return null;
            seen.add(id);
            return {
                id,
                name: body?.name || body?.id || id,
            };
        })
        .filter(Boolean);
}

function formatLensList(lensIds = []) {
    return lensIds.map((lensId) => labelLens(lensId)).join(' / ');
}

function ensureTrajectoryState() {
    const bodies = availableTrajectoryBodies();
    if (!bodies.length) {
        state.vectorFocusBodyId = 'all';
        state.vectorSample = null;
        return;
    }

    const ids = new Set(bodies.map((body) => body.id));
    if (state.vectorFocusBodyId !== 'all' && !ids.has(state.vectorFocusBodyId)) {
        state.vectorFocusBodyId = ids.has('moon') ? 'moon' : 'all';
    }

    if (state.vectorSample?.bodyId && !ids.has(state.vectorSample.bodyId)) {
        state.vectorSample = null;
    }
}

function getFocusedTrajectoryBody() {
    const bodies = availableTrajectoryBodies();
    if (!bodies.length) return null;
    if (state.vectorFocusBodyId === 'all') {
        return bodies.find((body) => body.id === 'moon') || bodies[0];
    }
    return bodies.find((body) => body.id === state.vectorFocusBodyId) || bodies[0];
}

function getSnapshotBody(bodyId) {
    const bodies = Array.isArray(state.snapshot?.bodies) ? state.snapshot.bodies : [];
    return bodies.find((body) => normalizeBodyId(body?.id || body?.name) === normalizeBodyId(bodyId)) || null;
}

function formatMetric(value, digits = 2) {
    const n = Number(value);
    return Number.isFinite(n) ? n.toFixed(digits) : '—';
}

function trajectoryReadoutMarkup() {
    const focusBody = getFocusedTrajectoryBody();
    if (!focusBody) {
        return '<div class="empty">Awaiting vector state.</div>';
    }

    const sample = state.vectorSample && normalizeBodyId(state.vectorSample.bodyId) === normalizeBodyId(focusBody.id)
        ? state.vectorSample
        : null;
    const currentBody = getSnapshotBody(focusBody.id) || {};
    const isSample = Boolean(sample);
    const stamp = sample?.at
        ? new Date(sample.at).toLocaleString()
        : (state.snapshot?.date ? new Date(state.snapshot.date).toLocaleString() : 'current');
    const position = {
        lon: sample?.lon ?? currentBody.longitude ?? currentBody.geocentric_longitude,
        lat: sample?.lat ?? currentBody.latitude ?? currentBody.ecliptic_latitude,
        ra: sample?.ra ?? currentBody.rightAscension ?? currentBody.right_ascension,
        dec: sample?.dec ?? currentBody.declination,
        dist: sample?.dist ?? currentBody.distance ?? currentBody.distance_au,
        speed: sample?.speed ?? currentBody.speed,
        retrograde: sample?.retrograde ?? currentBody.retrograde ?? currentBody.is_retrograde,
    };
    const offset = sample ? `${sample.offsetHours > 0 ? '+' : ''}${sample.offsetHours}h` : 'now';

    return `
        <div class="trajectory-readout">
            <div class="engine-head">
                <div class="engine-name">${focusBody.name}</div>
                <div class="engine-meta">${isSample ? offset : 'current'}</div>
            </div>
            <div class="trajectory-readout-grid">
                <div class="metric">
                    <div class="metric-value">${formatMetric(position.lon, 2)}°</div>
                    <div class="metric-label">lon</div>
                </div>
                <div class="metric">
                    <div class="metric-value">${formatMetric(position.lat, 2)}°</div>
                    <div class="metric-label">lat</div>
                </div>
                <div class="metric">
                    <div class="metric-value">${formatMetric(position.ra, 2)}°</div>
                    <div class="metric-label">RA</div>
                </div>
                <div class="metric">
                    <div class="metric-value">${formatMetric(position.dec, 2)}°</div>
                    <div class="metric-label">Dec</div>
                </div>
            </div>
            <div class="seer-support">dist: ${formatMetric(position.dist, 4)} AU / speed: ${formatMetric(position.speed, 4)}°/d / ${position.retrograde ? 'retrograde' : 'direct'}</div>
            <div class="seer-support">stamp: ${stamp}</div>
        </div>
    `;
}

function readLogs(key) {
    try {
        return JSON.parse(safeStorageGet(key) || '[]');
    } catch {
        return [];
    }
}

function writeLogs(key, entry) {
    const current = readLogs(key);
    current.unshift(entry);
    safeStorageSet(key, JSON.stringify(current.slice(0, 30)));
}

function reportRuntimeEvent(level, event, detail = {}) {
    const payload = {
        at: new Date().toISOString(),
        level,
        event,
        detail,
    };
    writeLogs(LOG_KEYS.runtime, payload);
    const sink = level === 'error' ? console.error : level === 'warn' ? console.warn : console.info;
    sink('[astrogrid]', event, detail);
}

function buildOraclePostmortem() {
    const oracleLogs = readLogs(LOG_KEYS.oracle).slice(0, 12);
    const counts = oracleLogs.reduce((acc, log) => {
        const key = String(log.call || 'none').toLowerCase();
        acc[key] = (acc[key] || 0) + 1;
        return acc;
    }, {});
    return {
        total: oracleLogs.length,
        pending: oracleLogs.filter((log) => log.score === 'pending').length,
        live: oracleLogs.filter((log) => log.live).length,
        local: oracleLogs.filter((log) => !log.live).length,
        dominantCall: Object.entries(counts).sort((a, b) => b[1] - a[1])[0]?.[0] || 'none',
    };
}

function emptyMarketOverlay(summary = 'Market overlay idle.') {
    return {
        connected: false,
        summary,
        updatedAt: null,
        regime: null,
        thesis: null,
        moneyMap: null,
        scorecard: null,
        universe: null,
        sectorFlows: null,
        sectorMap: null,
        featureSnapshot: [],
        activePatterns: [],
        crossReference: null,
    };
}

function marketOverlayFresh() {
    const updatedAt = state.backend.marketOverlay?.updatedAt;
    if (!updatedAt) return false;
    const updatedMs = parseDateMs(updatedAt);
    return updatedMs != null && (Date.now() - updatedMs) < MARKET_OVERLAY_TTL_MS;
}

function shouldUseMarketOverlay() {
    return Boolean(readToken()) && shouldPollRemote();
}

function topSectorProfile(overlay) {
    const sectorName = overlay?.sectorFlows?.bySector?.[0]?.sector;
    if (!sectorName) return null;
    return overlay?.sectorMap?.byName?.[sectorName] || null;
}

function tickerSectorContext(overlay, ticker) {
    const key = String(ticker || '').toUpperCase();
    if (!key) return null;
    return overlay?.sectorMap?.tickerIndex?.[key] || null;
}

async function refreshSharedMarketOverlay(force = false) {
    if (!shouldUseMarketOverlay() || !state.backend.connected) {
        state.backend.marketOverlay = emptyMarketOverlay('Market overlay idle. Live window only.');
        return;
    }
    if (!force && marketOverlayFresh()) {
        return;
    }

    const results = await Promise.allSettled([
        fetchJson(ASTROGRID_ENDPOINTS.regimeCurrent),
        fetchJson(ASTROGRID_ENDPOINTS.intelligenceThesis),
        fetchJson(ASTROGRID_ENDPOINTS.moneyMap),
        fetchJson(ASTROGRID_ENDPOINTS.scorecard),
        fetchJson(buildAstrogridAggregatedFlowsPath({ days: 30, period: 'weekly' })),
        fetchJson(ASTROGRID_ENDPOINTS.flowsSectors),
        fetchJson(ASTROGRID_ENDPOINTS.signalsSnapshot),
        fetchJson(ASTROGRID_ENDPOINTS.activePatterns),
        fetchJson(ASTROGRID_ENDPOINTS.crossReference),
    ]);

    const overlay = {
        connected: false,
        summary: 'Market overlay unavailable.',
        updatedAt: new Date().toISOString(),
        regime: results[0].status === 'fulfilled' ? normalizeAstrogridRegime(results[0].value) : null,
        thesis: results[1].status === 'fulfilled' ? normalizeAstrogridThesis(results[1].value) : null,
        moneyMap: results[2].status === 'fulfilled' ? normalizeAstrogridMoneyMap(results[2].value) : null,
        scorecard: results[3].status === 'fulfilled' ? normalizeAstrogridScorecard(results[3].value) : null,
        sectorFlows: results[4].status === 'fulfilled' ? normalizeAstrogridAggregatedFlows(results[4].value) : null,
        sectorMap: results[5].status === 'fulfilled' ? normalizeAstrogridSectorMap(results[5].value) : null,
        featureSnapshot: results[6].status === 'fulfilled' ? normalizeAstrogridSignalsSnapshot(results[6].value) : [],
        activePatterns: results[7].status === 'fulfilled' ? normalizeAstrogridActivePatterns(results[7].value) : [],
        crossReference: results[8].status === 'fulfilled' ? normalizeAstrogridCrossReference(results[8].value) : null,
    };

    let readyCount = [
        overlay.regime,
        overlay.thesis,
        overlay.moneyMap,
        overlay.scorecard,
        overlay.universe,
        overlay.sectorFlows,
        overlay.sectorMap,
        overlay.featureSnapshot.length ? overlay.featureSnapshot : null,
        overlay.activePatterns.length ? overlay.activePatterns : null,
        overlay.crossReference,
    ].filter(Boolean).length;

    const topSectorName = overlay.sectorFlows?.bySector?.[0]?.sector;
    const detailExpected = Boolean(topSectorName);
    if (topSectorName) {
        try {
            overlay.sectorDetail = normalizeAstrogridSectorDetail(
                await fetchJson(buildAstrogridSectorDetailPath(topSectorName)),
            );
        } catch {
            overlay.sectorDetail = null;
        }
    }

    if (overlay.sectorDetail) {
        readyCount += 1;
    }

    overlay.connected = readyCount > 0;
    if (overlay.connected) {
        overlay.summary = readyCount === 9 ? 'Market overlay live.' : `Market overlay partial (${readyCount}/9).`;
    }

    state.backend.marketOverlay = overlay;
}

async function refreshBackend() {
    state.backend.snapshot = null;
    state.backend.prophecy = null;
    state.backend.prophecyKey = '';
    state.backend.polling.lastAttemptAt = new Date().toISOString();
    if (!readToken()) {
        state.backend.connected = false;
        state.backend.summary = sameOriginApiBase()
            ? 'Shared session missing. Sign in to unlock the live layer.'
            : 'Local sky only. Paste a session token. This origin cannot read remote storage.';
        reportRuntimeEvent('info', 'shared_session_missing', {
            sameOrigin: sameOriginApiBase(),
        });
        state.backend.overview = null;
        state.backend.timeline = [];
        state.backend.correlations = [];
        state.backend.briefing = null;
        state.backend.prophecy = null;
        state.backend.marketOverlay = emptyMarketOverlay(
            sameOriginApiBase() ? 'Market overlay locked. Shared session missing.' : 'Market overlay locked. Paste a session token.',
        );
        return;
    }

    try {
        const snapshotPath = buildAstrogridSnapshotPath(state.selectedDateTime);
        const [snapshotResult, correlationsResult, briefingResult] = await Promise.allSettled([
            fetchJson(snapshotPath),
            fetchFirstJson(buildAstrogridCorrelationsCandidates()),
            fetchFirstJson(buildAstrogridBriefingCandidates()),
        ]);

        if (snapshotResult.status !== 'fulfilled') {
            throw snapshotResult.reason;
        }

        const snapshot = snapshotResult.value;
        state.backend.connected = true;
        state.backend.polling.lastSuccessAt = new Date().toISOString();
        state.backend.snapshot = snapshot;
        state.backend.summary = `Authoritative snapshot live. Source: ${snapshot.source || 'remote oracle'}.`;
        state.backend.overview = snapshot.signals || snapshot.grid || null;
        state.backend.timeline = normalizeAstrogridTimeline(snapshot);
        state.backend.correlations = correlationsResult.status === 'fulfilled'
            ? normalizeAstrogridCorrelations(correlationsResult.value)
            : [];
        state.backend.briefing = briefingResult.status === 'fulfilled' ? briefingResult.value : null;
        await refreshSharedMarketOverlay();
    } catch (error) {
        state.backend.connected = false;
        const detail = String(error?.message || '');
        if (error?.status === 401) {
            state.backend.summary = sameOriginApiBase()
                ? 'Shared session rejected. Sign in again.'
                : 'Session token rejected. Paste a fresh one.';
            reportRuntimeEvent('warn', 'backend_auth_denied', {
                detail,
                status: error?.status,
            });
        } else if (error?.status === 403 && /1010/.test(detail)) {
            state.backend.summary = 'The edge blocked this client before auth. Use same-origin AstroGrid or relax the rule.';
            reportRuntimeEvent('warn', 'backend_edge_blocked', {
                detail,
                status: error?.status,
            });
        } else if (error?.status === 403) {
            state.backend.summary = 'The remote oracle denied this request. Check edge policy and route permissions.';
            reportRuntimeEvent('warn', 'backend_forbidden', {
                detail,
                status: error?.status,
            });
        } else if (/failed to fetch/i.test(detail) && !sameOriginApiBase()) {
            state.backend.summary = 'Remote fetch failed at the browser edge. Same-origin AstroGrid or a server-side proxy is required.';
            reportRuntimeEvent('error', 'backend_fetch_failed', {
                detail,
                status: error?.status || null,
            });
        } else {
            state.backend.summary = `Local sky active. Remote snapshot failed: ${detail}`;
            reportRuntimeEvent('error', 'backend_refresh_failed', {
                detail,
                status: error?.status || null,
            });
        }
        state.backend.overview = null;
        state.backend.timeline = [];
        state.backend.correlations = [];
        state.backend.briefing = null;
        state.backend.prophecy = null;
        state.backend.marketOverlay = emptyMarketOverlay('Market overlay offline.');
    }
}

async function refreshProphecyOverlay() {
    if (!readToken() || !state.backend.connected || !state.snapshot || !state.engineOutputs.length || !state.seer) {
        state.backend.prophecy = null;
        state.backend.prophecyKey = '';
        return;
    }

    const prophecyKey = JSON.stringify({
        date: state.snapshot.date || state.snapshot.timestamp,
        mode: state.mode,
        lenses: [...state.activeLensIds].sort(),
        persona: state.personaId,
        question: state.question,
        seer: [state.seer.reading, state.seer.prediction, state.seer.confidence],
    });
    if (state.backend.prophecyKey === prophecyKey && state.backend.prophecy) {
        return;
    }

    try {
        state.backend.prophecyKey = prophecyKey;
        state.backend.prophecy = await fetchJson('/api/v1/astrogrid/interpret', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                snapshot: state.snapshot,
                engine_outputs: state.engineOutputs,
                seer: state.seer,
                question: state.question,
                mode: state.mode,
                lens_ids: state.activeLensIds,
            }),
        });
    } catch (error) {
        reportRuntimeEvent('warn', 'prophecy_overlay_unavailable', {
            detail: String(error?.message || error),
            status: error?.status || null,
        });
        state.backend.prophecy = {
            summary: 'Model overlay unavailable.',
            used_llm: false,
            backend: 'unavailable',
            model: null,
            threads: [{
                title: 'model status',
                detail: error.message,
                lenses: [],
                confidence: 0,
            }],
            seer: {
                reading: state.seer.reading,
                prediction: state.seer.prediction,
                why: state.seer.key_factors || [],
                warnings: ['Model overlay unavailable. Deterministic layer remains active.'],
            },
            engine_notes: [],
            tone_notes: ['Restore backend auth or model availability.'],
        };
    }
}

async function recompute() {
    const localSnapshot = buildSnapshot();
    state.archive = await loadLocalArchiveSnapshot(localSnapshot);
    await refreshBackend();
    state.snapshot = mergeSnapshotSources(localSnapshot, state.archive, state.backend.snapshot);
    state.vectorSample = null;
    ensureTrajectoryState();
    state.engineOutputs = computeEngineOutputs(state.snapshot, state.activeLensIds, state.mode);
    const seerSignalInput = Array.isArray(state.backend.snapshot?.signal_field) && state.backend.snapshot.signal_field.length
        ? state.backend.snapshot.signal_field
        : {
            ...state.snapshot.signals,
            ...flattenOverviewSignals(state.backend.overview),
        };
    const localSeer = computeSeer(state.engineOutputs, seerSignalInput);
    state.seer = state.backend.snapshot?.seer
        ? { ...localSeer, ...state.backend.snapshot.seer }
        : localSeer;
    state.threads = extractSkyThreads(state.snapshot, state.engineOutputs);
    await refreshProphecyOverlay();

    writeLogs(LOG_KEYS.seer, {
        at: new Date().toISOString(),
        mode: state.mode,
        lensIds: [...state.activeLensIds],
        reading: state.seer.reading,
        prediction: state.seer.prediction,
        confidence: state.seer.confidence,
    });
    const oracleRecord = currentOracleRecord(buildOracleDirective());
    if (oracleRecord) {
        writeLogs(LOG_KEYS.oracle, oracleRecord);
    }
    render();
}

let recomputeQueue = Promise.resolve();

function scheduleRemotePoll() {
    clearRemotePollTimer();
    syncPollingState();
    if (!state.backend.polling.active || typeof window === 'undefined') return;
    window.__astrogridRemotePollTimer = window.setTimeout(() => {
        if (typeof document !== 'undefined' && document.visibilityState === 'hidden') {
            scheduleRemotePoll();
            return;
        }
        scheduleRecompute('poll');
    }, REMOTE_POLL_INTERVAL_MS);
}

function scheduleRecompute(reason = 'manual') {
    recomputeQueue = recomputeQueue
        .catch(() => undefined)
        .then(() => recompute())
        .catch((error) => {
            reportRuntimeEvent('error', 'recompute_failed', {
                reason,
                detail: String(error?.message || error),
            });
            console.error(error);
            renderFatal(error);
        })
        .finally(() => {
            if (reason === 'poll' && !state.backend.connected) {
                syncPollingState();
                clearRemotePollTimer();
                return;
            }
            scheduleRemotePoll();
        });
    return recomputeQueue;
}

function flattenOverviewSignals(overview) {
    return normalizeAstrogridSignalMap(overview);
}

async function handlePersonaSubmit() {
    if (!state.seer) {
        reportRuntimeEvent('warn', 'persona_submit_without_seer', {
            personaId: state.personaId,
            question: state.question,
        });
        return;
    }
    const response = buildPersonaResponse({
        personaId: state.personaId,
        question: state.question,
        seer: state.seer,
        engineOutputs: state.engineOutputs,
        lensIds: state.activeLensIds,
        mode: state.mode,
    });

    writeLogs(LOG_KEYS.persona, {
        at: new Date().toISOString(),
        personaId: state.personaId,
        mode: state.mode,
        lensIds: [...state.activeLensIds],
        question: state.question,
        answer: response.answer,
    });

    state.personaResponse = response;
    await submitPredictionRecord(buildOracleDirective());
    render();
}

function toggleLens(lensId) {
    if (state.mode === 'solo') {
        state.activeLensIds = [lensId];
        render();
        scheduleRecompute();
        return;
    }

    const active = new Set(state.activeLensIds);
    if (active.has(lensId)) {
        active.delete(lensId);
    } else {
        active.add(lensId);
    }
    state.activeLensIds = [...active];
    if (state.activeLensIds.length === 0) {
        state.activeLensIds = ['western'];
    }
    render();
    scheduleRecompute();
}

function setMode(mode) {
    state.mode = mode;
    if (mode === 'solo' && state.activeLensIds.length > 1) {
        state.activeLensIds = [state.activeLensIds[0]];
    }
    render();
    scheduleRecompute();
}

function eventsMarkup() {
    const liveEvents = state.backend.timeline.slice(0, 6);
    const localEvents = state.snapshot ? buildLocalEvents(state.snapshot) : [];
    const events = liveEvents.length ? liveEvents : localEvents;
    if (!events.length) {
        return `<div class="empty">No event stream.</div>`;
    }
    return `<div class="event-list">${events.map((event) => `
        <div class="event-card">
            <div class="engine-head">
                <div class="engine-name">${event.name || event.event || 'Event'}</div>
                <div class="engine-meta">${event.date || event.datetime || ''}</div>
            </div>
            <div class="subtle">${event.description || event.detail || ''}</div>
        </div>
    `).join('')}</div>`;
}

function formatPct(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return 'n/a';
    return `${numeric > 0 ? '+' : ''}${numeric.toFixed(1)}%`;
}

function compactDirectiveLine(value, max = 72) {
    const text = String(value || '')
        .replace(/\s+/g, ' ')
        .replace(/\s*\/\s*/g, ' / ')
        .trim();
    if (!text) return '';
    if (text.length <= max) return text;
    return `${text.slice(0, Math.max(0, max - 1)).trimEnd()}…`;
}

function scorecardMarkup() {
    const scorecard = state.backend.marketOverlay?.scorecard;
    if (!scorecard?.items?.length) {
        return correlationsMarkup();
    }
    const overlay = state.backend.marketOverlay;

    const groups = scorecard.groups || [];
    const leaders = scorecard.leaders || [];
    const laggards = scorecard.laggards || [];
    const summary = scorecard.summary || {};
    const evaluation = scorecard.evaluation?.overall || {};
    const flowProfile = topSectorProfile(overlay);

    const summaryCards = [
        {
            name: 'hybrid',
            value: `${summary.bias || 'wait'} / ${summary.available || 0}/${summary.total || 0}`,
            detail: flowProfile?.topActor?.name
                ? `${flowProfile.name} / ${flowProfile.topActor.name}`
                : leaders.length ? `lead ${leaders.map((item) => item.symbol).join(' / ')}` : 'coverage unresolved',
            tone: (summary.compositeScore || 0) > 0 ? 'good' : (summary.compositeScore || 0) < 0 ? 'bad' : 'warn',
        },
        ...groups.map((group) => ({
            name: group.label.toLowerCase(),
            value: `${group.bias} / ${group.strongest || '—'}`,
            detail: (() => {
                const strongestContext = tickerSectorContext(overlay, group.strongest);
                if (strongestContext?.actor?.name) {
                    return `${strongestContext.sector} / ${strongestContext.actor.name}`;
                }
                return `${group.available}/${group.total} covered / weak ${group.weakest || '—'}`;
            })(),
            tone: (group.compositeScore || 0) > 0 ? 'good' : (group.compositeScore || 0) < 0 ? 'bad' : 'warn',
        })),
        evaluation.total_predictions ? {
            name: 'oracle',
            value: `${Math.round((evaluation.accuracy || 0) * 100)}% accuracy`,
            detail: `${evaluation.scored || 0} scored / ${evaluation.pending || 0} pending`,
            tone: (evaluation.accuracy || 0) >= 0.55 ? 'good' : 'warn',
        } : null,
    ].filter(Boolean);

    const itemCards = [
        ...leaders.slice(0, 2).map((item) => ({
            name: `${item.symbol} lead`,
            value: `${item.trend || 'live'} / ${formatPct(item.change5dPct)}`,
            detail: (() => {
                const context = tickerSectorContext(overlay, item.symbol);
                if (context?.actor?.name) {
                    return `${context.sector} / ${context.actor.name}`;
                }
                return item.bias || 'wait';
            })(),
            tone: (item.momentumScore || 0) > 0 ? 'good' : 'warn',
        })),
        ...laggards.slice(0, 1).map((item) => ({
            name: `${item.symbol} weak`,
            value: `${item.trend || 'live'} / ${formatPct(item.change5dPct)}`,
            detail: (() => {
                const context = tickerSectorContext(overlay, item.symbol);
                if (context?.actor?.name) {
                    return `${context.sector} / ${context.actor.name}`;
                }
                return item.bias || 'wait';
            })(),
            tone: (item.momentumScore || 0) < 0 ? 'bad' : 'warn',
        })),
        ...actorCards,
    ];

    return `<div class="event-list">${[...summaryCards, ...itemCards].map((card) => `
        <div class="event-card">
            <div class="engine-head">
                <div class="engine-name">${card.name}</div>
                <div class="engine-meta ${card.tone}">${card.value}</div>
            </div>
            <div class="subtle">${card.detail}</div>
        </div>
    `).join('')}</div>`;
}

function correlationsMarkup() {
    if (state.backend.marketOverlay?.connected) {
        return marketVoiceMarkup();
    }
    const correlations = state.backend.correlations.slice(0, 6);
    if (!correlations.length) {
        const featureRows = state.snapshot ? buildCelestialFeatureRows(state.snapshot) : [];
        const signalField = featureRows.length
            ? featureRows
            : Array.isArray(state.backend.snapshot?.signal_field) && state.backend.snapshot.signal_field.length
                ? state.backend.snapshot.signal_field
                : (state.snapshot ? buildLocalSignals(state.snapshot) : []);
        return signalField.length ? `<div class="event-list">${signalField.map((entry) => `
            <div class="event-card">
                <div class="engine-head">
                    <div class="engine-name">${entry.display_name || entry.name}</div>
                    <div class="engine-meta ${entry.signal === 'bullish' || entry.value > 0 ? 'good' : entry.signal === 'bearish' || entry.value < 0 ? 'bad' : 'warn'}">${entry.display || entry.label || entry.value}</div>
                </div>
                <div class="subtle">${entry.interpretation || entry.description || ''}</div>
            </div>
        `).join('')}</div>` : `<div class="empty">No signal field yet.</div>`;
    }
    return `<div class="event-list">${correlations.map((entry) => {
        const value = entry.correlation ?? entry.value ?? 0;
        return `
            <div class="event-card">
                <div class="engine-head">
                    <div class="engine-name">${entry.event || entry.name || 'Pattern'}</div>
                    <div class="engine-meta ${value > 0 ? 'good' : 'bad'}">${value > 0 ? '+' : ''}${(value * 100).toFixed(1)}%</div>
                </div>
                <div class="subtle">${entry.description || ''}</div>
            </div>
        `;
    }).join('')}</div>`;
}

function shortDateLabel(value) {
    if (!value) return 'now';
    const dt = new Date(value);
    if (Number.isNaN(dt.getTime())) return String(value);
    return dt.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function parseDateMs(value) {
    if (!value) return null;
    const dt = new Date(value);
    return Number.isNaN(dt.getTime()) ? null : dt.getTime();
}

function shouldPollRemote() {
    if (!readToken()) return false;
    const selectedMs = parseDateMs(state.selectedDateTime);
    if (selectedMs == null) return true;
    return Math.abs(Date.now() - selectedMs) <= REMOTE_POLL_LIVE_WINDOW_MS;
}

function syncPollingState() {
    const active = shouldPollRemote();
    state.backend.polling.active = active;
    state.backend.polling.intervalMs = REMOTE_POLL_INTERVAL_MS;
    state.backend.polling.nextAt = active ? new Date(Date.now() + REMOTE_POLL_INTERVAL_MS).toISOString() : null;
}

function triggerPhase(trigger) {
    const triggerMs = parseDateMs(trigger?.date || trigger?.datetime || trigger?.timestamp);
    const snapshotMs = parseDateMs(state.snapshot?.date);
    if (triggerMs == null || snapshotMs == null) return 'future';
    const deltaHours = (triggerMs - snapshotMs) / 3600000;
    if (Math.abs(deltaHours) <= 18) return 'active';
    if (deltaHours < 0) return 'past';
    return 'future';
}

function eventRank(event) {
    const snapshotMs = parseDateMs(state.snapshot?.date);
    const eventMs = parseDateMs(event?.date || event?.datetime || event?.timestamp);
    if (snapshotMs == null || eventMs == null) return Number.POSITIVE_INFINITY;
    const delta = eventMs - snapshotMs;
    if (Math.abs(delta) <= 18 * 3600000) return 0;
    if (delta > 0) return delta;
    return Math.abs(delta) + 365 * 24 * 3600000;
}

function formatSignedMetric(value) {
    const numeric = Number(value);
    if (!Number.isFinite(numeric)) return 'n/a';
    const rounded = Math.round(numeric * 100) / 100;
    return `${rounded > 0 ? '+' : ''}${rounded.toFixed(2)}`;
}

function compactUsd(value) {
    const amount = Number(value);
    if (!Number.isFinite(amount)) return 'n/a';
    const absolute = Math.abs(amount);
    if (absolute >= 1e12) return `${amount < 0 ? '-' : ''}$${(absolute / 1e12).toFixed(2)}T`;
    if (absolute >= 1e9) return `${amount < 0 ? '-' : ''}$${(absolute / 1e9).toFixed(2)}B`;
    if (absolute >= 1e6) return `${amount < 0 ? '-' : ''}$${(absolute / 1e6).toFixed(2)}M`;
    return `${amount < 0 ? '-' : ''}$${absolute.toFixed(0)}`;
}

function marketVoiceCards() {
    const overlay = state.backend.marketOverlay;
    if (!overlay?.connected) return [];
    // Dear future model: if the tape looks haunted, start with the payload, not the moon.
    const regime = overlay.regime;
    const thesis = overlay.thesis;
    const scorecard = overlay.scorecard;
    const topSector = overlay.sectorFlows?.bySector?.[0] || null;
    const topSectorDetail = topSectorProfile(overlay);
    const topLever = overlay.moneyMap?.levers?.[0] || null;
    const topPattern = (overlay.activePatterns || []).find((item) => item.actionable) || overlay.activePatterns?.[0] || null;
    const topRedFlag = overlay.crossReference?.redFlags?.[0] || null;
    const topLeader = scorecard?.leaders?.[0] || null;
    const topLaggard = scorecard?.laggards?.[0] || null;
    const scorecardSummary = scorecard?.summary || null;

    return [
        regime ? {
            label: 'regime',
            value: String(regime.state || 'uncalibrated').toLowerCase().replaceAll('_', ' '),
            detail: `${Math.round((regime.confidence || 0) * 100)}% / ${regime.posture || regime.baselineComparison || 'state live'}`,
            tone: /growth|bull|risk[-_ ]?on|expansion/i.test(regime.state || '') ? 'good' : /crisis|fragile|bear|risk[-_ ]?off/i.test(regime.state || '') ? 'bad' : 'warn',
        } : null,
        thesis ? {
            label: 'thesis',
            value: String(thesis.overallDirection || 'neutral').toLowerCase(),
            detail: thesis.keyDrivers?.[0]?.label || thesis.narrative || 'market voice live',
            tone: /bull/i.test(thesis.overallDirection || '') ? 'good' : /bear/i.test(thesis.overallDirection || '') ? 'bad' : 'warn',
        } : null,
        topSector ? {
            label: 'flow',
            value: `${topSector.sector} ${topSector.netFlow >= 0 ? 'bid' : 'drain'}`,
            detail: topSectorDetail?.topActor?.name
                ? `${topSectorDetail.topActor.name} / ${compactUsd(topSector.netFlow)}`
                : `${compactUsd(topSector.netFlow)} / ${topSector.acceleration || topSector.direction}`,
            tone: topSector.netFlow >= 0 ? 'good' : 'bad',
        } : topLever ? {
            label: 'flow',
            value: topLever.label,
            detail: topLever.detail || 'lever active',
            tone: 'warn',
        } : null,
        scorecardSummary ? {
            label: 'basket',
            value: `${scorecardSummary.bias || 'wait'} / ${topLeader?.symbol || 'mixed'}`,
            detail: topLeader
                ? `${topLeader.symbol} ${topLeader.trend || 'live'} / weak ${topLaggard?.symbol || 'n/a'}`
                : `${scorecardSummary.available || 0}/${scorecardSummary.total || 0} covered`,
            tone: (scorecardSummary.compositeScore || 0) > 0 ? 'good' : (scorecardSummary.compositeScore || 0) < 0 ? 'bad' : 'warn',
        } : null,
        topRedFlag ? {
            label: 'truth',
            value: topRedFlag.label,
            detail: topRedFlag.category || 'red flag',
            tone: 'bad',
        } : topPattern ? {
            label: 'pattern',
            value: `${topPattern.ticker ? `${topPattern.ticker} / ` : ''}${topPattern.pattern}`,
            detail: topPattern.nextExpected || 'active',
            tone: topPattern.actionable ? 'good' : 'warn',
        } : null,
    ].filter(Boolean);
}

function marketVoiceMarkup() {
    const cards = marketVoiceCards();
    if (!cards.length) {
        return `<div class="empty">No market overlay.</div>`;
    }
    return `<div class="event-list">${cards.map((card) => `
        <div class="event-card">
            <div class="engine-head">
                <div class="engine-name">${card.label}</div>
                <div class="engine-meta ${card.tone}">${card.value}</div>
            </div>
            <div class="subtle">${card.detail}</div>
        </div>
    `).join('')}</div>`;
}

function currentEventStream() {
    const liveEvents = state.backend.timeline.slice(0, 6);
    if (liveEvents.length) {
        return liveEvents.slice().sort((a, b) => eventRank(a) - eventRank(b)).slice(0, 6);
    }
    if (Array.isArray(state.snapshot?.events) && state.snapshot.events.length) {
        return state.snapshot.events.slice().sort((a, b) => eventRank(a) - eventRank(b)).slice(0, 6);
    }
    return state.snapshot ? buildLocalEvents(state.snapshot) : [];
}

function topAspect(snapshot) {
    if (!snapshot?.aspects?.length) return null;
    return snapshot.aspects
        .slice()
        .sort((a, b) => (a.orb_used ?? 99) - (b.orb_used ?? 99))[0];
}

function actionVerb() {
    const bias = Number(state.seer?.signal_bias ?? 0);
    const stress = Number(state.snapshot?.signals?.planetaryStress ?? 0);
    const retrogrades = Number(state.snapshot?.signals?.retrogradeCount ?? 0);
    const conflicts = (state.seer?.conflicts || []).length;

    if (conflicts >= 2 || retrogrades >= 2) return 'wait';
    if (bias >= 0.35 && stress <= 4) return 'press';
    if (bias <= -0.35) return 'hedge';
    if (stress >= 6) return 'fade';
    return 'probe';
}

function actionRule(action, trigger, aspect) {
    const triggerName = trigger?.name || 'the next window';
    const phase = triggerPhase(trigger);
    if (action === 'press') {
        if (aspect) {
            return phase === 'active'
                ? `only while ${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2} holds through ${triggerName}`
                : `only while ${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2} holds`;
        }
        return phase === 'active' ? `press only through ${triggerName}` : `only into ${triggerName}`;
    }
    if (action === 'hedge') {
        if (phase === 'active') return `protect through ${triggerName}`;
        if (phase === 'past') return `protect after ${triggerName}`;
        return `protect into ${triggerName}`;
    }
    if (action === 'fade') {
        if (phase === 'active') return `sell extension at ${triggerName}`;
        if (phase === 'past') return `sell rebound after ${triggerName}`;
        return `sell extension near ${triggerName}`;
    }
    if (action === 'wait') {
        if (phase === 'active') return `stand down through ${triggerName}`;
        if (phase === 'past') return `stand down after ${triggerName}`;
        return `stand down until ${triggerName}`;
    }
    if (phase === 'active') return `small size through ${triggerName}`;
    if (phase === 'past') return `small size after ${triggerName}`;
    return `small size until ${triggerName}`;
}

function firstCrossReferenceFlag() {
    return state.backend.marketOverlay?.crossReference?.redFlags?.[0] || null;
}

function firstTruthCheck() {
    return state.backend.marketOverlay?.crossReference?.checks?.[0] || null;
}

function buildInvalidationLine(trigger, aspect, leadHypothesis) {
    const fracture = (state.seer?.fracture_points || [])[0];
    if (fracture) return fracture;

    const redFlag = firstCrossReferenceFlag();
    if (redFlag?.label) {
        return redFlag.category ? `${redFlag.category}: ${redFlag.label}` : redFlag.label;
    }

    if (leadHypothesis?.bias === 'press') {
        if (state.snapshot?.void_of_course?.is_void) {
            return 'void seam holds';
        }
        if (aspect?.aspect_type && ['square', 'opposition'].includes(aspect.aspect_type)) {
            return `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2} stays hard`;
        }
        return 'bias flips or tape rolls';
    }

    if (leadHypothesis?.bias === 'hedge' || leadHypothesis?.bias === 'trim') {
        return 'pressure clears';
    }

    if (trigger?.name || trigger?.event) {
        return `${trigger.name || trigger.event} clears without follow-through`;
    }

    return 'field fails to align';
}

function buildTriggerLine(trigger, aspect, leadHypothesis) {
    if (leadHypothesis?.cue) return compactDirectiveLine(leadHypothesis.cue, 56);
    if (trigger?.name || trigger?.event) {
        return `${trigger.name || trigger.event} / ${shortDateLabel(trigger.date || trigger.datetime)}`;
    }
    if (aspect) {
        return `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`;
    }
    return 'await a cleaner print';
}

function buildOmenLine(leadHypothesis, aspect) {
    if (leadHypothesis?.title && leadHypothesis?.cue) {
        return `${leadHypothesis.title} / ${leadHypothesis.cue}`;
    }
    if (leadHypothesis?.title) return leadHypothesis.title;
    if (aspect) return `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`;
    return state.seer?.reading || 'field unresolved';
}

function buildTradeLine(action, actionDetail) {
    const primaryBranch = state.seer?.primary_branch || null;
    if (primaryBranch?.statement) return compactDirectiveLine(primaryBranch.statement, 64);
    return compactDirectiveLine(actionDetail || action || 'wait for alignment', 64);
}

function buildRiskLine() {
    const redFlag = firstCrossReferenceFlag();
    if (redFlag?.label) return compactDirectiveLine(redFlag.label, 56);
    const conflict = (state.seer?.conflicts || [])[0];
    if (conflict?.engine_id) return `${conflict.engine_id} disagrees`;
    const check = firstTruthCheck();
    if (check?.status && check?.label) return `${check.status}: ${check.label}`;
    return compactDirectiveLine(state.seer?.contradiction_note || 'risk contained', 56);
}

function buildForecastCards() {
    if (!state.snapshot || !state.seer) return [];
    const eventStream = currentEventStream();
    const trigger = eventStream[0] || null;
    const aspect = topAspect(state.snapshot);
    const bias = Number(state.seer.signal_bias ?? 0);
    const leadHypothesis = buildAstrogridHypotheses(state.snapshot, state.seer, state.backend.marketOverlay)[0] || null;
    const action = leadHypothesis?.bias || actionVerb();
    const actionDetail = leadHypothesis?.act || actionRule(action, trigger, aspect);
    const windowLabel = leadHypothesis?.cue?.split(' / ')[0] || trigger?.name || leadHypothesis?.title || state.snapshot.lunar.phase_name;
    const windowDetail = shortDateLabel(leadHypothesis?.window || trigger?.date || state.snapshot.date);
    const marketCards = marketVoiceCards();
    const thesisCard = marketCards.find((card) => card.label === 'thesis')
        || marketCards.find((card) => card.label === 'regime')
        || marketCards.find((card) => card.label === 'basket')
        || null;
    const triggerDetail = buildTriggerLine(trigger, aspect, leadHypothesis);
    const invalidationDetail = buildInvalidationLine(trigger, aspect, leadHypothesis);
    const omenDetail = buildOmenLine(leadHypothesis, aspect);
    const tradeDetail = buildTradeLine(action, actionDetail);
    const riskDetail = buildRiskLine();

    return [
        {
            sigil: bias >= 0.25 ? '▲' : bias <= -0.25 ? '▼' : '◌',
            label: 'bias',
            value: thesisCard?.value || state.seer.prediction || state.seer.reading,
            detail: thesisCard?.detail || (aspect ? `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}` : state.snapshot.lunar.phase_name),
        },
        {
            sigil: '◔',
            label: 'window',
            value: windowLabel,
            detail: windowDetail,
        },
        {
            sigil: '✦',
            label: 'trigger',
            value: triggerDetail,
            detail: 'confirm before size',
        },
        {
            sigil: '╳',
            label: 'invalidation',
            value: invalidationDetail,
            detail: 'cut if this prints',
        },
        {
            sigil: '☉',
            label: 'omen',
            value: omenDetail,
            detail: state.seer.reading,
        },
        {
            sigil: '⟡',
            label: 'trade',
            value: action,
            detail: tradeDetail,
        },
        {
            sigil: '⚠',
            label: 'risk',
            value: riskDetail,
            detail: 'respect the break first',
        },
    ];
}

function localDirectiveSubject(leadHypothesis, aspect, snapshot) {
    if (leadHypothesis?.title && leadHypothesis.title.toLowerCase() !== 'seer cut') {
        return leadHypothesis.title.toLowerCase();
    }
    if (aspect?.planet1 && aspect?.aspect_type && aspect?.planet2) {
        return `${aspect.planet1.toLowerCase()} ${aspect.aspect_type} ${aspect.planet2.toLowerCase()}`;
    }
    if (snapshot?.void_of_course?.is_void) {
        return 'void seam';
    }
    if (snapshot?.lunar?.phase_name) {
        return snapshot.lunar.phase_name.toLowerCase();
    }
    return 'field alignment';
}

function liveDirectiveContext(overlay) {
    const scorecard = overlay?.scorecard || null;
    const summary = scorecard?.summary || null;
    const leader = scorecard?.leaders?.[0] || null;
    const laggard = scorecard?.laggards?.[0] || null;
    const leaderContext = leader ? tickerSectorContext(overlay, leader.symbol) : null;
    const laggardContext = laggard ? tickerSectorContext(overlay, laggard.symbol) : null;
    const topSector = overlay?.sectorFlows?.bySector?.[0] || null;
    const topSectorDetail = topSectorProfile(overlay);
    const regime = overlay?.regime || null;
    const thesis = overlay?.thesis || null;

    if (!summary || !leader) return null;

    const callSubject = laggard?.symbol
        ? `${leader.symbol} / avoid ${laggard.symbol}`
        : leader.symbol;
    const setupParts = [
        leader ? `${leader.symbol} ${leader.trend || 'live'}` : null,
        leaderContext?.sector || topSector?.sector || null,
        leaderContext?.actor?.name || topSectorDetail?.topActor?.name || null,
        regime?.state ? String(regime.state).toLowerCase().replaceAll('_', ' ') : null,
    ].filter(Boolean);
    const noteParts = [
        summary.bias ? `basket ${summary.bias}` : null,
        thesis?.keyDrivers?.[0]?.label || null,
        laggardContext?.actor?.name ? `avoid ${laggardContext.actor.name}` : laggard?.symbol ? `avoid ${laggard.symbol}` : null,
    ].filter(Boolean);

    return {
        action: summary.bias || null,
        callSubject,
        setup: setupParts.join(' / '),
        note: noteParts.join(' / '),
    };
}

function buildOracleDirective() {
    if (!state.snapshot || !state.seer) return null;

    const eventStream = currentEventStream();
    const trigger = eventStream[0] || null;
    // If this starts speaking in tongues again, check the leader/laggard branch before inventing theology.
    const hypotheses = buildAstrogridHypotheses(state.snapshot, state.seer, state.backend.marketOverlay);
    const leadHypothesis = hypotheses[0] || null;
    const concreteHypothesis = hypotheses.find((item) => String(item?.title || '').toLowerCase() !== 'seer cut') || leadHypothesis;
    const overlay = state.backend.marketOverlay;
    const liveContext = overlay?.connected ? liveDirectiveContext(overlay) : null;
    const aspect = topAspect(state.snapshot);
    const action = liveContext?.action
        ? liveContext.action
        : overlay?.scorecard?.leaders?.[0]
        ? (leadHypothesis?.bias || actionVerb())
        : (concreteHypothesis?.bias || leadHypothesis?.bias || actionVerb());
    const triggerDetail = buildTriggerLine(trigger, aspect, leadHypothesis);
    const invalidationDetail = buildInvalidationLine(trigger, aspect, leadHypothesis);
    const marketLine = liveContext?.callSubject
        || overlay?.regime?.state?.toLowerCase()
        || localDirectiveSubject(concreteHypothesis, aspect, state.snapshot);
    const windowLabel = leadHypothesis?.window || trigger?.date || state.snapshot.date || 'now';

    const noteLine = liveContext?.note || buildTradeLine(action, leadHypothesis?.act || actionRule(action, trigger, topAspect(state.snapshot)));

    return {
        action,
        call: compactDirectiveLine(`${action} ${marketLine}`.trim(), 64),
        timing: compactDirectiveLine(`${shortDateLabel(windowLabel)} / ${trigger?.name || leadHypothesis?.title || state.snapshot.lunar.phase_name}`, 60),
        setup: compactDirectiveLine(liveContext?.setup || leadHypothesis?.cue || triggerDetail, 72),
        cut: compactDirectiveLine(invalidationDetail, 72),
        note: compactDirectiveLine(noteLine, 68),
    };
}

function currentOracleRecord(directive) {
    if (!directive) return null;
    return {
        at: new Date().toISOString(),
        date: state.snapshot?.date || state.selectedDateTime,
        live: Boolean(state.backend.marketOverlay?.connected),
        call: directive.call,
        timing: directive.timing,
        setup: directive.setup,
        invalidation: directive.cut,
        note: directive.note,
        score: 'pending',
    };
}

async function submitPredictionRecord(directive) {
    if (!directive || !readToken() || !state.snapshot || !state.seer) {
        return null;
    }
    const liveOrLocal = state.backend.marketOverlay?.connected
        ? 'live'
        : (state.archive ? 'archive' : 'local');
    try {
        const prediction = await fetchJson('/api/v1/astrogrid/predictions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                question: state.question,
                call: directive.call,
                timing: directive.timing,
                setup: directive.setup,
                invalidation: directive.cut,
                note: directive.note,
                mode: state.mode,
                lens_ids: state.activeLensIds,
                snapshot: state.snapshot,
                seer: state.seer,
                engine_outputs: state.engineOutputs,
                market_overlay_snapshot: state.backend.marketOverlay,
                target_universe: 'hybrid',
                horizon_label: /week|cycle/i.test(state.seer?.horizon || '') ? 'macro' : 'swing',
                live_or_local: liveOrLocal,
                publish_oracle: true,
            }),
        });
        state.latestPrediction = prediction;
        writeLogs(LOG_KEYS.oracle, {
            at: new Date().toISOString(),
            question: state.question,
            call: prediction.call,
            timing: prediction.timing,
            invalidation: prediction.invalidation,
            score: prediction.postmortem?.state || prediction.status || 'pending',
            predictionId: prediction.prediction_id,
        });
        return prediction;
    } catch (error) {
        reportRuntimeEvent('warn', 'prediction_submit_failed', {
            detail: String(error?.message || error),
            status: error?.status || null,
        });
        return null;
    }
}

function oracleBriefMarkup(directive) {
    if (!directive) {
        return '<div class="empty">Awaiting directive.</div>';
    }

    const meta = [
        state.mode,
        formatLensList(state.activeLensIds),
        state.seer?.confidence_band || null,
        state.seer?.horizon || null,
    ].filter(Boolean).join(' / ');

    return `
        <div class="oracle-directive">
            <div class="oracle-directive-row oracle-directive-row-lead">
                <span class="oracle-directive-label">call</span>
                <div class="oracle-directive-body oracle-directive-body-lead">${directive.call}</div>
            </div>
            <div class="oracle-directive-row">
                <span class="oracle-directive-label">timing</span>
                <div class="oracle-directive-body">${directive.timing}</div>
            </div>
            <div class="oracle-directive-row">
                <span class="oracle-directive-label">setup</span>
                <div class="oracle-directive-body">${directive.setup}</div>
            </div>
            <div class="oracle-directive-row">
                <span class="oracle-directive-label">invalid</span>
                <div class="oracle-directive-body">${directive.cut}</div>
            </div>
            ${directive.note ? `
                <div class="oracle-directive-row">
                    <span class="oracle-directive-label">note</span>
                    <div class="oracle-directive-body">${directive.note}</div>
                </div>
            ` : ''}
            ${meta ? `<div class="oracle-directive-meta">${meta}</div>` : ''}
        </div>
    `;
}

function buildHorizonCards() {
    const liveHorizon = String(state.seer?.horizon || 'cycles');
    const macroActive = /weeks|cycles/.test(liveHorizon) || Boolean(state.backend.marketOverlay?.connected);
    const swingActive = /days|weeks/.test(liveHorizon);
    return [
        {
            label: 'macro',
            status: macroActive ? 'active' : 'watch',
            detail: state.backend.marketOverlay?.regime?.state || 'regime overlay',
        },
        {
            label: 'swing',
            status: swingActive ? 'active' : 'watch',
            detail: liveHorizon === 'days' ? 'current primary horizon' : 'event windows',
        },
        {
            label: 'intraday',
            status: 'planned',
            detail: 'feed not wired yet',
        },
    ];
}

function horizonMarkup() {
    const cards = buildHorizonCards();
    return `<div class="horizon-grid">${cards.map((card) => `
        <div class="horizon-card ${card.status}">
            <div class="forecast-label">${card.label}</div>
            <div class="forecast-value">${card.status}</div>
            <div class="forecast-detail">${card.detail}</div>
        </div>
    `).join('')}</div>`;
}

function renderClaimMarkup(claims = []) {
    if (!claims.length) {
        return '';
    }
    return `<div class="claim-list">${claims.slice(0, 3).map((claim) => `
        <div class="claim-card">
            <div class="engine-head">
                <div class="engine-name">${claim.topic}</div>
                <div class="engine-meta">${claim.timeframe} / ${claim.direction}</div>
            </div>
            <div>${claim.statement}</div>
            <div class="seer-support">cue: ${claim.basis}</div>
        </div>
    `).join('')}</div>`;
}

function engineMarkup() {
    return `<div class="engine-list">${state.engineOutputs.map((engine) => `
        <div class="engine-card">
            <div class="engine-head">
                <div class="engine-name">${engine.engine_name}</div>
                <div class="engine-meta">${engine.confidence} / ${engine.horizon}</div>
            </div>
            <div class="seer-reading-mini">${engine.prediction}</div>
            <div class="seer-support">cue: ${(engine.feature_trace?.top_factors || []).slice(0, 2).join(' / ') || 'none'}</div>
            <div class="seer-support">time: ${engine.correspondence?.calendar || engine.correspondence?.ritual_window || 'none'}</div>
            <div class="seer-conflicts">risk: ${(engine.contradictions || []).slice(0, 2).join(' / ') || 'clear'}</div>
        </div>
    `).join('')}</div>`;
}

function logsMarkup() {
    const seerLogs = readLogs(LOG_KEYS.seer).slice(0, 5);
    const personaLogs = readLogs(LOG_KEYS.persona).slice(0, 5);
    const oracleLogs = readLogs(LOG_KEYS.oracle).slice(0, 5);
    const runtimeLogs = readLogs(LOG_KEYS.runtime).slice(0, 5);
    return `
        <div class="log-list">
            ${runtimeLogs.map((log) => `
                <div class="log-card">
                    <div class="engine-head">
                        <div class="engine-name">Runtime</div>
                        <div class="engine-meta">${new Date(log.at).toLocaleString()}</div>
                    </div>
                    <div>${log.event}</div>
                    <div class="seer-support">level: ${log.level}</div>
                    <div class="seer-conflicts">${compactDirectiveLine(JSON.stringify(log.detail || {}), 120)}</div>
                </div>
            `).join('')}
            ${oracleLogs.map((log) => `
                <div class="log-card">
                    <div class="engine-head">
                        <div class="engine-name">Oracle</div>
                        <div class="engine-meta">${new Date(log.at).toLocaleString()}</div>
                    </div>
                    <div>${log.call}</div>
                    <div class="seer-support">timing: ${log.timing}</div>
                    <div class="seer-support">invalid: ${log.invalidation}</div>
                    <div class="seer-conflicts">score: ${log.score}</div>
                </div>
            `).join('')}
            ${seerLogs.map((log) => `
                <div class="log-card">
                    <div class="engine-head">
                        <div class="engine-name">Seer</div>
                        <div class="engine-meta">${new Date(log.at).toLocaleString()}</div>
                    </div>
                    <div>${log.reading}</div>
                    <div class="seer-support">${log.prediction}</div>
                </div>
            `).join('')}
            ${personaLogs.map((log) => `
                <div class="log-card">
                    <div class="engine-head">
                        <div class="engine-name">${log.personaId}</div>
                        <div class="engine-meta">${new Date(log.at).toLocaleString()}</div>
                    </div>
                    <div class="seer-support">${log.question}</div>
                    <div>${log.answer}</div>
                </div>
            `).join('')}
        </div>
    `;
}

function worldMarkup() {
    const world = enrichWorldModel(buildSeedWorldModel(), state.snapshot, state.seer, state.backend.marketOverlay);
    const focusNode = world.nodes.find((node) => node.id === state.worldFocusId) || world.nodes[0] || null;
    const connectedEdges = focusNode
        ? world.edges.filter((edge) => edge.source === focusNode.id || edge.target === focusNode.id)
        : [];
    const tags = focusNode?.tags?.length ? focusNode.tags.join(' / ') : 'none';

    return `
        ${createWorldAtlas(world, { selectedNodeId: focusNode?.id || '' })}
        ${focusNode ? `
            <div class="hero-meta-grid" style="margin-top:14px;">
                <div class="hero-meta-card">
                    <div class="section-label">focus</div>
                    <div class="hero-branch-line">${focusNode.name}</div>
                    <div class="subtle">${focusNode.metrics?.headline || `${String(focusNode.scale || 'unknown').replaceAll('_', ' ')} / ${String(focusNode.type || 'node').replaceAll('_', ' ')}`}</div>
                </div>
                <div class="hero-meta-card">
                    <div class="section-label">score</div>
                    <div class="hero-branch-line">${formatSignedMetric(focusNode.metrics?.score)}</div>
                    <div class="subtle">${focusNode.metrics?.signal || String(focusNode.type || 'node').replaceAll('_', ' ')}</div>
                </div>
                <div class="hero-meta-card">
                    <div class="section-label">attached flows</div>
                    <div class="subtle">${connectedEdges.length ? connectedEdges.slice(0, 3).map((edge) => `${edge.meta?.label || edge.type}: ${edge.metrics?.headline || edge.metrics?.signal || 'unscored'}`).join(' / ') : 'none'}</div>
                </div>
                <div class="hero-meta-card">
                    <div class="section-label">detail</div>
                    <div class="subtle">${focusNode.metrics?.detail || tags}</div>
                </div>
                <div class="hero-meta-card">
                    <div class="section-label">window</div>
                    <div class="subtle">${focusNode.metrics?.window || focusNode.parentId || 'root'}</div>
                </div>
            </div>
        ` : ''}
    `;
}

function prophecyMarkup(prophecy) {
    if (!prophecy) {
        return '';
    }
    const threads = (prophecy.threads || []).slice(0, 2);
    const firstWarning = (prophecy.seer?.warnings || [])[0] || 'none';
    return `
        <div class="seer-llm-shell">
            <div class="engine-head">
                <div class="engine-name">model cut</div>
                <div class="engine-meta">${prophecy.used_llm ? 'llm' : 'fallback'} / ${prophecy.backend || 'none'}</div>
            </div>
            <div class="forecast-grid forecast-grid-compact">
                <div class="forecast-card">
                    <div class="forecast-sigil">✶</div>
                    <div class="forecast-label">cut</div>
                    <div class="forecast-value">${prophecy.summary || 'No cut.'}</div>
                </div>
                <div class="forecast-card">
                    <div class="forecast-sigil">⟡</div>
                    <div class="forecast-label">move</div>
                    <div class="forecast-value">${prophecy.seer?.prediction || 'hold'}</div>
                </div>
                <div class="forecast-card">
                    <div class="forecast-sigil">╳</div>
                    <div class="forecast-label">risk</div>
                    <div class="forecast-value">${firstWarning}</div>
                </div>
            </div>
            ${threads.length ? `<div class="seer-support">thread: ${threads.map((thread) => thread.title || 'thread').join(' / ')}</div>` : ''}
        </div>
    `;
}

function hypothesesMarkup(limit = null) {
    const hypotheses = buildAstrogridHypotheses(state.snapshot, state.seer, state.backend.marketOverlay);
    if (!hypotheses.length) {
        return '<div class="empty">No hypothesis field.</div>';
    }
    const visible = limit ? hypotheses.slice(0, limit) : hypotheses;
    return `<div class="hypothesis-grid">${visible.map((item) => `
        <div class="hypothesis-card">
            <div class="engine-head">
                <div class="engine-name">${item.title}</div>
                <div class="engine-meta">${item.window}</div>
            </div>
            <div class="forecast-row">
                <div class="forecast-sigil">${item.sigil}</div>
                <div>
                    <div class="hypothesis-bias">${item.bias}</div>
                    <div class="hypothesis-act">${compactDirectiveLine(item.act, 88)}</div>
                </div>
            </div>
            <div class="seer-support">${compactDirectiveLine(item.cue, 64)}</div>
        </div>
    `).join('')}</div>`;
}

function oracleStateMarkup(nextEvent) {
    if (!state.snapshot) {
        return '<div class="empty">Awaiting sky.</div>';
    }
    const source = state.backend.connected ? 'remote' : state.archive ? 'archive + local' : 'local';
    const universeCounts = state.backend.marketOverlay?.universe?.counts || null;
    const evidenceLine = universeCounts
        ? `${universeCounts.scoreable_now || 0} scoreable / ${universeCounts.degraded || 0} degraded`
        : 'coverage unknown';
    return `
        <div class="oracle-strip oracle-state-brief">
            <div class="oracle-strip-head">
                <div class="section-label">field</div>
                <div class="ag-summary-date">${state.snapshot.date}</div>
            </div>
            <div class="oracle-state-grid">
                <div class="oracle-state-item">
                    <span>source</span>
                    <strong>${source}</strong>
                </div>
                <div class="oracle-state-item">
                    <span>phase</span>
                    <strong>${state.snapshot.lunar.phase_name}</strong>
                </div>
                <div class="oracle-state-item oracle-state-item-wide">
                    <span>knot</span>
                    <strong>${state.snapshot.nakshatra.nakshatra_name}</strong>
                </div>
                <div class="oracle-state-item oracle-state-item-wide">
                    <span>next</span>
                    <strong>${nextEvent ? `${nextEvent.name || nextEvent.event} / ${shortDateLabel(nextEvent.date || nextEvent.datetime)}` : 'none'}</strong>
                </div>
                <div class="oracle-state-item oracle-state-item-wide">
                    <span>evidence</span>
                    <strong>${evidenceLine}</strong>
                </div>
            </div>
        </div>
    `;
}

function predictionPostmortemMarkup(prediction) {
    if (!prediction?.postmortem) {
        return '<div class="empty">No postmortem logged yet.</div>';
    }
    const targetStatuses = prediction?.market_overlay_snapshot?.scorecard?.target_statuses || [];
    const evidenceLine = targetStatuses.length
        ? targetStatuses.map((item) => `${item.symbol}:${item.status}`).join(' / ')
        : prediction.scoring_class || 'unknown';
    return `
        <div class="engine-card oracle-response-card">
            <div class="engine-head">
                <div class="engine-name">Postmortem</div>
                <div class="engine-meta">${prediction.postmortem.state || prediction.status || 'pending'}</div>
            </div>
            <div class="seer-support">id: ${prediction.prediction_id || 'pending'}</div>
            <div>${compactDirectiveLine(prediction.postmortem.summary || 'Pending review.', 220)}</div>
            <div class="seer-support">break: ${compactDirectiveLine(prediction.postmortem.invalidation_rule || prediction.invalidation || 'n/a', 140)}</div>
            <div class="seer-support">evidence: ${compactDirectiveLine(evidenceLine, 120)}</div>
            <div class="seer-conflicts">grid: ${(prediction.postmortem.dominant_grid_drivers || []).slice(0, 3).join(' / ') || 'thin'} | mystical: ${(prediction.postmortem.dominant_mystical_drivers || []).slice(0, 3).join(' / ') || 'thin'}</div>
        </div>
    `;
}

function buildVaultMystery(snapshot) {
    if (!snapshot) return null;
    const lunarPhase = snapshot?.lunar?.phase_name || 'Unknown Phase';
    const nakshatra = snapshot?.nakshatra?.nakshatra_name || 'Unknown Mansion';
    const aspect = topAspect(snapshot);
    const events = currentEventStream().slice(0, 3);
    const event = events[0] || null;
    const kp = Number(snapshot?.space_weather?.kp_index ?? snapshot?.signals?.spaceWeather?.kpIndex ?? snapshot?.signals?.retrogradeCount ?? 0);
    const solarWind = Number(snapshot?.space_weather?.solar_wind_km_s ?? snapshot?.signals?.spaceWeather?.solarWindKmS ?? snapshot?.aspects?.length ?? 0);
    const lunarAge = Number(snapshot?.lunar?.age_days ?? 0);
    const phaseWord = String(lunarPhase).split(' ')[0].toLowerCase();
    const nakshatraWord = String(nakshatra).replace(/[^a-z0-9]/gi, '').slice(0, 6).toUpperCase() || 'VEIL';
    const aspectWord = aspect ? `${String(aspect.planet1).slice(0, 2)}${String(aspect.aspect_type).slice(0, 2)}${String(aspect.planet2).slice(0, 2)}`.toUpperCase() : 'SKY';
    const eventWord = event?.name ? String(event.name).split(' ')[0].toUpperCase() : 'GATE';
    const sigil = `${phaseWord}.${nakshatraWord}.${aspectWord}.${eventWord}`.replace(/\.+/g, '.');
    const veiledSigil = sigil
        .split('.')
        .filter(Boolean)
        .map((part) => `${part.slice(0, 1)}${'•'.repeat(Math.max(part.length - 1, 2))}`)
        .join(' · ');
    const liveRotation = state.backend.polling.active
        ? Math.floor(Date.now() / (5 * 60 * 1000))
        : Math.floor((parseDateMs(snapshot.date) || 0) / 3600000);
    const liveShard = liveRotation.toString(36).toUpperCase();
    const stateSeal = [
        `K${(Math.round(kp * 10) ^ (liveRotation & 0xff)).toString(16).toUpperCase()}`,
        `S${(Math.round(solarWind) ^ ((liveRotation >> 1) & 0x1ff)).toString(16).toUpperCase()}`,
        `L${(Math.round(lunarAge * 100) ^ ((liveRotation >> 2) & 0x3ff)).toString(16).toUpperCase()}`,
    ].join('-');
    const baseLocks = [
        phaseWord.slice(0, 3).toUpperCase() || 'PHS',
        nakshatraWord.slice(-3) || 'VEI',
        aspectWord.slice(0, 4) || 'SKY',
        ...(events.map((item) => String(item?.name || item?.event || 'gate').replace(/[^a-z0-9]/gi, '').slice(0, 4).toUpperCase()).filter(Boolean)),
    ];
    const shift = baseLocks.length ? liveRotation % baseLocks.length : 0;
    const locks = baseLocks.slice(shift).concat(baseLocks.slice(0, shift)).slice(0, 5).map((lock, index) => {
        const salt = (liveShard[index] || liveShard[0] || 'X');
        return `${lock}${salt}`;
    });
    const witnesses = events.map((item, index) => ({
        mark: `w${index + 1}`,
        label: `witness ${index + 1}`,
        when: shortDateLabel(item?.date || item?.datetime || snapshot.date),
    }));

    return {
        sigil: veiledSigil,
        title: 'Witness Shards',
        riddle: `Five shards drift. Three witnesses remain. ${lunarPhase.toLowerCase()} brushes ${nakshatra.toLowerCase()} while ${aspect ? `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}` : 'the field keeps its mouth shut'}. The seal turns when the window turns.`,
        clue: `The cipher stays withheld. These shards do not disclose order, value, or terminal form. Witnesses attest drift only. The state seal proves alignment for a breath, then rots. Without the unreleased cipher, the chamber stays closed.`,
        window: event ? `${event.name || event.event} / ${shortDateLabel(event.date || event.datetime)}` : snapshot.date,
        locks,
        witnesses,
        stateSeal,
    };
}

function render() {
    const app = document.getElementById('app');
    const activePersona = PERSONAS.find((persona) => persona.id === state.personaId);
    const trajectoryBodies = availableTrajectoryBodies();
    const tokenMeta = decodeTokenMeta(readToken());
    const seerFactors = state.seer?.key_factors || [];
    const forecastCards = buildForecastCards();
    const snapshotSummary = state.snapshot
        ? `${state.snapshot.lunar.phase_name} / ${state.snapshot.aspects.length} aspects / ${state.snapshot.nakshatra.nakshatra_name}`
        : 'Awaiting sky.';
    const tokenSummary = tokenMeta
        ? `${tokenMeta.username || 'user'} / ${tokenMeta.role || 'role'} / ${tokenMeta.expired ? 'expired' : `live until ${tokenMeta.expiresAt.toLocaleString()}`}`
        : (sameOriginApiBase() ? 'shared session absent' : 'none');
    const operatorSummary = `${state.mode} / ${state.activeLensIds.length} lenses / ${state.backend.connected ? 'remote' : 'local'}`;
    const sharedSessionMode = sameOriginApiBase();
    const sharedSessionActive = hasSharedSession();
    const showDebugSessionControls = !sharedSessionMode;
    const nextEvent = currentEventStream()[0] || null;
    const directive = buildOracleDirective();
    const mystery = buildVaultMystery(state.snapshot);
    const pageSummary = {
        oracle: 'seer / state / hypotheses',
        observatory: 'vectors / bodies / aspects',
        atlas: 'world / flows / events',
        chamber: 'time / lenses / session',
    }[state.page] || 'oracle';
    const pageNav = `
        <div class="page-nav">
            ${PAGES.map((page) => `<button class="page-pill ${page.id === state.page ? 'active' : ''}" data-page="${page.id}">${page.label}</button>`).join('')}
        </div>
    `;
    const oracleQueryPanel = `
        <div class="panel oracle-query-panel">
            <div class="split-header">
                <h2>Instrument</h2>
                <div class="subtle">${activePersona ? activePersona.name : 'oracle'}</div>
            </div>
            <div class="field" style="margin-bottom:12px;">
                <span>query</span>
                <textarea id="persona-question" placeholder="Ask for the read, trigger, or invalidation.">${state.question}</textarea>
            </div>
            <div class="button-row" style="margin-bottom:10px;">
                <button class="button active" id="persona-ask">Ask</button>
                <select id="persona-select">
                    ${PERSONAS.map((persona) => `<option value="${persona.id}" ${persona.id === state.personaId ? 'selected' : ''}>${persona.name}</option>`).join('')}
                </select>
            </div>
            <div class="oracle-preset-row">
                ${ORACLE_PROMPT_PRESETS.map((prompt) => `<button class="pill" data-oracle-prompt="${prompt}">${prompt}</button>`).join('')}
            </div>
            <div class="oracle-disclaimer">
                Entertainment and research only.
            </div>
            ${oracleStateMarkup(nextEvent)}
            ${state.personaResponse ? `
                <div class="engine-card oracle-response-card">
                    <div class="engine-head">
                        <div class="engine-name">${state.personaResponse.persona_name}</div>
                        <div class="engine-meta">${state.personaResponse.mode}</div>
                    </div>
                    <div class="seer-support">lens: ${formatLensList(state.personaResponse.allowed_lenses || []) || state.personaResponse.declared_lens || 'none'}</div>
                    ${(state.personaResponse.excluded_lenses || []).length ? `<div class="seer-conflicts">excludes: ${formatLensList(state.personaResponse.excluded_lenses || [])}</div>` : ''}
                    <div>${compactDirectiveLine(state.personaResponse.answer, 220)}</div>
                </div>
            ` : '<div class="empty">Ask for a read.</div>'}
            ${predictionPostmortemMarkup(state.latestPrediction)}
        </div>
    `;
    const oraclePage = `
        <div class="oracle-grid">
            <div class="panel hero-panel oracle-hero-panel">
                <div class="split-header">
                    <h2>Oracle</h2>
                    <div class="subtle">${state.seer ? `${state.seer.confidence_band} / ${state.seer.horizon}` : 'Awaiting brief.'}</div>
                </div>
                ${state.seer ? `
                    <div class="seer-reading seer-reading-hero">${state.seer.reading}</div>
                    ${oracleBriefMarkup(directive)}
                    <div class="seer-support seer-support-hero">cue: ${seerFactors.length ? seerFactors.slice(0, 3).join(' / ') : 'none'}</div>
                ` : '<div class="empty">Awaiting voice.</div>'}
            </div>
            <div class="oracle-side">
                ${oracleQueryPanel}
                <div class="panel oracle-state-panel">
                    <div class="split-header">
                        <h2>Edges</h2>
                        <div class="subtle">${state.threads.length ? `${state.threads.length} live` : snapshotSummary}</div>
                    </div>
                    ${hypothesesMarkup(2)}
                </div>
            </div>
        </div>
    `;
    const observatoryPage = `
        <div class="stage-grid">
            <div class="panel tall">
                <div class="split-header">
                    <h2>Observatory</h2>
                    <div class="subtle">${state.snapshot ? `${state.snapshot.bodies.length} tracked bodies` : 'Awaiting sky.'}</div>
                </div>
                <div class="observatory-grid">
                    <div class="visual-shell">${state.snapshot ? createRadialSky(state.snapshot) : '<div class="empty">Awaiting sky.</div>'}</div>
                    <div class="visual-shell">${state.snapshot ? createSpacetimeField(state.snapshot) : '<div class="empty">Awaiting spacetime lattice.</div>'}</div>
                </div>
                <div class="visual-shell trajectory-shell" style="margin-top:14px;">
                    ${state.snapshot ? `
                        <div class="trajectory-toolbar">
                            <div class="button-row">
                                ${TRAJECTORY_PROJECTIONS.map((projection) => `<button class="button ${projection.id === state.vectorProjection ? 'active' : ''}" data-trajectory-projection="${projection.id}">${projection.label}</button>`).join('')}
                            </div>
                            <div class="button-row">
                                ${TRAJECTORY_HORIZONS.map((days) => `<button class="button ${days === state.vectorHorizonDays ? 'active' : ''}" data-trajectory-horizon="${days}">±${days}d</button>`).join('')}
                            </div>
                        </div>
                        <div class="trajectory-pill-row">
                            <button class="pill ${state.vectorFocusBodyId === 'all' ? 'active' : ''}" data-trajectory-focus="all">all</button>
                            ${trajectoryBodies.map((body) => `<button class="pill ${body.id === state.vectorFocusBodyId ? 'active' : ''}" data-trajectory-focus="${body.id}">${body.name}</button>`).join('')}
                        </div>
                        ${createTrajectoryAtlas(state.snapshot, {
                            projection: state.vectorProjection,
                            horizonDays: state.vectorHorizonDays,
                            stepHours: state.vectorStepHours,
                            focusedBodyId: state.vectorFocusBodyId,
                            selectedSampleKey: state.vectorSample ? `${state.vectorSample.bodyId}:${state.vectorSample.at}` : '',
                        })}
                        ${trajectoryReadoutMarkup()}
                    ` : '<div class="empty">Awaiting vector field.</div>'}
                </div>
                <div class="visual-shell" style="margin-top:14px;">${state.snapshot ? createAspectField(state.snapshot) : '<div class="empty">Awaiting aspect field.</div>'}</div>
            </div>
            <div class="stage-side">
                <div class="panel">
                    <div class="split-header">
                        <h2>Events</h2>
                        <div class="subtle">near windows</div>
                    </div>
                    ${eventsMarkup()}
                </div>
                <div class="panel">
                    <div class="split-header">
                        <h2>Objects</h2>
                        <div class="subtle">registry</div>
                    </div>
                    <div class="table-wrap">
                        ${state.snapshot ? createObjectTable(state.snapshot) : '<div class="empty">Awaiting object payload.</div>'}
                    </div>
                </div>
            </div>
        </div>
    `;
    const atlasPage = `
        <div class="hero-grid">
            <div class="panel">
                <div class="split-header">
                    <h2>Atlas</h2>
                    <div class="subtle">earth / moon / mars / orbital shells</div>
                </div>
                ${worldMarkup()}
            </div>
            <div class="hero-stack">
                <div class="panel">
                    <div class="split-header">
                        <h2>Scorecard</h2>
                        <div class="subtle">${state.backend.marketOverlay?.scorecard ? 'hybrid basket' : (state.backend.connected ? 'remote layer' : 'local layer')}</div>
                    </div>
                    ${scorecardMarkup()}
                </div>
                <div class="panel">
                    <div class="split-header">
                        <h2>Events</h2>
                        <div class="subtle">timing map</div>
                    </div>
                    ${eventsMarkup()}
                </div>
            </div>
        </div>
    `;
    const chamberPage = `
        <div class="support-grid">
            <div class="panel">
                <div class="split-header">
                    <h2>Chamber</h2>
                    <div class="subtle">${operatorSummary}</div>
                </div>
                <div class="operator-grid">
                    <div class="panel">
                        <div class="field">
                            <span>sky time</span>
                            <input id="dt-input" type="datetime-local" value="${state.selectedDateTime}">
                        </div>
                    </div>
                    <div class="panel">
                        <div class="field">
                            <span>lens mode</span>
                            <div class="button-row">
                                ${LENS_MODES.map((mode) => `<button class="button ${mode === state.mode ? 'active' : ''}" data-mode="${mode}">${mode}</button>`).join('')}
                            </div>
                        </div>
                    </div>
                    ${showDebugSessionControls ? `
                        <div class="panel">
                            <div class="field">
                                <span>oracle base</span>
                                <input id="api-base-input" type="text" value="${state.apiBaseUrl}">
                            </div>
                        </div>
                        <div class="panel">
                            <div class="field">
                                <span>session token</span>
                                <input id="api-token-input" type="password" value="${state.apiTokenOverride}" placeholder="Paste session token for remote polling">
                            </div>
                        </div>
                    ` : `
                        <div class="panel">
                            <div class="field">
                                <span>session path</span>
                                <div class="session-card">
                                    <div class="session-head">
                                        <strong>${sharedSessionActive ? 'shared session live' : 'shared session missing'}</strong>
                                        <span class="session-badge ${sharedSessionActive ? 'live' : 'missing'}">${sharedSessionActive ? 'live' : 'sign in'}</span>
                                    </div>
                                    <div class="subtle">${sharedSessionActive ? tokenSummary : 'AstroGrid reads the same-origin GRID session automatically.'}</div>
                                    <div class="button-row" style="margin-top:10px;">
                                        ${usingManualTokenOverride() ? '<button class="button" id="use-shared-session">Use shared session</button>' : ''}
                                        ${!sharedSessionActive ? '<button class="button active" id="open-shared-login">Open sign-in</button>' : ''}
                                    </div>
                                </div>
                            </div>
                        </div>
                    `}
                </div>
                <div class="hero-meta-grid" style="margin-top:12px;">
                    <div class="hero-meta-card">
                        <div class="section-label">session</div>
                        <div class="subtle">${tokenSummary}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">source</div>
                        <div class="subtle">${state.apiBaseUrl}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">poll</div>
                        <div class="subtle">${state.backend.polling.active ? `live / ${Math.round(state.backend.polling.intervalMs / 1000)}s` : 'idle / archive window'}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">last sync</div>
                        <div class="subtle">${state.backend.polling.lastSuccessAt ? new Date(state.backend.polling.lastSuccessAt).toLocaleTimeString() : 'none'}</div>
                    </div>
                </div>
            </div>
            <div class="panel">
                <div class="split-header">
                    <h2>Oracle State</h2>
                    <div class="subtle">${state.backend.connected ? 'live path' : 'local path'}</div>
                </div>
                <div class="hero-meta-grid">
                    <div class="hero-meta-card">
                        <div class="section-label">surface</div>
                        <div class="subtle">${pageSummary}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">window</div>
                        <div class="subtle">${nextEvent ? `${nextEvent.name || nextEvent.event} / ${shortDateLabel(nextEvent.date || nextEvent.datetime)}` : 'none'}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">time</div>
                        <div class="subtle">${state.snapshot?.date || state.selectedDateTime}</div>
                    </div>
                    <div class="hero-meta-card">
                        <div class="section-label">summary</div>
                        <div class="subtle">${state.backend.summary}</div>
                    </div>
                </div>
            </div>
            <div class="panel">
                <div class="split-header">
                    <h2>Lenses</h2>
                    <div class="subtle">choose the mask</div>
                </div>
                <div class="lens-grid">
                    ${Object.values(ENGINE_DEFINITIONS).map((engine) => `
                        <button class="pill ${state.activeLensIds.includes(engine.id) ? 'active' : ''}" data-lens="${engine.id}">
                            ${engine.name}
                        </button>
                    `).join('')}
                </div>
            </div>
            <div class="panel">
                <div class="split-header">
                    <h2>Engines</h2>
                    <div class="subtle">${state.mode}</div>
                </div>
                ${engineMarkup()}
            </div>
            <div class="panel">
                <div class="split-header">
                    <h2>Vault</h2>
                    <div class="subtle">${mystery ? `seal live / ${mystery.witnesses.length} witnesses` : 'awaiting signal'}</div>
                </div>
                ${mystery ? `
                    <div class="vault-shell">
                        <div class="vault-sigil">${mystery.sigil}</div>
                        <div class="vault-title">${mystery.title}</div>
                        <div class="vault-riddle">${mystery.riddle}</div>
                        <div class="vault-state-seal">seal ${mystery.stateSeal}</div>
                        <div class="vault-locks">${mystery.locks.map((lock) => `<span class="vault-lock">${lock}</span>`).join('')}</div>
                        <div class="vault-witnesses">
                            ${mystery.witnesses.map((witness) => `
                                <div class="vault-witness">
                                    <span>${witness.mark}</span>
                                    <strong>${witness.label}</strong>
                                    <em>${witness.when}</em>
                                </div>
                            `).join('')}
                        </div>
                        <div class="vault-clue">${mystery.clue}</div>
                    </div>
                ` : '<div class="empty">No vault signal.</div>'}
            </div>
        </div>
        <div class="panel" style="margin-top:16px;">
            <div class="split-header">
                <h2>Logs</h2>
                <div class="subtle">recent runs</div>
            </div>
            ${logsMarkup()}
        </div>
    `;

    app.innerHTML = `
        <div class="shell">
            <div class="masthead">
                <div>
                    <div class="brand-kicker">celestial signal / symbolic inference / orbital capital</div>
                    <div class="brand-title">ASTROGRID</div>
                    <div class="brand-subtitle">mystic oracle / alpha engine</div>
                </div>
                <div class="masthead-note">
                    <div class="status-badge ${state.backend.connected ? 'good' : 'warn'}">${state.backend.connected ? 'authoritative' : 'local'}</div>
                    <div class="subtle">${state.backend.connected ? 'live layer' : 'local layer'}</div>
                    <div class="subtle">${compactDirectiveLine(nextEvent ? `${nextEvent.name || nextEvent.event} / ${shortDateLabel(nextEvent.date || nextEvent.datetime)}` : 'no active window', 44)}</div>
                </div>
            </div>
            ${pageNav}
            ${state.page === 'oracle' ? oraclePage : ''}
            ${state.page === 'observatory' ? observatoryPage : ''}
            ${state.page === 'atlas' ? atlasPage : ''}
            ${state.page === 'chamber' ? chamberPage : ''}
        </div>
    `;

    document.getElementById('dt-input')?.addEventListener('change', (event) => {
        state.selectedDateTime = event.target.value;
        scheduleRecompute();
    });

    document.getElementById('api-base-input')?.addEventListener('change', (event) => {
        state.apiBaseUrl = event.target.value.trim() || window.location.origin;
        safeStorageSet(CONFIG_KEYS.apiBaseUrl, state.apiBaseUrl);
        scheduleRecompute();
    });

    document.getElementById('api-token-input')?.addEventListener('change', (event) => {
        state.apiTokenOverride = event.target.value.trim();
        if (state.apiTokenOverride) {
            safeStorageSet(CONFIG_KEYS.apiToken, state.apiTokenOverride);
        } else {
            safeStorageRemove(CONFIG_KEYS.apiToken);
        }
        scheduleRecompute();
    });

    document.getElementById('open-shared-login')?.addEventListener('click', () => {
        openSharedLogin();
    });

    document.getElementById('use-shared-session')?.addEventListener('click', () => {
        clearTokenOverride();
        scheduleRecompute();
    });

    document.querySelectorAll('[data-page]').forEach((button) => {
        button.addEventListener('click', () => {
            state.page = button.dataset.page || 'oracle';
            render();
        });
    });

    document.querySelectorAll('[data-mode]').forEach((button) => {
        button.addEventListener('click', () => setMode(button.dataset.mode));
    });

    document.querySelectorAll('[data-lens]').forEach((button) => {
        button.addEventListener('click', () => toggleLens(button.dataset.lens));
    });

    document.querySelectorAll('[data-trajectory-projection]').forEach((button) => {
        button.addEventListener('click', () => {
            state.vectorProjection = button.dataset.trajectoryProjection;
            state.vectorSample = null;
            render();
        });
    });

    document.querySelectorAll('[data-trajectory-horizon]').forEach((button) => {
        button.addEventListener('click', () => {
            state.vectorHorizonDays = Number(button.dataset.trajectoryHorizon) || 14;
            state.vectorSample = null;
            render();
        });
    });

    document.querySelectorAll('[data-trajectory-focus]').forEach((button) => {
        button.addEventListener('click', () => {
            state.vectorFocusBodyId = button.dataset.trajectoryFocus || 'all';
            if (state.vectorSample && state.vectorFocusBodyId !== 'all' && state.vectorSample.bodyId !== state.vectorFocusBodyId) {
                state.vectorSample = null;
            }
            render();
        });
    });

    document.querySelectorAll('[data-trajectory-sample]').forEach((element) => {
        element.addEventListener('click', () => {
            state.vectorSample = {
                bodyId: normalizeBodyId(element.dataset.bodyId),
                bodyName: element.dataset.bodyName || '',
                at: element.dataset.at || '',
                offsetHours: Number(element.dataset.offsetHours || 0),
                lon: Number(element.dataset.lon),
                lat: Number(element.dataset.lat),
                ra: Number(element.dataset.ra),
                dec: Number(element.dataset.dec),
                dist: Number(element.dataset.dist),
                speed: Number(element.dataset.speed),
                retrograde: element.dataset.retrograde === 'true',
            };
            if (state.vectorFocusBodyId === 'all') {
                state.vectorFocusBodyId = state.vectorSample.bodyId;
            }
            render();
        });
    });

    document.querySelectorAll('[data-world-node]').forEach((element) => {
        element.addEventListener('click', () => {
            state.worldFocusId = element.dataset.worldNode || 'earth';
            render();
        });
    });

    document.getElementById('persona-select')?.addEventListener('change', (event) => {
        state.personaId = event.target.value;
    });

    document.getElementById('persona-question')?.addEventListener('input', (event) => {
        state.question = event.target.value;
    });

    document.getElementById('persona-ask')?.addEventListener('click', handlePersonaSubmit);

    document.querySelectorAll('[data-oracle-prompt]').forEach((button) => {
        button.addEventListener('click', () => {
            state.question = button.dataset.oraclePrompt || state.question;
            handlePersonaSubmit();
        });
    });
}

function buildLocalEvents(snapshot) {
    const localEvents = [];
    const lunar = snapshot.lunar || {};
    localEvents.push({
        name: lunar.phase_name || 'Lunar phase',
        date: snapshot.date,
        description: `${Number(lunar.illumination || 0).toFixed(1)}% illumination.`,
    });

    snapshot.aspects
        .slice()
        .sort((a, b) => (a.orb_used ?? 99) - (b.orb_used ?? 99))
        .slice(0, 5)
        .forEach((aspect) => {
            localEvents.push({
                name: `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}`,
                date: snapshot.date,
                description: `Orb ${Number(aspect.orb_used ?? 0).toFixed(2)}°. ${aspect.applying ? 'Applying.' : 'Separating.'}`,
            });
        });

    if (snapshot.nakshatra?.nakshatra_name) {
        localEvents.push({
            name: `Nakshatra ${snapshot.nakshatra.nakshatra_name}`,
            date: snapshot.date,
            description: `${snapshot.nakshatra.quality || 'Unmarked'} quality. Pada ${snapshot.nakshatra.pada || '—'}.`,
        });
    }

    return localEvents.slice(0, 6);
}

function buildLocalSignals(snapshot) {
    return [
        {
            name: 'Planetary Stress',
            value: snapshot.signals.planetaryStress,
            display: `${snapshot.signals.planetaryStress}`,
            description: 'Hard aspect count derived from current sky geometry.',
        },
        {
            name: 'Retrograde Pressure',
            value: snapshot.signals.retrogradeCount ? -snapshot.signals.retrogradeCount : 0,
            display: `${snapshot.signals.retrogradeCount} active`,
            description: 'Retrograde bodies compress clean forward motion.',
        },
        {
            name: 'Lunar Illumination',
            value: (snapshot.signals.lunarIllumination / 100) - 0.5,
            display: `${Number(snapshot.signals.lunarIllumination).toFixed(1)}%`,
            description: `The moon is ${snapshot.signals.lunarPhase.toLowerCase()}.`,
        },
    ];
}

function renderFatal(error) {
    const app = document.getElementById('app');
    if (!app) return;
    app.innerHTML = `
        <div class="shell">
            <div class="panel">
                <div class="section-label">boot fault</div>
                <div class="seer-reading">AstroGrid failed to start.</div>
                <div class="seer-conflicts">${error?.message || String(error)}</div>
            </div>
        </div>
    `;
}

async function main() {
    window.addEventListener('storage', (event) => {
        if (!sameOriginApiBase()) return;
        if (event.key !== 'grid_token') return;
        if (usingManualTokenOverride()) return;
        scheduleRecompute('session');
    });
    render();
    await scheduleRecompute();
}

main().catch((error) => {
    console.error(error);
    renderFatal(error);
});
