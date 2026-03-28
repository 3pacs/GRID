import { computePosition, getFullEphemeris } from './lib/ephemeris.js';
import {
    getAstrogridDefaultApiBaseUrl,
    normalizeAstrogridCorrelations,
    normalizeAstrogridSignalMap,
    normalizeAstrogridTimeline,
} from './lib/contract.js';
import {
    buildAstrogridBriefingCandidates,
    buildAstrogridCorrelationsCandidates,
    buildAstrogridSnapshotPath,
    fetchFirstAstrogridCandidate,
} from './lib/endpoints.js';
import { ENGINE_DEFINITIONS, buildPersonaResponse, computeEngineOutputs, computeSeer, extractSkyThreads } from './engines.js';
import { buildAstrogridHypotheses, buildCelestialFeatureRows } from './lib/hypotheses.js';
import { createAspectField, createObjectTable, createRadialSky, createSpacetimeField, summarizeSky } from './visuals.js';
import { buildSeedWorldModel } from './lib/worldModel.js';

const LOG_KEYS = {
    seer: 'astrogrid_web_seer_logs',
    persona: 'astrogrid_web_persona_logs',
};
const CONFIG_KEYS = {
    apiBaseUrl: 'astrogrid_web_api_base_url',
    apiToken: 'astrogrid_web_api_token',
};

const LENS_MODES = ['solo', 'chorus', 'intersection', 'shadow'];
const PERSONAS = [
    { id: 'seer', name: 'Seer' },
    { id: 'qwen', name: 'Qwen Mask' },
    { id: 'western', name: 'Western Reader' },
    { id: 'vedic', name: 'Vedic Reader' },
    { id: 'hermetic', name: 'Hermetic Witness' },
    { id: 'taoist', name: 'Taoist Observer' },
    { id: 'babylonian', name: 'Babylonian Keeper' },
];

const state = {
    mode: 'chorus',
    activeLensIds: ['western', 'vedic', 'hermetic', 'taoist'],
    selectedDateTime: toLocalInput(new Date()),
    apiBaseUrl: loadApiBaseUrl(),
    apiTokenOverride: loadTokenOverride(),
    question: 'What should I watch now?',
    personaId: 'seer',
    personaResponse: null,
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
    },
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

function getBaseUrl() {
    return state.apiBaseUrl.replace(/\/$/, '');
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

let archiveSnapshotCache = null;
let archiveSnapshotAttempted = false;

async function loadLocalArchiveSnapshot(localSnapshot) {
    const targetDate = dateKey(localSnapshot?.date || state.selectedDateTime);
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
        const body = await response.json().catch(() => ({}));
        throw new Error(body.detail || response.statusText);
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

async function refreshBackend() {
    state.backend.snapshot = null;
    state.backend.prophecy = null;
    state.backend.prophecyKey = '';
    if (!readToken()) {
        state.backend.connected = false;
        state.backend.summary = sameOriginApiBase()
            ? 'Local sky only. Log into GRID on this origin to unlock the overlay.'
            : 'Local sky only. Paste a GRID JWT. This origin cannot read grid.stepdad.finance storage.';
        state.backend.overview = null;
        state.backend.timeline = [];
        state.backend.correlations = [];
        state.backend.briefing = null;
        state.backend.prophecy = null;
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
        state.backend.snapshot = snapshot;
        state.backend.summary = `Authoritative snapshot live. Source: ${snapshot.source || 'GRID'}.`;
        state.backend.overview = snapshot.signals || snapshot.grid || null;
        state.backend.timeline = normalizeAstrogridTimeline(snapshot);
        state.backend.correlations = correlationsResult.status === 'fulfilled'
            ? normalizeAstrogridCorrelations(correlationsResult.value)
            : [];
        state.backend.briefing = briefingResult.status === 'fulfilled' ? briefingResult.value : null;
    } catch (error) {
        state.backend.connected = false;
        state.backend.summary = `Local sky active. GRID snapshot failed: ${error.message}`;
        state.backend.overview = null;
        state.backend.timeline = [];
        state.backend.correlations = [];
        state.backend.briefing = null;
        state.backend.prophecy = null;
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
    render();
}

let recomputeQueue = Promise.resolve();

function scheduleRecompute() {
    recomputeQueue = recomputeQueue
        .catch(() => undefined)
        .then(() => recompute())
        .catch((error) => {
            console.error(error);
            renderFatal(error);
        });
    return recomputeQueue;
}

function flattenOverviewSignals(overview) {
    return normalizeAstrogridSignalMap(overview);
}

function handlePersonaSubmit() {
    if (!state.seer) return;
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

function correlationsMarkup() {
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

function currentEventStream() {
    const liveEvents = state.backend.timeline.slice(0, 6);
    if (liveEvents.length) return liveEvents;
    if (Array.isArray(state.snapshot?.events) && state.snapshot.events.length) {
        return state.snapshot.events.slice(0, 6);
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
    if (action === 'press') {
        return aspect ? `only while ${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2} holds` : `only into ${triggerName}`;
    }
    if (action === 'hedge') {
        return `protect into ${triggerName}`;
    }
    if (action === 'fade') {
        return `sell extension near ${triggerName}`;
    }
    if (action === 'wait') {
        return `stand down until ${triggerName}`;
    }
    return `small size until ${triggerName}`;
}

function buildForecastCards() {
    if (!state.snapshot || !state.seer) return [];
    const eventStream = currentEventStream();
    const trigger = eventStream[0] || null;
    const aspect = topAspect(state.snapshot);
    const bias = Number(state.seer.signal_bias ?? 0);
    const action = actionVerb();

    return [
        {
            sigil: bias >= 0.25 ? '▲' : bias <= -0.25 ? '▼' : '◌',
            label: 'bias',
            value: state.seer.prediction || state.seer.reading,
            detail: aspect ? `${aspect.planet1} ${aspect.aspect_type} ${aspect.planet2}` : state.snapshot.lunar.phase_name,
        },
        {
            sigil: '◔',
            label: 'window',
            value: trigger?.name || state.snapshot.lunar.phase_name,
            detail: shortDateLabel(trigger?.date || state.snapshot.date),
        },
        {
            sigil: '⟡',
            label: 'act',
            value: action,
            detail: actionRule(action, trigger, aspect),
        },
    ];
}

function renderClaimMarkup(claims = []) {
    if (!claims.length) {
        return '';
    }
    return `<div class="claim-list">${claims.slice(0, 2).map((claim) => `
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
    return `
        <div class="log-list">
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
    const world = buildSeedWorldModel();
    const nodes = world.nodes.filter((node) => ['sun', 'earth', 'leo', 'cislunar_space', 'moon', 'lunar_surface', 'mars', 'mars_surface'].includes(node.id));
    const capitalEdges = world.edges.filter((edge) => edge.type === 'capital');

    return `
        <div class="event-list">
            <div class="event-card">
                <div class="engine-head">
                    <div class="engine-name">Zoom Spine</div>
                    <div class="engine-meta">${nodes.length} nodes</div>
                </div>
                <div class="subtle">${nodes.map((node) => node.name).join(' -> ')}</div>
            </div>
            ${capitalEdges.map((edge) => `
                <div class="event-card">
                    <div class="engine-head">
                        <div class="engine-name">${edge.meta?.label || edge.type}</div>
                        <div class="engine-meta">${edge.scale}</div>
                    </div>
                    <div class="subtle">${edge.source} -> ${edge.target}</div>
                </div>
            `).join('')}
            <div class="event-card">
                <div class="engine-head">
                    <div class="engine-name">Layer Stack</div>
                    <div class="engine-meta">${world.layerStack.length} layers</div>
                </div>
                <div class="subtle">${world.layerStack.join(' / ')}</div>
            </div>
        </div>
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

function hypothesesMarkup() {
    const hypotheses = buildAstrogridHypotheses(state.snapshot, state.seer);
    if (!hypotheses.length) {
        return '<div class="empty">No hypothesis field.</div>';
    }
    return `<div class="hypothesis-grid">${hypotheses.map((item) => `
        <div class="hypothesis-card">
            <div class="engine-head">
                <div class="engine-name">${item.title}</div>
                <div class="engine-meta">${item.window}</div>
            </div>
            <div class="forecast-row">
                <div class="forecast-sigil">${item.sigil}</div>
                <div>
                    <div class="hypothesis-bias">${item.bias}</div>
                    <div class="hypothesis-act">${item.act}</div>
                </div>
            </div>
            <div class="seer-support">cue: ${item.cue}</div>
        </div>
    `).join('')}</div>`;
}

function render() {
    const app = document.getElementById('app');
    const summaryMarkup = state.snapshot ? summarizeSky(state.snapshot) : '<div class="empty">Awaiting sky.</div>';
    const activePersona = PERSONAS.find((persona) => persona.id === state.personaId);
    const seerFactors = state.seer?.key_factors || [];
    const seerConflicts = (state.seer?.conflicts || []).map((conflict) => {
        if (typeof conflict === 'string') return conflict;
        return `${conflict.engine_id} ${conflict.direction}`;
    });
    const prophecyOverlay = state.backend.prophecy;
    const forecastCards = buildForecastCards();

    app.innerHTML = `
        <div class="shell">
            <div class="masthead">
                <div>
                    <div class="brand-kicker">computed sky / lens engines / grid overlays</div>
                    <div class="brand-title">ASTROGRID</div>
                    <div class="brand-subtitle">Sky. Signal. Trigger.</div>
                </div>
                <div class="status-block">
                    <div class="status-label">server state</div>
                    <div class="status-value ${state.backend.connected ? 'good' : 'warn'}">${state.backend.connected ? 'authoritative snapshot live' : 'local observatory'}</div>
                    <div class="subtle">${state.backend.summary}</div>
                    <div class="subtle" style="margin-top:8px;">Archive: ${state.archive ? 'local snapshot loaded' : 'none'}</div>
                    <div class="subtle" style="margin-top:8px;">API: ${state.apiBaseUrl}</div>
                    <div class="subtle" style="margin-top:10px;"><a href="/">Return to GRID</a></div>
                </div>
            </div>

            <div class="control-grid">
                <div class="panel" style="grid-column: span 4;">
                    <div class="field">
                        <span>sky time</span>
                        <input id="dt-input" type="datetime-local" value="${state.selectedDateTime}">
                    </div>
                </div>
                <div class="panel" style="grid-column: span 4;">
                    <div class="field">
                        <span>lens mode</span>
                        <div class="button-row">
                            ${LENS_MODES.map((mode) => `<button class="button ${mode === state.mode ? 'active' : ''}" data-mode="${mode}">${mode}</button>`).join('')}
                        </div>
                    </div>
                </div>
                <div class="panel" style="grid-column: span 4;">
                    <div class="section-label">computed summary</div>
                    <div class="subtle">${state.snapshot ? `${state.snapshot.lunar.phase_name} / ${state.snapshot.aspects.length} aspects / ${state.snapshot.nakshatra.nakshatra_name}` : 'Awaiting sky.'}</div>
                </div>
            </div>

            <div class="control-grid">
                <div class="panel" style="grid-column: span 6;">
                    <div class="field">
                        <span>grid api base</span>
                        <input id="api-base-input" type="text" value="${state.apiBaseUrl}">
                    </div>
                </div>
                <div class="panel" style="grid-column: span 6;">
                    <div class="field">
                        <span>token override</span>
                        <input id="api-token-input" type="password" value="${state.apiTokenOverride}" placeholder="Paste GRID JWT for remote polling">
                    </div>
                </div>
            </div>

            <div class="panel" style="margin-bottom:16px;">
                <div class="split-header">
                    <h2>Lenses</h2>
                    <div class="subtle">Choose the mask.</div>
                </div>
                <div class="lens-grid">
                    ${Object.values(ENGINE_DEFINITIONS).map((engine) => `
                        <button class="pill ${state.activeLensIds.includes(engine.id) ? 'active' : ''}" data-lens="${engine.id}">
                            ${engine.name}
                        </button>
                    `).join('')}
                </div>
            </div>

            <div class="grid primary">
                <div class="panel tall">
                    <div class="split-header">
                        <h2>Observatory</h2>
                        <div class="subtle">${state.snapshot ? `${state.snapshot.bodies.length} tracked bodies` : 'Awaiting sky.'}</div>
                    </div>
                    <div class="ag-visual-grid">
                        <div class="visual-shell">${state.snapshot ? createRadialSky(state.snapshot) : '<div class="empty">Awaiting sky.</div>'}</div>
                        <div class="visual-shell">${state.snapshot ? createSpacetimeField(state.snapshot) : '<div class="empty">Awaiting field.</div>'}</div>
                    </div>
                    <div class="visual-shell" style="margin-top:14px;">${state.snapshot ? createAspectField(state.snapshot) : ''}</div>
                </div>

                <div class="grid">
                    <div class="panel">
                        <div class="split-header">
                            <h2>Celestial State</h2>
                            <div class="subtle">numbers first</div>
                        </div>
                        ${summaryMarkup}
                        <div class="readout">
                            <div class="metric">
                                <div class="metric-value">${state.snapshot ? state.snapshot.lunar.phase_name : '--'}</div>
                                <div class="metric-label">Lunar phase</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value">${state.snapshot ? state.snapshot.signals.planetaryStress : '--'}</div>
                                <div class="metric-label">Hard aspects</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value">${state.snapshot ? state.snapshot.signals.retrogradeCount : '--'}</div>
                                <div class="metric-label">Retrogrades</div>
                            </div>
                            <div class="metric">
                                <div class="metric-value">${state.snapshot ? state.snapshot.nakshatra.nakshatra_name : '--'}</div>
                                <div class="metric-label">Nakshatra</div>
                            </div>
                        </div>
                    </div>
                    <div class="panel">
                        <div class="split-header">
                            <h2>Seer</h2>
                            <div class="subtle">${state.seer ? state.seer.confidence_band : ''}</div>
                        </div>
                        ${state.seer ? `
                            <div class="seer-reading">${state.seer.reading}</div>
                            ${forecastCards.length ? `<div class="forecast-grid">${forecastCards.map((card) => `
                                <div class="forecast-card">
                                    <div class="forecast-sigil">${card.sigil}</div>
                                    <div class="forecast-label">${card.label}</div>
                                    <div class="forecast-value">${card.value}</div>
                                    <div class="forecast-detail">${card.detail}</div>
                                </div>
                            `).join('')}</div>` : ''}
                            <div class="seer-support">cue: ${seerFactors.length ? seerFactors.slice(0, 3).join(' / ') : 'none'}</div>
                            <div class="seer-conflicts">split: ${seerConflicts.length ? seerConflicts.slice(0, 2).join(' / ') : 'none'}</div>
                            ${prophecyMarkup(prophecyOverlay)}
                        ` : '<div class="empty">Awaiting voice.</div>'}
                    </div>
                </div>
            </div>

            <div class="grid secondary">
                <div class="panel">
                    <div class="split-header">
                        <h2>Engines</h2>
                        <div class="subtle">${state.mode}</div>
                    </div>
                    ${engineMarkup()}
                </div>
                <div class="panel">
                    <div class="split-header">
                        <h2>Persona</h2>
                        <div class="subtle">${activePersona ? activePersona.name : ''}</div>
                    </div>
                    <div class="field" style="margin-bottom:12px;">
                        <span>face</span>
                        <select id="persona-select">
                            ${PERSONAS.map((persona) => `<option value="${persona.id}" ${persona.id === state.personaId ? 'selected' : ''}>${persona.name}</option>`).join('')}
                        </select>
                    </div>
                    <div class="field" style="margin-bottom:12px;">
                        <span>question</span>
                        <textarea id="persona-question">${state.question}</textarea>
                    </div>
                    <div class="button-row" style="margin-bottom:12px;">
                        <button class="button active" id="persona-ask">Ask</button>
                    </div>
                    ${state.personaResponse ? `
                        <div class="engine-card">
                            <div class="engine-head">
                                <div class="engine-name">${state.personaResponse.persona_name}</div>
                                <div class="engine-meta">${state.personaResponse.mode}</div>
                            </div>
                            <div class="seer-support">lens: ${(state.personaResponse.allowed_lenses || []).join(' / ') || state.personaResponse.declared_lens || 'none'}</div>
                            ${(state.personaResponse.excluded_lenses || []).length ? `<div class="seer-conflicts">excludes: ${(state.personaResponse.excluded_lenses || []).join(' / ')}</div>` : ''}
                            <div>${state.personaResponse.answer}</div>
                        </div>
                    ` : '<div class="empty">Choose a face.</div>'}
                </div>
            </div>

            <div class="grid tertiary">
                <div class="panel">
                    <div class="split-header">
                        <h2>Signals</h2>
                        <div class="subtle">${state.backend.connected ? 'live overlay' : 'local only'}</div>
                    </div>
                    ${correlationsMarkup()}
                </div>
                <div class="panel">
                    <div class="split-header">
                        <h2>Events</h2>
                        <div class="subtle">near windows</div>
                    </div>
                    ${eventsMarkup()}
                </div>
                <div class="panel">
                    <div class="split-header">
                        <h2>Hypotheses</h2>
                        <div class="subtle">event-linked</div>
                    </div>
                    ${hypothesesMarkup()}
                </div>
            </div>

            <div class="grid tertiary">
                <div class="panel">
                    <div class="split-header">
                        <h2>World</h2>
                        <div class="subtle">Earth / Moon / Mars / satellites / capital</div>
                    </div>
                    ${worldMarkup()}
                </div>
            </div>

            <div class="panel" style="margin-top:16px;">
                <div class="split-header">
                    <h2>Objects</h2>
                    <div class="subtle">registry-grade payload</div>
                </div>
                <div class="table-wrap">
                    ${state.snapshot ? createObjectTable(state.snapshot) : '<div class="empty">Awaiting object payload.</div>'}
                </div>
            </div>
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

    document.querySelectorAll('[data-mode]').forEach((button) => {
        button.addEventListener('click', () => setMode(button.dataset.mode));
    });

    document.querySelectorAll('[data-lens]').forEach((button) => {
        button.addEventListener('click', () => toggleLens(button.dataset.lens));
    });

    document.getElementById('persona-select')?.addEventListener('change', (event) => {
        state.personaId = event.target.value;
    });

    document.getElementById('persona-question')?.addEventListener('input', (event) => {
        state.question = event.target.value;
    });

    document.getElementById('persona-ask')?.addEventListener('click', handlePersonaSubmit);
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
    render();
    await scheduleRecompute();
}

main().catch((error) => {
    console.error(error);
    renderFatal(error);
});
