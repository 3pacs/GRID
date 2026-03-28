import { create } from 'zustand';

const STORAGE_KEYS = {
    activeView: 'astrogrid_active_view',
    apiMode: 'astrogrid_api_mode',
    apiBaseUrl: 'astrogrid_api_base_url',
    apiToken: 'astrogrid_api_token',
    preferences: 'astrogrid_prefs',
};

const VALID_VIEWS = new Set([
    'orrery',
    'lunar',
    'ephemeris',
    'correlations',
    'timeline',
    'narrative',
    'settings',
]);

const DEFAULT_VIEW = 'orrery';
const DEFAULT_API_MODE = import.meta.env.VITE_ASTROGRID_API_MODE || 'demo';
const DEFAULT_API_BASE_URL = import.meta.env.VITE_ASTROGRID_API_BASE_URL || 'http://localhost:8000';
const DEFAULT_API_TOKEN = import.meta.env.VITE_ASTROGRID_API_TOKEN || '';

const defaultPreferences = {
    animateOrbits: true,
    showAspectLines: true,
    useLiveTelemetry: true,
    showChineseLayer: true,
    showSolarLayer: true,
    coordinateSystem: 'tropical',
};

function loadPreferences() {
    try {
        return JSON.parse(readStorage(STORAGE_KEYS.preferences, null) || '{}');
    } catch {
        return {};
    }
}

function readStorage(key, fallback) {
    if (typeof window === 'undefined') {
        return fallback;
    }

    const value = window.localStorage.getItem(key);
    return value == null || value === '' ? fallback : value;
}

function writeStorage(key, value) {
    if (typeof window === 'undefined') {
        return;
    }

    if (value == null || value === '') {
        window.localStorage.removeItem(key);
        return;
    }

    window.localStorage.setItem(key, value);
}

const useStore = create((set) => ({
    activeView: readStorage(STORAGE_KEYS.activeView, DEFAULT_VIEW),
    selectedDate: new Date().toISOString().slice(0, 10),
    celestialData: {},
    celestialStatus: 'idle',
    celestialNote: 'Session telemetry has not been loaded yet.',
    correlations: [],
    correlationData: [],
    narrativeData: null,
    briefing: '',
    loading: false,
    preferences: {
        ...defaultPreferences,
        ...loadPreferences(),
    },
    apiMode: readStorage(STORAGE_KEYS.apiMode, DEFAULT_API_MODE),
    apiBaseUrl: readStorage(STORAGE_KEYS.apiBaseUrl, DEFAULT_API_BASE_URL),
    apiToken: readStorage(STORAGE_KEYS.apiToken, DEFAULT_API_TOKEN),
    connectionStatus: DEFAULT_API_MODE === 'live' ? 'unknown' : 'demo',
    connectionMessage: DEFAULT_API_MODE === 'live'
        ? 'Live backend not tested yet.'
        : 'Using bundled celestial demo data.',

    setActiveView: (view) => {
        const nextView = VALID_VIEWS.has(view) ? view : DEFAULT_VIEW;
        if (typeof window !== 'undefined') {
            window.location.hash = `#/${nextView}`;
        }
        writeStorage(STORAGE_KEYS.activeView, nextView);
        set({ activeView: nextView });
    },

    setSelectedDate: (selectedDate) => set({ selectedDate }),
    setCelestialData: (data) => set({ celestialData: data }),
    setCelestialTelemetryState: (celestialStatus, celestialNote = '') =>
        set({ celestialStatus, celestialNote }),
    setCorrelations: (correlations) => set({ correlations, correlationData: correlations }),
    setCorrelationData: (correlationData) => set({ correlationData, correlations: correlationData }),
    setNarrativeData: (narrativeData) => set({ narrativeData }),
    setBriefing: (briefing) => set({ briefing }),
    setLoading: (loading) => set({ loading }),

    setPreference: (key, value) =>
        set((state) => {
            const preferences = { ...state.preferences, [key]: value };
            writeStorage(STORAGE_KEYS.preferences, JSON.stringify(preferences));
            return {
                preferences,
                ...(key === 'useLiveTelemetry' && value === false
                    ? {
                        celestialStatus: 'disabled',
                        celestialNote: 'Live telemetry is disabled for this session.',
                    }
                    : {}),
            };
        }),

    setApiMode: (apiMode) => {
        writeStorage(STORAGE_KEYS.apiMode, apiMode);
        set({
            apiMode,
            connectionStatus: apiMode === 'live' ? 'unknown' : 'demo',
            connectionMessage: apiMode === 'live'
                ? 'Live backend not tested yet.'
                : 'Using bundled celestial demo data.',
        });
    },

    setApiBaseUrl: (apiBaseUrl) => {
        writeStorage(STORAGE_KEYS.apiBaseUrl, apiBaseUrl);
        set({ apiBaseUrl });
    },

    setApiToken: (apiToken) => {
        writeStorage(STORAGE_KEYS.apiToken, apiToken);
        set({ apiToken });
    },

    setConnectionState: (connectionStatus, connectionMessage) => {
        set({ connectionStatus, connectionMessage });
    },
}));

export default useStore;
