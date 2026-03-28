import { create } from 'zustand';

const STORAGE_KEYS = {
    activeView: 'astrogrid_active_view',
    apiMode: 'astrogrid_api_mode',
    apiBaseUrl: 'astrogrid_api_base_url',
    apiToken: 'astrogrid_api_token',
};

const DEFAULT_VIEW = 'orrery';
const DEFAULT_API_MODE = import.meta.env.VITE_ASTROGRID_API_MODE || 'demo';
const DEFAULT_API_BASE_URL = import.meta.env.VITE_ASTROGRID_API_BASE_URL || 'http://localhost:8000';
const DEFAULT_API_TOKEN = import.meta.env.VITE_ASTROGRID_API_TOKEN || '';

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
    celestialData: {},
    correlations: [],
    briefing: '',
    loading: false,
    apiMode: readStorage(STORAGE_KEYS.apiMode, DEFAULT_API_MODE),
    apiBaseUrl: readStorage(STORAGE_KEYS.apiBaseUrl, DEFAULT_API_BASE_URL),
    apiToken: readStorage(STORAGE_KEYS.apiToken, DEFAULT_API_TOKEN),
    connectionStatus: DEFAULT_API_MODE === 'live' ? 'unknown' : 'demo',
    connectionMessage: DEFAULT_API_MODE === 'live'
        ? 'Live backend not tested yet.'
        : 'Using bundled celestial demo data.',

    setActiveView: (view) => {
        if (typeof window !== 'undefined') {
            window.location.hash = `#/${view}`;
        }
        writeStorage(STORAGE_KEYS.activeView, view);
        set({ activeView: view });
    },

    setCelestialData: (data) => set({ celestialData: data }),
    setCorrelations: (correlations) => set({ correlations }),
    setBriefing: (briefing) => set({ briefing }),
    setLoading: (loading) => set({ loading }),

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
