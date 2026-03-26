/**
 * AstroGrid Zustand global state store.
 */

import { create } from 'zustand';

const useStore = create((set, get) => ({
    // Navigation
    activeView: 'orrery',

    // Celestial state
    celestialData: {},

    // Correlations
    correlations: [],

    // Briefing
    briefing: '',

    // UI
    loading: false,

    // Auth — shares JWT with GRID PWA
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),

    // Actions
    setActiveView: (view) => {
        window.location.hash = `#/${view}`;
        set({ activeView: view });
    },

    setCelestialData: (data) => set({ celestialData: data }),
    setCorrelations: (correlations) => set({ correlations }),
    setBriefing: (briefing) => set({ briefing }),
    setLoading: (loading) => set({ loading }),

    setAuth: (token) => {
        localStorage.setItem('grid_token', token);
        set({ token, isAuthenticated: true });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        set({ token: null, isAuthenticated: false });
    },
}));

export default useStore;
