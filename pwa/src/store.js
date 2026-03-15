/**
 * Zustand global state store for GRID PWA.
 */

import { create } from 'zustand';

const useStore = create((set, get) => ({
    // Auth
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),

    // System
    systemStatus: null,
    wsConnected: false,

    // Regime
    currentRegime: null,
    regimeHistory: [],

    // Journal
    journalEntries: [],
    journalStats: null,

    // Models
    productionModels: {},
    allModels: [],

    // Discovery
    jobs: [],
    hypotheses: [],

    // UI
    activeView: 'dashboard',
    loading: {},
    errors: {},
    notifications: [],

    // Actions
    setAuth: (token) => {
        localStorage.setItem('grid_token', token);
        set({ token, isAuthenticated: true });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        set({ token: null, isAuthenticated: false });
    },

    setSystemStatus: (status) => set({ systemStatus: status }),
    setWsConnected: (connected) => set({ wsConnected: connected }),
    setCurrentRegime: (regime) => set({ currentRegime: regime }),
    setRegimeHistory: (history) => set({ regimeHistory: history }),
    setJournalEntries: (entries) => set({ journalEntries: entries }),
    setJournalStats: (stats) => set({ journalStats: stats }),
    setProductionModels: (models) => set({ productionModels: models }),
    setAllModels: (models) => set({ allModels: models }),
    setJobs: (jobs) => set({ jobs }),
    setHypotheses: (hypotheses) => set({ hypotheses }),

    addNotification: (type, message) => {
        const id = Date.now();
        set(state => ({
            notifications: [...state.notifications, { id, type, message }].slice(-5),
        }));
        setTimeout(() => {
            set(state => ({
                notifications: state.notifications.filter(n => n.id !== id),
            }));
        }, 5000);
    },

    setActiveView: (view) => set({ activeView: view }),

    setLoading: (key, value) => set(state => ({
        loading: { ...state.loading, [key]: value },
    })),

    setError: (key, error) => set(state => ({
        errors: { ...state.errors, [key]: error },
    })),

    // WebSocket handler
    handleWsMessage: (event) => {
        const { type, data } = event;
        switch (type) {
            case 'connected':
                set({ wsConnected: true });
                break;
            case 'regime_update':
                if (data) set({ currentRegime: data });
                break;
            case 'signal_update':
                break;
            case 'node_update':
                if (data) {
                    set(state => ({
                        systemStatus: state.systemStatus
                            ? { ...state.systemStatus, hyperspace: data }
                            : state.systemStatus
                    }));
                }
                break;
            case 'ping':
                set({ wsConnected: true });
                break;
            default:
                break;
        }
    },
}));

export default useStore;
