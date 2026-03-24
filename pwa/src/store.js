/**
 * Zustand global state store for GRID PWA.
 */

import { create } from 'zustand';

const useStore = create((set, get) => ({
    // Auth
    token: localStorage.getItem('grid_token') || null,
    isAuthenticated: !!localStorage.getItem('grid_token'),
    userRole: localStorage.getItem('grid_role') || 'admin',
    username: localStorage.getItem('grid_username') || 'operator',

    // System
    systemStatus: null,
    wsConnected: false,

    // Signals
    latestSignals: null,

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

    // Agents
    agentProgress: null,  // current run progress
    agentLastComplete: null,  // last completed run

    // UI
    activeView: 'dashboard',
    loading: {},
    errors: {},
    notifications: [],

    // Actions
    setAuth: (token, role = 'admin', username = 'operator') => {
        localStorage.setItem('grid_token', token);
        localStorage.setItem('grid_role', role);
        localStorage.setItem('grid_username', username);
        set({ token, isAuthenticated: true, userRole: role, username });
    },

    clearAuth: () => {
        localStorage.removeItem('grid_token');
        localStorage.removeItem('grid_role');
        localStorage.removeItem('grid_username');
        set({ token: null, isAuthenticated: false, userRole: 'admin', username: 'operator' });
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

    removeNotification: (id) => set(state => ({
        notifications: state.notifications.filter(n => n.id !== id),
    })),

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
                if (data) set({ latestSignals: data });
                break;
            case 'node_update':
                if (data) {
                    set(state => ({
                        systemStatus: state.systemStatus
                            ? { ...state.systemStatus, hyperspace: data }
                            : { hyperspace: data }
                    }));
                }
                break;
            case 'agent_progress':
                set({ agentProgress: data });
                break;
            case 'agent_run_complete':
                set({ agentProgress: null, agentLastComplete: data });
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
