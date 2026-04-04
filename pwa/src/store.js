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

    // Live WebSocket data
    livePriceUpdates: {},      // {ticker: {price, pct_1d, updated_at}}
    liveAlerts: [],            // [{severity, message, timestamp, id}]
    liveRecommendations: [],   // [{ticker, direction, strike, ...}]
    lastRegimeChange: null,    // {from, to, confidence, timestamp}

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

    // Push Notifications
    pushSupported: 'serviceWorker' in navigator && 'PushManager' in window,
    pushPermission: typeof Notification !== 'undefined' ? Notification.permission : 'default',
    pushSubscription: null,
    pushPreferences: {
        trade_recommendations: true,
        convergence_alerts: true,
        regime_changes: true,
        red_flags: true,
        price_alerts: true,
        price_alert_threshold: 5.0,
    },

    // UI
    theme: localStorage.getItem('grid_theme') || 'dark',
    activeView: 'home',
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

    setTheme: (name) => {
        localStorage.setItem('grid_theme', name);
        set({ theme: name });
    },

    setActiveView: (view) => set({ activeView: view }),

    setLoading: (key, value) => set(state => ({
        loading: { ...state.loading, [key]: value },
    })),

    setError: (key, error) => set(state => ({
        errors: { ...state.errors, [key]: error },
    })),

    // Push notification actions
    setPushPermission: (perm) => set({ pushPermission: perm }),
    setPushSubscription: (sub) => set({ pushSubscription: sub }),
    setPushPreferences: (prefs) => set({ pushPreferences: prefs }),

    // Live data actions
    setLivePriceUpdates: (prices) => set({ livePriceUpdates: prices }),
    pushAlert: (alert) => {
        const id = Date.now();
        const entry = { ...alert, id, timestamp: alert.timestamp || new Date().toISOString() };
        set(state => ({
            liveAlerts: [entry, ...state.liveAlerts].slice(0, 20),
        }));
        // Auto-dismiss after 15 seconds
        setTimeout(() => {
            set(state => ({
                liveAlerts: state.liveAlerts.filter(a => a.id !== id),
            }));
        }, 15000);
    },
    pushRecommendation: (rec) => {
        const id = Date.now();
        const entry = { ...rec, id, timestamp: rec.timestamp || new Date().toISOString() };
        set(state => ({
            liveRecommendations: [entry, ...state.liveRecommendations].slice(0, 20),
        }));
    },
    dismissAlert: (id) => set(state => ({
        liveAlerts: state.liveAlerts.filter(a => a.id !== id),
    })),
    dismissRecommendation: (id) => set(state => ({
        liveRecommendations: state.liveRecommendations.filter(r => r.id !== id),
    })),

    // Chat (Ask GRID)
    chatMessages: [],   // [{role, content, sources?, confidence?}]
    chatUnread: 0,

    addChatMessage: (msg) => set(state => ({
        chatMessages: [...state.chatMessages, msg],
    })),
    clearChat: () => set({ chatMessages: [], chatUnread: 0 }),
    setChatUnread: (n) => set({ chatUnread: n }),

    // WebSocket handler
    handleWsMessage: (event) => {
        const { type, data, severity, timestamp } = event;
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

            // ── New real-time event types ──
            case 'prices':
                if (data) {
                    set(state => ({
                        livePriceUpdates: { ...state.livePriceUpdates, ...data },
                    }));
                }
                break;
            case 'recommendation':
                if (data) {
                    get().pushRecommendation(data);
                    get().addNotification('info',
                        `New ${data.direction} rec: ${data.ticker} @ ${data.strike}`);
                }
                break;
            case 'alert':
                if (data) {
                    get().pushAlert({ ...data, severity: severity || data.severity || 'info' });
                }
                break;
            case 'regime_change':
                if (data) {
                    set({ lastRegimeChange: { ...data, timestamp } });
                    // Also update current regime state if provided
                    if (data.to) {
                        set(state => ({
                            currentRegime: state.currentRegime
                                ? { ...state.currentRegime, state: data.to, confidence: data.confidence }
                                : { state: data.to, confidence: data.confidence },
                        }));
                    }
                    get().addNotification('warning',
                        `Regime shift: ${data.from} → ${data.to} (${Math.round((data.confidence || 0) * 100)}%)`);
                }
                break;
            default:
                break;
        }
    },
}));

export default useStore;
