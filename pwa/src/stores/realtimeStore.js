/**
 * Realtime store slice — WebSocket state, live prices, alerts, recommendations, push, chat.
 */
import { create } from 'zustand';

const useRealtimeStore = create((set, get) => ({
    // WebSocket
    wsConnected: false,

    // Live data
    livePriceUpdates: {},
    liveAlerts: [],
    liveRecommendations: [],
    lastRegimeChange: null,

    // Push notifications
    pushSupported: typeof navigator !== 'undefined' && 'serviceWorker' in navigator && 'PushManager' in window,
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

    // Chat
    chatMessages: [],
    chatUnread: 0,

    // WebSocket setters
    setWsConnected: (connected) => set({ wsConnected: connected }),

    // Live data actions
    setLivePriceUpdates: (prices) => set({ livePriceUpdates: prices }),

    pushAlert: (alert) => {
        const id = Date.now();
        const entry = { ...alert, id, timestamp: alert.timestamp || new Date().toISOString() };
        set(state => ({
            liveAlerts: [entry, ...state.liveAlerts].slice(0, 20),
        }));
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

    // Push notification actions
    setPushPermission: (perm) => set({ pushPermission: perm }),
    setPushSubscription: (sub) => set({ pushSubscription: sub }),
    setPushPreferences: (prefs) => set({ pushPreferences: prefs }),

    // Chat actions
    addChatMessage: (msg) => set(state => ({
        chatMessages: [...state.chatMessages, msg],
    })),
    clearChat: () => set({ chatMessages: [], chatUnread: 0 }),
    setChatUnread: (n) => set({ chatUnread: n }),

    // WebSocket message handler — dispatches across stores
    handleWsMessage: (event) => {
        const { type, data, severity, timestamp } = event;
        // Lazy-load cross-store references to avoid circular imports
        const getDomain = () => require('./domainStore.js').default;
        const getUi = () => require('./uiStore.js').default;

        switch (type) {
            case 'connected':
                set({ wsConnected: true });
                break;
            case 'regime_update':
                if (data) {
                    try { getDomain().getState().setCurrentRegime(data); } catch (_) {}
                }
                break;
            case 'signal_update':
                if (data) {
                    try { getDomain().setState({ latestSignals: data }); } catch (_) {}
                }
                break;
            case 'node_update':
                if (data) {
                    try {
                        getDomain().setState(state => ({
                            systemStatus: state.systemStatus
                                ? { ...state.systemStatus, hyperspace: data }
                                : { hyperspace: data }
                        }));
                    } catch (_) {}
                }
                break;
            case 'agent_progress':
                try { getDomain().setState({ agentProgress: data }); } catch (_) {}
                break;
            case 'agent_run_complete':
                try { getDomain().setState({ agentProgress: null, agentLastComplete: data }); } catch (_) {}
                break;
            case 'ping':
                set({ wsConnected: true });
                break;
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
                    try {
                        getUi().getState().addNotification('info',
                            `New ${data.direction} rec: ${data.ticker} @ ${data.strike}`);
                    } catch (_) {}
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
                    if (data.to) {
                        try {
                            const cur = getDomain().getState().currentRegime;
                            getDomain().setState({
                                currentRegime: cur
                                    ? { ...cur, state: data.to, confidence: data.confidence }
                                    : { state: data.to, confidence: data.confidence },
                            });
                        } catch (_) {}
                    }
                    try {
                        getUi().getState().addNotification('warning',
                            `Regime shift: ${data.from} → ${data.to} (${Math.round((data.confidence || 0) * 100)}%)`);
                    } catch (_) {}
                }
                break;
            default:
                break;
        }
    },
}));

export default useRealtimeStore;
