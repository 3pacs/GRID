/**
 * Zustand global state store for GRID PWA.
 *
 * V5 MIGRATION: State is now decomposed into focused slices under ./stores/.
 * This file re-exports a unified hook for backwards compatibility —
 * existing views that import from './store.js' continue to work unchanged.
 *
 * For new code, import directly from the slice:
 *   import useAuthStore from './stores/authStore.js';
 *   import useUiStore from './stores/uiStore.js';
 */

import useAuthStore from './stores/authStore.js';
import useUiStore from './stores/uiStore.js';
import useDomainStore from './stores/domainStore.js';
import useRealtimeStore from './stores/realtimeStore.js';

const STORE_SLICES = [
    {
        store: useAuthStore,
        keys: new Set(['token', 'isAuthenticated', 'userRole', 'username']),
    },
    {
        store: useUiStore,
        keys: new Set(['theme', 'activeView', 'loading', 'errors', 'notifications']),
    },
    {
        store: useDomainStore,
        keys: new Set([
            'systemStatus',
            'latestSignals',
            'currentRegime',
            'regimeHistory',
            'journalEntries',
            'journalStats',
            'productionModels',
            'allModels',
            'jobs',
            'hypotheses',
            'agentProgress',
            'agentLastComplete',
        ]),
    },
    {
        store: useRealtimeStore,
        keys: new Set([
            'wsConnected',
            'livePriceUpdates',
            'liveAlerts',
            'liveRecommendations',
            'lastRegimeChange',
            'pushSupported',
            'pushPermission',
            'pushSubscription',
            'pushPreferences',
            'chatMessages',
            'chatUnread',
        ]),
    },
];

/**
 * Unified store hook — merges all slices into one selector interface.
 * This is the backwards-compat layer. All 83 properties + 35 actions
 * are accessible through this hook exactly as before.
 */
function useStore(selector) {
    const auth = useAuthStore(selector ? undefined : (s) => s);
    const ui = useUiStore(selector ? undefined : (s) => s);
    const domain = useDomainStore(selector ? undefined : (s) => s);
    const realtime = useRealtimeStore(selector ? undefined : (s) => s);

    const merged = { ...auth, ...ui, ...domain, ...realtime };

    if (selector) {
        return selector(merged);
    }
    return merged;
}

// Also make getState() work for imperative access (e.g., in api.js WebSocket)
useStore.getState = () => ({
    ...useAuthStore.getState(),
    ...useUiStore.getState(),
    ...useDomainStore.getState(),
    ...useRealtimeStore.getState(),
});

useStore.setState = (partial) => {
    const nextState = typeof partial === 'function'
        ? partial(useStore.getState())
        : partial;
    if (!nextState || typeof nextState !== 'object') return;

    for (const { store, keys } of STORE_SLICES) {
        const sliceUpdate = {};
        for (const [key, value] of Object.entries(nextState)) {
            if (keys.has(key)) {
                sliceUpdate[key] = value;
            }
        }
        if (Object.keys(sliceUpdate).length > 0) {
            store.setState(sliceUpdate);
        }
    }
};

export default useStore;
