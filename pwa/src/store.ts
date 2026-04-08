/**
 * Zustand global state store for GRID PWA — TypeScript version.
 *
 * V5 MIGRATION: State is decomposed into focused slices under ./stores/.
 * This file re-exports a unified hook for backwards compatibility.
 *
 * For new code, import directly from the slice:
 *   import useAuthStore from './stores/authStore.js';
 *   import useUiStore from './stores/uiStore.js';
 *
 * This typed version provides the same unified interface with type safety.
 */

import type { GridStore } from './types/index';

// Import slices — using .js extension for Vite resolution of existing files
import useAuthStore from './stores/authStore.js';
import useUiStore from './stores/uiStore.js';
import useDomainStore from './stores/domainStore.js';
import useRealtimeStore from './stores/realtimeStore.js';

/**
 * Unified store hook — merges all slices into one selector interface.
 * This is the backwards-compat layer. All properties + actions
 * are accessible through this hook exactly as before.
 */
function useStore<T = GridStore>(selector?: (state: GridStore) => T): T {
    const auth = useAuthStore(selector ? undefined : ((s: unknown) => s)) as Record<string, unknown>;
    const ui = useUiStore(selector ? undefined : ((s: unknown) => s)) as Record<string, unknown>;
    const domain = useDomainStore(selector ? undefined : ((s: unknown) => s)) as Record<string, unknown>;
    const realtime = useRealtimeStore(selector ? undefined : ((s: unknown) => s)) as Record<string, unknown>;

    const merged = { ...auth, ...ui, ...domain, ...realtime } as unknown as GridStore;

    if (selector) {
        return selector(merged);
    }
    return merged as unknown as T;
}

// Also make getState() work for imperative access (e.g., in api.ts WebSocket)
useStore.getState = (): GridStore => ({
    ...useAuthStore.getState(),
    ...useUiStore.getState(),
    ...useDomainStore.getState(),
    ...useRealtimeStore.getState(),
} as GridStore);

export default useStore;
