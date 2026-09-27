/**
 * God view store (G9) — the /api/v1/godview/latest payload and its load state.
 *
 * api.js resolves `{ error: true, status, message }` instead of rejecting;
 * that marker is kept out of `latest` and surfaced as `error`, so a failed
 * request can never render as an (empty) god view.
 */
import { create } from 'zustand';
import { api } from '../api.js';

const useGodViewStore = create((set, get) => ({
    latest: null,
    loading: false,
    error: null,
    requestSeq: 0,

    loadLatest: async ({ asOf = null } = {}) => {
        const seq = get().requestSeq + 1;
        set({ loading: true, error: null, requestSeq: seq });
        let result;
        try {
            result = await api.getGodViewLatest({ asOf });
        } catch (err) {
            result = { error: true, message: err?.message || 'Request failed' };
        }
        if (get().requestSeq !== seq) return; // a newer request superseded this one
        if (!result || typeof result !== 'object' || result.error === true || !result.pillars) {
            set({
                loading: false,
                latest: null,
                error: {
                    status: result?.status ?? null,
                    message: result?.message || 'God view response was not a pillar payload',
                },
            });
            return;
        }
        set({ loading: false, latest: result, error: null });
    },

    reset: () => set({ latest: null, loading: false, error: null }),
}));

export default useGodViewStore;
