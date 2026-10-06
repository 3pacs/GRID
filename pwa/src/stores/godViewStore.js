/**
 * God view store (G9) — the /api/v1/godview/latest payload and its load state.
 *
 * api.js resolves `{ error: true, status, message }` instead of rejecting;
 * that marker is kept out of `latest` and surfaced as `error`, so a failed
 * request can never render as an (empty) god view.
 */
import { create } from 'zustand';
import { api } from '../api.js';
import { createReadScope, safeRequestMessage, requireRead } from '../requestState.js';

let activeScope = null;

const useGodViewStore = create((set, get) => ({
    latest: null,
    loading: false,
    error: null,
    requestSeq: 0,

    loadLatest: async ({ asOf = null, force = true } = {}) => {
        activeScope?.cancel();
        const scope = createReadScope({ force });
        activeScope = scope;
        const seq = get().requestSeq + 1;
        set({ loading: true, error: null, requestSeq: seq });
        let result;
        try {
            result = requireRead(await scope.run(api.getGodViewLatest({ asOf, ...scope.options })));
        } catch (err) {
            result = { error: true, status: err?.status, code: err?.code };
        } finally {
            scope.finish();
        }
        if (get().requestSeq !== seq) return; // a newer request superseded this one
        if (!result || typeof result !== 'object' || result.error === true || !result.pillars) {
            set({
                loading: false,
                error: {
                    status: result?.status ?? null,
                    message: safeRequestMessage(result?.error ? result : { code: 'invalid_response' }),
                },
            });
            return;
        }
        set({ loading: false, latest: result, error: null });
    },

    cancel: () => {
        activeScope?.cancel();
        set({ loading: false, requestSeq: get().requestSeq + 1 });
    },
    reset: () => {
        activeScope?.cancel();
        set({ latest: null, loading: false, error: null, requestSeq: get().requestSeq + 1 });
    },
}));

export default useGodViewStore;
