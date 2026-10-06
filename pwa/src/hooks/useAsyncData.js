/**
 * useAsyncData — universal async data fetching hook.
 *
 * Replaces the 50+ duplicated useState(loading) + useEffect + catch patterns
 * across GRID views. Handles loading, error, refetch, and stale-while-revalidate.
 *
 * Usage:
 *   const { data, loading, error, refetch } = useAsyncData(
 *       () => api.getTrustScores(),
 *       { fallback: [] }
 *   );
 *
 *   const { data, loading } = useAsyncData(
 *       () => Promise.all([api.getA(), api.getB()]),
 *       { fallback: [null, null] }
 *   );
 */

import { useState, useEffect, useCallback, useRef } from 'react';
import { createReadScope, safeRequestMessage } from '../requestState.js';

/**
 * @param {() => Promise<T>} fetcher - Async function that returns the data
 * @param {Object} options
 * @param {T} options.fallback - Default value while loading or on error
 * @param {any[]} options.deps - Extra dependencies to trigger refetch (default: [])
 * @param {boolean} options.skip - Skip the initial fetch (default: false)
 * @returns {{ data: T, loading: boolean, error: Error|null, refetch: () => void, stale: boolean }}
 *
 * `api.js` never rejects on network/HTTP/parse failure — it resolves an
 * `{ error: true, status, message }` marker instead. A resolved marker is
 * routed through the same `setError` path as a thrown error, so `data` is
 * never left holding it.
 */
export function useAsyncData(fetcher, options = {}) {
    const { fallback = null, deps = [], skip = false, timeoutMs = null, safeErrors = false, resetOnChange = false } = options;

    const [data, setData] = useState(fallback);
    const [loading, setLoading] = useState(!skip);
    const [error, setError] = useState(null);
    const [stale, setStale] = useState(false);
    const mountedRef = useRef(true);
    const fetcherRef = useRef(fetcher);
    fetcherRef.current = fetcher;
    const scopeRef = useRef(null);
    const sequenceRef = useRef(0);
    const hasDataRef = useRef(false);

    const refetch = useCallback(async ({ force = true } = {}) => {
        if (!mountedRef.current) return;
        const sequence = ++sequenceRef.current;
        scopeRef.current?.cancel();
        const scope = createReadScope({ force, timeoutMs });
        scopeRef.current = scope;
        const owns = () => mountedRef.current && sequence === sequenceRef.current;
        const current = () => owns() && !scope.signal.aborted;
        setLoading(true);
        setError(null);
        if (hasDataRef.current) setStale(true);

        try {
            const result = await scope.run(fetcherRef.current({ ...scope, current }));
            if (result && typeof result === 'object' && result.error === true) {
                const err = new Error(result.message || 'Request failed');
                err.status = result.status;
                err.code = result.code;
                throw err;
            }
            if (current()) {
                setData(result);
                hasDataRef.current = true;
                setStale(false);
            }
        } catch (err) {
            if (owns()) {
                const error = safeErrors ? Object.assign(new Error(safeRequestMessage(err)), { status: err?.status, code: err?.code }) : err;
                setError(error);
            }
        } finally {
            scope.finish();
            if (owns()) {
                setLoading(false);
            }
        }
    }, deps); // eslint-disable-line react-hooks/exhaustive-deps

    useEffect(() => {
        mountedRef.current = true;
        if (resetOnChange) { setData(fallback); setStale(false); hasDataRef.current = false; }
        if (!skip) {
            refetch({ force: false });
        } else {
            setLoading(false);
        }
        return () => { mountedRef.current = false; sequenceRef.current += 1; scopeRef.current?.cancel(); };
    }, [refetch, skip]);

    return { data, loading, error, refetch, stale };
}
