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

/**
 * @param {() => Promise<T>} fetcher - Async function that returns the data
 * @param {Object} options
 * @param {T} options.fallback - Default value while loading or on error
 * @param {any[]} options.deps - Extra dependencies to trigger refetch (default: [])
 * @param {boolean} options.skip - Skip the initial fetch (default: false)
 * @returns {{ data: T, loading: boolean, error: Error|null, refetch: () => void, stale: boolean }}
 */
export function useAsyncData(fetcher, options = {}) {
    const { fallback = null, deps = [], skip = false } = options;

    const [data, setData] = useState(fallback);
    const [loading, setLoading] = useState(!skip);
    const [error, setError] = useState(null);
    const [stale, setStale] = useState(false);
    const mountedRef = useRef(true);
    const fetcherRef = useRef(fetcher);
    fetcherRef.current = fetcher;

    const refetch = useCallback(async () => {
        if (!mountedRef.current) return;
        setLoading(true);
        setError(null);
        if (data !== fallback) setStale(true);

        try {
            const result = await fetcherRef.current();
            if (mountedRef.current) {
                setData(result);
                setStale(false);
            }
        } catch (err) {
            if (mountedRef.current) {
                setError(err);
            }
        } finally {
            if (mountedRef.current) {
                setLoading(false);
            }
        }
    }, deps); // eslint-disable-line react-hooks/exhaustive-deps

    useEffect(() => {
        mountedRef.current = true;
        if (!skip) {
            refetch();
        }
        return () => { mountedRef.current = false; };
    }, [refetch, skip]);

    return { data, loading, error, refetch, stale };
}
