import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
const route = '/api/v1/surfacer/candidates?limit=18&fresh_only=false&queue_missing_data=false';
const deferred = () => { let resolve; const promise = new Promise(done => { resolve = done; }); return { promise, resolve }; };
const ok = data => ({ ok: true, status: 200, json: async () => data });
beforeEach(() => { vi.useFakeTimers(); api.clearCache(); localStorage.clear(); vi.stubGlobal('fetch', vi.fn()); });
afterEach(() => { vi.useRealTimers(); vi.unstubAllGlobals(); });
describe('Surfacer bounded shared transport', () => {
    it('deduplicates calls and preserves the 60-second successful TTL', async () => {
        const response = deferred(); fetch.mockReturnValueOnce(response.promise);
        const first = api.get(route), second = api.get(route);
        expect(fetch).toHaveBeenCalledTimes(1); response.resolve(ok({ candidates: [] }));
        await Promise.all([first, second]); await vi.advanceTimersByTimeAsync(59999); await api.get(route);
        expect(fetch).toHaveBeenCalledTimes(1); await vi.advanceTimersByTimeAsync(1);
        fetch.mockResolvedValueOnce(ok({ candidates: [] })); await api.get(route); expect(fetch).toHaveBeenCalledTimes(2);
    });
    it('aborts timeout and recovers without late cache pollution', async () => {
        const old = deferred(), recovery = deferred(); fetch.mockReturnValueOnce(old.promise).mockReturnValueOnce(recovery.promise);
        const first = api.get(route), signal = fetch.mock.calls[0][1].signal;
        await vi.advanceTimersByTimeAsync(30000); expect(await first).toMatchObject({ error: true, code: 'request_timeout' });
        expect(signal.aborted).toBe(true); const retry = api.get(route); old.resolve(ok({ late: true }));
        await vi.advanceTimersByTimeAsync(0); expect(api._cache.has(route)).toBe(false); expect(api._inflight.size).toBe(1);
        recovery.resolve(ok({ recovered: true })); expect(await retry).toEqual({ recovered: true }); expect(api._inflight.size).toBe(0);
    });
    it.each([true, false])('bounds body parsing when response.ok=%s', async successful => {
        const body = deferred(); fetch.mockResolvedValueOnce({ ok: successful, status: successful ? 200 : 503,
            statusText: 'Unavailable', json: () => body.promise, text: () => body.promise });
        const request = api.get(route); await vi.advanceTimersByTimeAsync(30000);
        expect(await request).toMatchObject({ code: 'request_timeout' }); body.resolve(successful ? { late: true } : 'Unavailable');
        await vi.advanceTimersByTimeAsync(0); expect(api._cache.has(route)).toBe(false);
    });
    it('shares pending preload with foreground read', async () => {
        const response = deferred(); fetch.mockReturnValueOnce(response.promise);
        const preload = api.preload([route]), foreground = api.get(route); expect(fetch).toHaveBeenCalledTimes(1);
        response.resolve(ok({ shared: true })); await preload; expect(await foreground).toEqual({ shared: true });
    });
    it('returns network and HTTP errors without caching then recovers', async () => {
        fetch.mockRejectedValueOnce(new Error('synthetic offline'))
            .mockResolvedValueOnce({ ok: false, status: 503, statusText: 'Unavailable', text: async () => '' })
            .mockResolvedValueOnce(ok({ recovered: true }));
        expect(await api.get(route)).toMatchObject({ error: true, status: 0 });
        expect(await api.get(route)).toMatchObject({ error: true, status: 503 }); expect(api._cache.has(route)).toBe(false);
        expect(await api.get(route)).toEqual({ recovered: true }); expect(fetch).toHaveBeenCalledTimes(3);
    });
});
