import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import { READ_DEADLINE_MS } from '../requestState.js';

const path = '/api/v1/godview/latest';
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const response = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const never = () => new Promise(() => {});

beforeEach(() => { vi.useFakeTimers(); api.clearCache(); global.fetch = vi.fn(); });
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });

describe('materialization read transport', () => {
    it.each(['fetch', 'body'])('bounds a never-settling %s and retries successfully', async stage => {
        global.fetch.mockImplementationOnce(stage === 'fetch' ? never : () => Promise.resolve({ ok: true, status: 200, json: never }));
        const pending = api.getGodViewLatest();
        await vi.advanceTimersByTimeAsync(READ_DEADLINE_MS);
        expect(await pending).toMatchObject({ error: true, code: 'timeout' });
        expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(true);
        global.fetch.mockResolvedValue(response({ pillars: {}, version: 2 }));
        expect(await api.getGodViewLatest({ gridForce: true })).toMatchObject({ version: 2 });
        expect(api._inflight.size).toBe(0);
    });

    it.each([524, 500, 422, 403])('does not read or expose private HTTP %s bodies', async status => {
        const text = vi.fn(() => Promise.resolve('<html>Cloudflare private database detail</html>'));
        global.fetch.mockResolvedValue({ ok: false, status, text });
        const result = await api.get(path);
        expect(result.error).toBe(true);
        expect(result.status).toBe(status);
        expect(result.message).not.toMatch(/html|Cloudflare|private|database/);
        expect(text).not.toHaveBeenCalled();
    });

    it('normalizes thrown network errors and successful-body parse failures', async () => {
        global.fetch.mockRejectedValueOnce(new Error('private host secret'));
        expect(await api.get(path)).toMatchObject({ error: true, code: 'network', message: 'The server could not be reached. Please retry.' });
        global.fetch.mockResolvedValueOnce({ ok: true, status: 200, json: () => Promise.reject(new Error('secret parser')) });
        expect(await api.get(path)).toMatchObject({ error: true, code: 'invalid_response' });
    });

    it('does not turn the service-worker offline sentinel into a cached empty success', async () => {
        global.fetch.mockResolvedValue(response({ error: 'offline' }));
        expect(await api.get(path)).toMatchObject({ error: true, code: 'network' });
        expect(api._cache.has(path)).toBe(false);
    });

    it('does not cache HTTP 200 domain error envelopes as fresh data', async () => {
        const path = '/api/v1/options/signals';
        global.fetch.mockResolvedValue(response({ error: 'private database detail', signals: [] }));
        await api.get(path);
        await api.get(path);
        expect(api._cache.has(path)).toBe(false);
        expect(global.fetch).toHaveBeenCalledTimes(2);
    });

    it('expires auth immediately on 401 even when its body never settles', async () => {
        localStorage.setItem('grid_token', 'synthetic-token');
        global.fetch.mockResolvedValue({ ok: false, status: 401, text: never });
        expect(await api.get(path)).toMatchObject({ status: 401 });
        expect(localStorage.getItem('grid_token')).toBeNull();
        expect(window.location.hash).toBe('#/login');
    });

    it('shares a read without letting one subscriber cancel another', async () => {
        const gate = deferred();
        global.fetch.mockReturnValue(gate.promise);
        const firstOwner = new AbortController();
        const secondOwner = new AbortController();
        const first = api.get(path, { signal: firstOwner.signal });
        const second = api.get(path, { signal: secondOwner.signal });
        expect(global.fetch).toHaveBeenCalledTimes(1);
        firstOwner.abort();
        expect(await first).toMatchObject({ code: 'cancelled' });
        expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(false);
        gate.resolve(response({ pillars: {}, zero: 0 }));
        expect(await second).toMatchObject({ zero: 0 });
    });

    it('aborts transport after the last subscriber leaves and discards late body results', async () => {
        const body = deferred();
        global.fetch.mockResolvedValue({ ok: true, status: 200, json: () => body.promise });
        const owner = new AbortController();
        const pending = api.get(path, { signal: owner.signal });
        await vi.advanceTimersByTimeAsync(1);
        owner.abort();
        expect(await pending).toMatchObject({ code: 'cancelled' });
        expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(true);
        body.resolve({ pillars: {}, version: 'old' });
        await vi.advanceTimersByTimeAsync(1);
        expect(api._cache.has(path)).toBe(false);
    });

    it('force bypasses cached and doomed in-flight reads; older completion cannot replace its cache', async () => {
        const old = deferred();
        global.fetch.mockReturnValueOnce(old.promise).mockResolvedValueOnce(response({ version: 2 }));
        const first = api.get(path);
        expect(await api.get(path, { gridForce: true })).toEqual({ version: 2 });
        old.resolve(response({ version: 1 }));
        await first;
        expect(await api.get(path)).toEqual({ version: 2 });
        expect(global.fetch).toHaveBeenCalledTimes(2);
        expect(global.fetch.mock.calls[1][1].cache).toBe('no-store');
    });

    it('clearCache fences prior completion and keeps response receipt age unchanged on a cache hit', async () => {
        global.fetch.mockResolvedValue(response({ pillars: {}, as_of: '2020-01-01' }));
        const first = await api.get(path);
        const receivedAt = api.getResponseInfo(first).receivedAt;
        await vi.advanceTimersByTimeAsync(20_000);
        const cached = await api.get(path);
        expect(api.getResponseInfo(cached)).toEqual({ receivedAt, ageMs: 20_000 });
        expect(cached.as_of).toBe('2020-01-01');
        const old = deferred();
        global.fetch.mockReturnValueOnce(old.promise);
        const pending = api.get(path, { gridForce: true });
        api.clearCache();
        old.resolve(response({ version: 'pre-invalidation' }));
        await pending;
        expect(api._cache.has(path)).toBe(false);
    });

    it('gives foreground a fresh start and pauses a slow warm queue with bounded concurrency', async () => {
        global.fetch.mockImplementation(url => url.endsWith(path) ? Promise.resolve(response({ selected: true })) : never());
        const warming = api.preload(['/api/v1/background/a', '/api/v1/background/b', '/api/v1/background/c'], { concurrency: 8 });
        expect(global.fetch).toHaveBeenCalledTimes(2);
        const selected = api.get(path);
        expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(true);
        expect(global.fetch.mock.calls[1][1].signal.aborted).toBe(true);
        expect(await selected).toEqual({ selected: true });
        await vi.advanceTimersByTimeAsync(READ_DEADLINE_MS);
        await warming;
        expect(global.fetch).toHaveBeenCalledTimes(4);
    });

    it('bounds the entire warm queue including pauses behind an unrelated hung foreground read', async () => {
        const foregroundGate = deferred();
        global.fetch.mockReturnValue(foregroundGate.promise);
        const foreground = api.get('/api/v1/unrelated-long-read');
        const warming = api.preload(['/api/v1/background/a', '/api/v1/background/b'], { budgetMs: 45_000 });
        await vi.advanceTimersByTimeAsync(45_000);
        expect(await warming).toMatchObject({ completed: 0, skipped: 2 });
        expect(global.fetch).toHaveBeenCalledTimes(1);
        foregroundGate.resolve(response({ ok: true }));
        await foreground;
        expect(api._foregroundWaiters.size).toBe(0);
    });

    it('navigation cancellation ends warm transport without touching an owned foreground request', async () => {
        global.fetch.mockImplementation(never);
        const navigation = new AbortController();
        const warming = api.preload(['/api/v1/background/a', '/api/v1/background/b', '/api/v1/background/c'], { signal: navigation.signal });
        navigation.abort();
        expect(await warming).toMatchObject({ completed: 2, failed: 2, skipped: 1 });
        expect(global.fetch.mock.calls.every(([, options]) => options.signal.aborted)).toBe(true);
    });

    it('does not apply the materialization deadline to mutations or unrelated long reads', async () => {
        const mutation = deferred();
        global.fetch.mockReturnValue(mutation.promise);
        const pending = api.post('/api/v1/options/recommendations/refresh');
        await vi.advanceTimersByTimeAsync(READ_DEADLINE_MS + 1);
        expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(false);
        mutation.resolve(response({ saved: true }));
        expect(await pending).toEqual({ saved: true });
    });
});
