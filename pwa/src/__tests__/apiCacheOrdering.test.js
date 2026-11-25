/**
 * #959 Codex follow-up: a slow older GET must never overwrite newer cached
 * data, and a GET that started before a cache clear must not repopulate it.
 */
import { describe, it, expect, beforeEach, vi } from 'vitest';

const localStorageMock = (() => {
    let store = {};
    return {
        getItem: vi.fn((key) => store[key] || null),
        setItem: vi.fn((key, val) => { store[key] = val; }),
        removeItem: vi.fn((key) => { delete store[key]; }),
        clear: vi.fn(() => { store = {}; }),
    };
})();
Object.defineProperty(global, 'localStorage', { value: localStorageMock });
Object.defineProperty(global, 'window', {
    value: {
        location: { origin: 'http://localhost:8000', protocol: 'http:', host: 'localhost:8000', hash: '' },
        localStorage: localStorageMock,
    },
});
global.fetch = vi.fn();

const { api } = await import('../api.js');

function deferredResponse() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, send: (data) => resolve({ ok: true, status: 200, json: async () => data }) };
}
const ok = (data) => Promise.resolve({ ok: true, status: 200, json: async () => data });

describe('api GET cache ordering', () => {
    beforeEach(() => {
        global.fetch.mockReset();
        api.clearCache();
    });

    it('a GET that started before a clear does not repopulate the cache', async () => {
        const slow = deferredResponse();
        global.fetch.mockReturnValueOnce(slow.promise).mockReturnValueOnce(ok({ v: 'new' }));
        const first = api._fetch('/api/v1/example');
        api.clearCache();
        slow.send({ v: 'old' });
        await first;
        const second = await api._fetch('/api/v1/example');
        expect(second).toEqual({ v: 'new' });
        expect(global.fetch).toHaveBeenCalledTimes(2);
    });

    it('a slow older response does not overwrite a newer one', async () => {
        const slow = deferredResponse();
        global.fetch.mockReturnValueOnce(slow.promise).mockReturnValueOnce(ok({ v: 'new' }));
        const older = api._fetch('/api/v1/example');
        await api._fetch('/api/v1/example', { gridForce: true });
        slow.send({ v: 'old' });
        await older;
        expect(await api._fetch('/api/v1/example')).toEqual({ v: 'new' });
        expect(global.fetch).toHaveBeenCalledTimes(2);
    });
});
