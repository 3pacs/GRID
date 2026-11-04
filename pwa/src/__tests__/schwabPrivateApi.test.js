/**
 * api.js: the Schwab private endpoints are never cached client-side and the
 * symbol list is normalised before it reaches the URL.
 */
import { beforeEach, describe, expect, it, vi } from 'vitest';

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

function ok(body) {
    return { ok: true, status: 200, headers: { get: () => 'application/json' }, json: async () => body, text: async () => JSON.stringify(body) };
}

describe('Schwab private api methods', () => {
    beforeEach(() => { global.fetch.mockReset(); api.token = 'admin-token'; });

    it('private paths are excluded from the GET cache', () => {
        expect(api._isCacheableGet('/api/v1/schwab/private/quotes?symbols=SPY')).toBe(false);
        expect(api._isCacheableGet('/api/v1/schwab/private/status')).toBe(false);
    });

    it('normalises symbols and sends the bearer header', async () => {
        global.fetch.mockResolvedValue(ok({ view: { rows: {} } }));
        await api.getSchwabPrivateQuotes([' spy ', 'qqq', '', null]);
        const [url, options] = global.fetch.mock.calls[0];
        expect(url).toBe('http://localhost:8000/api/v1/schwab/private/quotes?symbols=SPY%2CQQQ');
        expect(options.headers.Authorization).toBe('Bearer admin-token');
    });

    it('accepts a single symbol string', async () => {
        global.fetch.mockResolvedValue(ok({ view: { rows: {} } }));
        await api.getSchwabPrivateQuotes('iwm');
        expect(global.fetch.mock.calls[0][0]).toMatch(/symbols=IWM$/);
    });

    it('does not serve a second call from cache', async () => {
        global.fetch.mockResolvedValue(ok({ view: { rows: {} } }));
        await api.getSchwabPrivateStatus();
        await api.getSchwabPrivateStatus();
        expect(global.fetch).toHaveBeenCalledTimes(2);
    });

    it('returns the error object on 403 instead of throwing', async () => {
        global.fetch.mockResolvedValue({ ok: false, status: 403, headers: { get: () => 'application/json' }, text: async () => '{"detail":"Requires role: admin"}', json: async () => ({ detail: 'Requires role: admin' }) });
        const result = await api.getSchwabPrivateQuotes('SPY');
        expect(result.error).toBe(true);
        expect(result.status).toBe(403);
    });
});
