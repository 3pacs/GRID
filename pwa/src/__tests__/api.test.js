/**
 * Tests for API client (api.js).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

// Mock localStorage
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

// Mock window.location
Object.defineProperty(global, 'window', {
    value: {
        location: { origin: 'http://localhost:8000', protocol: 'http:', host: 'localhost:8000', hash: '' },
        localStorage: localStorageMock,
    },
});

// Mock fetch
global.fetch = vi.fn();

const { api, GRIDApiError } = await import('../api.js');

describe('GRIDApi', () => {
    beforeEach(() => {
        localStorageMock.clear();
        global.fetch.mockReset();
        api.token = null;
        vi.clearAllMocks();
    });

    describe('constructor defaults', () => {
        it('sets baseUrl from window.location.origin', () => {
            expect(api.baseUrl).toBe('http://localhost:8000');
        });

        it('has null _ws initially', () => {
            expect(api._ws).toBeNull();
        });

        it('has reconnect delay defaults', () => {
            expect(api._wsReconnectDelay).toBe(1000);
            expect(api._wsMaxDelay).toBe(30000);
        });
    });

    describe('token getter/setter', () => {
        it('stores token via setter', () => {
            api.token = 'my-token';
            expect(localStorageMock.setItem).toHaveBeenCalledWith('grid_token', 'my-token');
        });

        it('returns token via getter', () => {
            localStorageMock.setItem('grid_token', 'stored-token');
            expect(api.token).toBe('stored-token');
        });

        it('removes token when set to null', () => {
            api.token = 'to-remove';
            api.token = null;
            expect(localStorageMock.removeItem).toHaveBeenCalledWith('grid_token');
        });
    });

    describe('_fetch headers', () => {
        it('includes Authorization header when token is set', async () => {
            localStorageMock.setItem('grid_token', 'my-token');

            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({ status: 'ok' }),
            });

            await api._fetch('/api/v1/system/health');

            expect(global.fetch).toHaveBeenCalledWith(
                'http://localhost:8000/api/v1/system/health',
                expect.objectContaining({
                    headers: expect.objectContaining({
                        Authorization: 'Bearer my-token',
                    }),
                }),
            );
        });

        it('does not include Authorization when no token', async () => {
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({}),
            });

            await api._fetch('/api/v1/test');

            const callArgs = global.fetch.mock.calls[0];
            expect(callArgs[1].headers['Authorization']).toBeUndefined();
        });

        it('always includes Content-Type json header', async () => {
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({}),
            });

            await api._fetch('/api/v1/test');

            const callArgs = global.fetch.mock.calls[0];
            expect(callArgs[1].headers['Content-Type']).toBe('application/json');
        });
    });

    describe('getCurrent', () => {
        it('fetches /api/v1/regime/current', async () => {
            const mockData = { regime: 'risk-off', confidence: 0.9 };
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve(mockData),
            });

            const result = await api.getCurrent();

            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining('/api/v1/regime/current'),
                expect.any(Object),
            );
            expect(result).toEqual(mockData);
        });
    });

    describe('getStatus', () => {
        it('fetches /api/v1/system/status', async () => {
            const mockData = { status: 'healthy', uptime: 3600 };
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve(mockData),
            });

            const result = await api.getStatus();

            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining('/api/v1/system/status'),
                expect.any(Object),
            );
            expect(result).toEqual(mockData);
        });
    });

    describe('error handling', () => {
        it('throws GRIDApiError on non-ok response', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 422,
                statusText: 'Unprocessable Entity',
                json: () => Promise.resolve({ detail: 'Invalid data' }),
            });

            await expect(api._fetch('/api/v1/journal', { method: 'POST' }))
                .rejects.toThrow('Invalid data');
        });

        it('GRIDApiError contains status and detail', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 422,
                statusText: 'Unprocessable Entity',
                json: () => Promise.resolve({ detail: 'Validation error' }),
            });

            try {
                await api._fetch('/api/v1/fail');
                expect.fail('Should have thrown');
            } catch (err) {
                expect(err).toBeInstanceOf(GRIDApiError);
                expect(err.status).toBe(422);
                expect(err.message).toBe('Validation error');
            }
        });

        it('clears token on 401', async () => {
            localStorageMock.setItem('grid_token', 'expired-token');

            global.fetch.mockResolvedValue({
                ok: false,
                status: 401,
                json: () => Promise.resolve({ detail: 'Unauthorized' }),
            });

            await expect(api._fetch('/api/v1/regime/current'))
                .rejects.toThrow();

            expect(localStorageMock.removeItem).toHaveBeenCalledWith('grid_token');
        });

        it('handles json parse failure on error response', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 500,
                statusText: 'Internal Server Error',
                json: () => Promise.reject(new Error('not json')),
            });

            await expect(api._fetch('/api/v1/fail'))
                .rejects.toThrow(GRIDApiError);
        });
    });

    describe('API methods', () => {
        beforeEach(() => {
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({ data: 'test' }),
            });
        });

        it('login sends password in body', async () => {
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({ token: 'new-token', expires_in: 604800 }),
            });

            const result = await api.login('my-password');
            expect(result.token).toBe('new-token');
            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining('/api/v1/auth/login'),
                expect.objectContaining({
                    method: 'POST',
                    body: JSON.stringify({ password: 'my-password' }),
                }),
            );
        });
    });
});
