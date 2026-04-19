/**
 * Tests for API client (api.js).
 */

import { describe, it, expect, beforeEach, vi } from 'vitest';

const originalWebSocket = global.WebSocket;

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

const { api } = await import('../api.js');

describe('GRIDApi', () => {
    beforeEach(() => {
        localStorageMock.clear();
        global.fetch.mockReset();
        api.token = null;
        global.WebSocket = originalWebSocket;
        vi.useRealTimers();
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

    describe('websocket lifecycle', () => {
        it('does not reconnect after an intentional disconnect', () => {
            vi.useFakeTimers();
            const sockets = [];

            global.WebSocket = vi.fn(function MockWebSocket(url) {
                this.url = url;
                this.close = vi.fn(() => {
                    this.onclose?.();
                });
                sockets.push(this);
            });

            localStorageMock.setItem('grid_token', 'ws-token');
            api.connectWebSocket(vi.fn());
            expect(global.WebSocket).toHaveBeenCalledTimes(1);

            sockets[0].onclose();
            expect(api._wsReconnectTimer).not.toBeNull();

            api.disconnectWebSocket();
            vi.runAllTimers();

            expect(global.WebSocket).toHaveBeenCalledTimes(1);

            vi.useRealTimers();
            global.WebSocket = originalWebSocket;
        });

        it('does not let a stale socket close schedule a new reconnect', () => {
            vi.useFakeTimers();
            const sockets = [];

            global.WebSocket = vi.fn(function MockWebSocket(url) {
                this.url = url;
                this.close = vi.fn(() => {
                    this.onclose?.();
                });
                sockets.push(this);
            });

            localStorageMock.setItem('grid_token', 'ws-token');
            api.connectWebSocket(vi.fn());
            api.connectWebSocket(vi.fn());

            expect(global.WebSocket).toHaveBeenCalledTimes(2);

            vi.runAllTimers();
            expect(global.WebSocket).toHaveBeenCalledTimes(2);

            api.disconnectWebSocket();
            vi.useRealTimers();
            global.WebSocket = originalWebSocket;
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
        it('returns an error object on non-ok response', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 422,
                statusText: 'Unprocessable Entity',
                text: () => Promise.resolve(JSON.stringify({ detail: 'Invalid data' })),
            });

            const result = await api._fetch('/api/v1/journal', { method: 'POST' });

            expect(result).toEqual({
                error: true,
                status: 422,
                message: 'Invalid data',
            });
        });

        it('error object contains status and detail', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 422,
                statusText: 'Unprocessable Entity',
                text: () => Promise.resolve(JSON.stringify({ detail: 'Validation error' })),
            });

            const result = await api._fetch('/api/v1/fail');

            expect(result.error).toBe(true);
            expect(result.status).toBe(422);
            expect(result.message).toBe('Validation error');
        });

        it('clears token on 401', async () => {
            localStorageMock.setItem('grid_token', 'expired-token');

            global.fetch.mockResolvedValue({
                ok: false,
                status: 401,
                statusText: 'Unauthorized',
                text: () => Promise.resolve(JSON.stringify({ detail: 'Unauthorized' })),
            });

            const result = await api._fetch('/api/v1/regime/current');

            expect(result.error).toBe(true);
            expect(result.status).toBe(401);
            expect(localStorageMock.removeItem).toHaveBeenCalledWith('grid_token');
        });

        it('handles body parse failure on error response', async () => {
            global.fetch.mockResolvedValue({
                ok: false,
                status: 500,
                statusText: 'Internal Server Error',
                text: () => Promise.reject(new Error('not text')),
            });

            const result = await api._fetch('/api/v1/fail');

            expect(result).toEqual({
                error: true,
                status: 500,
                message: 'Internal Server Error',
            });
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
