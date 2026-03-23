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
    });

    describe('_fetch', () => {
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
    });

    describe('API methods', () => {
        beforeEach(() => {
            global.fetch.mockResolvedValue({
                ok: true,
                status: 200,
                json: () => Promise.resolve({ data: 'test' }),
            });
        });

        it('getStatus calls correct endpoint', async () => {
            await api.getStatus();
            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining('/api/v1/system/status'),
                expect.any(Object),
            );
        });

        it('getCurrent calls regime endpoint', async () => {
            await api.getCurrent();
            expect(global.fetch).toHaveBeenCalledWith(
                expect.stringContaining('/api/v1/regime/current'),
                expect.any(Object),
            );
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
