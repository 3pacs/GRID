import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    VIEW_REQUEST_TIMEOUT_MS, apiErrorMessage, isApiError, optionalApi, requireApi, withRequestTimeout,
} from '../utils/apiResult.js';

describe('apiResult helpers', () => {
    afterEach(() => { vi.useRealTimers(); });

    it('recognises api.js failure markers and backend error strings, not payloads', () => {
        expect(isApiError({ error: true, status: 503, message: 'down' })).toBe(true);
        expect(isApiError({ error: 'no snapshot' })).toBe(true);
        expect(isApiError({ error: null, data: [] })).toBe(false);
        expect(isApiError({ features: [] })).toBe(false);
        expect(isApiError(null)).toBe(false);
    });

    it('extracts a reason, never a bare boolean', () => {
        expect(apiErrorMessage({ error: true, message: 'Service Unavailable' })).toBe('Service Unavailable');
        expect(apiErrorMessage({ error: 'no snapshot' })).toBe('no snapshot');
        expect(apiErrorMessage({ error: true, status: 502 }, 'Globe data failed')).toBe('Globe data failed (HTTP 502)');
        expect(apiErrorMessage(new Error('boom'))).toBe('boom');
    });

    it('resolves a timeout marker after the view budget without rejecting', async () => {
        vi.useFakeTimers();
        const pending = withRequestTimeout(new Promise(() => {}), { label: 'Unified thesis' });
        await vi.advanceTimersByTimeAsync(VIEW_REQUEST_TIMEOUT_MS);
        const result = await pending;
        expect(result).toMatchObject({ error: true, code: 'request_timeout' });
        expect(result.message).toMatch(/^Unified thesis did not respond within 45 s/);
    });

    it('passes a prompt result through and clears its timer', async () => {
        vi.useFakeTimers();
        await expect(withRequestTimeout(Promise.resolve({ ok: 1 }))).resolves.toEqual({ ok: 1 });
        expect(vi.getTimerCount()).toBe(0);
    });

    it('requireApi throws the reason; optionalApi reports it', async () => {
        await expect(requireApi(Promise.resolve({ error: true, status: 500, message: 'db timeout' }), { label: 'Logs' }))
            .rejects.toThrow('db timeout');
        await expect(optionalApi(Promise.resolve({ error: true, message: 'cold cache' })))
            .resolves.toEqual({ value: null, error: 'cold cache' });
        await expect(optionalApi(Promise.reject(new Error('net'))))
            .resolves.toEqual({ value: null, error: 'net' });
        await expect(optionalApi(Promise.resolve({ rows: [] })))
            .resolves.toEqual({ value: { rows: [] }, error: null });
    });
});
