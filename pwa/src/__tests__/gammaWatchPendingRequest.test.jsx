// @vitest-environment jsdom
import React, { StrictMode } from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import GammaWatch from '../components/GammaWatch.jsx';
import Options from '../views/Options.jsx';
import ViewErrorBoundary from '../components/ViewErrorBoundary.jsx';
import { parseHashRoute } from '../routing.js';

// The API module and its complete fetch/body operation are real. Only external
// transports and unrelated store/graphics dependencies are synthetic. The auth
// module is also real and uses only this jsdom instance's synthetic storage.
vi.mock('../stores/realtimeStore.js', () => ({ default: { getState: () => ({}) } }));
vi.mock('../components/GEXProfile.jsx', () => ({ default: () => null }));
vi.mock('../components/VannaCharmViz.jsx', () => ({ default: () => null }));
vi.mock('../components/FlowTimeline.jsx', () => ({ default: () => null }));

const path = '/api/v1/gamma-watch/state';
const deferred = () => {
    let resolve;
    const promise = new Promise(yes => { resolve = yes; });
    return { promise, resolve };
};
const frame = () => {
    const now = Date.now() / 1000;
    return { status: 'available', data: {
        schema: 'gex-screen/2', symbol: 'SPY', served_at: now, evaluation_clock: 'ok',
        public_price: { status: 'unverified-public-latency', source: 'Yahoo chart · public, latency not guaranteed',
            source_time: now - 5, receipt_time: now - 1, values_available: true,
            current_interpretation: false, max_context_age_seconds: 120, price: 600 },
    } };
};
const ok = value => ({ ok: true, status: 200, json: vi.fn().mockResolvedValue(value) });
const tick = async ms => { await act(async () => { await vi.advanceTimersByTimeAsync(ms); }); };
const flush = async () => { await act(async () => {}); };
const stateCalls = () => globalThis.fetch.mock.calls.filter(([url]) => url.endsWith(path));
const healthy = () => screen.queryByText(/^SPY 600 ·/);
function mockState(implementation) {
    vi.stubGlobal('fetch', vi.fn((url, options) => {
        if (url.endsWith(path)) return implementation(options);
        if (url.includes('/options/signals?')) return Promise.resolve(ok({ signals: [] }));
        if (url.includes('/options/scan?') || url.endsWith('/options/100x')) return Promise.resolve(ok({ opportunities: [] }));
        throw new Error(`Unexpected synthetic endpoint: ${url}`);
    }));
}

describe('Gamma Watch finite whole-request lifecycle with the real API client', () => {
    beforeEach(() => {
        vi.useFakeTimers(); vi.setSystemTime(1000000);
        api.clearCache(); localStorage.clear(); window.location.hash = '#/options';
    });
    afterEach(() => {
        cleanup(); vi.clearAllTimers(); vi.useRealTimers(); vi.unstubAllGlobals();
    });

    it.each(['before headers', 'JSON body', 'error text body'])('ends Connecting with honest unknown status when pending %s', async phase => {
        const held = deferred();
        const body = phase === 'JSON body' ? { ok: true, status: 200, json: () => held.promise }
            : { ok: false, status: 503, statusText: 'Unavailable', text: () => held.promise };
        mockState(() => phase === 'before headers' ? held.promise : Promise.resolve(body));
        render(<GammaWatch />); await flush();
        await tick(14999);
        expect(screen.getByRole('status').textContent).toContain('Connecting');
        await tick(10001);
        expect(screen.getByRole('status').textContent).toContain('Request timed out; collector status unknown');
        expect(screen.getByRole('status').textContent).not.toContain('Collector unavailable');
        expect(stateCalls()).toHaveLength(1);
        expect(stateCalls()[0][1].signal.aborted).toBe(true);
        await tick(60000);
        expect(stateCalls()).toHaveLength(1);
        expect(api._cache.has(path)).toBe(false);
        // Ignore-abort transports may finish late; no display or poll resurrection.
        held.resolve(phase === 'before headers' ? ok(frame()) : phase === 'JSON body' ? frame() : JSON.stringify({ status: 'unavailable', data: null }));
        await flush();
        expect(healthy()).toBeNull();
        expect(api._cache.has(path)).toBe(false);
        expect(screen.getByRole('status').textContent).toContain('Request timed out; collector status unknown');
        await tick(5000); expect(stateCalls()).toHaveLength(1);
    });

    it('reaches the actual Gamma Watch through the default Options route and bounds its pending request', async () => {
        mockState(() => deferred().promise);
        const route = parseHashRoute('#/options');
        render(<ViewErrorBoundary viewName="options"><Options selectedTicker={route.selectedTicker ?? null} /></ViewErrorBoundary>);
        await flush(); fireEvent.click(screen.getByRole('button', { name: 'Gamma Watch', exact: true }));
        await tick(15000);
        expect(screen.queryByText('options Error')).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Request timed out; collector status unknown');
        expect(stateCalls()).toHaveLength(1);
        expect(globalThis.fetch).toHaveBeenCalledTimes(4);
    });

    it('distinguishes an explicit collector-unavailable response from transport/HTTP uncertainty without raw diagnostics', async () => {
        mockState(() => Promise.resolve({ ok: false, status: 503, statusText: 'Unavailable',
            text: async () => JSON.stringify({ status: 'unavailable', data: null, reason: 'PRIVATE_SENTINEL' }) }));
        render(<GammaWatch />); await flush();
        expect(screen.getByRole('status').textContent).toBe('Collector unavailable; no cached substitute.');
        expect(document.body.textContent).not.toContain('PRIVATE_SENTINEL');
    });

    it.each(['network', 'HTTP error', 'invalid JSON'])('keeps %s failure distinct from a known collector status', async failure => {
        mockState(() => failure === 'network' ? Promise.reject(new Error('PRIVATE_SENTINEL'))
            : Promise.resolve(failure === 'HTTP error'
                ? { ok: false, status: 500, statusText: 'PRIVATE_SENTINEL', text: async () => 'PRIVATE_SENTINEL' }
                : { ok: true, status: 200, json: async () => { throw new Error('PRIVATE_SENTINEL'); } }));
        render(<GammaWatch />); await flush();
        expect(screen.getByRole('status').textContent).toContain('collector status unknown');
        expect(document.body.textContent).not.toContain('PRIVATE_SENTINEL');
        await tick(20000); expect(stateCalls()).toHaveLength(1);
    });

    it('preserves one serialized five-second poll and clears prior values when its next request times out', async () => {
        const held = deferred(); let count = 0;
        mockState(() => ++count === 1 ? Promise.resolve(ok(frame())) : held.promise);
        render(<GammaWatch />); await flush(); expect(healthy()).toBeTruthy();
        await tick(4999); expect(stateCalls()).toHaveLength(1);
        await tick(1); expect(stateCalls()).toHaveLength(2);
        await tick(14999); expect(stateCalls()).toHaveLength(2);
        await tick(1); expect(healthy()).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Request timed out; collector status unknown');
        await tick(60000); expect(stateCalls()).toHaveLength(2);
    });

    it.each(['headers', 'text body'])('aborts on unmount and refuses late HTTP 401 %s authentication side effects', async phase => {
        const held = deferred(); const expired = vi.fn(); window.addEventListener('grid:auth-expired', expired);
        try {
            localStorage.setItem('grid_token', 'synthetic-token');
            localStorage.setItem('grid_role', 'synthetic-role');
            localStorage.setItem('grid_username', 'synthetic-user');
            mockState(() => phase === 'headers' ? held.promise : Promise.resolve({
                ok: false, status: 401, statusText: 'Unauthorized', text: () => held.promise,
            }));
            const mounted = render(<GammaWatch />); await flush();
            const signal = stateCalls()[0][1].signal;
            expect(signal).toBeDefined();
            mounted.unmount(); expect(signal.aborted).toBe(true);
            held.resolve(phase === 'headers' ? { ok: false, status: 401, statusText: 'Unauthorized', text: async () => 'PRIVATE_SENTINEL' } : 'PRIVATE_SENTINEL');
            await flush(); await tick(60000);
            expect(window.location.hash).toBe('#/options'); expect(expired).not.toHaveBeenCalled();
            expect(localStorage.getItem('grid_token')).toBe('synthetic-token');
            expect(localStorage.getItem('grid_role')).toBe('synthetic-role');
            expect(localStorage.getItem('grid_username')).toBe('synthetic-user');
            expect(stateCalls()).toHaveLength(1); expect(vi.getTimerCount()).toBe(0);
        } finally { window.removeEventListener('grid:auth-expired', expired); }
    });

    it('preserves real auth expiry on a completed HTTP 401 before cancellation', async () => {
        const expired = vi.fn(); window.addEventListener('grid:auth-expired', expired);
        try {
            for (const key of ['grid_token', 'grid_role', 'grid_username']) localStorage.setItem(key, 'synthetic-value');
            mockState(() => Promise.resolve({ ok: false, status: 401, statusText: 'Unauthorized', text: async () => '' }));
            render(<GammaWatch />); await flush();
            expect(stateCalls()[0][1].headers.Authorization).toBe('Bearer synthetic-value');
            expect(window.location.hash).toBe('#/login'); expect(expired).toHaveBeenCalledTimes(1);
            for (const key of ['grid_token', 'grid_role', 'grid_username']) expect(localStorage.getItem(key)).toBeNull();
            expect(screen.getByRole('status').textContent).toContain('collector status unknown');
        } finally { window.removeEventListener('grid:auth-expired', expired); }
    });

    it('isolates StrictMode cleanup and ignores the first generation late body', async () => {
        const held = deferred(); let count = 0;
        mockState(() => ++count === 1 ? Promise.resolve({ ok: true, status: 200, json: () => held.promise }) : Promise.resolve(ok(frame())));
        render(<StrictMode><GammaWatch /></StrictMode>); await flush();
        expect(stateCalls()).toHaveLength(2);
        expect(stateCalls()[0][1].signal).toBeDefined();
        expect(stateCalls()[0][1].signal.aborted).toBe(true);
        expect(healthy()).toBeTruthy();
        held.resolve({ status: 'unavailable', data: null }); await flush();
        expect(healthy()).toBeTruthy();
        await tick(4999); expect(stateCalls()).toHaveLength(2);
        await tick(1); expect(stateCalls()).toHaveLength(3);
    });

    it('keeps the existing unrelated GET cache and lack of a new Gamma deadline unchanged', async () => {
        const held = deferred(); vi.stubGlobal('fetch', vi.fn(() => held.promise));
        let completed = false;
        const request = api.get('/api/v1/synthetic-context').then(value => { completed = true; return value; });
        await tick(25000); expect(completed).toBe(false);
        held.resolve(ok({ synthetic: true })); await flush();
        expect(await request).toEqual({ synthetic: true });
        expect(await api.get('/api/v1/synthetic-context')).toEqual({ synthetic: true });
        expect(globalThis.fetch).toHaveBeenCalledTimes(1);
    });
});
