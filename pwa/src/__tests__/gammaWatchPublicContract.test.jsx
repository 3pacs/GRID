// @vitest-environment jsdom
import React from 'react';
import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import GammaWatch from '../components/GammaWatch.jsx';
import { adaptGammaWatch } from '../components/gammaWatchPublicAdapter.js';

vi.mock('../api.js', () => ({ api: { get: vi.fn() } }));

const statePath = '/api/v1/gamma-watch/state';
const envelope = data => ({ status: 'available', data });
const iso = seconds => new Date(seconds * 1000).toISOString();
const legacy = clock => ({
    served_at: clock,
    quote: { source: 'Yahoo chart · public, latency not guaranteed', price: 600, as_of: clock, received_at: clock },
});

// Synthetic public-only contracts. No capture/private payload or provider is used.
function publicFrame(now = 1000) {
    return {
        schema: 'gex-screen/2', symbol: 'SPY', served_at: now, evaluation_clock: 'ok',
        public_price: {
            status: 'unverified-public-latency', source: 'Yahoo chart · public, latency not guaranteed',
            source_time: now - 5, receipt_time: now - 1, values_available: true,
            current_interpretation: false, max_context_age_seconds: 120, price: 600,
        },
        delayed_gex: {
            status: 'unverified-delayed', source: 'ZeroGEX · delayed ~15 minutes',
            source_time: now - 900, receipt_time: now - 1, values_available: true,
            current_interpretation: false, max_context_age_seconds: 1800, spot: 599,
            levels: { call_wall: 605, put_wall: 590, gamma_flip: 0, max_pain: null },
        },
        research_model: {
            status: 'frozen-research', current_interpretation: false,
            chain_as_of: now - 900, computed_at: now - 1, grid: [700, 750, 800],
            variants: [{ name: 'paired OTM IV', iv_shock: 0, values: [-1, 0, 1] }],
        },
        sectors: {
            status: 'delayed-unqualified', computed_at: now - 1,
            rows: [{ symbol: 'SPY', status: 'delayed-unqualified', received_at: now - 1, price: 600, today_volume: 0, session_date: '2026-10-20', nearest_expiry: '2026-10-23' }],
        },
        structural: {
            official: [{ name: 'repo', status: 'published-history-unqualified', received_at: now - 1, rows: [{ operationDate: '2026-10-20', totalAmtAccepted: 1000 }] }],
            rebalance: { status: 'modeled-history-unqualified', computed_at: now - 1, rows: [{ period: 'month', baseline: '2026-09-30', as_of_date: '2026-10-20', scenarios: [{ equity_weight: .6, equity_purchase_per_initial_billion: 10000 }] }] },
        },
    };
}

function deferred() {
    let resolve, reject;
    const promise = new Promise((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
async function settle(request, value) {
    await act(async () => { request.resolve(value); await request.promise; });
}
async function advance(milliseconds) {
    await act(async () => { await vi.advanceTimersByTimeAsync(milliseconds); });
}
function mountPending() {
    const request = deferred();
    api.get.mockReturnValueOnce(request.promise);
    return { ...render(<GammaWatch />), request };
}
const priceLine = () => screen.queryByText(/^SPY 600 ·/);

describe('Gamma Watch public schema and actual effect contract', () => {
    beforeEach(() => {
        vi.useFakeTimers();
        vi.setSystemTime(1000000);
        api.get.mockReset();
        vi.stubGlobal('fetch', vi.fn(() => Promise.reject(new Error('Unexpected network'))));
    });
    afterEach(() => {
        cleanup();
        expect(globalThis.fetch).not.toHaveBeenCalled();
        vi.clearAllTimers();
        vi.useRealTimers();
        vi.unstubAllGlobals();
    });

    it('renders every healthy public panel when a deferred response arrives 100ms between display ticks', async () => {
        const { request, container } = mountPending();
        expect(screen.getByRole('status').textContent).toContain('Connecting');
        await advance(100); // The 1s display interval has not fired.
        await settle(request, envelope(publicFrame(1000.1)));
        expect(priceLine()).not.toBeNull();
        expect(screen.getByRole('status').textContent).toContain('GRID connection recent');
        expect(container.textContent).toContain('put wall 590');
        expect(container.textContent).toContain('Research grid and sensitivity variants · 3 points');
        expect(container.textContent).toContain('Official published history · 1 sources');
        expect(container.textContent).toContain('60% equity: $0.01m');
        expect(container.textContent).toContain('Sector context · delayed-unqualified');
        expect(api.get.mock.calls).toEqual([[statePath, { signal: expect.any(AbortSignal) }]]);
    });

    it('serializes one state-only request and starts the existing 5s delay after completion', async () => {
        const { request } = mountPending();
        await advance(5000);
        expect(api.get).toHaveBeenCalledTimes(1);
        await settle(request, envelope(publicFrame(1005)));
        const second = deferred();
        api.get.mockReturnValueOnce(second.promise);
        await advance(4999);
        expect(api.get).toHaveBeenCalledTimes(1);
        await advance(1);
        expect(api.get).toHaveBeenCalledTimes(2);
        await advance(10000); // A pending request cannot overlap with another poll.
        expect(api.get).toHaveBeenCalledTimes(2);
        await settle(second, envelope(publicFrame(Date.now() / 1000)));
        expect(api.get.mock.calls).toEqual([[statePath, { signal: expect.any(AbortSignal) }], [statePath, { signal: expect.any(AbortSignal) }]]);
    });

    it('ignores a late response after unmount and leaves no refresh or display timer', async () => {
        const { request, unmount, container } = mountPending();
        unmount();
        expect(vi.getTimerCount()).toBe(0);
        await settle(request, envelope(publicFrame()));
        await advance(60000);
        expect(container.textContent).toBe('');
        expect(vi.getTimerCount()).toBe(0);
        expect(api.get.mock.calls).toEqual([[statePath, { signal: expect.any(AbortSignal) }]]);
    });

    it('withholds a genuinely future server clock at the actual local response time', async () => {
        const { request } = mountPending();
        await advance(100);
        await settle(request, envelope(publicFrame(1000.101)));
        expect(priceLine()).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Display withheld');
    });

    it('withholds a wall-clock rollback during a pending response without clamping', async () => {
        const { request } = mountPending();
        vi.setSystemTime(999900);
        await settle(request, envelope(publicFrame(999.9)));
        expect(priceLine()).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Display withheld');
    });

    it('expires retained display values on a real interval callback when refresh is still pending', async () => {
        const { request } = mountPending();
        await settle(request, envelope(publicFrame()));
        const pending = deferred();
        api.get.mockReturnValueOnce(pending.promise);
        await advance(16000);
        expect(priceLine()).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Display withheld');
        expect(api.get).toHaveBeenCalledTimes(2);
    });

    it('rejects an impossible legacy calendar date instead of normalizing it into a fresh receipt', async () => {
        vi.setSystemTime(Date.parse('2026-03-02T00:00:00Z'));
        const { request } = mountPending();
        await settle(request, envelope(legacy('2026-02-30T00:00:00.000Z')));
        expect(priceLine()).toBeNull();
        expect(screen.getByRole('status').textContent).toContain('Display withheld');
    });

    it('reports expiry under a stricter source cap as stale while retaining healthy independent panels', async () => {
        const { request, container } = mountPending();
        const frame = publicFrame();
        frame.public_price.max_context_age_seconds = 1;
        await settle(request, envelope(frame));
        expect(screen.getByText(/^SPY Unavailable · stale ·/)).not.toBeNull();
        expect(container.textContent).toContain('put wall 590');
        expect(container.textContent).toContain('Research grid and sensitivity variants · 3 points');
    });

    it('keeps private payloads and error diagnostics out of the DOM and does not call private routes', async () => {
        const { request, container } = mountPending();
        const frame = publicFrame();
        frame.brokerage_quote = { price: 998877, secret: 'PRIVATE_SENTINEL' };
        frame.journal = 'PRIVATE_SENTINEL';
        frame.current_gex = { value: 998878 };
        frame.provider_error = 'PRIVATE_SENTINEL';
        await settle(request, envelope(frame));
        expect(priceLine()).not.toBeNull();
        expect(container.textContent).not.toMatch(/PRIVATE_SENTINEL|998877|998878/);
        expect(screen.getByRole('heading', { name: 'Private capabilities' })).not.toBeNull();
        expect(api.get.mock.calls).toEqual([[statePath, { signal: expect.any(AbortSignal) }]]);
    });

    it('clears cached display values on an actual mocked API rejection', async () => {
        const { request } = mountPending();
        await settle(request, envelope(publicFrame()));
        const next = deferred();
        api.get.mockReturnValueOnce(next.promise);
        await advance(5000);
        await act(async () => { next.reject(new Error('PRIVATE_SENTINEL')); await next.promise.catch(() => {}); });
        expect(screen.getByRole('status').textContent).toBe('Request failed; collector status unknown. No cached substitute.');
        expect(priceLine()).toBeNull();
        expect(document.body.textContent).not.toContain('PRIVATE_SENTINEL');
    });

    it('validates legacy leap days, explicit offsets and supported fractions without inventing clocks', () => {
        for (const clock of ['2024-02-29T00:00:00Z', '2000-02-29T23:59:59.123Z', '2026-03-01T00:00:00.123456+05:30', '2026-03-01T23:59:59.123456-05:30']) {
            const now = Date.parse(clock), view = adaptGammaWatch(envelope(legacy(clock)), now);
            expect(view.valid).toBe(true);
            expect(view.price.price).toBe(600);
            expect(view.price.sourceTime).toBe(now / 1000);
        }
        for (const clock of ['2025-02-29T00:00:00Z', '1900-02-29T00:00:00Z', '2026-04-31T00:00:00Z', '2026-01-01T24:00:00Z', '2026-01-01T00:00:00+24:00', '2026-01-01T00:00:00+00:60']) {
            const parsed = Date.parse(clock), now = Number.isFinite(parsed) ? parsed : 1000000;
            expect(adaptGammaWatch(envelope(legacy(clock)), now).valid).toBe(false);
        }
    });

    it('fails closed for unknown schemas and missing, invalid or reversed source clocks', () => {
        for (const value of [null, true, NaN, Infinity, '995', 1001, undefined]) {
            const frame = publicFrame();
            frame.public_price.source_time = value;
            expect(adaptGammaWatch(envelope(frame), 1000000).price.price).toBeNull();
        }
        const reversed = publicFrame();
        reversed.public_price.receipt_time = 994;
        expect(adaptGammaWatch(envelope(reversed), 1000000).price.status).toBe('clock-invalid');
        const future = publicFrame(1000.001);
        expect(adaptGammaWatch(envelope(future), 1000000).valid).toBe(false);
        const unknown = { ...publicFrame(), schema: 'gex-screen/3', quote: legacy(iso(1000)).quote };
        expect(adaptGammaWatch(envelope(unknown), 1000000).valid).toBe(false);
    });

    it('preserves zero versus missing levels and enforces effective cap boundaries and failure priority', () => {
        const frame = publicFrame();
        frame.public_price.max_context_age_seconds = 5;
        const admitted = adaptGammaWatch(envelope(frame), 1000000);
        expect(admitted.price.price).toBe(600);
        expect(admitted.delayed.levels.gamma_flip).toBe(0);
        expect(admitted.delayed.levels.max_pain).toBeNull();
        expect(adaptGammaWatch(envelope(frame), 1000000.1).price.status).toBe('stale');
        frame.public_price.status = 'failed';
        expect(adaptGammaWatch(envelope(frame), 1000000).price.status).toBe('failed');
        expect(adaptGammaWatch(envelope(frame), 1000000).price.price).toBeNull();
    });
});
