import React from 'react';
import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import GammaWatch from '../components/GammaWatch.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { get: vi.fn() } }));
afterEach(() => { cleanup(); vi.useRealTimers(); vi.resetAllMocks(); });

function publicState(maxAge = 120) {
    const now = Date.now() / 1000;
    const stamp = new Date().toISOString();
    return { status: 'available', data: {
        schema: 'gex-screen/2', symbol: 'SPY', served_at: now, evaluation_clock: 'ok',
        public_price: { source: 'Yahoo chart · public, latency not guaranteed', status: 'unverified-public-latency', price: 600,
            source_time: now - 5, receipt_time: now - 1, values_available: true, current_interpretation: false, max_context_age_seconds: maxAge },
        delayed_gex: { source: 'ZeroGEX · delayed ~15 minutes', status: 'unverified-delayed',
            source_time: now - 900, receipt_time: now - 1, values_available: true, current_interpretation: false, max_context_age_seconds: 1800,
            levels: { gamma_flip: 0 } },
        // A public frame must never expose an injected private breadth receipt.
        structural_live: { computed_at: stamp, rows: [{ symbol: '$TICK_PRIVATE', value: 987654, callback_at: stamp, direction_usable: true, status: 'fresh_receipt' }] },
    } };
}

it('ages public source and bridge receipts out even while a subsequent request hangs', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1000000);
    const received = new Date().toISOString();
    api.get.mockResolvedValueOnce(publicState(10)).mockImplementation(() => new Promise(() => {}));
    await act(async () => { render(<GammaWatch />); });
    expect(screen.getByText(/^SPY 600 · unverified-public-latency/)).toBeTruthy();
    expect(screen.getByText(/^Delayed levels:.*flip 0/)).toBeTruthy();
    expect(screen.queryByText('$TICK_PRIVATE')).toBeNull();
    expect(document.body.textContent).not.toContain('987654');

    // Source expiry is independent of the still-recent transport receipt.
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(api.get).toHaveBeenCalledTimes(2);
    expect(screen.getByText(/^SPY Unavailable · stale ·/)).toBeTruthy();
    expect(screen.getByText(/^Delayed levels:.*flip 0/)).toBeTruthy();
    expect(screen.getByRole('status').textContent).toContain('GRID connection recent');
    expect(screen.getByRole('status').textContent).toContain(`browser receipt ${received}`);

    // Starting the pending request must not renew either bridge clock.
    await act(async () => { await vi.advanceTimersByTimeAsync(10000); });
    expect(screen.getByRole('status').textContent).toContain('Display withheld');
    expect(screen.queryByText(/^SPY 600 ·/)).toBeNull();
    expect(screen.queryByText(/^Delayed levels:.*flip 0/)).toBeNull();
    expect(screen.getByRole('status').textContent).toContain(`browser receipt ${received}`);
    await act(async () => { await vi.advanceTimersByTimeAsync(6000); });
    expect(screen.getByRole('status').textContent).toContain('Request timed out; collector status unknown');
    expect(screen.queryByText(/^SPY 600 ·/)).toBeNull();
    expect(screen.queryByText(/^Delayed levels:.*flip 0/)).toBeNull();
    expect(screen.queryByText('$TICK_PRIVATE')).toBeNull();
    expect(document.body.textContent).not.toContain('987654');
    expect(api.get.mock.calls).toEqual([['/api/v1/gamma-watch/state', { signal: expect.any(AbortSignal) }], ['/api/v1/gamma-watch/state', { signal: expect.any(AbortSignal) }]]);
});

it('clears old public data when the bridge fails', async () => {
    vi.useFakeTimers();
    vi.setSystemTime(1000000);
    api.get.mockResolvedValueOnce(publicState()).mockRejectedValue(new Error('offline'));
    await act(async () => { render(<GammaWatch />); });
    expect(screen.getByRole('heading', { name: 'Gamma Watch · experimental public context' })).toBeTruthy();
    expect(screen.getByText(/^SPY 600 ·/)).toBeTruthy();
    expect(screen.getByText(/^Delayed levels:.*flip 0/)).toBeTruthy();
    expect(screen.queryByText('$TICK_PRIVATE')).toBeNull();
    expect(document.body.textContent).not.toContain('987654');
    await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
    expect(screen.queryByRole('heading', { name: 'Gamma Watch · experimental public context' })).toBeNull();
    expect(screen.queryByText(/^SPY 600 ·/)).toBeNull();
    expect(screen.queryByText(/^Delayed levels:.*flip 0/)).toBeNull();
    expect(screen.getByText('Request failed; collector status unknown. No cached substitute.')).toBeTruthy();
    expect(screen.queryByText('$TICK_PRIVATE')).toBeNull();
    expect(document.body.textContent).not.toContain('987654');
    expect(api.get.mock.calls).toEqual([['/api/v1/gamma-watch/state', { signal: expect.any(AbortSignal) }], ['/api/v1/gamma-watch/state', { signal: expect.any(AbortSignal) }]]);
});
