import React from 'react';
import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, expect, it, vi } from 'vitest';
import GammaWatch from '../components/GammaWatch.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { get: vi.fn() } }));
afterEach(() => { cleanup(); vi.useRealTimers(); vi.resetAllMocks(); });

it('ages live receipts out even while a subsequent request hangs', async () => {
    vi.useFakeTimers();
    const stamp = new Date().toISOString();
    api.get.mockResolvedValueOnce({ status: 'available', data: { served_at: stamp,
        structural_live: { computed_at: stamp, rows: [{ symbol: '$TICK', value: 0, callback_at: stamp, direction_usable: true, status: 'fresh_receipt' }] },
    } }).mockImplementation(() => new Promise(() => {}));
    await act(async () => { render(<GammaWatch />); });
    expect(screen.getByText('Fresh receipt; exchange time unknown')).toBeTruthy();
    expect(screen.getByText('0')).toBeTruthy();
    await act(async () => { await vi.advanceTimersByTimeAsync(22000); });
    expect(screen.queryByText('Fresh receipt; exchange time unknown')).toBeNull();
    expect(screen.getByText('stale')).toBeTruthy();
});

it('clears old data when the bridge fails', async () => {
    vi.useFakeTimers();
    api.get.mockResolvedValueOnce({ status: 'available', data: { served_at: new Date().toISOString() } }).mockRejectedValue(new Error('offline'));
    await act(async () => { render(<GammaWatch />); });
    expect(screen.getByText('Gamma Watch · shared collector')).toBeTruthy();
    await act(async () => { await vi.advanceTimersByTimeAsync(5100); });
    expect(screen.queryByText('Gamma Watch · shared collector')).toBeNull();
    expect(screen.getByText('Collector unavailable; no cached substitute.')).toBeTruthy();
});
