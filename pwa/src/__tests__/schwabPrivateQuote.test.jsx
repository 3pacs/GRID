import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SchwabPrivateQuote, { AGE_TICK_MS, count, describeError, describeRow } from '../components/SchwabPrivateQuote.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: { getSchwabPrivateQuotes: vi.fn() },
}));

let mono = 0; // fake monotonic clock in ms
beforeEach(() => {
    mono = 1000;
    vi.useFakeTimers();
    vi.spyOn(globalThis.performance, 'now').mockImplementation(() => mono);
});
afterEach(() => { cleanup(); vi.clearAllTimers(); vi.useRealTimers(); vi.restoreAllMocks(); vi.clearAllMocks(); });

const freshness = { as_of: '2026-10-31T14:29:59+00:00', vintage: '2026-10-31T14:30:00+00:00', evaluated_at: '2026-10-31T14:30:00+00:00', age_seconds: 1, stale: false, stale_after_seconds: 20 };

function row(over = {}) {
    return {
        schema: 'schwab-private-quote-view-v1', symbol: 'SPY', provider_symbol: 'SPY', quote_status: 'fresh', live: true,
        status: 'ok', last: 731.37, bid: 731.35, ask: 731.39, bid_size: 100, ask_size: 200,
        bid_size_exact: '100', ask_size_exact: '200', freshness, ...over,
    };
}

function view(rows) {
    return { snapshot: {}, view: { evaluated_at: freshness.evaluated_at, source: 'schwab-marketdata-v1', redistribution: 'prohibited', rows } };
}

async function readOnce() {
    await act(async () => { fireEvent.click(screen.getByText('Read private quote')); });
}

function tick(ms = AGE_TICK_MS) {
    act(() => { vi.advanceTimersByTime(ms); });
}

describe('describeRow', () => {
    it('labels LIVE only when the server says live and fresh and the row has not aged out', () => {
        expect(describeRow(row()).label).toBe('LIVE');
        expect(describeRow(row({ live: false })).label).toBe('FRESH');
        expect(describeRow(row({ quote_status: 'stale', live: true })).label).toBe('STALE');
        expect(describeRow(row({ quote_status: 'delayed', live: false })).label).toBe('DELAYED');
    });

    it('ages a fresh row out with elapsed time and never promotes', () => {
        expect(describeRow(row(), 18).label).toBe('LIVE');          // 1 + 18 = 19 <= 20
        const aged = describeRow(row(), 20);                          // 1 + 20 = 21 > 20
        expect(aged.label).toBe('STALE');
        expect(aged.live).toBe(false);
        expect(aged.agedOut).toBe(true);
        expect(aged.reason).toMatch(/aged out/);
        expect(aged.last).toBe('731.37');                             // values kept, label changed
        expect(describeRow(row({ live: false }), 0).live).toBe(false);
        expect(describeRow(row({ quote_status: 'stale', live: false }), -50).label).toBe('STALE');
        expect(describeRow(row(), -5).label).toBe('LIVE');            // negative elapsed is treated as 0
    });

    it('cannot bound a fresh row without age or gate metadata, so it is not live', () => {
        expect(describeRow(row({ freshness: { ...freshness, age_seconds: null } })).live).toBe(false);
        expect(describeRow(row({ freshness: { ...freshness, stale_after_seconds: undefined } })).label).toBe('STALE');
    });

    it('never shows numbers for unmeasured rows', () => {
        const d = describeRow(row({ quote_status: 'missing', live: false, status: 'unavailable', last: null, bid: null, ask: null, reason: 'no row', freshness: { as_of: null, vintage: freshness.vintage, age_seconds: null } }));
        expect(d.label).toBe('MISSING');
        expect([d.last, d.bid, d.ask, d.bidSize]).toEqual(['—', '—', '—', '—']);
        expect(d.asOf).toBeNull();
        expect(d.reason).toBe('no row');
    });

    it('carries identity, as-of, vintage and evaluation time through', () => {
        const d = describeRow(row({ provider_symbol: 'SPY.X' }));
        expect(d.symbol).toBe('SPY');
        expect(d.providerSymbol).toBe('SPY.X');
        expect(d.asOf).toBe(freshness.as_of);
        expect(d.vintage).toBe(freshness.vintage);
        expect(d.evaluatedAt).toBe(freshness.evaluated_at);
    });

    it('prefers the exact decimal string for counts and never shows a lossy number', () => {
        expect(count(9007199254740992, '9007199254740993')).toBe('9007199254740993');
        expect(count(9007199254740993, undefined)).toBe('n/a (>2^53)');
        expect(count(100, undefined)).toBe('100');
        expect(count(1.5, undefined)).toBe('—');
        expect(count(100, '12abc')).toBe('100');
        expect(describeRow(row({ bid_size: 9007199254740992, bid_size_exact: '9007199254740993' })).bidSize).toBe('9007199254740993');
    });

    it('treats garbage as unavailable', () => {
        expect(describeRow(null).label).toBe('UNAVAILABLE');
        expect(describeRow({ quote_status: 'live', live: true }).label).toBe('UNAVAILABLE');
        expect(describeRow(row({ last: 'NaN' })).last).toBe('—');
    });
});

describe('describeError', () => {
    it('maps auth, flag and validation failures', () => {
        expect(describeError({ error: true, status: 401 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 403 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 503 })).toMatch(/disabled or not configured/);
        expect(describeError({ error: true, status: 422 })).toMatch(/rejected/);
        expect(describeError({ error: true, status: 0 })).toMatch(/Network/);
        expect(describeError(undefined)).toMatch(/Network/);
    });
});

describe('<SchwabPrivateQuote />', () => {
    it('renders live and stale rows with their own labels', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: row(), QQQ: row({ symbol: 'QQQ', provider_symbol: 'QQQ', quote_status: 'stale', live: false, reason: 'aged out' }) }));
        render(<SchwabPrivateQuote symbols={['SPY', 'QQQ']} />);
        await readOnce();
        expect(screen.getAllByText('LIVE')).toHaveLength(1);
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getByText(/redistribution prohibited/)).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledWith(['SPY', 'QQQ']);
    });

    it('ages the LIVE badge out on the monotonic clock without touching the api', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: row() }));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono += 18_000; tick();                                   // 1 + 18 = 19 s: still live
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono += 2_500; tick();                                    // 21.5 s > 20 s: aged out
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        expect(screen.getByText('731.37')).toBeTruthy();          // values stay, clearly labelled
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(1);
    });

    it('never promotes back to LIVE even if the clock moves backwards', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: row() }));
        render(<SchwabPrivateQuote />);
        await readOnce();
        mono += 25_000; tick();
        expect(screen.getByText('STALE')).toBeTruthy();
        mono -= 25_000; tick();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
    });

    it('a fresh read resets the bound', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: row() }));
        render(<SchwabPrivateQuote />);
        await readOnce();
        mono += 25_000; tick();
        expect(screen.getByText('STALE')).toBeTruthy();
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(2);
    });

    it('clears its timer on unmount', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: row() }));
        const { unmount } = render(<SchwabPrivateQuote />);
        await readOnce();
        expect(vi.getTimerCount()).toBeGreaterThan(0);
        unmount();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('shows the auth message on 403, clears rows and stops the timer', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValueOnce(view({ SPY: row() })).mockResolvedValueOnce({ error: true, status: 403 });
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        await readOnce();
        expect(screen.getByText(/Admin sign-in required/)).toBeTruthy();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('shows the disabled message on 503', async () => {
        api.getSchwabPrivateQuotes.mockResolvedValue({ error: true, status: 503 });
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText(/disabled or not configured/)).toBeTruthy();
        expect(vi.getTimerCount()).toBe(0);
    });
});
