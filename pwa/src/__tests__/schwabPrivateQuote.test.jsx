import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SchwabPrivateQuote, { AGE_TICK_MS, count, describeError, describeRow, elapsedSince, isTrustedElapsed, monotonicNow } from '../components/SchwabPrivateQuote.jsx';
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
    it('labels LIVE only when the server says live and fresh, the row has not aged out and elapsed time is trusted', () => {
        expect(describeRow(row(), 0).label).toBe('LIVE');
        expect(describeRow(row({ live: false }), 0).label).toBe('FRESH');
        expect(describeRow(row({ quote_status: 'stale', live: true }), 0).label).toBe('STALE');
        expect(describeRow(row({ quote_status: 'delayed', live: false }), 0).label).toBe('DELAYED');
        expect(describeRow(row()).label).toBe('STALE');               // no elapsed argument = untrusted (r4)
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
    });

    it('treats untrusted elapsed time as a clock failure: STALE, never LIVE (r4)', () => {
        for (const bad of [null, undefined, NaN, -5, -0.001, Infinity, -Infinity, '5']) {
            const d = describeRow(row(), bad);
            expect(d.label).toBe('STALE');
            expect(d.live).toBe(false);
            expect(d.clockUntrusted).toBe(true);
            expect(d.reason).toMatch(/local clock untrusted/);
            expect(d.ageSeconds).toBeNull();
            expect(d.last).toBe('731.37');                            // values kept, clearly labelled
        }
        expect(describeRow(row(), 0).label).toBe('LIVE');            // zero is a trusted interval
        expect(describeRow(row({ quote_status: 'delayed', live: false }), null).label).toBe('DELAYED');
        expect(describeRow(row({ quote_status: 'delayed', live: false }), null).clockUntrusted).toBe(false);
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

describe('elapsedSince / monotonicNow / isTrustedElapsed (r4)', () => {
    it('measures from the given monotonic start and returns null for anything untrusted', () => {
        mono = 5000;
        expect(elapsedSince(2000)).toBe(3);
        expect(elapsedSince(5000)).toBe(0);
        expect(elapsedSince(9000)).toBeNull();     // rollback: reading earlier than the start
        expect(elapsedSince(null)).toBeNull();
        expect(elapsedSince(NaN)).toBeNull();
        expect(elapsedSince('2000')).toBeNull();
        expect(elapsedSince(Infinity)).toBeNull();
        mono = NaN;
        expect(elapsedSince(2000)).toBeNull();     // clock returns NaN
        mono = Infinity;
        expect(elapsedSince(2000)).toBeNull();     // clock returns a non-finite value
    });

    it('has no Date.now fallback: a missing or throwing performance.now is untrusted', () => {
        const original = globalThis.performance.now;
        globalThis.performance.now = undefined;
        expect(Number.isNaN(monotonicNow())).toBe(true);
        expect(elapsedSince(0)).toBeNull();
        globalThis.performance.now = () => { throw new Error('no clock'); };
        expect(Number.isNaN(monotonicNow())).toBe(true);
        globalThis.performance.now = original;
    });

    it('isTrustedElapsed accepts only finite non-negative numbers', () => {
        expect(isTrustedElapsed(0)).toBe(true);
        expect(isTrustedElapsed(12.5)).toBe(true);
        for (const bad of [null, undefined, NaN, -1, Infinity, '1', {}]) expect(isTrustedElapsed(bad)).toBe(false);
    });
});

// Clock-failure controls: the mocked api manipulates the fake monotonic clock while the request is "in flight".
function inFlightSet(value, result) {
    return () => { mono = value; return Promise.resolve(result); };
}

describe('<SchwabPrivateQuote /> clock failures (r4)', () => {
    const serverRow = () => row({ freshness: { ...freshness, age_seconds: 5, stale_after_seconds: 20 } });

    it('rollback during the request (before first render) renders STALE with the clock reason, never LIVE', async () => {
        mono = 50_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(10_000, view({ SPY: serverRow() })));   // clock went backwards in transit
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
        expect(screen.getByTestId('schwab-private-quote').querySelector('tr[data-clock-untrusted="true"]')).toBeTruthy();
        mono = 10_500; tick();                                                                           // clock recovers: still not promoted
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
    });

    it('NaN clock at request start renders STALE even when the clock is fine on arrival', async () => {
        mono = NaN;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(2_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
        mono = 3_000; tick();
        expect(screen.queryByText('LIVE')).toBeNull();
    });

    it('NaN clock on arrival renders STALE', async () => {
        mono = 1_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(NaN, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
    });

    it('missing performance.now (no Date.now fallback) renders STALE', async () => {
        const original = globalThis.performance.now;
        globalThis.performance.now = undefined;
        try {
            api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: serverRow() }));
            render(<SchwabPrivateQuote />);
            await readOnce();
            expect(screen.queryByText('LIVE')).toBeNull();
            expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
        } finally {
            globalThis.performance.now = original;
        }
    });

    it('rollback after first render on the timer fails closed and stays closed', async () => {
        mono = 1_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(1_200, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono = 500; tick();                                                                              // earlier than the request start
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
        mono = 2_000; tick();                                                                            // recovery does not promote
        expect(screen.queryByText('LIVE')).toBeNull();
    });

    it('fast positive control: trusted clock, 0.2 s transit, LIVE then aged out on the timer', async () => {
        mono = 1_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(1_200, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        expect(screen.queryByText(/local clock untrusted/)).toBeNull();
        mono = 16_200; tick();                                                                           // 5 + 15.2 = 20.2 s
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
    });

    it('slow positive control: trusted clock, 30 s transit, STALE by age (not by clock failure)', async () => {
        mono = 1_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(31_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        expect(screen.queryByText(/local clock untrusted/)).toBeNull();
    });

    it('a successful re-read with a trusted clock recovers from an untrusted state', async () => {
        mono = NaN;
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlightSet(1_000, view({ SPY: serverRow() })))
            .mockImplementationOnce(inFlightSet(1_100, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText(/local clock untrusted/)).toBeTruthy();
        await readOnce();                                                                                // start 1000, arrival 1100
        expect(screen.getByText('LIVE')).toBeTruthy();
    });
});

// Transit: the mocked api advances the fake monotonic clock while the request is "in flight".
function inFlight(ms, result) {
    return () => { mono += ms; return Promise.resolve(result); };
}

describe('<SchwabPrivateQuote /> request/response transit (r3)', () => {
    it("the reviewer's case: 5 s server age + 30 s transit against a 20 s gate renders STALE, never LIVE", async () => {
        const serverRow = row({ freshness: { ...freshness, age_seconds: 5, stale_after_seconds: 20 } });
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(30_000, view({ SPY: serverRow })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        expect(screen.getByTestId('schwab-private-quote').querySelector('tr[data-aged-out="true"]')).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(1);
    });

    it('transit just inside the gate is LIVE and then ages out on the timer', async () => {
        const serverRow = row({ freshness: { ...freshness, age_seconds: 5, stale_after_seconds: 20 } });
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(14_000, view({ SPY: serverRow })));  // 5 + 14 = 19 s
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono += 1_500; tick();                                    // 20.5 s > 20 s
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
    });

    it('transit exactly at the gate is still LIVE (bound is inclusive) and 1 ms more is not', async () => {
        const serverRow = row({ freshness: { ...freshness, age_seconds: 5, stale_after_seconds: 20 } });
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(15_000, view({ SPY: serverRow })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        cleanup();
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(15_001, view({ SPY: serverRow })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.queryByText('LIVE')).toBeNull();
    });

    it('a normal fast response is LIVE with the transit still counted', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(200, view({ SPY: row() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono += 18_700; tick();                                   // 1 + 0.2 + 18.7 = 19.9 s
        expect(screen.getByText('LIVE')).toBeTruthy();
        mono += 200; tick();                                      // 20.1 s
        expect(screen.queryByText('LIVE')).toBeNull();
    });

    it('a slow failed request leaves no rows and no timer', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(30_000, { error: true, status: 503 }));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText(/disabled or not configured/)).toBeTruthy();
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('a slow re-read after a live row never shows a false LIVE and cleans up on unmount', async () => {
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlight(100, view({ SPY: row() })))
            .mockImplementationOnce(inFlight(40_000, view({ SPY: row() })));
        const { unmount } = render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText('LIVE')).toBeTruthy();
        await readOnce();                                         // second read: 1 + 40 s old on arrival
        expect(screen.queryByText('LIVE')).toBeNull();
        expect(screen.getByText('STALE')).toBeTruthy();
        mono -= 40_000; tick();                                   // backwards clock: still no promotion
        expect(screen.queryByText('LIVE')).toBeNull();
        unmount();
        expect(vi.getTimerCount()).toBe(0);
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
