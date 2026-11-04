/**
 * SchwabPrivateQuote — deterministic controls, mapped to the controller's timing
 * acceptance matrix (CLAUDE-CONTROLLER-01a18502-DEPENDENCY-SCHWAB-TIMING-MATRIX-20261031,
 * sha256 43cb5ede…). Rows: M1 valid fast delivery · M2 invalid start · M3 invalid later
 * reading · M4 full backwards · M5 partial backwards · M6 equal/forward · M7 sticky ·
 * M8 trusted reread · M9 transit boundary · M10 request race/isolation · M11 cleanup.
 * Pending-reread addendum (CLAUDE-CONTROLLER-01a18502-DEPENDENCY-SCHWAB-REREAD-MATRIX-20261031,
 * sha256 3068b4ca…): R1 pending past expiry · R2 eventual success · R3 failure/rejection ·
 * R4 cancellation (not implemented) · R5 timeout (not implemented) · R6 unmount and late
 * completion · R7 superseded request / no-overlap · R8 clock failure while pending.
 * Fake clock (mocked performance.now), fake timers and fake promises only; no real time, provider or DB.
 */
import React from 'react';
import fs from 'node:fs';
import path from 'node:path';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import SchwabPrivateQuote, {
    AGE_TICK_MS, CLOCK_UNTRUSTED_REASON, count, describeError, describeRow, elapsedSince,
    isTrustedElapsed, monotonicNow, observeTiming, startTiming,
} from '../components/SchwabPrivateQuote.jsx';
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
const serverRow = () => row({ freshness: { ...freshness, age_seconds: 5, stale_after_seconds: 20 } }); // age 5 s, gate 20 s

function view(rows) {
    return { snapshot: {}, view: { evaluated_at: freshness.evaluated_at, source: 'schwab-marketdata-v1', redistribution: 'prohibited', rows } };
}

async function readOnce() {
    await act(async () => { fireEvent.click(screen.getByText('Read private quote')); });
}
function tick(ms = AGE_TICK_MS) {
    act(() => { vi.advanceTimersByTime(ms); });
}
// The api mock sets or advances the fake clock while the request is "in flight".
const inFlightSet = (value, result) => () => { mono = value; return Promise.resolve(result); };
const inFlight = (ms, result) => () => { mono += ms; return Promise.resolve(result); };
const live = () => screen.queryByText('LIVE');
const untrustedShown = () => screen.queryByText(new RegExp(CLOCK_UNTRUSTED_REASON));
const card = () => screen.getByTestId('schwab-private-quote');

// ── pure helpers ─────────────────────────────────────────────────────────────

describe('startTiming / observeTiming (M2, M3, M4, M5, M6, M7)', () => {
    it('M1/M6: valid start, equal reading keeps elapsed, forward reading advances it', () => {
        let t = startTiming(0);
        expect(t).toMatchObject({ start: 0, last: 0, elapsed: 0, trusted: true });
        t = observeTiming(t, 5000);
        expect(t).toMatchObject({ last: 5000, elapsed: 5, trusted: true });
        t = observeTiming(t, 5000);                                 // equal: no false rollback, no decrease
        expect(t).toMatchObject({ last: 5000, elapsed: 5, trusted: true });
        t = observeTiming(t, 7500);
        expect(t).toMatchObject({ last: 7500, elapsed: 7.5, trusted: true });
    });

    it("M5: the reviewer's case — start 0, reading 5000, then 1000 (>= start, < last) is untrusted, not frozen at 5 s", () => {
        const t = observeTiming(observeTiming(startTiming(0), 5000), 1000);
        expect(t.trusted).toBe(false);
        expect(t.elapsed).toBeNull();
        expect(t.reason).toMatch(/decreased against previous reading/);
    });

    it('M4: a reading before start is untrusted, at the response or at a tick', () => {
        expect(observeTiming(startTiming(1000), 999)).toMatchObject({ trusted: false, elapsed: null, reason: 'reading before request start' });
        expect(observeTiming(observeTiming(startTiming(1000), 3000), 999).trusted).toBe(false);
    });

    it('M2: an invalid start is untrusted from the beginning', () => {
        for (const bad of [NaN, Infinity, -Infinity, null, undefined, '0', {}]) {
            expect(startTiming(bad)).toMatchObject({ trusted: false, elapsed: null, reason: 'invalid start reading' });
        }
    });

    it('M3: an invalid later reading is untrusted', () => {
        for (const bad of [NaN, Infinity, null, undefined, '5000']) {
            expect(observeTiming(startTiming(0), bad)).toMatchObject({ trusted: false, elapsed: null, reason: 'invalid clock reading' });
        }
    });

    it('M7: untrusted is sticky through later valid, forward readings', () => {
        let t = observeTiming(startTiming(0), NaN);
        for (const reading of [1, 1000, 1e6]) {
            t = observeTiming(t, reading);
            expect(t.trusted).toBe(false);
            expect(t.elapsed).toBeNull();
        }
        expect(observeTiming(null, 1000).trusted).toBe(false);
        expect(observeTiming('x', 1000).trusted).toBe(false);
    });
});

describe('elapsedSince / monotonicNow / isTrustedElapsed', () => {
    it('elapsedSince returns null for anything untrusted and the interval otherwise', () => {
        mono = 5000;
        expect(elapsedSince(2000)).toBe(3);
        expect(elapsedSince(5000)).toBe(0);
        expect(elapsedSince(9000)).toBeNull();
        expect(elapsedSince(null)).toBeNull();
        expect(elapsedSince(NaN)).toBeNull();
        mono = NaN;
        expect(elapsedSince(2000)).toBeNull();
    });

    it('M2: has no Date.now fallback — missing or throwing performance.now is NaN, and the source never mentions Date.now', () => {
        const original = globalThis.performance.now;
        globalThis.performance.now = undefined;
        expect(Number.isNaN(monotonicNow())).toBe(true);
        globalThis.performance.now = () => { throw new Error('no clock'); };
        expect(Number.isNaN(monotonicNow())).toBe(true);
        globalThis.performance.now = original;
        const src = fs.readFileSync(path.resolve(__dirname, '../components/SchwabPrivateQuote.jsx'), 'utf8');
        const code = src.replace(/\/\*\*[\s\S]*?\*\//g, '');
        expect(code).not.toMatch(/Date\.now/);
    });

    it('isTrustedElapsed accepts only finite non-negative numbers', () => {
        expect(isTrustedElapsed(0)).toBe(true);
        expect(isTrustedElapsed(12.5)).toBe(true);
        for (const bad of [null, undefined, NaN, -1, Infinity, '1', {}]) expect(isTrustedElapsed(bad)).toBe(false);
    });
});

// ── describeRow ──────────────────────────────────────────────────────────────

describe('describeRow (M9 and labels)', () => {
    it('labels LIVE only when live, fresh, trusted and within the gate; no-argument call is untrusted', () => {
        expect(describeRow(row(), 0).label).toBe('LIVE');
        expect(describeRow(row({ live: false }), 0).label).toBe('FRESH');
        expect(describeRow(row({ quote_status: 'stale', live: true }), 0).label).toBe('STALE');
        expect(describeRow(row({ quote_status: 'delayed', live: false }), 0).label).toBe('DELAYED');
        expect(describeRow(row()).label).toBe('STALE');
    });

    it('M9: transit boundaries — 30 s STALE, 0.2 s LIVE, 15 s LIVE (inclusive), 15.001 s STALE', () => {
        const r = serverRow();
        expect(describeRow(r, 30).label).toBe('STALE');
        expect(describeRow(r, 0.2).label).toBe('LIVE');
        expect(describeRow(r, 15).label).toBe('LIVE');
        expect(describeRow(r, 15.001).label).toBe('STALE');
        expect(describeRow(r, 15.001).reason).toBe('aged out since evaluation');
        expect(describeRow(r, 15.001).last).toBe('731.37'); // values kept, label changed
    });

    it('M9: non-finite age or gate is never LIVE', () => {
        expect(describeRow(row({ freshness: { ...freshness, age_seconds: null } }), 0).live).toBe(false);
        expect(describeRow(row({ freshness: { ...freshness, age_seconds: NaN } }), 0).live).toBe(false);
        expect(describeRow(row({ freshness: { ...freshness, stale_after_seconds: undefined } }), 0).label).toBe('STALE');
        expect(describeRow(row({ freshness: { ...freshness, stale_after_seconds: Infinity } }), 0).live).toBe(false);
    });

    it('untrusted elapsed is a clock failure: STALE, never LIVE, non-fresh rows unaffected', () => {
        for (const bad of [null, undefined, NaN, -5, Infinity, '5']) {
            const d = describeRow(row(), bad);
            expect(d.label).toBe('STALE');
            expect(d.live).toBe(false);
            expect(d.clockUntrusted).toBe(true);
            expect(d.reason).toBe(CLOCK_UNTRUSTED_REASON);
            expect(d.ageSeconds).toBeNull();
        }
        expect(describeRow(row({ quote_status: 'delayed', live: false }), null).label).toBe('DELAYED');
        expect(describeRow(row({ quote_status: 'delayed', live: false }), null).clockUntrusted).toBe(false);
    });

    it('never shows numbers for unmeasured rows and carries identity, as-of, vintage, evaluation time', () => {
        const d = describeRow(row({ quote_status: 'missing', live: false, status: 'unavailable', last: null, bid: null, ask: null, reason: 'no row', freshness: { as_of: null, vintage: freshness.vintage, age_seconds: null } }), 0);
        expect(d.label).toBe('MISSING');
        expect([d.last, d.bid, d.ask, d.bidSize]).toEqual(['—', '—', '—', '—']);
        const e = describeRow(row({ provider_symbol: 'SPY.X' }), 0);
        expect([e.symbol, e.providerSymbol, e.asOf, e.vintage, e.evaluatedAt]).toEqual(['SPY', 'SPY.X', freshness.as_of, freshness.vintage, freshness.evaluated_at]);
    });

    it('prefers exact decimal strings for counts and never shows a lossy number', () => {
        expect(count(9007199254740992, '9007199254740993')).toBe('9007199254740993');
        expect(count(9007199254740993, undefined)).toBe('n/a (>2^53)');
        expect(count(100, undefined)).toBe('100');
        expect(count(1.5, undefined)).toBe('—');
        expect(describeRow(null).label).toBe('UNAVAILABLE');
    });
});

describe('describeError', () => {
    it('maps auth, flag and validation failures', () => {
        expect(describeError({ error: true, status: 401 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 403 })).toMatch(/Admin sign-in/);
        expect(describeError({ error: true, status: 503 })).toMatch(/disabled or not configured/);
        expect(describeError({ error: true, status: 422 })).toMatch(/rejected/);
        expect(describeError(undefined)).toMatch(/Network/);
    });
});

// ── component ────────────────────────────────────────────────────────────────

describe('<SchwabPrivateQuote /> M1 valid fast delivery and M9 transit boundaries', () => {
    it('M1: fast delivery is LIVE, then ages out on the timer', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(200, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        expect(untrustedShown()).toBeNull();
        mono += 14_000; tick();                                   // 5 + 14.2 = 19.2 s
        expect(live()).toBeTruthy();
        mono += 1_000; tick();                                    // 20.2 s
        expect(live()).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(1);
    });

    it('M9: 30 s transit → STALE on first render; 15 s → LIVE; 15.001 s → STALE', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(30_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        expect(untrustedShown()).toBeNull();
        cleanup();
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(15_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        cleanup();
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(15_001, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeNull();
    });

    it('renders live and stale rows with their own labels', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(100, view({ SPY: row(), QQQ: row({ symbol: 'QQQ', provider_symbol: 'QQQ', quote_status: 'stale', live: false, reason: 'aged out' }) })));
        render(<SchwabPrivateQuote symbols={['SPY', 'QQQ']} />);
        await readOnce();
        expect(screen.getAllByText('LIVE')).toHaveLength(1);
        expect(screen.getByText('STALE')).toBeTruthy();
        expect(screen.getByText(/redistribution prohibited/)).toBeTruthy();
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledWith(['SPY', 'QQQ']);
    });
});

describe('<SchwabPrivateQuote /> M2–M7 clock trust', () => {
    it('M2: invalid start (NaN) → untrusted on first render even with a fine clock on arrival', async () => {
        mono = NaN;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(2_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('invalid start reading');
        mono = 3_000; tick();
        expect(live()).toBeNull();
    });

    it('M2: missing performance.now (no Date.now fallback) → untrusted', async () => {
        const original = globalThis.performance.now;
        globalThis.performance.now = undefined;
        try {
            api.getSchwabPrivateQuotes.mockResolvedValue(view({ SPY: serverRow() }));
            render(<SchwabPrivateQuote />);
            await readOnce();
            expect(live()).toBeNull();
            expect(untrustedShown()).toBeTruthy();
        } finally {
            globalThis.performance.now = original;
        }
    });

    it('M3: invalid reading at the response → untrusted; at a later tick → sticky untrusted', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(NaN, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(untrustedShown()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('invalid clock reading');
        cleanup();
        mono = 1000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(200, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        mono = NaN; tick();
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
        mono = 5_000; tick();                                      // recovery does not promote
        expect(live()).toBeNull();
    });

    it('M4: full backwards at the response (before first render) and at a tick; recovery never promotes', async () => {
        mono = 50_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(10_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeNull();
        expect(card().getAttribute('data-timing-reason')).toBe('reading before request start');
        mono = 60_000; tick();
        expect(live()).toBeNull();
        cleanup();
        mono = 1_000;
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(200, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        mono = 500; tick();                                        // earlier than start
        expect(live()).toBeNull();
        expect(card().getAttribute('data-timing-reason')).toBe('reading before request start');
        mono = 9_000; tick();
        expect(live()).toBeNull();
    });

    it("M5: partial backwards before age-out — the reviewer's case as a tick: start 0, response 5000, tick 1000 → untrusted, not frozen LIVE", async () => {
        mono = 0;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(5_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();                               // 5 + 5 = 10 s <= 20 s
        mono = 1_000; tick();                                      // >= start, < last
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('reading decreased against previous reading');
        expect(card().querySelector('tr[data-clock-untrusted="true"]')).toBeTruthy();
        mono = 6_000; tick();                                      // forward again: sticky
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
    });

    it('M5: partial backwards after age-out — the row stays non-LIVE and becomes untrusted', async () => {
        mono = 0;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(5_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        mono = 16_000; tick();                                     // 5 + 16 = 21 s: aged out
        expect(live()).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
        mono = 8_000; tick();                                      // partial rollback after age-out
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
        mono = 30_000; tick();
        expect(live()).toBeNull();
    });

    it('M5: partial backwards with the response as the second observation after a prior tick is impossible within one request; the response is checked against start (M4) — documented', () => {
        // Within one request the response is always the first observation after start; a decrease
        // there is a full backwards movement (M4). This test records that mapping explicitly.
        expect(observeTiming(startTiming(0), -1).reason).toBe('reading before request start');
    });

    it('M6: equal readings on ticks keep LIVE and never decrease elapsed; forward readings advance it', async () => {
        mono = 0;
        api.getSchwabPrivateQuotes.mockImplementation(inFlightSet(5_000, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        tick(); tick(); tick();                                    // clock frozen at 5000: equal readings
        expect(live()).toBeTruthy();
        expect(untrustedShown()).toBeNull();
        mono = 15_000; tick();                                     // 5 + 15 = 20 s: inclusive bound, still LIVE
        expect(live()).toBeTruthy();
        mono = 15_001; tick();
        expect(live()).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
    });

    it('M7: after ordinary age-out a later clock change cannot promote', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(100, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        mono += 25_000; tick();
        expect(live()).toBeNull();
        mono -= 25_000; tick();                                    // backwards: untrusted now, still not LIVE
        expect(live()).toBeNull();
        mono += 100; tick();
        expect(live()).toBeNull();
    });
});

describe('<SchwabPrivateQuote /> pending reread (addendum R1–R8)', () => {
    function deferred() {
        let resolve, reject;
        const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
        return { promise, resolve, reject };
    }
    async function liveRowThenPendingReread() {
        const pending = deferred();
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })))
            .mockImplementationOnce(() => pending.promise);
        const utils = render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        await readOnce();                                          // reread starts, promise pending
        return { pending, ...utils };
    }

    it('R1: pending past expiry — the old row is cleared at reread start; advancing time never shows LIVE', async () => {
        const { pending } = await liveRowThenPendingReread();
        expect(live()).toBeNull();
        expect(screen.queryByText('731.37')).toBeNull();
        expect(screen.getByText(/previous values cleared/)).toBeTruthy();
        expect(vi.getTimerCount()).toBe(0);                        // no old timer survives
        mono += 30_000; tick(30_000);                              // well past the old row's bound
        expect(live()).toBeNull();
        expect(screen.queryByText('731.37')).toBeNull();
        mono += 100;
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(live()).toBeNull();                                 // 30.1 s whole-request interval: STALE on arrival
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
    });

    it('R1: a fast pending reread inside the old bound — clearing is immediate and the new row is bounded on its own', async () => {
        const { pending } = await liveRowThenPendingReread();
        mono += 2_000; tick(2_000);
        expect(live()).toBeNull();
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(live()).toBeTruthy();                               // 5 + 2.0 s on its own timing
        expect(vi.getTimerCount()).toBe(1);
    });

    it('R2: eventual success — only the new response\'s own timing can show LIVE; no old timer promotes', async () => {
        const { pending } = await liveRowThenPendingReread();
        mono += 200;
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(live()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('');
        mono += 15_000; tick();                                    // 5 + 15.2 = 20.2 s on the new request
        expect(live()).toBeNull();
        expect(screen.getByText(/aged out since evaluation/)).toBeTruthy();
    });

    it('R3: failure (handled error) during a pending reread — prior row does not return, no timer remains', async () => {
        const { pending } = await liveRowThenPendingReread();
        mono += 30_000; tick(30_000);
        await act(async () => { pending.resolve({ error: true, status: 503 }); });
        expect(live()).toBeNull();
        expect(screen.queryByText('731.37')).toBeNull();
        expect(screen.getByText(/disabled or not configured/)).toBeTruthy();
        expect(vi.getTimerCount()).toBe(0);
        mono += 5_000; tick(5_000);
        expect(live()).toBeNull();
    });

    it('R3: rejection of the pending promise — prior row does not return, no timer remains', async () => {
        const { pending } = await liveRowThenPendingReread();
        await act(async () => { pending.reject(new Error('boom')); });
        expect(live()).toBeNull();
        expect(screen.queryByText('731.37')).toBeNull();
        expect(screen.getByText(/Network error/)).toBeTruthy();
        expect(vi.getTimerCount()).toBe(0);
        expect(screen.getByRole('button').disabled).toBe(false);
    });

    it('R4/R5: cancellation and timeout are not implemented (documented); a pending read simply waits with nothing LIVE', async () => {
        const { pending } = await liveRowThenPendingReread();
        mono += 600_000; tick(600_000);                            // ten minutes: still pending, still nothing LIVE
        expect(live()).toBeNull();
        expect(screen.getByRole('button').disabled).toBe(true);
        const src = fs.readFileSync(path.resolve(__dirname, '../components/SchwabPrivateQuote.jsx'), 'utf8').replace(/\n \* /g, ' ');
        expect(src).toMatch(/No cancellation or timeout is implemented/);
        await act(async () => { pending.resolve({ error: true, status: 0 }); });
        expect(screen.getByRole('button').disabled).toBe(false);
    });

    it('R6: unmount during a pending reread, then late resolve — nothing updates, no timer installed', async () => {
        const { pending, unmount } = await liveRowThenPendingReread();
        unmount();
        expect(vi.getTimerCount()).toBe(0);
        mono += 100;
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(vi.getTimerCount()).toBe(0);
        expect(screen.queryByTestId('schwab-private-quote')).toBeNull();
    });

    it('R6: unmount during a pending reread, then late reject — nothing updates, no timer installed', async () => {
        const { pending, unmount } = await liveRowThenPendingReread();
        unmount();
        await act(async () => { pending.reject(new Error('late')); });
        expect(vi.getTimerCount()).toBe(0);
        expect(screen.queryByTestId('schwab-private-quote')).toBeNull();
    });

    it('R7: superseded request — overlap is prevented synchronously, so no older request can complete after a newer one begins', async () => {
        const pending = deferred();
        api.getSchwabPrivateQuotes.mockImplementationOnce(() => pending.promise);
        render(<SchwabPrivateQuote />);
        const button = screen.getByText('Read private quote');
        await act(async () => { fireEvent.click(button); fireEvent.click(button); fireEvent.click(button); });
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(1);
        expect(screen.getByRole('button').disabled).toBe(true);
        mono += 200;
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(live()).toBeTruthy();
        expect(screen.getByRole('button').disabled).toBe(false);
        expect(vi.getTimerCount()).toBe(1);
    });

    it('R8: clock failure while pending applies to the new request only — no old row exists to be promoted', async () => {
        const pending = deferred();
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })))
            .mockImplementationOnce(() => pending.promise);
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        mono = 500;                                                // clock rolled back before the reread starts: new start 500
        await readOnce();
        expect(live()).toBeNull();
        mono = 400;                                                // rollback while pending, relative to the new start
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(live()).toBeNull();
        expect(untrustedShown()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('reading before request start');
        mono = 9_000; tick();
        expect(live()).toBeNull();
    });

    it('R8: an earlier request\'s timer callback cannot act after a reread started (timer removed, sequence advanced)', async () => {
        const { pending } = await liveRowThenPendingReread();
        expect(vi.getTimerCount()).toBe(0);
        mono = 0; tick(5_000);                                     // nothing to tick; no state change, no throw
        expect(live()).toBeNull();
        mono = 900;                                                // below the reread's own start (1100): its own rollback
        await act(async () => { pending.resolve(view({ SPY: serverRow() })); });
        expect(card().getAttribute('data-timing-reason')).toBe('reading before request start');
        expect(live()).toBeNull();
    });
});

describe('<SchwabPrivateQuote /> M8 reread, M10 race isolation, M11 cleanup', () => {
    it('M8: a later successful, independently timed read resets the state after untrusted and after age-out', async () => {
        mono = NaN;
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlightSet(1_000, view({ SPY: serverRow() })))
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })))
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(untrustedShown()).toBeTruthy();
        await readOnce();                                          // start 1000, arrival 1100: trusted
        expect(live()).toBeTruthy();
        expect(card().getAttribute('data-timing-reason')).toBe('');
        mono += 25_000; tick();
        expect(live()).toBeNull();
        await readOnce();
        expect(live()).toBeTruthy();
        expect(vi.getTimerCount()).toBe(1);                        // exactly one timer for the current request
    });

    it('M8: a failed reread withholds values and does not restore a prior LIVE state', async () => {
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })))
            .mockImplementationOnce(inFlight(100, { error: true, status: 403 }));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(live()).toBeTruthy();
        await readOnce();
        expect(screen.getByText(/Admin sign-in required/)).toBeTruthy();
        expect(live()).toBeNull();
        expect(screen.queryByText('731.37')).toBeNull();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('M10: requests never overlap — a click while in flight is ignored', async () => {
        let resolveFirst;
        api.getSchwabPrivateQuotes.mockImplementationOnce(() => new Promise(r => { resolveFirst = r; }));
        render(<SchwabPrivateQuote />);
        await act(async () => { fireEvent.click(screen.getByText('Read private quote')); });
        const button = screen.getByRole('button');
        expect(button.disabled).toBe(true);
        await act(async () => { fireEvent.click(button); fireEvent.click(button); });
        expect(api.getSchwabPrivateQuotes).toHaveBeenCalledTimes(1);
        mono += 100;
        await act(async () => { resolveFirst(view({ SPY: serverRow() })); });
        expect(live()).toBeTruthy();
        expect(button.disabled).toBe(false);
    });

    it('M10/M11: a response settling after unmount installs no timer and exposes nothing', async () => {
        let resolveLater;
        api.getSchwabPrivateQuotes.mockImplementationOnce(() => new Promise(r => { resolveLater = r; }));
        const { unmount } = render(<SchwabPrivateQuote />);
        await act(async () => { fireEvent.click(screen.getByText('Read private quote')); });
        unmount();
        expect(vi.getTimerCount()).toBe(0);
        mono += 100;
        await act(async () => { resolveLater(view({ SPY: serverRow() })); });
        expect(vi.getTimerCount()).toBe(0);
        expect(screen.queryByTestId('schwab-private-quote')).toBeNull();
    });

    it('M10: an old request\'s untrusted timing is not applied to a new request', async () => {
        mono = NaN;
        api.getSchwabPrivateQuotes
            .mockImplementationOnce(inFlightSet(1_000, view({ SPY: serverRow() })))
            .mockImplementationOnce(inFlight(100, view({ SPY: serverRow() })));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(untrustedShown()).toBeTruthy();
        await readOnce();
        expect(live()).toBeTruthy();
        expect(untrustedShown()).toBeNull();
    });

    it('M11: error, disabled and auth responses leave no LIVE row and no timer; unmount clears the timer', async () => {
        for (const status of [0, 401, 403, 503]) {
            api.getSchwabPrivateQuotes.mockImplementation(inFlight(100, { error: true, status }));
            const { unmount } = render(<SchwabPrivateQuote />);
            await readOnce();
            expect(live()).toBeNull();
            expect(vi.getTimerCount()).toBe(0);
            unmount();
        }
        api.getSchwabPrivateQuotes.mockImplementation(inFlight(100, view({ SPY: serverRow() })));
        const { unmount } = render(<SchwabPrivateQuote />);
        await readOnce();
        expect(vi.getTimerCount()).toBe(1);
        unmount();
        expect(vi.getTimerCount()).toBe(0);
    });

    it('M11: a rejected api promise is treated as a network error, not a crash', async () => {
        api.getSchwabPrivateQuotes.mockImplementation(() => Promise.reject(new Error('boom')));
        render(<SchwabPrivateQuote />);
        await readOnce();
        expect(screen.getByText(/Network error/)).toBeTruthy();
        expect(live()).toBeNull();
        expect(vi.getTimerCount()).toBe(0);
    });
});
