// Independent reviewer tests. Candidate production files remain unchanged.
import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import GodView from '../views/GodView.jsx';
import Snapshots from '../views/Snapshots.jsx';
import Discovery from '../views/Discovery.jsx';
import Associations from '../views/Associations.jsx';
import Options from '../views/Options.jsx';
import useGodViewStore from '../stores/godViewStore.js';
import useStore from '../store.js';

const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const ok = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const never = () => new Promise(() => {});
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
const fixture = url => {
    if (url.includes('/godview/')) return { as_of: '2020-01-01', pillars: {} };
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/snapshots/compare/')) return { metrics_a: { synthetic_zero: 0 }, metrics_b: { synthetic_zero: 1 } };
    if (url.includes('/snapshots/')) return [{ id: 1, created_at: '2020-01-01', payload: { synthetic_zero: 0 } }];
    if (url.includes('/discovery/results/')) return { result: null };
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/hypotheses/results')) return { count: 0 };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/options/signals')) return { signals: [] };
    if (url.includes('/options/')) return { opportunities: [] };
    if (url.includes('/associations/anomalies')) return { anomalies: [] };
    if (url.includes('/correlation-matrix')) return { strong_pairs: [{ a: 'FOO', b: 'BAR', kind: 'interesting', corr: 0.5 }] };
    return {};
};
beforeEach(() => {
    vi.useFakeTimers(); api.clearCache(); useGodViewStore.getState().reset();
    useStore.setState({ jobs: [], hypotheses: [] });
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    global.fetch = vi.fn(url => Promise.resolve(ok(fixture(String(url)))));
});
afterEach(() => { cleanup(); useGodViewStore.getState().reset(); vi.useRealTimers(); vi.restoreAllMocks(); });

it.each([['God View', GodView], ['Snapshots', Snapshots], ['Discovery', Discovery], ['Associations', Associations], ['Options', Options]])('%s bounds uncooperative response-body parsing and recovers', async (_name, View) => {
    global.fetch.mockResolvedValue({ ok: true, status: 200, json: never });
    render(<View />);
    await act(() => vi.advanceTimersByTimeAsync(15_000));
    expect(document.body.textContent).toContain('timed out after 15 seconds');
    expect(global.fetch.mock.calls.every(([, options]) => options.signal.aborted)).toBe(true);
    global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(String(url)))));
    for (const retry of screen.getAllByRole('button', { name: 'Retry' })) fireEvent.click(retry);
    await flush();
    expect(document.body.textContent).not.toContain('timed out after 15 seconds');
});

it('foreground never joins the identical background URL and preserves its cache against a late body', async () => {
    const body = deferred(); const selected = deferred();
    global.fetch.mockResolvedValueOnce({ ok: true, status: 200, json: () => body.promise }).mockReturnValueOnce(selected.promise);
    const warming = api.preload(['/api/v1/godview/latest']);
    await flush();
    const foreground = api.getGodViewLatest();
    expect(global.fetch).toHaveBeenCalledTimes(2);
    expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(true);
    expect(global.fetch.mock.calls[1][1].signal.aborted).toBe(false);
    selected.resolve(ok({ pillars: {}, version: 'foreground' }));
    expect(await foreground).toMatchObject({ version: 'foreground' });
    body.resolve({ pillars: {}, version: 'background-late' });
    await warming;
    expect(await api.getGodViewLatest()).toMatchObject({ version: 'foreground' });
});

it('transport ownership survives one subscriber deadline and settles for the remaining owner', async () => {
    const gate = deferred(); global.fetch.mockReturnValue(gate.promise);
    const firstOwner = new AbortController(); const secondOwner = new AbortController();
    const first = api.getGodViewLatest({ signal: firstOwner.signal });
    await vi.advanceTimersByTimeAsync(200);
    const second = api.getGodViewLatest({ signal: secondOwner.signal });
    firstOwner.abort({ code: 'timeout' });
    expect(await first).toMatchObject({ code: 'timeout' });
    expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(false);
    gate.resolve(ok({ pillars: {}, value: 0 })); expect(await second).toMatchObject({ value: 0 });
    expect(api._inflight.size).toBe(0); expect(api._foregroundRequests).toBe(0);
});

it('receipt age is not reset on near-expiry cache hits and an expired cache triggers I/O', async () => {
    const first = await api.getGodViewLatest(); const receipt = api.getResponseInfo(first).receivedAt;
    await vi.advanceTimersByTimeAsync(59_999);
    expect(await api.getGodViewLatest()).toBe(first);
    expect(api.getResponseInfo(first)).toEqual({ receivedAt: receipt, ageMs: 59_999 });
    await vi.advanceTimersByTimeAsync(1);
    const second = await api.getGodViewLatest(); expect(second).not.toBe(first);
    expect(global.fetch).toHaveBeenCalledTimes(2);
});

it('God View rejects a 200 error-string envelope even when it includes partial pillars', async () => {
    global.fetch.mockResolvedValue(ok({ error: 'synthetic private failure', pillars: {}, as_of: '2020-01-01' }));
    render(<GodView />); await flush();
    expect(useGodViewStore.getState().latest).toBeNull();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
});

it('Associations lag rejects an HTTP 200 error string and offers a retry', async () => {
    render(<Associations />); await flush();
    global.fetch.mockResolvedValue(ok({ error: 'synthetic private database failure' }));
    fireEvent.click(screen.getByText('FOO × BAR', { exact: true })); await flush();
    expect(screen.getByText('Lag analysis unavailable')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
});

it('Discovery tested-count rejects an HTTP 200 error string instead of reporting a successful count', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/hypotheses/results') ? { error: 'synthetic private failure', count: 0 } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('Tested hypothesis count unavailable')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('0 tested hypotheses on record');
});

async function openComparison() {
    render(<Snapshots />); await flush();
    fireEvent.click(screen.getByRole('button', { name: 'Compare' }));
    const inputs = document.querySelectorAll('input[type="date"]');
    fireEvent.change(inputs[0], { target: { value: '2020-01-01' } });
    fireEvent.change(inputs[1], { target: { value: '2020-01-02' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare' })); await flush();
}

it('Snapshot comparison rejects an HTTP 200 error-string envelope and offers a retry', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/snapshots/compare/') ? { error: 'synthetic private failure' } : fixture(url))));
    await openComparison();
    expect(screen.getByText('Comparison failed')).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
});

it('Snapshot category change clears the previous category comparison error', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(url.includes('/snapshots/compare/') ? { ok: false, status: 404 } : ok(fixture(url))));
    await openComparison(); expect(screen.getByText('Comparison failed')).toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: 'orthogonality' })); await flush();
    expect(screen.queryByText('Comparison failed')).toBeNull();
});
