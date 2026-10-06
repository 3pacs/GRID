import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import { requireRead } from '../requestState.js';
import GodView from '../views/GodView.jsx';
import Snapshots from '../views/Snapshots.jsx';
import Discovery, { ResearchRunPanel } from '../views/Discovery.jsx';
import Associations from '../views/Associations.jsx';
import Options, { TickerRecommendations } from '../views/Options.jsx';
import useGodViewStore from '../stores/godViewStore.js';
import useStore from '../store.js';

const ok = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
const countPath = '/api/v1/discovery/hypotheses/results';
const researchPath = '/api/v1/snapshots/research/latest';
// String diagnostics (including empty) are the backend failure contract.
// The other explicit fields are malformed, never success-by-truthiness.
const diagnostics = ['', null, false, 0, [], { private_detail: 'secret' }, true, undefined];
const diagnosticRows = diagnostics.map(diagnostic => [diagnostic]);
const fixture = url => {
    if (url.includes('/godview/')) return { as_of: '2020-01-01', pillars: {} };
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/snapshots/compare/')) return { metrics_a: { saved_zero: 0 }, metrics_b: { saved_zero: 1 } };
    if (url.includes('/snapshots/')) return [{ id: 1, created_at: '2020-01-01', payload: { saved_zero: 0 } }];
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/discovery/results/')) return { result: null };
    if (url.includes('/hypotheses/results')) return { results: [], count: 0 };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/correlation-matrix')) return { strong_pairs: [{ a: 'FOO', b: 'BAR', kind: 'interesting', corr: 0.5 }] };
    if (url.includes('/lag-analysis')) return { lags: [] };
    if (url.includes('/associations/anomalies')) return { anomalies: [] };
    if (url.includes('/options/recommendations')) return { recommendations: [] };
    if (url.includes('/options/signals')) return { signals: [] };
    if (url.includes('/options/')) return { opportunities: [] };
    return {};
};
beforeEach(() => {
    vi.useFakeTimers(); api.clearCache(); useGodViewStore.getState().reset();
    useStore.setState({ jobs: [], hypotheses: [] });
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    global.fetch = vi.fn(url => Promise.resolve(ok(fixture(url))));
});
afterEach(() => { cleanup(); useGodViewStore.getState().reset(); vi.useRealTimers(); vi.restoreAllMocks(); });

it.each(diagnosticRows)('explicit error field %j fails the analytic guard even with a zero', diagnostic => {
    expect(() => requireRead({ count: 0, error: diagnostic })).toThrow();
    expect(requireRead({ count: 0 })).toEqual({ count: 0 });
});

it.each(diagnosticRows)('explicit diagnostic %j cannot poison counted-read cache after recovery', async diagnostic => {
    global.fetch.mockResolvedValueOnce(ok({ results: [], count: 0, error: diagnostic })).mockResolvedValue(ok({ count: 3 }));
    const failed = await api.get(`${countPath}?state=FAILED`);
    expect(failed.error).toBe(true);
    expect(() => requireRead(failed)).toThrow();
    expect(api.getResponseInfo(failed)).toBeNull();
    expect(api._cache.has(`${countPath}?state=FAILED`)).toBe(false);
    expect(await api.get(`${countPath}?state=FAILED`)).toEqual({ count: 3 });
    expect(global.fetch).toHaveBeenCalledTimes(2);
});

it.each([undefined, null, -1, '0', 0.25, Number.MAX_SAFE_INTEGER + 1, true, [], {}, Infinity, NaN].map(count => [count]))('invalid count %j is never admitted to the success cache', async count => {
    global.fetch.mockResolvedValueOnce(ok({ count })).mockResolvedValue(ok({ count: 0 }));
    const failed = await api.get(countPath);
    expect(failed).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(countPath)).toBe(false);
    expect(await api.get(countPath)).toEqual({ count: 0 });
    expect(global.fetch).toHaveBeenCalledTimes(2);
});

it('a genuine zero count is cached without resetting its receipt age', async () => {
    global.fetch.mockResolvedValue(ok({ results: [], count: 0 }));
    const first = await api.get(countPath);
    await vi.advanceTimersByTimeAsync(2_000);
    expect(await api.get(countPath)).toBe(first);
    expect(api.getResponseInfo(first).ageMs).toBe(2_000);
    expect(global.fetch).toHaveBeenCalledTimes(1);
});

async function compare() {
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    const dates = document.querySelectorAll('input[type="date"]');
    fireEvent.change(dates[0], { target: { value: '2020-01-01' } });
    fireEvent.change(dates[1], { target: { value: '2020-01-02' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    await flush();
}
const TickerView = () => <TickerRecommendations ticker="SPY" />;
const callers = [
    ['God View', GodView, '/godview/', 'God view unavailable'],
    ['Discovery count', Discovery, '/hypotheses/results', 'Tested hypothesis count unavailable'],
    ['Association lag', Associations, '/lag-analysis', 'Lag analysis unavailable'],
    ['Snapshot comparison', Snapshots, '/snapshots/compare/', 'Comparison failed'],
    ['Ticker recommendations', TickerView, '/options/recommendations', 'Trade recommendations unavailable.'],
    ['Options signals', Options, '/options/signals', 'Options data unavailable'],
];
for (const [name, View, path, title] of callers) {
    it.each(diagnosticRows)(`${name}: diagnostic %j restores Retry and never declares fresh success`, async diagnostic => {
        global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes(path) ? { ...fixture(url), error: diagnostic } : fixture(url))));
        render(<View />); await flush();
        if (View === Associations) { fireEvent.click(screen.getByText('FOO × BAR', { exact: true })); await flush(); }
        if (View === Snapshots) await compare();
        expect(screen.getByText(title)).toBeInTheDocument();
        expect(document.body.textContent).not.toMatch(/secret|0 tested hypotheses on record/);
        expect(screen.getByRole('button', { name: 'Retry', exact: true })).toBeEnabled();
        const calls = global.fetch.mock.calls.length;
        global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
        fireEvent.click(screen.getByRole('button', { name: 'Retry', exact: true })); await flush();
        expect(screen.queryByText(title)).toBeNull();
        const retried = global.fetch.mock.calls.slice(calls).filter(([url]) => url.includes(path));
        expect(retried).toHaveLength(1);
        expect(retried[0][1].cache).toBe('no-store');
    });
}

it.each([
    ['ok', null], ['started', null], ['running', null], ['failed', ''],
    ['failed', 'private stored diagnostic'], ['failed', 'offline'], ['timeout', null], ['abandoned', null],
])('stored %s research record with diagnostic %j remains valid persisted data', async (status, diagnostic) => {
    const record = { status, run_id: 'persisted-record-identity', phase: 'validation', error: diagnostic };
    global.fetch.mockResolvedValue(ok(record));
    const first = await api.get(researchPath);
    expect(first).toEqual(record);
    expect(await api.get(researchPath)).toBe(first);
    expect(global.fetch).toHaveBeenCalledTimes(1);
    render(<ResearchRunPanel />); await flush();
    expect(screen.getByText('persisted-record-identity')).toBeInTheDocument();
    expect(screen.getByText(status.toUpperCase())).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/private stored diagnostic|offline/);
    if (['failed', 'timeout', 'abandoned'].includes(status)) expect(screen.getByText('Stored research run reported a failure.')).toBeInTheDocument();
});

it.each([false, 0, [], {}, true].map(diagnostic => [diagnostic]))('malformed research diagnostic %j is unavailable and uncached', async diagnostic => {
    global.fetch.mockResolvedValue(ok({ status: 'failed', run_id: 'not-valid-record', error: diagnostic }));
    render(<ResearchRunPanel />); await flush();
    expect(screen.getByText('Research status unavailable')).toBeInTheDocument();
    expect(screen.queryByText('not-valid-record')).toBeNull();
    expect(api._cache.has(researchPath)).toBe(false);
});

it.each(diagnosticRows)('nested discovery result diagnostic %j stays unavailable and uncached', async diagnostic => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/discovery/results/orthogonality')
        ? { result: { error: diagnostic, effective_dimensionality: 9 } } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('Orthogonality result unavailable.')).toBeInTheDocument();
    expect(api._cache.has('/api/v1/discovery/results/orthogonality')).toBe(false);
});

it('the actual service-worker sentinel is unavailable without confusing a stored record', async () => {
    global.fetch.mockResolvedValue(ok({ error: 'offline' }));
    const failed = await api.get(researchPath);
    expect(failed).toMatchObject({ error: true, code: 'network' });
    expect(api._cache.has(researchPath)).toBe(false);
});
