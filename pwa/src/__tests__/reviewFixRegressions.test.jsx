import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import GodView from '../views/GodView.jsx';
import Snapshots from '../views/Snapshots.jsx';
import Discovery from '../views/Discovery.jsx';
import Associations from '../views/Associations.jsx';
import { TickerRecommendations } from '../views/Options.jsx';
import useGodViewStore from '../stores/godViewStore.js';
import useStore from '../store.js';

const ok = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
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

async function prepareComparison() {
    await flush();
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    const inputs = document.querySelectorAll('input[type="date"]');
    fireEvent.change(inputs[0], { target: { value: '2020-01-01' } });
    fireEvent.change(inputs[1], { target: { value: '2020-01-02' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    await flush();
    return inputs;
}
const TickerView = () => <TickerRecommendations ticker="SPY" />;

it.each([
    ['God View', GodView, '/godview/', 'God view unavailable'],
    ['Discovery count', Discovery, '/hypotheses/results', 'Tested hypothesis count unavailable'],
    ['Association lag', Associations, '/lag-analysis', 'Lag analysis unavailable'],
    ['Snapshot comparison', Snapshots, '/snapshots/compare/', 'Comparison failed'],
    ['Ticker recommendations', TickerView, '/options/recommendations', 'Trade recommendations unavailable.'],
])('%s rejects a domain failure and retries the same read fresh', async (_name, View, path, title) => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes(path)
        ? { ...fixture(url), error: 'private database failure' } : fixture(url))));
    render(<View />);
    await flush();
    if (View === Associations) { fireEvent.click(screen.getByText('FOO \u00d7 BAR', { exact: true })); await flush(); }
    if (View === Snapshots) await prepareComparison();
    expect(screen.getByText(title)).toBeInTheDocument();
    expect(document.body.textContent).not.toMatch(/private database|0 tested hypotheses on record/);
    const beforeRetry = global.fetch.mock.calls.length;
    global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
    fireEvent.click(screen.getByRole('button', { name: 'Retry', exact: true }));
    await flush();
    expect(screen.queryByText(title)).toBeNull();
    const retried = global.fetch.mock.calls.slice(beforeRetry).filter(([url]) => url.includes(path));
    expect(retried).toHaveLength(1);
    expect(retried[0][1].cache).toBe('no-store');
    expect(global.fetch.mock.calls.every(([, options]) => !options.method || options.method === 'GET')).toBe(true);
});

it.each([null, undefined, -1, '0'])('Discovery reports an unknown or invalid count (%s) as unavailable', async count => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/hypotheses/results') ? { count } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('Tested hypothesis count unavailable')).toBeInTheDocument();
    expect(screen.getByText('The server sent an unreadable response. Please retry.')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('0 tested hypotheses on record');
});

it('Discovery keeps a genuine zero count and a legitimate stored failed research-run record', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/snapshots/research/')
        ? { status: 'failed', run_id: 'saved-failed-run', phase: 'feature_list', error: 'private stored error', code_sha: 'deadbeef' }
        : fixture(url))));
    render(<Discovery />); await flush();
    expect(document.body.textContent).toContain('0 tested hypotheses on record');
    expect(screen.getByText('saved-failed-run')).toBeInTheDocument();
    expect(screen.getByText('Stored research run reported a failure.')).toBeInTheDocument();
    expect(screen.queryByText('Tested hypothesis count unavailable')).toBeNull();
    expect(document.body.textContent).not.toContain('private stored error');
});

it.each([0, 1])('Snapshot comparison date %s change clears the old error and compares the new identity', async dateIndex => {
    global.fetch.mockImplementation(url => Promise.resolve(url.includes('/snapshots/compare/') ? { ok: false, status: 404 } : ok(fixture(url))));
    render(<Snapshots />);
    const inputs = await prepareComparison();
    expect(screen.getByText('Comparison failed')).toBeInTheDocument();
    fireEvent.change(inputs[dateIndex], { target: { value: '2020-01-03' } });
    await flush();
    expect(screen.queryByText('Comparison failed')).toBeNull();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
    global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    await flush();
    expect(screen.getByText('Delta')).toBeInTheDocument();
    const compareCalls = global.fetch.mock.calls.filter(([url]) => url.includes('/snapshots/compare/'));
    expect(compareCalls.at(-1)[0]).toContain(`${dateIndex ? 'date_b' : 'date_a'}=2020-01-03`);
});

it.each(['category', 'date'])('Snapshot %s change cancels a pending comparison and rejects its late error', async identity => {
    const old = deferred();
    global.fetch.mockImplementation(url => url.includes('/snapshots/compare/') ? old.promise : Promise.resolve(ok(fixture(url))));
    render(<Snapshots />);
    const inputs = await prepareComparison();
    const request = global.fetch.mock.calls.find(([url]) => url.includes('/snapshots/compare/'));
    if (identity === 'category') fireEvent.click(screen.getByRole('button', { name: 'orthogonality' }));
    else fireEvent.change(inputs[1], { target: { value: '2020-01-03' } });
    await flush();
    expect(request[1].signal.aborted).toBe(true);
    old.resolve(ok({ error: 'late private failure' }));
    await flush();
    expect(screen.queryByText('Comparison failed')).toBeNull();
    expect(document.body.textContent).not.toMatch(/late private|Loading/);
});
