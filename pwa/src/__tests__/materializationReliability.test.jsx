import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import GodView from '../views/GodView.jsx';
import Snapshots from '../views/Snapshots.jsx';
import Discovery from '../views/Discovery.jsx';
import Associations from '../views/Associations.jsx';
import Options, { TickerRecommendations } from '../views/Options.jsx';
import useGodViewStore from '../stores/godViewStore.js';
import useStore from '../store.js';
import { api } from '../api.js';

const never = () => new Promise(() => {});
const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const ok = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const fixture = url => {
    if (url.includes('/godview/')) return { as_of: '2020-01-01', pillars: {} };
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/snapshots/')) return [];
    if (url.includes('/discovery/results/')) return { result: null };
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/hypotheses/results')) return { count: 0 };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/options/recommendations')) return { recommendations: [], history: [], scan_summary: {} };
    if (url.includes('/options/signals')) return { signals: [] };
    if (url.includes('/options/')) return { opportunities: [] };
    if (url.includes('/associations/anomalies')) return { anomalies: [] };
    return {};
};
const views = [
    ['God View', GodView], ['Snapshots', Snapshots], ['Discovery', Discovery], ['Associations', Associations], ['Options', Options],
];
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));

beforeEach(() => {
    vi.useFakeTimers();
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    api.clearCache();
    useGodViewStore.getState().reset();
    useStore.setState({ jobs: [], hypotheses: [] });
    global.fetch = vi.fn(url => Promise.resolve(ok(fixture(url))));
});
afterEach(() => { cleanup(); useGodViewStore.getState().reset(); vi.useRealTimers(); vi.restoreAllMocks(); });

describe.each(views)('%s offline acceptance', (_name, View) => {
    it('settles normal results and retains data dates independently of request receipt', async () => {
        render(<View />);
        await flush();
        expect(screen.queryByText(/Please retry/)).toBeNull();
        expect(screen.queryByText(/^Loading/)).toBeNull();
        expect(document.body.textContent).toMatch(/Data freshness|No options signals/);
    });

    it('restores controls within 15s, retries fresh, and never labels failure empty success', async () => {
        global.fetch.mockImplementation(never);
        render(<View />);
        await act(() => vi.advanceTimersByTimeAsync(15_000));
        expect(document.body.textContent).toMatch(/timed out after 15 seconds/);
        expect(global.fetch.mock.calls.every(([, options]) => options.signal.aborted)).toBe(true);
        const firstCount = global.fetch.mock.calls.length;
        const initialPaths = new Set(global.fetch.mock.calls.map(([url]) => url));
        global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
        for (const retry of screen.getAllByRole('button', { name: 'Retry' })) fireEvent.click(retry);
        await flush();
        expect(global.fetch.mock.calls.length).toBeGreaterThan(firstCount);
        expect(global.fetch.mock.calls.slice(firstCount).filter(([url]) => initialPaths.has(url)).every(([, options]) => options.cache === 'no-store')).toBe(true);
        expect(document.body.textContent).not.toMatch(/timed out after 15 seconds/);
    });

    it.each(['524', 'offline'])('renders a safe %s failure instead of a successful empty state', async mode => {
        global.fetch.mockImplementation(mode === 'offline'
            ? () => Promise.reject(new Error('private secret network host'))
            : () => Promise.resolve({ ok: false, status: 524, text: () => Promise.resolve('<html>Cloudflare private body</html>') }));
        render(<View />);
        await flush();
        expect(document.body.textContent).toMatch(/unavailable|unreachable|could not be reached/i);
        expect(document.body.textContent).not.toMatch(/Cloudflare|private|<html>|No options signals available|No snapshots found|No research run recorded/);
    });

    it('cancels on disposal; a late response cannot affect a fresh mount', async () => {
        const old = deferred();
        global.fetch.mockReturnValue(old.promise);
        const mounted = render(<View />);
        mounted.unmount();
        await flush();
        expect(global.fetch.mock.calls.every(([, options]) => options.signal.aborted)).toBe(true);
        global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
        render(<View />);
        await flush();
        old.resolve(ok({ error: true, status: 500, message: 'late private failure' }));
        await flush();
        expect(document.body.textContent).not.toMatch(/private|status 500/);
    });
});

it('Snapshots category changes abort old work and ignore late results', async () => {
    const old = deferred();
    global.fetch.mockImplementation(url => url.includes('/clustering') ? old.promise : Promise.resolve(ok([
        { id: 2, created_at: '2020-01-01', payload: { new_category_value: 7 } },
    ])));
    render(<Snapshots />);
    fireEvent.click(screen.getByRole('button', { name: 'orthogonality' }));
    await flush();
    expect(global.fetch.mock.calls[0][1].signal.aborted).toBe(true);
    expect(document.body.textContent).toMatch(/new category value/);
    old.resolve(ok([{ id: 1, payload: { old_category_value: 99 } }]));
    await flush();
    expect(document.body.textContent).not.toMatch(/old category value/);
});

it.each(['Signals', 'Dealer Flow'])('Options %s rejects HTTP 200 domain failures without private text or empty success', async tab => {
    global.fetch.mockImplementation(url => Promise.resolve(ok({ ...fixture(url), error: 'private database detail' })));
    render(<Options />);
    if (tab === 'Dealer Flow') fireEvent.click(screen.getByRole('button', { name: tab }));
    await flush();
    expect(document.body.textContent).toMatch(/The server could not return this data/);
    expect(document.body.textContent).not.toMatch(/private|database|No options signals available|LATEST GEX/);
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
});

it('Options ticker/tab changes cancel and reject late dealer data while read controls remain usable', async () => {
    const old = deferred();
    global.fetch.mockImplementation(url => url.includes('/derivatives/') && url.includes('SPY') ? old.promise : Promise.resolve(ok(
        url.includes('/derivatives/gex/') ? { ticker: 'QQQ', spot: 321, strikes: [] } : fixture(url),
    )));
    render(<Options />);
    fireEvent.click(screen.getByRole('button', { name: 'Dealer Flow' }));
    await flush();
    fireEvent.click(screen.getByRole('button', { name: 'QQQ' }));
    await flush();
    expect(global.fetch.mock.calls.filter(([url]) => url.includes('/derivatives/') && url.includes('SPY')).every(([, options]) => options.signal.aborted)).toBe(true);
    old.resolve(ok({ ticker: 'SPY', spot: 999999, strikes: [] }));
    await flush();
    expect(document.body.textContent).not.toContain('999999');
    fireEvent.click(screen.getByRole('button', { name: 'Signals' }));
    await flush();
    expect(screen.getByText('No options signals available')).toBeInTheDocument();
});

it('Ticker recommendations bound loading, offer retry and cancel a superseded ticker', async () => {
    global.fetch.mockImplementation(never);
    const view = render(<TickerRecommendations ticker="SPY" />);
    await act(() => vi.advanceTimersByTimeAsync(15_000));
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
    global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
    view.rerender(<TickerRecommendations ticker="QQQ" />);
    await flush();
    expect(screen.getByText(/No saved trade recommendations for QQQ/)).toBeInTheDocument();
});

it('Options saved trades settle within the deadline and read-only retry never calls recommendation refresh', async () => {
    render(<Options />);
    await flush();
    global.fetch.mockImplementation(never);
    fireEvent.click(screen.getByRole('button', { name: 'Trades' }));
    await act(() => vi.advanceTimersByTimeAsync(15_000));
    expect(screen.getByText('Saved trades unavailable')).toBeInTheDocument();
    global.fetch.mockImplementation(url => Promise.resolve(ok(fixture(url))));
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await flush();
    expect(document.body.textContent).not.toMatch(/timed out/);
    expect(global.fetch.mock.calls.every(([, options]) => !options.method || options.method === 'GET')).toBe(true);
});

it('Associations bounds lag loading, retries within the expanded card, and cancels collapsed late responses', async () => {
    const old = deferred();
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/correlation-matrix')
        ? { strong_pairs: [{ a: 'FOO', b: 'BAR', kind: 'interesting', corr: 0.5 }] } : fixture(url))));
    render(<Associations />);
    await flush();
    global.fetch.mockImplementation(never);
    fireEvent.click(screen.getByText('FOO × BAR', { exact: true }));
    await act(() => vi.advanceTimersByTimeAsync(15_000));
    expect(screen.getByText('Lag analysis unavailable')).toBeInTheDocument();
    global.fetch.mockReturnValue(old.promise);
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await flush();
    expect(screen.getByText('Computing lag analysis...')).toBeInTheDocument();
    fireEvent.click(screen.getByText('FOO × BAR', { exact: true }));
    await flush();
    expect(global.fetch.mock.calls.at(-1)[1].signal.aborted).toBe(true);
    old.resolve(ok({ direction: 'late-private-marker', strength: 1, lags: [{ lag: 1, correlation: 0.5 }] }));
    await flush();
    expect(document.body.textContent).not.toMatch(/late-private-marker/);
});

it('Associations cancellation fences a late spotlight timeseries after navigation', async () => {
    const old = deferred();
    global.fetch.mockImplementation(url => url.includes('/signals/timeseries') ? old.promise : Promise.resolve(ok(
        url.includes('/smart-heatmap') ? { z_scores: [{ feature: 'TEST_FEATURE', zscore: 1, family: 'test' }] } : fixture(url),
    )));
    const view = render(<Associations />);
    await flush();
    const request = global.fetch.mock.calls.find(([url]) => url.includes('/signals/timeseries'));
    expect(request).toBeTruthy();
    view.unmount();
    await flush();
    expect(request[1].signal.aborted).toBe(true);
    old.resolve(ok({ series: { TEST_FEATURE: [999, 998] } }));
    await flush();
    expect(document.body.textContent).not.toContain('TEST_FEATURE');
});
