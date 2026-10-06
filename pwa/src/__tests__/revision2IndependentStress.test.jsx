// Fresh reviewer-only probes; not part of the candidate commit.
import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import Discovery, { ResearchRunPanel } from '../views/Discovery.jsx';
import Snapshots from '../views/Snapshots.jsx';
import { TickerRecommendations } from '../views/Options.jsx';
import { api } from '../api.js';
import useStore from '../store.js';

const ok = data => ({ ok: true, status: 200, json: () => Promise.resolve(data) });
const gate = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
const fixture = url => {
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/snapshots/compare/')) return { metrics_a: { new_identity: 0 }, metrics_b: { new_identity: 1 } };
    if (url.includes('/snapshots/')) return [];
    if (url.includes('/hypotheses/results')) return { results: [], count: 0 };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/discovery/results/')) return { result: null };
    if (url.includes('/options/recommendations')) return { recommendations: [] };
    return {};
};
beforeEach(() => {
    vi.useFakeTimers(); api.clearCache(); useStore.setState({ jobs: [], hypotheses: [] });
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    global.fetch = vi.fn(url => Promise.resolve(ok(fixture(url))));
});
afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });

it.each([0.25, Number.MAX_SAFE_INTEGER + 1, true, [], {}, Infinity, NaN])('invalid count %j never becomes a successful count', async count => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/hypotheses/results') ? { count } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('Tested hypothesis count unavailable')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('tested hypotheses on record');
});

it('an empty exception string in the real count failure envelope is still unavailable, not zero', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/hypotheses/results') ? { results: [], count: 0, error: '' } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('Tested hypothesis count unavailable')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('0 tested hypotheses on record');
});

it('a fresh valid count recovery does not change or hide a stored failed research record', async () => {
    let countFails = true;
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/snapshots/research/')
        ? { status: 'failed', run_id: 'independent-saved-record', phase: 'validation', error: 'private stored diagnostic' }
        : url.includes('/hypotheses/results') && countFails ? { count: 0, error: 'database failed' } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('independent-saved-record')).toBeInTheDocument();
    expect(screen.getByText('Stored research run reported a failure.')).toBeInTheDocument();
    countFails = false; fireEvent.click(screen.getByRole('button', { name: 'Retry', exact: true })); await flush();
    expect(document.body.textContent).toContain('0 tested hypotheses on record');
    expect(screen.getByText('independent-saved-record')).toBeInTheDocument();
    expect(document.body.textContent).not.toContain('private stored diagnostic');
});

it('empty ticker clears a prior recommendation error without dispatching a new request', async () => {
    global.fetch.mockResolvedValue(ok({ error: 'failed recommendation query', recommendations: [] }));
    const view = render(<TickerRecommendations ticker="SPY" />); await flush();
    expect(screen.getByRole('button', { name: 'Retry' })).toBeEnabled();
    const calls = global.fetch.mock.calls.length;
    view.rerender(<TickerRecommendations ticker="" />); await flush();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
    expect(global.fetch).toHaveBeenCalledTimes(calls);
});

it('ticker replacement aborts an old domain failure and cannot poison a successful new identity', async () => {
    const old = gate();
    global.fetch.mockImplementation(url => url.includes('ticker=SPY') ? old.promise : Promise.resolve(ok(fixture(url))));
    const view = render(<TickerRecommendations ticker="SPY" />); await flush();
    const oldTransport = global.fetch.mock.calls[0][1].signal;
    view.rerender(<TickerRecommendations ticker="QQQ" />); await flush();
    expect(oldTransport.aborted).toBe(true);
    old.resolve(ok({ error: 'late private error', recommendations: [] })); await flush();
    expect(screen.getByText(/No saved trade recommendations for QQQ/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Retry' })).toBeNull();
    expect(document.body.textContent).not.toContain('late private error');
});

async function startComparison() {
    render(<Snapshots />); await flush(); fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true }));
    const inputs = document.querySelectorAll('input[type="date"]');
    fireEvent.change(inputs[0], { target: { value: '2020-01-01' } });
    fireEvent.change(inputs[1], { target: { value: '2020-01-02' } });
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true })); await flush();
    return inputs;
}

it.each([0, 1])('changing comparison date %s rejects late success, then accepts only new dates', async dateIndex => {
    const old = gate(); let first = true;
    global.fetch.mockImplementation(url => {
        if (url.includes('/snapshots/compare/') && first) { first = false; return old.promise; }
        return Promise.resolve(ok(fixture(url)));
    });
    const inputs = await startComparison();
    fireEvent.change(inputs[dateIndex], { target: { value: '2020-01-03' } }); await flush();
    expect(global.fetch.mock.calls.find(([url]) => url.includes('/snapshots/compare/'))[1].signal.aborted).toBe(true);
    old.resolve(ok({ metrics_a: { late_wrong_identity: 99 }, metrics_b: { late_wrong_identity: 100 } })); await flush();
    expect(document.body.textContent).not.toContain('late wrong identity');
    expect(screen.queryByText('Delta')).toBeNull();
    fireEvent.click(screen.getByRole('button', { name: 'Compare', exact: true })); await flush();
    expect(screen.getByText('new identity')).toBeInTheDocument();
    expect(global.fetch.mock.calls.at(-1)[0]).toContain(`${dateIndex === 0 ? 'date_a' : 'date_b'}=2020-01-03`);
});
