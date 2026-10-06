import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Discovery, { ResearchRunPanel } from '../views/Discovery.jsx';
import { TickerRecommendations } from '../views/Options.jsx';
import useStore from '../store.js';

const ok = data => ({ ok: true, status: 200, json: async () => data });
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
const audit = type => `/api/v1/discovery/results/${type}`;
const research = '/api/v1/snapshots/research/latest';
const recommendations = '/api/v1/options/recommendations?ticker=SPY';
// One source-backed checked-empty/record/partial control per affected read family.
const successful = [
    ['/api/v1/godview/latest', { pillars: { fed_liquidity: { status: 'unavailable', reason: 'never_run', data: null } } }],
    ['/api/v1/godview/history?pillar=fed_liquidity', { entries: [], total: 0, status: 'unavailable', reason: 'never_run' }],
    ['/api/v1/snapshots/latest/clustering?n=1', [{ payload: { error: 'historical recorded failure' }, as_of_date: '2020-01-01' }]],
    ['/api/v1/snapshots/history/clustering', []],
    ['/api/v1/snapshots/compare/clustering?date_a=2020-01-01&date_b=2020-01-02', { metrics_a: {}, metrics_b: {} }],
    [research, { status: 'no_runs' }],
    [`${research}?fixture=1`, { status: 'failed', run_id: 'stored-run', error: '' }],
    ['/api/v1/discovery/jobs', { jobs: [{ status: 'failed', result: { error: 'stored job failure' } }] }],
    ['/api/v1/discovery/hypotheses?state=FAILED', { hypotheses: [] }],
    ['/api/v1/discovery/hypotheses/results', { count: 0, results: [], research_lane_off: true }],
    [audit('orthogonality'), { result: null, message: 'No completed orthogonality audit found' }],
    [audit('clustering'), { result: null, message: 'No completed clustering run found' }],
    [`${audit('clustering')}?sector=Energy`, { result: null, message: "No clustering result for sector 'Energy'" }],
    [audit('clustering/sectors'), { sectors: [] }],
    ['/api/v1/discovery/smart-heatmap?orthogonal_only=true', { features: [], matrix: [], z_scores: [], families: [] }],
    ['/api/v1/associations/correlation-matrix?days=252', { features: [], matrix: [], strong_pairs: [] }],
    ['/api/v1/associations/anomalies?sigma_threshold=2.5', { anomalies: [], message: 'Insufficient data' }],
    ['/api/v1/associations/regime-features?days=504', { regimes: {}, message: 'No regime history found' }],
    ['/api/v1/associations/lag-analysis?feature_a=FOO&feature_b=BAR', { lags: [], strength: 0 }],
    ['/api/v1/signals/timeseries?features=FOO&days=30', { series: [], dates: [], count: 0 }],
    ['/api/v1/options/signals?limit=50', { signals: [], count: 0 }],
    ['/api/v1/options/scan?min_score=5', { opportunities: [], count: 0 }],
    ['/api/v1/options/100x', { opportunities: [], count: 0 }],
    [recommendations, { recommendations: [], scan_summary: { source: 'missing', data_status: 'missing', reason: 'no_saved_recommendations' } }],
    ['/api/v1/options/recommendations/history?limit=50', { history: [], count: 0, total: 0 }],
    ['/api/v1/options/history?days=30', { history: [], count: 0 }],
    ['/api/v1/derivatives/gex/SPY', { ticker: 'SPY', spot: 321, gex_aggregate: 0, per_strike: [], snap_date: '2020-01-01' }],
    ['/api/v1/derivatives/vanna-charm/SPY', { vanna_exposure: 0, charm_exposure: 0, snap_date: '2020-01-01' }],
    ['/api/v1/derivatives/flow-timeline/SPY?days=90', { history: [{ date: '2020-01-01', net_gex: 0 }], history_status: 'partial', failed_dates: 2 }],
].map(([path, data]) => ({ path, data }));
const failureContracts = [
    [audit('orthogonality'), { result: null, message: 'orthogonality snapshot unavailable' }, { result: { true_dimensionality: 3 }, as_of_date: '2020-01-01' }],
    [audit('clustering'), { result: null, message: 'clustering snapshot unavailable' }, { result: { best_k: 2 }, as_of_date: '2020-01-01' }],
    [`${audit('clustering')}?sector=Energy`, { result: null, message: 'clustering_sector snapshot unavailable' }, { result: { best_k: 2 } }],
    [audit('clustering/sectors'), { sectors: [], message: 'clustering_sector snapshots unavailable' }, { sectors: [] }],
    [research, { status: 'unavailable', reason: 'private SQL detail' }, { status: 'no_runs' }],
    [recommendations, { recommendations: [], scan_summary: { source: 'unavailable', data_status: 'unavailable', reason: 'saved_recommendations_read_failed' } }, { recommendations: [], scan_summary: { source: 'missing', data_status: 'missing' } }],
].map(([path, failed, recovered]) => ({ path, failed, recovered }));
beforeEach(() => {
    vi.useFakeTimers(); api.clearCache(); useStore.setState({ jobs: [], hypotheses: [] });
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    global.fetch = vi.fn(url => Promise.resolve(ok(defaultFixture(url))));
});
afterEach(() => { cleanup(); api.clearCache(); vi.useRealTimers(); vi.restoreAllMocks(); });
const defaultFixture = url => {
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/hypotheses/results')) return { count: 0, results: [] };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/discovery/results/')) return { result: null };
    return {};
};

it.each(successful)('$path preserves checked-empty, stored and partial data with truthful cache age', async ({ path, data }) => {
    global.fetch.mockResolvedValue(ok(data));
    const result = await api.get(path);
    expect(result).toEqual(data);
    await vi.advanceTimersByTimeAsync(1_000);
    expect(await api.get(path)).toBe(result);
    expect(api.getResponseInfo(result).ageMs).toBe(1_000);
    expect(global.fetch).toHaveBeenCalledTimes(1);
});
it.each(successful)('$path never admits HTTP failure bodies as cached/received success', async ({ path, data }) => {
    const body = vi.fn(async () => '<html>private database detail</html>');
    global.fetch.mockResolvedValueOnce({ ok: false, status: 503, text: body }).mockResolvedValue(ok(data));
    const failed = await api.get(path);
    expect(failed).toMatchObject({ error: true, status: 503 });
    expect(body).not.toHaveBeenCalled();
    expect(api.getResponseInfo(failed)).toBeNull();
    expect(api._cache.has(path)).toBe(false);
    expect(await api.get(path)).toEqual(data);
    expect(global.fetch).toHaveBeenCalledTimes(2);
});
it.each(failureContracts)('$path refuses its typed failed-read sentinel before cache/receipt and recovers ordinarily', async ({ path, failed, recovered }) => {
    global.fetch.mockResolvedValueOnce(ok(failed)).mockResolvedValue(ok(recovered));
    const result = await api.get(path);
    expect(result).toMatchObject({ error: true, code: 'http' });
    expect(JSON.stringify(result)).not.toMatch(/private SQL|snapshot unavailable|read_failed/);
    expect(api.getResponseInfo(result)).toBeNull();
    expect(api._cache.has(path)).toBe(false);
    expect(await api.get(path)).toEqual(recovered);
    expect(global.fetch).toHaveBeenCalledTimes(2);
});
it.each([
    ['/api/v1/discovery/jobs', 'jobs'], ['/api/v1/discovery/hypotheses', 'hypotheses'],
    ['/api/v1/options/signals', 'signals'], ['/api/v1/options/scan', 'opportunities'], ['/api/v1/options/100x', 'opportunities'],
    ['/api/v1/options/recommendations', 'recommendations'], ['/api/v1/options/recommendations/history', 'history'], ['/api/v1/options/history', 'history'],
    [audit('clustering/sectors'), 'sectors'],
])('%s does not cache a malformed required list', async (path, key) => {
    global.fetch.mockResolvedValueOnce(ok({ [key]: null })).mockResolvedValue(ok({ [key]: [] }));
    expect(await api.get(path)).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(path)).toBe(false);
    expect(await api.get(path)).toEqual({ [key]: [] });
});
it.each(['latest', 'history'])('snapshot %s does not cache a malformed bare-list response', async type => {
    const path = `/api/v1/snapshots/${type}/clustering`;
    global.fetch.mockResolvedValueOnce(ok({ snapshots: [] })).mockResolvedValue(ok([]));
    expect(await api.get(path)).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(path)).toBe(false);
    expect(await api.get(path)).toEqual([]);
});
it.each([null, {}, { status: 'failed' }, { status: 'unknown', run_id: 'record' }].map(data => [data]))('research records without a valid envelope/identity stay unavailable (%j)', async data => {
    global.fetch.mockResolvedValueOnce(ok(data)).mockResolvedValue(ok({ status: 'no_runs' }));
    expect(await api.get(research)).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(research)).toBe(false);
    expect(await api.get(research)).toEqual({ status: 'no_runs' });
});
it.each(['orthogonality', 'clustering'])('%s lookup failure, dated result and checked-empty remain different UI states', async type => {
    let failing = true;
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes(audit(type))
        ? failing ? { result: null, message: `${type} snapshot unavailable` } : { result: type === 'orthogonality' ? { true_dimensionality: 3 } : { best_k: 2 }, source: 'analytical_snapshots', as_of_date: '2020-01-01' }
        : defaultFixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText(type === 'orthogonality' ? 'Orthogonality result unavailable.' : 'Clustering result unavailable.')).toBeInTheDocument();
    failing = false; fireEvent.click(screen.getByRole('button', { name: 'Refresh saved discovery results' })); await flush();
    expect(screen.queryByText(type === 'orthogonality' ? 'Orthogonality result unavailable.' : 'Clustering result unavailable.')).toBeNull();
    expect(screen.getByText('As of 2020-01-01')).toBeInTheDocument();
});
it.each([
    [ResearchRunPanel, research, { status: 'unavailable', reason: 'private SQL detail' }, 'Research status unavailable'],
    [() => <TickerRecommendations ticker="SPY" />, '/options/recommendations', failureContracts.at(-1).failed, 'Trade recommendations unavailable.'],
])('typed failure-to-read restores safe Retry (%s)', async (View, path, failed, title) => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes(path) ? failed : defaultFixture(url))));
    render(<View />); await flush();
    expect(screen.getByText(title)).toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Retry', exact: true })).toBeEnabled();
    expect(document.body.textContent).not.toContain('private SQL');
});
