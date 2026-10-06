// Fresh reviewer-only contract probes. No candidate source changes or live IO.
import React from 'react';
import { act, cleanup, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Discovery from '../views/Discovery.jsx';
import useStore from '../store.js';

const ok = data => ({ ok: true, status: 200, json: async () => data });
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
const auditPath = type => `/api/v1/discovery/results/${type}`;
const researchPath = '/api/v1/snapshots/research/latest';
const fixture = url => {
    if (url.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (url.includes('/hypotheses/results')) return { results: [], count: 0 };
    if (url.includes('/hypotheses')) return { hypotheses: [] };
    if (url.includes('/discovery/jobs')) return { jobs: [] };
    if (url.includes('/discovery/results/')) return { result: null };
    return {};
};
beforeEach(() => {
    vi.useFakeTimers(); api.clearCache(); useStore.setState({ jobs: [], hypotheses: [] });
    window.matchMedia = vi.fn(() => ({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() }));
    global.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
    global.fetch = vi.fn(url => Promise.resolve(ok(fixture(url))));
});
afterEach(() => { cleanup(); api.clearCache(); vi.useRealTimers(); vi.restoreAllMocks(); });

it.each(['orthogonality', 'clustering'])('actual %s snapshot-read failure is unavailable, rather than checked-empty', async type => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes(auditPath(type))
        ? { result: null, message: `${type} snapshot unavailable` } : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText(type === 'orthogonality' ? 'Orthogonality result unavailable.' : 'Clustering result unavailable.')).toBeInTheDocument();
    expect(screen.queryByText(type === 'orthogonality' ? 'No completed orthogonality audit found.' : 'No completed clustering run found.')).toBeNull();
});

it.each(['orthogonality', 'clustering'])('actual %s unavailable sentinel cannot cache or block ordinary recovered reads', async type => {
    const path = auditPath(type);
    global.fetch.mockResolvedValueOnce(ok({ result: null, message: `${type} snapshot unavailable` }))
        .mockResolvedValue(ok({ result: { true_dimensionality: 3 }, source: 'analytical_snapshots' }));
    const failed = await api.get(path);
    expect(failed.error).toBe(true);
    expect(api.getResponseInfo(failed)).toBeNull();
    expect(api._cache.has(path)).toBe(false);
    expect((await api.get(path)).result.true_dimensionality).toBe(3);
    expect(global.fetch).toHaveBeenCalledTimes(2);
});

it('genuine checked-empty audit sentinels remain visible and cacheable', async () => {
    global.fetch.mockImplementation(url => Promise.resolve(ok(url.includes('/discovery/results/')
        ? { result: null, message: url.includes('orthogonality') ? 'No completed orthogonality audit found' : 'No completed clustering run found' }
        : fixture(url))));
    render(<Discovery />); await flush();
    expect(screen.getByText('No completed orthogonality audit found.')).toBeInTheDocument();
    expect(screen.getByText('No completed clustering run found.')).toBeInTheDocument();
    expect(api._cache.has(auditPath('orthogonality'))).toBe(true);
});

it.each([
    [{ status: 'failed', error: null }],
    [{ status: 'unknown_lifecycle', run_id: 'record', error: null }],
    [{ status: 'failed', run_id: 123, error: '' }],
])('nullable diagnostic exception requires the documented record identity/status: %j', async record => {
    global.fetch.mockResolvedValueOnce(ok(record)).mockResolvedValue(ok({ status: 'failed', run_id: 'valid-record', error: '' }));
    const failed = await api.get(researchPath);
    expect(failed.error).toBe(true);
    expect(api._cache.has(researchPath)).toBe(false);
    const recovered = await api.get(researchPath);
    expect(recovered.run_id).toBe('valid-record');
    expect(global.fetch).toHaveBeenCalledTimes(2);
});

it('a valid offline diagnostic record is persisted data on the exact research route with query parameters', async () => {
    const record = { status: 'failed', run_id: 'persisted-run', error: 'offline', phase: 'validation' };
    global.fetch.mockResolvedValue(ok(record));
    const path = `${researchPath}?fixture=1`;
    const first = await api.get(path);
    expect(first).toEqual(record);
    await vi.advanceTimersByTimeAsync(500);
    expect(await api.get(path)).toBe(first);
    expect(api.getResponseInfo(first).ageMs).toBe(500);
    expect(global.fetch).toHaveBeenCalledTimes(1);
});

it.each(['/api/v1/godview/latest', '/api/v1/discovery/hypotheses/results', '/api/v1/snapshots/research/latest/'])('the persisted-record diagnostic exemption does not broaden to %s', async path => {
    global.fetch.mockResolvedValue(ok({ status: 'failed', run_id: 'record', error: null, count: 0, pillars: {} }));
    const failed = await api.get(path);
    expect(failed).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(path)).toBe(false);
});

it('nested non-envelope metadata is retained while own result diagnostics are refused', async () => {
    const path = auditPath('orthogonality');
    const successful = { result: { true_dimensionality: 3, metadata: { error: 'historical metadata' } } };
    global.fetch.mockResolvedValueOnce(ok(successful));
    expect(await api.get(path)).toEqual(successful);
    api.clearCache();
    global.fetch.mockResolvedValueOnce(ok({ result: { true_dimensionality: 3, error: [] } }));
    expect(await api.get(path)).toMatchObject({ error: true, code: 'invalid_response' });
    expect(api._cache.has(path)).toBe(false);
});

it('R1b empty diagnostic cannot cache; an ordinary read recovers to genuine zero with original receipt age', async () => {
    const path = '/api/v1/discovery/hypotheses/results';
    global.fetch.mockResolvedValueOnce(ok({ results: [], count: 0, error: '' })).mockResolvedValue(ok({ results: [], count: 0 }));
    const failed = await api.get(path);
    expect(failed.error).toBe(true); expect(api.getResponseInfo(failed)).toBeNull(); expect(api._cache.has(path)).toBe(false);
    const recovered = await api.get(path); expect(recovered.count).toBe(0);
    await vi.advanceTimersByTimeAsync(999);
    expect(await api.get(path)).toBe(recovered); expect(api.getResponseInfo(recovered).ageMs).toBe(999);
    expect(global.fetch).toHaveBeenCalledTimes(2);
});
