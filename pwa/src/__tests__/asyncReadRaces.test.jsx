import React from 'react';
import { act, cleanup, fireEvent, render, screen } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import { useAsyncData } from '../hooks/useAsyncData.js';
import useGodViewStore from '../stores/godViewStore.js';
import { api } from '../api.js';

const deferred = () => { let resolve; const promise = new Promise(r => { resolve = r; }); return { promise, resolve }; };
const flush = () => act(() => vi.advanceTimersByTimeAsync(0));
function Harness({ fetcher, dependency = 'a' }) {
    const { data, loading, error, refetch, stale } = useAsyncData(fetcher,
        { deps: [dependency], timeoutMs: 15_000, safeErrors: true });
    return <><span data-testid="data">{JSON.stringify(data)}</span><span data-testid="loading">{String(loading)}</span>
        <span data-testid="error">{error?.message}</span><span data-testid="stale">{String(stale)}</span><button onClick={refetch}>Retry</button></>;
}
beforeEach(() => { vi.useFakeTimers(); api.clearCache(); useGodViewStore.getState().reset(); });
afterEach(() => { cleanup(); useGodViewStore.getState().reset(); vi.useRealTimers(); vi.restoreAllMocks(); });

it('repeated clicks cancel the old scope, and a late response/finally cannot replace newer data or loading', async () => {
    const first = deferred();
    const second = deferred();
    const third = deferred();
    const scopes = [];
    const fetcher = vi.fn(scope => { scopes.push(scope); return [first, second, third][scopes.length - 1].promise; });
    render(<Harness fetcher={fetcher} />);
    fireEvent.click(screen.getByText('Retry'));
    expect(scopes[0].signal.aborted).toBe(true);
    first.resolve({ version: 1 });
    await flush();
    expect(screen.getByTestId('loading').textContent).toBe('true');
    fireEvent.click(screen.getByText('Retry'));
    expect(scopes[1].signal.aborted).toBe(true);
    third.resolve({ version: 3 });
    await flush();
    second.resolve({ version: 2 });
    await flush();
    expect(screen.getByTestId('data').textContent).toBe('{"version":3}');
    expect(screen.getByTestId('loading').textContent).toBe('false');
});

it('dependency change and unmount cancel the right scopes despite an uncooperative callback', async () => {
    const scopes = [];
    const fetcher = vi.fn(scope => { scopes.push(scope); return new Promise(() => {}); });
    const view = render(<Harness fetcher={fetcher} />);
    view.rerender(<Harness fetcher={fetcher} dependency="b" />);
    expect(scopes[0].signal.aborted).toBe(true);
    expect(scopes[1].signal.aborted).toBe(false);
    view.unmount();
    expect(scopes[1].signal.aborted).toBe(true);
    await flush();
});

it('a failed refresh retains the prior response with a stale flag, never fresh data', async () => {
    const fetcher = vi.fn().mockResolvedValueOnce({ as_of: '2020-01-01', value: 0 }).mockImplementationOnce(() => new Promise(() => {}));
    render(<Harness fetcher={fetcher} />);
    await flush();
    fireEvent.click(screen.getByText('Retry'));
    await act(() => vi.advanceTimersByTimeAsync(15_000));
    expect(screen.getByTestId('data').textContent).toBe('{"as_of":"2020-01-01","value":0}');
    expect(screen.getByTestId('stale').textContent).toBe('true');
    expect(screen.getByTestId('error').textContent).toMatch(/timed out/);
    expect(screen.getByTestId('loading').textContent).toBe('false');
});

it('God View reset fences a pending completion and repeated calls receive independent force requests', async () => {
    const first = deferred();
    const second = deferred();
    const getLatest = vi.spyOn(api, 'getGodViewLatest').mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const old = useGodViewStore.getState().loadLatest();
    const newest = useGodViewStore.getState().loadLatest();
    expect(getLatest.mock.calls[0][0].signal.aborted).toBe(true);
    expect(getLatest.mock.calls[1][0].gridForce).toBe(true);
    second.resolve({ pillars: {}, as_of: 'new' });
    await newest;
    first.resolve({ pillars: {}, as_of: 'old' });
    await old;
    expect(useGodViewStore.getState().latest.as_of).toBe('new');
    const third = deferred();
    getLatest.mockReturnValueOnce(third.promise);
    const pending = useGodViewStore.getState().loadLatest();
    useGodViewStore.getState().reset();
    third.resolve({ pillars: {}, as_of: 'disposed' });
    await pending;
    expect(useGodViewStore.getState().latest).toBeNull();
    expect(useGodViewStore.getState().loading).toBe(false);
});
