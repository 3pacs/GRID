/**
 * #959 / review 6033941920: overlapping Dashboard regime reads. Only the newest
 * read is applied, and a read never overrides data that arrived after it started.
 */
import React from 'react';
import { render, screen, waitFor, act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Dashboard from '../views/Dashboard.jsx';
import { api } from '../api.js';
import { useWebSocket } from '../hooks/useWebSocket.js';

const storeState = {
    currentRegime: null, systemStatus: null, setCurrentRegime: vi.fn(), setSystemStatus: vi.fn(),
    setLoading: vi.fn(), addNotification: vi.fn(), livePriceUpdates: {},
};

vi.mock('../api.js', () => ({
    api: {
        getCurrent: vi.fn(), getStatus: vi.fn(), getThesis: vi.fn(), getIntelDashboard: vi.fn(),
        getAggregatedFlows: vi.fn(), getWatchlistPrices: vi.fn(), refreshWatchlistPrices: vi.fn(),
        getWatchlistEnriched: vi.fn(), listFlowBriefings: vi.fn(), loadFlowBriefingAudio: vi.fn(),
        getPostmortemLessons: vi.fn(), generateFlowBriefing: vi.fn(),
    },
}));
vi.mock('../store.js', () => ({ default: vi.fn(() => storeState) }));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: vi.fn(() => ({ isMobile: false })) }));
vi.mock('../hooks/useWebSocket.js', () => ({ useWebSocket: vi.fn(() => ({ connected: false, prices: {} })) }));
vi.mock('../components/StatusDot.jsx', () => ({ default: () => <div data-testid="status-dot" /> }));
vi.mock('../components/DashboardFlows.jsx', () => ({ default: () => <div data-testid="dashboard-flows" /> }));

const PRIOR = { state: 'GROWTH', confidence: 0.7, as_of: '2026-11-22' };
const NEWER = { state: 'CONTRACTION', confidence: 0.6, as_of: '2026-11-23' };
const FAILED = { error: true, status: 503, message: 'down' };

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = PRIOR;
    storeState.setCurrentRegime.mockReset();
    storeState.setCurrentRegime.mockImplementation((r) => { storeState.currentRegime = r; });
    useWebSocket.mockReturnValue({ connected: false, prices: {} });
    api.getStatus.mockResolvedValue({ database: { connected: true } });
    api.getThesis.mockResolvedValue({});
    api.getIntelDashboard.mockResolvedValue({});
    api.getAggregatedFlows.mockResolvedValue({});
    api.getWatchlistPrices.mockResolvedValue({ prices: {} });
    api.getWatchlistEnriched.mockResolvedValue({ items: [] });
    api.listFlowBriefings.mockResolvedValue({ briefings: [] });
    api.getPostmortemLessons.mockResolvedValue({ lessons: [], generated_at: null });
});

/** Start the mount read and the reconnect read, both still in flight. */
async function twoReadsInFlight() {
    const first = deferred();
    const second = deferred();
    api.getCurrent.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
    const view = render(<Dashboard onNavigate={vi.fn()} />);
    useWebSocket.mockReturnValue({ connected: true, prices: {} });
    view.rerender(<Dashboard onNavigate={vi.fn()} />);
    await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(2));
    return { view, first, second };
}

describe('Dashboard overlapping regime reads', () => {
    it('an older success arriving after a newer failure neither stores nor clears LAST KNOWN', async () => {
        const { view, first, second } = await twoReadsInFlight();
        await act(async () => { second.resolve(FAILED); });
        await waitFor(() => expect(screen.getByTestId('regime-state')).toHaveTextContent('GROWTH · LAST KNOWN'));
        await act(async () => { first.resolve(NEWER); });
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        expect(storeState.setCurrentRegime).not.toHaveBeenCalled();
        expect(screen.getByTestId('regime-state')).toHaveTextContent('GROWTH · LAST KNOWN');
    });

    it('an older failure arriving after a newer success does not stale-mark the newer data', async () => {
        const { view, first, second } = await twoReadsInFlight();
        await act(async () => { second.resolve(NEWER); });
        await waitFor(() => expect(storeState.setCurrentRegime).toHaveBeenCalledWith(NEWER));
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        await act(async () => { first.resolve(FAILED); });
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        expect(screen.getByTestId('regime-state')).toHaveTextContent(/^CONTRACTION$/);
    });

    it('a read that fails after a websocket push replaced the regime does not mark the pushed data', async () => {
        const only = deferred();
        api.getCurrent.mockReturnValueOnce(only.promise);
        const view = render(<Dashboard onNavigate={vi.fn()} />);
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(1));
        storeState.currentRegime = NEWER; // the realtime store's push
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        await act(async () => { only.resolve(FAILED); });
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        expect(screen.getByTestId('regime-state')).toHaveTextContent(/^CONTRACTION$/);
    });
});
