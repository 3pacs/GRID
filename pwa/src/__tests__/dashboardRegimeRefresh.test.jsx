/**
 * #959 / review 6033471175: a failed regime refresh after a good read must not
 * leave the old regime looking current. The shared store keeps the last good
 * read, so the header labels it LAST KNOWN until a newer read succeeds.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Dashboard from '../views/Dashboard.jsx';
import { api } from '../api.js';
import { useWebSocket } from '../hooks/useWebSocket.js';

const storeState = {
    currentRegime: null,
    systemStatus: null,
    setCurrentRegime: vi.fn(),
    setSystemStatus: vi.fn(),
    setLoading: vi.fn(),
    addNotification: vi.fn(),
    livePriceUpdates: {},
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

const GOOD = { state: 'GROWTH', confidence: 0.7, as_of: '2026-11-23' };

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = null;
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

/** Mount with a good read, then trigger the reconnect refresh, which returns `second`. */
async function goodThenRefresh(second) {
    api.getCurrent.mockResolvedValueOnce(GOOD).mockResolvedValueOnce(second);
    const view = render(<Dashboard onNavigate={vi.fn()} />);
    await waitFor(() => expect(storeState.setCurrentRegime).toHaveBeenCalledWith(GOOD));
    view.rerender(<Dashboard onNavigate={vi.fn()} />);
    expect(screen.getByTestId('regime-state')).toHaveTextContent(/^GROWTH$/);

    useWebSocket.mockReturnValue({ connected: true, prices: {} });
    view.rerender(<Dashboard onNavigate={vi.fn()} />);
    await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(2));
    return view;
}

describe('Dashboard regime header after a refresh', () => {
    it.each([
        ['a 503', { error: true, status: 503, message: 'down' }],
        ['a null', null],
    ])('labels the old regime LAST KNOWN when the refresh returns %s', async (_name, second) => {
        await goodThenRefresh(second);
        await waitFor(() => expect(screen.getByTestId('regime-state')).toHaveTextContent('GROWTH · LAST KNOWN'));
        expect(screen.getByTestId('regime-state').title).toMatch(/Latest regime read failed .*as of 2026-11-23/);
        // The stale confidence is not shown as current.
        expect(screen.queryByText('70%')).not.toBeInTheDocument();
        expect(storeState.setCurrentRegime).toHaveBeenCalledTimes(1);
    });

    it('control: a successful refresh keeps the header current, with no LAST KNOWN mark', async () => {
        const view = await goodThenRefresh({ state: 'GROWTH', confidence: 0.72, as_of: '2026-11-24' });
        await waitFor(() => expect(storeState.setCurrentRegime).toHaveBeenCalledTimes(2));
        view.rerender(<Dashboard onNavigate={vi.fn()} />);
        expect(screen.getByTestId('regime-state')).toHaveTextContent(/^GROWTH$/);
        expect(screen.getByText('72%')).toBeInTheDocument();
    });
});
