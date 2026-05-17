import React from 'react';
import { render, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Dashboard from '../views/Dashboard.jsx';
import { api } from '../api.js';

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
        getCurrent: vi.fn(),
        getStatus: vi.fn(),
        getThesis: vi.fn(),
        getIntelDashboard: vi.fn(),
        getAggregatedFlows: vi.fn(),
        getWatchlistPrices: vi.fn(),
        refreshWatchlistPrices: vi.fn(),
        getWatchlistEnriched: vi.fn(),
        listFlowBriefings: vi.fn(),
        getFlowBriefingAudioUrl: vi.fn((name) => `/audio/${name}`),
        getPostmortemLessons: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

vi.mock('../hooks/useDevice.js', () => ({
    useDevice: vi.fn(() => ({ isMobile: false })),
}));

vi.mock('../hooks/useWebSocket.js', () => ({
    useWebSocket: vi.fn(() => ({ connected: false, prices: {} })),
}));

vi.mock('../components/StatusDot.jsx', () => ({
    default: () => <div data-testid="status-dot" />,
}));

vi.mock('../components/DashboardFlows.jsx', () => ({
    default: () => <div data-testid="dashboard-flows" />,
}));

describe('Dashboard watchlist loading', () => {
    beforeEach(() => {
        storeState.currentRegime = null;
        storeState.systemStatus = null;
        storeState.livePriceUpdates = {};
        storeState.setCurrentRegime.mockReset();
        storeState.setSystemStatus.mockReset();
        storeState.setLoading.mockReset();
        storeState.addNotification.mockReset();

        api.getCurrent.mockReset();
        api.getStatus.mockReset();
        api.getThesis.mockReset();
        api.getIntelDashboard.mockReset();
        api.getAggregatedFlows.mockReset();
        api.getWatchlistPrices.mockReset();
        api.refreshWatchlistPrices.mockReset();
        api.getWatchlistEnriched.mockReset();
        api.listFlowBriefings.mockReset();
        api.getFlowBriefingAudioUrl.mockClear();

        api.getCurrent.mockResolvedValue({ state: 'NEUTRAL', confidence: 0.5 });
        api.getStatus.mockResolvedValue({ database: { connected: true } });
        api.getThesis.mockResolvedValue({});
        api.getIntelDashboard.mockResolvedValue({});
        api.getAggregatedFlows.mockResolvedValue({});
        api.getWatchlistPrices.mockResolvedValue({ prices: { SPY: { price: 500, pct_1d: 0.01 } }, fresh: true, cached: true });
        api.refreshWatchlistPrices.mockResolvedValue({ prices: { SPY: { price: 501, pct_1d: 0.02 } } });
        api.getWatchlistEnriched.mockResolvedValue({ items: [] });
        api.listFlowBriefings.mockResolvedValue({ briefings: [] });
        api.getPostmortemLessons.mockReset();
        api.getPostmortemLessons.mockResolvedValue({ lessons: [], generated_at: null });
    });

    it('uses cached watchlist prices on mount and avoids the refresh endpoint', async () => {
        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(api.getWatchlistPrices).toHaveBeenCalledTimes(1);
        });

        expect(api.refreshWatchlistPrices).not.toHaveBeenCalled();
    });
});
