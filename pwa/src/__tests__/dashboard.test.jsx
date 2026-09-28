import React from 'react';
import { render, screen, waitFor, fireEvent, within } from '@testing-library/react';
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
        loadFlowBriefingAudio: vi.fn(async (name) => `blob:/audio/${name}`),
        getPostmortemLessons: vi.fn(),
        generateFlowBriefing: vi.fn(),
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
        api.loadFlowBriefingAudio.mockClear();

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
        api.generateFlowBriefing.mockReset();
    });

    it('uses cached watchlist prices on mount and avoids the refresh endpoint', async () => {
        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(api.getWatchlistPrices).toHaveBeenCalledTimes(1);
        });

        expect(api.refreshWatchlistPrices).not.toHaveBeenCalled();
    });

    it('labels the PULSE row with the live price source (Wave 3 #11)', async () => {
        api.getWatchlistPrices.mockResolvedValue({
            prices: { SPY: { price: 500, pct_1d: 0.01, source: 'yfinance', updated_at: '2026-09-28T12:00:00Z' } },
            fresh: true, cached: true, source: 'yfinance',
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        await waitFor(() => {
            expect(screen.getByText('PULSE')).toBeInTheDocument();
        });
        const pulseLabel = screen.getByText('PULSE');
        expect(pulseLabel.title).toMatch(/yfinance/i);
    });

    it('renders a text-only on-demand briefing when no audio was made (paid TTS disabled)', async () => {
        // Wave 3 W3.4 (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927.md #5): with
        // GRID_ALLOW_PAID_LLM unset, audio_briefing.py returns script_text
        // with no audio_path. The Dashboard button must show that text
        // instead of silently reporting a failure.
        api.generateFlowBriefing.mockResolvedValue({
            status: 'SUCCESS',
            briefing: {
                script_text: 'Good morning. GRID Intelligence briefing for today.',
                audio_path: null,
                provider: 'local',
                briefing_date: '2026-09-28',
                generated_at: '2026-09-28T12:00:00Z',
            },
        });

        render(<Dashboard onNavigate={vi.fn()} />);

        const btn = await screen.findByRole('button', { name: 'Generate on request' });
        fireEvent.click(btn);

        await waitFor(() => {
            expect(screen.getByTestId('text-briefing')).toBeInTheDocument();
        });
        const textBriefing = screen.getByTestId('text-briefing');
        expect(within(textBriefing).getByText(/GRID Intelligence briefing for today/)).toBeInTheDocument();
        expect(within(textBriefing).getByText(/local/)).toBeInTheDocument();
    });
});
