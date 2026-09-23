import React from 'react';
import { act, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import WatchlistAnalysis from '../views/WatchlistAnalysis.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getTickerAnalysis: vi.fn(), getTickerOverview: vi.fn(), getTickerEdge: vi.fn(),
        getGEXProfile: vi.fn(), getVannaCharm: vi.fn(), getFlowTimeline: vi.fn(),
    },
}));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: () => ({ isMobile: false }) }));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => <div data-testid="price-chart" /> }));
vi.mock('../components/GEXProfile.jsx', () => ({
    default: ({ ticker, gexData }) => <div data-testid="gex-profile">gex:{ticker}:{gexData.gex_aggregate}</div>,
}));
vi.mock('../components/VannaCharmViz.jsx', () => ({
    default: ({ ticker, vannaCharmData }) => <div data-testid="vanna-charm">vanna:{ticker}:{vannaCharmData.vanna_exposure}</div>,
}));
vi.mock('../components/FlowTimeline.jsx', () => ({ default: () => null }));
vi.mock('../views/Options.jsx', () => ({ TickerRecommendations: () => null }));

const measuredGex = (ticker) => ({
    ticker, gex_aggregate: 0,
    profile: [{ spot: 100, gex: 0 }],
    per_strike: [{ strike: 100, net_gex: 0 }],
});

beforeEach(() => {
    vi.clearAllMocks();
    api.getTickerAnalysis.mockImplementation(async (ticker) => ({
        ticker, watchlist_item: { ticker, display_name: ticker, asset_type: 'stock' },
        price_history: [], options: [], related_features: [], tradingview_signals: [],
    }));
    api.getTickerOverview.mockResolvedValue({ error: true, status: 503 });
    api.getTickerEdge.mockResolvedValue({ error: true });
    api.getFlowTimeline.mockResolvedValue({ error: 'No usable GEX history is available' });
});

describe('Watchlist dealer availability', () => {
    it('shows explicit GEX and vanna/charm unavailable states for HTTP 200 error envelopes', async () => {
        api.getGEXProfile.mockResolvedValue({ error: 'No options data for AAPL', ticker: 'AAPL' });
        api.getVannaCharm.mockResolvedValue({ error: 'No options data for AAPL', ticker: 'AAPL' });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);

        expect(await screen.findByText('GEX profile unavailable.')).toBeInTheDocument();
        expect(screen.getByText('Vanna/charm unavailable.')).toBeInTheDocument();
        expect(screen.getByTestId('price-chart')).toBeInTheDocument();
        expect(screen.queryByTestId('gex-profile')).not.toBeInTheDocument();
        expect(screen.queryByTestId('vanna-charm')).not.toBeInTheDocument();
        expect(screen.queryByText('No options data for AAPL')).not.toBeInTheDocument();
    });

    it('distinguishes checked-empty dealer payloads from unavailable errors', async () => {
        api.getGEXProfile.mockResolvedValue({ ticker: 'AAPL', profile: [], per_strike: [], gex_aggregate: 0 });
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL', vanna_exposure: null, charm_exposure: null });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);

        expect(await screen.findByText('No GEX profile data available.')).toBeInTheDocument();
        expect(screen.getByText('No vanna/charm data available.')).toBeInTheDocument();
        expect(screen.queryByTestId('gex-profile')).not.toBeInTheDocument();
    });

    it('retains genuine measured zeros as populated dealer panels', async () => {
        api.getGEXProfile.mockResolvedValue(measuredGex('AAPL'));
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL', vanna_exposure: 0, charm_exposure: 0 });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);

        expect(await screen.findByTestId('gex-profile')).toHaveTextContent('gex:AAPL:0');
        expect(screen.getByTestId('vanna-charm')).toHaveTextContent('vanna:AAPL:0');
        expect(screen.queryByText('GEX profile unavailable.')).not.toBeInTheDocument();
        expect(screen.queryByText('Vanna/charm unavailable.')).not.toBeInTheDocument();
    });

    it('ignores an old ticker dealer response after navigation', async () => {
        let resolveOldGex;
        const oldGex = new Promise((resolve) => { resolveOldGex = resolve; });
        api.getGEXProfile.mockImplementation((ticker) => ticker === 'AAPL'
            ? oldGex : Promise.resolve({ error: 'No options data for MSFT', ticker }));
        api.getVannaCharm.mockImplementation(async (ticker) => ticker === 'AAPL'
            ? { ticker, vanna_exposure: 0, charm_exposure: 0 }
            : { error: 'No options data for MSFT', ticker });

        const { rerender } = render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        rerender(<WatchlistAnalysis ticker="MSFT" onBack={() => {}} />);
        expect(await screen.findByText('GEX profile unavailable.')).toBeInTheDocument();
        expect(screen.getByText('Vanna/charm unavailable.')).toBeInTheDocument();

        await act(async () => { resolveOldGex(measuredGex('AAPL')); });
        await waitFor(() => expect(screen.queryByTestId('gex-profile')).not.toBeInTheDocument());
        expect(screen.queryByTestId('vanna-charm')).not.toBeInTheDocument();
    });
});
