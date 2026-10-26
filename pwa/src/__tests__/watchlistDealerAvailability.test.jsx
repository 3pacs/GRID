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
vi.mock('../components/PriceChart.jsx', () => ({ default: ({ data, onPeriodChange }) =>
    <div data-testid="price-chart">
        <span data-testid="chart-values">{data.map(row => row.value).join(',')}</span>
        <button onClick={() => onPeriodChange('1M')}>Switch period</button>
    </div> }));
vi.mock('../components/GEXProfile.jsx', () => ({
    default: ({ ticker, gexData }) => <div data-testid="gex-profile">gex:{ticker}:{gexData.gex_aggregate}</div>,
}));
vi.mock('../components/VannaCharmViz.jsx', () => ({
    default: ({ ticker, vannaCharmData }) => <div data-testid="vanna-charm">vanna:{ticker}:{vannaCharmData.vanna_exposure}</div>,
}));
vi.mock('../components/FlowTimeline.jsx', () => ({ default: () => null }));
vi.mock('../views/Options.jsx', () => ({ TickerRecommendations: () => null }));

const measuredGex = (ticker) => ({
    ticker, spot: 100, gex_aggregate: 0,
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

    it('distinguishes checked-empty GEX from missing vanna evidence', async () => {
        api.getGEXProfile.mockResolvedValue({ ticker: 'AAPL', profile: [], per_strike: [], spot: 100, gex_aggregate: 0 });
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL', vanna_exposure: null, charm_exposure: null });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);

        expect(await screen.findByText('No GEX profile data available.')).toBeInTheDocument();
        expect(screen.getByText('Vanna/charm unavailable.')).toBeInTheDocument();
        expect(screen.queryByTestId('gex-profile')).not.toBeInTheDocument();
    });

    it('treats absent GEX metrics as unavailable without affirmative empty evidence', async () => {
        api.getGEXProfile.mockResolvedValue({
            ticker: 'AAPL', profile: null, per_strike: null, spot: null, gex_aggregate: null,
        });
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL' });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        expect(await screen.findByText('GEX profile unavailable.')).toBeInTheDocument();
        expect(screen.getByText('Vanna/charm unavailable.')).toBeInTheDocument();
        expect(screen.queryByText('No GEX profile data available.')).not.toBeInTheDocument();
    });

    it.each([
        [{ profile: [{ spot: 100 }], per_strike: [] }, 'missing GEX ordinate'],
        [{ profile: [{ spot: 100, gex: Number.NaN }], per_strike: [] }, 'nonfinite GEX ordinate'],
        [{ profile: [], per_strike: [{ strike: null, net_gex: 0 }] }, 'malformed strike'],
    ])('classifies malformed nonempty GEX as unavailable: %s (%s)', async (rows) => {
        api.getGEXProfile.mockResolvedValue({ ticker: 'AAPL', spot: 100, gex_aggregate: 0, ...rows });
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL', vanna_exposure: 0, charm_exposure: 0 });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        expect(await screen.findByText('GEX profile unavailable.')).toBeInTheDocument();
        expect(screen.queryByTestId('gex-profile')).not.toBeInTheDocument();
    });

    it.each([
        { vanna_exposure: 0 },
        { vanna_exposure: null, charm_exposure: 0 },
        { vanna_exposure: Number.POSITIVE_INFINITY, charm_exposure: 0 },
    ])('requires both finite vanna/charm aggregates, including true zero: %s', async (payload) => {
        api.getGEXProfile.mockResolvedValue(measuredGex('AAPL'));
        api.getVannaCharm.mockResolvedValue({ ticker: 'AAPL', ...payload });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        expect(await screen.findByText('Vanna/charm unavailable.')).toBeInTheDocument();
        expect(screen.queryByTestId('vanna-charm')).not.toBeInTheDocument();
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

    it('ignores prior ticker core, overview, and edge success or failure callbacks', async () => {
        let rejectOldAnalysis;
        let resolveOldOverview;
        let resolveOldEdge;
        api.getTickerAnalysis.mockImplementation(ticker => ticker === 'AAPL'
            ? new Promise((_resolve, reject) => { rejectOldAnalysis = reject; })
            : Promise.resolve({ ticker, watchlist_item: { ticker, display_name: 'New ticker', asset_type: 'stock' },
                price_history: [], options: [], related_features: [], tradingview_signals: [] }));
        api.getTickerOverview.mockImplementation(ticker => ticker === 'AAPL'
            ? new Promise(resolve => { resolveOldOverview = resolve; })
            : Promise.resolve({ error: true }));
        api.getTickerEdge.mockImplementation(ticker => ticker === 'AAPL'
            ? new Promise(resolve => { resolveOldEdge = resolve; })
            : Promise.resolve({ status: 'unavailable' }));
        api.getGEXProfile.mockResolvedValue({ error: 'No options data' });
        api.getVannaCharm.mockResolvedValue({ error: 'No options data' });
        const { rerender } = render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        rerender(<WatchlistAnalysis ticker="MSFT" onBack={() => {}} />);
        expect(await screen.findByText(/New ticker/)).toBeInTheDocument();
        expect(await screen.findByText('INSIDER EDGE UNAVAILABLE')).toBeInTheDocument();
        await act(async () => {
            rejectOldAnalysis(new Error('Old ticker failure'));
            resolveOldOverview({ sentiment: 'bullish', bottom_line: 'Old overview' });
            resolveOldEdge({ status: 'available', edge_summary: 'Old edge', leads: [{ title: 'Old lead' }] });
        });
        expect(screen.queryByText('Old ticker failure')).not.toBeInTheDocument();
        expect(screen.queryByText('Old overview')).not.toBeInTheDocument();
        expect(screen.queryByText('Old edge')).not.toBeInTheDocument();
        expect(screen.getByText('INSIDER EDGE UNAVAILABLE')).toBeInTheDocument();
    });

    it('ignores a late period refresh from the prior ticker', async () => {
        let resolveOldPeriod;
        api.getTickerAnalysis.mockImplementation((ticker, period) => ticker === 'AAPL' && period === '1M'
            ? new Promise(resolve => { resolveOldPeriod = resolve; })
            : Promise.resolve({ ticker, watchlist_item: { ticker, display_name: ticker, asset_type: 'stock' },
                price_history: [{ value: ticker === 'AAPL' ? 100 : 200 }],
                options: [], related_features: [], tradingview_signals: [] }));
        api.getGEXProfile.mockResolvedValue({ error: 'No options data' });
        api.getVannaCharm.mockResolvedValue({ error: 'No options data' });
        const { rerender } = render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        await waitFor(() => expect(screen.getByTestId('chart-values')).toHaveTextContent('100'));
        await act(async () => { screen.getByText('Switch period').click(); });
        rerender(<WatchlistAnalysis ticker="MSFT" onBack={() => {}} />);
        await waitFor(() => expect(screen.getByTestId('chart-values')).toHaveTextContent('200'));
        await act(async () => { resolveOldPeriod({ price_history: [{ value: 999 }], period: '1M' }); });
        expect(screen.getByTestId('chart-values')).toHaveTextContent('200');
    });
});
