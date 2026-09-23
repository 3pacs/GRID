import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import FlowTimeline from '../components/FlowTimeline.jsx';
import WatchlistAnalysis from '../views/WatchlistAnalysis.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getTickerAnalysis: vi.fn(),
        getTickerOverview: vi.fn(),
        getTickerEdge: vi.fn(),
        getGEXProfile: vi.fn(),
        getVannaCharm: vi.fn(),
        getFlowTimeline: vi.fn(),
    },
}));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: () => ({ isMobile: false }) }));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => <div data-testid="price-chart" /> }));
vi.mock('../views/Options.jsx', () => ({ TickerRecommendations: () => null }));

const measured = { date: '2026-09-23', net_gex: 0, regime: 'neutral', spot: 100 };
const timeline = (extra = {}) => ({
    ticker: 'AAPL', days: 90, history: [measured], opex_calendar: [],
    catalysts: [], gamma_flip_crossings: [], ...extra,
});

beforeAll(() => {
    vi.stubGlobal('ResizeObserver', class {
        observe() {}
        disconnect() {}
    });
    SVGElement.prototype.getTotalLength = () => 100;
});

describe('Watchlist flow availability', () => {
    it('shows partial dates without losing a measured zero', () => {
        render(<FlowTimeline ticker="AAPL" timelineData={timeline({ history_status: 'partial', failed_dates: 1 })} />);
        expect(screen.getByText('Some GEX dates are unavailable; only measured dates are shown.')).toBeInTheDocument();
        expect(screen.getByText('$0')).toBeInTheDocument();
    });

    it('distinguishes failed dated calculations from an unavailable stored-history query', () => {
        const { rerender } = render(<FlowTimeline ticker="AAPL" timelineData={timeline({ history_status: 'fallback', failed_dates: 2 })} />);
        expect(screen.getByText('Showing one latest GEX snapshot; 2 dated GEX calculations failed.')).toBeInTheDocument();
        rerender(<FlowTimeline ticker="AAPL" timelineData={timeline({ history_status: 'fallback', failed_dates: 0 })} />);
        expect(screen.getByText('Showing one latest GEX snapshot, not a measured daily timeline.')).toBeInTheDocument();
    });

    it('renders unavailable history as an error instead of a zero-GEX chart', () => {
        render(<FlowTimeline ticker="AAPL" timelineData={timeline({
            history: [], history_status: 'unavailable', error: 'No usable GEX history is available',
        })} />);
        expect(screen.getByText('No usable GEX history is available')).toBeInTheDocument();
        expect(screen.queryByText('$0')).not.toBeInTheDocument();
    });
});

describe('Watchlist overview availability', () => {
    beforeEach(() => {
        vi.clearAllMocks();
        api.getTickerAnalysis.mockResolvedValue({
            ticker: 'AAPL', watchlist_item: { ticker: 'AAPL', display_name: 'Apple', asset_type: 'stock' },
            price_history: [{ date: '2026-09-23', value: 100 }], options: [],
            related_features: [], tradingview_signals: [],
        });
        api.getTickerEdge.mockResolvedValue({ error: true });
        api.getGEXProfile.mockResolvedValue({ error: 'No options data' });
        api.getVannaCharm.mockResolvedValue({ error: 'No options data' });
        api.getFlowTimeline.mockResolvedValue({ error: 'No usable GEX history is available' });
    });

    it('shows a failed overview while keeping measured analysis visible', async () => {
        api.getTickerOverview.mockResolvedValue({ error: true, status: 503, message: 'Service unavailable' });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        expect(await screen.findByRole('status')).toHaveTextContent('AI overview unavailable. Other Watchlist data may still be available.');
        await waitFor(() => expect(screen.getByTestId('price-chart')).toBeInTheDocument());
        expect(api.getTickerOverview).toHaveBeenCalledTimes(1);
    });

    it('renders a successful overview without an unavailable notice', async () => {
        api.getTickerOverview.mockResolvedValue({ sentiment: 'neutral', overview: 'Measured context' });
        render(<WatchlistAnalysis ticker="AAPL" onBack={() => {}} />);
        expect(await screen.findByText('Measured context')).toBeInTheDocument();
        expect(screen.queryByText(/AI overview unavailable/)).not.toBeInTheDocument();
    });
});
