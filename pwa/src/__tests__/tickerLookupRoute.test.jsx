import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import TickerLookup from '../views/TickerLookup.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getDadTickerGold: vi.fn(),
        streamDadTickerGold: vi.fn(),
    },
}));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => <div data-testid="price-chart" /> }));

describe('TickerLookup route ticker', () => {
    beforeEach(() => {
        api.getDadTickerGold.mockReset().mockImplementation(async (ticker) => ({ ticker, grid_data: {}, signals: {} }));
        api.streamDadTickerGold.mockReset().mockReturnValue({ close: vi.fn() });
    });

    it('looks up the ticker in the hash query after applying the existing ticker cleanup', async () => {
        window.location.hash = '#/ticker-lookup?ticker=aapl';
        render(<TickerLookup />);

        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenCalledWith('AAPL', { refreshFinviz: false }));
        expect(screen.getByRole('heading', { name: 'AAPL' })).toBeInTheDocument();
        expect(api.streamDadTickerGold).toHaveBeenCalledWith('AAPL', expect.objectContaining({ refreshFinviz: false }));
    });

    it.each(['#/ticker-lookup', '#/ticker-lookup?ticker=***'])('keeps the RXT default when the route has no usable ticker (%s)', async (hash) => {
        window.location.hash = hash;
        render(<TickerLookup />);

        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenCalledWith('RXT', { refreshFinviz: false }));
        expect(screen.getByRole('heading', { name: 'RXT' })).toBeInTheDocument();
    });

    it('reacts to hash navigation while the view remains mounted, including returning to an earlier ticker', async () => {
        window.location.hash = '#/ticker-lookup?ticker=AAPL';
        render(<TickerLookup />);
        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenCalledWith('AAPL', { refreshFinviz: false }));

        window.location.hash = '#/ticker-lookup?ticker=MSFT';
        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenLastCalledWith('MSFT', { refreshFinviz: false }));
        expect(screen.getByRole('heading', { name: 'MSFT' })).toBeInTheDocument();

        window.location.hash = '#/ticker-lookup?ticker=AAPL';
        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenLastCalledWith('AAPL', { refreshFinviz: false }));
        expect(screen.getByRole('heading', { name: 'AAPL' })).toBeInTheDocument();
    });

    it('uses the submitted ticker for subsequent in-view searches', async () => {
        window.location.hash = '#/ticker-lookup?ticker=AAPL';
        render(<TickerLookup />);
        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenCalledWith('AAPL', { refreshFinviz: false }));

        fireEvent.change(screen.getByRole('textbox', { name: 'Ticker' }), { target: { value: 'msft' } });
        fireEvent.click(screen.getByRole('button', { name: 'Get Gold' }));

        await waitFor(() => expect(api.getDadTickerGold).toHaveBeenLastCalledWith('MSFT', { refreshFinviz: false }));
        expect(screen.getByRole('heading', { name: 'MSFT' })).toBeInTheDocument();
    });
});
