import React from 'react';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { TickerRecommendations } from '../views/Options.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { getOptionsRecommendations: vi.fn() } }));

describe('Watchlist ticker recommendations', () => {
    beforeEach(() => api.getOptionsRecommendations.mockReset());

    it('shows a saved recommendation and keeps measured zero', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'persisted' },
            recommendations: [{ ticker: 'AAPL', direction: 'CALL', strike: 200,
                confidence: 0, expected_return: 0, thesis: 'Saved thesis' }],
        });
        render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByText('Saved thesis')).toBeInTheDocument();
        expect(screen.getByText('TRADE RECOMMENDATIONS · 1')).toBeInTheDocument();
        expect(screen.getByText('+0.0% exp')).toBeInTheDocument();
    });

    it('distinguishes checked empty from unavailable', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'persisted' }, recommendations: [],
        });
        const view = render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByText('No active trade recommendations for AAPL.')).toBeInTheDocument();
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'unavailable' }, recommendations: [],
        });
        view.rerender(<TickerRecommendations ticker="MSFT" />);
        expect(await screen.findByText('Trade recommendations unavailable.')).toBeInTheDocument();
        expect(screen.queryByText('No active trade recommendations for MSFT.')).not.toBeInTheDocument();
    });

    it('shows unavailable for an error envelope and ignores a late prior ticker', async () => {
        let resolveAapl;
        api.getOptionsRecommendations.mockImplementation(ticker => ticker === 'AAPL'
            ? new Promise(resolve => { resolveAapl = resolve; })
            : Promise.resolve({ error: 'provider failed' }));
        const view = render(<TickerRecommendations ticker="AAPL" />);
        view.rerender(<TickerRecommendations ticker="MSFT" />);
        expect(await screen.findByText('Trade recommendations unavailable.')).toBeInTheDocument();
        resolveAapl({ scan_summary: { source: 'persisted' }, recommendations: [
            { ticker: 'AAPL', strike: 200, confidence: 0.8, thesis: 'Stale thesis' },
        ] });
        expect(screen.queryByText('Stale thesis')).not.toBeInTheDocument();
    });
});
