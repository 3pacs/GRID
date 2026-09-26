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

    it('renders persisted price fields, risk/reward, and keyed sanity status', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'persisted' },
            recommendations: [{ ticker: 'AAPL', direction: 'CALL', strike: 200,
                entry_price: 10, target_price: 12, stop_loss: 8,
                sanity_status: {
                    DATA_QUALITY: { status: 'PASS' }, DEALER_FLOW: { status: 'SKIP' },
                    CROSS_ASSET: { status: 'PASS' }, LLM_REVIEW: { status: 'SKIP' },
                    HISTORICAL_ANALOG: { status: 'PASS' },
                },
            }],
        });
        render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByText('$10.00')).toBeInTheDocument();
        expect(screen.getByText('$12.00')).toBeInTheDocument();
        expect(screen.getByText('$8.00')).toBeInTheDocument();
        expect(screen.getByText('ENTRY')).toBeInTheDocument();
        expect(screen.getByText('STOP')).toBeInTheDocument();
        expect(screen.getByTitle('Data quality: PASS')).toBeInTheDocument();
        expect(screen.getByTitle('Dealer flow: SKIP')).toBeInTheDocument();
    });

    it('distinguishes checked empty from unavailable', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'missing', data_status: 'missing' }, recommendations: [],
        });
        const view = render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByText('No saved trade recommendations for AAPL. No fresh scan was run.')).toBeInTheDocument();
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'unavailable' }, recommendations: [],
        });
        view.rerender(<TickerRecommendations ticker="MSFT" />);
        expect(await screen.findByText('Trade recommendations unavailable.')).toBeInTheDocument();
        expect(screen.queryByText('No saved trade recommendations for MSFT. No fresh scan was run.')).not.toBeInTheDocument();
    });

    it('labels old saved rows as stale without hiding the recommendation', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            generated_at: '2026-09-15T13:30:00+00:00',
            scan_summary: { source: 'persisted', data_status: 'stale', fresh_scan: false },
            recommendations: [{ ticker: 'AAPL', direction: 'CALL', strike: 200,
                confidence: 0.5, thesis: 'Old saved thesis' }],
        });
        render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByText('Old saved thesis')).toBeInTheDocument();
        expect(screen.getByText(/Saved recommendations are stale.*2026-09-15 UTC.*No fresh scan was run/)).toBeInTheDocument();
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
