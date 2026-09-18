import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi, beforeEach } from 'vitest';
import Portfolio from '../views/Portfolio.jsx';
import { api } from '../api.js';

// useDevice() reads matchMedia at mount; jsdom does not implement it.
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({
        matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn(),
    });
}

vi.mock('../api.js', () => ({
    api: {
        getPortfolio: vi.fn(),
    },
}));

/**
 * Batch-1 fake-data audit (C-H1, C-H2, C-M15, C-M16).
 *
 * GET /api/v1/watchlist/portfolio no longer serves a fabricated
 * total_value / total_pnl_1d / total_pnl_1m built from a hardcoded
 * ESTIMATED_PORTFOLIO = 125_000. This view must therefore render an em dash
 * for the unknown dollar value rather than "$0", while still showing the
 * percentage returns, which are genuinely measured.
 */

const DASH = '—';

const honestPayload = (overrides = {}) => ({
    total_value: null,
    total_value_basis: 'no_position_sizes_stored',
    weighted_return_1d_pct: 0.0234,
    return_1d_weight_coverage: 1.0,
    positions: [
        {
            ticker: 'AAPL', display_name: 'Apple', price: 200,
            change_1d: 0.0123, change_1w: 0.02, weight: 0.5,
            sector: 'Technology', asset_type: 'stock',
        },
        {
            ticker: 'SPY', display_name: 'S&P 500 ETF', price: 500,
            change_1d: 0.0345, change_1w: 0.01, weight: 0.5,
            sector: 'ETF', asset_type: 'etf',
        },
    ],
    positions_missing_price: 0,
    missing_price_tickers: [],
    weight_priced_total: 1.0,
    allocation: {
        by_sector: { Technology: 0.5, ETF: 0.5 },
        by_asset_type: { stock: 0.5, etf: 0.5 },
    },
    risk_metrics: {
        concentration_top3: 1.0,
        beta_proxy_by_asset_class: 1.05,
        beta_proxy_basis: 'asset_class_lookup (stock 1.1 / etf 1.0); not regressed against SPY',
        sector_diversification_score: 0.5,
    },
    options_pnl: {
        total_recommendations: 0, wins: 0, losses: 0, open: 0, total_return: 0,
    },
    ...overrides,
});

/** The four header tiles are the first grid after the page title. */
function headerTileText(container) {
    const tiles = container.querySelectorAll('div');
    return Array.from(tiles).map((n) => n.textContent).join(' | ');
}

describe('Portfolio dollar truth', () => {
    beforeEach(() => {
        api.getPortfolio.mockReset();
    });

    it('renders an em dash and no $ sign for a null total_value', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload());

        const { container } = render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('Portfolio Value')).toBeInTheDocument();
        });

        // No dollar sign anywhere on the page: every dollar field the old
        // endpoint served is now null, and the position table's fabricated
        // "1D P&L" column is gone.
        expect(container.textContent).not.toContain('$');
        expect(container.textContent).not.toContain('$0');
        expect(container.textContent).not.toContain('125');

        // The unknown value renders as the em dash with a stated reason.
        expect(screen.getByText(DASH)).toBeInTheDocument();
        expect(screen.getByText('no position sizes stored')).toBeInTheDocument();
    });

    it('still renders the measured percentage returns', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload());

        const { container } = render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('1D Return (weighted)')).toBeInTheDocument();
        });

        // Weighted portfolio return, and each position's own 1d/1w return.
        expect(screen.getByText('+2.34%')).toBeInTheDocument();
        expect(screen.getByText('+1.23%')).toBeInTheDocument();
        expect(screen.getByText('+3.45%')).toBeInTheDocument();
        expect(headerTileText(container)).toContain('100% of weight priced');
    });

    it('drops the old dollar P&L tiles and the per-position P&L column', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload());

        render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('Portfolio Value')).toBeInTheDocument();
        });

        expect(screen.queryByText('1D P&L')).not.toBeInTheDocument();
        expect(screen.queryByText('1M P&L (est)')).not.toBeInTheDocument();
        expect(screen.queryByText(/Beta \(Weighted\)/)).not.toBeInTheDocument();
    });

    it('labels the asset-class beta proxy for what it is', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload());

        render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('Beta Proxy (by asset class)')).toBeInTheDocument();
        });
        expect(screen.getByText(/asset_class_lookup/)).toBeInTheDocument();
    });

    it('surfaces holdings that could not be priced instead of hiding them', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload({
            positions_missing_price: 2,
            missing_price_tickers: ['BTC', 'TAO'],
            return_1d_weight_coverage: 0.5,
            weight_priced_total: 0.5,
        }));

        const { container } = render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('Missing Price')).toBeInTheDocument();
        });
        expect(screen.getByText('BTC, TAO')).toBeInTheDocument();
        expect(screen.getByText('2 without a price')).toBeInTheDocument();
        expect(headerTileText(container)).toContain('50% of weight priced');
    });

    it('renders an unavailable state, never 0%, when nothing could be priced', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload({
            positions: [],
            positions_missing_price: 2,
            missing_price_tickers: ['AAPL', 'SPY'],
            weighted_return_1d_pct: null,
            return_1d_weight_coverage: null,
            weight_priced_total: 0,
            allocation: { by_sector: {}, by_asset_type: {} },
            risk_metrics: {
                concentration_top3: null,
                beta_proxy_by_asset_class: null,
                beta_proxy_basis: 'asset_class_lookup; not regressed against SPY',
                sector_diversification_score: null,
            },
        }));

        const { container } = render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('1D Return (weighted)')).toBeInTheDocument();
        });

        expect(container.textContent).not.toContain('$');
        expect(container.textContent).not.toContain('0.00%');
        expect(screen.getByText('no priced position with a 1D return')).toBeInTheDocument();
        expect(screen.getAllByText('no priced positions').length).toBe(2);
        expect(
            screen.getByText('No priced positions. 2 holding(s) have no price.'),
        ).toBeInTheDocument();
    });

    it('handles an entirely empty watchlist without inventing a value', async () => {
        api.getPortfolio.mockResolvedValue(honestPayload({
            positions: [],
            positions_missing_price: 0,
            missing_price_tickers: [],
            weighted_return_1d_pct: null,
            return_1d_weight_coverage: null,
            weight_priced_total: 0,
            allocation: { by_sector: {}, by_asset_type: {} },
            risk_metrics: {
                concentration_top3: null,
                beta_proxy_by_asset_class: null,
                beta_proxy_basis: 'asset_class_lookup; not regressed against SPY',
                sector_diversification_score: null,
            },
        }));

        const { container } = render(<Portfolio />);

        await waitFor(() => {
            expect(screen.getByText('No positions in watchlist.')).toBeInTheDocument();
        });
        expect(container.textContent).not.toContain('$');
    });
});
