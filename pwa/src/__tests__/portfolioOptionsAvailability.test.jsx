import React from 'react';
import { render, screen } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Portfolio from '../views/Portfolio.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: { getPortfolio: vi.fn() } }));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: () => ({ isMobile: false }) }));

const basePortfolio = {
    total_value: 0,
    total_pnl_1d: 0,
    total_pnl_1d_pct: 0,
    total_pnl_1m: 0,
    positions: [],
    allocation: { by_sector: {}, by_asset_type: {} },
    risk_metrics: {
        concentration_top3: 0,
        beta_weighted: 0,
        sector_diversification_score: 0,
    },
};

describe('portfolio options availability', () => {
    beforeEach(() => api.getPortfolio.mockReset());

    it('shows unavailable instead of fabricated options metrics after query failure', async () => {
        api.getPortfolio.mockResolvedValue({
            ...basePortfolio,
            options_pnl: {
                status: 'unavailable', total_recommendations: null,
                wins: null, losses: null, open: null, total_return: null,
            },
        });
        render(<Portfolio />);
        expect(await screen.findByText('Options recommendations are currently unavailable.')).toBeInTheDocument();
        expect(screen.queryByText('No options recommendations tracked yet.')).not.toBeInTheDocument();
        expect(screen.queryByText('nullW / nullL')).not.toBeInTheDocument();
    });

    it('keeps a checked empty aggregate distinct from unavailable', async () => {
        api.getPortfolio.mockResolvedValue({
            ...basePortfolio,
            options_pnl: {
                status: 'available', total_recommendations: 0,
                wins: 0, losses: 0, open: 0, total_return: 0,
            },
        });
        render(<Portfolio />);
        expect(await screen.findByText('No options recommendations tracked yet.')).toBeInTheDocument();
        expect(screen.queryByText('Options recommendations are currently unavailable.')).not.toBeInTheDocument();
    });
});
