import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import TenYearPortfolio from '../views/TenYearPortfolio.jsx';
import { api } from '../api.js';

// Regression tests for TenYearPortfolio.jsx.
//
// api/routers/ten_year_portfolio.py's weekly endpoint can resolve to three
// shapes:
//   - a healthy payload (status: "ok" implicitly, real numbers)
//   - {"status": "empty", "message": "..."}                  (:286-289)
//   - {"status": "error", "error": "..."}                    (:298)
//
// The view's money()/pct()/number() formatters printed "$0"/"n/a" for
// null/undefined instead of an honest placeholder, and the "As of" /
// profile-description fallbacks were keyed off `data` alone, so an
// "empty" or "error" response (which never populates `data`) left "As
// of" stuck on the literal string "loading" forever, with no visible
// explanation, while the rest of the view rendered fabricated zeros.

vi.mock('../api.js', () => ({
    api: {
        getTenYearPortfolio: vi.fn(),
    },
}));

const HEALTHY_RESULT = {
    status: 'ok',
    as_of: '2026-09-12',
    capital: 1000000,
    benchmark: {
        ticker: 'QQQ',
        cagr: 0.15,
        total_return: 1.2,
        sparkline: [{ value: 100 }, { value: 115 }],
    },
    universe: { ranked_candidates: 5 },
    ranked: [],
    profiles: [
        {
            id: 'dad_chartist',
            label: 'Dad Chartist',
            description: 'Steady big companies with strong long-term charts.',
            top_n: 8,
            hold_buffer: 2,
            estimated_invested: 950000,
            estimated_residual_cash: 50000,
            max_position: 0.15,
            monte_carlo: {
                p10: 1200000,
                p50: 1800000,
                p90: 2600000,
                probability_above_start: 0.82,
                expected_annual_return: 0.11,
                annual_volatility: 0.18,
            },
            allocations: [
                {
                    ticker: 'AAPL',
                    score: 8.2,
                    cagr: 0.22,
                    relative_cagr: 0.05,
                    trend_r2: 0.9,
                    max_drawdown: -0.18,
                    target_dollars: 150000,
                    target_weight: 0.15,
                    sparkline: [{ value: 100 }, { value: 220 }],
                },
            ],
            weekly_policy: { exit_rule: 'Trim if rank falls out of buffer.' },
        },
    ],
    candidate_boards: [
        { id: 'frontier_infrastructure', label: 'Frontier Infrastructure', allocations: [] },
    ],
};

describe('TenYearPortfolio API status handling', () => {
    beforeEach(() => {
        api.getTenYearPortfolio.mockReset();
    });

    it('shows no "$0", no stuck "loading", and the router message when status is "empty"', async () => {
        api.getTenYearPortfolio.mockResolvedValue({
            status: 'empty',
            message: 'No eligible Yahoo adjusted-close price history found.',
        });

        render(<TenYearPortfolio />);

        await waitFor(() => {
            expect(screen.getAllByText(/No eligible Yahoo adjusted-close price history found\./).length).toBeGreaterThan(0);
        });

        expect(screen.queryByText('$0')).not.toBeInTheDocument();
        expect(screen.queryByText('loading')).not.toBeInTheDocument();
        expect(screen.getByText('As of').nextSibling.textContent).toBe('—');
    });

    it('shows an explicit error state (not a stuck loader) when status is "error"', async () => {
        api.getTenYearPortfolio.mockResolvedValue({
            status: 'error',
            error: 'Ten-year portfolio query failed.',
        });

        render(<TenYearPortfolio />);

        await waitFor(() => {
            expect(screen.getAllByText('Ten-year portfolio query failed.').length).toBeGreaterThan(0);
        });

        expect(screen.queryByText('$0')).not.toBeInTheDocument();
        expect(screen.queryByText('loading')).not.toBeInTheDocument();
        expect(screen.getByText('As of').nextSibling.textContent).toBe('—');
    });

    it('renders the healthy sample unchanged (real numbers, no placeholders)', async () => {
        api.getTenYearPortfolio.mockResolvedValue(HEALTHY_RESULT);

        render(<TenYearPortfolio />);

        await waitFor(() => {
            expect(screen.getByText('As of').nextSibling.textContent).toBe('2026-09-12');
        });

        expect(screen.getByText('Steady big companies with strong long-term charts.')).toBeInTheDocument();
        expect(screen.getByText('15%')).toBeInTheDocument(); // max_position via pct(0.15, 0)
        expect(screen.getAllByText(/AAPL/).length).toBeGreaterThan(0);
        expect(screen.getByText('5 ranked')).toBeInTheDocument();

        // No formatter fell back to a placeholder for real, present values.
        expect(screen.queryByText('$0')).not.toBeInTheDocument();
        expect(screen.queryByText('—')).not.toBeInTheDocument();
        expect(screen.queryByText('n/a')).not.toBeInTheDocument();
        expect(screen.queryByText('loading')).not.toBeInTheDocument();
    });
});

// Dad-mode (contributor role -> isSimpleUser()) renders a different, plain-
// language shell (see the `if (simple) { ... }` branch). It used to reuse a
// single `error` string for both the router's "empty" response and an
// actual load failure, so an empty response ("no eligible price history")
// showed the same "We could not load the plan just now..." wording as a
// real failure, plus a leftover "Press Update to build the plan." prompt —
// telling a non-technical user the load failed when it hadn't.
describe('TenYearPortfolio dad-mode (contributor) empty vs error copy', () => {
    beforeEach(() => {
        api.getTenYearPortfolio.mockReset();
        localStorage.setItem('grid_role', 'contributor');
    });

    afterEach(() => {
        localStorage.removeItem('grid_role');
    });

    it('says there is no data yet, not that loading failed, when status is "empty"', async () => {
        api.getTenYearPortfolio.mockResolvedValue({
            status: 'empty',
            message: 'No eligible Yahoo adjusted-close price history found.',
        });

        render(<TenYearPortfolio />);

        await waitFor(() => {
            expect(screen.getAllByText('No eligible price history yet, so there is no plan to show.').length).toBeGreaterThan(0);
        });

        expect(screen.queryByText('We could not load the plan just now. Please try Update again in a moment.')).not.toBeInTheDocument();
        expect(screen.queryByText('Press Update to build the plan.')).not.toBeInTheDocument();
        expect(screen.queryByText('$0')).not.toBeInTheDocument();
    });

    it('keeps the "could not load" failure copy when status is "error"', async () => {
        api.getTenYearPortfolio.mockResolvedValue({
            status: 'error',
            error: 'Ten-year portfolio query failed.',
        });

        render(<TenYearPortfolio />);

        await waitFor(() => {
            expect(screen.getByText('We could not load the plan just now. Please try Update again in a moment.')).toBeInTheDocument();
        });

        expect(screen.queryByText('No eligible price history yet, so there is no plan to show.')).not.toBeInTheDocument();
    });
});
