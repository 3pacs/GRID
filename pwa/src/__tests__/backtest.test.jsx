/**
 * Tests for the Backtest view's honesty labeling (GRID-WAVE3-HELD-WRITERS-
 * TRIAGE-20260927.md #7): the pitch backtest has no live schedule behind it
 * and its JSON has no generated_at of its own -- the API now supplies the
 * file's mtime plus a note, and the view must render both prominently.
 */
import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Backtest from '../views/Backtest.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getBacktestSummaryPitch: vi.fn(),
        getBacktestResults: vi.fn(),
        listPaperTrades: vi.fn(),
        runBacktest: vi.fn(),
        generateCharts: vi.fn(),
        createPaperTrade: vi.fn(),
        scorePredictions: vi.fn(),
        getChartUrl: vi.fn(() => 'http://test/chart.png'),
    },
}));

vi.mock('../components/ViewHelp.jsx', () => ({
    default: () => <div data-testid="view-help" />,
}));

describe('Backtest view honesty labeling', () => {
    beforeEach(() => {
        api.getBacktestSummaryPitch.mockReset();
        api.getBacktestResults.mockReset();
        api.listPaperTrades.mockReset();
        api.listPaperTrades.mockResolvedValue({ snapshots: [] });
        api.getBacktestResults.mockResolvedValue({});
    });

    it('renders the in-sample note and generated-on-request timestamp from the API', async () => {
        api.getBacktestSummaryPitch.mockResolvedValue({
            period: '2015-01-01..2026-03-22',
            grid: { cumulative_return: 0.4, sharpe: 0.163 },
            spy: {},
            sixty_forty: {},
            generated_at: '2026-03-24T12:00:00+00:00',
            note: 'pitch backtest — in-sample regime mapping, not an out-of-sample result',
        });

        render(<Backtest />);

        await waitFor(() => {
            expect(screen.getByText(/in-sample regime mapping/i)).toBeInTheDocument();
        });
        expect(screen.getByText(/Generated on request/i)).toBeInTheDocument();
    });

    it('renders nothing extra when the backend omits note/generated_at (defensive)', async () => {
        api.getBacktestSummaryPitch.mockResolvedValue({
            period: '2015-01-01..2026-03-22',
            grid: {}, spy: {}, sixty_forty: {},
        });

        render(<Backtest />);

        await waitFor(() => {
            expect(screen.getByText('GRID Performance')).toBeInTheDocument();
        });
        expect(screen.queryByText(/Generated on request/i)).not.toBeInTheDocument();
    });
});
