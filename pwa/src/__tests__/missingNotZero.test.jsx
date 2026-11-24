/**
 * #959 M1: a missing value or a failed request is never drawn as a measured 0,
 * and a page timer is never drawn as a backend heartbeat.
 *
 * Each case reproduces a payload the backend really sends (traced in the
 * read-only PWA sweep of 2026-11-24):
 * - options.py returns confidence None for a saved row with a NULL column;
 * - /backtest/summary raises 404, which api._fetch resolves as {error: true};
 * - oracle rows may carry NULL coherence / signal_strength;
 * - /regime/current sends UNCALIBRATED with 0.0 placeholders when it has no row;
 * - TenYearPortfolio's old "Heartbeat" was a 10 s browser interval.
 */
import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import { TickerRecommendations } from '../views/Options.jsx';
import Backtest from '../views/Backtest.jsx';
import { PredictionCard } from '../views/Predictions.jsx';
import Regime from '../views/Regime.jsx';
import TenYearPortfolio from '../views/TenYearPortfolio.jsx';

const storeState = { currentRegime: null, setCurrentRegime: vi.fn() };

vi.mock('../api.js', () => ({
    api: {
        getOptionsRecommendations: vi.fn(),
        getBacktestSummaryPitch: vi.fn(),
        getBacktestResults: vi.fn(),
        listPaperTrades: vi.fn(),
        getCurrent: vi.fn(),
        getAllActiveRegimes: vi.fn(),
        getHistory: vi.fn(),
        getTransitions: vi.fn(),
        getStrategyForRegime: vi.fn(),
        getRegimeSynthesis: vi.fn(),
        getTenYearPortfolio: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({ default: vi.fn(() => storeState) }));
vi.mock('../components/ViewHelp.jsx', () => ({ default: () => null }));

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = null;
    storeState.setCurrentRegime.mockReset();
});

describe('Options: a saved recommendation with no confidence', () => {
    it('draws "not stated", not a red 0', async () => {
        api.getOptionsRecommendations.mockResolvedValue({
            scan_summary: { source: 'persisted' },
            recommendations: [{ ticker: 'AAPL', direction: 'CALL', strike: 200,
                entry_price: 10, target_price: 12, stop_loss: 8, confidence: null }],
        });
        render(<TickerRecommendations ticker="AAPL" />);
        expect(await screen.findByLabelText('confidence not stated')).toBeInTheDocument();
    });
});

describe('Backtest: no results yet', () => {
    beforeEach(() => {
        api.listPaperTrades.mockResolvedValue({ snapshots: [] });
        api.getBacktestResults.mockResolvedValue({});
    });

    it('shows the 404 message instead of a row of 0.0% / 0.00 / $0', async () => {
        api.getBacktestSummaryPitch.mockResolvedValue({
            error: true, status: 404, message: 'No backtest results. Run /run first.',
        });
        render(<Backtest />);
        expect(await screen.findByText('No backtest results. Run /run first.')).toBeInTheDocument();
        expect(screen.queryByText('GRID Performance')).not.toBeInTheDocument();
        expect(screen.queryByText('$0')).not.toBeInTheDocument();
    });

    it('prints a dash for a missing metric and keeps a real 0', async () => {
        api.getBacktestSummaryPitch.mockResolvedValue({
            grid: { cumulative_return: 0, sharpe: null }, spy: {}, sixty_forty: {},
        });
        render(<Backtest />);
        await waitFor(() => expect(screen.getByText('GRID Performance')).toBeInTheDocument());
        expect(screen.getByText('0.0%')).toBeInTheDocument();
        expect(screen.queryByText('0.00')).not.toBeInTheDocument();
        expect(screen.getAllByText('—').length).toBeGreaterThan(0);
    });
});

describe('Predictions: unscored coherence and signal strength', () => {
    it('reads "unscored" for null and keeps a measured 0', () => {
        const base = {
            id: 1, ticker: 'SYNTH', direction: 'CALL', verdict: 'pending', signals: [],
            coherence: null, signal_strength: 0,
        };
        render(<PredictionCard pred={base} />);
        fireEvent.click(screen.getByText(/SYNTH/));
        expect(screen.getByText(/Coherence: unscored/)).toBeInTheDocument();
        expect(screen.getByText(/Signal: 0\.00/)).toBeInTheDocument();
    });
});

describe('Regime: no reading and failed reads', () => {
    beforeEach(() => {
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime.mockResolvedValue(null);
    });

    it('never stores a resolved error object as the regime', async () => {
        api.getCurrent.mockResolvedValue({ error: true, status: 503, message: 'down' });
        render(<Regime />);
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalled());
        await waitFor(() => expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument());
        expect(storeState.setCurrentRegime).not.toHaveBeenCalled();
    });

    it('does not draw UNCALIBRATED 0.0 placeholders as a 0 % shift risk', async () => {
        storeState.currentRegime = {
            state: 'UNCALIBRATED', confidence: 0, transition_probability: 0,
            baseline_comparison: 'No data — run auto_regime or wait for scheduled detection',
        };
        api.getCurrent.mockResolvedValue(storeState.currentRegime);
        render(<Regime />);
        expect(await screen.findByTestId('shift-risk-unavailable')).toHaveTextContent(/no regime reading yet/);
    });
});

describe('TenYearPortfolio: no browser-clock heartbeat', () => {
    it('shows the data as-of, not a "Heartbeat" that ticks with the page', async () => {
        api.getTenYearPortfolio.mockResolvedValue({ status: 'empty', message: 'No price history yet.' });
        render(<TenYearPortfolio />);
        await waitFor(() => expect(api.getTenYearPortfolio).toHaveBeenCalled());
        expect(screen.queryByText(/Heartbeat/)).not.toBeInTheDocument();
        expect(screen.queryByText(/heartbeat is live/i)).not.toBeInTheDocument();
        expect(await screen.findByText(/Data as of/)).toBeInTheDocument();
    });
});
