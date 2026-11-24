/**
 * #959 / review 6033471072: after a good read, a failed refresh or run must be
 * visible, and a measured value must keep its real meaning:
 * - Backtest: failed summary refresh and failed run after a good summary;
 * - Regime: failed refresh after a good read is labelled LAST KNOWN;
 * - Mobile Pipeline: the colour follows a health criterion, a measured 0/10
 *   is not green, and counts the route did not send read "?", not 0.
 */
import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Backtest from '../views/Backtest.jsx';
import Regime from '../views/Regime.jsx';
import MobileDashboard from '../views/mobile/MobileDashboard.jsx';
import { colors } from '../styles/shared.js';

const storeState = { currentRegime: null, setCurrentRegime: vi.fn(), userRole: 'admin' };

vi.mock('../api.js', () => ({
    api: {
        getBacktestSummaryPitch: vi.fn(), getBacktestResults: vi.fn(), listPaperTrades: vi.fn(),
        runBacktest: vi.fn(),
        getCurrent: vi.fn(), getAllActiveRegimes: vi.fn(), getHistory: vi.fn(), getTransitions: vi.fn(),
        getStrategyForRegime: vi.fn(), getRegimeSynthesis: vi.fn(),
        getStatus: vi.fn(), getPipelineHealth: vi.fn(), get100xOpportunities: vi.fn(), getSources: vi.fn(),
        getJournal: vi.fn(), getOperatorIssues: vi.fn(), getSnapshotLatest: vi.fn(),
    },
}));
vi.mock('../store.js', () => ({ default: vi.fn((sel) => (sel ? sel(storeState) : storeState)) }));
vi.mock('../components/ViewHelp.jsx', () => ({ default: () => null }));

const GOOD_SUMMARY = {
    grid: { cumulative_return: 0.4, sharpe: 1.2, final_value: 140000 }, spy: {}, sixty_forty: {},
    generated_at: '2026-11-23T12:00:00+00:00',
};

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = null;
    storeState.setCurrentRegime.mockReset();
    storeState.setCurrentRegime.mockImplementation((r) => { storeState.currentRegime = r; });
});

describe('Backtest after a good summary', () => {
    beforeEach(() => {
        api.listPaperTrades.mockResolvedValue({ snapshots: [] });
        api.getBacktestResults.mockResolvedValue({});
    });

    it('a failed summary refresh is shown above the previous results', async () => {
        api.getBacktestSummaryPitch.mockResolvedValueOnce(GOOD_SUMMARY)
            .mockResolvedValueOnce({ error: true, status: 500, message: 'Internal Server Error' });
        api.runBacktest.mockResolvedValue({ status: 'complete' });
        render(<Backtest />);
        await waitFor(() => expect(screen.getByText('GRID Performance')).toBeInTheDocument());
        fireEvent.click(screen.getByText('Run Pitch Backtest'));
        expect(await screen.findByTestId('summary-refresh-failed'))
            .toHaveTextContent(/Latest summary refresh failed: Internal Server Error\. Showing the previous results/);
    });

    it('a failed run says so and does not reload the old results as if they were the run', async () => {
        api.getBacktestSummaryPitch.mockResolvedValue(GOOD_SUMMARY);
        api.runBacktest.mockResolvedValue({ error: true, status: 500, message: 'run failed: no prices' });
        render(<Backtest />);
        await waitFor(() => expect(screen.getByText('GRID Performance')).toBeInTheDocument());
        fireEvent.click(screen.getByText('Run Pitch Backtest'));
        expect(await screen.findByText('run failed: no prices')).toBeInTheDocument();
        expect(api.getBacktestSummaryPitch).toHaveBeenCalledTimes(1);
    });
});

describe('Regime after a good read', () => {
    beforeEach(() => {
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });
        api.getStrategyForRegime.mockResolvedValue(null);
    });

    it.each([
        ['a 503', { error: true, status: 503, message: 'down' }],
        ['a null', null],
    ])('labels the old regime LAST KNOWN when the refresh returns %s', async (_name, second) => {
        api.getCurrent.mockResolvedValueOnce({ state: 'GROWTH', confidence: 0.7, transition_probability: 0.2 })
            .mockResolvedValueOnce(second);
        const view = render(<Regime />);
        await waitFor(() => expect(storeState.setCurrentRegime).toHaveBeenCalledTimes(1));
        view.rerender(<Regime />);
        expect(screen.getByTestId('regime-banner-label')).toHaveTextContent('CURRENT');
        fireEvent.click(screen.getByText('REFRESH'));
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(2));
        // Review 6033672651: mount and REFRESH are fresh network reads, never the 60 s cache.
        expect(api.getCurrent).toHaveBeenNthCalledWith(1, { force: true });
        expect(api.getCurrent).toHaveBeenNthCalledWith(2, { force: true });
        await waitFor(() => expect(screen.getByTestId('regime-banner-label'))
            .toHaveTextContent('LAST KNOWN · LATEST READ FAILED'));
        expect(storeState.setCurrentRegime).toHaveBeenCalledTimes(1);
    });
});

describe('Mobile Pipeline card: health criterion and unknown counts', () => {
    beforeEach(() => {
        api.getStatus.mockResolvedValue({ database: { connected: true } });
        api.getCurrent.mockResolvedValue({ state: 'NEUTRAL', confidence: 0.5 });
        api.get100xOpportunities.mockResolvedValue({ opportunities: [] });
        api.getSources.mockResolvedValue({ sources: [] });
        api.getJournal.mockResolvedValue({ entries: [] });
        api.getOperatorIssues.mockResolvedValue({ issues: [] });
        api.getSnapshotLatest.mockResolvedValue({ snapshots: [] });
    });

    const mount = async (summary) => {
        api.getPipelineHealth.mockResolvedValue({ availability: 'available', summary });
        render(<MobileDashboard onNavigate={vi.fn()} />);
        return screen.findByTestId('pipeline-dot');
    };

    it('a measured 0 of 10 healthy is red, not green, and still prints 0%', async () => {
        const dot = await mount({ total_sources: 10, healthy: 0, broken: 10, stale: 0 });
        expect(dot.dataset.tone).toBe(colors.red);
        expect(screen.getByText('0%')).toBeInTheDocument();
        expect(screen.getByText('0/10 healthy')).toBeInTheDocument();
    });

    it('10 of 10 healthy with a known zero broken is green', async () => {
        const dot = await mount({ total_sources: 10, healthy: 10, broken: 0, stale: 0 });
        expect(dot.dataset.tone).toBe(colors.green);
    });

    it('unknown broken/stale counts read "?" and keep the dot off green', async () => {
        const dot = await mount({ total_sources: 10, healthy: 10 });
        expect(dot.dataset.tone).toBe(colors.yellow);
        expect(screen.getByText('? broken / ? stale')).toBeInTheDocument();
    });

    it('a real zero broken count prints 0, not "?"', async () => {
        await mount({ total_sources: 10, healthy: 7, broken: 0, stale: 3 });
        expect(screen.getByText('0 broken / 3 stale')).toBeInTheDocument();
    });
});
