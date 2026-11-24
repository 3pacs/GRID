/**
 * #959 / review 6033941732: overlapping Regime reads, a cold read that is not a
 * real UNCALIBRATED answer, and Backtest runs that must report completion.
 */
import React from 'react';
import { render, screen, waitFor, fireEvent, act } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';
import Regime from '../views/Regime.jsx';
import Backtest from '../views/Backtest.jsx';

const storeState = { currentRegime: null, setCurrentRegime: vi.fn() };

vi.mock('../api.js', () => ({
    api: {
        getCurrent: vi.fn(), getAllActiveRegimes: vi.fn(), getHistory: vi.fn(), getTransitions: vi.fn(),
        getStrategyForRegime: vi.fn(), getRegimeSynthesis: vi.fn(),
        getBacktestSummaryPitch: vi.fn(), getBacktestResults: vi.fn(), listPaperTrades: vi.fn(), runBacktest: vi.fn(),
    },
}));
vi.mock('../store.js', () => ({ default: vi.fn(() => storeState) }));
vi.mock('../components/ViewHelp.jsx', () => ({ default: () => null }));

const PRIOR = { state: 'GROWTH', confidence: 0.7, transition_probability: 0.2 };
const NEWER = { state: 'CONTRACTION', confidence: 0.6, transition_probability: 0.3 };
const FAILED = { error: true, status: 503, message: 'down' };

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

beforeEach(() => {
    Object.values(api).forEach((fn) => fn.mockReset());
    storeState.currentRegime = null;
    storeState.setCurrentRegime.mockReset();
    storeState.setCurrentRegime.mockImplementation((r) => { storeState.currentRegime = r; });
    api.getAllActiveRegimes.mockResolvedValue(null);
    api.getHistory.mockResolvedValue({ history: [] });
    api.getTransitions.mockResolvedValue({ transitions: [] });
    api.getStrategyForRegime.mockResolvedValue(null);
});

describe('Regime overlapping reads', () => {
    async function mountThenRefresh() {
        storeState.currentRegime = PRIOR;
        const first = deferred();
        const second = deferred();
        api.getCurrent.mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise);
        const view = render(<Regime />);
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(1));
        fireEvent.click(screen.getByText('REFRESH'));
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalledTimes(2));
        return { view, first, second };
    }

    it('an older success after a newer failure neither stores nor clears LAST KNOWN', async () => {
        const { view, first, second } = await mountThenRefresh();
        await act(async () => { second.resolve(FAILED); });
        await act(async () => { first.resolve(NEWER); });
        view.rerender(<Regime />);
        expect(storeState.setCurrentRegime).not.toHaveBeenCalled();
        await waitFor(() => expect(screen.getByTestId('regime-banner-label'))
            .toHaveTextContent('LAST KNOWN · LATEST READ FAILED'));
    });

    it('an older failure after a newer success does not stale-mark the newer data', async () => {
        const { view, first, second } = await mountThenRefresh();
        await act(async () => { second.resolve(NEWER); });
        view.rerender(<Regime />);
        await act(async () => { first.resolve(FAILED); });
        view.rerender(<Regime />);
        expect(storeState.setCurrentRegime).toHaveBeenCalledWith(NEWER);
        await waitFor(() => expect(screen.getByTestId('regime-banner-label')).toHaveTextContent(/^CURRENT$/));
    });
});

describe('Regime cold read vs a real UNCALIBRATED answer', () => {
    it.each([['a 503', FAILED], ['a null', null]])('a cold %s reads NO VERIFIED READ, not CURRENT UNCALIBRATED', async (_n, reply) => {
        api.getCurrent.mockResolvedValue(reply);
        render(<Regime />);
        await waitFor(() => expect(api.getCurrent).toHaveBeenCalled());
        expect(await screen.findByTestId('regime-banner-label')).toHaveTextContent('NO VERIFIED READ');
        expect(screen.queryByText(/UNCALIBRATED/)).not.toBeInTheDocument();
    });

    it('control: the backend UNCALIBRATED reply is a real answer and reads CURRENT', async () => {
        const uncal = { state: 'UNCALIBRATED', confidence: 0, transition_probability: 0 };
        api.getCurrent.mockResolvedValue(uncal);
        const view = render(<Regime />);
        await waitFor(() => expect(storeState.setCurrentRegime).toHaveBeenCalledWith(uncal));
        view.rerender(<Regime />);
        expect(screen.getByTestId('regime-banner-label')).toHaveTextContent(/^CURRENT$/);
        expect(screen.getAllByText(/UNCALIBRATED/).length).toBeGreaterThan(0);
    });
});

describe('Backtest run must report completion', () => {
    beforeEach(() => {
        api.listPaperTrades.mockResolvedValue({ snapshots: [] });
        api.getBacktestResults.mockResolvedValue({});
        api.getBacktestSummaryPitch.mockResolvedValue({
            grid: { cumulative_return: 0.4 }, spy: {}, sixty_forty: {}, generated_at: '2026-11-23T12:00:00+00:00',
        });
    });

    it.each([['null', null], ['an untyped object', {}], ['an error', { error: true, message: 'boom' }]])(
        'a %s run response shows an error and does not reload the old summary', async (_n, reply) => {
            api.runBacktest.mockResolvedValue(reply);
            render(<Backtest />);
            await waitFor(() => expect(screen.getByText('GRID Performance')).toBeInTheDocument());
            fireEvent.click(screen.getByText('Run Pitch Backtest'));
            expect(await screen.findByText(reply?.message || 'Backtest run did not report completion')).toBeInTheDocument();
            expect(api.getBacktestSummaryPitch).toHaveBeenCalledTimes(1);
        });

    it('control: a {status: "complete"} run reloads the summary', async () => {
        api.runBacktest.mockResolvedValue({ status: 'complete' });
        render(<Backtest />);
        await waitFor(() => expect(screen.getByText('GRID Performance')).toBeInTheDocument());
        fireEvent.click(screen.getByText('Run Pitch Backtest'));
        await waitFor(() => expect(api.getBacktestSummaryPitch).toHaveBeenCalledTimes(2));
    });
});
