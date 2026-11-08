import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Predictions from '../views/Predictions.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getOracleScoreboard: vi.fn(),
        getOracleEvidence: vi.fn(),
        getOracleLatest: vi.fn(),
        getOraclePredictions: vi.fn(),
    },
}));

const EVIDENCE = {
    method: { min_windows: 30, min_outcomes_shown: 5 },
    summary: {
        outcomes: 21814, windows: 102, official_last_expiry: '2026-05-15', corrected_outcomes: 15743,
        corrected_last_expiry: '2026-11-03', excluded_implausible: 26, models_hidden_small: 15,
        models_tested: 1, models_with_edge: 0,
    },
    models: [
        { name: 'timeseries_enhanced', outcomes: 8064, windows: 94, raw_mean_pct: -0.145, excess_mean_pct: -0.083, lower_bound_pct: -0.259, verdict: 'no_edge' },
        { name: 'options_flow', outcomes: 351, windows: 2, raw_mean_pct: 2.64, excess_mean_pct: 0.4, lower_bound_pct: null, verdict: 'insufficient_windows' },
    ],
};

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Predictions view async data', () => {
    beforeEach(() => {
        api.getOracleScoreboard.mockReset();
        api.getOracleLatest.mockReset();
        api.getOracleEvidence.mockReset();
        api.getOraclePredictions.mockReset();
        api.getOracleEvidence.mockResolvedValue(EVIDENCE);
        api.getOracleLatest.mockResolvedValue({ streak: {}, recent_scored: [] });
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0 });
    });

    it('shows a loading skeleton while fetching, then renders the scoreboard', async () => {
        const gate = deferred();
        api.getOracleScoreboard.mockImplementation(() => gate.promise);

        render(<Predictions />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            overall: { accuracy: 0.62, scored: 40, total_pnl: 5.1 },
            models: [{ name: 'oracle_v1', accuracy: 0.62, cumulative_pnl: 5.1 }],
            by_ticker: [], calibration: {},
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.queryByText('MODEL TOURNAMENT')).not.toBeInTheDocument();
        expect(await screen.findByText('EVIDENCE BY MODEL')).toBeInTheDocument();
        expect(screen.getByTestId('evidence-headline')).toHaveTextContent('No model has shown an edge. 1 model has enough independent windows (30+) to test.');
        expect(screen.getByText('NO EDGE')).toBeInTheDocument();
        expect(screen.getByText('TOO FEW WINDOWS')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getOracleScoreboard.mockResolvedValueOnce({ error: true, message: 'oracle scoreboard unavailable' });

        render(<Predictions />);

        expect(await screen.findByText('oracle scoreboard unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getOracleScoreboard.mockResolvedValueOnce({
            overall: {}, models: [], by_ticker: [], calibration: {},
        });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getOracleScoreboard).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('oracle scoreboard unavailable')).not.toBeInTheDocument();
    });
});

describe('Predictions evidence panel', () => {
    beforeEach(() => {
        api.getOracleScoreboard.mockResolvedValue({ overall: {}, models: [], by_ticker: [], calibration: {} });
        api.getOracleLatest.mockResolvedValue({ streak: {}, recent_scored: [] });
        api.getOraclePredictions.mockResolvedValue({ predictions: [], total: 0 });
    });

    it('keeps the scoreboard when the evidence endpoint fails', async () => {
        api.getOracleEvidence.mockResolvedValue({ error: true, message: 'evidence unavailable' });
        render(<Predictions />);
        expect(await screen.findByText('HIT RATE (OFFICIAL)')).toBeInTheDocument();
        expect(screen.queryByText('EVIDENCE BY MODEL')).not.toBeInTheDocument();
    });
});
