import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Predictions from '../views/Predictions.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getOracleScoreboard: vi.fn(),
        getOracleLatest: vi.fn(),
        getOraclePredictions: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Predictions view async data', () => {
    beforeEach(() => {
        api.getOracleScoreboard.mockReset();
        api.getOracleLatest.mockReset();
        api.getOraclePredictions.mockReset();
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
        expect(screen.getByText('MODEL TOURNAMENT')).toBeInTheDocument();
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
