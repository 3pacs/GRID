import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Strategies from '../views/Strategies.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getPaperStrategies: vi.fn(),
        getStrategyHistory: vi.fn(),
        getBacktestWinners: vi.fn(),
        promoteToStrategy: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Strategies view async data', () => {
    beforeEach(() => {
        api.getPaperStrategies.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the active strategies', async () => {
        const gate = deferred();
        api.getPaperStrategies.mockImplementation(() => gate.promise);

        render(<Strategies />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({
            strategies: [{ id: 's1', leader: 'AAPL', follower: 'MSFT', status: 'ACTIVE', total_pnl: 12.5, win_rate: 0.6 }],
        });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText(/AAPL/)).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getPaperStrategies.mockResolvedValueOnce({ error: true, message: 'paper strategies unavailable' });

        render(<Strategies />);

        expect(await screen.findByText('paper strategies unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getPaperStrategies.mockResolvedValueOnce({ strategies: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getPaperStrategies).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('paper strategies unavailable')).not.toBeInTheDocument();
    });
});
