import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Regime from '../views/Regime.jsx';
import { api } from '../api.js';

const storeState = {
    currentRegime: null,
    setCurrentRegime: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getCurrent: vi.fn(),
        getAllActiveRegimes: vi.fn(),
        getHistory: vi.fn(),
        getTransitions: vi.fn(),
        getStrategyForRegime: vi.fn(),
        getRegimeSynthesis: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Regime view async data', () => {
    beforeEach(() => {
        storeState.currentRegime = null;
        storeState.setCurrentRegime.mockReset();
        api.getCurrent.mockReset();
        api.getAllActiveRegimes.mockReset();
        api.getHistory.mockReset();
        api.getTransitions.mockReset();
        api.getStrategyForRegime.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the transitions once loaded', async () => {
        const gate = deferred();
        api.getCurrent.mockImplementation(() => gate.promise);
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({
            transitions: [{ date: '2026-01-01', from_state: 'NEUTRAL', to_state: 'GROWTH', confidence: 0.8 }],
        });

        render(<Regime />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve(null);

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });

        fireEvent.click(screen.getByText('history'));
        expect(screen.getByText('TRANSITIONS (1)')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getCurrent.mockRejectedValueOnce(new Error('regime endpoint down'));
        api.getAllActiveRegimes.mockResolvedValue(null);
        api.getHistory.mockResolvedValue({ history: [] });
        api.getTransitions.mockResolvedValue({ transitions: [] });

        render(<Regime />);

        expect(await screen.findByText('regime endpoint down')).toBeInTheDocument();
        expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument();

        api.getCurrent.mockResolvedValueOnce(null);
        fireEvent.click(screen.getByRole('button', { name: 'Retry' }));

        await waitFor(() => {
            expect(api.getCurrent).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('regime endpoint down')).not.toBeInTheDocument();
    });
});
