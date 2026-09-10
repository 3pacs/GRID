import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import IntelligenceSearchView from '../views/IntelligenceSearchView.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        listBoards: vi.fn(),
        createBoard: vi.fn(),
        getBoard: vi.fn(),
        saveBoard: vi.fn(),
    },
}));

vi.mock('../components/IntelligenceSearch.jsx', () => ({
    default: () => <div data-testid="intelligence-search" />,
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('IntelligenceSearchView board list async data', () => {
    beforeEach(() => {
        api.listBoards.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the board select', async () => {
        const gate = deferred();
        api.listBoards.mockImplementation(() => gate.promise);

        render(<IntelligenceSearchView />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve([{ id: 'b1', name: 'Board One' }]);

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('Board One')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.listBoards.mockRejectedValueOnce(new Error('canvas boards unavailable'));

        render(<IntelligenceSearchView />);

        expect(await screen.findByText('canvas boards unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.listBoards.mockResolvedValueOnce([]);
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.listBoards).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('canvas boards unavailable')).not.toBeInTheDocument();
    });
});
