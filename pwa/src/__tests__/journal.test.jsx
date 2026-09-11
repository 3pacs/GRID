import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Journal from '../views/Journal.jsx';
import { api } from '../api.js';

const storeState = {
    journalEntries: [],
    journalStats: null,
    setJournalEntries: vi.fn((e) => { storeState.journalEntries = e; }),
    setJournalStats: vi.fn((s) => { storeState.journalStats = s; }),
};

vi.mock('../api.js', () => ({
    api: {
        getJournal: vi.fn(),
        getJournalStats: vi.fn(),
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

describe('Journal view async data', () => {
    beforeEach(() => {
        storeState.journalEntries = [];
        storeState.journalStats = null;
        storeState.setJournalEntries.mockClear();
        storeState.setJournalStats.mockClear();
        api.getJournal.mockReset();
        api.getJournalStats.mockReset();
        api.getJournalStats.mockResolvedValue({});
    });

    it('shows a loading skeleton while fetching, then renders the entries', async () => {
        const gate = deferred();
        api.getJournal.mockImplementation(() => gate.promise);

        render(<Journal onNavigate={() => {}} />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ entries: [{ id: 1, action_taken: 'BUY SPY', verdict: 'HELPED' }] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(storeState.setJournalEntries).toHaveBeenCalledWith([
            { id: 1, action_taken: 'BUY SPY', verdict: 'HELPED' },
        ]);
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getJournal.mockResolvedValueOnce({ error: true, message: 'journal store unavailable' });

        render(<Journal onNavigate={() => {}} />);

        expect(await screen.findByText('journal store unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getJournal.mockResolvedValueOnce({ entries: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getJournal).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('journal store unavailable')).not.toBeInTheDocument();
    });
});
