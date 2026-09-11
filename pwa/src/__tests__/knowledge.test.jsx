import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Knowledge from '../views/Knowledge.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getKnowledge: vi.fn(),
        getKnowledgeSummary: vi.fn(),
        getKnowledgeItem: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Knowledge view async data', () => {
    beforeEach(() => {
        api.getKnowledge.mockReset();
        api.getKnowledgeSummary.mockReset();
        api.getKnowledgeSummary.mockResolvedValue({ total: 0, this_week: 0, categories: [] });
    });

    it('shows a loading skeleton while fetching, then renders the entries', async () => {
        const gate = deferred();
        api.getKnowledge.mockImplementation(() => gate.promise);

        render(<Knowledge />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ entries: [{ id: 1, question: 'What drives regime shifts?', answer: 'Liquidity.', category: 'regime' }], total: 1 });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('What drives regime shifts?')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getKnowledge.mockResolvedValueOnce({ error: true, message: 'knowledge tree unavailable' });

        render(<Knowledge />);

        expect(await screen.findByText('knowledge tree unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getKnowledge.mockResolvedValueOnce({ entries: [], total: 0 });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getKnowledge).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('knowledge tree unavailable')).not.toBeInTheDocument();
    });
});
