import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Vault from '../views/Vault.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        vaultNotes: vi.fn(),
        vaultDashboard: vi.fn(),
        vaultSearch: vi.fn(),
        vaultChangeStatus: vi.fn(),
        vaultSync: vi.fn(),
        vaultActions: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Vault view async data', () => {
    beforeEach(() => {
        api.vaultNotes.mockReset();
        api.vaultDashboard.mockReset();
        api.vaultDashboard.mockResolvedValue({ review_items: [] });
    });

    it('shows a loading skeleton while fetching, then renders the notes list', async () => {
        const gate = deferred();
        api.vaultNotes.mockImplementation(() => gate.promise);

        render(<Vault />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ notes: [{ id: 'n1', title: 'Pipeline idea', domain: 'pipeline', status: 'inbox', vault_path: 'pipeline/n1.md' }], total: 1 });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('Pipeline idea')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.vaultNotes.mockRejectedValueOnce(new Error('vault notes unavailable'));

        render(<Vault />);

        expect(await screen.findByText('vault notes unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.vaultNotes.mockResolvedValueOnce({ notes: [], total: 0 });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.vaultNotes).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('vault notes unavailable')).not.toBeInTheDocument();
    });
});
