import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import IntelModeration from '../views/IntelModeration.jsx';
import { api } from '../api.js';

const storeState = { userRole: 'admin' };

vi.mock('../api.js', () => ({
    api: {
        listPendingIntel: vi.fn(),
        verifyIntel: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn((selector) => (selector ? selector(storeState) : storeState)),
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('IntelModeration view async data', () => {
    beforeEach(() => {
        storeState.userRole = 'admin';
        api.listPendingIntel.mockReset();
        api.verifyIntel.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the pending queue', async () => {
        const gate = deferred();
        api.listPendingIntel.mockImplementation(() => gate.promise);

        render(<IntelModeration />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve([{ id: 1, intel_type: 'tip', actor_id: 'actor-1', note: 'Watch this actor', submitted_by: 'anik' }]);

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('Watch this actor')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.listPendingIntel.mockResolvedValueOnce({ error: true, message: 'moderation queue unavailable' });

        render(<IntelModeration />);

        expect(await screen.findByText('moderation queue unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.listPendingIntel.mockResolvedValueOnce([]);
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.listPendingIntel).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('moderation queue unavailable')).not.toBeInTheDocument();
    });
});
