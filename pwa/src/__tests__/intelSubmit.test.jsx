import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import IntelSubmit from '../views/IntelSubmit.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getActorNetwork: vi.fn(),
        submitIntel: vi.fn(),
    },
}));

describe('IntelSubmit view async data', () => {
    beforeEach(() => {
        api.getActorNetwork.mockReset();
        api.submitIntel.mockReset();
    });

    it('renders the form immediately without gating it on the actor list load', async () => {
        let resolveActors;
        api.getActorNetwork.mockImplementation(() => new Promise((res) => { resolveActors = res; }));

        render(<IntelSubmit />);

        // Form is usable right away; the actor list load is non-blocking.
        expect(screen.getByPlaceholderText('Start typing actor name or id…')).toBeInTheDocument();
        expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();

        resolveActors({ nodes: [{ id: 'actor-1', label: 'Actor One', type: 'individual' }] });
        await waitFor(() => expect(api.getActorNetwork).toHaveBeenCalledTimes(1));
    });

    it('shows an error state on actor-list failure and refetches on retry', async () => {
        api.getActorNetwork.mockResolvedValueOnce({ error: true, message: 'unable to load tracked actors' });

        render(<IntelSubmit />);

        expect(await screen.findByText('unable to load tracked actors')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getActorNetwork.mockResolvedValueOnce({ nodes: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getActorNetwork).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('unable to load tracked actors')).not.toBeInTheDocument();
    });
});
