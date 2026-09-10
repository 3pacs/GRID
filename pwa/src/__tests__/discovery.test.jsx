import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Discovery from '../views/Discovery.jsx';
import { api } from '../api.js';

const storeState = {
    jobs: [],
    hypotheses: [],
    setJobs: vi.fn((jobs) => { storeState.jobs = jobs; }),
    setHypotheses: vi.fn((h) => { storeState.hypotheses = h; }),
    addNotification: vi.fn(),
};

vi.mock('../api.js', () => ({
    api: {
        getJobs: vi.fn(),
        getResults: vi.fn(),
        getHypotheses: vi.fn(),
        triggerOrthogonality: vi.fn(),
        triggerClustering: vi.fn(),
    },
}));

vi.mock('../store.js', () => ({
    default: vi.fn(() => storeState),
}));

if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Discovery view async data', () => {
    beforeEach(() => {
        storeState.jobs = [];
        storeState.hypotheses = [];
        storeState.setJobs.mockClear();
        storeState.setHypotheses.mockClear();
        storeState.addNotification.mockClear();
        api.getJobs.mockReset();
        api.getResults.mockReset();
        api.getHypotheses.mockReset();
        api.getHypotheses.mockResolvedValue({ hypotheses: [] });
        api.getResults.mockResolvedValue(null);
    });

    it('shows a loading skeleton while fetching, then renders the jobs list', async () => {
        const gate = deferred();
        api.getJobs.mockImplementation(() => gate.promise);

        render(<Discovery />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ jobs: [{ id: 'job-1', type: 'clustering', status: 'complete', started: '2026-09-01T00:00:00Z' }] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(storeState.setJobs).toHaveBeenCalledWith([{ id: 'job-1', type: 'clustering', status: 'complete', started: '2026-09-01T00:00:00Z' }]);
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getJobs.mockRejectedValueOnce(new Error('discovery jobs unavailable'));

        render(<Discovery />);

        expect(await screen.findByText('discovery jobs unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getJobs.mockResolvedValueOnce({ jobs: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getJobs).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('discovery jobs unavailable')).not.toBeInTheDocument();
    });
});
