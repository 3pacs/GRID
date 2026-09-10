import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import CrossReference from '../views/CrossReference.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getCrossReference: vi.fn(),
        getCrossRefHistory: vi.fn(),
        getCrossReferenceNarrative: vi.fn(),
    },
}));

const REAL_CHECK = {
    name: 'GDP vs Night Lights (China)',
    category: 'gdp',
    official_source: 'NBS GDP',
    official_value: 5.2,
    physical_source: 'VIIRS Night Lights',
    physical_value: 2.1,
    actual_divergence: 2.8,
    assessment: 'major',
    implication: 'Real growth likely below the headline figure.',
    confidence: 0.82,
    checked_at: '2026-09-10T00:00:00Z',
};

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('CrossReference view async data', () => {
    beforeEach(() => {
        api.getCrossReference.mockReset();
        api.getCrossRefHistory.mockReset();
        api.getCrossReferenceNarrative.mockReset();
        api.getCrossRefHistory.mockResolvedValue({ records: [] });
    });

    it('shows a loading skeleton while fetching', async () => {
        const gate = deferred();
        api.getCrossReference.mockImplementation(() => gate.promise);

        render(<CrossReference />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ checks: [] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
    });

    it('shows an error state on a rejected fetch, renders no chart, and refetches on retry', async () => {
        api.getCrossReference.mockRejectedValueOnce(new Error('cross-reference feed unavailable'));

        render(<CrossReference />);

        expect(await screen.findByText('cross-reference feed unavailable')).toBeInTheDocument();
        expect(screen.queryByText('Divergence Matrix')).not.toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getCrossReference.mockResolvedValueOnce({ checks: [REAL_CHECK] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getCrossReference).toHaveBeenCalledTimes(2);
        });
        await waitFor(() => {
            expect(screen.queryByText('cross-reference feed unavailable')).not.toBeInTheDocument();
        });
        expect(screen.getByText('Divergence Matrix')).toBeInTheDocument();
    });

    it('renders an honest empty state with no synthetic series when the payload is empty', async () => {
        api.getCrossReference.mockResolvedValue({ checks: [] });

        render(<CrossReference />);

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });

        expect(screen.getByText('NO DATA')).toBeInTheDocument();
        expect(screen.getByText('No live cross-reference checks available yet')).toBeInTheDocument();
        expect(screen.queryByText(/planned/i)).not.toBeInTheDocument();
        expect(screen.getAllByText('GAP').length).toBeGreaterThan(0);
    });

    it('renders the real payload as a chart and table, not a placeholder', async () => {
        api.getCrossReference.mockResolvedValue({ checks: [REAL_CHECK] });

        render(<CrossReference />);

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });

        expect(screen.getByText('LIVE')).toBeInTheDocument();
        expect(screen.getByText('Divergence Matrix')).toBeInTheDocument();

        fireEvent.click(screen.getByText('Checks (1)'));
        expect(screen.getByText('GDP vs Night Lights (China)')).toBeInTheDocument();
        expect(screen.queryByText(/planned/i)).not.toBeInTheDocument();
    });
});
