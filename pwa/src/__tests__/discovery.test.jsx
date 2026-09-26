import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Discovery, { ResearchRunPanel } from '../views/Discovery.jsx';
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
        // ResearchRunPanel (GRID W4c) calls the generic GET helper directly —
        // no dedicated named method, since api.js is claimed by another lane.
        get: vi.fn(),
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
        api.get.mockReset();
        api.get.mockResolvedValue({ status: 'no_runs' });
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

    it('keeps a populated orthogonality result visible when clustering fails', async () => {
        api.getJobs.mockResolvedValue({ jobs: [] });
        api.getResults.mockImplementation((type) => type === 'clustering'
            ? Promise.reject(new Error('503'))
            : Promise.resolve({ result: {
                n_features_analyzed: 7,
                true_dimensionality: 3,
                highly_correlated_pairs: [],
                as_of_date: '2026-09-19',
            } }));

        render(<Discovery />);

        expect(await screen.findByText('Clustering result unavailable.')).toBeInTheDocument();
        expect(screen.getByText('Features analyzed')).toBeInTheDocument();
        expect(screen.getByText('As of 2026-09-19')).toBeInTheDocument();
        expect(screen.queryByText('No completed clustering run found.')).not.toBeInTheDocument();
    });

    it('keeps a populated clustering result visible when orthogonality fails', async () => {
        api.getJobs.mockResolvedValue({ jobs: [] });
        api.getResults.mockImplementation((type) => type === 'orthogonality'
            ? Promise.reject(new Error('503'))
            : Promise.resolve({ result: {
                best_k: 4,
                pca_components_used: 2,
                variance_explained: 0.8,
            } }));

        render(<Discovery />);

        expect(await screen.findByText('Orthogonality result unavailable.')).toBeInTheDocument();
        expect(screen.getByText('Best k')).toBeInTheDocument();
        expect(screen.getByText('Result time unknown')).toBeInTheDocument();
    });

    it('shows checked-empty only for explicit null results and unavailable for invalid responses', async () => {
        api.getJobs.mockResolvedValue({ jobs: [] });
        api.getResults.mockImplementation((type) => Promise.resolve(type === 'orthogonality'
            ? { result: null }
            : { message: 'missing result field' }));

        render(<Discovery />);

        expect(await screen.findByText('No completed orthogonality audit found.')).toBeInTheDocument();
        expect(screen.getByText('Clustering result unavailable.')).toBeInTheDocument();
    });

    it('does not render a failed audit payload as a successful result', async () => {
        api.getJobs.mockResolvedValue({ jobs: [] });
        api.getResults.mockImplementation((type) => Promise.resolve(type === 'orthogonality'
            ? { result: { error: 'No eligible features', n_features_analyzed: 0 } }
            : { result: null }));

        render(<Discovery />);

        expect(await screen.findByText('Orthogonality result unavailable.')).toBeInTheDocument();
        expect(screen.getByText('No completed clustering run found.')).toBeInTheDocument();
        expect(screen.queryByText('Features analyzed')).not.toBeInTheDocument();
    });

    it('shows a settled result while the other section loads and preserves full job timestamps', async () => {
        const gate = deferred();
        api.getJobs.mockResolvedValue({ jobs: [{
            id: 'j1', type: 'clustering', status: 'complete',
            started: '2026-09-01T01:02:03Z', finished: '2026-09-01T01:04:05Z',
        }] });
        api.getResults.mockImplementation((type) => type === 'clustering' ? gate.promise : Promise.resolve({ result: null }));

        render(<Discovery />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);
        expect(await screen.findByText('No completed orthogonality audit found.')).toBeInTheDocument();
        expect(screen.getByText('Loading clustering result...')).toBeInTheDocument();
        gate.resolve({ result: null });
        expect(await screen.findByText('No completed clustering run found.')).toBeInTheDocument();
        expect(screen.getByText(/Started: 2026-09-01T01:02:03Z.*Finished: 2026-09-01T01:04:05Z/)).toBeInTheDocument();
    });
});

// GRID W4c — GET /api/v1/snapshots/research/latest, rendered by the
// self-contained ResearchRunPanel (api/routers/snapshots.py wraps
// scripts/research_status.py::latest_research_run_result).
describe('ResearchRunPanel', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('renders a failed run with its phase, error, and skip/failure reasons', async () => {
        api.get.mockResolvedValue({
            run_id: 'abc-123',
            status: 'failed',
            phase: 'feature_list',
            error: 'column "f.does_not_exist" does not exist',
            error_category: 'db_load_failure',
            iteration: null,
            iterations: 0,
            skip_reasons: ['fenced_before_iteration_2'],
            failure_reasons: ['backtest_error'],
            duration_s: 1.23,
            generation: 4,
            code_sha: 'deadbeefcafef00d',
            inputs: { feature_ids_count: 3, market_snapshot_keys: ['vix'], evaluation_version: null },
        });

        render(<ResearchRunPanel />);

        expect(await screen.findByText('FAILED')).toBeInTheDocument();
        expect(screen.getByText('abc-123')).toBeInTheDocument();
        expect(screen.getByText('phase: feature_list')).toBeInTheDocument();
        expect(screen.getByText('column "f.does_not_exist" does not exist')).toBeInTheDocument();
        expect(screen.getByText(/fenced_before_iteration_2/)).toBeInTheDocument();
        expect(screen.getByText(/backtest_error/)).toBeInTheDocument();
        expect(screen.getByText(/deadbeef/)).toBeInTheDocument();
        expect(screen.getByText(/eval version: —/)).toBeInTheDocument();
    });

    it('renders the honest "no research run recorded yet" state', async () => {
        api.get.mockResolvedValue({ status: 'no_runs' });

        render(<ResearchRunPanel />);

        expect(await screen.findByText('No research run recorded yet.')).toBeInTheDocument();
    });

    it('renders the honest "unavailable" state without throwing when the table is missing', async () => {
        api.get.mockResolvedValue({
            status: 'unavailable',
            reason: 'relation "analytical_snapshots" does not exist',
        });

        render(<ResearchRunPanel />);

        expect(await screen.findByText(
            'Research status unavailable: relation "analytical_snapshots" does not exist',
        )).toBeInTheDocument();
    });

    it('falls back to the unavailable state if the request itself rejects', async () => {
        api.get.mockRejectedValue(new Error('network error'));

        render(<ResearchRunPanel />);

        expect(await screen.findByText('Research status unavailable: network error')).toBeInTheDocument();
    });
});
