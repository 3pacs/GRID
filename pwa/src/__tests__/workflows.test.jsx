import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Workflows from '../views/Workflows.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getWorkflows: vi.fn(),
        getWorkflowWaves: vi.fn(),
        getWorkflowSchedule: vi.fn(),
        enableWorkflow: vi.fn(),
        disableWorkflow: vi.fn(),
        runWorkflow: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('Workflows view async data', () => {
    beforeEach(() => {
        api.getWorkflows.mockReset();
        api.getWorkflowWaves.mockReset();
        api.getWorkflowSchedule.mockReset();
        api.getWorkflowWaves.mockResolvedValue({ total_waves: 0, waves: [] });
        api.getWorkflowSchedule.mockResolvedValue({ total: 0, schedules: [] });
    });

    it('shows a loading skeleton while fetching, then renders the workflow list', async () => {
        const gate = deferred();
        api.getWorkflows.mockImplementation(() => gate.promise);

        render(<Workflows />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ workflows: [{ name: 'ingest_fred', group: 'ingestion', enabled: true, description: 'Pull FRED series' }] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('ingest_fred')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getWorkflows.mockResolvedValueOnce({ error: true, message: 'workflow registry unavailable' });

        render(<Workflows />);

        expect(await screen.findByText('workflow registry unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getWorkflows.mockResolvedValueOnce({ workflows: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getWorkflows).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('workflow registry unavailable')).not.toBeInTheDocument();
    });
});
