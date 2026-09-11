import React from 'react';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import SystemLogs from '../views/SystemLogs.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getLogs: vi.fn(),
        getConfig: vi.fn(),
        getSources: vi.fn(),
        updateSource: vi.fn(),
    },
}));

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('SystemLogs view async data', () => {
    beforeEach(() => {
        api.getLogs.mockReset();
    });

    it('shows a loading skeleton while fetching, then renders the log lines', async () => {
        const gate = deferred();
        api.getLogs.mockImplementation(() => gate.promise);

        render(<SystemLogs />);

        expect(screen.getAllByTestId('loading-skeleton').length).toBeGreaterThan(0);

        gate.resolve({ logs: ['2026-09-10 INFO boot complete'] });

        await waitFor(() => {
            expect(screen.queryByTestId('loading-skeleton')).not.toBeInTheDocument();
        });
        expect(screen.getByText('2026-09-10 INFO boot complete')).toBeInTheDocument();
    });

    it('shows an error state on failure and refetches on retry', async () => {
        api.getLogs.mockResolvedValueOnce({ error: true, message: 'log stream unavailable' });

        render(<SystemLogs />);

        expect(await screen.findByText('log stream unavailable')).toBeInTheDocument();
        const retryBtn = screen.getByRole('button', { name: 'Retry' });

        api.getLogs.mockResolvedValueOnce({ logs: [] });
        fireEvent.click(retryBtn);

        await waitFor(() => {
            expect(api.getLogs).toHaveBeenCalledTimes(2);
        });
        expect(screen.queryByText('log stream unavailable')).not.toBeInTheDocument();
    });
});
