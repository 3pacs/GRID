import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Operator from '../views/Operator.jsx';
import { api } from '../api.js';

// Regression test for Operator.jsx's "SERVER RESOURCES" DB line.
//
// api/schemas/system.py::DatabaseStatus defaults `size_mb` to 0.0 (not
// null) and `connected` to False. Operator.jsx rendered:
//   DB: {connected ? 'Connected' : 'Disconnected'}
//   {size_mb && ` · ${(size_mb / 1024).toFixed(1)}GB`}
// `size_mb && ...` is falsy-but-truthy-checked on the raw number: when the
// DB is disconnected and size_mb is the real 0.0 default, `0 && expr`
// evaluates to `0`, which React renders as a stray "0" text node —
// "DB: Disconnected0" in the real fixture-backed browser run.

vi.mock('../api.js', () => ({
    api: {
        getStatus: vi.fn(),
        getHermesStatus: vi.fn(),
        getOperatorIssues: vi.fn(),
        getSnapshotLatest: vi.fn(),
        getHealth: vi.fn(),
        getFreshness: vi.fn(),
    },
}));

describe('Operator SERVER RESOURCES DB line', () => {
    beforeEach(() => {
        api.getStatus.mockReset();
        api.getHermesStatus.mockReset();
        api.getOperatorIssues.mockReset();
        api.getSnapshotLatest.mockReset();
        api.getHealth.mockReset();
        api.getFreshness.mockReset();

        api.getHermesStatus.mockResolvedValue({ running: true, operator_state: {} });
        api.getHealth.mockResolvedValue(null);
        api.getFreshness.mockResolvedValue(null);
        api.getOperatorIssues.mockResolvedValue({ issues: [] });
        api.getSnapshotLatest.mockResolvedValue({ snapshots: [] });
    });

    it('renders exactly "DB: Disconnected" with no stray number when disconnected with the real size_mb=0 default', async () => {
        api.getStatus.mockResolvedValue({
            server: {},
            database: { connected: false, size_mb: 0 },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText(/^DB: /)).toBeInTheDocument();
        });

        const dbLine = screen.getByText(/^DB: /);
        expect(dbLine.textContent).toBe('DB: Disconnected');
        expect(dbLine.textContent).not.toMatch(/0$/);
    });

    it('renders "0.0GB" honestly when connected with a measured size_mb of 0', async () => {
        api.getStatus.mockResolvedValue({
            server: {},
            database: { connected: true, size_mb: 0 },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText(/^DB: /)).toBeInTheDocument();
        });

        const dbLine = screen.getByText(/^DB: /);
        expect(dbLine.textContent).toBe('DB: Connected · 0.0GB');
    });

    it('renders plain "DB: Connected" with no size suffix when size_mb is absent', async () => {
        api.getStatus.mockResolvedValue({
            server: {},
            database: { connected: true },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText(/^DB: /)).toBeInTheDocument();
        });

        const dbLine = screen.getByText(/^DB: /);
        expect(dbLine.textContent).toBe('DB: Connected');
    });
});
