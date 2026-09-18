import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Operator from '../views/Operator.jsx';
import { api } from '../api.js';

// Regression test for the Operator view crashing when the backend returns
// api.js's error-marker object ({ error: true, status, message }) instead
// of the expected { issues: [...] } / { snapshots: [...] } shape (or an
// array). Operator.jsx used `issuesRes?.issues || issuesRes || []` and
// `cyclesRes?.snapshots || cyclesRes || []`, which resolves to the truthy,
// non-array marker object itself, so `issues.map` / `recentCycles.map`
// later throw "issues.map is not a function" and the whole view unmounts.

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

describe('Operator view survives API error-marker responses', () => {
    beforeEach(() => {
        api.getStatus.mockReset();
        api.getHermesStatus.mockReset();
        api.getOperatorIssues.mockReset();
        api.getSnapshotLatest.mockReset();
        api.getHealth.mockReset();
        api.getFreshness.mockReset();

        api.getStatus.mockResolvedValue({ database: { connected: true } });
        api.getHermesStatus.mockResolvedValue({ running: true, operator_state: {} });
        api.getHealth.mockResolvedValue(null);
        api.getFreshness.mockResolvedValue(null);

        // Both list-bearing endpoints resolve to the api.js error marker
        // instead of throwing or returning the expected shape.
        api.getOperatorIssues.mockResolvedValue({
            error: true,
            status: 500,
            message: 'Issues backend exploded',
        });
        api.getSnapshotLatest.mockResolvedValue({
            error: true,
            status: 503,
            message: 'Cycle store offline',
        });
    });

    it('does not throw and shows per-panel unavailable states instead of crashing', async () => {
        expect(() => render(<Operator />)).not.toThrow();

        // Other panels (Hermes status, built from getStatus/getHermesStatus)
        // still render normally.
        await waitFor(() => {
            expect(screen.getByText('HERMES STATUS')).toBeInTheDocument();
            expect(screen.getByText('ONLINE')).toBeInTheDocument();
        });

        // The issues panel shows an honest error instead of crashing on
        // issues.map when issues holds the error-marker object.
        await waitFor(() => {
            expect(screen.getByText(/Issues unavailable/i)).toBeInTheDocument();
            expect(screen.getByText(/Issues backend exploded/i)).toBeInTheDocument();
        });

        // The recent-cycles panel shows an honest error instead of crashing
        // on recentCycles.map when recentCycles holds the error-marker object.
        await waitFor(() => {
            expect(screen.getByText(/Cycle history unavailable/i)).toBeInTheDocument();
            expect(screen.getByText(/Cycle store offline/i)).toBeInTheDocument();
        });
    });
});
