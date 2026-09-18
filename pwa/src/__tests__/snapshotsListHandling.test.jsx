import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Snapshots from '../views/Snapshots.jsx';
import { api } from '../api.js';

// Regression tests for Snapshots.jsx's list handling.
//
// api/routers/snapshots.py::get_latest_snapshots (:29-48) and
// ::get_snapshot_history (:51-63) both return a BARE array (list[dict]) —
// never `{snapshots: [...]}`. api.js's request helper never throws on
// network/HTTP/parse failure; it resolves an `{ error: true, status,
// message }` marker instead. The old code did
// `historyRes?.snapshots || historyRes || []` and
// `latestRes?.snapshots?.[0] || latestRes || null`, which:
//   - for the real bare-array contract, `res?.snapshots` is always
//     undefined, so history silently fell through to the raw array (this
//     one happened to work) while LATEST fell through to the *whole array*
//     instead of its first element (rendering a dash where a real date
//     belongs);
//   - for the error marker, the truthy non-array object flowed straight
//     into state and got treated as if it were snapshot data.

vi.mock('../api.js', () => ({
    api: {
        getSnapshotLatest: vi.fn(),
        getSnapshotHistory: vi.fn(),
        compareSnapshots: vi.fn(),
    },
}));

const SNAP_A = {
    id: 1,
    created_at: '2026-09-10T12:00:00',
    payload: { silhouette_score: 0.42, n_clusters: 5 },
};
const SNAP_B = {
    id: 2,
    created_at: '2026-09-11T12:00:00',
    payload: { silhouette_score: 0.48, n_clusters: 5 },
};

describe('Snapshots list handling per the real API contract', () => {
    beforeEach(() => {
        api.getSnapshotLatest.mockReset();
        api.getSnapshotHistory.mockReset();
        api.compareSnapshots.mockReset();
    });

    it('takes the first element of a bare array for LATEST and renders its date (not a dash)', async () => {
        api.getSnapshotLatest.mockResolvedValue([SNAP_B]);
        api.getSnapshotHistory.mockResolvedValue([SNAP_A, SNAP_B]);

        render(<Snapshots />);

        await waitFor(() => {
            expect(screen.getByText('LATEST SNAPSHOT')).toBeInTheDocument();
        });

        // The real snapshot date must render, not the "-" fallback that
        // shows when `latest` ends up holding the whole array (which has no
        // `.created_at`/`.snapshot_date` of its own). It appears twice here
        // (LATEST card + its HISTORY row).
        expect(screen.getAllByText('2026-09-11 12:00:00').length).toBe(2);
        expect(screen.queryByText('-')).not.toBeInTheDocument();
    });

    it('renders a bare array of two history entries directly (no unwrapping needed)', async () => {
        api.getSnapshotLatest.mockResolvedValue([SNAP_B]);
        api.getSnapshotHistory.mockResolvedValue([SNAP_A, SNAP_B]);

        render(<Snapshots />);

        await waitFor(() => {
            expect(screen.getByText('2026-09-10 12:00:00')).toBeInTheDocument();
        });
        // SNAP_B's date appears twice: once in the LATEST card, once in HISTORY.
        expect(screen.getAllByText('2026-09-11 12:00:00').length).toBe(2);
        expect(screen.queryByText(/No snapshots found/)).not.toBeInTheDocument();
    });

    it('shows "No snapshots found for <category>" for a genuinely empty array', async () => {
        api.getSnapshotLatest.mockResolvedValue([]);
        api.getSnapshotHistory.mockResolvedValue([]);

        render(<Snapshots />);

        await waitFor(() => {
            expect(screen.getByText(/No snapshots found for clustering/)).toBeInTheDocument();
        });
        expect(screen.queryByText(/Snapshots unavailable/)).not.toBeInTheDocument();
        expect(screen.queryByText('Unexpected response')).not.toBeInTheDocument();
    });

    it('shows "Snapshots unavailable: <message>" for the api.js error marker, not "No snapshots found"', async () => {
        api.getSnapshotLatest.mockResolvedValue({ error: true, status: 500, message: 'DB pool exhausted' });
        api.getSnapshotHistory.mockResolvedValue({ error: true, status: 500, message: 'DB pool exhausted' });

        render(<Snapshots />);

        await waitFor(() => {
            expect(screen.getAllByText('Snapshots unavailable: DB pool exhausted').length).toBeGreaterThan(0);
        });
        expect(screen.queryByText(/No snapshots found/)).not.toBeInTheDocument();
    });

    it('shows an explicit "Unexpected response" state for a malformed, non-array, non-error value', async () => {
        api.getSnapshotLatest.mockResolvedValue({ weird: 'shape' });
        api.getSnapshotHistory.mockResolvedValue({ weird: 'shape' });

        render(<Snapshots />);

        await waitFor(() => {
            expect(screen.getAllByText('Unexpected response').length).toBeGreaterThan(0);
        });
        expect(screen.queryByText(/No snapshots found/)).not.toBeInTheDocument();
    });

    it('shows the compare 404 message instead of silently rendering a blank comparison', async () => {
        api.getSnapshotLatest.mockResolvedValue([]);
        api.getSnapshotHistory.mockResolvedValue([]);
        api.compareSnapshots.mockResolvedValue({
            error: true,
            status: 404,
            message: 'No snapshot found for category "clustering" on 2026-01-01',
        });

        render(<Snapshots />);
        await waitFor(() => {
            expect(screen.getByText(/No snapshots found for clustering/)).toBeInTheDocument();
        });

        // Only the toggle button reads "Compare" before compare mode is on.
        fireEvent.click(screen.getByText('Compare'));

        const dateInputs = await waitFor(() => {
            const inputs = document.querySelectorAll('input[type="date"]');
            expect(inputs.length).toBe(2);
            return inputs;
        });
        fireEvent.change(dateInputs[0], { target: { value: '2026-01-01' } });
        fireEvent.change(dateInputs[1], { target: { value: '2026-01-08' } });

        // Now the submit button ("Compare") is enabled alongside the toggle
        // ("Compare: ON") — pick the actual button element that isn't disabled.
        const runButton = screen.getAllByText('Compare').find(el => el.tagName === 'BUTTON' && !el.disabled);
        fireEvent.click(runButton);

        await waitFor(() => {
            expect(screen.getByText('No snapshot found for category "clustering" on 2026-01-01')).toBeInTheDocument();
        });
    });
});
