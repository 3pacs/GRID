import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import Operator from '../views/Operator.jsx';
import { api } from '../api.js';

// F3: /system/freshness now carries a `daily_audit` snapshot of the
// pre-computed data_freshness_audit table (see
// api/routers/system.py::_read_daily_freshness_audit). These tests pin
// Operator.jsx's new "DAILY AUDIT" panel: an honest available/unavailable
// state, never a silently-missing section when the field is present.

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

describe('Operator daily audit panel', () => {
    beforeEach(() => {
        api.getStatus.mockReset();
        api.getHermesStatus.mockReset();
        api.getOperatorIssues.mockReset();
        api.getSnapshotLatest.mockReset();
        api.getHealth.mockReset();
        api.getFreshness.mockReset();

        api.getStatus.mockResolvedValue({ database: { connected: true } });
        api.getHermesStatus.mockResolvedValue({ running: true, operator_state: {} });
        api.getOperatorIssues.mockResolvedValue([]);
        api.getSnapshotLatest.mockResolvedValue([]);
        api.getHealth.mockResolvedValue(null);
    });

    it('shows the audit tickers-audited count when available', async () => {
        api.getFreshness.mockResolvedValue({
            families: [],
            overall_status: 'RED',
            daily_audit: {
                audited_at: '2026-09-26T07:13:05.902429+00:00',
                total_tickers: 718,
                buckets: [{ bucket: 'FRESH', ticker_count: 118 }],
                source_tables: ['ticker_metrics_daily'],
                availability: 'available',
                stale_reason: null,
            },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText('DAILY AUDIT')).toBeInTheDocument();
        });
        expect(screen.getByText(/718 tickers audited/)).toBeInTheDocument();
        expect(screen.queryByText(/Unavailable/)).not.toBeInTheDocument();
    });

    it('shows an honest unavailable reason instead of stale numbers when the audit is overdue', async () => {
        api.getFreshness.mockResolvedValue({
            families: [],
            overall_status: 'RED',
            daily_audit: {
                audited_at: '2026-09-20T05:00:00+00:00',
                total_tickers: 700,
                buckets: [{ bucket: 'DEAD', ticker_count: 700 }],
                source_tables: ['ticker_metrics_daily'],
                availability: 'unavailable',
                stale_reason: 'stale',
            },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText(/Unavailable \(audit run is overdue\)/)).toBeInTheDocument();
        });
    });

    it('shows the never-run reason when the audit table has no rows at all', async () => {
        api.getFreshness.mockResolvedValue({
            families: [],
            overall_status: 'RED',
            daily_audit: {
                audited_at: null,
                total_tickers: 0,
                buckets: [],
                source_tables: [],
                availability: 'unavailable',
                stale_reason: 'never_configured',
            },
        });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText(/Unavailable \(audit has never run\)/)).toBeInTheDocument();
        });
    });

    it('renders no daily audit panel when the backend response predates the field', async () => {
        api.getFreshness.mockResolvedValue({ families: [], overall_status: 'RED' });

        render(<Operator />);

        await waitFor(() => {
            expect(screen.getByText('HERMES STATUS')).toBeInTheDocument();
        });
        expect(screen.queryByText('DAILY AUDIT')).not.toBeInTheDocument();
    });
});
