import React from 'react';
import { render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import PipelineHealth from '../views/PipelineHealth.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        getPipelineHealth: vi.fn(),
    },
}));

describe('PipelineHealth availability contract', () => {
    it('renders an honest unavailable banner instead of zero counts when the backend cannot compute', async () => {
        api.getPipelineHealth.mockResolvedValue({
            summary: { total_sources: 0, healthy: 0, stale: 0, broken: 0 },
            sources: [],
            coverage: {},
            recent_errors: [],
            resolver_status: {},
            availability: 'unavailable',
            stale_reason: 'fetch_failed',
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText(/Pipeline health unavailable/)).toBeTruthy();
        });
        expect(screen.getByText(/fetch failed/)).toBeTruthy();
        // The old "0 sources / 0 healthy" summary tiles must not render in
        // this state -- there is no "Total Sources" label to find.
        expect(screen.queryByText('Total Sources')).toBeNull();
    });

    it('renders normal summary tiles when available', async () => {
        api.getPipelineHealth.mockResolvedValue({
            summary: { total_sources: 1, healthy: 1, stale: 0, broken: 0 },
            sources: [{
                name: 'yfinance',
                type: 'market',
                status: 'healthy',
                last_pull: new Date().toISOString(),
                rows_last_pull: 42,
                next_scheduled: null,
                freshness: 'green',
                series_count: 10,
                field_record: {
                    availability: 'available',
                    provenance: 'measured',
                    stale_reason: null,
                },
            }],
            coverage: {},
            recent_errors: [],
            resolver_status: {},
            availability: 'available',
            stale_reason: null,
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('Total Sources')).toBeTruthy();
        });
        expect(screen.queryByText(/Pipeline health unavailable/)).toBeNull();
    });

    it('shows the stale_reason column text per source row', async () => {
        api.getPipelineHealth.mockResolvedValue({
            summary: { total_sources: 2, healthy: 0, stale: 1, broken: 1 },
            sources: [
                {
                    name: 'Fed_Liquidity',
                    type: 'macro',
                    status: 'stale',
                    last_pull: new Date(Date.now() - 3 * 24 * 3600 * 1000).toISOString(),
                    rows_last_pull: 3,
                    next_scheduled: null,
                    freshness: 'yellow',
                    series_count: 2,
                    field_record: {
                        availability: 'available',
                        provenance: 'measured',
                        stale_reason: 'stale',
                    },
                },
                {
                    name: 'acme_widgets',
                    type: 'unknown',
                    status: 'broken',
                    last_pull: null,
                    rows_last_pull: 0,
                    next_scheduled: null,
                    freshness: 'red',
                    series_count: 0,
                    field_record: {
                        availability: 'unavailable',
                        provenance: null,
                        stale_reason: 'never_configured',
                    },
                },
            ],
            coverage: {},
            recent_errors: [],
            resolver_status: {},
            availability: 'available',
            stale_reason: null,
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('Fed_Liquidity')).toBeTruthy();
        });
        // Each row's stale_reason is rendered as human text, not the raw enum.
        expect(screen.getAllByText('stale').length).toBeGreaterThan(0);
        expect(screen.getByText(/never configured/)).toBeTruthy();
    });
});

// F3: pipeline-health now carries a `daily_audit` snapshot of the
// pre-computed data_freshness_audit table, independent of the live
// sources/coverage sections above. It must never be presented as current
// when the underlying daily run is stale or missing.
describe('PipelineHealth daily freshness audit panel', () => {
    const baseResponse = {
        summary: { total_sources: 0, healthy: 0, stale: 0, broken: 0 },
        sources: [],
        coverage: {},
        recent_errors: [],
        resolver_status: {},
        availability: 'available',
        stale_reason: null,
    };

    it('shows bucket counts and the as-of time when the audit is fresh', async () => {
        api.getPipelineHealth.mockResolvedValue({
            ...baseResponse,
            daily_audit: {
                audited_at: new Date(Date.now() - 2 * 3600 * 1000).toISOString(),
                total_tickers: 718,
                buckets: [
                    { bucket: 'FRESH', ticker_count: 118 },
                    { bucket: 'DEAD', ticker_count: 217 },
                ],
                source_tables: ['ticker_metrics_daily'],
                availability: 'available',
                stale_reason: null,
            },
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('DAILY FRESHNESS AUDIT')).toBeTruthy();
        });
        expect(screen.getByText(/718 tickers/)).toBeTruthy();
        expect(screen.getByText('118')).toBeTruthy();
        expect(screen.getByText('217')).toBeTruthy();
        expect(screen.queryByText(/Audit unavailable/)).toBeNull();
    });

    it('renders an honest unavailable state instead of the buckets when the audit is stale', async () => {
        api.getPipelineHealth.mockResolvedValue({
            ...baseResponse,
            daily_audit: {
                audited_at: new Date(Date.now() - 72 * 3600 * 1000).toISOString(),
                total_tickers: 700,
                buckets: [{ bucket: 'DEAD', ticker_count: 700 }],
                source_tables: ['ticker_metrics_daily'],
                availability: 'unavailable',
                stale_reason: 'stale',
            },
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText(/Audit unavailable: stale/)).toBeTruthy();
        });
    });

    it('renders the never-configured state when the audit table has no rows', async () => {
        api.getPipelineHealth.mockResolvedValue({
            ...baseResponse,
            daily_audit: {
                audited_at: null,
                total_tickers: 0,
                buckets: [],
                source_tables: [],
                availability: 'unavailable',
                stale_reason: 'never_configured',
            },
        });

        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText(/Audit unavailable: never configured/)).toBeTruthy();
        });
    });

    it('does not crash when the backend response predates the daily_audit field', async () => {
        api.getPipelineHealth.mockResolvedValue({ ...baseResponse });

        expect(() => render(<PipelineHealth />)).not.toThrow();

        await waitFor(() => {
            expect(screen.getByText('No audit data.')).toBeTruthy();
        });
    });
});
