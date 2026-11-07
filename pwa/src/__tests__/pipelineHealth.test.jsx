import React from 'react';
import { fireEvent, render, screen, waitFor } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import PipelineHealth, { matchesStatusFilter } from '../views/PipelineHealth.jsx';
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

// 2026-11-07: per-source status comes from pull_log as well as raw_series.
// The summary keeps healthy/stale/broken (broken = failing + stopped) and
// adds per-status counts; old backends without them still render.
describe('PipelineHealth pull_log run states', () => {
    const now = Date.now();
    const row = (name, status, extra = {}) => ({
        name,
        type: 'unknown',
        status,
        last_pull: null,
        rows_last_pull: 0,
        next_scheduled: null,
        freshness: 'red',
        series_count: null,
        field_record: { availability: 'unavailable', provenance: null, stale_reason: 'never_configured' },
        last_run_at: null,
        last_status: null,
        runs_7d: 0,
        ...extra,
    });
    const detailedResponse = {
        summary: {
            total_sources: 4, healthy: 1, stale: 0, broken: 2,
            quiet: 0, skipped_config: 0, failing: 1, stopped: 1, inactive: 1,
            active_sources: 3,
        },
        sources: [
            row('yfinance_options', 'healthy', {
                freshness: 'green',
                last_run_at: new Date(now - 2 * 3600 * 1000).toISOString(),
                last_status: 'SUCCESS',
                runs_7d: 9,
            }),
            row('DARKPOOL', 'failing', {
                last_run_at: new Date(now - 3600 * 1000).toISOString(),
                last_status: 'FAILED',
                runs_7d: 3,
                error: 'TIMEOUT: Exceeded 60s timeout',
                last_error: 'TIMEOUT: Exceeded 60s timeout',
            }),
            row('TradingView', 'stopped'),
            row('ALPHA_VANTAGE', 'inactive', { freshness: 'grey' }),
        ],
        coverage: {},
        recent_errors: [],
        resolver_status: {},
        availability: 'available',
        stale_reason: null,
        status_basis: 'pull_log+raw_series',
    };

    it('renders the per-status tiles and filters', async () => {
        api.getPipelineHealth.mockResolvedValue(detailedResponse);
        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('Inactive')).toBeTruthy();
        });
        expect(screen.getByText('Quiet')).toBeTruthy();
        expect(screen.getByText('Skipped (config)')).toBeTruthy();
        expect(screen.getByText('1 failing · 1 stopped')).toBeTruthy();
        expect(screen.getByText('Failing (1)')).toBeTruthy();
        expect(screen.getByText('Stopped (1)')).toBeTruthy();
        expect(screen.getByText('Inactive (1)')).toBeTruthy();
        expect(screen.getByRole('columnheader', { name: /Last Run/ })).toBeTruthy();
        expect(screen.getByText('Runs 7d')).toBeTruthy();
        // The failing source's (backend-redacted) last error is shown.
        expect(screen.getByText('TIMEOUT: Exceeded 60s timeout')).toBeTruthy();
    });

    it('filters to one status', async () => {
        api.getPipelineHealth.mockResolvedValue(detailedResponse);
        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('DARKPOOL')).toBeTruthy();
        });
        fireEvent.click(screen.getByText('Failing (1)'));
        expect(screen.getByText('DARKPOOL')).toBeTruthy();
        expect(screen.queryByText('TradingView')).toBeNull();
        expect(screen.queryByText('ALPHA_VANTAGE')).toBeNull();
    });

    it('keeps the legacy three filters for an older backend', async () => {
        api.getPipelineHealth.mockResolvedValue({
            ...detailedResponse,
            summary: { total_sources: 1, healthy: 0, stale: 0, broken: 1 },
            sources: [row('acme', 'broken')],
        });
        render(<PipelineHealth />);

        await waitFor(() => {
            expect(screen.getByText('Broken (1)')).toBeTruthy();
        });
        expect(screen.queryByText('Inactive')).toBeNull();
        expect(screen.queryByRole('columnheader', { name: /Last Run/ })).toBeNull();
    });

    it('matches the legacy broken filter to failing and stopped', () => {
        expect(matchesStatusFilter({ status: 'failing' }, 'broken')).toBe(true);
        expect(matchesStatusFilter({ status: 'stopped' }, 'broken')).toBe(true);
        expect(matchesStatusFilter({ status: 'broken' }, 'broken')).toBe(true);
        expect(matchesStatusFilter({ status: 'inactive' }, 'broken')).toBe(false);
        expect(matchesStatusFilter({ status: 'quiet' }, 'quiet')).toBe(true);
        expect(matchesStatusFilter({ status: 'quiet' }, 'all')).toBe(true);
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
