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
