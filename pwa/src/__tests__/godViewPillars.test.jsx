import React from 'react';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

const { default: GodViewPillars } = await import('../views/GodViewPillars.jsx');

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

describe('GodViewPillars view', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('shows a loading state while the CFTC pillar request is in flight', async () => {
        const gate = deferred();
        api.get.mockReturnValue(gate.promise);

        render(<GodViewPillars />);

        expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'loading');

        gate.resolve({ available: false, reason: 'never_configured' });
        await waitFor(() =>
            expect(screen.getByTestId('cftc-pillar-card')).not.toHaveAttribute('data-state', 'loading')
        );
    });

    it('renders the honest unavailable state for never_configured', async () => {
        api.get.mockResolvedValue({ available: false, status: 'unavailable', reason: 'never_configured', as_of: null });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'unavailable'));
        expect(screen.getByText(/never_configured/)).toBeInTheDocument();
    });

    it('renders an available generation with coverage, provenance badges, and field timestamps', async () => {
        api.get.mockResolvedValue({
            available: true,
            status: 'ok',
            pillar: 'cftc_positioning',
            as_of: '2026-09-18',
            coverage: 1.0,
            contracts_with_data: 4,
            contracts_expected: 4,
            stale_reason: null,
            generation_id: 'gen-abc-123',
            generation_published_at: '2026-09-18T12:00:00+00:00',
            contracts: { SP500: 'ES', NOTE10Y: 'ZN', GOLD: 'GC', CRUDE_OIL: 'CL' },
            fields: {
                ES: {
                    total_open_interest: {
                        availability: 'available', provenance: 'measured', value: 2500000, unit: 'contracts',
                        published_at: '2026-09-05', available_at: '2026-09-06T10:00:00+00:00', ingested_at: null,
                    },
                    noncommercial_net: {
                        availability: 'available', provenance: 'measured', value: 12345, unit: 'contracts',
                        published_at: '2026-09-05', available_at: '2026-09-06T10:00:00+00:00', ingested_at: null,
                    },
                    spec_net_pct_oi: {
                        availability: 'available', provenance: 'measured', value: 4.2, unit: 'pct',
                        published_at: '2026-09-05', available_at: null, ingested_at: null,
                    },
                    z_score_1y: {
                        availability: 'available', provenance: 'derived', value: 1.8, unit: 'zscore',
                        published_at: '2026-09-05', available_at: null, ingested_at: null,
                    },
                    z_score_3y: {
                        availability: 'unavailable', provenance: null, value: null, unit: 'zscore',
                        stale_reason: 'partial_history', published_at: null, available_at: null, ingested_at: null,
                    },
                    percentile_3y: {
                        availability: 'unavailable', provenance: null, value: null, unit: 'pct',
                        stale_reason: 'partial_history', published_at: null, available_at: null, ingested_at: null,
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        expect(screen.getByText(/SP500 \(ES\)/)).toBeInTheDocument();
        expect(screen.getAllByText('measured').length).toBeGreaterThan(0);
        expect(screen.getAllByText('derived').length).toBeGreaterThan(0);
        expect(screen.getAllByText('unavailable').length).toBeGreaterThan(0);
        expect(screen.getByText(/gen-abc-123/)).toBeInTheDocument();
    });

    it('renders a partial-coverage generation honestly (coverage < 1, no fabricated 100%)', async () => {
        api.get.mockResolvedValue({
            available: true,
            status: 'partial',
            as_of: '2026-09-18',
            coverage: 0.5,
            contracts_with_data: 2,
            contracts_expected: 4,
            stale_reason: null,
            generation_id: 'gen-partial',
            generation_published_at: '2026-09-18T12:00:00+00:00',
            contracts: { SP500: 'ES', NOTE10Y: 'ZN', GOLD: 'GC', CRUDE_OIL: 'CL' },
            fields: {},
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        expect(screen.getByText(/2\/4 contracts/)).toBeInTheDocument();
    });

    it('renders a stale_reason banner without hiding the underlying data', async () => {
        api.get.mockResolvedValue({
            available: true,
            status: 'partial',
            as_of: '2026-09-30',
            coverage: 1.0,
            contracts_with_data: 4,
            contracts_expected: 4,
            stale_reason: 'stale',
            generation_id: 'gen-stale',
            generation_published_at: '2026-09-05T12:00:00+00:00',
            contracts: { SP500: 'ES', NOTE10Y: 'ZN', GOLD: 'GC', CRUDE_OIL: 'CL' },
            fields: {},
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByText(/stale: stale/)).toBeInTheDocument());
    });

    it('surfaces a request failure as an error state, not a silent blank card', async () => {
        api.get.mockRejectedValue(new Error('network down'));

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'error'));
        expect(screen.getByText(/network down/)).toBeInTheDocument();
    });

    it('always renders the not-built-yet cards for the other five pillars', async () => {
        api.get.mockResolvedValue({ available: false, reason: 'never_configured' });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getAllByTestId('pillar-card-not-built').length).toBe(6));
        expect(screen.getAllByText('not built yet — no data').length).toBe(6);
    });

    it('shows an inferred badge for a field whose availability_basis is inferred_schedule', async () => {
        api.get.mockResolvedValue({
            available: true,
            status: 'partial',
            as_of: '2026-09-18',
            include_inferred: true,
            coverage: 1.0,
            contracts_with_data: 4,
            contracts_expected: 4,
            stale_reason: null,
            generation_id: 'gen-inferred',
            generation_published_at: '2026-09-18T12:00:00+00:00',
            contracts: { SP500: 'ES', NOTE10Y: 'ZN', GOLD: 'GC', CRUDE_OIL: 'CL' },
            fields: {
                ES: {
                    total_open_interest: {
                        availability: 'available', provenance: 'measured', value: 2500000, unit: 'contracts',
                        published_at: '2026-01-09', available_at: '2026-03-20T10:00:00+00:00', ingested_at: null,
                        availability_basis: 'inferred_schedule',
                        availability_basis_note: 'availability inferred from schedule; record revised/backfilled',
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        expect(screen.getByTestId('inferred-badge')).toBeInTheDocument();
        expect(screen.getByTestId('inferred-badge')).toHaveTextContent('inferred');
    });

    it('re-fetches with include_inferred=true when the toggle is checked', async () => {
        api.get.mockResolvedValue({ available: false, reason: 'never_configured' });

        render(<GodViewPillars />);

        await waitFor(() => expect(api.get).toHaveBeenCalledWith(expect.stringContaining('include_inferred=false')));

        fireEvent.click(screen.getByTestId('include-inferred-toggle'));

        await waitFor(() => expect(api.get).toHaveBeenCalledWith(expect.stringContaining('include_inferred=true')));
    });
});
