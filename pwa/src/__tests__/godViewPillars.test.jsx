import React from 'react';
import { render, screen, waitFor, fireEvent, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { api } from '../api.js';

vi.mock('../api.js', () => ({
    api: {
        get: vi.fn(),
    },
}));

const { default: GodViewPillars } = await import('../views/GodViewPillars.jsx');

const NEVER_CONFIGURED = { available: false, status: 'unavailable', reason: 'never_configured', as_of: null };

function deferred() {
    let resolve;
    const promise = new Promise((res) => { resolve = res; });
    return { promise, resolve };
}

/**
 * The view now fires three independent GET requests (cftc, fed_net_liquidity,
 * commodity_warehouses). Route the mock by path so a test about one pillar
 * doesn't leak its response into the other two cards' assertions.
 */
function mockPillars({ cftc, fed, cmdty, finra } = {}) {
    api.get.mockImplementation((path) => {
        if (path.includes('/pillars/cftc')) return Promise.resolve(cftc ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/fed_net_liquidity')) return Promise.resolve(fed ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/commodity_warehouses')) {
            return Promise.resolve(cmdty ?? { lme: NEVER_CONFIGURED, cushing_crude_stocks: NEVER_CONFIGURED });
        }
        if (path.includes('/pillars/finra_short_volume')) return Promise.resolve(finra ?? NEVER_CONFIGURED);
        return Promise.resolve(NEVER_CONFIGURED);
    });
}

describe('GodViewPillars view', () => {
    beforeEach(() => {
        api.get.mockReset();
    });

    it('shows a loading state while the CFTC pillar request is in flight', async () => {
        const gate = deferred();
        api.get.mockImplementation((path) => (path.includes('/pillars/cftc') ? gate.promise : Promise.resolve(NEVER_CONFIGURED)));

        render(<GodViewPillars />);

        expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'loading');

        gate.resolve(NEVER_CONFIGURED);
        await waitFor(() =>
            expect(screen.getByTestId('cftc-pillar-card')).not.toHaveAttribute('data-state', 'loading')
        );
    });

    it('renders the honest unavailable state for never_configured', async () => {
        mockPillars({ cftc: NEVER_CONFIGURED });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'unavailable'));
        expect(within(screen.getByTestId('cftc-pillar-card')).getByText(/never_configured/)).toBeInTheDocument();
    });

    it('renders an available generation with coverage, provenance badges, and field timestamps', async () => {
        mockPillars({
            cftc: {
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
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('cftc-pillar-card'));
        expect(card.getByText(/SP500 \(ES\)/)).toBeInTheDocument();
        expect(card.getAllByText('measured').length).toBeGreaterThan(0);
        expect(card.getAllByText('derived').length).toBeGreaterThan(0);
        expect(card.getAllByText('unavailable').length).toBeGreaterThan(0);
        expect(card.getByText(/gen-abc-123/)).toBeInTheDocument();
    });

    it('renders a partial-coverage generation honestly (coverage < 1, no fabricated 100%)', async () => {
        mockPillars({
            cftc: {
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
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        expect(within(screen.getByTestId('cftc-pillar-card')).getByText(/2\/4 contracts/)).toBeInTheDocument();
    });

    it('renders a stale_reason banner without hiding the underlying data', async () => {
        mockPillars({
            cftc: {
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
            },
        });

        render(<GodViewPillars />);

        await waitFor(() =>
            expect(within(screen.getByTestId('cftc-pillar-card')).getByText(/stale: stale/)).toBeInTheDocument()
        );
    });

    it('surfaces a request failure as an error state, not a silent blank card', async () => {
        api.get.mockImplementation((path) =>
            path.includes('/pillars/cftc') ? Promise.reject(new Error('network down')) : Promise.resolve(NEVER_CONFIGURED)
        );

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'error'));
        expect(within(screen.getByTestId('cftc-pillar-card')).getByText(/network down/)).toBeInTheDocument();
    });

    it('always renders the not-built-yet cards for the other three pillars, each with its own reason', async () => {
        mockPillars();

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getAllByTestId('pillar-card-not-built').length).toBe(3));
        expect(screen.getByText(/adapter exists but is unscheduled\/unverified live/)).toBeInTheDocument();
        expect(screen.getByText(/no measured source/)).toBeInTheDocument();
        expect(screen.getByText(/engine correctness unproven/)).toBeInTheDocument();
    });

    it('shows an inferred badge for a field whose availability_basis is inferred_schedule', async () => {
        mockPillars({
            cftc: {
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
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('cftc-pillar-card')).toHaveAttribute('data-state', 'available'));
        expect(screen.getByTestId('inferred-badge')).toBeInTheDocument();
        expect(screen.getByTestId('inferred-badge')).toHaveTextContent('inferred');
    });

    it('re-fetches with include_inferred=true when the toggle is checked', async () => {
        mockPillars();

        render(<GodViewPillars />);

        await waitFor(() => expect(api.get).toHaveBeenCalledWith(expect.stringContaining('include_inferred=false')));

        fireEvent.click(screen.getByTestId('include-inferred-toggle'));

        await waitFor(() => expect(api.get).toHaveBeenCalledWith(expect.stringContaining('include_inferred=true')));
    });

    it('renders the Fed net liquidity pillar with per-component and derived fields', async () => {
        mockPillars({
            fed: {
                available: true,
                status: 'ok',
                pillar: 'fed_net_liquidity',
                as_of: '2026-09-18',
                include_inferred: false,
                coverage: 1.0,
                stale_reason: null,
                generation_id: 'gen-fed-1',
                generation_published_at: '2026-09-18T12:00:00+00:00',
                fields: {
                    fed_assets_walcl: {
                        availability: 'available', provenance: 'measured', value: 7500000, unit: 'millions_usd',
                        availability_basis: 'observed_acquisition', availability_basis_note: null,
                    },
                    treasury_tga_wtregen: {
                        availability: 'available', provenance: 'measured', value: 700000, unit: 'millions_usd',
                        availability_basis: 'observed_acquisition', availability_basis_note: null,
                    },
                    reverse_repo_rrp: {
                        availability: 'available', provenance: 'measured', value: 300, unit: 'billions_usd',
                        availability_basis: 'observed_acquisition', availability_basis_note: null,
                    },
                    net_liquidity_usd_m: {
                        availability: 'available', provenance: 'derived', value: 6500000, unit: 'millions_usd',
                        availability_basis: 'observed_acquisition', availability_basis_note: null,
                    },
                    rrp_as_pct_of_peak: { availability: 'unavailable', provenance: null, value: null, unit: 'pct' },
                    delta_5d_m: { availability: 'unavailable', provenance: null, value: null, unit: 'millions_usd' },
                    delta_30d_m: { availability: 'unavailable', provenance: null, value: null, unit: 'millions_usd' },
                    liquidity_regime: { availability: 'available', provenance: 'derived', value: 'insufficient_history', unit: null },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('fed-liquidity-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('fed-liquidity-card'));
        expect(card.getByText(/6,500,000/)).toBeInTheDocument();
    });

    it('renders the commodity warehouse pillar: LME available, Cushing permanently unavailable', async () => {
        mockPillars({
            cmdty: {
                lme: {
                    available: true,
                    status: 'ok',
                    pillar: 'commodity_warehouses',
                    as_of: '2026-09-18',
                    coverage: 1.0,
                    metals_with_data: 1,
                    metals_expected: 6,
                    metals: ['copper'],
                    generation_id: 'gen-lme-1',
                    generation_published_at: '2026-09-18T12:00:00+00:00',
                    fields: {
                        copper: {
                            canceled_ratio: {
                                availability: 'available', provenance: 'measured', value: 0.42, unit: 'ratio_0_1',
                                availability_basis: 'unknown',
                                availability_basis_note: 'no official LME warehouse-stocks publication schedule is cited; availability_basis is \'unknown\' by design (not a data-quality issue) -- see godview/commodity_warehouse_pillar.py',
                            },
                            total_inventory: { availability: 'available', provenance: 'measured', value: 2000, unit: 'metric_tonnes' },
                            physical_tightness_flag: { availability: 'available', provenance: 'derived', value: true, unit: null },
                        },
                    },
                },
                cushing_crude_stocks: {
                    available: false,
                    status: 'unavailable',
                    reason: 'never_configured: no real EIA Cushing, OK crude-stocks series id is pulled anywhere in this codebase',
                    as_of: null,
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('commodity-warehouse-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('commodity-warehouse-card'));
        expect(within(card.getByTestId('lme-section')).getByText('copper')).toBeInTheDocument();
        expect(within(card.getByTestId('cushing-section')).getByText(/never_configured/)).toBeInTheDocument();
    });

    it('renders the FINRA short-volume pillar with the not-short-interest note and per-ticker fields', async () => {
        mockPillars({
            finra: {
                available: true,
                status: 'ok',
                pillar: 'finra_short_volume',
                as_of: '2026-09-16',
                include_inferred: false,
                note: "this is daily short-sale VOLUME executed on the trade date, NOT short INTEREST (a bi-monthly position snapshot) and never a squeeze score",
                symbols_with_data: 1,
                generation_id: 'gen-finra-1',
                generation_published_at: '2026-09-16T18:00:00+00:00',
                fields: {
                    AAPL: {
                        short_volume: { availability: 'available', provenance: 'measured', value: 600000, unit: 'shares' },
                        short_exempt_volume: { availability: 'available', provenance: 'measured', value: 0, unit: 'shares' },
                        total_volume: { availability: 'available', provenance: 'measured', value: 1000000, unit: 'shares' },
                        short_ratio: { availability: 'available', provenance: 'derived', value: 0.6, unit: 'ratio_0_1' },
                        short_ratio_20d_ma: { availability: 'unavailable', provenance: null, value: null, unit: 'ratio_0_1' },
                        is_spike: { availability: 'available', provenance: 'derived', value: true, unit: null },
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('finra-short-volume-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('finra-short-volume-card'));
        expect(card.getByTestId('not-short-interest-note')).toHaveTextContent(/NOT short INTEREST/);
        expect(card.getByText('AAPL')).toBeInTheDocument();
        expect(card.getByText('0.6')).toBeInTheDocument();
    });
});
