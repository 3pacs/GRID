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
function mockPillars({ cftc, fed, cmdty, finra, ftd, buyback, gex } = {}) {
    api.get.mockImplementation((path) => {
        if (path.includes('/pillars/cftc')) return Promise.resolve(cftc ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/fed_net_liquidity')) return Promise.resolve(fed ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/commodity_warehouses')) {
            return Promise.resolve(cmdty ?? { lme: NEVER_CONFIGURED, cushing_crude_stocks: NEVER_CONFIGURED });
        }
        if (path.includes('/pillars/finra_short_volume')) return Promise.resolve(finra ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/sec_regsho_ftd')) return Promise.resolve(ftd ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/buyback_blackouts')) return Promise.resolve(buyback ?? NEVER_CONFIGURED);
        if (path.includes('/pillars/dealer_gex')) return Promise.resolve(gex ?? NEVER_CONFIGURED);
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

    it('renders no not-built-yet cards now that every known pillar is built', async () => {
        mockPillars();

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('dealer-gex-card')).toHaveAttribute('data-state', 'unavailable'));
        expect(screen.queryAllByTestId('pillar-card-not-built').length).toBe(0);
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

    it('renders the SEC FTD pillar with the not-a-timeline note and per-CUSIP fields', async () => {
        mockPillars({
            ftd: {
                available: true,
                status: 'ok',
                pillar: 'sec_ftd',
                as_of: '2026-09-15',
                include_inferred: false,
                note: "outstanding balance as of one settlement date; never summed across dates, no T+35 buy-in timeline, no squeeze score",
                cusips_with_data: 1,
                generation_id: 'gen-ftd-1',
                generation_published_at: '2026-09-15T06:00:00+00:00',
                fields: {
                    Y4000A102: {
                        failed_shares: { availability: 'available', provenance: 'measured', value: 373, unit: 'shares' },
                        closing_price: { availability: 'available', provenance: 'measured', value: 16.99, unit: 'usd_per_share' },
                        total_failed_usd: { availability: 'available', provenance: 'derived', value: 6337.27, unit: 'usd' },
                        observation_age_days: { availability: 'available', provenance: 'derived', value: 29, unit: 'days' },
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('sec-ftd-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('sec-ftd-card'));
        expect(card.getByTestId('not-a-timeline-note')).toHaveTextContent(/no T\+35 buy-in timeline/);
        expect(card.getByText('Y4000A102')).toBeInTheDocument();
        expect(card.getByText('373')).toBeInTheDocument();
    });

    it('renders the buyback blackout pillar with the modeling-assumption and missing-input notes', async () => {
        mockPillars({
            buyback: {
                available: true,
                status: 'ok',
                pillar: 'buyback_blackouts',
                as_of: '2026-10-15',
                note: 'modeled quiet window = earnings_date -14d to +2d (common issuer self-imposed Rule 10b-18 compliance PRACTICE, not an SEC-mandated period -- the SEC\'s own Rule 10b5-1 statement is explicit: "we are not adopting a cooling-off period for issuers")',
                missing_input: 'issuer-level repurchase execution data (10-Q/10-K share-repurchase tables via EDGAR) does not exist in this database; no dollar or share buyback figure is ever computed here',
                issuers_with_data: 1,
                generation_id: 'gen-buyback-1',
                generation_published_at: '2026-10-15T00:00:00+00:00',
                issuers: {
                    AAPL: {
                        availability: 'available', provenance: 'modeled', value: 'quiet_window',
                        earnings_date_used: '2026-10-20', window_start: '2026-10-06', window_end: '2026-10-22',
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('buyback-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('buyback-card'));
        expect(card.getByTestId('modeling-assumption-note')).toHaveTextContent(/not adopting a cooling-off period for issuers/);
        expect(card.getByTestId('missing-input-note')).toHaveTextContent(/EDGAR/);
        expect(card.getByText('AAPL')).toBeInTheDocument();
        expect(card.getByText('quiet_window')).toBeInTheDocument();
    });

    it('renders the dealer GEX pillar with the sign-convention and missing-input notes', async () => {
        mockPillars({
            gex: {
                available: true,
                status: 'ok',
                pillar: 'dealer_gex',
                as_of: '2026-09-18',
                sign_convention_note: 'dealers modeled net short the customer side of both calls and puts; call OI contributes +gamma, put OI contributes -gamma to net dealer exposure (standard public GEX methodology, a stated modeling assumption -- options_snapshots carries no real dealer/customer position split)',
                gamma_assumptions_note: 'Black-Scholes gamma, r=0.0, q=0.0 (both assumed 0, a standard simplification); implied_vol read directly from options_snapshots, never solved for or defaulted',
                missing_input: 'no real captured options chain fixture exists to validate this engine against a known-correct GEX figure',
                tickers_with_data: 1,
                generation_id: 'gen-gex-1',
                generation_published_at: '2026-09-18T21:00:00+00:00',
                fields: {
                    AAPL: {
                        spot_price: { availability: 'available', provenance: 'measured', value: 100.0, unit: 'usd_per_share' },
                        net_gex_usd_m: { availability: 'available', provenance: 'modeled', value: 42.5, unit: 'usd_millions_per_1pct_move' },
                        gamma_flip_strike: { availability: 'available', provenance: 'modeled', value: 100.98, unit: 'usd_per_share' },
                        spot_to_flip_pct: { availability: 'available', provenance: 'modeled', value: 0.98, unit: 'pct' },
                        gex_regime: { availability: 'available', provenance: 'modeled', value: 'long_gamma', unit: null },
                        max_pain_strike: { availability: 'available', provenance: 'modeled', value: 100.0, unit: 'usd_per_share' },
                        put_call_oi_ratio: { availability: 'available', provenance: 'modeled', value: 1.0, unit: 'ratio' },
                        atm_iv: { availability: 'available', provenance: 'modeled', value: 0.3, unit: 'annualized_vol' },
                    },
                },
            },
        });

        render(<GodViewPillars />);

        await waitFor(() => expect(screen.getByTestId('dealer-gex-card')).toHaveAttribute('data-state', 'available'));
        const card = within(screen.getByTestId('dealer-gex-card'));
        expect(card.getByTestId('sign-convention-note')).toHaveTextContent(/\+gamma.*-gamma/);
        expect(card.getByTestId('gex-missing-input-note')).toHaveTextContent(/no real captured options chain fixture/);
        expect(card.getByText('AAPL')).toBeInTheDocument();
        expect(card.getByText('long_gamma')).toBeInTheDocument();
    });
});
