import React from 'react';
import { render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import GodView, { fmtUsdMillions, fmtNum } from '../views/GodView.jsx';
import useGodViewStore from '../stores/godViewStore.js';
import { api } from '../api.js';
import { drawerSections, routes } from '../routes.js';

vi.mock('../api.js', () => ({
    api: {
        getGodViewLatest: vi.fn(),
    },
}));

const RUN = {
    run_id: 'r-1', status: 'complete', started_at: '2026-09-24T21:30:00+00:00',
    finished_at: '2026-09-24T21:31:00+00:00', rows_written: 1, rows_skipped: 0, reasons: {}, code_sha: 'abc123',
};

function unavailable(pillar, reason, extra = {}) {
    return {
        pillar, status: 'unavailable', available: false, reason, as_of: null, release_at: null,
        available_at: null, availability_basis: null, provenance: null,
        estimated: pillar === 'dealer_gex', last_run: null, data: null, ...extra,
    };
}

const GEX_NOTE = 'Modeled estimate: dealer side assumed (long calls / short puts); not measured positioning.';

function fedAvailable(overrides = {}) {
    return {
        pillar: 'fed_liquidity', status: 'available', available: true, reason: null,
        as_of: '2026-09-23', release_at: '2026-09-24T20:30:00+00:00', available_at: '2026-09-24T21:02:00+00:00',
        availability_basis: 'observed_acquisition', provenance: 'measured', estimated: false,
        stale_after_days: 9, last_run: RUN,
        data: {
            obs_date: '2026-09-23', net_liquidity_usd_m: 5864145, walcl_usd_m: 6746548, tga_usd_m: 877028,
            rrp_usd_m: 5375, delta_1w_m: 0, delta_4w_m: null, rrp_as_pct_of_peak: null,
            liquidity_regime: 'insufficient_history', unit: 'USD millions',
        },
        ...overrides,
    };
}

function market(root, fields = {}) {
    return {
        market: root, cftc_market_code: 'X', market_name: `${root} market`, status: 'available', available: true,
        reason: null, report_date: '2026-09-22', noncommercial_net: 200, spec_net_pct_oi: 20,
        z_score_1y: 0.5, z_score_3y: 1.5, percentile_3y: 88, crowding_regime: 'ELEVATED_LONG', ...fields,
    };
}

function missingMarket(root) {
    return { market: root, cftc_market_code: 'Y', market_name: `${root} market`, status: 'unavailable', available: false, reason: 'no_row_for_market', report_date: null };
}

function payload(pillars, extra = {}) {
    return {
        as_of: '2026-09-26T18:00:00+00:00', as_of_source: 'server_now',
        schema: { migrated: true, missing: [] },
        pillars,
        ...extra,
    };
}

const cftcUnavailable = unavailable('cftc', 'never_run', { coverage: { tracked: 16, available: 0, stale: 0, unavailable: 16 } });
const gexNeverRun = unavailable('dealer_gex', 'never_run', { ticker: 'SPY', model_note: GEX_NOTE });

beforeEach(() => {
    useGodViewStore.getState().reset();
    api.getGodViewLatest.mockReset();
});

afterEach(() => {
    vi.clearAllMocks();
});

describe('GodView honest states', () => {
    it('shows a loading state before the payload arrives', async () => {
        let resolve;
        api.getGodViewLatest.mockReturnValue(new Promise((r) => { resolve = r; }));
        render(<GodView />);
        expect(screen.getByTestId('godview-loading')).toBeTruthy();
        resolve(payload({
            fed_liquidity: unavailable('fed_liquidity', 'never_run'), cftc: cftcUnavailable, dealer_gex: gexNeverRun,
        }));
        await waitFor(() => expect(screen.queryByTestId('godview-loading')).toBeNull());
    });

    it('renders an error with retry, never an empty god view, when the API fails', async () => {
        api.getGodViewLatest.mockResolvedValue({ error: true, status: 503, message: 'godview_store_unavailable' });
        render(<GodView />);
        await waitFor(() => expect(screen.getByText('God view unavailable')).toBeTruthy());
        expect(screen.getByText('godview_store_unavailable')).toBeTruthy();
        expect(screen.queryByTestId('pillar-fed')).toBeNull();
        expect(screen.getByText('Retry')).toBeTruthy();
    });

    it('renders every pillar unavailable with its reason before the schema is migrated', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: unavailable('fed_liquidity', 'schema_not_migrated'),
            cftc: unavailable('cftc', 'schema_not_migrated'),
            dealer_gex: unavailable('dealer_gex', 'schema_not_migrated', { ticker: 'SPY', model_note: GEX_NOTE }),
        }, { schema: { migrated: false, missing: ['godview_runs'] } }));
        render(<GodView />);
        await waitFor(() => expect(screen.getByTestId('pillar-fed')).toBeTruthy());
        for (const id of ['pillar-fed', 'pillar-cftc', 'pillar-gex']) {
            const card = screen.getByTestId(id);
            expect(card.getAttribute('data-status')).toBe('unavailable');
            expect(within(card).getByTestId('status-badge').textContent).toBe('UNAVAILABLE');
            expect(within(card).getByTestId('unavailable-reason').textContent).toMatch(/schema not migrated/);
            expect(within(card).getByText('--')).toBeTruthy();
            expect(within(card).getByTestId('provenance-line').textContent).toMatch(/no writer run recorded/);
        }
        expect(screen.getByTestId('godview-as-of').textContent).toMatch(/schema not migrated/);
        expect(screen.queryByText(/neutral|stable|\$0/i)).toBeNull();
    });

    it('renders available fed values with provenance, a real zero as zero and a null as --', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: fedAvailable(), cftc: cftcUnavailable, dealer_gex: gexNeverRun,
        }));
        render(<GodView />);
        const card = await screen.findByTestId('pillar-fed');
        expect(card.getAttribute('data-status')).toBe('available');
        expect(within(card).getByText('$5.864T')).toBeTruthy();
        expect(within(card).getByText('$0M')).toBeTruthy(); // delta_1w_m = 0 is a measurement
        const fourWeek = within(card).getByText('4-week change').previousSibling;
        expect(fourWeek.textContent).toBe('--'); // delta_4w_m = null
        expect(within(card).getByText('insufficient history')).toBeTruthy();
        const line = within(card).getByTestId('provenance-line').textContent;
        expect(line).toMatch(/obs 2026-09-23/);
        expect(line).toMatch(/published 2026-09-24T20:30:00\+00:00/);
        expect(line).toMatch(/acquired 2026-09-24T21:02:00\+00:00/);
        expect(line).toMatch(/basis observed acquisition/);
        expect(line).toMatch(/provenance measured/);
        expect(line).toMatch(/last writer run complete/);
    });

    it('greys a stale fed pillar and names the data date', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: fedAvailable({ status: 'stale', reason: 'stale' }), cftc: cftcUnavailable, dealer_gex: gexNeverRun,
        }));
        render(<GodView />);
        const card = await screen.findByTestId('pillar-fed');
        expect(card.getAttribute('data-status')).toBe('stale');
        expect(within(card).getByTestId('status-badge').textContent).toBe('STALE');
        expect(within(card).getByText('Stale: latest data 2026-09-23')).toBeTruthy();
        expect(within(card).getByText('$5.864T')).toBeTruthy();
    });

    it('renders CFTC partial coverage with z-scores, percentile, crowding and honest missing markets', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: unavailable('fed_liquidity', 'never_run'),
            cftc: {
                pillar: 'cftc', status: 'partial', available: true, reason: 'partial_coverage',
                as_of: '2026-09-22', release_at: '2026-09-25T19:30:00+00:00', available_at: '2026-09-25T20:30:00+00:00',
                availability_basis: 'observed_acquisition', provenance: 'measured', estimated: false,
                coverage: { tracked: 3, available: 1, stale: 1, unavailable: 1 }, last_run: RUN,
                data: {
                    markets: [
                        market('ES'),
                        market('GC', { status: 'stale', z_score_1y: null, z_score_3y: null, percentile_3y: null, crowding_regime: null }),
                        missingMarket('ZN'),
                    ],
                },
            },
            dealer_gex: gexNeverRun,
        }));
        render(<GodView />);
        const card = await screen.findByTestId('pillar-cftc');
        expect(card.getAttribute('data-status')).toBe('partial');
        expect(within(card).getByTestId('cftc-coverage').textContent).toMatch(/2 of 3 markets reported \(1 stale\)/);
        const es = within(card).getByTestId('cftc-row-ES');
        expect(es.textContent).toMatch(/\+1\.50/);
        expect(es.textContent).toMatch(/88%/);
        expect(es.textContent).toMatch(/elevated long/);
        const gc = within(card).getByTestId('cftc-row-GC');
        expect(gc.textContent).toMatch(/\(stale\)/);
        expect(gc.textContent).not.toMatch(/neutral/i);
        expect(within(gc).getAllByText('--').length).toBeGreaterThanOrEqual(4);
        const zn = within(card).getByTestId('cftc-row-ZN');
        expect(zn.getAttribute('data-status')).toBe('unavailable');
        expect(zn.textContent).toMatch(/no data/);
        expect(zn.textContent).not.toMatch(/0\.00|0%/);
    });

    it('labels the GEX pillar modeled while unavailable (never run)', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: unavailable('fed_liquidity', 'never_run'), cftc: cftcUnavailable, dealer_gex: gexNeverRun,
        }));
        render(<GodView />);
        const card = await screen.findByTestId('pillar-gex');
        expect(card.getAttribute('data-status')).toBe('unavailable');
        expect(within(card).getByTestId('unavailable-reason').textContent).toMatch(/Writer has never run/);
        expect(within(card).getByTestId('gex-model-note').textContent).toMatch(/not measured positioning/);
        expect(within(card).getByText(/DEALER GAMMA \(SPY, MODELED\)/)).toBeTruthy();
    });

    it('renders an available GEX row as an estimate with chain and spot provenance', async () => {
        api.getGodViewLatest.mockResolvedValue(payload({
            fed_liquidity: unavailable('fed_liquidity', 'never_run'),
            cftc: cftcUnavailable,
            dealer_gex: {
                pillar: 'dealer_gex', status: 'available', available: true, reason: null, as_of: '2026-09-28',
                release_at: '2026-09-28T20:31:00+00:00', available_at: '2026-09-28T20:31:00+00:00',
                availability_basis: 'observed_acquisition', provenance: 'modeled', estimated: true, basis: 'bs_own_dte',
                ticker: 'SPY', model_note: GEX_NOTE, last_run: RUN,
                data: {
                    obs_date: '2026-09-28', gex_aggregate: -2500000000, gex_normalized: -0.3, gamma_flip: null,
                    regime: 'SHORT_GAMMA', put_wall: 650, call_wall: 670, spot: 661.5, estimated: true,
                    basis: 'bs_own_dte', sign_convention: 'long calls / short puts', model_note: GEX_NOTE,
                    chain_capture_completed_at: '2026-09-28T20:31:00+00:00', spot_source: 'astrogrid.price_close_receipt',
                    spot_basis: 'prior_close', spot_obs_date: '2026-09-25', spot_available_at: '2026-09-26T06:47:00+00:00',
                },
            },
        }));
        render(<GodView />);
        const card = await screen.findByTestId('pillar-gex');
        expect(card.getAttribute('data-status')).toBe('available');
        expect(within(card).getByText('Gamma flip').previousSibling.textContent).toBe('--');
        expect(within(card).getByText('short gamma')).toBeTruthy();
        expect(within(card).getByText(/Estimated exposure under assumed dealer positions/)).toBeTruthy();
        expect(within(card).getByText(/Model basis: bs_own_dte/)).toBeTruthy();
        expect(within(card).getByText(/Reference price source: astrogrid.price_close_receipt/)).toBeTruthy();
        expect(within(card).getByTestId('gex-model-note').textContent).toMatch(/Sign convention: long calls \/ short puts/);
        expect(within(card).getByTestId('provenance-line').textContent).toMatch(/provenance modeled/);
    });

    it('ignores a non-pillar response instead of rendering it', async () => {
        api.getGodViewLatest.mockResolvedValue({ detail: 'something else' });
        render(<GodView />);
        await waitFor(() => expect(screen.getByText('God view unavailable')).toBeTruthy());
        expect(screen.queryByTestId('pillar-fed')).toBeNull();
    });
});

describe('GodView formatting', () => {
    it('keeps null as -- and zero as zero', () => {
        expect(fmtUsdMillions(null)).toBe('--');
        expect(fmtUsdMillions(Number.NaN)).toBe('--');
        expect(fmtUsdMillions(0)).toBe('$0M');
        expect(fmtUsdMillions(-12000, { signed: true })).toBe('-$12.0B');
        expect(fmtUsdMillions(50000, { signed: true })).toBe('+$50.0B');
        expect(fmtNum(null)).toBe('--');
        expect(fmtNum(0)).toBe('0.00');
    });
});

describe('god-view route', () => {
    it('is registered as a markets drawer view', () => {
        const route = routes.find((r) => r.id === 'god-view');
        expect(route?.component).toBe('./views/GodView.jsx');
        expect(route?.group).toBe('markets');
        const markets = drawerSections.find((s) => s.label === 'MARKETS');
        expect(markets.items.map((r) => r.id)).toContain('god-view');
    });
});
