import React from 'react';
import { render, screen, fireEvent, waitFor, within } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import WatchlistAnalysis, { classifyPanelResult } from '../views/WatchlistAnalysis.jsx';
import { api } from '../api.js';

/**
 * Derivatives panels on the ticker investigation view (GEX, Vanna/Charm, Flow
 * Timeline). Before this change a panel whose request failed — the router's
 * own `{"error": …}` shape or an api.js error object — simply vanished, so a
 * user could not tell "no options flow for this ticker" from "the puller is
 * broken". Now each panel is loading / ok / empty / failed independently, a
 * failed panel says so with a fixed explanation and a Retry button, and no
 * backend error text is ever rendered.
 */

vi.mock('../api.js', () => ({
    api: {
        getTickerAnalysis: vi.fn(),
        getTickerOverview: vi.fn(),
        getTickerEdge: vi.fn(),
        getGEXProfile: vi.fn(),
        getVannaCharm: vi.fn(),
        getFlowTimeline: vi.fn(),
    },
}));
// The three chart children are D3 canvases; stub them so the test covers the
// parent's state machine and nothing else.
vi.mock('../components/GEXProfile.jsx', () => ({
    default: ({ gexData }) => <div data-testid="gex-child">{`gex spot ${gexData.spot}`}</div>,
}));
vi.mock('../components/VannaCharmViz.jsx', () => ({
    default: () => <div data-testid="vanna-child">vanna chart</div>,
}));
vi.mock('../components/FlowTimeline.jsx', () => ({
    default: ({ timelineData }) => <div data-testid="flow-child">{`flow bars ${timelineData.history.length}`}</div>,
}));
vi.mock('../components/PriceChart.jsx', () => ({ default: () => <div data-testid="price-chart" /> }));
vi.mock('./Options.jsx', () => ({ TickerRecommendations: () => <div data-testid="recs" /> }));
vi.mock('../views/Options.jsx', () => ({ TickerRecommendations: () => <div data-testid="recs" /> }));
vi.mock('../hooks/useDevice.js', () => ({ useDevice: () => ({ isMobile: false }) }));

if (typeof globalThis.ResizeObserver === 'undefined') {
    globalThis.ResizeObserver = class { observe() {} unobserve() {} disconnect() {} };
}
if (typeof window.matchMedia !== 'function') {
    window.matchMedia = vi.fn().mockReturnValue({ matches: false, addEventListener: vi.fn(), removeEventListener: vi.fn() });
}

const SQL_LEAK = 'psycopg2.errors.UndefinedTable: relation "options_chain" does not exist LINE 1: SELECT * FROM /data/grid_v4/secret';

const GEX_OK = { available: true, spot: 101.5, snap_date: new Date().toISOString().slice(0, 10), per_strike: [], profile: [] };
const GEX_NO_DATA = { available: false, error: 'No options data for ACME on 2026-09-18', snap_date: null, ticker: 'ACME' };
const GEX_FAILED_ROUTER = { available: false, error: SQL_LEAK, ticker: 'ACME' };
const VANNA_OK = { ticker: 'ACME', spot: 101.5, vanna_exposure: 1, charm_exposure: -1, vanna_by_strike: [], charm_by_strike: [] };
const VANNA_FAILED_ROUTER = { error: SQL_LEAK, ticker: 'ACME' };
const FLOW_OK = { ticker: 'ACME', days: 90, history: [{ date: '2026-09-17', net_gex: 1 }], opex_calendar: [], catalysts: [] };
const FLOW_EMPTY = { ticker: 'ACME', days: 90, history: [], opex_calendar: [], catalysts: [] };
const API_500 = { error: true, status: 500, message: SQL_LEAK };
const API_NET = { error: true, status: 0, message: 'Failed to fetch' };

function deferred() {
    let resolve; let reject;
    const promise = new Promise((res, rej) => { resolve = res; reject = rej; });
    return { promise, resolve, reject };
}

function arm({ gex = GEX_OK, vanna = VANNA_OK, flow = FLOW_OK } = {}) {
    api.getTickerAnalysis.mockResolvedValue({ price_history: [{ date: '2026-09-17', value: 100 }], options: [], regime: null });
    api.getTickerOverview.mockResolvedValue({ error: true });
    api.getTickerEdge.mockResolvedValue({ error: true });
    api.getGEXProfile.mockResolvedValue(gex);
    api.getVannaCharm.mockResolvedValue(vanna);
    api.getFlowTimeline.mockResolvedValue(flow);
}

beforeEach(() => {
    for (const fn of Object.values(api)) fn.mockReset();
});

describe('classifyPanelResult', () => {
    it('maps api.js error objects to failed (unreachable vs http)', () => {
        expect(classifyPanelResult('gex', { status: 'fulfilled', value: API_NET })).toMatchObject({ status: 'failed', kind: 'unreachable' });
        expect(classifyPanelResult('flow', { status: 'fulfilled', value: API_500 })).toMatchObject({ status: 'failed', kind: 'http', httpStatus: 500 });
        expect(classifyPanelResult('vanna', { status: 'rejected', reason: new Error('x') })).toMatchObject({ status: 'failed', kind: 'unreachable' });
    });
    it('treats the engine’s no-chain messages as empty, anything else as failed', () => {
        expect(classifyPanelResult('gex', { status: 'fulfilled', value: GEX_NO_DATA })).toEqual({ status: 'empty' });
        expect(classifyPanelResult('gex', { status: 'fulfilled', value: GEX_FAILED_ROUTER })).toMatchObject({ status: 'failed', kind: 'service' });
        expect(classifyPanelResult('vanna', { status: 'fulfilled', value: { error: 'No GEX data available' } })).toEqual({ status: 'empty' });
        expect(classifyPanelResult('vanna', { status: 'fulfilled', value: VANNA_FAILED_ROUTER })).toMatchObject({ status: 'failed', kind: 'service' });
    });
    it('flow timeline: empty history is empty, rows are ok', () => {
        expect(classifyPanelResult('flow', { status: 'fulfilled', value: FLOW_EMPTY })).toEqual({ status: 'empty' });
        expect(classifyPanelResult('flow', { status: 'fulfilled', value: FLOW_OK })).toMatchObject({ status: 'ok' });
    });
    it('flags a GEX chain older than three days as stale and a fresh one as not', () => {
        expect(classifyPanelResult('gex', { status: 'fulfilled', value: GEX_OK })).toMatchObject({ status: 'ok', stale: false });
        expect(classifyPanelResult('gex', { status: 'fulfilled', value: { ...GEX_OK, snap_date: '2026-01-02' } })).toMatchObject({ status: 'ok', stale: true, asOf: '2026-01-02' });
    });
});

describe('WatchlistAnalysis derivatives panels', () => {
    it('renders all three panels when every request succeeds', async () => {
        arm();
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        await screen.findByTestId('gex-child');
        expect(screen.getByTestId('vanna-child')).toBeTruthy();
        expect(screen.getByTestId('flow-child')).toBeTruthy();
        expect(screen.queryByTestId('gex-panel-stale')).toBeNull();
    });

    it('shows loading skeletons for all three panels until the requests settle', async () => {
        const g = deferred(); const v = deferred(); const f = deferred();
        arm();
        api.getGEXProfile.mockReturnValue(g.promise);
        api.getVannaCharm.mockReturnValue(v.promise);
        api.getFlowTimeline.mockReturnValue(f.promise);
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        expect(await screen.findByTestId('gex-panel-loading')).toBeTruthy();
        expect(screen.getByTestId('vanna-panel-loading')).toBeTruthy();
        expect(screen.getByTestId('flow-panel-loading')).toBeTruthy();
        g.resolve(GEX_OK); v.resolve(VANNA_OK); f.resolve(FLOW_OK);
        await screen.findByTestId('gex-child');
        expect(screen.queryByTestId('flow-panel-loading')).toBeNull();
    });

    it.each([
        ['GEX', { gex: GEX_FAILED_ROUTER }, 'gex', ['vanna-child', 'flow-child']],
        ['Vanna/Charm', { vanna: VANNA_FAILED_ROUTER }, 'vanna', ['gex-child', 'flow-child']],
        ['Flow Timeline', { flow: API_500 }, 'flow', ['gex-child', 'vanna-child']],
    ])('%s failing alone shows its unavailable card and keeps the other two visible', async (_n, overrides, panel, others) => {
        arm(overrides);
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        const card = await screen.findByTestId(`${panel}-panel-failed`);
        expect(within(card).getAllByText(/unavailable/i).length).toBeGreaterThan(0);
        expect(within(card).getByTestId(`${panel}-panel-retry`)).toBeTruthy();
        for (const o of others) expect(await screen.findByTestId(o)).toBeTruthy();
        expect(screen.queryByTestId(`${panel}-child`)).toBeNull();
    });

    it('never renders backend error text (SQL, paths, exception names)', async () => {
        arm({ gex: GEX_FAILED_ROUTER, vanna: VANNA_FAILED_ROUTER, flow: API_500 });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        await screen.findByTestId('gex-panel-failed');
        await screen.findByTestId('vanna-panel-failed');
        await screen.findByTestId('flow-panel-failed');
        const html = document.body.innerHTML;
        for (const needle of ['psycopg2', 'SELECT', '/data/grid_v4', 'options_chain', 'UndefinedTable']) {
            expect(html).not.toContain(needle);
        }
        // the http category shows only the status number
        expect(within(screen.getByTestId('flow-panel-failed')).getByText(/status 500/)).toBeTruthy();
        expect(within(screen.getByTestId('gex-panel-failed')).getByText(/derivatives service reported a failure/)).toBeTruthy();
    });

    it('a network failure is explained as unreachable', async () => {
        arm({ vanna: API_NET });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        const card = await screen.findByTestId('vanna-panel-failed');
        expect(within(card).getByText(/could not be reached/)).toBeTruthy();
    });

    it('valid empty results render explicit empty cards, not failures, with no retry', async () => {
        arm({ gex: GEX_NO_DATA, vanna: { error: 'No options data for ACME on 2026-09-18' }, flow: FLOW_EMPTY });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        for (const p of ['gex', 'vanna', 'flow']) {
            const card = await screen.findByTestId(`${p}-panel-empty`);
            expect(within(card).getByText(/empty result, not a failure/)).toBeTruthy();
            expect(screen.queryByTestId(`${p}-panel-failed`)).toBeNull();
            expect(screen.queryByTestId(`${p}-panel-retry`)).toBeNull();
        }
    });

    it('marks a GEX chain older than three days as stale but still renders it', async () => {
        arm({ gex: { ...GEX_OK, snap_date: '2026-01-02' } });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        await screen.findByTestId('gex-child');
        expect(screen.getByTestId('gex-panel-stale').textContent).toMatch(/2026-01-02/);
    });

    it('recovers after Retry: a failed panel loads on the second request and the retry hits only that endpoint', async () => {
        arm({ flow: API_500 });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        const card = await screen.findByTestId('flow-panel-failed');
        expect(api.getFlowTimeline).toHaveBeenCalledTimes(1);
        api.getFlowTimeline.mockResolvedValueOnce(FLOW_OK);
        fireEvent.click(within(card).getByTestId('flow-panel-retry'));
        await screen.findByTestId('flow-child');
        expect(api.getFlowTimeline).toHaveBeenCalledTimes(2);
        expect(api.getGEXProfile).toHaveBeenCalledTimes(1);
        expect(api.getVannaCharm).toHaveBeenCalledTimes(1);
        expect(screen.queryByTestId('flow-panel-failed')).toBeNull();
    });

    it('a retry that fails again stays failed', async () => {
        arm({ gex: API_NET });
        render(<WatchlistAnalysis ticker="ACME" onBack={() => {}} />);
        const card = await screen.findByTestId('gex-panel-failed');
        fireEvent.click(within(card).getByTestId('gex-panel-retry'));
        await waitFor(() => expect(api.getGEXProfile).toHaveBeenCalledTimes(2));
        expect(await screen.findByTestId('gex-panel-failed')).toBeTruthy();
    });
});
