import React from 'react';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import GammaBoard from '../views/GammaBoard.jsx';
import { api } from '../api.js';
import { routes, drawerSections } from '../routes.js';

vi.mock('../api.js', () => ({ api: { getGammaBoardInstrument: vi.fn() } }));
const stamp = '2026-11-10T21:00:00Z';
const expiry = '2026-11-13';
function good(symbol, extra = {}) {
    const spot = symbol === 'SPX' ? 7500 : symbol === 'SPY' ? 750 : 680;
    return {
        status: 'available', symbol, spot, call_wall: spot + 5, put_wall: spot,
        expiries: [expiry], as_of: stamp, spot_as_of: null, session_open: true,
        source_label: 'Test chain', stale: false,
        cells: [{ strike: spot, expiry, net_gex: 10, put_gex: -15, call_gex: 25, call_oi: 120, put_oi: 30 },
            { strike: spot + 5, expiry, net_gex: -30, call_gex: 20, put_gex: -50 }],
        ...extra,
    };
}
const panel = s => within(screen.getByRole('article', { name: s + ' gamma exposure' }));
const flush = () => act(async () => {});
const tick = ms => act(async () => { await vi.advanceTimersByTimeAsync(ms); });
describe('Gamma board', () => {
    beforeEach(() => { vi.useFakeTimers(); api.getGammaBoardInstrument.mockReset(); });
    afterEach(() => { cleanup(); vi.useRealTimers(); vi.restoreAllMocks(); });
    it('keeps the three scoped surfaces together in the markets drawer and exposes exact cell details', async () => {
        expect(routes.find(r => r.id === 'gamma-board').component).toBe('./views/GammaBoard.jsx');
        expect(drawerSections.find(s => s.label === 'MARKETS').items.some(r => r.id === 'gamma-board')).toBe(true);
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(good(s)));
        render(<GammaBoard />); await flush();
        expect(screen.getAllByRole('table')).toHaveLength(3);
        fireEvent.change(screen.getByLabelText('Exposure'), { target: { value: 'put_gex' } });
        fireEvent.click(panel('SPX').getByRole('button', { name: /strike 7505, Put gamma/ }));
        expect(panel('SPX').getByText(/7,505 strike/)).toBeInTheDocument();
        expect(panel('SPY').getByText(/750 strike/)).toBeInTheDocument();
    });
    it('keeps independent intervals, stale snapshots and unavailable panels', async () => {
        let failSPY = false;
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(
            s === 'SPX' ? { symbol: s, status:'unavailable', reason:'Index chain unavailable.', session_open:false }
                : s === 'SPY' && failSPY ? { error: true } : good(s, { stale:s === 'QQQ',session_open:s === 'SPY' })));
        render(<GammaBoard />); await flush();
        expect(panel('SPX').queryByRole('table')).toBeNull();
        expect(panel('QQQ').getByText('Last available snapshot')).toBeInTheDocument();
        failSPY = true; await tick(60000);
        expect(api.getGammaBoardInstrument).toHaveBeenCalledTimes(4);
        expect(panel('SPY').getByRole('status')).toHaveTextContent('Showing last snapshot');
        expect(panel('SPY').getByRole('table')).toBeInTheDocument();
    });
    it('clears private prior data on access loss or authoritative unavailability', async () => {
        let first = true;
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(first || s === 'QQQ' ? good(s)
            : s === 'SPY' ? { error: true, status: 403 }
                : { symbol: s, status: 'unavailable', reason: 'Source disabled.' }));
        render(<GammaBoard />); await flush();
        first = false; fireEvent.click(screen.getByText('Refresh all')); await flush();
        expect(panel('SPY').queryByRole('table')).toBeNull();
        expect(panel('SPX').queryByRole('table')).toBeNull();
        expect(panel('QQQ').getByRole('table')).toBeInTheDocument();
    });
    it.each([401, 403])('receives HTTP %s from the authenticated request wrapper', async status => {
        const { api: real } = await vi.importActual('../api.js');
        const fetch = vi.spyOn(globalThis, 'fetch').mockResolvedValue(new Response('Denied', { status }));
        const signal = new AbortController().signal;
        const response = await real.getGammaBoardInstrument('QQQ', signal);
        expect(response).toMatchObject({ error: true, status });
        expect(fetch.mock.calls[0][0]).toMatch(/\/api\/v1\/gamma-watch\/board\/QQQ$/);
        expect(fetch.mock.calls[0][1]).toMatchObject({ cache: 'no-store', signal });
    });
    it('serializes polls, pauses while hidden and resumes on visibility', async () => {
        const blocked = {};
        let held = true, hidden = false;
        vi.spyOn(document, 'hidden', 'get').mockImplementation(() => hidden);
        api.getGammaBoardInstrument.mockImplementation(s => held ? new Promise(r => { blocked[s] = r; }) : Promise.resolve(good(s)));
        render(<GammaBoard />); await flush();
        fireEvent.click(screen.getByText('Refresh all')); expect(api.getGammaBoardInstrument).toHaveBeenCalledTimes(3);
        hidden = true; fireEvent(document,new Event('visibilitychange'));
        await act(async () => { held = false; for (const [s, r] of Object.entries(blocked)) r(good(s)); });
        await tick(300000); expect(api.getGammaBoardInstrument).toHaveBeenCalledTimes(3);
        hidden = false; fireEvent(document,new Event('visibilitychange')); await flush();
        expect(api.getGammaBoardInstrument).toHaveBeenCalledTimes(6);
    });
});
