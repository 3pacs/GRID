import React from 'react';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import GammaBoard from '../views/GammaBoard.jsx';
import { api } from '../api.js';
import { routes, drawerSections } from '../routes.js';

vi.mock('../api.js', () => ({ api: { getGammaBoardInstrument: vi.fn(), getGammaBoardIntraday: vi.fn() } }));
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
function prices(symbol, extra = {}) {
    return { symbol, status:'available', as_of:'2026-11-10T14:34:00Z', session_date:'2026-11-10',
        session_start:'2026-11-10T14:30:00Z', session_end:'2026-11-10T21:00:00Z', session_open:true,
        bars:[{ time:'2026-11-10T14:30:00Z', open:90, high:101, low:89, close:100, volume:15 },
            { time:'2026-11-10T14:33:00Z', open:100, high:103, low:98, close:102, volume:20 }],
        missing_bars:2, ...extra };
}
const panel = s => within(screen.getByRole('article', { name: s + ' gamma exposure' }));
const flush = () => act(async () => {});
const tick = ms => act(async () => { await vi.advanceTimersByTimeAsync(ms); });
describe('Gamma board', () => {
    beforeEach(() => { localStorage.removeItem('grid.gamma-board.extra-symbols.v1'); vi.useFakeTimers(); api.getGammaBoardInstrument.mockReset(); api.getGammaBoardIntraday.mockReset().mockImplementation(s => Promise.resolve(prices(s))); });
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
    it('draws all minute charts, price gaps, explicit wall snapshots, signed cells and share volume', async () => {
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(good(s,
            s === 'SPY' ? {as_of:'2026-11-09T18:00:00Z'} : {})));
        render(<GammaBoard />); await flush();
        expect(screen.getAllByRole('img', {name:/one-minute closing prices/})).toHaveLength(3);
        const svg=panel('QQQ').getByRole('img');
        const path=svg.querySelector('path').getAttribute('d');
        expect(path.split('M')).toHaveLength(3); // two single-minute samples separated by a gap
        expect(svg.querySelector('[aria-label="QQQ minute volume"]')).not.toBeNull();
        expect(panel('SPX').getByRole('img').querySelector('[aria-label="SPX minute volume"]')).toBeNull();
        expect(panel('SPX').getByText(/Index volume unavailable/)).toBeInTheDocument();
        expect(panel('QQQ').getByText(/Wall reference/)).toHaveTextContent('snapshot');
        expect(panel('SPY').queryByText(/Wall reference/)).toBeNull();
        expect(panel('SPX').getByText('− Negative')).toHaveClass('gb-negative');
        expect(panel('SPX').getByText('+ Positive')).toHaveClass('gb-positive');
        const neg=panel('SPX').getByRole('button',{name:/strike 7505, Net gamma/});
        const pos=panel('SPX').getByRole('button',{name:/strike 7500, Net gamma/});
        expect(neg.style.backgroundColor).toBe('rgb(248, 110, 116)');
        const [r,g,b]=pos.style.backgroundColor.match(/\d+/g).map(Number);expect(g).toBeGreaterThan(r);expect(g).toBeGreaterThan(b);
    });
    it('keeps either feed when the other is unavailable', async () => {
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(s === 'SPX' ? {symbol:s,status:'unavailable',reason:'No chain'} : good(s)));
        api.getGammaBoardIntraday.mockImplementation(s => Promise.resolve(s === 'QQQ' ? {symbol:s,status:'unavailable',reason:'No minute bars'} : prices(s)));
        render(<GammaBoard />); await flush();
        expect(panel('SPX').getByRole('img')).toBeInTheDocument();expect(panel('SPX').queryByRole('table')).toBeNull();
        expect(panel('QQQ').getByRole('table')).toBeInTheDocument();expect(panel('QQQ').queryByRole('img')).toBeNull();
        expect(panel('QQQ').getByText('No minute bars')).toBeInTheDocument();
    });
    it('allows unique plain tickers and limits the board to three added cards', async () => {
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(good(s)));
        render(<GammaBoard />);await flush();
        async function add(value) {
            fireEvent.change(screen.getByLabelText('Ticker to add'),{target:{value}});
            fireEvent.click(screen.getByText('Add',{selector:'button'}));await flush();
        }
        await add('SPY');expect(screen.getByText('SPY is already on the board.')).toBeInTheDocument();
        await add('../');expect(screen.getByText('Use a ticker with one to five letters.')).toBeInTheDocument();
        for(const sym of ['aapl','NVDA','MSFT']) await add(sym);
        expect(screen.getAllByRole('article')).toHaveLength(6);
        await add('IWM');expect(screen.getByText('Remove an added ticker to make room.')).toBeInTheDocument();
        fireEvent.click(screen.getByRole('button',{name:'Remove NVDA'}));
        await add('IWM');expect(panel('IWM').getByRole('img')).toBeInTheDocument();
        expect(screen.queryByRole('button',{name:'Remove SPX'})).toBeNull();
    });
    it('restores validated extra symbol names, capped, without keeping prices or provider data', async () => {
        const key = 'grid.gamma-board.extra-symbols.v1';
        localStorage.setItem(key, JSON.stringify(['SPY',null,'AAPL','AAPL','../','NVDA',{spot:123},'MSFT','IWM']));
        api.getGammaBoardInstrument.mockImplementation(s => Promise.resolve(good(s)));
        render(<GammaBoard />); await flush();
        expect(screen.getAllByRole('article')).toHaveLength(6);
        expect(JSON.parse(localStorage.getItem(key))).toEqual(['AAPL','NVDA','MSFT']);
        fireEvent.click(screen.getByRole('button', {name:'Remove NVDA'}));
        expect(JSON.parse(localStorage.getItem(key))).toEqual(['AAPL','MSFT']);
    });
    it('requests the independent minute feed via the private API without caching', async () => {
        const {api: real}=await vi.importActual('../api.js');
        const fetch=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response('Denied',{status:403}));
        const signal=new AbortController().signal;
        expect(await real.getGammaBoardIntraday('QQQ',signal)).toMatchObject({status:403,error:true});
        expect(fetch.mock.calls[0][0]).toMatch(/\/api\/v1\/gamma-watch\/intraday\/QQQ$/);
        expect(fetch.mock.calls[0][1]).toMatchObject({cache:'no-store',signal});
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
