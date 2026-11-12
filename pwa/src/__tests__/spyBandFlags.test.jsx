import React from 'react';
import { act, cleanup, fireEvent, render, screen, within } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import GammaBoard from '../views/GammaBoard.jsx';
import { api } from '../api.js';

vi.mock('../api.js', () => ({ api: {
    getSpyBandFlags:vi.fn(), getGammaSessionMap:vi.fn(), getGammaBoardInstrument:vi.fn(), getGammaBoardIntraday:vi.fn(),
}}));
const day='2026-11-13';
function stats(kind,bucket,hits,misses,pending,censored) {return {kind,bucket,hits,misses,pending,censored};}
const event={kind:'first_cross',red:745,lower:744,upper:747,target:747,close:745.12,
    signal_time:'2026-11-13T15:06:00Z',issued_at:'2026-11-13T15:06:08Z',deadline:'2026-11-13T15:37:00Z',
    outcome_time:'2026-11-13T15:17:00Z',status:'hit',bucket:'morning'};
function view(date=day) {return {status:'available',session_date:date,as_of:date+'T18:01:00Z',
    events:[event],stats:[stats('first_cross','all',1,3,6,4),stats('failed_reclaim','all',0,0,2,1),
    stats('first_cross','morning',1,0,1,0),stats('failed_reclaim','morning',0,0,2,1)]};}
beforeEach(()=>{
    localStorage.clear();vi.useFakeTimers();api.getSpyBandFlags.mockReset().mockImplementation((_,date)=>Promise.resolve(view(date || day)));
    for(const f of [api.getGammaSessionMap,api.getGammaBoardInstrument,api.getGammaBoardIntraday])
        f.mockImplementation(symbol=>Promise.resolve({symbol,status:'unavailable',reason:'Other source unavailable.'}));
});
afterEach(()=>{cleanup();vi.restoreAllMocks();vi.useRealTimers();});
const flush=()=>act(async()=>{});
const flags=()=>within(screen.getByRole('region',{name:'SPY band flags'}));
it('places flags only in SPY and shows ET evidence times, frozen targets, source clock, and independent family denominators',async()=>{
    render(<GammaBoard/>);await flush();
    expect(screen.getAllByRole('region',{name:'SPY band flags'})).toHaveLength(1);
    expect(flags().getByText(/Paired price and GEX inputs through/)).toHaveTextContent('13:01:00 ET');
    const list=within(flags().getByRole('list',{name:'SPY flags for selected session'}));
    expect(list.getByText('Red 745 → Target 747')).toBeInTheDocument();
    for(const stamp of ['10:06:00 ET','10:06:08 ET','10:37:00 ET','10:17:00 ET']) expect(list.getByText(stamp)).toBeInTheDocument();
    expect(flags().getByText('1 / 4 · 25%')).toBeInTheDocument();
    expect(flags().getByText('Misses 3 · Pending 6 · Censored 4')).toBeInTheDocument();
    fireEvent.click(flags().getByText('Time-of-day results'));
    expect(within(flags().getByRole('table')).getByText('1 / 1 · 100%')).toBeInTheDocument();
    fireEvent.change(flags().getByLabelText('Flag'),{target:{value:'failed_reclaim'}});
    expect(flags().getByRole('table',{name:'Failed reclaim time-of-day scores'})).toHaveTextContent('0 / 0 · Unscored');
    expect(api.getSpyBandFlags).toHaveBeenCalledTimes(1);
});
it('distinguishes empty observations, missing counters, and unavailable source from a measured zero-percent result',async()=>{
    api.getSpyBandFlags.mockResolvedValue({...view(),events:[],stats:[stats('first_cross','all',0,0,0,0),stats('failed_reclaim','all',null,3,null,1)],as_of:null});
    render(<GammaBoard/>);await flush();
    expect(flags().getByText('No flags recorded for this session.')).toBeInTheDocument();
    expect(flags().getByText(/No paired price and GEX minute/)).toBeInTheDocument();
    expect(flags().getByText('0 / 0 · Unscored')).toBeInTheDocument();
    expect(flags().queryByText(/0%|NaN/)).toBeNull();
    api.getSpyBandFlags.mockResolvedValue({status:'unavailable',reason:'Flag journal unavailable.'});
    fireEvent.click(screen.getByText('Refresh all'));await flush();
    expect(flags().getByText('Flag journal unavailable.')).toBeInTheDocument();
    expect(flags().queryByText('No flags recorded for this session.')).toBeNull();
});
it('polls latest, requests the selected archive date, clears old events, and never mixes dates',async()=>{
    render(<GammaBoard/>);await flush();
    await act(async()=>{await vi.advanceTimersByTimeAsync(60000);});
    expect(api.getSpyBandFlags).toHaveBeenCalledTimes(2);
    api.getSpyBandFlags.mockResolvedValue(view()); // mismatched archive reply
    fireEvent.change(screen.getByLabelText('Recorded session'),{target:{value:'2026-11-12'}});await flush();
    expect(api.getSpyBandFlags).toHaveBeenLastCalledWith(expect.any(AbortSignal),'2026-11-12');
    expect(flags().queryByRole('list')).toBeNull();
    await act(async()=>{await vi.advanceTimersByTimeAsync(300000);});
    expect(api.getSpyBandFlags).toHaveBeenCalledTimes(3);
});
it('uses a read-only, authenticated, no-store request for the selected flag session',async()=>{
    const {api:real}=await vi.importActual('../api.js');
    const fetch=vi.spyOn(globalThis,'fetch').mockResolvedValue(new Response('Denied',{status:403}));
    const signal=new AbortController().signal;
    expect(await real.getSpyBandFlags(signal,day)).toMatchObject({error:true,status:403});
    expect(fetch.mock.calls[0][0]).toMatch(/gamma-watch\/spy-band-flags\?session_date=2026-11-13$/);
    expect(fetch.mock.calls[0][1]).toMatchObject({cache:'no-store',signal});
    expect(fetch.mock.calls[0][1].method || 'GET').toBe('GET');
});
