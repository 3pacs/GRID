import React from 'react';
import {act, cleanup, fireEvent, render, screen, within} from '@testing-library/react';
import {afterEach, beforeEach, expect, it, vi} from 'vitest';
import GammaBoard from '../views/GammaBoard.jsx';
import GammaSessionMap, {sessionGeometry, validMap} from '../components/GammaSessionMap.jsx';
import {api} from '../api.js';

vi.mock('../api.js',()=>({api:{getGammaSessionMap:vi.fn(),setGammaSessionTracking:vi.fn(),
    getGammaBoardInstrument:vi.fn(),getGammaBoardIntraday:vi.fn()}}));
const start=Date.parse('2026-11-10T14:30:00Z');
function frame(symbol='SPY', extra={}) {
    return {symbol,status:'available',session_date:'2026-11-10',session_start:new Date(start).toISOString(),
        session_end:'2026-11-10T21:00:00Z',source:'Recorded chain',expiry:'2026-11-10',units:'USD per 1% underlying move',
        times:[start,start+60000,start+180000],strikes:[749,750,751],values:[[-2,null,-4],[0,5,6],[null,3,2]],
        spot:[[start,750],[start+60000,750.5],[start+180000,750.25]], ...extra};
}
let ctx;
beforeEach(()=>{
    ctx={clearRect:vi.fn(),fillRect:vi.fn(),beginPath:vi.fn(),moveTo:vi.fn(),lineTo:vi.fn(),stroke:vi.fn(),arc:vi.fn(),fill:vi.fn()};
    vi.spyOn(HTMLCanvasElement.prototype,'getContext').mockReturnValue(ctx);
    localStorage.clear();
    api.getGammaSessionMap.mockReset().mockImplementation(s=>Promise.resolve(frame(s)));
    api.setGammaSessionTracking.mockReset();
    for(const load of [api.getGammaBoardInstrument,api.getGammaBoardIntraday])
        load.mockReset().mockImplementation(s=>Promise.resolve({symbol:s,status:'unavailable',reason:'Latest source unavailable.'}));
});
afterEach(()=>{cleanup();vi.restoreAllMocks();});
it('paints only observed cells at their actual minute and breaks the spot path over absent minutes',()=>{
    const m=sessionGeometry(frame());
    expect(m.columns.get(start+120000)).toBeUndefined();
    expect(m.segments.map(s=>s.length)).toEqual([2,1]);
    render(<GammaSessionMap map={frame()} />);
    expect(ctx.fillRect).toHaveBeenCalledTimes(7);
    const width=780/390;
    expect([...new Set(ctx.fillRect.mock.calls.map(a=>Math.round(a[0]/width)))]).toEqual([0,3,1]);
    expect(ctx.fillRect.mock.calls.every(a=>a[2]===width)).toBe(true);
    const inspector=screen.getByRole('group',{name:/session heatmap/});
    fireEvent.keyDown(inspector,{key:'ArrowLeft'}); // strongest sample at 09:33
    expect(screen.getByText('No observation')).toBeInTheDocument();
    fireEvent.keyDown(inspector,{key:'ArrowLeft'});
    expect(screen.getByText('+$5')).toBeInTheDocument();
});
it('refuses malformed dimensions, duplicate minute slots and samples outside session bounds',()=>{
    expect(validMap(frame())).toBe(true);
    expect(validMap(frame('SPY',{values:[[1]]}))).toBe(false);
    expect(validMap(frame('SPY',{times:[start,start+1,start+60000]}))).toBe(false);
    expect(validMap(frame('SPY',{times:[start-60000,start,start+60000]}))).toBe(false);
});
it('renders three independent maps and never fills an unavailable or newly selected session from another symbol/date',async()=>{
    let release;
    api.getGammaSessionMap.mockImplementation((s,signal,date)=>{
        if(date) return new Promise(r=>{if(s==='SPY')release=r;});
        return Promise.resolve(s==='QQQ'?{symbol:s,status:'unavailable',reason:'No QQQ recording.'}:frame(s));
    });
    render(<GammaBoard/>);await act(async()=>{});
    expect(screen.getAllByRole('img',{name:/net gamma by strike and recorded minute/})).toHaveLength(2);
    expect(screen.getByText('No QQQ recording.')).toBeInTheDocument();
    fireEvent.change(screen.getByLabelText('Recorded session'),{target:{value:'2026-11-09'}});
    expect(screen.queryByRole('img',{name:/net gamma by strike and recorded minute/})).toBeNull();
    await act(async()=>{release(frame());}); // wrong date must fail validation
    expect(screen.queryByRole('img',{name:/net gamma by strike and recorded minute/})).toBeNull();
    expect(api.getGammaSessionMap.mock.calls.some(([sym,,day])=>sym==='SPY' && day==='2026-11-09')).toBe(true);
    fireEvent.click(screen.getByText('Latest session'));await act(async()=>{});
    expect(screen.getByRole('img',{name:/SPY net gamma by strike and recorded minute/})).toBeInTheDocument();
});
it('explains enabled future recording only for a confirmed empty latest pinned session',async()=>{
    api.getGammaSessionMap.mockImplementation(s=>Promise.resolve({symbol:s,status:'unavailable',tracking:true,
        session_date:'2026-11-10',reason:s==='QQQ'?'Storage unavailable.':'No strike history recorded for this session.'}));
    render(<GammaBoard/>);await act(async()=>{});
    expect(screen.getAllByText(/New minutes will appear/)).toHaveLength(2);
    expect(within(screen.getByRole('article',{name:'QQQ gamma exposure'})).queryByText(/New minutes will appear/)).toBeNull();
    fireEvent.change(screen.getByLabelText('Recorded session'),{target:{value:'2026-11-10'}});await act(async()=>{});
    expect(screen.queryByText(/New minutes will appear/)).toBeNull();
});
it('recording an extra ticker requires its explicit server acknowledgement, independently of the local card',async()=>{
    api.getGammaSessionMap.mockImplementation(s=>Promise.resolve({symbol:s,status:'unavailable',reason:'No recordings.',tracking:false}));
    api.setGammaSessionTracking.mockResolvedValueOnce({error:true,status:409}).mockResolvedValueOnce({symbol:'AAPL',tracking:true});
    render(<GammaBoard/>); await act(async()=>{});
    fireEvent.change(screen.getByLabelText('Ticker to add'),{target:{value:'AAPL'}});fireEvent.click(screen.getByRole('button',{name:'Add'}));await act(async()=>{});
    expect(api.setGammaSessionTracking).not.toHaveBeenCalled();
    const card=within(screen.getByRole('article',{name:'AAPL gamma exposure'}));
    fireEvent.click(card.getByText('Record future minutes'));await act(async()=>{});
    expect(card.getByRole('alert')).toHaveTextContent('Three extra tickers');
    fireEvent.click(card.getByText('Record future minutes'));await act(async()=>{});
    expect(card.getByText('Stop recording')).toBeInTheDocument();
    fireEvent.click(card.getByText('Remove'));
    expect(api.setGammaSessionTracking.mock.calls).toEqual([['AAPL',true],['AAPL',true]]);
});
it('reads the dated map and writes tracking through the private no-store API',async()=>{
    const {api:real}=await vi.importActual('../api.js');
    const fetch=vi.spyOn(globalThis,'fetch').mockImplementation(async()=>new Response('{}',{status:200}));
    const controller=new AbortController();
    await real.getGammaSessionMap('SPX',controller.signal,'2026-11-10');await real.setGammaSessionTracking('AAPL',true);
    expect(fetch.mock.calls[0][0]).toMatch(/time-map\/SPX\?session_date=2026-11-10$/);
    expect(fetch.mock.calls[0][1]).toMatchObject({signal:controller.signal,cache:'no-store'});
    expect(fetch.mock.calls[1][0]).toMatch(/time-map\/AAPL\/tracking$/);
    expect(fetch.mock.calls[1][1]).toMatchObject({method:'POST',cache:'no-store',body:'{"enabled":true}'});
});
