import React, { StrictMode } from 'react';
import { act, cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, afterEach, expect, it, vi } from 'vitest';
import Graph from 'graphology';
const mocks = vi.hoisted(() => ({ api: { getCanvasGraph: vi.fn(), getCanvasDots: vi.fn(), getLatestSweep: vi.fn(), getBoard: vi.fn(), getNodeDetail: vi.fn() } }));
vi.mock('../api.js', () => ({ api: mocks.api }));
vi.mock('../hooks/useEventStream.js', () => ({ useEventStream: vi.fn() }));
vi.mock('../canvas/hooks/useKeyboardShortcuts.js', () => ({ useKeyboardShortcuts: vi.fn() }));
vi.mock('../canvas/hooks/useCommunities.js', () => ({ useCommunities: () => ({ communities: new Map(), communityColors: {}, communityLabels: {} }) }));
vi.mock('../canvas/SigmaGraph.jsx', () => ({ default: () => <div data-testid="graph">Graph overview</div> }));
vi.mock('../canvas/LayerControls.jsx', () => ({ default: () => null }));
vi.mock('../canvas/TemporalScrubber.jsx', () => ({ default: () => null }));
vi.mock('../canvas/ContextMenu.jsx', () => ({ default: () => null }));
vi.mock('../canvas/panels/DetailPanel.jsx', () => ({ default: ({onInvestigate}) => <button onClick={onInvestigate}>Investigate selected</button> }));
vi.mock('../canvas/panels/SweepPanel.jsx', () => ({ default: ({sweep}) => <div data-testid="sweep">{sweep?.top_k?.map(x=>x.ticker).join(',') || 'No sweep'}</div> }));
import useCanvasStore from '../canvas/CanvasStore.js';
import GothamCanvas from '../canvas/GothamCanvas.jsx';
const payload = { nodes: [{id:'a:root',type:'actor',label:'Root',x:12,y:34}], edges: [] };
const dot = {type:'money_trail',description:'Synthetic full finding',inputs:{count:3},actors:['Root','Other']};
const deferred = () => { let resolve, reject; const promise = new Promise((a,b)=>{resolve=a;reject=b;}); return {promise, resolve, reject}; };
beforeEach(() => {
    vi.clearAllMocks(); sessionStorage.clear(); window.history.replaceState(null,'','#/canvas'); window.innerWidth = 1200;
    useCanvasStore.setState({ graph:new Graph(),selectedNode:null,detailPanelOpen:false,detailData:null,loading:false,contextMenu:{visible:false},activeLayers:new Set(['financial','insider']),boardId:null,boardName:'Untitled Investigation',searchQuery:'',sweep:null,verdictsByTicker:{} });
    mocks.api.getCanvasGraph.mockResolvedValue(payload); mocks.api.getCanvasDots.mockResolvedValue({connections:[dot]});
    mocks.api.getLatestSweep.mockResolvedValue({top_k:[{ticker:'TEST',verdict:'buy'}]}); mocks.api.getNodeDetail.mockResolvedValue({error:true});
    vi.stubGlobal('fetch',vi.fn(()=>{throw new Error('Network forbidden in synthetic tests');}));
});
afterEach(() => { cleanup(); vi.unstubAllGlobals(); });
const mounted = async (strict=false) => { const r=render(strict?<StrictMode><GothamCanvas/></StrictMode>:<GothamCanvas/>); await waitFor(()=>expect(mocks.api.getCanvasGraph).toHaveBeenCalled()); return r; };
const feed = () => screen.getByRole('button',{name:'Connected dots feed'});
it('default overview preserves graph request, no optional helper reads or confirmed empty claims', async()=>{
    await mounted(); expect(mocks.api.getCanvasGraph).toHaveBeenCalledWith('all',6,'all',null,250);
    expect(mocks.api.getCanvasDots).not.toHaveBeenCalled(); expect(mocks.api.getLatestSweep).not.toHaveBeenCalled();
    expect(feed()).toHaveTextContent('Not requested'); expect(screen.getByTestId('graph')).toBeVisible(); expect(fetch).not.toHaveBeenCalled();
});
it('explicit feed demand shares pending read across close/reopen, completion never reopens it',async()=>{
    const d=deferred();mocks.api.getCanvasDots.mockReturnValue(d.promise); await mounted();
    fireEvent.click(feed()); await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(1));
    fireEvent.click(feed());fireEvent.click(feed());fireEvent.click(feed());
    await act(async()=>d.resolve({connections:[dot]})); expect(feed()).toHaveAttribute('aria-expanded','false');
    expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(1);
});
it('Dots button retains full finding/provenance and explicit repeated demand revalidates',async()=>{
    await mounted();fireEvent.click(screen.getByRole('button',{name:'Connect dots'}));
    expect(await screen.findByText(dot.description)).toBeVisible(); expect(screen.getByText('count=3')).toBeVisible();
    fireEvent.click(screen.getByRole('button',{name:'Connect dots'})); await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(2));
});
it('manual retry recovers error; empty is distinguished from unavailable and not requested',async()=>{
    mocks.api.getCanvasDots.mockResolvedValueOnce({error:true}).mockResolvedValueOnce({connections:[]});await mounted();
    fireEvent.click(feed());fireEvent.click(await screen.findByRole('button',{name:'Retry Connected dots'}));
    expect(await screen.findByText(/No findings returned/)).toBeVisible();expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(2);
});
it('cached dots remain usable with honest error/freshness state and unchanged TTL timestamp',async()=>{
    const key='grid:canvas-dots:all', stamp=Date.now()-1000;
    sessionStorage.setItem(key,JSON.stringify({savedAt:stamp,value:[dot]})); mocks.api.getCanvasDots.mockRejectedValue(new Error('offline'));
    await mounted();fireEvent.click(feed());expect(await screen.findByText(dot.description)).toBeVisible();
    expect(await screen.findByRole('button',{name:'Retry Connected dots'})).toBeVisible(); expect(screen.getByText(/Showing previously loaded findings/)).toBeVisible();
    expect(JSON.parse(sessionStorage.getItem(key)).savedAt).toBe(stamp);
});
it('context change ignores late dots and does not request a new board without demand',async()=>{
    const d=deferred();mocks.api.getCanvasDots.mockReturnValueOnce(d.promise); await mounted();fireEvent.click(feed());
    await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(1)); act(()=>useCanvasStore.setState({boardId:'another-board'}));
    await act(async()=>d.resolve({connections:[dot]})); expect(screen.queryByText(dot.description)).not.toBeInTheDocument();
    expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(1);expect(sessionStorage.getItem('grid:canvas-dots:all')).toBeNull();
});
it('unmount suppresses dots session writes and feed state updates',async()=>{
    const d=deferred();mocks.api.getCanvasDots.mockReturnValue(d.promise);const r=await mounted();fireEvent.click(feed());
    await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledTimes(1));r.unmount();await act(async()=>d.resolve({connections:[dot]}));
    expect(sessionStorage.getItem('grid:canvas-dots:all')).toBeNull();
});
it('restored board keeps layout and demands verdict read even on mobile without side panel',async()=>{
    window.innerWidth=500;window.history.replaceState(null,'','#/canvas?board=saved');
    mocks.api.getBoard.mockResolvedValue({id:'saved',name:'Saved',filters:{layers:['financial','verdicts'],timeRange:{start:'2026-01-01',end:null}},graph_state:payload}); render(<GothamCanvas/>);
    await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledWith({horizonDays:90})); await waitFor(()=>expect(useCanvasStore.getState().sweep?.top_k[0].ticker).toBe('TEST'));
    expect(mocks.api.getCanvasDots).not.toHaveBeenCalled();expect(mocks.api.getCanvasGraph).not.toHaveBeenCalled();
    expect(useCanvasStore.getState().graph.getNodeAttribute('a:root','x')).toBe(12); expect(useCanvasStore.getState().timeRange.start).toBe('2026-01-01');expect(screen.queryByTestId('sweep')).toBeNull();
});
it('verdict demand preserves 90-day/latest fallback and shares pending request on layer toggles',async()=>{
    const d=deferred();mocks.api.getLatestSweep.mockReturnValueOnce(d.promise).mockResolvedValueOnce({top_k:[{ticker:'FALLBACK'}],provenance:'synthetic'});
    await mounted();act(()=>useCanvasStore.getState().toggleLayer('verdicts'));await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1));
    act(()=>useCanvasStore.getState().toggleLayer('verdicts'));act(()=>useCanvasStore.getState().toggleLayer('verdicts'));
    await act(async()=>d.resolve({error:true}));expect(await screen.findByText('FALLBACK')).toBeVisible();
    expect(mocks.api.getLatestSweep.mock.calls).toEqual([[{horizonDays:90}],[]]);expect(useCanvasStore.getState().sweep.provenance).toBe('synthetic');
});
it('sweep error has manual recovery',async()=>{
    mocks.api.getLatestSweep.mockResolvedValueOnce({error:true}).mockResolvedValueOnce({error:true}).mockResolvedValueOnce({top_k:[]});
    await mounted();act(()=>useCanvasStore.getState().toggleLayer('verdicts'));fireEvent.click(await screen.findByRole('button',{name:'Retry Sweep verdicts'}));
    await waitFor(()=>expect(useCanvasStore.getState().sweep?.top_k).toEqual([]));expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(3);
});
it('unmount during first sweep prevents fallback and store mutation',async()=>{
    const d=deferred();mocks.api.getLatestSweep.mockReturnValue(d.promise);useCanvasStore.setState({activeLayers:new Set(['verdicts'])});
    const r=await mounted();await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1));r.unmount();await act(async()=>d.resolve({error:true}));
    expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1);expect(useCanvasStore.getState().sweep).toBeNull();
});
it('StrictMode cleanup/setup reuses pending sweep and accepts result for current lifecycle',async()=>{
    const d=deferred();mocks.api.getLatestSweep.mockReturnValue(d.promise);useCanvasStore.setState({activeLayers:new Set(['verdicts'])});
    await mounted(true);await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1));await act(async()=>d.resolve({top_k:[{ticker:'STRICT'}]}));expect(await screen.findByText('STRICT')).toBeVisible();
});
it('unavailable optional response is explicit and retains cached findings',async()=>{
    sessionStorage.setItem('grid:canvas-dots:all',JSON.stringify({savedAt:Date.now(),value:[dot]}));mocks.api.getCanvasDots.mockResolvedValue(null);
    await mounted();fireEvent.click(feed());expect(await screen.findByText(/Persisted findings are unavailable/)).toBeVisible();
    expect(screen.getByText(dot.description)).toBeVisible();expect(screen.getByRole('button',{name:'Retry Connected dots'})).toBeVisible();
});
it('expired cached findings are not used; demand still revalidates without altering keys or TTL',async()=>{
    sessionStorage.setItem('grid:canvas-dots:all',JSON.stringify({savedAt:Date.now()-120001,value:[dot]}));const d=deferred();mocks.api.getCanvasDots.mockReturnValue(d.promise);
    await mounted();fireEvent.click(feed());await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledWith('all'));
    expect(screen.queryByText(dot.description)).toBeNull();await act(async()=>d.resolve({connections:[]}));expect(await screen.findByText(/No findings returned/)).toBeVisible();
});
it('stale sweep completion after board navigation cannot replace global verdict store',async()=>{
    const d=deferred(), d2=deferred();mocks.api.getLatestSweep.mockReturnValueOnce(d.promise).mockReturnValueOnce(d2.promise);
    useCanvasStore.setState({activeLayers:new Set(['verdicts'])});await mounted();await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1));
    act(()=>useCanvasStore.setState({boardId:'new-board'}));await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(2));
    await act(async()=>d.resolve({top_k:[{ticker:'STALE'}]}));expect(useCanvasStore.getState().sweep).toBeNull();
    await act(async()=>d2.resolve({top_k:[{ticker:'CURRENT'}]}));expect(await screen.findByText('CURRENT')).toBeVisible();
});
it('StrictMode failed first sweep still falls back once for the adopted generation',async()=>{
    const d=deferred();mocks.api.getLatestSweep.mockReturnValueOnce(d.promise).mockResolvedValueOnce({top_k:[{ticker:'ADOPTED'}]});
    useCanvasStore.setState({activeLayers:new Set(['verdicts'])});await mounted(true);await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(1));
    await act(async()=>d.resolve({error:true}));expect(await screen.findByText('ADOPTED')).toBeVisible();expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(2);
});
it('native feed and retry buttons are keyboard-focusable with accessible names and expansion state',async()=>{
    mocks.api.getCanvasDots.mockResolvedValue({error:true});await mounted();const b=feed();b.focus();expect(b).toHaveFocus();expect(b.tagName).toBe('BUTTON');expect(b).toHaveAttribute('type','button');
    fireEvent.click(b);const retry=await screen.findByRole('button',{name:'Retry Connected dots'});retry.focus();expect(retry).toHaveFocus();expect(retry.tagName).toBe('BUTTON');expect(b).toHaveAttribute('aria-expanded','true');
});
it('selected-node investigate retains explicit node context without fetching on mere selection',async()=>{
    await mounted();act(()=>useCanvasStore.getState().selectNode('a:root','actor'));
    await waitFor(()=>expect(screen.getByRole('button',{name:'Investigate selected'})).toBeVisible());expect(mocks.api.getCanvasDots).not.toHaveBeenCalled();
    fireEvent.click(screen.getByRole('button',{name:'Investigate selected'}));await waitFor(()=>expect(mocks.api.getCanvasDots).toHaveBeenCalledWith('a:root'));expect(screen.getByText('Context: a:root')).toBeVisible();
});
it('cached-first overview still revalidates full graph and preserves deep nodes/positions',async()=>{
    const d=deferred();const cached={nodes:[{id:'a:cached',type:'actor',label:'Cached',x:55,y:66,graph_depth:5}],edges:[]};
    sessionStorage.setItem('grid:canvas-graph:all:6:all:none:250',JSON.stringify({savedAt:Date.now(),value:cached}));mocks.api.getCanvasGraph.mockReturnValue(d.promise);
    await mounted();expect(useCanvasStore.getState().graph.hasNode('a:cached')).toBe(true);expect(useCanvasStore.getState().graph.getNodeAttribute('a:cached','x')).toBe(55);
    await act(async()=>d.resolve(payload));expect(useCanvasStore.getState().graph.hasNode('a:root')).toBe(true);expect(useCanvasStore.getState().graph.hasNode('a:cached')).toBe(false);
    expect(mocks.api.getCanvasDots).not.toHaveBeenCalled();expect(mocks.api.getLatestSweep).not.toHaveBeenCalled();
});
it('post-completion verdict reopen revalidates rather than introducing permanent success memoization',async()=>{
    await mounted();act(()=>useCanvasStore.getState().toggleLayer('verdicts'));await waitFor(()=>expect(useCanvasStore.getState().sweep?.top_k[0].ticker).toBe('TEST'));
    act(()=>useCanvasStore.getState().toggleLayer('verdicts'));act(()=>useCanvasStore.getState().toggleLayer('verdicts'));await waitFor(()=>expect(mocks.api.getLatestSweep).toHaveBeenCalledTimes(2));
});
