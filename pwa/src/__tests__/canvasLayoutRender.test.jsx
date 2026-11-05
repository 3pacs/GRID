import React, { StrictMode, useEffect, useState } from 'react';
import { act, cleanup, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, expect, it, vi } from 'vitest';
import Graph from 'graphology';
import forceAtlas2 from 'graphology-layout-forceatlas2';
const renderer=vi.hoisted(()=>({load:vi.fn(),sigma:{refresh:vi.fn()}}));
vi.mock('@react-sigma/core',()=>({SigmaContainer:({children})=><div>{children}</div>,useLoadGraph:()=>renderer.load,useSigma:()=>renderer.sigma,useRegisterEvents:()=>vi.fn()}));
vi.mock('@sigma/edge-curve',()=>({default:class {}}));
vi.mock('../canvas/CommunityHulls.jsx',()=>({default:()=>null}));
import { GraphLoader } from '../canvas/SigmaGraph.jsx';
import useCanvasStore from '../canvas/CanvasStore.js';
function fixture(n=250){const g=new Graph();for(let i=0;i<n;i++)g.addNode(String(i),{x:Math.sin(i)*80,y:Math.cos(i)*60,size:3});for(let i=1;i<n;i++)g.addEdge(String(i-1),String(i),{weight:0.2+i%7});return g;}
function Heartbeat(){const [ticks,setTicks]=useState(0);useEffect(()=>{const t=setInterval(()=>setTicks(n=>n+1),5);return()=>clearInterval(t);},[]);return <span aria-label="heartbeat">{ticks}</span>;}
beforeEach(()=>{vi.clearAllMocks();useCanvasStore.setState({graph:fixture()});});afterEach(cleanup);
it('real React DOM heartbeat commits between real scheduled layout batches, before completion',async()=>{
    render(<><GraphLoader/><Heartbeat/></>);expect(await screen.findByText(/Arranging graph \(0\/220\)/)).toBeVisible();
    await waitFor(()=>expect(screen.getByRole('status').textContent).toMatch(/Arranging graph \(([1-9]\d*)\/220\)/));
    expect(Number(screen.getByLabelText('heartbeat').textContent)).toBeGreaterThan(0);expect(renderer.sigma.refresh).not.toHaveBeenCalled();
    await waitFor(()=>expect(renderer.sigma.refresh).toHaveBeenCalledTimes(1),{timeout:5000});expect(screen.queryByRole('status')).toBeNull();
});
it('replacement cancels old generation, StrictMode completes only the current graph',async()=>{
    const old=useCanvasStore.getState().graph,oldState=old.export();render(<StrictMode><GraphLoader/></StrictMode>);
    await waitFor(()=>expect(screen.getByRole('status').textContent).not.toContain('(0/220)'));
    const current=fixture(24),baseline=current.copy();forceAtlas2.assign(baseline,{iterations:60,settings:{gravity:1,scalingRatio:2,barnesHutOptimize:true,slowDown:5,adjustSizes:false,strongGravityMode:false}});
    act(()=>useCanvasStore.setState({graph:current}));await waitFor(()=>expect(renderer.sigma.refresh).toHaveBeenCalledTimes(1),{timeout:5000});
    expect(old.export()).toEqual(oldState);current.forEachNode((id,a)=>expect(a.x).toBe(baseline.getNodeAttribute(id,'x')));
});
it('unmount suppresses late assignment and refresh',async()=>{
    const graph=useCanvasStore.getState().graph,initial=graph.export();const r=render(<GraphLoader/>);
    await waitFor(()=>expect(screen.getByRole('status').textContent).not.toContain('(0/220)'));r.unmount();
    await new Promise(resolve=>setTimeout(resolve,100));expect(graph.export()).toEqual(initial);expect(renderer.sigma.refresh).not.toHaveBeenCalled();
});
it.each(['full','partial'])('saved %s layout preserves supplied coordinates and unchanged fallback, no layout job',async kind=>{
    useCanvasStore.getState().loadGraph({nodes:[{id:'a',x:12.123456789,y:34},{id:'b',...(kind==='full'?{x:56,y:78}:{})}],edges:[{source:'a',target:'b'}]},{preserveLayout:true});
    const graph=useCanvasStore.getState().graph,initial=graph.export();render(<GraphLoader/>);
    await waitFor(()=>expect(renderer.sigma.refresh).toHaveBeenCalledTimes(1));await new Promise(resolve=>setTimeout(resolve,70));
    expect(graph.export()).toEqual(initial);expect(graph.getNodeAttribute('a','x')).toBe(12.123456789);expect(screen.queryByRole('status')).toBeNull();
});
it('mid-batch user pin pauses with truthful state and keeps current positions',async()=>{
    render(<GraphLoader/>);await waitFor(()=>expect(screen.getByRole('status').textContent).not.toContain('(0/220)'));
    const graph=useCanvasStore.getState().graph;act(()=>graph.setNodeAttribute('1','fixed',true));expect(await screen.findByText(/Layout paused after graph interaction/)).toBeVisible();expect(renderer.sigma.refresh).not.toHaveBeenCalled();
});
it('empty and >900 graphs preserve existing skip behavior',async()=>{
    useCanvasStore.setState({graph:new Graph()});const r=render(<GraphLoader/>);expect(renderer.load).not.toHaveBeenCalled();
    act(()=>useCanvasStore.setState({graph:fixture(901)}));await waitFor(()=>expect(renderer.sigma.refresh).toHaveBeenCalledTimes(1));expect(screen.queryByRole('status')).toBeNull();r.unmount();
});

it('replacement by empty graph cancels running job and clears arranging status',async()=>{
    const graph=useCanvasStore.getState().graph,initial=graph.export();render(<GraphLoader/>);
    await waitFor(()=>expect(screen.getByRole('status').textContent).not.toContain('(0/220)'));
    act(()=>useCanvasStore.setState({graph:new Graph()}));await waitFor(()=>expect(screen.queryByRole('status')).toBeNull());
    await new Promise(resolve=>setTimeout(resolve,70));expect(graph.export()).toEqual(initial);expect(renderer.sigma.refresh).not.toHaveBeenCalled();
});
