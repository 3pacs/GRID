import { expect, it, vi } from 'vitest';
import Graph from 'graphology';
import forceAtlas2 from 'graphology-layout-forceatlas2';
import { startIncrementalLayout } from '../canvas/incrementalLayout.js';
const settings={gravity:1,scalingRatio:2,barnesHutOptimize:true,slowDown:5,adjustSizes:false,strongGravityMode:false};
export function fixture(count=24) {
    const g=new Graph();
    for(let i=0;i<count;i++)g.addNode(String(i),{x:Math.sin(i*7)*100+i,y:Math.cos(i*11)*90-i,size:2+(i%4),fixed:i===0,label:`Node ${i}`,source_class:'synthetic'});
    for(let i=1;i<count;i++) {g.addEdge(String(i-1),String(i),{weight:0.2+(i%7),provenance:'synthetic'});if(i>2)g.addEdge(String(i-3),String(i),{weight:i%3});}
    return g;
}
function queued(g, options={}) {
    const queue=[];const onComplete=vi.fn(),onCancel=vi.fn(),onError=vi.fn(),onProgress=vi.fn();
    const cancel=startIncrementalLayout(g,{iterations:220,settings,maxBatchIterations:7,schedule:(cb)=>{const e={cb,active:true};queue.push(e);return()=>{e.active=false;};},now:()=>0,onComplete,onCancel,onError,onProgress,...options});
    const next=()=>{const e=queue.shift();if(e?.active)e.cb();}; const finish=()=>{let n=0;while(queue.length){next();if(++n>1000)throw Error('unbounded');}};
    return {next,finish,queue,cancel,onComplete,onCancel,onError,onProgress};
}
it.each([1,24,250])('same persistent matrices preserve exact full single-assign coordinates at %i nodes',count=>{
    const a=fixture(count),b=a.copy();const attrs=a.export();forceAtlas2.assign(b,{iterations:220,settings});const q=queued(a);q.finish();
    expect(q.onComplete).toHaveBeenCalledTimes(1);expect(q.onError).not.toHaveBeenCalled();
    a.forEachNode((id,v)=>{expect(v.x).toBeCloseTo(b.getNodeAttribute(id,'x'),5);expect(v.y).toBeCloseTo(b.getNodeAttribute(id,'y'),5);expect(v.source_class).toBe('synthetic');});
    expect(a.export().edges).toEqual(attrs.edges);
    const withoutPositions=(list)=>list.map(n=>({...n,attributes:Object.fromEntries(Object.entries(n.attributes).filter(([k])=>k!=='x'&&k!=='y'))}));
    expect(withoutPositions(a.export().nodes)).toEqual(withoutPositions(attrs.nodes));expect(a.order).toBe(count);expect(q.onProgress.mock.calls.at(-1)[0].completed).toBe(220);
});
it('yields before computation and between batches without reducing iteration total',()=>{
    const g=fixture(),before=g.export();const q=queued(g);expect(q.onProgress).not.toHaveBeenCalled();q.next();expect(q.onProgress).not.toHaveBeenCalled();
    q.next();expect(q.onProgress.mock.calls[0][0].completed).toBe(7);expect(q.onComplete).not.toHaveBeenCalled();expect(g.export()).toEqual(before);
    q.finish();expect(q.onProgress.mock.calls.at(-1)[0].completed).toBe(220);expect(q.onProgress.mock.calls.length).toBe(32);
});
it.each(['drag','pin','weight','topology'])('cancels without overwriting %s mutation during batches',kind=>{
    const g=fixture();const q=queued(g);q.next();q.next();
    if(kind==='drag')g.setNodeAttribute('1','x',12345);
    if(kind==='pin')g.setNodeAttribute('1','fixed',true);
    if(kind==='weight')g.setEdgeAttribute(g.edges()[0],'weight',999);
    if(kind==='topology')g.addNode('new',{x:1,y:2,size:3});
    const modified=g.export();q.finish();expect(g.export()).toEqual(modified);expect(q.onCancel).toHaveBeenCalledTimes(1);expect(q.onComplete).not.toHaveBeenCalled();
});
it('drag during initial paint wins before matrix initialization',()=>{const g=fixture();const q=queued(g);g.setNodeAttribute('1','x',999);q.finish();expect(q.onCancel).toHaveBeenCalledTimes(1);expect(g.getNodeAttribute('1','x')).toBe(999);});
it('explicit cleanup cancels scheduled work and suppresses completions',()=>{const g=fixture();const initial=g.export();const q=queued(g);q.next();q.next();q.cancel();q.finish();expect(g.export()).toEqual(initial);expect(q.onComplete).not.toHaveBeenCalled();});
it('fixed coordinates retain original double precision',()=>{const g=fixture();g.mergeNodeAttributes('0',{x:0.123456789123,y:0.987654321987});const q=queued(g);q.finish();expect(g.getNodeAttribute('0','x')).toBe(0.123456789123);expect(g.getNodeAttribute('0','y')).toBe(0.987654321987);});
it('invalid settings retain graph with explicit error',()=>{const g=fixture();const initial=g.export();const q=queued(g,{settings:{gravity:-1}});q.finish();expect(q.onError).toHaveBeenCalledTimes(1);expect(g.export()).toEqual(initial);});
