// Offline, deterministic fixtures only. No application/server configuration imports.
import Graph from 'graphology';
import forceAtlas2 from 'graphology-layout-forceatlas2';
import { performance } from 'node:perf_hooks';
import os from 'node:os';
import { startIncrementalLayout } from '../src/canvas/incrementalLayout.js';
const settings={gravity:1,scalingRatio:2,barnesHutOptimize:true,slowDown:5,adjustSizes:false,strongGravityMode:false};
function fixture(n){const g=new Graph();for(let i=0;i<n;i++)g.addNode(String(i),{x:Math.sin(i*7)*100+i,y:Math.cos(i*11)*90-i,size:2+i%4});for(let i=1;i<n;i++){g.addEdge(String(i-1),String(i),{weight:.2+i%7});if(i>2)g.addEdge(String(i-3),String(i),{weight:i%3});}return g;}
async function run(n,iterations){
    const a=fixture(n),b=a.copy();let ticks=0;const timer=setInterval(()=>ticks++,1);
    const start=performance.now();forceAtlas2.assign(a,{iterations,settings});const baseline=performance.now()-start;const baselineTicks=ticks;
    await new Promise(r=>setImmediate(r));ticks=0;const taskDurations=[];let setupMs,largestIterationMs=0;const nextStart=performance.now();
    await new Promise((resolve,reject)=>{
        const t=performance.now();startIncrementalLayout(b,{iterations,settings,schedule:cb=>{const id=setImmediate(()=>{const s=performance.now();cb();taskDurations.push(performance.now()-s);});return()=>clearImmediate(id);},onProgress:p=>{largestIterationMs=Math.max(largestIterationMs,p.largestIterationMs);},onComplete:()=>setImmediate(resolve),onError:reject,onCancel:reason=>reject(Error(reason))});setupMs=performance.now()-t;
    });
    const incremental=performance.now()-nextStart;const incrementalTicks=ticks;clearInterval(timer);
    let maxCoordinateError=0;b.forEachNode((id,v)=>{maxCoordinateError=Math.max(maxCoordinateError,Math.abs(v.x-a.getNodeAttribute(id,'x')),Math.abs(v.y-a.getNodeAttribute(id,'y')));});
    if(maxCoordinateError!==0)throw Error('Coordinate parity failed');
    return {nodes:n,edges:a.size,iterations,baseline:{totalWallMs:baseline,largestBlockingTaskMs:baseline,heartbeatTicksBeforeReturn:baselineTicks,largestSingleIterationMs:null},incremental:{totalWallMs:incremental,setupMs,largestSingleIterationMs:largestIterationMs,largestBlockingTaskMs:Math.max(setupMs,...taskDurations),scheduledTaskCount:taskDurations.length,heartbeatTicksBeforeCompletion:incrementalTicks},maxCoordinateError};
}
const output={kind:'bounded offline Node synthetic responsiveness experiment',node:process.version,platform:process.platform,arch:process.arch,cpu:os.cpus()[0].model,versions:{graphology:'0.26.0',forceAtlas2:'0.10.1'},settings,maxBatchMs:4,maxBatchIterations:8,warmup:'one unrecorded paired250node220iteration run',runs:[],limits:['Node setImmediate scheduling, not a browser paint or WebGL benchmark.',
'Baseline public assign exposes no per-iteration timing; its largest single iteration is unmeasured (null). Incremental per-iteration timestamps add overhead included in its wall time.','Largest task includes synchronous setup and full scheduled callbacks; individual FA2 iteration and final matrix apply are indivisible, so 4ms is a soft budget.','Fixture creation excluded from both runs; adapter setup and mutation guards included.','Each recorded pair runs synchronous baseline before incremental; warmup/order/GC can affect throughput.','No live latency, network, JSON parsing or real product root-cause claim.']};
await run(250,220);for(const [n,it]of[[250,220],[900,80]])for(let repeat=0;repeat<3;repeat++)output.runs.push({repeat:repeat+1,...await run(n,it)});
console.log(JSON.stringify(output,null,2));
