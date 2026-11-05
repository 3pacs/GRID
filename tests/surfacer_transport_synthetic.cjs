const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
const root = path.resolve(__dirname, '..');
let clock = 0, nextTimer = 0;
const timers = new Map();
const sandbox = {
    window: { location: { origin: 'https://synthetic.invalid' }, dispatchEvent() {} },
    getStoredToken: () => null, clearAuthSession() {}, useRealtimeStore: {},
    Map, AbortController, Event, TextEncoder, URLSearchParams,
    Date: class extends Date { static now() { return clock; } },
    setTimeout(fn, delay) { const id = ++nextTimer; timers.set(id, {fn, at: clock + delay}); return id; },
    clearTimeout(id) { timers.delete(id); },
};
vm.createContext(sandbox);
let source = fs.readFileSync(path.join(root, 'pwa/src/api.js'), 'utf8')
    .replace(/^import .*;\n/gm, '').replace(/^export /gm, '')
    .replace(/^const api = new GRIDApi\(\);/m, '').replace(/^\{ GRIDApiError \};/m, '');
vm.runInContext(source + '\nglobalThis.Client = GRIDApi;', sandbox);
const tick = async () => { for (let i = 0; i < 12; i++) await Promise.resolve(); };
const advance = async ms => { clock += ms; for (const [id, t] of [...timers]) if (t.at <= clock) { timers.delete(id); t.fn(); } await tick(); };
const deferred = () => { let resolve, reject; const promise = new Promise((a,b) => {resolve=a;reject=b}); return {promise,resolve,reject}; };
const ok = data => ({ok:true,status:200,json:async()=>data});
const route = '/api/v1/surfacer/candidates?limit=18&fresh_only=false&queue_missing_data=false';
(async () => {
    const client = new sandbox.Client(); let count=0; let pending=deferred(); let signal;
    sandbox.fetch = (url, options) => {count++;signal=options.signal;return pending.promise;};
    const p1=client.get(route), p2=client.get(route); assert.equal(count,1);
    pending.resolve(ok({candidates:[]})); await Promise.all([p1,p2]);
    await client.get(route); assert.equal(count,1);
    await advance(59999); await client.get(route); assert.equal(count,1);
    await advance(1); pending=deferred(); const p3=client.get(route); assert.equal(count,2);
    await advance(30000); assert.equal((await p3).code,'request_timeout'); assert(signal.aborted);
    assert.equal(client._inflight.size,0);
    assert(client._cache.get(route).expiresAt <= clock);
    client._cache.delete(route); // Existing expired entries may remain, but cannot be served.
    // Old request ignores abort and finishes after a manual retry: no old cache write.
    const old=pending; pending=deferred(); const retry=client.get(route); assert.equal(count,3);
    old.resolve(ok({old:true})); await tick(); assert(!client._cache.has(route)); assert.equal(client._inflight.size,1);
    pending.resolve(ok({recovered:true})); assert((await retry).recovered); assert.equal(client._inflight.size,0);
    // Deadline covers body parsing, not just receipt of headers.
    client.clearCache(); const body=deferred(); sandbox.fetch=async()=>({ok:true,status:200,json:()=>body.promise});
    const bodyRequest=client.get(route); await tick(); await advance(30000); assert.equal((await bodyRequest).code,'request_timeout');
    body.resolve({late:true}); await tick(); assert(!client._cache.has(route));
    // Error bodies are also bounded; no delayed auth side effect after timeout.
    const errorBody=deferred(); sandbox.fetch=async()=>({ok:false,status:401,text:()=>errorBody.promise});
    const errorRequest=client.get(route); await tick(); await advance(30000); assert.equal((await errorRequest).code,'request_timeout');
    errorBody.resolve('unauthorized'); await tick(); assert.equal(sandbox.window.location.hash,undefined);
    sandbox.fetch=async()=>({ok:false,status:503,statusText:'Unavailable',text:async()=>''});
    assert.equal((await client.get(route)).status,503); assert(!client._cache.has(route));
    sandbox.fetch=async()=>{throw Error('offline');}; assert.equal((await client.get(route)).message,'offline');
    sandbox.fetch=async()=>ok({again:true}); assert((await client.get(route)).again);
    assert.equal(timers.size,0);
    client.clearCache(); let preloadPending=deferred(); let preloadCalls=0;
    sandbox.fetch=()=>{preloadCalls++;return preloadPending.promise;};
    const preload=client.preload([route]); const foreground=client.get(route); assert.equal(preloadCalls,1);
    preloadPending.resolve(ok({preloaded:true})); await preload; assert((await foreground).preloaded);
    // Caller cancellation is forwarded rather than silently replaced.
    client.clearCache(); const caller=new AbortController(); sandbox.fetch=(url,opts)=>{signal=opts.signal;return new Promise((resolve,reject)=>signal.addEventListener('abort',()=>reject(Error('cancelled'))));};
    const cancelled=client._fetch(route,{signal:caller.signal}); caller.abort(); assert.equal((await cancelled).message,'cancelled'); assert(signal.aborted);
    // Other routes retain current unbounded behavior; opt-in is route-local.
    sandbox.fetch=async(url,opts)=>{assert.equal(opts.signal,undefined);return ok({other:true});};
    assert((await client.get('/api/v1/other')).other);
    // Exercise actual component load callback with synthetic state setters.
    const jsx=fs.readFileSync(path.join(root,'pwa/src/views/Surfacer.jsx'),'utf8');
    const start=jsx.indexOf('    const loadInFlight = useRef(false);'); const end=jsx.indexOf('\n    useEffect',start);
    const state={loading:null,error:null,payload:null,selected:''}; let uiCalls=0, uiPending=deferred();
    const ui={ useRef: v=>({current:v}), useCallback:fn=>fn, api:{get:()=>{uiCalls++;return uiPending.promise;}},
        setLoading:v=>state.loading=v,setError:v=>state.error=v,setPayload:v=>state.payload=v,
        setSelectedId:fn=>state.selected=fn(state.selected)};
    vm.createContext(ui); vm.runInContext(jsx.slice(start,end)+'\nglobalThis.load = load;',ui);
    const u1=ui.load(); await ui.load(); assert.equal(uiCalls,1); assert(state.loading);
    uiPending.resolve({error:true,message:'timeout'}); await u1; assert.equal(state.loading,false); assert.equal(state.error,'timeout');
    uiPending=deferred(); const u2=ui.load(); uiPending.reject(Error('unexpected')); await u2; assert.equal(state.loading,false); assert.match(state.error,/retry/);
    uiPending=deferred(); const u3=ui.load(); uiPending.resolve({candidates:[{id:'synthetic'}],meta:{calibration_note:'retained'}}); await u3;
    assert.equal(state.error,''); assert.equal(state.selected,'synthetic'); assert.equal(state.payload.meta.calibration_note,'retained');
    assert(jsx.includes("{loading ? 'Loading' : error ? 'Retry' : 'Refresh'}"));
    console.log('PASS frontend: pending dedup, 60s TTL boundary, fetch/body timeout, abort, late cache suppression, retry recovery, HTTP/network error, caller signal, scoped behavior, actual UI load states');
})().catch(err=>{console.error(err);process.exitCode=1;});
