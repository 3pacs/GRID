import React, { useEffect, useMemo, useRef, useState } from 'react';

const MINUTE = 60000;
const et = new Intl.DateTimeFormat('en-US', { timeZone:'America/New_York', hour:'numeric', minute:'2-digit' });
const fmt = new Intl.NumberFormat('en-US', { maximumFractionDigits:2 });
const compact = new Intl.NumberFormat('en-US', { notation:'compact', maximumFractionDigits:2 });
const signed = n => (n > 0 ? '+$' : n < 0 ? '−$' : '$') + compact.format(Math.abs(n));
const minute = t => Math.floor(t / MINUTE) * MINUTE;

export function validMap(map) {
    const start = Date.parse(map.session_start), end = Date.parse(map.session_end);
    if (!Number.isFinite(start) || !Number.isFinite(end) || end <= start || end - start > 24 * 60 * MINUTE) return false;
    const {times, strikes, values} = map;
    return Array.isArray(times) && times.length > 0 && times.length <= Math.ceil((end-start) / MINUTE)
        && times.every((t,i) => Number.isFinite(t) && t >= start && t < end && (!i || minute(t) > minute(times[i-1])))
        && Array.isArray(strikes) && strikes.length > 0 && strikes.every((k,i) => Number.isFinite(k) && k > 0 && (!i || k > strikes[i-1]))
        && Array.isArray(values) && values.length === strikes.length && values.every(row => Array.isArray(row) && row.length === times.length
            && row.every(value => value === null || Number.isFinite(value)));
}

export function sessionGeometry(map, showAll = false) {
    const start = Date.parse(map.session_start), end = Date.parse(map.session_end);
    const minutes = Math.ceil((end-start)/MINUTE);
    const columns = new Map(map.times.map((t,c) => [minute(t), c]));
    const spot = (Array.isArray(map.spot) ? map.spot : []).filter(p => Array.isArray(p) && Number.isFinite(p[0]) && p[0]>=start && p[0]<end && Number.isFinite(p[1]) && p[1]>0);
    const prices = spot.map(p => p[1]);
    // Keep the recorded price range in view, with an instrument-relative margin.
    const margin = prices.length ? prices[prices.length-1] * .0125 : 0;
    const minPrice = prices.length ? Math.min(...prices)-margin : -Infinity;
    const maxPrice = prices.length ? Math.max(...prices)+margin : Infinity;
    let rows = map.strikes.map((strike, i) => ({ strike, values: map.values[i] }));
    const near = rows.filter(r=>r.strike>=minPrice && r.strike<=maxPrice);
    if (!showAll && near.length) rows = near;
    // Equally spaced listed strike rows; interpolate the spot between its two
    // neighboring labels, including irregular strike ladders.
    function y(price) {
        let lo=0, hi=rows.length;
        while(lo<hi) { const mid=Math.floor((lo+hi)/2); if(rows[mid].strike<price) lo=mid+1; else hi=mid; }
        if(lo===0) return (rows.length-.5)/rows.length;
        if(lo===rows.length) return .5/rows.length;
        const fraction = (price-rows[lo-1].strike)/(rows[lo].strike-rows[lo-1].strike);
        return 1-(lo-.5+fraction)/rows.length;
    }
    const magnitude=[];
    let strongest=null;
    rows.forEach((r,i) => r.values.forEach((v,c) => {
        if (Number.isFinite(v)) {
            if(v) magnitude.push(Math.abs(v));
            if(!strongest || Math.abs(v)>Math.abs(strongest.value)) strongest = {row:i, t:minute(map.times[c]), value:v};
        }
    }));
    magnitude.sort((a,b)=>a-b);
    const cap = magnitude[Math.ceil(.95*magnitude.length)-1] || 0;
    let previous=null;
    const segments=[];
    for(const [t,p] of spot){
        if(p<rows[0].strike || p>rows[rows.length-1].strike){ previous=null;continue; }
        const mt=minute(t);
        if(previous===null || mt-previous!==MINUTE) segments.push([]);
        segments[segments.length-1].push([(mt+MINUTE/2-start)/(end-start),y(p)]);
        previous=mt;
    }
    return {start,end,minutes,columns,rows,cap,segments,strongest,latest:minute(map.times[map.times.length-1])};
}

const W=780,H=440;
function draw(canvas, m, times) {
    if (!canvas) return;
    const ctx=canvas.getContext('2d'); if(!ctx)return;
    ctx.clearRect(0,0,W,H);
    const stepY=H/m.rows.length, stepX=W/m.minutes;
    m.rows.forEach((r,i)=>r.values.forEach((value,c)=>{
        if(value===null)return;
        const p=m.cap ? Math.sqrt(Math.min(1,Math.abs(value)/m.cap)) : 0;
        const end=value<0?[248,110,116]:[81,210,130];
        ctx.fillStyle='rgb('+[48,60,65].map((v,j)=>Math.round(v+(end[j]-v)*p)).join(',')+')';
        // Exactly one minute wide. No extension to the next recorded time.
        const x=(minute(times[c])-m.start)/(m.end-m.start)*W;
        ctx.fillRect(x,H-(i+1)*stepY,stepX,Math.max(1,stepY-.7));
    }));
    for(const [color,width] of [['#101719',4.3],['#fff4cc',2.1]]) {
        ctx.strokeStyle=color;ctx.fillStyle=color;ctx.lineWidth=width;ctx.lineJoin='round';ctx.lineCap='round';
        for(const points of m.segments) {
            ctx.beginPath();
            points.forEach(([x,y],i)=>i?ctx.lineTo(x*W,y*H):ctx.moveTo(x*W,y*H));
            if(points.length===1){ctx.arc(points[0][0]*W,points[0][1]*H,width/2,0,2*Math.PI);ctx.fill();}
            else ctx.stroke();
        }
    }
}

const confirmTime = new Intl.DateTimeFormat('en-US', {timeZone:'America/New_York',hour:'2-digit',minute:'2-digit',second:'2-digit',hourCycle:'h23'});
function flagMarkers(map, ledger, model) {
    if(map.symbol !== 'SPY' || !ledger || ledger.session_date !== map.session_date ||
        (ledger.symbol != null && ledger.symbol !== 'SPY') || !Array.isArray(ledger.events)) return [];
    const strikeRows=new Map(model.rows.map((r,i)=>[r.strike,i]));
    return ledger.events.flatMap(e=>{
        if(!e || !['first_cross','failed_reclaim'].includes(e.kind) || !strikeRows.has(e.red) || !Number.isFinite(e.target)) return [];
        const t=Date.parse(e.signal_time);
        if(!Number.isFinite(t) || t<model.start || t>model.end) return [];
        const name=e.kind==='first_cross'?'First cross':'Failed reclaim';
        return [{key:e.kind+'/'+e.red+'/'+t, kind:e.kind, x:(t-model.start)/(model.end-model.start),
            y:1-(strikeRows.get(e.red)+.5)/model.rows.length,
            label:`${name} · ${confirmTime.format(t)} ET · red ${fmt.format(e.red)} → target ${fmt.format(e.target)}`}];
    });
}
export default function GammaSessionMap({ map, ledger }) {
    const [showAll,setShowAll]=useState(false);
    const [pick,setPick]=useState(null);
    const model=useMemo(()=>sessionGeometry(map,showAll),[map,showAll]);
    const markers=useMemo(()=>flagMarkers(map,ledger,model),[map,ledger,model]);
    const [selectedFlag,setSelectedFlag]=useState(null);
    const activeFlag=markers.find(m=>m.key===selectedFlag);
    const canvas=useRef(null);
    useEffect(()=>{draw(canvas.current,model,map.times);},[model,map.times]);
    const initial=model.strongest || {row:Math.floor(model.rows.length/2),t:model.latest};
    let row=pick ? model.rows.findIndex(r=>r.strike===pick.strike) : initial.row;
    if(row<0)row=initial.row;
    const t=pick?.t>=model.start && pick.t<model.end ? pick.t : initial.t;
    const k=model.rows[row].strike;
    const c=model.columns.get(t);
    const value=c===undefined?null:model.rows[row].values[c];
    function select(r,time){setSelectedFlag(null);setPick({strike:model.rows[Math.max(0,Math.min(r,model.rows.length-1))].strike,
        t:Math.max(model.start,Math.min(model.end-MINUTE,time))});}
    function pointer(e){
        if(e.target.closest?.('[data-flag-marker]'))return;
        const rect=e.currentTarget.getBoundingClientRect();
        select(model.rows.length-1-Math.floor((e.clientY-rect.top)/rect.height*model.rows.length),
            model.start+Math.floor((e.clientX-rect.left)/rect.width*model.minutes)*MINUTE);
    }
    const labels=[...new Set(Array.from({length:Math.min(5,model.rows.length)},(_,i)=>
        Math.round(i*(model.rows.length-1)/Math.max(1,Math.min(5,model.rows.length)-1))))];
    return <div className="gb-session-map">
        <div className="gb-time-meta">{map.session_date} · {map.expiry ? 'Expiry '+map.expiry : 'Recorded expiries'} · {map.times.length} recorded minutes</div>
        <div className="gb-time-legend"><span className="gb-negative">− GEX</span><i/><span className="gb-positive">+ GEX</span>
            <span>±{signed(model.cap).replace('+','')}</span><span className="gb-spot-key">━ Spot</span></div>
        <div className="gb-time-chart"><div className="gb-ylabels" aria-hidden="true">
            {labels.map(i=><span key={i} style={{top:((model.rows.length-i-.5)/model.rows.length)*100+'%'}}>{fmt.format(model.rows[i].strike)}</span>)}</div>
            <div className="gb-time-raster" role="group" tabIndex={0}
                aria-label={map.symbol+' session heatmap. Left and right select a minute. Up and down select a strike.'}
                onKeyDown={e=>{const delta={ArrowLeft:[0,-1],ArrowRight:[0,1],ArrowUp:[1,0],ArrowDown:[-1,0]}[e.key];
                    if(delta){e.preventDefault();select(row+delta[0],t+delta[1]*MINUTE);}}}
                onPointerDown={pointer} onPointerMove={e=>{if(e.pointerType==='mouse')pointer(e);}}>
                <canvas ref={canvas} width={W} height={H} role="img" aria-label={map.symbol+' net gamma by strike and recorded minute'} />
                {markers.map(m=><button key={m.key} type="button" data-flag-marker={m.kind} className="gb-time-flag"
                    style={{left:(m.x*100)+'%',top:(m.y*100)+'%'}} aria-label={m.label} title={m.label}
                    onFocus={()=>setSelectedFlag(m.key)} onClick={()=>setSelectedFlag(m.key)}>
                    <span aria-hidden="true">{m.kind==='first_cross'?'▲':'▼'}</span></button>)}
                <i className="gb-time-reticle" style={{left:100*(t+MINUTE/2-model.start)/(model.end-model.start)+'%',
                    top:100*(model.rows.length-row-.5)/model.rows.length+'%'}} />
            </div><div className="gb-xlabels"><span>{et.format(model.start)}</span><span>{et.format((model.end+model.start)/2)} ET</span><span>{et.format(model.end)}</span></div>
        </div>
        <div className="gb-time-inspect" aria-live="polite"><strong>{et.format(t)} ET · {fmt.format(k)} strike</strong>
            <b className={value>0?'gb-positive':value<0?'gb-negative':''}>{Number.isFinite(value)?signed(value):'No observation'}</b></div>
        {markers.length>0 && <div className="gb-map-flag-caption">
            <span>▲ First cross · ▼ Failed reclaim</span>
            {activeFlag && <strong aria-live="polite">{activeFlag.label}</strong>}
        </div>}
        <div className="gb-time-footer"><span>{map.source} · USD per 1% move</span>
            <button type="button" onClick={()=>{setShowAll(!showAll);setPick(null);}}>{showAll?'Near spot':'All strikes'}</button></div>
    </div>;
}
