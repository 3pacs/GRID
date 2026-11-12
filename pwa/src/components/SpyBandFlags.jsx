import React, { useState } from 'react';

const kinds = {
    first_cross: { name: 'First cross', target: 'Upper green strike', description: 'First qualifying close above red → upper green' },
    failed_reclaim: { name: 'Failed reclaim', target: 'Lower green strike', description: 'Reclaim followed by a close below red → lower green' },
};
const buckets = [
    ['opening', '09:30–09:59'], ['morning', '10:00–11:29'], ['midday', '11:30–13:29'],
    ['afternoon', '13:30–15:29'], ['closing', '15:30–close'],
];
const statuses = { pending: 'Pending', hit: 'Hit', miss: 'Miss', censored: 'Censored' };
const etOptions = { timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', second: '2-digit', hourCycle: 'h23' };
const et = new Intl.DateTimeFormat('en-US', etOptions);
const datedEt = new Intl.DateTimeFormat('en-US', { ...etOptions, month: 'short', day: 'numeric', year: 'numeric' });
const number = new Intl.NumberFormat('en-US', { maximumFractionDigits: 3 });
function time(stamp, full = false) {
    const millis = typeof stamp === 'string' ? Date.parse(stamp) : NaN;
    return Number.isFinite(millis) ? (full ? datedEt : et).format(millis) + ' ET' : '—';
}
function price(value) { return Number.isFinite(value) ? number.format(value) : '—'; }
function count(value) { return Number.isInteger(value) && value >= 0 ? String(value) : '—'; }
function rate(row) {
    if (!row || [row.hits, row.misses].some(v => !Number.isSafeInteger(v) || v < 0)) return '—';
    const resolved = row.hits + row.misses;
    if (resolved === 0) return '0 / 0 · Unscored';
    return row.hits + ' / ' + resolved + ' · ' + (100 * row.hits / resolved).toLocaleString('en-US', { maximumFractionDigits: 1 }) + '%';
}
function Family({kind, rows}) {
    const summary = rows.find(r => r.kind === kind && r.bucket === 'all');
    return <div className="gb-flag-family">
        <div className="gb-flag-line"><strong>{kinds[kind].name}</strong><b>{rate(summary)}</b></div>
        <p>{kinds[kind].description}</p><p>Misses {count(summary?.misses)} · Pending {count(summary?.pending)} · Censored {count(summary?.censored)}</p>
    </div>;
}
function ByTime({rows}) {
    const [kind,setKind]=useState('first_cross');
    return <details className="gb-flags-buckets"><summary>Time-of-day results</summary>
        <label>Flag <select value={kind} onChange={e=>setKind(e.target.value)}>
            {Object.entries(kinds).map(([key, family])=><option key={key} value={key}>{family.name}</option>)}</select></label>
        <table aria-label={kinds[kind].name+' time-of-day scores'}>
            <thead><tr><th>ET at confirmation</th><th>Hits / resolved</th><th>Miss</th><th>Pending</th><th>Cens.</th></tr></thead>
            <tbody>{buckets.map(([bucket, range])=>{
                const row=rows.find(r=>r.kind===kind && r.bucket===bucket);
                return <tr key={bucket}><th scope="row">{range}</th><td>{rate(row)}</td><td>{count(row?.misses)}</td><td>{count(row?.pending)}</td><td>{count(row?.censored)}</td></tr>;
            })}</tbody>
        </table>
    </details>;
}
function FlagEvent({event}) {
    const { name, target } = kinds[event.kind];
    return <li className="gb-flag-event">
        <div className="gb-flag-line"><strong>{name}</strong><b className={'gb-flag-'+event.status}>{statuses[event.status] || 'Unknown'}</b></div>
        <p><strong>Red {price(event.red)} → Target {price(event.target)}</strong> · {target}</p>
        <p>Bar close price {price(event.close)} · Green band {price(event.lower)}–{price(event.upper)}</p>
        <dl>{[
            ['Signal bar closed', time(event.signal_time)],
            ['Issued', time(event.issued_at)],
            ['Deadline', time(event.deadline)],
            [event.status==='hit'?'Target-touch bar closed':'Outcome through', time(event.outcome_time)],
        ].map(([label, val])=><div key={label}><dt>{label}</dt><dd>{val}</dd></div>)}</dl>
    </li>;
}
export default function SpyBandFlags({feed}) {
    const s=feed.snapshot;
    const events=Array.isArray(s?.events) ? s.events.filter(e=>e && Object.hasOwn(kinds,e.kind))
        .sort((a,b)=>(Date.parse(b.issued_at)||0)-(Date.parse(a.issued_at)||0)) : [];
    return <section className="gb-flags" aria-label="SPY band flags">
        <h3>SPY band flags</h3>
        {feed.status && <p className="gb-status" role="status">{feed.status}</p>}
        {s && <>
            <p className="gb-flag-clock">{s.as_of ? 'Paired price and GEX inputs through '+time(s.as_of,true) : 'No paired price and GEX minute reported for this session.'}</p>
            <h4>Session flags · {s.session_date}</h4>
            {s.start_date && s.session_date < s.start_date && <p>Prospective recording starts {s.start_date}.</p>}
            {events.length ? <ul aria-label="SPY flags for selected session" className="gb-event-list">
                {events.map(e=><FlagEvent key={e.kind+'/'+e.red+'/'+e.signal_time+'/'+e.issued_at} event={e}/>)}</ul> : <p>No flags recorded for this session.</p>}
            <h4>All sessions · hits / resolved</h4>
            {Object.keys(kinds).map(kind=><Family key={kind} kind={kind} rows={s.stats}/>)}
            <p className="gb-flag-footnote">Grades use the next 30 full minute bars after issuance, up to session close. Hit rate divides hits by hits plus misses. Censored flags lack a complete scoring window.</p>
            <ByTime rows={s.stats}/>
        </>}
    </section>;
}
