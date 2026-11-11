import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';
import './GammaBoard.css';

const SYMBOLS = { SPX: 'S&P 500 index', SPY: 'S&P 500 ETF', QQQ: 'Nasdaq-100 ETF' };
const OPEN_MS = 60_000;
const CLOSED_MS = 300_000;
const DEADLINE_MS = 30_000;
const exposures = { net_gex: 'Net gamma', call_gex: 'Call gamma', put_gex: 'Put gamma' };
const localClock = new Intl.DateTimeFormat('en-US', {
    month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit', timeZone: 'America/Los_Angeles', timeZoneName: 'short',
});
const dollars = new Intl.NumberFormat('en-US', { notation: 'compact', maximumFractionDigits: 2 });
function clock(value) {
    return value && Number.isFinite(Date.parse(value)) ? localClock.format(new Date(value)) : 'Unavailable';
}
function number(value) {
    return Number.isFinite(value) ? value.toLocaleString('en-US', { maximumFractionDigits: 2 }) : '—';
}
function usd(value) {
    return Number.isFinite(value) ? (value < 0 ? '−$' : '$') + dollars.format(Math.abs(value)) : '—';
}

// Each chain schedules its next request only after its current one completes.
function useChain(symbol, refresh) {
    const [state, setState] = useState({ snapshot: null, status: 'Loading chain…', loading: true });
    const manual = useRef(null);
    useEffect(() => {
        let active = true, busy = false, timer, controller, deadline;
        let interval = OPEN_MS;
        async function poll() {
            if (!active || document.hidden || busy) return;
            clearTimeout(timer);
            busy = true;
            controller = new AbortController();
            deadline = setTimeout(() => controller.abort(), DEADLINE_MS);
            setState(last => ({ ...last, loading: true }));
            try {
                const next = await api.getGammaBoardInstrument(symbol, controller.signal);
                if (!active) return;
                if (typeof next?.session_open === 'boolean') interval = next.session_open ? OPEN_MS : CLOSED_MS;
                if (next?.status === 401 || next?.status === 403) {
                    setState({ snapshot: null, loading: false, status: 'Administrator access required.' });
                    return;
                }
                if (next?.symbol === symbol && next.status === 'unavailable') {
                    setState({ snapshot: null, loading: false, status: next.reason || 'Chain unavailable.' });
                    return;
                }
                const valid = next?.symbol === symbol && next.status === 'available' && Number.isFinite(Date.parse(next.as_of))
                    && Array.isArray(next.cells) && next.cells.length && Number.isFinite(next.spot);
                if (controller.signal.aborted || !valid) {
                    throw new Error('Latest chain unavailable.');
                }
                setState(previous => {
                    if (previous.snapshot && next.source === previous.snapshot.source && Date.parse(next.as_of) < Date.parse(previous.snapshot.as_of)) {
                        return { ...previous, loading: false, status: 'Provider returned an older chain. Showing last snapshot.' };
                    }
                    return { snapshot: next, status: '', loading: false };
                });
            } catch (err) {
                if (active) setState(last => ({ ...last, loading: false,
                    status: (err?.message || 'Latest chain unavailable.') + (last.snapshot ? ' Showing last snapshot.' : '') }));
            } finally {
                clearTimeout(deadline);
                busy = false;
                if (active && !document.hidden) timer = setTimeout(poll, interval);
            }
        }
        function visibility() {
            clearTimeout(timer);
            if (!document.hidden) poll();
        }
        manual.current = poll;
        document.addEventListener('visibilitychange', visibility);
        poll();
        return () => {
            active = false;
            clearTimeout(timer);
            clearTimeout(deadline);
            controller?.abort();
            document.removeEventListener('visibilitychange', visibility);
            manual.current = null;
        };
    }, [symbol]);
    useEffect(() => { if (refresh) manual.current?.(); }, [refresh]);
    return state;
}

function shade(value, cap) {
    const intensity = Math.sqrt(Math.min(1, Math.abs(value) / cap));
    const endpoint = value < 0 ? [240, 160, 105] : [104, 214, 190];
    return {
        backgroundColor: 'rgb(' + [37, 49, 54].map((n, i) => Math.round(n + intensity * (endpoint[i] - n))).join(',') + ')',
        color: intensity > .48 ? '#111c20' : '#eef4f1',
    };
}

function Surface({ symbol, snapshot, metric, count }) {
    const [picked, setPicked] = useState(null);
    const { strikes, expiries, nearest, index, cap } = useMemo(() => {
        const cells = snapshot.cells;
        const all = [...new Set(cells.map(c => c.strike))].sort((a, b) => a - b);
        const nearest = all.reduce((a, b) => Math.abs(a - snapshot.spot) <= Math.abs(b - snapshot.spot) ? a : b);
        const n = count === 'all' ? all.length : Math.min(all.length, Number(count));
        const start = Math.max(0, Math.min(all.length - n, all.indexOf(nearest) - Math.floor(n / 2)));
        const strikes = all.slice(start, start + n);
        const visible = new Set(strikes);
        const magnitudes = cells.filter(c => visible.has(c.strike) && Number.isFinite(c[metric]) && c[metric] !== 0)
            .map(c => Math.abs(c[metric])).sort((a, b) => a - b);
        return { strikes, nearest, expiries: [...new Set(cells.map(c => c.expiry))].sort(),
            index: new Map(cells.map(c => [c.expiry + '/' + c.strike, c])),
            cap: magnitudes[Math.ceil(magnitudes.length * .95) - 1] || 0 };
    }, [snapshot, metric, count]);
    let selected = index.get(picked);
    if (!selected || !strikes.includes(selected.strike)) {
        selected = index.get(expiries[0] + '/' + nearest)
            || snapshot.cells.find(c => strikes.includes(c.strike));
    }
    return <>
        <div className="gb-legend">− Puts <i /> + Calls <span>· scale ±{usd(cap)}</span></div>
        <div className="gb-scroll">
            <table aria-label={symbol + ' exposure by expiry and strike'} className="gb-map">
                <thead><tr><th scope="col">Expiry</th>{strikes.map(k => <th key={k} scope="col">
                    {number(k)}{k === nearest && <small aria-label="nearest to spot">◇</small>}
                </th>)}</tr></thead>
                <tbody>{expiries.map(e => <tr key={e}><th scope="row">{e.slice(5).replace('-', '/')}</th>
                    {strikes.map(k => {
                        const key = e + '/' + k, cell = index.get(key);
                        if (!Number.isFinite(cell?.[metric])) return <td key={k} className="gb-absent" aria-label="No covered contract" />;
                        const v = cell[metric];
                        return <td key={k}><button type="button" style={shade(v, cap || 1)}
                            aria-pressed={cell === selected} aria-label={`${symbol} ${e} strike ${k}, ${exposures[metric]} ${usd(v)}`}
                            onClick={() => setPicked(key)}>{v > 0 ? '+' : v < 0 ? '−' : '0'}</button></td>;
                    })}
                </tr>)}</tbody>
            </table>
        </div>
        {selected && <div className="gb-pick" aria-live="polite">
            <strong>{number(selected.strike)} strike · {selected.expiry}</strong>
            <span>Net {usd(selected.net_gex)} · Calls {usd(selected.call_gex)} · Puts {usd(selected.put_gex)}</span>
            <span>Open interest · calls {number(selected.call_oi)} · puts {number(selected.put_oi)}</span>
        </div>}
    </>;
}

function Instrument({ symbol, metric, count, refresh }) {
    const { snapshot: s, status, loading } = useChain(symbol, refresh);
    return <article className="gb-card" aria-label={symbol + ' gamma exposure'}>
        <header><div><h2>{symbol}</h2><span>{SYMBOLS[symbol]}</span></div>
            <div className="gb-spot"><b>{number(s?.spot)}</b><span>Underlying spot</span></div></header>
        <dl className="gb-walls">{[['Call wall', number(s?.call_wall)], ['Put wall', number(s?.put_wall)], ['Net GEX', usd(s?.net_gex)]]
            .map(([label, val]) => <div key={label}><dt>{label}</dt><dd>{val}</dd></div>)}</dl>
        {s && <div className="gb-clock"><b>{!s.session_open ? 'Last available snapshot' : s.stale ? 'Stale chain' : 'Chain snapshot'}</b> · {clock(s.as_of)}<br />
            {s.session_open ? 'Regular session open' : 'Regular session closed'} · {s.expiries?.length ? s.expiries.length + ' expiries' : 'Expiry coverage unavailable'}</div>}
        {status && <p className="gb-status" role="status">{status}</p>}
        {s && <><Surface symbol={symbol} snapshot={s} metric={metric} count={count} />
            <details><summary>Source and quote times</summary><p>{s.source_label || 'Chain source unavailable'} · Feed latency {s.feed_latency || 'unknown'}</p><p>{s.method}</p><p>{s.scope}</p>
                <p>Spot as of · {clock(s.spot_as_of)}<br />Last underlying trade · {clock(s.last_trade_at)}
                    {s.underlying_quote_at && <><br />Underlying quote · {clock(s.underlying_quote_at)}</>}</p>
                <p>{s.coverage || (number(s.contracts) + ' covered contracts')} · {number(s.expiries?.length)} expiry dates.</p>
                {s.source_note && <p>{s.source_note}</p>}
            </details></>}
        {!s && !loading && <div className="gb-empty">Waiting for a usable {symbol} chain.</div>}
    </article>;
}

export default function GammaBoard() {
    const [metric, setMetric] = useState('net_gex');
    const [count, setCount] = useState('9');
    const [refresh, setRefresh] = useState(0);
    return <main className="gamma-board" style={{ '--gb-bg': colors.bg, '--gb-card': colors.card,
        '--gb-border': colors.border, '--gb-fg': colors.text, '--gb-muted': colors.textDim }}>
        <header className="gb-header"><div><h1>Gamma exposure</h1><p>SPX, SPY and QQQ · Modeled dollar exposure per 1% underlying move</p></div>
            <button type="button" onClick={() => setRefresh(v => v + 1)}>Refresh all</button></header>
        <div className="gb-controls"><span className="gb-cadence">Auto-refresh · 1 min during session · 5 min after close</span><label>Exposure <select value={metric} onChange={e => setMetric(e.target.value)}>
            {Object.entries(exposures).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>
            <label>Strike window <select value={count} onChange={e => setCount(e.target.value)}>
                <option value="9">9 near spot</option><option value="17">17 near spot</option><option value="all">All covered strikes</option>
            </select></label></div>
        <div className="gb-boards">{Object.keys(SYMBOLS).map(symbol =>
            <Instrument key={symbol} symbol={symbol} metric={metric} count={count} refresh={refresh} />)}</div>
        <footer><p>◇ marks the listed strike nearest spot. Each panel’s color scale saturates at the 95th percentile of its visible nonzero exposures. Striped cells have no covered contract.</p>
            <p>Walls mark the strike with the largest call or absolute put exposure across covered expiries. The model treats calls as positive and puts as negative. Open Source and quote times for the method and coverage of each chain.</p>
        </footer>
    </main>;
}
