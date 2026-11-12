import React, { useEffect, useMemo, useRef, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';
import './GammaBoard.css';
import SpyBandFlags from '../components/SpyBandFlags.jsx';
import GammaSessionMap, {validMap} from '../components/GammaSessionMap.jsx';
import GammaIntradayChart from '../components/GammaIntradayChart.jsx';

const SYMBOLS = { SPX: 'S&P 500 index', SPY: 'S&P 500 ETF', QQQ: 'Nasdaq-100 ETF' };
const EXTRA_KEY = 'grid.gamma-board.extra-symbols.v1';
const VALID_SYMBOL = /^[A-Z]{1,5}$/;
function loadExtras() {
    try {
        const stored = JSON.parse(window.localStorage.getItem(EXTRA_KEY));
        return Array.isArray(stored) ? [...new Set(stored.filter(s => typeof s === 'string' && VALID_SYMBOL.test(s) && !(s in SYMBOLS)))].slice(0, 3) : [];
    } catch { return []; }
}
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

const FEEDS = {
    flags: { load: (_symbol, signal, day) => api.getSpyBandFlags(signal, day),
        valid: next => /^\d{4}-\d{2}-\d{2}$/.test(next.session_date)
            && Array.isArray(next.events) && Array.isArray(next.stats) && next.stats.every(row => row && typeof row === 'object') },
    history: {load: (...args) => api.getGammaSessionMap(...args), valid: validMap},
    chain: { load: (...args) => api.getGammaBoardInstrument(...args), valid: next => Array.isArray(next.cells) && next.cells.length && Number.isFinite(next.spot) },
    intraday: { load: (...args) => api.getGammaBoardIntraday(...args), valid: next => Array.isArray(next.bars) && next.bars.length
        && Number.isFinite(Date.parse(next.session_start)) && Date.parse(next.session_end) > Date.parse(next.session_start) },
};
function observationTime(observation, kind) {
    return kind === 'history' ? observation.times.at(-1) : Date.parse(observation.as_of);
}
function useFeed(symbol, refresh, kind, sessionDate = '') {
    const [state, setState] = useState({ snapshot: null, status: `Loading ${kind}…`, loading: true });
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
                const readAt = performance.now();
                const next = await FEEDS[kind].load(symbol, controller.signal, sessionDate);
                if (!active) return;
                if (typeof next?.session_open === 'boolean') interval = next.session_open ? OPEN_MS : CLOSED_MS;
                if (next?.status === 401 || next?.status === 403) {
                    setState({ snapshot: null, loading: false, status: 'Administrator access required.' });
                    return;
                }
                const matchesSymbol = next?.symbol === symbol || (kind === 'flags' && next?.symbol == null);
                if (matchesSymbol && next?.status === 'unavailable') {
                    setState({ snapshot: null, loading: false, tracking:next.tracking, readAt, sessionDate:next.session_date, status: next.reason || `${kind} unavailable.` });
                    return;
                }
                const valid = matchesSymbol && next?.status === 'available'
                    && ((kind === 'history' || kind === 'flags') ? (!sessionDate || next.session_date === sessionDate) : Number.isFinite(Date.parse(next.as_of)))
                    && FEEDS[kind].valid(next);
                if (controller.signal.aborted || !valid) {
                    throw new Error(`Latest ${kind} unavailable.`);
                }
                setState(previous => {
                    if (previous.snapshot && next.source === previous.snapshot.source && observationTime(next,kind) < observationTime(previous.snapshot,kind)) {
                        return { ...previous, loading: false, status: 'Provider returned an older observation. Showing last snapshot.' };
                    }
                    return { snapshot: next, tracking:next.tracking, readAt, status: '', loading: false };
                });
            } catch (err) {
                if (active) setState(last => ({ ...last, loading: false,
                    status: (err?.message || `Latest ${kind} unavailable.`) + (last.snapshot ? ' Showing last snapshot.' : '') }));
            } finally {
                clearTimeout(deadline);
                busy = false;
                if (active && !document.hidden && !sessionDate) timer = setTimeout(poll, interval);
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
    }, [symbol, kind, sessionDate]);
    useEffect(() => { if (refresh) manual.current?.(); }, [refresh]);
    return state;
}

function shade(value, cap) {
    const intensity = Math.sqrt(Math.min(1, Math.abs(value) / cap));
    const endpoint = value < 0 ? [248, 110, 116] : [81, 210, 130];
    return {
        backgroundColor: 'rgb(' + [37, 49, 54].map((n, i) => Math.round(n + intensity * (endpoint[i] - n))).join(',') + ')',
        color: intensity > .48 ? '#111c20' : '#eef4f1',
    };
}

function signedColor(value) {
    if (!Number.isFinite(value) || value === 0) return '';
    return value < 0 ? 'gb-negative' : 'gb-positive';
}
function signedUsd(value) {
    return (value > 0 ? '+' : '') + usd(value);
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
        <div className="gb-legend"><b className="gb-negative">− Negative</b> <i /> <b className="gb-positive">+ Positive</b> <span>· scale ±{usd(cap)}</span></div>
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
            <span>Net <b className={signedColor(selected.net_gex)}>{signedUsd(selected.net_gex)}</b> · Calls <b className={signedColor(selected.call_gex)}>{signedUsd(selected.call_gex)}</b> · Puts <b className={signedColor(selected.put_gex)}>{signedUsd(selected.put_gex)}</b></span>
            <span>Open interest · calls {number(selected.call_oi)} · puts {number(selected.put_oi)}</span>
        </div>}
    </>;
}

function SessionHistory({symbol, refresh, sessionDate, extra, ledger}) {
    const [changed,setChanged]=useState(null);
    const [saving,setSaving]=useState(false);
    const [error,setError]=useState('');
    const h=useFeed(symbol, refresh, 'history', sessionDate);
    const tracked=changed && changed.at > (h.readAt ?? 0) ? changed.enabled : h.tracking;
    async function toggle() {
        if (saving || typeof tracked !== 'boolean') return;
        setSaving(true); setError('');
        try {
            const next=await api.setGammaSessionTracking(symbol,!tracked);
            if(next?.symbol===symbol && typeof next.tracking==='boolean') setChanged({enabled:next.tracking,at:performance.now()});
            else setError(next?.status===409 ? 'Three extra tickers are already being recorded.' : 'Recording change could not be confirmed.');
        } catch {setError('Recording change could not be confirmed.');}
        finally {setSaving(false);}
    }
    return <section className="gb-history" aria-label={symbol + ' recorded GEX history'}>
        <h3>GEX heatmap <small>strike × session time</small></h3>
        {h.status && <div className={h.snapshot ? 'gb-status' : 'gb-history-empty'} role="status">
            {!h.snapshot && <strong>{h.loading ? 'Loading session history' : 'Session heatmap unavailable'}</strong>}
            <span>{h.status}{h.sessionDate && !h.snapshot ? ' ('+h.sessionDate+')' : ''}</span>
            {!extra && !sessionDate && h.tracking === true && h.status === 'No strike history recorded for this session.' &&
                <span>Recording is enabled. New minutes will appear during regular trading hours when fresh quotes arrive.</span>}</div>}
        {h.snapshot && <GammaSessionMap map={h.snapshot} ledger={ledger} />}
        {extra && <div className="gb-record"><button type="button" disabled={saving || typeof tracked !== 'boolean'} onClick={toggle}>
            {saving?'Saving…':tracked?'Stop recording':'Record future minutes'}</button>
            <p>{tracked ? 'Server recording is enabled for future regular-session minutes, including while this page is closed.' : 'Enable server recording to collect future regular-session minutes while this page is closed.'}</p>
            {error && <p role="alert">{error}</p>}
        </div>}
    </section>;
}

function SpySession({refresh,sessionDate}) {
    const feed = useFeed('SPY', refresh, 'flags', sessionDate);
    return <>
        <SessionHistory symbol="SPY" refresh={refresh} sessionDate={sessionDate} ledger={feed.snapshot} />
        <SpyBandFlags feed={feed} />
    </>;
}

function Instrument({ symbol, metric, count, refresh, sessionDate, onRemove }) {
    const { snapshot: s, status, loading } = useFeed(symbol, refresh, 'chain');
    const intraday = useFeed(symbol, refresh, 'intraday');
    return <article className="gb-card" aria-label={symbol + ' gamma exposure'}>
        <header><div><h2>{symbol}</h2><span>{SYMBOLS[symbol] || 'Equity / ETF'}</span>{onRemove && <button className="gb-remove" type="button" aria-label={`Remove ${symbol}`} onClick={onRemove}>Remove</button>}</div>
            <div className="gb-spot"><b>{number(s?.spot)}</b><span>Latest chain spot</span></div></header>
        {symbol === 'SPY'
            ? <SpySession key={sessionDate} refresh={refresh} sessionDate={sessionDate} />
            : <SessionHistory key={sessionDate} symbol={symbol} refresh={refresh} sessionDate={sessionDate} extra={!!onRemove} />}
        <details className="gb-secondary"><summary>Latest expiry × strike snapshot</summary>
        <dl className="gb-walls">{[['Call wall', number(s?.call_wall)], ['Put wall', number(s?.put_wall)], ['Net GEX', signedUsd(s?.net_gex)]]
            .map(([label, val]) => <div key={label}><dt>{label}</dt><dd className={label === 'Net GEX' ? signedColor(s?.net_gex) : ''}>{val}</dd></div>)}</dl>
        {s && <div className="gb-clock"><b>{!s.session_open ? 'Last available snapshot' : s.stale ? 'Stale chain' : 'Chain snapshot'}</b> · {clock(s.as_of)}<br />
            {s.session_open ? 'Regular session open' : 'Regular session closed'} · {s.expiries?.length ? s.expiries.length + ' expiries' : 'Expiry coverage unavailable'}</div>}
        <h3 className="gb-surface-title">Gamma heatmap</h3>
        {status && <p className="gb-status" role="status">{status}</p>}
        {s && <><Surface symbol={symbol} snapshot={s} metric={metric} count={count} />
            <details><summary>Source and quote times</summary><p>{s.source_label || 'Chain source unavailable'} · Feed latency {s.feed_latency || 'unknown'}</p><p>{s.method}</p><p>{s.scope}</p>
                <p>Spot as of · {clock(s.spot_as_of)}<br />Last underlying trade · {clock(s.last_trade_at)}
                    {s.underlying_quote_at && <><br />Underlying quote · {clock(s.underlying_quote_at)}</>}</p>
                <p>{s.coverage || (number(s.contracts) + ' covered contracts')} · {number(s.expiries?.length)} expiry dates.</p>
                {s.source_note && <p>{s.source_note}</p>}
            </details></>}
        {!s && !loading && <div className="gb-empty">Waiting for a usable {symbol} chain.</div>}
        </details>
        <details className="gb-intraday gb-secondary" aria-label={symbol + ' intraday price'}>
            <summary>Latest intraday price</summary>
            {intraday.status && <p className="gb-status" role="status">{intraday.status}</p>}
            {intraday.snapshot && <GammaIntradayChart snapshot={intraday.snapshot} chain={s} />}
        </details>
    </article>;
}

export default function GammaBoard() {
    const [metric, setMetric] = useState('net_gex');
    const [count, setCount] = useState('9');
    const [refresh, setRefresh] = useState(0);
    const [extra, setExtra] = useState(loadExtras);
    useEffect(() => {
        try { window.localStorage.setItem(EXTRA_KEY, JSON.stringify(extra)); } catch { /* In-memory controls still work. */ }
    }, [extra]);
    const [sessionDate, setSessionDate] = useState('');
    const [entry, setEntry] = useState('');
    const [message, setMessage] = useState('');
    const symbols = [...Object.keys(SYMBOLS), ...extra];
    function add(event) {
        event.preventDefault();
        const symbol = entry.trim().toUpperCase();
        if (!VALID_SYMBOL.test(symbol)) return setMessage('Use a ticker with one to five letters.');
        if (symbols.includes(symbol)) return setMessage(symbol + ' is already on the board.');
        if (symbols.length >= 6) return setMessage('Remove an added ticker to make room.');
        setExtra([...extra, symbol]);
        setEntry('');
        setMessage('');
    }
    return <main className="gamma-board" style={{ '--gb-bg': colors.bg, '--gb-card': colors.card,
        '--gb-border': colors.border, '--gb-fg': colors.text, '--gb-muted': colors.textDim }}>
        <header className="gb-header"><div><h1>Gamma exposure</h1><p>Recorded net gamma exposure across strikes and trading time</p></div>
            <button type="button" onClick={() => setRefresh(v => v + 1)}>Refresh all</button></header>
        <div className="gb-controls"><span className="gb-cadence">{sessionDate ? 'Archived heatmap · latest chain below' : 'Latest · auto-refresh during the session'}</span>
            <label>Recorded session <input type="date" value={sessionDate} onChange={e=>setSessionDate(e.target.value)} /></label>
            {sessionDate && <button type="button" onClick={()=>setSessionDate('')}>Latest session</button>}
        </div>
        <details className="gb-filter-disclosure"><summary>Latest expiry surface controls</summary><div className="gb-controls"><label>Exposure <select value={metric} onChange={e => setMetric(e.target.value)}>
            {Object.entries(exposures).map(([k, v]) => <option key={k} value={k}>{v}</option>)}</select></label>
            <label>Strike window <select value={count} onChange={e => setCount(e.target.value)}>
                <option value="9">9 near spot</option><option value="17">17 near spot</option><option value="all">All covered strikes</option>
            </select></label></div></details>
        <form onSubmit={add} className="gb-add"><label>Add ticker <input aria-label="Ticker to add" autoCapitalize="characters" autoComplete="off"
            value={entry} onChange={e => { setEntry(e.target.value); setMessage(''); }} placeholder="e.g. AAPL" /></label><button type="submit">Add</button>
            <span aria-live="polite">{message || symbols.length + ' of 6 instruments'}</span></form>
        <div className="gb-boards">{symbols.map(symbol =>
            <Instrument key={symbol} symbol={symbol} metric={metric} count={count} refresh={refresh} sessionDate={sessionDate}
                onRemove={extra.includes(symbol) ? () => { setExtra(extra.filter(s => s !== symbol)); setMessage(''); } : undefined} />)}</div>
        <footer><p>Color saturates at the 95th percentile of visible nonzero exposure per panel. Blank minutes have no recording. In the expiry snapshot, ◇ marks the listed strike nearest spot.</p>
            <p>Walls mark the strike with the largest call or absolute put exposure across covered expiries. The model treats calls as positive and puts as negative. Open Source and quote times for the method and coverage of each chain.</p>
        </footer>
    </main>;
}
