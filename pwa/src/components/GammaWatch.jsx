import React, { useEffect, useRef, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';
import { adaptGammaWatch } from './gammaWatchPublicAdapter.js';

const number = v => Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : 'Unavailable';
const stamp = t => Number.isFinite(t) && Number.isFinite(new Date(t * 1000).getTime()) ? new Date(t * 1000).toISOString() : 'Unavailable';
const percent = v => Number.isFinite(v) ? `${number(v * 100)}%` : 'Unavailable';
const box = { background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 12 };
const snapshot = value => <pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontSize: 12 }}>{JSON.stringify(value, null, 2)}</pre>;

export default function GammaWatch() {
    const [data, setData] = useState(null);
    const [error, setError] = useState('');
    const [now, setNow] = useState(Date.now());
    const highwater = useRef(now);
    const clockValid = Number.isFinite(now) && now >= highwater.current;
    if (clockValid) highwater.current = now;
    useEffect(() => {
        let stopped = false;
        let timer;
        const load = async () => {
            try {
                const result = await api.get('/api/v1/gamma-watch/state');
                if (!stopped) {
                    const receivedAt = Date.now();
                    setNow(receivedAt);
                    setData(result.status === 'available' ? { envelope: result, receivedAt } : null);
                    setError(result.status === 'available' ? '' : 'Collector unavailable');
                }
            } catch {
                if (!stopped) { setData(null); setError('Collector unavailable; no cached substitute.'); }
            } finally { if (!stopped) timer = setTimeout(load, 5000); }
        };
        load();
        const clock = setInterval(() => setNow(Date.now()), 1000);
        return () => { stopped = true; clearTimeout(timer); clearInterval(clock); };
    }, []);
    if (!data) return <section style={box} role="status">{error || 'Connecting to Gamma Watch…'}</section>;
    const browserFresh = clockValid && now >= data.receivedAt && now - data.receivedAt <= 15000;
    const view = adaptGammaWatch(browserFresh ? data.envelope : null, now);
    const { price, delayed, research, sectors, rebalance, live } = view;
    return <div>
        <section style={box}>
            <h2>Gamma Watch · experimental public context</h2>
            <p role="status">{view.valid ? 'GRID connection recent' : 'Display withheld · stale, unavailable or unsupported schema'} · server evaluation {stamp(view.servedAt)} · browser receipt {stamp(data.receivedAt / 1000)}</p>
            <p>SPY {number(price.price)} · {price.status} · {price.source}</p>
            <p>Provider as-of {stamp(price.sourceTime)} · site receipt {stamp(price.receiptTime)} · exchange event time unverified.</p>
            <p>Delayed levels: put wall {number(delayed.levels.put_wall)} · call wall {number(delayed.levels.call_wall)} · flip {number(delayed.levels.gamma_flip)} · max pain {number(delayed.levels.max_pain)}</p>
            <p>ZeroGEX delayed context · {delayed.status} · provider as-of {stamp(delayed.sourceTime)} · site receipt {stamp(delayed.receiptTime)}</p>
            <p>Provider-reported spot {number(delayed.spot)} · receipt minus provider as-of {number(delayed.receiptLag)} seconds · snapshot phase {delayed.phase || 'unknown'}. Calculation basis and event time unverified; phase is provider metadata, exchange calendar unknown.</p>
            <p>Dealer inventory is assumed, not observed. Max pain is not a target. These experimental contexts are not current options, calibrated signals or GRID trading scores.</p>
            <a href="https://gex.stepdad.finance/" target="_blank" rel="noopener noreferrer">Open full Gamma Watch</a>
        </section>
        <section style={box}>
            <h3>Frozen historical research · not current GEX</h3>
            <p>{research.status} · chain capture {stamp(research.chainAsOf)} · model computation {stamp(research.computedAt)}</p>
            <details><summary>Research grid and sensitivity variants · {research.grid.length} points</summary>{snapshot(research)}</details>
        </section>
        <section style={box}>
            <h3>Futures, breadth and cross-market observations</h3>
            <p>{live.status} · computation {stamp(live.computedAt)} · exchange event time unknown.</p>
            <div style={{ overflowX: 'auto' }}><table style={{ width: '100%' }}><thead><tr><th>Symbol</th><th>Received value</th><th>Status</th><th>Callback receipt</th></tr></thead><tbody>
                {live.rows.map((r, i) => <tr key={`${r.symbol}-${i}`}><td>{r.symbol}</td><td>{number(r.value)}</td><td>{r.status}</td><td>{stamp(r.callbackAt)}</td></tr>)}
            </tbody></table></div>
        </section>
        <section style={box}>
            <h3>Rebalancing sensitivity · hypothetical demand</h3>
            <p>{rebalance.status} · computation {stamp(rebalance.computedAt)} · historical proxy only.</p>
            {rebalance.rows.map((r, i) => <div key={`${r.period}-${i}`}><strong>{r.period}: {r.baseline || 'unknown'} → {r.as_of_date || 'unknown'}</strong><p>SPY {percent(r.equity_return)}, AGG {percent(r.bond_return)}</p><p>{r.scenarios.map(s => `${percent(s.equity_weight)} equity: $${number(Number.isFinite(s.equity_purchase_per_initial_billion) ? s.equity_purchase_per_initial_billion / 1e6 : null)}m`).join(' · ')}</p></div>)}
            <p>Per initial $1 billion. Negative = hypothetical selling. Month and quarter are alternatives, not additive.</p>
        </section>
        <section style={box}>
            <h3>Official published history and sector context</h3>
            <p>Source dates, site receipts and computation times remain separate. Published history is not a current liquidity estimate; sector snapshots have unqualified source age and expiry labels.</p>
            <details><summary>Official published history · {view.official.length} sources</summary>{snapshot(view.official)}</details>
            <details><summary>Sector context · {sectors.status} · computed {stamp(sectors.computedAt)}</summary>{snapshot(sectors)}</details>
        </section>
        <section style={box}>
            <h3>Private capabilities</h3>
            <p>{view.privateStatus}</p>
            {view.rtd.price !== null && <p>Legacy RTD receipt-only price {number(view.rtd.price)} · collector receipt {stamp(view.rtd.receiptTime)} · exchange event time unknown.</p>}
        </section>
    </div>;
}
