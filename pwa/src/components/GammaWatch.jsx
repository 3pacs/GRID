import React, { useEffect, useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const number = v => Number.isFinite(v) ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : 'Unavailable';
const recent = (t, now, seconds) => Number.isFinite(Date.parse(t)) && now - Date.parse(t) >= -5000 && now - Date.parse(t) <= seconds * 1000;
const box = { background: colors.card, border: `1px solid ${colors.border}`, borderRadius: 10, padding: 16, marginBottom: 12 };

export default function GammaWatch() {
    const [data, setData] = useState(null);
    const [error, setError] = useState('');
    const [now, setNow] = useState(Date.now());
    useEffect(() => {
        let stopped = false;
        let timer;
        const load = async () => {
            try {
                const result = await api.get('/api/v1/gamma-watch/state');
                if (!stopped) {
                    setData(result.status === 'available' ? result.data : null);
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
    const q = data.quote || {};
    const g = data.gex || {};
    const live = data.structural_live || {};
    const rebalance = data.structural_rebalance || {};
    const transportFresh = recent(data.served_at, now, 20);
    const priceFresh = transportFresh && !data.quote_error && recent(q.as_of, now, q.is_rtd ? 20 : 120);
    return <div>
        <section style={box}>
            <h2>Gamma Watch · shared collector</h2>
            <p>{transportFresh ? 'GRID connection recent' : 'GRID connection stale'} · source served {data.served_at}</p>
            <p>SPY {number(q.price)} · {priceFresh ? 'recent receipt' : 'stale / unavailable'} · {q.source || 'source unavailable'} · {q.as_of || 'time unavailable'}</p>
            <p>Delayed levels: put wall {number(g.put_wall)} · call wall {number(g.call_wall)} · flip {number(g.gamma_flip)} · max pain {number(g.max_pain)}</p>
            <p>{g.source} · {g.as_of || 'timestamp unavailable'} · {data.gex_error || (recent(g.as_of, now, 1800) ? 'delayed context' : 'stale context')}</p>
            <p>Dealer inventory is assumed, not observed. Max pain is not a target. These inputs do not update GRID trading scores.</p>
            <a href="https://gex.stepdad.finance/" target="_blank" rel="noopener noreferrer">Open full Gamma Watch</a>
        </section>
        <section style={box}>
            <h3>Futures, breadth and cross-market observations</h3>
            <p>{live.note || data.structural_live_error || 'Unavailable'}</p>
            <div style={{ overflowX: 'auto' }}><table style={{ width: '100%' }}><thead><tr><th>Symbol</th><th>Received value</th><th>Status</th><th>Callback receipt</th></tr></thead><tbody>
                {(live.rows || []).map(r => <tr key={r.symbol}><td>{r.symbol}</td><td>{number(r.value)}</td><td>{transportFresh && !data.structural_live_error && recent(live.computed_at, now, 20) && recent(r.callback_at, now, 20) && r.direction_usable ? 'Fresh receipt; exchange time unknown' : (r.status === 'fresh_receipt' ? 'stale' : r.status || 'unavailable').replaceAll('_', ' ')}</td><td>{r.callback_at || 'Unavailable'}</td></tr>)}
            </tbody></table></div>
        </section>
        <section style={box}>
            <h3>Rebalancing sensitivity · hypothetical demand</h3>
            <p>{rebalance.status || 'Unavailable'} · captured {rebalance.computed_at || 'unknown'} · {data.structural_rebalance_error}</p>
            {(rebalance.rows || []).map(r => <div key={r.period}><strong>{r.period}: {r.baseline} → {r.as_of_date}</strong><p>SPY {number(r.equity_return * 100)}%, AGG {number(r.bond_return * 100)}%</p><p>{r.scenarios.map(s => `${s.equity_weight * 100}% equity: $${number(s.equity_purchase_per_initial_billion / 1e6)}m`).join(' · ')}</p></div>)}
            <p>Per initial $1 billion. Negative = hypothetical selling. Month and quarter are alternatives, not additive. {rebalance.assumptions}</p>
        </section>
        <section style={box}>
            <h3>Complete source snapshots</h3>
            <p>Source dates and receipt times are preserved. Missing inputs remain unavailable. Expand to inspect the original data and assumptions.</p>
            {['structural_official', 'sectors', 'model', 'broker_model', 'pressure', 'broker'].map(key => <details key={key}><summary>{key.replaceAll('_', ' ')} · {data[key + '_error'] ? 'refresh failed' : data[key] ? 'snapshot available; inspect source age' : 'unavailable'}</summary><pre style={{ whiteSpace: 'pre-wrap', overflowWrap: 'anywhere', fontSize: 12 }}>{JSON.stringify(data[key] ?? { status: 'unavailable' }, null, 2)}</pre></details>)}
            <p>The authenticated GRID API also exposes contract listings, the event journal and paginated recorded artifacts for research. No historical coverage is inferred before recording began.</p>
        </section>
    </div>;
}
