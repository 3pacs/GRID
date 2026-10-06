// Local-only fixture entrypoint. All API calls terminate here; it imports no
// app shell, auth bootstrap, warming, writer or provider runner.
import React, { useState } from 'react';
import { createRoot } from 'react-dom/client';
import GodView from '../views/GodView.jsx';
import Snapshots from '../views/Snapshots.jsx';
import Discovery from '../views/Discovery.jsx';
import Associations from '../views/Associations.jsx';
import Options from '../views/Options.jsx';
import { api } from '../api.js';

const views = { 'God View': GodView, Snapshots, Discovery, Associations, Options };
let mode = 'success';
let requests = 0;
let deniedMutations = 0;
const fixture = path => {
    if (path.includes('/godview/')) return { as_of: '2026-09-23T00:00:00Z', pillars: {
        fed_liquidity: { status: 'stale', reason: 'stale', as_of: '2026-09-23', provenance: 'measured', data: { obs_date: '2026-09-23', net_liquidity_usd_m: 0, delta_1w_m: 0, delta_4w_m: null } },
        cftc: { status: 'unavailable', reason: 'never_run' },
        dealer_gex: { status: 'unavailable', reason: 'no_verified_spot', ticker: 'SPY' },
    } };
    if (path.includes('/snapshots/research/')) return { status: 'no_runs' };
    if (path.includes('/snapshots/')) return [{ id: path, created_at: '2026-09-23T00:00:00Z', payload: { synthetic_zero: 0 } }];
    if (path.includes('/discovery/results/')) return { result: null };
    if (path.includes('/discovery/jobs')) return { jobs: [] };
    if (path.includes('/hypotheses/results')) return { count: 0 };
    if (path.includes('/hypotheses')) return { hypotheses: [] };
    if (path.includes('/options/recommendations')) return { recommendations: [], history: [], scan_summary: { data_status: 'stale' }, generated_at: '2026-09-23' };
    if (path.includes('/options/signals')) return { signals: [] };
    if (path.includes('/options/')) return { opportunities: [] };
    if (path.includes('/associations/anomalies')) return { anomalies: [] };
    if (path.includes('/derivatives/gex/')) return { ticker: path.split('/').pop(), spot: 321, per_strike: [], profile: [], gex_aggregate: 0 };
    if (path.includes('/derivatives/vanna-charm/')) return { ticker: path.split('/').pop(), vanna_exposure: 0, charm_exposure: 0 };
    if (path.includes('/derivatives/flow-timeline/')) return { history: [
        { date: '2026-09-22', net_gex: 0, spot: 321, regime: 'neutral' },
        { date: '2026-09-23', net_gex: 0, spot: 321, regime: 'neutral' },
    ], history_status: 'available' };
    return {};
};

window.fetch = async (url, options = {}) => {
    requests += 1;
    if (String(options.method || 'GET').toUpperCase() !== 'GET') {
        deniedMutations += 1;
        return { ok: false, status: 403 };
    }
    if (mode === 'timeout') return new Promise(() => {}); // deliberately ignores abort to test racing
    if (mode === 'offline') throw new Error('Synthetic offline failure');
    if (mode === '524') return { ok: false, status: 524, text: () => Promise.resolve('<html>Cloudflare synthetic private text</html>') };
    if (mode === 'bad JSON') return { ok: true, status: 200, json: () => Promise.reject(new Error('Synthetic parse failure')) };
    return { ok: true, status: 200, json: () => Promise.resolve(fixture(String(url))) };
};

function Preview() {
    const [view, setView] = useState('God View');
    const [generation, setGeneration] = useState(0);
    const [selectedMode, setSelectedMode] = useState(mode);
    const View = views[view];
    return <>
        <div style={{ padding: 16, borderBottom: '1px solid #555', fontFamily: 'sans-serif' }}>
            <strong>OFFLINE SYNTHETIC FIXTURES — no production data or provider calls</strong>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: 8, marginTop: 12 }}>
                <label>Screen <select aria-label="Screen" value={view} onChange={event => { api.clearCache(); setView(event.target.value); }}>{Object.keys(views).map(name => <option key={name}>{name}</option>)}</select></label>
                <label>Response <select aria-label="Response" value={selectedMode} onChange={event => { mode = event.target.value; setSelectedMode(mode); api.clearCache(); }}>{['success', 'timeout', '524', 'offline', 'bad JSON'].map(name => <option key={name}>{name}</option>)}</select></label>
                <button onClick={() => { api.clearCache(); setGeneration(value => value + 1); }}>Remount selected screen</button>
                <span>API fixtures requested: {requests}; mutations denied: {deniedMutations}</span>
            </div>
        </div>
        <View key={`${view}-${generation}`} />
    </>;
}
createRoot(document.getElementById('root')).render(<Preview />);
