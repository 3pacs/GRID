/**
 * TPS.jsx — Trump-Proximity Score watchlist (Phase 0 of GRID-4-product pivot).
 *
 * Default landing view per docs/planning/GRID-4-PRODUCT-PIVOT.md section 5.
 *
 *   Top half: ranked table — ticker, score (0-100), per-layer dots, evidence count.
 *   Bottom half: drill-down card for the selected row — full evidence chain,
 *                per-layer score breakdown, mini actor-network graph (reuses
 *                ActorNetwork.jsx via lazy import to avoid bloating the
 *                default-landing bundle).
 *
 * No fake metrics: rows with score == null render a "low coverage" badge and
 * are filtered out of the ranking by default (toggle to show).
 *
 * TODO(feature flag): the pivot doc says ship behind a flag for 5d before
 * making this the literal default route. There is no flag infrastructure
 * yet on the PWA (no LaunchDarkly / no localStorage convention) — this
 * TODO is the inline-comment fallback called for in the build brief.
 * The route is registered in routes.js and surfaces in the tab bar; the
 * "default landing" promotion will land in a follow-up PR once we have
 * 24h of snapshot data to display.
 */

import React, { lazy, Suspense, useCallback, useEffect, useMemo, useState } from 'react';
import { Crosshair, RefreshCw, ShieldAlert, AlertTriangle, Activity } from 'lucide-react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const ActorNetwork = lazy(() => import('./ActorNetwork.jsx'));

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const LAYER_LABELS = {
    direct_contracts: 'Contracts',
    lobbying_admin: 'Lobbying',
    congressional_30d: 'Congress',
    fara_admin: 'FARA',
    actor_hops: 'Network',
};

const LAYER_ORDER = [
    'direct_contracts',
    'lobbying_admin',
    'congressional_30d',
    'fara_admin',
    'actor_hops',
];

function scoreColor(score) {
    if (score == null) return colors.textDim;
    if (score >= 75) return '#f87171';
    if (score >= 50) return '#fbbf24';
    if (score >= 25) return '#60a5fa';
    return colors.textDim;
}

function fmtUSD(v) {
    if (v == null) return '—';
    if (v >= 1e9) return `$${(v / 1e9).toFixed(2)}B`;
    if (v >= 1e6) return `$${(v / 1e6).toFixed(2)}M`;
    if (v >= 1e3) return `$${(v / 1e3).toFixed(1)}k`;
    return `$${Number(v).toFixed(0)}`;
}

function coverageCount(coverage) {
    if (!coverage || typeof coverage !== 'object') return 0;
    return Object.values(coverage).filter(Boolean).length;
}

export default function TPS() {
    const [entries, setEntries] = useState([]);
    const [meta, setMeta] = useState({ total: 0, as_of: '' });
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [selectedTicker, setSelectedTicker] = useState('');
    const [drill, setDrill] = useState(null);
    const [drillLoading, setDrillLoading] = useState(false);

    const load = useCallback(async () => {
        setLoading(true);
        setError('');
        const data = await api.get('/api/v1/tps/today?limit=25');
        if (data?.error) {
            setError(data.message || 'TPS feed unavailable.');
            setEntries([]);
        } else {
            const rows = data?.entries || [];
            setEntries(rows);
            setMeta({ total: data?.total || rows.length, as_of: data?.as_of || '' });
            if (rows.length && !selectedTicker) setSelectedTicker(rows[0].ticker);
        }
        setLoading(false);
    }, [selectedTicker]);

    useEffect(() => { load(); }, []); // eslint-disable-line react-hooks/exhaustive-deps

    useEffect(() => {
        if (!selectedTicker) { setDrill(null); return; }
        let cancelled = false;
        setDrillLoading(true);
        api.get(`/api/v1/tps/${selectedTicker}?live=true`).then(d => {
            if (cancelled) return;
            setDrill(d?.error ? null : d);
            setDrillLoading(false);
        });
        return () => { cancelled = true; };
    }, [selectedTicker]);

    const summary = useMemo(() => {
        const scored = entries.filter(e => e.score != null);
        const avg = scored.length ? scored.reduce((s, e) => s + (e.score || 0), 0) / scored.length : null;
        return {
            count: entries.length,
            scored: scored.length,
            avg,
        };
    }, [entries]);

    return (
        <div style={styles.page}>
            <header style={styles.header}>
                <div>
                    <div style={styles.eyebrow}>
                        <Crosshair size={12} /> TPS · PHASE 0 · WATCHLIST
                    </div>
                    <h1 style={styles.title}>Trump-Proximity Score</h1>
                    <p style={styles.subtitle}>
                        Daily top-25 ranked by aggregated proximity across direct contracts,
                        lobbying, congressional activity, FARA edges, and actor-network hops.
                        Missing upstream layers propagate NULL — no silent defaults.
                    </p>
                </div>
                <button onClick={load} style={styles.refresh} disabled={loading}>
                    <RefreshCw size={14} /> {loading ? 'Loading…' : 'Refresh'}
                </button>
            </header>

            {error ? (
                <div style={styles.error}>
                    <AlertTriangle size={14} /> {error}
                </div>
            ) : null}

            <section style={styles.summaryStrip}>
                <SummaryStat label="As of" value={meta.as_of || '—'} mono />
                <SummaryStat label="Ranked" value={`${summary.count}`} />
                <SummaryStat label="Scored" value={`${summary.scored}`} />
                <SummaryStat
                    label="Mean score"
                    value={summary.avg == null ? '—' : summary.avg.toFixed(1)}
                />
            </section>

            <div style={styles.layout}>
                <div style={styles.tableWrap}>
                    <table style={styles.table}>
                        <thead>
                            <tr>
                                <th style={styles.th}>#</th>
                                <th style={styles.th}>Ticker</th>
                                <th style={styles.thRight}>Score</th>
                                <th style={styles.th}>Coverage</th>
                                <th style={styles.thRight}>Evidence</th>
                            </tr>
                        </thead>
                        <tbody>
                            {entries.map((e, idx) => {
                                const isSel = e.ticker === selectedTicker;
                                const cov = e.coverage || {};
                                const cn = coverageCount(cov);
                                return (
                                    <tr
                                        key={e.ticker}
                                        onClick={() => setSelectedTicker(e.ticker)}
                                        style={{ ...styles.tr, ...(isSel ? styles.trSel : {}) }}
                                    >
                                        <td style={styles.td}>{idx + 1}</td>
                                        <td style={{ ...styles.td, fontWeight: 700 }}>{e.ticker}</td>
                                        <td style={{ ...styles.tdRight, color: scoreColor(e.score) }}>
                                            {e.score == null ? 'n/a' : e.score.toFixed(1)}
                                        </td>
                                        <td style={styles.td}>
                                            <CoverageDots coverage={cov} />
                                            <span style={styles.covCount}>{cn}/5</span>
                                        </td>
                                        <td style={styles.tdRight}>
                                            {Array.isArray(e.evidence) ? e.evidence.length : 0}
                                        </td>
                                    </tr>
                                );
                            })}
                            {!entries.length && !loading ? (
                                <tr><td colSpan={5} style={styles.empty}>
                                    No snapshots yet — daily refresh runs at 06:00 ET.
                                </td></tr>
                            ) : null}
                        </tbody>
                    </table>
                </div>

                <aside style={styles.drillWrap}>
                    {!selectedTicker ? (
                        <div style={styles.drillEmpty}>Select a ticker to drill in.</div>
                    ) : drillLoading ? (
                        <div style={styles.drillEmpty}>Computing live…</div>
                    ) : !drill ? (
                        <div style={styles.drillEmpty}>No data for {selectedTicker}.</div>
                    ) : (
                        <TPSDrill ticker={selectedTicker} drill={drill} />
                    )}
                </aside>
            </div>
        </div>
    );
}

function TPSDrill({ ticker, drill }) {
    const layerScores = drill.layer_scores || {};
    const coverage = drill.coverage || {};
    const evidence = Array.isArray(drill.evidence) ? drill.evidence : [];
    return (
        <div>
            <div style={styles.drillHeader}>
                <h2 style={styles.drillTitle}>{ticker}</h2>
                <span style={{ ...styles.drillScore, color: scoreColor(drill.score) }}>
                    {drill.score == null ? 'n/a' : drill.score.toFixed(1)}
                </span>
            </div>

            {drill.score == null ? (
                <div style={styles.lowCov}>
                    <ShieldAlert size={14} /> Low coverage — no scored layer.
                </div>
            ) : null}

            <div style={styles.layerGrid}>
                {LAYER_ORDER.map(name => {
                    const s = layerScores[name];
                    const has = coverage[name];
                    return (
                        <div key={name} style={styles.layerCell}>
                            <div style={styles.layerLabel}>{LAYER_LABELS[name]}</div>
                            <div style={{
                                ...styles.layerScore,
                                color: has ? (s >= 0.5 ? '#fbbf24' : colors.text) : colors.textDim,
                            }}>
                                {has ? (s * 100).toFixed(0) : 'n/a'}
                            </div>
                        </div>
                    );
                })}
            </div>

            <h3 style={styles.evHeader}>
                <Activity size={12} /> EVIDENCE CHAIN ({evidence.length})
            </h3>
            <ul style={styles.evList}>
                {evidence.slice(0, 12).map((ev, i) => (
                    <li key={i} style={styles.evItem}>
                        <span style={styles.evLayer}>{LAYER_LABELS[ev.layer] || ev.layer}</span>
                        <span style={styles.evSource}>{ev.source}</span>
                        <span style={styles.evDetail}>{ev.detail}</span>
                        <span style={styles.evAmt}>{fmtUSD(ev.amount_usd)}</span>
                    </li>
                ))}
                {!evidence.length ? (
                    <li style={styles.evItem}><span style={styles.evDetail}>No evidence rows.</span></li>
                ) : null}
            </ul>

            <h3 style={styles.evHeader}>NETWORK MINI</h3>
            <div style={styles.mini}>
                <Suspense fallback={<div style={styles.drillEmpty}>Graph loading…</div>}>
                    <ActorNetwork compact focusTicker={ticker} />
                </Suspense>
            </div>
        </div>
    );
}

function CoverageDots({ coverage }) {
    return (
        <span style={{ display: 'inline-flex', gap: 3, marginRight: 6 }}>
            {LAYER_ORDER.map(name => (
                <span
                    key={name}
                    title={LAYER_LABELS[name]}
                    style={{
                        width: 8, height: 8, borderRadius: '50%',
                        background: coverage[name] ? '#34d399' : '#404040',
                        display: 'inline-block',
                    }}
                />
            ))}
        </span>
    );
}

function SummaryStat({ label, value, mono: useMono }) {
    return (
        <div style={styles.statCell}>
            <div style={styles.statLabel}>{label}</div>
            <div style={{ ...styles.statValue, fontFamily: useMono ? mono : undefined }}>{value}</div>
        </div>
    );
}

const styles = {
    page: {
        width: '100%',
        minHeight: 'calc(100vh - 64px)',
        background: colors.bg,
        color: colors.text,
        padding: 18,
        boxSizing: 'border-box',
    },
    header: { display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', gap: 12, marginBottom: 14 },
    eyebrow: { display: 'flex', alignItems: 'center', gap: 8, color: colors.accentLight || colors.accent, fontSize: 11, fontFamily: mono, fontWeight: 800, textTransform: 'uppercase', marginBottom: 8 },
    title: { margin: 0, color: '#E8F0F8', fontSize: 28, fontWeight: 800 },
    subtitle: { marginTop: 8, color: colors.textDim, fontSize: 14, maxWidth: 720 },
    refresh: { display: 'inline-flex', alignItems: 'center', gap: 6, padding: '8px 14px', background: colors.card, color: colors.text, border: `1px solid ${colors.border}`, borderRadius: 6, cursor: 'pointer', fontFamily: mono, fontSize: 12 },
    error: { display: 'flex', gap: 8, alignItems: 'center', color: '#f87171', background: 'rgba(248,113,113,0.08)', border: '1px solid rgba(248,113,113,0.25)', padding: 10, borderRadius: 6, marginBottom: 12, fontSize: 13 },
    summaryStrip: { display: 'grid', gridTemplateColumns: 'repeat(4, minmax(120px, 1fr))', gap: 10, marginBottom: 14 },
    statCell: { border: `1px solid ${colors.border}`, borderRadius: 6, padding: 10, background: colors.card },
    statLabel: { fontSize: 10, fontFamily: mono, fontWeight: 700, color: colors.textDim, textTransform: 'uppercase', marginBottom: 4 },
    statValue: { fontSize: 18, fontWeight: 700, color: '#E8F0F8' },
    layout: { display: 'grid', gridTemplateColumns: '1.4fr 1fr', gap: 16, alignItems: 'flex-start' },
    tableWrap: { border: `1px solid ${colors.border}`, borderRadius: 6, overflow: 'hidden', background: colors.card },
    table: { width: '100%', borderCollapse: 'collapse', fontSize: 13 },
    th: { textAlign: 'left', padding: '10px 12px', color: colors.textDim, fontFamily: mono, fontWeight: 700, fontSize: 11, textTransform: 'uppercase', borderBottom: `1px solid ${colors.border}` },
    thRight: { textAlign: 'right', padding: '10px 12px', color: colors.textDim, fontFamily: mono, fontWeight: 700, fontSize: 11, textTransform: 'uppercase', borderBottom: `1px solid ${colors.border}` },
    tr: { cursor: 'pointer', borderBottom: `1px solid ${colors.borderSubtle || colors.border}` },
    trSel: { background: 'rgba(96,165,250,0.08)' },
    td: { padding: '10px 12px', color: colors.text },
    tdRight: { padding: '10px 12px', color: colors.text, textAlign: 'right', fontFamily: mono, fontWeight: 700 },
    covCount: { fontFamily: mono, fontSize: 11, color: colors.textDim, marginLeft: 4 },
    empty: { padding: 24, textAlign: 'center', color: colors.textDim, fontStyle: 'italic' },
    drillWrap: { border: `1px solid ${colors.border}`, borderRadius: 6, padding: 14, background: colors.card, position: 'sticky', top: 16 },
    drillEmpty: { color: colors.textDim, padding: 16, textAlign: 'center', fontSize: 13 },
    drillHeader: { display: 'flex', alignItems: 'baseline', justifyContent: 'space-between', marginBottom: 10 },
    drillTitle: { margin: 0, color: '#E8F0F8', fontSize: 22, fontWeight: 800 },
    drillScore: { fontFamily: mono, fontSize: 28, fontWeight: 800 },
    lowCov: { display: 'flex', alignItems: 'center', gap: 6, color: '#fbbf24', fontSize: 12, marginBottom: 10 },
    layerGrid: { display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 6, marginBottom: 14 },
    layerCell: { border: `1px solid ${colors.border}`, borderRadius: 4, padding: 6, textAlign: 'center' },
    layerLabel: { fontSize: 10, fontFamily: mono, fontWeight: 700, color: colors.textDim, textTransform: 'uppercase' },
    layerScore: { fontSize: 16, fontWeight: 700, marginTop: 2 },
    evHeader: { display: 'flex', alignItems: 'center', gap: 6, fontSize: 11, fontFamily: mono, fontWeight: 700, color: colors.textDim, textTransform: 'uppercase', margin: '14px 0 8px' },
    evList: { listStyle: 'none', margin: 0, padding: 0 },
    evItem: { display: 'grid', gridTemplateColumns: '90px 120px 1fr 80px', gap: 6, padding: '6px 0', fontSize: 12, borderBottom: `1px solid ${colors.borderSubtle || colors.border}` },
    evLayer: { color: colors.accent, fontFamily: mono, fontSize: 11, fontWeight: 700, textTransform: 'uppercase' },
    evSource: { color: colors.text, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
    evDetail: { color: colors.textDim, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' },
    evAmt: { color: colors.text, textAlign: 'right', fontFamily: mono, fontWeight: 700 },
    mini: { height: 280, border: `1px solid ${colors.border}`, borderRadius: 4, overflow: 'hidden' },
};
