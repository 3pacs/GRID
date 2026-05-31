import React, { useState, useEffect } from 'react';
import { api } from '../../api.js';
import { colors } from '../../styles/shared.js';
import { AnswerView } from './answerFormat.jsx';

const SANS = "'IBM Plex Sans', -apple-system, sans-serif";
const MONO = "'IBM Plex Mono', monospace";

// ── Shared card shell + states ─────────────────────────────────────────

function Shell({ title, children, accent }) {
    return (
        <div style={{ ...CS.card, ...(accent ? { borderColor: `${colors.accent}55` } : null) }}>
            {title ? <div style={CS.title}>{title}</div> : null}
            {children}
        </div>
    );
}

function Loading() {
    return <div style={CS.dim}>Loading…</div>;
}

function Empty({ msg }) {
    return <div style={CS.dim}>{msg || 'No data yet.'}</div>;
}

/** Generic data-fetching hook with loading/error/empty handling. */
function useFetch(fn, deps) {
    const [state, setState] = useState({ loading: true, error: null, data: null });
    useEffect(() => {
        let alive = true;
        setState({ loading: true, error: null, data: null });
        Promise.resolve()
            .then(fn)
            .then((data) => { if (alive) setState({ loading: false, error: null, data }); })
            .catch((err) => { if (alive) setState({ loading: false, error: err?.message || 'Failed', data: null }); });
        return () => { alive = false; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);
    return state;
}

const fmtPct = (v) => (typeof v === 'number' ? `${v >= 0 ? '+' : ''}${(v * (Math.abs(v) < 1 ? 100 : 1)).toFixed(2)}%` : null);
const fmtNum = (v) => (typeof v === 'number' ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : null);

// ── Verdict ────────────────────────────────────────────────────────────

function VerdictCard({ title, props }) {
    const question = props?.question || title || '';
    const [text, setText] = useState('');
    const [status, setStatus] = useState('loading'); // loading | streaming | done | error
    const [errMsg, setErrMsg] = useState('');

    useEffect(() => {
        if (!question) { setStatus('done'); return undefined; }
        const controller = new AbortController();
        setText(''); setErrMsg(''); setStatus('loading');
        api.askStream(question, {
            history: [],
            signal: controller.signal,
            onDelta: (full) => { setText(full); setStatus('streaming'); },
        })
            .then(() => setStatus('done'))
            .catch((e) => {
                if (e?.name === 'AbortError') return;
                setErrMsg(e?.message || 'Failed'); setStatus('error');
            });
        return () => controller.abort();
    }, [question]);

    return (
        <Shell title={title || 'Your read'} accent>
            {status === 'loading' && <div style={CS.dim}>Thinking…</div>}
            {status === 'error' && <div style={CS.err}>Couldn’t reach GRID: {errMsg}</div>}
            {(status === 'streaming' || status === 'done') && text && (
                <div>
                    <AnswerView text={text} />
                    {status === 'streaming' && <span style={CS.caret} />}
                </div>
            )}
        </Shell>
    );
}

// ── Ticker pulse ───────────────────────────────────────────────────────

function TickerPulseCard({ title, props }) {
    const ticker = (props?.ticker || '').toUpperCase();
    const { loading, error, data } = useFetch(
        () => (ticker ? api.getTickerQuote(ticker) : Promise.resolve(null)),
        [ticker],
    );
    const price = data?.price;
    const change = data?.change_pct;
    const sentiment = data?.sentiment;
    const levels = data ? {
        'put/call': data.put_call_ratio,
        'max pain': data.max_pain,
        'IV': data.iv_atm,
    } : null;
    const hasLevels = levels && Object.values(levels).some((v) => v != null);
    return (
        <Shell title={title || ticker}>
            {!ticker && <Empty msg="No ticker." />}
            {ticker && loading && <Loading />}
            {ticker && error && <div style={CS.err}>No live data for {ticker}.</div>}
            {ticker && data && (
                <div style={CS.col}>
                    <div style={CS.priceRow}>
                        <span style={CS.ticker}>{ticker}</span>
                        {typeof price === 'number' && <span style={CS.price}>${fmtNum(price)}</span>}
                        {typeof change === 'number' && (
                            <span style={{ ...CS.change, color: change >= 0 ? colors.green : colors.red }}>
                                {fmtPct(change)}
                            </span>
                        )}
                    </div>
                    {sentiment && (
                        <div style={{ ...CS.badge, ...sentimentStyle(sentiment) }}>
                            {String(sentiment).toUpperCase()}
                        </div>
                    )}
                    {typeof price !== 'number' && <Empty msg={`Watching ${ticker} — no price yet.`} />}
                    {hasLevels && (
                        <div style={CS.levels}>
                            {Object.entries(levels).filter(([, v]) => v != null).map(([k, v]) => (
                                <span key={k} style={CS.level}>
                                    <span style={CS.levelK}>{k}</span> {fmtNum(v) ?? String(v)}
                                </span>
                            ))}
                        </div>
                    )}
                </div>
            )}
        </Shell>
    );
}

function sentimentStyle(s) {
    const v = String(s).toLowerCase();
    if (/bull|positive|up|risk.?on|green/.test(v)) return { background: colors.greenBg, color: colors.green };
    if (/bear|negative|down|risk.?off|red/.test(v)) return { background: colors.redBg, color: colors.red };
    return { background: colors.card, color: colors.textDim };
}

// ── Watchlist ──────────────────────────────────────────────────────────

function WatchlistCard({ title }) {
    const { loading, error, data } = useFetch(() => api.getWatchlist(), []);
    const items = Array.isArray(data) ? data : (data?.items || data?.watchlist || data?.tickers || []);
    return (
        <Shell title={title || 'My watchlist'}>
            {loading && <Loading />}
            {error && <div style={CS.err}>Couldn’t load watchlist.</div>}
            {data && (items.length === 0
                ? <Empty msg="Watchlist is empty." />
                : (
                    <div style={CS.chips}>
                        {items.slice(0, 30).map((it, i) => {
                            const t = (it.ticker || it.symbol || it.name || it) ?? '';
                            return <span key={i} style={CS.chip}>{String(t).toUpperCase()}</span>;
                        })}
                    </div>
                ))}
        </Shell>
    );
}

// ── Macro regime ───────────────────────────────────────────────────────

function MacroRegimeCard({ title }) {
    const { loading, error, data } = useFetch(() => api.getCurrent(), []);
    const regime = data?.regime;
    const conf = data?.confidence;
    const stress = data?.stress_index;
    return (
        <Shell title={title || 'The market right now'}>
            {loading && <Loading />}
            {error && <div style={CS.err}>Couldn’t load regime.</div>}
            {data && (regime
                ? (
                    <div style={CS.col}>
                        <div style={{ ...CS.regime, ...sentimentStyle(regime) }}>{String(regime).replace(/_/g, ' ')}</div>
                        <div style={CS.meta}>
                            {typeof conf === 'number' && <span>Confidence {(conf * (conf <= 1 ? 100 : 1)).toFixed(0)}%</span>}
                            {typeof stress === 'number' && <span>Stress {stress.toFixed(2)}</span>}
                        </div>
                    </div>
                )
                : <Empty msg="No regime read yet." />)}
        </Shell>
    );
}

// ── News momentum ──────────────────────────────────────────────────────

function NewsCard({ title }) {
    const { loading, error, data } = useFetch(() => api.getNewsMomentum(), []);
    const direction = data?.direction || data?.momentum_direction || data?.trend || data?.state;
    const summary = data?.summary || data?.headline || data?.description;
    return (
        <Shell title={title || "What's moving the news"}>
            {loading && <Loading />}
            {error && <div style={CS.err}>Couldn’t load news.</div>}
            {data && (
                <div style={CS.col}>
                    {direction && <div style={{ ...CS.badge, ...sentimentStyle(direction) }}>{String(direction).toUpperCase()}</div>}
                    {summary ? <div style={CS.body}>{summary}</div> : (!direction && <Empty msg="Quiet news flow." />)}
                </div>
            )}
        </Shell>
    );
}

// ── Money flow (sectors) ───────────────────────────────────────────────

function MoneyFlowCard({ title }) {
    const { loading, error, data } = useFetch(() => api.getSectorFlows(), []);
    const sectors = data?.sectors || [];
    const ranked = [...sectors]
        .filter((s) => typeof s?.sector_stress === 'number')
        .sort((a, b) => Math.abs(b.sector_stress) - Math.abs(a.sector_stress))
        .slice(0, 6);
    return (
        <Shell title={title || 'Where the money is moving'}>
            {loading && <Loading />}
            {error && <div style={CS.err}>Couldn’t load flows.</div>}
            {data && (ranked.length === 0
                ? <Empty msg="No flow data yet." />
                : (
                    <div style={CS.col}>
                        {ranked.map((s, i) => {
                            const name = s.name || s.sector || s.etf || `Sector ${i + 1}`;
                            const z = s.sector_stress;
                            return (
                                <div key={i} style={CS.flowRow}>
                                    <span style={CS.flowName}>{name}</span>
                                    <span style={{ ...CS.flowZ, color: z >= 0 ? colors.green : colors.red }}>
                                        {z >= 0 ? '▲' : '▼'} {Math.abs(z).toFixed(2)}
                                    </span>
                                </div>
                            );
                        })}
                    </div>
                ))}
        </Shell>
    );
}

// ── Registry ───────────────────────────────────────────────────────────

const REGISTRY = {
    verdict: VerdictCard,
    ticker_pulse: TickerPulseCard,
    watchlist: WatchlistCard,
    macro_regime: MacroRegimeCard,
    news: NewsCard,
    money_flow: MoneyFlowCard,
};

/** Render a composed layout. `verdict` cards span full width; others tile. */
export function WidgetGrid({ widgets }) {
    if (!Array.isArray(widgets) || widgets.length === 0) return null;
    return (
        <div style={CS.grid}>
            <style>{'@keyframes chatPulse{0%,100%{opacity:1}50%{opacity:0.25}}'}</style>
            {widgets.map((w, i) => {
                const Comp = REGISTRY[w.type];
                if (!Comp) return null;
                const span = w.type === 'verdict' ? CS.spanFull : null;
                return (
                    <div key={i} style={{ ...CS.cell, ...span }}>
                        <Comp title={w.title} props={w.props || {}} />
                    </div>
                );
            })}
        </div>
    );
}

const CS = {
    grid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
        gap: '14px',
        width: '100%',
    },
    cell: { minWidth: 0 },
    spanFull: { gridColumn: '1 / -1' },
    card: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '14px',
        padding: '16px 18px',
        height: '100%',
        boxSizing: 'border-box',
    },
    title: {
        fontSize: '12px', fontWeight: 700, textTransform: 'uppercase',
        letterSpacing: '0.06em', color: colors.textMuted, marginBottom: '10px',
    },
    col: { display: 'flex', flexDirection: 'column', gap: '10px' },
    body: { fontSize: '15px', lineHeight: 1.55, color: colors.text, fontFamily: SANS },
    dim: { fontSize: '13px', color: colors.textMuted, fontFamily: SANS },
    caret: {
        display: 'inline-block', width: '8px', height: '15px', marginLeft: '3px',
        background: colors.accent, verticalAlign: 'text-bottom', borderRadius: '1px',
        animation: 'chatPulse 1s ease-in-out infinite',
    },
    err: { fontSize: '13px', color: colors.red, fontFamily: SANS },
    priceRow: { display: 'flex', alignItems: 'baseline', gap: '10px', flexWrap: 'wrap' },
    ticker: { fontSize: '22px', fontWeight: 800, color: colors.text, fontFamily: MONO, letterSpacing: '1px' },
    price: { fontSize: '20px', fontWeight: 700, color: colors.text, fontFamily: MONO },
    change: { fontSize: '14px', fontWeight: 700, fontFamily: MONO },
    badge: {
        alignSelf: 'flex-start', fontSize: '11px', fontWeight: 700,
        padding: '3px 9px', borderRadius: '6px', letterSpacing: '0.04em',
    },
    levels: { display: 'flex', flexWrap: 'wrap', gap: '8px', marginTop: '2px' },
    level: { fontSize: '12px', fontFamily: MONO, color: colors.textDim },
    levelK: { color: colors.textMuted },
    chips: { display: 'flex', flexWrap: 'wrap', gap: '6px' },
    chip: {
        fontSize: '12px', fontFamily: MONO, color: colors.text,
        background: colors.bg, border: `1px solid ${colors.border}`,
        padding: '4px 9px', borderRadius: '6px',
    },
    regime: { alignSelf: 'flex-start', fontSize: '18px', fontWeight: 800, padding: '6px 14px', borderRadius: '8px', textTransform: 'capitalize' },
    meta: { display: 'flex', gap: '14px', fontSize: '12px', color: colors.textDim, fontFamily: MONO },
    flowRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', fontSize: '14px' },
    flowName: { color: colors.text, fontFamily: SANS, textTransform: 'capitalize' },
    flowZ: { fontFamily: MONO, fontWeight: 700, fontSize: '13px' },
};
