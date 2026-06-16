import React, { useState, useEffect, useCallback } from 'react';
import { api } from '../../api.js';
import { colors } from '../../styles/shared.js';
import { AnswerView } from './answerFormat.jsx';
import { tickerLabel, tickerName, plainSentiment, plainRegime, warmError } from './plain.js';

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

function Loading({ msg }) {
    return <div style={CS.dim}>{msg || 'One moment…'}</div>;
}

function Empty({ msg }) {
    return <div style={CS.dim}>{msg || 'Nothing to show here yet.'}</div>;
}

/** Calm error line + a big "Try again" button so he's never stuck. */
function ErrorState({ msg, onRetry }) {
    return (
        <div style={CS.col}>
            <div style={CS.err}>{msg}</div>
            {onRetry && (
                <button style={CS.retry} onClick={onRetry}>Try again</button>
            )}
        </div>
    );
}

/** Data-fetching hook with a reload() for retry. */
function useFetch(fn, deps) {
    const [state, setState] = useState({ loading: true, error: null, data: null });
    const [n, setN] = useState(0);
    const reload = useCallback(() => setN((x) => x + 1), []);
    useEffect(() => {
        let alive = true;
        setState({ loading: true, error: null, data: null });
        Promise.resolve()
            .then(fn)
            .then((data) => { if (alive) setState({ loading: false, error: null, data }); })
            .catch(() => { if (alive) setState({ loading: false, error: true, data: null }); });
        return () => { alive = false; };
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [...deps, n]);
    return { ...state, reload };
}

const fmtPct = (v) => (typeof v === 'number' ? `${v >= 0 ? '+' : ''}${(v * (Math.abs(v) < 1 ? 100 : 1)).toFixed(1)}%` : null);
const fmtNum = (v) => (typeof v === 'number' ? v.toLocaleString(undefined, { maximumFractionDigits: 2 }) : null);

function toneColor(tone) {
    return tone === 'up' ? colors.green : tone === 'down' ? colors.red : colors.textDim;
}
function toneBg(tone) {
    return tone === 'up' ? colors.greenBg : tone === 'down' ? colors.redBg : colors.card;
}

/** A plain "Looking up / shaky / calm" badge with a word, not a trader label. */
function MoodBadge({ raw }) {
    const { label, tone } = plainSentiment(raw);
    return <div style={{ ...CS.badge, background: toneBg(tone), color: toneColor(tone) }}>{label}</div>;
}

// ── Verdict ────────────────────────────────────────────────────────────

function VerdictCard({ title, props }) {
    const question = props?.question || title || '';
    const [text, setText] = useState('');
    const [status, setStatus] = useState('loading'); // loading | streaming | done | error
    const [n, setN] = useState(0);
    const retry = useCallback(() => setN((x) => x + 1), []);

    useEffect(() => {
        if (!question) { setStatus('done'); return undefined; }
        const controller = new AbortController();
        setText(''); setStatus('loading');
        api.askStream(question, {
            history: [],
            signal: controller.signal,
            onDelta: (full) => { setText(full); setStatus('streaming'); },
        })
            .then(() => setStatus('done'))
            .catch((e) => { if (e?.name !== 'AbortError') setStatus('error'); });
        return () => controller.abort();
    }, [question, n]);

    return (
        <Shell title={title || 'Your read'} accent>
            {status === 'loading' && (
                <div style={CS.thinking}>
                    <span style={CS.thinkDots}><i style={CS.dot} /><i style={{ ...CS.dot, animationDelay: '.2s' }} /><i style={{ ...CS.dot, animationDelay: '.4s' }} /></span>
                    Reading the market for you…
                </div>
            )}
            {status === 'error' && <ErrorState msg={warmError('verdict')} onRetry={retry} />}
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
    const { loading, error, data, reload } = useFetch(
        () => (ticker ? api.getTickerQuote(ticker) : Promise.resolve(null)),
        [ticker],
    );
    const price = data?.price;
    const change = data?.change_pct;
    // One plain sentence: what does today look like for this stock?
    const moodWord = data ? plainSentiment(data.sentiment).label.toLowerCase() : '';
    const line = (() => {
        if (!data || typeof price !== 'number') return null;
        const dir = typeof change === 'number'
            ? (change > 0.3 ? 'up' : change < -0.3 ? 'down' : 'about flat')
            : null;
        const name = tickerName(ticker);
        if (dir === 'up') return `${name} is up today — ${moodWord}.`;
        if (dir === 'down') return `${name} is down today — ${moodWord}.`;
        return `${name} is steady today — ${moodWord}.`;
    })();

    return (
        <Shell title={title || tickerLabel(ticker)}>
            {!ticker && <Empty msg="No stock picked." />}
            {ticker && loading && <Loading />}
            {ticker && error && <ErrorState msg={warmError('price')} onRetry={reload} />}
            {ticker && data && (
                <div style={CS.col}>
                    <div style={CS.priceRow}>
                        <span style={CS.ticker}>{tickerName(ticker)}</span>
                        {typeof price === 'number' && <span style={CS.price}>${fmtNum(price)}</span>}
                        {typeof change === 'number' && (
                            <span style={{ ...CS.change, color: change >= 0 ? colors.green : colors.red }}>
                                {fmtPct(change)}
                            </span>
                        )}
                    </div>
                    {data?.sentiment && <MoodBadge raw={data.sentiment} />}
                    {line ? <div style={CS.body}>{line}</div>
                        : <Empty msg={`${tickerName(ticker)}'s price isn’t updating right now (the market may be closed).`} />}
                </div>
            )}
        </Shell>
    );
}

// ── Watchlist ──────────────────────────────────────────────────────────

function WatchlistCard({ title }) {
    const { loading, error, data, reload } = useFetch(() => api.getWatchlist(), []);
    const items = Array.isArray(data) ? data : (data?.items || data?.watchlist || data?.tickers || []);
    return (
        <Shell title={title || 'My stocks'}>
            {loading && <Loading />}
            {error && <ErrorState msg={warmError()} onRetry={reload} />}
            {data && (items.length === 0
                ? <Empty msg="No stocks saved yet." />
                : (
                    <div style={CS.chips}>
                        {items.slice(0, 30).map((it, i) => {
                            const t = (it.ticker || it.symbol || it.name || it) ?? '';
                            return <span key={i} style={CS.chip}>{tickerName(t)}</span>;
                        })}
                    </div>
                ))}
        </Shell>
    );
}

// ── Macro regime ───────────────────────────────────────────────────────

function MacroRegimeCard({ title }) {
    const { loading, error, data, reload } = useFetch(() => api.getCurrent(), []);
    const regime = data?.regime;
    const r = regime ? plainRegime(regime) : null;
    return (
        <Shell title={title || 'The market right now'}>
            {loading && <Loading />}
            {error && <ErrorState msg={warmError()} onRetry={reload} />}
            {data && (r
                ? (
                    <div style={CS.col}>
                        <div style={{ ...CS.regime, background: toneBg(r.tone), color: toneColor(r.tone) }}>
                            {r.sentence}
                        </div>
                    </div>
                )
                : <Empty msg="The market read isn’t ready yet — check back shortly." />)}
        </Shell>
    );
}

// ── News momentum ──────────────────────────────────────────────────────

function NewsCard({ title }) {
    const { loading, error, data, reload } = useFetch(() => api.getNewsMomentum(), []);
    const direction = data?.direction || data?.momentum_direction || data?.trend || data?.state;
    const summary = data?.summary || data?.headline || data?.description;
    return (
        <Shell title={title || "What's in the news"}>
            {loading && <Loading />}
            {error && <ErrorState msg={warmError()} onRetry={reload} />}
            {data && (
                <div style={CS.col}>
                    {direction && <MoodBadge raw={direction} />}
                    {summary ? <div style={CS.body}>{summary}</div> : (!direction && <Empty msg="It’s quiet — no big news right now." />)}
                </div>
            )}
        </Shell>
    );
}

// ── Money flow (sectors) ───────────────────────────────────────────────

function MoneyFlowCard({ title }) {
    const { loading, error, data, reload } = useFetch(() => api.getSectorFlows(), []);
    const sectors = data?.sectors || [];
    const ranked = [...sectors]
        .filter((s) => typeof s?.sector_stress === 'number')
        .sort((a, b) => Math.abs(b.sector_stress) - Math.abs(a.sector_stress))
        .slice(0, 6);
    return (
        <Shell title={title || 'Where attention is going'}>
            {loading && <Loading />}
            {error && <ErrorState msg={warmError()} onRetry={reload} />}
            {data && (ranked.length === 0
                ? <Empty msg="Nothing notable moving right now." />
                : (
                    <div style={CS.col}>
                        {ranked.map((s, i) => {
                            const name = s.name || s.sector || s.etf || `Group ${i + 1}`;
                            const inflow = s.sector_stress >= 0;
                            return (
                                <div key={i} style={CS.flowRow}>
                                    <span style={CS.flowName}>{String(name).replace(/_/g, ' ')}</span>
                                    <span style={{ ...CS.flowWord, color: inflow ? colors.green : colors.red }}>
                                        {inflow ? '▲ money coming in' : '▼ money pulling out'}
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
            <style>{'@keyframes chatPulse{0%,100%{opacity:1}50%{opacity:0.25}}@keyframes sdBounce{0%,80%,100%{transform:translateY(0);opacity:.5}40%{transform:translateY(-5px);opacity:1}}'}</style>
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
        gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
        gap: '16px',
        width: '100%',
    },
    cell: { minWidth: 0 },
    spanFull: { gridColumn: '1 / -1' },
    card: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: '14px',
        padding: '18px 20px',
        height: '100%',
        boxSizing: 'border-box',
    },
    title: {
        fontSize: '15px', fontWeight: 700, color: colors.text, marginBottom: '12px',
    },
    col: { display: 'flex', flexDirection: 'column', gap: '12px' },
    body: { fontSize: '17px', lineHeight: 1.55, color: colors.text, fontFamily: SANS },
    dim: { fontSize: '15px', color: colors.textDim, fontFamily: SANS, lineHeight: 1.5 },
    thinking: { display: 'flex', alignItems: 'center', gap: '10px', fontSize: '17px', color: colors.text, fontFamily: SANS },
    thinkDots: { display: 'inline-flex', gap: '4px' },
    dot: { width: '8px', height: '8px', borderRadius: '50%', background: colors.accentLight || colors.accent, animation: 'sdBounce 1.2s infinite' },
    caret: {
        display: 'inline-block', width: '9px', height: '18px', marginLeft: '3px',
        background: colors.accent, verticalAlign: 'text-bottom', borderRadius: '1px',
        animation: 'chatPulse 1s ease-in-out infinite',
    },
    err: { fontSize: '16px', color: colors.red, fontFamily: SANS, lineHeight: 1.5 },
    retry: {
        alignSelf: 'flex-start', fontSize: '16px', fontWeight: 600, fontFamily: SANS,
        color: '#fff', background: colors.accent, border: 'none', borderRadius: '10px',
        padding: '12px 20px', minHeight: '48px', cursor: 'pointer',
    },
    priceRow: { display: 'flex', alignItems: 'baseline', gap: '12px', flexWrap: 'wrap' },
    ticker: { fontSize: '22px', fontWeight: 800, color: colors.text, fontFamily: SANS },
    price: { fontSize: '22px', fontWeight: 700, color: colors.text, fontFamily: MONO },
    change: { fontSize: '18px', fontWeight: 700, fontFamily: MONO },
    badge: {
        alignSelf: 'flex-start', fontSize: '15px', fontWeight: 700,
        padding: '6px 14px', borderRadius: '8px',
    },
    chips: { display: 'flex', flexWrap: 'wrap', gap: '8px' },
    chip: {
        fontSize: '16px', fontFamily: SANS, color: colors.text,
        background: colors.bg, border: `1px solid ${colors.border}`,
        padding: '8px 14px', borderRadius: '8px',
    },
    regime: { fontSize: '18px', fontWeight: 700, padding: '12px 16px', borderRadius: '10px', lineHeight: 1.4 },
    flowRow: { display: 'flex', justifyContent: 'space-between', alignItems: 'center', gap: '12px', fontSize: '17px' },
    flowName: { color: colors.text, fontFamily: SANS, textTransform: 'capitalize', fontWeight: 500 },
    flowWord: { fontFamily: SANS, fontWeight: 600, fontSize: '15px', whiteSpace: 'nowrap' },
};
