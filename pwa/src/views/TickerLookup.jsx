import React, { useEffect, useMemo, useRef, useState } from 'react';
import {
    AlertTriangle,
    BarChart3,
    CheckCircle2,
    Database,
    ExternalLink,
    FileSpreadsheet,
    Gauge,
    Layers,
    RefreshCw,
    Search,
    Sparkles,
    Target,
} from 'lucide-react';
import { api } from '../api.js';
import PriceChart from '../components/PriceChart.jsx';

const QUICK_TICKERS = ['RXT', 'PURR'];
const DETAIL_STATUS_IDLE = { evidence: 'idle', chart: 'idle', finviz: 'idle', options: 'idle' };
const DETAIL_STATUS_LOADING = { evidence: 'loading', chart: 'loading', finviz: 'loading', options: 'loading' };

function cleanTicker(value) {
    return String(value || '').replace(/[^A-Za-z0-9.^-]/g, '').toUpperCase().slice(0, 12);
}

function metric(value, fallback = '0') {
    if (value == null || value === '') return fallback;
    if (typeof value === 'number') return value.toLocaleString();
    return String(value);
}

function toneClass(tone) {
    if (tone === 'strong') return 'tl-strong';
    if (tone === 'watch') return 'tl-watch';
    if (tone === 'light') return 'tl-light';
    return 'tl-neutral';
}

function StatusPill({ status }) {
    const label = status === 'ready'
        ? 'Workbook match'
        : status === 'not_found'
            ? 'No match'
            : status === 'unavailable'
                ? 'Database offline'
                : 'Waiting';
    return <span className={`tl-status tl-status-${status || 'waiting'}`}>{label}</span>;
}

function LanePill({ lane }) {
    return (
        <div className={`tl-lane tl-lane-${lane.id}`}>
            <strong>{lane.label}</strong>
            <span>{metric(lane.file_hits + lane.sheet_hits + lane.evidence_rows)} hits</span>
        </div>
    );
}

function TradingViewActions({ tv, ticker, signals }) {
    if (!tv) return null;
    return (
        <div className="tl-tv-actions">
            <a href={tv.chart_url} target="_blank" rel="noreferrer">
                <ExternalLink size={17} /> Open TradingView
            </a>
            <a href={tv.symbol_search_url} target="_blank" rel="noreferrer">
                <Search size={17} /> Symbol search
            </a>
            <span>{signals?.length ? `${signals.length} webhook alerts` : `${ticker} chart lane ready`}</span>
        </div>
    );
}

function stateClass(state) {
    if (state === 'strong' || state === 'fresh') return 'tl-state-strong';
    if (state === 'watch' || state === 'aging') return 'tl-state-watch';
    if (state === 'caution' || state === 'stale') return 'tl-state-caution';
    return 'tl-state-muted';
}

function normalizeChartPayload(payload) {
    const grid = payload?.grid_data || {};
    return {
        ...grid,
        price_history: payload?.price_history || grid.price_history || [],
        metrics: payload?.metrics || grid.metrics || {},
        features: payload?.features || grid.features || [],
        source_freshness: payload?.source_freshness || grid.source_freshness || [],
        tradingview_signals: payload?.tradingview_signals || payload?.signals?.tradingview_signals || [],
        regime: payload?.regime || payload?.signals?.regime || null,
    };
}

function mergeTickerData(current, patch) {
    if (!patch) return current;
    const base = current || {};
    const next = {
        ...base,
        ...patch,
    };
    if (base.workbook || patch.workbook) {
        next.workbook = { ...(base.workbook || {}), ...(patch.workbook || {}) };
    }
    if (base.grid_data || patch.grid_data) {
        next.grid_data = { ...(base.grid_data || {}), ...(patch.grid_data || {}) };
    }
    if (base.signals || patch.signals) {
        next.signals = { ...(base.signals || {}), ...(patch.signals || {}) };
    }
    if (patch.finviz?.finviz) {
        next.finviz = patch.finviz.finviz;
    }
    if (patch.options?.options) {
        next.options = patch.options.options;
    }
    return next;
}

function DecisionStack({ decision }) {
    if (!decision) return null;
    return (
        <section className="tl-panel tl-wide tl-decision-panel">
            <div className="tl-section-head">
                <div>
                    <span>GRID decision stack</span>
                    <h2>{decision.stance || 'Waiting'}</h2>
                </div>
                <Gauge size={21} />
            </div>
            <div className="tl-decision-hero">
                <strong>{Number(decision.score || 0).toFixed(1)}</strong>
                <div>
                    <span>{decision.method}</span>
                    <div className="tl-score-track">
                        <span style={{ width: `${Math.min(100, Number(decision.score || 0))}%` }} />
                    </div>
                </div>
            </div>
            <div className="tl-decision-card-grid">
                {(decision.cards || []).map(card => (
                    <div key={card.source} className={`tl-decision-card ${stateClass(card.state)}`}>
                        <strong>{card.source}</strong>
                        <em>{Number(card.points || 0).toFixed(1)} pts</em>
                        <span>{card.detail}</span>
                    </div>
                ))}
            </div>
            <div className="tl-reason-grid">
                <div>
                    <strong>Reasons</strong>
                    {(decision.reasons || []).map(item => <span key={item}>{item}</span>)}
                </div>
                <div>
                    <strong>Blockers</strong>
                    {(decision.blockers || []).length
                        ? decision.blockers.map(item => <span key={item}>{item}</span>)
                        : <span>No major blockers in the current stack.</span>}
                </div>
            </div>
        </section>
    );
}

function FinvizPanel({ finviz, onRefresh, refreshing }) {
    const stats = finviz?.stats || [];
    return (
        <article className="tl-panel">
            <div className="tl-section-head">
                <div>
                    <span>Finviz in Postgres</span>
                    <h2>{finviz?.status || 'unavailable'}</h2>
                </div>
                <button
                    type="button"
                    className="tl-icon-button"
                    onClick={onRefresh}
                    disabled={refreshing}
                    title="Refresh Finviz"
                    aria-label="Refresh Finviz"
                >
                    {refreshing ? <RefreshCw size={18} className="tl-spin" /> : <Database size={18} />}
                </button>
            </div>
            <div className="tl-finviz-meta">
                <span>{finviz?.field_count || 0} fields</span>
                <span>{finviz?.freshness?.label || 'missing'}</span>
                <span>{finviz?.latest_obs_date || 'no date'}</span>
            </div>
            <div className="tl-finviz-grid">
                {stats.length ? stats.slice(0, 12).map(stat => (
                    <div key={stat.id}>
                        <span>{stat.label}</span>
                        <strong>{stat.raw_value ?? '-'}</strong>
                    </div>
                )) : <p className="tl-muted">No Finviz snapshot rows found for this ticker yet.</p>}
            </div>
        </article>
    );
}

function SourceFreshness({ sources }) {
    return (
        <article className="tl-panel">
            <div className="tl-section-head">
                <div>
                    <span>GRID freshness</span>
                    <h2>Source age</h2>
                </div>
            </div>
            <div className="tl-source-grid">
                {(sources || []).length ? sources.map(source => (
                    <div key={source.source} className={`tl-source-row ${stateClass(source.state)}`}>
                        <strong>{source.source}</strong>
                        <span>{source.label || source.state}</span>
                        <em>{source.age_hours == null ? '-' : `${Number(source.age_hours).toFixed(1)}h`}</em>
                    </div>
                )) : <p className="tl-muted">Freshness data unavailable.</p>}
            </div>
        </article>
    );
}

function EvidenceTable({ rows }) {
    if (!rows?.length) {
        return (
            <div className="tl-empty">
                <FileSpreadsheet size={22} />
                <span>No workbook evidence rows found for this ticker.</span>
            </div>
        );
    }

    return (
        <div className="tl-table-wrap">
            <table className="tl-table">
                <thead>
                    <tr>
                        <th>File</th>
                        <th>Sheet</th>
                        <th>Cell</th>
                        <th>Evidence</th>
                        <th>Score</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, index) => (
                        <tr key={`${row.file}-${row.sheet}-${row.cell}-${index}`}>
                            <td>{row.file || 'file'}</td>
                            <td>{row.sheet || row.source_type}</td>
                            <td>{row.cell || '-'}</td>
                            <td>
                                <div className="tl-evidence-text">{row.evidence_text || row.row_context || '-'}</div>
                                {row.column_header ? <div className="tl-subtle">Column: {row.column_header}</div> : null}
                            </td>
                            <td>{Number(row.context_score || 0).toFixed(1)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default function TickerLookup() {
    const streamRef = useRef(null);
    const [query, setQuery] = useState('RXT');
    const [activeTicker, setActiveTicker] = useState('RXT');
    const [data, setData] = useState(null);
    const [marketData, setMarketData] = useState(null);
    const [period, setPeriod] = useState('1Y');
    const [loading, setLoading] = useState(false);
    const [marketLoading, setMarketLoading] = useState(false);
    const [sectionStatus, setSectionStatus] = useState(DETAIL_STATUS_IDLE);
    const [error, setError] = useState('');

    const summary = data?.summary || {};
    const gold = data?.gold || {};
    const evidence = data?.workbook?.evidence || [];
    const files = data?.workbook?.files || [];
    const sheets = data?.workbook?.sheets || [];
    const decisionStack = data?.decision_stack || null;
    const finviz = data?.finviz || null;
    const gridData = data?.grid_data || {};
    const dadStats = data?.dad_stats || [];
    const sourceLanes = data?.source_lanes || [];
    const tvSignals = marketData?.tradingview_signals || data?.signals?.tradingview_signals || [];
    const prices = marketData?.price_history || [];
    const latestPrice = prices.length ? prices[prices.length - 1]?.value : marketData?.live_price?.price;
    const firstPrice = prices.length ? prices[0]?.value : null;
    const periodReturn = latestPrice != null && firstPrice ? ((latestPrice - firstPrice) / firstPrice) * 100 : null;
    const score = Number(gold.score || 0);

    const footprintBars = useMemo(() => {
        const values = [
            Number(summary.mentions || 0),
            Number(summary.file_count || 0) * 8,
            Number(summary.sheet_count || 0) * 4,
            Number(summary.evidence_score || 0),
        ];
        const max = Math.max(1, ...values);
        return values.map(value => Math.max(6, Math.round((value / max) * 100)));
    }, [summary]);

    function updateSectionStatus(section, status) {
        setSectionStatus(current => ({ ...current, [section]: status }));
    }

    async function loadMarket(nextTicker, nextPeriod = period) {
        setMarketLoading(true);
        updateSectionStatus('chart', 'loading');
        const result = await api.getDadTickerChart(nextTicker, { range: nextPeriod, points: 260 });
        setMarketLoading(false);
        if (!result?.error) {
            setMarketData(normalizeChartPayload(result));
            setData(current => mergeTickerData(current, { grid_data: result.grid_data, signals: result.signals }));
            updateSectionStatus('chart', 'ready');
        } else {
            setMarketData(null);
            updateSectionStatus('chart', 'error');
        }
    }

    async function handlePeriodChange(nextPeriod) {
        setPeriod(nextPeriod);
        await loadMarket(activeTicker, nextPeriod);
    }

    async function hydrateDetails(ticker, { refreshFinviz = false } = {}) {
        setSectionStatus(DETAIL_STATUS_LOADING);
        setMarketLoading(true);
        const [evidenceResult, chartResult, finvizResult, optionsResult] = await Promise.all([
            api.getDadTickerEvidence(ticker, { limit: 50, offset: 0 }),
            api.getDadTickerChart(ticker, { range: period, points: 260 }),
            api.getDadTickerFinviz(ticker, { refreshFinviz }),
            api.getDadTickerOptions(ticker, { days: 90, limit: 90 }),
        ]);

        setMarketLoading(false);
        if (!evidenceResult?.error) {
            setData(current => mergeTickerData(current, evidenceResult));
        }
        if (!chartResult?.error) {
            setMarketData(normalizeChartPayload(chartResult));
            setData(current => mergeTickerData(current, { grid_data: chartResult.grid_data, signals: chartResult.signals }));
        }
        if (!finvizResult?.error) {
            setData(current => mergeTickerData(current, { finviz: finvizResult.finviz }));
        }
        if (!optionsResult?.error) {
            setData(current => mergeTickerData(current, { options: optionsResult.options }));
        }
        setSectionStatus({
            evidence: evidenceResult?.error ? 'error' : 'ready',
            chart: chartResult?.error ? 'error' : 'ready',
            finviz: finvizResult?.error ? 'error' : 'ready',
            options: optionsResult?.error ? 'error' : 'ready',
        });
    }

    function startDetailStream(ticker, { refreshFinviz = false } = {}) {
        streamRef.current?.close();
        let completed = false;
        streamRef.current = api.streamDadTickerGold(ticker, {
            refreshFinviz,
            onEvent: (eventName, payload) => {
                if (eventName === 'compact') {
                    setData(current => mergeTickerData(current, payload));
                }
                if (eventName === 'evidence') {
                    setData(current => mergeTickerData(current, payload));
                    updateSectionStatus('evidence', 'ready');
                }
                if (eventName === 'chart') {
                    setMarketData(normalizeChartPayload(payload));
                    setMarketLoading(false);
                    setData(current => mergeTickerData(current, { grid_data: payload.grid_data, signals: payload.signals }));
                    updateSectionStatus('chart', 'ready');
                }
                if (eventName === 'finviz') {
                    setData(current => mergeTickerData(current, { finviz: payload.finviz }));
                    updateSectionStatus('finviz', 'ready');
                }
                if (eventName === 'options') {
                    setData(current => mergeTickerData(current, { options: payload.options }));
                    updateSectionStatus('options', 'ready');
                }
                if (eventName === 'error') {
                    completed = true;
                    setError(payload?.message || 'Ticker detail stream failed.');
                    setMarketLoading(false);
                }
            },
            onDone: () => {
                completed = true;
                setMarketLoading(false);
                streamRef.current = null;
            },
            onError: () => {
                if (completed) return;
                completed = true;
                streamRef.current?.close();
                streamRef.current = null;
                hydrateDetails(ticker, { refreshFinviz });
            },
        });
    }

    async function lookup(nextTicker = query, { refreshFinviz = false } = {}) {
        const ticker = cleanTicker(nextTicker);
        if (!ticker) return;
        streamRef.current?.close();
        streamRef.current = null;
        setQuery(ticker);
        setActiveTicker(ticker);
        setLoading(true);
        setMarketLoading(true);
        setSectionStatus(DETAIL_STATUS_LOADING);
        setError('');
        const result = await api.getDadTickerGold(ticker, { refreshFinviz });
        setLoading(false);
        if (result?.error) {
            setError(result.message || 'Ticker lookup failed.');
            setData(null);
            setMarketData(null);
            setMarketLoading(false);
            setSectionStatus(DETAIL_STATUS_IDLE);
            return;
        }
        setData(result);
        setMarketData(normalizeChartPayload({ grid_data: result.grid_data, signals: result.signals }));
        startDetailStream(ticker, { refreshFinviz });
    }

    useEffect(() => {
        lookup('RXT');
        return () => {
            streamRef.current?.close();
            streamRef.current = null;
        };
    }, []);

    return (
        <main className="tl-page">
            <style>{CSS}</style>

            <section className="tl-tool-band">
                <div className="tl-top">
                    <div>
                        <div className="tl-kicker">Dad ticker lookup</div>
                        <h1>{activeTicker}</h1>
                    </div>
                    <StatusPill status={data?.status || (loading ? 'waiting' : 'unavailable')} />
                </div>

                <form
                    className="tl-search"
                    onSubmit={(event) => {
                        event.preventDefault();
                        lookup(query);
                    }}
                >
                    <Search size={22} />
                    <input
                        value={query}
                        onChange={event => setQuery(cleanTicker(event.target.value))}
                        placeholder="Ticker"
                        aria-label="Ticker"
                    />
                    <button type="submit" disabled={loading || !query}>
                        {loading ? <RefreshCw size={19} className="tl-spin" /> : <Sparkles size={19} />}
                        Get Gold
                    </button>
                </form>

                <div className="tl-quick">
                    {QUICK_TICKERS.map(ticker => (
                        <button key={ticker} onClick={() => lookup(ticker)}>{ticker}</button>
                    ))}
                </div>

                {data ? (
                    <div className="tl-hydration-strip" aria-label="Ticker detail hydration">
                        {Object.entries(sectionStatus).map(([section, status]) => (
                            <span key={section} className={`tl-hydration-${status}`}>
                                <strong>{section}</strong>
                                {status}
                            </span>
                        ))}
                    </div>
                ) : null}
            </section>

            {error ? (
                <div className="tl-error"><AlertTriangle size={18} />{error}</div>
            ) : null}

            <section className="tl-grid">
                <article className={`tl-card tl-gold ${toneClass(gold.tone)}`}>
                    <div className="tl-card-head">
                        <span>Gold verdict</span>
                        <Sparkles size={20} />
                    </div>
                    <div className="tl-verdict">{loading ? 'Looking...' : gold.verdict || 'Waiting'}</div>
                    <p>{gold.one_liner || 'Run a ticker to pull the workbook evidence.'}</p>
                    <div className="tl-score-row">
                        <strong>{score}</strong>
                        <div className="tl-score-track">
                            <span style={{ width: `${Math.min(100, score)}%` }} />
                        </div>
                    </div>
                </article>

                <article className="tl-card">
                    <div className="tl-card-head">
                        <span>Workbook footprint</span>
                        <FileSpreadsheet size={20} />
                    </div>
                    <div className="tl-metrics">
                        <div><span>Mentions</span><strong>{metric(summary.mentions)}</strong></div>
                        <div><span>Files</span><strong>{metric(summary.file_count)}</strong></div>
                        <div><span>Sheets</span><strong>{metric(summary.sheet_count)}</strong></div>
                        <div><span>Evidence</span><strong>{metric(summary.evidence_score)}</strong></div>
                    </div>
                    <div className="tl-bars" aria-label="Workbook footprint bars">
                        {footprintBars.map((width, index) => <span key={index} style={{ width: `${width}%` }} />)}
                    </div>
                </article>

                <article className="tl-card">
                    <div className="tl-card-head">
                        <span>Dad-method fit</span>
                        <CheckCircle2 size={20} />
                    </div>
                    <div className="tl-signal-list">
                        {(data?.fit_signals || []).length ? data.fit_signals.map(signal => (
                            <div key={signal.label} className={`tl-signal tl-signal-${signal.state}`}>
                                <strong>{signal.label}</strong>
                                <span>{signal.detail}</span>
                            </div>
                        )) : (
                            <div className="tl-signal tl-signal-neutral">
                                <strong>No fit signals yet</strong>
                                <span>Lookup needs workbook evidence before scoring fit.</span>
                            </div>
                        )}
                    </div>
                </article>
            </section>

            <DecisionStack decision={decisionStack} />

            <section className="tl-two tl-wide tl-stack-grid">
                <FinvizPanel
                    finviz={finviz}
                    refreshing={loading || sectionStatus.finviz === 'loading'}
                    onRefresh={() => lookup(activeTicker, { refreshFinviz: true })}
                />
                <SourceFreshness sources={gridData?.source_freshness || []} />
            </section>

            <section className="tl-panel tl-wide tl-chart-panel">
                <div className="tl-section-head">
                    <div>
                        <span>Chart lane</span>
                        <h2>{activeTicker} vs Dad's chart question</h2>
                    </div>
                    <BarChart3 size={21} />
                </div>
                <div className="tl-chart-topline">
                    <div>
                        <strong>{latestPrice != null ? `$${Number(latestPrice).toLocaleString(undefined, { maximumFractionDigits: 2 })}` : 'Price loading'}</strong>
                        <span>{periodReturn != null ? `${periodReturn >= 0 ? '+' : ''}${periodReturn.toFixed(1)}% over ${period}` : 'Needs market history'}</span>
                    </div>
                    <TradingViewActions tv={data?.tradingview} ticker={activeTicker} signals={tvSignals} />
                </div>
                <div className="tl-price-chart">
                    {marketLoading ? <div className="tl-chart-loading">Loading chart...</div> : null}
                    <PriceChart
                        data={prices}
                        ticker={activeTicker}
                        period={period}
                        onPeriodChange={handlePeriodChange}
                        keyLevels={[]}
                        regime={marketData?.regime}
                    />
                </div>
                {tvSignals.length ? (
                    <div className="tl-tv-signal-list">
                        {tvSignals.slice(0, 5).map((signal, index) => (
                            <div key={`${signal.strategy}-${signal.timestamp}-${index}`}>
                                <strong>{signal.action || signal.strategy || 'alert'}</strong>
                                <span>{signal.message || signal.timestamp || 'TradingView webhook'}</span>
                            </div>
                        ))}
                    </div>
                ) : (
                    <p className="tl-muted">TradingView webhooks are supported when your alerts post into GRID. The button opens your TradingView chart/session for this symbol.</p>
                )}
            </section>

            <section className="tl-panel tl-wide">
                <div className="tl-section-head">
                    <div>
                        <span>Dad stats</span>
                        <h2>What to show him first</h2>
                    </div>
                    <Target size={21} />
                </div>
                <div className="tl-stat-card-grid">
                    {dadStats.map(stat => (
                        <div key={stat.id} className={`tl-stat-card tl-stat-${stat.state}`}>
                            <strong>{stat.label}</strong>
                            <p>{stat.why}</p>
                            <span>{stat.hits?.length ? `Workbook hit: ${stat.hits.join(', ')}` : stat.prompt}</span>
                        </div>
                    ))}
                </div>
            </section>

            {sourceLanes.length ? (
                <section className="tl-panel tl-wide">
                    <div className="tl-section-head">
                        <div>
                            <span>Source lanes</span>
                            <h2>Keep Dad, Anik, and gamble evidence separate</h2>
                        </div>
                    </div>
                    <div className="tl-lane-grid">
                        {sourceLanes.map(lane => <LanePill key={lane.id} lane={lane} />)}
                    </div>
                    <p className="tl-muted">This prevents Anik/main portfolio sheets and option logs from being mistaken for Dad's own long-term method.</p>
                </section>
            ) : null}

            <section className="tl-two">
                <article className="tl-panel">
                    <div className="tl-section-head">
                        <div>
                            <span>Evidence rows</span>
                            <h2>Workbook hits</h2>
                        </div>
                        <Layers size={21} />
                    </div>
                    <EvidenceTable rows={evidence} />
                </article>

                <aside className="tl-side">
                    <article className="tl-panel">
                        <div className="tl-section-head">
                            <div>
                                <span>Top files</span>
                                <h2>Where it appears</h2>
                            </div>
                        </div>
                        <div className="tl-file-list">
                            {files.length ? files.map(file => (
                                <div key={file.file}>
                                    <strong>{file.file}</strong>
                                    <span>{file.mentions} mentions · score {file.score}</span>
                                </div>
                            )) : <p className="tl-muted">No matching files yet.</p>}
                        </div>
                    </article>

                    <article className="tl-panel">
                        <div className="tl-section-head">
                            <div>
                                <span>Risk check</span>
                                <h2>Before acting</h2>
                            </div>
                            <AlertTriangle size={21} />
                        </div>
                        <ul className="tl-list">
                            {(data?.risks || []).map(item => <li key={item}>{item}</li>)}
                        </ul>
                    </article>

                    <article className="tl-panel">
                        <div className="tl-section-head">
                            <div>
                                <span>Next checks</span>
                                <h2>Review queue</h2>
                            </div>
                        </div>
                        <ul className="tl-list">
                            {(data?.next_actions || []).map(item => <li key={item}>{item}</li>)}
                        </ul>
                    </article>
                </aside>
            </section>

            {sheets.length ? (
                <section className="tl-panel tl-wide">
                    <div className="tl-section-head">
                        <div>
                            <span>Sheet concentration</span>
                            <h2>Strongest worksheet matches</h2>
                        </div>
                    </div>
                    <div className="tl-sheet-grid">
                        {sheets.map(sheet => (
                            <div key={`${sheet.file}-${sheet.sheet}`}>
                                <strong>{sheet.sheet}</strong>
                                <span>{sheet.file}</span>
                                <em>{sheet.mentions} mentions · score {sheet.score}</em>
                            </div>
                        ))}
                    </div>
                </section>
            ) : null}
        </main>
    );
}

const CSS = `
.tl-page {
    min-height: calc(100vh - 64px);
    background: #08100d;
    color: #e8f0ea;
    font-family: 'IBM Plex Sans', -apple-system, BlinkMacSystemFont, sans-serif;
    padding: 22px clamp(14px, 2.5vw, 34px) 44px;
    box-sizing: border-box;
}
.tl-tool-band {
    display: grid;
    grid-template-columns: minmax(240px, 1fr);
    gap: 16px;
    max-width: 1280px;
    margin: 0 auto 18px;
}
.tl-top {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
}
.tl-kicker,
.tl-section-head span,
.tl-card-head span {
    color: #82c6ff;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 0;
    text-transform: uppercase;
}
.tl-top h1 {
    margin: 4px 0 0;
    font-size: clamp(34px, 7vw, 74px);
    line-height: .9;
    letter-spacing: 0;
}
.tl-status {
    display: inline-flex;
    align-items: center;
    min-height: 34px;
    padding: 0 12px;
    border: 1px solid #2b3b34;
    border-radius: 8px;
    color: #c9d8d0;
    background: #101a16;
    font-weight: 700;
    white-space: nowrap;
}
.tl-status-ready { border-color: #2a8c61; color: #a9f3c9; }
.tl-status-not_found { border-color: #b78a2b; color: #ffd98a; }
.tl-status-unavailable { border-color: #9b4a48; color: #ffb0aa; }
.tl-search {
    display: grid;
    grid-template-columns: 28px minmax(0, 1fr) auto;
    align-items: center;
    gap: 12px;
    min-height: 72px;
    background: #101a16;
    border: 1px solid #263a31;
    border-radius: 8px;
    padding: 10px 12px 10px 18px;
    box-shadow: 0 10px 28px rgba(0,0,0,.26);
}
.tl-search svg { color: #82c6ff; }
.tl-search input {
    min-width: 0;
    height: 48px;
    border: 0;
    outline: 0;
    background: transparent;
    color: #f4fbf6;
    font-size: 28px;
    font-family: 'IBM Plex Mono', monospace;
    font-weight: 800;
    letter-spacing: 0;
}
.tl-search button,
.tl-quick button {
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    min-height: 46px;
    border: 0;
    border-radius: 8px;
    font-weight: 800;
    cursor: pointer;
}
.tl-search button {
    padding: 0 18px;
    background: #f4c542;
    color: #16120a;
}
.tl-search button:disabled { opacity: .58; cursor: wait; }
.tl-quick {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
}
.tl-quick button {
    padding: 0 13px;
    background: #14221d;
    color: #e7f1eb;
    border: 1px solid #274238;
}
.tl-hydration-strip {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
}
.tl-hydration-strip span {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-height: 30px;
    padding: 0 9px;
    border: 1px solid #263a31;
    border-radius: 8px;
    background: #0b1511;
    color: #a9bbb1;
    font-size: 12px;
    text-transform: uppercase;
}
.tl-hydration-strip strong {
    color: #f4fbf6;
    font-family: 'IBM Plex Mono', monospace;
    letter-spacing: 0;
}
.tl-hydration-ready { border-color: #2a8c61 !important; color: #a9f3c9 !important; }
.tl-hydration-loading { border-color: #b78a2b !important; color: #ffd98a !important; }
.tl-hydration-error { border-color: #9b4a48 !important; color: #ffb0aa !important; }
.tl-error {
    max-width: 1280px;
    margin: 0 auto 16px;
    display: flex;
    align-items: center;
    gap: 10px;
    color: #ffc0ba;
    background: #321715;
    border: 1px solid #743b36;
    border-radius: 8px;
    padding: 12px 14px;
}
.tl-grid,
.tl-two,
.tl-wide {
    max-width: 1280px;
    margin-left: auto;
    margin-right: auto;
}
.tl-grid {
    display: grid;
    grid-template-columns: 1.15fr .95fr 1.05fr;
    gap: 14px;
    margin-bottom: 14px;
}
.tl-card,
.tl-panel {
    background: #101a16;
    border: 1px solid #263a31;
    border-radius: 8px;
    padding: 16px;
    box-sizing: border-box;
}
.tl-card-head,
.tl-section-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 12px;
    margin-bottom: 12px;
}
.tl-section-head h2 {
    margin: 3px 0 0;
    font-size: 20px;
    letter-spacing: 0;
}
.tl-card-head svg,
.tl-section-head svg { color: #f4c542; }
.tl-icon-button {
    width: 38px;
    height: 38px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    border: 1px solid #2d473c;
    border-radius: 8px;
    background: #13221c;
    color: #d9f5e5;
    cursor: pointer;
}
.tl-icon-button:disabled {
    opacity: .55;
    cursor: wait;
}
.tl-gold { min-height: 210px; }
.tl-gold.tl-strong { border-color: #35ad73; background: linear-gradient(180deg, #123321, #101a16); }
.tl-gold.tl-watch { border-color: #d2a53a; background: linear-gradient(180deg, #312817, #101a16); }
.tl-gold.tl-light { border-color: #3d6f92; }
.tl-verdict {
    font-size: clamp(24px, 3vw, 38px);
    line-height: 1.05;
    font-weight: 850;
    letter-spacing: 0;
    margin: 10px 0;
}
.tl-card p {
    margin: 0;
    color: #a9bbb1;
    line-height: 1.45;
}
.tl-score-row {
    display: grid;
    grid-template-columns: 58px minmax(0, 1fr);
    align-items: center;
    gap: 12px;
    margin-top: 18px;
}
.tl-score-row strong {
    font-family: 'IBM Plex Mono', monospace;
    font-size: 30px;
}
.tl-score-track {
    height: 12px;
    background: #06100c;
    border-radius: 999px;
    overflow: hidden;
    border: 1px solid #263a31;
}
.tl-score-track span {
    display: block;
    height: 100%;
    background: linear-gradient(90deg, #34d399, #f4c542);
    border-radius: 999px;
}
.tl-metrics {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
}
.tl-metrics div {
    min-height: 70px;
    background: #08100d;
    border: 1px solid #21372e;
    border-radius: 8px;
    padding: 10px;
    box-sizing: border-box;
}
.tl-metrics span,
.tl-file-list span,
.tl-sheet-grid span,
.tl-muted,
.tl-subtle {
    color: #8fa69a;
    font-size: 13px;
}
.tl-metrics strong {
    display: block;
    margin-top: 6px;
    color: #fff;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 24px;
}
.tl-bars {
    display: grid;
    gap: 6px;
    margin-top: 14px;
}
.tl-bars span {
    display: block;
    height: 7px;
    border-radius: 999px;
    background: #82c6ff;
}
.tl-bars span:nth-child(2) { background: #34d399; }
.tl-bars span:nth-child(3) { background: #f4c542; }
.tl-bars span:nth-child(4) { background: #f87171; }
.tl-signal-list {
    display: grid;
    gap: 8px;
}
.tl-signal {
    border: 1px solid #263a31;
    border-radius: 8px;
    padding: 10px 11px;
    background: #08100d;
}
.tl-signal strong,
.tl-file-list strong,
.tl-sheet-grid strong {
    display: block;
    color: #f5fbf7;
}
.tl-signal span {
    display: block;
    color: #a9bbb1;
    margin-top: 3px;
    line-height: 1.35;
}
.tl-signal-strong { border-color: #2a8c61; }
.tl-signal-watch { border-color: #b78a2b; }
.tl-signal-neutral { border-color: #32473d; }
.tl-two {
    display: grid;
    grid-template-columns: minmax(0, 1.35fr) minmax(300px, .65fr);
    gap: 14px;
    align-items: start;
}
.tl-side {
    display: grid;
    gap: 14px;
}
.tl-table-wrap {
    overflow-x: auto;
    border: 1px solid #263a31;
    border-radius: 8px;
}
.tl-table {
    width: 100%;
    border-collapse: collapse;
    min-width: 760px;
}
.tl-table th,
.tl-table td {
    border-bottom: 1px solid #1d3028;
    padding: 10px;
    text-align: left;
    vertical-align: top;
    font-size: 13px;
}
.tl-table th {
    color: #82c6ff;
    background: #0a1410;
    font-family: 'IBM Plex Mono', monospace;
}
.tl-table td {
    color: #d9e7df;
}
.tl-evidence-text {
    max-width: 560px;
    line-height: 1.4;
}
.tl-empty {
    display: flex;
    align-items: center;
    gap: 10px;
    min-height: 76px;
    color: #a9bbb1;
    border: 1px dashed #31483d;
    border-radius: 8px;
    padding: 14px;
}
.tl-file-list {
    display: grid;
    gap: 10px;
}
.tl-file-list div {
    padding: 10px 0;
    border-bottom: 1px solid #21372e;
}
.tl-list {
    margin: 0;
    padding-left: 18px;
    color: #c9d8d0;
    line-height: 1.5;
}
.tl-list li + li { margin-top: 7px; }
.tl-wide {
    margin-top: 14px;
}
.tl-sheet-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
    gap: 10px;
}
.tl-sheet-grid div {
    min-height: 104px;
    border: 1px solid #263a31;
    border-radius: 8px;
    background: #08100d;
    padding: 12px;
    box-sizing: border-box;
}
.tl-sheet-grid em {
    display: block;
    margin-top: 8px;
    color: #f4c542;
    font-style: normal;
}
.tl-chart-panel {
    overflow: hidden;
}
.tl-chart-topline,
.tl-decision-hero {
    display: grid;
    grid-template-columns: minmax(180px, .35fr) minmax(0, 1fr);
    gap: 14px;
    align-items: center;
    margin-bottom: 14px;
}
.tl-chart-topline strong,
.tl-decision-hero strong {
    display: block;
    color: #ffffff;
    font-family: 'IBM Plex Mono', monospace;
    font-size: clamp(24px, 4vw, 42px);
    line-height: 1;
}
.tl-chart-topline span,
.tl-decision-hero span {
    display: block;
    color: #a9bbb1;
    line-height: 1.4;
}
.tl-tv-actions {
    display: flex;
    gap: 9px;
    align-items: center;
    flex-wrap: wrap;
}
.tl-tv-actions a {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-height: 36px;
    padding: 0 10px;
    border-radius: 8px;
    border: 1px solid #315543;
    background: #0b1511;
    color: #dff8ea;
    text-decoration: none;
    font-weight: 800;
}
.tl-tv-actions span {
    color: #8fa69a;
    font-size: 13px;
}
.tl-price-chart {
    position: relative;
    min-height: 360px;
    border: 1px solid #263a31;
    border-radius: 8px;
    background: #08100d;
    overflow: hidden;
}
.tl-chart-loading {
    position: absolute;
    z-index: 2;
    top: 12px;
    right: 12px;
    border: 1px solid #4b6a5b;
    border-radius: 8px;
    padding: 7px 10px;
    background: rgba(8,16,13,.92);
    color: #dff8ea;
    font-family: 'IBM Plex Mono', monospace;
    font-size: 12px;
}
.tl-tv-signal-list,
.tl-stat-card-grid,
.tl-lane-grid,
.tl-decision-card-grid,
.tl-finviz-grid,
.tl-source-grid {
    display: grid;
    gap: 10px;
}
.tl-tv-signal-list {
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
    margin-top: 12px;
}
.tl-tv-signal-list div,
.tl-stat-card,
.tl-lane,
.tl-decision-card,
.tl-finviz-grid div,
.tl-source-row,
.tl-reason-grid div {
    border: 1px solid #263a31;
    border-radius: 8px;
    background: #08100d;
    padding: 11px;
    box-sizing: border-box;
}
.tl-tv-signal-list strong,
.tl-stat-card strong,
.tl-lane strong,
.tl-decision-card strong,
.tl-finviz-grid strong,
.tl-source-row strong,
.tl-reason-grid strong {
    display: block;
    color: #f5fbf7;
}
.tl-tv-signal-list span,
.tl-stat-card span,
.tl-lane span,
.tl-decision-card span,
.tl-finviz-grid span,
.tl-source-row span,
.tl-reason-grid span {
    display: block;
    color: #a9bbb1;
    margin-top: 4px;
    line-height: 1.35;
}
.tl-stat-card-grid,
.tl-lane-grid,
.tl-decision-card-grid,
.tl-finviz-grid {
    grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
}
.tl-stat-card p {
    margin: 5px 0 8px;
    color: #c8d6ce;
    line-height: 1.4;
}
.tl-stat-present,
.tl-state-strong { border-color: #2a8c61; }
.tl-stat-needed,
.tl-state-watch { border-color: #b78a2b; }
.tl-state-caution { border-color: #9b4a48; }
.tl-state-muted { border-color: #32473d; }
.tl-lane-dad_method { border-color: #35ad73; }
.tl-lane-anik_main { border-color: #82c6ff; }
.tl-lane-gamble_sleeve { border-color: #d2a53a; }
.tl-decision-panel {
    border-color: #375f4a;
}
.tl-decision-card em,
.tl-source-row em {
    display: block;
    margin-top: 4px;
    color: #f4c542;
    font-style: normal;
    font-family: 'IBM Plex Mono', monospace;
}
.tl-reason-grid {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 10px;
    margin-top: 12px;
}
.tl-finviz-meta {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin-bottom: 12px;
}
.tl-finviz-meta span {
    border: 1px solid #263a31;
    border-radius: 8px;
    padding: 6px 9px;
    color: #c9d8d0;
    background: #08100d;
    font-size: 13px;
}
.tl-source-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto auto;
    gap: 8px;
    align-items: center;
}
.tl-spin {
    animation: tl-spin .9s linear infinite;
}
@keyframes tl-spin { to { transform: rotate(360deg); } }
@media (max-width: 920px) {
    .tl-grid,
    .tl-two,
    .tl-reason-grid,
    .tl-chart-topline,
    .tl-decision-hero {
        grid-template-columns: 1fr;
    }
}
@media (max-width: 620px) {
    .tl-search {
        grid-template-columns: 24px minmax(0, 1fr);
    }
    .tl-search button {
        grid-column: 1 / -1;
        width: 100%;
    }
    .tl-status {
        align-self: flex-start;
    }
    .tl-top {
        flex-direction: column;
    }
}
`;
