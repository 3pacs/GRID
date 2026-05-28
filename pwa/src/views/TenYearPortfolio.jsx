import React, { useEffect, useMemo, useState } from 'react';
import {
    Activity,
    BarChart3,
    Bot,
    CheckCircle2,
    Download,
    DollarSign,
    FileSpreadsheet,
    HeartPulse,
    KeyRound,
    MessageSquare,
    PieChart,
    RefreshCw,
    Send,
    Shield,
    Sparkles,
    TrendingUp,
    Upload,
} from 'lucide-react';
import { api } from '../api.js';

const SANS = "'IBM Plex Sans', -apple-system, BlinkMacSystemFont, sans-serif";
const MONO = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const PROFILE_ICONS = {
    dad_chartist: TrendingUp,
    conservative_compounder: Shield,
    nasdaq_plus: Activity,
    sleep_well_growth: PieChart,
};

const API_SETUP_KEY = 'grid_ten_year_api_setup';
const PRELOADED_PROMPT = `Build a 10-year plan for a $1,000,000 account. Use Dad's chart method: find stocks with 10-year up-and-right histories, compare them against Nasdaq/QQQ, prioritize durable compounders, run Monte Carlo risk ranges, review weekly, and only change positions when the ranking or chart breaks. Keep uploaded workbook holdings private and publish only sanitized rules plus a $1M model plan.`;
const DEFAULT_API_SETUP = {
    schwabAppKey: '',
    callbackUrl: 'https://127.0.0.1/grid/schwab/callback',
    dataMode: 'private-local-import',
};
const INITIAL_CHAT_MESSAGES = [
    {
        role: 'assistant',
        text: 'Oracle heartbeat is live. The $1M 10-year prompt, Monte Carlo ranges, workbook intake, exports, and API checklist are staged on this page.',
    },
];
const DEFAULT_PLAN_STEPS = [
    { step: 'Upload workbook', action: 'Extract methodology signals without returning holdings.' },
    { step: 'Run weekly query', action: 'Score Dad Chartist and Frontier Infrastructure boards.' },
    { step: 'Review Monte Carlo', action: 'Compare p10, p50, p90, and probability above start.' },
    { step: 'Export packet', action: 'Download the Excel plan for review.' },
];

function pct(value, digits = 1) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    return `${(Number(value) * 100).toFixed(digits)}%`;
}

function money(value) {
    if (value == null || Number.isNaN(Number(value))) return '$0';
    return Number(value).toLocaleString(undefined, {
        style: 'currency',
        currency: 'USD',
        maximumFractionDigits: 0,
    });
}

function number(value, digits = 1) {
    if (value == null || Number.isNaN(Number(value))) return 'n/a';
    return Number(value).toFixed(digits);
}

function formatTime(date) {
    if (!date) return 'waiting';
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function triggerDownload({ blob, filename }) {
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement('a');
    anchor.href = url;
    anchor.download = filename || 'grid-export.xlsx';
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

function plannerReply(text, workbookPlan, activeProfile) {
    const lower = text.toLowerCase();
    if (lower.includes('monte') || lower.includes('risk')) {
        const mc = activeProfile?.monte_carlo;
        return mc
            ? `Monte Carlo is wired for ${activeProfile.label}: p10 ${money(mc.p10)}, p50 ${money(mc.p50)}, p90 ${money(mc.p90)}, ${pct(mc.probability_above_start, 0)} above start.`
            : 'Monte Carlo is queued as soon as the weekly query returns a profile.';
    }
    if (lower.includes('upload') || lower.includes('excel') || lower.includes('workbook')) {
        return workbookPlan
            ? 'Workbook plan is sanitized and ready. I see methodology signals, private fields redacted, and the export packet can be downloaded now.'
            : 'Upload the workbook and I will turn it into a sanitized Dad Method plan without returning raw holdings.';
    }
    if (lower.includes('api') || lower.includes('think') || lower.includes('schwab')) {
        return 'Schwab/thinkorswim stays credential-gated. Save the app key and callback locally here first; account import should remain private until OAuth is confirmed.';
    }
    if (lower.includes('frontier') || lower.includes('ai') || lower.includes('uranium') || lower.includes('metal')) {
        return 'The frontier board is loaded separately from core compounders so AI, compute, uranium, metals, and supercomputer names can be reviewed without contaminating the main allocation.';
    }
    return 'Queued for the planner: I will fold that into the weekly prompt, the workbook-derived rules, and the next export packet.';
}

function makePath(series, width = 360, height = 150) {
    if (!series?.length) return '';
    const values = series.map(point => Number(point.value)).filter(Number.isFinite);
    if (!values.length) return '';
    const min = Math.min(...values);
    const max = Math.max(...values);
    const range = Math.max(max - min, 1);
    return series.map((point, index) => {
        const x = series.length === 1 ? 0 : (index / (series.length - 1)) * width;
        const y = height - ((Number(point.value) - min) / range) * height;
        return `${index === 0 ? 'M' : 'L'} ${x.toFixed(1)} ${y.toFixed(1)}`;
    }).join(' ');
}

function SparkChart({ pick, benchmark }) {
    const pickPath = makePath(pick?.sparkline);
    const benchPath = makePath(benchmark?.sparkline);
    const pickEnd = pick?.sparkline?.[pick.sparkline.length - 1]?.value;
    const benchEnd = benchmark?.sparkline?.[benchmark.sparkline.length - 1]?.value;

    return (
        <div className="ty-chart-panel">
            <div className="ty-chart-head">
                <div>
                    <div className="ty-eyebrow">10-year chart check</div>
                    <div className="ty-chart-title">{pick?.ticker || 'No ticker'} vs Nasdaq</div>
                </div>
                <div className="ty-chart-stats">
                    <span>{pick?.ticker}: {pickEnd ? `${number(pickEnd / 100, 2)}x` : 'n/a'}</span>
                    <span>QQQ: {benchEnd ? `${number(benchEnd / 100, 2)}x` : 'n/a'}</span>
                </div>
            </div>
            <svg viewBox="0 0 360 150" className="ty-chart" role="img" aria-label="Normalized price chart">
                <line x1="0" y1="118" x2="360" y2="118" className="ty-grid-line" />
                <line x1="0" y1="75" x2="360" y2="75" className="ty-grid-line" />
                <line x1="0" y1="32" x2="360" y2="32" className="ty-grid-line" />
                {benchPath && <path d={benchPath} className="ty-bench-line" />}
                {pickPath && <path d={pickPath} className="ty-pick-line" />}
            </svg>
            <div className="ty-legend">
                <span><i className="ty-dot ty-dot-pick" />{pick?.ticker || 'Pick'}</span>
                <span><i className="ty-dot ty-dot-bench" />QQQ</span>
            </div>
        </div>
    );
}

function MetricStrip({ profile }) {
    const first = profile?.allocations?.[0];
    return (
        <div className="ty-metrics">
            <div>
                <span>Top chart</span>
                <strong>{first?.ticker || 'n/a'}</strong>
            </div>
            <div>
                <span>Invested</span>
                <strong>{money(profile?.estimated_invested)}</strong>
            </div>
            <div>
                <span>Cash</span>
                <strong>{money(profile?.estimated_residual_cash)}</strong>
            </div>
            <div>
                <span>Max name</span>
                <strong>{pct(profile?.max_position, 0)}</strong>
            </div>
        </div>
    );
}

function PickTable({ picks }) {
    return (
        <div className="ty-table-wrap">
            <table className="ty-table">
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>Score</th>
                        <th>10y CAGR</th>
                        <th>Vs QQQ</th>
                        <th>Trend</th>
                        <th>Drawdown</th>
                        <th>Target</th>
                        <th>Shares</th>
                    </tr>
                </thead>
                <tbody>
                    {(picks || []).map((pick, index) => (
                        <tr key={pick.ticker}>
                            <td>
                                <div className="ty-ticker-cell">
                                    <span>{index + 1}</span>
                                    <strong>{pick.ticker}</strong>
                                </div>
                            </td>
                            <td>{number(pick.score, 1)}</td>
                            <td>{pct(pick.cagr)}</td>
                            <td className={pick.relative_cagr >= 0 ? 'ty-good' : 'ty-bad'}>{pct(pick.relative_cagr)}</td>
                            <td>{pct(pick.trend_r2, 0)}</td>
                            <td>{pct(pick.max_drawdown, 0)}</td>
                            <td>{money(pick.target_dollars)}</td>
                            <td>{pick.whole_shares?.toLocaleString?.() || 0}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

function ThemeTags({ themes }) {
    const shown = (themes || []).slice(0, 3);
    if (!shown.length) return <span className="ty-muted">screened</span>;
    return (
        <div className="ty-tags">
            {shown.map(theme => <span key={theme}>{theme}</span>)}
        </div>
    );
}

function CandidateTable({ candidates }) {
    return (
        <div className="ty-table-wrap">
            <table className="ty-table ty-frontier-table">
                <thead>
                    <tr>
                        <th>Ticker</th>
                        <th>Theme</th>
                        <th>Score</th>
                        <th>Years</th>
                        <th>CAGR</th>
                        <th>Vs QQQ</th>
                        <th>Trend</th>
                        <th>Drawdown</th>
                    </tr>
                </thead>
                <tbody>
                    {(candidates || []).map((pick, index) => (
                        <tr key={pick.ticker}>
                            <td>
                                <div className="ty-ticker-cell">
                                    <span>{index + 1}</span>
                                    <strong>{pick.ticker}</strong>
                                </div>
                            </td>
                            <td><ThemeTags themes={pick.themes} /></td>
                            <td>{number(pick.score, 1)}</td>
                            <td>{number(pick.years, 1)}</td>
                            <td>{pct(pick.cagr)}</td>
                            <td className={pick.relative_cagr >= 0 ? 'ty-good' : 'ty-bad'}>{pct(pick.relative_cagr)}</td>
                            <td>{pct(pick.trend_r2, 0)}</td>
                            <td>{pct(pick.max_drawdown, 0)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

export default function TenYearPortfolio() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState('');
    const [capital, setCapital] = useState(1000000);
    const [activeProfileId, setActiveProfileId] = useState('dad_chartist');
    const [workbookFile, setWorkbookFile] = useState(null);
    const [workbookPlan, setWorkbookPlan] = useState(null);
    const [workbookStatus, setWorkbookStatus] = useState('');
    const [workbookBusy, setWorkbookBusy] = useState(false);
    const [apiSetup, setApiSetup] = useState(DEFAULT_API_SETUP);
    const [apiSaved, setApiSaved] = useState(false);
    const [heartbeatAt, setHeartbeatAt] = useState(() => new Date());
    const [chatInput, setChatInput] = useState('');
    const [chatMessages, setChatMessages] = useState(INITIAL_CHAT_MESSAGES);

    const load = async () => {
        setLoading(true);
        setError('');
        const result = await api.getTenYearPortfolio({ capital, years: 10 });
        if (result?.error || result?.status === 'error') {
            setError(result?.message || result?.error || 'Portfolio query failed');
        } else {
            setData(result);
            if (!result.profiles?.some(profile => profile.id === activeProfileId)) {
                setActiveProfileId(result.profiles?.[0]?.id || 'dad_chartist');
            }
        }
        setLoading(false);
    };

    useEffect(() => {
        load();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        if (typeof localStorage === 'undefined') return;
        try {
            const saved = localStorage.getItem(API_SETUP_KEY);
            if (saved) {
                setApiSetup({ ...DEFAULT_API_SETUP, ...JSON.parse(saved) });
            }
        } catch (_) {
            setApiSetup(DEFAULT_API_SETUP);
        }
    }, []);

    useEffect(() => {
        const timer = window.setInterval(() => setHeartbeatAt(new Date()), 10000);
        return () => window.clearInterval(timer);
    }, []);

    const activeProfile = useMemo(() => (
        data?.profiles?.find(profile => profile.id === activeProfileId) || data?.profiles?.[0]
    ), [data, activeProfileId]);
    const topPick = activeProfile?.allocations?.[0];
    const frontierBoard = data?.candidate_boards?.find(board => board.id === 'frontier_infrastructure')
        || data?.candidate_boards?.[0];
    const monteCarlo = activeProfile?.monte_carlo;
    const methodSignals = workbookPlan?.workbook_summary?.method_signals || [];
    const planSteps = workbookPlan?.master_plan?.steps?.length
        ? workbookPlan.master_plan.steps
        : DEFAULT_PLAN_STEPS;

    const handleWorkbookAnalyze = async () => {
        if (!workbookFile) {
            setWorkbookStatus('Choose an Excel or CSV workbook first.');
            return;
        }
        setWorkbookBusy(true);
        setWorkbookStatus('Scanning workbook in memory...');
        const result = await api.uploadTenYearWorkbook(workbookFile, { capital, years: 10 });
        if (result?.error || result?.status === 'error') {
            setWorkbookStatus(result?.message || result?.error || 'Workbook analysis failed.');
        } else {
            setWorkbookPlan(result);
            setWorkbookStatus('Sanitized Dad Method plan is ready.');
        }
        setWorkbookBusy(false);
    };

    const handleModelExport = async () => {
        setWorkbookStatus('Building current GRID model export...');
        try {
            triggerDownload(await api.exportTenYearModel({ capital, years: 10 }));
            setWorkbookStatus('Current GRID model export downloaded.');
        } catch (exc) {
            setWorkbookStatus(exc.message || 'Export failed.');
        }
    };

    const handleWorkbookExport = async () => {
        if (!workbookFile) {
            setWorkbookStatus('Choose a workbook before exporting the private plan.');
            return;
        }
        setWorkbookBusy(true);
        setWorkbookStatus('Building sanitized workbook export...');
        try {
            triggerDownload(await api.exportTenYearWorkbook(workbookFile, { capital, years: 10 }));
            setWorkbookStatus('Sanitized workbook plan export downloaded.');
        } catch (exc) {
            setWorkbookStatus(exc.message || 'Workbook export failed.');
        }
        setWorkbookBusy(false);
    };

    const handleApiSave = () => {
        if (typeof localStorage !== 'undefined') {
            localStorage.setItem(API_SETUP_KEY, JSON.stringify(apiSetup));
        }
        setApiSaved(true);
        window.setTimeout(() => setApiSaved(false), 1800);
    };

    const handleChatSubmit = event => {
        event.preventDefault();
        const text = chatInput.trim();
        if (!text) return;
        const reply = plannerReply(text, workbookPlan, activeProfile);
        setChatMessages(messages => [
            ...messages.slice(-5),
            { role: 'user', text },
            { role: 'assistant', text: reply },
        ]);
        setChatInput('');
        setHeartbeatAt(new Date());
    };

    return (
        <div className="ty-page">
            <style>{CSS}</style>
            <header className="ty-header">
                <div>
                    <div className="ty-eyebrow">GRID weekly query</div>
                    <h1>10-Year Compounder</h1>
                    <p>
                        $1M model portfolio built from long-term up-and-right charts,
                        weekly rank checks, and Nasdaq-relative strength.
                    </p>
                </div>
                <div className="ty-controls">
                    <label>
                        <span>Capital</span>
                        <input
                            type="number"
                            min="10000"
                            step="50000"
                            value={capital}
                            onChange={event => setCapital(Number(event.target.value || 0))}
                        />
                    </label>
                    <button onClick={load} disabled={loading}>
                        <RefreshCw size={16} />
                        {loading ? 'Running' : 'Run weekly'}
                    </button>
                </div>
            </header>

            {error && <div className="ty-error">{error}</div>}

            <section className="ty-profile-strip">
                {(data?.profiles || []).map(profile => {
                    const Icon = PROFILE_ICONS[profile.id] || TrendingUp;
                    return (
                        <button
                            key={profile.id}
                            className={profile.id === activeProfileId ? 'active' : ''}
                            onClick={() => setActiveProfileId(profile.id)}
                        >
                            <Icon size={17} />
                            <span>{profile.label}</span>
                        </button>
                    );
                })}
                {!data && [0, 1, 2, 3].map(item => <div key={item} className="ty-skel" />)}
            </section>

            <section className="ty-oracle">
                <div className="ty-section-head ty-oracle-head">
                    <div>
                        <div className="ty-eyebrow">Dad Method Oracle</div>
                        <h2>Plan room for the $1M 10-year mandate</h2>
                    </div>
                    <div className="ty-heartbeat">
                        <HeartPulse size={15} />
                        <span>Heartbeat {formatTime(heartbeatAt)}</span>
                    </div>
                </div>

                <div className="ty-oracle-grid">
                    <div className="ty-oracle-card ty-prompt-card">
                        <div className="ty-card-title">
                            <Sparkles size={17} />
                            <strong>Preloaded prompt</strong>
                        </div>
                        <textarea
                            readOnly
                            value={PRELOADED_PROMPT.replace('$1,000,000', money(capital))}
                            aria-label="Preloaded 10-year portfolio prompt"
                        />
                        <div className="ty-action-row">
                            <button onClick={handleModelExport}>
                                <Download size={16} />
                                Export model
                            </button>
                            <label className="ty-upload-button">
                                <Upload size={16} />
                                <span>{workbookFile?.name || 'Choose workbook'}</span>
                                <input
                                    type="file"
                                    accept=".xlsx,.xlsm,.csv"
                                    onChange={event => {
                                        setWorkbookFile(event.target.files?.[0] || null);
                                        setWorkbookPlan(null);
                                        setWorkbookStatus('');
                                    }}
                                />
                            </label>
                            <button onClick={handleWorkbookAnalyze} disabled={workbookBusy || !workbookFile}>
                                <FileSpreadsheet size={16} />
                                Analyze
                            </button>
                            <button onClick={handleWorkbookExport} disabled={workbookBusy || !workbookFile}>
                                <Download size={16} />
                                Export plan
                            </button>
                        </div>
                        {workbookStatus && <div className="ty-workbook-status">{workbookStatus}</div>}
                    </div>

                    <div className="ty-oracle-card ty-monte-card">
                        <div className="ty-card-title">
                            <BarChart3 size={17} />
                            <strong>Monte Carlo</strong>
                        </div>
                        <div className="ty-monte-grid">
                            <div>
                                <span>P10</span>
                                <strong>{money(monteCarlo?.p10)}</strong>
                            </div>
                            <div>
                                <span>P50</span>
                                <strong>{money(monteCarlo?.p50)}</strong>
                            </div>
                            <div>
                                <span>P90</span>
                                <strong>{money(monteCarlo?.p90)}</strong>
                            </div>
                        </div>
                        <div className="ty-probability">
                            <div>
                                <span>Above start</span>
                                <strong>{pct(monteCarlo?.probability_above_start, 0)}</strong>
                            </div>
                            <div className="ty-probability-rail">
                                <i style={{ width: `${Math.max(4, Math.min(100, Number(monteCarlo?.probability_above_start || 0) * 100))}%` }} />
                            </div>
                        </div>
                        <p>
                            {pct(monteCarlo?.expected_annual_return)} expected return proxy,
                            {' '}{pct(monteCarlo?.annual_volatility)} annual volatility.
                        </p>
                    </div>

                    <div className="ty-oracle-card ty-plan-card">
                        <div className="ty-card-title">
                            <CheckCircle2 size={17} />
                            <strong>{workbookPlan ? 'Sanitized master plan' : 'Ready checklist'}</strong>
                        </div>
                        {methodSignals.length > 0 && (
                            <div className="ty-method-tags">
                                {methodSignals.slice(0, 8).map(item => (
                                    <span key={item.id || item.label}>{item.label}</span>
                                ))}
                            </div>
                        )}
                        <div className="ty-plan-steps">
                            {planSteps.slice(0, 5).map(item => (
                                <div key={item.step}>
                                    <strong>{item.step}</strong>
                                    <span>{item.action}</span>
                                </div>
                            ))}
                        </div>
                    </div>

                    <div className="ty-oracle-card ty-api-card">
                        <div className="ty-card-title">
                            <KeyRound size={17} />
                            <strong>API setup</strong>
                        </div>
                        <label>
                            <span>Schwab / thinkorswim app key</span>
                            <input
                                value={apiSetup.schwabAppKey}
                                onChange={event => setApiSetup({ ...apiSetup, schwabAppKey: event.target.value })}
                                placeholder="paste key when ready"
                            />
                        </label>
                        <label>
                            <span>OAuth callback</span>
                            <input
                                value={apiSetup.callbackUrl}
                                onChange={event => setApiSetup({ ...apiSetup, callbackUrl: event.target.value })}
                            />
                        </label>
                        <label>
                            <span>Data mode</span>
                            <select
                                value={apiSetup.dataMode}
                                onChange={event => setApiSetup({ ...apiSetup, dataMode: event.target.value })}
                            >
                                <option value="private-local-import">Private local import</option>
                                <option value="paper-model-only">Paper model only</option>
                                <option value="manual-csv-review">Manual CSV review</option>
                            </select>
                        </label>
                        <button onClick={handleApiSave}>
                            <KeyRound size={16} />
                            {apiSaved ? 'Saved' : 'Save setup'}
                        </button>
                    </div>

                    <div className="ty-oracle-card ty-chat-card">
                        <div className="ty-card-title">
                            <MessageSquare size={17} />
                            <strong>Planner chat</strong>
                        </div>
                        <div className="ty-chat-log">
                            {chatMessages.map((message, index) => (
                                <div key={`${message.role}-${index}`} className={`ty-chat-message ${message.role}`}>
                                    {message.role === 'assistant' && <Bot size={15} />}
                                    <span>{message.text}</span>
                                </div>
                            ))}
                        </div>
                        <form onSubmit={handleChatSubmit} className="ty-chat-form">
                            <input
                                value={chatInput}
                                onChange={event => setChatInput(event.target.value)}
                                placeholder="Ask for a screen, export, rule, or risk check"
                            />
                            <button type="submit" aria-label="Send planner message">
                                <Send size={16} />
                            </button>
                        </form>
                    </div>
                </div>
            </section>

            <main className="ty-layout">
                <section className="ty-main">
                    <MetricStrip profile={activeProfile} />
                    <SparkChart pick={topPick} benchmark={data?.benchmark} />
                    <div className="ty-policy">
                        <div>
                            <DollarSign size={18} />
                            <strong>{activeProfile?.label || 'Profile'}</strong>
                        </div>
                        <p>{activeProfile?.description || 'Waiting for the weekly portfolio query.'}</p>
                        <p>{activeProfile?.weekly_policy?.exit_rule || ''}</p>
                    </div>
                </section>

                <aside className="ty-side">
                    <div className="ty-side-block">
                        <span>As of</span>
                        <strong>{data?.as_of || 'loading'}</strong>
                    </div>
                    <div className="ty-side-block">
                        <span>Benchmark</span>
                        <strong>QQQ {pct(data?.benchmark?.cagr)} CAGR</strong>
                    </div>
                    <div className="ty-side-block">
                        <span>Universe</span>
                        <strong>{data?.universe?.ranked_candidates ?? 0} ranked</strong>
                    </div>
                    <div className="ty-note">
                        Research screen, not financial advice. The first version uses GRID Yahoo price history and chart-quality rules; fundamentals can be added next.
                    </div>
                </aside>
            </main>

            <section className="ty-picks-section">
                <div className="ty-section-head">
                    <div>
                        <div className="ty-eyebrow">Weekly buy or hold list</div>
                        <h2>{activeProfile?.label || 'Profile'} allocation</h2>
                    </div>
                    <div className="ty-small-stat">
                        Hold buffer: rank {activeProfile ? activeProfile.top_n + activeProfile.hold_buffer : 'n/a'}
                    </div>
                </div>
                {loading && !data ? (
                    <div className="ty-loading">Running GRID chart query...</div>
                ) : (
                    <PickTable picks={activeProfile?.allocations || []} />
                )}
            </section>

            <section className="ty-picks-section">
                <div className="ty-section-head">
                    <div>
                        <div className="ty-eyebrow">Second candidate list</div>
                        <h2>{frontierBoard?.label || 'Frontier Infrastructure'}</h2>
                    </div>
                    <div className="ty-small-stat">
                        {frontierBoard?.universe?.ranked_candidates ?? 0} ranked from {frontierBoard?.universe?.requested_candidates ?? 0} candidates
                    </div>
                </div>
                <p className="ty-section-copy">
                    {frontierBoard?.description || 'AI, compute, uranium, metals, and supercomputer candidates are ranked separately from the core compounder allocation.'}
                </p>
                {loading && !data ? (
                    <div className="ty-loading">Running GRID thematic screen...</div>
                ) : (
                    <CandidateTable candidates={frontierBoard?.ranked || []} />
                )}
            </section>
        </div>
    );
}

const CSS = `
.ty-page {
    min-height: 100vh;
    background: #071014;
    color: #D6E3EA;
    font-family: ${SANS};
    padding: 24px;
}
.ty-header {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 24px;
    padding-bottom: 20px;
    border-bottom: 1px solid rgba(132, 154, 166, 0.22);
}
.ty-eyebrow {
    font-family: ${MONO};
    color: #8FB3B2;
    font-size: 11px;
    text-transform: uppercase;
    letter-spacing: 0;
    margin-bottom: 7px;
}
.ty-header h1 {
    margin: 0;
    font-size: 34px;
    line-height: 1.05;
    letter-spacing: 0;
    color: #F3F7F7;
}
.ty-header p {
    max-width: 690px;
    margin: 10px 0 0;
    color: #9FB4BF;
    font-size: 15px;
    line-height: 1.55;
}
.ty-controls {
    display: flex;
    align-items: flex-end;
    gap: 10px;
    flex-wrap: wrap;
    justify-content: flex-end;
}
.ty-controls label {
    display: grid;
    gap: 6px;
    color: #829AA5;
    font-size: 11px;
    font-family: ${MONO};
    text-transform: uppercase;
}
.ty-controls input {
    width: 152px;
    height: 38px;
    border: 1px solid rgba(132, 154, 166, 0.35);
    background: #0D1B20;
    color: #E4EFF3;
    border-radius: 6px;
    padding: 0 10px;
    font-family: ${MONO};
}
.ty-controls button,
.ty-profile-strip button {
    height: 38px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    border-radius: 6px;
    border: 1px solid rgba(92, 201, 167, 0.45);
    background: #11342F;
    color: #DDF5EC;
    font-weight: 700;
    cursor: pointer;
}
.ty-controls button {
    padding: 0 14px;
}
.ty-controls button:disabled {
    opacity: 0.55;
    cursor: default;
}
.ty-profile-strip {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    margin: 18px 0;
}
.ty-profile-strip button {
    min-width: 172px;
    padding: 0 12px;
    background: #0B171B;
    border-color: rgba(132, 154, 166, 0.28);
    color: #BFD2DA;
}
.ty-profile-strip button.active {
    background: #153F37;
    border-color: #5CC9A7;
    color: #FFFFFF;
}
.ty-skel {
    height: 38px;
    width: 172px;
    border-radius: 6px;
    background: linear-gradient(90deg, #0B171B, #13262C, #0B171B);
}
.ty-oracle {
    margin: 4px 0 18px;
}
.ty-oracle-head {
    margin-bottom: 12px;
}
.ty-heartbeat {
    display: inline-flex;
    align-items: center;
    gap: 7px;
    min-height: 30px;
    color: #E8C37E;
    font-family: ${MONO};
    font-size: 12px;
    white-space: nowrap;
}
.ty-oracle-grid {
    display: grid;
    grid-template-columns: minmax(0, 1.3fr) minmax(280px, 0.7fr);
    gap: 14px;
}
.ty-oracle-card {
    padding: 15px;
    min-width: 0;
}
.ty-prompt-card,
.ty-plan-card,
.ty-chat-card {
    grid-column: span 1;
}
.ty-chat-card {
    grid-row: span 2;
}
.ty-card-title {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #F3F7F7;
    margin-bottom: 11px;
}
.ty-card-title svg {
    color: #E8C37E;
}
.ty-prompt-card textarea {
    width: 100%;
    min-height: 124px;
    resize: vertical;
    border: 1px solid rgba(132, 154, 166, 0.25);
    background: #071014;
    color: #D6E3EA;
    border-radius: 6px;
    padding: 12px;
    line-height: 1.45;
    font-family: ${SANS};
    font-size: 14px;
}
.ty-action-row {
    display: flex;
    flex-wrap: wrap;
    gap: 8px;
    margin-top: 11px;
}
.ty-action-row button,
.ty-upload-button,
.ty-api-card button,
.ty-chat-form button {
    min-height: 36px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    border: 1px solid rgba(92, 201, 167, 0.42);
    background: #11342F;
    color: #DDF5EC;
    border-radius: 6px;
    padding: 0 11px;
    font-weight: 800;
    cursor: pointer;
}
.ty-action-row button:disabled {
    opacity: 0.45;
    cursor: default;
}
.ty-upload-button {
    max-width: 260px;
    border-color: rgba(232, 195, 126, 0.45);
    background: #2C2417;
    color: #F7E5BF;
    overflow: hidden;
}
.ty-upload-button span {
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
}
.ty-upload-button input {
    display: none;
}
.ty-workbook-status {
    margin-top: 10px;
    color: #BFD2DA;
    font-family: ${MONO};
    font-size: 12px;
}
.ty-monte-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px;
}
.ty-monte-grid > div,
.ty-probability,
.ty-plan-steps > div {
    border: 1px solid rgba(132, 154, 166, 0.18);
    background: #081317;
    border-radius: 6px;
}
.ty-monte-grid > div {
    padding: 10px;
    display: grid;
    gap: 5px;
}
.ty-monte-grid span,
.ty-probability span,
.ty-api-card label span {
    color: #7E98A4;
    font-family: ${MONO};
    font-size: 11px;
    text-transform: uppercase;
}
.ty-monte-grid strong,
.ty-probability strong {
    color: #F3F7F7;
    font-size: 16px;
    overflow-wrap: anywhere;
}
.ty-probability {
    margin-top: 9px;
    padding: 10px;
    display: grid;
    gap: 8px;
}
.ty-probability > div:first-child {
    display: flex;
    justify-content: space-between;
    gap: 10px;
}
.ty-probability-rail {
    height: 8px;
    background: #16242A;
    border-radius: 999px;
    overflow: hidden;
}
.ty-probability-rail i {
    display: block;
    height: 100%;
    background: linear-gradient(90deg, #E8C37E, #5CC9A7);
    border-radius: 999px;
}
.ty-monte-card p {
    margin: 10px 0 0;
    color: #9FB4BF;
    font-size: 13px;
    line-height: 1.45;
}
.ty-method-tags {
    display: flex;
    flex-wrap: wrap;
    gap: 6px;
    margin-bottom: 10px;
}
.ty-method-tags span {
    border: 1px solid rgba(167, 139, 250, 0.34);
    color: #DCD4FF;
    background: #17152A;
    border-radius: 999px;
    padding: 4px 8px;
    font-family: ${MONO};
    font-size: 11px;
}
.ty-plan-steps {
    display: grid;
    gap: 7px;
}
.ty-plan-steps > div {
    padding: 10px;
    display: grid;
    gap: 4px;
}
.ty-plan-steps strong {
    color: #F3F7F7;
    font-size: 13px;
}
.ty-plan-steps span {
    color: #9FB4BF;
    font-size: 12px;
    line-height: 1.4;
}
.ty-api-card {
    display: grid;
    gap: 10px;
}
.ty-api-card .ty-card-title {
    margin-bottom: 0;
}
.ty-api-card label {
    display: grid;
    gap: 5px;
}
.ty-api-card input,
.ty-api-card select,
.ty-chat-form input {
    min-height: 36px;
    border: 1px solid rgba(132, 154, 166, 0.28);
    background: #071014;
    color: #E4EFF3;
    border-radius: 6px;
    padding: 0 10px;
    min-width: 0;
}
.ty-chat-card {
    display: grid;
    grid-template-rows: auto minmax(170px, 1fr) auto;
}
.ty-chat-log {
    display: grid;
    gap: 8px;
    align-content: start;
    max-height: 318px;
    overflow-y: auto;
    padding-right: 2px;
}
.ty-chat-message {
    display: flex;
    gap: 8px;
    align-items: flex-start;
    border-radius: 8px;
    padding: 10px;
    color: #D6E3EA;
    line-height: 1.42;
    font-size: 13px;
}
.ty-chat-message.assistant {
    background: #0E2227;
    border: 1px solid rgba(92, 201, 167, 0.18);
}
.ty-chat-message.user {
    background: #1E2430;
    border: 1px solid rgba(167, 139, 250, 0.20);
}
.ty-chat-message svg {
    flex: 0 0 auto;
    margin-top: 2px;
    color: #5CC9A7;
}
.ty-chat-form {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 42px;
    gap: 8px;
    margin-top: 10px;
}
.ty-chat-form button {
    padding: 0;
}
.ty-layout {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 280px;
    gap: 18px;
    align-items: start;
}
.ty-main {
    display: grid;
    gap: 14px;
}
.ty-metrics {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
}
.ty-metrics > div,
.ty-chart-panel,
.ty-policy,
.ty-side,
.ty-oracle-card,
.ty-picks-section {
    border: 1px solid rgba(132, 154, 166, 0.22);
    background: #0B171B;
    border-radius: 8px;
}
.ty-metrics > div {
    padding: 13px 14px;
    display: grid;
    gap: 5px;
    min-width: 0;
}
.ty-metrics span,
.ty-side-block span {
    font-family: ${MONO};
    color: #7E98A4;
    font-size: 11px;
    text-transform: uppercase;
}
.ty-metrics strong,
.ty-side-block strong {
    color: #F3F7F7;
    font-size: 18px;
    overflow-wrap: anywhere;
}
.ty-chart-panel {
    padding: 16px;
}
.ty-chart-head {
    display: flex;
    justify-content: space-between;
    align-items: flex-start;
    gap: 16px;
}
.ty-chart-title {
    font-size: 22px;
    font-weight: 800;
    color: #F3F7F7;
}
.ty-chart-stats {
    display: flex;
    gap: 8px;
    flex-wrap: wrap;
    justify-content: flex-end;
    font-family: ${MONO};
    color: #9FB4BF;
    font-size: 12px;
}
.ty-chart {
    width: 100%;
    height: 260px;
    margin-top: 10px;
    overflow: visible;
}
.ty-grid-line {
    stroke: rgba(132, 154, 166, 0.16);
    stroke-width: 1;
}
.ty-bench-line,
.ty-pick-line {
    fill: none;
    stroke-linecap: round;
    stroke-linejoin: round;
}
.ty-bench-line {
    stroke: #8AA0B8;
    stroke-width: 2;
    opacity: 0.78;
}
.ty-pick-line {
    stroke: #5CC9A7;
    stroke-width: 3;
}
.ty-legend {
    display: flex;
    gap: 14px;
    font-family: ${MONO};
    color: #AFC2CA;
    font-size: 12px;
}
.ty-dot {
    display: inline-block;
    width: 8px;
    height: 8px;
    border-radius: 999px;
    margin-right: 6px;
}
.ty-dot-pick { background: #5CC9A7; }
.ty-dot-bench { background: #8AA0B8; }
.ty-policy {
    padding: 15px;
}
.ty-policy > div {
    display: flex;
    align-items: center;
    gap: 8px;
    color: #F3F7F7;
    margin-bottom: 7px;
}
.ty-policy p {
    margin: 6px 0 0;
    color: #9FB4BF;
    line-height: 1.5;
    font-size: 14px;
}
.ty-side {
    padding: 15px;
    display: grid;
    gap: 13px;
}
.ty-side-block {
    display: grid;
    gap: 5px;
    border-bottom: 1px solid rgba(132, 154, 166, 0.16);
    padding-bottom: 12px;
}
.ty-side-block strong {
    font-size: 15px;
}
.ty-note {
    color: #829AA5;
    font-size: 12px;
    line-height: 1.45;
}
.ty-picks-section {
    margin-top: 18px;
    padding: 16px;
}
.ty-section-head {
    display: flex;
    justify-content: space-between;
    gap: 14px;
    align-items: flex-start;
    margin-bottom: 14px;
}
.ty-section-head h2 {
    margin: 0;
    color: #F3F7F7;
    font-size: 22px;
    letter-spacing: 0;
}
.ty-small-stat {
    font-family: ${MONO};
    color: #9FB4BF;
    font-size: 12px;
    white-space: nowrap;
}
.ty-table-wrap {
    overflow-x: auto;
}
.ty-table {
    width: 100%;
    min-width: 860px;
    border-collapse: collapse;
    font-size: 13px;
}
.ty-frontier-table {
    min-width: 900px;
}
.ty-table th {
    text-align: left;
    color: #7E98A4;
    font-family: ${MONO};
    font-weight: 700;
    text-transform: uppercase;
    font-size: 11px;
    padding: 10px 8px;
    border-bottom: 1px solid rgba(132, 154, 166, 0.20);
}
.ty-table td {
    padding: 11px 8px;
    color: #D6E3EA;
    border-bottom: 1px solid rgba(132, 154, 166, 0.10);
    font-family: ${MONO};
}
.ty-ticker-cell {
    display: flex;
    align-items: center;
    gap: 9px;
}
.ty-ticker-cell span {
    width: 24px;
    height: 24px;
    border-radius: 999px;
    display: inline-flex;
    align-items: center;
    justify-content: center;
    background: #11272D;
    color: #8FB3B2;
    font-size: 11px;
}
.ty-ticker-cell strong {
    color: #F3F7F7;
    font-family: ${MONO};
}
.ty-good { color: #5CC9A7 !important; }
.ty-bad { color: #E08B6D !important; }
.ty-muted {
    color: #7E98A4;
}
.ty-tags {
    display: flex;
    gap: 5px;
    flex-wrap: wrap;
}
.ty-tags span {
    border: 1px solid rgba(143, 179, 178, 0.25);
    background: #0E2227;
    color: #BFD2DA;
    border-radius: 999px;
    padding: 3px 7px;
    font-family: ${MONO};
    font-size: 11px;
    white-space: nowrap;
}
.ty-section-copy {
    color: #9FB4BF;
    line-height: 1.5;
    margin: -4px 0 14px;
    font-size: 14px;
}
.ty-error,
.ty-loading {
    border: 1px solid rgba(224, 139, 109, 0.35);
    background: rgba(224, 139, 109, 0.10);
    color: #FFD6C8;
    border-radius: 8px;
    padding: 12px 14px;
    margin: 14px 0;
}
.ty-loading {
    border-color: rgba(132, 154, 166, 0.24);
    background: #091317;
    color: #9FB4BF;
}
@media (max-width: 900px) {
    .ty-page { padding: 16px; }
    .ty-header,
    .ty-chart-head,
    .ty-section-head {
        flex-direction: column;
        align-items: stretch;
    }
    .ty-controls { justify-content: flex-start; }
    .ty-oracle-grid { grid-template-columns: 1fr; }
    .ty-chat-card { grid-row: auto; }
    .ty-layout { grid-template-columns: 1fr; }
    .ty-metrics { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .ty-chart { height: 210px; }
}
@media (max-width: 520px) {
    .ty-header h1 { font-size: 28px; }
    .ty-profile-strip button { width: 100%; }
    .ty-metrics { grid-template-columns: 1fr; }
    .ty-controls input { width: 138px; }
    .ty-action-row button,
    .ty-upload-button {
        width: 100%;
        max-width: none;
    }
    .ty-monte-grid { grid-template-columns: 1fr; }
}
`;
