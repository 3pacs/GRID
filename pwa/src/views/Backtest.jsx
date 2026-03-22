import React, { useState, useEffect, useRef } from 'react';
import { api } from '../api.js';
import { shared, colors } from '../styles/shared.js';

const REGIME_COLORS = {
    GROWTH: '#22C55E', NEUTRAL: '#F59E0B', FRAGILE: '#F97316', CRISIS: '#EF4444',
};

function MiniEquityChart({ data }) {
    const canvasRef = useRef(null);

    useEffect(() => {
        if (!canvasRef.current || !data?.dates?.length) return;
        const canvas = canvasRef.current;
        const ctx = canvas.getContext('2d');
        const w = canvas.width = canvas.offsetWidth * 2;
        const h = canvas.height = canvas.offsetHeight * 2;
        ctx.scale(2, 2);
        const cw = w / 2, ch = h / 2;

        ctx.clearRect(0, 0, cw, ch);

        const drawLine = (values, color, lineWidth = 1.5) => {
            if (!values?.length) return;
            const min = Math.min(...values);
            const max = Math.max(...values);
            const range = max - min || 1;
            ctx.beginPath();
            ctx.strokeStyle = color;
            ctx.lineWidth = lineWidth;
            values.forEach((v, i) => {
                const x = (i / (values.length - 1)) * cw;
                const y = ch - ((v - min) / range) * (ch - 20) - 10;
                if (i === 0) ctx.moveTo(x, y); else ctx.lineTo(x, y);
            });
            ctx.stroke();
        };

        // Regime background bands
        if (data.regimes?.length) {
            const segW = cw / data.regimes.length;
            data.regimes.forEach((r, i) => {
                ctx.fillStyle = (REGIME_COLORS[r] || '#333') + '15';
                ctx.fillRect(i * segW, 0, segW + 1, ch);
            });
        }

        drawLine(data.spy, '#8B8B8B', 1);
        drawLine(data.sixty_forty, '#B8922A', 1);
        drawLine(data.grid, '#1A6EBF', 2);
    }, [data]);

    return (
        <canvas
            ref={canvasRef}
            style={{ width: '100%', height: '200px', borderRadius: '8px', background: colors.bg }}
        />
    );
}

function MetricCard({ label, value, sub, color }) {
    return (
        <div style={shared.metric}>
            <div style={{ ...shared.metricValue, fontSize: '16px', color: color || '#E8F0F8' }}>
                {value}
            </div>
            <div style={shared.metricLabel}>{label}</div>
            {sub && <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>{sub}</div>}
        </div>
    );
}

export default function Backtest() {
    const [activeTab, setActiveTab] = useState('results');
    const [summary, setSummary] = useState(null);
    const [fullResults, setFullResults] = useState(null);
    const [charts, setCharts] = useState(null);
    const [paperTrades, setPaperTrades] = useState([]);
    const [selectedSnapshot, setSelectedSnapshot] = useState(null);
    const [scored, setScored] = useState(null);
    const [running, setRunning] = useState(false);
    const [generatingCharts, setGeneratingCharts] = useState(false);
    const [creatingSnapshot, setCreatingSnapshot] = useState(false);
    const [error, setError] = useState(null);
    const [startDate, setStartDate] = useState('2015-01-01');
    const [capital, setCapital] = useState('100000');

    useEffect(() => {
        loadSummary();
        loadPaperTrades();
    }, []);

    const loadSummary = async () => {
        try {
            const s = await api.getBacktestSummaryPitch();
            setSummary(s);
            const r = await api.getBacktestResults();
            setFullResults(r);
        } catch {}
    };

    const loadPaperTrades = async () => {
        try {
            const r = await api.listPaperTrades();
            setPaperTrades(r.snapshots || []);
        } catch {}
    };

    const runBacktest = async () => {
        setRunning(true);
        setError(null);
        try {
            await api.runBacktest(startDate, parseFloat(capital));
            await loadSummary();
        } catch (e) {
            setError(e.message);
        }
        setRunning(false);
    };

    const genCharts = async () => {
        setGeneratingCharts(true);
        try {
            const r = await api.generateCharts();
            setCharts(r.charts || {});
        } catch (e) {
            setError(e.message);
        }
        setGeneratingCharts(false);
    };

    const createSnapshot = async () => {
        setCreatingSnapshot(true);
        try {
            const snap = await api.createPaperTrade();
            setSelectedSnapshot(snap);
            loadPaperTrades();
        } catch (e) {
            setError(e.message);
        }
        setCreatingSnapshot(false);
    };

    const scoreAll = async () => {
        try {
            const r = await api.scorePredictions();
            setScored(r.scored || []);
        } catch {}
    };

    const gm = summary?.grid || {};
    const spyM = summary?.spy || {};
    const sfM = summary?.sixty_forty || {};
    const ps = summary?.position_sizing || {};

    return (
        <div style={shared.container}>
            <div style={shared.header}>Backtest & Track Record</div>

            <div style={shared.tabs}>
                {['results', 'charts', 'paper-trades', 'sizing'].map(t => (
                    <button key={t} style={shared.tab(activeTab === t)} onClick={() => setActiveTab(t)}>
                        {t === 'paper-trades' ? 'Paper Trades' : t === 'sizing' ? 'Sizing' : t.charAt(0).toUpperCase() + t.slice(1)}
                    </button>
                ))}
            </div>

            {/* RESULTS TAB */}
            {activeTab === 'results' && (
                <>
                    {/* Run Controls */}
                    <div style={shared.card}>
                        <div style={{ display: 'flex', gap: '8px', alignItems: 'flex-end', flexWrap: 'wrap' }}>
                            <div>
                                <span style={shared.label}>Start Date</span>
                                <input style={{ ...shared.input, width: '130px' }}
                                    value={startDate} onChange={e => setStartDate(e.target.value)} />
                            </div>
                            <div>
                                <span style={shared.label}>Capital ($)</span>
                                <input style={{ ...shared.input, width: '100px' }}
                                    value={capital} onChange={e => setCapital(e.target.value)} />
                            </div>
                            <button
                                style={{ ...shared.button, ...(running ? shared.buttonDisabled : {}) }}
                                onClick={runBacktest} disabled={running}
                            >
                                {running ? 'Running Backtest...' : 'Run Pitch Backtest'}
                            </button>
                        </div>
                        {error && <div style={shared.error}>{error}</div>}
                    </div>

                    {/* Equity Chart */}
                    {fullResults?.equity_curve && (
                        <div style={shared.card}>
                            <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Equity Curve</div>
                            <MiniEquityChart data={{
                                ...fullResults.equity_curve,
                                regimes: fullResults.regime_timeline?.regimes,
                            }} />
                            <div style={{ display: 'flex', gap: '16px', marginTop: '8px', justifyContent: 'center' }}>
                                <span style={{ fontSize: '11px' }}><span style={{ color: '#1A6EBF' }}>■</span> GRID</span>
                                <span style={{ fontSize: '11px' }}><span style={{ color: '#8B8B8B' }}>■</span> SPY</span>
                                <span style={{ fontSize: '11px' }}><span style={{ color: '#B8922A' }}>■</span> 60/40</span>
                            </div>
                        </div>
                    )}

                    {/* Performance Metrics */}
                    {summary && (
                        <>
                            <div style={{ ...shared.sectionTitle }}>GRID Performance</div>
                            <div style={shared.metricGrid}>
                                <MetricCard label="Total Return" value={`${((gm.cumulative_return || 0) * 100).toFixed(1)}%`}
                                    color={gm.cumulative_return > 0 ? colors.green : colors.red} />
                                <MetricCard label="Annual Return" value={`${((gm.annualized_return || 0) * 100).toFixed(1)}%`} />
                                <MetricCard label="Sharpe" value={(gm.sharpe || 0).toFixed(2)}
                                    color={gm.sharpe > 1 ? colors.green : colors.textDim} />
                                <MetricCard label="Sortino" value={(gm.sortino || 0).toFixed(2)} />
                                <MetricCard label="Max DD" value={`${((gm.max_drawdown || 0) * 100).toFixed(1)}%`}
                                    color={colors.red} />
                                <MetricCard label="Calmar" value={(gm.calmar || 0).toFixed(2)} />
                                <MetricCard label="Final Value" value={`$${(gm.final_value || summary.grid?.final_value || 0).toLocaleString()}`} />
                            </div>

                            <div style={{ ...shared.sectionTitle }}>vs Benchmarks</div>
                            <div style={shared.card}>
                                <div style={shared.row}>
                                    <span style={{ fontSize: '13px', fontWeight: 600 }}>SPY (Buy & Hold)</span>
                                    <div style={{ display: 'flex', gap: '16px', fontSize: '12px', fontFamily: colors.mono }}>
                                        <span>Return: {((spyM.cumulative_return || 0) * 100).toFixed(1)}%</span>
                                        <span>Sharpe: {(spyM.sharpe || 0).toFixed(2)}</span>
                                        <span>Max DD: {((spyM.max_drawdown || 0) * 100).toFixed(1)}%</span>
                                    </div>
                                </div>
                                <div style={{ ...shared.row, borderBottom: 'none' }}>
                                    <span style={{ fontSize: '13px', fontWeight: 600 }}>60/40 Portfolio</span>
                                    <div style={{ display: 'flex', gap: '16px', fontSize: '12px', fontFamily: colors.mono }}>
                                        <span>Return: {((sfM.cumulative_return || 0) * 100).toFixed(1)}%</span>
                                        <span>Sharpe: {(sfM.sharpe || 0).toFixed(2)}</span>
                                        <span>Max DD: {((sfM.max_drawdown || 0) * 100).toFixed(1)}%</span>
                                    </div>
                                </div>
                            </div>

                            {/* Regime Stats */}
                            {summary.regime_stats && (
                                <>
                                    <div style={shared.sectionTitle}>Performance by Regime</div>
                                    <div style={shared.card}>
                                        {Object.entries(summary.regime_stats).map(([regime, stats]) => (
                                            <div key={regime} style={{ ...shared.row, flexWrap: 'wrap', gap: '8px' }}>
                                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px', minWidth: '120px' }}>
                                                    <span style={shared.badge(REGIME_COLORS[regime] || '#333')}>{regime}</span>
                                                    <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                                        {stats.days}d ({(stats.pct_of_total * 100).toFixed(0)}%)
                                                    </span>
                                                </div>
                                                <div style={{ display: 'flex', gap: '12px', fontSize: '11px', fontFamily: colors.mono, color: colors.textDim }}>
                                                    <span>Ann: {(stats.annualized_return * 100).toFixed(1)}%</span>
                                                    <span>Win: {(stats.win_rate * 100).toFixed(0)}%</span>
                                                    <span>Vol: {(stats.volatility * 100).toFixed(1)}%</span>
                                                </div>
                                            </div>
                                        ))}
                                    </div>
                                </>
                            )}
                        </>
                    )}

                    {!summary && !running && (
                        <div style={{ ...shared.card, textAlign: 'center', color: colors.textMuted, padding: '40px' }}>
                            No backtest results yet. Run one above.
                        </div>
                    )}
                </>
            )}

            {/* CHARTS TAB */}
            {activeTab === 'charts' && (
                <>
                    <div style={shared.card}>
                        <button
                            style={{ ...shared.button, ...(generatingCharts ? shared.buttonDisabled : {}) }}
                            onClick={genCharts} disabled={generatingCharts}
                        >
                            {generatingCharts ? 'Generating Charts...' : 'Generate Pitch Charts'}
                        </button>
                    </div>
                    {charts && Object.entries(charts).map(([name, path]) => (
                        <div key={name} style={shared.card}>
                            <div style={{ ...shared.sectionTitle, marginTop: 0, textTransform: 'capitalize' }}>
                                {name.replace(/_/g, ' ')}
                            </div>
                            <img
                                src={api.getChartUrl(name)}
                                alt={name}
                                style={{ width: '100%', borderRadius: '8px' }}
                                onError={(e) => { e.target.style.display = 'none'; }}
                            />
                        </div>
                    ))}
                    {!charts && (
                        <div style={{ ...shared.card, textAlign: 'center', color: colors.textMuted, padding: '40px' }}>
                            Run a backtest first, then generate charts.
                        </div>
                    )}
                </>
            )}

            {/* PAPER TRADES TAB */}
            {activeTab === 'paper-trades' && (
                <>
                    <div style={shared.card}>
                        <div style={{ display: 'flex', gap: '8px', flexWrap: 'wrap' }}>
                            <button
                                style={{ ...shared.button, ...(creatingSnapshot ? shared.buttonDisabled : {}) }}
                                onClick={createSnapshot} disabled={creatingSnapshot}
                            >
                                {creatingSnapshot ? 'Creating...' : 'Timestamp Regime Call'}
                            </button>
                            <button style={shared.buttonSmall} onClick={scoreAll}>
                                Score Predictions
                            </button>
                        </div>
                    </div>

                    {/* Active Snapshot */}
                    {selectedSnapshot && (
                        <div style={{ ...shared.card, borderColor: colors.accent }}>
                            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '12px' }}>
                                <span style={shared.badge(REGIME_COLORS[selectedSnapshot.regime] || '#333')}>
                                    {selectedSnapshot.regime}
                                </span>
                                <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                    {selectedSnapshot.timestamp}
                                </span>
                            </div>
                            <div style={shared.metricGrid}>
                                <MetricCard label="Confidence" value={`${(selectedSnapshot.confidence * 100).toFixed(0)}%`} />
                                <MetricCard label="Features" value={selectedSnapshot.features_in_model} />
                                <MetricCard label="Posture" value={selectedSnapshot.posture} />
                            </div>
                            <div style={{ ...shared.sectionTitle }}>Predictions</div>
                            {selectedSnapshot.predictions?.map((p, i) => (
                                <div key={i} style={{ ...shared.row }}>
                                    <div>
                                        <span style={{ fontSize: '13px', fontWeight: 600, color: colors.text }}>
                                            {p.asset}
                                        </span>
                                        <span style={{
                                            ...shared.badge(p.direction.includes('UP') ? '#1A7A4A' : p.direction.includes('DOWN') ? '#8B1F1F' : '#5A3A00'),
                                            marginLeft: '8px',
                                        }}>
                                            {p.direction}
                                        </span>
                                    </div>
                                    <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                        {p.horizon_days}d | {(p.confidence * 100).toFixed(0)}%
                                    </span>
                                </div>
                            ))}
                            <div style={{ ...shared.sectionTitle }}>Scoring Dates</div>
                            {selectedSnapshot.scoring_dates && Object.entries(selectedSnapshot.scoring_dates).map(([k, v]) => (
                                <div key={k} style={{ fontSize: '12px', color: colors.textDim, marginBottom: '4px' }}>
                                    {k}: {v}
                                </div>
                            ))}
                        </div>
                    )}

                    {/* Scored Predictions */}
                    {scored?.length > 0 && (
                        <>
                            <div style={shared.sectionTitle}>Scored Predictions</div>
                            <div style={shared.card}>
                                {scored.map((s, i) => (
                                    <div key={i} style={shared.row}>
                                        <div>
                                            <span style={{ fontSize: '12px', color: colors.textMuted }}>{s.snapshot_date}</span>
                                            <span style={{ fontSize: '13px', fontWeight: 600, color: colors.text, marginLeft: '8px' }}>
                                                {s.asset} {s.predicted_direction}
                                            </span>
                                        </div>
                                        <span style={{
                                            fontSize: '12px', fontFamily: colors.mono,
                                            color: s.actual_return > 0 ? colors.green : colors.red,
                                        }}>
                                            {s.status === 'SCORED' ? `${(s.actual_return * 100).toFixed(2)}%` : s.status}
                                        </span>
                                    </div>
                                ))}
                            </div>
                        </>
                    )}

                    {/* Paper Trade Archive */}
                    <div style={shared.sectionTitle}>Archive ({paperTrades.length})</div>
                    <div style={shared.card}>
                        {paperTrades.length === 0 ? (
                            <div style={{ color: colors.textMuted, fontSize: '13px', textAlign: 'center', padding: '20px' }}>
                                No paper trades yet. Create your first regime call above.
                            </div>
                        ) : (
                            paperTrades.map((pt, i) => (
                                <div key={i} style={{ ...shared.row, cursor: 'pointer' }}
                                    onClick={async () => {
                                        const snap = await api.getPaperTrade(pt.filename);
                                        setSelectedSnapshot(snap);
                                    }}>
                                    <div>
                                        <span style={shared.badge(REGIME_COLORS[pt.regime] || '#333')}>
                                            {pt.regime}
                                        </span>
                                        <span style={{ fontSize: '12px', color: colors.textMuted, marginLeft: '8px' }}>
                                            {(pt.confidence * 100).toFixed(0)}% conf
                                        </span>
                                    </div>
                                    <span style={{ fontSize: '11px', color: colors.textMuted }}>
                                        {pt.n_predictions} predictions
                                    </span>
                                </div>
                            ))
                        )}
                    </div>
                </>
            )}

            {/* POSITION SIZING TAB */}
            {activeTab === 'sizing' && (
                <>
                    <div style={shared.card}>
                        <div style={{ ...shared.sectionTitle, marginTop: 0 }}>Kelly Criterion Model</div>
                        <div style={{ fontSize: '12px', color: colors.textDim, lineHeight: '1.6', marginBottom: '12px' }}>
                            f* = (p × b - q) / b, then Half-Kelly: f = f* / 2
                        </div>
                        {ps.half_kelly_fraction != null ? (
                            <div style={shared.metricGrid}>
                                <MetricCard label="Half-Kelly" value={`${(ps.half_kelly_fraction * 100).toFixed(1)}%`}
                                    color={colors.accent} />
                                <MetricCard label="Win Rate" value={`${(ps.win_rate * 100).toFixed(1)}%`} />
                                <MetricCard label="W/L Ratio" value={ps.win_loss_ratio?.toFixed(2) || '—'} />
                                <MetricCard label="Avg Win" value={`${(ps.avg_win * 10000).toFixed(1)}bps`} />
                                <MetricCard label="Avg Loss" value={`${(ps.avg_loss * 10000).toFixed(1)}bps`} />
                            </div>
                        ) : (
                            <div style={{ color: colors.textMuted, fontSize: '13px' }}>
                                Run a backtest first to compute sizing parameters.
                            </div>
                        )}
                    </div>

                    {ps.regime_adjusted_sizes && (
                        <div style={shared.card}>
                            <div style={shared.sectionTitle}>Regime-Adjusted Position Sizes</div>
                            {Object.entries(ps.regime_adjusted_sizes).map(([regime, size]) => (
                                <div key={regime} style={shared.row}>
                                    <span style={shared.badge(REGIME_COLORS[regime] || '#333')}>{regime}</span>
                                    <span style={{ fontSize: '14px', fontWeight: 600, color: colors.text, fontFamily: colors.mono }}>
                                        {(size * 100).toFixed(1)}%
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}

                    <div style={shared.card}>
                        <div style={shared.sectionTitle}>Portfolio Constraints</div>
                        {[
                            ['Max Single Position', '10% of portfolio'],
                            ['Max Sector Exposure', '25% of portfolio'],
                            ['Max Correlation Cluster', '30% of portfolio'],
                            ['Max Regime Leverage', '1.0× GROWTH, 0.5× CRISIS'],
                            ['Daily VaR Limit', '2% (99th percentile)'],
                            ['Max Drawdown Trigger', '15% → force CAPITAL_PRESERVATION'],
                        ].map(([label, value], i) => (
                            <div key={i} style={shared.row}>
                                <span style={{ fontSize: '13px', color: colors.text }}>{label}</span>
                                <span style={{ fontSize: '12px', color: colors.textDim, fontFamily: colors.mono }}>{value}</span>
                            </div>
                        ))}
                    </div>

                    <div style={shared.card}>
                        <div style={shared.sectionTitle}>Posture Allocations</div>
                        {['AGGRESSIVE', 'BALANCED', 'DEFENSIVE', 'CAPITAL_PRESERVATION'].map(posture => {
                            const regime = Object.entries({
                                GROWTH: 'AGGRESSIVE', NEUTRAL: 'BALANCED',
                                FRAGILE: 'DEFENSIVE', CRISIS: 'CAPITAL_PRESERVATION',
                            }).find(([, v]) => v === posture)?.[0] || '';
                            return (
                                <div key={posture} style={{ marginBottom: '12px' }}>
                                    <div style={{ display: 'flex', alignItems: 'center', gap: '8px', marginBottom: '6px' }}>
                                        <span style={shared.badge(REGIME_COLORS[regime] || '#333')}>{regime}</span>
                                        <span style={{ fontSize: '12px', color: colors.textMuted }}>→ {posture}</span>
                                    </div>
                                </div>
                            );
                        })}
                    </div>
                </>
            )}
        </div>
    );
}
