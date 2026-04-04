import React, { useState } from 'react';
import { api } from '../api.js';
import { colors } from '../styles/shared.js';

const SECTOR_LIST = [
    'Technology', 'Financials', 'Energy', 'Healthcare',
    'Consumer Discretionary', 'Consumer Staples', 'Industrials',
    'Materials', 'Real Estate', 'Utilities', 'Communication Services',
];

const SIGNAL_COLORS = {
    STRONG_INFLOW: '#22C55E',
    INFLOW: '#4ADE80',
    NEUTRAL: '#5A7080',
    OUTFLOW: '#F87171',
    STRONG_OUTFLOW: '#EF4444',
};

const SENTIMENT_COLORS = {
    bullish: '#22C55E',
    neutral: '#F59E0B',
    bearish: '#EF4444',
};

/**
 * CapitalFlowAnalysis — deep research panel triggered on demand.
 * Shows sector rotation heatmap, relative strength, monetary backdrop,
 * and LLM narrative synthesis.
 */
const TIMEFRAMES = ['Current', '1W', '1M', '3M', '6M', '1Y'];

function CompareView({ dataA, dataB, tfA, tfB }) {
    if (!dataA?.relative_strength || !dataB?.relative_strength) return null;
    const sectors = [...new Set([...Object.keys(dataA.relative_strength), ...Object.keys(dataB.relative_strength)])];
    return (
        <div style={{ marginBottom: '16px' }}>
            <div style={sectionHeader}>COMPARE: {tfA} vs {tfB}</div>
            <div style={{ overflowX: 'auto' }}>
                <table style={{ width: '100%', fontSize: '11px', borderCollapse: 'collapse' }}>
                    <thead>
                        <tr>
                            <th style={thStyle}>Sector</th>
                            <th style={thStyle}>{tfA} Signal</th>
                            <th style={thStyle}>{tfA} 1m vs SPY</th>
                            <th style={thStyle}>{tfB} Signal</th>
                            <th style={thStyle}>{tfB} 1m vs SPY</th>
                            <th style={thStyle}>Shift</th>
                        </tr>
                    </thead>
                    <tbody>
                        {sectors.map(s => {
                            const a = dataA.relative_strength[s] || {};
                            const b = dataB.relative_strength[s] || {};
                            const aVal = a.vs_spy?.['1m'] || 0;
                            const bVal = b.vs_spy?.['1m'] || 0;
                            const shift = aVal - bVal;
                            const shiftColor = shift > 2 ? '#22C55E' : shift < -2 ? '#EF4444' : colors.textMuted;
                            return (
                                <tr key={s}>
                                    <td style={tdStyle}>{s}</td>
                                    <td style={{ ...tdStyle, color: SIGNAL_COLORS[a.signal] || colors.textMuted, fontWeight: 600 }}>
                                        {(a.signal || 'N/A').replace('_', ' ')}
                                    </td>
                                    <td style={tdNumStyle(aVal)}>{fmtPct(aVal)}</td>
                                    <td style={{ ...tdStyle, color: SIGNAL_COLORS[b.signal] || colors.textMuted, fontWeight: 600 }}>
                                        {(b.signal || 'N/A').replace('_', ' ')}
                                    </td>
                                    <td style={tdNumStyle(bVal)}>{fmtPct(bVal)}</td>
                                    <td style={{ ...tdStyle, color: shiftColor, fontWeight: 700 }}>
                                        {shift > 0 ? '+' : ''}{shift.toFixed(1)}%
                                    </td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </div>
    );
}

function FlowAttribution({ data }) {
    if (!data?.relative_strength) return null;
    const entries = Object.entries(data.relative_strength)
        .filter(([, rs]) => rs.top_actors?.length > 0)
        .sort((a, b) => Math.abs(b[1].vs_spy?.['1m'] || 0) - Math.abs(a[1].vs_spy?.['1m'] || 0));
    if (!entries.length) return null;
    return (
        <div style={{ marginBottom: '16px' }}>
            <div style={sectionHeader}>FLOW ATTRIBUTION — WHO'S DRIVING</div>
            <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
                {entries.slice(0, 6).map(([sector, rs]) => (
                    <div key={sector} style={{
                        background: colors.bg, borderRadius: '8px', padding: '10px 12px',
                        border: `1px solid ${colors.border}`,
                    }}>
                        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '6px' }}>
                            <span style={{ fontSize: '12px', fontWeight: 600, color: colors.text }}>{sector}</span>
                            <span style={{
                                fontSize: '10px', fontWeight: 700,
                                color: SIGNAL_COLORS[rs.signal] || colors.textMuted,
                                fontFamily: "'JetBrains Mono', monospace",
                            }}>{(rs.signal || '').replace('_', ' ')}</span>
                        </div>
                        <div style={{ display: 'flex', flexWrap: 'wrap', gap: '4px' }}>
                            {(rs.top_actors || []).slice(0, 5).map((mover, i) => (
                                <span key={i} style={{
                                    fontSize: '10px', padding: '2px 6px', borderRadius: '3px',
                                    background: (mover.contribution || 0) > 0 ? '#22C55E15' : '#EF444415',
                                    color: (mover.contribution || 0) > 0 ? '#22C55E' : '#EF4444',
                                    fontFamily: "'JetBrains Mono', monospace",
                                }}>
                                    {mover.ticker || mover.name} {mover.contribution > 0 ? '+' : ''}{mover.contribution?.toFixed(1)}%
                                </span>
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}

export default function CapitalFlowAnalysis() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState(null);
    const [selectedSectors, setSelectedSectors] = useState([]);
    const [sectorPickerOpen, setSectorPickerOpen] = useState(false);
    const [showNarrative, setShowNarrative] = useState(true);
    const [timeframe, setTimeframe] = useState('Current');
    const [compareMode, setCompareMode] = useState(false);
    const [compareTf, setCompareTf] = useState('1Y');
    const [compareData, setCompareData] = useState(null);

    // Auto-run on mount
    React.useEffect(() => { runResearch(false); }, []);

    const runResearch = async (force = false) => {
        setLoading(true);
        setError(null);
        try {
            const sectors = selectedSectors.length > 0 ? selectedSectors : null;
            // Compute as_of date based on timeframe
            let asOf = null;
            if (timeframe !== 'Current') {
                const now = new Date();
                const map = { '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365 };
                const days = map[timeframe] || 0;
                if (days > 0) {
                    const d = new Date(now.getTime() - days * 86400000);
                    asOf = d.toISOString().split('T')[0];
                }
            }
            const result = await api.getCapitalFlowResearch(sectors, asOf, force);
            setData(result);

            // If compare mode, also fetch comparison timeframe
            if (compareMode && compareTf !== timeframe) {
                const compMap = { '1W': 7, '1M': 30, '3M': 90, '6M': 180, '1Y': 365 };
                const compDays = compMap[compareTf] || 365;
                const compDate = new Date(Date.now() - compDays * 86400000).toISOString().split('T')[0];
                const compResult = await api.getCapitalFlowResearch(sectors, compDate, false);
                setCompareData(compResult);
            }
        } catch (err) {
            setError(err.message || 'Research failed');
        }
        setLoading(false);
    };

    // Re-run when timeframe changes
    React.useEffect(() => { if (data) runResearch(false); }, [timeframe]);

    const toggleSector = (s) => {
        setSelectedSectors(prev =>
            prev.includes(s) ? prev.filter(x => x !== s) : [...prev, s]
        );
    };

    return (
        <div style={{
            background: colors.card, borderRadius: '12px',
            border: `1px solid ${colors.border}`, overflow: 'hidden',
        }}>
            {/* Header */}
            <div style={{
                padding: '14px 16px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
            }}>
                <div>
                    <div style={{
                        fontSize: '11px', fontWeight: 700, color: colors.accent,
                        letterSpacing: '1.5px', fontFamily: "'JetBrains Mono', monospace",
                    }}>CAPITAL FLOW ANALYSIS</div>
                    <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px' }}>
                        Deep sector rotation & flow research
                    </div>
                </div>
                <div style={{ display: 'flex', gap: '6px' }}>
                    <button
                        onClick={() => setSectorPickerOpen(!sectorPickerOpen)}
                        style={{
                            ...btnStyle,
                            background: sectorPickerOpen ? colors.accent : colors.card,
                            color: sectorPickerOpen ? '#fff' : colors.textMuted,
                        }}
                    >
                        {selectedSectors.length > 0 ? `${selectedSectors.length} sectors` : 'All sectors'}
                    </button>
                    <button
                        onClick={() => runResearch(false)}
                        disabled={loading}
                        style={{
                            ...btnStyle,
                            background: loading ? '#2A3A50' : colors.accent,
                            color: '#fff',
                        }}
                    >
                        {loading ? 'Researching...' : 'Run Research'}
                    </button>
                    {data && (
                        <button
                            onClick={() => runResearch(true)}
                            disabled={loading}
                            style={{ ...btnStyle, color: colors.textMuted }}
                            title="Force fresh data pull"
                        >
                            Refresh
                        </button>
                    )}
                </div>
            </div>

            {/* Sector Picker Dropdown */}
            {sectorPickerOpen && (
                <div style={{
                    padding: '10px 16px', borderBottom: `1px solid ${colors.border}`,
                    display: 'flex', flexWrap: 'wrap', gap: '6px',
                }}>
                    {SECTOR_LIST.map(s => {
                        const selected = selectedSectors.includes(s);
                        return (
                            <button
                                key={s}
                                onClick={() => toggleSector(s)}
                                style={{
                                    background: selected ? colors.accent + '30' : 'transparent',
                                    border: `1px solid ${selected ? colors.accent : colors.border}`,
                                    borderRadius: '4px', padding: '4px 10px', fontSize: '11px',
                                    color: selected ? colors.accent : colors.textMuted,
                                    cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                                }}
                            >{s}</button>
                        );
                    })}
                    <button
                        onClick={() => setSelectedSectors([])}
                        style={{ ...btnStyle, fontSize: '10px', color: colors.textMuted }}
                    >Clear All</button>
                </div>
            )}

            {/* Timeframe selector + compare toggle */}
            <div style={{
                padding: '8px 16px', borderBottom: `1px solid ${colors.border}`,
                display: 'flex', gap: '4px', overflowX: 'auto', alignItems: 'center',
            }}>
                {TIMEFRAMES.map(tf => (
                    <button key={tf} onClick={() => setTimeframe(tf)}
                        style={{
                            background: timeframe === tf ? colors.accent + '30' : 'transparent',
                            border: `1px solid ${timeframe === tf ? colors.accent : colors.border}`,
                            borderRadius: '4px', padding: '4px 10px', fontSize: '10px',
                            color: timeframe === tf ? colors.accent : colors.textMuted,
                            cursor: 'pointer', fontFamily: "'JetBrains Mono', monospace",
                            fontWeight: timeframe === tf ? 700 : 400,
                            whiteSpace: 'nowrap',
                        }}
                    >{tf}</button>
                ))}
                <div style={{ marginLeft: 'auto', display: 'flex', gap: '4px', alignItems: 'center' }}>
                    <button onClick={() => { setCompareMode(!compareMode); if (!compareMode) setCompareData(null); }}
                        style={{
                            ...btnStyle, fontSize: '10px', padding: '4px 8px',
                            background: compareMode ? colors.accent + '30' : 'transparent',
                            color: compareMode ? colors.accent : colors.textMuted,
                            border: `1px solid ${compareMode ? colors.accent : colors.border}`,
                        }}>Compare</button>
                    {compareMode && (
                        <select value={compareTf}
                            onChange={e => { setCompareTf(e.target.value); }}
                            style={{
                                background: colors.bg, border: `1px solid ${colors.border}`,
                                borderRadius: '4px', padding: '3px 6px', fontSize: '10px',
                                color: colors.text, fontFamily: "'JetBrains Mono', monospace",
                            }}>
                            {TIMEFRAMES.filter(tf => tf !== timeframe).map(tf => (
                                <option key={tf} value={tf}>{tf}</option>
                            ))}
                        </select>
                    )}
                </div>
            </div>

            {/* Loading state */}
            {loading && (
                <div style={{ padding: '24px', textAlign: 'center' }}>
                    <div style={{ fontSize: '13px', color: colors.accent, marginBottom: '8px' }}>
                        Running deep capital flow research...
                    </div>
                    <div style={{ fontSize: '11px', color: colors.textMuted }}>
                        Pulling sector ETFs, FRED monetary data, credit spreads, SEC filings, dark pool activity, options positioning...
                    </div>
                    <div style={{
                        width: '200px', height: '3px', background: '#1A2840',
                        borderRadius: '2px', margin: '12px auto', overflow: 'hidden',
                    }}>
                        <div style={{
                            width: '40%', height: '100%', background: colors.accent,
                            borderRadius: '2px', animation: 'pulse 1.5s ease-in-out infinite',
                        }} />
                    </div>
                </div>
            )}

            {/* Error */}
            {error && (
                <div style={{ padding: '16px', color: colors.red, fontSize: '13px' }}>
                    {error}
                </div>
            )}

            {/* Results */}
            {data && !loading && (
                <div style={{ padding: '12px 16px' }}>
                    {/* Metadata bar */}
                    <div style={{
                        display: 'flex', gap: '12px', marginBottom: '12px',
                        fontSize: '10px', color: colors.textMuted,
                        fontFamily: "'JetBrains Mono', monospace",
                    }}>
                        <span>as_of: {data.as_of}</span>
                        <span>sources: {data.metadata?.sources_pulled?.length || 0}</span>
                        {data.metadata?.errors?.length > 0 && (
                            <span style={{ color: colors.yellow }}>
                                {data.metadata.errors.length} errors
                            </span>
                        )}
                    </div>

                    {/* Sector Rotation Heatmap */}
                    {data.relative_strength && Object.keys(data.relative_strength).length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={sectionHeader}>SECTOR ROTATION MAP</div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                                {Object.entries(data.relative_strength)
                                    .sort((a, b) => (b[1].vs_spy?.['1m'] || 0) - (a[1].vs_spy?.['1m'] || 0))
                                    .map(([sector, rs]) => {
                                        const signal = rs.signal || 'NEUTRAL';
                                        const color = SIGNAL_COLORS[signal] || colors.textMuted;
                                        const val1m = rs.vs_spy?.['1m'] || 0;
                                        const val3m = rs.vs_spy?.['3m'] || 0;
                                        return (
                                            <div key={sector} style={{
                                                background: color + '15',
                                                border: `1px solid ${color}40`,
                                                borderRadius: '8px', padding: '10px 12px',
                                                minWidth: '140px', flex: '1 1 140px',
                                            }}>
                                                <div style={{
                                                    fontSize: '12px', fontWeight: 700, color,
                                                    marginBottom: '4px',
                                                }}>{sector}</div>
                                                <div style={{ fontSize: '10px', color: colors.textMuted }}>
                                                    1m: {val1m > 0 ? '+' : ''}{val1m}% | 3m: {val3m > 0 ? '+' : ''}{val3m}%
                                                </div>
                                                <div style={{
                                                    fontSize: '10px', fontWeight: 600, color,
                                                    marginTop: '2px',
                                                }}>{signal.replace('_', ' ')}</div>
                                            </div>
                                        );
                                    })}
                            </div>
                        </div>
                    )}

                    {/* Compare Mode */}
                    {compareMode && compareData && (
                        <CompareView dataA={data} dataB={compareData} tfA={timeframe} tfB={compareTf} />
                    )}

                    {/* Flow Attribution */}
                    <FlowAttribution data={data} />

                    {/* YoY Comparison */}
                    {data.yoy_comparison && Object.keys(data.yoy_comparison).length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={sectionHeader}>HISTORICAL COMPARISON (Q RETURNS)</div>
                            <div style={{ overflowX: 'auto' }}>
                                <table style={{ width: '100%', fontSize: '11px', borderCollapse: 'collapse' }}>
                                    <thead>
                                        <tr>
                                            <th style={thStyle}>Sector</th>
                                            <th style={thStyle}>Current Q</th>
                                            <th style={thStyle}>1Y Ago</th>
                                            <th style={thStyle}>2Y Ago</th>
                                            <th style={thStyle}>3Y Ago</th>
                                            <th style={thStyle}>5Y Ago</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {Object.entries(data.yoy_comparison).map(([sector, comp]) => (
                                            <tr key={sector}>
                                                <td style={tdStyle}>{sector}</td>
                                                <td style={tdNumStyle(comp.current_q)}>{fmtPct(comp.current_q)}</td>
                                                <td style={tdNumStyle(comp['1y_ago_q'])}>{fmtPct(comp['1y_ago_q'])}</td>
                                                <td style={tdNumStyle(comp['2y_ago_q'])}>{fmtPct(comp['2y_ago_q'])}</td>
                                                <td style={tdNumStyle(comp['3y_ago_q'])}>{fmtPct(comp['3y_ago_q'])}</td>
                                                <td style={tdNumStyle(comp['5y_ago_q'])}>{fmtPct(comp['5y_ago_q'])}</td>
                                            </tr>
                                        ))}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    )}

                    {/* Monetary Backdrop */}
                    {data.monetary && Object.keys(data.monetary).length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={sectionHeader}>MONETARY BACKDROP</div>
                            <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(150px, 1fr))', gap: '8px' }}>
                                {Object.entries(data.monetary).map(([label, info]) => (
                                    <div key={label} style={{
                                        background: colors.bg, borderRadius: '8px',
                                        padding: '10px', border: `1px solid ${colors.border}`,
                                    }}>
                                        <div style={{ fontSize: '10px', color: colors.textMuted, marginBottom: '4px' }}>
                                            {label.replace(/_/g, ' ').toUpperCase()}
                                        </div>
                                        <div style={{ fontSize: '14px', fontWeight: 700, color: colors.text,
                                            fontFamily: "'JetBrains Mono', monospace" }}>
                                            {typeof info.value === 'number' ? info.value.toLocaleString() : info.value}
                                        </div>
                                        {info.yoy_pct != null && (
                                            <div style={{
                                                fontSize: '10px', marginTop: '2px',
                                                color: info.yoy_pct >= 0 ? colors.green : colors.red,
                                            }}>
                                                YoY: {info.yoy_pct > 0 ? '+' : ''}{info.yoy_pct}%
                                            </div>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* Options Positioning */}
                    {data.options_positioning && Object.keys(data.options_positioning).length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={sectionHeader}>OPTIONS POSITIONING</div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                                {Object.entries(data.options_positioning).map(([sector, info]) => {
                                    const sentColor = SENTIMENT_COLORS[info.sentiment] || colors.textMuted;
                                    return (
                                        <div key={sector} style={{
                                            background: sentColor + '12',
                                            border: `1px solid ${sentColor}30`,
                                            borderRadius: '6px', padding: '8px 10px',
                                            fontSize: '11px',
                                        }}>
                                            <span style={{ color: colors.text, fontWeight: 600 }}>{sector}</span>
                                            <span style={{ color: colors.textMuted, margin: '0 6px' }}>P/C:</span>
                                            <span style={{ color: sentColor, fontWeight: 600 }}>
                                                {info.put_call_ratio} ({info.sentiment})
                                            </span>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    )}

                    {/* SEC Filing Velocity */}
                    {data.sec_velocity && Object.keys(data.sec_velocity).length > 0 && (
                        <div style={{ marginBottom: '16px' }}>
                            <div style={sectionHeader}>SEC FILING VELOCITY</div>
                            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '6px' }}>
                                {Object.entries(data.sec_velocity).map(([sector, info]) => (
                                    <div key={sector} style={{
                                        background: info.trend === 'spiking' ? colors.redBg : colors.bg,
                                        border: `1px solid ${info.trend === 'spiking' ? colors.red + '40' : colors.border}`,
                                        borderRadius: '6px', padding: '8px 10px', fontSize: '11px',
                                    }}>
                                        <span style={{ color: colors.text }}>{sector}: </span>
                                        <span style={{
                                            color: info.trend === 'spiking' ? colors.red : colors.textDim,
                                            fontWeight: 600,
                                        }}>
                                            {info.latest}/wk
                                        </span>
                                        {info.trend === 'spiking' && (
                                            <span style={{ color: colors.red, marginLeft: '4px' }}>SPIKE</span>
                                        )}
                                    </div>
                                ))}
                            </div>
                        </div>
                    )}

                    {/* LLM Narrative */}
                    {data.narrative && (
                        <div style={{ marginTop: '16px' }}>
                            <div style={{
                                display: 'flex', justifyContent: 'space-between',
                                alignItems: 'center', marginBottom: '8px',
                            }}>
                                <div style={sectionHeader}>LLM SYNTHESIS</div>
                                <button
                                    onClick={() => setShowNarrative(!showNarrative)}
                                    style={{ ...btnStyle, fontSize: '10px', color: colors.textMuted }}
                                >
                                    {showNarrative ? 'Collapse' : 'Expand'}
                                </button>
                            </div>
                            {showNarrative && (
                                <div style={{
                                    background: colors.bg, borderRadius: '8px',
                                    border: `1px solid ${colors.border}`,
                                    padding: '14px', fontSize: '12px', lineHeight: '1.7',
                                    color: colors.textDim, whiteSpace: 'pre-wrap',
                                    fontFamily: "'JetBrains Mono', monospace",
                                    maxHeight: '500px', overflowY: 'auto',
                                }}>
                                    {data.narrative}
                                </div>
                            )}
                        </div>
                    )}

                    {/* No data prompt */}
                    {!data.narrative && Object.keys(data.sectors).length === 0 && (
                        <div style={{
                            padding: '20px', textAlign: 'center',
                            color: colors.textMuted, fontSize: '13px',
                        }}>
                            No data available. Connect to the database and run ingestion first.
                        </div>
                    )}
                </div>
            )}

            {/* Empty state */}
            {!data && !loading && !error && (
                <div style={{
                    padding: '24px', textAlign: 'center', color: colors.textMuted,
                }}>
                    <div style={{ fontSize: '13px', marginBottom: '4px' }}>
                        Deep capital flow research across all sectors
                    </div>
                    <div style={{ fontSize: '11px' }}>
                        Pulls ETF flows, FRED monetary, credit, SEC filings, dark pool, options
                    </div>
                </div>
            )}
        </div>
    );
}

// Style helpers
const btnStyle = {
    background: colors.card, border: `1px solid ${colors.border}`,
    borderRadius: '6px', padding: '6px 12px', fontSize: '11px',
    color: colors.textMuted, cursor: 'pointer',
    fontFamily: "'JetBrains Mono', monospace",
};

const sectionHeader = {
    fontSize: '10px', fontWeight: 700, color: colors.accent,
    letterSpacing: '1.5px', marginBottom: '8px',
    fontFamily: "'JetBrains Mono', monospace",
};

const thStyle = {
    textAlign: 'left', padding: '6px 8px', color: colors.textMuted,
    borderBottom: `1px solid ${colors.border}`,
    fontFamily: "'JetBrains Mono', monospace", fontWeight: 600,
};

const tdStyle = {
    padding: '6px 8px', color: colors.text,
    borderBottom: `1px solid ${colors.border}15`,
    fontFamily: "'JetBrains Mono', monospace",
};

const tdNumStyle = (val) => ({
    ...tdStyle,
    color: val > 0 ? '#22C55E' : val < 0 ? '#EF4444' : colors.textMuted,
    fontWeight: 600,
});

const fmtPct = (val) => {
    if (val == null) return '--';
    return `${val > 0 ? '+' : ''}${val}%`;
};
