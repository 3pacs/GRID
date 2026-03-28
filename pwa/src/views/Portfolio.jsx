import React, { useEffect, useState, useRef, useCallback, useMemo } from 'react';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { useDevice } from '../hooks/useDevice.js';

/* ── Design constants ── */

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', -apple-system, sans-serif";

const SECTOR_COLORS = {
    Technology: '#3B82F6',
    Energy: '#22C55E',
    Financials: '#F59E0B',
    Healthcare: '#EC4899',
    'Consumer Discretionary': '#8B5CF6',
    'Communication Services': '#06B6D4',
    Industrials: '#78716C',
    'Real Estate': '#D946EF',
    Utilities: '#14B8A6',
    Materials: '#FB923C',
    'Consumer Staples': '#A3E635',
    Crypto: '#F59E0B',
    ETF: '#1A6EBF',
    Commodities: '#FB923C',
    Other: '#5A7080',
};

/* ── Formatters ── */

const fmtDollar = (v) => {
    if (v == null) return '--';
    const n = typeof v === 'number' ? v : parseFloat(v);
    if (isNaN(n)) return '--';
    if (Math.abs(n) >= 1e6) return `$${(n / 1e6).toFixed(1)}M`;
    if (Math.abs(n) >= 1e3) return `$${(n / 1e3).toFixed(1)}K`;
    return `$${n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
};

const fmtPct = (v) => {
    if (v == null) return '--';
    const pct = (v * 100).toFixed(2);
    return v >= 0 ? `+${pct}%` : `${pct}%`;
};

const fmtPrice = (v) => {
    if (v == null) return '--';
    const n = typeof v === 'number' ? v : parseFloat(v);
    if (isNaN(n)) return '--';
    if (n >= 10000) return n.toLocaleString(undefined, { maximumFractionDigits: 0 });
    return n.toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const pnlColor = (v) => {
    if (v == null || v === 0) return colors.textDim;
    return v > 0 ? colors.green : colors.red;
};

/* ── Donut Chart (D3-free, pure SVG) ── */

function DonutChart({ data, size = 200 }) {
    // data: [{label, value, color}]
    const total = data.reduce((s, d) => s + d.value, 0);
    if (total === 0) return null;

    const [hovered, setHovered] = useState(null);
    const cx = size / 2;
    const cy = size / 2;
    const outerR = size / 2 - 4;
    const innerR = outerR * 0.58;

    let cumAngle = -Math.PI / 2;
    const arcs = data.map((d, i) => {
        const sweep = (d.value / total) * 2 * Math.PI;
        const startAngle = cumAngle;
        const endAngle = cumAngle + sweep;
        cumAngle = endAngle;

        const largeArc = sweep > Math.PI ? 1 : 0;
        const x1 = cx + outerR * Math.cos(startAngle);
        const y1 = cy + outerR * Math.sin(startAngle);
        const x2 = cx + outerR * Math.cos(endAngle);
        const y2 = cy + outerR * Math.sin(endAngle);
        const x3 = cx + innerR * Math.cos(endAngle);
        const y3 = cy + innerR * Math.sin(endAngle);
        const x4 = cx + innerR * Math.cos(startAngle);
        const y4 = cy + innerR * Math.sin(startAngle);

        const path = [
            `M ${x1} ${y1}`,
            `A ${outerR} ${outerR} 0 ${largeArc} 1 ${x2} ${y2}`,
            `L ${x3} ${y3}`,
            `A ${innerR} ${innerR} 0 ${largeArc} 0 ${x4} ${y4}`,
            'Z',
        ].join(' ');

        return { path, color: d.color, label: d.label, pct: d.value / total, i };
    });

    return (
        <div style={{ position: 'relative', width: size, height: size }}>
            <svg width={size} height={size}>
                {arcs.map((a) => (
                    <path
                        key={a.i}
                        d={a.path}
                        fill={a.color}
                        opacity={hovered === null || hovered === a.i ? 1 : 0.35}
                        stroke={colors.card}
                        strokeWidth="1.5"
                        style={{ cursor: 'pointer', transition: 'opacity 0.2s' }}
                        onMouseEnter={() => setHovered(a.i)}
                        onMouseLeave={() => setHovered(null)}
                    />
                ))}
            </svg>
            {/* Center label */}
            <div style={{
                position: 'absolute', top: '50%', left: '50%',
                transform: 'translate(-50%, -50%)', textAlign: 'center',
                pointerEvents: 'none',
            }}>
                {hovered !== null ? (
                    <>
                        <div style={{ fontSize: '12px', color: colors.textDim, fontFamily: MONO }}>
                            {arcs[hovered].label}
                        </div>
                        <div style={{ fontSize: '16px', fontWeight: 700, color: '#E8F0F8', fontFamily: MONO }}>
                            {(arcs[hovered].pct * 100).toFixed(1)}%
                        </div>
                    </>
                ) : (
                    <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO }}>
                        ALLOCATION
                    </div>
                )}
            </div>
        </div>
    );
}

/* ── Legend ── */

function DonutLegend({ data }) {
    const total = data.reduce((s, d) => s + d.value, 0);
    return (
        <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
            {data.map((d, i) => (
                <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                    <div style={{
                        width: 10, height: 10, borderRadius: 2,
                        background: d.color, flexShrink: 0,
                    }} />
                    <span style={{ fontSize: '12px', color: colors.text, fontFamily: SANS, flex: 1 }}>
                        {d.label}
                    </span>
                    <span style={{ fontSize: '12px', color: colors.textDim, fontFamily: MONO }}>
                        {total > 0 ? `${(d.value / total * 100).toFixed(1)}%` : '--'}
                    </span>
                </div>
            ))}
        </div>
    );
}

/* ── Metric Card ── */

function MetricCard({ label, value, subtext, color }) {
    return (
        <div style={{
            ...shared.metric,
            background: colors.bg,
            border: `1px solid ${colors.borderSubtle}`,
            borderRadius: tokens.radius.md,
            padding: '14px 16px',
            minWidth: '120px',
        }}>
            <div style={{ ...shared.metricLabel, marginBottom: '4px' }}>{label}</div>
            <div style={{
                ...shared.metricValue,
                fontSize: '18px',
                color: color || '#E8F0F8',
            }}>{value}</div>
            {subtext && (
                <div style={{ fontSize: '10px', color: colors.textMuted, marginTop: '2px', fontFamily: MONO }}>
                    {subtext}
                </div>
            )}
        </div>
    );
}

/* ── Sortable Position Table ── */

function PositionTable({ positions }) {
    const [sortKey, setSortKey] = useState('weight');
    const [sortDir, setSortDir] = useState(-1); // -1 = desc

    const toggleSort = (key) => {
        if (sortKey === key) {
            setSortDir(d => d * -1);
        } else {
            setSortKey(key);
            setSortDir(-1);
        }
    };

    const sorted = useMemo(() => {
        return [...positions].sort((a, b) => {
            const av = a[sortKey] ?? -Infinity;
            const bv = b[sortKey] ?? -Infinity;
            return (av > bv ? 1 : av < bv ? -1 : 0) * sortDir;
        });
    }, [positions, sortKey, sortDir]);

    const cols = [
        { key: 'ticker', label: 'TICKER', align: 'left' },
        { key: 'price', label: 'PRICE', align: 'right' },
        { key: 'change_1d', label: '1D CHG', align: 'right' },
        { key: 'weight', label: 'WEIGHT', align: 'right' },
        { key: 'pnl_1d', label: '1D P&L', align: 'right' },
        { key: 'sector', label: 'SECTOR', align: 'left' },
    ];

    const arrow = (key) => {
        if (sortKey !== key) return '';
        return sortDir > 0 ? ' \u25B2' : ' \u25BC';
    };

    return (
        <div style={{ overflowX: 'auto' }}>
            <table style={{
                width: '100%', borderCollapse: 'collapse',
                fontSize: '12px', fontFamily: MONO,
            }}>
                <thead>
                    <tr>
                        {cols.map(c => (
                            <th
                                key={c.key}
                                onClick={() => toggleSort(c.key)}
                                style={{
                                    textAlign: c.align, padding: '8px 10px',
                                    color: sortKey === c.key ? colors.accent : colors.textMuted,
                                    fontSize: '10px', fontWeight: 700,
                                    letterSpacing: '1px', cursor: 'pointer',
                                    borderBottom: `1px solid ${colors.border}`,
                                    userSelect: 'none', whiteSpace: 'nowrap',
                                }}
                            >
                                {c.label}{arrow(c.key)}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {sorted.map((p) => (
                        <tr key={p.ticker} style={{ borderBottom: `1px solid ${colors.borderSubtle}` }}>
                            <td style={{ padding: '10px', color: '#E8F0F8', fontWeight: 600 }}>
                                {p.ticker}
                                {p.display_name && (
                                    <span style={{ color: colors.textMuted, fontWeight: 400, marginLeft: '6px', fontSize: '11px' }}>
                                        {p.display_name}
                                    </span>
                                )}
                            </td>
                            <td style={{ padding: '10px', textAlign: 'right', color: colors.text }}>
                                {fmtPrice(p.price)}
                            </td>
                            <td style={{
                                padding: '10px', textAlign: 'right',
                                color: pnlColor(p.change_1d),
                            }}>
                                {fmtPct(p.change_1d)}
                            </td>
                            <td style={{ padding: '10px', textAlign: 'right', color: colors.textDim }}>
                                {(p.weight * 100).toFixed(1)}%
                            </td>
                            <td style={{
                                padding: '10px', textAlign: 'right',
                                color: pnlColor(p.pnl_1d), fontWeight: 600,
                            }}>
                                {p.pnl_1d >= 0 ? '+' : ''}{fmtDollar(p.pnl_1d)}
                            </td>
                            <td style={{ padding: '10px', color: colors.textMuted, fontSize: '11px' }}>
                                <span style={{
                                    display: 'inline-block', width: 8, height: 8,
                                    borderRadius: 2, marginRight: 6,
                                    background: SECTOR_COLORS[p.sector] || SECTOR_COLORS.Other,
                                }} />
                                {p.sector}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

/* ── Options P&L Section ── */

function OptionsPnL({ data }) {
    if (!data || data.total_recommendations === 0) {
        return (
            <div style={{ ...shared.card, textAlign: 'center', color: colors.textMuted, padding: '24px' }}>
                No options recommendations tracked yet.
            </div>
        );
    }

    const winRate = data.total_recommendations > 0
        ? ((data.wins / (data.wins + data.losses)) * 100).toFixed(1)
        : '0.0';

    return (
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(110px, 1fr))', gap: '10px' }}>
            <MetricCard
                label="Win Rate"
                value={`${winRate}%`}
                subtext={`${data.wins}W / ${data.losses}L`}
                color={parseFloat(winRate) >= 50 ? colors.green : colors.red}
            />
            <MetricCard
                label="Total Return"
                value={fmtPct(data.total_return)}
                color={pnlColor(data.total_return)}
            />
            <MetricCard
                label="Open"
                value={data.open}
                subtext="active positions"
            />
            <MetricCard
                label="Total Recs"
                value={data.total_recommendations}
            />
        </div>
    );
}

/* ── Main View ── */

export default function Portfolio() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const { isMobile } = useDevice();

    useEffect(() => {
        let cancelled = false;
        setLoading(true);
        api.getPortfolio().then(res => {
            if (cancelled) return;
            if (res.error) {
                setError(res.message || 'Failed to load portfolio');
            } else {
                setData(res);
            }
            setLoading(false);
        });
        return () => { cancelled = true; };
    }, []);

    if (loading) {
        return (
            <div style={{ ...shared.container, textAlign: 'center', paddingTop: '80px' }}>
                <div style={{ color: colors.textMuted, fontFamily: MONO, fontSize: '13px' }}>
                    Loading portfolio analytics...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={shared.container}>
                <div style={shared.error}>{error}</div>
            </div>
        );
    }

    if (!data) return null;

    // Build donut data from sector allocation
    const sectorDonut = Object.entries(data.allocation?.by_sector || {})
        .filter(([, v]) => v > 0)
        .sort((a, b) => b[1] - a[1])
        .map(([label, value]) => ({
            label,
            value,
            color: SECTOR_COLORS[label] || SECTOR_COLORS.Other,
        }));

    const risk = data.risk_metrics || {};

    return (
        <div style={{ ...shared.container, maxWidth: '1000px' }}>
            {/* ── Header ── */}
            <div style={{ marginBottom: tokens.space.xl }}>
                <div style={{ ...shared.header, marginBottom: '4px' }}>Portfolio</div>
                <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO }}>
                    Watchlist as portfolio -- estimated from equal-weight allocation
                </div>
            </div>

            {/* ── Portfolio Summary Cards ── */}
            <div style={{
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr 1fr' : 'repeat(4, 1fr)',
                gap: '10px',
                marginBottom: tokens.space.xl,
            }}>
                <MetricCard
                    label="Portfolio Value"
                    value={fmtDollar(data.total_value)}
                    subtext="estimated"
                />
                <MetricCard
                    label="1D P&L"
                    value={`${data.total_pnl_1d >= 0 ? '+' : ''}${fmtDollar(data.total_pnl_1d)}`}
                    subtext={fmtPct(data.total_pnl_1d_pct)}
                    color={pnlColor(data.total_pnl_1d)}
                />
                <MetricCard
                    label="1M P&L (est)"
                    value={`${data.total_pnl_1m >= 0 ? '+' : ''}${fmtDollar(data.total_pnl_1m)}`}
                    color={pnlColor(data.total_pnl_1m)}
                />
                <MetricCard
                    label="Positions"
                    value={data.positions?.length || 0}
                />
            </div>

            {/* ── Allocation + Risk row ── */}
            <div style={{
                display: 'grid',
                gridTemplateColumns: isMobile ? '1fr' : '1fr 1fr',
                gap: tokens.space.lg,
                marginBottom: tokens.space.xl,
            }}>
                {/* Donut chart */}
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>SECTOR ALLOCATION</div>
                    {sectorDonut.length > 0 ? (
                        <div style={{
                            display: 'flex',
                            alignItems: isMobile ? 'center' : 'flex-start',
                            flexDirection: isMobile ? 'column' : 'row',
                            gap: '20px', paddingTop: '8px',
                        }}>
                            <DonutChart data={sectorDonut} size={isMobile ? 180 : 200} />
                            <DonutLegend data={sectorDonut} />
                        </div>
                    ) : (
                        <div style={{ color: colors.textMuted, fontSize: '12px', padding: '20px 0' }}>
                            No sector data available.
                        </div>
                    )}
                </div>

                {/* Risk metrics */}
                <div style={shared.card}>
                    <div style={shared.sectionTitle}>RISK METRICS</div>
                    <div style={{
                        display: 'grid', gridTemplateColumns: '1fr',
                        gap: '10px', marginTop: '8px',
                    }}>
                        <MetricCard
                            label="Top-3 Concentration"
                            value={`${(risk.concentration_top3 * 100).toFixed(1)}%`}
                            subtext={risk.concentration_top3 > 0.7 ? 'high concentration' : 'diversified'}
                            color={risk.concentration_top3 > 0.7 ? colors.yellow : colors.green}
                        />
                        <MetricCard
                            label="Beta (Weighted)"
                            value={risk.beta_weighted?.toFixed(2) || '--'}
                            subtext={risk.beta_weighted > 1.3 ? 'aggressive' : risk.beta_weighted < 0.8 ? 'defensive' : 'moderate'}
                            color={risk.beta_weighted > 1.3 ? colors.yellow : colors.text}
                        />
                        <MetricCard
                            label="Sector Diversification"
                            value={`${((risk.sector_diversification_score || 0) * 100).toFixed(0)}%`}
                            subtext={risk.sector_diversification_score > 0.7 ? 'well diversified' : 'concentrated'}
                            color={risk.sector_diversification_score > 0.7 ? colors.green : colors.yellow}
                        />
                    </div>
                </div>
            </div>

            {/* ── Position Table ── */}
            <div style={{ ...shared.card, marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>POSITIONS</div>
                {data.positions?.length > 0 ? (
                    <PositionTable positions={data.positions} />
                ) : (
                    <div style={{ color: colors.textMuted, fontSize: '12px', padding: '20px 0', textAlign: 'center' }}>
                        No positions in watchlist.
                    </div>
                )}
            </div>

            {/* ── Options P&L ── */}
            <div style={{ ...shared.card, marginBottom: tokens.space.xl }}>
                <div style={shared.sectionTitle}>OPTIONS P&L</div>
                <div style={{ marginTop: '8px' }}>
                    <OptionsPnL data={data.options_pnl} />
                </div>
            </div>
        </div>
    );
}
