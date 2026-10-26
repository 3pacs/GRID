/**
 * TrendTracker — Market Trend Divergence Analysis view.
 *
 * Second instance of the reusable "lens" pattern (first was CrossReference).
 * Applies divergence analysis to market trends across six categories:
 * Momentum, Regime, Sector Rotation, Volatility, Liquidity, Correlations.
 *
 * Category summary grid + expandable trend cards + D3 multi-line chart + narrative panel.
 */
import React, { useState, useEffect, useRef, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { shared, colors, tokens } from '../styles/shared.js';
import { formatDateTime } from '../utils/formatTime.js';
import ErrorState from '../components/ErrorState.jsx';

// ── Constants ────────────────────────────────────────────────────────────────

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";

const CATEGORIES = [
    { key: 'momentum', label: 'Momentum', icon: '\u2197' },
    { key: 'regime', label: 'Regime', icon: '\u26A0' },
    { key: 'sector_rotation', label: 'Sector Rotation', icon: '\u21C4' },
    { key: 'volatility', label: 'Volatility', icon: '\u2248' },
    { key: 'liquidity', label: 'Liquidity', icon: '\u2B06' },
    { key: 'correlation', label: 'Correlations', icon: '\u2194' },
];

const DIRECTION_COLORS = {
    bullish: colors.green,
    bearish: colors.red,
    neutral: colors.textMuted,
    transitioning: colors.yellow,
};

const DIRECTION_BG = {
    bullish: colors.greenBg,
    bearish: colors.redBg,
    neutral: '#1A2840',
    transitioning: colors.yellowBg,
};

const DIRECTION_ARROWS = {
    bullish: '\u25B2',
    bearish: '\u25BC',
    neutral: '\u25CF',
    transitioning: '\u25C6',
};

const LOOKBACK_OPTIONS = [30, 60, 90, 180, 365];

// ── Styles ────────────────────────────────────────────────────────────────

const s = {
    container: {
        padding: tokens.space.lg, maxWidth: '1100px', margin: '0 auto',
        minHeight: '100vh',
    },
    header: { marginBottom: '8px' },
    title: {
        fontSize: '22px', fontWeight: 800, color: '#E8F0F8',
        fontFamily: mono, letterSpacing: '3px',
    },
    subtitle: {
        fontSize: '12px', color: colors.textMuted, fontFamily: mono,
        letterSpacing: '1px', marginTop: '4px',
    },
    sectionTitle: {
        ...shared.sectionTitle, marginTop: '28px', marginBottom: '12px',
        fontSize: '11px',
    },

    // Lookback selector
    lookbackRow: {
        display: 'flex', gap: '6px', marginTop: '12px', flexWrap: 'wrap',
    },
    lookbackBtn: (active) => ({
        padding: '6px 14px', borderRadius: tokens.radius.sm,
        fontSize: '11px', fontWeight: 700, fontFamily: mono,
        cursor: 'pointer', border: 'none',
        background: active ? colors.accent : colors.card,
        color: active ? '#fff' : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
    }),

    // Category cards grid
    catGrid: {
        display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(160px, 1fr))',
        gap: '10px', marginTop: '12px',
    },
    catCard: (direction) => ({
        ...shared.cardGradient,
        padding: '14px 16px',
        borderLeft: `3px solid ${DIRECTION_COLORS[direction] || colors.textMuted}`,
        cursor: 'pointer',
        transition: 'all 0.2s ease',
        position: 'relative',
        overflow: 'hidden',
    }),
    catLabel: {
        fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px',
        fontFamily: mono, color: colors.textMuted, marginBottom: '6px',
    },
    catHeadline: {
        fontSize: '12px', fontWeight: 600, color: colors.text, fontFamily: mono,
        lineHeight: 1.5, marginBottom: '8px',
        overflow: 'hidden', textOverflow: 'ellipsis',
        display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
        wordBreak: 'break-word',
    },
    catMetrics: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    },
    dirBadge: (direction) => ({
        display: 'inline-flex', alignItems: 'center', gap: '4px',
        padding: '4px 8px', borderRadius: '999px',
        fontSize: '10px', fontWeight: 800, fontFamily: mono,
        background: DIRECTION_BG[direction] || '#1A2840',
        color: DIRECTION_COLORS[direction] || colors.textMuted,
        whiteSpace: 'nowrap', minWidth: '32px',
    }),
    strengthBar: {
        width: '60px', height: '4px', background: colors.bg,
        borderRadius: '2px', overflow: 'hidden',
    },
    strengthFill: (strength, direction) => ({
        width: `${Math.round(strength * 100)}%`,
        height: '100%',
        background: DIRECTION_COLORS[direction] || colors.textMuted,
        borderRadius: '2px',
        transition: 'width 0.4s ease',
    }),
    catCount: {
        fontSize: '10px', color: colors.textMuted, fontFamily: mono,
    },

    // Trend cards
    trendCard: {
        ...shared.cardGradient,
        marginBottom: '8px',
        overflow: 'hidden',
        transition: 'all 0.3s ease',
    },
    trendHeader: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        cursor: 'pointer', padding: '14px 16px',
    },
    trendLeft: {
        display: 'flex', alignItems: 'center', gap: '10px', flex: 1, minWidth: 0,
    },
    trendName: {
        fontSize: '13px', fontWeight: 700, color: '#E8F0F8', fontFamily: mono,
        overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap',
    },
    trendRight: {
        display: 'flex', alignItems: 'center', gap: '10px', flexShrink: 0,
    },
    confidenceBadge: {
        fontSize: '10px', fontWeight: 600, fontFamily: mono,
        color: colors.textMuted, padding: '2px 8px',
        background: colors.bg, borderRadius: '4px',
    },
    expandIcon: (expanded) => ({
        fontSize: '12px', color: colors.textMuted, fontFamily: mono,
        transition: 'transform 0.2s ease',
        transform: expanded ? 'rotate(180deg)' : 'rotate(0deg)',
    }),
    trendBody: {
        padding: '0 16px 16px 16px',
    },
    trendDesc: {
        fontSize: '12px', color: colors.textDim, fontFamily: mono,
        lineHeight: 1.6, marginBottom: '14px',
    },

    // Evidence columns
    evidenceGrid: {
        display: 'grid', gridTemplateColumns: '1fr 1fr',
        gap: '12px', marginBottom: '14px',
    },
    evidenceCol: {
        background: colors.bg, borderRadius: tokens.radius.md,
        padding: '12px',
    },
    evidenceTitle: (isSupport) => ({
        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
        fontFamily: mono, marginBottom: '8px',
        color: isSupport ? colors.green : colors.red,
    }),
    evidenceItem: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono,
        lineHeight: 1.5, marginBottom: '4px',
        display: 'flex', gap: '6px',
    },
    evidenceDot: (isSupport) => ({
        width: '4px', height: '4px', borderRadius: '50%',
        background: isSupport ? colors.green : colors.red,
        marginTop: '6px', flexShrink: 0,
    }),

    // Implications
    implRow: {
        marginBottom: '12px',
    },
    implTitle: {
        fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px',
        fontFamily: mono, color: colors.accent, marginBottom: '6px',
    },
    implItem: {
        fontSize: '11px', color: colors.textDim, fontFamily: mono,
        lineHeight: 1.5, marginBottom: '3px', paddingLeft: '12px',
    },

    // Ticker pills
    tickerRow: {
        display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '8px',
    },
    tickerPill: {
        padding: '3px 10px', borderRadius: tokens.radius.pill,
        fontSize: '10px', fontWeight: 700, fontFamily: mono,
        background: `${colors.accent}20`, color: colors.accent,
        border: `1px solid ${colors.accent}30`,
    },

    // Timeline
    timeline: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        marginTop: '8px', padding: '8px 0',
        borderTop: `1px solid ${colors.borderSubtle}`,
    },
    timelineLabel: {
        fontSize: '10px', color: colors.textMuted, fontFamily: mono,
    },

    // Narrative
    narrativePanel: {
        ...shared.cardGradient,
        marginTop: '20px', padding: tokens.space.xl,
    },
    narrativeText: {
        fontSize: '12px', color: colors.textDim, fontFamily: mono,
        lineHeight: 1.7, whiteSpace: 'pre-wrap',
    },

    // Chart
    chartWrap: {
        ...shared.cardGradient,
        marginTop: '12px', padding: '16px',
        overflowX: 'auto',
    },
    chartToggleRow: {
        display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '10px',
    },
    chartToggle: (active, color) => ({
        padding: '4px 10px', borderRadius: tokens.radius.sm,
        fontSize: '10px', fontWeight: 600, fontFamily: mono,
        cursor: 'pointer', border: `1px solid ${color}40`,
        background: active ? `${color}20` : 'transparent',
        color: active ? color : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
    }),

    // Tabs
    tabs: { ...shared.tabs, marginBottom: '4px', marginTop: '20px' },
    tab: (active) => shared.tab(active),

    // Loading
    loadingBar: {
        height: '2px', background: colors.bg, borderRadius: '1px',
        marginBottom: '16px', overflow: 'hidden',
    },
    loadingFill: {
        height: '100%', background: colors.accent,
        borderRadius: '1px',
        animation: 'trendLoadSlide 1.5s ease infinite',
        width: '40%',
    },

    // Filter
    filterRow: {
        display: 'flex', gap: '6px', flexWrap: 'wrap', marginBottom: '10px',
    },
    filterBtn: (active) => ({
        padding: '6px 12px', borderRadius: tokens.radius.sm,
        fontSize: '10px', fontWeight: 700, fontFamily: mono,
        cursor: 'pointer', border: `1px solid ${active ? colors.accent : colors.border}`,
        background: active ? `${colors.accent}20` : 'transparent',
        color: active ? colors.accent : colors.textMuted,
        transition: `all ${tokens.transition.fast}`,
    }),
};

// ── Keyframes ────────────────────────────────────────────────────────────

const ANIMATION_ID = 'trend-tracker-keyframes';
function ensureKeyframes() {
    if (document.getElementById(ANIMATION_ID)) return;
    const style = document.createElement('style');
    style.id = ANIMATION_ID;
    style.textContent = `
        @keyframes trendLoadSlide {
            0% { transform: translateX(-100%); }
            100% { transform: translateX(350%); }
        }
        @keyframes trendFadeIn {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
        }
        .trend-card-enter {
            animation: trendFadeIn 0.3s ease forwards;
        }
        .trend-cat-card:hover {
            transform: translateY(-2px);
            box-shadow: 0 4px 16px rgba(0,0,0,0.4);
        }
    `;
    document.head.appendChild(style);
}


// ── D3 Trend Comparison Chart ────────────────────────────────────────────

const CHART_COLORS = [
    colors.green, colors.red, colors.yellow, colors.accent,
    '#F97316', '#A855F7', '#06B6D4', '#EC4899', '#84CC16', '#14B8A6',
];

function TrendComparisonChart({ trends, visibleTrends, width: w = 700, height: h = 220 }) {
    // Renders the *current* strength the API reported for each visible trend.
    // The /intelligence/trends payload carries no per-trend history, so this
    // chart deliberately has no time axis: an earlier version drew a 12-month
    // trajectory from random-number noise anchored to the current value and
    // presented it as measured history. Nothing here is synthesized.
    const ref = useRef(null);

    useEffect(() => {
        if (!ref.current || !trends || trends.length === 0) return;

        const svg = d3.select(ref.current);
        svg.selectAll('*').remove();

        const visible = trends
            .map((trend, i) => ({ trend, i }))
            .filter(({ i }) => visibleTrends.has(i));
        if (visible.length === 0) {
            svg.attr('width', w).attr('height', 0);
            return;
        }

        const rowH = 22;
        const margin = { top: 8, right: 48, bottom: 8, left: 8 };
        const height = Math.max(h, margin.top + margin.bottom + visible.length * rowH);
        svg.attr('width', w).attr('height', height);

        const cw = w - margin.left - margin.right;
        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);
        const x = d3.scaleLinear().domain([0, 1]).range([0, cw]);

        visible.forEach(({ trend, i }, row) => {
            const color = CHART_COLORS[i % CHART_COLORS.length];
            const strength = Math.max(0, Math.min(1, Number(trend.strength) || 0));
            const y = row * rowH;

            g.append('rect')
                .attr('x', 0).attr('y', y + 4)
                .attr('width', cw).attr('height', rowH - 8)
                .attr('fill', colors.border).attr('opacity', 0.25);

            g.append('rect')
                .attr('x', 0).attr('y', y + 4)
                .attr('width', 0).attr('height', rowH - 8)
                .attr('fill', color).attr('opacity', 0.85)
                .transition().duration(500).delay(row * 60).ease(d3.easeCubicOut)
                .attr('width', x(strength));

            g.append('text')
                .attr('x', cw + 6).attr('y', y + rowH / 2 + 3)
                .attr('font-size', '9px').attr('font-family', mono)
                .attr('fill', colors.textMuted)
                .text(strength.toFixed(2));
        });
    }, [trends, visibleTrends, w, h]);

    return <svg ref={ref} style={{ display: 'block', width: '100%' }} />;
}


// ── Strength Meter mini-component ────────────────────────────────────────

function StrengthMeter({ strength, direction, width = 60 }) {
    return (
        <div style={{ ...s.strengthBar, width: `${width}px` }}>
            <div style={s.strengthFill(strength, direction)} />
        </div>
    );
}


// ── Main Component ───────────────────────────────────────────────────────

export default function TrendTracker() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [lookbackDays, setLookbackDays] = useState(90);
    const [activeTab, setActiveTab] = useState('trends');
    const [expandedTrend, setExpandedTrend] = useState(null);
    const [filterCategory, setFilterCategory] = useState(null);
    const [visibleChartTrends, setVisibleChartTrends] = useState(new Set());

    useEffect(() => {
        ensureKeyframes();
        loadData(lookbackDays);
    }, [lookbackDays]);

    // Initialize chart visibility when data loads
    useEffect(() => {
        if (data?.trends) {
            // Show first 5 trends by default
            setVisibleChartTrends(new Set(data.trends.slice(0, 5).map((_, i) => i)));
        }
    }, [data]);

    async function loadData(days) {
        setLoading(true);
        setError(null);
        try {
            const result = await api.getTrends(days);
            if (result && result.error) {
                // api.js resolves HTTP/network failures to an error marker
                // rather than throwing; surface it instead of hiding it.
                throw new Error(result.message || 'Failed to load trend data');
            }
            const trends = Array.isArray(result?.trends) ? result.trends : [];
            setData({
                trends,
                category_summaries: result?.category_summaries || {},
                narrative: result?.narrative || '',
                generated_at: result?.generated_at || null,
            });
        } catch (err) {
            setError(err?.message || 'Failed to load trend data');
            setData(null);
        } finally {
            setLoading(false);
        }
    }

    const handleCategoryClick = useCallback((catKey) => {
        setFilterCategory(prev => prev === catKey ? null : catKey);
        setActiveTab('trends');
    }, []);

    const toggleTrend = useCallback((idx) => {
        setExpandedTrend(prev => prev === idx ? null : idx);
    }, []);

    const toggleChartTrend = useCallback((idx) => {
        setVisibleChartTrends(prev => {
            const next = new Set(prev);
            if (next.has(idx)) {
                next.delete(idx);
            } else {
                next.add(idx);
            }
            return next;
        });
    }, []);

    if (loading) {
        return (
            <div style={s.container}>
                <div style={s.header}>
                    <div style={s.title}>TREND TRACKER</div>
                    <div style={s.subtitle}>Divergence analysis across market trends</div>
                </div>
                <div style={s.loadingBar}>
                    <div style={s.loadingFill} />
                </div>
                <div style={{ textAlign: 'center', color: colors.textMuted, fontSize: '13px', fontFamily: mono, padding: '60px 0' }}>
                    Analyzing trends across 6 categories...
                </div>
            </div>
        );
    }

    if (!data) {
        return (
            <div style={s.container}>
                <div style={s.header}>
                    <div style={s.title}>TREND TRACKER</div>
                    <div style={s.subtitle}>Divergence analysis across market trends</div>
                </div>
                <ErrorState
                    error={error || 'Failed to load trend data'}
                    onRetry={() => loadData(lookbackDays)}
                    title="Trend data unavailable"
                />
            </div>
        );
    }

    if (!data.trends || data.trends.length === 0) {
        return (
            <div style={s.container}>
                <div style={s.header}>
                    <div style={s.title}>TREND TRACKER</div>
                    <div style={s.subtitle}>Divergence analysis across market trends</div>
                </div>
                <div style={s.lookbackRow}>
                    {LOOKBACK_OPTIONS.map(d => (
                        <button
                            key={d}
                            style={s.lookbackBtn(d === lookbackDays)}
                            onClick={() => setLookbackDays(d)}
                        >
                            {d}D
                        </button>
                    ))}
                </div>
                <div style={{ ...shared.card, textAlign: 'center', padding: '40px', color: colors.textMuted, fontFamily: mono, fontSize: '13px' }}>
                    No trends detected for the last {lookbackDays} days.
                </div>
            </div>
        );
    }

    const { trends, category_summaries, narrative } = data;

    // Filter trends by category if selected
    const filteredTrends = filterCategory
        ? trends.filter(t => t.category === filterCategory)
        : trends;

    // Stats
    const bullishCount = trends.filter(t => t.direction === 'bullish').length;
    const bearishCount = trends.filter(t => t.direction === 'bearish').length;
    const avgStrength = trends.length > 0
        ? (trends.reduce((sum, t) => sum + t.strength, 0) / trends.length)
        : 0;

    return (
        <div style={s.container}>
            {/* Header */}
            <div style={s.header}>
                <div style={s.title}>TREND TRACKER</div>
                <div style={s.subtitle}>Divergence analysis across market trends</div>
            </div>

            {/* Lookback selector */}
            <div style={s.lookbackRow}>
                {LOOKBACK_OPTIONS.map(d => (
                    <button
                        key={d}
                        style={s.lookbackBtn(d === lookbackDays)}
                        onClick={() => setLookbackDays(d)}
                    >
                        {d}D
                    </button>
                ))}
            </div>

            {/* Score row */}
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr 1fr', gap: '10px', marginTop: '16px' }}>
                <div style={{ ...shared.cardGradient, textAlign: 'center', padding: '14px 12px' }}>
                    <div style={{ fontSize: '28px', fontWeight: 800, fontFamily: mono, color: colors.green }}>
                        {bullishCount}
                    </div>
                    <div style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', color: colors.textMuted, fontFamily: mono, marginTop: '2px' }}>
                        BULLISH
                    </div>
                </div>
                <div style={{ ...shared.cardGradient, textAlign: 'center', padding: '14px 12px' }}>
                    <div style={{ fontSize: '28px', fontWeight: 800, fontFamily: mono, color: colors.red }}>
                        {bearishCount}
                    </div>
                    <div style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', color: colors.textMuted, fontFamily: mono, marginTop: '2px' }}>
                        BEARISH
                    </div>
                </div>
                <div style={{ ...shared.cardGradient, textAlign: 'center', padding: '14px 12px' }}>
                    <div style={{ fontSize: '28px', fontWeight: 800, fontFamily: mono, color: colors.yellow }}>
                        {(avgStrength * 100).toFixed(0)}%
                    </div>
                    <div style={{ fontSize: '10px', fontWeight: 700, letterSpacing: '1.5px', color: colors.textMuted, fontFamily: mono, marginTop: '2px' }}>
                        AVG STRENGTH
                    </div>
                </div>
            </div>

            {/* Category Summary Cards */}
            <div style={s.sectionTitle}>TREND CATEGORIES</div>
            <div style={s.catGrid}>
                {CATEGORIES.map(cat => {
                    const summary = category_summaries?.[cat.key] || {};
                    const direction = summary.direction || 'neutral';
                    const isActive = filterCategory === cat.key;

                    return (
                        <div
                            key={cat.key}
                            className="trend-cat-card"
                            style={{
                                ...s.catCard(direction),
                                ...(isActive ? { borderColor: colors.accent, borderWidth: '1px', borderStyle: 'solid' } : {}),
                            }}
                            onClick={() => handleCategoryClick(cat.key)}
                        >
                            <div style={s.catLabel}>
                                {cat.icon} {cat.label.toUpperCase()}
                            </div>
                            <div style={s.catHeadline}>
                                {summary.headline || 'No data'}
                            </div>
                            <div style={s.catMetrics}>
                                <div style={s.dirBadge(direction)}>
                                    {DIRECTION_ARROWS[direction]} {direction.toUpperCase()}
                                </div>
                                <StrengthMeter strength={summary.strength || 0} direction={direction} width={50} />
                            </div>
                            <div style={{ ...s.catCount, marginTop: '6px' }}>
                                {summary.trend_count || 0} trend{(summary.trend_count || 0) !== 1 ? 's' : ''}
                            </div>
                        </div>
                    );
                })}
            </div>

            {/* Tabs */}
            <div style={s.tabs}>
                {['trends', 'chart', 'narrative'].map(tab => (
                    <button key={tab} style={s.tab(activeTab === tab)} onClick={() => setActiveTab(tab)}>
                        {tab === 'trends' ? 'Active Trends' : tab === 'chart' ? 'Comparison' : 'Narrative'}
                    </button>
                ))}
            </div>

            {/* Active Trends Tab */}
            {activeTab === 'trends' && (
                <div>
                    {/* Category filter */}
                    <div style={s.filterRow}>
                        <button
                            style={s.filterBtn(!filterCategory)}
                            onClick={() => setFilterCategory(null)}
                        >
                            ALL ({trends.length})
                        </button>
                        {CATEGORIES.map(cat => {
                            const count = trends.filter(t => t.category === cat.key).length;
                            if (count === 0) return null;
                            return (
                                <button
                                    key={cat.key}
                                    style={s.filterBtn(filterCategory === cat.key)}
                                    onClick={() => setFilterCategory(prev => prev === cat.key ? null : cat.key)}
                                >
                                    {cat.label} ({count})
                                </button>
                            );
                        })}
                    </div>

                    {/* Trend cards */}
                    {filteredTrends.map((trend, i) => {
                        const globalIdx = trends.indexOf(trend);
                        const isExpanded = expandedTrend === globalIdx;

                        return (
                            <div key={globalIdx} className="trend-card-enter" style={s.trendCard}>
                                {/* Header — always visible */}
                                <div style={s.trendHeader} onClick={() => toggleTrend(globalIdx)}>
                                    <div style={s.trendLeft}>
                                        <div style={s.dirBadge(trend.direction)}>
                                            {DIRECTION_ARROWS[trend.direction]}
                                        </div>
                                        <div style={s.trendName}>{trend.name}</div>
                                    </div>
                                    <div style={s.trendRight}>
                                        <StrengthMeter strength={trend.strength} direction={trend.direction} width={50} />
                                        <div style={s.confidenceBadge}>
                                            {(trend.confidence * 100).toFixed(0)}%
                                        </div>
                                        <div style={s.expandIcon(isExpanded)}>
                                            {'\u25BC'}
                                        </div>
                                    </div>
                                </div>

                                {/* Body — expanded */}
                                {isExpanded && (
                                    <div style={s.trendBody}>
                                        {/* Description */}
                                        <div style={s.trendDesc}>{trend.description}</div>

                                        {/* Evidence columns */}
                                        <div style={s.evidenceGrid}>
                                            <div style={s.evidenceCol}>
                                                <div style={s.evidenceTitle(true)}>SUPPORTING EVIDENCE</div>
                                                {(trend.supporting_evidence || []).map((ev, j) => (
                                                    <div key={j} style={s.evidenceItem}>
                                                        <div style={s.evidenceDot(true)} />
                                                        <span>{ev}</span>
                                                    </div>
                                                ))}
                                                {(!trend.supporting_evidence || trend.supporting_evidence.length === 0) && (
                                                    <div style={{ ...s.evidenceItem, color: colors.textMuted }}>None</div>
                                                )}
                                            </div>
                                            <div style={s.evidenceCol}>
                                                <div style={s.evidenceTitle(false)}>CONTRADICTING EVIDENCE</div>
                                                {(trend.contradicting_evidence || []).map((ev, j) => (
                                                    <div key={j} style={s.evidenceItem}>
                                                        <div style={s.evidenceDot(false)} />
                                                        <span>{ev}</span>
                                                    </div>
                                                ))}
                                                {(!trend.contradicting_evidence || trend.contradicting_evidence.length === 0) && (
                                                    <div style={{ ...s.evidenceItem, color: colors.textMuted }}>None</div>
                                                )}
                                            </div>
                                        </div>

                                        {/* Implications */}
                                        {trend.implications && trend.implications.length > 0 && (
                                            <div style={s.implRow}>
                                                <div style={s.implTitle}>IMPLICATIONS</div>
                                                {trend.implications.map((imp, j) => (
                                                    <div key={j} style={s.implItem}>{'\u2192'} {imp}</div>
                                                ))}
                                            </div>
                                        )}

                                        {/* Affected tickers */}
                                        {trend.tickers_affected && trend.tickers_affected.length > 0 && (
                                            <div style={s.tickerRow}>
                                                {trend.tickers_affected.map(t => (
                                                    <span key={t} style={s.tickerPill}>{t}</span>
                                                ))}
                                            </div>
                                        )}

                                        {/* Timeline */}
                                        <div style={s.timeline}>
                                            <div style={s.timelineLabel}>
                                                Started: {trend.started || 'Unknown'}
                                            </div>
                                            <div style={s.timelineLabel}>
                                                Strength: {(trend.strength * 100).toFixed(0)}% | Confidence: {(trend.confidence * 100).toFixed(0)}%
                                            </div>
                                        </div>
                                    </div>
                                )}
                            </div>
                        );
                    })}

                    {filteredTrends.length === 0 && (
                        <div style={{ ...shared.card, textAlign: 'center', padding: '30px', color: colors.textMuted, fontFamily: mono, fontSize: '13px' }}>
                            No trends detected for this category.
                        </div>
                    )}
                </div>
            )}

            {/* Trend Comparison Chart Tab */}
            {activeTab === 'chart' && (
                <div style={s.chartWrap}>
                    <div style={{ ...s.sectionTitle, marginTop: 0 }}>CURRENT TREND STRENGTH</div>
                    <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, marginBottom: '8px' }}>
                        Strength history is not available yet; the trends API reports current strength only.
                    </div>

                    {/* Toggles */}
                    <div style={s.chartToggleRow}>
                        {trends.map((trend, i) => {
                            const color = CHART_COLORS[i % CHART_COLORS.length];
                            return (
                                <button
                                    key={i}
                                    style={s.chartToggle(visibleChartTrends.has(i), color)}
                                    onClick={() => toggleChartTrend(i)}
                                >
                                    {DIRECTION_ARROWS[trend.direction]} {trend.name.length > 25 ? trend.name.slice(0, 25) + '...' : trend.name}
                                </button>
                            );
                        })}
                    </div>

                    <TrendComparisonChart
                        trends={trends}
                        visibleTrends={visibleChartTrends}
                        width={700}
                        height={220}
                    />

                    {/* Legend */}
                    <div style={{ display: 'flex', gap: '12px', flexWrap: 'wrap', marginTop: '10px' }}>
                        {trends.filter((_, i) => visibleChartTrends.has(i)).map((trend) => {
                            const origIdx = trends.indexOf(trend);
                            const color = CHART_COLORS[origIdx % CHART_COLORS.length];
                            return (
                                <div key={origIdx} style={{ display: 'flex', alignItems: 'center', gap: '4px' }}>
                                    <div style={{ width: '12px', height: '3px', background: color, borderRadius: '1px' }} />
                                    <span style={{ fontSize: '9px', fontFamily: mono, color: colors.textMuted }}>
                                        {trend.name.length > 30 ? trend.name.slice(0, 30) + '...' : trend.name}
                                    </span>
                                </div>
                            );
                        })}
                    </div>
                </div>
            )}

            {/* Narrative Tab */}
            {activeTab === 'narrative' && (
                <div style={s.narrativePanel}>
                    <div style={{ ...s.sectionTitle, marginTop: 0 }}>WHAT THE TRENDS ARE TELLING US</div>
                    <div style={s.narrativeText}>
                        {narrative || 'No narrative available.'}
                    </div>

                    {/* Convergence / Divergence indicator */}
                    <div style={{ marginTop: '16px', padding: '12px', background: colors.bg, borderRadius: tokens.radius.md }}>
                        <div style={{ fontSize: '9px', fontWeight: 700, letterSpacing: '1.5px', fontFamily: mono, color: colors.yellow, marginBottom: '6px' }}>
                            SIGNAL ALIGNMENT
                        </div>
                        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
                            <div style={{
                                width: '100%', height: '6px', background: colors.border,
                                borderRadius: '3px', position: 'relative', overflow: 'hidden',
                            }}>
                                {/* Bullish portion */}
                                <div style={{
                                    position: 'absolute', left: 0, top: 0, height: '100%',
                                    width: `${trends.length > 0 ? (bullishCount / trends.length * 100) : 0}%`,
                                    background: colors.green, borderRadius: '3px 0 0 3px',
                                }} />
                                {/* Bearish portion */}
                                <div style={{
                                    position: 'absolute', right: 0, top: 0, height: '100%',
                                    width: `${trends.length > 0 ? (bearishCount / trends.length * 100) : 0}%`,
                                    background: colors.red, borderRadius: '0 3px 3px 0',
                                }} />
                            </div>
                        </div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginTop: '4px' }}>
                            <span style={{ fontSize: '10px', fontFamily: mono, color: colors.green }}>
                                {bullishCount} Bullish
                            </span>
                            <span style={{ fontSize: '10px', fontFamily: mono, color: colors.textMuted }}>
                                {trends.length - bullishCount - bearishCount} Mixed
                            </span>
                            <span style={{ fontSize: '10px', fontFamily: mono, color: colors.red }}>
                                {bearishCount} Bearish
                            </span>
                        </div>
                    </div>
                </div>
            )}

            {/* Generated timestamp */}
            {data.generated_at && (
                <div style={{ fontSize: '10px', color: colors.textMuted, fontFamily: mono, textAlign: 'right', marginTop: '12px' }}>
                    Generated: {formatDateTime(data.generated_at)}
                </div>
            )}
        </div>
    );
}
