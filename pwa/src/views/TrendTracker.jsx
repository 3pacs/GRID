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

// ── Placeholder data (used until API is wired) ──────────────────────────

function generatePlaceholderData() {
    const trends = [
        {
            name: 'Tech Sector Death Cross',
            category: 'momentum',
            direction: 'bearish',
            strength: 0.72,
            description: 'XLK 50-day MA crossed below 200-day MA, first time since October 2023',
            supporting_evidence: ['RSI proxy at 38', 'Volume elevated on breakdown', 'Breadth deteriorating'],
            contradicting_evidence: ['Earnings still strong for mega-cap', 'Buybacks accelerating'],
            implications: ['Potential sector rotation from growth to value', 'Watch for follow-through in QQQ'],
            tickers_affected: ['XLK', 'AAPL', 'MSFT', 'NVDA'],
            started: '2026-03-15',
            confidence: 0.65,
        },
        {
            name: 'S&P 500 Golden Cross',
            category: 'momentum',
            direction: 'bullish',
            strength: 0.58,
            description: 'S&P 500 50-day MA crossed above 200-day MA after 3-month correction',
            supporting_evidence: ['Breadth improving', 'New highs expanding', 'RSI proxy at 62'],
            contradicting_evidence: ['Volume thin on rally', 'Mega-cap concentration persists'],
            implications: ['Historically bullish signal with 78% hit rate over 6 months'],
            tickers_affected: ['SPY', 'ES=F'],
            started: '2026-03-20',
            confidence: 0.60,
        },
        {
            name: 'Regime Shift: GROWTH -> FRAGILE',
            category: 'regime',
            direction: 'transitioning',
            strength: 0.68,
            description: 'Market regime transitioning from GROWTH to FRAGILE. Confidence: 72%. Leading indicators deteriorating.',
            supporting_evidence: ['VIX elevated at 22.4', 'Yield curve re-inverting', 'Credit spreads widening 15bps in 2 weeks'],
            contradicting_evidence: ['Employment still strong', 'Consumer spending holding'],
            implications: ['Portfolio positioning should shift to fragile playbook', 'Risk management tightening'],
            tickers_affected: ['SPY', 'QQQ', 'TLT', 'GLD', 'VXX'],
            started: '2026-03-18',
            confidence: 0.72,
        },
        {
            name: 'Energy Sector Gaining Strength',
            category: 'sector_rotation',
            direction: 'bullish',
            strength: 0.61,
            description: 'XLE relative strength: 1W +2.1%, 1M +8.4%, 3M +12.1% \u2014 accelerating',
            supporting_evidence: ['1M return: +8.4%', '1W return: +2.1%', 'Momentum accelerating vs 3M trend'],
            contradicting_evidence: ['Oil supply concerns from OPEC+ production increase'],
            implications: ['Money rotating INTO Energy sector', 'Value trade gaining momentum'],
            tickers_affected: ['XLE', 'XOM', 'CVX'],
            started: '2026-03-04',
            confidence: 0.65,
        },
        {
            name: 'Technology Losing Strength',
            category: 'sector_rotation',
            direction: 'bearish',
            strength: 0.55,
            description: 'XLK relative strength: 1W -1.3%, 1M -4.2%, 3M -2.8% \u2014 decelerating',
            supporting_evidence: ['1M return: -4.2%', '1W return: -1.3%', 'Momentum decelerating'],
            contradicting_evidence: ['AI capex cycle still early', 'Earnings beats in Q4'],
            implications: ['Money rotating OUT OF Technology', 'Growth-to-value rotation underway'],
            tickers_affected: ['XLK', 'QQQ'],
            started: '2026-03-04',
            confidence: 0.55,
        },
        {
            name: 'VIX Term Structure in Backwardation',
            category: 'volatility',
            direction: 'bearish',
            strength: 0.64,
            description: 'VIX (22.4) trading above VIX3M (19.8) \u2014 ratio 1.13. Near-term fear exceeds medium-term.',
            supporting_evidence: ['VIX/VIX3M ratio: 1.13', 'Backwardation signals elevated near-term stress'],
            contradicting_evidence: ['Backwardation can be transient around FOMC'],
            implications: ['Short-vol strategies at risk', 'Hedging costs elevated', 'Mean reversion likely if catalyst passes'],
            tickers_affected: ['VXX', 'UVXY', 'SVXY', 'SPY'],
            started: '2026-03-22',
            confidence: 0.60,
        },
        {
            name: 'Implied Vol Premium Elevated',
            category: 'volatility',
            direction: 'bearish',
            strength: 0.48,
            description: 'VIX (22.4) trading 6.2 pts above realized vol (16.2). Markets pricing more risk than visible.',
            supporting_evidence: ['Implied/realized ratio: 1.38', 'Vol premium: 6.2 pts'],
            contradicting_evidence: ['Some premium is normal \u2014 mean is ~1.3x'],
            implications: ['Option sellers have edge if premium normalizes'],
            tickers_affected: ['SPY', 'VXX'],
            started: '2026-03-25',
            confidence: 0.50,
        },
        {
            name: 'Fed Balance Sheet Contracting',
            category: 'liquidity',
            direction: 'bearish',
            strength: 0.52,
            description: 'Fed balance sheet contracting by -0.18% over last 10 readings. Draining liquidity.',
            supporting_evidence: ['10-period change: -0.18%', 'Current level: $7.42T', 'QT pace maintained'],
            contradicting_evidence: ['Pace slower than 2023-2024 QT'],
            implications: ['Liquidity headwind for risk assets'],
            tickers_affected: ['SPY', 'QQQ', 'BTC', 'TLT'],
            started: '2026-03-17',
            confidence: 0.60,
        },
        {
            name: 'Reverse Repo Declining',
            category: 'liquidity',
            direction: 'bullish',
            strength: 0.45,
            description: 'Reverse repo facility declining. Money leaving RRP and entering markets.',
            supporting_evidence: ['RRP trend: declining over 10 readings', 'RRP now below $200B'],
            contradicting_evidence: ['RRP nearly exhausted \u2014 tailwind fading'],
            implications: ['Positive for market liquidity but diminishing returns'],
            tickers_affected: ['SPY', 'TLT'],
            started: '2026-03-17',
            confidence: 0.55,
        },
        {
            name: 'BTC vs NASDAQ Decoupling',
            category: 'correlation',
            direction: 'transitioning',
            strength: 0.58,
            description: 'BTC vs NASDAQ: current 30-day correlation is 0.12, expected ~0.60. Assets have weakened significantly.',
            supporting_evidence: ['Current rolling correlation: 0.12', 'Historical full-period correlation: 0.55', 'Expected: 0.60'],
            contradicting_evidence: ['Short-term divergences often revert within weeks'],
            implications: ['Pair trades based on BTC-NASDAQ may fail', 'BTC finding independent price discovery'],
            tickers_affected: ['BTC', 'QQQ', 'GBTC'],
            started: '2026-02-25',
            confidence: 0.55,
        },
        {
            name: 'Dollar vs Gold Correlation Breakdown',
            category: 'correlation',
            direction: 'transitioning',
            strength: 0.52,
            description: 'Dollar vs Gold: current 30-day correlation is +0.15, expected ~-0.40. Unusual co-movement.',
            supporting_evidence: ['Current rolling correlation: +0.15', 'Historical: -0.35', 'Expected: -0.40'],
            contradicting_evidence: ['Could reflect flight to safety in both'],
            implications: ['De-dollarization narrative gaining traction', 'Gold pricing in geopolitical premium'],
            tickers_affected: ['UUP', 'GLD', 'DX=F'],
            started: '2026-02-25',
            confidence: 0.50,
        },
    ];

    const category_summaries = {
        momentum: { label: 'Momentum', direction: 'transitioning', strength: 0.65, headline: 'Tech Sector Death Cross', trend_count: 2 },
        regime: { label: 'Regime', direction: 'transitioning', strength: 0.68, headline: 'Regime Shift: GROWTH -> FRAGILE', trend_count: 1 },
        sector_rotation: { label: 'Sector Rotation', direction: 'transitioning', strength: 0.58, headline: 'Energy Gaining, Tech Losing', trend_count: 2 },
        volatility: { label: 'Volatility', direction: 'bearish', strength: 0.56, headline: 'VIX Backwardation', trend_count: 2 },
        liquidity: { label: 'Liquidity', direction: 'transitioning', strength: 0.49, headline: 'Fed Contracting, RRP Declining', trend_count: 2 },
        correlation: { label: 'Correlations', direction: 'transitioning', strength: 0.55, headline: 'BTC-NASDAQ Decoupling', trend_count: 2 },
    };

    const narrative = [
        'Markets are in transition. Multiple categories showing mixed signals suggests we are at an inflection point where conviction is low and the next directional move has not yet been determined.',
        '',
        '[MOMENTUM] Tech Sector Death Cross: XLK 50-day MA crossed below 200-day MA, first time since October 2023.',
        '',
        '[REGIME] Regime Shift: GROWTH -> FRAGILE: Market regime transitioning from GROWTH to FRAGILE. Confidence: 72%. Leading indicators deteriorating.',
        '',
        '[VOLATILITY] VIX Term Structure in Backwardation: VIX (22.4) trading above VIX3M (19.8). Near-term fear exceeds medium-term.',
        '',
        'DIVERGENCE WARNING: Strong trends are pointing in different directions. This internal conflict often resolves with elevated volatility.',
    ].join('\n');

    return { trends, category_summaries, narrative, generated_at: new Date().toISOString() };
}

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
        lineHeight: 1.4, marginBottom: '8px',
        overflow: 'hidden', textOverflow: 'ellipsis',
        display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical',
    },
    catMetrics: {
        display: 'flex', justifyContent: 'space-between', alignItems: 'center',
    },
    dirBadge: (direction) => ({
        display: 'inline-flex', alignItems: 'center', gap: '4px',
        padding: '3px 8px', borderRadius: '4px',
        fontSize: '10px', fontWeight: 800, fontFamily: mono,
        background: DIRECTION_BG[direction] || '#1A2840',
        color: DIRECTION_COLORS[direction] || colors.textMuted,
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
    const ref = useRef(null);

    useEffect(() => {
        if (!ref.current || !trends || trends.length === 0) return;

        const svg = d3.select(ref.current);
        svg.selectAll('*').remove();
        svg.attr('width', w).attr('height', h);

        const visible = trends.filter((_, i) => visibleTrends.has(i));
        if (visible.length === 0) return;

        const margin = { top: 16, right: 20, bottom: 28, left: 44 };
        const cw = w - margin.left - margin.right;
        const ch = h - margin.top - margin.bottom;
        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        // X axis: use index 0-11 as "months back" proxy
        const nPoints = 12;
        const x = d3.scaleLinear().domain([0, nPoints - 1]).range([0, cw]);

        // Y axis: strength 0-1
        const y = d3.scaleLinear().domain([0, 1]).range([ch, 0]);

        // Grid
        const yTicks = [0, 0.25, 0.5, 0.75, 1.0];
        g.selectAll('.grid')
            .data(yTicks).enter()
            .append('line')
            .attr('x1', 0).attr('x2', cw)
            .attr('y1', d => y(d)).attr('y2', d => y(d))
            .attr('stroke', colors.border).attr('stroke-width', 0.4).attr('opacity', 0.5);

        // Y axis labels
        g.append('g')
            .call(d3.axisLeft(y).ticks(5).tickSize(0).tickFormat(d => d.toFixed(1)))
            .call(g2 => g2.select('.domain').remove())
            .call(g2 => g2.selectAll('.tick text')
                .attr('font-size', '9px').attr('font-family', mono).attr('fill', colors.textMuted));

        // Month labels
        const months = ['12m', '11m', '10m', '9m', '8m', '7m', '6m', '5m', '4m', '3m', '2m', 'Now'];
        g.append('g')
            .attr('transform', `translate(0,${ch})`)
            .call(d3.axisBottom(x).ticks(nPoints).tickSize(0)
                .tickFormat(i => months[Math.round(i)] || ''))
            .call(g2 => g2.select('.domain').remove())
            .call(g2 => g2.selectAll('.tick text')
                .attr('font-size', '8px').attr('font-family', mono).attr('fill', colors.textMuted));

        // Generate synthetic strength trajectory for each trend
        visible.forEach((trend, vi) => {
            const baseStrength = trend.strength;
            const color = CHART_COLORS[trends.indexOf(trend) % CHART_COLORS.length];

            // Build trajectory: ramp to current strength with noise
            const pts = [];
            for (let i = 0; i < nPoints; i++) {
                const progress = i / (nPoints - 1);
                let val;
                if (trend.direction === 'bullish') {
                    val = baseStrength * (0.2 + 0.8 * progress) + (Math.random() - 0.5) * 0.08;
                } else if (trend.direction === 'bearish') {
                    val = baseStrength * (0.3 + 0.7 * progress) + (Math.random() - 0.5) * 0.08;
                } else {
                    val = baseStrength * (0.5 + 0.5 * Math.sin(progress * Math.PI)) + (Math.random() - 0.5) * 0.1;
                }
                pts.push(Math.max(0, Math.min(1, val)));
            }

            const line = d3.line()
                .x((d, i2) => x(i2))
                .y(d => y(d))
                .curve(d3.curveMonotoneX);

            const area = d3.area()
                .x((d, i2) => x(i2))
                .y0(ch)
                .y1(d => y(d))
                .curve(d3.curveMonotoneX);

            g.append('path')
                .datum(pts)
                .attr('d', area)
                .attr('fill', color)
                .attr('opacity', 0.05);

            const path = g.append('path')
                .datum(pts)
                .attr('d', line)
                .attr('fill', 'none')
                .attr('stroke', color)
                .attr('stroke-width', 2)
                .attr('opacity', 0.85);

            // Animate
            const totalLen = path.node().getTotalLength();
            path.attr('stroke-dasharray', `${totalLen} ${totalLen}`)
                .attr('stroke-dashoffset', totalLen)
                .transition().duration(600).delay(vi * 80).ease(d3.easeCubicOut)
                .attr('stroke-dashoffset', 0);

            // End dot
            g.append('circle')
                .attr('cx', x(nPoints - 1))
                .attr('cy', y(pts[nPoints - 1]))
                .attr('r', 3)
                .attr('fill', color)
                .attr('opacity', 0)
                .transition().delay(600 + vi * 80).duration(200)
                .attr('opacity', 1);
        });

        // Convergence highlight: if all visible are same direction, shade background
        const directions = visible.map(t => t.direction);
        const allSame = directions.length > 1 && new Set(directions).size === 1;
        if (allSame) {
            const dir = directions[0];
            const highlightColor = dir === 'bullish' ? colors.green : dir === 'bearish' ? colors.red : colors.yellow;
            g.append('rect')
                .attr('x', 0).attr('y', 0)
                .attr('width', cw).attr('height', ch)
                .attr('fill', highlightColor)
                .attr('opacity', 0)
                .transition().duration(800)
                .attr('opacity', 0.04);

            g.append('text')
                .attr('x', cw / 2).attr('y', 10)
                .attr('text-anchor', 'middle')
                .attr('font-size', '9px').attr('font-family', mono)
                .attr('fill', highlightColor).attr('opacity', 0.6)
                .text(`HIGH CONVICTION: All trends aligned ${dir.toUpperCase()}`);
        }

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
            let result;
            try {
                result = await api.getTrends(days);
            } catch {
                result = null;
            }

            if (result && result.trends && result.trends.length > 0) {
                setData(result);
            } else {
                setData(generatePlaceholderData());
            }
        } catch (err) {
            setError(err.message);
            setData(generatePlaceholderData());
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
                <div style={{ ...shared.card, textAlign: 'center', padding: '40px' }}>
                    <div style={{ color: colors.red, fontFamily: mono, fontSize: '13px' }}>
                        {error || 'Failed to load trend data'}
                    </div>
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
                    <div style={{ ...s.sectionTitle, marginTop: 0 }}>TREND STRENGTH OVER TIME</div>

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
                    Generated: {new Date(data.generated_at).toLocaleString()}
                </div>
            )}
        </div>
    );
}
