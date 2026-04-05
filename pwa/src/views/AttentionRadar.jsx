/**
 * AttentionRadar — Public Attention Spike Detection view.
 *
 * Shows entities with unusual attention from Wikipedia pageviews and Google Trends.
 * Like a radar screen for public attention before it becomes price action.
 *
 * Layout:
 *   1. Alert cards — entities with score > 60, sorted descending
 *   2. Bubble chart — all tracked entities as force-directed circles
 *   3. Timeline — recent anomaly events, horizontally scrollable
 */
import React, { useState, useEffect, useRef, useCallback, useMemo } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { shared, colors, tokens, confidence as confColors } from '../styles/shared.js';
import { formatDateTime } from '../utils/formatTime.js';

// ── Constants ────────────────────────────────────────────────────────────────

const mono = "'JetBrains Mono', 'IBM Plex Mono', monospace";
const POLL_INTERVAL = 120_000; // 2 min
const HIGH_THRESHOLD = 60;
const BUBBLE_MIN_R = 14;
const BUBBLE_MAX_R = 56;

const SCORE_COLORS = [
    { stop: 0, color: '#1A3A5C' },
    { stop: 30, color: '#1A6EBF' },
    { stop: 50, color: '#F59E0B' },
    { stop: 70, color: '#EF6C00' },
    { stop: 90, color: '#EF4444' },
];

function scoreColor(score) {
    for (let i = SCORE_COLORS.length - 1; i >= 0; i--) {
        if (score >= SCORE_COLORS[i].stop) return SCORE_COLORS[i].color;
    }
    return SCORE_COLORS[0].color;
}

function priceColor(move) {
    if (move == null) return colors.textMuted;
    if (move > 0.5) return colors.green;
    if (move < -0.5) return colors.red;
    return colors.textMuted;
}

function zScoreColor(z) {
    if (z == null) return colors.accent;
    const t = Math.min(Math.abs(z) / 5, 1);
    // Interpolate blue (#1A6EBF) -> red (#EF4444)
    const r = Math.round(26 + t * (239 - 26));
    const g = Math.round(110 + t * (68 - 110));
    const b = Math.round(191 + t * (68 - 191));
    return `rgb(${r},${g},${b})`;
}

// ── Placeholder data ─────────────────────────────────────────────────────────

function generatePlaceholderAlerts() {
    return [
        { entity: 'NVIDIA Corporation', score: 92, wikipedia_zscore: 4.8, trends_breakout: 3.2, date: '2026-04-04', ticker: 'NVDA', price_move_5d: 8.4, confidence: 'confirmed' },
        { entity: 'Federal Reserve', score: 85, wikipedia_zscore: 3.9, trends_breakout: 2.8, date: '2026-04-04', ticker: null, price_move_5d: null, confidence: 'derived' },
        { entity: 'Taiwan Semiconductor', score: 78, wikipedia_zscore: 3.4, trends_breakout: 2.5, date: '2026-04-03', ticker: 'TSM', price_move_5d: -3.2, confidence: 'confirmed' },
        { entity: 'BlackRock', score: 74, wikipedia_zscore: 3.1, trends_breakout: 2.1, date: '2026-04-03', ticker: 'BLK', price_move_5d: 1.8, confidence: 'estimated' },
        { entity: 'Treasury Department', score: 71, wikipedia_zscore: 2.9, trends_breakout: 2.4, date: '2026-04-03', ticker: null, price_move_5d: null, confidence: 'derived' },
        { entity: 'Palantir Technologies', score: 68, wikipedia_zscore: 2.7, trends_breakout: 1.9, date: '2026-04-02', ticker: 'PLTR', price_move_5d: 5.1, confidence: 'confirmed' },
        { entity: 'OpenAI', score: 65, wikipedia_zscore: 2.5, trends_breakout: 2.2, date: '2026-04-02', ticker: null, price_move_5d: null, confidence: 'rumored' },
        { entity: 'Bitcoin', score: 62, wikipedia_zscore: 2.3, trends_breakout: 1.7, date: '2026-04-02', ticker: 'BTC', price_move_5d: -2.1, confidence: 'confirmed' },
        { entity: 'JPMorgan Chase', score: 55, wikipedia_zscore: 1.8, trends_breakout: 1.4, date: '2026-04-01', ticker: 'JPM', price_move_5d: 0.9, confidence: 'confirmed' },
        { entity: 'Elon Musk', score: 52, wikipedia_zscore: 1.6, trends_breakout: 1.9, date: '2026-04-01', ticker: 'TSLA', price_move_5d: -4.3, confidence: 'estimated' },
        { entity: 'SEC', score: 48, wikipedia_zscore: 1.4, trends_breakout: 1.2, date: '2026-03-31', ticker: null, price_move_5d: null, confidence: 'derived' },
        { entity: 'Apple Inc', score: 44, wikipedia_zscore: 1.1, trends_breakout: 1.0, date: '2026-03-31', ticker: 'AAPL', price_move_5d: 0.3, confidence: 'confirmed' },
        { entity: 'Citadel Securities', score: 41, wikipedia_zscore: 1.0, trends_breakout: 0.8, date: '2026-03-30', ticker: null, price_move_5d: null, confidence: 'inferred' },
        { entity: 'Saudi Aramco', score: 38, wikipedia_zscore: 0.9, trends_breakout: 1.1, date: '2026-03-30', ticker: '2222.SR', price_move_5d: -0.7, confidence: 'estimated' },
        { entity: 'China PBOC', score: 35, wikipedia_zscore: 0.7, trends_breakout: 0.9, date: '2026-03-29', ticker: null, price_move_5d: null, confidence: 'derived' },
    ];
}

// ── Styles ───────────────────────────────────────────────────────────────────

const S = {
    page: {
        background: colors.bg,
        minHeight: '100vh',
        padding: tokens.space.lg,
        fontFamily: colors.sans,
        color: colors.text,
    },
    headerRow: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'space-between',
        marginBottom: tokens.space.xl,
        flexWrap: 'wrap',
        gap: tokens.space.md,
    },
    title: {
        fontSize: '26px',
        fontWeight: 700,
        fontFamily: mono,
        letterSpacing: '-0.5px',
        background: `linear-gradient(135deg, ${colors.accent}, ${colors.accentLight || '#2A8EDF'})`,
        WebkitBackgroundClip: 'text',
        WebkitTextFillColor: 'transparent',
        backgroundClip: 'text',
    },
    subtitle: {
        fontSize: tokens.fontSize.sm,
        color: colors.textDim,
        fontFamily: mono,
        marginTop: '2px',
    },
    scanline: {
        height: '2px',
        background: `linear-gradient(90deg, transparent, ${colors.accent}44, ${colors.accent}, ${colors.accent}44, transparent)`,
        marginBottom: tokens.space.xl,
        borderRadius: '1px',
        animation: 'scanPulse 3s ease-in-out infinite',
    },
    sectionLabel: {
        ...shared.sectionTitle,
        marginBottom: tokens.space.md,
        display: 'flex',
        alignItems: 'center',
        gap: tokens.space.sm,
    },
    alertGrid: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fill, minmax(280px, 1fr))',
        gap: tokens.space.md,
        marginBottom: tokens.space.xxl,
    },
    alertCard: (borderColor) => ({
        background: colors.gradientCard,
        border: `1px solid ${colors.border}`,
        borderLeft: `3px solid ${borderColor}`,
        borderRadius: tokens.radius.md,
        padding: '14px 16px',
        cursor: 'default',
        transition: `all ${tokens.transition.fast}`,
        position: 'relative',
        overflow: 'hidden',
    }),
    alertCardHover: {
        background: colors.cardHover,
        boxShadow: `0 0 20px ${colors.accent}15`,
    },
    entityName: {
        fontSize: '14px',
        fontWeight: 600,
        color: colors.text,
        marginBottom: '6px',
        fontFamily: colors.sans,
    },
    ticker: {
        fontSize: tokens.fontSize.xs,
        fontFamily: mono,
        padding: '2px 6px',
        borderRadius: tokens.radius.sm,
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        color: colors.accent,
        marginLeft: '6px',
    },
    scoreBar: {
        height: '4px',
        borderRadius: '2px',
        background: colors.border,
        marginBottom: '8px',
        overflow: 'hidden',
    },
    scoreBarFill: (score) => ({
        height: '100%',
        width: `${score}%`,
        borderRadius: '2px',
        background: `linear-gradient(90deg, ${scoreColor(Math.max(0, score - 20))}, ${scoreColor(score)})`,
        boxShadow: `0 0 6px ${scoreColor(score)}55`,
        transition: 'width 0.6s ease',
    }),
    metricRow: {
        display: 'flex',
        gap: tokens.space.md,
        marginTop: '6px',
        flexWrap: 'wrap',
    },
    metricItem: {
        fontSize: tokens.fontSize.xs,
        color: colors.textDim,
        fontFamily: mono,
        lineHeight: '1.4',
    },
    metricVal: {
        color: colors.text,
        fontWeight: 600,
    },
    priceMove: (move) => ({
        fontSize: tokens.fontSize.sm,
        fontWeight: 700,
        fontFamily: mono,
        color: priceColor(move),
    }),
    confidenceDot: (level) => ({
        display: 'inline-block',
        width: '6px',
        height: '6px',
        borderRadius: '50%',
        background: confColors[level] || colors.textMuted,
        marginRight: '4px',
    }),
    bubbleContainer: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        marginBottom: tokens.space.xxl,
        position: 'relative',
        overflow: 'hidden',
    },
    bubbleOverlay: {
        position: 'absolute',
        top: 0,
        left: 0,
        right: 0,
        bottom: 0,
        pointerEvents: 'none',
        background: `radial-gradient(ellipse at center, transparent 40%, ${colors.bg}88 100%)`,
    },
    radarRings: {
        position: 'absolute',
        top: '50%',
        left: '50%',
        transform: 'translate(-50%, -50%)',
        pointerEvents: 'none',
    },
    tooltip: {
        position: 'absolute',
        background: colors.glassOverlay,
        backdropFilter: 'blur(12px)',
        WebkitBackdropFilter: 'blur(12px)',
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: '10px 14px',
        fontSize: tokens.fontSize.sm,
        fontFamily: mono,
        color: colors.text,
        pointerEvents: 'none',
        zIndex: 10,
        maxWidth: '260px',
        boxShadow: colors.shadow.lg,
    },
    timelineWrap: {
        background: colors.card,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.md,
        padding: '14px 0 14px 16px',
        overflowX: 'auto',
        WebkitOverflowScrolling: 'touch',
        scrollbarWidth: 'thin',
    },
    timelineTrack: {
        display: 'flex',
        gap: tokens.space.md,
        paddingRight: tokens.space.lg,
        minWidth: 'max-content',
    },
    timelineEvent: (accentColor) => ({
        flex: '0 0 auto',
        width: '200px',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderTop: `2px solid ${accentColor}`,
        borderRadius: tokens.radius.sm,
        padding: '10px 12px',
    }),
    timelineDate: {
        fontSize: tokens.fontSize.xs,
        color: colors.textMuted,
        fontFamily: mono,
        marginBottom: '4px',
    },
    timelineName: {
        fontSize: tokens.fontSize.sm,
        fontWeight: 600,
        color: colors.text,
        marginBottom: '4px',
    },
    timelineScore: {
        fontSize: tokens.fontSize.xs,
        fontFamily: mono,
        color: colors.textDim,
    },
    loading: {
        textAlign: 'center',
        padding: '60px 20px',
        color: colors.textDim,
        fontFamily: mono,
        fontSize: tokens.fontSize.md,
    },
    error: {
        ...shared.error,
        textAlign: 'center',
        padding: tokens.space.xl,
    },
    thresholdControl: {
        display: 'flex',
        alignItems: 'center',
        gap: tokens.space.sm,
        fontSize: tokens.fontSize.sm,
        color: colors.textDim,
        fontFamily: mono,
    },
    thresholdInput: {
        width: '54px',
        background: colors.bg,
        border: `1px solid ${colors.border}`,
        borderRadius: tokens.radius.sm,
        color: colors.text,
        padding: '6px 8px',
        fontSize: tokens.fontSize.sm,
        fontFamily: mono,
        textAlign: 'center',
    },
    emptyState: {
        textAlign: 'center',
        padding: '40px 20px',
        color: colors.textMuted,
        fontFamily: mono,
        fontSize: tokens.fontSize.md,
    },
};

// ── Keyframe injection ───────────────────────────────────────────────────────

const KEYFRAMES_ID = 'attention-radar-keyframes';
function ensureKeyframes() {
    if (document.getElementById(KEYFRAMES_ID)) return;
    const style = document.createElement('style');
    style.id = KEYFRAMES_ID;
    style.textContent = `
        @keyframes scanPulse {
            0%, 100% { opacity: 0.4; }
            50% { opacity: 1; }
        }
        @keyframes radarSweep {
            0% { transform: rotate(0deg); }
            100% { transform: rotate(360deg); }
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(8px); }
            to { opacity: 1; transform: translateY(0); }
        }
    `;
    document.head.appendChild(style);
}

// ── Alert Card Component ─────────────────────────────────────────────────────

function AlertCard({ alert }) {
    const [hovered, setHovered] = useState(false);
    const borderColor = alert.price_move_5d != null
        ? priceColor(alert.price_move_5d)
        : scoreColor(alert.score);

    return (
        <div
            style={{
                ...S.alertCard(borderColor),
                ...(hovered ? S.alertCardHover : {}),
                animation: 'fadeIn 0.3s ease',
            }}
            onMouseEnter={() => setHovered(true)}
            onMouseLeave={() => setHovered(false)}
        >
            <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <div style={S.entityName}>
                    {alert.entity}
                    {alert.ticker && <span style={S.ticker}>{alert.ticker}</span>}
                </div>
                <span style={{ fontSize: '18px', fontWeight: 700, fontFamily: mono, color: scoreColor(alert.score) }}>
                    {alert.score}
                </span>
            </div>

            <div style={S.scoreBar}>
                <div style={S.scoreBarFill(alert.score)} />
            </div>

            <div style={S.metricRow}>
                <div style={S.metricItem}>
                    Wiki Z: <span style={{ ...S.metricVal, color: zScoreColor(alert.wikipedia_zscore) }}>
                        {alert.wikipedia_zscore?.toFixed(1) ?? '--'}
                    </span>
                </div>
                <div style={S.metricItem}>
                    Trends: <span style={S.metricVal}>
                        {alert.trends_breakout?.toFixed(1) ?? '--'}x
                    </span>
                </div>
                {alert.price_move_5d != null && (
                    <div style={S.priceMove(alert.price_move_5d)}>
                        {alert.price_move_5d > 0 ? '+' : ''}{alert.price_move_5d.toFixed(1)}%
                    </div>
                )}
            </div>

            <div style={{ display: 'flex', alignItems: 'center', marginTop: '6px', gap: '4px' }}>
                <span style={S.confidenceDot(alert.confidence)} />
                <span style={{ fontSize: tokens.fontSize.xs, color: colors.textMuted, fontFamily: mono }}>
                    {alert.confidence || 'unknown'}
                </span>
                <span style={{ marginLeft: 'auto', fontSize: tokens.fontSize.xs, color: colors.textMuted, fontFamily: mono }}>
                    {alert.date}
                </span>
            </div>
        </div>
    );
}

// ── Bubble Chart Component ───────────────────────────────────────────────────

function BubbleChart({ alerts }) {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const [tooltipData, setTooltipData] = useState(null);
    const [tooltipPos, setTooltipPos] = useState({ x: 0, y: 0 });
    const simulationRef = useRef(null);

    const drawChart = useCallback(() => {
        const container = containerRef.current;
        const svg = svgRef.current;
        if (!container || !svg || !alerts.length) return;

        const width = container.clientWidth;
        const height = Math.max(360, Math.min(500, width * 0.5));
        const cx = width / 2;
        const cy = height / 2;

        const scoreExtent = d3.extent(alerts, d => d.score);
        const rScale = d3.scaleSqrt()
            .domain(scoreExtent)
            .range([BUBBLE_MIN_R, BUBBLE_MAX_R]);

        d3.select(svg).selectAll('*').remove();
        d3.select(svg).attr('width', width).attr('height', height);

        const defs = d3.select(svg).append('defs');

        // Radar ring gradients
        const radialGrad = defs.append('radialGradient').attr('id', 'radar-bg');
        radialGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.accent).attr('stop-opacity', 0.04);
        radialGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.bg).attr('stop-opacity', 0);

        // Background radar rings
        const g = d3.select(svg).append('g');
        const maxRing = Math.min(cx, cy) * 0.9;
        [0.25, 0.5, 0.75, 1.0].forEach(pct => {
            g.append('circle')
                .attr('cx', cx).attr('cy', cy)
                .attr('r', maxRing * pct)
                .attr('fill', 'none')
                .attr('stroke', colors.border)
                .attr('stroke-width', 0.5)
                .attr('stroke-dasharray', '4,4')
                .attr('opacity', 0.5);
        });

        // Crosshairs
        g.append('line').attr('x1', cx).attr('y1', 10).attr('x2', cx).attr('y2', height - 10)
            .attr('stroke', colors.border).attr('stroke-width', 0.5).attr('opacity', 0.3);
        g.append('line').attr('x1', 10).attr('y1', cy).attr('x2', width - 10).attr('y2', cy)
            .attr('stroke', colors.border).attr('stroke-width', 0.5).attr('opacity', 0.3);

        // Sweep line
        const sweepGroup = d3.select(svg).append('g')
            .attr('transform', `translate(${cx},${cy})`);
        const sweepGrad = defs.append('linearGradient').attr('id', 'sweep-grad')
            .attr('x1', '0').attr('y1', '0').attr('x2', '1').attr('y2', '0');
        sweepGrad.append('stop').attr('offset', '0%').attr('stop-color', colors.accent).attr('stop-opacity', 0.3);
        sweepGrad.append('stop').attr('offset', '100%').attr('stop-color', colors.accent).attr('stop-opacity', 0);
        sweepGroup.append('line')
            .attr('x1', 0).attr('y1', 0)
            .attr('x2', maxRing).attr('y2', 0)
            .attr('stroke', 'url(#sweep-grad)')
            .attr('stroke-width', 2);
        sweepGroup.style('animation', 'radarSweep 8s linear infinite')
            .style('transform-origin', 'center');

        // Nodes
        const nodes = alerts.map(d => ({
            ...d,
            r: rScale(d.score),
            x: cx + (Math.random() - 0.5) * width * 0.5,
            y: cy + (Math.random() - 0.5) * height * 0.5,
        }));

        // Bubble glow filter
        const filter = defs.append('filter').attr('id', 'bubble-glow');
        filter.append('feGaussianBlur').attr('stdDeviation', '3').attr('result', 'blur');
        filter.append('feMerge').selectAll('feMergeNode')
            .data(['blur', 'SourceGraphic']).enter()
            .append('feMergeNode').attr('in', d => d);

        const bubbleG = d3.select(svg).append('g');
        const bubbles = bubbleG.selectAll('g.bubble')
            .data(nodes)
            .enter().append('g')
            .attr('class', 'bubble')
            .style('cursor', 'pointer');

        // Outer glow
        bubbles.append('circle')
            .attr('r', d => d.r + 3)
            .attr('fill', d => zScoreColor(d.wikipedia_zscore))
            .attr('opacity', 0.15)
            .attr('filter', 'url(#bubble-glow)');

        // Main circle
        bubbles.append('circle')
            .attr('r', d => d.r)
            .attr('fill', d => zScoreColor(d.wikipedia_zscore))
            .attr('fill-opacity', 0.25)
            .attr('stroke', d => zScoreColor(d.wikipedia_zscore))
            .attr('stroke-width', 1.5)
            .attr('stroke-opacity', 0.7);

        // Label
        bubbles.append('text')
            .text(d => d.r > 20 ? (d.ticker || d.entity.split(' ')[0]) : '')
            .attr('text-anchor', 'middle')
            .attr('dy', '0.35em')
            .attr('fill', colors.text)
            .attr('font-size', d => Math.max(9, Math.min(13, d.r * 0.4)))
            .attr('font-family', mono)
            .attr('font-weight', 600)
            .attr('pointer-events', 'none');

        // Score label below
        bubbles.append('text')
            .text(d => d.r > 24 ? d.score : '')
            .attr('text-anchor', 'middle')
            .attr('dy', d => d.r * 0.4 + 12)
            .attr('fill', colors.textDim)
            .attr('font-size', '9px')
            .attr('font-family', mono)
            .attr('pointer-events', 'none');

        // Hover interactions
        bubbles.on('mouseenter', (event, d) => {
            const rect = container.getBoundingClientRect();
            setTooltipData(d);
            setTooltipPos({
                x: event.clientX - rect.left + 12,
                y: event.clientY - rect.top - 10,
            });
        }).on('mousemove', (event, d) => {
            const rect = container.getBoundingClientRect();
            setTooltipPos({
                x: event.clientX - rect.left + 12,
                y: event.clientY - rect.top - 10,
            });
        }).on('mouseleave', () => {
            setTooltipData(null);
        });

        // Force simulation
        if (simulationRef.current) simulationRef.current.stop();
        const simulation = d3.forceSimulation(nodes)
            .force('center', d3.forceCenter(cx, cy).strength(0.05))
            .force('charge', d3.forceManyBody().strength(-8))
            .force('collide', d3.forceCollide(d => d.r + 4).strength(0.8))
            .force('x', d3.forceX(cx).strength(0.03))
            .force('y', d3.forceY(cy).strength(0.03))
            .alphaDecay(0.02)
            .on('tick', () => {
                bubbles.attr('transform', d => `translate(${d.x},${d.y})`);
            });
        simulationRef.current = simulation;
    }, [alerts]);

    useEffect(() => {
        ensureKeyframes();
        drawChart();
        const handleResize = () => drawChart();
        window.addEventListener('resize', handleResize);
        return () => {
            window.removeEventListener('resize', handleResize);
            if (simulationRef.current) simulationRef.current.stop();
        };
    }, [drawChart]);

    return (
        <div ref={containerRef} style={S.bubbleContainer}>
            <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />
            <div style={S.bubbleOverlay} />
            {tooltipData && (
                <div style={{ ...S.tooltip, left: tooltipPos.x, top: tooltipPos.y }}>
                    <div style={{ fontWeight: 700, marginBottom: '4px', color: colors.text }}>
                        {tooltipData.entity}
                        {tooltipData.ticker && <span style={{ color: colors.accent, marginLeft: '6px' }}>{tooltipData.ticker}</span>}
                    </div>
                    <div>Score: <span style={{ color: scoreColor(tooltipData.score), fontWeight: 600 }}>{tooltipData.score}</span></div>
                    <div>Wiki Z-Score: <span style={{ color: zScoreColor(tooltipData.wikipedia_zscore) }}>{tooltipData.wikipedia_zscore?.toFixed(2)}</span></div>
                    <div>Trends Breakout: {tooltipData.trends_breakout?.toFixed(1)}x</div>
                    {tooltipData.price_move_5d != null && (
                        <div>5d Price: <span style={{ color: priceColor(tooltipData.price_move_5d), fontWeight: 600 }}>
                            {tooltipData.price_move_5d > 0 ? '+' : ''}{tooltipData.price_move_5d.toFixed(1)}%
                        </span></div>
                    )}
                    <div style={{ marginTop: '2px', color: colors.textMuted, fontSize: tokens.fontSize.xs }}>
                        {tooltipData.confidence} | {tooltipData.date}
                    </div>
                </div>
            )}
        </div>
    );
}

// ── Timeline Component ───────────────────────────────────────────────────────

function AnomalyTimeline({ alerts }) {
    const sorted = useMemo(
        () => [...alerts].sort((a, b) => (b.date || '').localeCompare(a.date || '')),
        [alerts]
    );

    if (!sorted.length) return null;

    return (
        <div style={S.timelineWrap}>
            <div style={S.timelineTrack}>
                {sorted.map((a, i) => (
                    <div key={`${a.entity}-${i}`} style={S.timelineEvent(scoreColor(a.score))}>
                        <div style={S.timelineDate}>{a.date}</div>
                        <div style={S.timelineName}>{a.entity}</div>
                        <div style={S.timelineScore}>
                            Score {a.score} | Z {a.wikipedia_zscore?.toFixed(1)} | {a.trends_breakout?.toFixed(1)}x
                        </div>
                        {a.price_move_5d != null && (
                            <div style={{ ...S.priceMove(a.price_move_5d), marginTop: '4px', fontSize: tokens.fontSize.xs }}>
                                {a.ticker}: {a.price_move_5d > 0 ? '+' : ''}{a.price_move_5d.toFixed(1)}%
                            </div>
                        )}
                    </div>
                ))}
            </div>
        </div>
    );
}

// ── Main View ────────────────────────────────────────────────────────────────

export default function AttentionRadar() {
    const [alerts, setAlerts] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [threshold, setThreshold] = useState(30);

    const fetchAlerts = useCallback(async () => {
        try {
            const res = await api.get(`/api/intel/attention/alerts?threshold=${threshold}`);
            setAlerts(res.alerts || []);
            setError(null);
        } catch (err) {
            console.warn('AttentionRadar: API unavailable, using placeholder data', err.message);
            setAlerts(generatePlaceholderAlerts());
            setError(null);
        } finally {
            setLoading(false);
        }
    }, [threshold]);

    useEffect(() => {
        fetchAlerts();
        const interval = setInterval(fetchAlerts, POLL_INTERVAL);
        return () => clearInterval(interval);
    }, [fetchAlerts]);

    const highAlerts = useMemo(
        () => alerts
            .filter(a => a.score >= HIGH_THRESHOLD)
            .sort((a, b) => b.score - a.score),
        [alerts]
    );

    const allSorted = useMemo(
        () => [...alerts].sort((a, b) => b.score - a.score),
        [alerts]
    );

    if (loading) {
        return (
            <div style={S.page}>
                <div style={S.loading}>
                    <span style={{ animation: 'scanPulse 1.5s ease-in-out infinite' }}>
                        SCANNING ATTENTION SIGNALS...
                    </span>
                </div>
            </div>
        );
    }

    return (
        <div style={S.page}>
            {/* Header */}
            <div style={S.headerRow}>
                <div>
                    <div style={S.title}>ATTENTION RADAR</div>
                    <div style={S.subtitle}>
                        {alerts.length} entities tracked | {highAlerts.length} high-attention alerts
                    </div>
                </div>
                <div style={S.thresholdControl}>
                    <span>MIN SCORE</span>
                    <input
                        type="number"
                        min={0}
                        max={100}
                        value={threshold}
                        onChange={e => setThreshold(Math.max(0, Math.min(100, Number(e.target.value) || 0)))}
                        style={S.thresholdInput}
                    />
                </div>
            </div>

            <div style={S.scanline} />

            {error && <div style={S.error}>{error}</div>}

            {/* High-attention alert cards */}
            {highAlerts.length > 0 && (
                <>
                    <div style={S.sectionLabel}>
                        <span style={{ color: colors.red, fontSize: '10px' }}>&#x25CF;</span>
                        HIGH ATTENTION ALERTS
                    </div>
                    <div style={S.alertGrid}>
                        {highAlerts.map((a, i) => (
                            <AlertCard key={`${a.entity}-${i}`} alert={a} />
                        ))}
                    </div>
                </>
            )}

            {/* Bubble chart */}
            <div style={S.sectionLabel}>
                <span style={{ color: colors.accent, fontSize: '10px' }}>&#x25CF;</span>
                ENTITY ATTENTION MAP
            </div>
            {allSorted.length > 0 ? (
                <BubbleChart alerts={allSorted} />
            ) : (
                <div style={S.emptyState}>No entities above threshold.</div>
            )}

            {/* Timeline */}
            <div style={S.sectionLabel}>
                <span style={{ color: colors.yellow, fontSize: '10px' }}>&#x25CF;</span>
                ANOMALY TIMELINE
            </div>
            {allSorted.length > 0 ? (
                <AnomalyTimeline alerts={allSorted} />
            ) : (
                <div style={S.emptyState}>No anomaly events to display.</div>
            )}
        </div>
    );
}
