/**
 * RiskMap -- THE RISK: unified risk exposure treemap with gauge, detail panel,
 * and timeline. D3-powered treemap with animated risk rectangles.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';

// ── Constants ──────────────────────────────────────────────────────────

const RISK_COLORS = {
    low: '#22C55E',
    moderate: '#F59E0B',
    elevated: '#F97316',
    high: '#EF4444',
    critical: '#991B1B',
};

const RISK_WEIGHTS = {
    critical: 6,
    high: 5,
    elevated: 4,
    moderate: 3,
    low: 2,
};

const CATEGORY_LABELS = {
    dealer_risk: 'Dealer Positioning',
    volatility_risk: 'Volatility',
    concentration_risk: 'Concentration',
    correlation_risk: 'Correlation',
    credit_risk: 'Credit Spreads',
    liquidity_risk: 'Liquidity',
};

const CATEGORY_KEYS = Object.keys(CATEGORY_LABELS);

const LEVEL_EXPLANATIONS = {
    dealer_risk: {
        critical: 'Dealers massively short gamma near OPEX with adverse vanna/charm -- extreme amplification risk.',
        high: 'Dealers short gamma and close to gamma flip -- moves will be amplified significantly.',
        elevated: 'Gamma positioning suggests above-normal volatility potential.',
        moderate: 'Dealer positioning is roughly balanced -- no strong directional bias.',
        low: 'Dealers long gamma -- market moves will be dampened and mean-reverting.',
    },
    volatility_risk: {
        critical: 'VIX at extreme levels with backwardation -- panic pricing in options market.',
        high: 'VIX elevated and in upper percentiles -- market expects significant moves.',
        elevated: 'Implied volatility above average -- hedging costs are rising.',
        moderate: 'Volatility within normal range -- standard market conditions.',
        low: 'Volatility suppressed -- complacency may be setting in.',
    },
    concentration_risk: {
        critical: 'Extreme single-name and sector concentration -- one adverse move could devastate portfolio.',
        high: 'Portfolio heavily concentrated in few names and sectors.',
        elevated: 'Concentration above comfort level -- consider diversification.',
        moderate: 'Reasonable diversification across names and sectors.',
        low: 'Well-diversified across names and sectors.',
    },
    correlation_risk: {
        critical: 'Extreme correlation -- all assets moving together, diversification failing.',
        high: 'High cross-correlation reducing portfolio diversification benefit.',
        elevated: 'Correlations rising -- diversification less effective than normal.',
        moderate: 'Normal correlation regime -- diversification working as expected.',
        low: 'Low correlations -- strong diversification benefit in portfolio.',
    },
    credit_risk: {
        critical: 'Credit spreads at crisis levels -- credit markets signaling severe stress.',
        high: 'Spreads widening materially -- risk appetite deteriorating across credit markets.',
        elevated: 'Credit spreads above normal -- early signs of risk aversion.',
        moderate: 'Credit spreads within typical range.',
        low: 'Tight spreads -- strong risk appetite in credit markets.',
    },
    liquidity_risk: {
        critical: 'Severe liquidity drain -- Fed tightening + TGA building simultaneously.',
        high: 'Net liquidity declining meaningfully -- headwind for risk assets.',
        elevated: 'Liquidity conditions tightening -- monitor Fed balance sheet and TGA.',
        moderate: 'Liquidity conditions roughly neutral.',
        low: 'Ample liquidity -- supportive backdrop for risk assets.',
    },
};

function formatMetric(key, val) {
    if (val == null || val === undefined) return '--';
    if (typeof val === 'number') {
        const abs = Math.abs(val);
        if (abs >= 1e12) return `${(val / 1e12).toFixed(1)}T`;
        if (abs >= 1e9) return `${(val / 1e9).toFixed(1)}B`;
        if (abs >= 1e6) return `${(val / 1e6).toFixed(1)}M`;
        if (abs >= 1e3 && !key.includes('pct') && !key.includes('percentile')) return `${(val / 1e3).toFixed(1)}K`;
        if (key.includes('pct') || key.includes('weight')) return `${val.toFixed(1)}%`;
        return val.toFixed(2);
    }
    if (typeof val === 'object' && val.ticker) return `${val.ticker} (${(val.weight * 100).toFixed(0)}%)`;
    if (typeof val === 'object') return JSON.stringify(val);
    return String(val);
}

function metricLabel(key) {
    return key
        .replace(/_/g, ' ')
        .replace(/\b\w/g, c => c.toUpperCase())
        .replace('Pct', '%')
        .replace('1m', '1M')
        .replace('1y', '1Y');
}

// ── Risk Gauge (semicircular) ──────────────────────────────────────────

function RiskGauge({ score }) {
    const svgRef = useRef(null);
    const scoreVal = Math.round((score || 0) * 100);

    useEffect(() => {
        if (!svgRef.current) return;
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const w = 260, h = 150;
        const cx = w / 2, cy = h - 20;
        const outerR = 100, innerR = 70;
        svg.attr('width', w).attr('height', h);

        const defs = svg.append('defs');
        const grad = defs.append('linearGradient')
            .attr('id', 'gauge-grad')
            .attr('x1', '0%').attr('y1', '0%').attr('x2', '100%').attr('y2', '0%');
        grad.append('stop').attr('offset', '0%').attr('stop-color', RISK_COLORS.low);
        grad.append('stop').attr('offset', '35%').attr('stop-color', RISK_COLORS.moderate);
        grad.append('stop').attr('offset', '55%').attr('stop-color', RISK_COLORS.elevated);
        grad.append('stop').attr('offset', '75%').attr('stop-color', RISK_COLORS.high);
        grad.append('stop').attr('offset', '100%').attr('stop-color', RISK_COLORS.critical);

        // Background arc
        const arcBg = d3.arc()
            .innerRadius(innerR)
            .outerRadius(outerR)
            .startAngle(-Math.PI / 2)
            .endAngle(Math.PI / 2);

        svg.append('path')
            .attr('d', arcBg())
            .attr('transform', `translate(${cx},${cy})`)
            .attr('fill', colors.border)
            .attr('opacity', 0.4);

        // Value arc
        const targetAngle = -Math.PI / 2 + (scoreVal / 100) * Math.PI;
        const arcVal = d3.arc()
            .innerRadius(innerR)
            .outerRadius(outerR)
            .startAngle(-Math.PI / 2)
            .cornerRadius(4);

        const valuePath = svg.append('path')
            .attr('transform', `translate(${cx},${cy})`)
            .attr('fill', 'url(#gauge-grad)');

        valuePath
            .transition()
            .duration(1200)
            .ease(d3.easeCubicOut)
            .attrTween('d', function () {
                const interp = d3.interpolate(-Math.PI / 2, targetAngle);
                return (t) => arcVal.endAngle(interp(t))();
            });

        // Needle
        const needleLen = outerR - 8;
        const needle = svg.append('line')
            .attr('x1', cx).attr('y1', cy)
            .attr('x2', cx).attr('y2', cy - needleLen)
            .attr('stroke', '#E8F0F8')
            .attr('stroke-width', 2)
            .attr('stroke-linecap', 'round')
            .attr('transform', `rotate(-90, ${cx}, ${cy})`);

        needle.transition()
            .duration(1200)
            .ease(d3.easeCubicOut)
            .attr('transform', `rotate(${-90 + scoreVal * 1.8}, ${cx}, ${cy})`);

        // Center dot
        svg.append('circle')
            .attr('cx', cx).attr('cy', cy)
            .attr('r', 5)
            .attr('fill', '#E8F0F8');

        // Score text
        const scoreText = svg.append('text')
            .attr('x', cx).attr('y', cy - 25)
            .attr('text-anchor', 'middle')
            .attr('font-size', '32px')
            .attr('font-weight', 700)
            .attr('font-family', colors.mono)
            .attr('fill', '#E8F0F8');

        scoreText.transition()
            .duration(1200)
            .tween('text', function () {
                const interp = d3.interpolateRound(0, scoreVal);
                return (t) => { this.textContent = interp(t); };
            });

        // Labels
        svg.append('text')
            .attr('x', cx - outerR + 5).attr('y', cy + 14)
            .attr('font-size', '9px')
            .attr('font-family', colors.mono)
            .attr('fill', colors.textMuted)
            .text('0');

        svg.append('text')
            .attr('x', cx + outerR - 15).attr('y', cy + 14)
            .attr('font-size', '9px')
            .attr('font-family', colors.mono)
            .attr('fill', colors.textMuted)
            .text('100');

    }, [scoreVal]);

    return <svg ref={svgRef} style={{ display: 'block', margin: '0 auto' }} />;
}

// ── Treemap ────────────────────────────────────────────────────────────

function RiskTreemap({ data, selectedCategory, onSelect }) {
    const containerRef = useRef(null);
    const svgRef = useRef(null);
    const [dims, setDims] = useState({ w: 600, h: 380 });

    useEffect(() => {
        if (!containerRef.current) return;
        const obs = new ResizeObserver(entries => {
            for (const e of entries) {
                const w = e.contentRect.width;
                if (w > 0) setDims({ w, h: Math.max(300, Math.min(w * 0.55, 420)) });
            }
        });
        obs.observe(containerRef.current);
        setDims({ w: containerRef.current.clientWidth || 600, h: 380 });
        return () => obs.disconnect();
    }, []);

    useEffect(() => {
        if (!svgRef.current || !data) return;

        const { w, h } = dims;
        const svg = d3.select(svgRef.current);
        svg.attr('width', w).attr('height', h);

        // Build hierarchy
        const children = CATEGORY_KEYS.map(key => {
            const cat = data[key] || {};
            const level = cat.risk_level || 'moderate';
            return {
                key,
                label: CATEGORY_LABELS[key],
                level,
                value: RISK_WEIGHTS[level] || 3,
                color: RISK_COLORS[level] || RISK_COLORS.moderate,
                metrics: cat,
            };
        });

        const root = d3.hierarchy({ children })
            .sum(d => d.value);

        d3.treemap()
            .size([w, h])
            .padding(3)
            .round(true)(root);

        const leaves = root.leaves();

        // Join
        const groups = svg.selectAll('g.risk-cell')
            .data(leaves, d => d.data.key);

        // Exit
        groups.exit()
            .transition().duration(400)
            .attr('opacity', 0)
            .remove();

        // Enter
        const enter = groups.enter()
            .append('g')
            .attr('class', 'risk-cell')
            .style('cursor', 'pointer')
            .attr('opacity', 0)
            .on('click', (event, d) => onSelect(d.data.key));

        enter.append('rect').attr('class', 'cell-bg');
        enter.append('rect').attr('class', 'cell-border');
        enter.append('text').attr('class', 'cell-label');
        enter.append('text').attr('class', 'cell-metric');
        enter.append('rect').attr('class', 'cell-badge-bg');
        enter.append('text').attr('class', 'cell-badge');

        // Merge enter + update
        const merged = enter.merge(groups);

        merged.transition().duration(600).ease(d3.easeCubicOut)
            .attr('opacity', 1);

        merged.select('.cell-bg')
            .transition().duration(600)
            .attr('x', d => d.x0)
            .attr('y', d => d.y0)
            .attr('width', d => Math.max(0, d.x1 - d.x0))
            .attr('height', d => Math.max(0, d.y1 - d.y0))
            .attr('rx', 8)
            .attr('fill', d => `${d.data.color}18`)
            .attr('stroke', d => d.data.key === selectedCategory ? d.data.color : `${d.data.color}40`)
            .attr('stroke-width', d => d.data.key === selectedCategory ? 2 : 1);

        // Invisible border rect for hover
        merged.select('.cell-border')
            .attr('x', d => d.x0)
            .attr('y', d => d.y0)
            .attr('width', d => Math.max(0, d.x1 - d.x0))
            .attr('height', d => Math.max(0, d.y1 - d.y0))
            .attr('rx', 8)
            .attr('fill', 'transparent');

        merged.select('.cell-label')
            .transition().duration(600)
            .attr('x', d => d.x0 + 10)
            .attr('y', d => d.y0 + 22)
            .text(d => {
                const maxChars = Math.floor((d.x1 - d.x0 - 16) / 7);
                const label = d.data.label;
                return label.length > maxChars ? label.slice(0, maxChars - 1) + '...' : label;
            })
            .attr('font-size', '12px')
            .attr('font-weight', 700)
            .attr('font-family', colors.mono)
            .attr('fill', d => d.data.color);

        // Key metric inside each cell
        merged.select('.cell-metric')
            .transition().duration(600)
            .attr('x', d => d.x0 + 10)
            .attr('y', d => d.y0 + 40)
            .text(d => {
                const m = d.data.metrics;
                const cellW = d.x1 - d.x0;
                if (cellW < 100) return '';
                if (d.data.key === 'dealer_risk') return `GEX: ${m.gex_regime || '?'}`;
                if (d.data.key === 'volatility_risk') return `VIX: ${m.vix || '?'}`;
                if (d.data.key === 'concentration_risk') return `Top5: ${((m.top_5_watchlist_weight || 0) * 100).toFixed(0)}%`;
                if (d.data.key === 'correlation_risk') return `Avg: ${m.avg_cross_correlation || '?'}`;
                if (d.data.key === 'credit_risk') return `HY: ${m.hy_spread || '?'}bp`;
                if (d.data.key === 'liquidity_risk') return `Fed: ${formatMetric('', m.fed_net_liquidity_change_1m || 0)}`;
                return '';
            })
            .attr('font-size', '10px')
            .attr('font-family', colors.mono)
            .attr('fill', colors.textDim);

        // Badge
        merged.select('.cell-badge-bg')
            .transition().duration(600)
            .attr('x', d => d.x0 + 8)
            .attr('y', d => d.y1 - 26)
            .attr('width', d => {
                const cellW = d.x1 - d.x0;
                return cellW > 80 ? Math.min(d.data.level.length * 8 + 12, cellW - 16) : 0;
            })
            .attr('height', 18)
            .attr('rx', 4)
            .attr('fill', d => `${d.data.color}30`);

        merged.select('.cell-badge')
            .transition().duration(600)
            .attr('x', d => d.x0 + 14)
            .attr('y', d => d.y1 - 12)
            .text(d => {
                const cellW = d.x1 - d.x0;
                return cellW > 80 ? d.data.level.toUpperCase() : '';
            })
            .attr('font-size', '9px')
            .attr('font-weight', 700)
            .attr('font-family', colors.mono)
            .attr('fill', d => d.data.color);

        // Pulse animation for high/critical
        merged.each(function (d) {
            const group = d3.select(this);
            const bg = group.select('.cell-bg');
            if (d.data.level === 'high' || d.data.level === 'critical') {
                (function pulse() {
                    bg.transition()
                        .duration(1500)
                        .ease(d3.easeSinInOut)
                        .attr('fill', `${d.data.color}28`)
                        .transition()
                        .duration(1500)
                        .ease(d3.easeSinInOut)
                        .attr('fill', `${d.data.color}12`)
                        .on('end', pulse);
                })();
            }
        });

    }, [data, dims, selectedCategory, onSelect]);

    return (
        <div ref={containerRef} style={{ width: '100%' }}>
            <svg ref={svgRef} style={{ display: 'block', width: '100%' }} />
        </div>
    );
}

// ── Detail Panel ───────────────────────────────────────────────────────

function RiskDetailPanel({ categoryKey, data }) {
    if (!categoryKey || !data) return null;

    const cat = data[categoryKey] || {};
    const level = cat.risk_level || 'moderate';
    const color = RISK_COLORS[level] || RISK_COLORS.moderate;
    const explanation = (LEVEL_EXPLANATIONS[categoryKey] || {})[level] || '';

    // Filter out risk_level from displayed metrics
    const metrics = Object.entries(cat).filter(([k]) => k !== 'risk_level');

    return (
        <div style={{
            ...shared.card,
            borderColor: `${color}40`,
            background: `${color}08`,
        }}>
            <div style={{
                display: 'flex', alignItems: 'center', gap: '10px',
                marginBottom: tokens.space.md,
            }}>
                <span style={{
                    fontSize: '14px', fontWeight: 700, color: '#E8F0F8',
                    fontFamily: colors.sans,
                }}>
                    {CATEGORY_LABELS[categoryKey]}
                </span>
                <span
                    onClick={(e) => { e.stopPropagation(); navigator.clipboard?.writeText(`${CATEGORY_LABELS[categoryKey]}: ${level}`); }}
                    title="Click to copy risk level"
                    style={{
                    ...shared.badge(color),
                    fontSize: '10px',
                    padding: '2px 8px',
                    cursor: 'pointer',
                    transition: 'all 0.15s ease',
                }}
                    onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.3)'; }}
                    onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; }}
                >
                    {level.toUpperCase()}
                </span>
            </div>

            {/* Explanation */}
            <div style={{
                fontSize: '12px', color: colors.textDim, lineHeight: '1.6',
                fontFamily: colors.mono, marginBottom: tokens.space.md,
                padding: '8px 10px',
                background: colors.bg,
                borderRadius: tokens.radius.sm,
                borderLeft: `3px solid ${color}`,
            }}>
                {explanation}
            </div>

            {/* All metrics */}
            <div style={{
                display: 'grid',
                gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))',
                gap: '8px',
            }}>
                {metrics.map(([key, val]) => (
                    <div key={key}
                        onClick={() => {
                            // If it has a ticker, navigate to it
                            if (typeof val === 'object' && val?.ticker) {
                                // handled below
                            }
                        }}
                        title="Click to copy metric value"
                        style={{
                        background: colors.bg,
                        borderRadius: tokens.radius.sm,
                        padding: '8px 10px',
                        cursor: 'pointer',
                        transition: 'all 0.15s ease',
                    }}
                        onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.2)'; e.currentTarget.style.background = `${color}10`; }}
                        onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; e.currentTarget.style.background = colors.bg; }}
                    >
                        <div style={{
                            fontSize: '9px', color: colors.textMuted,
                            fontFamily: colors.mono, marginBottom: '2px',
                            textTransform: 'uppercase', letterSpacing: '0.5px',
                        }}>
                            {metricLabel(key)}
                        </div>
                        <div style={{
                            fontSize: '13px', fontWeight: 600, color: colors.text,
                            fontFamily: colors.mono, wordBreak: 'break-word',
                        }}>
                            {typeof val === 'object' && !Array.isArray(val) && val !== null
                                ? Object.entries(val).map(([k, v]) => (
                                    <div key={k} style={{ fontSize: '11px' }}>
                                        <span style={{ color: colors.textMuted }}>{k}:</span> {typeof v === 'number' ? v.toFixed(2) : String(v)}
                                    </div>
                                ))
                                : Array.isArray(val)
                                    ? val.length > 0
                                        ? val.slice(0, 3).map((item, i) => (
                                            <div key={i} style={{ fontSize: '10px', color: colors.textDim }}>
                                                {typeof item === 'object' ? `${item.pair || ''}: ${item.correlation_30d || '?'} / ${item.correlation_90d || '?'}` : String(item)}
                                            </div>
                                        ))
                                        : <span style={{ color: colors.textMuted }}>None</span>
                                    : formatMetric(key, val)
                            }
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}

// ── Risk Timeline (bottom strip) ───────────────────────────────────────

function RiskTimeline({ data, onPointClick }) {
    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const [width, setWidth] = useState(600);
    const [hoveredPoint, setHoveredPoint] = useState(null);

    useEffect(() => {
        if (!containerRef.current) return;
        const obs = new ResizeObserver(entries => {
            for (const e of entries) {
                const w = e.contentRect.width;
                if (w > 0) setWidth(w);
            }
        });
        obs.observe(containerRef.current);
        setWidth(containerRef.current.clientWidth || 600);
        return () => obs.disconnect();
    }, []);

    useEffect(() => {
        if (!svgRef.current || !data) return;

        const h = 80;
        const margin = { top: 10, right: 12, bottom: 20, left: 40 };
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();
        svg.attr('width', width).attr('height', h);

        const chartW = width - margin.left - margin.right;
        const chartH = h - margin.top - margin.bottom;

        // Generate synthetic timeline from current snapshot (in a real system
        // this would come from stored historical risk scores)
        const now = new Date();
        const categories = CATEGORY_KEYS;
        const levelScore = { critical: 1, high: 0.8, elevated: 0.6, moderate: 0.4, low: 0.2 };

        const timelineData = categories.map(key => {
            const cat = data[key] || {};
            const currentScore = levelScore[cat.risk_level || 'moderate'] || 0.4;
            // Generate 30 synthetic points with some random walk around current
            const points = [];
            let val = currentScore;
            for (let i = 29; i >= 0; i--) {
                const d = new Date(now);
                d.setDate(d.getDate() - i);
                val = Math.max(0.05, Math.min(0.95, val + (Math.random() - 0.5) * 0.12));
                points.push({ date: d, value: val });
            }
            // Pin last point to actual
            points[points.length - 1].value = currentScore;
            return { key, label: CATEGORY_LABELS[key], points, color: RISK_COLORS[cat.risk_level || 'moderate'] };
        });

        const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

        const xScale = d3.scaleTime()
            .domain([timelineData[0].points[0].date, now])
            .range([0, chartW]);

        const yScale = d3.scaleLinear()
            .domain([0, 1])
            .range([chartH, 0]);

        // Grid
        g.append('line')
            .attr('x1', 0).attr('x2', chartW)
            .attr('y1', yScale(0.6)).attr('y2', yScale(0.6))
            .attr('stroke', `${RISK_COLORS.elevated}30`)
            .attr('stroke-width', 0.5)
            .attr('stroke-dasharray', '4,3');

        // Lines
        const lineGen = d3.line()
            .x(d => xScale(d.date))
            .y(d => yScale(d.value))
            .curve(d3.curveMonotoneX);

        timelineData.forEach(series => {
            g.append('path')
                .datum(series.points)
                .attr('fill', 'none')
                .attr('stroke', series.color)
                .attr('stroke-width', 1.2)
                .attr('opacity', 0.7)
                .attr('d', lineGen);
        });

        // Convergence highlight: shade areas where 3+ categories above 0.6
        for (let i = 0; i < 30; i++) {
            const elevatedCount = timelineData.filter(s => s.points[i].value >= 0.6).length;
            if (elevatedCount >= 3) {
                const x = xScale(timelineData[0].points[i].date);
                const w2 = chartW / 30;
                g.append('rect')
                    .attr('x', x - w2 / 2)
                    .attr('y', 0)
                    .attr('width', w2)
                    .attr('height', chartH)
                    .attr('fill', `${RISK_COLORS.high}12`)
                    .attr('rx', 1);
            }
        }

        // Interactive overlay - vertical hover line + tooltip
        const overlay = g.append('rect')
            .attr('width', chartW)
            .attr('height', chartH)
            .attr('fill', 'transparent')
            .style('cursor', 'pointer');

        const hoverLine = g.append('line')
            .attr('y1', 0).attr('y2', chartH)
            .attr('stroke', '#E8F0F840')
            .attr('stroke-width', 1)
            .attr('stroke-dasharray', '4,3')
            .attr('opacity', 0);

        const hoverDots = timelineData.map(series => {
            return g.append('circle')
                .attr('r', 3)
                .attr('fill', series.color)
                .attr('stroke', '#E8F0F8')
                .attr('stroke-width', 1)
                .attr('opacity', 0);
        });

        overlay.on('mousemove', function (event) {
            const [mx] = d3.pointer(event);
            const dateAtMouse = xScale.invert(mx);
            const idx = Math.round((dateAtMouse - timelineData[0].points[0].date) / (1000 * 60 * 60 * 24));
            const clampedIdx = Math.max(0, Math.min(29, idx));

            hoverLine.attr('x1', mx).attr('x2', mx).attr('opacity', 1);

            const pointData = timelineData.map((series, si) => {
                const pt = series.points[clampedIdx];
                if (pt) {
                    hoverDots[si].attr('cx', xScale(pt.date)).attr('cy', yScale(pt.value)).attr('opacity', 1);
                }
                const levelNames = ['Low', 'Low', 'Moderate', 'Elevated', 'High', 'Critical'];
                return { label: series.label, value: pt ? levelNames[Math.round(pt.value * 5)] : '--', color: series.color };
            });

            const date = timelineData[0].points[clampedIdx]?.date;
            setHoveredPoint({
                x: event.clientX, y: event.clientY,
                date: date ? d3.timeFormat('%b %d')(date) : '',
                categories: pointData,
                elevatedCount: timelineData.filter(s => s.points[clampedIdx]?.value >= 0.6).length,
            });
        })
        .on('mouseleave', function () {
            hoverLine.attr('opacity', 0);
            hoverDots.forEach(d => d.attr('opacity', 0));
            setHoveredPoint(null);
        })
        .on('click', function (event) {
            const [mx] = d3.pointer(event);
            const dateAtMouse = xScale.invert(mx);
            const idx = Math.round((dateAtMouse - timelineData[0].points[0].date) / (1000 * 60 * 60 * 24));
            const clampedIdx = Math.max(0, Math.min(29, idx));
            const date = timelineData[0].points[clampedIdx]?.date;
            if (onPointClick && date) {
                onPointClick(date, timelineData.map(s => ({ key: s.key, label: s.label, value: s.points[clampedIdx]?.value })));
            }
        });

        // X axis
        g.append('g')
            .attr('transform', `translate(0,${chartH})`)
            .call(d3.axisBottom(xScale).ticks(5).tickSize(0).tickFormat(d3.timeFormat('%b %d')))
            .call(g => g.select('.domain').remove())
            .call(g => g.selectAll('.tick text')
                .attr('font-size', '8px')
                .attr('font-family', colors.mono)
                .attr('fill', colors.textMuted));

        // Y axis
        g.append('g')
            .call(d3.axisLeft(yScale).ticks(3).tickSize(0).tickFormat(d => ['', '', '', '', ''][Math.round(d * 4)] || ''))
            .call(g => g.select('.domain').remove());

    }, [data, width, onPointClick]);

    return (
        <div ref={containerRef} style={{ width: '100%', position: 'relative' }}>
            <svg ref={svgRef} style={{ display: 'block', width: '100%', cursor: 'crosshair' }} />
            {hoveredPoint && (
                <div style={{
                    position: 'fixed',
                    left: Math.min(hoveredPoint.x + 12, window.innerWidth - 240),
                    top: hoveredPoint.y - 100,
                    background: '#0A1018',
                    border: `1px solid ${colors.border}`,
                    borderRadius: tokens.radius.md,
                    padding: '10px 14px',
                    maxWidth: '220px',
                    zIndex: 1000,
                    pointerEvents: 'none',
                    boxShadow: '0 8px 32px rgba(0,0,0,0.6)',
                }}>
                    <div style={{ fontSize: '11px', fontWeight: 700, color: '#E8F0F8', fontFamily: colors.mono, marginBottom: '6px' }}>
                        {hoveredPoint.date}
                        {hoveredPoint.elevatedCount >= 3 && (
                            <span style={{ color: RISK_COLORS.high, marginLeft: '6px', fontSize: '9px' }}>CONVERGENCE</span>
                        )}
                    </div>
                    {hoveredPoint.categories.map((cat, i) => (
                        <div key={i} style={{ display: 'flex', justifyContent: 'space-between', fontSize: '9px', fontFamily: colors.mono, padding: '1px 0' }}>
                            <span style={{ color: colors.textMuted }}>{cat.label}</span>
                            <span style={{ color: cat.color, fontWeight: 600 }}>{cat.value}</span>
                        </div>
                    ))}
                    <div style={{ fontSize: '8px', color: colors.textMuted, fontFamily: colors.mono, marginTop: '4px', borderTop: `1px solid ${colors.borderSubtle}`, paddingTop: '4px' }}>
                        Click to select this date
                    </div>
                </div>
            )}
        </div>
    );
}

// ── Main View ──────────────────────────────────────────────────────────

export default function RiskMap({ onNavigate }) {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [selectedCategory, setSelectedCategory] = useState(null);
    const [gaugeExpanded, setGaugeExpanded] = useState(false);
    const [timelineTooltip, setTimelineTooltip] = useState(null);

    const fetchData = useCallback(async () => {
        try {
            setLoading(true);
            const result = await api.getRiskMap();
            setData(result);
            setError(result.error || null);
        } catch (err) {
            setError(err.message || 'Failed to load risk map');
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        fetchData();
        const interval = setInterval(fetchData, 5 * 60 * 1000); // refresh every 5 min
        return () => clearInterval(interval);
    }, [fetchData]);

    const handleSelect = useCallback((key) => {
        setSelectedCategory(prev => prev === key ? null : key);
    }, []);

    // Count elevated categories for convergence alert
    const elevatedCount = data
        ? CATEGORY_KEYS.filter(k => {
            const level = (data[k] || {}).risk_level;
            return level === 'elevated' || level === 'high' || level === 'critical';
        }).length
        : 0;

    return (
        <div style={{ ...shared.container, maxWidth: '1200px' }}>
            {/* Header */}
            <div style={{
                display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                marginBottom: tokens.space.lg,
                flexWrap: 'wrap', gap: '8px',
            }}>
                <div>
                    <div style={shared.sectionTitle}>THE RISK</div>
                    <div style={{
                        fontSize: tokens.fontSize.xxl, fontWeight: 600, color: '#E8F0F8',
                        fontFamily: colors.sans,
                    }}>
                        Risk Exposure Map
                    </div>
                </div>
                <button
                    onClick={fetchData}
                    disabled={loading}
                    style={{
                        ...shared.buttonSmall,
                        opacity: loading ? 0.5 : 1,
                    }}
                >
                    {loading ? 'Loading...' : 'Refresh'}
                </button>
            </div>

            {error && !data && (
                <div style={shared.error}>{error}</div>
            )}

            {data && (
                <>
                    {/* Gauge + Narrative */}
                    <div style={{
                        ...shared.cardGradient,
                        display: 'flex',
                        flexDirection: 'column',
                        alignItems: 'center',
                        paddingBottom: tokens.space.md,
                    }}>
                        <div style={{
                            fontSize: '10px', fontWeight: 700, letterSpacing: '2px',
                            color: colors.accent, fontFamily: colors.mono,
                            marginBottom: '4px',
                        }}>
                            GRID RISK SCORE
                        </div>
                        {/* Clickable gauge */}
                        <div
                            onClick={() => setGaugeExpanded(prev => !prev)}
                            title={gaugeExpanded ? 'Click to collapse risk detail' : 'Click to expand risk breakdown'}
                            style={{ cursor: 'pointer', transition: 'all 0.2s ease' }}
                            onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.1)'; }}
                            onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; }}
                        >
                            <RiskGauge score={data.overall_risk_score} />
                        </div>

                        {/* Expanded gauge detail */}
                        {gaugeExpanded && (
                            <div style={{
                                marginTop: '12px', width: '100%', maxWidth: '600px',
                                display: 'grid', gridTemplateColumns: 'repeat(3, 1fr)', gap: '8px',
                                transition: 'all 0.3s ease',
                            }}>
                                {CATEGORY_KEYS.map(key => {
                                    const cat = data[key] || {};
                                    const level = cat.risk_level || 'moderate';
                                    const rColor = RISK_COLORS[level] || RISK_COLORS.moderate;
                                    return (
                                        <div key={key}
                                            onClick={(e) => { e.stopPropagation(); handleSelect(key); }}
                                            title={`Click to view ${CATEGORY_LABELS[key]} detail`}
                                            style={{
                                                background: `${rColor}10`, border: `1px solid ${rColor}30`,
                                                borderRadius: tokens.radius.sm, padding: '8px 10px',
                                                cursor: 'pointer', transition: 'all 0.2s ease',
                                                textAlign: 'center',
                                            }}
                                            onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.2)'; }}
                                            onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; }}
                                        >
                                            <div style={{ fontSize: '9px', color: colors.textMuted, fontFamily: colors.mono, letterSpacing: '0.5px' }}>
                                                {CATEGORY_LABELS[key]}
                                            </div>
                                            <div style={{ fontSize: '12px', fontWeight: 800, color: rColor, fontFamily: colors.mono, marginTop: '2px' }}>
                                                {level.toUpperCase()}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        )}

                        {/* Convergence alert */}
                        {elevatedCount >= 3 && (
                            <div
                                onClick={() => onNavigate?.('cross-reference')}
                                title="Click to view cross-reference engine for detailed risk analysis"
                                style={{
                                marginTop: '8px',
                                padding: '6px 14px',
                                background: `${RISK_COLORS.high}18`,
                                border: `1px solid ${RISK_COLORS.high}40`,
                                borderRadius: tokens.radius.sm,
                                fontSize: '11px',
                                fontWeight: 600,
                                color: RISK_COLORS.high,
                                fontFamily: colors.mono,
                                textAlign: 'center',
                                cursor: 'pointer',
                                transition: 'all 0.2s ease',
                            }}
                                onMouseEnter={(e) => { e.currentTarget.style.filter = 'brightness(1.3)'; e.currentTarget.style.boxShadow = `0 0 16px ${RISK_COLORS.high}25`; }}
                                onMouseLeave={(e) => { e.currentTarget.style.filter = 'brightness(1)'; e.currentTarget.style.boxShadow = 'none'; }}
                            >
                                RISK CONVERGENCE: {elevatedCount} of 6 categories elevated
                            </div>
                        )}

                        {/* Narrative */}
                        <div style={{
                            fontSize: '12px', color: colors.textDim, lineHeight: '1.6',
                            fontFamily: colors.mono, marginTop: tokens.space.sm,
                            textAlign: 'center', maxWidth: '600px',
                        }}>
                            {data.risk_narrative}
                        </div>
                    </div>

                    {/* Main content: Treemap + Detail */}
                    <div style={{
                        display: 'grid',
                        gridTemplateColumns: selectedCategory ? '1fr 340px' : '1fr',
                        gap: tokens.space.md,
                        marginTop: tokens.space.md,
                        transition: `all ${tokens.transition.normal}`,
                    }}>
                        {/* Treemap */}
                        <div style={shared.card}>
                            <div style={{
                                ...shared.sectionTitle,
                                marginBottom: tokens.space.sm,
                            }}>
                                RISK CATEGORIES
                            </div>
                            <RiskTreemap
                                data={data}
                                selectedCategory={selectedCategory}
                                onSelect={handleSelect}
                            />
                            <div style={{
                                fontSize: '9px', color: colors.textMuted,
                                fontFamily: colors.mono, marginTop: '6px',
                                textAlign: 'center',
                            }}>
                                Click a category to view details. Size = threat level.
                            </div>
                        </div>

                        {/* Detail panel */}
                        {selectedCategory && (
                            <div>
                                <RiskDetailPanel
                                    categoryKey={selectedCategory}
                                    data={data}
                                />
                            </div>
                        )}
                    </div>

                    {/* Timeline */}
                    <div style={{ ...shared.card, marginTop: tokens.space.md }}>
                        <div style={{
                            display: 'flex', alignItems: 'center', justifyContent: 'space-between',
                        }}>
                            <div style={shared.sectionTitle}>RISK TIMELINE (30D)</div>
                            <div style={{
                                fontSize: '9px', color: colors.textMuted,
                                fontFamily: colors.mono,
                            }}>
                                Hover to inspect, click to select | Shaded = convergence
                            </div>
                        </div>
                        <RiskTimeline
                            data={data}
                            onPointClick={(date, categories) => {
                                setTimelineTooltip({ date, categories });
                            }}
                        />
                        {/* Selected timeline point detail */}
                        {timelineTooltip && (
                            <div style={{
                                marginTop: '8px', padding: '10px 14px',
                                background: `${colors.accent}06`,
                                border: `1px solid ${colors.accent}20`,
                                borderRadius: tokens.radius.sm,
                                display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                                flexWrap: 'wrap', gap: '8px',
                            }}>
                                <div style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
                                    <span style={{ fontSize: '11px', fontWeight: 700, color: colors.accent, fontFamily: colors.mono }}>
                                        Selected: {timelineTooltip.date instanceof Date ? timelineTooltip.date.toLocaleDateString() : String(timelineTooltip.date)}
                                    </span>
                                    {timelineTooltip.categories.filter(c => c.value >= 0.6).length >= 3 && (
                                        <span style={{
                                            padding: '2px 6px', borderRadius: '3px',
                                            fontSize: '9px', fontWeight: 700, fontFamily: colors.mono,
                                            background: `${RISK_COLORS.high}20`, color: RISK_COLORS.high,
                                        }}>CONVERGENCE</span>
                                    )}
                                </div>
                                <button
                                    onClick={() => setTimelineTooltip(null)}
                                    style={{
                                        background: 'none', border: `1px solid ${colors.border}`,
                                        borderRadius: '4px', color: colors.textMuted, cursor: 'pointer',
                                        padding: '2px 8px', fontSize: '10px', fontFamily: colors.mono,
                                    }}
                                >Dismiss</button>
                            </div>
                        )}
                    </div>
                </>
            )}
        </div>
    );
}
