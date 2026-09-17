/**
 * RiskMap -- THE RISK: unified risk exposure treemap with gauge, detail panel,
 * and timeline. D3-powered treemap with animated risk rectangles.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';
import { formatDate } from '../utils/formatTime.js';

// ── Constants ──────────────────────────────────────────────────────────

const RISK_COLORS = {
    low: '#22C55E',
    moderate: '#F59E0B',
    elevated: '#F97316',
    high: '#EF4444',
    critical: '#991B1B',
    // Sub-system reported no data (backend `available: false`). Rendered
    // grey and never as a default 'moderate' reading.
    unknown: '#64748B',
};

const RISK_WEIGHTS = {
    critical: 6,
    high: 5,
    elevated: 4,
    moderate: 3,
    low: 2,
    unknown: 1,
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
    const hasScore = typeof score === 'number' && Number.isFinite(score);
    const scoreVal = hasScore ? Math.round(score * 100) : null;

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

        if (scoreVal === null) {
            // No overall score (every sub-system unavailable): draw the empty
            // dial and say so instead of animating a needle to 0.
            const arcEmpty = d3.arc()
                .innerRadius(innerR).outerRadius(outerR)
                .startAngle(-Math.PI / 2).endAngle(Math.PI / 2);
            svg.append('path')
                .attr('d', arcEmpty())
                .attr('transform', `translate(${cx},${cy})`)
                .attr('fill', colors.border)
                .attr('opacity', 0.4);
            svg.append('text')
                .attr('x', cx).attr('y', cy - 25)
                .attr('text-anchor', 'middle')
                .attr('font-size', '14px')
                .attr('font-family', colors.mono)
                .attr('fill', colors.textMuted)
                .text('UNAVAILABLE');
            return;
        }

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
            const level = cat.risk_level || 'unknown';
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
    const level = cat.risk_level || 'unknown';
    const color = RISK_COLORS[level] || RISK_COLORS.unknown;
    const explanation = level === 'unknown'
        ? `No data for this risk sub-system${cat.reason ? ` (${cat.reason})` : ''}.`
        : (LEVEL_EXPLANATIONS[categoryKey] || {})[level] || '';

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

function RiskTimeline({ data }) {
    // The risk-map API returns a single current snapshot per category and
    // no stored history. An earlier version of this component drew a 30-day
    // "RISK TIMELINE" from a random walk anchored to the current level, with
    // real calendar dates, hover readings and a convergence band -- fabricated
    // history presented as observations. Until a backend history series
    // exists, this renders only the real current levels and says plainly
    // that history is unavailable.
    const rows = CATEGORY_KEYS.map(key => {
        const cat = data?.[key] || {};
        // Backend marks an unmeasured sub-system with available:false and
        // risk_level 'unknown'; both render as UNAVAILABLE, never as a level.
        const measured = cat.available !== false && cat.risk_level && cat.risk_level !== 'unknown';
        const level = measured ? cat.risk_level : null;
        const known = level && RISK_COLORS[level];
        return { key, label: CATEGORY_LABELS[key], level, color: known ? RISK_COLORS[level] : colors.textMuted };
    });
    const asOf = data?.generated_at ? formatDate(new Date(data.generated_at)) : null;

    return (
        <div style={{ width: '100%' }}>
            <div style={{
                fontSize: '11px', color: colors.textMuted, fontFamily: colors.mono,
                padding: '10px 0 8px',
            }}>
                No 30-day risk history is stored yet. Showing the current snapshot only
                {asOf ? ` (as of ${asOf})` : ''}.
            </div>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '8px' }}>
                {rows.map(r => (
                    <div key={r.key} style={{
                        display: 'flex', alignItems: 'center', gap: '6px',
                        padding: '4px 8px', border: `1px solid ${colors.border}`,
                        borderRadius: tokens.radius.sm, fontFamily: colors.mono, fontSize: '10px',
                    }}>
                        <span style={{ width: '8px', height: '8px', borderRadius: '50%', background: r.color, display: 'inline-block' }} />
                        <span style={{ color: colors.textMuted }}>{r.label}</span>
                        <span style={{ color: r.color, fontWeight: 700 }}>
                            {r.level ? r.level.toUpperCase() : 'UNAVAILABLE'}
                        </span>
                    </div>
                ))}
            </div>
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
    const [riskSearch, setRiskSearch] = useState('');
    const [treemapZoom, setTreemapZoom] = useState(1);
    const fullScreenRef = useRef(null);
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

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

    const handleTreemapZoomIn = useCallback(() => {
        setTreemapZoom(prev => Math.min(prev * 1.3, 3));
    }, []);

    const handleTreemapZoomOut = useCallback(() => {
        setTreemapZoom(prev => Math.max(prev * 0.7, 0.5));
    }, []);

    const handleTreemapFit = useCallback(() => {
        setTreemapZoom(1);
        setSelectedCategory(null);
    }, []);

    const handleRiskSearch = useCallback((query) => {
        setRiskSearch(query);
        if (query) {
            const q = query.toLowerCase().trim();
            const match = CATEGORY_KEYS.find(k =>
                (CATEGORY_LABELS[k] || '').toLowerCase().includes(q) || k.toLowerCase().includes(q)
            );
            if (match) setSelectedCategory(match);
        }
    }, []);

    // Click-to-zoom into a category: when selected, scale treemap to emphasize it
    const effectiveTreemapZoom = selectedCategory ? Math.max(treemapZoom, 1.1) : treemapZoom;

    return (
        <div ref={fullScreenRef} style={{ ...shared.container, maxWidth: '1200px', background: isFullScreen ? colors.bg : undefined }}>
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
                                    const level = cat.risk_level || 'unknown';
                                    const rColor = RISK_COLORS[level] || RISK_COLORS.unknown;
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
                            <div style={{ position: 'relative' }}>
                                <ChartControls
                                    onZoomIn={handleTreemapZoomIn}
                                    onZoomOut={handleTreemapZoomOut}
                                    onFitScreen={handleTreemapFit}
                                    onFullScreen={toggleFullScreen}
                                    isFullScreen={isFullScreen}
                                    onSearch={handleRiskSearch}
                                    searchPlaceholder="Search risk..."
                                    compact
                                />
                                <div style={{
                                    transform: `scale(${effectiveTreemapZoom})`,
                                    transformOrigin: 'top left',
                                    transition: 'transform 0.3s ease',
                                }}>
                                    <RiskTreemap
                                        data={data}
                                        selectedCategory={selectedCategory}
                                        onSelect={handleSelect}
                                    />
                                </div>
                            </div>
                            <div style={{
                                fontSize: '9px', color: colors.textMuted,
                                fontFamily: colors.mono, marginTop: '6px',
                                textAlign: 'center',
                            }}>
                                Click a category to zoom in and view details. Size = threat level.
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

                    {/* Current snapshot per category (no stored history yet) */}
                    <div style={{ ...shared.card, marginTop: tokens.space.md }}>
                        <div style={shared.sectionTitle}>RISK LEVELS (CURRENT SNAPSHOT)</div>
                        <RiskTimeline data={data} />
                    </div>
                </>
            )}
        </div>
    );
}
