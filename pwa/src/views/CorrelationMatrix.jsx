/**
 * CorrelationMatrix -- Interactive D3 heatmap showing cross-asset
 * correlations with regime toggling, breakdown alerts, and PCA summary.
 */
import React, { useEffect, useRef, useState, useCallback } from 'react';
import * as d3 from 'd3';
import { api } from '../api.js';
import { colors, tokens, shared } from '../styles/shared.js';
import ChartControls from '../components/ChartControls.jsx';
import useFullScreen from '../hooks/useFullScreen.js';

// ── Security helper ──────────────────────────────────────────────────────────
// Feature names come from the API and are interpolated into tooltip innerHTML.
// Escape them to prevent stored XSS from unexpected API payloads.
function escapeHtml(str) {
    if (str == null) return '';
    return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;')
        .replace(/'/g, '&#x27;');
}

const CELL_SIZE = 52;
const LABEL_PAD = 90;
const TRANSITION_MS = 600;

const REGIME_TABS = ['ALL', 'GROWTH', 'FRAGILE', 'CRISIS'];
const PERIOD_OPTIONS = [30, 60, 90, 180, 365];

const MONO = "'JetBrains Mono', monospace";
const SANS = "'IBM Plex Sans', sans-serif";

/* ─── Color scale: -1 red, 0 white, +1 blue (matching GRID dark theme) ─── */
function corrColor(v) {
    if (v == null || isNaN(v)) return colors.card;
    // -1 = #EF4444 (red), 0 = #1A2840 (dark neutral), +1 = #3B82F6 (blue)
    if (v >= 0) {
        const t = Math.min(v, 1);
        const r = Math.round(26 + (59 - 26) * t);
        const g = Math.round(40 + (130 - 40) * t);
        const b = Math.round(64 + (246 - 64) * t);
        return `rgb(${r},${g},${b})`;
    } else {
        const t = Math.min(Math.abs(v), 1);
        const r = Math.round(26 + (239 - 26) * t);
        const g = Math.round(40 + (68 - 40) * t);
        const b = Math.round(64 + (68 - 64) * t);
        return `rgb(${r},${g},${b})`;
    }
}

function corrLabel(v) {
    if (v == null) return '';
    const abs = Math.abs(v);
    if (abs > 0.7) return v > 0 ? 'Strong positive' : 'Strong negative';
    if (abs > 0.4) return v > 0 ? 'Moderate positive' : 'Moderate negative';
    if (abs > 0.15) return v > 0 ? 'Weak positive' : 'Weak negative';
    return 'Near zero';
}

function corrExplain(v) {
    if (v == null) return '';
    if (Math.abs(v) < 0.15) return 'These assets move independently';
    return v > 0 ? 'These assets tend to move together' : 'These assets tend to move opposite';
}

/* ─── Scatter Plot Mini-Modal ─── */
function ScatterModal({ pair, onClose }) {
    if (!pair) return null;
    return (
        <div onClick={onClose} style={{
            position: 'fixed', top: 0, left: 0, right: 0, bottom: 0,
            background: 'rgba(0,0,0,0.7)', zIndex: 200,
            display: 'flex', alignItems: 'center', justifyContent: 'center',
        }}>
            <div onClick={e => e.stopPropagation()} style={{
                ...shared.cardGradient, maxWidth: '420px', width: '90%',
                padding: '20px',
            }}>
                <div style={{ fontSize: '12px', fontFamily: MONO, color: colors.accent,
                    fontWeight: 700, letterSpacing: '1px', marginBottom: '8px' }}>
                    {pair[0]} vs {pair[1]}
                </div>
                <div style={{ fontSize: '11px', color: colors.textDim, fontFamily: MONO }}>
                    Correlation: <span style={{
                        color: pair[2] > 0 ? '#3B82F6' : colors.red,
                        fontWeight: 600 }}>{pair[2]?.toFixed(3)}</span>
                </div>
                <div style={{ fontSize: '11px', color: colors.textMuted, marginTop: '4px',
                    fontFamily: SANS }}>
                    {corrExplain(pair[2])}
                </div>
                <ScatterPlot featureA={pair[0]} featureB={pair[1]} />
                <button onClick={onClose} style={{
                    ...shared.buttonSmall, marginTop: '10px', width: '100%',
                }}>Close</button>
            </div>
        </div>
    );
}

function ScatterPlot({ featureA, featureB }) {
    const svgRef = useRef(null);

    useEffect(() => {
        // Minimal placeholder scatter -- real data would need timeseries fetch
        if (!svgRef.current) return;
        const svg = d3.select(svgRef.current);
        svg.selectAll('*').remove();

        const w = 360, h = 200, m = { t: 10, r: 10, b: 30, l: 40 };
        svg.attr('width', w).attr('height', h);

        // Generate synthetic scatter from random walk (placeholder until timeseries endpoint)
        const n = 60;
        const data = Array.from({ length: n }, (_, i) => ({
            x: Math.sin(i * 0.1) + Math.random() * 0.5,
            y: Math.sin(i * 0.1 + 0.3) + Math.random() * 0.5,
        }));

        const xScale = d3.scaleLinear()
            .domain(d3.extent(data, d => d.x)).nice()
            .range([m.l, w - m.r]);
        const yScale = d3.scaleLinear()
            .domain(d3.extent(data, d => d.y)).nice()
            .range([h - m.b, m.t]);

        const g = svg.append('g');

        g.append('g').attr('transform', `translate(0,${h - m.b})`)
            .call(d3.axisBottom(xScale).ticks(5).tickSize(0))
            .call(g => g.select('.domain').remove())
            .selectAll('text').attr('fill', colors.textMuted)
            .attr('font-size', '8px').attr('font-family', MONO);

        g.append('g').attr('transform', `translate(${m.l},0)`)
            .call(d3.axisLeft(yScale).ticks(4).tickSize(0))
            .call(g => g.select('.domain').remove())
            .selectAll('text').attr('fill', colors.textMuted)
            .attr('font-size', '8px').attr('font-family', MONO);

        g.selectAll('circle')
            .data(data).enter().append('circle')
            .attr('cx', d => xScale(d.x))
            .attr('cy', d => yScale(d.y))
            .attr('r', 3)
            .attr('fill', colors.accent)
            .attr('opacity', 0.6);

        // Axis labels
        g.append('text').attr('x', w / 2).attr('y', h - 2)
            .attr('text-anchor', 'middle').attr('font-size', '9px')
            .attr('font-family', MONO).attr('fill', colors.textMuted)
            .text(featureA);
        g.append('text')
            .attr('transform', `rotate(-90)`).attr('x', -h / 2).attr('y', 10)
            .attr('text-anchor', 'middle').attr('font-size', '9px')
            .attr('font-family', MONO).attr('fill', colors.textMuted)
            .text(featureB);
    }, [featureA, featureB]);

    return <svg ref={svgRef} style={{ display: 'block', width: '100%', marginTop: '10px' }} />;
}

/* ─── Main Component ─── */
export default function CorrelationMatrix() {
    const [data, setData] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState(null);
    const [regime, setRegime] = useState('ALL');
    const [period, setPeriod] = useState(90);
    const [scatterPair, setScatterPair] = useState(null);

    const svgRef = useRef(null);
    const containerRef = useRef(null);
    const tooltipRef = useRef(null);
    const fullScreenRef = useRef(null);
    const [cellSearch, setCellSearch] = useState('');
    const { isFullScreen, toggleFullScreen } = useFullScreen(fullScreenRef);

    // Fetch data
    const fetchData = useCallback(async () => {
        setLoading(true);
        setError(null);
        try {
            const result = await api.getDiscoveryCorrelationMatrix(period, regime.toLowerCase());
            setData(result);
        } catch (err) {
            setError(err.message || 'Failed to load correlation matrix');
        } finally {
            setLoading(false);
        }
    }, [period, regime]);

    useEffect(() => { fetchData(); }, [fetchData]);

    // Get active matrix based on regime tab
    const getActiveMatrix = useCallback(() => {
        if (!data) return null;
        if (regime === 'ALL') return data.matrix;
        const rm = data.regime_matrices?.[regime];
        return rm || data.matrix;
    }, [data, regime]);

    // D3 heatmap render
    useEffect(() => {
        if (!svgRef.current || !data || !data.features?.length) return;

        const matrix = getActiveMatrix();
        if (!matrix || !matrix.length) return;

        const features = data.features;
        const n = features.length;
        const size = CELL_SIZE;
        const totalW = LABEL_PAD + n * size + 20;
        const totalH = LABEL_PAD + n * size + 20;

        const svg = d3.select(svgRef.current);
        svg.attr('width', totalW).attr('height', totalH);

        // Transition existing cells or create new ones
        let g = svg.select('g.heatmap-g');
        if (g.empty()) {
            svg.selectAll('*').remove();
            g = svg.append('g').attr('class', 'heatmap-g')
                .attr('transform', `translate(${LABEL_PAD}, ${LABEL_PAD})`);

            // X labels (rotated)
            g.selectAll('.x-label')
                .data(features).enter()
                .append('text')
                .attr('class', 'x-label')
                .attr('x', (_, i) => i * size + size / 2)
                .attr('y', -8)
                .attr('text-anchor', 'start')
                .attr('transform', (_, i) => `rotate(-45, ${i * size + size / 2}, -8)`)
                .attr('font-size', '10px')
                .attr('font-family', MONO)
                .attr('fill', colors.text)
                .text(d => d);

            // Y labels
            g.selectAll('.y-label')
                .data(features).enter()
                .append('text')
                .attr('class', 'y-label')
                .attr('x', -6)
                .attr('y', (_, i) => i * size + size / 2 + 4)
                .attr('text-anchor', 'end')
                .attr('font-size', '10px')
                .attr('font-family', MONO)
                .attr('fill', colors.text)
                .text(d => d);
        }

        // Build cell data
        const cells = [];
        for (let i = 0; i < n; i++) {
            for (let j = 0; j < n; j++) {
                cells.push({ i, j, v: matrix[i]?.[j] ?? 0 });
            }
        }

        // Cells with animated transitions
        const rects = g.selectAll('.corr-cell').data(cells, d => `${d.i}-${d.j}`);

        rects.enter()
            .append('rect')
            .attr('class', 'corr-cell')
            .attr('x', d => d.j * size + 1)
            .attr('y', d => d.i * size + 1)
            .attr('width', size - 2)
            .attr('height', size - 2)
            .attr('rx', 3)
            .attr('fill', d => corrColor(d.v))
            .attr('stroke', colors.bg)
            .attr('stroke-width', 1)
            .style('cursor', 'pointer')
            .on('mouseenter', function (event, d) {
                d3.select(this).attr('stroke', colors.accent).attr('stroke-width', 2);
                if (tooltipRef.current) {
                    const tt = tooltipRef.current;
                    tt.style.display = 'block';
                    // features[] are API-sourced column names — escape to prevent stored XSS.
                    tt.innerHTML = `
                        <div style="font-weight:700;color:${colors.text};margin-bottom:4px">
                            ${escapeHtml(features[d.i])} x ${escapeHtml(features[d.j])}
                        </div>
                        <div style="font-size:15px;font-weight:700;color:${d.v > 0 ? '#3B82F6' : d.v < -0.15 ? colors.red : colors.textDim}">
                            ${d.v.toFixed(3)}
                        </div>
                        <div style="font-size:10px;color:${colors.textMuted};margin-top:2px">
                            ${corrLabel(d.v)} -- ${corrExplain(d.v)}
                        </div>
                    `;
                }
            })
            .on('mouseleave', function () {
                d3.select(this).attr('stroke', colors.bg).attr('stroke-width', 1);
                if (tooltipRef.current) tooltipRef.current.style.display = 'none';
            })
            .on('click', (event, d) => {
                if (d.i !== d.j) {
                    setScatterPair([features[d.i], features[d.j], d.v]);
                }
            })
            .merge(rects)
            .transition().duration(TRANSITION_MS).ease(d3.easeCubicInOut)
            .attr('fill', d => corrColor(d.v));

        // Cell value text
        const texts = g.selectAll('.corr-text').data(cells, d => `${d.i}-${d.j}`);

        texts.enter()
            .append('text')
            .attr('class', 'corr-text')
            .attr('x', d => d.j * size + size / 2)
            .attr('y', d => d.i * size + size / 2 + 4)
            .attr('text-anchor', 'middle')
            .attr('font-size', n > 10 ? '8px' : '9px')
            .attr('font-family', MONO)
            .attr('fill', d => Math.abs(d.v) > 0.5 ? '#fff' : colors.textDim)
            .attr('pointer-events', 'none')
            .text(d => d.i === d.j ? '' : d.v.toFixed(2))
            .merge(texts)
            .transition().duration(TRANSITION_MS)
            .attr('fill', d => Math.abs(d.v) > 0.5 ? '#fff' : colors.textDim)
            .tween('text', function (d) {
                const node = this;
                const prev = parseFloat(node.textContent) || 0;
                const interp = d3.interpolateNumber(prev, d.v);
                return t => {
                    if (d.i !== d.j) node.textContent = interp(t).toFixed(2);
                };
            });

    }, [data, regime, getActiveMatrix]);

    // Search highlight: dim cells not matching search query
    useEffect(() => {
        if (!svgRef.current || !data?.features?.length) return;
        const q = cellSearch.toLowerCase().trim();
        const features = data.features;
        const svg = d3.select(svgRef.current);

        svg.selectAll('.corr-cell')
            .attr('opacity', (d) => {
                if (!q) return 1;
                const fA = (features[d.i] || '').toLowerCase();
                const fB = (features[d.j] || '').toLowerCase();
                return (fA.includes(q) || fB.includes(q)) ? 1 : 0.15;
            });
        svg.selectAll('.corr-text')
            .attr('opacity', (d) => {
                if (!q) return 1;
                const fA = (features[d.i] || '').toLowerCase();
                const fB = (features[d.j] || '').toLowerCase();
                return (fA.includes(q) || fB.includes(q)) ? 1 : 0.1;
            });
    }, [cellSearch, data]);

    // Zoom handlers for the heatmap scroll container
    const handleZoomIn = useCallback(() => {
        if (!containerRef.current) return;
        const el = containerRef.current;
        const current = parseFloat(el.style.transform?.match(/scale\(([\d.]+)\)/)?.[1] || '1');
        el.style.transform = `scale(${Math.min(current * 1.3, 3)})`;
        el.style.transformOrigin = 'top left';
    }, []);

    const handleZoomOut = useCallback(() => {
        if (!containerRef.current) return;
        const el = containerRef.current;
        const current = parseFloat(el.style.transform?.match(/scale\(([\d.]+)\)/)?.[1] || '1');
        el.style.transform = `scale(${Math.max(current * 0.7, 0.3)})`;
        el.style.transformOrigin = 'top left';
    }, []);

    const handleFitScreen = useCallback(() => {
        if (!containerRef.current) return;
        containerRef.current.style.transform = 'scale(1)';
    }, []);

    const handleSearch = useCallback((query) => {
        setCellSearch(query);
    }, []);

    // ── Render ──
    if (loading && !data) {
        return (
            <div style={{ ...shared.container, maxWidth: '1200px' }}>
                <div style={shared.header}>CORRELATION MATRIX</div>
                <div style={{ ...shared.card, textAlign: 'center', padding: '60px 20px',
                    color: colors.textMuted, fontFamily: MONO, fontSize: '12px' }}>
                    Loading cross-asset correlations...
                </div>
            </div>
        );
    }

    if (error) {
        return (
            <div style={{ ...shared.container, maxWidth: '1200px' }}>
                <div style={shared.header}>CORRELATION MATRIX</div>
                <div style={{ ...shared.card, textAlign: 'center', padding: '40px 20px',
                    color: colors.red, fontFamily: MONO, fontSize: '12px' }}>
                    {error}
                </div>
            </div>
        );
    }

    const features = data?.features || [];
    const pca = data?.pca || { components: [], total_variance: 0 };
    const breakdowns = data?.breakdowns || [];
    const currentRegime = data?.current_regime || 'UNKNOWN';
    const hasRegimeData = regime !== 'ALL' && data?.regime_matrices?.[regime];

    return (
        <div ref={fullScreenRef} style={{ ...shared.container, maxWidth: '1200px', background: isFullScreen ? colors.bg : undefined }}>
            {/* Header */}
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center',
                flexWrap: 'wrap', gap: '10px', marginBottom: '16px' }}>
                <div>
                    <div style={shared.header}>CORRELATION MATRIX</div>
                    <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO,
                        marginTop: '-12px' }}>
                        {features.length} assets | {data?.n_observations || 0} observations | regime: <span
                            style={{ color: currentRegime === 'GROWTH' ? colors.green
                                : currentRegime === 'CRISIS' ? colors.red
                                : colors.yellow, fontWeight: 600 }}>{currentRegime}</span>
                    </div>
                </div>
                {/* Period selector */}
                <div style={{ display: 'flex', gap: '4px' }}>
                    {PERIOD_OPTIONS.map(p => (
                        <button key={p} onClick={() => setPeriod(p)}
                            style={shared.tab(period === p)}>
                            {p}d
                        </button>
                    ))}
                </div>
            </div>

            {/* Regime toggle bar */}
            <div style={{ ...shared.tabs, marginBottom: '12px' }}>
                {REGIME_TABS.map(r => (
                    <button key={r} onClick={() => setRegime(r)}
                        style={{
                            ...shared.tab(regime === r),
                            opacity: (r !== 'ALL' && !data?.regime_matrices?.[r]) ? 0.4 : 1,
                        }}>
                        {r}
                        {r !== 'ALL' && !data?.regime_matrices?.[r] && (
                            <span style={{ fontSize: '8px', marginLeft: '4px', color: colors.textMuted }}>
                                (no data)
                            </span>
                        )}
                    </button>
                ))}
                {loading && (
                    <span style={{ fontSize: '10px', color: colors.textMuted, fontFamily: MONO,
                        alignSelf: 'center', marginLeft: '8px' }}>
                        updating...
                    </span>
                )}
            </div>

            {/* Main layout: heatmap + sidebar */}
            <div style={{ display: 'flex', gap: '16px', flexWrap: 'wrap' }}>
                {/* Heatmap */}
                <div style={{ flex: '1 1 600px', minWidth: 0 }}>
                    <div ref={containerRef} style={{
                        ...shared.card, padding: '8px', overflowX: 'auto',
                        position: 'relative',
                    }}>
                        <ChartControls
                            onZoomIn={handleZoomIn}
                            onZoomOut={handleZoomOut}
                            onFitScreen={handleFitScreen}
                            onFullScreen={toggleFullScreen}
                            isFullScreen={isFullScreen}
                            onSearch={handleSearch}
                            searchPlaceholder="Search asset..."
                        />
                        {/* Tooltip */}
                        <div ref={tooltipRef} style={{
                            display: 'none', position: 'absolute', top: '8px', right: '8px',
                            background: colors.glassOverlay, backdropFilter: 'blur(8px)',
                            border: `1px solid ${colors.border}`, borderRadius: tokens.radius.sm,
                            padding: '8px 12px', fontSize: '11px', fontFamily: MONO,
                            zIndex: 10, minWidth: '180px', pointerEvents: 'none',
                        }} />
                        <svg ref={svgRef} style={{ display: 'block' }} />
                        {/* Legend */}
                        <div style={{ display: 'flex', alignItems: 'center', gap: '6px',
                            padding: '8px 4px 4px', justifyContent: 'center' }}>
                            <span style={{ fontSize: '9px', color: colors.red, fontFamily: MONO }}>-1.0</span>
                            <div style={{
                                width: '120px', height: '10px', borderRadius: '5px',
                                background: `linear-gradient(90deg, ${corrColor(-1)}, ${corrColor(-0.5)}, ${corrColor(0)}, ${corrColor(0.5)}, ${corrColor(1)})`,
                            }} />
                            <span style={{ fontSize: '9px', color: '#3B82F6', fontFamily: MONO }}>+1.0</span>
                            <span style={{ fontSize: '9px', color: colors.textMuted, fontFamily: MONO,
                                marginLeft: '8px' }}>Click cell for scatter plot</span>
                        </div>
                    </div>
                </div>

                {/* Sidebar: PCA + Breakdowns */}
                <div style={{ flex: '0 0 280px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
                    {/* PCA Summary */}
                    <div style={shared.cardGradient}>
                        <div style={shared.sectionTitle}>PRINCIPAL COMPONENTS</div>
                        {pca.components.length > 0 ? (
                            <>
                                <div style={{ fontSize: '13px', color: colors.text, fontFamily: MONO,
                                    marginBottom: '10px' }}>
                                    {(pca.total_variance * 100).toFixed(0)}% of variance explained
                                    by top {pca.components.length} factors
                                </div>
                                {pca.components.map(comp => (
                                    <div key={comp.id} style={{
                                        ...shared.metric, marginBottom: '8px', padding: '8px',
                                    }}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between',
                                            marginBottom: '4px' }}>
                                            <span style={{ fontSize: '11px', fontWeight: 700,
                                                color: colors.accent, fontFamily: MONO }}>
                                                {comp.id}
                                            </span>
                                            <span style={{ fontSize: '11px', fontWeight: 600,
                                                color: colors.text, fontFamily: MONO }}>
                                                {(comp.variance_pct * 100).toFixed(1)}%
                                            </span>
                                        </div>
                                        {/* Variance bar */}
                                        <div style={{ height: '4px', background: colors.border,
                                            borderRadius: '2px', marginBottom: '6px' }}>
                                            <div style={{
                                                height: '100%', borderRadius: '2px',
                                                background: colors.accent,
                                                width: `${comp.variance_pct * 100}%`,
                                                transition: `width ${TRANSITION_MS}ms ease`,
                                            }} />
                                        </div>
                                        <div style={{ fontSize: '9px', color: colors.textMuted,
                                            fontFamily: MONO }}>
                                            {comp.interpretation}
                                        </div>
                                        <div style={{ display: 'flex', gap: '4px', marginTop: '4px',
                                            flexWrap: 'wrap' }}>
                                            {comp.top_features.map(f => (
                                                <span key={f.feature} style={{
                                                    fontSize: '9px', padding: '1px 5px',
                                                    borderRadius: '3px',
                                                    background: f.loading > 0 ? 'rgba(59,130,246,0.15)' : 'rgba(239,68,68,0.15)',
                                                    color: f.loading > 0 ? '#3B82F6' : colors.red,
                                                    fontFamily: MONO,
                                                }}>
                                                    {f.feature} {f.loading > 0 ? '+' : ''}{f.loading.toFixed(2)}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                ))}
                            </>
                        ) : (
                            <div style={{ fontSize: '11px', color: colors.textMuted, fontFamily: MONO }}>
                                Insufficient data for PCA
                            </div>
                        )}
                    </div>

                    {/* Breakdown Alerts */}
                    {breakdowns.length > 0 && (
                        <div style={shared.cardGradient}>
                            <div style={shared.sectionTitle}>CORRELATION BREAKDOWNS</div>
                            {breakdowns.map((b, idx) => {
                                const delta = Math.abs(b.current_corr - b.historical_corr);
                                return (
                                    <div key={idx} style={{
                                        padding: '8px', marginBottom: '6px',
                                        background: b.diverging ? 'rgba(239,68,68,0.08)' : 'rgba(245,158,11,0.06)',
                                        borderRadius: tokens.radius.sm,
                                        border: `1px solid ${b.diverging ? 'rgba(239,68,68,0.2)' : colors.borderSubtle}`,
                                        cursor: 'pointer',
                                    }} onClick={() => setScatterPair([b.pair[0], b.pair[1], b.current_corr])}>
                                        <div style={{ display: 'flex', justifyContent: 'space-between',
                                            marginBottom: '4px' }}>
                                            <span style={{ fontSize: '11px', fontWeight: 600,
                                                color: colors.text, fontFamily: MONO }}>
                                                {b.pair[0]} -- {b.pair[1]}
                                            </span>
                                            {b.diverging && (
                                                <span style={{ fontSize: '8px', padding: '1px 5px',
                                                    borderRadius: '3px', background: 'rgba(239,68,68,0.2)',
                                                    color: colors.red, fontWeight: 700, fontFamily: MONO }}>
                                                    DIVERGING
                                                </span>
                                            )}
                                        </div>
                                        <div style={{ fontSize: '10px', color: colors.textDim,
                                            fontFamily: MONO }}>
                                            <span style={{ color: b.current_corr > 0 ? '#3B82F6' : colors.red }}>
                                                {b.current_corr.toFixed(2)} now
                                            </span>
                                            {' vs '}
                                            <span style={{ color: colors.textMuted }}>
                                                {b.historical_corr.toFixed(2)} historical
                                            </span>
                                        </div>
                                        <div style={{ fontSize: '9px', color: colors.textMuted,
                                            fontFamily: SANS, marginTop: '3px' }}>
                                            {b.diverging
                                                ? `${b.pair[0]}-${b.pair[1]} decoupling: ${b.current_corr.toFixed(1)} now vs ${b.historical_corr.toFixed(1)} historical -- something changed`
                                                : `Shift of ${delta.toFixed(2)} -- worth monitoring`}
                                        </div>
                                    </div>
                                );
                            })}
                        </div>
                    )}
                </div>
            </div>

            {/* Scatter plot modal */}
            <ScatterModal pair={scatterPair} onClose={() => setScatterPair(null)} />
        </div>
    );
}
