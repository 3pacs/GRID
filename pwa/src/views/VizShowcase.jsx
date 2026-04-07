/**
 * VizShowcase — Demo view for all 5 high-performance visualization libraries.
 *
 * Generates synthetic data to demonstrate each library's capabilities
 * at scale. Used for benchmarking and integration testing.
 */
import { useState, useMemo, lazy, Suspense } from 'react';
import { useStore } from '../store';
import { themes } from '../styles/shared';

// Lazy-load heavy viz components for code splitting
const UPlotChart = lazy(() => import('../components/viz/UPlotChart'));
const SigmaGraph = lazy(() => import('../components/viz/SigmaGraph'));
const EChartsPanel = lazy(() => import('../components/viz/EChartsPanel'));
const PerspectiveGrid = lazy(() => import('../components/viz/PerspectiveGrid'));
const ReglScatter = lazy(() => import('../components/viz/ReglScatter'));

const POINT_COUNTS = [10_000, 100_000, 500_000, 1_000_000];

// ── Synthetic data generators ──────────────────────────────────

function generateTimeSeries(count) {
    const now = Math.floor(Date.now() / 1000);
    const step = 60; // 1 minute bars
    const timestamps = new Float64Array(count);
    const open = new Float64Array(count);
    const high = new Float64Array(count);
    const low = new Float64Array(count);
    const close = new Float64Array(count);
    const volume = new Float64Array(count);

    let price = 100;
    for (let i = 0; i < count; i++) {
        timestamps[i] = now - (count - i) * step;
        const change = (Math.random() - 0.498) * 2;
        const o = price;
        const c = price + change;
        const h = Math.max(o, c) + Math.random() * 0.5;
        const l = Math.min(o, c) - Math.random() * 0.5;
        open[i] = o;
        high[i] = h;
        low[i] = l;
        close[i] = c;
        volume[i] = Math.random() * 1000000 + 100000;
        price = c;
    }
    return [timestamps, open, high, low, close, volume];
}

function generateNetworkData(nodeCount) {
    const categories = ['sovereign', 'regional', 'institutional', 'individual'];
    const nodes = [];
    const edges = [];
    for (let i = 0; i < nodeCount; i++) {
        nodes.push({
            id: `n${i}`,
            label: `Actor ${i}`,
            category: categories[i % categories.length],
            size: 3 + Math.random() * 8,
        });
    }
    // Create sparse edges (avg ~3 edges per node)
    const edgeCount = Math.min(nodeCount * 3, 5000);
    for (let i = 0; i < edgeCount; i++) {
        const src = Math.floor(Math.random() * nodeCount);
        const tgt = Math.floor(Math.random() * nodeCount);
        if (src !== tgt) {
            edges.push({
                source: `n${src}`,
                target: `n${tgt}`,
                weight: Math.random() * 5,
            });
        }
    }
    return { nodes, edges };
}

function generateHeatmapData() {
    const categories = ['GDP', 'Trade', 'Inflation', 'Employment', 'Credit', 'Housing', 'Energy', 'Liquidity'];
    const regions = ['US', 'China', 'EU', 'Japan', 'EM', 'UK', 'India', 'Brazil'];
    const data = [];
    for (let x = 0; x < categories.length; x++) {
        for (let y = 0; y < regions.length; y++) {
            data.push([x, y, +(Math.random()).toFixed(2)]);
        }
    }
    return { data, xLabels: categories, yLabels: regions };
}

function generateTableData(count) {
    const tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA', 'BRK.B', 'JPM', 'V'];
    const signals = ['BUY', 'SELL', 'HOLD', 'STRONG_BUY', 'STRONG_SELL'];
    const sources = ['congressional', 'insider', 'darkpool', 'social', 'scanner'];
    const rows = [];
    for (let i = 0; i < count; i++) {
        rows.push({
            id: i,
            timestamp: new Date(Date.now() - Math.random() * 86400000 * 90).toISOString(),
            ticker: tickers[i % tickers.length],
            signal: signals[Math.floor(Math.random() * signals.length)],
            confidence: +(Math.random() * 0.9 + 0.1).toFixed(3),
            source: sources[Math.floor(Math.random() * sources.length)],
            price: +(50 + Math.random() * 450).toFixed(2),
            volume: Math.floor(Math.random() * 10000000),
            pnl: +((Math.random() - 0.4) * 20).toFixed(2),
        });
    }
    return rows;
}

function generateScatterPoints(count) {
    const points = new Float32Array(count * 2);
    const categories = new Array(count);
    const cats = ['regime_a', 'regime_b', 'regime_c', 'regime_d'];
    for (let i = 0; i < count; i++) {
        const cat = Math.floor(Math.random() * 4);
        const cx = (cat % 2) * 4 - 2;
        const cy = Math.floor(cat / 2) * 4 - 2;
        points[i * 2] = cx + (Math.random() - 0.5) * 3;
        points[i * 2 + 1] = cy + (Math.random() - 0.5) * 3;
        categories[i] = cats[cat];
    }
    return { points, categories };
}

function generateSankeyData() {
    const nodes = [
        { name: 'Fed' }, { name: 'Treasury' }, { name: 'Banks' },
        { name: 'Equities' }, { name: 'Bonds' }, { name: 'Crypto' },
        { name: 'Tech' }, { name: 'Energy' }, { name: 'Finance' },
        { name: 'Consumer' }, { name: 'Healthcare' },
    ];
    const links = [
        { source: 'Fed', target: 'Banks', value: 500 },
        { source: 'Fed', target: 'Treasury', value: 300 },
        { source: 'Treasury', target: 'Bonds', value: 250 },
        { source: 'Banks', target: 'Equities', value: 400 },
        { source: 'Banks', target: 'Crypto', value: 80 },
        { source: 'Banks', target: 'Bonds', value: 120 },
        { source: 'Equities', target: 'Tech', value: 200 },
        { source: 'Equities', target: 'Energy', value: 100 },
        { source: 'Equities', target: 'Finance', value: 80 },
        { source: 'Equities', target: 'Consumer', value: 60 },
        { source: 'Equities', target: 'Healthcare', value: 50 },
        { source: 'Crypto', target: 'Tech', value: 40 },
    ];
    return { nodes, links };
}

function generateRadarData() {
    const indicators = [
        { name: 'Momentum', max: 100 },
        { name: 'Volatility', max: 100 },
        { name: 'Liquidity', max: 100 },
        { name: 'Correlation', max: 100 },
        { name: 'Regime Stability', max: 100 },
        { name: 'Signal Strength', max: 100 },
    ];
    const values = [
        { name: 'Current', data: indicators.map(() => Math.floor(Math.random() * 80 + 20)) },
        { name: '30d Avg', data: indicators.map(() => Math.floor(Math.random() * 60 + 30)) },
    ];
    return { indicators, values };
}

// ── Styles ─────────────────────────────────────────────────────

const s = {
    page: {
        padding: '24px',
        maxWidth: 1400,
        margin: '0 auto',
    },
    header: {
        marginBottom: 24,
    },
    title: {
        fontSize: 20,
        fontWeight: 600,
        color: '#E2E8F0',
        fontFamily: 'IBM Plex Mono',
        margin: 0,
    },
    subtitle: {
        fontSize: 13,
        color: '#5A7080',
        fontFamily: 'IBM Plex Mono',
        marginTop: 4,
    },
    controls: {
        display: 'flex',
        gap: 8,
        marginBottom: 20,
        flexWrap: 'wrap',
        alignItems: 'center',
    },
    btn: (active) => ({
        padding: '6px 14px',
        borderRadius: 4,
        border: '1px solid ' + (active ? '#1A6EBF' : '#1A2332'),
        background: active ? 'rgba(26, 110, 191, 0.15)' : '#111B2A',
        color: active ? '#1A6EBF' : '#8AA0B8',
        cursor: 'pointer',
        fontSize: 12,
        fontFamily: 'IBM Plex Mono',
    }),
    section: {
        marginBottom: 28,
        background: '#0D1520',
        border: '1px solid #1A2332',
        borderRadius: 8,
        overflow: 'hidden',
    },
    sectionHeader: {
        padding: '12px 16px',
        borderBottom: '1px solid #1A2332',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
    },
    sectionTitle: {
        fontSize: 14,
        fontWeight: 600,
        color: '#E2E8F0',
        fontFamily: 'IBM Plex Mono',
        margin: 0,
    },
    badge: (color) => ({
        fontSize: 10,
        padding: '2px 8px',
        borderRadius: 10,
        background: color + '22',
        color: color,
        fontFamily: 'IBM Plex Mono',
    }),
    sectionBody: {
        padding: 16,
    },
    grid2: {
        display: 'grid',
        gridTemplateColumns: 'repeat(auto-fit, minmax(500px, 1fr))',
        gap: 20,
    },
    fallback: {
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
        height: 200,
        color: '#5A7080',
        fontFamily: 'IBM Plex Mono',
        fontSize: 13,
    },
    stat: {
        fontSize: 11,
        color: '#5A7080',
        fontFamily: 'IBM Plex Mono',
    },
};

function LoadingFallback() {
    return <div style={s.fallback}>Loading visualization...</div>;
}

// ── Main Component ─────────────────────────────────────────────

export default function VizShowcase() {
    const theme = useStore((st) => st.theme) || 'dark';
    const t = themes[theme] || themes.dark;

    const [pointCount, setPointCount] = useState(100_000);
    const [networkSize, setNetworkSize] = useState(500);
    const [scatterCount, setScatterCount] = useState(100_000);
    const [tableRows, setTableRows] = useState(1000);

    // Memoize generated data to avoid regeneration on every render
    const tsData = useMemo(() => generateTimeSeries(pointCount), [pointCount]);
    const networkData = useMemo(() => generateNetworkData(networkSize), [networkSize]);
    const heatmapData = useMemo(() => generateHeatmapData(), []);
    const sankeyData = useMemo(() => generateSankeyData(), []);
    const radarData = useMemo(() => generateRadarData(), []);
    const tableData = useMemo(() => generateTableData(tableRows), [tableRows]);
    const scatterData = useMemo(() => generateScatterPoints(scatterCount), [scatterCount]);

    const tsOptions = useMemo(() => ({
        title: `Time Series — ${pointCount.toLocaleString()} points`,
        series: [
            {},
            { label: 'Open', stroke: '#8AA0B8', width: 1 },
            { label: 'High', stroke: '#10B981', width: 1 },
            { label: 'Low', stroke: '#EF4444', width: 1 },
            { label: 'Close', stroke: '#1A6EBF', width: 2 },
        ],
    }), [pointCount]);

    return (
        <div style={s.page}>
            <div style={s.header}>
                <h1 style={s.title}>Visualization Engine Showcase</h1>
                <p style={s.subtitle}>
                    5 high-performance libraries integrated into GRID. Stress-test with up to 1M+ data points.
                </p>
            </div>

            {/* ── 1. uPlot: Time Series ──────────────────────── */}
            <div style={s.section}>
                <div style={s.sectionHeader}>
                    <div>
                        <h2 style={s.sectionTitle}>uPlot — Time Series</h2>
                        <span style={s.stat}>Canvas 2D | Up to 5M points | 45KB bundle</span>
                    </div>
                    <span style={s.badge('#10B981')}>OHLC + Volume</span>
                </div>
                <div style={s.sectionBody}>
                    <div style={s.controls}>
                        {POINT_COUNTS.map(n => (
                            <button key={n} style={s.btn(pointCount === n)} onClick={() => setPointCount(n)}>
                                {n.toLocaleString()} pts
                            </button>
                        ))}
                    </div>
                    <Suspense fallback={<LoadingFallback />}>
                        <UPlotChart
                            data={[tsData[0], tsData[1], tsData[2], tsData[3], tsData[4]]}
                            options={tsOptions}
                            height={320}
                        />
                    </Suspense>
                </div>
            </div>

            <div style={s.grid2}>
                {/* ── 2. ECharts: Heatmap ──────────────────────── */}
                <div style={s.section}>
                    <div style={s.sectionHeader}>
                        <div>
                            <h2 style={s.sectionTitle}>ECharts — Heatmap</h2>
                            <span style={s.stat}>Canvas | Progressive rendering</span>
                        </div>
                        <span style={s.badge('#F59E0B')}>CrossReference</span>
                    </div>
                    <div style={s.sectionBody}>
                        <Suspense fallback={<LoadingFallback />}>
                            <EChartsPanel
                                option={EChartsPanel.heatmapOption({
                                    ...heatmapData,
                                    title: 'Government Stats vs Reality',
                                })}
                                height={350}
                            />
                        </Suspense>
                    </div>
                </div>

                {/* ── 3. ECharts: Radar ──────────────────────── */}
                <div style={s.section}>
                    <div style={s.sectionHeader}>
                        <div>
                            <h2 style={s.sectionTitle}>ECharts — Radar</h2>
                            <span style={s.stat}>Canvas | Multi-dimension</span>
                        </div>
                        <span style={s.badge('#8B5CF6')}>Regime Analysis</span>
                    </div>
                    <div style={s.sectionBody}>
                        <Suspense fallback={<LoadingFallback />}>
                            <EChartsPanel
                                option={EChartsPanel.radarOption({
                                    ...radarData,
                                    title: 'Regime Health Radar',
                                })}
                                height={350}
                            />
                        </Suspense>
                    </div>
                </div>
            </div>

            {/* ── 4. ECharts: Sankey ──────────────────────────── */}
            <div style={s.section}>
                <div style={s.sectionHeader}>
                    <div>
                        <h2 style={s.sectionTitle}>ECharts — Sankey Flow</h2>
                        <span style={s.stat}>Canvas | Gradient edges | MoneyFlow replacement</span>
                    </div>
                    <span style={s.badge('#1A6EBF')}>Capital Flows</span>
                </div>
                <div style={s.sectionBody}>
                    <Suspense fallback={<LoadingFallback />}>
                        <EChartsPanel
                            option={EChartsPanel.sankeyOption({
                                ...sankeyData,
                                title: 'Fed → Markets → Sectors',
                            })}
                            height={400}
                        />
                    </Suspense>
                </div>
            </div>

            {/* ── 5. Sigma.js: Network Graph ──────────────────── */}
            <div style={s.section}>
                <div style={s.sectionHeader}>
                    <div>
                        <h2 style={s.sectionTitle}>Sigma.js — Network Graph</h2>
                        <span style={s.stat}>WebGL | ForceAtlas2 layout | Up to 50K nodes</span>
                    </div>
                    <span style={s.badge('#EF4444')}>Actor Network</span>
                </div>
                <div style={s.sectionBody}>
                    <div style={s.controls}>
                        {[100, 500, 1000, 2000].map(n => (
                            <button key={n} style={s.btn(networkSize === n)} onClick={() => setNetworkSize(n)}>
                                {n} nodes
                            </button>
                        ))}
                    </div>
                    <Suspense fallback={<LoadingFallback />}>
                        <SigmaGraph
                            nodes={networkData.nodes}
                            edges={networkData.edges}
                            height={500}
                            layout="forceatlas2"
                            onNodeClick={(id, attrs) => console.log('Node clicked:', id, attrs)}
                        />
                    </Suspense>
                </div>
            </div>

            {/* ── 6. regl-scatterplot: Massive Scatter ────────── */}
            <div style={s.section}>
                <div style={s.sectionHeader}>
                    <div>
                        <h2 style={s.sectionTitle}>regl-scatterplot — Massive Scatter</h2>
                        <span style={s.stat}>WebGL | Lasso selection | Up to 20M points</span>
                    </div>
                    <span style={s.badge('#EC4899')}>Regime Clustering</span>
                </div>
                <div style={s.sectionBody}>
                    <div style={s.controls}>
                        {POINT_COUNTS.map(n => (
                            <button key={n} style={s.btn(scatterCount === n)} onClick={() => setScatterCount(n)}>
                                {n.toLocaleString()} pts
                            </button>
                        ))}
                    </div>
                    <Suspense fallback={<LoadingFallback />}>
                        <ReglScatter
                            points={scatterData.points}
                            colorBy={scatterData.categories}
                            height={450}
                            pointSize={3}
                        />
                    </Suspense>
                </div>
            </div>

            {/* ── 7. Perspective: Data Grid ────────────────────── */}
            <div style={s.section}>
                <div style={s.sectionHeader}>
                    <div>
                        <h2 style={s.sectionTitle}>FINOS Perspective — Data Grid</h2>
                        <span style={s.stat}>WASM + WebGL | Streaming | Pivot + Filter</span>
                    </div>
                    <span style={s.badge('#06B6D4')}>Intel Dashboard</span>
                </div>
                <div style={s.sectionBody}>
                    <div style={s.controls}>
                        {[100, 1000, 5000, 10000].map(n => (
                            <button key={n} style={s.btn(tableRows === n)} onClick={() => setTableRows(n)}>
                                {n.toLocaleString()} rows
                            </button>
                        ))}
                    </div>
                    <Suspense fallback={<LoadingFallback />}>
                        <PerspectiveGrid
                            data={tableData}
                            height={450}
                            plugin="Datagrid"
                            groupBy={['ticker']}
                        />
                    </Suspense>
                </div>
            </div>
        </div>
    );
}
