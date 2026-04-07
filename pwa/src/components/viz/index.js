/**
 * Viz — High-performance visualization components for GRID.
 *
 * Library        | Best For              | Max Points  | Renderer
 * ───────────────┼───────────────────────┼─────────────┼──────────
 * UPlotChart     | Time-series, OHLC     | 5M          | Canvas 2D
 * SigmaGraph     | Network graphs        | 50K nodes   | WebGL
 * EChartsPanel   | Heatmaps, Sankey, etc | Millions    | Canvas/WebGL
 * PerspectiveGrid| Streaming data tables | Millions    | WASM + WebGL
 * ReglScatter    | Massive scatter plots | 20M         | WebGL
 */
export { default as UPlotChart } from './UPlotChart';
export { default as SigmaGraph } from './SigmaGraph';
export { default as EChartsPanel } from './EChartsPanel';
export { default as PerspectiveGrid } from './PerspectiveGrid';
export { default as ReglScatter } from './ReglScatter';
