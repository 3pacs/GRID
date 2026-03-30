# Visualization Libraries Evaluation — Hybrid JS+Python Stack

> 30 libraries compared. Ranked picks and production architecture for GRID's financial, scientific, and network data types.

---

## TL;DR — Recommended Stack

| Layer | Library | Purpose | Priority |
|-------|---------|---------|----------|
| **Core** | Plotly.py + Plotly.js | 80% of charts, identical JSON schema Python↔JS | P0 |
| **Network** | 3d-force-graph (Three.js) | 3D force-directed graphs (ActorNetwork, energy network) | P0 |
| **Dashboard** | ECharts | High-perf canvas charts, sankey, treemap, timeline | P1 |
| **Scientific** | PyVista + vtk-js (via Trame) | Volume rendering, isosurfaces, Jupyter→web | P2 |
| **Geospatial** | deck.gl + pydeck | Large-scale spatial data, animated flows | P2 |
| **Bespoke** | Three.js / react-three-fiber | Custom shaders, novel encodings | P3 |
| **Existing** | D3.js | Keep for MoneyFlow sankey, custom force graphs | Keep |

**Skip:** Mayavi (unmaintained since May 2021), ipyvolume (pre-1.0, stalled), Chart.js (wrong fit)

**Also consider:**
- **ECharts-GL** — strong Plotly alternative for polished dashboard 3D with built-in timeline animation
- **cosmos.gl** — GPU-accelerated force layout for massive graphs (100K+ nodes)
- **Panel (HoloViz)** — Python dashboarding framework that unifies Plotly, Bokeh, and Matplotlib in one app
- **Dash** — Plotly's own web app framework (alternative to building React components manually)
- **react-three-fiber** — React renderer for Three.js (preferred over raw Three.js in React apps)

---

## 2D Libraries — Top 10 Compared

| Library | Stars | Sankey | Force Graph | Bubble Map | Heatmap | Treemap | Perf | Hybrid Fit |
|---------|-------|--------|-------------|------------|---------|---------|------|------------|
| **D3.js** | ~112.6K | ✅ | ✅ Best | ✅ | ✅ | ✅ | Moderate | Moderate |
| **ECharts** | ~66K | ✅ | ✅ | ✅ | ✅ | ✅ | Excellent | Good |
| **Plotly.js** | ~18.2K | ✅ | ⚠️ Limited | ✅ | ✅ | ✅ | Good | **Excellent** |
| **Chart.js** | ~67.3K | ❌ | ⚠️ Plugin | ⚠️ Plugin | ⚠️ Plugin | ⚠️ Plugin | Good | Limited |
| **Vega-Lite** | ~5.3K | ❌ | ❌ | ✅ | ✅ | ❌ | Moderate | **Excellent** |
| **Bokeh** | ~20.3K | ❌ | ✅ (NetworkX) | ✅ | ✅ | ✅ | Good | **Excellent** |
| **Matplotlib** | ~22.6K | ⚠️ | ⚠️ Static | ✅ | ✅ | ⚠️ | Moderate | Poor |
| **Altair** | ~10.2K | ❌ | ❌ | ✅ | ✅ | ❌ | Moderate | Good |
| **Recharts** | ~26.9K | ✅ | ❌ | ❌ | ❌ | ✅ | Moderate | Moderate |
| **Nivo** | ~14K | ✅ | ✅ | ✅ | ✅ | ✅ | Good | Moderate |

### 2D Per-Library Notes

- **D3.js** — Gravity well of 2D viz; `d3-force` and `d3-sankey` are reference implementations. SVG rendering degrades above ~10K elements. No Python bindings. Everything is hand-built — use as escape hatch, not daily driver.
- **ECharts** — v6.0 shipped July 2025. Canvas incremental rendering handles 10M+ data points. ECharts-GL adds 3D surface, scatter3D, bar3D, GPU ForceAtlas2 graph layout. Python: `pyecharts`. Caveat: Baidu-origin docs uneven in English.
- **Plotly.js** — v3.4.0 shipped Feb 2026. 40+ trace types. WebGL variants (`scattergl`, `heatmapgl`) handle 100K+ points. **~3.4MB minified bundle** is the biggest downside (use `plotly.js-dist-min` ~1MB for partial). No physics engine for force-directed graphs — positions must be pre-computed.
- **Chart.js** — ~5.8M npm downloads/week (tied with D3). Only 8 core chart types; sankey/heatmap/force require inconsistent community plugins. No Python integration.
- **Vega-Lite/Altair** — Cleanest declarative grammar and JSON interchange. **No sankeys, no force graphs, no treemaps** in Vega-Lite (exist in full Vega but at much higher complexity).
- **Bokeh** — NumFOCUS-sponsored. BokehJS runs standalone; Bokeh Server enables Python-callback-driven interactivity with streaming data. WebGL scatter handles millions of points. Sankey missing natively (requires HoloViews). Force graphs via NetworkX integration. Excellent for real-time dashboards with Python backends.
- **Matplotlib** — Publication-quality static figures only. `mpld3` and `ipympl` provide limited browser interactivity. Use for PDF/PNG report generation, not interactive exploration.
- **Recharts** — Composable JSX components, SVG-only with **no Canvas fallback** — performance drops above 10K points.
- **Nivo** — 25+ chart types across SVG, Canvas, and HTML rendering modes. react-spring animations. Supports server-side rendering. Stronger React choice than Recharts for chart-type diversity.

### 2D Top 3
1. **Plotly.js** — Best hybrid-stack bridge, broadest scientific chart coverage
2. **ECharts** — Best native chart diversity and canvas rendering performance
3. **D3.js** — Best for bespoke custom visualizations (escape hatch)

---

## 3D Libraries — Top 10 Compared

| Library | Stars | Volume Render | Isosurface | Point Cloud | 3D Graph | Large Data | Jupyter | Hybrid Fit |
|---------|-------|---------------|------------|-------------|----------|------------|---------|------------|
| **Three.js** | ~111.4K | ⚠️ Custom | ⚠️ Custom | ✅ | ✅ (3d-force-graph) | ✅ Millions | ⚠️ | Good (JS) |
| **vtk-js** | ~1.5K | ✅ Native | ✅ Native | ✅ | ❌ | ⚠️ Moderate | ✅ Trame | Good |
| **VTK** | ~3K | ✅ Best | ✅ Best | ✅ | ⚠️ Basic | ✅ 100M+ | ✅ PyVista | Good (Py) |
| **PyVista** | ~3K | ✅ | ✅ | ✅ | ⚠️ Manual | ✅ Millions | ✅ First-class | Good |
| **deck.gl** | ~13.7K | ❌ | ❌ | ✅ | ⚠️ | ✅ Millions | ✅ pydeck | **Excellent** |
| **kepler.gl** | ~11.6K | ❌ | ❌ | ✅ (geo) | ⚠️ Arcs | ✅ Millions | ✅ | Excellent (geo) |
| **VisPy** | ~3.6K | ✅ | ✅ | ✅ | ⚠️ Manual | ✅ Millions | ⚠️ | Moderate |
| **vedo** | ~2.2K | ✅ | ✅ | ✅ | ⚠️ Manual | ⚠️ | ⚠️ | Limited |
| **Mayavi** | ~1.4K | ✅ | ✅ | ✅ | ⚠️ | ⚠️ | ⚠️ Fragile | ❌ Avoid |
| **K3D-jupyter** | ~1K | ✅ | ✅ | ✅ | ⚠️ Manual | ⚠️ | ✅ Native | Moderate |

### 3D Per-Library Notes

- **Three.js** — Most popular 3D library on Earth. Full WebGL/WebGPU rendering, custom shaders, instanced meshes, LOD, post-processing. **3d-force-graph** (~5.8K stars, built on Three.js + d3-force-3d) is the definitive 3D network graph solution — supports custom Three.js node/link objects, DAG mode, particle animations along edges, VR/AR modes. Handles thousands of nodes interactively. No built-in scientific viz primitives (volume rendering = custom raycasting shaders).
- **VTK/PyVista** — VTK is 30-year gold standard for scientific viz. Hundreds of algorithms: FlyingEdges3D isosurfaces, GPU ray casting volumes, streamlines, tensor glyphs. C++ core handles 100M+ cells. PyVista wraps it in ~3 lines per viz with first-class Jupyter support via Trame.
- **vtk-js** — Kitware-maintained port of VTK rendering to WebGL. **Native hardware-accelerated volume rendering** in JavaScript — rare capability.
- **deck.gl** — Designed at Uber for millions of geospatial points at 60fps via WebGL2 instancing. 50+ layer types including TripsLayer (animated paths with fade trails). `pydeck` bindings with binary data transfer. Non-geo use supported (OrbitView) but less polished.
- **kepler.gl** — No-code drag-and-drop UI on top of deck.gl. Time-series playback slider, layer controls, filters. **2.5D only** (extruded map, not arbitrary 3D). Geospatial-only.
- **VisPy** — Fastest raw GPU rendering in Python via direct OpenGL — 320 signals × 10K points smoothly. Steep learning curve (requires GLSL knowledge). Jupyter integration partial.
- **vedo** — Easiest VTK wrapper, simpler than PyVista for quick scenes (3 lines). 300+ examples. Desktop-oriented; web requires Trame/K3D.
- **Mayavi** — Last release May 2021. **Avoid for new projects.** PyVista and vedo are modern replacements.
- **K3D-jupyter** — Purpose-built for Jupyter-native 3D with built-in time-series animation. Tightly coupled to Jupyter widget protocol — **not extractable for standalone web apps**.
- **cosmos.gl** — GPU-accelerated force layout for massive graphs (100K+ nodes). Consider when 3d-force-graph hits perf limits.

### 3D Top 3
1. **Three.js + 3d-force-graph** — Browser-deployed 3D networks, custom financial viz
2. **PyVista** — Pythonic scientific 3D with Jupyter support
3. **deck.gl/pydeck** — Large-scale data with native JS+Python bindings

---

## 4D Libraries (3D + Time) — Compared

| Library | Time-Step Anim | 4D Volume | Financial 4D | Scientific 4D | Hybrid Fit |
|---------|----------------|-----------|--------------|---------------|------------|
| **ParaView** | ★★★ | ★★★ | ❌ | ✅ Best | Moderate |
| **napari** | ★★★ | ★★★ | ❌ | ✅ Imaging | Low |
| **ipyvolume** | ★★☆ | ★★☆ | ✅ Good | ✅ Good | Good |
| **PyVista** | ★★☆ | ★★☆ | ⚠️ | ✅ Excellent | Good |
| **deck.gl** | ★★★ | ☆☆☆ | ⚠️ Geo | ❌ | **Excellent** |
| **Plotly.js** | ★☆☆ | ★☆☆ | ✅ Good | ⚠️ Basic | **Excellent** |

### 4D Per-Library Notes

- **ParaView** — Most complete 4D scientific viz tool. Reads time-series natively from HDF5/XDMF, NetCDF, VTK. Built-in time slider, keyframe animation, temporal interpolation. Catalyst for in-situ viz of running simulations. Scales to thousands of processors. Heavy desktop app — Trame bridges to web but latency is inherent.
- **napari** — CZI-funded, n-dimensional native (t, z, y, x). Dimension sliders, lazy loading via dask/zarr for 100GB+ datasets, 300+ community plugins. GPU rendering via VisPy. Desktop-only (Qt) — no web component.
- **ipyvolume** — Unique: all plot properties accept lists of arrays for time snapshots, enabling native time-animated 3D scatter/quiver/volume in Jupyter. Pre-1.0, stalled development — prototyping only.
- **Plotly 4D** — `animation_frame` works well for 2D. **3D trace animation is limited** — `scatter3d` supports basic frame transitions, but `surface` and `volume` lack smooth animation. For animated vol surfaces, requires manual `Plotly.animate()` with frame arrays. 4th dimension as color/size in static 3D scatter is straightforward.
- **deck.gl TripsLayer** — Canonical 4D geospatial animation: movement paths with fade trails, thousands of animated paths at 60fps. `DataFilterExtension` enables real-time GPU temporal filtering of millions of points.

### 4D Top 3
1. **ParaView + PyVista** — Scientific time-varying volumetric data
2. **deck.gl/pydeck** — Geospatial 4D time-series in browser
3. **Plotly + ipyvolume** — Financial 4D, Jupyter-native exploration

---

## Mapping to GRID Frontend Views

| GRID View | Current Tech | Recommended Addition | Why |
|-----------|-------------|---------------------|-----|
| **MoneyFlow** (sankey) | D3.js | Keep D3; add Plotly sankey for simpler flows | D3 sankey is best-in-class |
| **ActorNetwork** (force graph) | D3 (building) | **3d-force-graph** for 3D mode | Dramatic edge-crossing reduction in 3D |
| **CrossReference** | — | Plotly heatmap + scatter | Best hybrid fit |
| **Predictions** | — | Plotly line/scatter + ECharts gauge | Calibration charts |
| **IntelDashboard** | — | ECharts (dashboard perf) | 10M+ point canvas rendering |
| **TrendTracker** | — | Plotly surface (vol surface) + ECharts timeline | Financial 3D surfaces |

---

## 2D→3D Natural Extensions

| 2D Visualization | 3D Extension | Library | Approach |
|-----------------|--------------|---------|----------|
| Bubble map | 3D scatter cloud | Plotly `scatter3d` | z-axis = 5th metric |
| Bubble map (geo) | Extruded hex map | deck.gl `HexagonLayer` | Height = 3rd metric |
| Force graph | 3D force graph | 3d-force-graph | Reduces edge crossing |
| Sankey | 3D streamtubes | Plotly `streamtube` | Continuous flow fields |
| Sankey | Animated 2.5D sankey | D3 transitions | Particles along links |
| Heatmap | 3D surface | Plotly `surface` | Height = value, color = 2nd metric |
| Treemap | Extruded 3D treemap | Three.js custom | Height = performance, area = market cap |
| Time series | 3D waterfall/ridge | Plotly `surface` | Stack series along z-axis by entity |
| Any 2D | Space-time cube | Three.js / deck.gl | z-axis = time, x-y = spatial/data dims |

---

## Wrap vs Use As-Is

**Wrap and extend** (invest custom code):
- **Three.js** — Build domain abstractions for financial trading floors, order books
- **deck.gl** — Subclass `Layer` for custom GLSL data layers
- **D3.js** — Compose modules for bespoke 2D viz
- **3d-force-graph** — Inject custom Three.js objects for domain-specific networks

**Use as-is** (don't fight the abstraction):
- **Plotly** — 40+ trace types cover most needs. Customization beyond JSON schema is painful.
- **PyVista** — Drop to raw VTK only when PyVista's API doesn't cover your filter. One-liners for 95%.
- **kepler.gl** — Complete application, not a library. Use for what it does; don't extend.
- **ECharts** — JSON config extensive enough. `custom` series type exists as escape hatch but rarely worth it.

---

## Performance Reference — Data Point Limits

| Library | Rendering | Comfortable Limit | Hard Limit | Notes |
|---------|-----------|-------------------|------------|-------|
| D3.js | SVG | ~5K elements | ~10K | DOM-bound; degrades with node count |
| D3.js | Canvas | ~50K | ~200K | Manual hit-testing needed |
| ECharts | Canvas | ~1M | ~10M+ | Incremental rendering, progressive loading |
| Plotly.js | SVG | ~10K | ~50K | Default mode |
| Plotly.js | WebGL | ~100K | ~1M+ | `scattergl`, `heatmapgl` variants |
| Chart.js | Canvas | ~10K | ~50K | Decimation plugin helps |
| Recharts | SVG | ~5K | ~10K | No Canvas fallback |
| Nivo | Canvas | ~50K | ~100K | Choose Canvas mode explicitly |
| Three.js | WebGL | ~1M | ~10M+ | Instanced meshes, LOD, custom shaders |
| 3d-force-graph | WebGL | ~5K nodes | ~50K | Switch to `ngraphForceLayout` for >5K |
| cosmos.gl | WebGL/GPU | ~100K nodes | ~1M+ | GPU force simulation |
| deck.gl | WebGL2 | ~1M | ~10M+ | Instanced rendering, binary transfer |
| PyVista/VTK | OpenGL | ~10M cells | ~100M+ | C++ backend, parallel processing |
| Bokeh | WebGL | ~100K | ~1M+ | WebGL scatter mode |

---

## Bundle Size Reference

| Library | Minified Size | Tree-Shakeable | Notes |
|---------|--------------|----------------|-------|
| Plotly.js (full) | ~3.4 MB | No | Largest downside |
| plotly.js-dist-min | ~1 MB | No | Partial bundle, all trace types |
| ECharts | ~1 MB | Yes (v5+) | Import only needed charts |
| D3.js (full) | ~280 KB | Yes | Import individual modules |
| Chart.js | ~200 KB | Yes | Tree-shake unused controllers |
| Three.js | ~650 KB | Yes | Core only; addons separate |
| 3d-force-graph | ~150 KB | No | + Three.js peer dep |
| deck.gl | ~300 KB | Yes | Per-layer imports |
| Recharts | ~150 KB | Partial | + D3 deps |
| Nivo | ~50-150 KB/pkg | Yes | Separate packages per chart type |
