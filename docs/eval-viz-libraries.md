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

**Skip:** Mayavi (unmaintained), ipyvolume (stalled), Chart.js (wrong fit)

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

---

## Wrap vs Use As-Is

**Wrap and extend** (invest custom code):
- **Three.js** — Build domain abstractions for financial trading floors, order books
- **deck.gl** — Subclass `Layer` for custom GLSL data layers
- **D3.js** — Compose modules for bespoke 2D viz
- **3d-force-graph** — Inject custom Three.js objects for domain-specific networks

**Use as-is** (don't fight the abstraction):
- **Plotly** — 40+ trace types cover most needs
- **PyVista** — One-liners for 95% of scientific viz
- **kepler.gl** — Complete app, not a library
- **ECharts** — JSON config is extensive enough
