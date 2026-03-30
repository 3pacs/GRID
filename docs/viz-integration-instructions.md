# Visualization Integration Instructions

> Discrete tasks for subagents to integrate each recommended library into GRID's PWA.
> Each section is self-contained — hand one section to a subagent.

---

## Task 1: Install Plotly.js as Core Charting Library

**Priority:** P0
**Estimated scope:** Small
**Files to modify:** `pwa/package.json`, `pwa/src/` (new wrapper)

### Steps
1. `cd grid/pwa && npm install plotly.js-dist-min react-plotly.js`
   - Use `plotly.js-dist-min` (~1MB) instead of full `plotly.js` (~3.4MB) — includes all trace types
2. Create `pwa/src/components/charts/PlotlyChart.tsx` — a thin React wrapper:
   ```tsx
   import Plot from 'react-plotly.js';
   // Re-export with GRID default layout (dark theme, font, margins)
   // Accept `data`, `layout`, `config` props
   // Set default config: { responsive: true, displayModeBar: false }
   ```
3. Create `pwa/src/components/charts/index.ts` barrel export
4. Verify build: `npm run build` — check bundle size didn't explode

### Where to use
- **CrossReference** view: heatmaps, scatter plots
- **Predictions** view: calibration line charts, confidence intervals
- **TrendTracker** view: 3D volatility surfaces (`surface` trace), time series
- Any new chart that needs Python↔JS parity (backend can generate same Plotly JSON)

### Backend complement
- `pip install plotly` in `requirements.txt`
- API endpoints can return Plotly figure JSON directly — frontend renders as-is
- Pattern: `plotly.io.to_json(fig)` → API response → `<Plot data={response.data} layout={response.layout} />`

---

## Task 2: Integrate 3d-force-graph for ActorNetwork 3D View

**Priority:** P0
**Estimated scope:** Medium
**Files to modify:** `pwa/package.json`, ActorNetwork view component

### Steps
1. `cd grid/pwa && npm install 3d-force-graph`
   - Also install `three` if not already present (peer dependency)
2. Create `pwa/src/components/charts/ForceGraph3D.tsx`:
   ```tsx
   import ForceGraph3D from '3d-force-graph';
   // Wrap in useEffect + useRef pattern for React integration
   // Accept: { nodes: [{id, group, size, label}], links: [{source, target, value}] }
   // Configure: d3AlphaDecay(0.02), d3VelocityDecay(0.3) for stable layouts
   // Node rendering: SpriteText for labels, color by group
   // Link rendering: width by value, opacity 0.6, particles for flow
   ```
3. Wire to ActorNetwork API endpoint:
   - `GET /api/intelligence/actor-network` should return `{nodes: [...], links: [...]}`
   - Map actor trust scores → node size, actor type → node color/group
   - Map relationship strength → link width
4. Add camera controls: auto-rotate toggle, zoom-to-node on click, highlight neighbors on hover
5. Performance: for >5K nodes, enable `ngraphForceLayout` instead of default d3-force-3d

### Key features to implement
- Click node → sidebar with actor details (trust score, connections, recent signals)
- Right-click → "Expand network" (fetch 2nd-degree connections)
- Toggle 2D/3D mode (library supports both)
- Color scheme: map to actor categories from `intelligence/actor_network.py`

---

## Task 3: Add ECharts for Dashboard-Grade Performance Charts

**Priority:** P1
**Estimated scope:** Small
**Files to modify:** `pwa/package.json`, `pwa/src/` (new wrapper)

### Steps
1. `cd grid/pwa && npm install echarts echarts-for-react`
2. Create `pwa/src/components/charts/EChart.tsx`:
   ```tsx
   import ReactECharts from 'echarts-for-react';
   // Thin wrapper with GRID dark theme
   // Register theme once: echarts.registerTheme('grid-dark', {...})
   // Accept `option` prop (ECharts JSON config)
   ```
3. Use for performance-critical views:
   - **IntelDashboard**: multiple small charts updating in real-time (gauge, bar, line)
   - **TrendTracker**: timeline with dataZoom slider for regime visualization
   - Large dataset heatmaps (Canvas rendering >> SVG for >10K cells)

### When to use ECharts vs Plotly
- **ECharts**: dashboard layouts, real-time updates, canvas perf, timeline/dataZoom
- **Plotly**: scientific charts, 3D surfaces, Python↔JS JSON parity, exploration

---

## Task 4: Add deck.gl/pydeck for Geospatial MoneyFlow

**Priority:** P2
**Estimated scope:** Medium
**Files to modify:** `pwa/package.json`, MoneyFlow or new GeoFlow view

### Steps
1. `cd grid/pwa && npm install deck.gl @deck.gl/react @deck.gl/layers @luma.gl/core`
2. Create `pwa/src/components/charts/DeckGLMap.tsx`:
   ```tsx
   import DeckGL from '@deck.gl/react';
   import { ArcLayer, ScatterplotLayer, HexagonLayer } from '@deck.gl/layers';
   // Accept layers config + viewport state
   // Use OrbitView for non-geo data, MapView for geo
   ```
3. Use cases:
   - **Global money flows**: ArcLayer showing capital movement between countries
   - **Energy network geographic view**: ScatterplotLayer for facilities + ArcLayer for trade routes
   - **GDELT tension map**: HexagonLayer aggregating geopolitical events by location

### Backend complement
- `pip install pydeck` in `requirements.txt`
- Can prototype in Jupyter with `pydeck.Deck()` then export layer configs to frontend

---

## Task 5: Set Up PyVista + Trame for Scientific 3D (Backend)

**Priority:** P2
**Estimated scope:** Medium
**Files to modify:** `requirements.txt`, new `visualization/` module

### Steps
1. `pip install pyvista trame trame-vtk trame-vuetify`
2. Create `grid/visualization/scientific.py`:
   ```python
   import pyvista as pv
   from trame.app import get_server
   from trame.widgets import vtk as vtk_widgets

   def create_volatility_surface(strikes, expiries, ivs):
       """Generate 3D volatility surface mesh."""
       grid = pv.StructuredGrid(...)  # strikes x expiries x ivs
       return grid

   def create_regime_volume(features, labels, timestamps):
       """3D volume of regime states over time."""
       ...
   ```
3. Trame server can run alongside FastAPI or as separate service
4. Embed in PWA via iframe or Trame's JavaScript client

### Use cases
- Volatility surface visualization (strikes × expiry × IV)
- Regime clustering 3D scatter (PC1 × PC2 × PC3, color = regime)
- Feature importance landscape

---

## Task 6: Create Plotly Backend Service for Python→JS Chart Parity

**Priority:** P1
**Estimated scope:** Small
**Files to modify:** `requirements.txt`, `api/routers/` (new router)

### Steps
1. Add `plotly` to `requirements.txt`
2. Create `api/routers/charts.py`:
   ```python
   from fastapi import APIRouter
   import plotly.graph_objects as go
   import plotly.io as pio

   router = APIRouter(prefix="/api/charts", tags=["charts"])

   @router.get("/volatility-surface/{symbol}")
   async def volatility_surface(symbol: str):
       """Return Plotly figure JSON for vol surface."""
       fig = go.Figure(data=[go.Surface(x=strikes, y=expiries, z=ivs)])
       return json.loads(pio.to_json(fig))

   @router.get("/regime-scatter")
   async def regime_scatter(as_of: str):
       """Return Plotly figure JSON for regime clustering."""
       ...

   @router.get("/trust-heatmap")
   async def trust_heatmap():
       """Return Plotly figure JSON for source trust matrix."""
       ...
   ```
3. Frontend consumes: `fetch('/api/charts/trust-heatmap').then(fig => <Plot {...fig} />)`

### Key principle
- Backend generates full Plotly figure JSON (data + layout)
- Frontend renders with zero transformation
- Same figure works in Jupyter notebooks AND browser

---

## Task 7: Upgrade MoneyFlow Sankey (Keep D3, Add Animation)

**Priority:** P1
**Estimated scope:** Small
**Files to modify:** Existing MoneyFlow component

### Steps
1. Keep existing D3 sankey — it's best-in-class for this
2. Add animated particle flow along sankey links:
   ```js
   // Use d3-timer to animate circles along link paths
   // Particle speed ∝ flow magnitude
   // Particle count ∝ flow volume
   ```
3. Add time scrubbing: slider to show flow changes over time
   - Each time step = new sankey data → animate transitions with `d3.transition()`
4. Consider adding a "3D mode" toggle using Plotly `sankey` trace for simpler flows

---

## Task 8: Shared Chart Theme and Config

**Priority:** P0 (do first)
**Estimated scope:** Small
**Files to create:** `pwa/src/components/charts/theme.ts`

### Steps
1. Create `pwa/src/components/charts/theme.ts`:
   ```ts
   // GRID dark theme colors
   export const GRID_COLORS = {
     bg: '#0a0a0f',
     paper: '#12121a',
     text: '#e0e0e0',
     grid: '#1a1a2e',
     accent: '#00d4ff',
     positive: '#00e676',
     negative: '#ff1744',
     series: ['#00d4ff', '#7c4dff', '#ff6e40', '#00e676', '#ffea00', '#ff1744'],
   };

   // Plotly default layout
   export const PLOTLY_LAYOUT = { ... };

   // ECharts registered theme
   export const ECHARTS_THEME = { ... };

   // 3d-force-graph color scheme
   export const FORCE_GRAPH_COLORS = { ... };
   ```
2. All chart wrappers import from this single source of truth
3. Match existing PWA design language

---

## Integration Order

```
Phase 1 (P0): Theme → Plotly wrapper → 3d-force-graph → ActorNetwork 3D
Phase 2 (P1): ECharts wrapper → Charts API router → MoneyFlow animation
Phase 3 (P2): deck.gl → PyVista/Trame → GeoFlow view
Phase 4 (P3): Three.js custom viz (as needed per feature)
```

Each phase is independently deployable. Phase 1 gives the highest impact.
