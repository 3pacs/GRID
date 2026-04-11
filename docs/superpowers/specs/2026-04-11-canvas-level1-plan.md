# Canvas Level 1: Analyst-Grade — Implementation Plan

**Spec:** `2026-04-11-canvas-evolution-design.md`
**Goal:** Unified WebGL canvas workspace with investigation boards, typed nodes, rich edges, temporal scrubbing, and layer system.

---

## Phase 1: Foundation (Rendering Engine + Canvas Shell)

### 1A. Install dependencies

```bash
cd grid/pwa && npm install \
  sigma@^3.0.0 \
  graphology@^0.26.0 \
  graphology-types@^0.24.0 \
  @react-sigma/core@^5.0.0 \
  @sigma/edge-curve@^3.0.0 \
  @sigma/node-border@^3.0.0 \
  graphology-layout-forceatlas2@^0.10.0 \
  graphology-communities-louvain@^2.0.0 \
  graphology-layout@^0.6.0
```

### 1B. Create canvas directory structure

```
pwa/src/canvas/
├── GothamCanvas.jsx          -- Main workspace (layout shell)
├── CanvasStore.js            -- Zustand slice for canvas state
├── SigmaGraph.jsx            -- Sigma container with React hooks
├── programs/                 -- Custom WebGL node/edge renderers
│   └── (Phase 3)
├── panels/                   -- Side panels
│   └── (Phase 4)
└── hooks/                    -- Canvas-specific hooks
    └── (Phase 2+)
```

### 1C. Create `CanvasStore.js`

Zustand store managing:
- `graph` — graphology Graph instance
- `selectedNode` — currently selected node ID + type
- `hoveredNode` — currently hovered node ID
- `activeLayers` — Set of active layer names
- `timeRange` — { start, end } for temporal filter
- `boardId` — current investigation board ID
- `boardName` — current board name
- `boards` — list of saved boards (id, name, updated_at)
- `searchResults` — command palette results
- `detailPanelOpen` — boolean
- Actions: `addNode`, `removeNode`, `expandNode`, `setTimeRange`, `toggleLayer`, `saveBoard`, `loadBoard`

### 1D. Create `GothamCanvas.jsx`

The main workspace layout:
- Full viewport height (`calc(100vh - 56px)` minus nav)
- CSS Grid: `grid-template-columns: 1fr 360px` (main + detail panel)
- Top command bar with search, layers, temporal, board controls
- Center: `<SigmaGraph />` WebGL canvas
- Right: `<DetailPanel />` (collapsible)
- Bottom-left: minimap overlay
- Bottom: intel feed ticker (optional, can defer)
- Dark theme matching `shared.js` colors (`#080C10` bg)

### 1E. Create `SigmaGraph.jsx`

Sigma.js React container:
- `<SigmaContainer>` from @react-sigma/core
- `useLoadGraph` hook to load graphology Graph from store
- `useRegisterEvents` for click, hover, right-click, double-click
- `useCamera` for zoom-to-fit, focus on node
- ForceAtlas2 layout running in Web Worker
- Settings: `defaultNodeColor`, `defaultEdgeColor`, `labelFont`, `labelColor`, `renderEdgeLabels`
- Edge program: `@sigma/edge-curve` for curved edges
- Event handlers wire to CanvasStore (select, hover, expand)

### 1F. Wire into App.jsx

- Add `GothamCanvas` lazy import
- Add route case `'canvas'` in `renderView()` switch
- Add "Canvas" nav item in NavBar.jsx
- Hash route: `#/canvas` and `#/canvas/{boardId}`

**Checkpoint:** Empty canvas renders at `#/canvas` with dark background, Sigma.js WebGL context active, force layout running on empty graph. Verify 60fps with dev tools.

---

## Phase 2: Data Pipeline (Backend → Graph)

### 2A. Create unified graph endpoint

**File:** `api/routers/canvas.py` (new router)

```python
@router.get("/graph")
async def get_canvas_graph(
    center_actor: str | None = Query(None),
    center_ticker: str | None = Query(None),
    depth: int = Query(2, ge=1, le=4),
    layers: str = Query("financial,insider"),
    since: str | None = Query(None),  # ISO date
    limit: int = Query(500, ge=10, le=5000),
    _token: str = Depends(require_auth),
) -> dict:
    """Build a graph around a center entity with typed nodes and edges."""
```

**Logic:**
1. Parse `layers` into set of category filters
2. If `center_actor`: load from `actors` table, expand `actor_connections` to `depth`
3. If `center_ticker`: load from `signal_registry` for that ticker, find related actors
4. For each actor node: fetch recent signals (filtered by `since`)
5. For each actor pair: fetch `wealth_flows` and `dollar_flows`
6. Build response: `{ nodes: [{id, type, label, ...attrs}], edges: [{source, target, type, ...attrs}] }`
7. Apply `limit` (prioritize by influence_score, then signal count)

**Node response shape:**
```json
{
  "id": "actor:jerome_powell",
  "type": "actor",
  "label": "Jerome Powell",
  "tier": "sovereign",
  "category": "central_bank",
  "influence": 0.98,
  "trust_score": 0.91,
  "net_worth": null,
  "aum": null,
  "title": "Chair of Federal Reserve",
  "signal_count": 14,
  "x": null, "y": null
}
```

**Edge response shape:**
```json
{
  "source": "actor:jerome_powell",
  "target": "actor:john_williams",
  "type": "institutional",
  "strength": 0.85,
  "confidence": "confirmed",
  "flow_amount": 0,
  "label": "Fed Board"
}
```

### 2B. Create node detail endpoint

```python
@router.get("/node/{node_type}/{node_id}")
async def get_node_detail(node_type: str, node_id: str, ...) -> dict:
```

Returns rich detail for the selected node:
- **actor**: trust breakdown, recent actions, wealth flows, board seats, positions
- **ticker**: price, sector, related actors, recent signals, options data
- **signal**: source info, historical accuracy, related signals
- **event**: description, related actors, impact assessment

### 2C. Create expand endpoint

```python
@router.get("/expand/{node_type}/{node_id}")
async def expand_node(node_type: str, node_id: str, depth: int = 1, ...) -> dict:
```

Returns additional nodes and edges to add to the existing graph (incremental expansion).

### 2D. Register router in `api/main.py`

```python
from api.routers import canvas
app.include_router(canvas.router, prefix="/api/v1/canvas", tags=["canvas"])
```

### 2E. Create `useCanvasData.js` hook

Frontend hook that:
- Calls `/api/v1/canvas/graph` with current filters
- Transforms response into graphology `graph.import()` format
- Merges incremental expansions without losing existing positions
- Handles loading/error states

**Checkpoint:** Search for "BlackRock" → API returns graph data → Sigma renders 50+ nodes with edges. Force layout positions them. Zoom/pan works at 60fps.

---

## Phase 3: Visual Identity (Custom Node/Edge Programs)

### 3A. Actor node program

**File:** `pwa/src/canvas/programs/ActorNode.js`

Custom Sigma WebGL node program:
- Circle shape with tier-colored fill (gold/blue/purple/cyan)
- Border ring encoding trust score (green→yellow→red gradient)
- Size proportional to `influence_score * 30 + 8` (min 8px, max 38px)
- Label below node in IBM Plex Mono
- Hover state: brighter fill + scale 1.2x
- Selected state: white border ring + glow shadow

### 3B. Ticker node program

**File:** `pwa/src/canvas/programs/TickerNode.js`

- Rounded square shape (custom GLSL)
- Sector color fill (from existing sector color map)
- Price change badge: small circle top-right, green if up, red if down
- Size proportional to signal count
- Label: ticker symbol in bold monospace

### 3C. Signal node program

**File:** `pwa/src/canvas/programs/SignalNode.js`

- Diamond/rotated square shape
- Green fill for bullish, red for bearish, gold for neutral
- Opacity proportional to confidence (0.3 minimum for visibility)
- Size proportional to magnitude
- Label: source type abbreviation (INS, CON, DP, OPT)

### 3D. Event node program

**File:** `pwa/src/canvas/programs/EventNode.js`

- Hexagon shape
- Category color (earnings=blue, regulatory=yellow, macro=gold, deal=green)
- Pulsing animation if event is upcoming (within 7 days)
- Countdown badge showing days until event
- Size proportional to impact magnitude

### 3E. Edge programs

**File:** `pwa/src/canvas/programs/FlowEdge.js`

Extend `@sigma/edge-curve`:
- Color by relationship type (blue=board, green=fund, red=adversarial, gold=political)
- Width: `strength * 3 + 0.5` px
- Opacity: confidence mapping (0.2 rumored → 1.0 confirmed)
- Dashed pattern for connections older than 90 days

**Note:** Animated flow particles are Level 2. Level 1 gets static but rich edges.

**Checkpoint:** Graph renders with distinct node shapes and colors per type. Edges show type/strength encoding. Visually distinguishable categories at a glance.

---

## Phase 4: Interaction Layer

### 4A. Command Palette integration

**File:** Extend existing `pwa/src/components/CommandPalette.jsx`

Add canvas-specific commands:
- Results from `/api/v1/search` tagged with type icons
- "Add to canvas" action for actors, tickers, hypotheses
- On select: call `canvasStore.expandNode(type, id)`
- Canvas-specific shortcuts at top: "Fit to view", "Clear canvas", "Save board"

### 4B. Context menu

**File:** `pwa/src/canvas/ContextMenu.jsx`

- On right-click node: show positioned menu with actions
- Actions per node type (see spec Section 1.6)
- Glass morphism background matching shared.js theme
- Click outside to dismiss
- Keyboard: Enter to execute, Escape to dismiss

### 4C. Detail panel

**File:** `pwa/src/canvas/panels/DetailPanel.jsx`

- 360px wide, right side, collapsible
- Fetches data from `/api/v1/canvas/node/{type}/{id}` on selection
- Sections vary by node type (see spec Section 1.12)
- Scrollable content area
- Action buttons at bottom: Expand, Investigate, Hide, Pin
- Uses shared.js card/badge/metric styles for consistency

### 4D. Keyboard shortcuts

**File:** `pwa/src/canvas/hooks/useKeyboardShortcuts.js`

Wire shortcuts from spec Section 1.11:
- `Cmd+K`: focus command palette
- `F`: fit graph to viewport (sigma camera)
- `E`: expand selected node
- `Delete/Backspace`: hide selected nodes
- `1-8`: toggle layers
- `Escape`: deselect / close panels
- `L`: toggle labels on/off

**Checkpoint:** Right-click a node → context menu appears with actions. Click "Expand" → new nodes appear. Select node → detail panel slides in with rich intel. Keyboard shortcuts work.

---

## Phase 5: Layers + Temporal

### 5A. Layer system

**File:** `pwa/src/canvas/LayerControls.jsx`

- Floating panel (bottom-left, above minimap) or top bar dropdown
- 8 layer toggles (see spec Section 1.8)
- Each toggle adds/removes nodes by category with opacity animation
- Store active layers in `CanvasStore.activeLayers`
- Graph filtering: `graph.filterNodes(node => activeLayers.has(node.category))`

### 5B. Temporal scrubber

**File:** `pwa/src/canvas/TemporalScrubber.jsx`

- Horizontal slider in command bar area
- Range selector: 7d / 30d / 90d / 365d buttons
- Drag handle shows current date
- Filtering: show only nodes/edges where `signal_date >= scrubber.start && signal_date <= scrubber.end`
- Nodes with activity at current time pulse (attribute animation)
- Play/pause button, speed control (1x/5x/10x)

### 5C. Community detection

**File:** `pwa/src/canvas/hooks/useCommunities.js`

- Run `graphology-communities-louvain` on graph when it changes
- Assign community attribute to each node
- Draw convex hull overlays for each community (SVG layer over Sigma canvas)
- Color hulls by dominant category
- Label with most influential member
- Toggle on/off

**Checkpoint:** Toggle "Political" layer → politician nodes appear/disappear. Scrub timeline → signals fade in/out by date. Community hulls group related actors.

---

## Phase 6: Investigation Boards

### 6A. Backend board persistence

**File:** `api/routers/canvas.py` (extend)

```python
@router.post("/boards")
@router.get("/boards")
@router.get("/boards/{board_id}")
@router.put("/boards/{board_id}")
@router.delete("/boards/{board_id}")
@router.post("/boards/{board_id}/fork")
```

**Table:** `investigation_boards` (see spec)

### 6B. Frontend board management

**File:** `pwa/src/canvas/BoardManager.jsx`

- Board picker dropdown in command bar
- "Save" button (Cmd+S) → serialize graph + camera + filters → PUT
- "New Board" → create empty board
- "Fork" → duplicate current board
- Auto-save every 60 seconds (debounced)
- Board list shows name + node count + last updated

### 6C. Board URL routing

- Hash: `#/canvas/{boardId}`
- On load: fetch board state → deserialize into graphology → render
- New canvas (no board): `#/canvas`
- Navigate to board: `#/canvas/abc123`

**Checkpoint:** Save a board → reload page → board restores with same nodes, positions, zoom level. Fork a board → independent copy.

---

## Phase 7: Polish + Performance

### 7A. Minimap

- Use sigma's built-in minimap renderer or custom overlay
- Bottom-left corner, 200x150px
- Shows full graph extent with viewport rectangle
- Click minimap to navigate

### 7B. Loading states

- Skeleton loading for detail panel
- Spinner overlay during graph fetch
- Progressive render: show nodes immediately, edges after layout

### 7C. Performance optimization

- Limit label rendering to nodes above a size threshold at current zoom
- Use graphology's `forEachNode` for efficient iteration
- Debounce temporal scrubber to avoid re-render storm
- Web Worker for ForceAtlas2 (already built into graphology-layout-forceatlas2)

### 7D. Mobile adaptation

- On mobile: detail panel becomes bottom sheet (slide up)
- Context menu adapts to long-press
- Layer controls in hamburger menu
- Temporal scrubber full-width at bottom

**Checkpoint:** Canvas loads in < 200ms. 1000-node graph at 60fps. Mobile layout works on 390px screen.

---

## Files Modified (Existing)

| File | Change |
|------|--------|
| `pwa/package.json` | Add sigma, graphology, @react-sigma deps |
| `pwa/src/App.jsx` | Add GothamCanvas lazy import + route case |
| `pwa/src/components/NavBar.jsx` | Add "Canvas" nav item |
| `pwa/src/components/CommandPalette.jsx` | Add canvas-aware commands |
| `pwa/src/api.js` | Add canvas API methods |
| `api/main.py` | Register canvas router |
| `schema.sql` | Add investigation_boards + canvas_activity tables |

## Files Created (New)

| File | LOC Est. | Purpose |
|------|----------|---------|
| `api/routers/canvas.py` | ~400 | Canvas graph + board API endpoints |
| `pwa/src/canvas/GothamCanvas.jsx` | ~250 | Main workspace layout |
| `pwa/src/canvas/CanvasStore.js` | ~150 | Zustand canvas state |
| `pwa/src/canvas/SigmaGraph.jsx` | ~200 | Sigma.js WebGL container |
| `pwa/src/canvas/ContextMenu.jsx` | ~150 | Right-click menu |
| `pwa/src/canvas/panels/DetailPanel.jsx` | ~350 | Node intelligence panel |
| `pwa/src/canvas/LayerControls.jsx` | ~120 | Layer toggles |
| `pwa/src/canvas/TemporalScrubber.jsx` | ~180 | Timeline slider |
| `pwa/src/canvas/BoardManager.jsx` | ~150 | Board save/load/fork |
| `pwa/src/canvas/programs/ActorNode.js` | ~100 | GLSL actor renderer |
| `pwa/src/canvas/programs/TickerNode.js` | ~100 | GLSL ticker renderer |
| `pwa/src/canvas/programs/SignalNode.js` | ~80 | GLSL signal renderer |
| `pwa/src/canvas/programs/EventNode.js` | ~80 | GLSL event renderer |
| `pwa/src/canvas/programs/FlowEdge.js` | ~80 | Edge renderer |
| `pwa/src/canvas/hooks/useCanvasData.js` | ~120 | Data fetch + transform |
| `pwa/src/canvas/hooks/useTemporalFilter.js` | ~80 | Time-based filtering |
| `pwa/src/canvas/hooks/useCommunities.js` | ~60 | Louvain detection |
| `pwa/src/canvas/hooks/useKeyboardShortcuts.js` | ~60 | Keyboard handler |
| `pwa/src/canvas/hooks/useBoardPersistence.js` | ~100 | Board auto-save |

**Total new code:** ~2,810 LOC estimated

---

## Execution Order

```
Phase 1 (Foundation)     → Can test: empty canvas renders
Phase 2 (Data Pipeline)  → Can test: real data on canvas
Phase 3 (Visual Identity)→ Can test: distinct node types visible
Phase 4 (Interaction)    → Can test: click/expand/detail works
Phase 5 (Layers+Time)    → Can test: filter and scrub
Phase 6 (Boards)         → Can test: save/load investigations
Phase 7 (Polish)         → Can test: performance + mobile
```

Each phase is independently deployable. Each has a clear checkpoint.
