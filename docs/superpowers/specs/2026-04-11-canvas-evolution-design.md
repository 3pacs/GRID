# Canvas Evolution: From Preschool to Palantir

**Date:** 2026-04-11
**Status:** Design Complete, Ready for Implementation

---

## The Problem

The GRID canvas is at **18.5% of its potential**. The backend has 130 intelligence modules, 495+ actors, 5M+ connections, 443K signals, and 28 intelligence modules producing forensics, causation chains, event timelines, and trust-scored evidence. The frontend shows circles on lines. Disconnected views. No investigation workflow. No temporal dimension. D3 Canvas 2D that chokes past 1K nodes.

This is a Palantir-class data engine with a preschool UI.

---

## Two-Level Evolution

### Level 1: Analyst-Grade Canvas
**Goal:** A single unified workspace where every piece of intelligence is one click away. Professional graph visualization that handles 100K nodes. Investigation boards you can save, name, and return to. The canvas IS the product.

### Level 2: Command Center
**Goal:** Real-time streaming intelligence. AI-driven investigation. Causation chains. Geographic overlays. Hypothesis boards that evolve. The canvas thinks with you.

---

## Level 1: Analyst-Grade Canvas

### 1.1 Rendering Engine Upgrade

**Current:** D3.js + Canvas 2D (ActorNetwork.jsx, ~600 LOC). Animated money-flow particles exist but hit performance ceiling at ~500 nodes.

**Target:** Sigma.js v3 + Graphology + @react-sigma/core

| Package | Purpose |
|---------|---------|
| `sigma` (v3.0.2) | WebGL graph renderer, custom GLSL node/edge programs |
| `graphology` | Graph data structure (nodes, edges, attributes) |
| `@react-sigma/core` (v5) | React 18 hooks: useSigma, useRegisterEvents, useLoadGraph |
| `@sigma/edge-curve` | Curved bezier edges |
| `graphology-layout-forceatlas2` | Force-directed layout in Web Worker (non-blocking) |
| `graphology-communities-louvain` | Community detection for cluster hulls |

**Why Sigma:** Only WebGL library that handles 100K+ nodes at 60fps with custom GLSL shaders and first-class React integration. Everything else either caps at 20K (cytoscape, react-force-graph) or has no graph semantics (deck.gl, pixi.js).

**Bundle impact:** ~150KB gzipped total. Current D3 is ~130KB. Marginal increase.

### 1.2 Node Type System

Every entity on the canvas is a typed node with distinct visual treatment.

| Node Type | Shape | Color | Size Encoding | Ring/Badge |
|-----------|-------|-------|---------------|------------|
| **actor** | Circle | Tier color (gold/blue/purple/cyan) | `influence_score * 30` | Trust ring (green=high, red=low) |
| **ticker** | Rounded square | Sector color | Market cap (log) | Price change badge (+/-%) |
| **signal** | Diamond | Direction (green=bullish, red=bearish) | Confidence (0-1) | Source type icon |
| **event** | Hexagon | Category color | Impact magnitude | Countdown badge (days until) |
| **hypothesis** | Triangle | Status color (blue=testing, green=passed, red=killed) | Evidence count | Confidence ring |
| **flow** | Small dot (on edge) | Flow type (green=money, red=outflow, gold=influence) | Amount (log scale) | None |

Each type is a custom Sigma WebGL node program (GLSL vertex + fragment shader). Nodes are NOT all circles anymore.

### 1.3 Edge Intelligence

Edges carry meaning. They're not just lines.

| Edge Attribute | Visual Encoding |
|----------------|----------------|
| **type** | Color (blue=board, green=fund, red=adversarial, gold=political, gray=inferred) |
| **strength** | Width (0.5px weak → 4px strong) |
| **confidence** | Opacity (0.2 rumored → 1.0 confirmed) |
| **direction** | Arrow head |
| **flow amount** | Animated particles along edge (size = log amount, speed = recency) |
| **age** | Dash pattern (solid = recent, dashed = old) |

Curved edges via `@sigma/edge-curve` prevent overlap on parallel connections.

### 1.4 Unified Workspace Layout

Replace the current separate pages (ActorNetwork, Flows, Associations, etc.) with a single canvas workspace.

```
+------------------------------------------------------------------+
|  [Search...]  [Layers v]  [Time --|----]  [Save]  [Boards v]     |
+------------------------------------------------------------------+
|                                                    +-------------+|
|                                                    | DETAIL      ||
|                                                    | PANEL       ||
|            MAIN GRAPH CANVAS                       |             ||
|            (sigma.js WebGL)                        | Actor: JPow ||
|                                                    | Trust: 0.91 ||
|                                                    | Flows: $2.1T||
|                                                    | Signals: 14 ||
|  +--------+                                        | Actions:    ||
|  |minimap |                                        | [Expand]    ||
|  +--------+                                        | [Investigate||
|                                                    | [Hide]      ||
+------------------------------------------------------------------+
|  Intel Feed: [signal] [signal] [signal] >>>                      |
+------------------------------------------------------------------+
```

**Components:**

1. **Command Bar** (top) — Search, layer toggles, temporal scrubber, board management
2. **Main Canvas** (center) — Full-viewport Sigma.js WebGL graph
3. **Detail Panel** (right, collapsible) — Rich intel for selected node
4. **Minimap** (bottom-left overlay) — Sigma built-in minimap
5. **Intel Feed** (bottom ticker) — Scrolling real-time intelligence signals

### 1.5 Search-First UX

The canvas starts empty or with a saved board. Users build investigations by searching.

**Command Palette** (Cmd+K / Ctrl+K):
- Type actor name → add actor node + immediate neighbors
- Type ticker → add company node + insider/congressional signals
- Type event → add event node + related actors
- Type "sector:technology" → add sector cluster
- Type "hypothesis:..." → add hypothesis node + evidence

Search hits the existing `/api/v1/search` endpoint (already indexes views, tickers, features, hypotheses, actors).

### 1.6 Context Menu (Right-Click)

Right-click any node for contextual actions:

**Actor node:**
- Expand connections (depth 1/2/3)
- Show wealth flows
- Show trading history
- View trust score breakdown
- Pin/unpin position
- Hide from canvas
- Start investigation thread

**Ticker node:**
- Show insider activity
- Show congressional trades
- Show options positioning
- Show price history overlay
- Connect to related actors
- Add catalyst events

**Signal node:**
- View source data
- Show related signals (same ticker/actor)
- View confidence breakdown
- Mark as investigated

### 1.7 Investigation Boards

Persist canvas state so investigations survive across sessions.

**Database table: `investigation_boards`**
```sql
CREATE TABLE investigation_boards (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    graph_state JSONB NOT NULL,  -- serialized graphology graph
    camera_state JSONB,          -- zoom, pan position
    filters JSONB,               -- active layers, time range
    pinned_nodes TEXT[],         -- manually positioned nodes
    annotations JSONB,           -- user notes on nodes/edges
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
```

**Board operations:**
- Save current canvas state (Cmd+S)
- Load a saved board (board picker dropdown)
- Auto-save every 60 seconds
- Fork a board (duplicate for branching investigations)
- Delete a board

### 1.8 Layer System

Toggle data layers on/off to focus the investigation:

| Layer | What it shows | Default |
|-------|--------------|---------|
| **Political** | Politicians, PAC flows, lobbying, regulation | OFF |
| **Financial** | Fund managers, 13F positions, AUM | ON |
| **Insider** | Form 4 trades, cluster buys, insider rings | ON |
| **News** | Breaking news events, sentiment shifts | OFF |
| **Options** | Gamma walls, unusual flow, 100x opportunities | OFF |
| **Macro** | Central banks, rate decisions, liquidity | OFF |
| **Offshore** | Panama/Pandora papers, shell companies | OFF |
| **Predictions** | GRID recommendations, hypothesis outcomes | OFF |

Each layer maps to a `category` filter on nodes and edges. Toggle adds/removes nodes with animated opacity transition.

### 1.9 Temporal Scrubber

A timeline slider that controls the canvas state at any point in time.

- Range: last 7/30/90/365 days (selectable)
- Scrubbing changes which signals, flows, and events are visible
- Nodes pulse when they have activity at the current time
- Edges appear/disappear based on when connections were active
- Play/pause button auto-advances through time
- Speed control (1x, 5x, 10x)

**Data requirement:** All signals, flows, and events already have timestamps. The scrubber filters the graphology graph by `signal_date`/`flow_date`/`created_at`.

### 1.10 Community Detection & Visual Clusters

Run `graphology-communities-louvain` on the loaded graph to detect clusters.

- Draw translucent convex hull around each community
- Color hulls by dominant category
- Label each cluster with the most influential actor's name
- Collapse/expand clusters on double-click (aggregate node)

### 1.11 Keyboard Shortcuts

| Key | Action |
|-----|--------|
| `Cmd+K` | Open command palette (search) |
| `Cmd+S` | Save board |
| `Cmd+Z` | Undo last action |
| `F` | Fit graph to viewport |
| `L` | Toggle labels |
| `1-8` | Toggle layers 1-8 |
| `Space` | Play/pause temporal scrubber |
| `Delete` | Hide selected nodes |
| `E` | Expand selected node |
| `I` | Open detail panel for selected |
| `Esc` | Deselect / close panels |

### 1.12 Detail Panel

When a node is selected, the right panel shows rich intelligence:

**For actors:**
- Photo/avatar placeholder, name, title, tier badge
- Trust score gauge (0-1) with trend arrow
- Influence rank (#N of total)
- Recent actions (last 30 days) as timeline
- Connected actors (top 10 by strength)
- Wealth flows (in/out with amounts)
- Board seats
- Political affiliations
- Known positions (tickers + direction)
- Motivation model tag

**For tickers:**
- Current price + change
- Sector/subsector
- Related actors (insiders, congressional, institutional)
- Recent signals (sorted by confidence)
- Options positioning (gamma, vanna, IV percentile)
- Catalyst calendar (upcoming events)

**For signals:**
- Source, confidence, direction
- Actor who generated the signal
- Historical accuracy of this source
- Related signals (same period, same ticker)
- Price impact since signal date

---

## Level 2: Command Center

Everything in Level 1 plus:

### 2.1 Real-Time Streaming

WebSocket pushes live intelligence events directly into the graph:

- New signal → node appears with glow animation + sound cue
- Price move → ticker node pulses (green/red)
- New connection discovered → edge draws itself in
- Hypothesis killed → node turns red and shrinks
- Breaking news → event node drops in from top with ripple

**Implementation:** Extend existing WebSocket handler (`store.js:handleWsMessage`) with new event types: `graph_node_add`, `graph_edge_add`, `graph_node_update`, `graph_signal_alert`.

### 2.2 AI Investigation Assistant

A natural language interface embedded in the canvas.

- "Who is connected to both Pelosi and Citadel?"
- "Show me all insider buying before AAPL earnings"
- "Why did this hypothesis get killed?"
- "What's the money trail from BlackRock to this ticker?"
- "Compare congressional trading accuracy vs insider accuracy"

The AI queries the backend intelligence modules and materializes results as graph additions. Uses existing Hermes/Ollama infrastructure.

### 2.3 Causation Chain Visualization

Wire `intelligence/causation.py` output to the canvas:

```
[Actor: Powell] --rate_decision--> [Event: Rate Hold]
    --yield_curve--> [Signal: Curve Steepens]
        --sector_rotation--> [Ticker: XLF +2.3%]
            --insider_cluster--> [Actor: Dimon Buys]
```

Render as a directed path with animated flow. Each link labeled with the causal mechanism. Click any link to see the evidence.

### 2.4 Split View

Three-panel layout for deep investigation:

```
+-------------------+---------------------------+
|                   |                           |
|   GRAPH CANVAS    |    TIMELINE VIEW          |
|                   |    (horizontal events)    |
|                   |                           |
+-------------------+---------------------------+
|                DETAIL / EVIDENCE PANEL         |
+------------------------------------------------+
```

- Graph and timeline are linked: selecting a node highlights its events, selecting an event highlights its actors
- Detail panel shows evidence chains for any selected item
- Panels are resizable with drag handles

### 2.5 Hypothesis Board Mode

A special canvas mode for tracking market theses:

- Hypothesis nodes are central, evidence nodes orbit them
- Supporting evidence = green edges pulling toward hypothesis
- Contradicting evidence = red edges pushing away
- Node glow intensity = net evidence score
- When evidence tips past threshold → hypothesis auto-promoted/killed
- Historical hypotheses shown with outcome (green checkmark / red X)

### 2.6 Evidence Lineage

Click any signal or data point to see its full provenance:

```
[Raw Data: FRED API]
    → [Resolved: PIT Store, vintage 2026-04-10]
        → [Feature: yield_curve_slope, z=2.1]
            → [Signal: curve_steepening, conf=0.87]
                → [Hypothesis: risk_on_rotation]
                    → [Recommendation: Long XLF May $45C]
```

Renders as a vertical waterfall inside the detail panel. Every link is clickable to inspect the raw data.

### 2.7 Geographic Overlay

Use deck.gl as a secondary layer:

- Toggle between graph view and geo view
- Actors placed on world map at their headquarters/jurisdiction
- Flow arcs between locations (thickness = amount)
- Heat map overlay for regional signal density
- Country risk scoring from GDELT tension data

### 2.8 Multi-Board Workspaces

Tabbed interface for parallel investigations:

- Each tab is an independent investigation board
- Tabs show board name + node count
- Drag nodes between tabs
- "Compare boards" mode: side-by-side view

### 2.9 Anomaly Highlighting

Automatic visual alerts when the system detects anomalies:

- Nodes with extreme z-scores get a pulsing red halo
- Edges with sudden strength changes get a lightning effect
- Clusters that fragment get a breaking-apart animation
- New circular flows get highlighted with a rotating ring

### 2.10 Export & Reporting

- Screenshot canvas as PNG (high-res)
- Export investigation as PDF report (graph image + evidence + timeline + findings)
- Export graph data as JSON/CSV for further analysis
- Share board via URL (read-only mode for collaborators)

---

## Technical Architecture

### New Files (Level 1)

```
pwa/src/
├── canvas/
│   ├── GothamCanvas.jsx           -- Main canvas workspace component
│   ├── CanvasStore.js             -- Zustand store for canvas state
│   ├── CommandPalette.jsx         -- Cmd+K search overlay
│   ├── DetailPanel.jsx            -- Right-side intelligence panel
│   ├── IntelFeed.jsx              -- Bottom scrolling signal ticker
│   ├── LayerControls.jsx          -- Layer toggle sidebar
│   ├── TemporalScrubber.jsx       -- Timeline slider component
│   ├── ContextMenu.jsx            -- Right-click menu
│   ├── Minimap.jsx                -- Graph minimap overlay
│   ├── BoardManager.jsx           -- Save/load/fork boards
│   ├── programs/
│   │   ├── ActorNode.js           -- Custom GLSL actor node program
│   │   ├── TickerNode.js          -- Custom GLSL ticker node program
│   │   ├── SignalNode.js          -- Custom GLSL signal node program
│   │   ├── EventNode.js           -- Custom GLSL event node program
│   │   ├── HypothesisNode.js      -- Custom GLSL hypothesis node program
│   │   ├── FlowEdge.js            -- Animated flow edge program
│   │   └── StrengthEdge.js        -- Strength-encoded edge program
│   └── hooks/
│       ├── useCanvasData.js       -- Fetch + transform graph data
│       ├── useTemporalFilter.js   -- Time-based graph filtering
│       ├── useCommunities.js      -- Louvain community detection
│       ├── useKeyboardShortcuts.js
│       └── useBoardPersistence.js -- Auto-save, load, fork
```

### New API Endpoints (Level 1)

```
POST   /api/v1/canvas/boards              -- Create board
GET    /api/v1/canvas/boards              -- List boards
GET    /api/v1/canvas/boards/{id}         -- Get board state
PUT    /api/v1/canvas/boards/{id}         -- Update board state
DELETE /api/v1/canvas/boards/{id}         -- Delete board
POST   /api/v1/canvas/boards/{id}/fork    -- Fork a board

GET    /api/v1/canvas/graph               -- Unified graph endpoint
       ?center_actor={id}                 -- Ego graph around actor
       &depth=2                           -- Expansion depth
       &layers=financial,insider          -- Active layers
       &since=2026-01-01                  -- Temporal filter
       &limit=500                         -- Max nodes

GET    /api/v1/canvas/node/{type}/{id}    -- Detail panel data
GET    /api/v1/canvas/expand/{type}/{id}  -- Expand node neighbors
GET    /api/v1/canvas/timeline            -- Events for temporal scrubber
```

### New Database Tables

```sql
-- Investigation boards
CREATE TABLE investigation_boards (
    id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
    name TEXT NOT NULL,
    description TEXT,
    graph_state JSONB NOT NULL,
    camera_state JSONB,
    filters JSONB,
    pinned_nodes TEXT[],
    annotations JSONB DEFAULT '[]',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Canvas activity log (what the user explored)
CREATE TABLE canvas_activity (
    id SERIAL PRIMARY KEY,
    board_id TEXT REFERENCES investigation_boards(id),
    action TEXT NOT NULL,  -- 'expand', 'search', 'filter', 'annotate'
    target_type TEXT,
    target_id TEXT,
    metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
```

### Data Flow

```
User searches "BlackRock"
    → Command Palette hits /api/v1/search?q=BlackRock
    → Returns actor_id
    → Frontend calls /api/v1/canvas/graph?center_actor=blackrock&depth=2
    → Backend:
        1. Load actor from actors table
        2. Load connections from actor_connections (depth 2)
        3. Load recent signals for all connected entities
        4. Load wealth_flows for all actors
        5. Apply layer filters
        6. Apply temporal filter
        7. Return { nodes: [...], edges: [...], metadata: {...} }
    → Frontend builds graphology Graph
    → Sigma.js renders with custom programs
    → ForceAtlas2 runs in Web Worker
    → Community detection runs
    → Hull overlays drawn
    → User sees rich, interactive network
```

### Performance Budget

| Metric | Target |
|--------|--------|
| Initial render (500 nodes) | < 200ms |
| Force layout convergence (500 nodes) | < 2s |
| Interaction latency (zoom/pan/drag) | < 16ms (60fps) |
| Node expansion (fetch + render) | < 500ms |
| Board save/load | < 300ms |
| Search results | < 200ms |
| Max nodes before degradation | 50,000 |

---

## What's NOT Changing

- Backend intelligence modules stay as-is (they're solid)
- Existing API endpoints remain (new canvas endpoints are additive)
- Other views (Dashboard, Regime, Backtest, etc.) remain as separate pages
- Zustand stays (new canvas store is a separate slice)
- Vite build pipeline stays
- Authentication stays the same

---

## Dependencies to Install

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

---

## Success Criteria

### Level 1 Done When:
1. Canvas renders 10K+ nodes at 60fps
2. 5 distinct node types with custom WebGL rendering
3. Edges encode type, strength, confidence, and flow
4. Search adds entities to canvas in < 500ms
5. Boards persist across sessions
6. Right-click context menus work on all node types
7. Temporal scrubber filters graph by date range
8. Layer toggles show/hide data categories
9. Community detection draws cluster hulls
10. Detail panel shows rich intel for any selected node

### Level 2 Done When:
1. Live signals stream into graph via WebSocket
2. AI assistant answers graph queries in natural language
3. Causation chains render as directed paths
4. Split view (graph + timeline + detail) works
5. Hypothesis board mode tracks evidence accumulation
6. Geographic overlay places actors on world map
7. Multi-tab workspaces support parallel investigations
8. Export produces PDF investigation reports
