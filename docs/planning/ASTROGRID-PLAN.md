# AstroGrid — Celestial Intelligence Interface

## Vision

AstroGrid is GRID's celestial arm — a standalone interface that visualizes planetary cycles, lunar phases, solar activity, Vedic nakshatras, and Chinese astrological data alongside market behavior. It answers one question: **do celestial cycles correlate with market regimes, and if so, when do the next inflection points occur?**

It runs as a separate PWA sharing GRID's backend, accessible at its own URL path (`/astrogrid/`).

---

## What Already Exists (Foundation)

### Backend (5 celestial pullers, 23 features, all working):
- `ingestion/celestial/lunar.py` — phase, illumination, eclipse proximity (6 features)
- `ingestion/celestial/planetary.py` — retrogrades, aspects, volatility index (5 features)
- `ingestion/celestial/solar.py` — Kp index, sunspots, geomagnetic (7 features, NOAA SWPC)
- `ingestion/celestial/vedic.py` — nakshatras, tithi, Rahu-Ketu, dasha (5 features)
- `ingestion/celestial/chinese.py` — zodiac, elements, flying stars, I Ching (6 features)
- `api/routers/celestial.py` — GET /api/v1/signals/celestial (categorized feature endpoint)

### Frontend (minimal, embedded in GRID):
- Celestial tab in Signals view — flat feature cards, no visualization
- No 3D, no interactivity, no narrative

---

## Architecture

```
/data/grid_v4/grid_repo/grid/
├── pwa/                          # Existing GRID app (unchanged)
├── astrogrid/                    # NEW — standalone AstroGrid app
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── main.jsx              # Entry point
│       ├── App.jsx               # Router + layout
│       ├── store.js              # Zustand store (astro-specific)
│       ├── api.js                # API client (extends GRID's auth)
│       ├── styles/
│       │   └── tokens.js         # Design tokens (dark cosmic theme)
│       ├── views/
│       │   ├── Orrery.jsx        # 3D solar system / planetary positions
│       │   ├── LunarDashboard.jsx # Moon phase wheel + market overlay
│       │   ├── Ephemeris.jsx     # Interactive date calculator
│       │   ├── Correlations.jsx  # Astro-market correlation heatmap
│       │   ├── Timeline.jsx      # Celestial event timeline + market events
│       │   ├── Narrative.jsx     # LLM-generated celestial briefing
│       │   └── Settings.jsx      # Feature toggles, date range, preferences
│       ├── components/
│       │   ├── PlanetaryOrrery.jsx    # Three.js 3D orrery
│       │   ├── MoonPhaseWheel.jsx     # SVG/Canvas moon visualization
│       │   ├── NakshatraWheel.jsx     # 27-segment Vedic wheel
│       │   ├── RetrogradeBanner.jsx   # Active retrograde alert bar
│       │   ├── EclipseCountdown.jsx   # Next eclipse with market context
│       │   ├── CorrelationHeatmap.jsx # D3 heatmap: celestial × market
│       │   ├── CelestialTimeline.jsx  # Scrollable event ribbon
│       │   ├── SolarActivityGauge.jsx # Kp index + geomagnetic gauge
│       │   ├── ChineseCalendar.jsx    # Year/element/flying star display
│       │   └── NavBar.jsx             # AstroGrid bottom nav
│       └── lib/
│           ├── ephemeris.js      # Client-side planetary calculations
│           ├── aspects.js        # Aspect geometry (conjunction, opposition, etc.)
│           └── interpret.js      # Celestial interpretation helpers
├── api/routers/
│   └── astrogrid.py              # NEW — expanded celestial API
└── analysis/
    └── astro_correlations.py     # NEW — celestial-market correlation engine
```

### Build & Deploy
- Separate Vite config, separate `package.json`
- Builds to `../astrogrid_dist/`
- Served by FastAPI as static files at `/astrogrid/`
- Shares GRID's auth (same JWT, same login)
- Proxied through same Cloudflare tunnel

---

## Phase Breakdown

### ASTRO-01: App Scaffold + Dark Cosmic Theme
**Goal:** Standalone AstroGrid app boots, authenticates, shows a landing page.

- `astrogrid/` directory with Vite + React + Zustand
- Dependencies: three.js, @react-three/fiber, @react-three/drei, d3, zustand, lucide-react
- Design tokens: deep space theme (navy/black bg, stellar blue accent, nebula purple highlights, golden celestial accents)
- Shared auth with GRID (reads same JWT from localStorage)
- Bottom nav: Orrery, Moon, Stars, Correlations, Timeline, Narrative
- FastAPI static mount at `/astrogrid/` in api/main.py
- Landing page with current celestial state summary

### ASTRO-02: 3D Planetary Orrery
**Goal:** Interactive 3D solar system showing current planetary positions, retrogrades, and aspects.

- Three.js scene via @react-three/fiber
- Sun at center, planets on orbital paths (scaled for visibility, not accuracy)
- Current positions computed from Keplerian elements (reuse planetary.py math in JS)
- Retrograde planets highlighted (red glow, reversed orbit arrow)
- Aspect lines drawn between planets (conjunction=gold, opposition=red, trine=green, square=orange)
- Click planet → side panel with: current sign, degree, speed, aspect list, market correlation
- Time scrubber: drag to see positions on any date (past/future)
- Tooltip: "Mercury in retrograde: historically correlated with X% more volatile days"

### ASTRO-03: Ephemeris Calculator + Lunar Dashboard
**Goal:** Interactive date-based celestial lookup and rich moon visualization.

- **Ephemeris:** Pick any date → see all planetary positions, active aspects, lunar phase, nakshatra, Chinese calendar data. Compare two dates side-by-side.
- **Lunar Dashboard:** SVG moon phase wheel (current phase with illumination gradient). Moon phase calendar (month view with mini-moons per day). Phase vs. market returns scatter plot. "Trading around the moon" statistics (new moon ±3 days vs full moon ±3 days performance).
- **Nakshatra Wheel:** 27-segment circular chart. Current nakshatra highlighted. Historical market returns per nakshatra (color intensity = strength of correlation).

### ASTRO-04: Market-Astro Correlation Engine
**Goal:** Systematic statistical analysis of celestial-market relationships.

- New `analysis/astro_correlations.py`:
  - For each celestial feature × market feature pair, compute: Pearson correlation, lead/lag (±30 days), regime-conditional correlation
  - Bootstrap significance testing (reject if p > 0.05)
  - Output: ranked list of significant correlations with confidence intervals
- New API: `GET /api/v1/astrogrid/correlations?market_feature=SPY&celestial_category=lunar`
- Frontend heatmap: celestial features on Y-axis, market features on X-axis, color = correlation strength
- Drill-down: click cell → time series overlay chart showing both features
- Discovery mode: "What celestial events preceded the last 5 major drawdowns?"

### ASTRO-05: Celestial Narrative Synthesis
**Goal:** LLM-generated celestial briefing combining all astro data with market context.

- New prompt template for Qwen: "Given these celestial conditions [lunar phase, retrogrades, aspects, nakshatras, solar activity] and current market regime [from GRID], synthesize a celestial market outlook."
- Daily celestial briefing (separate from GRID's market briefing)
- Historical pattern matching: "The last time Mercury was retrograde during a full moon in Aries with Kp > 5 was [date] — market did [X]"
- Confidence calibration: explicitly state which correlations have statistical backing vs. which are traditional/speculative
- Wired into intelligence loop (daily at 05:00 alongside market briefing)

### ASTRO-06: Celestial Event Timeline
**Goal:** Scrollable timeline of past and upcoming celestial events overlaid with market events.

- Horizontal scrollable ribbon: eclipses, retrogrades, major aspects, equinoxes/solstices
- Market events overlaid: regime changes, major drawdowns, rallies
- Filter by: event type, planet, zodiac sign
- Zoom: day/week/month/year
- "Upcoming" section: next 30 days of celestial events with market context from correlation engine
- Export: iCal feed of celestial events

---

## API Expansion (astrogrid.py)

New endpoints beyond the existing `/api/v1/signals/celestial`:

```
GET  /api/v1/astrogrid/overview          # Current state of all celestial systems
GET  /api/v1/astrogrid/ephemeris?date=   # Full ephemeris for any date
GET  /api/v1/astrogrid/correlations      # Astro-market correlation results
GET  /api/v1/astrogrid/timeline          # Event timeline with market overlay
GET  /api/v1/astrogrid/briefing          # Latest celestial narrative
POST /api/v1/astrogrid/compare           # Compare two dates' celestial state
GET  /api/v1/astrogrid/retrograde        # Active/upcoming retrogrades
GET  /api/v1/astrogrid/eclipses          # Eclipse calendar with market history
GET  /api/v1/astrogrid/nakshatra         # Current nakshatra with market stats
GET  /api/v1/astrogrid/lunar/calendar    # Monthly moon phase calendar
GET  /api/v1/astrogrid/solar/activity    # Current solar weather
```

---

## Design Language

AstroGrid has its own visual identity while sharing GRID's DNA:

- **Background:** Deep space gradient (#050810 → #0A1628)
- **Accent:** Stellar blue (#4A9EFF) + nebula purple (#8B5CF6)
- **Celestial gold:** #D4A574 (for planetary bodies, aspect lines)
- **Typography:** Same IBM Plex family as GRID
- **Cards:** Slightly more translucent than GRID (glass morphism with backdrop-filter)
- **Animations:** Smooth orbital transitions, phase wheel rotation, constellation twinkle
- **Mobile-first:** Same PWA pattern as GRID, installable separately

---

## Dependencies

```json
{
  "three": "^0.170.0",
  "@react-three/fiber": "^8.17.0",
  "@react-three/drei": "^9.115.0",
  "d3": "^7.9.0",
  "zustand": "^4.5.0",
  "react": "^18.3.0",
  "react-dom": "^18.3.0",
  "lucide-react": "^0.344.0"
}
```

---

## Execution Order

1. **ASTRO-01** (scaffold) — must be first, everything builds on it
2. **ASTRO-06** (API expansion) — backend endpoints needed by all views
3. **ASTRO-02** (orrery) + **ASTRO-03** (ephemeris/lunar) — can parallelize
4. **ASTRO-04** (correlations) — needs data flowing first
5. **ASTRO-05** (narrative) — needs correlations + all views for context
6. **ASTRO-06** (timeline) — polish piece, last
