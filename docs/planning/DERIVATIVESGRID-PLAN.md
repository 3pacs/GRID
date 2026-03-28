# DerivativesGrid — Dealer Flow & Volatility Intelligence Interface

## Vision

DerivativesGrid is the Cem Karsan lens on GRID — a standalone interface that makes dealer positioning, gamma exposure, vanna/charm flows, vol surfaces, and options structure the PRIMARY narrative for understanding market direction. The core thesis: **dealers hedging their book ARE the market's mechanical force. Price is downstream of positioning.**

It runs as a separate PWA at `/derivatives/`, same pattern as AstroGrid.

---

## What Already Exists (Backend — Strong Foundation)

### Dealer Gamma Engine (`physics/dealer_gamma.py`)
- Full Black-Scholes Greeks: gamma, delta, vanna, charm
- `DealerGammaEngine`: aggregate GEX from options chains
- Outputs: gex_aggregate, gamma_flip, gamma_wall, put_wall, call_wall, dealer_delta, vanna_exposure, charm_exposure, regime (LONG/SHORT/NEUTRAL), per-strike GEX profile
- `compute_all_tickers()`, `get_market_gex_summary()`

### Options Pipeline (`ingestion/options.py`)
- 40 tickers, Yahoo Finance chains
- Daily signals: PCR, max pain, IV skew, IV ATM, term structure slope, OI concentration, IV wings (25d put/call)
- `options_snapshots` table (per-contract) + `options_daily_signals` (aggregated)

### Options Scanner (`discovery/options_scanner.py`)
- Mispricing detection: PCR extremes, skew dislocation, max pain divergence, OI spikes, term structure inversion, IV percentile, gamma imbalance
- 100x opportunity flagging

### Physics Module (`physics/`)
- `transforms.py`: energy decomposition, OU process, Hurst exponent, Langevin dynamics
- `momentum.py`: sentiment momentum with physics analogs
- `news_energy.py`: narrative energy decomposition
- `conventions.py`: unit/dimension system

### Current Frontend (Minimal)
- `Options.jsx`: flat signal cards, scanner results, 100x tab — no visualization
- `Physics.jsx`: energy dashboard, Hurst, OU params — no dealer flow
- Neither view tells the Karsan story

---

## Architecture

```
/data/grid_v4/grid_repo/grid/
├── derivatives/                      # NEW — standalone DerivativesGrid app
│   ├── index.html
│   ├── package.json
│   ├── vite.config.js
│   └── src/
│       ├── main.jsx
│       ├── App.jsx
│       ├── store.js
│       ├── api.js
│       ├── styles/
│       │   └── tokens.js            # Dark terminal aesthetic
│       ├── views/
│       │   ├── DealerFlow.jsx        # THE core view — GEX regime, gamma flip, walls
│       │   ├── VolSurface.jsx        # 3D vol surface + skew curves
│       │   ├── GammaProfile.jsx      # GEX by strike chart, vanna/charm decomposition
│       │   ├── TermStructure.jsx     # IV term structure + calendar spread analysis
│       │   ├── FlowNarrative.jsx     # LLM-generated dealer flow briefing
│       │   ├── PositionHeatmap.jsx   # OI heatmap by strike × expiry
│       │   ├── Scanner.jsx           # Mispricing opportunities
│       │   └── Settings.jsx
│       ├── components/
│       │   ├── GEXChart.jsx          # Interactive GEX vs spot chart
│       │   ├── VolSurface3D.jsx      # Three.js vol surface
│       │   ├── SkewCurve.jsx         # IV smile/smirk by strike
│       │   ├── GammaWalls.jsx        # Visual put wall / call wall / flip zone
│       │   ├── VannaCharmGauge.jsx   # Vanna + charm flow gauges
│       │   ├── DealerRegimeBadge.jsx # LONG_GAMMA / SHORT_GAMMA indicator
│       │   ├── OIHeatmap.jsx         # D3 heatmap: strike × expiry
│       │   ├── MaxPainChart.jsx      # Max pain analysis visual
│       │   ├── PCRGauge.jsx          # Put/call ratio gauge
│       │   ├── TermCurve.jsx         # Term structure line chart
│       │   ├── NavBar.jsx
│       │   └── TickerSelector.jsx    # Quick-switch between 40 tickers
│       └── lib/
│           ├── greeks.js             # Client-side BS Greeks calculator
│           └── interpret.js          # Karsan-style interpretation helpers
├── api/routers/
│   └── derivatives.py               # NEW — expanded derivatives API
└── analysis/
    └── vol_surface.py                # NEW — vol surface construction
```

---

## Views

### 1. DealerFlow (Home View) — "Where are dealers positioned?"
The Karsan command center. Everything flows from here.

**Top banner:** Current dealer regime — LONG GAMMA (green, "dealers dampening moves, mean-reversion likely") or SHORT GAMMA (red, "dealers amplifying moves, trend continuation/acceleration")

**Key metrics row:**
- GEX Aggregate ($ billions, with direction arrow)
- Gamma Flip (strike price — "above = long gamma, below = short gamma")
- Vanna Exposure (sensitivity to vol changes — "if IV spikes, dealers must buy/sell $X")
- Charm Exposure (sensitivity to time — "over the weekend, dealers will need to adjust $X")
- Net Delta (directional dealer exposure)
- Days to OpEx (with gamma pin explanation)

**GEX Profile Chart:** Interactive chart showing GEX by spot price (x-axis: price, y-axis: $ gamma). Current spot price vertical line. Gamma flip highlighted. Put wall and call wall marked. Shade regions: green = long gamma zone, red = short gamma zone.

**Interpretation panel:** Plain-English narrative: "SPY dealers are short $2.3B gamma. The gamma flip is at 520. We're currently at 515 — in the short gamma zone. This means dealer hedging will AMPLIFY moves. If we move down, dealers sell into weakness. The put wall at 500 provides a support magnet. The vanna exposure of -$1.8B means if VIX spikes, dealers must sell additional delta — accelerating any selloff. Charm flow of +$300M/day means time decay is slowly reducing dealer short gamma — the regime is healing."

### 2. VolSurface — "What is the market pricing?"
**3D Surface:** Three.js surface plot (x: strike/moneyness, y: days to expiry, z: implied vol). Color = IV level (cool blues for low, hot reds for high). Interactive rotation/zoom. Click point = show exact strike, expiry, IV, Greeks.

**Skew Panel:** 2D IV smile/smirk curves for selected expiries. Overlay multiple expiries to see skew evolution. Skew metric: 25d put IV - 25d call IV. Historical skew percentile.

**What this tells you:** "The skew is steep (puts expensive vs calls) — market pricing downside protection. Term structure is inverted near-term — event risk is being priced for this week's FOMC."

### 3. GammaProfile — "Where are the walls?"
**Strike-by-strike GEX bar chart.** Each bar = net gamma at that strike. Color: green = call gamma dominated, red = put gamma dominated.

**Decomposition tabs:**
- Gamma only (standard GEX)
- Vanna component (how much of the flow is vol-driven)
- Charm component (how much is time-driven)
- Combined (total dealer flow = gamma + vanna + charm)

**Walls visualization:** Horizontal bars showing put_wall, call_wall, gamma_flip with current spot. "Price is magnetically attracted to max gamma walls. The call wall at 530 acts as resistance — dealers sell into rallies there."

### 4. TermStructure — "What's the vol calendar saying?"
**Term structure curve:** IV by expiry date (x: DTE, y: IV). Normal (upward sloping) vs inverted (near-term elevated).

**Calendar spread analysis:** Highlight steepest curve segments = best calendar spread opportunities.

**Historical overlay:** Compare today's term structure to 1w ago, 1m ago.

**Interpretation:** "Term structure inverted in 0-14 DTE — market pricing imminent event risk. This typically resolves within 3 days post-event. The 30-60 DTE segment is cheap relative to history (15th percentile)."

### 5. FlowNarrative — "What's the dealer flow story?"
LLM-generated briefing combining:
- Current GEX regime and recent changes
- Vanna/charm flow projections ("over the next 3 days, charm will reduce short gamma by $X")
- OpEx dynamics ("Friday's monthly OpEx will release $X of gamma — expect volatility expansion")
- Vol surface observations (skew, term structure anomalies)
- Historical analog ("the last time dealers were this short gamma with this skew was [date]")

### 6. PositionHeatmap — "Where is the open interest?"
**D3 heatmap:** x-axis = strike prices, y-axis = expiry dates. Color intensity = OI size. Separate views: calls, puts, net. Click cell → detail panel with volume, IV, Greeks.

### 7. Scanner — "What's mispriced?"
Enhanced version of existing options scanner. Each opportunity shows:
- Ticker + direction
- Thesis narrative
- Supporting Greeks context (why dealer flow makes this interesting)
- Score + confidence
- Estimated payoff

---

## New API Endpoints (`api/routers/derivatives.py`)

```
GET  /api/v1/derivatives/overview         # Market-wide dealer positioning summary
GET  /api/v1/derivatives/gex/{ticker}     # Full GEX profile for a ticker
GET  /api/v1/derivatives/regime           # Current dealer regime with interpretation
GET  /api/v1/derivatives/walls/{ticker}   # Put wall, call wall, gamma flip
GET  /api/v1/derivatives/vanna-charm/{ticker}  # Vanna + charm exposure breakdown
GET  /api/v1/derivatives/vol-surface/{ticker}  # Full vol surface data
GET  /api/v1/derivatives/skew/{ticker}    # Skew curve by expiry
GET  /api/v1/derivatives/term-structure/{ticker}  # Term structure curve
GET  /api/v1/derivatives/oi-heatmap/{ticker}  # OI by strike × expiry
GET  /api/v1/derivatives/flow-narrative   # LLM dealer flow briefing
GET  /api/v1/derivatives/signals          # All daily options signals
GET  /api/v1/derivatives/scan             # Mispricing scan with dealer context
GET  /api/v1/derivatives/history/{ticker} # Historical GEX/regime timeline
```

---

## New Analysis Module (`analysis/vol_surface.py`)

Vol surface construction and analysis:
- Build implied vol surface from options_snapshots data
- Interpolate via SVI (Stochastic Volatility Inspired) parameterization
- Compute: surface moneyness grid, delta grid, variance swap levels
- Detect: butterfly arbitrage, calendar arbitrage, skew anomalies
- Historical percentile ranking for each surface point

---

## Design Language

Terminal-grade aesthetic. This is for traders, not tourists.

- **Background:** #0A0E14 (near-black with blue tint)
- **Primary accent:** #00D4AA (teal — think Bloomberg terminal green)
- **Danger:** #FF4757 (hot red for short gamma / risk)
- **Caution:** #FFA502 (amber for elevated positioning)
- **Safe:** #2ED573 (green for long gamma / stable)
- **Vol surface colors:** Cool-to-hot gradient (blue → purple → red → orange)
- **Typography:** JetBrains Mono for everything (full terminal feel)
- **Cards:** Sharp corners (2px radius), thin borders, minimal glass
- **Charts:** D3 with crosshair cursors, hover tooltips, no animations

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

1. **DERIV-01**: App scaffold + design tokens + nav (same pattern as AstroGrid)
2. **DERIV-02**: Derivatives API router (surfaces existing backend data)
3. **DERIV-03**: Vol surface analysis module
4. **DERIV-04**: DealerFlow view (core — the hero)
5. **DERIV-05**: GammaProfile + GEX chart components
6. **DERIV-06**: VolSurface 3D + skew curves
7. **DERIV-07**: TermStructure + OI heatmap
8. **DERIV-08**: Flow narrative (LLM briefing)
9. **DERIV-09**: Scanner enhancement with dealer context
