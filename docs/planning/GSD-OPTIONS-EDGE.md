# GSD Plan — Options Edge & Intelligence Pipeline

**Date:** 2026-03-28
**Focus:** Generate specific, high-conviction options trades with supervised sanity checks, dealer gamma visualization, and an intelligence advantage from alternative signal sources with trust scoring.

---

## The Thesis

Cem Karsan's insight: dealer hedging flows are the dominant short-term force in markets. When dealers are short gamma, they amplify moves. When long gamma, they dampen them. GEX, vanna, and charm tell you WHERE the market is being pulled. Options flow tells you WHO is positioning. Combined with an intelligence edge from alternative sources, we can identify specific high-probability setups.

---

## Part 1: Options Trade Recommendations

### What exists
- `physics/dealer_gamma.py` — GEX, gamma flip, walls, vanna/charm (complete)
- `discovery/options_scanner.py` — 7-signal mispricing detector, composite score, payoff estimation
- `alerts/hundredx_digest.py` — 3-layer sanity pipeline (sanity → LLM review → cross-verify)
- `ingestion/options.py` — 37 tickers daily, options_snapshots + options_daily_signals

### What's missing
The scanner finds opportunities but doesn't output actionable trades. Need:

**OPT-1: Trade Recommendation Engine** (`trading/options_recommender.py`)
- Input: MispricingOpportunity from scanner + GEX profile + dealer regime
- Output: specific trade with:
  - Ticker, direction (CALL/PUT)
  - Strike (optimized: max gamma bang for buck, not just "10-20% OTM")
  - Expiry (based on charm decay curve — pick the sweet spot)
  - Entry price (mid of bid-ask from latest chain snapshot)
  - Target price (based on expected move from GEX profile)
  - Stop loss (based on gamma flip point or wall levels)
  - Expected return (payoff × probability from scanner confidence)
  - Max risk ($, not %)
  - Kelly fraction for position sizing
- Strategy types: naked calls/puts, vertical spreads, straddles when IV is low + GEX is negative

**OPT-2: Supervised Sanity Pipeline** (extend `alerts/hundredx_digest.py`)
- Layer 1: Data quality (IV in range, OI sufficient, spread < 20% of mid)
- Layer 2: Dealer flow check (does GEX regime support this direction?)
- Layer 3: Cross-asset check (does momentum/news energy align?)
- Layer 4: LLM review (structured prompt with all context)
- Layer 5: Historical analog check (similar setups in last 12 months → what happened?)
- Each layer returns PASS/FAIL/SUSPECT with reason
- Only PASS-all-5 trades get surfaced to operator

**OPT-3: Outcome Tracking & Self-Improvement**
- Log every recommendation with full context at generation time
- Track actual P&L when expiry hits (or when closed)
- Score each signal source's contribution to winners vs losers
- Feed scores back into scanner weights (auto-researcher loop)
- Monthly: LLM generates "what we learned" report from outcomes
- Kill signals that consistently fail, amplify signals that consistently win

**OPT-4: API Endpoint**
```
GET /api/v1/options/recommendations
  → [{ticker, direction, strike, expiry, entry, target, stop,
      expected_return, kelly_fraction, confidence, sanity_status,
      thesis, dealer_context, generated_at}]

GET /api/v1/options/recommendations/history
  → past recommendations with outcomes + P&L

POST /api/v1/options/recommendations/refresh
  → trigger fresh scan + recommendation generation
```

---

## Part 2: Dealer Gamma Visualization

### The vision
An interactive D3 visualization that shows the "invisible hand" — where dealer hedging flows are pulling the market. The operator should be able to see at a glance:
- Am I in a gamma amplification zone or dampening zone?
- Where are the walls (support/resistance from options positioning)?
- What happens if we move 2% in either direction?

### VIZ-1: GEX Profile Chart (primary)
- **X-axis**: Price (spot ± 15%)
- **Y-axis**: Net gamma exposure ($B notional)
- **Key elements**:
  - GEX curve showing gamma exposure at each strike
  - Zero line (gamma flip point) — bright line, labeled
  - Current spot price — vertical marker with pulse animation
  - Gamma wall (max call gamma) — green zone
  - Put wall (max put gamma) — red zone
  - Shaded regions: GREEN above zero (dealer dampening), RED below (amplifying)
  - Annotations: "Dealers are SHORT gamma here — moves will be amplified"
- **Interactivity**: hover any point → show exact GEX value + what it means

### VIZ-2: Vanna/Charm Compass
- Circular gauge showing:
  - Vanna exposure (IV sensitivity) — how IV changes affect dealer delta
  - Charm exposure (time sensitivity) — how passage of time affects dealer delta
  - Combined vector → "Dealers will need to BUY/SELL X delta by Friday"
- Color: green (favorable flow) / red (adverse flow)

### VIZ-3: Flow Timeline
- Horizontal timeline showing:
  - Past GEX readings (did we cross the flip point recently?)
  - OpEx calendar (monthly, weekly, quarterly — different dealer behavior)
  - Upcoming catalysts (earnings, FOMC, etc.)
  - Projected gamma decay (how the profile changes by next OpEx)

### VIZ-4: Trade Recommendation Cards
- Below the GEX profile, show the active recommendations
- Each card: ticker | CALL/PUT | strike | expiry | expected return
- Visual: strike overlaid on the GEX profile chart
- Sanity status pills (5 green checks, or X for failures)
- Click → full thesis + dealer context

---

## Part 3: Intelligence Edge — Alternative Signal Sources

### The principle
Everyone has the same FRED data, the same Bloomberg terminal. The edge comes from:
1. Getting information FASTER (before it's priced)
2. Getting information others DON'T HAVE (alternative sources)
3. Scoring source RELIABILITY over time (trust, not just data)

### INTEL-1: Rumor & Whisper Network
Sources to integrate (ranked by potential alpha):
- **Congressional trading disclosures** (45-day lag but predictive) — EDGAR/Quiver Quant
- **Corporate insider filings** (Form 4) — SEC EDGAR, parsed daily
- **Dark pool prints** — FINRA ADF/ATS data (delayed but patterns matter)
- **Unusual options activity** — already have this, but add whale tracking (>$1M premium)
- **Reddit/Twitter smart money accounts** — track specific accounts, not subreddits
- **Polymarket/Kalshi odds shifts** — we have integration, add rapid-change alerts
- **Patent filings** — already have USPTO, add velocity detection (filing surges)
- **Supply chain signals** — freight rates, semiconductor lead times, container bookings
- **Job posting velocity** — company hiring/firing patterns as leading indicator
- **App download rankings** — Sensor Tower/AppAnnie proxies for revenue

### INTEL-2: Source Trust Scoring
For every signal source (human account, API, data feed):
- Track: what they said, when they said it, what actually happened
- Compute: accuracy rate, lead time (how early before the move), hit rate by category
- Rank: trusted sources bubble up, unreliable ones sink
- Flag: if someone is consistently right on material non-public info → they're likely connected
- The system doesn't care WHY they're right — just that they ARE right, repeatedly
- Store: `signal_sources` table with trust_score, hit_count, miss_count, avg_lead_time_hours
- Auto-weight: high-trust sources get amplified in the recommendation engine

### INTEL-3: Speed Layer
- WebSocket feeds for real-time: unusual options flow, dark pool prints, social mentions
- Conflict detection: when our intelligence says X but price says Y → flag divergence
- Staleness scoring: data older than its natural cadence gets deprioritized
- Alert on convergence: when 3+ independent sources point the same direction simultaneously

---

## Execution Plan

```
PHASE 1 — Trade Engine (dispatch NOW):
  OPT-1  Trade recommendation engine
  OPT-2  5-layer sanity pipeline
  VIZ-1  GEX profile chart

PHASE 2 — Self-Improvement:
  OPT-3  Outcome tracking + auto-researcher feedback loop
  VIZ-2  Vanna/charm compass
  VIZ-4  Trade recommendation cards on detail page

PHASE 3 — Intelligence Edge:
  INTEL-1  Congressional trades + Form 4 + dark pool ingestion
  INTEL-2  Source trust scoring framework
  VIZ-3   Flow timeline with catalysts

PHASE 4 — Speed & Scale:
  INTEL-3  WebSocket real-time layer
  INTEL-1  (continued) Social smart money tracking, supply chain signals
```

---

## Constraints
- All LLM calls degrade gracefully (rule-based fallback)
- Options recommendations MUST pass all 5 sanity layers before surfacing
- Trust scores are immutable history (append-only, never edit past scores)
- No live trading until paper-trading validation shows consistent edge
- Check .coordination.md before every commit
