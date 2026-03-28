# GRID Master Plan — Palantir for the Markets

**Identity:** A visual intelligence system that shows the flow of money and information — who pulls the levers, how much is moving, when, and why.

**The gap:** We have 46 views, 13 intelligence modules, 32 data pullers. But three critical dimensions are weak: HOW MUCH, WHEN (sequence), and WHY (causation). Without those, we have dashboards. With them, we have Palantir.

---

## The Three Missing Dimensions

### 1. HOW MUCH — Dollar Flow Quantification

**Problem:** We know direction (buy/sell) but not precise dollar amounts. Congressional trades are ranges ($1M-$5M). Insider filings have share counts but not always dollar values. Dark pool has volume but not dollar flow. ETF flow is estimated.

**Solution:** Build a unified dollar flow aggregation layer that normalizes everything into estimated USD amounts with confidence bands.

**Tasks (independent, parallel):**

**HOWMUCH-1: Dollar flow normalizer** (`intelligence/dollar_flows.py`)
- For each signal type, convert to estimated USD:
  - Congressional: midpoint of reported range
  - Insider: shares × price at transaction date
  - Dark pool: volume × VWAP estimate
  - 13F: quarterly holdings × price, compute delta between quarters
  - ETF flows: daily dollar volume × flow direction heuristic
  - Whale options: contracts × 100 × premium
- Store: `dollar_flows` table with source, actor, ticker, amount_usd, confidence, date
- API: `GET /api/v1/intelligence/dollar-flows?ticker=&actor=&min_amount=`

**HOWMUCH-2: Flow aggregation engine** (`analysis/flow_aggregator.py`)
- Aggregate dollar flows by: time period, sector, actor tier, direction
- Compute: net flow per sector per week, biggest movers, flow acceleration
- Answer: "How much money moved into tech this week? $2.3B net inflow"
- This feeds directly into the Money Flow visualization

**HOWMUCH-3: Flow visualization upgrade** (update `MoneyFlow.jsx`)
- Link thickness = actual dollar amount (not just direction)
- Labels on links: "$2.3B" not just "inflow"
- Hover: exact amount, sources, confidence band
- Time slider: watch dollar amounts change over 30/60/90 days

---

### 2. WHEN — Sequence Timeline

**Problem:** We have timestamps on everything but no view that shows the SEQUENCE of events leading to a price move. "First X happened, then Y, then Z, then the price moved." The story is in the timing.

**Solution:** Build a forensic timeline that reconstructs the chain of events before any significant price move.

**Tasks (independent, parallel):**

**WHEN-1: Event sequence builder** (`intelligence/event_sequence.py`)
- For any ticker or sector, build a chronological timeline of ALL events:
  - Congressional trades (with lag from transaction to disclosure)
  - Insider filings (transaction date, not filing date)
  - Dark pool volume spikes
  - Whale options flow
  - News articles (with sentiment)
  - Prediction market shifts
  - Price moves (>1% daily)
  - Regime changes
  - Cross-reference divergences
  - Earnings dates
  - FOMC/CPI/macro events
- Output: ordered list of events with timestamps, type, actor, direction, amount
- Compute: lead time (how far before the price move each event occurred)

**WHEN-2: Pattern detection** (extend `intelligence/sleuth.py`)
- Look for recurring sequences: "Every time event A happens, event B follows within X days"
- Track: which sequences are predictive vs coincidental
- Score: each pattern by historical hit rate
- This is the auto-discovery engine for new trading signals

**WHEN-3: Timeline visualization** (create `pwa/src/views/Timeline.jsx` or update existing)
- Horizontal timeline with vertical event markers
- Color by type (congressional=gold, insider=blue, dark pool=purple, etc.)
- Size by dollar amount
- Price line overlaid
- Click any event → detail panel
- Drag to select a range → show all events in that window
- "Play" button: animate through time showing events appearing one by one
- Filter: by event type, actor, direction

**WHEN-4: Forensic analyzer** (`intelligence/forensics.py`)
- Given a price move, look backwards: what events preceded it?
- Auto-generate a narrative: "NVDA dropped 5% on March 15. In the preceding 2 weeks: 2 insiders sold $12M, dark pool volume was 2x average, congressional committee member exited position, IV was elevated. The move was preceded by 4 warning signals with average lead time of 8 days."
- Store forensic reports for pattern learning

---

### 3. WHY — Causal Connection Engine

**Problem:** We detect signals but don't explain WHY actors are acting. "Pelosi bought NVDA" — but WHY? Committee hearing on AI defense contracts next week? New NVIDIA government contract announced? Upcoming earnings beat she heard about?

**Solution:** Build a causal inference layer that connects actor actions to events, policies, contracts, and information flows.

**Tasks (independent, parallel):**

**WHY-1: Event-action connector** (`intelligence/causation.py`)
- For each actor action, search for potential causes:
  - Congressional: upcoming committee hearings, pending legislation, government contracts
  - Insider: upcoming earnings, product launches, FDA decisions, contract wins
  - Institutional: rebalancing calendar, fund mandate changes, macro regime shifts
- Sources: congressional calendar (public), SEC filings (8-K), government contracts (USASpending.gov), FDA calendar
- Output: `{action, potential_causes: [{cause, probability, evidence, lead_time}]}`

**WHY-2: Government contract tracker** (`ingestion/altdata/gov_contracts.py`)
- Source: USASpending.gov API (free, public)
- Track: new contract awards, modifications, who won, how much
- Map: contractor → public company ticker (e.g., Raytheon → RTX)
- Detect: "RTX won $500M defense contract" → explains why defense committee members bought RTX
- Store: `GOV_CONTRACT:{agency}:{contractor}:{amount}`

**WHY-3: Legislative tracker** (`ingestion/altdata/legislation.py`)
- Source: Congress.gov API (free, public)
- Track: new bills, committee hearings, votes
- Map: bill topics → affected sectors/tickers
- Detect: "New AI regulation bill in committee" → affects NVDA, MSFT, GOOGL
- Connect: committee members on this bill who also traded affected tickers = informed trading

**WHY-4: Causal narrative generator** (extend LLM task queue)
- For each lever-puller action, LLM generates the probable "why":
  - Input: actor profile + action + timeline of surrounding events + committee assignments + contract data + legislation
  - Output: "Most likely explanation: [X]. Confidence: [Y]. Alternative explanations: [Z]"
- Store narratives for pattern learning
- Feed into the actor detail panel in the UI

**WHY-5: "Why did this move?" view** (create `pwa/src/views/WhyView.jsx`)
- Input: select a ticker and a date range where a significant move occurred
- Output: forensic reconstruction showing:
  - The price move
  - All preceding events on the timeline
  - Causal connections drawn between events and the move
  - Dollar amounts involved
  - Key actors and their motivations
  - LLM narrative explaining the full story
- This is the signature feature. This is what makes it Palantir.

---

## Logical Dependencies

```
HOWMUCH-1 (dollar normalizer) ──→ HOWMUCH-2 (aggregation) ──→ HOWMUCH-3 (viz)
                                                              ↑
WHEN-1 (event builder) ──→ WHEN-2 (patterns) ──→ WHEN-3 (timeline viz)
                       └──→ WHEN-4 (forensics) ──────────────→ WHY-5 (why view)
                                                              ↑
WHY-1 (connector) ──────────────────────────────────────────→ WHY-5
WHY-2 (gov contracts) ──→ WHY-1                              ↑
WHY-3 (legislation) ──→ WHY-1                                |
WHY-4 (LLM narratives) ──────────────────────────────────────┘
```

## What can run in parallel (no dependencies)

**Wave 1 (all independent):**
- HOWMUCH-1: Dollar flow normalizer
- WHEN-1: Event sequence builder
- WHY-2: Government contract tracker
- WHY-3: Legislative tracker

**Wave 2 (depends on Wave 1):**
- HOWMUCH-2: Flow aggregation (needs HOWMUCH-1)
- WHEN-2: Pattern detection (needs WHEN-1)
- WHEN-4: Forensic analyzer (needs WHEN-1)
- WHY-1: Event-action connector (needs WHY-2, WHY-3)

**Wave 3 (depends on Wave 2):**
- HOWMUCH-3: Flow visualization upgrade (needs HOWMUCH-2)
- WHEN-3: Timeline visualization (needs WHEN-1, WHEN-2)
- WHY-4: Causal narrative generator (needs WHY-1)
- WHY-5: "Why did this move?" view (needs WHEN-4, WHY-1, WHY-4)

---

## After this is built, GRID answers:

1. **"Show me where the money is flowing"** → Money Flow with actual dollar amounts
2. **"Who moved it?"** → Actor network with named players and dollar amounts
3. **"How much?"** → Precise dollar estimates with confidence bands
4. **"When did they act?"** → Forensic timeline showing the exact sequence
5. **"Why did they act?"** → Causal connections to contracts, legislation, events
6. **"What happened next?"** → Price moves that followed, with pattern scoring
7. **"What will happen now?"** → Thesis based on current signals matching past patterns

That's Palantir for the markets. No logical holes.
