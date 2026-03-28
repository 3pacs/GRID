# GSD Plan — Updated 2026-03-28

**Focus:** GRID Intelligence Platform — the machine that sees what others can't
**68 commits shipped tonight. Here's what's done and what's next.**

---

## DONE (this session)

### Phase A: Watchlist Core — COMPLETE
- [x] A1 — Ticker search + autocomplete
- [x] A2 — Batch price refresh + 5-min cache
- [x] A3 — Swipe-to-delete on mobile, hover X on desktop
- [x] A4 — Progressive loading, DB write-back caching
- [x] Routing wired, live yfinance fallback

### Phase B: Ticker Detail Page — COMPLETE
- [x] B1 — Structured AI overview (4 sections + bottom line)
- [x] B2 — Interactive D3 price chart (timeframes, volume, key levels, regime bands)
- [x] B3 — Capital flow peer comparison (D3 bar chart)
- [x] B4 — Options intelligence (positioning chart, PCR bar, IV gauge, interpretation)
- [x] B5 — Insider edge panel (congressional, insider, dark pool, whale, convergence)
- [x] GEX profile + vanna/charm compass + flow timeline

### Phase C: Server Health — MOSTLY COMPLETE
- [x] C1 — Eurostat date format fixed, KOSIS/USDA guards, NYFed revision_behavior
- [x] C2 — Old repo deleted (29GB), loose files cleaned, models relocated
- [x] C3 — 80+ entity_map entries, seed_v2 fixes, resolver needs re-run
- [ ] C4 — Full pipeline test on server (needs restart)

### Options Edge — COMPLETE
- [x] Trade recommendation engine + 5-layer sanity
- [x] Outcome tracking + self-improving scanner weights
- [x] Trade cards with confidence circles, risk/reward bars, sanity dots
- [x] Hypothesis sanity overhaul (LLM review, plain English names)
- [x] Backtest scanner sanity (no crypto self-correlation)

### Intelligence Layer — COMPLETE
- [x] Trust scoring (Bayesian, recency decay)
- [x] Lever-puller tracking + motivation modeling
- [x] Actor network (100+ named, wealth flows, pocket-lining)
- [x] Cross-reference lie detector (24 checks)
- [x] Source accuracy audit + auto-priority
- [x] Post-mortem engine (5 failure categories)
- [x] Thesis page (10 models, unified direction, agreement matrix)
- [x] Thesis tracker (version, score, learn)
- [x] Sleuth (investigative AI, leads, rabbit holes)
- [x] Trend tracker (6 categories)

### Data Sources — 11 NEW
- [x] Congressional trades, insider filings, dark pool
- [x] Whale flow, prediction odds, smart money
- [x] Supply chain, Fed liquidity, ETF flows, 13F
- [x] Earnings calendar, news scraper

### Frontend — COMPLETE
- [x] 7 World View tabs + drawer menu
- [x] Premium dashboard, command palette (Cmd+K), Ask GRID chat
- [x] Predictions, strategies, correlation matrix, sector dive, portfolio
- [x] Pipeline health, settings, architecture viz, market diary, earnings
- [x] Onboarding tour, theme system, mobile responsive
- [x] WebSocket real-time, push notifications, error boundaries

### Infrastructure — COMPLETE
- [x] LLM task queue (24/7, priority-based, never idle)
- [x] Hermes wired with all intelligence modules
- [x] 652 tests passing

---

## NOT DONE (next session)

### Server Activation
- [ ] Restart grid-api + grid-hermes
- [ ] Add 8GB swap
- [ ] Run resolver to close data gaps
- [ ] First full intelligence cycle
- [ ] Rebuild PWA on server
- [ ] Check TAO miner
- [ ] Rotate GitHub PAT
- [ ] Generate VAPID keys for push

### Data Gaps
- [ ] Alternative: 6% → run celestial/patent/AIS pullers + resolver
- [ ] Systemic: 0% → investigate OFR endpoints
- [ ] Trade: 33% → run Comtrade puller + resolver
- [ ] Rates: 60% → run international pullers + resolver

### Testing
- [ ] Walk through all 7 world views with real data
- [ ] Test all D3 visualizations
- [ ] Test mobile on actual phone
- [ ] Test Ask GRID with real questions
- [ ] Load test with multiple tickers

### Merge Codex AstroGrid
- [ ] Rebase `codex/astrogrid-dedup` onto main
- [ ] Resolve conflicts
- [ ] Test AstroGrid views

### Self-Improvement
- [ ] Run improvement cycle daily for a week
- [ ] Score thesis accuracy
- [ ] Score trade recommendations
- [ ] Adjust scanner weights from outcomes
- [ ] Review post-mortems for systemic issues

### Future Features
- [ ] Drag-to-reorder watchlist
- [ ] Streaming AI responses
- [ ] Redis caching layer
- [ ] API rate limiting for external consumers
- [ ] Split watchlist.py (1400+ lines)
- [ ] Extract reusable viz components
