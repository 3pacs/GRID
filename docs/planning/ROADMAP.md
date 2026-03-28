# Roadmap

## Milestone v1.1: Always-On Intelligence & Data Resilience

### Phase 1: 24/7 LLM Intelligence Loop — COMPLETE
**Goal:** Wire the onboard Qwen 32B to run continuously — generating briefings, researching gaps, testing hypotheses, and synthesizing narratives around the clock.
**Delivered:** Intelligence loop daemon in api/main.py — hourly briefings (actionable format), 4h capital flows (DB persistence), 6h price fallback, nightly research + taxonomy audit, daily wiki history + CoinGecko + social sentiment. All LLM prompts use orthogonality-optimized feature selection. Social sentiment + Google Trends fed into briefing context.

### Phase 2: Data Source Resilience — COMPLETE
**Goal:** Every data family has at least 2 sources. Failed pulls auto-fallback. Freshness monitoring alerts when data goes stale.
**Delivered:** Price fallback chain (Stooq→AlphaVantage→TwelveData) wired into scheduler. CoinGecko as primary crypto. /api/v1/system/freshness endpoint with per-family GREEN/YELLOW/RED status. User-contributed API supervision deferred (needs BOINC coordinator).

### Phase 3: Hypothesis Engine — COMPLETE
**Goal:** All hypotheses have lag_structures and can be tested. Passed hypotheses surface with interpretation. Promote to Feature workflow.
**Delivered:** 18 CANDIDATE hypotheses given leader/follower mappings. 10 tested, 8 PASSED. 5 new TACTICAL hypotheses from backtest (PYPL→XLK Sharpe 3.07, CMCSA→XLC 61% WR, MSFT→AMD Sharpe 1.06). TestedHypotheses UI in Discovery view. Promote to Feature endpoint + button for PASSED hypotheses. PROMOTED state in UI.

### Phase 4: UX Narrative — COMPLETE
**Goal:** No raw numbers anywhere. Every view leads with interpretation.
**Delivered:** interpret.js shared module (z-scores, features, options, correlations, regime drivers). Regime drivers show "why this matters". Options explain P/C, IV, max pain positioning. SignalCard has z-score interpretation. Signals view Snapshot tab groups by family with summaries. Briefings rewritten: Bottom Line → Regime → What Changed → Risks → Opportunities → Tomorrow.

### Phase 5: Navigation & Polish — COMPLETE
**Goal:** Consistent UI. Bottom nav covers all workflows.
**Delivered:** Bottom nav: Home, Brief, Regime, Flows, Options, Discover, More. NAVIGATE/ACTIONS grids removed. shared.card unified across Regime, Dashboard, Options, Discovery. Consistent 10px radius, 14px 16px padding everywhere.

### Phase 6: Physics-Driven Intelligence — COMPLETE
**Goal:** Wire dealer gamma, momentum, news energy into actionable setups.
**Delivered:** _apply_physics_scores() in Sankey endpoint. GEX per ticker, momentum scoring, news energy. Composite force score with HIGH/MODERATE/LOW labels. Setups sorted by force magnitude. Action text enhanced with physics context.

### Phase 7: Data Gap Closure — COMPLETE
**Goal:** Fill dead data families. Backfill macro. Wire auto-fallback.
**Delivered:** FRED 20/20 series pulled. Auto-fallback in scheduler. Freshness monitoring endpoint. CoinGecko + wiki history in scheduler. 92 taxonomy fixes. 43+ equity tickers populated.

### Phase 8: Backtest Pipeline — COMPLETE
**Goal:** Automated discovery of high-Sharpe strategies.
**Delivered:** backtest_scanner.py scans all feature pairs. 874 winners found (Sharpe>0.8, WR>52%). Auto-generates TACTICAL hypotheses. POST /api/v1/discovery/backtest-scan endpoint. Top: BTC→SOL (Sharpe 21), ETH→SOL (Sharpe 20), MSFT→AMD (Sharpe 1.06).

### Phase 9: Taxonomy Guardian — COMPLETE
**Goal:** Automated daily taxonomy audit.
**Delivered:** taxonomy_audit.py: detects misclassifications, stale data, missing features, impossible values. Auto-fixes. Nightly 2:30 AM schedule. POST /api/v1/system/taxonomy-audit endpoint. Coverage per family with GREEN/YELLOW/RED.

### Phase 10: Social Sentiment — COMPLETE
**Goal:** Reddit, Bluesky, Google Trends sentiment tracking.
**Delivered:** social_sentiment.py: Reddit (12 subs), Bluesky (public API), Google Trends (pytrends). Per-ticker bullish/bearish/neutral scoring. Fed into briefing prompts. Wired into intelligence loop daily.

### Phase 11: Paper Trading Bot — COMPLETE
**Goal:** Execute TACTICAL hypothesis signals automatically. Track P&L. Kill underperformers.
**Delivered:** trading/signal_executor.py: hourly signal loop checks all ACTIVE strategies, fires on leader >1% move, Kelly-sized position on follower, auto-closes after expected_lag. Wired into intelligence loop + manual POST /trading/execute-signals endpoint. Paper engine with open/close/kill/dashboard/kelly. 12 strategies registered.

### Phase 12: Exchange Integrations — COMPLETE
**Goal:** Hyperliquid (perps), Polymarket (prediction markets), Kalshi (event contracts). Multi-wallet, small amounts, grow them.
**Delivered:**
- EXCH-01: trading/hyperliquid.py — HyperliquidTrader (testnet default, $100 max position, 20% drawdown). 4 API endpoints.
- EXCH-02: trading/prediction_markets.py — PolymarketTrader (CLOB API, $500/trade, $5K portfolio). 3 API endpoints.
- EXCH-03: trading/prediction_markets.py — KalshiTrader (REST + JWT auth). 3 API endpoints.
- EXCH-04: trading/wallet_manager.py — WalletManager with create/kill/pause/resume, auto-kill on drawdown breach, aggregated dashboard. 8 API endpoints.

### Phase 12.5: Sentiment Data Pipeline — COMPLETE
**Goal:** Deep sentiment and alternative data ingestion with ML scoring.
**Delivered:** AlphaVantage News Sentiment puller (daily, 11 tickers). HuggingFace financial-news-multisource (57M rows, weekly streaming). Pushshift Reddit backfill (streaming .zst, 12 finance subs). FinBERT scoring pipeline (GPU, scores all text sources). All registered in scheduler.

### Phase 13: AstroGrid — IN PROGRESS
**Goal:** Standalone celestial intelligence interface. 3D planetary visualization, ephemeris, market-astro correlation engine, and narrative synthesis. Separate app sharing GRID's backend.
See ASTROGRID-PLAN.md for full architecture.

### Phase 14: Oracle & Data Completion — IN PROGRESS
**Goal:** Self-improving prediction engine with scored track record. Fill remaining 124 zero-data features. Supervised intelligence on all outbound signals.
**Delivered (2026-03-26):**
- Oracle engine: 5 competing models, signal/anti-signal, weight evolution, immutable prediction journal
- 100x digest: 3-layer supervised filter (sanity → LLM review → cross-verify), kills spam
- Options puller fix: skip near-expiry garbage, quality gate on scanner
- Viz intelligence engine: 11 rules, VizSpec protocol, LivingGraph renderer
- Bulk data: CBOE 35yr VIX/SKEW, Binance 5yr crypto, DeFiLlama, Open-Meteo 5yr, EIA
- 2M+ raw rows ingested, 328K resolved
**Remaining:** Fix resolver mappings (wn_*, FRED date parse, analyst int64). Fill intl macro via existing pullers. EIA electricity v2 format. Compute derived features. OFR/Comtrade/VIIRS/Patents endpoints.

### Phase 15: Crucix iOS & Hermes Email — PLANNED
**Goal:** Crucix OSINT interface reformatted for iOS. Hermes email (hermes@stepdad.finance) with sender allowlist.
**Tasks:** PWA Crucix button, iOS safe-area/touch-target styling, Cloudflare email routing, inbound email processing with LLM, sender allowlist guard.

### Phase 16: Living Graphs & UX Polish — PLANNED
**Goal:** Wire all living graph renderers to real data. Phase space trajectory, sector orbital, feature force network. Flows page rework. Watchlist redesign. Hypothesis UI.
**Tasks:** Regime trajectory API endpoint, orbital sector data endpoint, correlation → force network transform. Flows narrative summary. Watchlist briefing cards.

---

## Requirements Traceability

| REQ-ID | Phase | Status |
|--------|-------|--------|
| LOOP-01 | 1 | Done |
| LOOP-02 | 1 | Done |
| LOOP-03 | 1 | Done |
| LOOP-04 | 1 | Done |
| DATA-05 | 2 | Done |
| DATA-06 | 2 | Done |
| DATA-07 | 2 | Done |
| DATA-08 | 2 | Deferred |
| HYPO-01 | 3 | Done |
| HYPO-02 | 3 | Done |
| HYPO-03 | 3 | Done |
| UX-07 | 4 | Done |
| UX-08 | 4 | Done |
| UX-09 | 4 | Done |
| UX-10 | 4 | Done |
| UX-11 | 5 | Done |
| UX-12 | 5 | Done |
| PHYS-01 | 6 | Done |
| PHYS-02 | 6 | Done |
| PHYS-03 | 6 | Done |
| PHYS-04 | 6 | Done |
| DATA-09 | 7 | Done |
| DATA-10 | 7 | Done |
| DATA-11 | 7 | Done |
| DATA-12 | 7 | Done |
| BT-01 | 8 | Done |
| BT-02 | 8 | Done |
| BT-03 | 8 | Done |
| TAX-01 | 9 | Done |
| TAX-02 | 9 | Done |
| SENT-01 | 10 | Done |
| SENT-02 | 10 | Done |
| TRADE-01 | 11 | Done — signal_executor.py hourly loop |
| TRADE-02 | 11 | Done — paper_strategies table with per-strategy P&L |
| TRADE-03 | 11 | Done — _check_kill auto-disables on drawdown/win rate threshold |
| TRADE-04 | 11 | Done — kelly_position_size method |
| EXCH-01 | 12 | Done — hyperliquid.py testnet integration |
| EXCH-02 | 12 | Done — prediction_markets.py Polymarket CLOB |
| EXCH-03 | 12 | Done — prediction_markets.py Kalshi REST |
| EXCH-04 | 12 | Done — wallet_manager.py multi-wallet |
| SENT-03 | 12.5 | Done — AlphaVantage + HF news + Pushshift + FinBERT |
| ASTRO-01 | 13 | In Progress — App scaffold |
| ASTRO-02 | 13 | Planned — 3D planetary orrery |
| ASTRO-03 | 13 | Planned — Ephemeris calculator |
| ASTRO-04 | 13 | Planned — Market-astro correlation engine |
| ASTRO-05 | 13 | Planned — Celestial narrative synthesis |
| ASTRO-06 | 13 | Planned — API router expansion |
| ORACLE-01 | 14 | Done — oracle/engine.py 5-model ensemble |
| ORACLE-02 | 14 | Done — Signal + anti-signal architecture |
| ORACLE-03 | 14 | Done — Self-improving weight evolution loop |
| ORACLE-04 | 14 | Done — oracle/report.py email digest |
| ORACLE-05 | 14 | Done — Wired into hermes_operator 6h cycle |
| DIGEST-01 | 14 | Done — 100x bundled digest with strikes |
| DIGEST-02 | 14 | Done — 3-layer supervised sanity check |
| DIGEST-03 | 14 | Done — Options puller near-expiry fix |
| DATA-13 | 14 | Done — CBOE VIX/SKEW 35yr bulk download |
| DATA-14 | 14 | Done — Binance/CoinGecko/DeFiLlama crypto bulk |
| DATA-15 | 14 | Done — Open-Meteo 5yr weather, EIA energy |
| DATA-16 | 14 | Remaining — 124 features need resolver fix + intl pullers |
| VIZ-01 | 14 | Done — viz_intelligence.py 11 rules |
| VIZ-02 | 14 | Done — VizSpec protocol + API |
| VIZ-03 | 14 | Done — LivingGraph.jsx universal renderer |
| VIZ-04 | 16 | Planned — Wire PhaseSpace to regime trajectory |
| VIZ-05 | 16 | Planned — Wire Orbital to sector rotation |
| VIZ-06 | 16 | Planned — Wire ForceNetwork to correlations |
| CRUCIX-01 | 15 | Planned — iOS reformat |
| CRUCIX-02 | 15 | Planned — PWA button integration |
| EMAIL-01 | 15 | Planned — hermes@stepdad.finance setup |
| EMAIL-02 | 15 | Planned — Sender allowlist guard |
