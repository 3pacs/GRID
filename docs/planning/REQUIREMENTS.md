# Requirements — Milestone v1.1

## 24/7 LLM Intelligence Loop

- [ ] **LOOP-01**: Hourly briefings auto-generate via cron with actionable format (Bottom Line → Regime → Action)
- [ ] **LOOP-02**: Nightly auto-research: gap analysis → hypothesis generation → LLM multi-angle research
- [ ] **LOOP-03**: Capital flow research auto-runs every 4h, persists to DB (not just cache), preloaded on PWA launch
- [ ] **LOOP-04**: Wiki history + RSS ingested daily and fed to briefing prompts for historical context

## Data Source Resilience

- [ ] **DATA-05**: yfinance failure auto-triggers fallback chain (Stooq → AlphaVantage → TwelveData)
- [ ] **DATA-06**: CoinGecko as primary crypto source, with freshness-gated pulls
- [ ] **DATA-07**: Data freshness monitoring with per-family staleness alerts
- [ ] **DATA-08**: User-contributed API endpoints land in source_catalog with trust_score supervision

## Hypothesis Engine

- [ ] **HYPO-01**: Define lag_structure (leader/follower features) for all 18 CANDIDATE hypotheses
- [ ] **HYPO-02**: Hypothesis tester produces PASSED/FAILED verdicts with correlation/lag metrics
- [ ] **HYPO-03**: "Promote to Feature" workflow for PASSED hypotheses

## UX — Narrative Intelligence

- [ ] **UX-07**: Every z-score has an interpretation tooltip
- [ ] **UX-08**: Regime view shows "why this matters" for each driver
- [ ] **UX-09**: Options view explains positioning implications
- [ ] **UX-10**: Associations explains correlation significance in plain English

## Navigation & Polish

- [ ] **UX-11**: Consistent card style across all views
- [ ] **UX-12**: Mobile-first responsive layouts everywhere

## Out of Scope (this milestone)

- EDGAR scraping via BOINC (needs coordinator deployment first)
- Full TradingAgents autonomous execution
- Historical replay slider for capital flows
