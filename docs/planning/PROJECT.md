# GRID v4 — Trading Intelligence Platform

## What This Is

GRID is an intelligence amplifier for a solo operator — a multi-layered quantitative trading platform that combines 64+ data sources, regime detection, hypothesis testing, capital flow analysis, OSINT aggregation, and LLM-powered narrative synthesis into a single self-reinforcing flywheel. The human stays in the loop; the system makes them faster, more informed, and harder to surprise.

## Core Value

**Decision quality** — every component exists to improve the operator's ability to identify regime shifts, validate hypotheses, and act on high-conviction opportunities with full context.

## Architecture

- **grid_repo** (Python/FastAPI): Core engine — 40+ modules, 23 API routers, 64 pullers, 15 DB tables, ML pipeline, physics engines, React PWA
- **Crucix** (Node.js): OSINT aggregator — 27 sources, delta engine, 3D globe, Telegram/Discord alerts
- **grid_app** (Python/DuckDB): Hypothesis registry, flywheel scores, calibration log

## Key Constraints

- PIT (point-in-time) correctness is non-negotiable — no lookahead bias
- Decision journal is immutable (DB triggers)
- Model governance: CANDIDATE -> SHADOW -> STAGING -> PRODUCTION -> FLAGGED -> RETIRED
- Graceful degradation — no single point of failure

## Current Milestone: v1.0 — Data Reliability & UX Polish

**Goal:** Make GRID's data pipeline resilient with backup sources, and make the PWA tell stories instead of dumping numbers.

**Target features:**
- Backup data sources (CoinGecko, Stooq, AlphaVantage, Wiki history) for resilience
- Insight-driven UX — every number has context, every view tells a narrative
- Unified navigation schema (bottom nav pattern)
- Flows page narrative layout
- Enriched watchlist with sector context
- Timeframe comparison visual
- LLM prompt optimization via orthogonality
- Hypothesis results in Discovery view
- Context window expanded to 8192

## Active Requirements

- DATA-01: Crypto prices via CoinGecko when yfinance fails
- DATA-02: Equity price fallback via Stooq/AlphaVantage
- DATA-03: Historical context via Wikipedia "On This Day" + RSS feeds
- DATA-04: Freshness checks before pulling (don't waste free API quotas)
- UX-01: Flows page grouped by flow direction with narrative summary
- UX-02: Enriched watchlist with insight lines
- UX-03: Briefing formatted with AI narrative, not raw data
- UX-04: Bottom nav as primary navigation (removed redundant grids)
- UX-05: Timeframe comparison component (5D/5W/3M/1Y/5Y)
- UX-06: Hypothesis results UI in Discovery view
- INFRA-01: Context window 8192 for richer LLM output
- INFRA-02: Prompt optimization via orthogonality-based feature selection

## Validated Requirements (shipped this session)

- [x] DATA-01: CoinGecko puller built and tested
- [x] DATA-02: Price fallback puller built (Stooq/AV/12Data)
- [x] DATA-03: Wiki history puller built and tested
- [x] UX-01: Flows page reworked with FlowSummary, flow grouping, insight lines
- [x] UX-02: Enriched watchlist API + frontend
- [x] UX-03: Briefing display reformatted
- [x] UX-04: NavBar updated, NAVIGATE/ACTIONS grids removed
- [x] UX-05: TimeframeComparison.jsx built + API endpoint
- [x] UX-06: TestedHypotheses section in Discovery
- [x] INFRA-01: .env updated, llama.cpp restarted at 8192
- [x] INFRA-02: prompt_optimizer.py built, integrated into market_briefing.py

## Key Decisions

- CoinGecko free tier (no key) as primary crypto source; yfinance as fallback
- Stooq.com (no key) as always-available equity price backup
- Bottom nav: Home, Brief, Regime, Flows, Options, Discover, More
- Orthogonality-based prompt optimization replaces naive truncation

## Context

- Server: grid-svr, NVIDIA RTX PRO 4000 24GB
- GPU model: Qwen2.5-32B-Instruct-Q4_K_M (8192 ctx)
- Public URL: https://grid.stepdad.finance
- 489 tests passing, all services running

## Evolution

This document evolves at phase transitions and milestone boundaries.

---
*Last updated: 2026-03-25*
