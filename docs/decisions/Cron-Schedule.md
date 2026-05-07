---
source: /Users/anikdang/grid_obsidian/Architecture/Cron-Schedule.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Cron Schedule

GRID runs scheduled tasks through two systems: the in-process intelligence loop (in `api/main.py`) and the [[Hermes-Operator]] daemon.

## Intelligence Loop (api/main.py)

Started as a daemon thread when the API boots.

| Frequency | Time | Task | Module |
|-----------|------|------|--------|
| Every 1h | — | Paper trading signal execution | `trading.signal_executor.execute_signals` |
| Every 1h | — | Hourly market briefing | `ollama.market_briefing.MarketBriefingEngine` |
| Every 4h | — | Capital flow refresh | `analysis.capital_flows.CapitalFlowResearchEngine` |
| Every 6h | — | Price fallback (stale tickers) | `ingestion.price_fallback.PriceFallbackPuller` |
| Daily | 02:00 | Nightly research | `analysis.research_agent.run_full_research` |
| Daily | 02:30 | Taxonomy audit | `analysis.taxonomy_audit.run_taxonomy_audit` |
| Daily | 06:00 | Daily context (wiki, CoinGecko, social sentiment) | Multiple pullers |
| Daily | 07:00 | Options recommendations | `trading.options_recommender.OptionsRecommender` |
| Daily | 10:00 | Celestial briefing | `ollama.celestial_briefing` |
| Daily | 15:00 | Dealer flow briefing | `ollama.dealer_flow_briefing` |
| Daily | 18:00 | Evening context refresh | Same as 06:00 |
| Weekly | Sun 03:00 | Astro correlations | `analysis.astro_correlations.AstroCorrelationEngine` |
| Every 7d | — | Options tracker scoring | `trading.options_tracker.run_improvement_cycle` |

## Ingestion Scheduler (`ingestion/scheduler.py`)

Started as a daemon thread from `api/main.py`. Handles all data source pulling.

**Daily pulls**: [[FRED]], yfinance, [[BLS]], [[EDGAR]], options chains, Tiingo, [[CoinGecko]], social sentiment, fear/greed index, Google Trends, CBOE indices, Fed speeches, [[Baltic Dry Index|Baltic Dry]]

**Monthly pulls**: International sources ([[ECB]], BCB, BIS, IMF, OECD, Eurostat, MAS, KOSIS, RBI, ABS_AU), physical data ([[USDA]], patents, VIIRS, OFR), trade data (Comtrade, CEPII, WIOD, Atlas ECI)

**Idempotency**: Uses `_last_run` timestamps to prevent duplicate pulls on server restart.

## Hermes Operator (`scripts/hermes_operator.py`)

Autonomous daemon running every 5 minutes (300s cycle):

1. **Health Monitor** — checks DB, data freshness, LLM availability
2. **Pull Fixer** — detects failed pulls, diagnoses, retries (max 3x)
3. **Pipeline Runner** — runs full pipeline on schedule (every 6h)
4. **Data Gatherer** — fills historical gaps
5. **Autoresearch** — generates hypotheses when healthy
6. **Self-Diagnostics** — reads own error logs, proposes fixes

**[[Hermes Scheduler|Hermes]] Configuration:**
- `CYCLE_INTERVAL_SECONDS = 300` (5 min between cycles)
- `CYCLE_TIMEOUT_SECONDS = 900` (15 min max per cycle)
- `PIPELINE_INTERVAL_HOURS = 6`
- `DATA_FRESHNESS_THRESHOLD_HOURS = 26`
- `MAX_PULL_RETRIES = 3`
- `SOURCE_COOLDOWN_MINUTES = 30`
- `SOURCE_MAX_CONSECUTIVE_FAILS = 5` (extends cooldown to 6h)
- `TIMEOUT_BLACKLIST_HOURS = 24`

**Source Registry**: [[Hermes Scheduler|Hermes]] maintains a complete registry mapping source names to module paths, classes, and API key requirements. Covers: [[FRED]], yfinance, [[EDGAR]], [[BLS]], Google Trends, CBOE, Fed speeches, fear/greed, Baltic, Crucix bridge, and many more.

## Startup Tasks (api/main.py lifespan)

Run once at server boot:
1. Database health check
2. Agent WebSocket progress broadcast registration
3. Agent scheduler start
4. Ingestion scheduler thread start
5. Server-log git sink (ERROR+ to `.server-logs/errors.jsonl`)
6. Operator inbox (two-way git communication)
7. Capital flow pre-computation (cached, non-blocking)
8. Intelligence loop thread start
9. Push notification integration

## Related

- [[Hermes-Operator]] — Autonomous healing daemon
- [[Ingestion-Core]] — Core data pullers
- [[Trading-Layer]] — Signal execution scheduling
- [[Orchestration-Layer]] — LLM task queue
