---
source: /Users/anikdang/grid_obsidian/Architecture/Trading-Layer.md
promoted_at: 2026-04-13
promoted_via: OBSIDIAN-2 (task #76)
---
# Trading Layer

11 files implementing paper trading, live exchange integration, options recommendations, and prediction market trading.

## Modules

### Paper Trading Engine
- **`paper_engine.py`** — Core paper trading engine. Tracks hypothetical trades with P&L, supports multiple strategies. Reads signals from [[Signal-Registry]], writes to `paper_trades` table.
- **`signal_executor.py`** — Executes trading signals from the intelligence layer. Called hourly by the [[Cron-Schedule]]. Opens/closes positions based on conviction scores.
- **`circuit_breaker.py`** — Strategy circuit breaker. Kills strategies that breach drawdown or loss limits.

### Live Exchange Integration
- **`hyperliquid.py`** — Hyperliquid perpetual futures integration. Balance, positions, trade execution, close. Real exchange with real money.
- **`wallet_manager.py`** — Multi-wallet management. Each wallet has its own risk limits, kill switch, pause/resume.

### Prediction Markets
- **`prediction_markets.py`** — [[Polymarket]] integration (buy, portfolio, markets)
- **`prediction_pmxt.py`** — PMXT (prediction market exchange) trader. Combined Polymarket + Kalshi.

### Options
- **`options_recommender.py`** — Generates options trade recommendations based on vol surface, [[Dealer Gamma|GEX]], dealer positioning. Called daily at 07:00. See [[Options-Analytics]].
- **`options_tracker.py`** — Tracks and scores options recommendations over time. Weekly improvement cycle.

### Strategy
- **`strategy151.py`** — Strategy #151 implementation (specific trading strategy)

## Architecture

```
Signals (intelligence, regime, oracle)
    ↓
signal_executor.py (hourly)
    ↓
paper_engine.py (paper trades)    hyperliquid.py (live perps)
    ↓                                 ↓
paper_trades table                real P&L
    ↓
options_recommender.py (daily)
    ↓
options_tracker.py (weekly scoring)
```

## Dependencies

- **Reads from**: [[Signal-Registry]], [[Feature-Registry]], `resolved_series`, regime state
- **Writes to**: `paper_trades`, `trade_log`, `strategy_history`
- **API**: [[API-Endpoints-Master]] — `/api/v1/trading/*` (29 endpoints), `/api/v1/options/*` (7 endpoints)
- **Scheduling**: signal_executor hourly, options_recommender daily, options_tracker weekly

## Known Issues

- Hyperliquid integration is live — requires careful position management
- Circuit breaker must be tested under extreme volatility scenarios
- [[Options Tracker|Options tracker]] scoring cycle can be slow for large recommendation sets
