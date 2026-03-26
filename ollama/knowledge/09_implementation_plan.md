# GRID Implementation Plan — March 2026

## System State
- 489 tests passing, all services running
- 76 actors across 7 sectors with live data
- 67/100 hypotheses passed backtesting
- Regime: NEUTRAL (S=0.538, dS/dt=+0.083, 43% confidence, worsening toward FRAGILE)
- Capital flowing INTO Energy (+10.1% vs SPY), OUT OF Tech/Financials

## Architecture
- Regime engine uses continuous Stress Index S(t) with first derivative dS/dt
- Feature weights in FEATURE_WEIGHTS dict (tunable via /api/v1/regime/weights)
- Sector map at analysis/sector_map.py with actors, influence weights, connected features
- Research agent at analysis/research_agent.py with gap analysis, hypothesis generation
- Hypothesis tester at analysis/hypothesis_tester.py with lagged cross-correlation
- TradingView webhooks at /api/v1/tradingview/webhook
- Options via Yahoo Finance crumb API (37 tickers)

## Priority Items (for next development session)

### HIGH
1. **Flows page layout** — needs narrative flow like dashboard, group by flow direction, insight-first
2. **Watchlist redesign** — each ticker needs mini briefing card with sector context, influence, options, connected features
3. **Timeframe comparison** — 5y/1y/3m/5w/5d side-by-side for any feature. API: GET /api/v1/signals/timeframes

### MEDIUM
4. **LLM prompt optimization** — use orthogonality to SELECT most informative features, not truncate
5. **Capital flow granularity** — timeframe selector (1W/1M/3M/6M/1Y), compare mode, flow attribution

### LOW
6. **Context window** — restart llama.cpp with --ctx-size 8192
7. **Hypothesis UI** — show 67 passed hypotheses in Discovery view with lag charts

## User Preferences
- Favorite feature: Capital Flow Analysis
- Style: insight-first, narrative flow, actionable
- Pain points: heatmap is useless, watchlist broken, flows page doesn't read logically
- Wants: timeframe visuals for pattern recognition, weight sliders, orthogonal prompt selection
