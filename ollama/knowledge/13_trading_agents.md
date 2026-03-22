# TradingAgents — Multi-Agent Deliberation Pipeline

## Overview

GRID integrates a multi-agent AI deliberation system that runs analyst agents
(fundamentals, sentiment, news, technical) in a structured bull/bear debate,
anchored to GRID's regime context. Each run produces a final BUY/SELL/HOLD
decision with full reasoning, logged to the decision journal.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│  1. CONTEXT GATHERING                               │
│     GRIDContext builds snapshot:                     │
│     - Current regime state + confidence              │
│     - Feature values (latest PIT)                   │
│     - Transition probability                        │
│     - Recent journal entries                        │
├─────────────────────────────────────────────────────┤
│  2. ANALYST PHASE                                    │
│     Four analyst agents run in parallel:             │
│     - Fundamentals analyst                          │
│     - Sentiment analyst                             │
│     - News analyst                                  │
│     - Technical analyst                             │
│     Each produces a report with conviction score    │
├─────────────────────────────────────────────────────┤
│  3. BULL / BEAR DEBATE                               │
│     Two debater agents argue the case:               │
│     - Bull makes the case for the trade             │
│     - Bear argues against                           │
│     - Multiple rounds of rebuttal                   │
│     - Debate is logged verbatim                     │
├─────────────────────────────────────────────────────┤
│  4. RISK ASSESSMENT                                  │
│     Risk agent evaluates:                            │
│     - Position sizing given regime                   │
│     - Tail risk scenarios                           │
│     - Correlation with existing positions           │
│     - Stop-loss and take-profit levels              │
├─────────────────────────────────────────────────────┤
│  5. FINAL DECISION                                   │
│     Synthesizer agent produces:                      │
│     - BUY / SELL / HOLD verdict                     │
│     - Decision reasoning (text)                     │
│     - Confidence score                              │
│     - Counterfactual: "what if we're wrong?"        │
│     → Logged to agent_runs + decision_journal       │
└─────────────────────────────────────────────────────┘
```

## Regime Anchoring

The key differentiator is that every agent receives GRID's regime context:
- **CRISIS regime**: Agents are prompted to be defensive, emphasize capital preservation
- **GROWTH regime**: Agents evaluate risk-on opportunities
- **TRANSITION regime**: Agents flag uncertainty, recommend reduced sizing
- This prevents the agents from making decisions that contradict GRID's macro view

## Scheduling

- **Manual**: POST `/api/v1/agents/run` with `{"ticker": "AAPL"}`
- **Scheduled**: Configurable cron (default: weekdays 5 PM)
- **Progress**: Real-time WebSocket updates during runs (stage, detail, %)

## Backtest & Evaluation

- POST `/api/v1/agents/backtest` — Compare agent decisions vs journal outcomes
- Metrics: agreement rate with GRID, helped/harmed counts, win rate
- Breakdown by regime — did agents add value in specific market conditions?

## Key Files

- `agents/runner.py` — AgentRunner orchestration
- `agents/context.py` — GRIDContext builder (regime + features snapshot)
- `agents/adapter.py` — Parser: agent output → journal schema
- `agents/scheduler.py` — Cron scheduling with background loop
- `agents/progress.py` — WebSocket progress emitter
- `api/routers/agents.py` — REST endpoints

## Database Tables

- `agent_runs` — Full deliberation record (analyst_reports, debate, risk, decision)
- `decision_journal` — Decision logged with agent's recommendation
