# GRID API Reference

Base URL: `/api/v1`

All endpoints except `/health` require JWT authentication via `Authorization: Bearer <token>` header.

## Agents

GRID API — TradingAgents router.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/agents/status` | Check whether agents are enabled and which LLM is configured. |
| `POST` | `/api/v1/agents/run` | Trigger a new TradingAgents deliberation run. |
| `POST` | `/api/v1/agents/run/sync` | Trigger and wait for a TradingAgents run (blocking). |
| `GET` | `/api/v1/agents/runs` | List recent agent runs. |
| `GET` | `/api/v1/agents/runs/{run_id}` | Get full details of a specific agent run. |
| `POST` | `/api/v1/agents/backtest` | Backtest agent decisions against journal outcomes. |
| `GET` | `/api/v1/agents/backtest/summary` | Quick summary comparing agent vs GRID performance. |
| `GET` | `/api/v1/agents/schedule` | Get agent schedule status. |
| `POST` | `/api/v1/agents/schedule/start` | Start the agent scheduler. |
| `POST` | `/api/v1/agents/schedule/stop` | Stop the agent scheduler. |

## Backtest

GRID API — Backtest & paper trade endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/backtest/run` | Run the full pitch backtest. |
| `GET` | `/api/v1/backtest/results` | Get latest full backtest results (includes equity curve data). |
| `GET` | `/api/v1/backtest/summary` | Get pitch-ready summary of latest backtest. |
| `POST` | `/api/v1/backtest/charts` | Generate all pitch charts from latest backtest results. |
| `GET` | `/api/v1/backtest/charts/{name}` | Serve a generated chart image. |
| `POST` | `/api/v1/backtest/paper-trade` | Create a timestamped paper trade snapshot. |
| `GET` | `/api/v1/backtest/paper-trades` | List all paper trade snapshots. |
| `GET` | `/api/v1/backtest/paper-trades/{filename}` | Get a specific paper trade snapshot. |
| `POST` | `/api/v1/backtest/paper-trade/score` | Score all expired paper trade predictions. |

## Config

System configuration endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/config/sources` | Return all rows from source_catalog. |
| `PUT` | `/api/v1/config/sources/{source_id}` | Update source configuration. |
| `GET` | `/api/v1/config/features` | Return all rows from feature_registry. |
| `PUT` | `/api/v1/config/features/{feature_id}` | Update feature configuration (model_eligible only). |

## Discovery

Discovery engine endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/v1/discovery/orthogonality` | Trigger orthogonality audit as background task. |
| `POST` | `/api/v1/discovery/clustering` | Trigger cluster discovery as background task. |
| `GET` | `/api/v1/discovery/jobs` | Return list of recent jobs. |
| `GET` | `/api/v1/discovery/results/orthogonality` | Return most recent orthogonality results. |
| `GET` | `/api/v1/discovery/results/clustering` | Return most recent clustering results. |
| `GET` | `/api/v1/discovery/hypotheses` | Return all hypotheses from hypothesis_registry. |

## Journal

Decision journal endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/journal/stats` | Return journal performance summary. |
| `GET` | `/api/v1/journal/{entry_id}` | Return a single journal entry. |
| `PUT` | `/api/v1/journal/{entry_id}/outcome` | Record outcome for an existing journal entry. |

## Models

Model registry endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/models/production` | Return current production model for each layer. |
| `GET` | `/api/v1/models/{model_id}` | Return a single model with full details. |
| `POST` | `/api/v1/models/{model_id}/transition` | Transition a model to a new state. |
| `POST` | `/api/v1/models/{model_id}/rollback` | Rollback a model. |

## Ollama

GRID API — LLM integration endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/ollama/status` | Check LLM server availability and model info. |
| `POST` | `/api/v1/ollama/briefing` | Generate a market briefing (hourly/daily/weekly). |
| `GET` | `/api/v1/ollama/briefing/latest` | Get the most recent saved briefing of the given type. |
| `GET` | `/api/v1/ollama/briefings` | List saved briefing files. |
| `GET` | `/api/v1/ollama/briefings/{filename}` | Read a specific saved briefing file. |
| `POST` | `/api/v1/ollama/ask` | Ask a free-form question with optional context. |
| `POST` | `/api/v1/ollama/explain` | Explain the economic mechanism behind a feature relationship. |
| `POST` | `/api/v1/ollama/hypotheses` | Generate falsifiable hypothesis candidates from a pattern. |
| `POST` | `/api/v1/ollama/regime-analysis` | Analyze a regime transition with economic context. |
| `GET` | `/api/v1/ollama/insights` | List recent LLM insight files. |
| `POST` | `/api/v1/ollama/insights/review` | Generate an insight review for the given period. |

## Options

Options scanner API endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/options/signals` | Return latest options daily signals. |
| `GET` | `/api/v1/options/scan` | Run the mispricing scanner and return flagged opportunities. |
| `GET` | `/api/v1/options/100x` | Return only 100x+ flagged mispricing opportunities. |
| `GET` | `/api/v1/options/history` | Return historical mispricing scan results. |

## Physics

GRID API — Market physics endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/physics/verify` | Run full market physics verification suite. |
| `GET` | `/api/v1/physics/conventions` | List all financial conventions. |
| `GET` | `/api/v1/physics/conventions/{domain}` | Get convention for a specific domain. |
| `GET` | `/api/v1/physics/ou/{feature}` | Estimate Ornstein-Uhlenbeck parameters for a feature. |
| `GET` | `/api/v1/physics/hurst/{feature}` | Compute Hurst exponent for a feature. |
| `GET` | `/api/v1/physics/energy/{feature}` | Compute kinetic/potential/total energy decomposition for a feature. |

## Regime

Regime state endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/regime/current` | Return current inferred regime state. |
| `GET` | `/api/v1/regime/all-active` | Return all active regime states with their latest readings. |
| `GET` | `/api/v1/regime/synthesis` | LLM-powered regime synthesis — interprets combined signals. |
| `GET` | `/api/v1/regime/history` | Return regime history. |
| `GET` | `/api/v1/regime/transitions` | Return all detected regime transitions. |

## Signals

Live signals endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/signals/snapshot` | Return current feature snapshot. |
| `GET` | `/api/v1/signals/crucix` | Return latest Crucix-sourced signals. |

## System

System status and health endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/system/health` | Health check — no auth required. |
| `GET` | `/api/v1/system/status` | Comprehensive system status. |
| `GET` | `/api/v1/system/logs` | Return recent log lines. |
| `GET` | `/api/v1/system/alerts` | Return active server alerts for critical conditions. |
| `POST` | `/api/v1/system/restart-hyperspace` | Restart the Hyperspace node. |

## Workflows

GRID API — Workflow management endpoints.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/api/v1/workflows/enabled` | List only enabled workflows. |
| `POST` | `/api/v1/workflows/{name}/enable` | Enable a workflow by name. |
| `POST` | `/api/v1/workflows/{name}/disable` | Disable a workflow by name. |
| `POST` | `/api/v1/workflows/{name}/run` | Execute a workflow by name (synchronous — may take a while). |
| `GET` | `/api/v1/workflows/{name}/validate` | Validate a workflow file for correctness. |
| `GET` | `/api/v1/workflows/waves` | Show the wave execution plan for enabled workflows. |
| `GET` | `/api/v1/workflows/schedule` | Show scheduled workflows and their timing. |

## WebSocket

| Method | Path | Description |
|--------|------|-------------|
| `WS` | `/ws` | Real-time updates. Auth via first message: `{"type": "auth", "token": "..."}` |

### Message Types (server to client)

| Type | Description |
|------|-------------|
| `connected` | Initial connection confirmation with uptime |
| `regime_update` | Regime state change detected |
| `signal_update` | Trading signal update |
| `node_update` | Hyperspace node status change |
| `agent_progress` | TradingAgents run progress |
| `agent_run_complete` | Agent deliberation finished |
| `ping` | Keepalive (every 10s) |
