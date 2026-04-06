# TradingAgents × GRID Integration Plan

## Architecture: Parallel Signal Source

[[TradingAgents]] runs independently alongside GRID's existing inference pipeline.
Both produce recommendations that get logged to the [[Decision Journal|decision journal]] for
comparison. GRID's regime/signals context is injected into TradingAgents' analyst
prompts so agents are regime-aware, but GRID's own inference remains unchanged.

## Integration Points

### 1. New Module: `grid/agents/` — TradingAgents Wrapper

**`grid/agents/__init__.py`** — Package init
**`grid/agents/config.py`** — Agent-specific config (LLM provider, model, debate rounds)
**`grid/agents/context.py`** — Builds GRID context (regime, signals, features) for injection into agent prompts
**`grid/agents/runner.py`** — Orchestrates a TradingAgents run: fetches GRID context → injects into agents → runs propagation → logs result to journal
**`grid/agents/adapter.py`** — Adapts between GRID's data formats and TradingAgents' expected inputs

### 2. Config Additions: `grid/config.py`

Add settings:
- `AGENTS_ENABLED: bool = False`
- `AGENTS_LLM_PROVIDER: str = "hyperspace"` (hyperspace | openai | anthropic)
- `AGENTS_LLM_MODEL: str = "auto"`
- `AGENTS_OPENAI_API_KEY: str = ""`
- `AGENTS_ANTHROPIC_API_KEY: str = ""`
- `AGENTS_DEBATE_ROUNDS: int = 1`
- `AGENTS_DEFAULT_TICKER: str = "SPY"`

### 3. Database: New Table `agent_runs`

Tracks each TradingAgents run with full deliberation context:
- `id`, `run_timestamp`, `ticker`, `as_of_date`
- `grid_regime_state`, `grid_confidence` — regime context at run time
- `analyst_reports` (JSONB) — each analyst's output
- `bull_bear_debate` (JSONB) — researcher deliberation
- `risk_assessment` (JSONB) — risk team output
- `final_decision` (TEXT) — BUY/SELL/HOLD
- `decision_reasoning` (TEXT)
- `decision_journal_id` — FK to decision_journal for [[Cross Reference|cross-reference]]
- `llm_provider`, `llm_model` — which LLM was used
- `duration_seconds` — wall-clock time

### 4. API: New Router `grid/api/routers/agents.py`

Endpoints:
- `POST /api/v1/agents/run` — Trigger a new agent run (ticker, date)
- `GET /api/v1/agents/runs` — List recent runs
- `GET /api/v1/agents/runs/{id}` — Get full run details (analyst reports, debate, decision)
- `GET /api/v1/agents/status` — Check if agents are enabled/available

### 5. PWA: New View `grid/pwa/src/views/Agents.jsx`

- Trigger new agent runs
- View run history with expandable details
- See analyst reports, bull/bear debate, risk assessment, final decision
- Compare agent decisions vs GRID recommendations in the journal

### 6. Integration with Decision Journal

Agent decisions are logged to the existing `decision_journal` table with:
- `grid_recommendation` = the agent's final decision
- `baseline_recommendation` = GRID's own inference recommendation
- `action_taken` = "AGENT_ADVISORY" (operator reviews before acting)
- Annotation linking to the `agent_runs.id` for full deliberation trail

### 7. Dependencies

Add `tradingagents` to `requirements.txt` plus any LLM provider SDKs not already present.

## File Changes Summary

| Action | Path |
|--------|------|
| CREATE | `grid/agents/__init__.py` |
| CREATE | `grid/agents/config.py` |
| CREATE | `grid/agents/context.py` |
| CREATE | `grid/agents/runner.py` |
| CREATE | `grid/agents/adapter.py` |
| CREATE | `grid/api/routers/agents.py` |
| CREATE | `grid/pwa/src/views/Agents.jsx` |
| MODIFY | `grid/config.py` — add agent settings |
| MODIFY | `grid/schema.sql` — add agent_runs table |
| MODIFY | `grid/api/main.py` — register agents router |
| MODIFY | `grid/pwa/src/App.jsx` — add Agents route |
| MODIFY | `grid/requirements.txt` — add tradingagents dependency |
