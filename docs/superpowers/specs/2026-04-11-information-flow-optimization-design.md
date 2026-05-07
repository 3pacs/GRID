# GRID Information Flow Optimization — Design Spec

**Date:** 2026-04-11
**Status:** Draft v2 → Review
**Author:** Claude (session 2026-04-11)
**Scope:** Unified Information Contract Layer wiring all 97 modules

**v2 changelog (2026-04-11):** Post-verification pass. Resolved an architectural inconsistency between §3.2 and §3.4 — handlers live exclusively in `contracts/handlers/` as thin adapters; ROUTES now points at handler module paths, not consumer module paths. Every contract's consumer list has been remapped to the actual existing public API of each module (verified file-by-file against the codebase). Each consumer now carries one of three tags: **[EXISTING]** (direct call to an existing public function), **[GLUE]** (requires a small additive public method on the consumer module, specified inline), or **[NEW]** (requires a new supporting module). Also reworked C11 `PullLifecycle` emission strategy because `BasePuller` has no `run()` hook to wrap.

---

## 1. Problem Statement

The GRID platform has grown to 97 modules across 14 architectural layers — 50+ pullers, 87 intelligence modules, 79 API routers, 66 frontend views. A systematic audit identified **23 distinct information-flow defects** falling into four categories:

1. **Broken feedback loops** — modules that produce learning signals ([[Postmortem|postmortem]], oracle scoring, backtest verdicts, options trade outcomes) but whose outputs never reach the modules that should learn from them.
2. **Unsurfaced intelligence** — seven intelligence modules ([[Postmortem|postmortem]], trust_scorer, source_audit, [[Sleuth|sleuth]], thesis_tracker, dollar_flows, conflict resolver) produce rich data that has no API endpoint and is therefore invisible to operators and the oracle.
3. **Missing event emissions** — lifecycle events (puller start/fail, model promotion attempt, investigation progress, prediction invalidation, trade order lifecycle) are never emitted, so the frontend cannot react in real time.
4. **Entity resolution divergence** — `normalization/entity_map.py` and `intelligence/actor_discovery.py` resolve entities independently with no reconciliation.

The consequence is a system that looks sophisticated on the surface but does not close its own learning loops. Failed predictions do not decay the trust of the signals that informed them. Regime transitions do not re-weight the oracle. Backtest verdicts do not appear in the journal. Each module is individually correct; the system as a whole does not learn.

This spec defines a non-disruptive fix: a thin **Information Contract Layer** that formalises every cross-module information flow through a typed, auditable, asynchronously-dispatched event bus.

---

## 2. Goals and Non-Goals

### Goals

- **G1.** Every cross-module information flow is declared exactly once in a machine-readable route table.
- **G2.** Every payload that crosses a module boundary is a typed, validated Pydantic schema — no dict leakage.
- **G3.** Every emitted event is persisted with a `correlation_id` that allows full lineage tracing from source data to final prediction to post-expiry outcome.
- **G4.** Failed consumer dispatches fall into a dead-letter store with automatic retry (1 min, 10 min, 1 hr) and manual replay on demand.
- **G5.** All 13 contracts land incrementally without rewriting the internals of any existing module — producers add one emit call, consumers add one handler.
- **G6.** Seven new API endpoints expose previously-hidden intelligence (postmortem, trust scores, [[Source Audit|source audit]], [[Sleuth|sleuth]] leads, thesis evolution, [[Dollar Flows|dollar flows]], conflict audit).
- **G7.** Six new SSE channels broadcast previously-missing lifecycle events to the frontend.
- **G8.** Observability: every contract dispatch is counted, timed, and surfaced at `/api/v1/contracts/metrics`.

### Non-Goals

- **NG1.** Not rewriting existing module internals. The contract layer is additive.
- **NG2.** Not replacing `events/bus.py` or its Redpanda/PG-NOTIFY fallback. The contracts layer sits on top.
- **NG3.** Not introducing a new message broker. The existing dual-mode bus is sufficient.
- **NG4.** Not changing PIT correctness semantics. `assert_no_lookahead()` remains unchanged.
- **NG5.** Not changing the [[Decision Journal|decision journal]] immutability contract.
- **NG6.** No synchronous feedback loops. Everything is async with eventual consistency (≤6 hr propagation, bounded by the oracle cycle).

---

## 3. Architecture

### 3.1 Layer Placement

```
┌──────────────────────────────────────────────────────────────────┐
│                      Existing Producers                          │
│   pullers  discovery  inference  oracle  validation  trading     │
│   intelligence  normalization  governance  canvas  journal       │
└─────────────────────────────┬────────────────────────────────────┘
                              │ emit(Contract, payload, correlation_id)
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                  Information Contract Layer                      │
│                                                                  │
│   contracts/schemas.py       → 13 Pydantic models                │
│   contracts/router.py        → ROUTES dict (single source)       │
│   contracts/dispatcher.py    → validate + dispatch + dead-letter │
│   contracts/handlers/*.py    → one file per consumer             │
│   contracts/correlation.py   → correlation_id generation & trace │
│   contracts/observability.py → metrics                           │
│   contracts/replay.py        → manual + auto retry               │
└─────────────────────────────┬────────────────────────────────────┘
                              │ events/bus.emit() + Redpanda / PG NOTIFY
                              ▼
┌──────────────────────────────────────────────────────────────────┐
│                      Existing Consumers                          │
│ trust_scorer  model_evolver  journal  governance  alerts         │
│ options_scanner  oracle  inference  investigation  SSE  frontend │
└──────────────────────────────────────────────────────────────────┘
```

The contract layer is a thin wrapper around `events/bus.py`. Producers call `contracts.emit(ContractSchema(...))`. The dispatcher subscribes to the underlying event bus, validates payloads against the schema, looks up consumers in `router.ROUTES`, and invokes each consumer handler in a bounded worker pool. Exceptions are caught, logged, and persisted to `contracts_dead_letter`.

### 3.2 Single Source of Truth: `contracts/router.py`

All information flow is declared in one dictionary. **All handlers live in `contracts/handlers/*.py`** — they are thin adapters that import the existing consumer module and call its public API. Consumer modules do not import anything from `contracts/`. This one-way dependency is the whole point of the adapter layer: the intelligence, oracle, trading, and journal modules stay contract-agnostic.

```python
# contracts/router.py
from contracts.schemas import (
    PostmortemCompleted, PredictionScored, BacktestGateVerdict,
    OptionsTradeOutcome, CrossReferenceAnomaly, LeverageRiskUpdate,
    RegimeTransition, SignalFired, HypothesisGenerated,
    ActorMaterialized, PullLifecycle, ForensicsTrace,
    InvestigationProgress,
)

# Handler references are qualified paths within contracts.handlers.*
ROUTES: dict[type, list[str]] = {
    PostmortemCompleted: [
        "contracts.handlers.trust.on_postmortem_completed",
        "contracts.handlers.model_evolver.on_postmortem_completed",
        "contracts.handlers.alerts.on_postmortem_completed",
    ],
    PredictionScored: [
        "contracts.handlers.model_evolver.on_prediction_scored",
        "contracts.handlers.journal.on_prediction_scored",
        "contracts.handlers.postmortem.on_prediction_scored",
        "contracts.handlers.trust.on_prediction_scored",
    ],
    BacktestGateVerdict: [
        "contracts.handlers.journal.on_backtest_gate_verdict",
        "contracts.handlers.governance.on_backtest_gate_verdict",
        "contracts.handlers.alerts.on_backtest_gate_verdict",
    ],
    OptionsTradeOutcome: [
        "contracts.handlers.options_scanner.on_options_trade_outcome",
        "contracts.handlers.trust.on_options_trade_outcome",
        "contracts.handlers.postmortem.on_options_trade_outcome",
    ],
    CrossReferenceAnomaly: [
        "contracts.handlers.alerts.on_cross_reference_anomaly",
        "contracts.handlers.canvas.on_cross_reference_anomaly",
        "contracts.handlers.sleuth.on_cross_reference_anomaly",
    ],
    LeverageRiskUpdate: [
        "contracts.handlers.inference.on_leverage_risk_update",
        "contracts.handlers.oracle.on_leverage_risk_update",
        "contracts.handlers.alerts.on_leverage_risk_update",
    ],
    RegimeTransition: [
        "contracts.handlers.trust.on_regime_transition",
        "contracts.handlers.oracle.on_regime_transition",
        "contracts.handlers.trading.on_regime_transition",
        "contracts.handlers.sse.on_regime_transition",
    ],
    SignalFired: [
        "contracts.handlers.oracle.on_signal_fired",
        "contracts.handlers.actor_bridge.on_signal_fired",
        "contracts.handlers.canvas.on_signal_fired",
        "contracts.handlers.sse.on_signal_fired",
    ],
    HypothesisGenerated: [
        "contracts.handlers.hypothesis.on_hypothesis_generated",
        "contracts.handlers.validation.on_hypothesis_generated",
        "contracts.handlers.governance.on_hypothesis_generated",
    ],
    ActorMaterialized: [
        "contracts.handlers.entity_map.on_actor_materialized",
        "contracts.handlers.actor_network.on_actor_materialized",
        "contracts.handlers.canvas.on_actor_materialized",
    ],
    PullLifecycle: [
        "contracts.handlers.alerts.on_pull_lifecycle",
        "contracts.handlers.data_health.on_pull_lifecycle",
        "contracts.handlers.sse.on_pull_lifecycle",
    ],
    ForensicsTrace: [
        "contracts.handlers.causation.on_forensics_trace",
        "contracts.handlers.sleuth.on_forensics_trace",
        "contracts.handlers.canvas.on_forensics_trace",
    ],
    InvestigationProgress: [
        "contracts.handlers.sse.on_investigation_progress",
    ],
}
```

To add a new flow, a developer modifies exactly one file. To audit all flows, a developer reads exactly one file. A static test (`tests/contracts/test_router_integrity.py`) imports every handler path in `ROUTES` at test time — if any handler is missing or renamed, CI fails immediately.

### 3.3 Schemas (`contracts/schemas.py`)

All contracts inherit from a common base:

```python
class BaseContract(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: UUID            = Field(default_factory=uuid4)
    timestamp: datetime       = Field(default_factory=lambda: datetime.now(UTC))
    producer_module: str                                    # e.g. "intelligence.postmortem"
    correlation_id: UUID                                    # lineage root
    schema_version: int       = 1
```

Example concrete contract:

```python
class PostmortemCompleted(BaseContract):
    prediction_id: UUID
    ticker: str
    verdict: Literal["HIT", "MISS", "PARTIAL"]
    realized_pnl: Decimal
    signals_used: list[SignalRef]                  # each has source, trust, weight
    root_cause: str                                # LLM-generated
    contributing_signal_ids: list[UUID]            # subset of signals_used

class SignalRef(BaseModel):
    model_config = ConfigDict(frozen=True)
    signal_id: UUID
    source: str                                     # "congressional", "insider", ...
    trust_at_prediction: float                      # snapshot, not current
    weight_at_prediction: float
```

Frozen + `extra="forbid"` means contracts cannot be mutated and cannot silently accept typo'd fields. This is the academic-grade rigour the requirements call for.

### 3.4 Dispatcher (`contracts/dispatcher.py`)

```python
class Dispatcher:
    def __init__(self, bus: EventBus, routes: dict, engine: Engine, pool_size: int = 8):
        self.bus = bus
        self.routes = routes
        self.engine = engine                              # passed to every handler
        self.pool = ThreadPoolExecutor(max_workers=pool_size)

    def start(self) -> None:
        for contract_type in self.routes:
            channel = _contract_to_channel(contract_type)
            self.bus.subscribe(channel, self._handle)

    def _handle(self, raw_payload: dict) -> None:
        contract_type = _resolve_type(raw_payload["contract_type"])
        try:
            event = contract_type(**raw_payload["data"])
        except ValidationError as e:
            _write_dead_letter(raw_payload, "SCHEMA_INVALID", str(e))
            return

        for handler_path in self.routes[contract_type]:
            self.pool.submit(self._invoke, event, handler_path)

    def _invoke(self, event, handler_path: str):
        try:
            handler = _resolve_handler(handler_path)     # importlib lookup
            with _timer(f"{type(event).__name__}:{handler_path}"):
                handler(event, engine=self.engine)       # all handlers take (event, engine)
            _audit_success(event, handler_path)
        except Exception as e:
            _write_dead_letter(event, handler_path, e)
            _increment_failure_metric(type(event).__name__, handler_path)
```

**Handler signature convention:** every handler in `contracts/handlers/` has the same signature `def on_<contract_snake>(event: ContractType, engine: Engine) -> None`. This makes the dispatcher trivial and lets `tests/contracts/test_router_integrity.py` verify every handler statically.

### 3.5 Correlation IDs & Lineage

Every puller (the entry point for all data) generates a `correlation_id` on ingest and attaches it to the `raw_series` row. All downstream derivations — [[Conflict Resolution|conflict resolution]], feature computation, regime detection, hypothesis generation, inference, oracle prediction, postmortem — carry the same `correlation_id` through every contract emit. This means:

```sql
SELECT * FROM contracts_audit
WHERE correlation_id = '...'
ORDER BY emitted_at;
```

returns the complete lineage of any signal from its source puller to its final outcome. This is the post-graduate traceability the spec promises.

A new endpoint `GET /api/v1/contracts/lineage/{correlation_id}` renders the lineage as a DAG for visual inspection in the PWA.

### 3.6 Dead-Letter & Replay

```sql
CREATE TABLE contracts_dead_letter (
    id           BIGSERIAL PRIMARY KEY,
    event_id     UUID NOT NULL,
    contract_type TEXT NOT NULL,
    payload      JSONB NOT NULL,
    consumer     TEXT NOT NULL,             -- "module.handler"
    error_type   TEXT NOT NULL,             -- "SCHEMA_INVALID" | "CONSUMER_EXCEPTION"
    error_detail TEXT NOT NULL,
    retry_count  INT DEFAULT 0,
    next_retry_at TIMESTAMPTZ,
    failed_at    TIMESTAMPTZ DEFAULT NOW(),
    resolved_at  TIMESTAMPTZ
);

CREATE INDEX contracts_dead_letter_retry
    ON contracts_dead_letter (next_retry_at)
    WHERE resolved_at IS NULL;
```

**Retry strategy (automatic):** 3 attempts at 1 min, 10 min, 1 hr. Then the event stays in dead-letter until manually resolved.

**Replay (manual, any time):**
- CLI: `python -m contracts.replay --contract PostmortemCompleted --since 2026-04-10`
- API: `POST /api/v1/contracts/dead-letter/{id}/replay`
- PWA: Replay button on `/ops/dead-letters` page

### 3.7 Observability

`contracts/observability.py` tracks:

- `contracts_emitted_total{contract_type}`
- `contracts_dispatched_total{contract_type, consumer}`
- `contracts_failed_total{contract_type, consumer, error_type}`
- `contracts_handler_duration_seconds{contract_type, consumer}` (histogram)
- `contracts_dead_letter_depth{contract_type}` (gauge)

Exposed at `GET /api/v1/contracts/metrics` in Prometheus text format.

Alert: if `contracts_dead_letter_depth` for any contract exceeds 100 in 24 h, fire `alerts.email.notify_dead_letter_backlog`.

### 3.8 Audit Table

```sql
CREATE TABLE contracts_audit (
    id               BIGSERIAL PRIMARY KEY,
    event_id         UUID NOT NULL,
    contract_type    TEXT NOT NULL,
    producer_module  TEXT NOT NULL,
    correlation_id   UUID NOT NULL,
    emitted_at       TIMESTAMPTZ DEFAULT NOW(),
    dispatched_to    TEXT[] NOT NULL,
    payload_hash     TEXT NOT NULL
);

CREATE INDEX contracts_audit_correlation ON contracts_audit (correlation_id);
CREATE INDEX contracts_audit_type_time   ON contracts_audit (contract_type, emitted_at DESC);
```

Every emit writes a row. The `payload_hash` allows idempotency checks — if a producer emits the same event twice, we can detect it.

---

## 4. The 13 Contracts

Each contract fixes one or more of the 23 defects. Mapping:

| # | Contract | Fixes Defects |
|---|----------|---------------|
| C1 | `PostmortemCompleted` | Broken: postmortem→trust, postmortem→model_evolver |
| C2 | `PredictionScored` | Broken: oracle→weights, oracle→journal outcome linkage, oracle→postmortem |
| C3 | `BacktestGateVerdict` | Broken: backtest→journal, backtest→governance |
| C4 | `OptionsTradeOutcome` | Broken: tracker→scanner weights, tracker→trust |
| C5 | `CrossReferenceAnomaly` | Broken: cross_reference→alerts, cross_reference→sleuth |
| C6 | `LeverageRiskUpdate` | Broken: leverage→inference gate, leverage→oracle |
| C7 | `RegimeTransition` | Broken: regime→trust, regime→oracle, regime→trading, regime→SSE (incomplete) |
| C8 | `SignalFired` | Broken: signal→oracle formalisation, signal→actor linkage, signal→canvas |
| C9 | `HypothesisGenerated` | Broken: hypothesis_engine→discovery, hypothesis_engine→validation |
| C10 | `ActorMaterialized` | Broken: actor_discovery→entity_map merge, actor_discovery→canvas |
| C11 | `PullLifecycle` | Missing events: pull_started, pull_failed, pull_completed |
| C12 | `ForensicsTrace` | Broken: forensics→causation_graph unification |
| C13 | `InvestigationProgress` | Missing events: investigation step progress |

**Tag key:** `[EXISTING]` = handler calls an existing public function verbatim. `[GLUE]` = handler requires a small additive public method on the consumer module (signature specified). `[NEW]` = requires a new supporting module or table.

### 4.1 Contract Detail: C1 `PostmortemCompleted`

**Producer:** `intelligence/postmortem.py::generate_prediction_postmortem()` — emits at the end of the existing function, after the `PostMortem` dataclass is persisted.

**Payload:** `prediction_id`, `ticker`, `verdict`, `realized_pnl`, `signals_used[]`, `root_cause`, `contributing_signal_ids[]`, `correlation_id`

**Consumers:**
1. `contracts.handlers.trust.on_postmortem_completed` → **[GLUE]** — `intelligence/trust_scorer.py` gains a new public function:
   ```python
   def apply_failure_feedback(engine: Engine, signal_ids: list[UUID], weight: float = 1.0) -> dict:
       """Bayesian decay for a subset of signal sources after a confirmed failure."""
   ```
   Implementation wraps existing `update_trust_scores()` primitives and the Bayesian update math already in the module.
2. `contracts.handlers.model_evolver.on_postmortem_completed` → **[GLUE]** — writes the failure event into a new `model_evolver_feedback_queue` table (`model_evolver` already has `evolve_cycle()`; the next scheduled cycle drains the queue). No new public method on `ModelEvolver` required — the handler does the queue write directly.
3. `contracts.handlers.alerts.on_postmortem_completed` → **[EXISTING]** — `alerts.email.send_alert(subject, body, severity="warning")` when `|realized_pnl|` exceeds threshold; the handler formats the body.

**Implementation cost:** 1 emit line in `postmortem.py`, 3 adapter handlers (~20 LOC each), 1 new function in `trust_scorer.py`, 1 new table for the evolver queue.

### 4.2 Contract Detail: C2 `PredictionScored`

**Producer:** `oracle/engine.py::OracleEngine` — emits inside the existing scoring cycle, after the per-prediction verdict is written.

**Payload:** `prediction_id`, `decision_id`, `ticker`, `verdict`, `expected_direction`, `realized_direction`, `confidence`, `brier_component`, `signals_used[]`, `model_weights_at_prediction`, `correlation_id`

**Consumers:**
1. `contracts.handlers.model_evolver.on_prediction_scored` → **[GLUE]** — writes per-prediction outcome into `model_evolver_feedback_queue` (same table as C1). `evolve_cycle()` is unchanged and drains the queue on its next run.
2. `contracts.handlers.journal.on_prediction_scored` → **[EXISTING]** — calls `journal.log.DecisionJournal(engine).record_outcome(decision_id, outcome_value, verdict, annotation)`. Direct map; the contract carries `decision_id` explicitly so the handler does no lookup.
3. `contracts.handlers.postmortem.on_prediction_scored` → **[EXISTING]** — if `verdict == "MISS"`, enqueues an `llm_taskqueue` job of type `postmortem_analysis` that calls the existing `intelligence.postmortem.generate_prediction_postmortem(engine, prediction_id)`.
4. `contracts.handlers.trust.on_prediction_scored` → **[GLUE]** — calls the same `trust_scorer.apply_failure_feedback()` on MISS or a new `apply_success_feedback()` on HIT. Both are thin wrappers over the existing Bayesian update math.

**Note on the evolver feedback queue:** Creating a queue is cheaper than forcing `evolve_cycle()` to accept per-event args, and preserves its existing scheduled-cycle semantics. The queue gives us accumulation across multiple predictions between cycles.

### 4.3 Contract Detail: C3 `BacktestGateVerdict`

**Producer:** `validation/gates.py::GateChecker.check_all_gates()` — emits after returning the verdict dict.

**Payload:** `hypothesis_id`, `model_version_id`, `gate_name`, `verdict` (PASS/FAIL), `metrics`, `promotion_target_state`, `operator_id`, `correlation_id`

**Consumers:**
1. `contracts.handlers.journal.on_backtest_gate_verdict` → **[EXISTING with adapter]** — uses `DecisionJournal.log_decision()` with a synthesised `inferred_state="GATE_VERDICT"`, `grid_recommendation=verdict`, `action_taken="PROMOTED"|"REJECTED"`. No schema change to `decision_journal` table; we reuse the existing columns with a distinct state label. Adding a new journal entry type is therefore a **labelling convention**, not a schema migration.
2. `contracts.handlers.governance.on_backtest_gate_verdict` → **[EXISTING]** — calls `governance.registry.ModelRegistry(engine).transition(model_version_id, promotion_target_state, operator_id, reason)` on PASS.
3. `contracts.handlers.alerts.on_backtest_gate_verdict` → **[EXISTING]** — calls `alerts.email.send_alert()` with promotion result.

### 4.4 Contract Detail: C4 `OptionsTradeOutcome`

**Producer:** `trading/options_tracker.py` — emits on position close (at existing outcome-recording site).

**Payload:** `trade_id`, `ticker`, `strategy`, `pnl`, `signal_mix[]` (7 scanner signals with weights), `hit_levels`, `duration`, `correlation_id`

**Consumers:**
1. `contracts.handlers.options_scanner.on_options_trade_outcome` → **[GLUE]** — `discovery/options_scanner.py::OptionsScanner` gains a new public method:
   ```python
   def update_signal_weights(self, outcome: dict) -> None:
       """Nudge the 7-signal weights based on a closed trade outcome."""
   ```
2. `contracts.handlers.trust.on_options_trade_outcome` → **[GLUE]** — calls the same `trust_scorer.apply_failure_feedback()` / `apply_success_feedback()` added in C1/C2 with the option-signal source ids.
3. `contracts.handlers.postmortem.on_options_trade_outcome` → **[EXISTING]** — enqueues `intelligence.postmortem.generate_postmortem(engine, trade_id)` via `llm_taskqueue` on losses.

### 4.5 Contract Detail: C5 `CrossReferenceAnomaly`

**Producer:** `intelligence/cross_reference.py` — emits when confidence delta crosses severity threshold.

**Payload:** `statistic`, `official_value`, `reality_proxy_value`, `confidence_delta`, `evidence_links[]`, `severity`, `correlation_id`

**Consumers:**
1. `contracts.handlers.alerts.on_cross_reference_anomaly` → **[EXISTING]** — `alerts.email.alert_on_discovery_insight(title, description, data)`.
2. `contracts.handlers.canvas.on_cross_reference_anomaly` → **[GLUE]** — the existing `api/routers/canvas_investigate.py::auto_investigate()` is a [[FastAPI]] endpoint, not a library function. The handler must call the underlying implementation. We extract the core logic from `auto_investigate()` into a library function:
   ```python
   # api/routers/canvas_investigate.py
   def seed_investigation(engine: Engine, query: str, *, depth: int = 2, max_nodes: int = 50) -> int:
       """Core library function used by both the FastAPI endpoint and the contracts handler."""
   async def auto_investigate(request: InvestigateRequest) -> InvestigateResponse:
       board_id = await run_in_threadpool(seed_investigation, engine, request.query, ...)
       ...
   ```
   This is a light refactor of the existing endpoint — pure extraction, no behaviour change.
3. `contracts.handlers.sleuth.on_cross_reference_anomaly` → **[GLUE]** — `intelligence/sleuth.py::Sleuth` gains a public `add_lead(lead: Lead) -> int` method. The `Lead` dataclass already exists; only the add method is new.

### 4.6 Contract Detail: C6 `LeverageRiskUpdate`

**Producer:** `intelligence/leverage_network.py` — emits at the end of the existing leverage-scan function.

**Payload:** `system_leverage_index`, `top_leveraged_actors[]`, `critical_threshold_breached` (bool), `components`, `correlation_id`

**Consumers:**
1. `contracts.handlers.inference.on_leverage_risk_update` → **[GLUE]** — writes into a new `inference_risk_state` table (single-row, upserted). `inference/live.py::LiveInference` gains a new public method `get_risk_damper() -> float` that reads this row. The inference loop multiplies its output confidence by the damper.
2. `contracts.handlers.oracle.on_leverage_risk_update` → **[GLUE]** — same pattern: reads from `inference_risk_state`. `oracle/engine.py::OracleEngine` gains `get_risk_damper()` and applies it to the final ensemble confidence.
3. `contracts.handlers.alerts.on_leverage_risk_update` → **[EXISTING]** — `alerts.email.send_alert()` when `critical_threshold_breached`.

**Why a table instead of direct method calls:** Leverage updates arrive asynchronously and need to persist across dispatcher restarts. A single-row state table is the simplest durable cache.

### 4.7 Contract Detail: C7 `RegimeTransition`

**Producer:** `discovery/clustering.py::ClusterDiscovery.run_cluster_discovery()` — emits when `get_transition_leaders()` detects a new state.

**Payload:** `from_state`, `to_state`, `confidence`, `triggering_features[]`, `transition_probability_matrix`, `correlation_id`

**Consumers:**
1. `contracts.handlers.trust.on_regime_transition` → **[GLUE]** — `trust_scorer.py` gains `set_regime_context(regime: str) -> None`. Future `update_trust_scores()` calls read this context when weighting sources.
2. `contracts.handlers.oracle.on_regime_transition` → **[GLUE]** — `oracle/engine.py` gains `set_regime_context(regime: str, confidence: float) -> None`. Next cycle's weights are conditioned on it.
3. `contracts.handlers.trading.on_regime_transition` → **[GLUE]** — `trading/options_recommender.py::OptionsRecommender` gains `set_regime(regime: str) -> None`.
4. `contracts.handlers.sse.on_regime_transition` → **[GLUE]** — new helper `events/sse.py::broadcast(channel: str, payload: dict)` (see §4.11 note). Broadcasts on `grid_regime_change` with confidence and triggering features included, fixing the existing UI gap.

### 4.8 Contract Detail: C8 `SignalFired` (Enriched)

**Producer:** every altdata puller after normalisation — emitted from `contracts/emit.py::signal_fired()` called by each puller at its existing signal-write site.

**Payload:** `signal_id`, `source`, `signal_type`, `strength`, `ticker`, `actor_hint`, `raw_row_ids[]`, `correlation_id`

**Consumers:**
1. `contracts.handlers.oracle.on_signal_fired` → **[GLUE]** — writes into a new `oracle_signal_queue` table; `OracleEngine` reads this on its next cycle. Same queue pattern as the evolver feedback queue in C2.
2. `contracts.handlers.actor_bridge.on_signal_fired` → **[EXISTING]** — calls `intelligence.actor_signal_bridge.enrich_signals_with_actors(engine, [{signal_id, ticker, ...}])`. This function already exists and does exactly what we need.
3. `contracts.handlers.canvas.on_signal_fired` → **[NEW helper]** — new function `api/routers/canvas.py::emit_signal_to_live_boards(engine, signal_row)` that writes into existing `canvas_signals` junction table for any board whose filter matches. Extracts logic already present in the existing `_attach_signals()` helper inside `canvas_investigate.py`.
4. `contracts.handlers.sse.on_signal_fired` → **[GLUE]** — via `events/sse.py::broadcast()`.

### 4.9 Contract Detail: C9 `HypothesisGenerated`

**Producer:** `intelligence/hypothesis_engine.py` — emits when a new hypothesis is registered in `hypothesis_registry`.

**Payload:** `hypothesis_id`, `statement`, `layer`, `feature_ids[]`, `lag_structure`, `predecessor_id`, `correlation_id`

**Consumers:**
1. `contracts.handlers.hypothesis.on_hypothesis_generated` → **[EXISTING via SQL]** — the handler simply writes the hypothesis row to `hypothesis_registry` if not already present. This **replaces** the earlier-spec claim that `discovery/clustering.py` has a `register_hypothesis()` method (it does not). Clustering is a discovery engine, not a registry, so the hypothesis registry write lives in the handler.
2. `contracts.handlers.validation.on_hypothesis_generated` → **[GLUE]** — creates a `validation_run` row in state `PENDING` so the scheduled validation loop picks it up. `validation/gates.py::GateChecker` remains a pure reader; scheduling is a SQL insert by the handler.
3. `contracts.handlers.governance.on_hypothesis_generated` → **[GLUE]** — calls a new `governance.registry.ModelRegistry.create_candidate(hypothesis_id, layer, params) -> int` method that inserts a CANDIDATE row (not a transition; `transition()` requires an existing row).

### 4.10 Contract Detail: C10 `ActorMaterialized`

**Producer:** `intelligence/actor_discovery.py::auto_discover_actors()` and `enrich_actor()` — emit when a new or updated actor record lands.

**Payload:** `actor_id`, `canonical_name`, `aliases[]`, `wealth_estimate`, `discovery_source`, `confidence_label`, `correlation_id`

**Consumers:**
1. `contracts.handlers.entity_map.on_actor_materialized` → **[GLUE]** — `normalization/entity_map.py::EntityMap` gains a `merge_actor(actor: dict) -> None` method. The `EntityMap` class already exists; only this merge method is new. This fixes defect #22 (entity resolution divergence) by making `entity_map` the authoritative merge point.
2. `contracts.handlers.actor_network.on_actor_materialized` → **[GLUE]** — new public function `intelligence/actor_network.py::upsert_actor(engine: Engine, actor: dict) -> str` that upserts into the existing `actor_network` table. None of the three existing public functions (`track_wealth_migration`, `persist_wealth_flows`, `assess_pocket_lining`) do this, so a new small function is required.
3. `contracts.handlers.canvas.on_actor_materialized` → **[NEW helper]** — new function `api/routers/canvas.py::emit_actor_to_live_boards(engine, actor)` that updates any canvas board pinned to this actor.

### 4.11 Contract Detail: C11 `PullLifecycle`

**Producer:** per-puller emission via a context manager, **not** a base class wrap — because `BasePuller` has no `run()` hook and pullers call `_insert_raw()` directly rather than implementing a common `pull()` method.

**New helper in `contracts/emit.py`:**
```python
@contextmanager
def pull_lifecycle(engine: Engine, puller_name: str, correlation_id: UUID | None = None):
    """Wrap a puller block; emits STARTED, COMPLETED (with row_count), or FAILED."""
    cid = correlation_id or uuid4()
    started = time.time()
    emit(PullLifecycle(state="STARTED", puller_name=puller_name, correlation_id=cid))
    rows = {"count": 0}
    try:
        yield rows                         # caller increments rows["count"]
        emit(PullLifecycle(state="COMPLETED", puller_name=puller_name,
                           row_count=rows["count"], duration_s=time.time()-started,
                           correlation_id=cid))
    except Exception as e:
        emit(PullLifecycle(state="FAILED", puller_name=puller_name,
                           error=str(e), duration_s=time.time()-started,
                           correlation_id=cid))
        raise
```

Wiring each puller is a **one-line change**: wrap the existing pull body in `with pull_lifecycle(engine, "FRED") as rows:`. That is ~50 identical one-liners, mechanically added, with no behavioural risk.

**Payload:** `puller_name`, `state`, `row_count`, `duration_s`, `error`, `correlation_id`

**Consumers:**
1. `contracts.handlers.alerts.on_pull_lifecycle` → **[EXISTING]** — `alerts.email.alert_on_failure(source, error)` on FAILED.
2. `contracts.handlers.data_health.on_pull_lifecycle` → **[NEW module]** — `intelligence/data_health.py` does **not** exist and must be created. It is a very small module: one table `data_health_events`, one function `record_lifecycle(event)`, and a query helper for the health dashboard. The module is defined in §5 alongside the new endpoints.
3. `contracts.handlers.sse.on_pull_lifecycle` → **[GLUE]** — via `events/sse.py::broadcast()`.

### 4.12 Contract Detail: C12 `ForensicsTrace`

**Producer:** `intelligence/forensics.py` — emits when a reconstructed price-move trace is persisted.

**Payload:** `trace_id`, `ticker`, `window`, `reconstructed_sequence[]`, `suspected_levers[]`, `correlation_id`

**Consumers:**
1. `contracts.handlers.causation.on_forensics_trace` → **[GLUE]** — `intelligence/causation_graph.py` gains a new public function `ingest_forensics_trace(engine, trace) -> None` that merges the [[Forensics|forensics]] trace into the causation graph's storage. The existing `trace_causal_chain()`, `find_longest_chains()`, and `load_causal_chains()` functions continue to work unchanged; the new ingestion function writes the same underlying storage they read. This fixes defect #9 (parallel causation engines) by making the causation graph authoritative.
2. `contracts.handlers.sleuth.on_forensics_trace` → **[GLUE]** — `Sleuth` class gains an `ingest_trace(trace) -> list[Lead]` method.
3. `contracts.handlers.canvas.on_forensics_trace` → **[NEW helper]** — `emit_trace_to_live_boards()` writes into `canvas_traces` for any board pinned to the ticker.

### 4.13 Contract Detail: C13 `InvestigationProgress`

**Producer:** `api/routers/canvas_investigate.py::seed_investigation()` — emits at each step of the LLM research loop.

**Payload:** `board_id`, `step`, `total_steps`, `description`, `partial_nodes[]`, `correlation_id`

**Consumers:**
1. `contracts.handlers.sse.on_investigation_progress` → **[GLUE]** — via `events/sse.py::broadcast()`. Fixes the spinner-only UX issue.

### 4.14 The `events/sse.py::broadcast()` Helper

Several consumers above require an SSE broadcast helper. The existing `api/routers/sse.py` only exposes an endpoint (`event_stream`), not a library function for in-process broadcasting. To avoid importing [[FastAPI]] inside handlers, we add one small helper:

```python
# events/sse.py (new file, replaces ad-hoc helpers)
def broadcast(channel: str, payload: dict) -> None:
    """Emit to the local SSE fan-out without going through FastAPI."""
    from events.bus import emit_sync
    emit_sync(channel, payload)
```

`events/bus.py` already has `emit_sync()`, and `api/routers/sse.py::event_stream` already subscribes to those channels via `bus.subscribe()`. So this helper is a three-line passthrough, not new infrastructure.

---

## 4b. Consolidated Glue-Method & New-Module Inventory

Every **[GLUE]** and **[NEW]** tag in §4 resolves to one of the entries below. If an item is not listed here, it does not need to exist.

### New small public methods on existing modules

| Module | New method | Purpose |
|--------|-----------|---------|
| `intelligence/trust_scorer.py` | `apply_failure_feedback(engine, signal_ids, weight)` | Bayesian decay for a signal subset |
| `intelligence/trust_scorer.py` | `apply_success_feedback(engine, signal_ids, weight)` | Bayesian boost for a signal subset |
| `intelligence/trust_scorer.py` | `set_regime_context(regime)` | Condition future scoring on regime |
| `oracle/engine.py` | `set_regime_context(regime, confidence)` | Condition next cycle on regime |
| `oracle/engine.py` | `get_risk_damper() → float` | Read `inference_risk_state` |
| `inference/live.py` | `get_risk_damper() → float` | Read `inference_risk_state` |
| `discovery/options_scanner.py` | `OptionsScanner.update_signal_weights(outcome)` | Tune the 7-signal weights |
| `trading/options_recommender.py` | `OptionsRecommender.set_regime(regime)` | Adapt strategy per regime |
| `intelligence/actor_network.py` | `upsert_actor(engine, actor)` | Add/update actor row |
| `intelligence/causation_graph.py` | `ingest_forensics_trace(engine, trace)` | Merge forensics into causation |
| `intelligence/sleuth.py` | `Sleuth.add_lead(lead)` | Persist a new Lead |
| `intelligence/sleuth.py` | `Sleuth.ingest_trace(trace)` | Derive leads from a forensics trace |
| `normalization/entity_map.py` | `EntityMap.merge_actor(actor)` | Authoritative actor merge |
| `governance/registry.py` | `ModelRegistry.create_candidate(hypothesis_id, layer, params)` | Insert CANDIDATE row |
| `api/routers/canvas_investigate.py` | `seed_investigation(engine, query, depth, max_nodes)` | Extract from existing async endpoint for reuse |
| `api/routers/canvas.py` | `emit_signal_to_live_boards(engine, signal)` | Live-board signal fan-out |
| `api/routers/canvas.py` | `emit_actor_to_live_boards(engine, actor)` | Live-board actor fan-out |
| `api/routers/canvas.py` | `emit_trace_to_live_boards(engine, trace)` | Live-board trace fan-out |

Every one of these is ≤50 LOC and purely additive. No existing function signature changes. No caller needs to be updated.

### New tables

| Table | Purpose | Owning contract |
|-------|---------|-----------------|
| `contracts_audit` | Full emission lineage | infrastructure |
| `contracts_dead_letter` | Failed dispatches for retry/replay | infrastructure |
| `model_evolver_feedback_queue` | Per-prediction outcome buffer between scheduled `evolve_cycle()` runs | C1 + C2 |
| `oracle_signal_queue` | Per-signal buffer between oracle cycles | C8 |
| `inference_risk_state` | Single-row durable risk damper | C6 |
| `data_health_events` | Pull lifecycle history | C11 |

### New supporting modules

| Module | Purpose | Approximate size |
|--------|---------|-----------------|
| `events/sse.py` | `broadcast(channel, payload)` helper over existing `events/bus.emit_sync()` | ~15 LOC |
| `intelligence/data_health.py` | `record_lifecycle(event)`, `recent_failures()`, `coverage_snapshot()` reading `data_health_events` | ~120 LOC |
| `contracts/emit.py` | `pull_lifecycle()` context manager + typed emit helpers per contract | ~80 LOC |

### Light refactors to existing files

| File | Change |
|------|--------|
| `api/routers/canvas_investigate.py` | Extract `seed_investigation()` library function from the existing `auto_investigate()` async endpoint; endpoint calls it via `run_in_threadpool`. Pure extraction, no behaviour change. |
| All ~50 pullers | Wrap main pull body in `with pull_lifecycle(engine, "<name>") as rows:` — one line per puller. |

That is the complete delta. Every other file in the repository is untouched.

---

## 5. New API Surface

Seven endpoints expose previously-hidden intelligence. Each is a thin wrapper around an existing module.

| Endpoint | Backing Module | Purpose |
|----------|----------------|---------|
| `GET /api/v1/intel/postmortem` | `intelligence/postmortem.py` | List failure analyses, filter by time/ticker/verdict |
| `GET /api/v1/intel/trust-scores` | `intelligence/trust_scorer.py` | Current Bayesian trust by source, with history |
| `GET /api/v1/intel/source-audit` | `intelligence/source_audit.py` | Accuracy comparison dashboard |
| `GET /api/v1/intel/sleuth/leads` | `intelligence/sleuth.py` | Open investigative leads |
| `GET /api/v1/intel/thesis/{id}/evolution` | `intelligence/thesis_tracker.py` | Thesis versions over time |
| `GET /api/v1/flows/dollar-flows/{sector}` | `intelligence/dollar_flows.py` | USD-normalised sector flows |
| `GET /api/v1/normalization/conflicts` | `normalization/resolver.py` | Resolver conflict audit trail |

Plus three contract infrastructure endpoints:

| Endpoint | Purpose |
|----------|---------|
| `GET /api/v1/contracts/metrics` | Prometheus metrics |
| `GET /api/v1/contracts/lineage/{correlation_id}` | Full lineage DAG for a correlation id |
| `POST /api/v1/contracts/dead-letter/{id}/replay` | Manual replay |

All endpoints reuse existing auth (`require_auth`) and existing DB engine.

---

## 6. New SSE Channels

Added to `events/channels.py`:

| Channel | Source Contract |
|---------|-----------------|
| `grid_postmortem_completed` | C1 |
| `grid_leverage_risk_update` | C6 |
| `grid_hypothesis_generated` | C9 |
| `grid_actor_materialized` | C10 |
| `grid_investigation_progress` | C13 |
| `grid_pull_lifecycle` | C11 |

Frontend `realtimeStore.js` subscribes, dispatches to domain stores, triggers live UI updates in the respective views.

---

## 7. Implementation Phases

All 13 contracts are in scope, but sequenced to close the highest-value feedback loops first.

### Phase 1 — Infrastructure (no behaviour change)

- `contracts/schemas.py` (base + 13 schemas)
- `contracts/router.py` (ROUTES dict, empty handler stubs)
- `contracts/dispatcher.py`
- `contracts/correlation.py`
- `contracts/observability.py`
- `contracts/replay.py`
- DB [[migrations]] for `contracts_audit`, `contracts_dead_letter`
- API endpoints `/api/v1/contracts/*`
- Observability metrics endpoint
- Unit test scaffolding

**Exit criterion:** infrastructure boots cleanly, zero contracts wired, all tests green.

### Phase 2 — Feedback loops (the critical fix)

- C1 `PostmortemCompleted`
- C2 `PredictionScored`
- C3 `BacktestGateVerdict`
- C7 `RegimeTransition`

**Exit criterion:** end-to-end integration test — simulate oracle cycle with a seeded failure, verify trust decays, weights evolve, journal entry appears, alert fires. Lineage query returns complete DAG.

### Phase 3 — Signal & actor flow

- C4 `OptionsTradeOutcome`
- C8 `SignalFired` (enriched)
- C9 `HypothesisGenerated`
- C10 `ActorMaterialized` (fixes entity divergence)

**Exit criterion:** regression test proving entity_map and actor_network stay in sync after 1000 simulated actor discoveries.

### Phase 4 — Intelligence surfacing

- C5 `CrossReferenceAnomaly`
- C6 `LeverageRiskUpdate`
- C11 `PullLifecycle` (base class change only)
- C12 `ForensicsTrace`
- C13 `InvestigationProgress`
- Seven new intel API endpoints
- Six new SSE channels wired to frontend stores

**Exit criterion:** frontend PWA displays postmortems, trust scores, leads, thesis evolution, investigation progress in real time. No polling anywhere.

### Phase 5 — Operations

- Dead-letter dashboard in PWA (`/ops/dead-letters`)
- Prometheus metrics exported and Grafana board
- CLI replay tool
- Full integration test suite at 85%+ coverage

---

## 8. Testing Strategy

### 8.1 Unit Tests

- `tests/contracts/test_schemas.py` — every schema rejects malformed payloads; frozen contract cannot be mutated.
- `tests/contracts/test_router.py` — `ROUTES` dict references only real modules and real handler names (static import check).
- `tests/contracts/test_dispatcher.py` — success path, schema-invalid path, consumer-exception path, dead-letter write, retry scheduling.
- `tests/contracts/test_correlation.py` — correlation_id generation, propagation, lineage query.
- `tests/contracts/handlers/test_{contract}.py` — one file per contract, each handler in isolation with mocked DB.

### 8.2 Integration Tests

- `tests/integration/test_feedback_loop_c1.py` — emit PostmortemCompleted, assert trust_scorer, model_evolver, alerts all called with correct args.
- `tests/integration/test_feedback_loop_c2_c3.py` — simulate oracle cycle, verify full outcome chain.
- `tests/integration/test_entity_merge_c10.py` — 1000 actors discovered, entity_map and actor_network stay consistent.
- `tests/integration/test_dead_letter_replay.py` — inject handler exception, verify dead-letter, manually replay, verify success.

### 8.3 End-to-End Test

- `tests/e2e/test_full_lineage.py` — simulate a puller emitting a signal, trace correlation_id through normalise → feature → hypothesis → validation → inference → oracle → prediction → scoring → postmortem → trust decay. Verify `contracts_audit` contains every step.

Target: 85%+ overall coverage on `contracts/`, 100% on `schemas.py` and `router.py`.

---

## 9. Operational Runbook

### 9.1 Day-to-day

- Operator opens `/ops/dead-letters` dashboard daily. Any non-zero depth is triaged.
- Replay button on dashboard resolves known-good events.
- Correlation lineage viewer lets you trace any anomalous prediction end-to-end.

### 9.2 When a contract is added

1. Define schema in `contracts/schemas.py`.
2. Add route in `contracts/router.py`.
3. Write handler in `contracts/handlers/{module}.py`.
4. Add emit call at producer.
5. Write integration test.
6. Deploy.

That is the entire procedure. No other file in the codebase needs to change.

### 9.3 When a handler starts failing

1. Alert fires if dead-letter depth > 100 in 24 h.
2. Operator queries `/api/v1/contracts/metrics` to find the failing `{contract, handler}` pair.
3. Operator inspects dead-letter entries via PWA dashboard.
4. Fix the bug in the handler.
5. Replay the dead-letter backlog (CLI or PWA).

---

## 10. Risks and Mitigations

| Risk | Mitigation |
|------|-----------|
| Dispatcher becomes bottleneck | Bounded thread pool, per-contract worker pools if needed, metrics show it |
| Schema change breaks contract | `schema_version` field, all handlers accept forward-compatible extra fields gated by version |
| Correlation_id not propagated in legacy path | Static analysis test scans codebase for any `emit(Contract, ...)` missing `correlation_id=` arg |
| Infinite loop (A emits → B emits → A emits) | Dispatcher detects cycle via per-correlation_id emission counter, caps at 32 |
| Dead-letter grows unbounded | Retention policy: resolved rows purged after 30 days, unresolved alerts at 100 and pages operator at 1000 |
| Redpanda fallback to PG NOTIFY loses events under load | PG NOTIFY is fire-and-forget — on PG path we write to `contracts_audit` synchronously before NOTIFY so audit is durable even if dispatcher crashes |
| SQL injection in handler DB writes | All handler SQL goes through `sqlalchemy.text().bindparams()` per `.claude/rules/security.md`. The `contracts/handlers/` directory is linted for bare `.format()` / f-string SQL as part of CI. |
| Handler files exceed 800-line limit | Each handler file covers one consumer domain (`trust`, `journal`, `alerts`, etc.). If a file approaches 400 LOC, split by contract (`trust_postmortem.py`, `trust_regime.py`). Hard cap enforced by CI. |
| Zero-coverage glue targets | `entity_map.py`, `clustering.py`, `gates.py`, `governance/registry.py`, `inference/live.py` are in the ATTENTION.md #22 zero-coverage list. Any new public method added to these modules per §4b must land with its own test file covering the new method (not the whole module). |

---

## 11. Success Metrics

Thirty days after full rollout:

- **Feedback loops closed:** `trust_scorer` trust deltas correlate with postmortem verdicts (r > 0.6).
- **Weight evolution active:** oracle model weights show non-trivial variance over 30 days.
- **Unsurfaced intelligence visible:** all 7 new intel endpoints return data, frontend views render them.
- **Real-time lifecycle events:** all 6 new SSE channels broadcasting, frontend receives within 1 s.
- **Zero silent failures:** dead-letter dashboard shows < 10 unresolved entries at any time.
- **Full lineage coverage:** every prediction has a traceable `correlation_id` chain back to source pullers.
- **Entity resolution convergence:** zero divergence between `entity_map` and `actor_network` for 30 consecutive days.

---

## 12. Out of Scope (Explicitly)

- Rewriting the 6.2 MB intelligence layer.
- Moving to a new message broker.
- Changing PIT correctness semantics.
- Schema migration automation (deferred to a later iteration).
- Per-tenant isolation (single-tenant platform).
- Federated learning or cross-instance contract sync.

---

## 13. Appendix: Defect-to-Contract Traceability

All 23 audit defects are closed:

| Defect | Contract | Notes |
|--------|----------|-------|
| 1. Postmortem → Trust | C1 | Closes primary learning loop |
| 2. Oracle → Model Evolver | C2 | Closes weight evolution loop |
| 3. Backtest → Journal | C3 | Gate verdicts now journaled |
| 4. Options Tracker → Scanner | C4 | Self-improving weights |
| 5. Trust → Oracle | C2 + C7 | Trust applied via PredictionScored and RegimeTransition |
| 6. Cross-Reference → Alerts | C5 | Plus auto-investigation |
| 7. Leverage → Risk Gates | C6 | Inference gate integrated |
| 8. Regime → Trading | C7 | Strategy adapts per regime |
| 9. Forensics ↔ Causation | C12 | Parallel engines unified |
| 10. Postmortem API | §5 | New endpoint |
| 11. Trust Scores API | §5 | New endpoint |
| 12. Source Audit API | §5 | New endpoint |
| 13. Sleuth Leads API | §5 | New endpoint |
| 14. Thesis Evolution API | §5 | New endpoint |
| 15. Dollar Flows API | §5 | New endpoint |
| 16. Conflict Audit API | §5 | New endpoint |
| 17. Pull lifecycle events | C11 | Base class emission |
| 18. Promotion events | C3 | BacktestGateVerdict |
| 19. Investigation progress | C13 | Step-level events |
| 20. Prediction invalidation | C2 | Part of PredictionScored |
| 21. Trade lifecycle events | C4 | OptionsTradeOutcome + extensions |
| 22. Entity resolution divergence | C10 | Mandatory merge on materialisation |
| 23. Hypothesis engine ↔ discovery | C9 | HypothesisGenerated |

All 23 defects close cleanly against the 13 contracts and 7 endpoints. Nothing left to chance.
