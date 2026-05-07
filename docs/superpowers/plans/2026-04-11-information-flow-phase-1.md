# Information Flow Phase 1 — Contracts Infrastructure Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-[[development]] (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship the `contracts/` infrastructure layer — schemas, correlation ids, emit helpers, router, dead-letter store, dispatcher, retry scheduler, observability, and API endpoints — with zero behaviour change to any existing module. Phase 2 will wire actual contracts on top of this foundation.

**[[architecture|Architecture]]:** Thin adapter layer above existing `events/bus.py`. Producers call `contracts.emit(ContractInstance)`; emit writes a row to `contracts_audit`, propagates `correlation_id` through `ContextVar`, and forwards to the existing `EventBus.emit_sync()` fan-out. A `Dispatcher` subscribes to each routed channel, validates payloads against Pydantic schemas, invokes handlers in a bounded thread pool, and writes failures to `contracts_dead_letter` for automatic retry (1min/10min/1hr) or manual replay at any time.

**Tech Stack:** Python 3.11, Pydantic v2 (`BaseModel`, frozen + `extra="forbid"`), [[SQLAlchemy]] 2.0 (`text().bindparams()` only — no f-string SQL per security rules), pytest, [[FastAPI]], existing `events/bus.py` module, existing `tests/conftest.py` fixtures (`mock_engine`, `pg_engine`).

**Spec reference:** `docs/superpowers/specs/2026-04-11-information-flow-optimization-design.md` — Phase 1 is §7 "Phase 1 — Infrastructure (no behaviour change)".

**Non-goals for Phase 1:** No handlers. No producer wiring. No changes to `intelligence/`, `oracle/`, `discovery/`, `validation/`, `trading/`, `journal/`, `governance/`, or any puller. ROUTES is an empty dict. The dispatcher runs but routes zero contracts.

---

## File Structure

Phase 1 creates these files:

```
grid/
├── contracts/
│   ├── __init__.py                  — public API surface (emit, Dispatcher, correlation helpers)
│   ├── schemas.py                   — BaseContract + 13 concrete Pydantic models
│   ├── channels.py                  — contract-type → event-bus channel mapping
│   ├── correlation.py               — correlation_id ContextVar + helpers
│   ├── emit.py                      — emit(contract) + pull_lifecycle() context manager
│   ├── dead_letter.py               — write, list pending, mark resolved, retry scheduling
│   ├── router.py                    — ROUTES dict (empty in Phase 1) + static resolver
│   ├── dispatcher.py                — Dispatcher class
│   ├── retry_scheduler.py           — background retry worker thread
│   ├── observability.py             — counters, histograms, metrics text export
│   ├── replay.py                    — manual replay logic + CLI entry point
│   └── handlers/
│       └── __init__.py              — empty package (populated in Phase 2+)
│
├── api/routers/
│   └── contracts.py                 — /api/v1/contracts/{metrics,lineage,dead-letter}
│
├── scripts/migrations/
│   └── 20260411_contracts_infrastructure.sql
│
└── tests/contracts/
    ├── __init__.py
    ├── conftest.py                  — contracts-specific fixtures
    ├── test_schemas.py
    ├── test_channels.py
    ├── test_correlation.py
    ├── test_emit.py
    ├── test_dead_letter.py
    ├── test_router_integrity.py
    ├── test_dispatcher.py
    ├── test_retry_scheduler.py
    ├── test_observability.py
    ├── test_api_contracts.py
    └── test_replay_cli.py
```

Phase 1 modifies these files:

```
grid/
└── api/main.py                      — register router, start dispatcher in startup hook
```

**All other files in the repository are untouched.**

---

## Task 1: Database Migration

**Files:**
- Create: `grid/scripts/migrations/20260411_contracts_infrastructure.sql`
- Test: `tests/contracts/test_migration.py`

- [ ] **Step 1: Write the migration SQL**

Create `grid/scripts/migrations/20260411_contracts_infrastructure.sql` with exactly this content:

```sql
-- Contracts infrastructure: audit trail + dead-letter store
-- Spec: docs/superpowers/specs/2026-04-11-information-flow-optimization-design.md
-- Phase 1 of information flow optimization.

BEGIN;

CREATE TABLE IF NOT EXISTS contracts_audit (
    id               BIGSERIAL PRIMARY KEY,
    event_id         UUID NOT NULL,
    contract_type    TEXT NOT NULL,
    producer_module  TEXT NOT NULL,
    correlation_id   UUID NOT NULL,
    emitted_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    dispatched_to    TEXT[] NOT NULL DEFAULT '{}',
    payload_hash     TEXT NOT NULL,
    schema_version   INT NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS contracts_audit_correlation
    ON contracts_audit (correlation_id);

CREATE INDEX IF NOT EXISTS contracts_audit_type_time
    ON contracts_audit (contract_type, emitted_at DESC);

CREATE INDEX IF NOT EXISTS contracts_audit_event_id
    ON contracts_audit (event_id);

CREATE TABLE IF NOT EXISTS contracts_dead_letter (
    id              BIGSERIAL PRIMARY KEY,
    event_id        UUID NOT NULL,
    contract_type   TEXT NOT NULL,
    payload         JSONB NOT NULL,
    consumer        TEXT NOT NULL,
    error_type      TEXT NOT NULL,
    error_detail    TEXT NOT NULL,
    retry_count     INT NOT NULL DEFAULT 0,
    next_retry_at   TIMESTAMPTZ,
    failed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    resolved_at     TIMESTAMPTZ,
    correlation_id  UUID
);

CREATE INDEX IF NOT EXISTS contracts_dead_letter_retry
    ON contracts_dead_letter (next_retry_at)
    WHERE resolved_at IS NULL;

CREATE INDEX IF NOT EXISTS contracts_dead_letter_type_unresolved
    ON contracts_dead_letter (contract_type, failed_at DESC)
    WHERE resolved_at IS NULL;

COMMIT;
```

- [ ] **Step 2: Write the test that applies the migration and checks the tables exist**

Create `grid/tests/contracts/__init__.py` (empty) and `grid/tests/contracts/test_migration.py`:

```python
"""Smoke test: the contracts-infrastructure migration creates both tables."""
from __future__ import annotations

from pathlib import Path

import pytest
from sqlalchemy import text


MIGRATION_PATH = (
    Path(__file__).resolve().parents[2]
    / "scripts"
    / "migrations"
    / "20260411_contracts_infrastructure.sql"
)


@pytest.mark.integration
def test_migration_creates_contracts_audit(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(MIGRATION_PATH.read_text()))
        row = conn.execute(
            text(
                "SELECT to_regclass(:name)::text AS name"
            ).bindparams(name="contracts_audit")
        ).fetchone()
        assert row is not None and row[0] == "contracts_audit"


@pytest.mark.integration
def test_migration_creates_contracts_dead_letter(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(MIGRATION_PATH.read_text()))
        row = conn.execute(
            text(
                "SELECT to_regclass(:name)::text AS name"
            ).bindparams(name="contracts_dead_letter")
        ).fetchone()
        assert row is not None and row[0] == "contracts_dead_letter"
```

- [ ] **Step 3: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_migration.py -v
```
Expected: FAIL (migration file missing, or tables not present). If Postgres is not available locally the test auto-skips via the `pg_engine` fixture — in that case re-run on a host with Postgres access before shipping.

- [ ] **Step 4: Run the test and confirm it passes after the migration file is in place**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_migration.py -v
```
Expected: 2 passed (or 2 skipped if Postgres unavailable — the CI integration-test job must run them).

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add scripts/migrations/20260411_contracts_infrastructure.sql \
        tests/contracts/__init__.py \
        tests/contracts/test_migration.py
git commit -m "feat: contracts infrastructure DB migration (audit + dead-letter tables)"
```

---

## Task 2: BaseContract + 13 Pydantic Schemas

**Files:**
- Create: `grid/contracts/__init__.py` (empty for now — populated at the end)
- Create: `grid/contracts/schemas.py`
- Test: `tests/contracts/test_schemas.py`

- [ ] **Step 1: Write the schema tests**

Create `grid/tests/contracts/test_schemas.py`:

```python
"""BaseContract + concrete schema contracts are frozen, typed, validated."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from contracts.schemas import (
    BaseContract,
    PostmortemCompleted,
    PredictionScored,
    BacktestGateVerdict,
    OptionsTradeOutcome,
    CrossReferenceAnomaly,
    LeverageRiskUpdate,
    RegimeTransition,
    SignalFired,
    HypothesisGenerated,
    ActorMaterialized,
    PullLifecycle,
    ForensicsTrace,
    InvestigationProgress,
    SignalRef,
    ALL_CONTRACTS,
)


CID = uuid4()


def _base_kwargs() -> dict:
    return {"producer_module": "test.producer", "correlation_id": CID}


def test_base_contract_auto_fields():
    class Dummy(BaseContract):
        pass

    c = Dummy(**_base_kwargs())
    assert isinstance(c.event_id, UUID)
    assert isinstance(c.timestamp, datetime)
    assert c.timestamp.tzinfo is not None
    assert c.correlation_id == CID
    assert c.schema_version == 1


def test_base_contract_is_frozen():
    class Dummy(BaseContract):
        pass

    c = Dummy(**_base_kwargs())
    with pytest.raises(ValidationError):
        c.producer_module = "mutated"


def test_base_contract_forbids_extra_fields():
    class Dummy(BaseContract):
        pass

    with pytest.raises(ValidationError):
        Dummy(**_base_kwargs(), unknown_field="x")


def test_postmortem_completed_roundtrip():
    sig = SignalRef(
        signal_id=uuid4(),
        source="congressional",
        trust_at_prediction=0.72,
        weight_at_prediction=0.35,
    )
    c = PostmortemCompleted(
        **_base_kwargs(),
        prediction_id=uuid4(),
        ticker="NVDA",
        verdict="MISS",
        realized_pnl=Decimal("-1250.00"),
        signals_used=[sig],
        root_cause="rate shock",
        contributing_signal_ids=[sig.signal_id],
    )
    assert c.verdict == "MISS"
    assert c.signals_used[0].source == "congressional"


def test_postmortem_verdict_literal_enforced():
    with pytest.raises(ValidationError):
        PostmortemCompleted(
            **_base_kwargs(),
            prediction_id=uuid4(),
            ticker="NVDA",
            verdict="BOGUS",          # not in Literal set
            realized_pnl=Decimal("0"),
            signals_used=[],
            root_cause="x",
            contributing_signal_ids=[],
        )


def test_prediction_scored_required_fields():
    c = PredictionScored(
        **_base_kwargs(),
        prediction_id=uuid4(),
        decision_id=42,
        ticker="SPY",
        verdict="HIT",
        expected_direction="UP",
        realized_direction="UP",
        confidence=0.81,
        brier_component=0.036,
        signals_used=[],
        model_weights_at_prediction={"flow_momentum": 0.4, "regime_contrarian": 0.2},
    )
    assert c.decision_id == 42


def test_backtest_gate_verdict_required_fields():
    c = BacktestGateVerdict(
        **_base_kwargs(),
        hypothesis_id=uuid4(),
        model_version_id=7,
        gate_name="shadow_to_staging",
        verdict="PASS",
        metrics={"sharpe": 1.2, "win_rate": 0.63},
        promotion_target_state="STAGING",
        operator_id="hermes",
    )
    assert c.verdict == "PASS"


def test_options_trade_outcome_required_fields():
    c = OptionsTradeOutcome(
        **_base_kwargs(),
        trade_id=101,
        ticker="TSLA",
        strategy="long_call",
        pnl=Decimal("450.00"),
        signal_mix={"pcr": 0.3, "skew": 0.2, "gamma_wall": 0.5},
        hit_levels={"target": True, "stop": False},
        duration_s=86400,
    )
    assert c.pnl == Decimal("450.00")


def test_cross_reference_anomaly_required_fields():
    c = CrossReferenceAnomaly(
        **_base_kwargs(),
        statistic="retail_sales_yoy",
        official_value=Decimal("3.2"),
        reality_proxy_value=Decimal("0.8"),
        confidence_delta=0.62,
        evidence_links=["https://sat.example/img1"],
        severity="HIGH",
    )
    assert c.severity == "HIGH"


def test_leverage_risk_update_required_fields():
    c = LeverageRiskUpdate(
        **_base_kwargs(),
        system_leverage_index=0.84,
        top_leveraged_actors=["actor:archegos", "actor:ltcm"],
        critical_threshold_breached=True,
        components={"margin_debt": 0.9, "repo": 0.7},
    )
    assert c.critical_threshold_breached is True


def test_regime_transition_required_fields():
    c = RegimeTransition(
        **_base_kwargs(),
        from_state="NEUTRAL",
        to_state="FRAGILE",
        confidence=0.77,
        triggering_features=["vix", "move"],
        transition_probability_matrix=[[0.8, 0.2], [0.3, 0.7]],
    )
    assert c.to_state == "FRAGILE"


def test_signal_fired_required_fields():
    c = SignalFired(
        **_base_kwargs(),
        signal_id=uuid4(),
        source="insider",
        signal_type="cluster_buy",
        strength=0.66,
        ticker="AMD",
        actor_hint="actor:ceo_lisa_su",
        raw_row_ids=[1001, 1002],
    )
    assert c.strength == 0.66


def test_hypothesis_generated_required_fields():
    c = HypothesisGenerated(
        **_base_kwargs(),
        hypothesis_id=uuid4(),
        statement="Dealer gamma flips short below 4500 SPX",
        layer="EXECUTION",
        feature_ids=["gex_spx", "spx_close"],
        lag_structure={"gex_spx": 1},
        predecessor_id=None,
    )
    assert c.layer == "EXECUTION"


def test_actor_materialized_required_fields():
    c = ActorMaterialized(
        **_base_kwargs(),
        actor_id="actor:powell_jerome",
        canonical_name="Jerome H. Powell",
        aliases=["J. Powell", "Chair Powell"],
        wealth_estimate=Decimal("50000000"),
        discovery_source="fed_bio",
        confidence_label="confirmed",
    )
    assert c.canonical_name == "Jerome H. Powell"


def test_pull_lifecycle_required_fields():
    c = PullLifecycle(
        **_base_kwargs(),
        puller_name="fred",
        state="COMPLETED",
        row_count=1240,
        duration_s=12.5,
        error=None,
    )
    assert c.state == "COMPLETED"


def test_pull_lifecycle_state_literal_enforced():
    with pytest.raises(ValidationError):
        PullLifecycle(
            **_base_kwargs(),
            puller_name="fred",
            state="BOGUS",
            row_count=0,
            duration_s=0,
            error=None,
        )


def test_forensics_trace_required_fields():
    c = ForensicsTrace(
        **_base_kwargs(),
        trace_id=uuid4(),
        ticker="SPY",
        window_start=datetime.now(timezone.utc),
        window_end=datetime.now(timezone.utc),
        reconstructed_sequence=[{"t": 0, "event": "block"}],
        suspected_levers=["dealer_gamma_flip"],
    )
    assert c.ticker == "SPY"


def test_investigation_progress_required_fields():
    c = InvestigationProgress(
        **_base_kwargs(),
        board_id=9,
        step=3,
        total_steps=10,
        description="fetching 13F holdings",
        partial_nodes=[],
    )
    assert c.step == 3


def test_all_contracts_registry_is_complete():
    assert len(ALL_CONTRACTS) == 13
    names = {cls.__name__ for cls in ALL_CONTRACTS}
    expected = {
        "PostmortemCompleted", "PredictionScored", "BacktestGateVerdict",
        "OptionsTradeOutcome", "CrossReferenceAnomaly", "LeverageRiskUpdate",
        "RegimeTransition", "SignalFired", "HypothesisGenerated",
        "ActorMaterialized", "PullLifecycle", "ForensicsTrace",
        "InvestigationProgress",
    }
    assert names == expected
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_schemas.py -v
```
Expected: FAIL — `contracts.schemas` module does not exist.

- [ ] **Step 3: Create the empty package marker**

Create `grid/contracts/__init__.py` with a single line:

```python
"""GRID contracts infrastructure — Phase 1."""
```

- [ ] **Step 4: Write the schemas module**

Create `grid/contracts/schemas.py`:

```python
"""Contract schemas for the GRID information-flow layer.

Every cross-module information flow in the GRID platform is a subclass of
``BaseContract``. Contracts are immutable, strictly-typed Pydantic models
with ``extra="forbid"`` — unknown fields raise at construction time so that
schema drift cannot hide behind dict-shaped payloads.
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field


# ---------- shared models ----------


class SignalRef(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    signal_id: UUID
    source: str
    trust_at_prediction: float
    weight_at_prediction: float


# ---------- base ----------


class BaseContract(BaseModel):
    """Common envelope fields for every contract.

    Subclasses add their typed payload fields. All contracts are frozen
    (attempting to mutate an instance raises ValidationError) and reject
    unknown fields at construction time.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc)
    )
    producer_module: str
    correlation_id: UUID
    schema_version: int = 1


# ---------- 13 concrete contracts ----------


class PostmortemCompleted(BaseContract):
    prediction_id: UUID
    ticker: str
    verdict: Literal["HIT", "MISS", "PARTIAL"]
    realized_pnl: Decimal
    signals_used: list[SignalRef]
    root_cause: str
    contributing_signal_ids: list[UUID]


class PredictionScored(BaseContract):
    prediction_id: UUID
    decision_id: int
    ticker: str
    verdict: Literal["HIT", "MISS", "PARTIAL"]
    expected_direction: Literal["UP", "DOWN", "FLAT"]
    realized_direction: Literal["UP", "DOWN", "FLAT"]
    confidence: float
    brier_component: float
    signals_used: list[SignalRef]
    model_weights_at_prediction: dict[str, float]


class BacktestGateVerdict(BaseContract):
    hypothesis_id: UUID
    model_version_id: int
    gate_name: str
    verdict: Literal["PASS", "FAIL"]
    metrics: dict[str, float]
    promotion_target_state: Literal[
        "CANDIDATE", "SHADOW", "STAGING", "PRODUCTION", "FLAGGED", "RETIRED"
    ]
    operator_id: str


class OptionsTradeOutcome(BaseContract):
    trade_id: int
    ticker: str
    strategy: str
    pnl: Decimal
    signal_mix: dict[str, float]
    hit_levels: dict[str, bool]
    duration_s: int


class CrossReferenceAnomaly(BaseContract):
    statistic: str
    official_value: Decimal
    reality_proxy_value: Decimal
    confidence_delta: float
    evidence_links: list[str]
    severity: Literal["LOW", "MEDIUM", "HIGH", "CRITICAL"]


class LeverageRiskUpdate(BaseContract):
    system_leverage_index: float
    top_leveraged_actors: list[str]
    critical_threshold_breached: bool
    components: dict[str, float]


class RegimeTransition(BaseContract):
    from_state: str
    to_state: str
    confidence: float
    triggering_features: list[str]
    transition_probability_matrix: list[list[float]]


class SignalFired(BaseContract):
    signal_id: UUID
    source: str
    signal_type: str
    strength: float
    ticker: str | None = None
    actor_hint: str | None = None
    raw_row_ids: list[int]


class HypothesisGenerated(BaseContract):
    hypothesis_id: UUID
    statement: str
    layer: Literal["REGIME", "TACTICAL", "EXECUTION"]
    feature_ids: list[str]
    lag_structure: dict[str, int]
    predecessor_id: UUID | None = None


class ActorMaterialized(BaseContract):
    actor_id: str
    canonical_name: str
    aliases: list[str]
    wealth_estimate: Decimal | None = None
    discovery_source: str
    confidence_label: Literal[
        "confirmed", "derived", "estimated", "rumored", "inferred"
    ]


class PullLifecycle(BaseContract):
    puller_name: str
    state: Literal["STARTED", "COMPLETED", "FAILED", "CONFLICT_DETECTED"]
    row_count: int = 0
    duration_s: float = 0.0
    error: str | None = None


class ForensicsTrace(BaseContract):
    trace_id: UUID
    ticker: str
    window_start: datetime
    window_end: datetime
    reconstructed_sequence: list[dict[str, Any]]
    suspected_levers: list[str]


class InvestigationProgress(BaseContract):
    board_id: int
    step: int
    total_steps: int
    description: str
    partial_nodes: list[dict[str, Any]]


# Registry used by dispatcher + router-integrity test.
ALL_CONTRACTS: tuple[type[BaseContract], ...] = (
    PostmortemCompleted,
    PredictionScored,
    BacktestGateVerdict,
    OptionsTradeOutcome,
    CrossReferenceAnomaly,
    LeverageRiskUpdate,
    RegimeTransition,
    SignalFired,
    HypothesisGenerated,
    ActorMaterialized,
    PullLifecycle,
    ForensicsTrace,
    InvestigationProgress,
)
```

- [ ] **Step 5: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_schemas.py -v
```
Expected: 18 passed.

- [ ] **Step 6: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/__init__.py contracts/schemas.py tests/contracts/test_schemas.py
git commit -m "feat: contracts schemas — BaseContract + 13 frozen Pydantic models"
```

---

## Task 3: Contract → Channel Mapping

**Files:**
- Create: `grid/contracts/channels.py`
- Test: `tests/contracts/test_channels.py`

Every contract type maps to a single event-bus channel name. Centralising that mapping in one module keeps dispatcher, emit, and router from each rolling their own naming convention.

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_channels.py`:

```python
from __future__ import annotations

from contracts.channels import channel_for, contract_for_channel, ALL_CHANNELS
from contracts.schemas import (
    ALL_CONTRACTS,
    PostmortemCompleted,
    PredictionScored,
    PullLifecycle,
)


def test_channel_for_postmortem():
    assert channel_for(PostmortemCompleted) == "grid_contracts_postmortem_completed"


def test_channel_for_prediction_scored():
    assert channel_for(PredictionScored) == "grid_contracts_prediction_scored"


def test_channel_for_pull_lifecycle():
    assert channel_for(PullLifecycle) == "grid_contracts_pull_lifecycle"


def test_every_contract_has_a_channel():
    for cls in ALL_CONTRACTS:
        ch = channel_for(cls)
        assert ch.startswith("grid_contracts_")
        assert ch == ch.lower()


def test_all_channels_is_complete():
    assert len(ALL_CHANNELS) == len(ALL_CONTRACTS)


def test_reverse_lookup_roundtrip():
    for cls in ALL_CONTRACTS:
        ch = channel_for(cls)
        assert contract_for_channel(ch) is cls
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_channels.py -v
```
Expected: FAIL — `contracts.channels` does not exist.

- [ ] **Step 3: Implement the channels module**

Create `grid/contracts/channels.py`:

```python
"""Contract-type → event-bus channel mapping."""
from __future__ import annotations

import re

from contracts.schemas import ALL_CONTRACTS, BaseContract


_CAMEL_SPLIT = re.compile(r"(?<!^)(?=[A-Z])")


def _to_snake(name: str) -> str:
    return _CAMEL_SPLIT.sub("_", name).lower()


def channel_for(contract_cls: type[BaseContract]) -> str:
    """Return the event-bus channel name for a contract type."""
    return f"grid_contracts_{_to_snake(contract_cls.__name__)}"


# Reverse lookup cache: channel → contract class.
_CHANNEL_TO_CONTRACT: dict[str, type[BaseContract]] = {
    channel_for(cls): cls for cls in ALL_CONTRACTS
}


def contract_for_channel(channel: str) -> type[BaseContract] | None:
    """Return the contract class for a channel, or None if unknown."""
    return _CHANNEL_TO_CONTRACT.get(channel)


ALL_CHANNELS: tuple[str, ...] = tuple(
    channel_for(cls) for cls in ALL_CONTRACTS
)
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_channels.py -v
```
Expected: 6 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/channels.py tests/contracts/test_channels.py
git commit -m "feat: contracts channel naming (grid_contracts_<snake>)"
```

---

## Task 4: Correlation ID Helpers

**Files:**
- Create: `grid/contracts/correlation.py`
- Test: `tests/contracts/test_correlation.py`

Correlation ids propagate through code via a `ContextVar`, so a puller that emits `PullLifecycle` and then calls normalization → features → inference can have every downstream contract carry the same id without threading it through every function signature.

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_correlation.py`:

```python
from __future__ import annotations

from uuid import UUID

from contracts.correlation import (
    new_correlation_id,
    get_current_correlation_id,
    correlation_scope,
)


def test_new_correlation_id_is_uuid():
    cid = new_correlation_id()
    assert isinstance(cid, UUID)


def test_get_current_returns_none_outside_scope():
    assert get_current_correlation_id() is None


def test_correlation_scope_sets_and_resets():
    outer_before = get_current_correlation_id()
    with correlation_scope() as cid:
        assert get_current_correlation_id() == cid
    assert get_current_correlation_id() == outer_before


def test_correlation_scope_accepts_explicit_id():
    fixed = new_correlation_id()
    with correlation_scope(fixed) as cid:
        assert cid == fixed
        assert get_current_correlation_id() == fixed


def test_nested_scopes_restore_parent():
    with correlation_scope() as parent:
        with correlation_scope() as child:
            assert get_current_correlation_id() == child
            assert child != parent
        assert get_current_correlation_id() == parent
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_correlation.py -v
```
Expected: FAIL — `contracts.correlation` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/correlation.py`:

```python
"""Correlation id propagation for the contracts layer.

Uses a ``ContextVar`` so that any code running under a ``correlation_scope()``
sees the same id — no need to thread an argument through every function call.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import Iterator
from uuid import UUID, uuid4


_current_cid: ContextVar[UUID | None] = ContextVar(
    "contracts_correlation_id", default=None
)


def new_correlation_id() -> UUID:
    """Return a fresh correlation id."""
    return uuid4()


def get_current_correlation_id() -> UUID | None:
    """Return the current correlation id, or None if not inside a scope."""
    return _current_cid.get()


@contextmanager
def correlation_scope(cid: UUID | None = None) -> Iterator[UUID]:
    """Bind a correlation id for the duration of the ``with`` block.

    If *cid* is None a new id is generated.
    """
    if cid is None:
        cid = new_correlation_id()
    token = _current_cid.set(cid)
    try:
        yield cid
    finally:
        _current_cid.reset(token)
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_correlation.py -v
```
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/correlation.py tests/contracts/test_correlation.py
git commit -m "feat: correlation id contextvar + scope helper"
```

---

## Task 5: Contracts-Level Conftest (shared test fixtures)

**Files:**
- Create: `grid/tests/contracts/conftest.py`

Several later tests share a fake `EventBus`. Putting it in a `conftest.py` avoids duplication.

- [ ] **Step 1: Create the conftest**

Create `grid/tests/contracts/conftest.py`:

```python
"""Shared fixtures for contracts tests."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Callable

import pytest


class FakeBus:
    """Minimal drop-in for events.bus.EventBus for tests.

    Captures emitted payloads per channel and fans out to registered
    in-process subscribers exactly like the real bus.
    """

    def __init__(self) -> None:
        self._subs: dict[str, list[Callable[[dict], None]]] = defaultdict(list)
        self.emitted: list[tuple[str, dict[str, Any]]] = []

    def subscribe(self, channel: str, cb: Callable[[dict], None]) -> None:
        self._subs[channel].append(cb)

    def emit_sync(self, channel: str, payload: dict[str, Any]):
        self.emitted.append((channel, payload))
        for cb in self._subs.get(channel, []):
            cb({"channel": channel, "payload": payload})
        return payload


@pytest.fixture
def fake_bus() -> FakeBus:
    return FakeBus()
```

- [ ] **Step 2: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add tests/contracts/conftest.py
git commit -m "test: shared FakeBus fixture for contracts tests"
```

---

## Task 6: Emit Helper + PullLifecycle Context Manager

**Files:**
- Create: `grid/contracts/emit.py`
- Test: `tests/contracts/test_emit.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_emit.py`:

```python
from __future__ import annotations

from uuid import uuid4

import pytest

from contracts import emit as emit_mod
from contracts.correlation import correlation_scope, new_correlation_id
from contracts.schemas import PullLifecycle, SignalFired


def test_emit_returns_event_id(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)

    c = PullLifecycle(
        producer_module="test",
        correlation_id=new_correlation_id(),
        puller_name="unit",
        state="COMPLETED",
        row_count=3,
        duration_s=0.1,
    )
    event_id = emit_mod.emit(c)

    assert event_id == c.event_id
    assert len(fake_bus.emitted) == 1
    channel, payload = fake_bus.emitted[0]
    assert channel == "grid_contracts_pull_lifecycle"
    assert payload["puller_name"] == "unit"


def test_emit_writes_audit_row(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    audit_calls = []

    def fake_audit(engine, contract, payload_hash):
        audit_calls.append((contract.event_id, payload_hash))

    monkeypatch.setattr(emit_mod, "_write_audit", fake_audit)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

    c = SignalFired(
        producer_module="test",
        correlation_id=new_correlation_id(),
        signal_id=uuid4(),
        source="insider",
        signal_type="cluster_buy",
        strength=0.5,
        raw_row_ids=[1],
    )
    emit_mod.emit(c)

    assert len(audit_calls) == 1
    assert audit_calls[0][0] == c.event_id
    assert isinstance(audit_calls[0][1], str)
    assert len(audit_calls[0][1]) == 64  # sha256 hex digest


def test_pull_lifecycle_emits_started_and_completed(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

    with emit_mod.pull_lifecycle("fred") as rows:
        rows["count"] = 42

    states = [p["state"] for _, p in fake_bus.emitted]
    assert states == ["STARTED", "COMPLETED"]
    assert fake_bus.emitted[1][1]["row_count"] == 42
    assert fake_bus.emitted[1][1]["duration_s"] >= 0


def test_pull_lifecycle_emits_failed_on_exception(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

    with pytest.raises(RuntimeError):
        with emit_mod.pull_lifecycle("fred"):
            raise RuntimeError("api down")

    states = [p["state"] for _, p in fake_bus.emitted]
    assert states == ["STARTED", "FAILED"]
    assert "api down" in fake_bus.emitted[1][1]["error"]


def test_emit_reuses_current_correlation_id_when_contract_unset(fake_bus, monkeypatch):
    """A contract always carries its own cid, but the pull_lifecycle helper
    should reuse the ambient scope rather than spawning a fresh one."""
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

    with correlation_scope() as parent_cid:
        with emit_mod.pull_lifecycle("fred"):
            pass

    cids = {p["correlation_id"] for _, p in fake_bus.emitted}
    assert cids == {str(parent_cid)}
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_emit.py -v
```
Expected: FAIL — `contracts.emit` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/emit.py`:

```python
"""Emit helpers for the contracts layer.

``emit(contract)`` writes the contract to ``contracts_audit`` and forwards the
serialised payload to the existing event bus. ``pull_lifecycle()`` is a
context manager that wraps puller bodies and emits STARTED / COMPLETED /
FAILED ``PullLifecycle`` contracts.
"""
from __future__ import annotations

import hashlib
import json
import time
from contextlib import contextmanager
from typing import Any, Iterator
from uuid import UUID

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from contracts.channels import channel_for
from contracts.correlation import (
    correlation_scope,
    get_current_correlation_id,
    new_correlation_id,
)
from contracts.schemas import BaseContract, PullLifecycle

# Late-bound to allow monkeypatching in tests.
from events.bus import bus  # noqa: E402


def _get_engine() -> Engine:
    """Return the shared database engine.

    Resolved lazily so that importing contracts.emit does not force the API
    engine to initialise at import time.
    """
    from api.dependencies import get_db_engine

    return get_db_engine()


def _serialise(contract: BaseContract) -> dict[str, Any]:
    """Pydantic serialisation using ``model_dump(mode='json')`` so UUID /
    Decimal / datetime are JSON-safe."""
    return contract.model_dump(mode="json")


def _payload_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def _write_audit(engine: Engine, contract: BaseContract, payload_hash: str) -> None:
    sql = text(
        """
        INSERT INTO contracts_audit (
            event_id, contract_type, producer_module,
            correlation_id, emitted_at, dispatched_to,
            payload_hash, schema_version
        ) VALUES (
            :event_id, :contract_type, :producer_module,
            :correlation_id, :emitted_at, :dispatched_to,
            :payload_hash, :schema_version
        )
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql.bindparams(
                event_id=str(contract.event_id),
                contract_type=type(contract).__name__,
                producer_module=contract.producer_module,
                correlation_id=str(contract.correlation_id),
                emitted_at=contract.timestamp,
                dispatched_to=[],
                payload_hash=payload_hash,
                schema_version=contract.schema_version,
            )
        )


def emit(contract: BaseContract) -> UUID:
    """Emit a contract.

    1. Serialise to JSON-safe dict.
    2. Compute payload_hash for idempotency detection.
    3. Write a row to ``contracts_audit``.
    4. Forward to the local event bus on the contract's channel.

    Returns the event id.
    """
    payload = _serialise(contract)
    payload_hash = _payload_hash(payload)
    channel = channel_for(type(contract))

    try:
        _write_audit(_get_engine(), contract, payload_hash)
    except Exception as exc:
        # Never let audit-write failure block the emit path — the dispatcher
        # will still attempt delivery via the bus, and the dead-letter store
        # will capture downstream failures.
        log.warning(
            "contracts.emit: audit write failed for {ct}: {e}",
            ct=type(contract).__name__, e=str(exc),
        )

    bus.emit_sync(channel, payload)
    return contract.event_id


# ---------- pull lifecycle ----------


@contextmanager
def pull_lifecycle(puller_name: str) -> Iterator[dict[str, int]]:
    """Wrap a puller block and emit STARTED / COMPLETED / FAILED contracts.

    Example::

        with pull_lifecycle("fred") as rows:
            for r in fetch_rows():
                insert(r)
                rows["count"] += 1
    """
    # Use the ambient correlation scope if present, otherwise spawn one.
    ambient = get_current_correlation_id()
    if ambient is None:
        scope_cm = correlation_scope()
    else:
        scope_cm = correlation_scope(ambient)

    with scope_cm as cid:
        started_at = time.time()
        emit(
            PullLifecycle(
                producer_module=f"ingestion.{puller_name}",
                correlation_id=cid,
                puller_name=puller_name,
                state="STARTED",
            )
        )
        rows: dict[str, int] = {"count": 0}
        try:
            yield rows
        except Exception as exc:
            emit(
                PullLifecycle(
                    producer_module=f"ingestion.{puller_name}",
                    correlation_id=cid,
                    puller_name=puller_name,
                    state="FAILED",
                    row_count=rows.get("count", 0),
                    duration_s=time.time() - started_at,
                    error=str(exc),
                )
            )
            raise
        else:
            emit(
                PullLifecycle(
                    producer_module=f"ingestion.{puller_name}",
                    correlation_id=cid,
                    puller_name=puller_name,
                    state="COMPLETED",
                    row_count=rows.get("count", 0),
                    duration_s=time.time() - started_at,
                )
            )
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_emit.py -v
```
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/emit.py tests/contracts/test_emit.py
git commit -m "feat: contracts.emit + pull_lifecycle context manager"
```

---

## Task 7: Dead-Letter Store

**Files:**
- Create: `grid/contracts/dead_letter.py`
- Test: `tests/contracts/test_dead_letter.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_dead_letter.py`:

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import text

import pytest

from contracts.dead_letter import (
    DeadLetterEntry,
    RETRY_SCHEDULE,
    mark_resolved,
    record_failure,
    pending_retries,
    schedule_next_retry,
)


@pytest.mark.integration
def test_record_failure_writes_row(pg_engine):
    _reset_tables(pg_engine)

    eid = uuid4()
    record_failure(
        pg_engine,
        event_id=eid,
        contract_type="PullLifecycle",
        payload={"puller_name": "fred", "state": "STARTED"},
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="boom",
        correlation_id=uuid4(),
    )

    with pg_engine.begin() as conn:
        row = conn.execute(
            text("SELECT event_id, retry_count, consumer FROM contracts_dead_letter")
        ).fetchone()
    assert str(row[0]) == str(eid)
    assert row[1] == 0
    assert row[2] == "contracts.handlers.alerts.on_pull_lifecycle"


@pytest.mark.integration
def test_pending_retries_returns_due_entries_only(pg_engine):
    _reset_tables(pg_engine)
    now = datetime.now(timezone.utc)
    _insert_row(pg_engine, retries=0, next_retry_at=now - timedelta(seconds=10))
    _insert_row(pg_engine, retries=0, next_retry_at=now + timedelta(hours=1))
    _insert_row(pg_engine, retries=0, next_retry_at=None)

    due = pending_retries(pg_engine, now=now)
    assert len(due) == 1


@pytest.mark.integration
def test_mark_resolved_sets_resolved_at(pg_engine):
    _reset_tables(pg_engine)
    entry_id = _insert_row(pg_engine, retries=0)

    mark_resolved(pg_engine, entry_id)

    with pg_engine.begin() as conn:
        row = conn.execute(
            text("SELECT resolved_at FROM contracts_dead_letter WHERE id = :id")
            .bindparams(id=entry_id)
        ).fetchone()
    assert row[0] is not None


def test_schedule_next_retry_first_attempt():
    now = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)
    when = schedule_next_retry(retry_count=0, now=now)
    assert when == now + timedelta(seconds=RETRY_SCHEDULE[0])


def test_schedule_next_retry_second_attempt():
    now = datetime(2026, 4, 11, 12, 0, 0, tzinfo=timezone.utc)
    when = schedule_next_retry(retry_count=1, now=now)
    assert when == now + timedelta(seconds=RETRY_SCHEDULE[1])


def test_schedule_next_retry_exhausted_returns_none():
    assert schedule_next_retry(retry_count=len(RETRY_SCHEDULE)) is None


# ---- helpers ----


def _reset_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("DELETE FROM contracts_dead_letter"))
        conn.execute(text("DELETE FROM contracts_audit"))


def _insert_row(engine, retries: int, next_retry_at=None) -> int:
    with engine.begin() as conn:
        result = conn.execute(
            text(
                """
                INSERT INTO contracts_dead_letter (
                    event_id, contract_type, payload, consumer,
                    error_type, error_detail, retry_count, next_retry_at
                ) VALUES (
                    :eid, 'PullLifecycle', '{}'::jsonb, 'h',
                    'CONSUMER_EXCEPTION', 'x', :rc, :nra
                ) RETURNING id
                """
            ).bindparams(eid=str(uuid4()), rc=retries, nra=next_retry_at)
        )
        return result.fetchone()[0]
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_dead_letter.py -v
```
Expected: FAIL — `contracts.dead_letter` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/dead_letter.py`:

```python
"""Dead-letter store for the contracts layer.

Failed handler dispatches land here and are either retried automatically on
a 1min / 10min / 1hr schedule, or replayed manually at any time via CLI or
the PWA ops dashboard.
"""
from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.engine import Engine


# Retry cadence in seconds: 1 min, 10 min, 1 hr.
RETRY_SCHEDULE: tuple[int, ...] = (60, 600, 3600)


@dataclass(frozen=True)
class DeadLetterEntry:
    id: int
    event_id: UUID
    contract_type: str
    payload: dict[str, Any]
    consumer: str
    error_type: str
    error_detail: str
    retry_count: int
    next_retry_at: datetime | None
    failed_at: datetime
    correlation_id: UUID | None


def record_failure(
    engine: Engine,
    *,
    event_id: UUID,
    contract_type: str,
    payload: dict[str, Any],
    consumer: str,
    error_type: str,
    error_detail: str,
    correlation_id: UUID | None = None,
) -> int:
    """Write a new dead-letter row and schedule its first retry."""
    now = datetime.now(timezone.utc)
    next_retry = schedule_next_retry(retry_count=0, now=now)

    sql = text(
        """
        INSERT INTO contracts_dead_letter (
            event_id, contract_type, payload, consumer,
            error_type, error_detail, retry_count, next_retry_at,
            failed_at, correlation_id
        ) VALUES (
            :event_id, :contract_type, CAST(:payload AS JSONB), :consumer,
            :error_type, :error_detail, 0, :next_retry_at,
            :failed_at, :correlation_id
        )
        RETURNING id
        """
    )
    with engine.begin() as conn:
        result = conn.execute(
            sql.bindparams(
                event_id=str(event_id),
                contract_type=contract_type,
                payload=json.dumps(payload, default=str),
                consumer=consumer,
                error_type=error_type,
                error_detail=error_detail,
                next_retry_at=next_retry,
                failed_at=now,
                correlation_id=str(correlation_id) if correlation_id else None,
            )
        )
        return int(result.fetchone()[0])


def pending_retries(
    engine: Engine, now: datetime | None = None, limit: int = 100
) -> list[DeadLetterEntry]:
    """Return unresolved entries whose ``next_retry_at`` is due."""
    now = now or datetime.now(timezone.utc)
    sql = text(
        """
        SELECT id, event_id, contract_type, payload, consumer,
               error_type, error_detail, retry_count, next_retry_at,
               failed_at, correlation_id
        FROM contracts_dead_letter
        WHERE resolved_at IS NULL
          AND next_retry_at IS NOT NULL
          AND next_retry_at <= :now
        ORDER BY next_retry_at
        LIMIT :limit
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql.bindparams(now=now, limit=limit)).fetchall()

    out: list[DeadLetterEntry] = []
    for r in rows:
        out.append(
            DeadLetterEntry(
                id=int(r[0]),
                event_id=UUID(str(r[1])),
                contract_type=str(r[2]),
                payload=r[3] if isinstance(r[3], dict) else json.loads(r[3]),
                consumer=str(r[4]),
                error_type=str(r[5]),
                error_detail=str(r[6]),
                retry_count=int(r[7]),
                next_retry_at=r[8],
                failed_at=r[9],
                correlation_id=UUID(str(r[10])) if r[10] else None,
            )
        )
    return out


def mark_resolved(engine: Engine, entry_id: int) -> None:
    """Mark a dead-letter entry as resolved (after successful retry/replay)."""
    sql = text(
        """
        UPDATE contracts_dead_letter
        SET resolved_at = NOW()
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(sql.bindparams(id=entry_id))


def bump_retry(engine: Engine, entry_id: int, retry_count: int) -> None:
    """Record a failed retry and schedule the next one (or give up)."""
    next_retry = schedule_next_retry(retry_count=retry_count + 1)
    sql = text(
        """
        UPDATE contracts_dead_letter
        SET retry_count = :rc, next_retry_at = :nra
        WHERE id = :id
        """
    )
    with engine.begin() as conn:
        conn.execute(
            sql.bindparams(id=entry_id, rc=retry_count + 1, nra=next_retry)
        )


def schedule_next_retry(
    retry_count: int, now: datetime | None = None
) -> datetime | None:
    """Compute when the next retry should run, or None if budget exhausted."""
    if retry_count >= len(RETRY_SCHEDULE):
        return None
    now = now or datetime.now(timezone.utc)
    return now + timedelta(seconds=RETRY_SCHEDULE[retry_count])
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_dead_letter.py -v
```
Expected: 6 passed (3 integration tests skip if Postgres unavailable).

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/dead_letter.py tests/contracts/test_dead_letter.py
git commit -m "feat: contracts dead-letter store with 1/10/60 min retry schedule"
```

---

## Task 8: Router (empty ROUTES + integrity resolver)

**Files:**
- Create: `grid/contracts/router.py`
- Test: `tests/contracts/test_router_integrity.py`

ROUTES is empty in Phase 1; Phase 2 adds real handler bindings. The resolver + integrity test ship now so Phase 2 can rely on static import verification.

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_router_integrity.py`:

```python
from __future__ import annotations

import importlib

import pytest

from contracts.router import ROUTES, resolve_handler


def test_routes_is_a_dict():
    assert isinstance(ROUTES, dict)


def test_every_handler_in_routes_is_importable():
    """Every handler path in ROUTES must resolve to a real callable.

    In Phase 1 ROUTES is empty, so this test passes trivially. In Phase 2+
    it catches typos and renames at test time instead of at dispatch time.
    """
    for contract_type, handler_paths in ROUTES.items():
        assert isinstance(handler_paths, list)
        for path in handler_paths:
            handler = resolve_handler(path)
            assert callable(handler), f"{path} is not callable"


def test_resolve_handler_imports_dotted_path():
    # Use an existing stdlib function as a stand-in.
    handler = resolve_handler("json.dumps")
    assert callable(handler)


def test_resolve_handler_raises_on_missing_module():
    with pytest.raises(ModuleNotFoundError):
        resolve_handler("nonexistent_module_xyz.func")


def test_resolve_handler_raises_on_missing_attribute():
    with pytest.raises(AttributeError):
        resolve_handler("json.not_a_real_function")
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_router_integrity.py -v
```
Expected: FAIL — `contracts.router` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/router.py`:

```python
"""Contract routing table.

ROUTES maps each contract type to the list of handler paths that should be
invoked when the contract fires. Handler paths are dotted Python imports in
the ``contracts.handlers.*`` namespace.

**Phase 1:** ROUTES is empty. Phase 2 will add the 13 contract bindings
defined in the spec.
"""
from __future__ import annotations

import importlib
from typing import Callable

from contracts.schemas import BaseContract


ROUTES: dict[type[BaseContract], list[str]] = {
    # Phase 2 additions will go here, e.g.:
    # PostmortemCompleted: [
    #     "contracts.handlers.trust.on_postmortem_completed",
    #     ...
    # ],
}


def resolve_handler(dotted_path: str) -> Callable:
    """Import a handler from a dotted path.

    Raises ModuleNotFoundError or AttributeError if the path is invalid.
    """
    module_path, _, attr = dotted_path.rpartition(".")
    if not module_path:
        raise ValueError(f"handler path must be dotted: {dotted_path!r}")
    module = importlib.import_module(module_path)
    return getattr(module, attr)
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_router_integrity.py -v
```
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/router.py tests/contracts/test_router_integrity.py
git commit -m "feat: contracts router with empty ROUTES + static handler resolver"
```

---

## Task 9: Dispatcher

**Files:**
- Create: `grid/contracts/dispatcher.py`
- Test: `tests/contracts/test_dispatcher.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_dispatcher.py`:

```python
from __future__ import annotations

from uuid import uuid4

import pytest

from contracts.dispatcher import Dispatcher
from contracts.schemas import PullLifecycle


class _Recorder:
    def __init__(self):
        self.calls: list = []

    def __call__(self, event, engine=None):
        self.calls.append(event)


def _make_contract() -> PullLifecycle:
    return PullLifecycle(
        producer_module="test",
        correlation_id=uuid4(),
        puller_name="fred",
        state="COMPLETED",
        row_count=5,
        duration_s=0.25,
    )


def test_dispatcher_routes_valid_payload_to_handler(fake_bus, monkeypatch):
    handler = _Recorder()
    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_dispatcher._installed_handler"],
    )

    # Install the recorder as the resolved module attribute.
    import sys
    fake_mod = type(sys)("tests.contracts.test_dispatcher")
    fake_mod._installed_handler = handler
    sys.modules["tests.contracts.test_dispatcher"] = fake_mod

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    contract = _make_contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", contract.model_dump(mode="json"))

    d.wait_idle()
    assert len(handler.calls) == 1
    assert handler.calls[0].puller_name == "fred"
    assert dead_letters == []


def test_dispatcher_writes_dead_letter_on_schema_violation(fake_bus):
    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    fake_bus.emit_sync(
        "grid_contracts_pull_lifecycle", {"not": "a valid payload"}
    )
    d.wait_idle()

    assert len(dead_letters) == 1
    assert dead_letters[0]["error_type"] == "SCHEMA_INVALID"


def test_dispatcher_writes_dead_letter_on_handler_exception(
    fake_bus, monkeypatch
):
    def boom(event, engine=None):
        raise RuntimeError("handler broke")

    import sys
    fake_mod = type(sys)("tests.contracts.test_dispatcher")
    fake_mod._boom = boom
    sys.modules["tests.contracts.test_dispatcher"] = fake_mod

    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_dispatcher._boom"],
    )

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    contract = _make_contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", contract.model_dump(mode="json"))
    d.wait_idle()

    assert len(dead_letters) == 1
    assert dead_letters[0]["error_type"] == "CONSUMER_EXCEPTION"
    assert "handler broke" in dead_letters[0]["error_detail"]
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_dispatcher.py -v
```
Expected: FAIL — `contracts.dispatcher` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/dispatcher.py`:

```python
"""Contract dispatcher.

Subscribes to every channel in ``contracts.router.ROUTES``, validates raw
payloads against the registered Pydantic schemas, and invokes each handler
in a bounded thread pool. Failures are written to the dead-letter store.
"""
from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor, wait
from typing import Any, Callable
from uuid import UUID

from loguru import logger as log
from pydantic import ValidationError

from contracts.channels import channel_for, contract_for_channel
from contracts.router import ROUTES, resolve_handler
from contracts.schemas import BaseContract


DeadLetterWriter = Callable[..., Any]


class Dispatcher:
    """Forwards contract events from the bus to registered handlers."""

    def __init__(
        self,
        bus,
        engine,
        dead_letter_writer: DeadLetterWriter,
        pool_size: int = 8,
    ) -> None:
        self._bus = bus
        self._engine = engine
        self._write_dead_letter = dead_letter_writer
        self._pool = ThreadPoolExecutor(max_workers=pool_size)
        self._pending: set = set()
        self._pending_lock = threading.Lock()

    # ---- lifecycle ----

    def start(self) -> None:
        """Subscribe to every channel represented in ROUTES, plus every
        contract channel (so schema-invalid payloads on unmapped channels
        still land in dead-letter)."""
        subscribed: set[str] = set()
        for contract_type in ROUTES:
            ch = channel_for(contract_type)
            self._bus.subscribe(ch, self._on_event)
            subscribed.add(ch)

        # Also subscribe to every known contract channel — Phase 1 ROUTES is
        # empty, so we still want validation to fire for any rogue payload.
        from contracts.schemas import ALL_CONTRACTS
        for cls in ALL_CONTRACTS:
            ch = channel_for(cls)
            if ch not in subscribed:
                self._bus.subscribe(ch, self._on_event)

    def wait_idle(self, timeout: float = 5.0) -> None:
        """Block until every in-flight handler has completed.

        Used by tests to assert on handler side-effects after emitting.
        """
        with self._pending_lock:
            pending = list(self._pending)
        if pending:
            wait(pending, timeout=timeout)

    # ---- event handling ----

    def _on_event(self, event) -> None:
        # Events from FakeBus are dicts; events from the real bus are
        # Event dataclass instances with a ``.payload`` attribute.
        if isinstance(event, dict):
            channel = event["channel"]
            raw_payload = event["payload"]
        else:
            channel = event.channel
            raw_payload = event.payload

        contract_cls = contract_for_channel(channel)
        if contract_cls is None:
            log.warning("dispatcher: unknown channel {ch}", ch=channel)
            return

        try:
            contract = contract_cls(**raw_payload)
        except ValidationError as e:
            self._write_dead_letter(
                event_id=_safe_uuid(raw_payload.get("event_id")),
                contract_type=contract_cls.__name__,
                payload=raw_payload,
                consumer="<schema>",
                error_type="SCHEMA_INVALID",
                error_detail=str(e),
                correlation_id=_safe_uuid(raw_payload.get("correlation_id")),
            )
            return

        for handler_path in ROUTES.get(contract_cls, []):
            self._submit(contract, handler_path)

    def _submit(self, contract: BaseContract, handler_path: str) -> None:
        fut = self._pool.submit(self._invoke, contract, handler_path)
        with self._pending_lock:
            self._pending.add(fut)
        fut.add_done_callback(self._drop_pending)

    def _drop_pending(self, fut) -> None:
        with self._pending_lock:
            self._pending.discard(fut)

    def _invoke(self, contract: BaseContract, handler_path: str) -> None:
        try:
            handler = resolve_handler(handler_path)
            handler(contract, engine=self._engine)
        except Exception as exc:
            self._write_dead_letter(
                event_id=contract.event_id,
                contract_type=type(contract).__name__,
                payload=contract.model_dump(mode="json"),
                consumer=handler_path,
                error_type="CONSUMER_EXCEPTION",
                error_detail=f"{type(exc).__name__}: {exc}",
                correlation_id=contract.correlation_id,
            )


def _safe_uuid(value) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_dispatcher.py -v
```
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/dispatcher.py tests/contracts/test_dispatcher.py
git commit -m "feat: contracts dispatcher with schema validation + dead-letter"
```

---

## Task 10: Retry Scheduler

**Files:**
- Create: `grid/contracts/retry_scheduler.py`
- Test: `tests/contracts/test_retry_scheduler.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_retry_scheduler.py`:

```python
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from contracts.dead_letter import DeadLetterEntry
from contracts.retry_scheduler import RetryScheduler


def _entry(retry_count: int = 0) -> DeadLetterEntry:
    return DeadLetterEntry(
        id=1,
        event_id=uuid4(),
        contract_type="PullLifecycle",
        payload={
            "producer_module": "t",
            "correlation_id": str(uuid4()),
            "puller_name": "fred",
            "state": "COMPLETED",
        },
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="x",
        retry_count=retry_count,
        next_retry_at=datetime.now(timezone.utc) - timedelta(seconds=1),
        failed_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
    )


def test_retry_scheduler_marks_resolved_on_success(monkeypatch):
    engine = MagicMock()
    pending = [_entry()]

    monkeypatch.setattr(
        "contracts.retry_scheduler.pending_retries",
        lambda engine, now=None: pending,
    )
    resolved: list[int] = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )
    bumped: list = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.bump_retry",
        lambda *a, **kw: bumped.append((a, kw)),
    )

    handler_mock = MagicMock()
    monkeypatch.setattr(
        "contracts.retry_scheduler.resolve_handler", lambda path: handler_mock
    )

    sched = RetryScheduler(engine=engine)
    sched.run_once()

    assert resolved == [1]
    assert bumped == []
    handler_mock.assert_called_once()


def test_retry_scheduler_bumps_on_failure(monkeypatch):
    engine = MagicMock()
    pending = [_entry(retry_count=0)]

    monkeypatch.setattr(
        "contracts.retry_scheduler.pending_retries",
        lambda engine, now=None: pending,
    )
    resolved: list[int] = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )
    bumped: list = []
    monkeypatch.setattr(
        "contracts.retry_scheduler.bump_retry",
        lambda engine, entry_id, retry_count: bumped.append(
            (entry_id, retry_count)
        ),
    )

    def broken(*args, **kwargs):
        raise RuntimeError("still broken")

    monkeypatch.setattr(
        "contracts.retry_scheduler.resolve_handler", lambda path: broken
    )

    sched = RetryScheduler(engine=engine)
    sched.run_once()

    assert resolved == []
    assert bumped == [(1, 0)]
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_retry_scheduler.py -v
```
Expected: FAIL — `contracts.retry_scheduler` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/retry_scheduler.py`:

```python
"""Background retry scheduler for dead-letter entries.

Periodically scans for due retries and re-invokes their handlers. Successful
retries are marked resolved. Failures bump the retry counter and schedule
the next attempt per ``RETRY_SCHEDULE`` in ``contracts.dead_letter``.
"""
from __future__ import annotations

import threading
import time
from typing import Any

from loguru import logger as log

from contracts.channels import contract_for_channel, channel_for
from contracts.dead_letter import (
    DeadLetterEntry,
    bump_retry,
    mark_resolved,
    pending_retries,
)
from contracts.router import resolve_handler
from contracts.schemas import ALL_CONTRACTS


_CONTRACTS_BY_NAME: dict[str, type] = {cls.__name__: cls for cls in ALL_CONTRACTS}


class RetryScheduler:
    """Runs dead-letter retries on a fixed cadence."""

    def __init__(self, engine: Any, poll_interval_s: float = 30.0) -> None:
        self._engine = engine
        self._poll_interval = poll_interval_s
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        self._thread = threading.Thread(
            target=self._loop, name="contracts-retry", daemon=True
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)

    def _loop(self) -> None:
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:
                log.warning("retry scheduler loop error: {e}", e=str(exc))
            self._stop.wait(self._poll_interval)

    def run_once(self) -> None:
        entries = pending_retries(self._engine)
        for entry in entries:
            self._attempt(entry)

    def _attempt(self, entry: DeadLetterEntry) -> None:
        contract_cls = _CONTRACTS_BY_NAME.get(entry.contract_type)
        if contract_cls is None:
            log.warning(
                "retry: unknown contract {ct} on entry {id}",
                ct=entry.contract_type, id=entry.id,
            )
            return

        try:
            contract = contract_cls(**entry.payload)
            handler = resolve_handler(entry.consumer)
            handler(contract, engine=self._engine)
        except Exception as exc:
            log.info(
                "retry failed for entry {id} (attempt {rc}): {e}",
                id=entry.id, rc=entry.retry_count, e=str(exc),
            )
            bump_retry(self._engine, entry.id, entry.retry_count)
            return

        mark_resolved(self._engine, entry.id)
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_retry_scheduler.py -v
```
Expected: 2 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/retry_scheduler.py tests/contracts/test_retry_scheduler.py
git commit -m "feat: contracts retry scheduler (auto redelivery of dead letters)"
```

---

## Task 11: Observability — counters + metrics endpoint helpers

**Files:**
- Create: `grid/contracts/observability.py`
- Test: `tests/contracts/test_observability.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_observability.py`:

```python
from __future__ import annotations

from contracts.observability import (
    emitted,
    dispatched,
    failed,
    record_duration,
    snapshot,
    render_prometheus,
    reset,
)


def setup_function(_func):
    reset()


def test_emitted_counter_increments():
    emitted("PullLifecycle")
    emitted("PullLifecycle")
    emitted("SignalFired")
    snap = snapshot()
    assert snap["emitted"]["PullLifecycle"] == 2
    assert snap["emitted"]["SignalFired"] == 1


def test_dispatched_counter_keys_by_handler():
    dispatched("PullLifecycle", "contracts.handlers.alerts.x")
    dispatched("PullLifecycle", "contracts.handlers.alerts.x")
    dispatched("PullLifecycle", "contracts.handlers.sse.y")
    snap = snapshot()
    assert snap["dispatched"][("PullLifecycle", "contracts.handlers.alerts.x")] == 2
    assert snap["dispatched"][("PullLifecycle", "contracts.handlers.sse.y")] == 1


def test_failed_counter_by_error_type():
    failed("PullLifecycle", "contracts.handlers.x", "CONSUMER_EXCEPTION")
    snap = snapshot()
    assert snap["failed"][("PullLifecycle", "contracts.handlers.x", "CONSUMER_EXCEPTION")] == 1


def test_duration_histogram_records_samples():
    record_duration("PullLifecycle", "contracts.handlers.x", 0.12)
    record_duration("PullLifecycle", "contracts.handlers.x", 0.08)
    snap = snapshot()
    assert snap["duration_count"][("PullLifecycle", "contracts.handlers.x")] == 2
    assert abs(snap["duration_sum"][("PullLifecycle", "contracts.handlers.x")] - 0.20) < 1e-9


def test_render_prometheus_produces_text_format():
    emitted("PullLifecycle")
    dispatched("PullLifecycle", "h.x")
    failed("PullLifecycle", "h.x", "BOOM")
    record_duration("PullLifecycle", "h.x", 0.05)

    body = render_prometheus()
    assert "# HELP contracts_emitted_total" in body
    assert 'contracts_emitted_total{contract="PullLifecycle"} 1' in body
    assert 'contracts_dispatched_total{contract="PullLifecycle",consumer="h.x"} 1' in body
    assert 'contracts_failed_total{contract="PullLifecycle",consumer="h.x",error="BOOM"} 1' in body
    assert "contracts_handler_duration_seconds_sum" in body
    assert "contracts_handler_duration_seconds_count" in body
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_observability.py -v
```
Expected: FAIL — `contracts.observability` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/observability.py`:

```python
"""In-process contracts metrics.

Thread-safe counters + a simple sum/count histogram rendered as Prometheus
text format. Intentionally tiny — we do not depend on prometheus_client so
that tests and dev installations stay lightweight.
"""
from __future__ import annotations

import threading
from collections import defaultdict
from typing import Any


_lock = threading.Lock()
_emitted: dict[str, int] = defaultdict(int)
_dispatched: dict[tuple[str, str], int] = defaultdict(int)
_failed: dict[tuple[str, str, str], int] = defaultdict(int)
_duration_sum: dict[tuple[str, str], float] = defaultdict(float)
_duration_count: dict[tuple[str, str], int] = defaultdict(int)


def emitted(contract_type: str) -> None:
    with _lock:
        _emitted[contract_type] += 1


def dispatched(contract_type: str, consumer: str) -> None:
    with _lock:
        _dispatched[(contract_type, consumer)] += 1


def failed(contract_type: str, consumer: str, error_type: str) -> None:
    with _lock:
        _failed[(contract_type, consumer, error_type)] += 1


def record_duration(contract_type: str, consumer: str, seconds: float) -> None:
    key = (contract_type, consumer)
    with _lock:
        _duration_sum[key] += seconds
        _duration_count[key] += 1


def snapshot() -> dict[str, Any]:
    """Return a copy of the current metric state (for tests / API)."""
    with _lock:
        return {
            "emitted": dict(_emitted),
            "dispatched": dict(_dispatched),
            "failed": dict(_failed),
            "duration_sum": dict(_duration_sum),
            "duration_count": dict(_duration_count),
        }


def reset() -> None:
    """Clear all metrics. Test-only."""
    with _lock:
        _emitted.clear()
        _dispatched.clear()
        _failed.clear()
        _duration_sum.clear()
        _duration_count.clear()


def render_prometheus() -> str:
    """Render metrics as Prometheus text format."""
    with _lock:
        em = dict(_emitted)
        dp = dict(_dispatched)
        fl = dict(_failed)
        dsum = dict(_duration_sum)
        dcount = dict(_duration_count)

    lines: list[str] = []

    lines.append("# HELP contracts_emitted_total Number of contracts emitted.")
    lines.append("# TYPE contracts_emitted_total counter")
    for ct, n in sorted(em.items()):
        lines.append(f'contracts_emitted_total{{contract="{ct}"}} {n}')

    lines.append("# HELP contracts_dispatched_total Number of handler dispatches.")
    lines.append("# TYPE contracts_dispatched_total counter")
    for (ct, consumer), n in sorted(dp.items()):
        lines.append(
            f'contracts_dispatched_total{{contract="{ct}",consumer="{consumer}"}} {n}'
        )

    lines.append("# HELP contracts_failed_total Number of handler failures.")
    lines.append("# TYPE contracts_failed_total counter")
    for (ct, consumer, err), n in sorted(fl.items()):
        lines.append(
            f'contracts_failed_total{{contract="{ct}",consumer="{consumer}",error="{err}"}} {n}'
        )

    lines.append("# HELP contracts_handler_duration_seconds Handler latency.")
    lines.append("# TYPE contracts_handler_duration_seconds summary")
    for (ct, consumer), s in sorted(dsum.items()):
        c = dcount.get((ct, consumer), 0)
        lines.append(
            f'contracts_handler_duration_seconds_sum{{contract="{ct}",consumer="{consumer}"}} {s}'
        )
        lines.append(
            f'contracts_handler_duration_seconds_count{{contract="{ct}",consumer="{consumer}"}} {c}'
        )

    return "\n".join(lines) + "\n"
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_observability.py -v
```
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/observability.py tests/contracts/test_observability.py
git commit -m "feat: contracts observability (counters + prometheus text export)"
```

---

## Task 12: Wire Observability Into Dispatcher & Emit

**Files:**
- Modify: `grid/contracts/emit.py` — call `observability.emitted()` after bus fan-out
- Modify: `grid/contracts/dispatcher.py` — call `dispatched`, `failed`, `record_duration`
- Test: `tests/contracts/test_observability_wiring.py`

- [ ] **Step 1: Write the integration test**

Create `grid/tests/contracts/test_observability_wiring.py`:

```python
from __future__ import annotations

import sys
from uuid import uuid4

import pytest

from contracts import emit as emit_mod
from contracts import observability as obs
from contracts.dispatcher import Dispatcher
from contracts.schemas import PullLifecycle


def setup_function(_):
    obs.reset()


def _contract() -> PullLifecycle:
    return PullLifecycle(
        producer_module="test",
        correlation_id=uuid4(),
        puller_name="fred",
        state="COMPLETED",
    )


def test_emit_increments_emitted_counter(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

    emit_mod.emit(_contract())
    snap = obs.snapshot()
    assert snap["emitted"]["PullLifecycle"] == 1


def test_dispatcher_records_dispatched_on_success(fake_bus, monkeypatch):
    def handler(event, engine=None):
        pass

    fake_mod = type(sys)("tests.contracts.test_observability_wiring")
    fake_mod._ok_handler = handler
    sys.modules["tests.contracts.test_observability_wiring"] = fake_mod

    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_observability_wiring._ok_handler"],
    )

    d = Dispatcher(
        bus=fake_bus, engine=None, dead_letter_writer=lambda **kw: None
    )
    d.start()
    c = _contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", c.model_dump(mode="json"))
    d.wait_idle()

    snap = obs.snapshot()
    key = ("PullLifecycle", "tests.contracts.test_observability_wiring._ok_handler")
    assert snap["dispatched"][key] == 1
    assert snap["duration_count"][key] == 1


def test_dispatcher_records_failed_on_handler_exception(fake_bus, monkeypatch):
    def handler(event, engine=None):
        raise RuntimeError("no")

    fake_mod = type(sys)("tests.contracts.test_observability_wiring")
    fake_mod._broken = handler
    sys.modules["tests.contracts.test_observability_wiring"] = fake_mod

    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_observability_wiring._broken"],
    )

    d = Dispatcher(
        bus=fake_bus, engine=None, dead_letter_writer=lambda **kw: None
    )
    d.start()
    c = _contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", c.model_dump(mode="json"))
    d.wait_idle()

    snap = obs.snapshot()
    failing = [k for k in snap["failed"] if k[2] == "CONSUMER_EXCEPTION"]
    assert len(failing) == 1
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_observability_wiring.py -v
```
Expected: FAIL — observability is not yet wired into emit/dispatcher.

- [ ] **Step 3: Patch `contracts/emit.py`**

In `grid/contracts/emit.py`, at the top of the file add:

```python
from contracts import observability as _obs
```

and at the bottom of `emit()` (after `bus.emit_sync(...)`, before the `return`):

```python
    _obs.emitted(type(contract).__name__)
    return contract.event_id
```

(The return line already exists — add the `_obs.emitted()` call immediately above it.)

- [ ] **Step 4: Patch `contracts/dispatcher.py`**

In `grid/contracts/dispatcher.py`, add at the top of the file:

```python
import time

from contracts import observability as _obs
```

Replace the body of `_invoke()` with:

```python
    def _invoke(self, contract: BaseContract, handler_path: str) -> None:
        started = time.time()
        try:
            handler = resolve_handler(handler_path)
            handler(contract, engine=self._engine)
        except Exception as exc:
            _obs.failed(
                type(contract).__name__, handler_path, "CONSUMER_EXCEPTION"
            )
            self._write_dead_letter(
                event_id=contract.event_id,
                contract_type=type(contract).__name__,
                payload=contract.model_dump(mode="json"),
                consumer=handler_path,
                error_type="CONSUMER_EXCEPTION",
                error_detail=f"{type(exc).__name__}: {exc}",
                correlation_id=contract.correlation_id,
            )
            return

        duration = time.time() - started
        _obs.dispatched(type(contract).__name__, handler_path)
        _obs.record_duration(type(contract).__name__, handler_path, duration)
```

Also add a `SCHEMA_INVALID` failure metric in `_on_event()` after the validation exception: immediately before the `return` in the `except ValidationError` block, insert:

```python
            _obs.failed(contract_cls.__name__, "<schema>", "SCHEMA_INVALID")
```

- [ ] **Step 5: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_observability_wiring.py tests/contracts/test_dispatcher.py tests/contracts/test_emit.py -v
```
Expected: all passed (observability wiring + prior dispatcher + emit tests still green).

- [ ] **Step 6: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/emit.py contracts/dispatcher.py tests/contracts/test_observability_wiring.py
git commit -m "feat: wire observability into contracts emit + dispatcher"
```

---

## Task 13: Replay (manual) + CLI

**Files:**
- Create: `grid/contracts/replay.py`
- Test: `tests/contracts/test_replay_cli.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_replay_cli.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from contracts.dead_letter import DeadLetterEntry
from contracts.replay import replay_entry, replay_many, build_parser


def _entry(entry_id: int = 1, ct: str = "PullLifecycle") -> DeadLetterEntry:
    return DeadLetterEntry(
        id=entry_id,
        event_id=uuid4(),
        contract_type=ct,
        payload={
            "producer_module": "t",
            "correlation_id": str(uuid4()),
            "puller_name": "fred",
            "state": "COMPLETED",
        },
        consumer="contracts.handlers.alerts.on_pull_lifecycle",
        error_type="CONSUMER_EXCEPTION",
        error_detail="x",
        retry_count=1,
        next_retry_at=None,
        failed_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
    )


def test_replay_entry_marks_resolved_on_success(monkeypatch):
    engine = MagicMock()
    ok = MagicMock()
    monkeypatch.setattr("contracts.replay.resolve_handler", lambda p: ok)
    resolved: list = []
    monkeypatch.setattr(
        "contracts.replay.mark_resolved",
        lambda engine, entry_id: resolved.append(entry_id),
    )

    result = replay_entry(engine, _entry(entry_id=7))
    assert result is True
    assert resolved == [7]
    ok.assert_called_once()


def test_replay_entry_returns_false_on_failure(monkeypatch):
    engine = MagicMock()

    def broken(*a, **k):
        raise RuntimeError("still broken")

    monkeypatch.setattr("contracts.replay.resolve_handler", lambda p: broken)
    monkeypatch.setattr(
        "contracts.replay.mark_resolved", lambda *a, **k: pytest.fail("should not mark")
    )
    result = replay_entry(engine, _entry())
    assert result is False


def test_replay_many_counts_successes_and_failures(monkeypatch):
    engine = MagicMock()

    outcomes = iter([True, False, True])
    monkeypatch.setattr(
        "contracts.replay.replay_entry",
        lambda engine, entry: next(outcomes),
    )
    report = replay_many(engine, [_entry(1), _entry(2), _entry(3)])
    assert report == {"success": 2, "failed": 1}


def test_cli_parser_accepts_flags():
    parser = build_parser()
    args = parser.parse_args(["--contract", "PullLifecycle", "--limit", "10"])
    assert args.contract == "PullLifecycle"
    assert args.limit == 10
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_replay_cli.py -v
```
Expected: FAIL — `contracts.replay` does not exist.

- [ ] **Step 3: Implement the module**

Create `grid/contracts/replay.py`:

```python
"""Manual replay for dead-letter entries.

Usage (CLI)::

    python -m contracts.replay                       # replay all due entries
    python -m contracts.replay --contract PullLifecycle
    python -m contracts.replay --limit 10
    python -m contracts.replay --entry 42            # replay a single entry by id

API usage: ``replay_entry(engine, entry)`` is what the FastAPI replay
endpoint calls.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass
from typing import Iterable

from loguru import logger as log
from sqlalchemy import text

from contracts.dead_letter import DeadLetterEntry, mark_resolved, pending_retries
from contracts.router import resolve_handler
from contracts.schemas import ALL_CONTRACTS


_CONTRACTS_BY_NAME: dict[str, type] = {cls.__name__: cls for cls in ALL_CONTRACTS}


def replay_entry(engine, entry: DeadLetterEntry) -> bool:
    """Re-run a single dead-letter entry. Returns True on success."""
    contract_cls = _CONTRACTS_BY_NAME.get(entry.contract_type)
    if contract_cls is None:
        log.warning("replay: unknown contract {ct}", ct=entry.contract_type)
        return False

    try:
        contract = contract_cls(**entry.payload)
        handler = resolve_handler(entry.consumer)
        handler(contract, engine=engine)
    except Exception as exc:
        log.info(
            "replay failed for entry {id}: {e}", id=entry.id, e=str(exc)
        )
        return False

    mark_resolved(engine, entry.id)
    return True


def replay_many(engine, entries: Iterable[DeadLetterEntry]) -> dict[str, int]:
    success = 0
    failed = 0
    for entry in entries:
        if replay_entry(engine, entry):
            success += 1
        else:
            failed += 1
    return {"success": success, "failed": failed}


def replay_filtered(
    engine, contract_type: str | None = None, limit: int = 100
) -> dict[str, int]:
    entries = _load_filtered(engine, contract_type=contract_type, limit=limit)
    return replay_many(engine, entries)


def _load_filtered(
    engine, contract_type: str | None, limit: int
) -> list[DeadLetterEntry]:
    sql = text(
        """
        SELECT id, event_id, contract_type, payload, consumer,
               error_type, error_detail, retry_count, next_retry_at,
               failed_at, correlation_id
        FROM contracts_dead_letter
        WHERE resolved_at IS NULL
          AND (:contract_type IS NULL OR contract_type = :contract_type)
        ORDER BY failed_at DESC
        LIMIT :limit
        """
    )
    import json as _json
    from uuid import UUID
    out: list[DeadLetterEntry] = []
    with engine.begin() as conn:
        rows = conn.execute(
            sql.bindparams(contract_type=contract_type, limit=limit)
        ).fetchall()
    for r in rows:
        out.append(
            DeadLetterEntry(
                id=int(r[0]),
                event_id=UUID(str(r[1])),
                contract_type=str(r[2]),
                payload=r[3] if isinstance(r[3], dict) else _json.loads(r[3]),
                consumer=str(r[4]),
                error_type=str(r[5]),
                error_detail=str(r[6]),
                retry_count=int(r[7]),
                next_retry_at=r[8],
                failed_at=r[9],
                correlation_id=UUID(str(r[10])) if r[10] else None,
            )
        )
    return out


# ---------- CLI ----------


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="contracts.replay",
        description="Replay dead-letter contract entries.",
    )
    p.add_argument("--contract", type=str, default=None)
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--entry", type=int, default=None, help="single entry id")
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from api.dependencies import get_db_engine
    engine = get_db_engine()

    if args.entry is not None:
        entries = _load_filtered(engine, contract_type=None, limit=1000)
        match = [e for e in entries if e.id == args.entry]
        report = replay_many(engine, match)
    else:
        report = replay_filtered(engine, args.contract, args.limit)
    print(f"replay complete: {report}")
    return 0 if report["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_replay_cli.py -v
```
Expected: 4 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/replay.py tests/contracts/test_replay_cli.py
git commit -m "feat: contracts replay module + CLI entry point"
```

---

## Task 14: API Router — metrics, lineage, dead-letter replay

**Files:**
- Create: `grid/api/routers/contracts.py`
- Test: `tests/contracts/test_api_contracts.py`

- [ ] **Step 1: Write the test**

Create `grid/tests/contracts/test_api_contracts.py`:

```python
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from contracts import observability as obs
from api.routers.contracts import router


def _client(monkeypatch) -> TestClient:
    # Bypass auth for the test.
    from api import auth
    monkeypatch.setattr(auth, "require_auth", lambda *a, **k: {"role": "admin"})

    app = FastAPI()
    app.include_router(router, prefix="/api/v1/contracts")
    return TestClient(app)


def setup_function(_):
    obs.reset()


def test_metrics_endpoint_returns_prometheus_text(monkeypatch):
    obs.emitted("PullLifecycle")
    client = _client(monkeypatch)

    r = client.get("/api/v1/contracts/metrics")
    assert r.status_code == 200
    assert r.headers["content-type"].startswith("text/plain")
    assert "contracts_emitted_total" in r.text


def test_lineage_endpoint_returns_empty_for_unknown_correlation(monkeypatch):
    engine = MagicMock()
    engine.begin.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = []

    from api.routers import contracts as api_mod
    monkeypatch.setattr(api_mod, "get_db_engine", lambda: engine)

    client = _client(monkeypatch)
    r = client.get(f"/api/v1/contracts/lineage/{uuid4()}")
    assert r.status_code == 200
    assert r.json() == {"events": []}


def test_dead_letter_replay_endpoint_returns_success(monkeypatch):
    from api.routers import contracts as api_mod

    engine = MagicMock()
    monkeypatch.setattr(api_mod, "get_db_engine", lambda: engine)

    def fake_load(engine, contract_type, limit):
        from contracts.dead_letter import DeadLetterEntry
        return [
            DeadLetterEntry(
                id=1,
                event_id=uuid4(),
                contract_type="PullLifecycle",
                payload={
                    "producer_module": "t",
                    "correlation_id": str(uuid4()),
                    "puller_name": "fred",
                    "state": "COMPLETED",
                },
                consumer="x",
                error_type="CONSUMER_EXCEPTION",
                error_detail="x",
                retry_count=0,
                next_retry_at=None,
                failed_at=datetime.now(timezone.utc),
                correlation_id=None,
            )
        ]

    monkeypatch.setattr(api_mod, "_load_filtered", fake_load)
    monkeypatch.setattr(api_mod, "replay_entry", lambda engine, entry: True)

    client = _client(monkeypatch)
    r = client.post("/api/v1/contracts/dead-letter/1/replay")
    assert r.status_code == 200
    assert r.json() == {"success": 1, "failed": 0}
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_api_contracts.py -v
```
Expected: FAIL — `api.routers.contracts` does not exist.

- [ ] **Step 3: Implement the router**

Create `grid/api/routers/contracts.py`:

```python
"""FastAPI router for contracts infrastructure endpoints.

Endpoints:
    GET  /api/v1/contracts/metrics
    GET  /api/v1/contracts/lineage/{correlation_id}
    POST /api/v1/contracts/dead-letter/{entry_id}/replay
"""
from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from contracts import observability as obs
from contracts.replay import _load_filtered, replay_entry


router = APIRouter(tags=["contracts"])


@router.get("/metrics")
def contracts_metrics(user=Depends(require_auth)) -> Response:
    """Prometheus text-format metrics for the contracts layer."""
    return Response(
        content=obs.render_prometheus(),
        media_type="text/plain; version=0.0.4; charset=utf-8",
    )


@router.get("/lineage/{correlation_id}")
def contracts_lineage(correlation_id: UUID, user=Depends(require_auth)) -> dict:
    """Full emission history for a given correlation id."""
    engine = get_db_engine()
    sql = text(
        """
        SELECT event_id, contract_type, producer_module,
               emitted_at, dispatched_to, payload_hash
        FROM contracts_audit
        WHERE correlation_id = :cid
        ORDER BY emitted_at
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(sql.bindparams(cid=str(correlation_id))).fetchall()
    return {
        "events": [
            {
                "event_id": str(r[0]),
                "contract_type": r[1],
                "producer_module": r[2],
                "emitted_at": r[3].isoformat() if r[3] else None,
                "dispatched_to": list(r[4]) if r[4] else [],
                "payload_hash": r[5],
            }
            for r in rows
        ]
    }


@router.post("/dead-letter/{entry_id}/replay")
def contracts_dead_letter_replay(
    entry_id: int, user=Depends(require_auth)
) -> dict:
    """Manually replay a single dead-letter entry."""
    engine = get_db_engine()
    entries = _load_filtered(engine, contract_type=None, limit=1000)
    match = [e for e in entries if e.id == entry_id]
    if not match:
        raise HTTPException(
            status_code=404, detail=f"dead-letter entry {entry_id} not found"
        )
    ok = replay_entry(engine, match[0])
    return {"success": 1 if ok else 0, "failed": 0 if ok else 1}
```

- [ ] **Step 4: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_api_contracts.py -v
```
Expected: 3 passed.

- [ ] **Step 5: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add api/routers/contracts.py tests/contracts/test_api_contracts.py
git commit -m "feat: /api/v1/contracts router (metrics, lineage, replay)"
```

---

## Task 15: Package Public API + Wire Into `api/main.py`

**Files:**
- Modify: `grid/contracts/__init__.py` — re-export public surface
- Create: `grid/contracts/handlers/__init__.py` — empty package marker for Phase 2
- Modify: `grid/api/main.py` — register router + start dispatcher at startup
- Test: `tests/contracts/test_public_api.py`

- [ ] **Step 1: Write the test for the package public API**

Create `grid/tests/contracts/test_public_api.py`:

```python
from __future__ import annotations


def test_public_exports():
    import contracts
    assert hasattr(contracts, "emit")
    assert hasattr(contracts, "pull_lifecycle")
    assert hasattr(contracts, "Dispatcher")
    assert hasattr(contracts, "new_correlation_id")
    assert hasattr(contracts, "correlation_scope")


def test_handlers_package_importable():
    import contracts.handlers  # noqa: F401
```

- [ ] **Step 2: Run the test and confirm it fails**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_public_api.py -v
```
Expected: FAIL — `contracts.__init__` has no public exports.

- [ ] **Step 3: Populate `contracts/__init__.py`**

Replace `grid/contracts/__init__.py` with:

```python
"""GRID contracts infrastructure.

Public surface used by producers, dispatchers, and tests.
"""
from __future__ import annotations

from contracts.correlation import (
    correlation_scope,
    get_current_correlation_id,
    new_correlation_id,
)
from contracts.dispatcher import Dispatcher
from contracts.emit import emit, pull_lifecycle

__all__ = [
    "emit",
    "pull_lifecycle",
    "Dispatcher",
    "correlation_scope",
    "get_current_correlation_id",
    "new_correlation_id",
]
```

- [ ] **Step 4: Create the handlers package**

Create `grid/contracts/handlers/__init__.py` with a single line:

```python
"""Phase 2 contract handlers — empty in Phase 1."""
```

- [ ] **Step 5: Run the test and confirm it passes**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/test_public_api.py -v
```
Expected: 2 passed.

- [ ] **Step 6: Wire the router + dispatcher into `api/main.py`**

Open `grid/api/main.py` and make these three edits:

**Edit 1 — add imports near the other router imports** (search for `from api.routers import` and add to the list):

```python
from api.routers import contracts as contracts_router
```

**Edit 2 — include the router next to the other `app.include_router(...)` calls:**

```python
app.include_router(
    contracts_router.router, prefix="/api/v1/contracts", tags=["contracts"]
)
```

**Edit 3 — start the dispatcher + retry scheduler in the existing startup hook.** `api/main.py` uses either a classic `@app.on_event("startup")` handler or a `lifespan` context manager (check which with `grep -n 'on_event\|lifespan' api/main.py` before editing). Add this block at the end of the startup function body (or before the `yield` in the lifespan context manager), keeping everything else unchanged:

```python
    # --- contracts dispatcher + retry scheduler ---
    from contracts.dispatcher import Dispatcher
    from contracts.retry_scheduler import RetryScheduler
    from contracts.dead_letter import record_failure
    from events.bus import bus as _bus
    from api.dependencies import get_db_engine as _get_engine

    _engine = _get_engine()

    def _dead_letter_writer(**kwargs):
        record_failure(_engine, **kwargs)

    app.state.contracts_dispatcher = Dispatcher(
        bus=_bus,
        engine=_engine,
        dead_letter_writer=_dead_letter_writer,
    )
    app.state.contracts_dispatcher.start()

    app.state.contracts_retry = RetryScheduler(engine=_engine)
    app.state.contracts_retry.start()
    log.info("contracts dispatcher + retry scheduler started")
```

And in the corresponding shutdown hook (`@app.on_event("shutdown")`), add:

```python
    if hasattr(app.state, "contracts_retry"):
        app.state.contracts_retry.stop()
```

- [ ] **Step 7: Smoke-run the full contracts test suite**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/ -v
```
Expected: every test in `tests/contracts/` passes. Integration tests (tagged `@pytest.mark.integration`) skip automatically if Postgres is unavailable.

- [ ] **Step 8: Smoke-import the FastAPI app**

```bash
cd /Users/anikdang/dev/GRID && python -c "from api.main import app; print(sorted(r.path for r in app.routes if '/contracts' in getattr(r, 'path', '')))"
```
Expected output (one line):
```
['/api/v1/contracts/dead-letter/{entry_id}/replay', '/api/v1/contracts/lineage/{correlation_id}', '/api/v1/contracts/metrics']
```

- [ ] **Step 9: Commit**

```bash
cd /Users/anikdang/dev/GRID
git add contracts/__init__.py contracts/handlers/__init__.py \
        api/main.py tests/contracts/test_public_api.py
git commit -m "feat: contracts package public API + wire dispatcher into api.main startup"
```

---

## Task 16: Phase 1 Acceptance — Full-Suite Sanity Check

- [ ] **Step 1: Run the entire contracts test suite**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/contracts/ -v --tb=short
```
Expected: all green. Integration tests auto-skip if Postgres is unavailable; when running against a Postgres host, make sure the migration has been applied first:

```bash
cd /Users/anikdang/dev/GRID
psql "$GRID_DATABASE_URL" -f scripts/migrations/20260411_contracts_infrastructure.sql
python -m pytest tests/contracts/ -v -m integration
```

- [ ] **Step 2: Run the full project suite to confirm no regression**

```bash
cd /Users/anikdang/dev/GRID && python -m pytest tests/ -x --ignore=tests/contracts/ -q
```
Expected: existing tests unchanged (nothing in Phase 1 touches any consumer module).

- [ ] **Step 3: Manually exercise the metrics endpoint**

Start the API:
```bash
cd /Users/anikdang/dev/GRID && python -m uvicorn api.main:app --reload --port 8000
```

In a second terminal (using the admin token from the existing auth flow):
```bash
curl -sS -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
    http://localhost:8000/api/v1/contracts/metrics
```
Expected output: Prometheus text format with zero counts for every metric (no contracts have been emitted yet — Phase 1 is infrastructure only).

- [ ] **Step 4: Manually check lineage endpoint returns empty**

```bash
curl -sS -H "Authorization: Bearer $GRID_ADMIN_TOKEN" \
    http://localhost:8000/api/v1/contracts/lineage/00000000-0000-0000-0000-000000000000
```
Expected: `{"events":[]}`.

- [ ] **Step 5: Tag the Phase 1 completion in git**

```bash
cd /Users/anikdang/dev/GRID
git tag -a contracts-phase-1 -m "contracts infrastructure phase 1 complete — 13 schemas, dispatcher, retry, observability, API"
```

Phase 1 is complete. Phase 2 will begin adding real routes to `ROUTES` and writing `contracts/handlers/*.py`.

---

## Spec Coverage (self-check)

| Spec section | Covered by |
|--------------|-----------|
| §3.1 Layer placement | Tasks 2, 8, 9 (schemas, router, dispatcher) |
| §3.2 ROUTES single source of truth | Task 8 |
| §3.3 Schemas (BaseContract + concrete) | Task 2 |
| §3.4 Dispatcher | Task 9 + observability wiring in Task 12 |
| §3.5 Correlation IDs & lineage | Tasks 4, 6, 14 (correlation module, emit propagation, lineage endpoint) |
| §3.6 Dead-letter + retry (1/10/60 min) + manual replay | Tasks 7, 10, 13, 14 |
| §3.7 Observability (counters, prometheus) | Tasks 11, 12, 14 |
| §3.8 Audit table | Tasks 1, 6 (migration + audit writes) |
| §4.11 PullLifecycle context manager | Task 6 |
| §4.14 events/sse.py broadcast | **Deferred to Phase 2** — no handlers in Phase 1 need it yet |
| §4b glue inventory | **Deferred to Phase 2** — no consumer modules touched in Phase 1 |
| §5 new API endpoints | Phase 1 ships only contracts infra endpoints (metrics/lineage/replay); the 7 intel endpoints land in Phase 4 |
| §6 new SSE channels | **Deferred to Phase 2–4** — channel names exist but nothing broadcasts yet |
| §7 Phase 1 exit criterion | Task 16 |
| §8 Testing strategy (unit + integration split) | Throughout; pg_engine fixture auto-skips in CI without Postgres |
| §10 Risks (SQL safety via bindparams, 800 LOC cap, zero-coverage glue) | Every SQL site uses `text().bindparams()`; no handler file exists yet |

All Phase 1 spec items are covered. Phase 2+ items are deferred by design and explicitly called out as "Deferred" above.
