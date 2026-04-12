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
