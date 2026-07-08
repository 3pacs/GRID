"""Focused tests for contracts.handlers.journal.on_prediction_scored.

Covers the verdict→outcome mapping, Brier-style ``realized_value``, the
``outcome IS NULL`` idempotency guard, ``contracts_audit`` correlation
join, and the must-not-raise contract guarantee.
"""
from __future__ import annotations

from uuid import UUID

import pytest

from contracts.handlers.journal import on_prediction_scored
from contracts.schemas import PredictionScored, SignalRef


class RecordingResult:
    def __init__(self, rowcount: int = 0) -> None:
        self.rowcount = rowcount


class RecordingConnection:
    def __init__(self, rowcount: int = 0) -> None:
        self.calls: list[tuple[str, dict | None]] = []
        self._rowcount = rowcount

    def execute(self, statement, params=None) -> RecordingResult:
        self.calls.append((str(statement), params))
        return RecordingResult(self._rowcount)


class RecordingTransaction:
    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> RecordingConnection:
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class RecordingEngine:
    def __init__(self, rowcount: int = 0) -> None:
        self.conn = RecordingConnection(rowcount=rowcount)

    def begin(self) -> RecordingTransaction:
        return RecordingTransaction(self.conn)


class RaisingEngine:
    def begin(self):  # noqa: D401 - test double
        raise RuntimeError("db is down")


def _event(
    *,
    prediction_id: str = "11111111-1111-1111-1111-111111111111",
    verdict: str = "HIT",
    confidence: float = 0.82,
    correlation_id: str = "22222222-2222-2222-2222-222222222222",
) -> PredictionScored:
    return PredictionScored(
        event_id=UUID("33333333-3333-3333-3333-333333333333"),
        producer_module="oracle.engine",
        correlation_id=UUID(correlation_id),
        prediction_id=UUID(prediction_id),
        decision_id=42,
        ticker="XOM",
        verdict=verdict,
        expected_direction="UP",
        realized_direction="UP" if verdict == "HIT" else "DOWN",
        confidence=confidence,
        brier_component=0.10,
        signals_used=[
            SignalRef(
                signal_id=UUID("44444444-4444-4444-4444-444444444444"),
                source="oracle.regime.detector",
                trust_at_prediction=0.7,
                weight_at_prediction=1.0,
            )
        ],
        model_weights_at_prediction={"oracle.regime.detector": 1.0},
    )


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


@pytest.mark.parametrize(
    ("verdict", "expected_outcome", "expected_realised"),
    [
        ("HIT", "PROFIT", 1.0),
        ("MISS", "LOSS", 0.0),
        ("PARTIAL", "PARTIAL", 0.5),
    ],
)
def test_on_prediction_scored_maps_verdict_to_outcome_and_realised(
    verdict, expected_outcome, expected_realised
):
    engine = RecordingEngine(rowcount=1)

    on_prediction_scored(_event(verdict=verdict), engine=engine)

    assert len(engine.conn.calls) == 1
    sql, params = engine.conn.calls[0]
    assert params["oc"] == expected_outcome
    assert params["rv"] == expected_realised
    assert "UPDATE decision_journal" in _normalized(sql)


def test_on_prediction_scored_binds_correlation_id_for_audit_join():
    engine = RecordingEngine(rowcount=0)
    correlation_id = "abcdef00-0000-4000-8000-000000000000"

    on_prediction_scored(
        _event(correlation_id=correlation_id), engine=engine
    )

    sql, params = engine.conn.calls[0]
    normalized = _normalized(sql)
    assert params["cid"] == correlation_id
    assert "CAST(:cid AS UUID)" in normalized
    assert "FROM contracts_audit" in normalized
    assert "WHERE correlation_id = CAST(:cid AS UUID)" in normalized


def test_on_prediction_scored_only_touches_open_provisional_rows():
    engine = RecordingEngine(rowcount=0)

    on_prediction_scored(_event(), engine=engine)

    sql, _ = engine.conn.calls[0]
    # Idempotency guard: closed rows must never be re-stamped.
    assert "outcome IS NULL" in _normalized(sql)


def test_on_prediction_scored_empty_prediction_id_is_noop():
    engine = RecordingEngine()
    evt = _event()
    # Subvert the frozen pydantic instance just for this no-op check.
    object.__setattr__(evt, "prediction_id", "")

    on_prediction_scored(evt, engine=engine)

    assert engine.conn.calls == []


def test_on_prediction_scored_swallows_db_exceptions():
    engine = RaisingEngine()

    # Best-effort contract: handler must never raise on DB failure so the
    # contracts bus keeps flowing.
    on_prediction_scored(_event(), engine=engine)
