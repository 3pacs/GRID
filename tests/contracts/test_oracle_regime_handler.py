"""Focused tests for contracts.handlers.oracle_regime.on_regime_transition."""
from __future__ import annotations

import json
from uuid import UUID

from contracts.handlers.oracle_regime import on_regime_transition
from contracts.schemas import RegimeTransition


class RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))


class RecordingTransaction:
    def __init__(self, conn: RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> RecordingConnection:
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class RecordingEngine:
    def __init__(self) -> None:
        self.conn = RecordingConnection()

    def begin(self) -> RecordingTransaction:
        return RecordingTransaction(self.conn)


def _event() -> RegimeTransition:
    return RegimeTransition(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        producer_module="oracle.regime.detector",
        correlation_id=UUID("22222222-2222-2222-2222-222222222222"),
        from_state="TIGHTENING",
        to_state="EXPANSION",
        confidence=0.875,
        triggering_features=["credit_spread", "liquidity_impulse"],
        transition_probability_matrix=[
            [0.1, 0.7, 0.2],
            [0.2, 0.6, 0.2],
        ],
    )


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def test_on_regime_transition_shapes_json_payload_params():
    engine = RecordingEngine()

    on_regime_transition(_event(), engine=engine)

    assert len(engine.conn.calls) == 2
    insert_sql, params = engine.conn.calls[1]

    assert "CAST(:tf AS JSONB)" in _normalized(insert_sql)
    assert "CAST(:tpm AS JSONB)" in _normalized(insert_sql)
    assert params == {
        "eid": "11111111-1111-1111-1111-111111111111",
        "fr": "TIGHTENING",
        "to": "EXPANSION",
        "c": 0.875,
        "tf": json.dumps(["credit_spread", "liquidity_impulse"]),
        "tpm": json.dumps([[0.1, 0.7, 0.2], [0.2, 0.6, 0.2]]),
        "prod": "oracle.regime.detector",
        "cid": "22222222-2222-2222-2222-222222222222",
    }
    assert json.loads(params["tf"]) == ["credit_spread", "liquidity_impulse"]
    assert json.loads(params["tpm"]) == [[0.1, 0.7, 0.2], [0.2, 0.6, 0.2]]


def test_on_regime_transition_insert_claims_event_id_idempotency():
    engine = RecordingEngine()

    on_regime_transition(_event(), engine=engine)

    create_sql = _normalized(engine.conn.calls[0][0])
    insert_sql = _normalized(engine.conn.calls[1][0])

    assert "event_id UUID NOT NULL UNIQUE" in create_sql
    assert "INSERT INTO regime_transitions_audit" in insert_sql
    assert "CAST(:eid AS UUID)" in insert_sql
    assert "ON CONFLICT (event_id) DO NOTHING" in insert_sql
