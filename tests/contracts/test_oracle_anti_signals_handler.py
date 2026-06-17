"""Focused tests for contracts.handlers.oracle_anti_signals."""
from __future__ import annotations

import json
from decimal import Decimal
from uuid import UUID

import pytest

from contracts.handlers.oracle_anti_signals import on_cross_reference_anomaly
from contracts.schemas import CrossReferenceAnomaly


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
        self.begin_count = 0

    def begin(self) -> RecordingTransaction:
        self.begin_count += 1
        return RecordingTransaction(self.conn)


def _event(severity: str = "HIGH") -> CrossReferenceAnomaly:
    return CrossReferenceAnomaly(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        producer_module="cross_reference.engine",
        correlation_id=UUID("22222222-2222-2222-2222-222222222222"),
        statistic="payrolls_vs_power_load",
        official_value=Decimal("1200.50"),
        reality_proxy_value=Decimal("950.25"),
        confidence_delta=0.42,
        evidence_links=[
            "s3://grid-evidence/payrolls-power-load.json",
            "https://example.test/evidence/power-load",
        ],
        severity=severity,
    )


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


@pytest.mark.parametrize("severity", ["MEDIUM", "HIGH", "CRITICAL"])
def test_on_cross_reference_anomaly_persists_medium_and_above(severity):
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(severity), engine=engine)

    assert engine.begin_count == 1
    assert len(engine.conn.calls) == 2
    _, params = engine.conn.calls[1]
    assert params["sev"] == severity


def test_on_cross_reference_anomaly_auto_creates_table():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(), engine=engine)

    create_sql = _normalized(engine.conn.calls[0][0])
    assert "CREATE TABLE IF NOT EXISTS oracle_anti_signals" in create_sql
    assert "event_id UUID NOT NULL UNIQUE" in create_sql
    assert "evidence_links JSONB" in create_sql
    assert "created_at TIMESTAMPTZ DEFAULT now()" in create_sql


def test_on_cross_reference_anomaly_insert_claims_event_id_idempotency():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(), engine=engine)

    insert_sql = _normalized(engine.conn.calls[1][0])
    _, params = engine.conn.calls[1]

    assert "INSERT INTO oracle_anti_signals" in insert_sql
    assert "CAST(:eid AS UUID)" in insert_sql
    assert "CAST(:el AS JSONB)" in insert_sql
    assert "CAST(:cid AS UUID)" in insert_sql
    assert "ON CONFLICT (event_id) DO NOTHING" in insert_sql
    assert params == {
        "eid": "11111111-1111-1111-1111-111111111111",
        "stat": "payrolls_vs_power_load",
        "sev": "HIGH",
        "ov": 1200.5,
        "rv": 950.25,
        "cd": 0.42,
        "el": json.dumps(
            [
                "s3://grid-evidence/payrolls-power-load.json",
                "https://example.test/evidence/power-load",
            ]
        ),
        "prod": "cross_reference.engine",
        "cid": "22222222-2222-2222-2222-222222222222",
    }
    assert json.loads(params["el"]) == [
        "s3://grid-evidence/payrolls-power-load.json",
        "https://example.test/evidence/power-load",
    ]


def test_on_cross_reference_anomaly_low_severity_does_not_persist():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event("LOW"), engine=engine)

    assert engine.begin_count == 0
    assert engine.conn.calls == []
