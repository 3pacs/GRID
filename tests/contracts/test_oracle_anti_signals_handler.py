"""Focused tests for contracts.handlers.oracle_anti_signals.on_cross_reference_anomaly.

The §7.3 ``CrossReferenceAnomaly`` route fully depends on this handler to
persist anomaly rows into ``oracle_anti_signals`` so the next oracle
scoring cycle can pick them up via ``oracle.engine._find_anti_signals``.

Coverage: severity gating (LOW skip / MEDIUM+ persist / unknown skip),
table auto-create, ON CONFLICT idempotency, JSON encoding of
``evidence_links``, ``correlation_id`` None-handling, numeric coercion,
and the non-fatal exception swallow contract (PUNCH-LIST-2026-05-13
contracts/ [P1] line 89).
"""
from __future__ import annotations

import json
from decimal import Decimal
from uuid import UUID

import pytest
from loguru import logger as log

from contracts.handlers.oracle_anti_signals import on_cross_reference_anomaly
from contracts.schemas import CrossReferenceAnomaly


# ---- engine doubles ----


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


class RaisingEngine:
    """Models a DB outage: every transaction-open raises."""

    def begin(self):
        raise RuntimeError("simulated DB outage")


# ---- helpers ----


def _normalized(sql: str) -> str:
    return " ".join(sql.split())


def _event(severity: str = "HIGH", **overrides) -> CrossReferenceAnomaly:
    base = {
        "event_id": UUID("11111111-1111-1111-1111-111111111111"),
        "producer_module": "intelligence.cross_reference",
        "correlation_id": UUID("22222222-2222-2222-2222-222222222222"),
        "statistic": "cpi_yoy",
        "official_value": Decimal("3.2"),
        "reality_proxy_value": Decimal("5.8"),
        "confidence_delta": -0.42,
        "evidence_links": ["bls.gov/cpi", "shadowstats/alt-cpi"],
        "severity": severity,
    }
    base.update(overrides)
    return CrossReferenceAnomaly(**base)


@pytest.fixture
def loguru_records():
    """Capture loguru records into a list (the handler uses loguru, not stdlib)."""
    records: list[dict] = []
    sink_id = log.add(
        lambda msg: records.append(
            {"level": msg.record["level"].name, "message": msg.record["message"]}
        ),
        level="DEBUG",
    )
    try:
        yield records
    finally:
        log.remove(sink_id)


# ---- severity gating ----


@pytest.mark.parametrize("severity", ["MEDIUM", "HIGH", "CRITICAL"])
def test_persists_when_severity_meets_floor(severity: str):
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(severity=severity), engine=engine)

    # Two writes: CREATE TABLE IF NOT EXISTS, then INSERT.
    assert len(engine.conn.calls) == 2
    _, params = engine.conn.calls[1]
    assert params["sev"] == severity


def test_low_severity_skips_db_write(loguru_records):
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(severity="LOW"), engine=engine)

    assert engine.conn.calls == []
    assert any(
        rec["level"] == "DEBUG" and "LOW severity skip" in rec["message"]
        for rec in loguru_records
    )


# ---- table auto-create + insert shape ----


def test_create_table_idempotent_and_keyed_on_event_id():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(), engine=engine)

    create_sql = _normalized(engine.conn.calls[0][0])
    assert "CREATE TABLE IF NOT EXISTS oracle_anti_signals" in create_sql
    assert "event_id UUID NOT NULL UNIQUE" in create_sql


def test_insert_uses_on_conflict_do_nothing_for_idempotency():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(), engine=engine)

    insert_sql = _normalized(engine.conn.calls[1][0])
    assert "INSERT INTO oracle_anti_signals" in insert_sql
    assert "CAST(:eid AS UUID)" in insert_sql
    assert "CAST(:cid AS UUID)" in insert_sql
    assert "CAST(:el AS JSONB)" in insert_sql
    assert "ON CONFLICT (event_id) DO NOTHING" in insert_sql


def test_insert_params_carry_event_payload():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(), engine=engine)

    _, params = engine.conn.calls[1]
    assert params["eid"] == "11111111-1111-1111-1111-111111111111"
    assert params["cid"] == "22222222-2222-2222-2222-222222222222"
    assert params["stat"] == "cpi_yoy"
    assert params["sev"] == "HIGH"
    assert params["prod"] == "intelligence.cross_reference"
    assert params["ov"] == pytest.approx(3.2)
    assert params["rv"] == pytest.approx(5.8)
    assert params["cd"] == pytest.approx(-0.42)
    # evidence_links is JSON-encoded so the DB driver can bind it to JSONB
    # via CAST(:el AS JSONB).
    assert json.loads(params["el"]) == ["bls.gov/cpi", "shadowstats/alt-cpi"]


def test_empty_evidence_links_encodes_as_empty_json_array():
    engine = RecordingEngine()

    on_cross_reference_anomaly(_event(evidence_links=[]), engine=engine)

    _, params = engine.conn.calls[1]
    assert params["el"] == "[]"
    assert json.loads(params["el"]) == []


def test_decimal_values_coerced_to_float_for_numeric_bind():
    engine = RecordingEngine()

    on_cross_reference_anomaly(
        _event(
            official_value=Decimal("1.000000001"),
            reality_proxy_value=Decimal("2.5"),
        ),
        engine=engine,
    )

    _, params = engine.conn.calls[1]
    assert isinstance(params["ov"], float)
    assert isinstance(params["rv"], float)
    assert params["ov"] == pytest.approx(1.000000001)
    assert params["rv"] == pytest.approx(2.5)


# ---- non-fatal contract ----


def test_db_failure_is_swallowed_and_logged(loguru_records):
    engine = RaisingEngine()

    # Must not propagate — contract bus has to keep flowing.
    on_cross_reference_anomaly(_event(), engine=engine)

    assert any(
        rec["level"] == "WARNING"
        and "oracle_anti_signals.on_cross_reference_anomaly" in rec["message"]
        for rec in loguru_records
    )
