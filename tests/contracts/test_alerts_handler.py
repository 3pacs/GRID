"""Focused tests for contracts.handlers.alerts."""
from __future__ import annotations

import sys
import types
from decimal import Decimal
from uuid import UUID

import pytest

from contracts.handlers import alerts as alerts_handler
from contracts.schemas import CrossReferenceAnomaly, RegimeTransition


class _Result:
    def __init__(self, rowcount: int) -> None:
        self.rowcount = rowcount


class _RecordingConnection:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict | None]] = []
        self._alerted_event_ids: set[str] = set()

    def execute(self, statement, params=None):
        sql = str(statement)
        self.calls.append((sql, params))
        if "INSERT INTO contract_alert_log" not in sql:
            return _Result(0)

        event_id = params["eid"]
        if event_id in self._alerted_event_ids:
            return _Result(0)
        self._alerted_event_ids.add(event_id)
        return _Result(1)


class _RecordingTransaction:
    def __init__(self, conn: _RecordingConnection) -> None:
        self.conn = conn

    def __enter__(self) -> _RecordingConnection:
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _RecordingEngine:
    def __init__(self) -> None:
        self.conn = _RecordingConnection()

    @property
    def calls(self) -> list[tuple[str, dict | None]]:
        return self.conn.calls

    def begin(self) -> _RecordingTransaction:
        return _RecordingTransaction(self.conn)


def _install_fake_alert_module(monkeypatch, module_name: str, **attrs):
    package = types.ModuleType("alerts")
    package.__path__ = []
    module = types.ModuleType(module_name)
    for key, value in attrs.items():
        setattr(module, key, value)

    monkeypatch.setitem(sys.modules, "alerts", package)
    monkeypatch.setitem(sys.modules, module_name, module)
    setattr(package, module_name.rsplit(".", 1)[1], module)
    return module


def _cross_reference_event(severity: str = "HIGH") -> CrossReferenceAnomaly:
    return CrossReferenceAnomaly(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        producer_module="oracle.cross_reference",
        correlation_id=UUID("22222222-2222-2222-2222-222222222222"),
        statistic="cpi_yoy",
        official_value=Decimal("3.10"),
        reality_proxy_value=Decimal("4.25"),
        confidence_delta=0.42,
        evidence_links=["s3://grid/evidence/cpi-yoy.json"],
        severity=severity,
    )


def _regime_transition_event() -> RegimeTransition:
    return RegimeTransition(
        event_id=UUID("33333333-3333-3333-3333-333333333333"),
        producer_module="oracle.regime.detector",
        correlation_id=UUID("44444444-4444-4444-4444-444444444444"),
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


def _contract_alert_log_inserts(
    engine: _RecordingEngine,
) -> list[dict | None]:
    return [
        params
        for sql, params in engine.calls
        if "INSERT INTO contract_alert_log" in sql
    ]


@pytest.mark.parametrize("severity", ["LOW", "MEDIUM"])
def test_cross_reference_anomaly_below_high_skips_db_and_push(
    monkeypatch,
    severity: str,
) -> None:
    push_calls: list[dict[str, str]] = []

    def notify_red_flag(*, title: str, description: str) -> int:
        push_calls.append({"title": title, "description": description})
        return 1

    _install_fake_alert_module(
        monkeypatch,
        "alerts.push_notify",
        notify_red_flag=notify_red_flag,
    )
    engine = _RecordingEngine()

    alerts_handler.on_cross_reference_anomaly(
        _cross_reference_event(severity),
        engine=engine,
    )

    assert engine.calls == []
    assert push_calls == []


@pytest.mark.parametrize("severity", ["HIGH", "CRITICAL"])
def test_cross_reference_anomaly_pages_high_or_critical_with_late_push(
    monkeypatch,
    severity: str,
) -> None:
    push_calls: list[dict[str, str]] = []

    def notify_red_flag(*, title: str, description: str) -> int:
        push_calls.append({"title": title, "description": description})
        return 1

    _install_fake_alert_module(
        monkeypatch,
        "alerts.push_notify",
        notify_red_flag=notify_red_flag,
    )
    engine = _RecordingEngine()

    alerts_handler.on_cross_reference_anomaly(
        _cross_reference_event(severity),
        engine=engine,
    )

    assert push_calls == [
        {
            "title": f"Cross-reference anomaly: cpi_yoy ({severity})",
            "description": (
                "Statistic 'cpi_yoy' diverged from physical-reality proxy "
                "with confidence delta +0.420. "
                "See oracle_anti_signals for the full row."
            ),
        }
    ]
    assert _contract_alert_log_inserts(engine) == [
        {
            "eid": "11111111-1111-1111-1111-111111111111",
            "k": "cross_reference",
        }
    ]

    create_sql = _normalized(engine.calls[0][0])
    insert_sql = _normalized(engine.calls[1][0])
    assert "CREATE TABLE IF NOT EXISTS contract_alert_log" in create_sql
    assert "event_id UUID PRIMARY KEY" in create_sql
    assert "ON CONFLICT (event_id) DO NOTHING" in insert_sql


def test_cross_reference_anomaly_replay_dedups_before_push(
    monkeypatch,
) -> None:
    push_calls: list[dict[str, str]] = []

    def notify_red_flag(*, title: str, description: str) -> int:
        push_calls.append({"title": title, "description": description})
        return 1

    _install_fake_alert_module(
        monkeypatch,
        "alerts.push_notify",
        notify_red_flag=notify_red_flag,
    )
    engine = _RecordingEngine()
    event = _cross_reference_event("HIGH")

    alerts_handler.on_cross_reference_anomaly(event, engine=engine)
    alerts_handler.on_cross_reference_anomaly(event, engine=engine)

    assert len(push_calls) == 1
    assert _contract_alert_log_inserts(engine) == [
        {
            "eid": "11111111-1111-1111-1111-111111111111",
            "k": "cross_reference",
        },
        {
            "eid": "11111111-1111-1111-1111-111111111111",
            "k": "cross_reference",
        },
    ]


def test_regime_transition_pages_with_late_email_and_dedups(
    monkeypatch,
) -> None:
    email_calls: list[dict[str, object]] = []

    def alert_on_regime_change(
        *,
        from_regime: str,
        to_regime: str,
        confidence: float,
    ) -> None:
        email_calls.append(
            {
                "from_regime": from_regime,
                "to_regime": to_regime,
                "confidence": confidence,
            }
        )

    _install_fake_alert_module(
        monkeypatch,
        "alerts.email",
        alert_on_regime_change=alert_on_regime_change,
    )
    engine = _RecordingEngine()
    event = _regime_transition_event()

    alerts_handler.on_regime_transition(event, engine=engine)
    alerts_handler.on_regime_transition(event, engine=engine)

    assert email_calls == [
        {
            "from_regime": "TIGHTENING",
            "to_regime": "EXPANSION",
            "confidence": 0.875,
        }
    ]
    assert _contract_alert_log_inserts(engine) == [
        {
            "eid": "33333333-3333-3333-3333-333333333333",
            "k": "regime_transition",
        },
        {
            "eid": "33333333-3333-3333-3333-333333333333",
            "k": "regime_transition",
        },
    ]
