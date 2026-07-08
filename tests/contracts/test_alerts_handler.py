"""Focused tests for contracts.handlers.alerts."""
from __future__ import annotations

import sys
from decimal import Decimal
from types import ModuleType, SimpleNamespace
from uuid import UUID, uuid4

import pytest
from loguru import logger as log

from contracts.handlers import alerts
from contracts.schemas import CrossReferenceAnomaly, RegimeTransition


@pytest.fixture
def loguru_records():
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


def _cross_reference(severity: str = "HIGH") -> CrossReferenceAnomaly:
    return CrossReferenceAnomaly(
        event_id=UUID("11111111-1111-1111-1111-111111111111"),
        producer_module="test.cross_lens",
        correlation_id=uuid4(),
        statistic="warehouse_inventory",
        official_value=Decimal("100.0"),
        reality_proxy_value=Decimal("142.0"),
        confidence_delta=0.42,
        evidence_links=["proxy:1"],
        severity=severity,
    )


def _regime_transition() -> RegimeTransition:
    return RegimeTransition(
        event_id=UUID("22222222-2222-2222-2222-222222222222"),
        producer_module="test.regime",
        correlation_id=uuid4(),
        from_state="NEUTRAL",
        to_state="CRISIS",
        confidence=0.91,
        triggering_features=["vix", "credit_spread"],
        transition_probability_matrix=[[0.2, 0.8], [0.3, 0.7]],
    )


def test_cross_reference_below_high_severity_is_silent_noop(monkeypatch):
    record_calls: list[tuple[str, str]] = []

    def record_alert(_engine, *, event_id: str, kind: str) -> bool:
        record_calls.append((event_id, kind))
        return True

    monkeypatch.setattr(alerts, "_record_alert", record_alert)

    alerts.on_cross_reference_anomaly(_cross_reference("MEDIUM"), engine=object())

    assert record_calls == []


def test_cross_reference_replay_does_not_notify(monkeypatch):
    notify_calls: list[dict] = []
    push_mod = ModuleType("alerts.push_notify")
    push_mod.notify_red_flag = lambda **kw: notify_calls.append(kw)
    monkeypatch.setitem(sys.modules, "alerts.push_notify", push_mod)
    monkeypatch.setattr(alerts, "_record_alert", lambda *a, **kw: False)

    alerts.on_cross_reference_anomaly(_cross_reference("HIGH"), engine=object())

    assert notify_calls == []


def test_cross_reference_high_severity_sends_red_flag(monkeypatch):
    record_calls: list[tuple[str, str]] = []
    notify_calls: list[dict] = []

    def record_alert(_engine, *, event_id: str, kind: str) -> bool:
        record_calls.append((event_id, kind))
        return True

    push_mod = ModuleType("alerts.push_notify")
    push_mod.notify_red_flag = lambda **kw: notify_calls.append(kw)
    monkeypatch.setitem(sys.modules, "alerts.push_notify", push_mod)
    monkeypatch.setattr(alerts, "_record_alert", record_alert)

    alerts.on_cross_reference_anomaly(_cross_reference("HIGH"), engine=object())

    assert record_calls == [
        ("11111111-1111-1111-1111-111111111111", "cross_reference")
    ]
    assert notify_calls == [
        {
            "title": "Cross-reference anomaly: warehouse_inventory (HIGH)",
            "description": (
                "Statistic 'warehouse_inventory' diverged from "
                "physical-reality proxy with confidence delta +0.420. "
                "See oracle_anti_signals for the full row."
            ),
        }
    ]


def test_cross_reference_push_failure_is_non_fatal(monkeypatch, loguru_records):
    def fail_notify(**_kw):
        raise RuntimeError("push offline")

    push_mod = ModuleType("alerts.push_notify")
    push_mod.notify_red_flag = fail_notify
    monkeypatch.setitem(sys.modules, "alerts.push_notify", push_mod)
    monkeypatch.setattr(alerts, "_record_alert", lambda *a, **kw: True)

    alerts.on_cross_reference_anomaly(_cross_reference("CRITICAL"), engine=object())

    warnings = [r["message"] for r in loguru_records if r["level"] == "WARNING"]
    assert any("push_notify failed: push offline" in msg for msg in warnings)


def test_regime_transition_without_event_id_is_silent_noop(monkeypatch):
    record_calls: list[dict] = []
    evt = SimpleNamespace(event_id="", from_state="NEUTRAL", to_state="CRISIS")

    def record_alert(*_args, **kwargs) -> bool:
        record_calls.append(kwargs)
        return True

    monkeypatch.setattr(alerts, "_record_alert", record_alert)

    alerts.on_regime_transition(evt, engine=object())

    assert record_calls == []


def test_regime_transition_replay_does_not_email(monkeypatch):
    email_calls: list[dict] = []
    email_mod = ModuleType("alerts.email")
    email_mod.alert_on_regime_change = lambda **kw: email_calls.append(kw)
    monkeypatch.setitem(sys.modules, "alerts.email", email_mod)
    monkeypatch.setattr(alerts, "_record_alert", lambda *a, **kw: False)

    alerts.on_regime_transition(_regime_transition(), engine=object())

    assert email_calls == []


def test_regime_transition_sends_email(monkeypatch):
    record_calls: list[tuple[str, str]] = []
    email_calls: list[dict] = []

    def record_alert(_engine, *, event_id: str, kind: str) -> bool:
        record_calls.append((event_id, kind))
        return True

    email_mod = ModuleType("alerts.email")
    email_mod.alert_on_regime_change = lambda **kw: email_calls.append(kw)
    monkeypatch.setitem(sys.modules, "alerts.email", email_mod)
    monkeypatch.setattr(alerts, "_record_alert", record_alert)

    alerts.on_regime_transition(_regime_transition(), engine=object())

    assert record_calls == [
        ("22222222-2222-2222-2222-222222222222", "regime_transition")
    ]
    assert email_calls == [
        {
            "from_regime": "NEUTRAL",
            "to_regime": "CRISIS",
            "confidence": 0.91,
        }
    ]


def test_regime_transition_email_failure_is_non_fatal(monkeypatch, loguru_records):
    def fail_email(**_kw):
        raise RuntimeError("smtp offline")

    email_mod = ModuleType("alerts.email")
    email_mod.alert_on_regime_change = fail_email
    monkeypatch.setitem(sys.modules, "alerts.email", email_mod)
    monkeypatch.setattr(alerts, "_record_alert", lambda *a, **kw: True)

    alerts.on_regime_transition(_regime_transition(), engine=object())

    warnings = [r["message"] for r in loguru_records if r["level"] == "WARNING"]
    assert any("email failed: smtp offline" in msg for msg in warnings)
