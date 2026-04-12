from __future__ import annotations

from uuid import uuid4

import pytest

from contracts import emit as emit_mod
from contracts.correlation import correlation_scope, new_correlation_id
from contracts.schemas import PullLifecycle, SignalFired


def test_emit_returns_event_id(fake_bus, monkeypatch):
    monkeypatch.setattr(emit_mod, "bus", fake_bus)
    monkeypatch.setattr(emit_mod, "_write_audit", lambda *a, **k: None)
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: object())

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
