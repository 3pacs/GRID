"""``contracts.emit`` issues ``pg_notify`` inside the audit transaction.

This is the cross-process leg that lets contracts emitted in ``grid-hermes``
reach the API's SSE listener. The notify must be in the same transaction as
the ``contracts_audit`` insert (so it fires only on commit) and must respect
PostgreSQL's NOTIFY payload limit.
"""
from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

import importlib

# ``contracts/__init__.py`` re-exports the ``emit`` *function* as the package
# attribute ``contracts.emit``, so both ``from contracts import emit`` and
# ``import contracts.emit as m`` resolve to the function. Fetch the module
# from the import system directly so ``_get_engine`` can be monkeypatched.
emit_mod = importlib.import_module("contracts.emit")
from contracts.channels import channel_for  # noqa: E402
from contracts.schemas import SignalFired


class _Conn:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Any]] = []

    def execute(self, statement, params=None):
        self.calls.append((str(statement), params))
        return MagicMock()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _engine_with(conn: _Conn) -> MagicMock:
    engine = MagicMock()
    engine.begin.return_value = conn
    return engine


def _signal(**overrides: Any) -> SignalFired:
    base: dict[str, Any] = dict(
        producer_module="tests.notify",
        correlation_id=uuid4(),
        signal_id=uuid4(),
        source="insider",
        signal_type="cluster_buy",
        strength=0.8,
        ticker="NVDA",
        raw_row_ids=[1, 2, 3],
    )
    base.update(overrides)
    return SignalFired(**base)


@pytest.fixture
def conn(monkeypatch) -> _Conn:
    c = _Conn()
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: _engine_with(c))
    return c


def test_emit_notifies_channel_in_audit_transaction(conn: _Conn) -> None:
    contract = _signal()
    emit_mod.emit(contract)

    assert len(conn.calls) == 2, "audit insert then pg_notify, one transaction"
    insert_sql, _ = conn.calls[0]
    notify_sql, notify_params = conn.calls[1]
    assert "contracts_audit" in insert_sql
    assert "pg_notify" in notify_sql
    assert notify_params["channel"] == channel_for(SignalFired)

    payload = json.loads(notify_params["payload"])
    assert payload["event_id"] == str(contract.event_id)
    assert payload["ticker"] == "NVDA"
    assert "truncated" not in payload


def test_oversized_payload_is_reduced_to_an_envelope(conn: _Conn) -> None:
    contract = _signal(raw_row_ids=list(range(4000)))  # well over 7800 bytes as JSON
    emit_mod.emit(contract)

    _, notify_params = conn.calls[1]
    raw = notify_params["payload"]
    assert len(raw.encode("utf-8")) <= emit_mod.NOTIFY_MAX_BYTES
    payload = json.loads(raw)
    assert payload["truncated"] is True
    assert payload["event_id"] == str(contract.event_id)
    assert payload["contract_type"] == "SignalFired"
    assert payload["ticker"] == "NVDA"


def test_notify_payload_helper_passes_small_payloads_through() -> None:
    contract = _signal()
    payload = contract.model_dump(mode="json")
    assert json.loads(emit_mod.notify_payload(contract, payload)) == json.loads(json.dumps(payload, default=str))


def test_audit_failure_still_emits_locally(monkeypatch) -> None:
    """The audit write (and therefore the notify) failing must not block the
    in-process bus — existing behaviour, re-pinned."""
    engine = MagicMock()
    engine.begin.side_effect = RuntimeError("db down")
    monkeypatch.setattr(emit_mod, "_get_engine", lambda: engine)

    seen: list = []
    emit_mod.bus.subscribe(channel_for(SignalFired), seen.append)
    try:
        emit_mod.emit(_signal())
    finally:
        emit_mod.bus.unsubscribe(channel_for(SignalFired), seen.append)
    assert len(seen) == 1
