from __future__ import annotations

import sys
from uuid import uuid4

import pytest

import importlib

from contracts import observability as obs

emit_mod = importlib.import_module("contracts.emit")
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
