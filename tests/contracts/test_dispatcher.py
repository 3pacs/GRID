from __future__ import annotations

import threading
from uuid import uuid4


from contracts.dispatcher import Dispatcher
from contracts.schemas import PredictionScored, PullLifecycle, SignalRef


class _Recorder:
    def __init__(self):
        self.calls: list = []

    def __call__(self, event, engine=None):
        self.calls.append(event)


def _make_contract() -> PullLifecycle:
    return PullLifecycle(
        producer_module="test",
        correlation_id=uuid4(),
        puller_name="fred",
        state="COMPLETED",
        row_count=5,
        duration_s=0.25,
    )


def _make_prediction_scored() -> PredictionScored:
    return PredictionScored(
        producer_module="test",
        correlation_id=uuid4(),
        prediction_id=uuid4(),
        decision_id=17,
        ticker="GRID",
        verdict="HIT",
        expected_direction="UP",
        realized_direction="UP",
        confidence=0.8,
        brier_component=0.04,
        signals_used=[
            SignalRef(
                signal_id=uuid4(),
                source="test-source",
                trust_at_prediction=0.5,
                weight_at_prediction=0.25,
            )
        ],
        model_weights_at_prediction={"test-source": 0.25},
    )


def test_dispatcher_routes_valid_payload_to_handler(fake_bus, monkeypatch):
    handler = _Recorder()
    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_dispatcher._installed_handler"],
    )

    # Install the recorder as the resolved module attribute.
    import sys
    fake_mod = type(sys)("tests.contracts.test_dispatcher")
    fake_mod._installed_handler = handler
    sys.modules["tests.contracts.test_dispatcher"] = fake_mod

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    contract = _make_contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", contract.model_dump(mode="json"))

    d.wait_idle()
    assert len(handler.calls) == 1
    assert handler.calls[0].puller_name == "fred"
    assert dead_letters == []


def test_dispatcher_runs_route_handlers_for_one_contract_in_order(
    fake_bus, monkeypatch
):
    first_started = threading.Event()
    second_started = threading.Event()
    handler_events: list[tuple[str, bool] | str] = []

    def first(event, engine=None):
        first_started.set()
        second_started.wait(timeout=0.2)
        handler_events.append("first-completed")

    def second(event, engine=None):
        first_started.wait(timeout=1)
        second_started.set()
        handler_events.append(
            ("second-saw-first", "first-completed" in handler_events)
        )

    import sys
    fake_mod = type(sys)("tests.contracts.test_dispatcher")
    fake_mod._first_ordered = first
    fake_mod._second_ordered = second
    sys.modules["tests.contracts.test_dispatcher"] = fake_mod

    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PredictionScored,
        [
            "tests.contracts.test_dispatcher._first_ordered",
            "tests.contracts.test_dispatcher._second_ordered",
        ],
    )

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    contract = _make_prediction_scored()
    fake_bus.emit_sync(
        "grid_contracts_prediction_scored", contract.model_dump(mode="json")
    )
    d.wait_idle()

    assert handler_events == [
        "first-completed",
        ("second-saw-first", True),
    ]
    assert dead_letters == []


def test_dispatcher_writes_dead_letter_on_schema_violation(fake_bus):
    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    fake_bus.emit_sync(
        "grid_contracts_pull_lifecycle", {"not": "a valid payload"}
    )
    d.wait_idle()

    assert len(dead_letters) == 1
    assert dead_letters[0]["error_type"] == "SCHEMA_INVALID"


def test_dispatcher_writes_dead_letter_on_handler_exception(
    fake_bus, monkeypatch
):
    def boom(event, engine=None):
        raise RuntimeError("handler broke")

    import sys
    fake_mod = type(sys)("tests.contracts.test_dispatcher")
    fake_mod._boom = boom
    sys.modules["tests.contracts.test_dispatcher"] = fake_mod

    monkeypatch.setitem(
        __import__("contracts.router", fromlist=["ROUTES"]).ROUTES,
        PullLifecycle,
        ["tests.contracts.test_dispatcher._boom"],
    )

    dead_letters: list = []
    d = Dispatcher(
        bus=fake_bus,
        engine=None,
        dead_letter_writer=lambda **kw: dead_letters.append(kw),
    )
    d.start()

    contract = _make_contract()
    fake_bus.emit_sync("grid_contracts_pull_lifecycle", contract.model_dump(mode="json"))
    d.wait_idle()

    assert len(dead_letters) == 1
    assert dead_letters[0]["error_type"] == "CONSUMER_EXCEPTION"
    assert "handler broke" in dead_letters[0]["error_detail"]
