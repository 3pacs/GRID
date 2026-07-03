"""Regression tests for analysis.money_flow_engine.layer_credit helpers."""
from __future__ import annotations

from datetime import date

from loguru import logger as log

from analysis.money_flow_engine import layer_credit
from analysis.money_flow_engine.types import FlowNode


class _RaisingEngine:
    """Fake SQLAlchemy engine whose connect() always raises."""

    def connect(self):
        raise RuntimeError("simulated connection failure")


class _NoRowEngine:
    """Fake engine whose SELECT returns no rows."""

    def connect(self):
        return _NoRowConn()


class _NoRowConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, *_args, **_kwargs):
        return _NoRowResult()


class _NoRowResult:
    def fetchone(self):
        return None


def _capture_warnings():
    records: list[dict] = []
    sink_id = log.add(
        lambda msg: records.append(
            {"level": msg.record["level"].name, "message": msg.record["message"]}
        ),
        level="WARNING",
    )
    return records, sink_id


def test_money_market_node_fallback_when_engine_is_none():
    node = layer_credit._build_money_market_node(engine=None, as_of=date(2026, 7, 3))

    assert isinstance(node, FlowNode)
    assert node.id == "money_market_funds"
    assert node.value == layer_credit._EST_MONEY_MARKET_FUNDS
    assert node.source == "estimate"
    assert node.confidence == "estimated"


def test_money_market_node_falls_back_and_warns_on_query_failure():
    records, sink_id = _capture_warnings()
    try:
        node = layer_credit._build_money_market_node(
            engine=_RaisingEngine(), as_of=date(2026, 7, 3)
        )
    finally:
        log.remove(sink_id)

    assert node.value == layer_credit._EST_MONEY_MARKET_FUNDS
    assert node.source == "estimate"
    assert node.confidence == "estimated"

    warnings = [r["message"] for r in records if r["level"] == "WARNING"]
    assert any("money_market_funds FRED lookup failed" in msg for msg in warnings), (
        f"expected fallback warning, got: {warnings}"
    )


def test_money_market_node_stays_estimate_when_query_returns_no_row():
    records, sink_id = _capture_warnings()
    try:
        node = layer_credit._build_money_market_node(
            engine=_NoRowEngine(), as_of=date(2026, 7, 3)
        )
    finally:
        log.remove(sink_id)

    assert node.value == layer_credit._EST_MONEY_MARKET_FUNDS
    assert node.source == "estimate"
    assert node.confidence == "estimated"
    warnings = [r["message"] for r in records if r["level"] == "WARNING"]
    assert not any(
        "money_market_funds FRED lookup failed" in msg for msg in warnings
    ), "no-row path is not an error, should not warn"
