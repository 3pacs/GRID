"""New dealer narratives require measured SPY GEX; GET never creates tables."""

from __future__ import annotations

import json
from typing import Self

import pytest

from ollama import dealer_flow_briefing as flow


def test_generation_without_measured_spy_spot_does_not_write(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flow, "_gather_positioning_data", lambda _db: {"gex": {}})
    monkeypatch.setattr(flow, "_ensure_table", lambda _db: pytest.fail("must not create table"))
    monkeypatch.setattr(flow, "_get_llm_client", lambda: pytest.fail("must not prompt"))

    result = flow.generate_dealer_flow_briefing(object())

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["content"] is None
    assert result["positioning_data"] is None
    assert result["as_of"] is None


class _CaptureWriter:
    def __init__(self) -> None:
        self.params: dict | None = None

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, _statement: object, params: dict) -> None:
        self.params = params


def test_generation_stamps_only_a_resolved_series_spot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _CaptureWriter()
    monkeypatch.setattr(flow, "_ensure_table", lambda _db: None)
    monkeypatch.setattr(
        flow,
        "_gather_positioning_data",
        lambda _db: {"gex": {"SPY": {"spot": 767.12, "spot_source": "resolved_series"}}},
    )
    monkeypatch.setattr(flow, "_build_prompt", lambda _data: ("system", "user"))
    monkeypatch.setattr(flow, "_get_llm_client", lambda: None)
    monkeypatch.setattr(flow, "_generate_fallback", lambda _data: "Measured briefing")

    result = flow.generate_dealer_flow_briefing(engine)

    assert result["content"] == "Measured briefing"
    assert result["positioning_data"]["spot_contract"] == flow.SPOT_CONTRACT
    assert engine.params is not None
    assert json.loads(engine.params["p"])["spot_contract"] == flow.SPOT_CONTRACT


class _ReadOnlyConnection:
    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: object) -> _ReadOnlyConnection:
        assert str(statement).lstrip().upper().startswith("SELECT ")
        return self

    def fetchone(self) -> None:
        return None


class _ReadOnlyEngine:
    def connect(self) -> _ReadOnlyConnection:
        return _ReadOnlyConnection()


def test_reading_missing_briefing_never_creates_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flow, "_ensure_table", lambda _db: pytest.fail("GET must not create table"))

    result = flow.get_latest_flow_briefing(_ReadOnlyEngine())

    assert result["content"] is None
    assert result["stale"] is True
