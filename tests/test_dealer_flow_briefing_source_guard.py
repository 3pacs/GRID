"""New dealer narratives require dated SPY GEX; GET never creates tables."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Self

import pytest

from ollama import dealer_flow_briefing as flow


def _dated_spy() -> dict:
    today = date.today()
    captured = datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(hours=1)
    return {
        "spot": 767.12, "spot_source": "spy_close_receipt",
        "spot_basis": "prior_completed_unadjusted_close",
        "spot_receipt_id": 123, "spot_obs_date": (today - timedelta(days=1)).isoformat(),
        "spot_available_at": datetime.combine(today, datetime.min.time(), timezone.utc).isoformat(),
        "spot_receipt_created_at": (datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(minutes=30)).isoformat(),
        "spot_release_date": today.isoformat(), "spot_vintage_date": today.isoformat(),
        "snap_date": today.isoformat(), "chain_snap_date": today.isoformat(),
        "chain_created_at": captured.isoformat(),
        "chain_created_at_max": captured.isoformat(),
    }


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


def test_generation_stamps_only_a_verified_dated_spot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    engine = _CaptureWriter()
    monkeypatch.setattr(flow, "_ensure_table", lambda _db: None)
    monkeypatch.setattr(
        flow,
        "_gather_positioning_data",
        lambda _db: {"gex": {"SPY": _dated_spy()}},
    )
    monkeypatch.setattr(flow, "_build_prompt", lambda _data: ("system", "user"))
    monkeypatch.setattr(flow, "_get_llm_client", lambda: None)
    monkeypatch.setattr(flow, "_generate_fallback", lambda _data: "Measured briefing")

    result = flow.generate_dealer_flow_briefing(engine)

    assert result["content"] == "Measured briefing"
    assert result["positioning_data"]["spot_contract"] == flow.SPOT_CONTRACT
    assert engine.params is not None
    assert json.loads(engine.params["p"])["spot_contract"] == flow.SPOT_CONTRACT


@pytest.mark.parametrize("change", [
    {"spot_obs_date": "2024-01-02"},
    {"spot_release_date": "2099-01-01"},
    {"spot_vintage_date": "2099-01-01"},
    {"spot_available_at": "2099-01-01T00:00:00+00:00"},
    {"spot_receipt_created_at": "2099-01-01T00:00:00+00:00"},
    {"chain_snap_date": "2099-01-01"},
])
def test_v2_guard_rejects_stale_future_or_revised_saved_spot(change: dict) -> None:
    assert not flow.valid_spy_gex_profile({**_dated_spy(), **change}, date.today())


class _ReadOnlyConnection:
    def __init__(self, row: tuple | None = None) -> None:
        self.row = row

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: object) -> _ReadOnlyConnection:
        assert str(statement).lstrip().upper().startswith("SELECT ")
        return self

    def fetchone(self) -> tuple | None:
        return self.row


class _ReadOnlyEngine:
    def __init__(self, row: tuple | None = None) -> None:
        self.row = row

    def connect(self) -> _ReadOnlyConnection:
        return _ReadOnlyConnection(self.row)


def test_reading_missing_briefing_never_creates_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(flow, "_ensure_table", lambda _db: pytest.fail("GET must not create table"))

    result = flow.get_latest_flow_briefing(_ReadOnlyEngine())

    assert result["content"] is None
    assert result["stale"] is True


def test_legacy_v1_saved_row_is_withheld_even_if_dated_today() -> None:
    saved = (date.today(), "Legacy dealer narrative", {
        "spot_contract": "resolved_series_only_v1",
        "gex": {"SPY": {"spot": 767.12, "spot_source": "resolved_series"}},
    }, datetime.now(timezone.utc))
    result = flow.get_latest_flow_briefing(_ReadOnlyEngine(saved))
    assert result["content"] is None
    assert result["positioning_data"] is None
    assert result["stale"] is True
