"""New dealer narratives require dated SPY GEX; GET never creates tables."""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Self

import pytest

from ollama import dealer_flow_briefing as flow

SESSION_DAY = date(2026, 9, 25)


@pytest.fixture(autouse=True)
def _session_day(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(flow, "_utc_day", lambda: SESSION_DAY)


def _dated_spy() -> dict:
    today = SESSION_DAY
    captured = datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(hours=19)
    return {
        "estimated": True,
        "basis": "options_open_interest_with_assumed_dealer_sign_and_black_scholes",
        "spot": 767.12, "spot_source": "spy_close_receipt",
        "spot_basis": "prior_completed_unadjusted_close",
        "spot_receipt_id": 123, "spot_obs_date": (today - timedelta(days=1)).isoformat(),
        "spot_available_at": datetime.combine(today, datetime.min.time(), timezone.utc).isoformat(),
        "spot_receipt_created_at": (datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(minutes=30)).isoformat(),
        "spot_release_date": today.isoformat(), "spot_vintage_date": today.isoformat(),
        "snap_date": today.isoformat(), "chain_snap_date": today.isoformat(),
        "chain_batch_id": "11111111-1111-4111-8111-111111111111",
        "chain_capture_ordinal": 1,
        "chain_capture_started_at": captured.isoformat(),
        "chain_capture_completed_at": (captured + timedelta(minutes=1)).isoformat(),
        "chain_provider_regular_market_at_min": (captured - timedelta(hours=2)).isoformat(),
        "chain_provider_regular_market_at_max": (captured - timedelta(hours=2)).isoformat(),
        "chain_created_at": (captured + timedelta(minutes=2)).isoformat(),
        "chain_created_at_max": (captured + timedelta(minutes=2)).isoformat(),
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
    {"chain_batch_id": None},
    {"chain_capture_ordinal": None},
    {"chain_capture_started_at": None},
    {"chain_capture_completed_at": "2099-01-01T00:00:00+00:00"},
])
def test_v4_guard_rejects_stale_future_or_revised_saved_spot(change: dict) -> None:
    assert not flow.valid_spy_gex_profile({**_dated_spy(), **change}, SESSION_DAY)


def test_saved_profile_requires_new_source_quote_contract() -> None:
    old = _dated_spy()
    old.pop("chain_provider_regular_market_at_min")
    old.pop("chain_provider_regular_market_at_max")
    assert not flow.valid_spy_gex_profile(old, SESSION_DAY)
    assert flow.SPOT_CONTRACT.endswith("_v6")


def test_saturday_profile_cannot_be_served_as_current(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    saturday = date(2026, 9, 26)
    monkeypatch.setattr(flow, "_utc_day", lambda: saturday)
    profile = _dated_spy()
    profile.update({"snap_date": saturday.isoformat(),
                    "chain_snap_date": saturday.isoformat()})
    assert not flow.valid_spy_gex_profile(profile, saturday)


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


@pytest.mark.parametrize("contract", [
    "resolved_series_only_v1", "spy_receipt_chain_pit_v2",
    "spy_receipt_chain_batch_pit_v3",
    "spy_receipt_chain_batch_pit_v4",
    "spy_receipt_chain_batch_pit_v5",
])
def test_legacy_saved_row_is_withheld_even_if_dated_today(contract: str) -> None:
    saved = (SESSION_DAY, "Legacy dealer narrative", {
        "spot_contract": contract,
        "gex": {"SPY": {"spot": 767.12, "spot_source": "resolved_series"}},
    }, datetime.now(timezone.utc))
    result = flow.get_latest_flow_briefing(_ReadOnlyEngine(saved))
    assert result["content"] is None
    assert result["positioning_data"] is None
    assert result["stale"] is True


def test_saved_v5_with_otherwise_valid_spy_profile_is_withheld() -> None:
    saved = (SESSION_DAY, "Old contract narrative", {
        "spot_contract": "spy_receipt_chain_batch_pit_v5",
        "gex": {"SPY": _dated_spy()},
    }, datetime.now(timezone.utc))
    result = flow.get_latest_flow_briefing(_ReadOnlyEngine(saved))
    assert result["content"] is None
    assert result["stale"] is True


def test_downward_flip_uses_computed_gex_sign_in_briefing_prompt() -> None:
    """Spot above one flip can still have negative modeled GEX."""
    profile = {
        **_dated_spy(), "spot": 102.0, "spot_obs_date": "2026-09-23",
        "gex_aggregate": -357535.0, "gamma_flip": 99.30,
        "gamma_flip_crossings": 1, "regime": "SHORT_GAMMA",
    }

    block = flow._fmt_ticker_block("SPY", profile)
    system_prompt, user_prompt = flow._build_prompt({"gex": {"SPY": profile}})

    assert "Modeled SHORT GAMMA" in block
    assert "LONG GAMMA (above flip)" not in block
    assert "either side can have either GEX sign" in system_prompt
    assert "crossing a flip price alone does not establish a regime direction" in user_prompt
