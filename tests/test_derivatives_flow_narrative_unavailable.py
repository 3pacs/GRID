"""An inline flow narrative must not turn unavailable GEX into market facts."""

from __future__ import annotations

import asyncio
import sys
import types
from datetime import date, datetime, timedelta, timezone

import pytest

from api.routers import derivatives
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


class _GEX:
    def __init__(self, profile: dict) -> None:
        self.profile = profile

    def compute_gex_profile(self, ticker: str) -> dict:
        assert ticker == "SPY"
        return self.profile


def _without_saved_briefing(monkeypatch: pytest.MonkeyPatch) -> None:
    briefing = types.ModuleType("ollama.dealer_flow_briefing")
    briefing.SPOT_CONTRACT = flow.SPOT_CONTRACT
    briefing.valid_spy_gex_profile = flow.valid_spy_gex_profile
    briefing.get_latest_flow_briefing = lambda _db: {}
    monkeypatch.setitem(sys.modules, "ollama.dealer_flow_briefing", briefing)
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: object())


def test_missing_spot_returns_unavailable_without_false_narrative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _without_saved_briefing(monkeypatch)
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: _GEX({
            "available": False,
            "status": "unavailable",
            "reason": "no measured close for SPY in resolved_series",
            "source": "resolved_series",
            "error": "No spot price for SPY",
            "spot": None,
            "regime": None,
        }),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["reason"] == "no measured close for SPY in resolved_series"
    assert result["source"] == "resolved_series"
    assert result["content"] is None
    assert result["positioning_data"] is None
    assert result["briefing_date"] is None
    assert result["as_of"] is None
    assert result["stale"] is True
    assert result["error"] == "No spot price for SPY"


def test_measured_spot_still_builds_inline_narrative(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _without_saved_briefing(monkeypatch)
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: _GEX({
            **_dated_spy(),
            "gex_aggregate": 1_000_000.0,
            "regime": "LONG_GAMMA",
            "gamma_flip": 760.0,
            "put_wall": 750.0,
            "call_wall": 775.0,
            "vanna_exposure": 10.0,
            "charm_exposure": 20.0,
        }),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert "SPY prior verified close was $767.12" in result["content"]
    assert "LONG GAMMA" in result["content"]
    assert result["positioning_data"]["gex"]["SPY"]["spot"] == 767.12
    assert result["briefing_date"] == date.today().isoformat()


def test_legacy_error_profile_without_availability_fields_stays_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _without_saved_briefing(monkeypatch)
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: _GEX({"error": "No options chain for SPY", "ticker": "SPY"}),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert result["content"] is None
    assert result["status"] == "unavailable"
    assert result["reason"] == "No options chain for SPY"


@pytest.mark.parametrize("positioning_data", [
    {"gex": {"SPY": {"spot": 785.0}}},
    {"spot_contract": "resolved_series_only_v1",
     "gex": {"SPY": {"spot": 0.0, "spot_source": "resolved_series"}}},
])
def test_unverified_saved_briefing_cannot_bypass_missing_spot(
    monkeypatch: pytest.MonkeyPatch,
    positioning_data: dict,
) -> None:
    _without_saved_briefing(monkeypatch)
    briefing = sys.modules["ollama.dealer_flow_briefing"]
    briefing.get_latest_flow_briefing = lambda _db: {
        "content": "SPY is trading at $785.00",
        "positioning_data": positioning_data,
        "briefing_date": "2026-09-24",
        "stale": False,
    }
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: _GEX({"error": "No spot price for SPY", "ticker": "SPY"}),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert result["content"] is None
    assert result["status"] == "unavailable"


def test_source_guarded_saved_briefing_is_retained(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _without_saved_briefing(monkeypatch)
    briefing = sys.modules["ollama.dealer_flow_briefing"]
    saved = {
        "content": "Measured SPY close $767.12",
        "positioning_data": {
            "spot_contract": flow.SPOT_CONTRACT,
            "gex": {"SPY": _dated_spy()},
        },
        "briefing_date": date.today().isoformat(),
        "stale": False,
    }
    briefing.get_latest_flow_briefing = lambda _db: saved
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: pytest.fail("source-guarded saved briefing should not recompute GEX"),
    )

    assert asyncio.run(derivatives.get_flow_narrative()) is saved


def test_stale_saved_briefing_falls_back_to_current_unavailable_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _without_saved_briefing(monkeypatch)
    briefing = sys.modules["ollama.dealer_flow_briefing"]
    briefing.get_latest_flow_briefing = lambda _db: {
        "content": "Yesterday SPY was trading at $785.00",
        "positioning_data": {
            "spot_contract": "resolved_series_only_v1",
            "gex": {"SPY": {"spot": 785.0, "spot_source": "resolved_series"}},
        },
        "stale": True,
    }
    monkeypatch.setattr(
        derivatives,
        "_get_gex_engine",
        lambda: _GEX({"error": "No spot price for SPY", "ticker": "SPY"}),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert result["content"] is None
    assert result["status"] == "unavailable"
