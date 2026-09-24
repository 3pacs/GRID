"""An inline flow narrative must not turn unavailable GEX into market facts."""

from __future__ import annotations

import asyncio
import sys
import types

import pytest

from api.routers import derivatives


class _GEX:
    def __init__(self, profile: dict) -> None:
        self.profile = profile

    def compute_gex_profile(self, ticker: str) -> dict:
        assert ticker == "SPY"
        return self.profile


def _without_saved_briefing(monkeypatch: pytest.MonkeyPatch) -> None:
    briefing = types.ModuleType("ollama.dealer_flow_briefing")
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
            "spot": 767.12,
            "gex_aggregate": 1_000_000.0,
            "regime": "LONG_GAMMA",
            "gamma_flip": 760.0,
            "put_wall": 750.0,
            "call_wall": 775.0,
            "vanna_exposure": 10.0,
            "charm_exposure": 20.0,
            "snap_date": "2026-09-24",
        }),
    )

    result = asyncio.run(derivatives.get_flow_narrative())

    assert "SPY is trading at $767.12" in result["content"]
    assert "LONG GAMMA" in result["content"]
    assert result["positioning_data"]["gex"]["SPY"]["spot"] == 767.12
    assert result["briefing_date"] == "2026-09-24"


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
