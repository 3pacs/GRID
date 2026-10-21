"""/derivatives default-day contract at the API edge.

The engine resolves ``snap_date=None`` to the latest completed capture and
labels the profile (``default_resolution``, ``chain_age_seconds``); the
routers must surface those fields on ``/gex``, ``/walls``, ``/regime`` and
``/vanna-charm``, keep the unavailable envelope's reason, and verify the SPY
chain/close pair against the chain's own day rather than the UTC calendar day.
The dealer-flow briefing keeps its same-day contract. No database: the engine
is faked like the other derivatives tests.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

from api.routers import derivatives
from ollama import dealer_flow_briefing as flow
from store.availability import unavailable

CHAIN_DAY = date(2026, 9, 24)  # Thursday
NOW = datetime(2026, 9, 25, 2, 55, tzinfo=timezone.utc)  # next UTC day, before its ~22:20Z capture
_MIDNIGHT = datetime.combine(CHAIN_DAY, datetime.min.time(), timezone.utc)
_CAPTURED = _MIDNIGHT + timedelta(hours=22, minutes=20)
_COMPLETED = _CAPTURED + timedelta(seconds=46)


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(derivatives, "_utc_day", lambda: NOW.date())
    monkeypatch.setattr(flow, "_utc_day", lambda: NOW.date())


def _profile(**over) -> dict:
    quote = _MIDNIGHT + timedelta(hours=20)  # 16:00 New York on CHAIN_DAY
    profile = {
        "default_resolution": "last_completed_capture",
        "ticker": "SPY", "snap_date": CHAIN_DAY.isoformat(),
        "chain_snap_date": CHAIN_DAY.isoformat(),
        "chain_age_seconds": int((NOW - _COMPLETED).total_seconds()),
        "chain_batch_id": "11111111-1111-4111-8111-111111111111",
        "chain_capture_ordinal": 1,
        "chain_capture_started_at": _CAPTURED.isoformat(),
        "chain_capture_completed_at": _COMPLETED.isoformat(),
        "chain_provider_regular_market_at_min": quote.isoformat(),
        "chain_provider_regular_market_at_max": quote.isoformat(),
        "chain_created_at": _CAPTURED.isoformat(),
        "chain_created_at_max": _CAPTURED.isoformat(),
        "spot": 766.48, "spot_source": "spy_close_receipt",
        "spot_basis": "prior_completed_unadjusted_close",
        "spot_obs_date": (CHAIN_DAY - timedelta(days=1)).isoformat(),
        "spot_available_at": (_MIDNIGHT + timedelta(minutes=30)).isoformat(),
        "spot_receipt_created_at": (_MIDNIGHT + timedelta(hours=1)).isoformat(),
        "spot_release_date": CHAIN_DAY.isoformat(),
        "spot_vintage_date": CHAIN_DAY.isoformat(),
        "spot_receipt_id": 42,
        "estimated": True,
        "basis": "options_open_interest_with_assumed_dealer_sign_and_black_scholes",
        "gex_aggregate": -1.2e9, "gex_normalized": -1.6,
        "gamma_flip": 770.0, "gamma_wall": 760.0, "put_wall": 750.0, "call_wall": 780.0,
        "regime": "SHORT_GAMMA",
        "vanna_exposure": 1.0, "charm_exposure": 2.0,
        "per_strike": [],
    }
    profile.update(over)
    return profile


def _no_capture_envelope() -> dict:
    reason = "no completed options capture for SPY on or before 2026-09-25 (last NYSE trading day)"
    result = unavailable(reason, source="options_capture_batches", snap_date=None,
                         chain_snap_date=None, chain_age_seconds=None, spot=None, regime=None,
                         gamma_flip=None, put_wall=None, call_wall=None, gamma_wall=None,
                         gex_aggregate=None, gex_normalized=None, vanna_exposure=None,
                         charm_exposure=None, per_strike=None, profile=None)
    result.update({"ticker": "SPY", "error": reason, "default_resolution": "last_completed_capture"})
    return result


class _Engine:
    """Fake engine: the default call shape (no kwargs) returns the labelled profile."""

    def __init__(self, default: dict, explicit: dict | None = None) -> None:
        self.default = default
        self.explicit = explicit
        self.calls: list[tuple] = []

    def compute_gex_profile(self, ticker, **kwargs):
        self.calls.append((ticker, kwargs))
        if kwargs.get("snap_date") is None:
            return self.default
        return self.explicit


def _patched(monkeypatch: pytest.MonkeyPatch, default: dict, explicit: dict | None = None) -> _Engine:
    engine = _Engine(default, explicit)
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: engine)
    return engine


def test_gex_default_surfaces_engine_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _patched(monkeypatch, _profile())
    result = asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id=None))

    assert engine.calls == [("SPY", {})]
    assert result["default_resolution"] == "last_completed_capture"
    assert result["chain_snap_date"] == "2026-09-24"
    assert result["chain_age_seconds"] == 4 * 3600 + 34 * 60 + 14
    assert "replayed_batch" not in result


def test_gex_default_without_any_capture_keeps_unavailable_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _no_capture_envelope())
    result = asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id=None))

    assert result["available"] is False and result["status"] == "unavailable"
    assert "on or before 2026-09-25" in result["reason"]
    assert result["error"] == result["reason"]
    assert result["default_resolution"] == "last_completed_capture"


def test_walls_default_and_explicit_day(monkeypatch: pytest.MonkeyPatch) -> None:
    explicit = {k: v for k, v in _profile(snap_date="2026-09-23", chain_snap_date="2026-09-23").items()
                if k != "default_resolution"}
    engine = _patched(monkeypatch, _profile(), explicit)

    walls = asyncio.run(derivatives.get_walls("spy", snap_date=None, capture_batch_id=None))
    assert walls["default_resolution"] == "last_completed_capture"
    assert walls["chain_age_seconds"] == _profile()["chain_age_seconds"]
    assert walls["chain_snap_date"] == "2026-09-24" and walls["put_wall"] == 750.0
    assert walls["replayed_batch"] is False

    walls = asyncio.run(derivatives.get_walls("spy", snap_date=date(2026, 9, 23), capture_batch_id=None))
    assert engine.calls[-1] == ("SPY", {"snap_date": date(2026, 9, 23)})
    assert walls["chain_snap_date"] == "2026-09-23"
    assert walls["default_resolution"] is None
    assert walls["chain_age_seconds"] == _profile()["chain_age_seconds"]


def test_walls_propagate_the_unavailable_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _no_capture_envelope())
    walls = asyncio.run(derivatives.get_walls("spy", snap_date=None, capture_batch_id=None))

    assert walls["available"] is False and walls["status"] == "unavailable"
    assert walls["error"] == walls["reason"]
    assert walls["source"] == "options_capture_batches"
    assert walls["default_resolution"] == "last_completed_capture"
    assert "put_wall" not in walls  # no all-None walls masquerading as a profile


def test_regime_serves_the_prior_session_chain_with_its_age(monkeypatch: pytest.MonkeyPatch) -> None:
    # 02:55Z the day after the chain: the UTC day pin would have rejected this.
    _patched(monkeypatch, _profile())
    result = asyncio.run(derivatives.get_regime())

    assert result.get("available") is not False
    assert result["regime"] == "SHORT_GAMMA"
    assert result["chain_snap_date"] == "2026-09-24"
    assert result["snap_date"] == "2026-09-24"
    assert result["chain_age_seconds"] == _profile()["chain_age_seconds"]
    assert result["default_resolution"] == "last_completed_capture"
    assert result["estimated"] is True and result["spot_source"] == "spy_close_receipt"


def test_regime_without_any_capture_is_unavailable_with_reason(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _no_capture_envelope())
    result = asyncio.run(derivatives.get_regime())

    assert result["available"] is False
    assert result["regime"] is None
    assert "on or before 2026-09-25" in result["reason"]
    assert result["error"] == result["reason"]


@pytest.mark.parametrize("change", [
    {"chain_snap_date": "2026-09-26", "snap_date": "2026-09-26"},  # Saturday chain
    {"chain_snap_date": "2026-09-25"},  # chain day disagrees with its own clocks
    {"chain_batch_id": None},
    {"spot_source": "guess"},
    {"chain_snap_date": None},
])
def test_regime_still_rejects_an_undated_pair(monkeypatch: pytest.MonkeyPatch, change: dict) -> None:
    _patched(monkeypatch, _profile(**change))
    result = asyncio.run(derivatives.get_regime())

    assert result["available"] is False
    assert result["regime"] is None and result["gex_aggregate"] is None


def test_vanna_charm_carries_age_fields(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, _profile())
    result = asyncio.run(derivatives.get_vanna_charm("SPY"))

    assert "error" not in result
    assert result["chain_age_seconds"] == _profile()["chain_age_seconds"]
    assert result["default_resolution"] == "last_completed_capture"
    assert result["chain_snap_date"] == "2026-09-24"


def test_briefing_contract_still_pins_the_chain_to_today() -> None:
    # The API edge accepts the dated prior-session pair; the briefing does not.
    assert flow.dated_spy_gex_profile(_profile(), CHAIN_DAY) is True
    assert flow.valid_spy_gex_profile(_profile(), CHAIN_DAY) is False
    assert flow.valid_spy_gex_profile(_profile(), NOW.date()) is False
    assert derivatives._verified_gex_profile(_profile()) is True
