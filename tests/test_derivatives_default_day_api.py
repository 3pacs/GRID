"""/derivatives read routes opt into the latest completed capture; held-current gates do not.

Only ``/gex`` and ``/walls`` without a day or batch call
``compute_latest_completed_gex_profile`` and surface its age fields.
``/regime`` and ``/vanna-charm`` keep the base UTC-day default and the
same-day ``valid_spy_gex_profile`` gate (PR #890 review R3), so a prior-session
SPY pair stays unavailable there. No database: the engine is faked like the
other derivatives tests.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

import pytest

from api.routers import derivatives
from ollama import dealer_flow_briefing as flow
from store.availability import unavailable

CHAIN_DAY = date(2026, 9, 24)  # Thursday
NOW = datetime(2026, 9, 25, 2, 55, tzinfo=timezone.utc)
_MIDNIGHT = datetime.combine(CHAIN_DAY, datetime.min.time(), timezone.utc)
_CAPTURED = _MIDNIGHT + timedelta(hours=22, minutes=20)
_COMPLETED = _CAPTURED + timedelta(seconds=46)
AGE = int((NOW - _COMPLETED).total_seconds())


@pytest.fixture(autouse=True)
def _clock(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(derivatives, "_utc_day", lambda: NOW.date())
    monkeypatch.setattr(flow, "_utc_day", lambda: NOW.date())


def _profile(**over) -> dict:
    quote = _MIDNIGHT + timedelta(hours=20)
    profile = {
        "ticker": "SPY", "snap_date": CHAIN_DAY.isoformat(),
        "chain_snap_date": CHAIN_DAY.isoformat(),
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
        "spot_receipt_id": 42, "estimated": True,
        "basis": "options_open_interest_with_assumed_dealer_sign_and_black_scholes",
        "gex_aggregate": -1.2e9, "gex_normalized": -1.6, "gamma_flip": 770.0,
        "gamma_wall": 760.0, "put_wall": 750.0, "call_wall": 780.0,
        "regime": "SHORT_GAMMA", "vanna_exposure": 1.0, "charm_exposure": 2.0,
        "per_strike": [],
    }
    profile.update(over)
    return profile


def _latest() -> dict:
    return {**_profile(), "default_resolution": "last_completed_capture",
            "chain_read_cutoff": NOW.isoformat(), "chain_age_seconds": AGE}


def _no_capture() -> dict:
    reason = "no completed options capture for SPY on or before 2026-09-25 (last NYSE trading day)"
    result = unavailable(reason, source="options_capture_batches", chain_age_seconds=None,
                         put_wall=None, call_wall=None, spot=None, regime=None)
    result.update({"ticker": "SPY", "error": reason, "default_resolution": "last_completed_capture",
                   "chain_read_cutoff": NOW.isoformat()})
    return result


class _Engine:
    def __init__(self, latest: dict | None = None, dated: dict | None = None) -> None:
        self.latest, self.dated = latest, dated
        self.calls: list[tuple] = []

    def compute_latest_completed_gex_profile(self, ticker):
        self.calls.append(("latest", ticker))
        return self.latest

    def compute_gex_profile(self, ticker, **kwargs):
        self.calls.append(("profile", ticker, kwargs))
        return self.dated


def _patched(monkeypatch: pytest.MonkeyPatch, **kw) -> _Engine:
    engine = _Engine(**kw)
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: engine)
    return engine


def test_gex_without_day_opts_into_latest_completed(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _patched(monkeypatch, latest=_latest())
    result = asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id=None))

    assert engine.calls == [("latest", "SPY")]
    assert result["default_resolution"] == "last_completed_capture"
    assert result["chain_age_seconds"] == AGE == 4 * 3600 + 34 * 60 + 14
    assert result["chain_read_cutoff"] == NOW.isoformat()


def test_gex_with_explicit_day_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _patched(monkeypatch, dated=_profile())
    result = asyncio.run(derivatives.get_gex("spy", snap_date=CHAIN_DAY, capture_batch_id=None))

    assert engine.calls == [("profile", "SPY", {"snap_date": CHAIN_DAY})]
    assert "default_resolution" not in result and "chain_age_seconds" not in result


def test_gex_without_any_capture_keeps_the_unavailable_envelope(monkeypatch: pytest.MonkeyPatch) -> None:
    _patched(monkeypatch, latest=_no_capture())
    result = asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id=None))

    assert result["available"] is False and result["status"] == "unavailable"
    assert "on or before 2026-09-25" in result["reason"] and result["error"] == result["reason"]


def test_walls_default_explicit_and_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _patched(monkeypatch, latest=_latest(), dated=_profile())
    walls = asyncio.run(derivatives.get_walls("spy", snap_date=None, capture_batch_id=None))
    assert walls["chain_age_seconds"] == AGE and walls["put_wall"] == 750.0
    assert walls["default_resolution"] == "last_completed_capture"

    walls = asyncio.run(derivatives.get_walls("spy", snap_date=CHAIN_DAY, capture_batch_id=None))
    assert engine.calls[-1] == ("profile", "SPY", {"snap_date": CHAIN_DAY})
    assert walls["default_resolution"] is None and walls["chain_age_seconds"] is None

    engine.latest = _no_capture()
    walls = asyncio.run(derivatives.get_walls("spy", snap_date=None, capture_batch_id=None))
    assert walls["available"] is False and walls["error"] == walls["reason"]
    assert walls["source"] == "options_capture_batches"
    assert "put_wall" not in walls


def test_regime_keeps_the_utc_day_default_and_same_day_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    # A valid prior-session pair is still refused by /regime (held-current boundary).
    engine = _patched(monkeypatch, latest=_latest(), dated=_profile())
    result = asyncio.run(derivatives.get_regime())

    assert engine.calls == [("profile", "SPY", {})]
    assert result["available"] is False and result["regime"] is None
    assert "chain_age_seconds" not in result


def test_vanna_charm_keeps_the_utc_day_default_and_same_day_gate(monkeypatch: pytest.MonkeyPatch) -> None:
    engine = _patched(monkeypatch, latest=_latest(), dated=_profile())
    result = asyncio.run(derivatives.get_vanna_charm("SPY"))

    assert engine.calls == [("profile", "SPY", {})]
    assert "error" in result


def test_regime_still_serves_a_same_day_pair(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(derivatives, "_utc_day", lambda: CHAIN_DAY)
    monkeypatch.setattr(flow, "_utc_day", lambda: CHAIN_DAY)
    _patched(monkeypatch, dated=_profile())
    result = asyncio.run(derivatives.get_regime())

    assert result["regime"] == "SHORT_GAMMA" and result.get("available") is not False


def test_briefing_validator_is_the_base_contract() -> None:
    assert not hasattr(flow, "dated_spy_gex_profile")
    assert flow.valid_spy_gex_profile(_profile(), CHAIN_DAY) is False  # UTC day is NOW.date()
