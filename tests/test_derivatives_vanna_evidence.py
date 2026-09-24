"""Dealer routes label modeled sensitivities and never infer forced trades."""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from api.routers import derivatives


def _profile() -> dict:
    today = datetime.now(timezone.utc).astimezone().date()
    first = datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(hours=1)
    completed = first + timedelta(minutes=2)
    return {
        "ticker": "SPY", "snap_date": today.isoformat(),
        "chain_snap_date": today.isoformat(),
        "chain_created_at": first.isoformat(),
        "chain_created_at_max": first.isoformat(),
        "chain_batch_id": "11111111-1111-4111-8111-111111111111",
        "chain_capture_completed_at": completed.isoformat(),
        "spot": 100.0, "spot_source": "spy_close_receipt",
        "spot_basis": "prior_completed_unadjusted_close",
        "spot_obs_date": (today - timedelta(days=1)).isoformat(),
        "spot_available_at": datetime.combine(today, datetime.min.time(), timezone.utc).isoformat(),
        "spot_receipt_created_at": (first - timedelta(minutes=30)).isoformat(),
        "spot_release_date": today.isoformat(),
        "spot_vintage_date": today.isoformat(),
        "spot_receipt_id": 42,
        "estimated": True,
        "basis": "options_open_interest_with_assumed_dealer_sign_and_black_scholes",
        "gex_aggregate": 500.0, "gex_normalized": 0.1,
        "regime": "LONG_GAMMA", "gamma_flip": 98.0,
        "vanna_exposure": 0, "charm_exposure": 0,
        "per_strike": [],
    }


def _engine(monkeypatch: pytest.MonkeyPatch, profile: dict) -> None:
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: SimpleNamespace(
        compute_gex_profile=lambda _ticker: profile,
    ))


@pytest.mark.parametrize("change", [
    {"charm_exposure": None},
    {"vanna_exposure": float("nan")},
    {"charm_exposure": float("inf")},
])
def test_missing_or_nonfinite_sensitivity_is_unavailable(
    monkeypatch: pytest.MonkeyPatch, change: dict,
) -> None:
    _engine(monkeypatch, {**_profile(), **change})
    result = asyncio.run(derivatives.get_vanna_charm("SPY"))
    assert result == {"error": "Vanna/charm exposure unavailable", "ticker": "SPY"}


def test_modeled_zero_is_retained_without_trade_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _engine(monkeypatch, _profile())
    result = asyncio.run(derivatives.get_vanna_charm("SPY"))
    assert result["vanna_exposure"] == 0
    assert result["charm_exposure"] == 0
    assert result["net_dealer_delta_change"] is None
    assert result["estimated"] is True
    assert result["spot_source"] == "spy_close_receipt"
    assert result["chain_batch_id"] == _profile()["chain_batch_id"]
    assert "required trades" in result["interpretation"]
    assert "error" not in result


@pytest.mark.parametrize("regime", ["LONG_GAMMA", "SHORT_GAMMA", "NEUTRAL"])
def test_regime_interpretation_is_conditional_and_source_labeled(
    monkeypatch: pytest.MonkeyPatch, regime: str,
) -> None:
    _engine(monkeypatch, {**_profile(), "regime": regime})
    result = asyncio.run(derivatives.get_regime())
    assert result["estimated"] is True
    assert result["chain_capture_completed_at"] == _profile()["chain_capture_completed_at"]
    assert result["spot_source"] == "spy_close_receipt"
    assert "actual" in result["interpretation"].lower()
    assert "realized" not in result["interpretation"].lower()
    assert "will" not in result["interpretation"].lower()


def test_both_routes_reject_unverified_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    _engine(monkeypatch, {**_profile(), "chain_batch_id": None})
    regime = asyncio.run(derivatives.get_regime())
    vanna = asyncio.run(derivatives.get_vanna_charm("SPY"))
    assert regime["available"] is False
    assert regime["gex_aggregate"] is None
    assert "error" in vanna
