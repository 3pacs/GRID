"""Vanna/charm route preserves observed zero without inventing absent metrics."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from api.routers import derivatives


@pytest.mark.parametrize("profile", [
    {"charm_exposure": 0},
    {"vanna_exposure": None, "charm_exposure": 0},
    {"vanna_exposure": float("nan"), "charm_exposure": 0},
    {"vanna_exposure": 0, "charm_exposure": float("inf")},
])
def test_missing_or_nonfinite_aggregate_is_unavailable(monkeypatch, profile):
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: SimpleNamespace(
        compute_gex_profile=lambda _ticker: profile,
    ))
    result = asyncio.run(derivatives.get_vanna_charm("AAPL"))
    assert result == {"error": "Vanna/charm exposure unavailable", "ticker": "AAPL"}


def test_observed_zero_is_retained(monkeypatch):
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: SimpleNamespace(
        compute_gex_profile=lambda _ticker: {
            "vanna_exposure": 0, "charm_exposure": 0, "spot": 100, "per_strike": [],
        },
    ))
    result = asyncio.run(derivatives.get_vanna_charm("AAPL"))
    assert result["vanna_exposure"] == 0
    assert result["charm_exposure"] == 0
    assert result["net_dealer_delta_change"] == 0
    assert "error" not in result
