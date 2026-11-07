"""Ticker Lookup honesty fixes (2026-11-07 live check on NVDA).

* SEC reported values printed as ``2.28984e+11 USD``.
* The regime blocker named the recommendation (DEFENSIVE) as if it were the
  regime (FRAGILE).
* TradingView is push-only, but "no recent rows" cost every ticker 3 points
  as a stale source.
"""

from __future__ import annotations

from api import dad_sec_fundamentals as sec
from api.routers import dad


def test_reported_values_have_no_exponent_and_a_short_form() -> None:
    assert sec._exact_text(228984000000.0) == "228,984,000,000"
    assert sec._exact_text(0.0) == "0"
    assert sec._exact_text(4.5612) == "4.5612"
    assert sec._display_text(228984000000.0, "USD") == "$228.98B"
    assert sec._display_text(-1.2e7, "USD") == "-$12.00M"
    assert sec._display_text(4.5612, "USD/shares") == "$4.56"


def _stack(**overrides):
    grid = {"source_freshness": overrides.pop("freshness", [])}
    signals = {"regime": overrides.pop("regime", None), "trusted": [], "tradingview": []}
    return dad._grid_decision_stack(None, {"score": 0}, grid, {}, None, signals)


def test_tradingview_silence_is_not_a_stale_source() -> None:
    out = _stack(freshness=[{"source": "TradingView", "state": "missing"},
                            {"source": "yfinance", "state": "fresh"}])
    assert not any("Stale or missing" in b for b in out["blockers"])
    out = _stack(freshness=[{"source": "TradingView", "state": "missing"},
                            {"source": "SEC_INSIDER", "state": "stale"}])
    assert any(b == "Stale or missing source rows: SEC_INSIDER." for b in out["blockers"])


def test_regime_blocker_names_the_state_and_the_recommendation() -> None:
    out = _stack(regime={"state": "FRAGILE", "grid_recommendation": "DEFENSIVE"})
    assert "Current GRID regime is FRAGILE (recommendation: DEFENSIVE)." in out["blockers"]
