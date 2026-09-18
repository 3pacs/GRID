"""tests/godview/test_dealer_gex_validation.py

Validates ``godview/dealer_gex_pillar.py``'s dealer-GEX engine against a REAL,
sourced options-chain fixture -- ``tests/godview/fixtures/
options_chain_SPY_20260918.json`` (provenance in the sidecar
``.SOURCE.md`` next to it: a live SPY chain pulled via ``yfinance``
1.7.0, a repo dependency, no credentials, expiry 2026-10-16, captured
2026-09-18) -- using an INDEPENDENTLY WRITTEN expected-value calculation.

This module never imports the engine's gamma/GEX helper functions
(``black_scholes_gamma``, ``contract_dollar_gamma``, ``signed_gamma_contribution``,
``compute_cumulative_by_strike``, ``find_gamma_flip``, ``compute_max_pain``,
``compute_put_call_oi_ratio``, ``resolve_atm_iv``, ``coverage_fraction``) for the
expected side. It re-implements every one of them from scratch below, including
its own ``math.erf``-based normal pdf/cdf, and only then calls the engine's
``_compute_gex_for_chain`` -- the actual function under test -- on the identical
input rows, and compares.

WHAT THIS PROVES: that ``dealer_gex_pillar.py`` computes NUMERICALLY CORRECT
Black-Scholes gamma, net/call/put GEX, gamma-flip strike, max pain, put/call OI
ratio, and ATM IV -- under its own STATED, DISCLOSED assumptions (r=q=0, chain-
reported implied_vol, the call-positive/put-negative sign convention, 100-share
contract multiplier) -- on a real, sourced options chain.

WHAT THIS DOES NOT PROVE: that any of these numbers describe actual dealer
positioning. ``options_snapshots`` (the only options data this codebase has)
carries no dealer-vs-customer position split at all -- there is no column, no
table, nowhere in this database that records who actually holds which side of
an option. The "dealers are short both sides" convention is, and remains, a
MODELED assumption, not a measurement. Every field this pillar produces stays
``provenance='modeled'`` for exactly that reason, and the API
(``api/routers/godview_pillars.py::get_dealer_gex_pillar`` /
``_gex_field_records``) must keep saying so -- checked below with a real
assertion, not just prose (see ``test_router_still_reports_gex_fields_as_modeled``).
"""

from __future__ import annotations

import json
import math
import os
from datetime import date
from pathlib import Path
from typing import Any

import pytest

from godview.dealer_gex_pillar import (
    CONTRACT_MULTIPLIER as ENGINE_CONTRACT_MULTIPLIER,
    DIVIDEND_YIELD as ENGINE_DIVIDEND_YIELD,
    RISK_FREE_RATE as ENGINE_RISK_FREE_RATE,
    UNIT_USD_PER_1PCT_MOVE as ENGINE_UNIT,
    _compute_gex_for_chain,
)

FIXTURE_PATH = Path(__file__).resolve().parent / "fixtures" / "options_chain_SPY_20260918.json"

# ---------------------------------------------------------------------------
# Named assumption constants, stated explicitly (per the validation task).
# These are declared independently here and only CROSS-CHECKED against the
# engine's own constants (test_engine_constants_match_stated_assumptions) --
# never derived from them.
# ---------------------------------------------------------------------------

EXPECTED_CONTRACT_MULTIPLIER = 100  # shares per standard options contract, FINRA
EXPECTED_CALL_SIGN = 1.0  # dealers modeled short the call side of customer flow -> +gamma
EXPECTED_PUT_SIGN = -1.0  # dealers modeled short the put side of customer flow -> -gamma
EXPECTED_RISK_FREE_RATE = 0.0  # r, assumed 0 -- matches the engine's stated simplification
EXPECTED_DIVIDEND_YIELD = 0.0  # q, assumed 0 -- matches the engine's stated simplification
EXPECTED_IV_SOURCE_NOTE = (
    "implied_vol taken directly from the fixture's chain-reported value "
    "(Yahoo's own IV solve); never re-solved for or defaulted here"
)
EXPECTED_SKIP_RULE_NOTE = (
    "a row with missing/non-positive implied_vol, or expiry <= snap_date "
    "(non-positive time-to-expiry), is skipped -- exactly the engine's own rule"
)
EXPECTED_UNIT = "usd_per_1pct_move"  # dollar gamma per 1% underlying move, pre-$M scaling
TOLERANCE_REL = 1e-6  # relative tolerance: both sides run the identical formula in
# a different code path (different loop/dict-insertion order), so floating-point
# summation order can differ at the ~1e-13-relative level; 1e-6 is generous headroom
# on top of that noise floor while still catching any real algorithmic disagreement.


def test_engine_constants_match_stated_assumptions():
    """The engine's own module-level constants must equal the assumptions this
    test declares independently above -- read for comparison only, never used
    to compute the expected side below."""
    assert ENGINE_CONTRACT_MULTIPLIER == EXPECTED_CONTRACT_MULTIPLIER
    assert ENGINE_RISK_FREE_RATE == EXPECTED_RISK_FREE_RATE
    assert ENGINE_DIVIDEND_YIELD == EXPECTED_DIVIDEND_YIELD
    assert ENGINE_UNIT == EXPECTED_UNIT


# ---------------------------------------------------------------------------
# Independent Black-Scholes gamma: own code, math.erf-based normal pdf/cdf.
# Not imported from godview/dealer_gex_pillar.py at all.
# ---------------------------------------------------------------------------


def own_norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erf. Gamma itself only needs the pdf below;
    this is included to independently re-derive the erf-based N(x) building
    block the rest of Black-Scholes (delta, price) would need, and is
    deliberately left unused by the gamma formula itself."""
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def own_norm_pdf(x: float) -> float:
    """Standard normal pdf, derived from erf's own defining relation
    (d/dx CDF(x) = pdf(x)) via a symmetric finite difference -- not merely
    the textbook closed form typed in independently, but numerically pinned
    to the erf-based CDF above, for a genuinely independent construction."""
    h = 1e-6
    return (own_norm_cdf(x + h) - own_norm_cdf(x - h)) / (2 * h)


def own_black_scholes_gamma(spot: float, strike: float, t_years: float, sigma: float) -> float | None:
    if spot <= 0 or strike <= 0 or t_years <= 0 or sigma <= 0:
        return None
    sqrt_t = math.sqrt(t_years)
    d1 = (
        math.log(spot / strike)
        + (EXPECTED_RISK_FREE_RATE - EXPECTED_DIVIDEND_YIELD + 0.5 * sigma * sigma) * t_years
    ) / (sigma * sqrt_t)
    denom = spot * sigma * sqrt_t
    if denom <= 0:
        return None
    discount = math.exp(-EXPECTED_DIVIDEND_YIELD * t_years)
    return discount * own_norm_pdf(d1) / denom


def own_contract_dollar_gamma(spot: float, gamma: float, open_interest: float) -> float:
    return gamma * spot * spot * 0.01 * EXPECTED_CONTRACT_MULTIPLIER * open_interest


def own_gamma_flip(cumulative: list[tuple[float, float]]) -> float | None:
    """Own linear-interpolation zero crossing, own loop -- no shared code with
    godview.dealer_gex_pillar.find_gamma_flip."""
    for i in range(len(cumulative) - 1):
        s1, c1 = cumulative[i]
        s2, c2 = cumulative[i + 1]
        if c1 == 0:
            return s1
        if (c1 < 0) != (c2 < 0):
            if c2 == c1:
                continue
            frac = -c1 / (c2 - c1)
            return s1 + frac * (s2 - s1)
    return None


def own_max_pain(rows: list[dict[str, Any]]) -> float | None:
    strikes = sorted({r["strike"] for r in rows})
    if not strikes:
        return None
    best_k, best_payout = None, None
    for k in strikes:
        payout = 0.0
        for r in rows:
            oi = r.get("open_interest") or 0.0
            if r["opt_type"] == "call":
                payout += oi * max(k - r["strike"], 0.0)
            else:
                payout += oi * max(r["strike"] - k, 0.0)
        if best_payout is None or payout < best_payout:
            best_payout, best_k = payout, k
    return best_k


def own_put_call_oi_ratio(rows: list[dict[str, Any]]) -> float | None:
    call_oi = sum((r.get("open_interest") or 0.0) for r in rows if r["opt_type"] == "call")
    put_oi = sum((r.get("open_interest") or 0.0) for r in rows if r["opt_type"] == "put")
    if call_oi <= 0:
        return None
    return put_oi / call_oi


def own_atm_iv(rows: list[dict[str, Any]], spot: float) -> float | None:
    usable = [r for r in rows if r.get("implied_vol") is not None]
    if not usable:
        return None
    best = None
    best_dist = None
    for r in usable:
        dist = abs(r["strike"] - spot)
        if best_dist is None or dist < best_dist:
            best, best_dist = r, dist
    return best["implied_vol"]


def expected_gex_for_chain(rows: list[dict[str, Any]], spot: float, snap_date: date) -> dict[str, Any]:
    """Fully independent expected-value calculation, mirroring the engine's
    documented rules (skip missing/non-positive IV or non-positive T; never
    default IV; sum signed per-strike dollar gamma; USD per 1% underlying
    move; contract multiplier 100) but using none of its code."""
    strike_net_gamma: dict[float, float] = {}
    call_gex_total = 0.0
    put_gex_total = 0.0
    used = 0

    for r in rows:
        iv = r.get("implied_vol")
        oi = r.get("open_interest") or 0.0
        t_years = (r["expiry"] - snap_date).days / 365.25
        if iv is None or iv <= 0 or t_years <= 0:
            continue
        gamma = own_black_scholes_gamma(spot, r["strike"], t_years, iv)
        if gamma is None:
            continue
        dollar_gamma = own_contract_dollar_gamma(spot, gamma, oi)
        sign = EXPECTED_CALL_SIGN if r["opt_type"] == "call" else EXPECTED_PUT_SIGN
        strike_net_gamma[r["strike"]] = strike_net_gamma.get(r["strike"], 0.0) + sign * dollar_gamma
        if r["opt_type"] == "call":
            call_gex_total += dollar_gamma
        else:
            put_gex_total += dollar_gamma
        used += 1

    running = 0.0
    cumulative: list[tuple[float, float]] = []
    for strike in sorted(strike_net_gamma):
        running += strike_net_gamma[strike]
        cumulative.append((strike, running))

    net_gex = cumulative[-1][1] if cumulative else 0.0
    gamma_flip = own_gamma_flip(cumulative)

    return {
        "net_gex": net_gex,
        "call_gex": call_gex_total,
        "put_gex": -put_gex_total,  # signed, matching the engine's own stored sign
        "gamma_flip_strike": gamma_flip,
        "max_pain_strike": own_max_pain(rows),
        "put_call_oi_ratio": own_put_call_oi_ratio(rows),
        "atm_iv": own_atm_iv(rows, spot),
        "contracts_used": used,
        "contracts_present": len(rows),
    }


# ---------------------------------------------------------------------------
# Fixture loading
# ---------------------------------------------------------------------------


def _load_fixture_rows() -> tuple[list[dict[str, Any]], float, date]:
    if not FIXTURE_PATH.exists():
        pytest.fail(
            f"missing sourced fixture at {FIXTURE_PATH} -- see the task's "
            "fallback rule: this test must not synthesize data and call it sourced"
        )
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    expiry = date.fromisoformat(raw["expiry"])
    snap_date = date.fromisoformat(raw["snap_date"])
    spot = float(raw["spot_price"])
    rows = [
        {
            "opt_type": r["opt_type"],
            "strike": r["strike"],
            "open_interest": r["open_interest"],
            "implied_vol": r["implied_vol"],
            "expiry": expiry,
        }
        for r in raw["rows"]
    ]
    return rows, spot, snap_date


def test_fixture_has_expected_shape():
    rows, spot, snap_date = _load_fixture_rows()
    raw = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert raw["symbol"] == "SPY"
    assert len(rows) > 100  # a real chain, not a toy handful of rows
    assert spot > 0
    assert snap_date < rows[0]["expiry"]  # single-expiry fixture, capture predates expiry
    assert any(r["opt_type"] == "call" for r in rows)
    assert any(r["opt_type"] == "put" for r in rows)


def test_expected_vs_engine_agreement():
    """The core validation: an independently computed expected result, and the
    engine's own ``_compute_gex_for_chain`` (the function under test, imported
    directly -- this IS the engine, not reimplemented), on the SAME rows,
    must agree within TOLERANCE_REL."""
    rows, spot, snap_date = _load_fixture_rows()

    expected = expected_gex_for_chain(rows, spot, snap_date)
    engine_result = _compute_gex_for_chain(rows, spot, snap_date)

    assert engine_result is not None

    assert engine_result["contracts_used"] == expected["contracts_used"]
    assert engine_result["contracts_present"] == expected["contracts_present"]

    assert engine_result["net_gex"] == pytest.approx(expected["net_gex"], rel=TOLERANCE_REL)
    assert engine_result["call_gex"] == pytest.approx(expected["call_gex"], rel=TOLERANCE_REL)
    assert engine_result["put_gex"] == pytest.approx(expected["put_gex"], rel=TOLERANCE_REL)

    if expected["gamma_flip_strike"] is None:
        assert engine_result["gamma_flip_strike"] is None
    else:
        assert engine_result["gamma_flip_strike"] == pytest.approx(
            expected["gamma_flip_strike"], rel=TOLERANCE_REL
        )

    if expected["max_pain_strike"] is None:
        assert engine_result["max_pain_strike"] is None
    else:
        assert engine_result["max_pain_strike"] == pytest.approx(expected["max_pain_strike"], rel=TOLERANCE_REL)

    if expected["put_call_oi_ratio"] is None:
        assert engine_result["put_call_oi_ratio"] is None
    else:
        assert engine_result["put_call_oi_ratio"] == pytest.approx(
            expected["put_call_oi_ratio"], rel=TOLERANCE_REL
        )

    if expected["atm_iv"] is None:
        assert engine_result["atm_iv"] is None
    else:
        assert engine_result["atm_iv"] == pytest.approx(expected["atm_iv"], rel=TOLERANCE_REL)

    print(
        "\n[dealer_gex validation] SPY, expiry from fixture, "
        f"spot={spot}, contracts_used={engine_result['contracts_used']}/"
        f"{engine_result['contracts_present']}\n"
        f"  net_gex   expected={expected['net_gex']:,.2f}  engine={engine_result['net_gex']:,.2f}\n"
        f"  call_gex  expected={expected['call_gex']:,.2f}  engine={engine_result['call_gex']:,.2f}\n"
        f"  put_gex   expected={expected['put_gex']:,.2f}  engine={engine_result['put_gex']:,.2f}\n"
        f"  gamma_flip_strike expected={expected['gamma_flip_strike']}  engine={engine_result['gamma_flip_strike']}\n"
        f"  max_pain_strike   expected={expected['max_pain_strike']}  engine={engine_result['max_pain_strike']}\n"
        f"  put_call_oi_ratio expected={expected['put_call_oi_ratio']}  engine={engine_result['put_call_oi_ratio']}\n"
        f"  atm_iv    expected={expected['atm_iv']}  engine={engine_result['atm_iv']}\n"
    )


def test_coverage_matches_independent_usable_row_count():
    """coverage_fraction = contracts_used / contracts_present must match this
    test's OWN count of usable rows under the engine's stated skip rule."""
    rows, spot, snap_date = _load_fixture_rows()
    expected = expected_gex_for_chain(rows, spot, snap_date)
    engine_result = _compute_gex_for_chain(rows, spot, snap_date)

    own_usable = 0
    for r in rows:
        iv = r.get("implied_vol")
        t_years = (r["expiry"] - snap_date).days / 365.25
        if iv is not None and iv > 0 and t_years > 0:
            own_usable += 1

    assert own_usable == expected["contracts_used"] == engine_result["contracts_used"]
    assert len(rows) == expected["contracts_present"] == engine_result["contracts_present"]


# ---------------------------------------------------------------------------
# The router must keep saying "modeled" -- checked with a real assertion.
# ---------------------------------------------------------------------------


def test_router_still_reports_gex_fields_as_modeled():
    """api/routers/godview_pillars.py::_gex_field_records must still tag every
    derived GEX field provenance='modeled' (never 'measured' or 'derived') and
    the resolved spot price provenance='measured'. If this ever regresses,
    this test -- not just the module docstring -- catches it."""
    # api.routers.godview_pillars -> api.dependencies -> db -> config.settings
    # raises at *import* time if DB_PASSWORD is unset -- same pre-existing gap
    # tests/godview/test_router_defaults_pure.py already documents and works
    # around; this test never opens a real DB connection (it only calls a pure
    # dict-building helper), so the placeholder is safe.
    os.environ.setdefault("DB_PASSWORD", "test-only-placeholder-unused")
    os.environ.setdefault("ENVIRONMENT", "development")

    from api.routers.godview_pillars import _gex_field_records

    fake_row = {
        "obs_date": date(2026, 9, 18),
        "generation_id": "test-generation",
        "availability_basis": "unknown",
        "ticker": "SPY",
        "spot_price": 761.69,
        "net_gex_usd_m": 123.4,
        "call_gex_usd_m": 200.0,
        "put_gex_usd_m": -76.6,
        "gamma_flip_strike": 755.0,
        "spot_to_flip_pct": -0.88,
        "gex_regime": "long_gamma",
        "max_pain_strike": 760.0,
        "put_call_oi_ratio": 0.9,
        "atm_iv": 0.15,
        "coverage_fraction": 1.0,
    }

    records = _gex_field_records(fake_row)

    assert records["spot_price"]["provenance"] == "measured"
    for name in (
        "net_gex_usd_m",
        "call_gex_usd_m",
        "put_gex_usd_m",
        "gamma_flip_strike",
        "spot_to_flip_pct",
        "gex_regime",
        "max_pain_strike",
        "put_call_oi_ratio",
        "atm_iv",
    ):
        assert records[name]["provenance"] == "modeled", f"{name} regressed off provenance='modeled'"
