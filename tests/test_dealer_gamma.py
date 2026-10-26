from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from physics.dealer_gamma import FLIP_SEARCH_STEP_PCT, DealerGammaEngine, bs_gamma

SNAP_DATE = date(2026, 1, 15)
RISK_FREE_RATE = 0.01
SPOT = 100.0
RANGE_PCT = 0.20
N_POINTS = 81


def _base_rows(
    *, open_interest_multiplier: float = 1.0, swap_option_types: bool = False
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = [
        {
            "strike": 90.0,
            "opt_type": "call",
            "open_interest": 1_000.0,
            "implied_volatility": 0.30,
            "dte": 30.0,
        },
        {
            "strike": 90.0,
            "opt_type": "put",
            "open_interest": 4_000.0,
            "implied_volatility": 0.30,
            "dte": 30.0,
        },
        {
            "strike": 100.0,
            "opt_type": "call",
            "open_interest": 10_000.0,
            "implied_volatility": 0.25,
            "dte": 30.0,
        },
        {
            "strike": 100.0,
            "opt_type": "put",
            "open_interest": 1_000.0,
            "implied_volatility": 0.25,
            "dte": 30.0,
        },
        {
            "strike": 110.0,
            "opt_type": "call",
            "open_interest": 3_000.0,
            "implied_volatility": 0.30,
            "dte": 30.0,
        },
        {
            "strike": 110.0,
            "opt_type": "put",
            "open_interest": 500.0,
            "implied_volatility": 0.30,
            "dte": 30.0,
        },
    ]

    for row in rows:
        row["open_interest"] = float(row["open_interest"]) * open_interest_multiplier
        if swap_option_types:
            row["opt_type"] = "put" if row["opt_type"] == "call" else "call"

    return rows


def _compute_profile(
    monkeypatch: pytest.MonkeyPatch,
    rows: list[dict[str, float | str]],
    *,
    spot: float = SPOT,
    risk_free_rate: float = RISK_FREE_RATE,
) -> dict:
    chain = pd.DataFrame(rows)
    chain_time = datetime.combine(SNAP_DATE, datetime.min.time(), timezone.utc) + timedelta(hours=19)
    chain.attrs.update(snap_date=SNAP_DATE, created_at_min=chain_time,
                       created_at_max=chain_time,
                       batch_id="11111111-1111-4111-8111-111111111111",
                       capture_ordinal=1,
                       capture_started_at=chain_time,
                       capture_completed_at=chain_time + timedelta(minutes=1),
                       provider_regular_market_at_min=chain_time - timedelta(hours=2),
                       provider_regular_market_at_max=chain_time - timedelta(hours=2))
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=risk_free_rate)
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: chain)
    monkeypatch.setattr(engine, "_get_spot_receipt", lambda _ticker, _time: {
        "price": spot, "receipt_id": 1,
        "obs_date": SNAP_DATE - timedelta(days=1),
        "available_at": chain_time - timedelta(hours=1),
        "receipt_created_at": chain_time - timedelta(minutes=30),
        "release_date": SNAP_DATE, "vintage_date": SNAP_DATE,
    })

    return engine.compute_gex_profile(
        "XYZ",
        SNAP_DATE,
        spot_range_pct=RANGE_PCT,
        n_points=N_POINTS,
    )


def _row_gex(row: dict[str, float | str], spot: float = SPOT) -> float:
    # Standard convention: dealers modeled LONG calls (+1) / SHORT puts (-1).
    # See DEALER_CALL_SIGN / DEALER_PUT_SIGN in physics/dealer_gamma.py.
    option_sign = 1.0 if row["opt_type"] == "call" else -1.0
    gamma = bs_gamma(
        spot,
        float(row["strike"]),
        float(row["dte"]) / 365.0,
        RISK_FREE_RATE,
        float(row["implied_volatility"]),
    )
    return option_sign * gamma * float(row["open_interest"]) * 100.0 * spot


def _expected_per_strike(rows: list[dict[str, float | str]]) -> dict[float, dict[str, float]]:
    expected: dict[float, dict[str, float]] = {}
    for row in rows:
        strike = float(row["strike"])
        bucket = expected.setdefault(
            strike,
            {"call_gex": 0.0, "put_gex": 0.0, "net_gex": 0.0},
        )
        key = "call_gex" if row["opt_type"] == "call" else "put_gex"
        row_gex = _row_gex(row)
        bucket[key] += row_gex
        bucket["net_gex"] += row_gex
    return expected


def _expected_gamma_flip(rows: list[dict[str, float | str]]) -> tuple[float | None, int]:
    """Independent reimplementation of _find_gamma_flip's fine search grid
    (FLIP_SEARCH_STEP_PCT of SPOT per step — NOT the coarser RANGE_PCT/
    N_POINTS used elsewhere in this file for the chart-oriented profile
    curve) and its nearest-to-spot crossing selection."""
    lo = SPOT * (1.0 - RANGE_PCT)
    hi = SPOT * (1.0 + RANGE_PCT)
    step = SPOT * FLIP_SEARCH_STEP_PCT
    n = max(round((hi - lo) / step) + 1, 2)
    prices = np.linspace(lo, hi, n)
    gex_values = [
        sum(_row_gex(row, spot=float(price)) for row in rows)
        for price in prices
    ]

    crossings: list[float] = []
    for i in range(1, len(gex_values)):
        prev_gex = gex_values[i - 1]
        curr_gex = gex_values[i]
        if prev_gex * curr_gex < 0:
            ratio = abs(prev_gex) / (abs(prev_gex) + abs(curr_gex) + 1e-12)
            crossings.append(float(prices[i - 1] + ratio * (prices[i] - prices[i - 1])))

    if not crossings:
        return None, 0
    nearest = min(crossings, key=lambda p: abs(p - SPOT))
    return nearest, len(crossings)


def test_compute_gex_profile_aggregates_per_strike_and_selects_walls(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _base_rows()
    profile = _compute_profile(monkeypatch, rows)

    expected = _expected_per_strike(rows)
    by_strike = {row["strike"]: row for row in profile["per_strike"]}

    assert profile["gex_aggregate"] == round(
        sum(bucket["net_gex"] for bucket in expected.values()), 0
    )
    assert set(by_strike) == {90.0, 100.0, 110.0}

    for strike, bucket in expected.items():
        actual = by_strike[strike]
        assert actual["call_gex"] == pytest.approx(bucket["call_gex"], abs=1e-3)
        assert actual["put_gex"] == pytest.approx(bucket["put_gex"], abs=1e-3)
        assert actual["net_gex"] == pytest.approx(bucket["net_gex"], abs=1e-3)

    assert profile["gamma_wall"] == 100.0
    assert profile["put_wall"] == 90.0
    assert profile["call_wall"] == 100.0


def test_compute_gex_profile_interpolates_gamma_flip_from_crossing_grid(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rows = _base_rows()
    expected_flip, expected_crossings = _expected_gamma_flip(rows)

    profile = _compute_profile(monkeypatch, rows)

    assert expected_flip is not None
    assert profile["gamma_flip"] == pytest.approx(round(expected_flip, 2), abs=0.01)
    assert profile["gamma_flip_crossings"] == expected_crossings
    assert profile["gamma_flip_crossings"] == 1  # unambiguous for this simple fixture
    # Dealers long calls / short puts: below the flip GEX is negative
    # (short gamma), above it GEX is positive (long gamma).
    assert profile["profile"][0]["gex"] < 0
    assert profile["profile"][-1]["gex"] > 0


@pytest.mark.parametrize(
    ("open_interest_multiplier", "swap_option_types", "expected_regime"),
    [
        (1.0, False, "NEUTRAL"),
        # Base fixture (heavy call OI at the dominant strike) -> dealers
        # long calls net long gamma once OI is scaled up.
        (20.0, False, "LONG_GAMMA"),
        # Swapped call/put labels flip which leg carries the heavy OI ->
        # dealers net short gamma.
        (20.0, True, "SHORT_GAMMA"),
    ],
)
def test_compute_gex_profile_tags_regime_from_normalized_gex(
    monkeypatch: pytest.MonkeyPatch,
    open_interest_multiplier: float,
    swap_option_types: bool,
    expected_regime: str,
) -> None:
    rows = _base_rows(
        open_interest_multiplier=open_interest_multiplier,
        swap_option_types=swap_option_types,
    )

    profile = _compute_profile(monkeypatch, rows)

    assert profile["regime"] == expected_regime
    if expected_regime == "LONG_GAMMA":
        assert profile["gex_normalized"] > 0.5
    elif expected_regime == "SHORT_GAMMA":
        assert profile["gex_normalized"] < -0.5
    else:
        assert abs(profile["gex_normalized"]) < 0.5


# ── Sign-convention regression coverage (2026-09-24 GEX sign fix) ─────────


def test_calls_only_chain_gives_positive_gex_and_long_gamma_regime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A chain with only calls: dealers modeled long calls -> GEX > 0,
    regime LONG_GAMMA, and no put_wall (there is no put leg at all)."""
    rows = [
        {"strike": 100.0, "opt_type": "call", "open_interest": 200_000.0,
         "implied_volatility": 0.25, "dte": 30.0},
    ]
    profile = _compute_profile(monkeypatch, rows)

    assert profile["gex_aggregate"] > 0
    assert profile["regime"] == "LONG_GAMMA"
    assert profile["put_wall"] is None
    assert profile["call_wall"] == 100.0


def test_puts_only_chain_gives_negative_gex_and_short_gamma_regime(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A chain with only puts: dealers modeled short puts -> GEX < 0,
    regime SHORT_GAMMA, and no call_wall (there is no call leg at all)."""
    rows = [
        {"strike": 100.0, "opt_type": "put", "open_interest": 200_000.0,
         "implied_volatility": 0.25, "dte": 30.0},
    ]
    profile = _compute_profile(monkeypatch, rows)

    assert profile["gex_aggregate"] < 0
    assert profile["regime"] == "SHORT_GAMMA"
    assert profile["call_wall"] is None
    assert profile["put_wall"] == 100.0


def test_gex_negative_below_flip_positive_above_with_puts_low_calls_high(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Puts clustered below spot, calls clustered above: near the low end
    of the profile range the nearby put leg dominates (GEX < 0, short
    gamma); near the high end the nearby call leg dominates (GEX > 0,
    long gamma) — the textbook shape the dealers-long-calls/short-puts
    convention predicts, and the opposite of what the pre-fix sign gave."""
    rows = [
        {"strike": 90.0, "opt_type": "put", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 95.0, "opt_type": "put", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 105.0, "opt_type": "call", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 110.0, "opt_type": "call", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
    ]
    profile = _compute_profile(monkeypatch, rows)

    assert profile["profile"][0]["gex"] < 0    # low end of range: puts dominate
    assert profile["profile"][-1]["gex"] > 0   # high end of range: calls dominate
    assert profile["gamma_flip"] is not None
    assert profile["put_wall"] is not None and profile["put_wall"] < SPOT
    assert profile["call_wall"] is not None and profile["call_wall"] > SPOT


def test_compute_gex_profile_unavailable_when_spot_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No measured spot -> an explicit unavailable result: no regime, no
    flip, no walls, nothing fabricated — never a strike-derived number."""
    rows = _base_rows()
    chain = pd.DataFrame(rows)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: chain)
    monkeypatch.setattr(engine, "_get_spot_receipt", lambda _ticker, _time: None)

    result = engine.compute_gex_profile("XYZ", SNAP_DATE)

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["reason"]
    assert "error" in result  # legacy key several existing callers still check
    for field in (
        "regime", "gamma_flip", "gamma_flip_crossings", "gamma_wall",
        "put_wall", "call_wall", "gex_aggregate", "gex_normalized",
        "dealer_delta", "vanna_exposure", "charm_exposure", "profile",
        "per_strike", "spot",
    ):
        assert result[field] is None


# ── Per-contract pricing regression coverage (2026-09-24 multi-expiry fix) ─
#
# _compute_per_strike used to `groupby("strike")` and take ONE row's dte for
# ALL of that strike's open interest, regardless of how many distinct
# expiries shared the strike. That badly misprices gamma whenever a strike
# carries both near-dated and far-dated open interest, and it made
# gex_aggregate (per-strike-grouped) computable from a different pricing
# than gamma_flip/the profile curve (_gex_at_spots_vectorized, always
# per-contract) — regime and flip could come from two different
# calculations. Fixed to price every contract (row) with its own dte/IV and
# aggregate signed contract-level GEX up to the strike.


def test_gex_aggregate_matches_vectorized_per_contract_computation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """sum(per_strike net_gex) must equal the fully-vectorized per-contract
    GEX at the same spot — the two code paths price the same chain the same
    way. A chain with strikes spanning multiple expiries is exactly the
    case that used to make them disagree."""
    rows = [
        {"strike": 90.0, "opt_type": "put", "open_interest": 3_000.0,
         "implied_volatility": 0.28, "dte": 5.0},
        {"strike": 90.0, "opt_type": "put", "open_interest": 1_500.0,
         "implied_volatility": 0.32, "dte": 90.0},
        {"strike": 100.0, "opt_type": "call", "open_interest": 4_000.0,
         "implied_volatility": 0.22, "dte": 2.0},
        {"strike": 100.0, "opt_type": "call", "open_interest": 2_500.0,
         "implied_volatility": 0.26, "dte": 45.0},
        {"strike": 100.0, "opt_type": "put", "open_interest": 1_200.0,
         "implied_volatility": 0.24, "dte": 45.0},
        {"strike": 110.0, "opt_type": "call", "open_interest": 2_000.0,
         "implied_volatility": 0.30, "dte": 20.0},
    ]
    chain = pd.DataFrame(rows)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)

    per_strike = engine._compute_per_strike(chain, SPOT)
    grouped_total = sum(s["net_gex"] for s in per_strike)
    vectorized_total = engine._gex_at_spot(chain, SPOT)

    assert grouped_total == pytest.approx(vectorized_total, rel=1e-9, abs=1.0)


def test_mixed_expiry_strike_prices_each_contract_with_its_own_dte(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A strike holding a 1-DTE contract and a 60-DTE contract must price
    each with its own T, not one dte applied to the strike's whole OI."""
    rows = [
        {"strike": 100.0, "opt_type": "call", "open_interest": 1_000.0,
         "implied_volatility": 0.25, "dte": 1.0},
        {"strike": 100.0, "opt_type": "call", "open_interest": 4_000.0,
         "implied_volatility": 0.25, "dte": 60.0},
    ]
    profile = _compute_profile(monkeypatch, rows)

    expected_call_gex = (
        bs_gamma(SPOT, 100.0, 1.0 / 365.0, RISK_FREE_RATE, 0.25) * 1_000.0 * 100.0 * SPOT
        + bs_gamma(SPOT, 100.0, 60.0 / 365.0, RISK_FREE_RATE, 0.25) * 4_000.0 * 100.0 * SPOT
    )
    # The old bug would have priced ALL 5,000 OI at whichever dte pandas'
    # groupby happened to see first for this strike — a materially
    # different (much larger, since it skews toward the near-dated 1-DTE
    # gamma) number than pricing each leg at its own T.
    wrongly_priced_at_1dte = bs_gamma(
        SPOT, 100.0, 1.0 / 365.0, RISK_FREE_RATE, 0.25
    ) * 5_000.0 * 100.0 * SPOT

    by_strike = {row["strike"]: row for row in profile["per_strike"]}
    actual_call_gex = by_strike[100.0]["call_gex"]

    assert actual_call_gex == pytest.approx(expected_call_gex, rel=1e-6)
    assert actual_call_gex != pytest.approx(wrongly_priced_at_1dte, rel=1e-3)
    # Nearest expiry at the strike is reported, not an arbitrary one.
    assert by_strike[100.0]["dte"] == 1.0


def test_walls_do_not_collapse_onto_atm_strike_when_most_oi_is_long_dated(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tiny near-0DTE cluster sitting at the ATM strike must not drag
    put_wall/call_wall onto that strike when the real open-interest
    concentration is elsewhere and correctly-dated. Before the fix, the
    ATM strike's small near-dated OI and its much larger long-dated OI at
    the same strike were both mispriced at the near-dated T, inflating
    that strike's gamma exposure past the strikes that actually carry the
    bulk of the (correctly near-dated) open interest."""
    rows = [
        # ATM strike: a small 1-DTE cluster plus a larger, but still
        # modest, 180-DTE cluster — under the old bug both would have been
        # priced at 1-DTE.
        {"strike": SPOT, "opt_type": "call", "open_interest": 200.0,
         "implied_volatility": 0.25, "dte": 1.0},
        {"strike": SPOT, "opt_type": "put", "open_interest": 200.0,
         "implied_volatility": 0.25, "dte": 1.0},
        {"strike": SPOT, "opt_type": "call", "open_interest": 2_000.0,
         "implied_volatility": 0.25, "dte": 180.0},
        {"strike": SPOT, "opt_type": "put", "open_interest": 2_000.0,
         "implied_volatility": 0.25, "dte": 180.0},
        # The real, correctly near-dated open-interest concentration, away
        # from spot, an order of magnitude larger than the ATM strike's.
        {"strike": 90.0, "opt_type": "put", "open_interest": 100_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 110.0, "opt_type": "call", "open_interest": 100_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
    ]
    profile = _compute_profile(monkeypatch, rows)

    assert profile["put_wall"] == 90.0
    assert profile["call_wall"] == 110.0
    assert profile["put_wall"] != SPOT
    assert profile["call_wall"] != SPOT


def test_regime_sign_matches_which_side_of_the_flip_spot_is_on(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """This fixture has positive GEX above its flip, and both pricing paths
    must agree at spot. The direction is chain-specific, not a flip rule."""
    rows = [
        {"strike": 90.0, "opt_type": "put", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 95.0, "opt_type": "put", "open_interest": 5_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 105.0, "opt_type": "call", "open_interest": 300_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
        {"strike": 110.0, "opt_type": "call", "open_interest": 300_000.0,
         "implied_volatility": 0.30, "dte": 30.0},
    ]
    chain = pd.DataFrame(rows)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    profile = _compute_profile(monkeypatch, rows)
    vectorized_at_spot = engine._gex_at_spot(chain, SPOT)

    assert profile["gex_aggregate"] == pytest.approx(round(vectorized_at_spot, 0), abs=1.0)
    assert (profile["gex_aggregate"] > 0) == (vectorized_at_spot > 0)
    assert profile["regime"] == "LONG_GAMMA"
    assert profile["gex_aggregate"] > 0
    assert profile["gamma_flip"] is not None
    assert SPOT > profile["gamma_flip"]  # true for this fixture only


def test_single_flip_can_have_negative_gex_above_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Real Black-Scholes chain disproves above-flip implies long gamma."""
    rows = [
        {"strike": 95.0, "opt_type": "call", "open_interest": 1000.0,
         "implied_volatility": 0.2, "dte": 30.0},
        {"strike": 105.0, "opt_type": "put", "open_interest": 1000.0,
         "implied_volatility": 0.2, "dte": 30.0},
    ]
    profile = _compute_profile(monkeypatch, rows, spot=102.0, risk_free_rate=0.05)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=0.05)
    chain = pd.DataFrame(rows)

    assert profile["gamma_flip_crossings"] == 1
    assert profile["gamma_flip"] == pytest.approx(99.30, abs=0.02)
    assert profile["spot"] > profile["gamma_flip"]
    assert profile["gex_aggregate"] < 0
    assert engine._gex_at_spot(chain, 95.0) > 0
    assert engine._gex_at_spot(chain, 102.0) < 0


# ── _find_gamma_flip: nearest-to-spot selection (2026-09-24 fine-grid fix) ─
#
# The flip search used to scan a coarse grid (the same n_points as the
# chart-oriented profile curve) and return the FIRST sign crossing found
# scanning from the low end of the range. Real chains routinely cross zero
# more than once; the first crossing from far below spot is not necessarily
# the one that describes dealer positioning at today's spot. Fixed to scan
# a fine, fixed-resolution grid (FLIP_SEARCH_STEP_PCT of spot) and return
# the crossing NEAREST spot, plus how many crossings it found.


def _patch_vectorized_gex(
    monkeypatch: pytest.MonkeyPatch, engine: DealerGammaEngine, gex_fn,
) -> None:
    """Stub out the chain-array prep + vectorized evaluator so
    _find_gamma_flip's grid/selection logic can be tested against a
    hand-crafted GEX(spot) curve, independent of chain/Greek plumbing."""
    monkeypatch.setattr(engine, "_prepare_chain_arrays", lambda _chain: (None,) * 5)
    monkeypatch.setattr(engine, "_gex_at_spots_vectorized", gex_fn)


def test_find_gamma_flip_returns_crossing_nearest_spot_not_first_from_bottom(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Two crossings in range: one far from spot (~92), one near it
    (~99). The flip must be the near one, not the first-from-the-bottom
    (~92) one the old code would have returned."""
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    spot = 100.0

    def fake_gex(_strikes, _T, _iv, _oi, _sign, spots):
        return np.array([
            -1_000_000.0 if p < 92.0 else (1_000_000.0 if p < 99.0 else -500_000.0)
            for p in spots
        ])

    _patch_vectorized_gex(monkeypatch, engine, fake_gex)

    flip, crossings = engine._find_gamma_flip(pd.DataFrame(), spot, 0.10)

    assert crossings == 2
    assert flip is not None
    assert 98.5 < flip < 99.5  # the crossing near spot, not the ~92 one
    assert abs(flip - spot) < abs(92.0 - spot)


def test_find_gamma_flip_no_crossing_returns_none_and_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Aggregate GEX the same sign throughout the range -> (None, 0), never
    a fabricated price."""
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)

    def fake_gex_always_positive(_strikes, _T, _iv, _oi, _sign, spots):
        return np.full(len(spots), 1_000_000.0)

    _patch_vectorized_gex(monkeypatch, engine, fake_gex_always_positive)

    flip, crossings = engine._find_gamma_flip(pd.DataFrame(), 100.0, 0.10)

    assert flip is None
    assert crossings == 0


@pytest.mark.parametrize(
    ("curve", "expected_crossings"),
    [
        (lambda prices: prices - 100.0, 1),
        (lambda prices: 100.0 - prices, 1),
        (lambda prices: (prices - 100.0) ** 2, 0),
        (lambda prices: -(prices - 100.0) ** 2, 0),
        (lambda prices: np.zeros_like(prices), 0),
        (lambda prices: np.where(abs(prices - 100.0) <= 0.1, 0, prices - 100.0), 1),
    ],
)
def test_find_gamma_flip_handles_exact_grid_zero_and_touching_zero(
    monkeypatch: pytest.MonkeyPatch, curve, expected_crossings: int,
) -> None:
    engine = DealerGammaEngine(MagicMock())
    _patch_vectorized_gex(monkeypatch, engine,
                          lambda _strikes, _T, _iv, _oi, _sign, prices: curve(prices))

    flip, crossings = engine._find_gamma_flip(pd.DataFrame(), 100.0, 0.10)

    assert crossings == expected_crossings
    if expected_crossings:
        assert flip == pytest.approx(100.0, abs=0.11)
    else:
        assert flip is None


def test_find_gamma_flip_uses_fine_grid_independent_of_n_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """_find_gamma_flip takes no n_points argument — its grid resolution is
    fixed (FLIP_SEARCH_STEP_PCT of spot), not tied to the profile curve's
    (much coarser) point count. A 30% range at 0.1% steps is ~300 points,
    materially finer than the old 50-point default."""
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    spot = 100.0
    range_pct = 0.15
    seen_lengths: list[int] = []

    def fake_gex(_strikes, _T, _iv, _oi, _sign, spots):
        seen_lengths.append(len(spots))
        return np.array([-1.0 if p < spot else 1.0 for p in spots])  # one crossing, at spot

    _patch_vectorized_gex(monkeypatch, engine, fake_gex)

    engine._find_gamma_flip(pd.DataFrame(), spot, range_pct)

    assert seen_lengths == [round(2 * range_pct / FLIP_SEARCH_STEP_PCT) + 1]
    assert seen_lengths[0] > 250
