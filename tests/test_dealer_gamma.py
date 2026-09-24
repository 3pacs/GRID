from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import numpy as np
import pandas as pd
import pytest

from physics.dealer_gamma import DealerGammaEngine, bs_gamma


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
) -> dict:
    chain = pd.DataFrame(rows)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: chain)
    monkeypatch.setattr(engine, "_get_spot", lambda _ticker, _snap_date: spot)

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


def _expected_gamma_flip(rows: list[dict[str, float | str]]) -> float | None:
    prices = np.linspace(SPOT * (1.0 - RANGE_PCT), SPOT * (1.0 + RANGE_PCT), N_POINTS)
    gex_values = [
        sum(_row_gex(row, spot=float(price)) for row in rows)
        for price in prices
    ]

    for i in range(1, len(gex_values)):
        prev_gex = gex_values[i - 1]
        curr_gex = gex_values[i]
        if prev_gex * curr_gex < 0:
            ratio = abs(prev_gex) / (abs(prev_gex) + abs(curr_gex) + 1e-12)
            return float(prices[i - 1] + ratio * (prices[i] - prices[i - 1]))

    return None


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
    expected_flip = _expected_gamma_flip(rows)

    profile = _compute_profile(monkeypatch, rows)

    assert expected_flip is not None
    assert profile["gamma_flip"] == pytest.approx(round(expected_flip, 2), abs=0.01)
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


# ── _get_spot: real spot only, never an options strike ────────────────────


def _mock_connect(engine: DealerGammaEngine, fetchone_results: list) -> MagicMock:
    """Wire ``engine.engine.connect()`` to a context manager whose
    successive ``execute(...).fetchone()`` calls return ``fetchone_results``
    in order."""
    mock_conn = MagicMock()
    mock_conn.__enter__ = MagicMock(return_value=mock_conn)
    mock_conn.__exit__ = MagicMock(return_value=False)
    mock_conn.execute.return_value.fetchone.side_effect = fetchone_results
    engine.engine.connect = MagicMock(return_value=mock_conn)
    return mock_conn


def test_get_spot_reads_options_daily_signals_as_primary_source() -> None:
    engine = DealerGammaEngine(MagicMock())
    _mock_connect(engine, [(773.38,)])

    spot = engine._get_spot("SPY", date(2026, 9, 22))

    assert spot == 773.38


def test_get_spot_falls_back_to_resolved_series_when_no_daily_signal_row() -> None:
    engine = DealerGammaEngine(MagicMock())
    _mock_connect(engine, [None, (767.81,)])

    spot = engine._get_spot("SPY", date(2026, 9, 24))

    assert spot == 767.81


def test_get_spot_returns_none_and_never_queries_a_strike_when_no_source_has_data() -> None:
    """No options_daily_signals row and no resolved_series row -> None.

    Also proves there is no third, strike-based fallback query left: the
    old bug used the highest-open-interest CALL strike as a fake spot.
    """
    engine = DealerGammaEngine(MagicMock())
    mock_conn = _mock_connect(engine, [None, None])

    spot = engine._get_spot("SPY", date(2026, 9, 24))

    assert spot is None
    assert mock_conn.execute.call_count == 2
    executed_sql = " ".join(str(c.args[0]) for c in mock_conn.execute.call_args_list)
    assert "options_snapshots" not in executed_sql
    assert "open_interest" not in executed_sql


def test_compute_gex_profile_unavailable_when_spot_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No measured spot -> an explicit unavailable result: no regime, no
    flip, no walls, nothing fabricated — never a strike-derived number."""
    rows = _base_rows()
    chain = pd.DataFrame(rows)
    engine = DealerGammaEngine(MagicMock(), risk_free_rate=RISK_FREE_RATE)
    monkeypatch.setattr(engine, "_load_chain", lambda _ticker, _snap_date: chain)
    monkeypatch.setattr(engine, "_get_spot", lambda _ticker, _snap_date: None)

    result = engine.compute_gex_profile("XYZ", SNAP_DATE)

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["reason"]
    assert "error" in result  # legacy key several existing callers still check
    for field in (
        "regime", "gamma_flip", "gamma_wall", "put_wall", "call_wall",
        "gex_aggregate", "gex_normalized", "dealer_delta",
        "vanna_exposure", "charm_exposure", "profile", "per_strike", "spot",
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
    """The regime label (from the per-strike aggregate) and the gamma flip
    (from the vectorized per-contract curve) must agree: a spot clearly
    above the flip must be LONG_GAMMA with GEX > 0, and that same GEX must
    equal the vectorized per-contract GEX at that exact spot."""
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
    monkeypatch.setattr(engine, "_load_chain", lambda _t, _d: chain)
    monkeypatch.setattr(engine, "_get_spot", lambda _t, _d: SPOT)

    profile = engine.compute_gex_profile("XYZ", SNAP_DATE, spot_range_pct=RANGE_PCT, n_points=N_POINTS)
    vectorized_at_spot = engine._gex_at_spot(chain, SPOT)

    assert profile["gex_aggregate"] == pytest.approx(round(vectorized_at_spot, 0), abs=1.0)
    assert (profile["gex_aggregate"] > 0) == (vectorized_at_spot > 0)
    assert profile["regime"] == "LONG_GAMMA"
    assert profile["gex_aggregate"] > 0
    assert profile["gamma_flip"] is not None
    assert SPOT > profile["gamma_flip"]  # spot is on the long-gamma side of the flip
