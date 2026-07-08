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
    option_sign = -1.0 if row["opt_type"] == "call" else 1.0
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
    assert profile["profile"][0]["gex"] > 0
    assert profile["profile"][-1]["gex"] < 0


@pytest.mark.parametrize(
    ("open_interest_multiplier", "swap_option_types", "expected_regime"),
    [
        (1.0, False, "NEUTRAL"),
        (20.0, False, "SHORT_GAMMA"),
        (20.0, True, "LONG_GAMMA"),
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
