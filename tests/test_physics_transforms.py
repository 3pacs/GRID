"""Reference-value tests for public physics transform helpers."""

from __future__ import annotations

import math

import numpy as np
import pandas as pd
import pytest

from physics.transforms import (
    estimate_ou_parameters,
    hurst_exponent,
    kinetic_energy,
    market_temperature,
    potential_energy,
    total_energy,
)


def test_kinetic_energy_uses_half_squared_rolling_log_return() -> None:
    prices = pd.Series(
        [100.0, 105.0, 110.25, 115.7625],
        index=pd.date_range("2024-01-01", periods=4),
    )

    result = kinetic_energy(prices, window=2)

    expected = pd.Series(
        [np.nan, np.nan, 0.004760960239360261, 0.004760960239360261],
        index=prices.index,
    )
    pd.testing.assert_series_equal(result, expected)


def test_potential_energy_normalizes_displacement_from_rolling_equilibrium() -> None:
    series = pd.Series(
        [10.0, 12.0, 14.0, 16.0, 18.0],
        index=pd.date_range("2024-01-01", periods=5),
    )

    result = potential_energy(series, window=4)

    expected = pd.Series(
        [np.nan, 0.25, 0.5, 0.675, 0.675],
        index=series.index,
    )
    pd.testing.assert_series_equal(result, expected)


def test_total_energy_adds_short_window_kinetic_and_long_window_potential() -> None:
    prices = pd.Series(
        [10.0, 12.0, 14.0, 16.0, 18.0],
        index=pd.date_range("2024-01-01", periods=5),
    )

    result = total_energy(prices, short_window=2, long_window=4)

    expected = pd.Series(
        [
            np.nan,
            np.nan,
            0.5566067830084408,
            0.7163804874050759,
            0.7065794709310793,
        ],
        index=prices.index,
    )
    pd.testing.assert_series_equal(result, expected)
    pd.testing.assert_series_equal(
        result,
        kinetic_energy(prices, 2) + potential_energy(prices, 4),
    )


def test_market_temperature_returns_annualized_rolling_variance() -> None:
    returns = pd.Series(
        [0.01, -0.01, 0.03, -0.03, 0.05],
        index=pd.date_range("2024-01-01", periods=5),
    )

    result = market_temperature(returns, window=4)

    expected = pd.Series(
        [np.nan, 0.0504, 0.1008, 0.168, 0.336],
        index=returns.index,
    )
    pd.testing.assert_series_equal(result, expected)


def test_estimate_ou_parameters_recovers_deterministic_mean_reversion() -> None:
    values = [10.0]
    for _ in range(39):
        values.append(0.5 + 0.8 * values[-1])

    result = estimate_ou_parameters(pd.Series(values), dt=1.0)

    assert result == {
        "theta": 0.2,
        "mu": 2.5,
        "sigma": 0.0,
        "half_life_days": 3.5,
        "r_squared": 1.0,
        "mean_reverting": True,
    }


def test_estimate_ou_parameters_reports_no_reversion_for_positive_slope() -> None:
    result = estimate_ou_parameters(pd.Series(range(40)), dt=1.0)

    assert result["theta"] == 0.0
    assert result["mu"] == 19.5
    assert result["sigma"] == pytest.approx(11.69045194450012)
    assert math.isinf(result["half_life_days"])
    assert math.isnan(result["r_squared"])
    assert result["mean_reverting"] is False


def test_estimate_ou_parameters_returns_nan_fields_for_short_series() -> None:
    result = estimate_ou_parameters(pd.Series(range(10)), dt=1.0)

    assert set(result) == {"theta", "mu", "sigma", "half_life_days", "r_squared"}
    assert all(math.isnan(value) for value in result.values())


def test_hurst_exponent_returns_reference_values_for_deterministic_series() -> None:
    assert hurst_exponent(pd.Series(np.arange(1, 101, dtype=float)), max_lag=20) == (
        pytest.approx(1.0901453837271464)
    )
    assert hurst_exponent(pd.Series([1.0, 2.0] * 50), max_lag=20) == pytest.approx(
        0.22474195952486334
    )


def test_hurst_exponent_returns_nan_when_series_is_too_short() -> None:
    assert math.isnan(hurst_exponent(pd.Series(range(19)), max_lag=20))
