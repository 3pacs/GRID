"""Reference-value tests for the Black-Scholes helpers in analysis/vol_surface.py.

Covers _bs_vega, _bs_theta_call, _bs_theta_put, _bs_volga, _bs_call_price,
_bs_put_price against textbook Black-Scholes math + degenerate-input guards.

Closes punch-list line 147 (PUNCH-LIST-2026-05-13.md auditor 2026-06-21):
analysis/vol_surface.py greeks helpers had zero direct coverage despite
feeding the SVI fit + dealer-gamma + valuation paths.
"""
from __future__ import annotations

import math

import pytest
from scipy.stats import norm

from analysis.vol_surface import (
    _bs_call_price,
    _bs_put_price,
    _bs_theta_call,
    _bs_theta_put,
    _bs_vega,
    _bs_volga,
)
from physics.dealer_gamma import _d1


# Canonical textbook scenario: ATM, 1Y, r=5%, sigma=20%.
_S, _K, _T, _R, _SIGMA = 100.0, 100.0, 1.0, 0.05, 0.20


def _expected_d1_d2() -> tuple[float, float]:
    d1 = _d1(_S, _K, _T, _R, _SIGMA)
    d2 = d1 - _SIGMA * math.sqrt(_T)
    return d1, d2


def test_bs_call_and_put_obey_put_call_parity() -> None:
    """C - P == S - K * exp(-rT) — exact for non-dividend Black-Scholes."""
    call = _bs_call_price(_S, _K, _T, _R, _SIGMA)
    put = _bs_put_price(_S, _K, _T, _R, _SIGMA)
    rhs = _S - _K * math.exp(-_R * _T)
    assert call - put == pytest.approx(rhs, abs=1e-9)


def test_bs_vega_matches_textbook_formula() -> None:
    """Vega = S * phi(d1) * sqrt(T) and is the same for calls and puts."""
    d1, _ = _expected_d1_d2()
    expected = _S * norm.pdf(d1) * math.sqrt(_T)
    assert _bs_vega(_S, _K, _T, _R, _SIGMA) == pytest.approx(expected, rel=1e-9)


def test_bs_volga_identity_holds() -> None:
    """Volga = Vega * d1 * d2 / sigma — the closed-form definition in the module."""
    d1, d2 = _expected_d1_d2()
    vega = _bs_vega(_S, _K, _T, _R, _SIGMA)
    expected = vega * d1 * d2 / _SIGMA
    assert _bs_volga(_S, _K, _T, _R, _SIGMA) == pytest.approx(expected, rel=1e-9)


def test_bs_theta_call_and_put_match_per_day_formula() -> None:
    """Theta (per calendar day) = annualized theta / 365 for both call and put."""
    d1, d2 = _expected_d1_d2()
    common = -(_S * norm.pdf(d1) * _SIGMA) / (2.0 * math.sqrt(_T))
    expected_call = (common - _R * _K * math.exp(-_R * _T) * norm.cdf(d2)) / 365.0
    expected_put = (common + _R * _K * math.exp(-_R * _T) * norm.cdf(-d2)) / 365.0
    assert _bs_theta_call(_S, _K, _T, _R, _SIGMA) == pytest.approx(expected_call, rel=1e-9)
    assert _bs_theta_put(_S, _K, _T, _R, _SIGMA) == pytest.approx(expected_put, rel=1e-9)


@pytest.mark.parametrize(
    "S, K, expected_call, expected_put",
    [
        (105.0, 100.0, 5.0, 0.0),   # ITM call / OTM put
        (95.0, 100.0, 0.0, 5.0),    # OTM call / ITM put
        (100.0, 100.0, 0.0, 0.0),   # ATM
    ],
)
def test_bs_prices_collapse_to_intrinsic_at_expiry(
    S: float, K: float, expected_call: float, expected_put: float
) -> None:
    """At T=0 the BS price MUST equal max(S-K, 0) for calls and max(K-S, 0) for puts."""
    assert _bs_call_price(S, K, 0.0, _R, _SIGMA) == pytest.approx(expected_call)
    assert _bs_put_price(S, K, 0.0, _R, _SIGMA) == pytest.approx(expected_put)


def test_bs_prices_collapse_to_discounted_intrinsic_when_sigma_zero() -> None:
    """sigma=0 with T>0 -> certain payoff = max(S - K*exp(-rT), 0) for calls."""
    expected_call = max(0.0, _S - _K * math.exp(-_R * _T))
    expected_put = max(0.0, _K * math.exp(-_R * _T) - _S)
    assert _bs_call_price(_S, _K, _T, _R, 0.0) == pytest.approx(expected_call)
    assert _bs_put_price(_S, _K, _T, _R, 0.0) == pytest.approx(expected_put)


@pytest.mark.parametrize(
    "S, T, sigma",
    [
        (100.0, 0.0, 0.20),    # T=0
        (100.0, 1.0, 0.0),     # sigma=0
        (100.0, 1.0, -0.10),   # sigma<0
        (0.0, 1.0, 0.20),      # S=0
        (-5.0, 1.0, 0.20),     # S<0
    ],
)
def test_bs_greeks_return_zero_on_degenerate_inputs(
    S: float, T: float, sigma: float
) -> None:
    """Vega/theta/volga MUST return 0.0 on the early-return guards."""
    assert _bs_vega(S, 100.0, T, _R, sigma) == 0.0
    assert _bs_theta_call(S, 100.0, T, _R, sigma) == 0.0
    assert _bs_theta_put(S, 100.0, T, _R, sigma) == 0.0
    assert _bs_volga(S, 100.0, T, _R, sigma) == 0.0
