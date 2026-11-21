"""
Tests for physics/greeks/black_scholes.py (GEX-3 / Wave 1).

Reference scenario used throughout:
    S=100, K=100, T=0.25, r=0.05, sigma=0.20, q=0.0  (ATM call, 3-month)

All reference values were computed analytically from the closed-form
Black-Scholes Greek definitions using the math.erf CDF, independent of the
module under test.
"""

from __future__ import annotations

import math

import numpy as np
import pytest

from physics.greeks.black_scholes import (
    T_MIN,
    charm,
    color,
    d1,
    d2,
    delta,
    gamma,
    speed,
    theta,
    vanna,
    vega,
    vomma,
    zomma,
)

# Reference ATM scenario
S0, K0, T0, R0, SIG0, Q0 = 100.0, 100.0, 0.25, 0.05, 0.20, 0.0

# Analytically computed reference values (see test docstring for formulas)
REF_D1 = 0.17500000000000002
REF_D2 = 0.07500000000000001
REF_CALL_DELTA = 0.5694601832076737
REF_PUT_DELTA = -0.43053981679232634
REF_GAMMA = 0.03928800094473793
REF_VANNA = -0.14733000354276726
REF_CHARM_CALL = -0.13750800330658278
REF_VOMMA = 1.2891375309992137
REF_SPEED = -0.0010804200259802931
REF_COLOR = -0.08098239194734105
REF_ZOMMA = -0.19386172966169118

TOL = 1e-10
LOOSE = 1e-6


# ── 1. d1 / d2 identity and reference ────────────────────────────────

def test_d1_reference_value():
    assert d1(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_D1, abs=TOL)


def test_d2_reference_value():
    assert d2(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_D2, abs=TOL)


def test_d2_equals_d1_minus_sigma_sqrt_T():
    """d2 = d1 - sigma * sqrt(T) is the definitional identity."""
    for S in (80.0, 100.0, 120.0):
        for T in (0.01, 0.25, 1.0, 2.0):
            for sigma in (0.10, 0.25, 0.50):
                expected = d1(S, K0, T, R0, sigma, Q0) - sigma * math.sqrt(T)
                assert d2(S, K0, T, R0, sigma, Q0) == pytest.approx(expected, abs=TOL)


# ── 2. Delta boundary behaviour ─────────────────────────────────────

def test_atm_call_delta_near_half():
    """ATM call delta for short-dated ATM should be ~0.5 (slightly over because r>0)."""
    val = delta(100.0, 100.0, 0.01, 0.05, 0.20, 0.0, is_call=True)
    assert 0.45 < val < 0.55


def test_deep_itm_call_delta_approaches_one():
    val = delta(500.0, 100.0, 0.25, 0.05, 0.20, 0.0, is_call=True)
    assert val == pytest.approx(1.0, abs=1e-6)


def test_deep_otm_call_delta_approaches_zero():
    val = delta(10.0, 100.0, 0.25, 0.05, 0.20, 0.0, is_call=True)
    assert val == pytest.approx(0.0, abs=1e-6)


def test_deep_itm_put_delta_approaches_negative_one():
    val = delta(10.0, 100.0, 0.25, 0.05, 0.20, 0.0, is_call=False)
    assert val == pytest.approx(-1.0, abs=1e-6)


def test_deep_otm_put_delta_approaches_zero():
    val = delta(500.0, 100.0, 0.25, 0.05, 0.20, 0.0, is_call=False)
    assert val == pytest.approx(0.0, abs=1e-6)


def test_call_reference_delta():
    assert delta(S0, K0, T0, R0, SIG0, Q0, True) == pytest.approx(REF_CALL_DELTA, abs=TOL)


def test_put_reference_delta():
    assert delta(S0, K0, T0, R0, SIG0, Q0, False) == pytest.approx(REF_PUT_DELTA, abs=TOL)


# ── 3. Put-call delta parity ─────────────────────────────────────────

def test_put_call_delta_parity_no_dividend():
    """call_delta - put_delta = e^(-qT). With q=0 this is exactly 1."""
    cd = delta(S0, K0, T0, R0, SIG0, 0.0, True)
    pd = delta(S0, K0, T0, R0, SIG0, 0.0, False)
    assert (cd - pd) == pytest.approx(1.0, abs=TOL)


def test_put_call_delta_parity_with_dividend():
    """With q>0: call_delta - put_delta = e^(-qT)."""
    q = 0.03
    cd = delta(S0, K0, T0, R0, SIG0, q, True)
    pd = delta(S0, K0, T0, R0, SIG0, q, False)
    assert (cd - pd) == pytest.approx(math.exp(-q * T0), abs=TOL)


# ── 4. Gamma peaks near ATM ─────────────────────────────────────────

def test_gamma_peaks_near_atm():
    strikes = np.array([70.0, 85.0, 100.0, 115.0, 130.0])
    gammas = np.array([gamma(S0, K, T0, R0, SIG0, Q0) for K in strikes])
    # ATM strike should have the max gamma
    assert int(np.argmax(gammas)) == 2


def test_gamma_reference_value():
    assert gamma(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_GAMMA, abs=TOL)


# ── 5. Higher-order Greek reference values ──────────────────────────

def test_vanna_reference_value():
    assert vanna(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_VANNA, abs=LOOSE)


def test_charm_call_reference_value():
    assert charm(S0, K0, T0, R0, SIG0, Q0, True) == pytest.approx(REF_CHARM_CALL, abs=LOOSE)


def test_vomma_reference_value():
    assert vomma(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_VOMMA, abs=LOOSE)


def test_speed_reference_value():
    assert speed(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_SPEED, abs=LOOSE)


def test_color_reference_value():
    assert color(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_COLOR, abs=LOOSE)


def test_zomma_reference_value():
    assert zomma(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(REF_ZOMMA, abs=LOOSE)


# ── 6. Array input preserves shape ──────────────────────────────────

def test_array_input_preserves_shape_1d():
    S_arr = np.array([80.0, 90.0, 100.0, 110.0, 120.0])
    g = gamma(S_arr, K0, T0, R0, SIG0, Q0)
    assert isinstance(g, np.ndarray)
    assert g.shape == S_arr.shape
    # Each element matches scalar call
    for i, s in enumerate(S_arr):
        assert g[i] == pytest.approx(gamma(float(s), K0, T0, R0, SIG0, Q0), abs=LOOSE)


def test_array_input_preserves_shape_2d():
    S_arr = np.array([[80.0, 100.0], [120.0, 140.0]])
    d = delta(S_arr, K0, T0, R0, SIG0, Q0, True)
    assert d.shape == (2, 2)


def test_array_all_greeks_run():
    """All 10 functions accept numpy arrays without crashing and return sane shapes."""
    S_arr = np.linspace(80.0, 120.0, 20)
    for fn in (d1, d2, gamma, vanna, vomma, speed, color, zomma):
        out = fn(S_arr, K0, T0, R0, SIG0, Q0)
        assert out.shape == S_arr.shape
        assert np.all(np.isfinite(out))
    for is_call in (True, False):
        assert delta(S_arr, K0, T0, R0, SIG0, Q0, is_call).shape == S_arr.shape
        assert charm(S_arr, K0, T0, R0, SIG0, Q0, is_call).shape == S_arr.shape


# ── 7. T=0 edge case: time-decay Greeks return 0 ────────────────────

def test_gamma_t_zero_returns_zero_scalar():
    assert gamma(100.0, 100.0, 0.0, 0.05, 0.20) == 0.0


def test_all_time_decay_greeks_t_zero_returns_zero():
    kwargs = dict(S=100.0, K=100.0, T=0.0, r=0.05, sigma=0.20, q=0.0)
    assert gamma(**kwargs) == 0.0
    assert vanna(**kwargs) == 0.0
    assert charm(**kwargs, is_call=True) == 0.0
    assert charm(**kwargs, is_call=False) == 0.0
    assert vomma(**kwargs) == 0.0
    assert speed(**kwargs) == 0.0
    assert color(**kwargs) == 0.0
    assert zomma(**kwargs) == 0.0


def test_t_zero_delta_is_intrinsic():
    """At expiry, delta is 1 for ITM calls, 0 for OTM calls."""
    assert delta(150.0, 100.0, 0.0, 0.05, 0.20, 0.0, True) == 1.0
    assert delta(50.0, 100.0, 0.0, 0.05, 0.20, 0.0, True) == 0.0
    assert delta(50.0, 100.0, 0.0, 0.05, 0.20, 0.0, False) == -1.0
    assert delta(150.0, 100.0, 0.0, 0.05, 0.20, 0.0, False) == 0.0


def test_t_zero_array_returns_zero_for_gamma():
    T_arr = np.array([0.0, 0.0, 0.0])
    out = gamma(100.0, 100.0, T_arr, 0.05, 0.20)
    assert np.all(out == 0.0)


# ── 8. sigma=0 edge case ────────────────────────────────────────────

def test_all_time_decay_greeks_sigma_zero_returns_zero():
    kwargs = dict(S=100.0, K=100.0, T=0.25, r=0.05, sigma=0.0, q=0.0)
    assert gamma(**kwargs) == 0.0
    assert vanna(**kwargs) == 0.0
    assert charm(**kwargs, is_call=True) == 0.0
    assert vomma(**kwargs) == 0.0
    assert speed(**kwargs) == 0.0
    assert color(**kwargs) == 0.0
    assert zomma(**kwargs) == 0.0
    assert d1(**kwargs) == 0.0
    assert d2(**kwargs) == 0.0


def test_sigma_zero_array_returns_zero():
    sigma_arr = np.array([0.0, 0.0, 0.20])
    out = gamma(100.0, 100.0, 0.25, 0.05, sigma_arr)
    assert out[0] == 0.0
    assert out[1] == 0.0
    assert out[2] > 0.0


# ── 9. No NaN or inf propagation ────────────────────────────────────

def test_no_nan_in_output_for_sensible_inputs():
    rng = np.random.default_rng(42)
    S_arr = rng.uniform(50.0, 200.0, 100)
    K_arr = rng.uniform(50.0, 200.0, 100)
    T_arr = rng.uniform(0.01, 2.0, 100)
    sigma_arr = rng.uniform(0.05, 0.80, 100)

    for fn in (gamma, vanna, vomma, speed, color, zomma):
        out = fn(S_arr, K_arr, T_arr, 0.05, sigma_arr)
        assert np.all(np.isfinite(out)), f"{fn.__name__} produced NaN/inf"


def test_no_nan_at_tmin_boundary():
    """At T=T_MIN exactly, Greeks stay finite."""
    for fn in (gamma, vanna, vomma, speed, color, zomma):
        out = fn(100.0, 100.0, T_MIN, 0.05, 0.20)
        assert math.isfinite(out), f"{fn.__name__} not finite at T_MIN"


# ── 10. Parity: scalar == array[0] ──────────────────────────────────

def test_vectorized_matches_scalar_pointwise():
    """Vectorized form applied to array matches scalar form applied elementwise."""
    S_arr = np.array([85.0, 95.0, 105.0, 115.0])
    vec_gamma = gamma(S_arr, K0, T0, R0, SIG0, Q0)
    for i, s in enumerate(S_arr):
        scalar_val = gamma(float(s), K0, T0, R0, SIG0, Q0)
        assert vec_gamma[i] == pytest.approx(scalar_val, abs=1e-12)


def test_vectorized_charm_matches_scalar():
    S_arr = np.array([85.0, 95.0, 105.0, 115.0])
    vec_charm = charm(S_arr, K0, T0, R0, SIG0, Q0, True)
    for i, s in enumerate(S_arr):
        scalar_val = charm(float(s), K0, T0, R0, SIG0, Q0, True)
        assert vec_charm[i] == pytest.approx(scalar_val, abs=1e-12)


# ── vega and theta (added 2026-11-21) ────────────────────────────────


def _price(S, K, T, r, sigma, q, is_call):
    """Closed-form Black-Scholes price with math.erf, independent of the module under test."""
    cdf = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
    d1_ = (math.log(S / K) + (r - q + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2_ = d1_ - sigma * math.sqrt(T)
    if is_call:
        return S * math.exp(-q * T) * cdf(d1_) - K * math.exp(-r * T) * cdf(d2_)
    return K * math.exp(-r * T) * cdf(-d2_) - S * math.exp(-q * T) * cdf(-d1_)


@pytest.mark.parametrize(("S", "K", "T", "r", "sigma", "q"), [
    (100.0, 100.0, 0.25, 0.05, 0.20, 0.0), (733.0, 700.0, 0.08, 0.04, 0.25, 0.012),
    (70.0, 75.0, 1.1, 0.04, 0.18, 0.03), (250.0, 180.0, 0.02, 0.05, 0.60, 0.0),
])
@pytest.mark.parametrize("is_call", [True, False])
def test_vega_and_theta_match_finite_differences_of_the_price(S, K, T, r, sigma, q, is_call):
    h_sigma, h_t = 1e-5, 1e-6
    fd_vega = (_price(S, K, T, r, sigma + h_sigma, q, is_call) - _price(S, K, T, r, sigma - h_sigma, q, is_call)) / (2 * h_sigma)
    # theta is the change per year as calendar time passes, i.e. as T shrinks.
    fd_theta = (_price(S, K, T - h_t, r, sigma, q, is_call) - _price(S, K, T + h_t, r, sigma, q, is_call)) / (2 * h_t)
    assert vega(S, K, T, r, sigma, q) == pytest.approx(fd_vega, rel=1e-6, abs=1e-8)
    assert theta(S, K, T, r, sigma, q, is_call=is_call) == pytest.approx(fd_theta, rel=1e-5, abs=1e-7)


def test_vega_and_theta_reference_scenario_and_shapes():
    assert vega(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(19.644000472368965, abs=1e-9)
    assert theta(S0, K0, T0, R0, SIG0, Q0) == pytest.approx(-10.474151034, abs=1e-6)
    assert isinstance(vega(S0, K0, T0, R0, SIG0), float) and isinstance(theta(S0, K0, T0, R0, SIG0), float)
    strikes = np.array([90.0, 100.0, 110.0])
    both = theta(S0, strikes, T0, R0, SIG0, 0.0, is_call=np.array([True, False, True]))
    assert both.shape == (3,) and vega(S0, strikes, T0, R0, SIG0).shape == (3,)
    assert both[1] == pytest.approx(theta(S0, 100.0, T0, R0, SIG0, is_call=False))
    assert vega(S0, strikes, T0, R0, SIG0)[1] == pytest.approx(vega(S0, 100.0, T0, R0, SIG0))


def test_vega_and_theta_are_zero_for_unusable_inputs():
    for bad in ((S0, K0, 0.0, R0, SIG0), (S0, K0, T0, R0, 0.0), (0.0, K0, T0, R0, SIG0), (S0, 0.0, T0, R0, SIG0)):
        assert vega(*bad) == 0.0 and theta(*bad) == 0.0
    out = vega(S0, K0, np.array([0.0, T0]), R0, SIG0)
    assert out[0] == 0.0 and out[1] > 0 and not np.isnan(out).any()
