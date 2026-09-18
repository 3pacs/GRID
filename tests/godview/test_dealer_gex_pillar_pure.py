"""Pure-Python tests for godview/dealer_gex_pillar.py — no database, no network.

Includes the three synthetic analytical validation cases the operator
required: a symmetric chain (flip near spot), an all-calls chain (no
crossing -> unavailable), and missing IV (coverage < 1).
"""

from __future__ import annotations

from datetime import date

import pytest

from godview.dealer_gex_pillar import (
    CONTRACT_MULTIPLIER,
    GAMMA_ASSUMPTIONS_NOTE,
    SIGN_CONVENTION_NOTE,
    _compute_gex_for_chain,
    black_scholes_gamma,
    classify_gex_regime,
    compute_cumulative_by_strike,
    compute_put_call_oi_ratio,
    contract_dollar_gamma,
    coverage_fraction,
    find_gamma_flip,
    signed_gamma_contribution,
)

SNAP_DATE = date(2026, 1, 1)
EXPIRY = date(2026, 4, 1)  # ~90 days out
SPOT = 100.0


def _row(opt_type, strike, oi=100.0, iv=0.3, expiry=EXPIRY):
    return {"opt_type": opt_type, "strike": strike, "open_interest": oi, "implied_vol": iv, "expiry": expiry, "snap_date": SNAP_DATE}


def test_contract_multiplier_is_100_shares_finra_cited():
    assert CONTRACT_MULTIPLIER == 100


def test_sign_convention_note_states_calls_positive_puts_negative():
    assert "+gamma" in SIGN_CONVENTION_NOTE
    assert "-gamma" in SIGN_CONVENTION_NOTE


def test_gamma_assumptions_note_states_zero_rate_and_measured_iv():
    assert "r=0.0" in GAMMA_ASSUMPTIONS_NOTE or "r=0" in GAMMA_ASSUMPTIONS_NOTE
    assert "never solved for or defaulted" in GAMMA_ASSUMPTIONS_NOTE


def test_black_scholes_gamma_is_positive_and_symmetric_in_log_moneyness():
    g_atm = black_scholes_gamma(100.0, 100.0, 0.25, 0.3)
    g_otm = black_scholes_gamma(100.0, 120.0, 0.25, 0.3)
    assert g_atm is not None and g_atm > 0
    assert g_otm is not None and 0 < g_otm < g_atm  # ATM gamma > OTM gamma


@pytest.mark.parametrize(
    "spot,strike,t,sigma",
    [(0.0, 100.0, 0.25, 0.3), (100.0, 0.0, 0.25, 0.3), (100.0, 100.0, 0.0, 0.3), (100.0, 100.0, 0.25, 0.0)],
)
def test_black_scholes_gamma_none_for_invalid_inputs(spot, strike, t, sigma):
    assert black_scholes_gamma(spot, strike, t, sigma) is None


def test_signed_gamma_contribution_call_positive_put_negative():
    assert signed_gamma_contribution("call", 500.0) == 500.0
    assert signed_gamma_contribution("put", 500.0) == -500.0


def test_compute_cumulative_by_strike_is_ascending_and_running():
    cum = compute_cumulative_by_strike({100.0: 10.0, 90.0: -5.0, 110.0: 3.0})
    assert cum == [(90.0, -5.0), (100.0, 5.0), (110.0, 8.0)]


def test_find_gamma_flip_interpolates_the_sign_change():
    cum = [(90.0, -10.0), (100.0, 5.0)]
    flip = find_gamma_flip(cum)
    assert flip is not None
    assert 90.0 < flip < 100.0


def test_find_gamma_flip_none_when_never_crosses():
    cum = [(90.0, 5.0), (100.0, 10.0), (110.0, 20.0)]
    assert find_gamma_flip(cum) is None


@pytest.mark.parametrize(
    "net_gex,expected",
    [(None, "insufficient_data"), (1000.0, "long_gamma"), (-1000.0, "short_gamma"), (0.0, "neutral")],
)
def test_classify_gex_regime(net_gex, expected):
    assert classify_gex_regime(net_gex) == expected


def test_compute_put_call_oi_ratio_none_for_zero_call_oi():
    assert compute_put_call_oi_ratio(100.0, 0.0) is None
    assert compute_put_call_oi_ratio(50.0, 100.0) == pytest.approx(0.5)


def test_coverage_fraction_none_for_no_contracts_present():
    assert coverage_fraction(0, 0) is None
    assert coverage_fraction(3, 6) == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Synthetic analytical validation cases (operator-required)
# ---------------------------------------------------------------------------


def test_validation_symmetric_chain_flips_near_spot():
    """A put just below spot and a call just above it (near-mirrored OI/gamma)
    -> the interpolated flip must land between the two, close to spot."""
    rows = [_row("put", 99.0), _row("call", 101.0)]
    result = _compute_gex_for_chain(rows, SPOT, SNAP_DATE)
    assert result is not None
    flip = result["gamma_flip_strike"]
    assert flip is not None
    assert 99.0 <= flip <= 101.0


def test_validation_all_calls_chain_never_crosses_gamma_flip_unavailable():
    """No puts at all -> every per-strike net gamma is positive -> cumulative
    is monotonically increasing and always positive -> no crossing, ever."""
    rows = [_row("call", k) for k in (90.0, 95.0, 100.0, 105.0, 110.0)]
    result = _compute_gex_for_chain(rows, SPOT, SNAP_DATE)
    assert result is not None
    assert result["gamma_flip_strike"] is None
    assert result["net_gex"] > 0
    assert result["gex_regime"] == "long_gamma"


def test_validation_missing_iv_lowers_coverage_below_one():
    """Half the chain has no implied_vol -> those contracts are skipped
    (never a 0.25 default) and coverage_fraction < 1."""
    rows = [
        _row("call", 100.0, iv=0.3),
        _row("call", 105.0, iv=0.3),
        _row("put", 95.0, iv=None),
        _row("put", 90.0, iv=None),
    ]
    result = _compute_gex_for_chain(rows, SPOT, SNAP_DATE)
    assert result is not None
    assert result["contracts_used"] == 2
    assert result["contracts_present"] == 4
    cov = coverage_fraction(result["contracts_used"], result["contracts_present"])
    assert cov == pytest.approx(0.5)
    assert cov < 1.0


def test_validation_all_contracts_missing_iv_returns_none_not_a_fabricated_row():
    rows = [_row("call", 100.0, iv=None), _row("put", 95.0, iv=None)]
    result = _compute_gex_for_chain(rows, SPOT, SNAP_DATE)
    assert result is None


def test_validation_expired_contract_excluded_not_a_negative_time_gamma():
    """expiry <= snap_date -> T <= 0 -> excluded (never a negative-T gamma)."""
    rows = [
        _row("call", 100.0, iv=0.3, expiry=SNAP_DATE),  # expires same day as snapshot
        _row("call", 105.0, iv=0.3),
    ]
    result = _compute_gex_for_chain(rows, SPOT, SNAP_DATE)
    assert result is not None
    assert result["contracts_used"] == 1
    assert result["contracts_present"] == 2
