"""Implied volatility and greeks derived from an archived end-of-day chain.

Chains here are synthetic: prices come from Black's model at a known forward
and a known volatility per strike, so the test knows the right answer.
"""

from __future__ import annotations

import math
from typing import Any

import pytest

from analysis import options_eod_greeks as g
from physics.greeks import black_scholes as bs

DAY = "2026-11-19"            # Thursday; cash close 21:00Z
RATE = 0.04
FORWARD = 100.6
EXPIRIES = {"2026-11-20": 1.0 / 365.0, "2026-12-18": 29.0 / 365.0}
STRIKES = (80.0, 90.0, 95.0, 100.0, 105.0, 110.0, 120.0)


def _vol(strike: float) -> float:
    return 0.22 + 0.002 * (100.0 - strike)   # a plain skew


def _black(forward: float, strike: float, t: float, vol: float, is_call: bool) -> float:
    cdf = lambda x: 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))
    d1 = (math.log(forward / strike) + 0.5 * vol * vol * t) / (vol * math.sqrt(t))
    d2 = d1 - vol * math.sqrt(t)
    df = math.exp(-RATE * t)
    return df * (forward * cdf(d1) - strike * cdf(d2)) if is_call else df * (strike * cdf(-d2) - forward * cdf(-d1))


def _chain(expiries: tuple[str, ...] = ("2026-12-18",), half_spread: float = 0.0) -> dict[str, Any]:
    contracts = []
    for expiry in expiries:
        t = EXPIRIES[expiry]
        for strike in STRIKES:
            for opt_type in ("call", "put"):
                price = _black(FORWARD, strike, t, _vol(strike), opt_type == "call")
                contracts.append({"expiry": expiry, "opt_type": opt_type, "strike": strike, "open_interest": 10,
                                  "bid": price - half_spread, "ask": price + half_spread, "quote_ts": None,
                                  "last_price": None, "last_ts": None, "volume": None})
    return {"ticker": "KO", "day": DAY, "provider": "databento", "audience": "private",
            "parts": {"close": {"status": "ok"}}, "contracts": contracts}


def _by(result: dict[str, Any], expiry: str, opt_type: str, strike: float) -> dict[str, Any]:
    return next(c for c in result["contracts"]
                if c["expiry"] == expiry and c["opt_type"] == opt_type and c["strike"] == strike)


def test_time_runs_from_this_close_to_the_expiry_close() -> None:
    result = g.chain_greeks(_chain(("2026-11-20", "2026-12-18")), RATE)
    assert result["as_of"] == "2026-11-19T21:00:00Z"
    assert result["expiries"]["2026-11-20"]["t_years"] == pytest.approx(1.0 / 365.0)
    assert result["expiries"]["2026-12-18"]["t_years"] == pytest.approx(29.0 / 365.0)
    # 2026-11-27 closes at 13:00 New York: three hours less than a full day count.
    half_day = g.chain_greeks({**_chain(), "contracts": [dict(c, expiry="2026-11-27") for c in _chain()["contracts"]]}, RATE)
    assert half_day["expiries"]["2026-11-27"]["t_years"] == pytest.approx((8.0 - 3.0 / 24.0) / 365.0)


def test_forward_volatility_and_greeks_are_recovered_from_model_prices() -> None:
    result = g.chain_greeks(_chain(), RATE)
    t = EXPIRIES["2026-12-18"]

    info = result["expiries"]["2026-12-18"]
    assert info["forward"] == pytest.approx(FORWARD, abs=1e-9) and info["pairs"] == 5
    assert info["forward_range"] == pytest.approx(0.0, abs=1e-9)
    spot_star = FORWARD * math.exp(-RATE * t)
    for strike in STRIKES:
        for opt_type in ("call", "put"):
            row, is_call = _by(result, "2026-12-18", opt_type, strike), opt_type == "call"
            assert row["no_value"] is None
            assert row["implied_vol"] == pytest.approx(_vol(strike), abs=1e-6)
            assert row["delta"] == pytest.approx(bs.delta(spot_star, strike, t, RATE, _vol(strike), 0.0, is_call=is_call), abs=1e-5)
            assert row["gamma"] == pytest.approx(bs.gamma(spot_star, strike, t, RATE, _vol(strike), 0.0), abs=1e-6)
            assert row["vega"] == pytest.approx(bs.vega(spot_star, strike, t, RATE, _vol(strike), 0.0) / 100.0, abs=1e-6)
            assert row["theta"] == pytest.approx(bs.theta(spot_star, strike, t, RATE, _vol(strike), 0.0, is_call=is_call) / 365.0, abs=1e-6)
            assert row["in_the_money"] is ((strike < FORWARD) if is_call else (strike > FORWARD))
    call, put = _by(result, "2026-12-18", "call", 100.0), _by(result, "2026-12-18", "put", 100.0)
    assert call["delta"] - put["delta"] == pytest.approx(1.0, abs=1e-6)       # parity at the adjusted spot
    assert call["gamma"] == pytest.approx(put["gamma"]) and call["vega"] == pytest.approx(put["vega"])
    assert call["theta"] < 0 and call["vega"] > 0
    assert result["method"] == "eod-greeks/1" and result["rate"] == RATE
    assert result["counts"] == {"contracts": 14, "valued": 14, "two_sided": 14}
    assert len(result["limits"]) == 3 and "American" in result["limits"][0]


def test_a_quoted_spread_moves_nothing_when_it_is_symmetric_and_the_input_is_not_changed() -> None:
    chain = _chain(half_spread=0.03)
    before = [dict(c) for c in chain["contracts"]]
    result = g.chain_greeks(chain, RATE)
    assert chain["contracts"] == before
    assert _by(result, "2026-12-18", "call", 100.0)["implied_vol"] == pytest.approx(_vol(100.0), abs=1e-6)
    assert _by(result, "2026-12-18", "call", 100.0)["mid"] == pytest.approx(_black(FORWARD, 100.0, EXPIRIES["2026-12-18"], _vol(100.0), True))


def test_forward_uses_the_strikes_nearest_the_money_and_survives_one_bad_quote() -> None:
    chain = _chain()
    for c in chain["contracts"]:
        if c["strike"] == 105.0 and c["opt_type"] == "call":
            c["bid"] += 0.40
            c["ask"] += 0.40     # one stale call among the five nearest strikes
    info = g.chain_greeks(chain, RATE)["expiries"]["2026-12-18"]
    assert info["forward"] == pytest.approx(FORWARD, abs=1e-9)        # the median ignores it
    assert info["forward_range"] == pytest.approx(0.40 * math.exp(RATE * EXPIRIES["2026-12-18"]), abs=1e-6)
    # Deep strikes are never used: only the five smallest |C - P|.
    assert g.implied_forward([(k, 100.6 - k if k < 100.6 else 0.0, 0.0 if k < 100.6 else k - 100.6) for k in
                              (50.0, 100.0, 101.0, 150.0)], 0.0, 0.0)["pairs"] == 4
    assert g.implied_forward([], 0.1, RATE) is None
    assert g.implied_forward([(1.0, 0.0, 50.0)], 0.1, RATE) is None    # a non-positive forward is refused


def test_contracts_without_a_usable_quote_get_a_reason_not_a_number() -> None:
    chain = _chain()
    rows = {(c["opt_type"], c["strike"]): c for c in chain["contracts"]}
    rows[("call", 80.0)].update(bid=None)                 # one-sided
    rows[("put", 80.0)].update(bid=0.0)                   # no bid
    rows[("call", 120.0)].update(bid=1.0, ask=0.5)        # crossed
    rows[("put", 120.0)].update(bid=float("nan"))
    rows[("call", 90.0)].update(bid=0.01, ask=0.02)       # below intrinsic value: no volatility fits
    result = g.chain_greeks(chain, RATE)

    for key in (("call", 80.0), ("put", 80.0), ("call", 120.0), ("put", 120.0)):
        row = _by(result, "2026-12-18", *key)
        assert row["mid"] is None and row["implied_vol"] is None and row["delta"] is None
        assert row["no_value"] == "no two-sided quote"
    bad = _by(result, "2026-12-18", "call", 90.0)
    assert bad["mid"] == pytest.approx(0.015) and bad["implied_vol"] is None and bad["gamma"] is None
    assert bad["no_value"] == "mid outside the model's bounds" and bad["in_the_money"] is True
    assert result["counts"] == {"contracts": 14, "valued": 9, "two_sided": 10}
    assert _by(result, "2026-12-18", "put", 100.0)["implied_vol"] == pytest.approx(_vol(100.0), abs=1e-6)


def test_an_expiry_with_no_time_left_or_no_pair_is_reported_not_valued() -> None:
    chain = _chain()
    same_day = [dict(c, expiry=DAY) for c in chain["contracts"]]
    calls_only = [dict(c, expiry="2027-06-18") for c in chain["contracts"] if c["opt_type"] == "call"]
    result = g.chain_greeks({**chain, "contracts": same_day + chain["contracts"] + calls_only}, RATE)

    assert result["expiries"][DAY]["no_value"] == "expires at this close"
    assert result["expiries"]["2027-06-18"]["no_value"] == "no strike with a two-sided call and put"
    assert all(c["implied_vol"] is None and c["no_value"] for c in result["contracts"] if c["expiry"] != "2026-12-18")
    assert result["counts"] == {"contracts": 35, "valued": 14, "two_sided": 35}
    assert list(result["expiries"]) == [DAY, "2026-12-18", "2027-06-18"]


def test_next_day_expiry_is_valued() -> None:
    result = g.chain_greeks(_chain(("2026-11-20",)), RATE)
    assert _by(result, "2026-11-20", "call", 100.0)["implied_vol"] == pytest.approx(_vol(100.0), abs=1e-5)


@pytest.mark.parametrize("rate", [None, "0.04", True, float("nan"), 4.0, -0.2])
def test_rate_must_be_a_plausible_annual_decimal(rate: Any) -> None:
    with pytest.raises(ValueError, match="rate"):
        g.chain_greeks(_chain(), rate)


def test_empty_chain_is_an_empty_answer() -> None:
    result = g.chain_greeks({"ticker": "KO", "day": DAY, "contracts": []}, RATE)
    assert result["contracts"] == [] and result["expiries"] == {} and result["counts"]["valued"] == 0
