"""Synthetic mechanism controls, not market-edge evidence."""
from copy import deepcopy
import math

import pytest

from scripts.intraday_lab.volatility_repricing import YEAR_SECONDS, compute_repricing, proxy_price


def quote(t, *, spot=769, iv=.2, right="C"):
    expiry = 86400
    mid = proxy_price(spot, 769, (expiry-t)/YEAR_SECONDS, iv, .04, .01, right)
    return {"instrument": "OPTION", "source_id": "synthetic_option_chain", "event_at": t,
            "available_at": t + 1, "clock": "trusted_receipt", "status": "available",
            "values": {"contract_id": "SPY-test", "underlying": "SPY", "expiry_at": expiry,
                       "right": right, "strike": 769, "multiplier": 100,
                       "bid": mid-.01, "ask": mid+.01, "bid_size": 10, "ask_size": 10,
                       "spot": spot, "iv": iv, "rate": .04, "yield": .01,
                       "synchronized": True, "iv_basis": "provider"}}


def packet(**changes):
    return {"decision_at": 1001, "session": "synthetic-session", "provenance": "synthetic",
            "observations": [quote(900), quote(1000, **changes)]}


def vals(p):
    return compute_repricing(p)["contracts"]["SPY-test"]["values"]


def test_spot_move_and_theta_do_not_become_iv_repricing():
    v = vals(packet(spot=770))
    assert v["spot_component"] > 0
    assert v["time_component"] < 0
    assert v["iv_change_vol_points"] == v["iv_component"] == 0
    assert v["model_residual"] == pytest.approx(0, abs=1e-12)
    assert v["midpoint_change"] == pytest.approx(v["spot_component"] + v["time_component"])


@pytest.mark.parametrize("right", ["C", "P"])
def test_vol_rise_increases_both_sides_without_directional_vote(right):
    p = packet(iv=.25, right=right)
    p["observations"][0] = quote(900, right=right)
    result = compute_repricing(p)
    v = result["contracts"]["SPY-test"]["values"]
    assert v["iv_change_vol_points"] == pytest.approx(5)
    assert v["iv_component"] > 0
    assert "direction" not in result


def test_constant_iv_time_decay_and_spread_uncertainty_are_explicit():
    v = vals(packet())
    assert v["time_component"] < 0
    assert v["iv_component"] == v["spot_component"] == 0
    assert v["quote_uncertainty"] == pytest.approx(.02)
    assert v["years_remaining"] == pytest.approx((86400-1000)/YEAR_SECONDS)


@pytest.mark.parametrize("defect", ["future", "clock", "stale", "mixed_source", "duplicate", "identity",
                                     "null", "crossed", "no_size", "unsynchronized", "nan", "expired", "unknown_iv"])
def test_defective_pairs_are_unavailable_not_neutral(defect):
    p = packet()
    last = p["observations"][-1]
    if defect == "future": last["available_at"] = 1002
    elif defect == "clock": last["clock"] = "ANIK_callback"
    elif defect == "stale": p["decision_at"] = 1011
    elif defect == "mixed_source": last["source_id"] = "another"
    elif defect == "duplicate": last["event_at"] = 900
    elif defect == "identity": last["values"]["strike"] = 770
    elif defect == "null": last["values"]["iv"] = None
    elif defect == "crossed": last["values"]["bid"] = last["values"]["ask"] + .01
    elif defect == "no_size": last["values"]["bid_size"] = 0
    elif defect == "unsynchronized": last["values"]["synchronized"] = False
    elif defect == "nan": last["values"]["spot"] = math.nan
    elif defect == "expired": last["values"]["expiry_at"] = 999
    elif defect == "unknown_iv": last["values"]["iv_basis"] = "unknown"
    result = compute_repricing(p)
    assert result["status"] == "unavailable"
    assert result["contracts"]["SPY-test"]["value"] is None


def test_overflow_is_json_safe_unavailable():
    p = packet()
    p["observations"][-1]["values"]["rate"] = -1e308
    assert compute_repricing(p)["status"] == "unavailable"


def test_residual_not_silently_attributed_to_iv():
    p = packet()
    p["observations"][-1]["values"]["bid"] += .5
    p["observations"][-1]["values"]["ask"] += .5
    v = vals(p)
    assert v["model_residual"] == pytest.approx(.5)
    assert v["iv_component"] == 0


def test_multiple_contracts_keep_identity_and_lineage_separate():
    p = packet()
    second = deepcopy(p["observations"])
    for row in second:
        row["values"]["contract_id"] = "SPY-other-expiry"
        row["values"]["expiry_at"] += 86400
    p["observations"] += second
    r = compute_repricing(p)
    assert len(r["contracts"]) == 2
    assert r["contracts"]["SPY-test"]["latest_available_at"] == 1001
    assert r["contracts"]["SPY-test"]["identity"]["expiry_at"] == 86400


def test_absent_options_are_explicitly_unavailable():
    p = packet()
    p["observations"] = []
    assert compute_repricing(p)["status"] == "unavailable"
