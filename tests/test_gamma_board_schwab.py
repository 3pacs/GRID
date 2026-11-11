from datetime import datetime, timezone

import pytest

from api.gamma_board_schwab import aggregate

NOW = datetime(2026, 11, 11, 17, tzinfo=timezone.utc)
MS = int(NOW.timestamp() * 1000)


def chain():
    j = {
        "status": "SUCCESS",
        "symbol": "$SPX",
        "isDelayed": False,
        "underlying": {
            "symbol": "$SPX",
            "last": 7500.0,
            "tradeTime": MS,
            "quoteTime": MS,
            "delayed": False,
        },
    }
    for name, letter, oi in (("call", "C", 10), ("put", "P", 4)):
        row = {
            "symbol": "SPXW  261111" + letter + "07500000",
            "optionRoot": "SPXW",
            "strikePrice": 7500.0,
            "putCall": name.upper(),
            "expirationDate": "2026-11-11",
            "multiplier": 100.0,
            "mini": False,
            "nonStandard": False,
            "settlementType": "P",
            "gamma": 0.01,
            "openInterest": oi,
            "quoteTimeInLong": MS - 2000,
        }
        j[name + "ExpDateMap"] = {"2026-11-11:0": {"7500.0": [row]}}
    return j


def test_exact_net_and_source_clocks_and_same_day_am_filter():
    j = chain()
    rows = j["callExpDateMap"]["2026-11-11:0"]["7500.0"]
    rows.append(
        dict(
            rows[0],
            symbol="SPX   261111C07500000",
            optionRoot="SPX",
            settlementType="A",
        )
    )
    board = aggregate("SPX", j, now=NOW)
    assert board["net_gex"] == 3375000.0
    assert board["contracts"] == 2 and board["call_wall"] == board["put_wall"] == 7500
    assert board["cells"][0]["put_gex"] == -2250000.0
    assert board["fresh"] and board["as_of"] != board["spot_as_of"]


def test_delayed_and_undated_spot_never_live():
    for mode in ("delayed", "undated"):
        j = chain()
        if mode == "delayed":
            j["isDelayed"] = True
        else:
            j["underlying"]["tradeTime"] = None
        b = aggregate("SPX", j, now=NOW)
        assert b["stale"] and not b["fresh"]
        if mode == "undated":
            assert b["spot_as_of"] is None


def test_provider_symbol_mismatch_and_duplicate_rejected():
    j = chain()
    with pytest.raises(ValueError, match="identity"):
        aggregate("SPY", j, now=NOW)
    rows = j["callExpDateMap"]["2026-11-11:0"]["7500.0"]
    rows.extend(rows[:])
    with pytest.raises(ValueError, match="Duplicate"):
        aggregate("SPX", j, now=NOW)


def test_negative_sentinel_not_included_or_sold_as_real_zero():
    j = chain()
    j["putExpDateMap"]["2026-11-11:0"]["7500.0"][0]["gamma"] = -999.0
    with pytest.raises(ValueError, match="No dated option quotes"):
        aggregate("SPX", j, now=NOW)


def test_whole_chain_zero_is_unavailable_not_zero_gex():
    from api.gamma_board_schwab import MissingOpenInterest

    j = chain()
    for side in ("call", "put"):
        j[side + "ExpDateMap"]["2026-11-11:0"]["7500.0"][0]["openInterest"] = 0
    with pytest.raises(MissingOpenInterest):
        aggregate("SPX", j, now=NOW)
    exact = {"SPXW261111C07500000": 10, "SPXW261111P07500000": 4}
    b = aggregate("SPX", j, now=NOW, daily_oi=exact)
    assert b["net_gex"] == 3375000 and b["call_wall"] == b["put_wall"] == 7500
    with pytest.raises(ValueError):
        aggregate("SPX", j, now=NOW, daily_oi={**exact, "SPXW261111C07500000": None})
    with pytest.raises(MissingOpenInterest):
        aggregate("SPX", j, now=NOW, daily_oi={"SPX261111C07500000": 10})
