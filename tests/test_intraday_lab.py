"""Research integrity controls; synthetic outcomes cannot establish an edge."""
from copy import deepcopy
import math
import json
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.intraday_lab.features import FEATURES, compute
from scripts.intraday_lab.evaluate import evaluate, outcome
from scripts.intraday_lab.capture import normalize


def row(t, instrument="SPY", **values):
    return {"instrument": instrument, "event_at": t, "available_at": t,
            "source_id": "fixture", "clock": "trusted_receipt", "status": "available", "values": values}


def packet(t=1200):
    return {"decision_at": t, "session": "2026-10-05", "provenance": "synthetic",
            "observations": [row(i, price=100 + i / 10000) for i in range(0, t + 1, 5)]}


def test_no_neutral_votes_for_absent_inputs():
    p = packet()
    f = compute(p)["features"]
    assert f["price_momentum"]["status"] == "available"
    for name in ("es_depth", "es_ofi", "es_absorption", "breadth", "auction", "gamma_range"):
        assert f[name]["value"] is None


@pytest.mark.parametrize("mutation", ["future_receipt", "untrusted", "stale", "gap", "nan", "mixed_source"])
def test_bad_price_history_withholds_momentum(mutation):
    p = packet()
    if mutation == "future_receipt":
        for r in p["observations"]:
            r["available_at"] = 1201
    elif mutation == "untrusted":
        for r in p["observations"]:
            r["clock"] = "ANIK_callback"
    elif mutation == "stale":
        p["decision_at"] += 30
    elif mutation == "gap":
        p["observations"] = [r for r in p["observations"] if r["event_at"] not in (1000, 1005, 1010)]
    elif mutation == "nan":
        p["observations"][-1]["values"]["price"] = math.nan
        p["observations"][-2]["values"]["price"] = math.nan
        p["observations"][-3]["values"]["price"] = math.nan
    else:
        p["observations"][-1]["source_id"] = "other"
    assert compute(p)["features"]["price_momentum"]["status"] == "unavailable"


def test_depth_ofi_and_verified_absorption():
    p = packet()
    for t in range(1140, 1201, 5):
        p["observations"] += [row(t, "ES_BOOK", bid=5000, ask=5000.25, bid_size=100 + t - 1140, ask_size=50),
                              row(t, "ES", price=5000)]
    p["observations"].append(row(1200, "ES_FLOW", buy_volume=90, sell_volume=10,
                                 window_seconds=60, classification="verified_aggressor"))
    f = compute(p)["features"]
    assert f["es_depth"]["value"] > .3
    assert f["es_ofi"]["value"] > 0
    assert f["es_absorption"]["value"] == -.8
    p["observations"][-1]["values"]["classification"] = "midpoint_guess"
    assert compute(p)["features"]["es_aggression"]["value"] is None


def test_breadth_and_leader_disagreement_is_candidate_only():
    p = packet()
    p["observations"].append(row(1200, "BREADTH", tick=-800, up_volume=100, down_volume=900))
    for t in range(1140, 1201, 5):
        p["observations"].append(row(t, "NQ", price=100 + (t - 1140) / 100))
    f = compute(p)["features"]
    assert f["breadth_divergence"]["value"] < 0
    assert f["nq_lead"]["value"] > 0


def test_auction_requires_relevant_universe_and_close_window():
    p = packet()
    p["observations"].append(row(1200, "AUCTION", buy_notional=90, sell_notional=10,
                                 seconds_to_close=500, universe="SPX_CONSTITUENTS"))
    assert compute(p)["features"]["auction"]["value"] == .8
    p["observations"][-1]["values"]["seconds_to_close"] = 1000
    assert compute(p)["features"]["auction"]["value"] is None


def test_compression_gamma_and_fixed_boundary_failures():
    p = packet()
    for r in p["observations"]:
        t = r["event_at"]
        r["values"]["price"] = 100 + (.15 if t % 10 else -.15) if t <= 900 else 100 + (.01 if t % 10 else -.01)
    p["observations"].append(row(1200, "GEX", net_gex=100, spot=100, call_wall=100.1, put_wall=99.9))
    f = compute(p)["features"]
    assert f["range_compression"]["value"] > .4
    assert f["gamma_range"]["value"] == f["range_compression"]["value"]
    assert f["failed_breaks"]["status"] == "available"


def forward(t, price, bid=None, ask=None):
    p = {"decision_at": t, "session": "2026-10-05", "provenance": "prospective",
         "observations": [row(t, price=price)]}
    if bid is not None:
        p["observations"].append(row(t, "SPYU", bid=bid, ask=ask, bid_size=100, ask_size=100))
    return p


def test_actual_spyu_spread_can_erase_underlying_gain():
    p = forward(0, 100, 49.9, 50.1)
    future = [forward(t, 100 + t / 6000, 50., 50.2) for t in range(5, 61, 5)]
    y = outcome(p, future, 60)
    assert y["forward_bps"] > 0
    assert y["spyu_long_net_bps"] < 0
    for r in future[-1]["observations"]:
        if r["instrument"] == "SPYU":
            r["available_at"] = 61
    assert outcome(p, future, 60)["spyu_long_net_bps"] is None


def test_gaps_cross_session_and_retrospective_labels_are_unscoreable():
    p = forward(0, 100)
    future = [forward(t, 100) for t in range(5, 61, 5)]
    assert outcome(p, future, 60)
    assert outcome(p, future[3:], 60) is None
    future[-1]["session"] = "2026-10-06"
    assert outcome(p, future, 60) is None
    report = evaluate([packet()])
    assert report["status"] == "EXPLORATORY_NO_VALIDATED_EDGE"
    assert report["excluded_nonprospective"] == 1
    assert len(report["results"]) == len(FEATURES) * 3 * 2
    assert report["holdout_examined"] is False
    assert all(r["split"] != "holdout" for r in report["results"])
    assert all(r["active"] == 0 for r in report["results"])


def test_appending_future_input_cannot_change_features():
    p = packet()
    original = compute(p)
    changed = deepcopy(p)
    changed["observations"].append(row(1205, price=1000))
    assert compute(changed) == original


def test_reject_duplicate_decisions():
    with pytest.raises(ValueError, match="duplicate"):
        evaluate([packet(), packet()])


def test_paired_baseline_and_cost_sensitivity_on_identical_timestamps():
    packets = []
    for day in range(5, 10):
        offset = (day - 5) * 10000
        p = packet()
        p["provenance"] = "prospective"  # fixture exercises the admitted-packet contract
        p["session"] = f"2026-10-{day:02}"
        p["decision_at"] += offset
        for r in p["observations"]:
            r["event_at"] += offset
            r["available_at"] += offset
        p["observations"] += [row(1200 + offset, "ES_BOOK", bid=5000, ask=5000.25, bid_size=900, ask_size=100),
                              row(1200 + offset, "SPYU", bid=49.9, ask=50.1, bid_size=100, ask_size=100)]
        packets.append(p)
        for t in range(1205, 1261, 5):
            q = forward(t + offset, 100 + t / 10000, 50., 50.2)
            q["session"] = p["session"]
            packets.append(q)
    report = evaluate(packets, include_holdout=True)
    result = next(r for r in report["results"] if r["feature"] == "es_depth" and r["seconds"] == 60 and r["split"] == "holdout")
    assert result["active"] == result["economic_observations"] == 1
    assert result["paired_excess_bps"] == 0  # same long decision as momentum baseline
    assert result["mean_spyu_net_bps"] < 0
    scenarios = result["extra_cost_sensitivities"]
    assert scenarios["10"]["mean_net_bps"] == pytest.approx(scenarios["0"]["mean_net_bps"] - 10)
    assert result["paired_excess_lower"] is None  # can't infer significance from one session


def test_finite_overflow_does_not_create_neutral_depth_or_flow():
    p = packet()
    p["observations"] += [row(1200, "ES_BOOK", bid=5000, ask=5000.25, bid_size=1e308, ask_size=1e308),
                          row(1200, "ES_FLOW", buy_volume=1e308, sell_volume=1e308,
                              window_seconds=60, classification="verified_aggressor")]
    f = compute(p)["features"]
    assert f["es_depth"]["value"] is None
    assert f["es_aggression"]["value"] is None


def test_inventory_cannot_admit_anik_clock_or_rtd_exchange_time():
    state = {"quote": {"price": 769, "as_of": "2026-10-02T16:00:00Z"}}
    p = normalize(state, 1790956810, "ANIK")
    assert p["provenance"] != "prospective"
    assert p["observations"][0]["clock"] != "trusted_receipt"
    state["quote"]["is_rtd"] = True
    p = normalize(state, 1790956810, "grid-svr")
    assert p["observations"][0]["available_at"] == 1790956810
    assert p["observations"][0]["event_at"] is None
    assert p["observations"][0]["status"] == "unavailable"


def test_cli_publishes_json_safe_input_rejection(tmp_path):
    source, output = tmp_path / "bad.jsonl", tmp_path / "receipt.json"
    source.write_text('{"decision_at": NaN, "session": "fixture"}\n', encoding="utf-8")
    subprocess.run([sys.executable, "-m", "scripts.intraday_lab", str(source), "--output", str(output)],
                   cwd=Path(__file__).resolve().parents[1], check=True, capture_output=True)
    receipt = json.loads(output.read_text(encoding="utf-8"))
    assert receipt["status"] == "INPUT_REJECTED"
    assert len(receipt["input_sha256"]) == 64
    assert "NaN" not in output.read_text(encoding="utf-8")


@pytest.mark.parametrize("instrument,invalid,feature", [
    ("ES_BOOK", {"ask": 4999}, "es_depth"),
    ("ES_BOOK", {"bid_size": 0}, "es_depth"),
    ("ES_BOOK", {"ask_size": -1}, "es_depth"),
    ("BREADTH", {"up_volume": -1}, "breadth"),
    ("BREADTH", {"down_volume": -1}, "breadth"),
])
@pytest.mark.parametrize("age", [10, 10.000001, 60])
def test_invalid_latest_cannot_refresh_eligible_measurement(instrument, invalid, feature, age):
    p = packet()
    valid = ({"bid": 5000, "ask": 5000.25, "bid_size": 900, "ask_size": 100}
             if instrument == "ES_BOOK" else {"tick": -800, "up_volume": 100, "down_volume": 900})
    measured = row(1200 - age, instrument, **valid)
    measured["available_at"] += 1
    p["observations"].append(measured)
    before = compute(p)["features"][feature]
    invalid_row = row(1200, instrument, **dict(valid, **invalid))
    p["observations"].append(invalid_row)
    after = compute(p)["features"][feature]
    assert after == before
    if age <= 10:
        assert after["status"] == "available"
        assert after["latest_available_at"] == measured["available_at"]
    else:
        assert after["status"] == "unavailable" and after["value"] is None
        assert "latest_available_at" not in after
    if instrument == "BREADTH":
        divergence = compute(p)["features"]["breadth_divergence"]
        assert divergence["status"] == after["status"]
    # A real eligible fresh replacement recovers, not a fresh invalid receipt.
    invalid_row["values"] = valid
    recovered = compute(p)["features"][feature]
    assert recovered["status"] == "available"
    assert recovered["latest_available_at"] == 1200


def test_filtered_book_freshness_also_guards_ofi_and_partial_repair():
    p = packet()
    valid = {"bid": 5000, "ask": 5000.25, "bid_size": 900, "ask_size": 100}
    p["observations"] += [row(t, "ES_BOOK", **dict(valid, bid_size=900 + t - 1140))
                          for t in range(1140, 1181, 10)]
    for t in (1190, 1200):
        p["observations"].append(row(t, "ES_BOOK", **dict(valid, ask=4999, bid_size=0)))
    withheld = compute(p)["features"]
    assert all(withheld[name]["status"] == "unavailable" for name in ("es_depth", "es_ofi"))
    # Correcting the cross without restoring displayed size is insufficient.
    p["observations"][-1]["values"]["ask"] = valid["ask"]
    assert compute(p)["features"]["es_depth"]["status"] == "unavailable"
    p["observations"][-1]["values"] = dict(valid, bid_size=960)
    partial = compute(p)["features"]
    assert partial["es_depth"]["status"] == "available"
    assert partial["es_ofi"]["status"] == "unavailable"  # Missing eligible 1190 leaves a 20s gap.
    p["observations"][-2]["values"] = dict(valid, bid_size=950)
    restored = compute(p)["features"]
    assert all(restored[name]["status"] == "available" for name in ("es_depth", "es_ofi"))
    assert all(restored[name]["latest_available_at"] == 1200 for name in ("es_depth", "es_ofi"))


@pytest.mark.parametrize("instrument", ["ES_BOOK", "BREADTH"])
@pytest.mark.parametrize("defect", ["future_event", "future_receipt", "unknown_clock"])
def test_invalid_timing_cannot_supply_fresh_recovery(instrument, defect):
    p = packet()
    valid = ({"bid": 5000, "ask": 5000.25, "bid_size": 900, "ask_size": 100}
             if instrument == "ES_BOOK" else {"tick": -800, "up_volume": 100, "down_volume": 900})
    p["observations"].append(row(1140, instrument, **valid))
    newest = row(1200, instrument, **valid)
    if defect == "future_event":
        newest["event_at"] = newest["available_at"] = 1201
    elif defect == "future_receipt":
        newest["available_at"] = 1201
    else:
        newest["clock"] = "unknown"
    p["observations"].append(newest)
    names = ("es_depth", "es_ofi") if instrument == "ES_BOOK" else ("breadth", "breadth_divergence")
    result = compute(p)["features"]
    assert all(result[name]["status"] == "unavailable" and result[name]["value"] is None for name in names)
    newest.update(event_at=1200, available_at=1200, clock="trusted_receipt")
    assert compute(p)["features"][names[0]]["status"] == "available"


def test_missing_unknown_and_valid_zero_breadth_are_distinct():
    p = {"decision_at": 1200, "session": "synthetic", "observations": []}
    missing = compute(p)
    assert missing["provenance"] == "unknown"
    assert missing["features"]["breadth"]["value"] is None
    p["observations"].append(row(1200, "BREADTH", tick=0, up_volume=100, down_volume=100))
    zero = compute(p)["features"]["breadth"]
    assert zero["status"] == "available" and zero["value"] == 0
    p["observations"][-1]["clock"] = "unknown"
    unknown = compute(p)["features"]["breadth"]
    assert unknown["status"] == "unavailable" and unknown["value"] is None
