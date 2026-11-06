"""Economic and temporal contracts for frozen Astrogrid research."""
import hashlib
import json
import zipfile
from datetime import datetime, timedelta, timezone
from itertools import pairwise
from pathlib import Path

import numpy as np
import pytest

from evaluation import astrogrid_edge as edge

UTC = timezone.utc


def event(day="2026-08-01", direction=1, change=.01, momentum=-1):
    return {"symbol": "BTC", "day": day, "signal_end": day + "T01:00:00+00:00",
            "direction": direction, "return": change, "momentum": momentum,
            "forecast_return": direction * change}


@pytest.mark.parametrize("direction", [-1, 1])
def test_fees_and_slippage_charge_entry_and_exit_notional(direction):
    values = edge.strategy_returns([event(direction=direction)])
    assert values[0, 0] == pytest.approx(direction * .01 - .0012 * 2.01)
    assert values[0, 1] == 0  # cash never pays trade fees
    assert values[0, 5] == pytest.approx(-.0012 * 2.01)  # analytic coin-flip expectation
    assert values[0, 7] == pytest.approx(values[0, 0] if direction == 1 else 0)


def test_cost_scenarios_do_not_change_gross_direction_or_baselines():
    e = [event(change=-.02, direction=-1)]
    assert edge.strategy_returns(e, 0)[0, 0] == .02
    assert edge.strategy_returns(e, 20)[0, 0] == pytest.approx(.02 - .001 * 1.98)
    assert edge.strategy_returns(e, 24)[0, 2] == pytest.approx(-.02 - .0012 * 1.98)
    with pytest.raises(ValueError):
        edge.strategy_returns(e, float("nan"))


def bars(n=30, gap=None):
    start = int(edge.SPLIT.timestamp() * 1000)
    return {"BTC": [edge.Bar(start + (i + 1) * edge.STEP, 99, 100 + i, 1_000_000, 700_000, 1000)
                    for i in range(n) if i != gap]}


def test_primary_entry_is_after_signal_and_exit_respects_actual_due_rounding():
    chosen, _ = edge.replay_events(bars(), edge.SPLIT, edge.SPLIT + timedelta(hours=3))
    first = chosen[0]
    assert first["signal_price"] == 100
    assert first["entry_price"] == 101
    assert first["outcome_price"] == 107
    assert first["return"] == pytest.approx(107 / 101 - 1)
    assert first["forecast_return"] == pytest.approx(107 / 100 - 1)
    assert datetime.fromisoformat(first["outcome_end"]) - datetime.fromisoformat(first["signal_end"]) == timedelta(minutes=35)
    for before, after in pairwise(chosen):
        assert before["outcome_end"] <= after["signal_end"]


def test_missing_window_and_crossing_split_never_get_imputed():
    chosen, counts = edge.replay_events(bars(gap=3), edge.SPLIT, edge.SPLIT + timedelta(minutes=50))
    assert counts["missing_window"] == 1
    assert counts["boundary_purged"] > 0
    assert chosen == []


def test_changing_future_prices_cannot_change_which_events_are_selected():
    original = bars()
    modified = {"BTC": [edge.Bar(b.end, b.opening, b.close * (2 if i % 2 else .5),
                                 b.quote, b.buy_quote, b.trades) for i, b in enumerate(original["BTC"])]}
    one, _ = edge.replay_events(original, edge.SPLIT, edge.SPLIT + timedelta(hours=3))
    two, _ = edge.replay_events(modified, edge.SPLIT, edge.SPLIT + timedelta(hours=3))
    assert [e["signal_end"] for e in one] == [e["signal_end"] for e in two]


def test_bootstrap_keeps_all_assets_on_each_date_together_and_is_deterministic():
    events, values = [], []
    for i in range(21):
        day = (edge.SPLIT + timedelta(days=i)).date().isoformat()
        for sign in (-1, 1):
            events.append(event(day))
            values.append([sign * (i + 1), 3])
    boot = edge.daily_bootstrap(events, np.asarray(values), n_boot=100)
    assert np.array_equal(boot, edge.daily_bootstrap(events, np.asarray(values), n_boot=100))
    assert np.all(boot[:, 0] == 0)
    assert np.all(boot[:, 1] == 3)


def test_summary_does_not_treat_qualifying_target_hit_as_net_profit():
    e = [event(change=.0015)]
    summary = edge.summarize(e, edge.SPLIT, edge.END)
    assert summary["forecast_close_basis"]["hit_rate"] == 1
    assert summary["strategies"]["flow"]["mean_net_bps"] < 0
    assert not summary["sample"]["minimum_sample_met"]


@pytest.mark.parametrize("scale", [1, 1000])
def test_provider_timestamp_units_validated_explicitly(tmp_path, scale):
    start = int(edge.SPLIT.timestamp() * 1000)
    row = [start * scale, "100", "101", "99", "100", "10000",
           (start + edge.STEP) * scale - 1, "1000000", "1000", "7000", "700000", "0"]
    archive = tmp_path / "bars.zip"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("bars.csv", ",".join(map(str, row)))
    assert edge.parse_archive(archive, "BTC")[0].end == start + edge.STEP
    row[10] = "1000001"
    with zipfile.ZipFile(archive, "w") as z:
        z.writestr("bars.csv", ",".join(map(str, row)))
    with pytest.raises(ValueError):
        edge.parse_archive(archive, "BTC")


def test_published_report_matches_frozen_protocol_and_declared_sample():
    root = Path(__file__).resolve().parents[1]
    r = json.loads((root / "astrogrid_web/research/spot-flow-edge-v1.json").read_text())
    assert r["protocol_sha256"] == hashlib.sha256((root / edge.PROTOCOL).read_bytes()).hexdigest()
    assert r["origin"] == "historical_archive_replay_not_prospective"
    assert r["test"]["sample"]["events"] == 10854
    assert r["test"]["sample"]["utc_dates"] == 92
    assert r["test"]["sample"]["minimum_sample_met"]
    assert len(r["archive_manifest"]) == 36
    assert sum(q["rows"] for q in r["data_quality"].values()) == 315360
    assert r["verdict"] == "NO_POSITIVE_NET_RETURN"
    assert r["test"]["strategies"]["flow"]["mean_net_95ci_bps"][1] < 0
    for interval in r["test"]["paired_advantages"].values():
        assert interval["simultaneous_lower_bps"] <= 0
