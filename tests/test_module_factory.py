"""Module factory, factory 1 (3pacs/GRID#1155): tests at seam S1.

Run file + fixture SPY minute bars -> factory ledger reopened with its anchor
-> ``summarize()`` + report. Plus the max-T statistics and the ledger rules.
"""

import hashlib
import json
import sqlite3
from datetime import date, timedelta

import numpy as np
import pytest

from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.offline_research_proof import (
    block_permutations,
    max_t_adjusted,
    net_t_matrix,
)


def sessions(n, first=date(2023, 1, 3)):
    d, out = first, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def make_bars(path, days, plant=0.0, seed=7, short_day=None):
    """SPY minute bars 09:30..15:59 as a random walk. ``plant``: added to every bar from 14:00 to 14:59
    (spread evenly) on sessions whose 13:59 close is in the top 15% of the day's range so far."""
    rng = np.random.default_rng(seed)
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                " primary key (sym, day, hm))")
    px = 400.0
    for day in days:
        n_bars = 200 if day == short_day else 390
        closes, hi, lo, planted_on = [], None, None, False
        for i in range(n_bars):
            hm = "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))
            step = rng.normal(0, 0.05)
            if plant and hm >= "14:00" and hm < "15:00" and planted_on:
                step += plant / 60
            o = px
            px = px + step
            h, l = max(o, px) + 0.01, min(o, px) - 0.01
            hi = h if hi is None else max(hi, h)
            lo = l if lo is None else min(lo, l)
            if hm == "13:59":
                planted_on = (px - lo) / (hi - lo) >= 0.85 if hi > lo else False
            if hm < "13:59":
                planted_on = False
            con.execute("insert into bars values ('SPY', ?, ?, ?, ?, ?, ?, 1000)", (day, hm, o, h, l, px))
            closes.append(px)
    con.commit()
    con.close()


def staged(tmp_path, plant=0.0, n=320, seed=7, short_day=None, first=date(2023, 1, 3)):
    data = tmp_path / "factory-data" / "dev"
    (data / "edge9").mkdir(parents=True)
    days = sessions(n, first)
    make_bars(data / mf.DATA_FILE, days, plant=plant, seed=seed, short_day=short_day)
    digest = hashlib.sha256((data / mf.DATA_FILE).read_bytes()).hexdigest()
    (data / "MANIFEST.json").write_text(json.dumps({
        "dev_first": "2022-11-14", "dev_last": "2026-11-10",
        "files": [{"path": mf.DATA_FILE, "sha256": digest, "days": [days[0], days[-1]]}],
    }))
    return data, days


def run_file(tmp_path, days, **over):
    body = {
        "run_id": "factory-test-1",
        "first_session": days[0],
        "last_session": days[-1],
        "decision_times": ["14:00"],
        "horizons": ["60m"],
        "features": ["F1", "F10", "F11", "F1_shift30"],
        "z_thresholds": [1.0],
        "cost_per_share": 0.04,
        "alpha": 0.05,
        "perms": 499,
        "seed": 1,
        "min_n": 30,
        "z_history": 20,
        **over,
    }
    path = tmp_path / f"{body['run_id']}.json"
    path.write_text(json.dumps(body))
    return path, body


def file_ledger(tmp_path, genesis=True):
    (tmp_path / "ledger").mkdir(exist_ok=True)
    (tmp_path / "anchor").mkdir(exist_ok=True)
    paths = tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"
    if genesis:
        lse.Ledger.create_factory(paths[0], ledger_id="factory-test", anchor=paths[1])
    return paths


# --- seam S1: run file + bars -> ledger + summarize + report ------------------------


def test_planted_effect_is_ranked_first_and_selected(tmp_path):
    data, days = staged(tmp_path, plant=0.9)
    rf, body = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")

    top = rep["ranking"][0]
    assert top["feature"] == "F10|z>=1|long" and top["family"] == "SPY|change|14:00+60m"
    assert top["selected"] and top["adjusted_p"] <= 0.05 and top["r"] > 0
    assert rep["trial_count"] == 16 == mf.RunFile.from_dict(body).trial_count()
    assert rep["max_t"]["critical_t"] > 0
    assert (tmp_path / "out" / "report.md").read_text().startswith("# Factory run factory-test-1")

    summary = lse.summarize(lse.Ledger(lp, anchor=ap))  # reopened with its anchor
    assert summary["program"] == "module_factory" and not summary["promotion_allowed"]
    assert (summary["runs_declared"], summary["runs_recorded"], summary["run_open"]) == (1, 1, None)
    assert summary["candidates_declared"] == summary["candidates_measured"] == 16
    assert summary["candidates_selected"] == len(rep["selected"]) >= 1
    assert summary["runs"][0]["critical_t"] == pytest.approx(rep["max_t"]["critical_t"])


def test_noise_selects_little_and_the_bar_scales_with_trials(tmp_path):
    data, days = staged(tmp_path, plant=0.0, seed=11)
    lp, ap = file_ledger(tmp_path)
    small, _ = run_file(tmp_path, days, run_id="noise-small")
    big, _ = run_file(tmp_path, days, run_id="noise-big", z_thresholds=[0.5, 1.0, 1.5],
                      decision_times=["11:00", "14:00"], horizons=["30m", "60m"])
    a = mf.run(small, data, lse.Ledger(lp, anchor=ap), tmp_path / "a")
    b = mf.run(big, data, lse.Ledger(lp, anchor=ap), tmp_path / "b")
    assert b["trial_count"] == 2 * 2 * 4 * 3 * 2 * 2
    assert len(b["selected"]) <= 1
    assert b["max_t"]["critical_t"] > a["max_t"]["critical_t"]  # more modules, higher bar
    s = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert s["runs_recorded"] == 2 and s["candidates_declared"] == a["trial_count"] + b["trial_count"]


def test_rows_are_point_in_time_and_short_sessions_are_skipped(tmp_path):
    data, days = staged(tmp_path, n=60, short_day=sessions(60)[30])
    spec = mf.RunFile.from_dict(run_file(tmp_path, days, target_unit="dollars")[1])
    families, cov = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec)
    assert cov["sessions_skipped_short"] == 1
    rows = families["SPY|change|14:00+60m"]
    assert len(rows) == 60 - 1 - 1  # first session has no prior close; the short one is skipped
    assert all(r["cost"] == 0.04 and r["target"] == r["target_dollars"] for r in rows)
    for row in rows:
        for sig in row["features"].values():
            assert sig["known_at"] == row["decision_at"]
        assert row["label_end"] > row["decision_at"]
    # z needs z_history earlier sessions: nothing fires on the first 20 rows
    assert all(s["value"] is None for row in rows[:20] for s in row["features"].values())


def test_feature_definitions_match_edge9():
    bars = {f"{h:02d}:{m:02d}": (0, 0, 0, 0, 0) for h in range(9, 16) for m in range(60) if (h, m) >= (9, 30)}
    day = {k: (100.0, 100.0, 100.0, 100.0, 1) for k in bars}
    day["10:00"] = (100.0, 104.0, 100.0, 103.0, 1)
    day["13:29"] = (101.0, 101.0, 98.0, 101.0, 1)
    day["13:59"] = (102.0, 102.0, 102.0, 102.0, 1)
    prev = {k: (100.0, 101.0, 96.0, 100.0, 1) for k in bars}
    prev["15:59"] = (100.0, 101.0, 96.0, 100.0, 1)
    raw = mf.raw_features(mf.Session(day), mf.Session(prev), "13:59")
    assert raw["F1"] == pytest.approx(0.02)
    assert raw["F1_shift30"] == pytest.approx(0.02 - 0.01)  # F1 now minus F1 at the 13:29 bar (close 101)
    assert raw["F10"] == pytest.approx((102 - 98) / (104 - 98))
    assert raw["F11"] == pytest.approx((102 - 96) / (101 - 96))


# --- guards -------------------------------------------------------------------------


def test_holdout_is_refused(tmp_path):
    with pytest.raises(mf.RunFileError, match="holdout"):
        mf.RunFile.from_dict(run_file(tmp_path, ["2022-11-11", "2023-01-03"])[1])
    with pytest.raises(mf.RunFileError, match="holdout"):
        mf.check_data(tmp_path / "gex-edge9-data" / "holdout")
    data, days = staged(tmp_path, n=40, first=date(2022, 11, 1))
    spec = mf.RunFile.from_dict(run_file(tmp_path, ["2022-11-14", days[-1]])[1])
    with pytest.raises(mf.RunFileError, match="before dev"):
        mf.load_sessions(data / mf.DATA_FILE, spec)


def test_manifest_mismatch_abandons_the_declared_run(tmp_path):
    data, days = staged(tmp_path, n=60)
    with sqlite3.connect(data / mf.DATA_FILE) as con:
        con.execute("update bars set c = c + 1 where hm = '10:00'")
    rf, _ = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    with pytest.raises(mf.RunFileError, match="manifest"):
        mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")
    s = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert (s["runs_declared"], s["runs_abandoned"], s["runs_recorded"], s["run_open"]) == (1, 1, 0, None)
    assert s["candidates_declared"] == 16  # abandoned runs still count


def test_run_file_refuses_unknown_keys_and_bad_values(tmp_path):
    days = sessions(5)
    for over in ({"surprise": 1}, {"horizons": ["90"]}, {"decision_times": ["09:45"]}, {"alpha": 0.2},
                 {"decision_times": ["15:00"], "horizons": ["90m"]}, {"features": ["F99"]}):
        with pytest.raises(mf.RunFileError):
            mf.RunFile.from_dict(run_file(tmp_path, days, **over)[1])


# --- ledger rules -------------------------------------------------------------------


def declared(ledger, run_id="r1", trial_count=3):
    return lse.record_factory_declared(ledger, run_id=run_id, run_file={"run_id": run_id},
                                       data_manifest_sha256="0" * 64, trial_count=trial_count)


def test_factory_ledger_takes_factory_records_only_and_one_open_run():
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    with pytest.raises(ValueError):
        lse.Ledger.create_factory(None, ledger_id=lse.CANONICAL_LEDGER_ID)
    with pytest.raises(ValueError, match="refuses"):
        lse.record_hypothesis_batch(ledger, scorer_version="v", evaluated_at="2026-01-01T00:00:00+00:00", trials=[])
    declared(ledger)
    with pytest.raises(ValueError, match="still open"):
        declared(ledger, "r2")
    with pytest.raises(ValueError, match="exactly the trials"):
        lse.record_factory_result(ledger, trials=[], max_t={}, report_sha256="a", frozen_sha256="b")
    lse.record_factory_abandoned(ledger, reason="test")
    declared(ledger, "r2")
    assert lse.declared_trial_count(ledger) == 6


def test_s11_ledger_refuses_factory_records():
    ledger = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="refuses"):
        declared(ledger)


def test_factory_chain_survives_reload_and_detects_edits(tmp_path):
    lp, ap = file_ledger(tmp_path)
    ledger = lse.Ledger(lp, anchor=ap)
    declared(ledger)
    lines = lp.read_bytes().split(b"\n")
    lines[1] = lines[1].replace(b'"trial_count":3', b'"trial_count":2')
    lp.write_bytes(b"\n".join(lines))
    with pytest.raises(ValueError):
        lse.Ledger(lp, anchor=ap)


# --- max-T statistics ---------------------------------------------------------------


def test_net_t_matrix_matches_direct_computation():
    rng = np.random.default_rng(3)
    y = rng.normal(0, 1, 50)
    sides = np.zeros((50, 2))
    sides[::3, 0] = 1
    sides[1::4, 1] = -1
    t = net_t_matrix(y[None, :], sides, 0.04)[0]
    for j in range(2):
        fired = sides[:, j] != 0
        net = sides[fired, j] * y[fired] - 0.04
        assert t[j] == pytest.approx(net.mean() / (net.std(ddof=1) / np.sqrt(len(net))), rel=1e-5)


def test_max_t_adjusted_is_monotone_above_marginal_and_matches_brute_force():
    from analysis.offline_research_proof import Protocol

    rng = np.random.default_rng(5)
    n, perms = 80, 299
    days = sessions(n)
    y = rng.normal(0, 1, n)
    y[::5] += 1.0
    signals = {"a": [1.0 if i % 5 == 0 else None for i in range(n)],
               "b": [1.0 if i % 2 == 0 else None for i in range(n)],
               "c": [-1.0 if i % 3 == 0 else None for i in range(n)]}
    rows = [{"decision_at": f"{d}T14:00:00-05:00", "target": float(y[i]),
             "features": {k: {"value": v[i]} for k, v in signals.items()}} for i, d in enumerate(days)]
    proto = Protocol(run_id="x", features=("a", "b", "c"), split="2030-01-01T00:00:00+00:00",
                     end="2030-01-02T00:00:00+00:00", perms=perms, seed=9, statistic="net_when_fires",
                     selection="max_t", selection_alpha=0.05, cost=0.0)
    ledger = []
    for k, v in signals.items():
        fired = [(s, y[i]) for i, s in enumerate(v) if s is not None]
        net = np.array([s * t for s, t in fired])
        ledger.append({"family": "target", "feature": k, "status": "tested",
                       "t": float(net.mean() / (net.std(ddof=1) / np.sqrt(len(net))))})
    marginal, adjusted, critical, _ = max_t_adjusted({"target": rows}, ledger, proto, 1)
    assert all(a >= m - 1e-12 for a, m in zip(adjusted, marginal))
    order = np.argsort([-t["t"] for t in ledger])
    assert all(adjusted[order[i]] <= adjusted[order[i + 1]] + 1e-12 for i in range(2))
    # brute force Westfall-Young over the same permutations
    perm = block_permutations(n, 1, perms, 9)
    sides = np.array([[v[i] or 0.0 for v in signals.values()] for i in range(n)])
    null = net_t_matrix(y[perm], sides, 0.0)
    obs = np.array([t["t"] for t in ledger])
    raw = []
    for rank, j in enumerate(order):
        rest = order[rank:]
        raw.append((1 + (null[:, rest].max(axis=1) >= obs[j] - 1e-6).sum()) / (perms + 1))
    expect = np.maximum.accumulate(raw)
    for rank, j in enumerate(order):
        assert adjusted[j] == pytest.approx(expect[rank])
    assert critical == pytest.approx(np.quantile(null.max(axis=1), 0.95))


def test_vol_unit_scales_by_earlier_sessions_only_and_converts_the_cost(tmp_path):
    data, days = staged(tmp_path, n=60)
    spec = mf.RunFile.from_dict(run_file(tmp_path, days, vol_history=10)[1])
    families, cov = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec)
    rows = families["SPY|change|14:00+60m"]
    assert cov["labels_unscaled"] == 10 and len(rows) == 60 - 1 - 10
    dollars = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), mf.RunFile.from_dict(
        run_file(tmp_path, days, target_unit="dollars")[1]))[0]["SPY|change|14:00+60m"]
    past = [abs(r["target_dollars"]) for r in dollars]
    for i, row in enumerate(rows):
        scale = float(np.median(past[i:i + 10]))  # the 10 sessions before this one
        assert row["target"] == pytest.approx(row["target_dollars"] / scale)
        assert row["cost"] == pytest.approx(0.04 / scale)
    bps = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), mf.RunFile.from_dict(
        run_file(tmp_path, days, target_unit="bps")[1]))[0]["SPY|change|14:00+60m"]
    assert all(r["cost"] > 0 and abs(r["target"]) > abs(r["target_dollars"]) for r in bps)


def test_report_carries_specs_dollars_and_drift(tmp_path):
    data, days = staged(tmp_path, plant=0.9)
    rf, _ = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")
    assert rep["target_unit"] == "vol"
    assert set(rep["feature_specs"]) == {"F1", "F10", "F11", "F1_shift30"}
    assert all({"definition", "unit", "source", "live_source", "pit"} <= set(v) for v in rep["feature_specs"].values())
    top = rep["ranking"][0]
    assert top["net_dollars"] > 0
    f = rep["coverage"]["features"]["F10@14:00"]
    assert 0 < f["coverage_z"] <= 1 and f["z_drift"] is not None
    trials = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_RESULT)[0]["trials"]
    assert all("net_dollars" in t for t in trials)


def test_net_t_matrix_with_a_cost_per_session():
    rng = np.random.default_rng(4)
    y = rng.normal(0, 1, 40)
    cost = rng.uniform(0.01, 0.2, 40)
    sides = np.zeros((40, 1))
    sides[::2, 0] = 1
    sides[1::7, 0] = -1
    y_nan = y.copy()
    y_nan[5] = np.nan
    t = net_t_matrix(y_nan[None, :], sides, cost)[0, 0]
    fired = (sides[:, 0] != 0) & np.isfinite(y_nan)
    net = sides[fired, 0] * y[fired] - cost[fired]
    assert t == pytest.approx(net.mean() / (net.std(ddof=1) / np.sqrt(len(net))), rel=1e-5)
