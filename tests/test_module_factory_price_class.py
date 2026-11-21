"""Module factory, factory 2 (3pacs/GRID#1156): the price/momentum class and fire-overlap clusters.

Seam S1: run file + fixture minute bars for SPY, QQQ, RSP, HYG, VIXY and TLT -> factory
ledger reopened with its anchor -> ``summarize()`` + report (ranking, bar, clusters and their
representatives, the co-firing matrix). Plus point-in-time checks across symbols.
"""

import copy
import hashlib
import json
import sqlite3
from datetime import date, timedelta
from pathlib import Path

import numpy as np
import pytest

from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.factory_stats import FactoryProtocol, cofiring, discover
from analysis.offline_research_proof import digest

EXAMPLE = Path(__file__).resolve().parents[1] / "docs/research/factory-runs/factory2-price-class-20261119.json"
# per-minute step sd and the share of minutes with a trade (the thin ETFs skip minutes on Nasdaq)
WALK = {"SPY": (400.0, 0.05, 1.0), "QQQ": (350.0, 0.06, 1.0), "RSP": (150.0, 0.02, 0.97),
        "HYG": (75.0, 0.01, 0.9), "VIXY": (20.0, 0.02, 0.75), "TLT": (90.0, 0.03, 1.0)}


def sessions(n, first=date(2023, 1, 3)):
    d, out = first, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def make_bars(path, days, plant=0.0, seed=7, early=None):
    """Six symbols as random walks, SPY and QQQ with every minute, the others with gaps.

    ``plant``: added to SPY over 14:00..14:59 on sessions where TLT is up >= 0.6% on its prior close
    at the 13:59 bar (an F5 effect). ``early``: (symbol, day) given one extra bar before dev.
    """
    rng = np.random.default_rng(seed)
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                " primary key (sym, day, hm))")
    px = {s: w[0] for s, w in WALK.items()}
    prior_close = dict(px)
    rows = []
    for day in days:
        tlt_up = False
        for i in range(390):
            hm = "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))
            common = rng.normal()
            for sym, (_, sd, share) in WALK.items():
                step = sd * (0.6 * common + 0.8 * rng.normal()) if sym in ("SPY", "QQQ", "RSP") else sd * rng.normal()
                if sym == "SPY" and plant and "14:00" <= hm < "15:00" and tlt_up:
                    step += plant / 60
                o = px[sym]
                px[sym] = o + step
                if sym == "TLT" and hm == "13:59":
                    tlt_up = px["TLT"] / prior_close["TLT"] - 1 >= 0.006
                if share < 1.0 and hm not in ("09:30", "15:59") and rng.random() > share:
                    continue  # no trade this minute: no bar
                rows.append((sym, day, hm, o, max(o, px[sym]) + 0.01, min(o, px[sym]) - 0.01, px[sym], 100))
        prior_close = dict(px)
    if early:
        rows.append((early[0], early[1], "15:59", 1.0, 1.0, 1.0, 1.0, 1))
    con.executemany("insert into bars values (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.commit()
    con.close()


def staged(tmp_path, n=200, **kw):
    data = tmp_path / "factory-data" / "dev"
    (data / "edge9").mkdir(parents=True)
    days = sessions(n)
    make_bars(data / mf.DATA_FILE, days, **kw)
    sha = hashlib.sha256((data / mf.DATA_FILE).read_bytes()).hexdigest()
    (data / "MANIFEST.json").write_text(json.dumps({
        "dev_first": "2022-11-14", "dev_last": "2026-11-10",
        "files": [{"path": mf.DATA_FILE, "sha256": sha, "days": [days[0], days[-1]]}],
    }))
    return data, days


def run_file(tmp_path, days, **over):
    body = {
        "run_id": "factory2-test",
        "first_session": days[0],
        "last_session": days[-1],
        "decision_times": ["13:30", "14:00"],
        "horizons": ["30m", "60m"],
        "features": ["F1", "F2", "F3", "F5", "F5_shift30", "F6"],
        "z_thresholds": [1.0],
        "perms": 299,
        "seed": 3,
        "z_history": 20,
        "beta_history": 20,
        **over,
    }
    path = tmp_path / f"{body['run_id']}.json"
    path.write_text(json.dumps(body))
    return path, body


def file_ledger(tmp_path):
    (tmp_path / "ledger").mkdir()
    (tmp_path / "anchor").mkdir()
    lp, ap = tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"
    lse.Ledger.create_factory(lp, ledger_id="factory2-test", anchor=ap)
    return lp, ap


def rows_for(data, body):
    spec = mf.RunFile.from_dict(body)
    bars = mf.load_bars(data / mf.DATA_FILE, spec)
    return spec, mf.build_rows(bars.pop("SPY"), spec, cross=bars)


# --- seam S1 --------------------------------------------------------------------------------


def test_price_class_run_ranks_clusters_and_ledgers_at_seam_s1(tmp_path):
    data, days = staged(tmp_path, plant=2.0)
    rf, body = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")

    assert rep["trial_count"] == 2 * 2 * 6 * 2 * 2 == mf.RunFile.from_dict(body).trial_count()
    top = rep["ranking"][0]
    assert top["feature"] == "F5|z>=1|long" and top["family"].startswith("SPY|change|14:00+")
    assert top["selected"] and top["representative"]
    assert set(rep["coverage"]["symbols"]) == set(mf.SYMBOLS)
    assert rep["feature_specs"]["F5"]["live_source"].startswith("none yet")

    # clusters: one representative each, the best tested trial; the planted idea is one cluster
    clusters = {c["cluster"]: c for c in rep["clusters"]}
    best = clusters[top["cluster"]]
    assert best["representative"]["trial_id"] == top["trial_id"] and "14:00|F5|z>=1" in best["signals"]
    assert rep["clusters"][0]["cluster"] == top["cluster"]  # ordered by representative t
    assert rep["cofiring"]["representatives_selected"] >= 1
    assert sum(r["selected"] for r in rep["ranking"] if r["cluster"] == top["cluster"]) >= 2  # same idea twice
    reps = [c["representative"] for c in rep["clusters"] if c["representative"]]
    assert len({r["trial_id"] for r in reps}) == len(reps) == rep["cofiring"]["clusters_with_tested_trials"]

    # the matrix is beside the report, bound to it by hash; no two clusters link at the threshold
    matrix = json.loads((tmp_path / "out" / "cofiring.json").read_text())
    assert digest(matrix) == rep["cofiring"]["sha256"] and matrix["basis"].startswith("fire overlap")
    both = np.array(matrix["both"], dtype=float)
    fires = np.array(matrix["fires"], dtype=float)
    with np.errstate(divide="ignore", invalid="ignore"):
        jac = np.nan_to_num(both / (fires[:, None] + fires[None, :] - both))
    where = {s: c["cluster"] for c in rep["clusters"] for s in c["signals"]}
    names = matrix["signals"]
    for i in range(len(names)):
        for j in range(i + 1, len(names)):
            if where[names[i]] != where[names[j]]:
                assert jac[i, j] < 0.5
    assert "Clusters by fire overlap" in (tmp_path / "out" / "report.md").read_text()

    summary = lse.summarize(lse.Ledger(lp, anchor=ap))  # reopened with its anchor
    assert (summary["runs_declared"], summary["runs_recorded"], summary["run_open"]) == (1, 1, None)
    assert summary["candidates_declared"] == summary["candidates_measured"] == rep["trial_count"]
    run = summary["runs"][0]
    assert run["clusters"] == rep["cofiring"]["clusters"]
    assert run["representatives_selected"] == rep["cofiring"]["representatives_selected"]
    assert run["critical_t"] == pytest.approx(rep["max_t"]["critical_t"])
    trials = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_RESULT)[0]["trials"]
    assert sum(t["representative"] for t in trials) == rep["cofiring"]["clusters_with_tested_trials"]


def test_example_run_file_is_the_full_price_class():
    spec = mf.RunFile.from_dict(json.loads(EXAMPLE.read_text()))
    assert spec.decision_times == tuple(f"{h:02d}:{m:02d}" for h in range(10, 16) for m in (0, 30))[:11]
    assert spec.horizons == ("15m", "30m", "60m", "close")
    assert set(spec.features) == set(mf.ALL_FEATURES) and spec.symbols() == mf.SYMBOLS
    assert spec.trial_count() == 11 * 4 * len(mf.ALL_FEATURES) * 2 * 2 * 2
    assert spec.first_session >= mf.DEV_FIRST


def test_frequent_run_file_is_the_four_live_features_at_low_thresholds():
    """Plan stage 1 (#1153): the same windows as factory 3, only the features a live slot records,
    thresholds low enough that a rule fires on a large share of sessions."""
    runs = EXAMPLE.parent
    body = json.loads((runs / "factory-frequent-price-20261120.json").read_text())
    spec = mf.RunFile.from_dict(body)
    assert spec.run_id == "factory-frequent-price-20261120"
    assert spec.features == mf.FEATURES == ("F1", "F10", "F11", "F1_shift30") and spec.symbols() == ("SPY",)
    assert spec.z_thresholds == (0.25, 0.5) and spec.sides == ("long", "short")
    assert spec.class_shares == {"price": 1.0} and spec.class_counts() == {"price": 1376}
    assert len(spec.families()) == 11 * 4 - 1 and "SPY|change|15:00+60m" not in spec.families()
    assert len(spec.modules()) == 4 * 2 * 2 * 2 and spec.trial_count() == 43 * 32 == 1376
    assert "F1|z>=0.25|long" in spec.modules() and "F1_shift30|z<=0.5|short" in spec.modules()
    # every other setting is factory 3's; the seed is used by no other run file
    factory3 = json.loads((runs / "factory3-tape-calendar-20261119.json").read_text())
    differs = {"run_id", "features", "class_shares", "z_thresholds", "seed"}
    assert set(body) == set(factory3)
    assert {k: v for k, v in body.items() if k not in differs} == \
        {k: v for k, v in factory3.items() if k not in differs}
    seeds = [json.loads(f.read_text())["seed"] for f in sorted(runs.glob("*.json"))]
    assert seeds.count(body["seed"]) == 1


def test_run_file_refuses_bad_factory2_settings(tmp_path):
    days = sessions(5)
    for over in ({"cluster_by": "returns"}, {"cluster_threshold": 0}, {"cluster_threshold": 1.5},
                 {"stale_minutes": -1}, {"beta_history": 5}, {"features": ["F7"]}, {"symbol": "QQQ"}):
        with pytest.raises(mf.RunFileError):
            mf.RunFile.from_dict(run_file(tmp_path, days, **over)[1])
    assert mf.RunFile.from_dict(run_file(tmp_path, days, features=["F1", "F10"])[1]).symbols() == ("SPY",)
    assert mf.RunFile.from_dict(run_file(tmp_path, days, features=["F2"])[1]).symbols() == ("SPY", "HYG", "VIXY")


# --- clusters come from fire overlap only -----------------------------------------------------


def _rows(fires, targets):
    days = sessions(len(targets))
    return [{"decision_at": f"{d}T14:00:00-05:00", "target": float(targets[i]),
             "features": {m: {"value": (1.0 if m.endswith("long") else -1.0) if i in fires[m.split("|")[0]] else None}
                          for m in MODULES}} for i, d in enumerate(days)]


MODULES = tuple(f"{f}|z>=1|{side}" for f in ("A", "B", "C") for side in ("long", "short"))


def _trials(rows):
    proto = FactoryProtocol(run_id="x", features=MODULES, families=("SPY|change|14:00+60m",),
                            split="2030-01-01T00:00:00+00:00", end="2030-01-02T00:00:00+00:00",
                            perms=99, seed=1, selection_alpha=0.05, min_n=30)
    for i, row in enumerate(rows):
        row.update(origin=proto.origin, label_end=row["decision_at"].replace("14:00", "15:00"), horizon="14:00+60m",
                   label="change", target_known_at=row["decision_at"].replace("14:00", "15:00"))
        for f in row["features"].values():
            f["known_at"] = row["decision_at"]
    return discover(proto, {"SPY|change|14:00+60m": rows})["payload"]["ledger"]


def test_clusters_come_from_fire_overlap_never_from_returns():
    fires = {"A": set(range(0, 120, 2)), "B": set(range(0, 120, 2)) - {0, 2}, "C": set(range(1, 120, 2))}
    rng = np.random.default_rng(1)
    first = {"SPY|change|14:00+60m": _rows(fires, rng.normal(0, 1, 120))}
    other = {"SPY|change|14:00+60m": _rows(fires, rng.normal(5, 3, 120))}  # same fires, other returns
    a = cofiring(first, _trials(first["SPY|change|14:00+60m"]), mf.signal_name)
    b = cofiring(other, _trials(other["SPY|change|14:00+60m"]), mf.signal_name)
    assert a[0] == b[0]  # the matrix never sees a return
    assert sorted(map(sorted, (c["signals"] for c in a[1]))) == sorted(map(sorted, (c["signals"] for c in b[1])))
    groups = sorted(sorted(c["signals"]) for c in a[1])
    assert groups == [["14:00|A|z>=1", "14:00|B|z>=1"], ["14:00|C|z>=1"]]
    assert a[0]["fires"] == [60, 58, 60] and a[0]["both"][0][1] == 58 and a[0]["both"][0][2] == 0
    # long and short of a rule are one signal; one representative per cluster, the best t in it
    for clusters, membership, trials in ((a[1], a[2], _trials(first["SPY|change|14:00+60m"])),):
        by = {t["trial_id"]: t for t in trials}
        for c in clusters:
            members = [by[i] for i, (cid, _) in membership.items() if cid == c["cluster"]]
            assert len(members) == 2 * len(c["signals"])
            assert by[c["representative"]]["t"] == max(t["t"] for t in members)
            assert sum(membership[t["trial_id"]][1] for t in members) == 1
    # phi on the same fires: A and C never co-fire, so they never link
    phi = cofiring(first, _trials(first["SPY|change|14:00+60m"]), mf.signal_name, by="phi", threshold=0.5)
    assert sorted(sorted(c["signals"]) for c in phi[1]) == groups
    with pytest.raises(ValueError):
        cofiring(first, [], mf.signal_name, by="returns")


# --- point in time across symbols ---------------------------------------------------------------


def test_later_bars_of_any_symbol_never_move_an_earlier_decision(tmp_path):
    data, days = staged(tmp_path, n=70)
    _, body = run_file(tmp_path, days, features=list(mf.ALL_FEATURES), decision_times=["10:30", "14:00"],
                       horizons=["30m"], z_history=20, beta_history=20)
    spec, (before, _) = rows_for(data, body)
    cut = days[50]
    with sqlite3.connect(data / mf.DATA_FILE) as con:  # every symbol: from 14:00 on the cut day, and every later day
        con.execute("update bars set o = o * 1.05, h = h * 1.07, l = l * 0.95, c = c * 1.06 "
                    "where (day = ? and hm >= '14:00') or day > ?", (cut, cut))
        con.execute("delete from bars where sym in ('VIXY', 'HYG') and day > ? and hm like '%5'", (cut,))
    _, (after, _) = rows_for(data, body)

    def signals(families, until):
        return {(r["decision_at"], name): s["value"] for rows in families.values() for r in rows
                if r["decision_at"] <= until for name, s in r["features"].items()}

    until = f"{cut}T14:00:00-05:00"
    assert signals(before, until) == signals(after, until)
    fired = {k: v for k, v in signals(before, until).items() if v is not None}
    assert {name.split("|")[0] for _, name in fired} >= {"F2", "F3", "F4", "F5", "F6", "F2_shift30", "F4_shift30"}
    later = "9999"
    assert signals(before, later) != signals(after, later)  # the change is real; it only reaches later decisions


def test_cross_features_read_no_bar_after_the_decision_bar():
    def session(price, last="15:59", step=0.0):
        bars = {}
        for i in range(9 * 60 + 30, mf._minutes(last) + 1):
            p = price + step * (i - 570)
            bars[mf._hm(i)] = (p, p, p, p, 1)
        return mf.Session(bars)

    px = {s: session(p, step=0.01) for s, p in (("SPY", 100), ("QQQ", 300), ("RSP", 50), ("HYG", 80),
                                                ("VIXY", 20), ("TLT", 90))}
    prior = {s: 1.0 + p.close("09:30") for s, p in px.items()}
    betas = {"VIXY": -2.0, "HYG": 0.5}
    at = mf.class_features(px, prior, betas, "13:59", 10)
    cut = {s: mf.Session({hm: b for hm, b in p.bars.items() if hm <= "13:59"}) for s, p in px.items()}
    assert mf.class_features(cut, prior, betas, "13:59", 10) == at and all(v is not None for v in at.values())


def test_definitions_match_edge9_and_stale_thin_prices_are_missing():
    def session(points):
        return mf.Session({hm: (c, c, c, c, 1) for hm, c in points.items()})

    px = {"SPY": session({"09:30": 100.0, "13:29": 101.0, "13:59": 102.0}),
          "QQQ": session({"09:30": 400.0, "13:29": 400.0, "13:59": 408.0}),
          "RSP": session({"09:30": 50.0, "13:59": 51.5}),
          "HYG": session({"09:30": 80.0, "13:52": 80.8}),
          "VIXY": session({"09:30": 20.0, "13:40": 19.0}),  # 19 minutes stale at 13:59
          "TLT": session({"09:30": 90.0, "13:59": 89.1})}
    prior = {"SPY": 100.0, "QQQ": 400.0, "RSP": 50.0, "HYG": 80.0, "VIXY": 20.0, "TLT": 90.0}
    betas = {"VIXY": 2.0, "HYG": 0.5}
    f = mf.class_features(px, prior, betas, "13:59", 10)
    assert f["F2"] is None  # VIXY's last trade is older than stale_minutes: missing, never filled
    assert f["F3"] == pytest.approx(0.03 - 0.02)
    assert f["F4"] == pytest.approx(0.01 - 0.5 * 0.02)
    assert f["F5"] == pytest.approx(-0.01)
    assert f["F6"] == pytest.approx(0.02 - (102 / 101 - 1))
    assert mf.class_features(px, prior, betas, "13:59", 20)["F2"] == pytest.approx(-0.05 - 2.0 * 0.02)
    assert mf.class_features(px, prior, {}, "13:59", 20)["F2"] is None  # no beta yet
    assert mf.ols_slope([1, 2, 3], [2, 4, 6]) == pytest.approx(2.0) and mf.ols_slope([1, 1], [0, 1]) is None
    # S6: last 3 sessions of a month only; month-to-date to the prior close, from the previous month's last close
    cal = ["2023-01-27", "2023-01-30", "2023-01-31", "2023-02-01", "2023-02-22", "2023-02-23", "2023-02-24",
           "2023-02-27", "2023-02-28"]
    closes = {"SPY": {"2023-01-31": 100.0, "2023-02-23": 103.0}, "TLT": {"2023-01-31": 50.0, "2023-02-23": 49.0}}
    assert mf.month_end("2023-02-01", cal, closes) == 0.0
    assert mf.month_end("2023-02-24", cal, closes) == pytest.approx(0.03 - (-0.02))
    assert mf.month_end("2023-02-27", cal, closes) is None  # the prior close is missing


def test_settled_close_needs_a_last_bar_from_1550():
    def s(last):
        return mf.Session({"09:30": (1, 1, 1, 1, 1), last: (2, 2, 2, 2, 1)})

    assert mf.settled_close(s("15:50")) == 2 and mf.settled_close(s("15:49")) is None
    assert mf.settled_close(None) is None


def test_spy_only_run_matches_the_factory_1_path(tmp_path):
    data, days = staged(tmp_path, n=60)
    _, body = run_file(tmp_path, days, features=["F1", "F10", "F11", "F1_shift30"], decision_times=["14:00"],
                       horizons=["60m"])
    spec = mf.RunFile.from_dict(body)
    assert spec.symbols() == ("SPY",)
    spy_only = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec)[0]["SPY|change|14:00+60m"]
    _, (wide, _) = rows_for(data, {**body, "features": body["features"] + ["F2", "F6", "S6"]})
    wide = wide["SPY|change|14:00+60m"]
    assert len(spy_only) == len(wide)
    for a, b in zip(spy_only, wide):  # adding the class changes no SPY feature, label or cost
        assert {k: v for k, v in a.items() if k != "features"} == {k: v for k, v in b.items() if k != "features"}
        assert a["features"] == {m: b["features"][m] for m in a["features"]}
    # the SPY-only feature set is still exactly what the live parity check compares
    assert mf.FEATURES == ("F1", "F10", "F11", "F1_shift30")


# --- guards ----------------------------------------------------------------------------------------


def test_a_cross_symbol_with_holdout_days_is_refused(tmp_path):
    data, days = staged(tmp_path, n=30, early=("HYG", "2022-11-10"))
    spec = mf.RunFile.from_dict(run_file(tmp_path, days, features=["F4"])[1])
    with pytest.raises(mf.RunFileError, match="HYG sessions before dev"):
        mf.load_bars(data / mf.DATA_FILE, spec)
    with pytest.raises(mf.RunFileError, match="holdout"):
        mf.check_data(tmp_path / "edge9" / "holdout")
    with pytest.raises(mf.RunFileError, match="need bars"):
        mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec)  # cross bars not supplied


def test_rows_share_one_signal_table_per_decision(tmp_path):
    data, days = staged(tmp_path, n=40)
    _, (families, cov) = rows_for(data, run_file(tmp_path, days, z_history=20)[1])
    a, b = families["SPY|change|14:00+30m"], families["SPY|change|14:00+60m"]
    shared = {r["decision_at"]: id(r["features"]) for r in a}
    assert any(shared.get(r["decision_at"]) == id(r["features"]) for r in b)
    assert cov["stale_minutes"] == 10 and cov["symbols"]["VIXY"]["median_bars"] < 390
    copy.deepcopy(families)  # rows stay plain data
