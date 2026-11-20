"""Module factory, factory 5 (3pacs/GRID#1159): the macro class (FRED dailies, point in time).

Seam S1: run file (macro features, class shares, the snapshot's sha256) + fixture SPY bars + a fixture macro
snapshot built by ``factory_macro.build_snapshot`` -> factory ledger reopened with its anchor -> ``summarize()``
+ report (ranking, bar, classes, buckets, macro coverage). Plus point in time: a value published at 16:15 ET on
D is not seen at 15:00 on D and is seen at 10:00 on D+1; a revised value never appears before its vintage; a
change to anything not yet published moves no earlier decision.
"""

import bisect
import csv
import hashlib
import json
import math
import sqlite3
import statistics
from datetime import date, datetime, timedelta
from itertools import pairwise

import numpy as np
import pytest

from analysis import factory_macro as fm
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from analysis.research_real_panel import Publication

NY = mf.NY


def weekdays(first, last):
    d, out = date.fromisoformat(first), []
    while d <= date.fromisoformat(last):
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def calendar_days(first, last):
    d, out = date.fromisoformat(first), []
    while d <= date.fromisoformat(last):
        out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def hm(i):
    return "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))


def at(day, t):
    return datetime.fromisoformat(f"{day}T{t}").replace(tzinfo=NY)


def make_bars(path, days, drift_days=(), plant=0.0, seed=5):
    """SPY as a random walk; on ``drift_days`` it drifts up by ``plant`` dollars over 14:00..14:59."""
    rng = np.random.default_rng(seed)
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                " primary key (sym, day, hm))")
    px, rows, drift = 400.0, [], set(drift_days)
    for day in days:
        for i in range(390):
            t = hm(i)
            step = 0.05 * rng.normal() + (plant / 60 if day in drift and "14:00" <= t < "15:00" else 0.0)
            o, px = px, px + step
            rows.append(("SPY", day, t, o, max(o, px) + 0.01, min(o, px) - 0.01, px, 1000))
    con.executemany("insert into bars values (?, ?, ?, ?, ?, ?, ?, ?)", rows)
    con.commit()
    con.close()


def macro_series(first, last, seed=9, hot=0.25):
    """{series: [(obs_date, value)]} random walks; VIXCLS spikes on a random ``hot`` share of its dates.
    Returns (series, hot VIX dates)."""
    rng = np.random.default_rng(seed)
    out, spikes = {}, set()
    for sid in fm.RAW_SERIES + tuple(fm.STAGED_SERIES):
        dates = calendar_days(first, last) if sid == "DFF" else weekdays(first, last)
        level = {"VIXCLS": 18.0, "VXVCLS": 20.0, "BAMLH0A0HYM2": 3.5, "DEXJPUS": 140.0, "RRPONTSYD": 400.0}.get(sid, 4.0)
        vals = []
        for d in dates:
            level += 0.05 * rng.normal()
            v = level
            if sid == "VIXCLS" and rng.random() < hot:
                v, _ = level + 8.0, spikes.add(d)
            vals.append((d, round(v, 4)))
        out[sid] = vals
    return out, spikes


def default_vintages(first, last, cut=None, early=(999.0, 5.0)):
    """Two vintages per daily observation: a first print the next day and a revision (x 1.3) four days later.
    With ``cut``, every vintage not known before it takes a value from ``early`` instead."""
    out = {}
    for j, sid in enumerate(fm.VINTAGE_SERIES):
        for d in calendar_days(first, last):
            k = date.fromisoformat(d).toordinal()
            v1 = round(100 + 40 * math.sin(0.37 * k + j) + 15 * math.sin(1.9 * k), 3)
            rs1, rs2 = (date.fromisoformat(d) + timedelta(days=1)).isoformat(), \
                (date.fromisoformat(d) + timedelta(days=5)).isoformat()
            vs = [(rs1, v1), (rs2, round(v1 * 1.3, 3))]
            if cut is not None:
                vs = [(rs, v if fm.vintage_known(rs) < cut else early[i]) for i, (rs, v) in enumerate(vs)]
            out.setdefault(sid, {})[d] = vs
    return out


def write_snapshot(root, series, vintages=None, staged_before=("2022-11-01", "2022-11-02")):
    """CSV exports as psql would write them, a staged VXVCLS file (with lines before dev), then the snapshot."""
    root.mkdir(parents=True, exist_ok=True)
    with open(root / "obs.csv", "w", newline="") as f:
        w = csv.writer(f)
        for sid in fm.RAW_SERIES:
            for d, v in series[sid]:
                w.writerow([sid, d, "" if v is None else v, f"{(date.fromisoformat(d) + timedelta(days=1))}T20:20:00.000000"])
    if vintages is None:
        vintages = default_vintages(series["DFF"][0][0], series["DFF"][-1][0])
    last = series["DFF"][-1][0]
    with open(root / "vintages.csv", "w", newline="") as f:
        w = csv.writer(f)
        for sid, by in vintages.items():
            for d, vs in sorted(by.items()):
                for i, (rs, v) in enumerate(vs):
                    if rs > last:
                        continue
                    end = vs[i + 1][0] if i + 1 < len(vs) else "9999-12-31"
                    w.writerow([sid, d, rs, end, "" if v is None else v])
    staged = root / "staged"
    (staged / "fred").mkdir(parents=True, exist_ok=True)
    with open(staged / "fred" / "VXVCLS.csv", "w") as f:
        f.write("observation_date,VXVCLS\n")
        for d in staged_before:
            f.write(f"{d},not-a-number\n")  # before dev: skipped unparsed (it would not parse)
        for d, v in series["VXVCLS"]:
            f.write(f"{d},{v}\n")
    out = root / "macro.db"
    meta = fm.build_snapshot(root / "obs.csv", root / "vintages.csv", staged, out, last=last)
    return out, meta


def staged_bars(tmp_path, days, **kw):
    data = tmp_path / "factory-data" / "dev"
    (data / "edge9").mkdir(parents=True)
    make_bars(data / mf.DATA_FILE, days, **kw)
    sha = hashlib.sha256((data / mf.DATA_FILE).read_bytes()).hexdigest()
    (data / "MANIFEST.json").write_text(json.dumps({
        "dev_first": "2022-11-14", "dev_last": "2026-11-10",
        "files": [{"path": mf.DATA_FILE, "sha256": sha, "days": [days[0], days[-1]]}],
    }))
    return data


def file_ledger(tmp_path):
    (tmp_path / "ledger").mkdir()
    (tmp_path / "anchor").mkdir()
    lp, ap = tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"
    lse.Ledger.create_factory(lp, ledger_id="factory5-test", anchor=ap)
    return lp, ap


def body_for(days, snapshot, **over):
    return {
        "run_id": "factory5-test",
        "first_session": days[0],
        "last_session": days[-1],
        "decision_times": ["13:30", "14:00"],
        "horizons": ["30m", "60m"],
        "features": ["VIX", "HYOAS_D1", "S4", "EPU5", "SOFRFF"],
        "z_thresholds": [1.0],
        "class_shares": {"macro": 1.0},
        "perms": 299,
        "seed": 5,
        "z_history": 20,
        "macro_sha256": mf.sha256_file(snapshot) if snapshot.exists() else "0" * 64,
        **over,
    }


def write_run_file(tmp_path, body):
    path = tmp_path / f"{body['run_id']}.json"
    path.write_text(json.dumps(body))
    return path


def latest_known(sid, rows, decided):
    known = fm.known_times(sid, [d for d, _ in rows])
    i = bisect.bisect_left(known, decided) - 1
    return rows[i][0] if i >= 0 else None


def xnys_days(first, last):
    cal = mf.FactoryCalendar()
    return [s.key.split(":")[1] for s in cal.sessions if first <= s.key.split(":")[1] <= last
            and (s.closes - s.opens).total_seconds() == 390 * 60]


# --- seam S1 --------------------------------------------------------------------------------


def test_macro_run_ranks_and_ledgers_at_seam_s1(tmp_path):
    days = xnys_days("2023-01-03", "2023-12-29")
    series, spikes = macro_series("2022-11-14", days[-1])
    drift = [d for d in days if latest_known("VIXCLS", series["VIXCLS"], at(d, "14:00")) in spikes]
    data = staged_bars(tmp_path, days, drift_days=drift, plant=2.0)
    snap, meta = write_snapshot(tmp_path / "macro", series)
    body = body_for(days, snap)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(write_run_file(tmp_path, body), data, lse.Ledger(lp, anchor=ap), tmp_path / "out",
                 macro_snapshot=snap)

    spec = mf.RunFile.from_dict(body)
    assert rep["trial_count"] == 4 * 5 * 4 == spec.trial_count()
    top = rep["ranking"][0]
    assert top["family"] == "SPY|change|14:00+60m" and top["feature"] == "VIX|z>=1|long" and top["selected"]
    assert top["class"] == "macro"

    # classes, specs and the macro coverage
    assert rep["classes"]["macro"]["modules"] == 80 and rep["classes"]["macro"]["share_declared"] == 1.0
    assert rep["feature_specs"]["EPU5"]["basis"] == "alfred-vintage"
    assert rep["feature_specs"]["S4"]["series"] == ["BAMLH0A0HYM2"]
    mac = rep["coverage"]["macro"]
    assert mac["snapshot"]["content_sha256"] == meta["content_sha256"]
    assert mac["snapshot"]["staged_files"]["VXVCLS"]["lines_before_dev_skipped_unparsed"] == 2
    assert set(mac["features"]) == set(body["features"])
    for f, c in mac["features"].items():
        assert c["decisions"] == sum(c[k] for k in fm.COUNTS if k != "decisions"), f
        assert c["value"] > 0, f
    assert mac["series"]["USEPUINDXD"]["revised_observations"] > 0

    # every bucket, the macro basis and group included
    b = rep["buckets"]
    assert set(b) == {"class", "decision_time", "horizon", "feature", "rule", "side", "basis", "group"}
    assert set(b["basis"]) == {"unrevised-latest", "alfred-vintage"}
    assert set(b["group"]) == {"vol", "credit", "uncertainty", "funding"}
    assert sum(x["tested"] for x in b["group"].values()) == rep["tested"]
    md = (tmp_path / "out" / "report.md").read_text()
    assert "Macro class: snapshot factory-macro/1" in md and "| macro feature | decisions |" in md
    assert "| basis |" in md and "| group |" in md

    summary = lse.summarize(lse.Ledger(lp, anchor=ap))  # reopened with its anchor
    assert (summary["runs_declared"], summary["runs_recorded"], summary["run_open"]) == (1, 1, None)
    assert summary["candidates_declared"] == summary["candidates_measured"] == rep["trial_count"]
    assert summary["runs"][0]["classes"]["macro"]["selected"] >= 1
    declared = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_DECLARED)[0]
    assert declared["run_file"]["macro_sha256"] == mf.sha256_file(snap)


def test_a_snapshot_that_does_not_match_the_run_file_abandons_the_declared_run(tmp_path):
    days = xnys_days("2023-01-03", "2023-03-31")
    series, _ = macro_series("2022-11-14", days[-1])
    data = staged_bars(tmp_path, days)
    snap, _ = write_snapshot(tmp_path / "macro", series)
    other, _ = write_snapshot(tmp_path / "macro2", macro_series("2022-11-14", days[-1], seed=10)[0])
    body = body_for(days, snap)
    lp, ap = file_ledger(tmp_path)
    with pytest.raises(mf.RunFileError, match="macro_sha256"):
        mf.run(write_run_file(tmp_path, body), data, lse.Ledger(lp, anchor=ap), tmp_path / "out",
               macro_snapshot=other)
    summary = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert (summary["runs_declared"], summary["runs_abandoned"], summary["run_open"]) == (1, 1, None)
    with pytest.raises(mf.RunFileError, match="passed with, and only with"):  # refused before declaring
        mf.run(write_run_file(tmp_path, body), data, lse.Ledger(lp, anchor=ap), tmp_path / "out2")
    assert lse.summarize(lse.Ledger(lp, anchor=ap))["runs_declared"] == 1


def test_run_file_guards_for_the_macro_class(tmp_path):
    days = xnys_days("2023-01-03", "2023-02-28")
    snap = tmp_path / "x.db"
    snap.write_bytes(b"x")
    ok = body_for(days, snap)
    assert mf.RunFile.from_dict(ok).class_counts() == {"macro": 80}
    for over in ({"macro_sha256": None}, {"macro_sha256": "ab"}, {"macro_stale_days": 0},
                 {"macro_stale_days": 31}, {"class_shares": {"macro": 0.5, "price": 0.5}},
                 {"features": ["F1"], "class_shares": None}):  # a sha without macro features
        with pytest.raises(mf.RunFileError):
            mf.RunFile.from_dict({**ok, **over})


def test_example_run_file_is_the_macro_class():
    spec = mf.RunFile.from_dict(json.loads((mf.Path(__file__).resolve().parents[1] /
                                            "docs/research/factory-runs/factory5-macro-20261119.json").read_text()))
    assert set(spec.features) == set(mf.MACRO_SPECS) and spec.class_counts() == {"macro": spec.trial_count()}
    assert len(spec.families()) == 43 and "SPY|change|15:00+60m" not in spec.families()
    assert spec.trial_count() == 43 * 19 * 8
    assert (spec.cluster_by, spec.cluster_threshold, spec.cost_per_share) == ("jaccard", 0.5, 0.04)
    assert spec.macro_sha256 and spec.oi_archive_sha256 is None


# --- point in time --------------------------------------------------------------------------


def _inputs(tmp_path, series, vintages=None, closes=None, stale=7):
    snap, _ = write_snapshot(tmp_path, series, vintages)
    return fm.MacroInputs(fm.MacroSnapshot(snap), closes or {}, stale_days=stale)


def _flat(first, last, **values):
    base, _ = macro_series(first, last, hot=0.0)
    for sid, rows in base.items():
        base[sid] = [(d, values.get(sid, 1.0)) for d, _ in rows]
    return base


@pytest.mark.parametrize("real_rule", [False, True])
def test_a_value_published_at_1615_on_d_is_unseen_at_1500_and_seen_at_1000_next_day(tmp_path, monkeypatch, real_rule):
    # D = Tue 2023-01-10 (EST). DGS2 dated Mon 01-09 posts on D: at 16:15 ET under the test rule (21:15Z in
    # winter), at 16:17 ET under GRID's H.15 rule (21:17Z).
    if not real_rule:
        monkeypatch.setitem(fm.RULES, "FRB_H15", Publication(1, "business", "21:15", "test: 16:15 ET in winter"))
    series = _flat("2022-11-14", "2023-01-20", DGS2=4.0)
    series["DGS2"] = [(d, 4.5 if d == "2023-01-09" else 4.0) for d, _ in series["DGS2"]]
    known = dict(zip([d for d, _ in series["DGS2"]], fm.known_times("DGS2", [d for d, _ in series["DGS2"]])))
    assert known["2023-01-09"].astimezone(NY).strftime("%Y-%m-%d %H:%M") == \
        ("2023-01-10 16:17" if real_rule else "2023-01-10 16:15")
    m = _inputs(tmp_path, series)
    d, d1 = "2023-01-10", "2023-01-11"
    assert m.features_at(d, at(d, "15:00"), ("Y2_D1",))["Y2_D1"] == 0.0  # newest known: Fri 01-06
    assert m.features_at(d, at(d, "16:00"), ("Y2_D1",))["Y2_D1"] == 0.0  # still before publication
    assert m.features_at(d1, at(d1, "10:00"), ("Y2_D1",))["Y2_D1"] == pytest.approx(0.5)  # 01-09 minus 01-06


@pytest.mark.parametrize("rule", ["FRB_H15", "FRED_H15_SPREAD", "ICE_BOFA", "CBOE_VIX", "NYFED_RRP", "NYFED_SOFR"])
def test_every_next_day_rule_posts_after_the_last_decision(rule):
    # an observation dated D-1 is never usable at 15:00 ET on D, and always at 10:00 ET on D+1
    for d_prev, d, d_next in (("2023-01-09", "2023-01-10", "2023-01-11"), ("2023-07-10", "2023-07-11", "2023-07-12"),
                              ("2023-11-03", "2023-11-06", "2023-11-07")):  # winter, summer, over a weekend
        k = fm.publication_times([d_prev], fm.RULES[rule])[0].to_pydatetime()
        assert k >= at(d, "15:00") and k < at(d_next, "10:00"), (rule, d_prev, k)
    k = fm.publication_times(["2023-11-08"], fm.RULES["FRB_H10"])[0].to_pydatetime()  # H.10: 8 calendar days
    assert at("2023-11-16", "15:00") < k < at("2023-11-17", "10:00")


def test_a_revised_value_never_appears_before_its_vintage(tmp_path):
    series = _flat("2022-11-14", "2023-02-10")
    vint = {sid: {d: [((date.fromisoformat(d) + timedelta(days=1)).isoformat(), 100.0)]
                  for d in calendar_days("2022-11-14", "2023-02-10")} for sid in fm.VINTAGE_SERIES}
    vint["USEPUINDXD"]["2023-01-09"] = [("2023-01-10", 100.0), ("2023-01-12", 300.0)]  # revised on 01-12
    m = _inputs(tmp_path, series, vint)
    # 15:00 ET on 01-12: the newest known date is 01-10 (first print 01-11 counts from 01-12 00:00 CT), and the
    # 01-12 revision of 01-09 counts only from 01-13 00:00 CT: the first print is used
    assert m.features_at("2023-01-12", at("2023-01-12", "15:00"), ("EPU5",))["EPU5"] == pytest.approx(100.0)
    assert m.features_at("2023-01-13", at("2023-01-13", "00:59"), ("EPU5",))["EPU5"] == pytest.approx(100.0)
    assert m.features_at("2023-01-13", at("2023-01-13", "10:00"), ("EPU5",))["EPU5"] == pytest.approx(140.0)
    s = m.vseries["USEPUINDXD"]
    i = s.dates.index("2023-01-09")
    assert s.value_as_of(i, at("2023-01-12", "23:59")) == 100.0 and s.value_as_of(i, at("2023-01-13", "10:00")) == 300.0
    assert s.value_as_of(i, at("2023-01-10", "10:00")) is None  # before its first print
    with pytest.raises(fm.MacroDataError, match="complete only"):
        s.value_as_of(i, at("2023-03-01", "10:00"))  # past MAX_AGE_DAYS: refused, never guessed


def test_a_late_first_print_leaves_the_older_date_missing_until_it_is_known(tmp_path):
    series = _flat("2022-11-14", "2023-02-10")
    vint = {sid: {d: [((date.fromisoformat(d) + timedelta(days=1)).isoformat(), 100.0)]
                  for d in calendar_days("2022-11-14", "2023-02-10")} for sid in fm.VINTAGE_SERIES}
    vint["USEPUINDXD"]["2023-01-09"] = [("2023-01-16", 200.0)]  # first printed a week late
    m = _inputs(tmp_path, series, vint)
    assert m.features_at("2023-01-12", at("2023-01-12", "10:00"), ("EPU5",))["EPU5"] is None
    assert m.counts["EPU5"]["missing"] == 1
    # a day earlier the newest known date is 01-08 (01-09 and 01-10 are not printed yet): 01-04..01-08 are used
    assert m.features_at("2023-01-11", at("2023-01-11", "10:00"), ("EPU5",))["EPU5"] == pytest.approx(100.0)
    assert m.features_at("2023-01-17", at("2023-01-17", "10:00"), ("EPU5",))["EPU5"] == pytest.approx(100.0)


def test_unpublished_values_never_move_an_earlier_decision(tmp_path):
    days = xnys_days("2023-01-03", "2023-08-31")
    series, _ = macro_series("2022-11-14", days[-1], hot=0.2)
    body = body_for(days, tmp_path / "unused", decision_times=["10:00", "14:00"], horizons=["30m"],
                    features=sorted(set(mf.MACRO_SPECS) - {"S1", "S2"}), class_shares=None,
                    macro_sha256="0" * 64)
    spec = mf.RunFile.from_dict(body)
    data = staged_bars(tmp_path, days)
    sessions = mf.load_sessions(data / mf.DATA_FILE, spec)
    closes = {d: mf.settled_close(s) for d, s in sessions.items()}
    cut = at(days[120], "14:00")

    def signals(snap_series, root, vint=None):
        snap, _ = write_snapshot(root, snap_series, vint)
        macro = fm.MacroInputs(fm.MacroSnapshot(snap), closes)
        fams, _ = mf.build_rows(sessions, spec, calendar=mf.FactoryCalendar(), macro=macro)
        return {(r["decision_at"], n): s["value"] for rows in fams.values() for r in rows
                for n, s in r["features"].items() if datetime.fromisoformat(r["decision_at"]) <= cut}

    before = signals(series, tmp_path / "a")
    later = {}
    for sid, rows in series.items():  # every observation not known by the cut changes
        known = fm.known_times(sid, [d for d, _ in rows])
        later[sid] = [(d, v * 3 + 7 if k >= cut else v) for (d, v), k in zip(rows, known)]
    vint = default_vintages("2022-11-14", days[-1], cut=cut)  # and so does every vintage not known by the cut
    after = signals(later, tmp_path / "b", vint)
    assert before == after
    fired = {n.split("|")[0] for (_, n), v in before.items() if v is not None}
    assert fired == set(spec.features)  # every feature fired before the cut, so the check bites on each

    # the check bites: a reader that sees values one day before their publication is caught
    real = fm.known_times
    fm_known = lambda sid, dates: [k - timedelta(days=1) for k in real(sid, dates)]
    try:
        fm.known_times = fm_known
        early_before = signals(series, tmp_path / "c")
        early_after = signals(later, tmp_path / "d", vint)
    finally:
        fm.known_times = real
    assert early_before != early_after


# --- definitions and guards -----------------------------------------------------------------


def test_spy_dials_match_edge9_definitions(tmp_path):
    series = _flat("2022-11-14", "2024-06-28")
    days = weekdays("2022-11-14", "2024-06-28")
    rng = np.random.default_rng(3)
    closes = dict(zip(days, 400 * np.exp(np.cumsum(0.01 * rng.normal(size=len(days))))))
    series["VIXCLS"] = [(d, 15.0 + i % 7) for i, (d, _) in enumerate(series["VIXCLS"])]
    m = _inputs(tmp_path, series, closes=closes)
    day, prior = "2024-06-26", "2024-06-25"
    got = m.features_at(day, at(day, "10:00"), ("S1", "S2", "S5", "VIX"), prior_day=prior)
    c = [closes[d] for d in days if d < day]
    assert got["S1"] == pytest.approx(c[-1] / (sum(c[-200:]) / 200) - 1)
    assert got["S2"] == pytest.approx(c[-1] / max(c[-252:]) - 1)
    o = "2024-06-24"  # VIX dated Mon posts Tue 23:59Z, so Wed 10:00 sees Mon
    vix = dict(series["VIXCLS"])[o]
    assert got["VIX"] == vix
    cc = [closes[d] for d in days if d <= o][-21:]
    rv = 100 * math.sqrt(252) * statistics.stdev([math.log(b / a) for a, b in pairwise(cc)])
    assert got["S5"] == pytest.approx(vix - rv)
    assert m.features_at(day, at(day, "10:00"), ("S1",), prior_day="2024-06-24")["S1"] is None  # prior missing
    early = m.features_at("2023-03-01", at("2023-03-01", "10:00"), ("S1", "S2"), prior_day="2023-02-28")
    assert early == {"S1": None, "S2": None}  # 200 / 252 sessions of dev history are not there yet


def test_spread_ratio_staleness_and_missing_values(tmp_path):
    series = _flat("2022-11-14", "2023-03-31")
    series["SOFR"] = [(d, 4.30) for d, _ in series["SOFR"]]
    series["DFF"] = [(d, 4.33) for d, _ in series["DFF"]]
    series["VXVCLS"] = [(d, 20.0) for d, _ in series["VXVCLS"] if d != "2023-02-06"]  # one VIX3M date missing
    series["VIXCLS"] = [(d, 18.0) for d, _ in series["VIXCLS"]]
    series["BAMLH0A0HYM2"] = [(d, v) for d, v in series["BAMLH0A0HYM2"] if not "2023-02-13" <= d <= "2023-02-28"]
    series["DGS2"] = [(d, None if d == "2023-02-08" else 4.0) for d, _ in series["DGS2"]]
    m = _inputs(tmp_path, series)
    assert m.features_at("2023-02-01", at("2023-02-01", "10:00"), ("SOFRFF",))["SOFRFF"] == pytest.approx(-0.03)
    # S3: VIXCLS dated Mon 02-06 is known Wed 10:00 but VXVCLS is missing that date: the newest common date is 02-03
    assert m.features_at("2023-02-08", at("2023-02-08", "10:00"), ("S3",))["S3"] == pytest.approx(0.9)
    # HY OAS has no observation for two weeks: stale after macro_stale_days, never carried
    assert m.features_at("2023-02-16", at("2023-02-16", "10:00"), ("HYOAS",))["HYOAS"] == 1.0  # 02-10, 3 days old
    assert m.features_at("2023-03-01", at("2023-03-01", "10:00"), ("HYOAS",))["HYOAS"] is None
    assert m.counts["HYOAS"]["stale"] >= 1
    # a null value is missing, never zero; its change and the next one too
    assert m.features_at("2023-02-10", at("2023-02-10", "10:00"), ("Y2_D1",))["Y2_D1"] is None
    assert m.features_at("2023-02-13", at("2023-02-13", "10:00"), ("Y2_D1",))["Y2_D1"] is None
    assert m.counts["Y2_D1"]["missing"] == 2
    # 20-observation changes need 20 earlier observations of dev history
    assert m.features_at("2022-12-01", at("2022-12-01", "10:00"), ("Y2_D20",))["Y2_D20"] is None
    assert m.counts["Y2_D20"]["history"] == 1


def test_snapshot_refuses_the_holdout_and_tampering(tmp_path):
    series = _flat("2022-11-14", "2023-01-31")
    snap, meta = write_snapshot(tmp_path / "ok", series)
    assert meta["staged_files"]["VXVCLS"]["lines_before_dev_skipped_unparsed"] == 2
    fm.MacroSnapshot(snap)
    early = {**series, "DGS2": [("2022-11-11", 4.0)] + series["DGS2"]}
    with pytest.raises(fm.MacroDataError, match="holdout"):
        write_snapshot(tmp_path / "early", early)
    with pytest.raises(fm.MacroDataError, match="holdout"):
        fm.export_sql(first="2022-11-01")
    with pytest.raises(fm.MacroDataError, match="holdout"):
        fm.build_snapshot(tmp_path / "ok" / "obs.csv", tmp_path / "ok" / "vintages.csv",
                          tmp_path / "holdout", tmp_path / "x.db")
    bad = tmp_path / "bad.db"
    bad.write_bytes(snap.read_bytes())
    with sqlite3.connect(bad) as con:
        con.execute("update obs set value = value + 1 where series_id = 'DGS2' and obs_date = '2023-01-05'")
    with pytest.raises(fm.MacroDataError, match="content_sha256"):
        fm.MacroSnapshot(bad)
    with sqlite3.connect(bad) as con:
        con.execute("insert into obs values ('DGS2', '2022-11-10', 4.0, null)")
    with pytest.raises(fm.MacroDataError, match="holdout"):
        fm.MacroSnapshot(bad)
    with pytest.raises(fm.MacroDataError, match="write-once"):
        fm.build_snapshot(tmp_path / "ok" / "obs.csv", tmp_path / "ok" / "vintages.csv", tmp_path / "ok" / "staged",
                          snap)


def test_snapshot_keeps_the_first_value_grid_saw_and_checks_the_rules(tmp_path):
    root = tmp_path / "s"
    series = _flat("2022-11-14", "2023-01-31")
    write_snapshot(root, series)
    with open(root / "obs.csv", newline="") as f:
        rows = list(csv.reader(f))
    dup = next(r for r in rows if r[0] == "DGS2")
    rows.insert(rows.index(dup) + 1, [dup[0], dup[1], "9.99", "2023-03-01T00:00:00.000000"])  # a later re-pull
    with open(root / "obs.csv", "w", newline="") as f:
        csv.writer(f).writerows(rows)
    meta = fm.build_snapshot(root / "obs.csv", root / "vintages.csv", root / "staged", root / "again.db",
                             last="2023-01-31")
    assert (meta["duplicate_rows_dropped"], meta["duplicate_rows_conflicting"]) == (1, 1)
    assert dict(fm.MacroSnapshot(root / "again.db").obs["DGS2"])[dup[1]] == float(dup[2])
    check = meta["series"]["DGS2"]["first_seen_check"]  # fixture: seen at 20:20Z the next day, rule 21:17Z
    assert check["live_dates"] > 0 and check["share_seen_before_rule"] > 0.5
    assert meta["series"]["USEPUINDXD"]["revision"] == "vintages" and meta["series"]["DGS2"]["revision"] == "unrevised"


def test_revised_series_without_vintages_are_not_in_the_class():
    used = set(fm.SERIES)
    for sid in used:
        if fm.SERIES[sid]["revision"] == "unrevised":
            assert not fm.revised(sid), sid  # never a series on GRID's revised list as if unrevised
        else:
            assert sid in fm.VINTAGE_SERIES and fm.SERIES[sid]["origin"] == fm.VINT
    assert "DTWEXBGS" in fm.EXCLUDED and fm.revised("DTWEXBGS")
    for f, (kind, inputs) in fm.FEATURES.items():
        assert set(inputs) <= used and f in mf.MACRO_SPECS, f
    report = fm.series_report()
    assert set(report["used"]) == used and not any(v["in_grid_revised_list"] for s, v in report["used"].items()
                                                   if fm.SERIES[s]["revision"] == "unrevised")


def test_adding_the_macro_class_changes_no_spy_row(tmp_path):
    days = xnys_days("2023-01-03", "2023-06-30")
    series, _ = macro_series("2022-11-14", days[-1])
    data = staged_bars(tmp_path, days)
    snap, _ = write_snapshot(tmp_path / "m", series)
    plain = mf.RunFile.from_dict({**body_for(days, snap), "features": ["F1", "F10"], "class_shares": None,
                                  "macro_sha256": None})
    wide = mf.RunFile.from_dict({**body_for(days, snap), "features": ["F1", "F10", "VIX", "S4"],
                                 "class_shares": None})
    sessions = mf.load_sessions(data / mf.DATA_FILE, plain)
    closes = {d: mf.settled_close(s) for d, s in sessions.items()}
    a, _ = mf.build_rows(sessions, plain, calendar=mf.FactoryCalendar())
    b, _ = mf.build_rows(sessions, wide, calendar=mf.FactoryCalendar(),
                         macro=fm.MacroInputs(fm.MacroSnapshot(snap), closes))
    for fam in a:
        assert len(a[fam]) == len(b[fam])
        for x, y in zip(a[fam], b[fam]):
            assert {k: v for k, v in x.items() if k != "features"} == {k: v for k, v in y.items() if k != "features"}
            assert x["features"] == {k: y["features"][k] for k in x["features"]}
    assert mf.FEATURES == ("F1", "F10", "F11", "F1_shift30") and set(mf.ALL_FEATURES) == set(mf.SPECS)
    with pytest.raises(mf.RunFileError, match="macro snapshot"):
        mf.build_rows(sessions, wide, calendar=mf.FactoryCalendar())


def test_macro_registry_is_tagged_honestly():
    for f in mf.MACRO_SPECS:
        reg = mf.REGISTRY[f]
        assert reg["class"] == "macro" == mf.FEATURE_CLASS[f] and reg["kind"] == "level"
        assert {"definition", "unit", "source", "live_source", "pit", "symbols", "basis", "group", "series"} <= set(reg)
        assert reg["basis"] in ("spy-bars", "unrevised-latest", "alfred-vintage"), f
        assert ("vintage" in reg["pit"]) == (reg["basis"] == "alfred-vintage"), f
    assert mf.REGISTRY["S3"]["live_source"].startswith("none yet")
    assert {f for f, s in mf.MACRO_SPECS.items() if s["basis"] == "alfred-vintage"} == {"EPU5", "EMU5"}
    assert "macro" in mf.CLASSES and not set(mf.MACRO_SPECS) & set(mf.SPECS)
