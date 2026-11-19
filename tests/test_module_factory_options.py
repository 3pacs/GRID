"""Module factory, factory 4 (3pacs/GRID#1158): the options-positioning class.

Seam S1: run file (class shares) + fixture SPY bars + a fixture gexhist store -> factory ledger reopened
with its anchor -> ``summarize()`` + report (ranking, bar, classes, buckets by basis). Plus: the level
rule is the live page's own vendored function (pinned bytes, parity with a gex checkout when present);
the GEX unit golden fixture (#1158, 2026-11-16); and point in time: later quotes, later-published open
interest and a level that appears only after the decision never move an earlier decision.
"""

import hashlib
import json
import math
import os
import shutil
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pytest

from analysis import factory_options as fo
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "docs/research/factory-runs/factory4-options-positioning-20261119.json"
LEVELS = fo.load_levels()
FIXTURE_MARKS = ("09:55", "10:25", "13:25", "13:55", "14:00", "14:25", "14:30", "14:55")


def sessions(n, first=date(2023, 1, 3)):
    d, out = first, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def hm(i):
    return "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))


def bs_price(S, K, T, iv, call):
    return float(fo._bs_price(S, np.array([float(K)]), T, np.array([iv]), np.array([call]), LEVELS.RATE,
                              LEVELS.DIVY)[0])


def make_store(data, days, plant=0.0, seed=7):
    """SPY bars (a quiet random walk) and a gexhist store: 0DTE OI at $1 strikes within $25 of the open, with a
    large call position $2 above the open on a random 30% of sessions (else $7 above), a large put position $5
    below, and quotes priced at IV 15% at the fixture marks. On the 30% sessions SPY drifts down by ``plant``
    dollars over 14:00..14:59 (a near-ceiling effect at the 14:00 decision)."""
    rng = np.random.default_rng(seed)
    (data / "edge9").mkdir(parents=True)
    (data / "edge2").mkdir(parents=True)
    bars = sqlite3.connect(data / mf.DATA_FILE)
    bars.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                 " primary key (sym, day, hm))")
    g = sqlite3.connect(data / fo.DATA_FILE)
    g.executescript("""
        create table sessions (day text primary key, open_px real, n_contracts integer);
        create table oi (day text, cp text, strike integer, oi integer, primary key (day, cp, strike));
        create table q5 (day text, cp text, strike integer, hm text, bid real, ask real, primary key (day, cp, strike, hm));
        create table fetch_log (day text primary key, status text, oi_rows integer, q_rows integer, cost_usd real,
                                fetched_at text);""")
    px, hot_days = 400.0, set()
    for day in days:
        hot = rng.random() < 0.3
        if hot:
            hot_days.add(day)
        rows, closes = [], {}
        for i in range(390):
            t = hm(i)
            step = 0.02 * rng.normal()
            if plant and hot and "14:00" <= t < "15:00":
                step -= plant / 60
            o = px
            px = o + step
            rows.append(("SPY", day, t, o, max(o, px) + 0.01, min(o, px) - 0.01, px, 1000))
            closes[t] = px
        bars.executemany("insert into bars values (?, ?, ?, ?, ?, ?, ?, ?)", rows)
        open_px = rows[0][3]
        base = int(open_px)
        oi = []
        for k in range(base - 25, base + 26):
            near = math.exp(-(((k - open_px) / 8.0) ** 2))
            oi.append((day, "C", k, int(800 * near) + 50 + (50000 if k == base + (2 if hot else 7) else 0)))
            oi.append((day, "P", k, int(800 * near) + 50 + (40000 if k == base - 5 else 0)))
        g.executemany("insert into oi values (?, ?, ?, ?)", oi)
        q = []
        for m in FIXTURE_MARKS:
            S = closes[m]
            T = max((fo.et(day, "16:00") - fo.et(day, m) - timedelta(minutes=1)).total_seconds() / (365 * 86400.0),
                    LEVELS.T_FLOOR)
            for _d, cp, k, _v in oi:
                p = bs_price(S, k, T, 0.15, cp == "C")
                q.append((day, cp, k, m, round(max(p - 0.01, 0.0), 4), round(p + 0.01, 4)))
        g.executemany("insert into q5 values (?, ?, ?, ?, ?, ?)", q)
        g.execute("insert into sessions values (?, ?, ?)", (day, open_px, len(oi)))
        g.execute("insert into fetch_log values (?, 'ok', ?, ?, 0, '2026-11-12T00:00:00+00:00')", (day, len(oi), len(q)))
    bars.commit()
    bars.close()
    g.commit()
    g.close()
    return hot_days


def staged(tmp_path, n=150, **kw):
    data = tmp_path / "factory-data" / "dev"
    days = sessions(n)
    hot = make_store(data, days, **kw)
    files = []
    for rel in (mf.DATA_FILE, fo.DATA_FILE):
        files.append({"path": rel, "sha256": hashlib.sha256((data / rel).read_bytes()).hexdigest(),
                      "days": [days[0], days[-1]]})
    (data / "MANIFEST.json").write_text(json.dumps({"dev_first": "2022-11-14", "dev_last": "2026-11-10",
                                                    "files": files}))
    return data, days, hot


def body_for(days, **over):
    return {
        "run_id": "factory4-test",
        "first_session": days[0],
        "last_session": days[-1],
        "decision_times": ["13:30", "14:00"],
        "horizons": ["30m", "60m"],
        "features": ["OIR1", "OIS1", "OIMID", "CWALL", "FLIP", "NGEX"],
        "z_thresholds": [1.0],
        "perms": 299,
        "seed": 4,
        "z_history": 20,
        "vol_history": 10,
        **over,
    }


def file_ledger(tmp_path):
    (tmp_path / "ledger").mkdir()
    (tmp_path / "anchor").mkdir()
    lp, ap = tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"
    lse.Ledger.create_factory(lp, ledger_id="factory4-test", anchor=ap)
    return lp, ap


def rows_for(data, body, archive=None):
    spec = mf.RunFile.from_dict(body)
    gex = fo.GexHist(data / fo.DATA_FILE, spec.decision_times) if any(
        f in fo.DTE0_FEATURES for f in spec.features) else None
    options = fo.OptionsInputs(gex, fo.OIArchive(archive) if archive else None)
    families, coverage = mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec, options=options)
    return families, coverage


def raws(data, body, archive=None):
    """{(decision_at, feature): fired side} for every row and module."""
    families, _ = rows_for(data, body, archive)
    return {(r["decision_at"], name): s["value"] for rows in families.values() for r in rows
            for name, s in r["features"].items()}


def feature_values(data, body, day, t, archive=None):
    """The raw options features at one decision (direct, through the same inputs the run uses)."""
    spec = mf.RunFile.from_dict(body)
    sessions_ = mf.load_sessions(data / mf.DATA_FILE, spec)
    gex = fo.GexHist(data / fo.DATA_FILE, spec.decision_times) if any(
        f in fo.DTE0_FEATURES for f in spec.features) else None
    opt = fo.OptionsInputs(gex, fo.OIArchive(archive) if archive else None)
    wanted = tuple(f for f in spec.features if mf.FEATURE_CLASS[f] == "options")
    return opt.features_at(day, sessions_[day], t, fo.et(day, t), wanted)


def values(data, body, archive=None):
    """{(decision_at, feature): raw value} for every session and decision time, through the run's own inputs."""
    spec = mf.RunFile.from_dict(body)
    sessions_ = mf.load_sessions(data / mf.DATA_FILE, spec)
    gex = fo.GexHist(data / fo.DATA_FILE, spec.decision_times) if any(
        f in fo.DTE0_FEATURES for f in spec.features) else None
    opt = fo.OptionsInputs(gex, fo.OIArchive(archive) if archive else None)
    wanted = tuple(f for f in spec.features if mf.FEATURE_CLASS[f] == "options")
    out = {}
    for day in sorted(sessions_):
        for t in spec.decision_times:
            for f, v in opt.features_at(day, sessions_[day], t, fo.et(day, t), wanted).items():
                out[(fo.et(day, t).isoformat(), f)] = v
    return out


def before(sig, until):
    return {k: v for k, v in sig.items() if k[0] <= until}


# --- seam S1 --------------------------------------------------------------------------------


def test_options_run_ranks_and_ledgers_at_seam_s1(tmp_path):
    data, days, _hot = staged(tmp_path, plant=1.2)
    body = body_for(days, class_shares={"options": 1.0})
    run_file = tmp_path / "run.json"
    run_file.write_text(json.dumps(body))
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(run_file, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")

    spec = mf.RunFile.from_dict(body)
    assert rep["trial_count"] == 2 * 2 * 6 * 4 == spec.trial_count()
    top = rep["ranking"][0]
    # near the OI ceiling, then down (the drift runs 14:00..15:00, so 13:30+60m holds half of it)
    assert top["family"] in ("SPY|change|13:30+60m", "SPY|change|14:00+60m") and top["selected"]
    assert top["class"] == "options" and top["feature"].endswith("|short")
    planted = [r for r in rep["ranking"] if r["feature"] == "OIR1|z<=1|short" and r["family"].endswith("+60m")]
    assert len(planted) == 2 and all(r["selected"] for r in planted)
    assert len({r["cluster"] for r in rep["ranking"][:8]}) == 1  # one idea (the planted OI ceiling), one cluster

    specs = rep["feature_specs"]
    assert specs["OIR1"]["basis"] == "oi-unsigned" and specs["OIR1"]["measurement"].startswith("observed OCC")
    for f in ("CWALL", "FLIP", "NGEX"):
        assert specs[f]["measurement"].startswith("measurement-unqualified")
    assert specs["FLIP"]["basis"] == specs["NGEX"]["basis"] == "gamma-signed-assumed"
    assert specs["CWALL"]["basis"] == "gamma-unsigned" and specs["CWALL"]["formula"] == fo.FORMULA["id"]

    opt = rep["coverage"]["options"]
    assert opt["levels_function"]["files"]["levels.py"]["sha256"] == fo.VENDORED["levels.py"]["sha256"]
    assert opt["level_rule"]["rule"] == "nearest-position/1" and opt["marks_used"] == {"13:30": "13:25",
                                                                                        "14:00": "13:55"}
    assert opt["counts"]["book"] == opt["counts"]["gamma"] == opt["counts"]["decisions"] > 0
    assert opt["oi_share_with_iv_within_1pct"]["median"] == 1.0

    b = rep["buckets"]
    assert set(b) == {"class", "decision_time", "horizon", "feature", "rule", "side", "basis"}
    assert set(b["basis"]) == {"oi-unsigned", "gamma-unsigned", "gamma-signed-assumed"}
    assert sum(x["tested"] for x in b["basis"].values()) == rep["tested"]
    assert rep["classes"]["options"]["share_declared"] == 1.0
    md = (tmp_path / "out" / "report.md").read_text()
    assert "Options class: levels.horizon_book" in md and "measurement-unqualified" in md

    summary = lse.summarize(lse.Ledger(lp, anchor=ap))  # reopened with its anchor
    assert (summary["runs_declared"], summary["runs_recorded"], summary["run_open"]) == (1, 1, None)
    assert summary["runs"][0]["classes"]["options"]["trials"] == rep["trial_count"]
    assert summary["runs"][0]["classes"]["options"]["selected"] >= 1


def test_example_run_file_is_the_options_class():
    spec = mf.RunFile.from_dict(json.loads(EXAMPLE.read_text()))
    assert len(spec.families()) == 43 and "SPY|change|15:00+60m" not in spec.families()
    assert set(spec.features) == set(mf.OPTIONS_SPECS) and spec.symbols() == ("SPY",)
    assert spec.trial_count() == 43 * 10 * 2 * 2 * 2 == spec.class_counts()["options"]
    assert spec.class_shares == {"options": 1.0} and len(spec.oi_archive_sha256) == 64
    assert (spec.cluster_by, spec.cluster_threshold, spec.first_session) == ("jaccard", 0.5, mf.DEV_FIRST)


def test_run_file_refuses_forward_only_and_unpinned_archive():
    days = sessions(5)
    ok = body_for(days)
    mf.RunFile.from_dict(ok)
    sha = "a" * 64
    for over, match in (({"features": ["OIR1", "FLOWC1"]}, "forward-only"),
                        ({"features": ["POSREC"]}, "forward-only"),
                        ({"features": ["OIR1A"]}, "oi_archive_sha256"),
                        ({"oi_archive_sha256": sha}, "oi_archive_sha256"),
                        ({"features": ["OIR1A"], "oi_archive_sha256": "XYZ"}, "oi_archive_sha256")):
        with pytest.raises(mf.RunFileError, match=match):
            mf.RunFile.from_dict({**ok, **over})
    mf.RunFile.from_dict({**ok, "features": ["OIR1A", "OIS1A"], "oi_archive_sha256": sha})


def test_run_needs_the_pinned_archive_copy(tmp_path):
    data, days, _ = staged(tmp_path, n=40)
    arch = tmp_path / "oi.db"
    make_archive(arch, days, late={})
    body = body_for(days, features=["OIR1A"], oi_archive_sha256="0" * 64)
    run_file = tmp_path / "run.json"
    run_file.write_text(json.dumps(body))
    lp, ap = file_ledger(tmp_path)
    with pytest.raises(mf.RunFileError, match="passed with, and only with"):
        mf.run(run_file, data, lse.Ledger(lp, anchor=ap), tmp_path / "out0")
    with pytest.raises(mf.RunFileError, match="does not match"):  # declared, then abandoned before reading it
        mf.run(run_file, data, lse.Ledger(lp, anchor=ap), tmp_path / "out1", oi_archive=arch)
    summary = lse.summarize(lse.Ledger(lp, anchor=ap))
    assert (summary["runs_declared"], summary["runs_abandoned"], summary["run_open"]) == (1, 1, None)


# --- the shared level function ----------------------------------------------------------------


def test_vendored_levels_are_the_pinned_gex_bytes(tmp_path):
    for name, pin in fo.VENDORED.items():
        raw = (fo.VENDOR_DIR / name).read_bytes()
        assert hashlib.sha256(raw).hexdigest() == pin["sha256"] and fo.git_blob(raw) == pin["blob"]
    assert LEVELS.POSITION_RULE == "nearest-position/1" and LEVELS.horizon_book.__module__.endswith("levels")
    assert "volume_quality" not in __import__("sys").modules  # the shim is scoped to the vendored import
    bad = tmp_path / "vendor"
    shutil.copytree(fo.VENDOR_DIR, bad)
    (bad / "levels.py").write_bytes((bad / "levels.py").read_bytes().replace(b"POSITION_OI_FRAC = 0.5",
                                                                             b"POSITION_OI_FRAC = 0.4"))
    with pytest.raises(fo.OptionsDataError, match="does not match"):
        fo.load_levels(bad)


@pytest.mark.skipif(not os.environ.get("GEX_REPO"), reason="set GEX_REPO to a gex checkout to check parity")
def test_vendored_levels_match_a_gex_checkout():
    assert fo.gex_file_parity(Path(os.environ["GEX_REPO"])) == {name: True for name in fo.VENDORED}


def oday(day="2023-03-01", spot=400.0, iv=0.15, t="14:00"):
    oi = {}
    for k in range(375, 426):
        near = math.exp(-(((k - spot) / 8.0) ** 2))
        oi[("C", k)] = int(800 * near) + 50 + (50000 if k == 404 else 0)
        oi[("P", k)] = int(800 * near) + 50 + (40000 if k == 393 else 0)
    m = fo.mark_for(t)
    T = (fo.et(day, "16:00") - fo.et(day, m) - timedelta(minutes=1)).total_seconds() / (365 * 86400.0)
    marks = {m: {key: (0.99 * bs_price(spot, key[1], T, iv, key[0] == "C"),
                       1.01 * bs_price(spot, key[1], T, iv, key[0] == "C")) for key in oi}}
    return fo.OptionDay(day, oi, fo.oi_known_at(day), marks)


def test_factory_levels_are_the_page_rule_and_archive_rows_match_contract_rows():
    d, spot, t = "2023-03-01", 400.3, "14:00"
    od = oday(d)
    rows, n_iv = fo.dte0_rows(od, LEVELS, fo.mark_for(t), 400.0)
    assert len(rows) == 102 and n_iv >= 40
    assert all(r["iv"] is not None and abs(r["iv"] - 0.15) < 1e-6 for r in rows if abs(r["strike"] - 400) <= 3)
    book = LEVELS.horizon_book(rows, spot, fo.et(d, t), "dte0", d)
    pl = book["position_levels"]
    # the page's own rule, applied by its own function to its own per-strike table
    above = [v for v in book["per_strike"] if v["strike"] > spot]
    below = [v for v in book["per_strike"] if v["strike"] < spot]
    assert pl == LEVELS.position_levels(above, below, spot)
    assert (pl["ceilings"][0]["strike"], pl["floors"][0]["strike"]) == (404.0, 393.0)
    f = fo._levels_features(book, spot)
    assert f["OIR1"] == pytest.approx((404 - spot) / spot) and f["OIS1"] == pytest.approx((spot - 393) / spot)
    w = fo.wall_features(book, spot)
    assert w["CWALL"] == pytest.approx((book["call_wall"] - spot) / spot) and -1 <= w["NGEX"] <= 1
    # every expiry summed per strike (archive rows) gives the same levels as the contract rows
    contracts = [{**r, "expiry": e, "oi": r["oi"] * share, "iv": None}
                 for r in rows for e, share in (("2023-03-01", 0.25), ("2023-03-03", 0.75))]
    arch = {"oi": {}}
    for r in contracts:
        arch["oi"][(r["strike"], r["side"])] = arch["oi"].get((r["strike"], r["side"]), 0.0) + r["oi"]
    a = LEVELS.horizon_book(fo.archive_rows(arch, d), spot, fo.et(d, t), "all", None)["position_levels"]
    c = LEVELS.horizon_book(contracts, spot, fo.et(d, t), "all", None)["position_levels"]
    assert a == c


def test_gex_unit_golden_fixture_is_dollars_per_one_percent():
    """#1158 (2026-11-16): gamma 0.02, OI 1,000, x100, spot $750 -> $11.25M per 1% move (GRID's daily-context unit,
    $ per $1 move, is $1.5M). The factory carries the GEX unit and never converts; the spot 100 case hides it."""
    for spot in (50.0, 100.0, 500.0, 750.0, 1000.0):
        rows = [{"expiry": "2026-11-20", "strike": spot, "side": "call", "oi": 1000.0, "gamma": 0.02, "iv": 0.2,
                 "volume": None, "volume_src": None, "src": "t"}]
        book = LEVELS.horizon_book(rows, spot, fo.et("2026-11-19", "12:00"), "all", None)
        per_1pct = book["per_strike"][0]["call_gex_m"] * 1e6
        grid_per_dollar = 0.02 * 1000 * 100 * spot
        assert per_1pct == pytest.approx(0.02 * 1000 * 100 * spot * spot * 0.01, rel=1e-3, abs=5e3)
        assert (per_1pct == pytest.approx(grid_per_dollar, rel=1e-3)) == (spot == 100.0)
    assert book["per_strike"][0]["call_gex_m"] == pytest.approx(0.02 * 1000 * 100 * 1000 ** 2 * 0.01 / 1e6)
    at750 = LEVELS.horizon_book([{**rows[0], "strike": 750.0}], 750.0, fo.et("2026-11-19", "12:00"), "all", None)
    assert at750["per_strike"][0]["call_gex_m"] == 11.25 and "1%" in fo.FORMULA["unit"]
    zero = LEVELS.horizon_book([{**rows[0], "oi": 0.0}], 750.0, fo.et("2026-11-19", "12:00"), "all", None)
    assert zero is None  # zero OI contributes nothing


def test_implied_vols_recover_and_refuse():
    S, T = 400.0, 2.0 / (365 * 24)
    K = np.array([390.0, 400.0, 410.0, 402.0])
    calls = np.array([True, False, True, False])
    iv = np.array([0.30, 0.15, 0.12, 0.25])
    mid = fo._bs_price(S, K, T, iv, calls, LEVELS.RATE, LEVELS.DIVY)
    assert np.allclose(fo.implied_vols(S, K, T, mid, calls, LEVELS.RATE, LEVELS.DIVY), iv, atol=1e-6)
    out = fo.implied_vols(S, K, T, np.array([0.0, 500.0, np.nan, mid[3]]), calls, LEVELS.RATE, LEVELS.DIVY)
    assert np.isnan(out[:3]).all() and abs(out[3] - 0.25) < 1e-6


def test_missing_quotes_withhold_gamma_but_keep_oi_levels():
    od = oday()
    od.marks = {}
    rows, n_iv = fo.dte0_rows(od, LEVELS, fo.mark_for("14:00"), 400.0)
    assert n_iv == 0 and all(r["iv"] is None for r in rows)
    assert not fo.gamma_ok(rows, 400.3, LEVELS.POSITION_WINDOW_PCT)
    book = LEVELS.horizon_book(rows, 400.3, fo.et(od.day, "14:00"), "dte0", od.day)
    assert fo._levels_features(book, 400.3)["OIR1"] is not None


def test_holdout_sessions_are_refused(tmp_path):
    data, days, _ = staged(tmp_path, n=3)
    with sqlite3.connect(data / fo.DATA_FILE) as con:
        con.execute("insert into sessions values ('2022-11-11', 400, 0)")
    with pytest.raises(fo.OptionsDataError, match="holdout"):
        fo.GexHist(data / fo.DATA_FILE, ("14:00",))


# --- point in time ----------------------------------------------------------------------------


def test_marks_and_oi_clock():
    assert [fo.mark_for(t) for t in ("09:36", "09:37", "10:00", "10:02", "14:00", "15:00")] == \
        [None, "09:35", "09:55", "10:00", "13:55", "14:55"]
    assert fo.oi_known_at("2026-03-02").isoformat() == "2026-03-02T07:30:00-05:00"
    assert fo.oi_known_at("2026-07-01", "2026-07-01T10:30:00Z").isoformat() == "2026-07-01T07:30:00-04:00"
    assert fo.oi_known_at("2026-07-01", "2026-07-01T14:15:00Z") == datetime.fromisoformat("2026-07-01T14:15:00+00:00")


def test_later_quotes_never_move_an_earlier_decision(tmp_path, monkeypatch):
    data, days, _ = staged(tmp_path, n=60, plant=1.0)
    body = body_for(days, features=["OIR1", "CWALL", "PWALL", "HWALL", "FLIP", "NGEX"],
                    decision_times=["10:30", "14:00"], horizons=["30m"])
    cut = days[45]
    until = f"{cut}T14:00:00-05:00"

    def mutate_and_compare():
        base = {**raws(data, body), **values(data, body)}
        with sqlite3.connect(data / fo.DATA_FILE) as con:  # every mark from 14:00 on the cut day, and every later day
            con.execute("create table keep as select * from q5")
            con.execute("update q5 set bid = bid * 3 + 1, ask = ask * 3 + 1.2 where (day = ? and hm >= '14:00') "
                        "or day > ?", (cut, cut))
        after = {**raws(data, body), **values(data, body)}
        with sqlite3.connect(data / fo.DATA_FILE) as con:
            con.execute("delete from q5")
            con.execute("insert into q5 select * from keep")
            con.execute("drop table keep")
        return base, after

    base, after = mutate_and_compare()
    assert before(base, until) == before(after, until)
    assert any(v is not None for (k, n), v in before(base, until).items() if n.startswith(("CWALL|", "NGEX|")))
    assert all(base[(until, f)] is not None for f in ("CWALL", "PWALL", "HWALL", "NGEX"))
    assert base != after  # the change is real; it reaches later decisions
    # the check bites: a reader allowed the mark at T itself is caught
    honest = fo.mark_for
    monkeypatch.setattr(fo, "mark_for", lambda t: honest(fo._hm(fo._minutes(t) + 2)))
    base, after = mutate_and_compare()
    assert before(base, until) != before(after, until)


def test_a_wall_that_appears_only_after_the_decision_is_not_seen(tmp_path):
    data, days, _ = staged(tmp_path, n=30)
    body = body_for(days, features=["CWALL", "OIR1"], decision_times=["13:30", "14:00", "14:30"])
    day = days[20]
    seen = {t: feature_values(data, body, day, t) for t in ("14:00", "14:30")}
    spot = mf.load_sessions(data / mf.DATA_FILE, mf.RunFile.from_dict(body))[day].close("14:29")
    k = round(spot * (1 + seen["14:30"]["CWALL"]))  # the call wall at 14:30, as built from the 14:25 mark
    with sqlite3.connect(data / fo.DATA_FILE) as con:  # from the 14:00 mark that strike is quoted near zero:
        con.execute("update q5 set bid = 0.0, ask = 0.01 where day = ? and cp = 'C' and strike = ? and hm >= '14:00'",
                    (day, k))  # its modeled gamma collapses and the call wall moves
    now = {t: feature_values(data, body, day, t) for t in ("14:00", "14:30")}
    assert now["14:00"] == seen["14:00"]  # 14:00 reads the 13:55 mark: the new wall is not seen
    assert now["14:30"]["CWALL"] != seen["14:30"]["CWALL"]
    assert now["14:30"]["OIR1"] == seen["14:30"]["OIR1"]  # quotes never move an OI level


def test_later_published_oi_never_moves_an_earlier_decision(tmp_path, monkeypatch):
    data, days, _ = staged(tmp_path, n=60, plant=1.0)
    body = body_for(days, features=["OIR1", "OIS1", "OIMID", "CWALL"], decision_times=["10:00", "10:30", "14:00"],
                    horizons=["30m"])
    late = days[45]
    honest = fo.GexHist.day

    def published_late(self, day):  # this session's OI reached the archive only at 10:15 ET
        od = honest(self, day)
        if od is not None and day == late:
            od.known_at = fo.et(day, "10:15")
        return od

    monkeypatch.setattr(fo.GexHist, "day", published_late)
    vals = {t: feature_values(data, body, late, t) for t in ("10:00", "10:30")}
    assert set(vals["10:00"].values()) == {None}  # not yet published: unavailable, never a stale or neutral value
    assert vals["10:30"]["OIR1"] is not None
    base = raws(data, body)
    with sqlite3.connect(data / fo.DATA_FILE) as con:  # the late day's OI and every later day's
        con.execute("update oi set oi = 0 where day >= ? and cp = 'P' and oi > 10000", (late,))  # the put position
    after = raws(data, body)
    until = f"{late}T10:00:00-05:00"
    assert before(base, until) == before(after, until)
    assert base != after


def make_archive(path, days, late):
    """An oi.db-shaped OCC archive: SPY OI for each day's own expiry and the next Friday's; ``late``: {day:
    published_at} for releases after the usual 06:30 ET."""
    con = sqlite3.connect(path)
    con.executescript("""
        create table oi (day text not null, root text not null, expiry text not null, cp text not null,
            strike_milli integer not null, oi integer not null, primary key (day, root, expiry, cp, strike_milli));
        create table fetch_log (day text not null, root text not null, status text not null, contracts integer,
            oi_total integer, disagree integer, publishers integer, published_at text, cost_usd real, detail text,
            fetched_at text not null, primary key (day, root));""")
    for day in days:
        d = date.fromisoformat(day)
        fri = (d + timedelta(days=(4 - d.weekday()) % 7 or 7)).isoformat()
        rows = []
        for k in range(370, 431):
            for e in (day, fri):
                rows.append((day, "SPY", e, "C", k * 1000, 100 + (20000 if (k == 405 and e == fri) else 0)))
                rows.append((day, "SPY", e, "P", k * 1000, 100 + (15000 if (k == 392 and e == fri) else 0)))
        con.executemany("insert into oi values (?, ?, ?, ?, ?, ?)", rows)
        pub = late.get(day) or fo.et(day, "06:30").astimezone(fo.NY).isoformat()
        con.execute("insert into fetch_log values (?, 'SPY', 'ok', ?, 0, 0, 18, ?, 0, null, '2026-11-17T00:00:00Z')",
                    (day, len(rows), pub))
    con.commit()
    con.close()


def test_an_archive_level_published_after_the_decision_is_not_seen(tmp_path):
    data, days, _ = staged(tmp_path, n=40)
    day = days[30]
    arch = tmp_path / "oi.db"
    make_archive(arch, days, late={day: fo.et(day, "10:15").isoformat()})
    with sqlite3.connect(arch) as con:  # the late release holds a new, closer call position
        con.execute("update oi set oi = 90000 where day = ? and cp = 'C' and strike_milli = ?", (day, 402000))
    body = body_for(days, features=["OIR1A", "OIS1A"], oi_archive_sha256="0" * 64,
                    decision_times=["10:00", "10:30"])
    spec = mf.RunFile.from_dict(body)
    sess = mf.load_sessions(data / mf.DATA_FILE, spec)[day]
    at10 = feature_values(data, body, day, "10:00", archive=arch)
    at1030 = feature_values(data, body, day, "10:30", archive=arch)
    assert at10 == {"OIR1A": None, "OIS1A": None}
    spot = sess.close("10:29")
    assert at1030["OIR1A"] == pytest.approx((402 - spot) / spot)
    base = raws(data, body, archive=arch)
    with sqlite3.connect(arch) as con:  # later days' releases drop the call position
        con.execute("update oi set oi = 100 where day > ? and cp = 'C'", (day,))
    after = raws(data, body, archive=arch)
    until = f"{day}T10:30:00-05:00"
    assert before(base, until) == before(after, until) and base != after


# --- unchanged ----------------------------------------------------------------------------------


def test_spy_only_feature_set_is_unchanged():
    assert mf.FEATURES == ("F1", "F10", "F11", "F1_shift30") and tuple(mf.FEATURE_SPECS) == mf.FEATURES
    assert set(mf.OPTIONS_SPECS).isdisjoint(mf.SPECS) and mf.ALL_FEATURES == tuple(mf.SPECS)
    assert all(mf.FEATURE_CLASS[f] == "options" for f in mf.OPTIONS_SPECS)
    assert set(mf.FORWARD_ONLY).isdisjoint(mf.KNOWN_FEATURES)


def test_options_registry_keeps_concentration_apart_from_assumed_gamma():
    for f, spec in mf.OPTIONS_SPECS.items():
        assert {"definition", "unit", "source", "live_source", "pit", "symbols", "basis", "measurement"} <= set(spec)
        assert mf.REGISTRY[f]["class"] == "options" and spec["kind"] == "level"
    basis = {f: s["basis"] for f, s in mf.OPTIONS_SPECS.items()}
    assert {f for f, b in basis.items() if b == "oi-unsigned"} == {"OIR1", "OIS1", "OIMID", "OIR1A", "OIS1A"}
    assert {f for f, b in basis.items() if b == "gamma-signed-assumed"} == {"FLIP", "NGEX"}
    for f in ("CWALL", "PWALL", "HWALL", "FLIP", "NGEX"):
        assert mf.OPTIONS_SPECS[f]["measurement"].startswith("measurement-unqualified")
    for f in ("OIR1", "OIS1", "OIMID", "OIR1A", "OIS1A"):
        assert "unsigned" in mf.OPTIONS_SPECS[f]["measurement"] and "gamma" not in mf.OPTIONS_SPECS[f]["basis"]
