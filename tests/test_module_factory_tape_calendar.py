"""Module factory, factory 3 (3pacs/GRID#1157): the tape and calendar classes.

Seam S1: run file (with class shares) + fixture SPY minute bars -> factory ledger reopened with its
anchor -> ``summarize()`` + report (ranking, bar, classes, buckets). Plus point-in-time checks: later
bars never move an earlier decision, and an event announced later never moves an earlier decision.
"""

import copy
import hashlib
import json
import sqlite3
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pytest

from analysis import factory_session_calendar as fc
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "docs/research/factory-runs/factory3-tape-calendar-20261119.json"
TAPE = ("VR30", "VRDAY", "RR30", "EWR30", "VWAP", "W1L", "W1H")
CAL = ("MON", "TUE", "WED", "THU", "FRI", "OPEX", "FOMC", "FOMC_EVE", "CPI", "NFP")


def sessions(n, first=date(2023, 1, 3)):
    d, out = first, []
    while len(out) < n:
        if d.weekday() < 5:
            out.append(d.isoformat())
        d += timedelta(days=1)
    return out


def hm(i):
    return "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))


def make_bars(path, days, plant=0.0, seed=11):
    """SPY as a random walk with U-shaped lognormal minute volumes.

    ``plant``: on a random quarter of the sessions, volume over 13:30..13:59 is tripled and SPY drifts up by
    ``plant`` dollars over 14:00..14:59 (a relative-volume effect at the 14:00 decision).
    """
    rng = np.random.default_rng(seed)
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                " primary key (sym, day, hm))")
    px, rows = 400.0, []
    for day in days:
        hot = rng.random() < 0.25
        level = rng.lognormal(0, 0.2)
        for i in range(390):
            t = hm(i)
            step = 0.05 * rng.normal()
            if plant and hot and "14:00" <= t < "15:00":
                step += plant / 60
            o = px
            px = o + step
            u = 1.0 + 2.0 * ((i - 195) / 195) ** 2
            v = 1000 * level * u * rng.lognormal(0, 0.3)
            if hot and "13:30" <= t < "14:00":
                v *= 3
            wick = abs(0.03 * rng.normal())
            rows.append(("SPY", day, t, o, max(o, px) + wick, min(o, px) - wick, px, int(v)))
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


def body_for(days, **over):
    return {
        "run_id": "factory3-test",
        "first_session": days[0],
        "last_session": days[-1],
        "decision_times": ["13:30", "14:00"],
        "horizons": ["30m", "60m"],
        "features": ["VR30", "EWR30", "VWAP", "W1L", "FRI", "FOMC"],
        "z_thresholds": [1.0],
        "perms": 299,
        "seed": 3,
        "z_history": 20,
        "tape_history": 10,
        **over,
    }


def write_run_file(tmp_path, body):
    path = tmp_path / f"{body['run_id']}.json"
    path.write_text(json.dumps(body))
    return path


def file_ledger(tmp_path):
    (tmp_path / "ledger").mkdir()
    (tmp_path / "anchor").mkdir()
    lp, ap = tmp_path / "ledger" / "factory.jsonl", tmp_path / "anchor" / "factory.anchor.jsonl"
    lse.Ledger.create_factory(lp, ledger_id="factory3-test", anchor=ap)
    return lp, ap


def event_doc(extra=()):
    """A small complete factory-events/1 document (tests only; the run reads the pinned one)."""
    events = [{"event": "fomc", "day": "2023-03-22", "time_et": "14:00",
               "known_at": "2022-12-31T00:00:00-05:00", "basis": "fed_schedule_prior_year"},
              {"event": "cpi", "day": "2023-03-14", "time_et": "08:30",
               "known_at": "2023-03-14T00:00:00-04:00", "basis": "release_day_bound"}, *extra]
    return {"schema": fc.EVENT_SCHEMA, "coverage": {"start": "2023-01-01", "end": "2023-12-31",
                                                     "events": list(fc.EVENTS), "complete": True},
            "events": events}


def rows_for(data, body, events=None):
    spec = mf.RunFile.from_dict(body)
    return mf.build_rows(mf.load_sessions(data / mf.DATA_FILE, spec), spec, events=events)


def signals(families, until):
    return {(r["decision_at"], name): s["value"] for rows in families.values() for r in rows
            if r["decision_at"] <= until for name, s in r["features"].items()}


# --- seam S1 --------------------------------------------------------------------------------


def test_tape_calendar_run_ranks_classes_and_ledgers_at_seam_s1(tmp_path):
    data, days = staged(tmp_path, plant=2.0)
    body = body_for(days, class_shares={"tape": 0.7778, "calendar": 0.2222})
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(write_run_file(tmp_path, body), data, lse.Ledger(lp, anchor=ap), tmp_path / "out")

    spec = mf.RunFile.from_dict(body)
    assert rep["trial_count"] == 2 * 2 * (3 * 4 + 1 * 2 + 2 * 2) == spec.trial_count()
    top = rep["ranking"][0]
    assert top["family"].startswith("SPY|change|14:00+") and top["selected"] and top["class"] == "tape"
    assert top["feature"].split("|")[0] in ("VR30", "EWR30") and top["feature"].endswith("|z>=1|long")

    # classes: declared shares match the modules; every trial is classed
    assert rep["classes"]["tape"]["modules"] == 2 * 2 * 14 and rep["classes"]["calendar"]["modules"] == 2 * 2 * 4
    assert rep["classes"]["tape"]["share_declared"] == 0.7778 and rep["classes"]["tape"]["share"] == 0.7778
    assert rep["feature_specs"]["VR30"]["live_source"].startswith("research-only")
    assert rep["feature_specs"]["FOMC"]["kind"] == "flag" and rep["feature_specs"]["FRI"]["class"] == "calendar"
    assert rep["coverage"]["event_calendar"].startswith(fc.EVENT_SCHEMA) and fc.EVENT_PIN[1] in \
        rep["coverage"]["event_calendar"]

    # every bucket is reported, including the dead ones
    b = rep["buckets"]
    assert set(b) == {"class", "decision_time", "horizon", "feature", "rule", "side"}
    assert set(b["decision_time"]) == {"13:30", "14:00"} and set(b["class"]) == {"tape", "calendar"}
    assert set(b["feature"]) == set(body["features"]) and "on" in b["rule"]
    assert sum(x["tested"] for x in b["class"].values()) == rep["tested"]
    for row in b["class"].values():
        assert row["chance"] == pytest.approx(0.05 * row["tested"], abs=0.05)
    md = (tmp_path / "out" / "report.md").read_text()
    assert "| class | features | modules | share | declared share |" in md and "| decision_time |" in md

    # flag modules: one rule, both sides; the planted idea is one cluster with one representative
    flags = {t["feature"] for t in json.loads((tmp_path / "out" / "discovery-frozen.json").read_text())
             ["payload"]["ledger"] if t["feature"].startswith(("FRI|", "W1L|", "FOMC|"))}
    assert flags == {f"{f}|on|{s}" for f in ("FRI", "W1L", "FOMC") for s in ("long", "short")}
    cl = {c["cluster"]: c for c in rep["clusters"]}[top["cluster"]]
    assert cl["representative"]["trial_id"] == top["trial_id"] and cl["representative"]["class"] == "tape"

    summary = lse.summarize(lse.Ledger(lp, anchor=ap))  # reopened with its anchor
    assert (summary["runs_declared"], summary["runs_recorded"], summary["run_open"]) == (1, 1, None)
    assert summary["candidates_declared"] == summary["candidates_measured"] == rep["trial_count"]
    run = summary["runs"][0]
    assert run["classes"]["tape"]["trials"] == rep["classes"]["tape"]["modules"]
    assert run["classes"]["calendar"]["trials"] == rep["classes"]["calendar"]["modules"]
    assert run["classes"]["tape"]["selected"] >= 1
    trials = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_RESULT)[0]["trials"]
    assert {t["class"] for t in trials} == {"tape", "calendar"}
    declared = lse.Ledger(lp, anchor=ap).of_kind(lse.FACTORY_DECLARED)[0]
    assert declared["run_file"]["class_shares"] == {"tape": 0.7778, "calendar": 0.2222}


def test_example_run_file_is_the_tape_and_calendar_classes():
    spec = mf.RunFile.from_dict(json.loads(EXAMPLE.read_text()))
    assert spec.decision_times == tuple(f"{h:02d}:{m:02d}" for h in range(10, 16) for m in (0, 30))[:11]
    assert spec.horizons == ("15m", "30m", "60m", "close")
    assert "SPY|change|15:00+60m" not in spec.families() and "SPY|change|15:00+close" in spec.families()
    assert len(spec.families()) == 11 * 4 - 1
    assert set(spec.features) == set(mf.TAPE_SPECS) | set(mf.CALENDAR_SPECS) and spec.symbols() == ("SPY",)
    per_family = 5 * 2 * 2 * 2 + 2 * 2 + 10 * 2
    assert spec.trial_count() == 43 * per_family
    assert spec.class_counts() == {"tape": 43 * 44, "calendar": 43 * 20}
    assert (spec.cluster_by, spec.cluster_threshold, spec.first_session) == ("jaccard", 0.5, mf.DEV_FIRST)


def test_run_file_checks_class_shares_and_skip_windows():
    days = sessions(5)
    ok = body_for(days, class_shares={"tape": 0.7778, "calendar": 0.2222})
    assert mf.RunFile.from_dict(ok).class_counts() == {"tape": 2 * 2 * 14, "calendar": 2 * 2 * 4}
    for over in ({"class_shares": {"tape": 0.5, "calendar": 0.5}},          # not what the modules imply
                 {"class_shares": {"tape": 1.0}},                            # a class left out
                 {"class_shares": {"tape": 0.7778, "calendar": 0.2222, "price": 0.0}},
                 {"skip_windows": ["15:00+60m"]},                            # not a window of this run
                 {"skip_windows": ["13:30+30m", "13:30+30m"]},
                 {"skip_windows": ["13:30+30m", "13:30+60m", "14:00+30m", "14:00+60m"]},
                 {"tape_history": 5}, {"features": ["F7"]}, {"features": ["F8"]}, {"features": ["SPREAD"]}):
        with pytest.raises(mf.RunFileError):
            mf.RunFile.from_dict({**ok, **over})
    with pytest.raises(mf.RunFileError, match="actual"):
        mf.RunFile.from_dict({**ok, "class_shares": {"tape": 0.6, "calendar": 0.4}})
    skipped = mf.RunFile.from_dict({**ok, "skip_windows": ["14:00+60m"]})
    assert skipped.families() == ("SPY|change|13:30+30m", "SPY|change|13:30+60m", "SPY|change|14:00+30m")
    assert skipped.trial_count() == 3 * len(skipped.modules())
    # flags take one rule, "on", per side; level features keep the factory 1 names and order
    mods = mf.RunFile.from_dict({**ok, "features": ["F1", "FRI"], "class_shares": None}).modules()
    assert mods == ("F1|z>=1|long", "F1|z>=1|short", "F1|z<=1|long", "F1|z<=1|short", "FRI|on|long", "FRI|on|short")


# --- point in time ----------------------------------------------------------------------------


def test_later_bars_never_move_an_earlier_decision(tmp_path, monkeypatch):
    data, days = staged(tmp_path, n=70, plant=1.0)
    body = body_for(days, features=list(TAPE) + ["MON", "FRI", "OPEX"], decision_times=["10:30", "14:00"],
                    horizons=["30m"])
    cut = days[50]
    until = f"{cut}T14:00:00-05:00"

    def mutate_and_compare():
        before, _ = rows_for(data, body)
        with sqlite3.connect(data / mf.DATA_FILE) as con:  # from 14:00 on the cut day, and every later day
            con.execute("create table if not exists keep as select * from bars")
            con.execute("update bars set o = o * 1.05, h = h * 1.08, l = l * 0.95, c = c * 1.06, v = v * 7 "
                        "where (day = ? and hm >= '14:00') or day > ?", (cut, cut))
        after, _ = rows_for(data, body)
        with sqlite3.connect(data / mf.DATA_FILE) as con:
            con.execute("delete from bars")
            con.execute("insert into bars select * from keep")
            con.execute("drop table keep")
        return before, after

    before, after = mutate_and_compare()
    assert signals(before, until) == signals(after, until)
    fired = {name.split("|")[0] for (_, name), v in signals(before, until).items() if v is not None}
    assert fired >= {"VR30", "VRDAY", "RR30", "EWR30", "VWAP", "MON", "FRI"}
    assert signals(before, "9999") != signals(after, "9999")  # the change is real; it reaches later decisions

    # the check bites: a tape reader that peeks two minutes past the decision bar is caught
    honest = mf.tape_window
    monkeypatch.setattr(mf, "tape_window", lambda day, bar: honest(day, mf._hm(mf._minutes(bar) + 2)))
    before, after = mutate_and_compare()
    assert signals(before, until) != signals(after, until)


def test_tape_quantities_read_no_bar_after_the_decision_bar():
    rng = np.random.default_rng(5)
    bars, px = {}, 100.0
    for i in range(390):
        o, px = px, px + 0.05 * rng.normal()
        bars[hm(i)] = (o, max(o, px) + 0.01, min(o, px) - 0.01, px, int(rng.integers(500, 5000)))
    base = {h: 2000.0 for h in bars}
    for i in (40, 41, 120):  # big volume at a 30-bar closing low
        o = bars[hm(i)][0]
        bars[hm(i)] = (o, o + 0.001, o - 60.0, o - 60.0, 10_000)
    whole = mf.Session(bars)
    for bar in ("10:29", "11:59", "13:59"):
        cut = mf.Session({k: b for k, b in bars.items() if k <= bar})
        assert mf.tape_window(whole, bar) == mf.tape_window(cut, bar)
        assert mf.w1_events(whole, base, bar) == mf.w1_events(cut, base, bar)
        assert all(h <= bar for h, _ in mf.w1_events(whole, base, bar))


def test_an_event_announced_later_never_moves_an_earlier_decision(tmp_path):
    data, days = staged(tmp_path, n=120)
    body = body_for(days, features=["FOMC", "FOMC_EVE", "CPI", "NFP"], decision_times=["11:00", "13:30"],
                    horizons=["30m"])
    base = mf.FactoryEvents(doc=event_doc())
    # an extra FOMC on 2023-05-24, announced at 12:00 ET on 2023-05-23, and a CPI announced after its own day
    late = mf.FactoryEvents(doc=event_doc((
        {"event": "fomc", "day": "2023-05-24", "time_et": "14:00", "known_at": "2023-05-23T12:00:00-04:00",
         "basis": "test"},
        {"event": "cpi", "day": "2023-04-12", "time_et": "08:30", "known_at": "2023-04-13T00:00:00-04:00",
         "basis": "test"})))
    a, _ = rows_for(data, body, base)
    b, _ = rows_for(data, body, late)
    sa, sb = signals(a, "9999"), signals(b, "9999")
    moved = {k for k in sa if sa[k] != sb[k]}
    assert moved  # the later announcement does reach later decisions
    assert all(k[0] >= "2023-05-23T13:30" for k in moved)  # never a decision before its known_at
    assert {k[0][:16] for k in moved} == {"2023-05-23T13:30", "2023-05-24T11:00", "2023-05-24T13:30"}
    assert sb[("2023-05-23T11:00:00-04:00", "FOMC_EVE|on|long")] is None  # 11:00 is before the 12:00 notice
    assert sb[("2023-05-23T13:30:00-04:00", "FOMC_EVE|on|long")] == 1.0
    assert sa[("2023-03-22T11:00:00-04:00", "FOMC|on|short")] == -1.0
    assert sa[("2023-03-21T11:00:00-04:00", "FOMC_EVE|on|long")] == 1.0
    assert sa[("2023-03-14T11:00:00-04:00", "CPI|on|long")] == 1.0
    assert all(sb[(f"2023-04-12T{t}-04:00", "CPI|on|long")] is None for t in ("11:00:00", "13:30:00"))
    with pytest.raises(mf.RunFileError, match="event manifest"):
        rows_for(data, body)  # event features never run without a manifest


def test_events_outside_the_manifest_coverage_are_unknown_not_quiet():
    ev = mf.FactoryEvents(doc=event_doc())
    t = datetime.fromisoformat("2024-03-20T11:00:00-04:00")
    assert ev.known("2024-03-20", t) is None and ev.known("2023-03-21", t) == frozenset()
    assert ev.known("2023-03-22", t) == {"fomc"}
    with pytest.raises(ValueError):
        ev.known("2023-03-22", datetime.fromisoformat("2023-03-22T11:00"))  # naive clock
    out = mf.calendar_features("2024-03-20", t, "2024-03-21", {"2024-03": "2024-03-15"}, ev)
    assert out["FOMC"] is None and out["CPI"] is None and out["FOMC_EVE"] is None
    assert out["WED"] == 1.0 and out["MON"] == 0.0 and out["OPEX"] == 0.0
    assert mf.calendar_features("2024-03-15", t, None, {"2024-03": "2024-03-15"}, None)["OPEX"] == 1.0
    assert mf.calendar_features("2024-03-15", t, None, {"2024-03": None}, None)["OPEX"] is None


# --- the pinned event manifest ------------------------------------------------------------------


def test_pinned_event_manifest_is_derived_from_the_grid_calendar():
    ev = fc.FactoryEvents()
    raw = (ROOT / "docs/reference/calendar-manifests" / fc.EVENT_PIN[0]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == fc.EVENT_PIN[1] and fc.EVENT_PIN[1] in ev.version
    doc = json.loads(raw)
    assert (doc["coverage"]["start"], doc["coverage"]["end"]) == fc.EVENT_COVERAGE
    src = (ROOT / fc.EVENT_SOURCE).read_bytes()
    if hashlib.sha256(src).hexdigest() != doc["derived_from"]["sha256"]:
        pytest.skip("config/econ_release_dates.json was regenerated; the pinned manifest keeps its own copy")
    assert fc.event_manifest_bytes(fc.derive_events(json.loads(src), doc["derived_from"]["sha256"])) == raw


def test_event_manifest_known_at_rules_and_one_report_a_month():
    doc = json.loads((ROOT / "docs/reference/calendar-manifests" / fc.EVENT_PIN[0]).read_bytes())
    by = {}
    for e in doc["events"]:
        by.setdefault(e["event"], []).append(e)
        known = datetime.fromisoformat(e["known_at"])
        if e["event"] == "fomc":
            assert known.date() == date(int(e["day"][:4]) - 1, 12, 31) and e["time_et"] == "14:00"
        else:  # BLS dates count only from their own day
            assert known.date().isoformat() == e["day"] and known.hour == 0 and e["time_et"] == "08:30"
        assert known.utcoffset() is not None
    for name in ("cpi", "nfp"):
        months = [e["day"][:7] for e in by[name]]
        assert len(months) == len(set(months))  # one monthly report a month, the extra FRED dates excluded
    assert {(x["event"], x["day"]) for x in doc["excluded"]} == set(fc.EVENT_EXCLUDED)
    assert "2024-11-07" in {e["day"] for e in by["fomc"]}  # the post-election Thursday statement
    assert {"2025-10-24", "2025-12-18"} <= {e["day"] for e in by["cpi"]}  # shutdown-moved dates, day-only
    with pytest.raises(ValueError, match="pinned sha256"):
        fc.FactoryEvents(pin=(fc.EVENT_PIN[0], "0" * 64))
    with pytest.raises(ValueError):
        fc.FactoryEvents(doc={**doc, "coverage": {**doc["coverage"], "complete": False}})


def test_opex_is_the_third_friday_or_the_session_before_a_holiday():
    cal = fc.FactoryCalendar()
    days = [s.key.split(":")[1] for s in cal.sessions]
    opex = mf.opex_days(days)
    assert opex["2023-01"] == "2023-01-20" and opex["2024-12"] == "2024-12-20"
    assert opex["2025-04"] == "2025-04-17"  # Good Friday 2025-04-18
    assert opex["2026-06"] == "2026-06-18"  # Juneteenth, Friday 2026-06-19
    assert opex["2026-11"] is None  # the third Friday (11-20) is past the pinned calendar
    assert all(date.fromisoformat(d).weekday() in (3, 4) for d in opex.values() if d)


# --- definitions ----------------------------------------------------------------------------------


def test_tape_definitions_and_missing_volume_is_never_zero():
    bars = {hm(i): (100.0, 100.5, 99.5, 100.0, 100) for i in range(60)}
    bars[hm(59)] = (100.0, 101.0, 99.0, 100.6, 400)  # 10:29
    w = mf.tape_window(mf.Session(bars), "10:29")
    assert w["vol30"] == 29 * 100 + 400 and w["cumvol"] == 59 * 100 + 400
    assert w["range30"] == pytest.approx(101.0 - 99.0)
    vwap = (59 * 100 * 100.0 + 400 * (101.0 + 99.0 + 100.6) / 3) / (59 * 100 + 400)
    assert w["vwap_gap"] == pytest.approx(100.6 / vwap - 1)
    f = mf.tape_features(w, {"vol30": 1650.0, "cumvol": 3150.0, "range30": 0.5}, [("10:05", "W1L")], "10:29")
    assert f["VR30"] == pytest.approx(3300 / 1650) and f["VRDAY"] == pytest.approx(6300 / 3150)
    assert f["RR30"] == pytest.approx(4.0) and f["EWR30"] == pytest.approx(2.0 / 4.0)
    assert (f["W1L"], f["W1H"]) == (1.0, 0.0)
    assert mf.tape_features(w, {"vol30": 1650.0}, [("09:59", "W1L")], "10:29")["W1L"] == 0.0  # outside 30 bars
    empty = mf.tape_features(w, {}, None, "10:29")  # no baseline yet: every ratio and marker is missing
    assert empty == {**dict.fromkeys(("VR30", "VRDAY", "RR30", "EWR30", "W1L", "W1H")), "VWAP": w["vwap_gap"]}
    gap = dict(bars)
    gap[hm(40)] = (*gap[hm(40)][:4], None)  # the live recorder's null-volume minute
    g = mf.tape_window(mf.Session(gap), "10:29")
    assert g["vol30"] is None and g["cumvol"] is None and g["vwap_gap"] is None and g["range30"] is not None
    short = {k: b for k, b in bars.items() if k != hm(45)}
    assert mf.tape_window(mf.Session(short), "10:29")["vol30"] is None  # an incomplete 30-bar window
    assert mf.tape_window(mf.Session(bars), "10:30") == dict.fromkeys(("vol30", "cumvol", "range30", "vwap_gap"))


def test_w1_is_edge8s_w_rule_on_bars():
    def session(spike_at, rng_width=0.01, volume=5000):
        bars = {}
        for i in range(120):
            c = 100.0 - 0.01 * i  # a steady fall: every bar closes at its 30-bar low
            bars[hm(i)] = (c + 0.01, c + 0.02, c - 0.02, c, 1000)
        o, _h, _low, c, _ = bars[spike_at]
        bars[spike_at] = (o, c + rng_width / 2, c - rng_width / 2, c, volume)
        return mf.Session(bars)

    base = {hm(i): 1000.0 for i in range(120)}
    assert mf.w1_events(session("10:40"), base, "11:29") == [("10:40", "W1L")]
    assert mf.w1_events(session("10:40", volume=2999), base, "11:29") == []  # below 3x the minute's median
    assert mf.w1_events(session("10:40", rng_width=0.2), base, "11:29") == []  # range above the session median
    assert mf.w1_events(session("09:40"), base, "11:29") == []  # edge8 fires from 09:45
    assert mf.w1_events(session("10:40"), None, "11:29") == []
    assert mf.w1_events(session("10:40"), base, "10:39") == []  # not yet
    f = mf.tape_features(mf.tape_window(session("10:40"), "10:59"), {}, mf.w1_events(session("10:40"), base, "11:29"),
                         "10:59")
    assert (f["W1L"], f["W1H"]) == (1.0, 0.0)


def test_w1_needs_the_minute_volume_base_from_earlier_sessions_only(tmp_path):
    data, days = staged(tmp_path, n=40)
    body = body_for(days, features=["W1L", "W1H"], decision_times=["14:00"], horizons=["30m"])
    families, cov = rows_for(data, body)
    rows = families["SPY|change|14:00+30m"]
    # tape_history 10: no marker base until 10 earlier calendar sessions have been seen
    assert all(r["features"]["W1L|on|long"]["value"] is None for r in rows if r["decision_at"] < days[10])
    assert cov["tape_history"] == 10 and cov["features"]["W1L@14:00"]["coverage_z"] > 0.5


def test_adding_the_classes_changes_no_price_row(tmp_path):
    data, days = staged(tmp_path, n=60)
    body = body_for(days, features=["F1", "F10", "F11", "F1_shift30"], decision_times=["14:00"], horizons=["60m"])
    spy_only = rows_for(data, body)[0]["SPY|change|14:00+60m"]
    wide = rows_for(data, {**body, "features": body["features"] + ["VR30", "W1L", "FRI", "OPEX"]})[0]
    wide = wide["SPY|change|14:00+60m"]
    assert len(spy_only) == len(wide)
    for a, b in zip(spy_only, wide):
        assert {k: v for k, v in a.items() if k != "features"} == {k: v for k, v in b.items() if k != "features"}
        assert a["features"] == {m: b["features"][m] for m in a["features"]}
    assert mf.FEATURES == ("F1", "F10", "F11", "F1_shift30") and set(mf.ALL_FEATURES) == set(mf.SPECS)
    copy.deepcopy(wide)


def test_feature_registry_tags_every_class_honestly():
    # factory 4 (#1158) adds the options class and factory 5 (#1159) the macro class; they are tagged in
    # tests/test_module_factory_options.py and tests/test_module_factory_macro.py
    assert set(mf.REGISTRY) == set(mf.SPECS) | set(mf.TAPE_SPECS) | set(mf.CALENDAR_SPECS) | set(mf.OPTIONS_SPECS) \
        | set(mf.MACRO_SPECS)
    assert not set(mf.TAPE_SPECS) & {"F7", "F8", "F9"}  # aggressor-flow dials are not rebuilt
    for f, spec in mf.REGISTRY.items():
        assert {"definition", "unit", "source", "live_source", "pit", "symbols", "class", "kind"} <= set(spec), f
        assert spec["class"] == mf.FEATURE_CLASS[f] and spec["kind"] in ("level", "flag")
    for f in ("VR30", "VRDAY", "EWR30", "VWAP", "W1L", "W1H"):
        assert mf.REGISTRY[f]["live_source"].startswith("research-only")
    assert "bars_1m_live" in mf.REGISTRY["RR30"]["live_source"]
    assert mf.FLAGS == {"W1L", "W1H", *mf.CALENDAR_SPECS}


# ── forward parts (factory 7 prerequisite, #1161) ──────────────────────────────

MANIFESTS = ROOT / "docs/reference/calendar-manifests"
ET = ZoneInfo("America/New_York")


def private_calendars(tmp_path):
    """Pinned XNYS bytes copied into private files (the reader refuses group-writable ones)."""
    out = tmp_path / "calendars"
    out.mkdir()
    for name, _ in fc.PINS:
        (out / name).write_bytes((MANIFESTS / name).read_bytes())
        (out / name).chmod(0o600)
    return out


def test_forward_xnys_part_is_the_live_recorder_manifest_sliced(tmp_path):
    live = (MANIFESTS / fc.LIVE_XNYS[0]).read_bytes()
    assert hashlib.sha256(live).hexdigest() == fc.LIVE_XNYS[1]
    name, sha = fc.PINS[-1]
    assert fc.slice_xnys(live, fc.FORWARD_START, "2027-12-31") == (MANIFESTS / name).read_bytes()
    assert hashlib.sha256((MANIFESTS / name).read_bytes()).hexdigest() == sha
    # the live manifest and the research part agree on every session they share
    research = json.loads((MANIFESTS / fc.PINS[-2][0]).read_bytes())
    shared = [s for s in json.loads(live)["sessions"] if s["key"].split(":")[1] <= research["end"]]
    assert shared and shared == [s for s in research["sessions"] if s["key"].split(":")[1] >= "2026-10-01"]
    cal = fc.FactoryCalendar(private_calendars(tmp_path))
    assert cal.previous_day("2026-11-11") == "2026-11-10"
    assert cal.for_day("2026-11-26") is None  # Thanksgiving
    assert cal.previous_day("2026-11-27") == "2026-11-25"
    assert cal.for_day("2027-12-31") is not None
    with pytest.raises(ValueError, match="outside qualified"):
        cal.for_day("2028-01-03")
    with pytest.raises(ValueError, match="outside"):
        fc.slice_xnys(live, "2026-09-30", "2026-12-31")


def test_forward_event_part_extends_without_moving_an_earlier_answer():
    both = fc.FactoryEvents()
    first = fc.FactoryEvents(pin=fc.EVENT_PIN)
    assert (both.start, both.end) == ("2022-11-01", "2026-12-31")
    assert fc.EVENT_PIN[1] in both.version and fc.FORWARD_EVENT_PIN[1] in both.version
    noon = lambda d: datetime.fromisoformat(f"{d}T12:00").replace(tzinfo=ET)
    d = date(2022, 11, 1)
    while d.isoformat() <= "2026-11-10":
        assert both.known(d.isoformat(), noon(d)) == first.known(d.isoformat(), noon(d))
        d += timedelta(days=1)
    at = lambda s: datetime.fromisoformat(s).replace(tzinfo=ET)
    assert both.known("2026-12-10", at("2026-12-10T09:00")) == {"cpi"}
    assert both.known("2026-12-10", at("2026-12-09T23:59")) == frozenset()  # BLS dates count from their own day
    assert both.known("2026-12-09", at("2026-12-09T10:00")) == {"fomc"}
    assert both.known("2026-12-04", at("2026-12-04T10:00")) == {"nfp"}
    assert both.known("2026-11-20", at("2026-11-20T10:00")) == frozenset()
    assert both.known("2027-01-04", at("2027-01-04T10:00")) is None  # past the forward part: unknown, not quiet
    assert first.known("2026-12-10", at("2026-12-10T09:00")) is None


def test_forward_event_part_is_derived_and_bounded_by_its_source():
    raw = (MANIFESTS / fc.FORWARD_EVENT_PIN[0]).read_bytes()
    assert hashlib.sha256(raw).hexdigest() == fc.FORWARD_EVENT_PIN[1]
    doc = json.loads(raw)
    assert doc["coverage"]["start"] == fc.FORWARD_START
    src = (ROOT / fc.EVENT_SOURCE).read_bytes()
    if hashlib.sha256(src).hexdigest() != doc["derived_from"]["sha256"]:
        pytest.skip("config/econ_release_dates.json was regenerated; check_forward_events compares instead")
    econ = json.loads(src)
    sha = doc["derived_from"]["sha256"]
    assert fc.event_manifest_bytes(fc.derive_forward_events(econ, sha, fc.FORWARD_START, "2026-12-31")) == raw
    assert fc.forward_event_end_bound(econ) == "2026-12-31"
    with pytest.raises(ValueError, match="only through"):
        fc.derive_forward_events(econ, sha, fc.FORWARD_START, "2027-01-31")
    assert fc.check_forward_events(econ, sha) == []
    moved = copy.deepcopy(econ)  # a shutdown-style reschedule after the part was pinned
    moved["events"]["cpi"] = [d for d in moved["events"]["cpi"] if d != "2026-12-10"] + ["2026-12-18"]
    assert fc.check_forward_events(moved, sha) == [
        {"part": fc.FORWARD_EVENT_PIN[0], "event": "cpi", "day": "2026-12-10", "change": "removed"},
        {"part": fc.FORWARD_EVENT_PIN[0], "event": "cpi", "day": "2026-12-18", "change": "added"},
    ]


def test_event_parts_must_be_contiguous(tmp_path):
    for name, _ in fc.EVENT_PINS:
        (tmp_path / name).write_bytes((MANIFESTS / name).read_bytes())
    gap = json.loads((tmp_path / fc.FORWARD_EVENT_PIN[0]).read_bytes())
    gap["coverage"]["start"] = "2026-11-12"
    raw = fc.event_manifest_bytes(gap)
    (tmp_path / "gap.json").write_bytes(raw)
    with pytest.raises(ValueError, match="contiguous"):
        fc.FactoryEvents(tmp_path, pins=(fc.EVENT_PIN, ("gap.json", hashlib.sha256(raw).hexdigest())))
