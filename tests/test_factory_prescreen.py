"""Older-data pre-screen (analysis/factory_prescreen.py): one registered look before a live slot.

Synthetic bars only, laid on the pinned 2018-2022 calendar. No real older data is opened.

* the look is the factory's own arithmetic on the older window, for the one registered module;
* a planted effect passes, noise does not, and a rule that only rides a rising market fails the baseline test;
* a registration goes on the ledger first, reading no data; the look names it and a witnessed head;
* the data file's bytes are hashed once and those bytes are what is queried; sidecars and repeated minutes are refused;
* fires on neighbouring days are resampled together, and a pass cannot rest on five fires;
* a trial is registered once, a store ten times, and the ledger re-checks every record on reopen;
* seam: a rule selected by a real factory run on development bars is pre-screened from the ledger alone.
"""
from __future__ import annotations

import dataclasses
import hashlib
import json
import random
import sqlite3

import numpy as np
import pytest

from analysis import factory_prescreen as fp
from analysis import ledger_steered_exploration as lse
from analysis import module_factory as mf
from tests.test_module_factory import file_ledger, run_file, staged

FAMILY, MODULE = "SPY|change|14:00+60m", "F10|z>=1|long"
T1 = "a" * 64
DEV_RUN = {"run_id": "dev-run", "first_session": "2023-01-03", "last_session": "2024-04-01", "decision_times": ["14:00"],
           "horizons": ["60m"], "features": ["F1", "F10", "F11", "F1_shift30"], "z_thresholds": [1.0],
           "cost_per_share": 0.04, "alpha": 0.05, "perms": 499, "seed": 1, "min_n": 30, "z_history": 20}
RECORDED = "2026-11-19T15:59:09+00:00"
REGISTERED = "2026-11-20T03:00:00+00:00"
LOOKED = "2026-11-21T03:00:00+00:00"
PRIOR_USE = "Edge search 9 looked at this store once, on 2026-11-12, for three other rules; all three failed."
FIRES_FROM_TEST = "2018-08-01"   # the registered split is 2019-01-02; tests shorten the warm-up to keep the bars small


def older_days(n):
    """The first ``n`` sessions of the older store's window, from the pinned calendar."""
    days = [s.key.split(":")[1] for s in fp.OlderCalendar().sessions]
    return [d for d in days if d >= fp.FIRST_SESSION][:n]


def make_older(path, days, plant=0.0, drift=0.0, seed=11, volume=False):
    """SPY minute bars for ``days`` on the pinned calendar (a half day gets its real length).

    ``plant``: added over 14:00-14:59 on sessions whose 13:59 close is in the top 15% of the day's range so far
    (the rule F10 z>=1 finds). ``drift``: added over 14:00-14:59 on EVERY session (a rising market, no rule).
    ``volume``: each session gets its own volume level (otherwise every bar trades 1000)."""
    rng = np.random.default_rng(seed)
    cal = fp.OlderCalendar()
    con = sqlite3.connect(path)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer,"
                " primary key (sym, day, hm))")
    px = 280.0
    for day in days:
        s = cal.for_day(day)
        hi = lo = None
        planted_on = False
        level = float(rng.lognormal(0, 0.4)) if volume else 1.0
        for i in range(int((s.closes - s.opens).total_seconds() // 60)):
            hm = "{:02d}:{:02d}".format(*divmod(9 * 60 + 30 + i, 60))
            step = rng.normal(0, 0.05)
            if "14:00" <= hm < "15:00":
                step += (plant / 60 if planted_on else 0.0) + drift / 60
            o, px = px, px + step
            h, low = max(o, px) + 0.01, min(o, px) - 0.01
            hi, lo = (h if hi is None else max(hi, h)), (low if lo is None else min(lo, low))
            if hm == "13:59":
                planted_on = (px - lo) / (hi - lo) >= 0.85
            v = int(1000 * level * rng.uniform(0.5, 1.5)) if volume else 1000
            con.execute("insert into bars values ('SPY', ?, ?, ?, ?, ?, ?, ?)", (day, hm, o, h, low, px, v))
    con.commit()
    con.close()
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def stores(tmp_path_factory):
    """Three older stores, built once: a planted effect, pure noise, and a rising market with no rule."""
    root = tmp_path_factory.mktemp("older")
    days = older_days(520)
    out = {}
    for name, kwargs in (("planted", {"plant": 0.9}), ("noise", {}), ("rising", {"drift": 0.5})):
        path = root / f"{name}.db"
        out[name] = (path, make_older(path, days, **kwargs))
    return out


@pytest.fixture(autouse=True)
def short_warmup(monkeypatch):
    monkeypatch.setattr(fp, "FIRES_FROM", FIRES_FROM_TEST)


def ledger_with(trials=None, run_body=DEV_RUN):
    """A factory ledger holding one recorded run whose result names ``trials``. Returns (ledger, result sha256)."""
    trials = trials or [{"trial_id": T1, "family": FAMILY, "feature": MODULE, "status": "tested", "selected": True,
                         "representative": True, "cluster": "c1", "class": "price"}]
    ledger = lse.Ledger.create_factory(None, ledger_id="factory-test")
    lse.record_factory_declared(ledger, run_id=run_body["run_id"], run_file=run_body, data_manifest_sha256="0" * 64,
                                trial_count=len(trials), recorded_at="2026-11-19T15:57:57+00:00")
    out = lse.record_factory_result(ledger, trials=trials, max_t={"critical_t": 3.0}, report_sha256="1" * 64,
                                    frozen_sha256="2" * 64, recorded_at=RECORDED)
    return ledger, out["sha256"]


def registration(tmp_path, result_sha, data_sha, name="reg.json", **over):
    body = {"schema": fp.SCHEMA,
            "candidate": {"run_id": "dev-run", "trial_id": T1, "result_sha256": result_sha, "family": FAMILY, "feature": MODULE},
            "data_sha256": data_sha, "terms": dict(fp.TERMS), "prior_use": PRIOR_USE, "registered_at": REGISTERED}
    for key, value in over.items():
        if key in body["candidate"]:
            body["candidate"][key] = value
        else:
            body[key] = value
    path = tmp_path / name
    path.write_text(json.dumps(body, sort_keys=True))
    return path


def witness(ledger, **over):
    """The ledger's head now, as the operator would state it after seeing it off-host."""
    return {"ledger_records": len(ledger.records), "ledger_head_sha256": ledger.head,
            "where": "test fixture: no off-host copy exists", **over}


def run_look(tmp_path, stores, which, ledger=None, result_sha=None, **over):
    """Register, then look. Returns (look, ledger, registration path, registration record)."""
    if ledger is None:
        ledger, result_sha = ledger_with()
    path, data_sha = stores[which]
    reg = registration(tmp_path, result_sha, data_sha, **over)
    registered = fp.register(reg, ledger, recorded_at=REGISTERED)
    return fp.prescreen(reg, path, ledger, witness(ledger), looked_at=LOOKED), ledger, reg, registered


# --- the calendar and the terms -------------------------------------------------------------------


def test_the_older_calendar_is_pinned_and_knows_the_closures():
    cal = fp.OlderCalendar()
    days = [s.key.split(":")[1] for s in cal.sessions]
    assert (days[0], days[-1], len(days)) == ("2018-04-02", "2022-11-11", 1165)
    assert cal.for_day("2018-12-05") is None and cal.for_day("2019-07-04") is None   # Bush funeral; a holiday
    assert (cal.for_day("2019-07-03").closes - cal.for_day("2019-07-03").opens).total_seconds() == 210 * 60
    assert cal.previous_day(fp.FIRST_SESSION) == "2018-04-30"   # the first data session has a qualified prior
    assert fp.LAST_SESSION < mf.DEV_FIRST                       # the window ends before development starts
    with pytest.raises(ValueError):
        cal.for_day("2022-11-14")


def test_the_terms_are_the_ledgers_terms():
    assert (fp.TERMS["alpha"], fp.TERMS["looks"], fp.TERMS["min_fires"]) == (0.05, 10, 40)
    assert (lse.PRESCREEN_ALPHA, lse.PRESCREEN_MAX_LOOKS, lse.PRESCREEN_MIN_FIRES) == (0.05, 10, 40)
    assert (fp.TERMS["block_sessions"], fp.TERMS["trim_fires"], fp.TERMS["evidence"]) == (5, 5, lse.PRESCREEN_EVIDENCE)
    assert fp.TERMS_SHA256 == hashlib.sha256(lse.canonical(fp.TERMS)).hexdigest() and fp.VERSION == "factory-prescreen/2"


# --- registration first, then the look ------------------------------------------------------------


def test_a_planted_effect_passes_and_both_records_are_on_the_ledger(tmp_path, stores):
    lp, ap = file_ledger(tmp_path)
    ledger = lse.Ledger(lp, anchor=ap)
    lse.record_factory_declared(ledger, run_id="dev-run", run_file=DEV_RUN, data_manifest_sha256="0" * 64, trial_count=1,
                                recorded_at="2026-11-19T15:57:57+00:00")
    sha = lse.record_factory_result(ledger, trials=[{"trial_id": T1, "family": FAMILY, "feature": MODULE, "status": "tested",
                                                     "selected": True, "representative": True}],
                                    max_t={}, report_sha256="1" * 64, frozen_sha256="2" * 64, recorded_at=RECORDED)["sha256"]
    out, _, reg, registered = run_look(tmp_path, stores, "planted", ledger=ledger, result_sha=sha)

    g, r = registered["record"], out["record"]
    assert (g["kind"], g["seq"], r["kind"], r["seq"]) == (lse.FACTORY_PRESCREEN_REGISTERED, 3, lse.FACTORY_PRESCREEN, 4)
    assert g["registration_sha256"] == hashlib.sha256(reg.read_bytes()).hexdigest() == r["registration_sha256"]
    assert g["prior_use"] == PRIOR_USE and g["terms_sha256"] == fp.TERMS_SHA256 and g["version"] == fp.VERSION
    assert r["registered_sha256"] == registered["sha256"]
    assert r["registration_witness"]["ledger_records"] == 4 and "checked by a person" in r["registration_witness"]["checked"]
    assert r["verdict"] == "pass" and r["n"] >= 40 and r["evidence"] == lse.PRESCREEN_EVIDENCE
    assert r["lower_bound"] > 0 and r["excess_lower_bound"] > 0 and r["trimmed_mean_net"] > 0
    assert r["mean_net"] > r["trimmed_mean_net"] and 0 < r["top_share"] < 1 and r["mean_net"] > r["baseline"]["mean_net"]
    assert r["alpha_per_test"] == 0.005 and r["boot"] == {"b": 10000, "seed": fp.BOOT_SEED, "block_sessions": 5}
    assert r["store"] == {"id": fp.STORE_ID, "data_sha256": stores["planted"][1], "first_session": "2018-05-01",
                          "last_session": "2022-11-11", "fires_from": FIRES_FROM_TEST, "previously_exposed": True}
    assert r["rule"] == {"family": FAMILY, "feature": MODULE} == g["rule"]
    assert sum(y["n"] for y in r["by_year"].values()) == r["n"] and r["first_fire"] >= FIRES_FROM_TEST

    reopened = lse.Ledger(lp, anchor=ap)   # the chain and both records' rules re-verify from the file
    pre = lse.summarize(reopened)["live"]["prescreen"]
    assert (pre["registered"], pre["registered_not_looked"], pre["looked"], pre["passed"]) == (1, 0, 1, 1)
    assert pre["stores"] == {fp.STORE_ID: {"registered": 1, "left": 9}} and pre["evidence"] == lse.PRESCREEN_EVIDENCE
    assert lse.summarize(reopened)["live"]["looked"] == 0   # an older-data look is not a live verdict


def test_registering_reads_no_data_and_a_look_needs_its_registration(tmp_path, stores, monkeypatch):
    ledger, sha = ledger_with()
    reg = registration(tmp_path, sha, stores["noise"][1])
    with pytest.raises(fp.PrescreenError, match="register it first"):
        fp.prescreen(reg, stores["noise"][0], ledger, witness(ledger))
    monkeypatch.setattr(fp, "load_older", lambda *a, **k: pytest.fail("a registration opened the data"))
    monkeypatch.setattr(fp, "_sealed", lambda *a, **k: pytest.fail("a registration opened the data"))
    done = fp.register(reg, ledger)
    assert done["record"]["kind"] == lse.FACTORY_PRESCREEN_REGISTERED
    assert lse.summarize(ledger)["live"]["prescreen"]["registered_not_looked"] == 1
    with pytest.raises(ValueError, match="already registered"):
        fp.register(registration(tmp_path, sha, stores["noise"][1], name="again.json", prior_use=PRIOR_USE + " Again."), ledger)


@pytest.mark.parametrize("change, message", [
    ({"ledger_head_sha256": "9" * 64}, "recorded off-host"),            # not a head of this ledger
    ({"ledger_records": 3}, "recorded off-host"),                       # a head from before the registration
    ({"ledger_records": 99}, "recorded off-host"),
    ({"where": ""}, "recorded off-host"),
])
def test_a_look_needs_a_head_at_or_after_its_registration(tmp_path, change, message):
    ledger, sha = ledger_with()
    reg = registration(tmp_path, sha, "5" * 64)
    fp.register(reg, ledger)
    seen = witness(ledger)
    if change.get("ledger_records") == 3:
        seen = {**seen, "ledger_records": 3, "ledger_head_sha256": ledger.line_sha(2)}   # the result record: a true, earlier head
    else:
        seen = {**seen, **change}
    with pytest.raises(ValueError, match=message):
        fp.prescreen(reg, tmp_path / "there-is-no-such-file.db", ledger, seen)   # refused before the data is opened
    assert ledger.of_kind(lse.FACTORY_PRESCREEN) == []


def test_noise_does_not_pass(tmp_path, stores):
    out, ledger, _, _ = run_look(tmp_path, stores, "noise")
    assert out["record"]["verdict"] in ("fail", "underpowered")
    assert lse.summarize(ledger)["live"]["prescreen"]["passed"] == 0


def test_a_rule_that_only_rides_a_rising_market_fails_the_baseline(tmp_path, stores):
    r = run_look(tmp_path, stores, "rising")[0]["record"]
    assert r["n"] >= 40 and r["mean_net"] > 0 and r["lower_bound"] > 0   # it made money on its fires
    assert r["baseline"]["mean_net"] > 0 and r["excess_lower_bound"] <= 0   # no more than always being long did
    assert r["verdict"] == "fail"


def test_the_same_inputs_give_the_same_look(tmp_path, stores):
    a = run_look(tmp_path, stores, "planted")[0]["record"]
    b = run_look(tmp_path, stores, "planted")[0]["record"]
    own = ("seq", "prev_sha256", "result_sha256", "registration_sha256", "registered_sha256", "registration_witness")
    assert lse.canonical({k: v for k, v in a.items() if k not in own}) == lse.canonical({k: v for k, v in b.items() if k not in own})
    assert a["lower_bound"] == b["lower_bound"] and a["excess_lower_bound"] == b["excess_lower_bound"]


# --- the arithmetic -------------------------------------------------------------------------------


def test_the_rows_are_the_factorys_rows(stores):
    """Same fires and nets as build_rows gives when the full declared run is built on the same sessions."""
    ledger, sha = ledger_with()
    cand = {"run_id": "dev-run", "trial_id": T1, "result_sha256": sha, "family": FAMILY, "feature": MODULE}
    run, module, family = fp.candidate_run(ledger, cand)
    assert (module, family, run.features, run.target_unit) == (MODULE, FAMILY, ("F10",), "dollars")
    assert (run.first_session, run.last_session, run.z_history) == (fp.FIRST_SESSION, fp.LAST_SESSION, 20)
    sessions = fp.load_older(*stores["planted"])
    rows, coverage = fp.session_rows(run, module, family, sessions, fp.OlderCalendar())

    full = dataclasses.replace(mf.RunFile.from_dict({**DEV_RUN, "target_unit": "dollars"}),
                               first_session=fp.FIRST_SESSION, last_session=fp.LAST_SESSION)
    families, _ = mf.build_rows(sessions, full, calendar=fp.OlderCalendar())
    want = [(r["decision_at"][:10], r["features"][MODULE]["value"] is not None, r["target_dollars"] - 0.04)
            for r in families[FAMILY] if r["decision_at"][:10] >= FIRES_FROM_TEST]
    assert [(r["day"], r["fired"], r["net"]) for r in rows] == want and any(r["fired"] for r in rows)
    assert coverage["first_eligible"] > fp.FIRST_SESSION


def test_a_volume_rule_is_computed_as_the_factory_computes_it(tmp_path):
    """The candidate the factory has today is a tape rule (relative volume). Same rows as the full run would give."""
    family, module = "SPY|change|14:00+close", "VR30|z<=1|long"
    tape_run = {**DEV_RUN, "horizons": ["60m", "close"], "features": ["VR30", "VRDAY", "RR30", "MON", "OPEX"],
                "z_thresholds": [1.0, 2.0], "tape_history": 20, "class_shares": {"tape": 0.75, "calendar": 0.25}}
    ledger, sha = ledger_with([{"trial_id": T1, "family": family, "feature": module, "status": "tested", "selected": True,
                                "representative": True, "class": "tape"}], run_body=tape_run)
    run, got_module, got_family = fp.candidate_run(
        ledger, {"run_id": "dev-run", "trial_id": T1, "result_sha256": sha, "family": family, "feature": module})
    assert (got_module, got_family, run.features, run.z_thresholds, run.tape_history) == (module, family, ("VR30",), (1.0,), 20)
    path = tmp_path / "tape.db"
    sessions = fp.load_older(path, make_older(path, older_days(200), volume=True))
    rows, _ = fp.session_rows(run, module, family, sessions, fp.OlderCalendar())

    full = dataclasses.replace(mf.RunFile.from_dict({**tape_run, "features": ["VR30", "VRDAY", "RR30"], "class_shares": None,
                                                     "target_unit": "dollars"}),
                               first_session=fp.FIRST_SESSION, last_session=fp.LAST_SESSION)
    families, _ = mf.build_rows(sessions, full, calendar=fp.OlderCalendar())
    want = [(r["decision_at"][:10], r["features"][module]["value"] is not None, r["target_dollars"] - 0.04)
            for r in families[family] if r["decision_at"][:10] >= FIRES_FROM_TEST]
    assert [(r["day"], r["fired"], r["net"]) for r in rows] == want
    assert 5 <= sum(r["fired"] for r in rows) < len(rows) / 2   # a quiet half hour is the minority of sessions


def test_a_skipped_window_of_the_run_changes_no_other_window_and_has_no_trial(stores):
    """The declared run's skip_windows drop whole windows. The reduced run carries none: nothing else depends on them."""
    skipping = {**DEV_RUN, "horizons": ["60m", "close"], "skip_windows": ["14:00+close"]}
    ledger, sha = ledger_with(run_body=skipping)
    cand = {"run_id": "dev-run", "trial_id": T1, "result_sha256": sha, "family": FAMILY, "feature": MODULE}
    run, module, family = fp.candidate_run(ledger, cand)
    assert run.skip_windows == () and run.horizons == ("60m",)
    sessions = fp.load_older(*stores["planted"])
    rows, _ = fp.session_rows(run, module, family, sessions, fp.OlderCalendar())
    for body in (skipping, {**skipping, "skip_windows": []}):   # the full run with and without its skipped window
        full = dataclasses.replace(mf.RunFile.from_dict({**body, "target_unit": "dollars"}),
                                   first_session=fp.FIRST_SESSION, last_session=fp.LAST_SESSION)
        families, _ = mf.build_rows(sessions, full, calendar=fp.OlderCalendar())
        want = [(r["decision_at"][:10], r["features"][MODULE]["value"] is not None, r["target_dollars"] - 0.04)
                for r in families[FAMILY] if r["decision_at"][:10] >= FIRES_FROM_TEST]
        assert [(r["day"], r["fired"], r["net"]) for r in rows] == want
    skipped, sha2 = ledger_with([{"trial_id": T1, "family": "SPY|change|14:00+close", "feature": MODULE, "status": "tested",
                                  "selected": True, "representative": True}], run_body=skipping)
    with pytest.raises(fp.PrescreenError, match="skipped window"):
        fp.candidate_run(skipped, {**cand, "result_sha256": sha2, "family": "SPY|change|14:00+close"})


# --- the look's statistics ------------------------------------------------------------------------


def test_fewer_than_forty_fires_is_underpowered_whatever_they_made():
    rows = [{"day": f"2019-01-{i % 28 + 1:02d}", "fired": i < 39, "net": 5.0 if i < 39 else 0.0} for i in range(300)]
    out = fp.look(rows, b=200)
    assert (out["verdict"], out["n"], out["lower_bound"], out["excess_lower_bound"], out["trimmed_mean_net"]) == (
        "underpowered", 39, None, None, None)


def test_five_large_fires_cannot_carry_a_pass():
    rows = [{"day": "2019-01-02", "fired": i % 5 == 0, "net": 0.0} for i in range(300)]   # 60 fires
    fires = [r for r in rows if r["fired"]]
    for i, r in enumerate(fires):
        r["net"] = 50.0 if i % 12 == 0 else -0.05   # five big winners spread through the window, the rest lose a little
    out = fp.look(rows, b=300)
    assert out["n"] == 60 and out["mean_net"] > 4 and out["trimmed_mean_net"] == -0.05 and out["top_share"] > 1
    assert out["verdict"] == "fail"


def test_fires_on_neighbouring_days_are_resampled_together():
    rnd = random.Random(3)
    weeks = [rnd.gauss(0.3, 1.0) for _ in range(80)]   # one shock a week, shared by its five sessions
    rows = [{"day": "2019-01-02", "fired": True, "net": weeks[i // 5]} for i in range(400)]
    together = fp.block_bounds(rows, 0.05, b=2000, block=5)[0]
    apart = fp.block_bounds(rows, 0.05, b=2000, block=1)[0]     # the same sessions treated as independent
    mean = sum(r["net"] for r in rows) / len(rows)
    assert together < apart < mean and (mean - together) > 1.7 * (mean - apart)   # about sqrt(5) wider


def test_noise_passes_no_more_often_than_stated():
    passes = 0
    for seed in range(100):
        rnd = random.Random(seed)
        rows = [{"day": "2019-01-02", "fired": i % 5 == 0, "net": rnd.gauss(0.0, 1.0)} for i in range(250)]
        passes += fp.look(rows, b=300)["verdict"] == "pass"
    assert passes <= 2   # three conditions, the bounds at 0.005 each: about 0.5 expected in 100


# --- refusals, before any data is read ------------------------------------------------------------


@pytest.mark.parametrize("change, message", [
    ({"result_sha256": "9" * 64}, "not on this ledger"),
    ({"trial_id": "7" * 64}, "not in that result"),
    ({"feature": "F1|z>=1|long"}, "not the rule of that trial"),
    ({"terms": {**fp.TERMS, "min_fires": 20}}, "terms"),
    ({"terms": {**fp.TERMS, "alpha": 0.5}}, "terms"),
    ({"terms": {**fp.TERMS, "version": "factory-prescreen/1"}}, "terms"),
    ({"prior_use": "none"}, "prior_use"),
    ({"schema": "factory-prescreen-registration/2"}, "schema"),
    ({"registered_at": "2026-11-19T00:00:00+00:00"}, "cannot predate the result"),
    ({"data_sha256": "abc"}, "data_sha256"),
])
def test_a_bad_registration_is_refused_and_nothing_is_appended(tmp_path, change, message):
    ledger, sha = ledger_with()
    reg = registration(tmp_path, sha, "5" * 64, **change)
    with pytest.raises(ValueError, match=message):
        fp.register(reg, ledger)
    with pytest.raises(ValueError):
        fp.prescreen(reg, tmp_path / "there-is-no-such-file.db", ledger, witness(ledger))   # a read would raise OSError
    assert len(ledger.records) == 3


@pytest.mark.parametrize("trial, message", [
    ({"selected": False}, "only a selected trial"),
    ({"representative": False}, "only a selected trial"),
    ({"feature": "FOMC|on|long"}, "the older store cannot supply it"),
    ({"feature": "VIX|z>=1|long"}, "the older store cannot supply it"),
    ({"family": "XS|close_1d", "feature": "reversal_21d|within_sector|q10"}, "not a SPY intraday module"),
])
def test_only_a_selected_representative_of_a_supported_class_is_registered(tmp_path, trial, message):
    body = {"trial_id": T1, "family": FAMILY, "feature": MODULE, "status": "tested", "selected": True,
            "representative": True, **trial}
    name = body["feature"].split("|")[0]
    run_body = {**DEV_RUN, "features": sorted({*DEV_RUN["features"], *([name] if name in mf.KNOWN_FEATURES else [])})}
    ledger, sha = ledger_with([body], run_body=run_body)
    reg = registration(tmp_path, sha, "5" * 64, feature=body["feature"], family=body["family"])
    with pytest.raises(ValueError, match=message):
        fp.register(reg, ledger)
    assert len(ledger.records) == 3


# --- the data file --------------------------------------------------------------------------------


def registered_ledger(tmp_path, data_sha, name="reg.json"):
    ledger, sha = ledger_with()
    reg = registration(tmp_path, sha, data_sha, name=name)
    fp.register(reg, ledger)
    return ledger, reg


def test_the_bytes_that_are_hashed_are_the_bytes_that_are_queried(tmp_path, stores, monkeypatch):
    path, data_sha = stores["noise"]
    ledger, reg = registered_ledger(tmp_path, data_sha)
    opened = []
    real = fp.sqlite3.connect
    monkeypatch.setattr(fp.sqlite3, "connect", lambda target, *a, **k: (opened.append(str(target)), real(target, *a, **k))[1])
    fp.prescreen(reg, path, ledger, witness(ledger), looked_at=LOOKED)
    assert opened and all(str(path) not in target for target in opened)   # never a second open of the path


@pytest.mark.parametrize("suffix", ["-wal", "-shm", "-journal"])
def test_a_sidecar_beside_the_data_file_is_refused(tmp_path, suffix):
    db = tmp_path / "bars.db"
    data_sha = make_older(db, older_days(3))
    (tmp_path / f"bars.db{suffix}").write_bytes(b"x")
    with pytest.raises(fp.PrescreenError, match="sidecar"):
        fp.load_older(db, data_sha)


def test_a_wal_mode_file_a_repeated_minute_and_a_development_session_are_refused(tmp_path):
    wal = tmp_path / "wal.db"
    make_older(wal, older_days(3))
    con = sqlite3.connect(wal)
    con.execute("pragma journal_mode=wal")
    con.execute("insert into bars values ('SPY', '2018-05-04', '09:30', 1, 1, 1, 1, 1)")
    con.commit()
    con.close()   # the last connection checkpoints and removes the sidecars; the header still says WAL
    assert not (tmp_path / "wal.db-wal").exists()
    with pytest.raises(fp.PrescreenError, match="WAL mode"):
        fp.load_older(wal, hashlib.sha256(wal.read_bytes()).hexdigest())

    twice = tmp_path / "twice.db"
    con = sqlite3.connect(twice)
    con.execute("create table bars (sym text, day text, hm text, o real, h real, l real, c real, v integer)")   # no key
    for close in (1.0, 2.0):
        con.execute("insert into bars values ('SPY', '2018-05-01', '09:30', 1, 1, 1, ?, 1)", (close,))
    con.commit()
    con.close()
    with pytest.raises(fp.PrescreenError, match="two rows for SPY 2018-05-01 09:30"):
        fp.load_older(twice, hashlib.sha256(twice.read_bytes()).hexdigest())

    dev = tmp_path / "dev.db"
    make_older(dev, older_days(3))
    con = sqlite3.connect(dev)
    con.execute("insert into bars values ('SPY', '2022-11-14', '09:30', 1, 1, 1, 1, 1)")
    con.commit()
    con.close()
    with pytest.raises(fp.PrescreenError, match="development sessions refused"):
        fp.load_older(dev, hashlib.sha256(dev.read_bytes()).hexdigest())
    with pytest.raises(fp.PrescreenError, match="not the registered one"):
        fp.load_older(dev, "5" * 64)


def test_the_factory_still_refuses_the_older_window():
    with pytest.raises(mf.RunFileError, match="holdout refused"):
        mf.RunFile.from_dict({**DEV_RUN, "first_session": fp.FIRST_SESSION})


def test_a_registration_takes_one_look(tmp_path, stores):
    out, ledger, reg, _ = run_look(tmp_path, stores, "noise")
    with pytest.raises(ValueError, match="already had its look"):
        fp.prescreen(reg, stores["noise"][0], ledger, witness(ledger))
    assert len(ledger.of_kind(lse.FACTORY_PRESCREEN)) == 1


# --- the ledger's own rules -----------------------------------------------------------------------


def many_trials(n):
    return [{"trial_id": f"{i:064x}", "family": FAMILY, "feature": MODULE, "status": "tested", "selected": True}
            for i in range(1, n + 1)]


def reg_record(result_sha, trial_id, **over):
    return {"run_id": "dev-run", "trial_id": trial_id, "result_sha256": result_sha, "registration_sha256": "3" * 64,
            "rule": {"family": FAMILY, "feature": MODULE}, "store": {"id": fp.STORE_ID, "data_sha256": "4" * 64},
            "version": fp.VERSION, "terms_sha256": fp.TERMS_SHA256, "prior_use": PRIOR_USE, "registered_at": REGISTERED, **over}


def outcome(ledger, registered, **over):
    g = registered["record"]
    base = {k: g[k] for k in ("run_id", "trial_id", "result_sha256", "registration_sha256", "rule", "store", "version", "terms_sha256")}
    return {**base, "registered_sha256": registered["sha256"], "registration_witness": witness(ledger), "verdict": "fail",
            "n": 60, "mean_net": 0.01, "lower_bound": -0.2, "excess_mean": 0.0, "excess_lower_bound": -0.3,
            "trimmed_mean_net": -0.01, "alpha_per_test": 0.005, "evidence": lse.PRESCREEN_EVIDENCE, "looked_at": LOOKED, **over}


def test_a_store_takes_ten_registrations_ever():
    ledger, sha = ledger_with(many_trials(12))
    for i in range(1, 11):
        lse.record_factory_prescreen_registered(ledger, reg_record(sha, f"{i:064x}"))
    with pytest.raises(ValueError, match="has had its 10 registrations"):
        lse.record_factory_prescreen_registered(ledger, reg_record(sha, f"{11:064x}"))
    assert lse.summarize(ledger)["live"]["prescreen"]["stores"][fp.STORE_ID] == {"registered": 10, "left": 0}
    lse.record_factory_prescreen_registered(
        ledger, reg_record(sha, f"{11:064x}", store={"id": "another-store", "data_sha256": "6" * 64}))
    with pytest.raises(ValueError, match="cannot change between looks"):
        ledger2, sha2 = ledger_with(many_trials(2))
        lse.record_factory_prescreen_registered(ledger2, reg_record(sha2, f"{1:064x}"))
        lse.record_factory_prescreen_registered(
            ledger2, reg_record(sha2, f"{2:064x}", store={"id": fp.STORE_ID, "data_sha256": "8" * 64}))


@pytest.mark.parametrize("change, message", [
    ({"verdict": "pass"}, "both lower bounds and the trimmed mean are above zero"),
    ({"verdict": "pass", "lower_bound": 0.1, "excess_lower_bound": 0.1}, "trimmed mean are above zero"),   # rests on five fires
    ({"verdict": "pass", "lower_bound": 0.1, "trimmed_mean_net": 0.1}, "above zero"),                      # no better than always-on
    ({"lower_bound": 0.1, "excess_lower_bound": 0.1, "trimmed_mean_net": 0.1}, "above zero"),               # a pass called a fail
    ({"verdict": "underpowered"}, "fewer than 40"),
    ({"n": 39}, "need 40 fires"),
    ({"trimmed_mean_net": None}, "need 40 fires, both lower bounds and the trimmed mean"),
    ({"alpha_per_test": 0.05}, "per-look alpha is fixed"),
    ({"evidence": "independent evidence"}, "is labelled"),
    ({"registered_sha256": "9" * 64}, "must name its registration record"),
    ({"registration_sha256": "9" * 64}, "is not its registration's"),
    ({"rule": {"family": FAMILY, "feature": "F1|z>=1|long"}}, "rule of its trial"),
    ({"store": {"id": fp.STORE_ID, "data_sha256": "8" * 64}}, "store or data file is not its registration's"),
    ({"looked_at": "2026-11-20T02:59:59+00:00"}, "cannot predate"),
])
def test_the_ledger_refuses_a_look_that_breaks_the_rules(change, message):
    ledger, sha = ledger_with()
    registered = lse.record_factory_prescreen_registered(ledger, reg_record(sha, T1), recorded_at=REGISTERED)
    with pytest.raises(ValueError, match=message):
        lse.record_factory_prescreen(ledger, outcome(ledger, registered, **change))
    assert ledger.of_kind(lse.FACTORY_PRESCREEN) == []
    lse.record_factory_prescreen(ledger, outcome(ledger, registered))   # and the honest look is taken


def test_reopening_applies_the_rules_not_only_the_hash_chain(tmp_path, monkeypatch):
    lp, ap = file_ledger(tmp_path)
    ledger = lse.Ledger(lp, anchor=ap)
    lse.record_factory_declared(ledger, run_id="dev-run", run_file=DEV_RUN, data_manifest_sha256="0" * 64, trial_count=1,
                                recorded_at="2026-11-19T15:57:57+00:00")
    sha = lse.record_factory_result(ledger, trials=many_trials(1), max_t={}, report_sha256="1" * 64,
                                    frozen_sha256="2" * 64, recorded_at=RECORDED)["sha256"]
    registered = lse.record_factory_prescreen_registered(ledger, reg_record(sha, f"{1:064x}"), recorded_at=REGISTERED)
    lse.Ledger(lp, anchor=ap)   # a registration alone reopens
    with monkeypatch.context() as patch:   # a writer that skipped the rules: the chain and anchor are valid
        patch.setattr(lse, "validate_factory_prescreen", lambda *a, **k: None)
        lse.record_factory_prescreen(ledger, outcome(ledger, registered, verdict="pass"))
    with pytest.raises(ValueError, match="above zero"):
        lse.Ledger(lp, anchor=ap)


def test_an_s11_ledger_takes_no_registration_and_no_look():
    s11 = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="factory ledger"):
        lse.record_factory_prescreen_registered(s11, reg_record("9" * 64, T1))
    with pytest.raises(ValueError, match="factory ledger"):
        lse.record_factory_prescreen(s11, {"kind": lse.FACTORY_PRESCREEN})


# --- seam: a real factory run on development bars, then register and look, by the commands alone ---------


def test_a_rule_the_factory_selected_is_registered_and_prescreened_by_command(tmp_path, stores, capsys):
    data, days = staged(tmp_path, plant=0.9)
    rf, _ = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")
    top = rep["ranking"][0]
    assert top["selected"] and (top["family"], top["feature"]) == (FAMILY, MODULE)
    result_sha = next(iter(lse.factory_result_index(lse.Ledger(lp, anchor=ap))))
    path, data_sha = stores["planted"]
    reg = registration(tmp_path, result_sha, data_sha, run_id="factory-test-1", trial_id=top["trial_id"],
                       registered_at="2100-01-01T00:00:00+00:00")
    paths = ["--registration", str(reg), "--ledger", str(lp), "--anchor", str(ap)]
    look_args = ["prescreen", *paths, "--data", str(path), "--witness-where", "test fixture: no off-host copy"]

    assert mf.main([*look_args, "--witness-records", "3", "--witness-head", "0" * 64]) == 2   # not registered yet
    assert "register it first" in json.loads(capsys.readouterr().out)["refused"]
    assert mf.main(["prescreen-register", *paths]) == 0
    done = json.loads(capsys.readouterr().out)
    assert done["ok"] and done["seq"] == 3 and done["records"] == 4 and done["rule"] == {"family": FAMILY, "feature": MODULE}
    seen = ["--witness-records", str(done["records"]), "--witness-head", done["head_sha256"]]
    assert mf.main([*look_args, *seen]) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["verdict"] == "pass" and out["registrations_left"] == 9 and out["evidence"] == lse.PRESCREEN_EVIDENCE
    assert mf.main([*look_args, *seen]) == 2   # a second look at the same registration is refused
    assert "already had its look" in json.loads(capsys.readouterr().out)["refused"]
    assert lse.summarize(lse.Ledger(lp, anchor=ap))["live"]["prescreen"]["passed"] == 1
