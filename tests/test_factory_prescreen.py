"""Older-data pre-screen (analysis/factory_prescreen.py): one registered look before a live slot.

Synthetic bars only, laid on the pinned 2018-2022 calendar. No real older data is opened.

* the look is the factory's own arithmetic on the older window, for the one registered module;
* a planted effect passes, noise does not, and a rule that only rides a rising market fails the baseline test;
* the trial, the store's looks and the registration are checked before the data file is opened;
* a trial takes one look, a store takes ten, and the ledger re-checks every look on reopen;
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


def run_look(tmp_path, stores, which, ledger=None, result_sha=None, **over):
    if ledger is None:
        ledger, result_sha = ledger_with()
    path, data_sha = stores[which]
    reg = registration(tmp_path, result_sha, data_sha, **over)
    return fp.prescreen(reg, path, ledger, looked_at=LOOKED), ledger, reg


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


# --- the look -------------------------------------------------------------------------------------


def test_a_planted_effect_passes_and_is_on_the_ledger(tmp_path, stores):
    lp, ap = file_ledger(tmp_path)
    ledger = lse.Ledger(lp, anchor=ap)
    lse.record_factory_declared(ledger, run_id="dev-run", run_file=DEV_RUN, data_manifest_sha256="0" * 64, trial_count=1,
                                recorded_at="2026-11-19T15:57:57+00:00")
    sha = lse.record_factory_result(ledger, trials=[{"trial_id": T1, "family": FAMILY, "feature": MODULE, "status": "tested",
                                                     "selected": True, "representative": True}],
                                    max_t={}, report_sha256="1" * 64, frozen_sha256="2" * 64, recorded_at=RECORDED)["sha256"]
    out, _, reg = run_look(tmp_path, stores, "planted", ledger=ledger, result_sha=sha)

    r = out["record"]
    assert r["verdict"] == "pass" and r["n"] >= 40
    assert r["lower_bound"] > 0 and r["excess_lower_bound"] > 0 and r["mean_net"] > r["baseline"]["mean_net"]
    assert r["alpha_per_test"] == 0.005 and r["boot"] == {"b": 10000, "seed": fp.BOOT_SEED}
    assert r["registration_sha256"] == hashlib.sha256(reg.read_bytes()).hexdigest()
    assert r["store"] == {"id": fp.STORE_ID, "data_sha256": stores["planted"][1], "first_session": "2018-05-01",
                          "last_session": "2022-11-11", "fires_from": FIRES_FROM_TEST}   # the split it used, on the record
    assert r["rule"] == {"family": FAMILY, "feature": MODULE}
    assert sum(y["n"] for y in r["by_year"].values()) == r["n"] and r["first_fire"] >= FIRES_FROM_TEST
    assert r["calendar"] == fp.OlderCalendar().version

    reopened = lse.Ledger(lp, anchor=ap)   # the chain and the look's rules re-verify from the file
    pre = lse.summarize(reopened)["live"]["prescreen"]
    assert (pre["looked"], pre["passed"], pre["failed"], pre["underpowered"]) == (1, 1, 0, 0)
    assert pre["stores"] == {fp.STORE_ID: {"looks": 1, "left": 9}}
    assert lse.summarize(reopened)["live"]["looked"] == 0   # an older-data look is not a live verdict


def test_noise_does_not_pass(tmp_path, stores):
    out, ledger, _ = run_look(tmp_path, stores, "noise")
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
    own = ("seq", "prev_sha256", "result_sha256", "registration_sha256")   # each test ledger has its own genesis time
    assert lse.canonical({k: v for k, v in a.items() if k not in own}) == lse.canonical({k: v for k, v in b.items() if k not in own})
    assert a["lower_bound"] == b["lower_bound"] and a["excess_lower_bound"] == b["excess_lower_bound"]


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


def test_the_bound_is_the_live_looks_bound():
    nets = [0.1 * ((i * 7) % 11 - 4) + 0.05 for i in range(60)]
    # gex-signals factory_slots.boot_lower(nets, 0.005, b=2000, seed=20261120), run 2026-11-20
    assert fp.boot_lower(nets, 0.005, b=2000, seed=20261120) == 0.04499999999999999


def test_fewer_than_forty_fires_is_underpowered_whatever_they_made():
    rows = [{"day": f"2019-01-{i % 28 + 1:02d}", "fired": i < 39, "net": 5.0 if i < 39 else 0.0} for i in range(300)]
    out = fp.look(rows, b=200)
    assert (out["verdict"], out["n"], out["lower_bound"], out["excess_lower_bound"]) == ("underpowered", 39, None, None)


def test_noise_passes_no_more_often_than_stated():
    passes = 0
    for seed in range(100):
        rnd = random.Random(seed)
        rows = [{"day": "2019-01-02", "fired": i < 50, "net": rnd.gauss(0.0, 1.0)} for i in range(250)]
        passes += fp.look(rows, b=300)["verdict"] == "pass"
    assert passes <= 2   # two conditions at 0.005 each: about 0.5 expected in 100


# --- refusals, before any data is read ------------------------------------------------------------


@pytest.mark.parametrize("change, message", [
    ({"result_sha256": "9" * 64}, "must name a factory_run_result"),
    ({"trial_id": "7" * 64}, "is not a trial of run"),
    ({"feature": "F1|z>=1|long"}, "not the rule of that trial"),
    ({"terms": {**fp.TERMS, "min_fires": 20}}, "terms"),
    ({"terms": {**fp.TERMS, "alpha": 0.5}}, "terms"),
    ({"prior_use": "none"}, "prior_use"),
    ({"schema": "factory-prescreen-registration/2"}, "schema"),
    ({"registered_at": "2026-11-19T00:00:00+00:00"}, "predates the result"),
    ({"data_sha256": "abc"}, "data_sha256"),
])
def test_refused_before_the_data_file_is_opened(tmp_path, change, message):
    ledger, sha = ledger_with()
    reg = registration(tmp_path, sha, "5" * 64, **change)
    with pytest.raises(ValueError, match=message):
        fp.prescreen(reg, tmp_path / "there-is-no-such-file.db", ledger)   # a read would raise FileNotFoundError
    assert ledger.of_kind(lse.FACTORY_PRESCREEN) == []


@pytest.mark.parametrize("trial, message", [
    ({"selected": False}, "only a selected trial"),
    ({"representative": False}, "only a selected trial"),
    ({"feature": "FOMC|on|long"}, "the older store cannot supply it"),
    ({"feature": "VIX|z>=1|long"}, "the older store cannot supply it"),
])
def test_only_a_selected_representative_of_a_supported_class_gets_a_look(tmp_path, trial, message):
    body = {"trial_id": T1, "family": FAMILY, "feature": MODULE, "status": "tested", "selected": True,
            "representative": True, **trial}
    run_body = {**DEV_RUN, "features": sorted({*DEV_RUN["features"], body["feature"].split("|")[0]})}
    ledger, sha = ledger_with([body], run_body=run_body)
    reg = registration(tmp_path, sha, "5" * 64, feature=body["feature"])
    with pytest.raises(ValueError, match=message):
        fp.prescreen(reg, tmp_path / "there-is-no-such-file.db", ledger)


def test_the_data_file_must_be_the_registered_one_and_hold_no_development_session(tmp_path, stores):
    ledger, sha = ledger_with()
    path = stores["noise"][0]
    with pytest.raises(fp.PrescreenError, match="not the registered one"):
        fp.prescreen(registration(tmp_path, sha, "5" * 64), path, ledger)
    dev = tmp_path / "dev.db"
    make_older(dev, older_days(3))
    con = sqlite3.connect(dev)
    con.execute("insert into bars values ('SPY', '2022-11-14', '09:30', 1, 1, 1, 1, 1)")
    con.commit()
    con.close()
    dev_sha = hashlib.sha256(dev.read_bytes()).hexdigest()
    with pytest.raises(fp.PrescreenError, match="development sessions refused"):
        fp.prescreen(registration(tmp_path, sha, dev_sha, name="dev.json"), dev, ledger)
    assert ledger.of_kind(lse.FACTORY_PRESCREEN) == []


def test_the_factory_still_refuses_the_older_window():
    with pytest.raises(mf.RunFileError, match="holdout refused"):
        mf.RunFile.from_dict({**DEV_RUN, "first_session": fp.FIRST_SESSION})


def test_a_trial_takes_one_look(tmp_path, stores):
    out, ledger, _ = run_look(tmp_path, stores, "noise")
    sha = out["record"]["result_sha256"]
    with pytest.raises(ValueError, match="already had its pre-screen look"):
        fp.prescreen(registration(tmp_path, sha, stores["planted"][1], name="again.json"), stores["planted"][0], ledger)
    assert len(ledger.of_kind(lse.FACTORY_PRESCREEN)) == 1


# --- the ledger's own rules -----------------------------------------------------------------------


def outcome(result_sha, trial_id, **over):
    return {"run_id": "dev-run", "trial_id": trial_id, "result_sha256": result_sha, "registration_sha256": "3" * 64,
            "store": {"id": fp.STORE_ID, "data_sha256": "4" * 64}, "verdict": "fail", "n": 60, "mean_net": 0.01,
            "lower_bound": -0.2, "excess_mean": 0.0, "excess_lower_bound": -0.3, "alpha_per_test": 0.005,
            "looked_at": LOOKED, **over}


def many_trials(n):
    return [{"trial_id": f"{i:064x}", "family": FAMILY, "feature": MODULE, "status": "tested", "selected": True}
            for i in range(1, n + 1)]


def test_a_store_takes_ten_looks_ever():
    ledger, sha = ledger_with(many_trials(12))
    for i in range(1, 11):
        lse.record_factory_prescreen(ledger, outcome(sha, f"{i:064x}"))
    with pytest.raises(ValueError, match="has had its 10 looks"):
        lse.record_factory_prescreen(ledger, outcome(sha, f"{11:064x}"))
    assert lse.summarize(ledger)["live"]["prescreen"]["stores"][fp.STORE_ID] == {"looks": 10, "left": 0}
    lse.record_factory_prescreen(ledger, outcome(sha, f"{11:064x}", store={"id": "another-store", "data_sha256": "6" * 64}))


@pytest.mark.parametrize("change, message", [
    ({"verdict": "pass"}, "both lower bounds are above zero"),
    ({"verdict": "pass", "lower_bound": 0.1}, "both lower bounds are above zero"),
    ({"lower_bound": 0.1, "excess_lower_bound": 0.1}, "both lower bounds are above zero"),
    ({"verdict": "underpowered"}, "fewer than 40"),
    ({"n": 39}, "need 40 fires"),
    ({"excess_lower_bound": None}, "need 40 fires and both lower bounds"),
    ({"alpha_per_test": 0.05}, "per-look alpha is fixed"),
    ({"store": {"id": fp.STORE_ID, "data_sha256": "xyz"}}, "sha256 of the data file"),
    ({"registration_sha256": None}, "registration file"),
    ({"looked_at": "2026-11-19T00:00:00+00:00"}, "cannot predate"),
])
def test_the_ledger_refuses_a_look_that_breaks_the_rules(change, message):
    ledger, sha = ledger_with()
    with pytest.raises(ValueError, match=message):
        lse.record_factory_prescreen(ledger, outcome(sha, T1, **change))
    assert ledger.of_kind(lse.FACTORY_PRESCREEN) == []


def test_a_stores_data_file_cannot_change_between_looks():
    ledger, sha = ledger_with(many_trials(2))
    lse.record_factory_prescreen(ledger, outcome(sha, f"{1:064x}"))
    with pytest.raises(ValueError, match="cannot change between looks"):
        lse.record_factory_prescreen(ledger, outcome(sha, f"{2:064x}", store={"id": fp.STORE_ID, "data_sha256": "8" * 64}))


def test_reopening_applies_the_rules_not_only_the_hash_chain(tmp_path, monkeypatch):
    lp, ap = file_ledger(tmp_path)
    ledger = lse.Ledger(lp, anchor=ap)
    lse.record_factory_declared(ledger, run_id="dev-run", run_file=DEV_RUN, data_manifest_sha256="0" * 64, trial_count=1,
                                recorded_at="2026-11-19T15:57:57+00:00")
    sha = lse.record_factory_result(ledger, trials=many_trials(1), max_t={}, report_sha256="1" * 64,
                                    frozen_sha256="2" * 64, recorded_at=RECORDED)["sha256"]
    with monkeypatch.context() as patch:   # a writer that skipped the rules: the chain and anchor are valid
        patch.setattr(lse, "validate_factory_prescreen", lambda *a, **k: None)
        lse.record_factory_prescreen(ledger, outcome(sha, f"{1:064x}", verdict="pass"))
    with pytest.raises(ValueError, match="both lower bounds are above zero"):
        lse.Ledger(lp, anchor=ap)


def test_an_s11_ledger_takes_no_look():
    s11 = lse.Ledger.create(None, catalog=lse.synthetic_catalog(), ledger_id="s11-test")
    with pytest.raises(ValueError, match="factory ledger"):
        lse.record_factory_prescreen(s11, outcome("9" * 64, T1))


# --- seam: a real factory run on development bars, then the look, from the ledger alone -----------------


def test_a_rule_the_factory_selected_is_prescreened_from_the_ledger(tmp_path, stores, capsys):
    data, days = staged(tmp_path, plant=0.9)
    rf, _ = run_file(tmp_path, days)
    lp, ap = file_ledger(tmp_path)
    rep = mf.run(rf, data, lse.Ledger(lp, anchor=ap), tmp_path / "out")
    top = rep["ranking"][0]
    assert top["selected"] and (top["family"], top["feature"]) == (FAMILY, MODULE)
    ledger = lse.Ledger(lp, anchor=ap)
    result_sha = next(iter(lse.factory_result_index(ledger)))
    path, data_sha = stores["planted"]
    reg = registration(tmp_path, result_sha, data_sha, run_id="factory-test-1", trial_id=top["trial_id"],
                       registered_at="2100-01-01T00:00:00+00:00")

    args = ["prescreen", "--registration", str(reg), "--data", str(path), "--ledger", str(lp), "--anchor", str(ap)]
    assert mf.main(args) == 0
    out = json.loads(capsys.readouterr().out)
    assert out["ok"] and out["verdict"] == "pass" and out["looks_left"] == 9
    assert mf.main(args) == 2   # a second look at the same trial is refused
    assert "already had its pre-screen look" in json.loads(capsys.readouterr().out)["refused"]
    assert lse.summarize(lse.Ledger(lp, anchor=ap))["live"]["prescreen"]["passed"] == 1
