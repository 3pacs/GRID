"""Desk brief (3pacs/GRID#1164): clocks, labels, adapters and the wording guard, on synthetic records only."""

from __future__ import annotations

import json
import os
import sqlite3
import stat
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import create_engine, text

from analysis import desk_brief as B
from analysis import desk_brief_sources as S
from intelligence.regime import market_state as MS

DAY = date(2026, 11, 20)
PREV = date(2026, 11, 19)
SESSION = B.SessionTimes(DAY, datetime(2026, 11, 20, 14, 30, tzinfo=UTC), datetime(2026, 11, 20, 21, 0, tzinfo=UTC))
POST = B.default_as_of("post_close", SESSION)
PRE = B.default_as_of("pre_open", SESSION)


def z(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


# ── clocks ───────────────────────────────────────────────────────────────────

def test_registered_clocks():
    assert PRE == z("2026-11-20T14:00:00Z")                 # 09:00 ET
    assert POST == z("2026-11-21T03:20:00Z")                # 22:20 ET in winter
    summer = B.SessionTimes(date(2026, 7, 1), z("2026-07-01T13:30:00Z"), z("2026-07-01T20:00:00Z"))
    assert B.default_as_of("post_close", summer) == z("2026-07-02T02:20:00Z")   # after the Hermes window
    with pytest.raises(ValueError):
        B.default_as_of("midday", SESSION)


def test_parse_clock_refuses_naive():
    assert B.parse_clock("2026-11-20T21:30:01") is None
    assert B.parse_clock("2026-11-20T21:30:01Z") == z("2026-11-20T21:30:01Z")
    assert B.parse_clock("garbage") is None


def test_section_after_clock_drops_figures():
    s = B.Section("x", "X", "ok", "src", POST + timedelta(minutes=1), ("figure 1",), {"a": 1})
    out = B.clocked(s, POST)
    assert out.status == "after_clock" and out.lines == () and out.facts == {}


def test_empty_section_stays_empty():
    out = B.clocked(B.Section("x", "X", "empty", "src", None, (), {}, ("no file yet",)), POST)
    assert out.status == "empty"


def test_section_without_clock_is_unknown():
    out = B.clocked(B.Section("x", "X", "ok", "src", None, ("figure",)), POST)
    assert out.status == "unknown" and not out.lines


def test_live_read_grace_is_bounded():
    s = B.Section("r", "R", "ok", "url", POST + timedelta(minutes=3), ("x",), {"live": True})
    assert B.clocked(s, POST).status == "ok"
    late = B.Section("r", "R", "ok", "url", POST + timedelta(hours=2), ("x",), {"live": True})
    assert B.clocked(late, POST).status == "after_clock"


def test_build_brief_refuses_generation_before_clock():
    with pytest.raises(ValueError):
        B.build_brief("post_close", SESSION, POST, [], [], as_of_registered=True, generated_at=POST - timedelta(1))


# ── labels ───────────────────────────────────────────────────────────────────

def _trial(**kw):
    base = dict(trial_id="t/1", name="T", source="db:t", registration="REG.md", have=6, need=100, unit="sessions",
                clock=POST - timedelta(hours=6))
    base.update(kw)
    return B.Trial(**base)


def test_forward_trial_label_is_unproven_n_of_n():
    card = _trial().card(POST)
    assert card["label"] == "unproven, 6 of 100" and card["reasons"] == []


def test_met_count_still_unproven():
    assert _trial(have=100).card(POST)["label"].startswith("unproven, 100 of 100; sample requirement met")


def test_spent_and_elsewhere_labels():
    assert _trial(status="SPENT", have=213, need=30).card(POST)["label"].startswith("look spent")
    assert _trial(counts_in="trader-stage0").card(POST)["label"] == "counts kept under trader-stage0"


def test_records_rows_render_counts_only():
    sec, trials = S.records("u", POST, fetch=lambda u, t: _records("2026-11-21T03:21:00+00:00"))
    md = B.render_markdown(B.build_brief("post_close", SESSION, POST, [sec], trials, as_of_registered=True,
                                         generated_at=POST))
    assert "counts only (no per-session row)" in md and "no row for the session" not in md


def test_missing_count_is_labelled_not_found():
    assert _trial(need=None).card(POST)["label"] == "unproven; registered count not supplied"


def test_stale_trial_is_a_data_problem():
    b = B.build_brief("post_close", SESSION, POST, [], [_trial(clock=POST - timedelta(days=9))],
                      as_of_registered=True, generated_at=POST)
    assert any(p["source"] == "t/1" and "stale_observation" in p["detail"] for p in b["data_problems"])


def test_trial_read_after_clock_loses_counts():
    t = B.trial_clocked(_trial(clock=POST + timedelta(hours=1)), POST)
    assert t.have is None and "after the brief's clock" in t.withheld


def test_render_has_no_outcome_columns_and_states_clock():
    b = B.build_brief("post_close", SESSION, POST, [B.Section("m", "M", "ok", "s", POST, ("line",))],
                      [_trial(today="traded")], as_of_registered=True, generated_at=POST)
    md = B.render_markdown(b)
    assert "As of **2026-11-21T03:20:00Z**" in md and "unproven, 6 of 100" in md
    for word in ("mean", "hit rate", "P&L", "win rate"):
        assert word not in md


# ── ledger adapter ───────────────────────────────────────────────────────────

def _fly_db(tmp_path: Path) -> Path:
    p = tmp_path / "ledger.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE fly_days (day TEXT PRIMARY KEY, version TEXT, status TEXT, reason TEXT, graded_at TEXT, x2 REAL)")
    rows = [("2026-11-12", "traded", None, "2026-11-12T21:30:01+00:00"),
            ("2026-11-13", "traded", None, "2026-11-13T21:30:01+00:00"),
            ("2026-11-11", "traded", None, "2026-11-11T21:30:01+00:00"),     # before count_from
            ("2026-11-14", "traded", None, "2026-11-14T21:30:01"),           # naive clock: not counted
            ("2026-11-19", "traded", None, "2026-11-21T09:00:00+00:00"),     # graded after the brief's clock
            ("2026-11-20", "ungraded", "x2_no_line", "2026-11-20T21:30:01+00:00")]
    con.executemany("INSERT INTO fly_days VALUES (?, 'fly-shadow/1', ?, ?, ?, 1.5)", rows)
    con.commit()
    con.close()
    return p


FLY = {"id": "fly-shadow/1", "name": "Fly", "table": "fly_days", "day": "day", "clock": "graded_at",
       "status": "status", "reason": "reason", "counts": ["traded"], "count_from": "2026-11-12", "need": 100,
       "unit": "traded sessions", "decides": "15:00 ET"}


def test_ledger_trial_counts_and_withheld_reason(tmp_path):
    t = S.ledger_trial({**FLY, "db": str(_fly_db(tmp_path))}, "post_close", SESSION, PREV, POST)
    assert t.have == 2                                   # 11-12, 11-13; not 11-11, the naive row, or the late row
    assert t.today is None and t.withheld.startswith("ungraded: x2_no_line")
    assert "1 row(s) without a readable clock" in t.withheld


def test_backfilled_pre_open_shows_not_yet_decided(tmp_path):
    db = str(_fly_db(tmp_path))
    t = S.ledger_trial({**FLY, "db": db}, "pre_open", SESSION, PREV, PRE)
    assert t.today is None and "not yet recorded" not in (t.withheld or "")   # the 11-20 row came after 09:00
    post_early = S.ledger_trial({**FLY, "db": db}, "post_close", SESSION, PREV,
                                z("2026-11-20T21:00:00Z"))
    assert "not yet recorded at the brief's clock" in post_early.withheld


def test_ledger_trial_pre_open_says_when_it_decides(tmp_path):
    t = S.ledger_trial({**FLY, "db": str(_fly_db(tmp_path))}, "pre_open", SESSION, PREV, PRE)
    assert t.today is None and t.decides == "15:00 ET"


def test_ledger_trial_opens_read_only(tmp_path):
    db = _fly_db(tmp_path)
    con = S.ro_sqlite(db)
    with pytest.raises(sqlite3.OperationalError):
        con.execute("DELETE FROM fly_days")
    con.close()


def test_ledger_trial_refuses_unknown_identifier(tmp_path):
    t = S.ledger_trial({**FLY, "db": str(_fly_db(tmp_path)), "status": "status; DROP TABLE fly_days"},
                       "post_close", SESSION, PREV, POST)
    assert t.have is None and "read failed" in t.withheld


def test_missing_ledger_is_withheld_not_zero(tmp_path):
    t = S.ledger_trial({**FLY, "db": str(tmp_path / "nope.db")}, "post_close", SESSION, PREV, POST)
    assert t.have is None and "unreadable" in t.withheld


def test_previous_session_decision(tmp_path):
    p = tmp_path / "on.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE nights (decision_date TEXT, recorded_at TEXT, traded INTEGER, reason TEXT)")
    con.execute("CREATE TABLE outcomes (decision_date TEXT, graded_at TEXT, traded INTEGER)")
    con.execute("INSERT INTO nights VALUES ('2026-11-19', '2026-11-20T01:30:00Z', 1, 'gated in')")
    con.execute("INSERT INTO outcomes VALUES ('2026-11-18', '2026-11-20T01:30:00Z', 1)")
    con.commit()
    con.close()
    spec = {"id": "overnight-shadow/1", "name": "ON", "db": str(p), "table": "nights", "day": "decision_date",
            "clock": "recorded_at", "status": "traded", "reason": "reason", "counts": [1],
            "status_words": {"1": "traded the night"}, "count_table": "outcomes", "count_day": "decision_date",
            "count_clock": "graded_at", "count_status": "traded", "need": 250, "unit": "nights",
            "decision": "previous_session"}
    t = S.ledger_trial(spec, "pre_open", SESSION, PREV, PRE)
    assert t.today == "traded the night (2026-11-19)" and t.have == 1


def test_selector_choices_without_outcomes(tmp_path):
    p = tmp_path / "sel.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE decisions (day TEXT, chooser TEXT, choice TEXT, decided_at TEXT)")
    con.execute("CREATE TABLE outcomes (day TEXT, play TEXT, dollars REAL, graded_at TEXT)")
    con.execute("INSERT INTO decisions VALUES ('2026-11-20', 'default', 'no_trade', '2026-11-20T19:46:01+00:00')")
    con.execute("INSERT INTO outcomes VALUES ('2026-11-19', 'fly_1500', 42.0, '2026-11-19T21:10:01+00:00')")
    con.commit()
    con.close()
    t = S.selector({"id": "selector/1", "name": "Sel", "db": str(p)}, "post_close", SESSION, POST)
    assert t.today == "default: no_trade" and t.have == 1
    shown = {k: v for k, v in t.to_dict(POST).items() if k not in ("source", "registration")}
    assert "42" not in json.dumps(shown)


def test_plays_names_only(tmp_path):
    p = tmp_path / "plays.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE nights (decision_date TEXT, recorded_at TEXT, choices_json TEXT)")
    con.execute("CREATE TABLE outcomes (decision_date TEXT, graded_at TEXT)")
    choices = [{"play": "reversal_5d", "on": True, "expected_net_bp": 15.76, "p_positive": 0.99},
               {"play": "post_earnings_surprise", "on": False, "held_back": "279 nights of record, under 1000"}]
    con.execute("INSERT INTO nights VALUES ('2026-11-20', '2026-11-21T01:50:13Z', ?)", (json.dumps(choices),))
    con.commit()
    con.close()
    t = S.plays({"id": "desk-plays", "name": "Plays", "db": str(p)}, "post_close", SESSION, PREV, POST)
    assert t.today == "night of 2026-11-20: on reversal_5d" and "held back" in t.withheld
    shown = json.dumps({k: v for k, v in t.to_dict(POST).items() if k not in ("source", "registration")})
    assert "15.76" not in shown and "0.99" not in shown


# ── records, files, live state ───────────────────────────────────────────────

def _records(generated_at: str) -> bytes:
    return json.dumps({"schema": "gex-records/1", "status": "ok", "generated_at": generated_at,
                       "counts": {"INTERIM": 1}, "spent": [{"id": "x"}], "errors": {"trader": "unreadable"},
                       "forward": [{"id": "rules-2", "name": "BUY/SELL call", "source": "panel #7", "have": 12,
                                    "need": 100, "unit": "entries", "status": "INTERIM"}]}).encode()


def test_records_trials_counts_only():
    sec, trials = S.records("u", POST, fetch=lambda u, t: _records("2026-11-21T03:21:00+00:00"))
    assert sec.status == "ok" and "trader: unreadable" in sec.problems
    assert trials[0].card(POST)["label"] == "unproven, 12 of 100"
    b = B.build_brief("post_close", SESSION, POST, [sec], trials, as_of_registered=True, generated_at=POST)
    assert b["sections"][0]["status"] == "ok"           # read 60 s late: inside the live grace
    assert "read live 60 s after the brief's clock" in b["sections"][0]["lines"]
    assert not any(p["source"] == "records" and "read live" in p["detail"] for p in b["data_problems"])


def test_records_read_long_after_clock_not_used():
    sec, trials = S.records("u", POST, fetch=lambda u, t: _records("2026-11-22T12:00:00+00:00"))
    b = B.build_brief("post_close", SESSION, POST, [sec], trials, as_of_registered=False, generated_at=z("2026-11-22T12:00:00Z"))
    assert b["sections"][0]["status"] == "after_clock" and b["trials"][0]["have"] is None


def test_records_unreachable_is_unknown():
    def boom(u, t):
        raise OSError("refused")
    sec, trials = S.records("u", POST, fetch=boom)
    assert sec.status == "unknown" and trials == []


def test_desk_health_and_evening(tmp_path):
    h = tmp_path / "health"
    h.mkdir()
    (h / "2026-11-19.json").write_text(json.dumps({"day": "2026-11-19", "checked_at": "2026-11-20T07:55:02Z",
                                                    "checks": 10, "ok": ["prices"], "problems": ["fund files late"]}))
    (h / "2026-11-20.json").write_text(json.dumps({"day": "2026-11-20", "checked_at": "2026-11-21T07:55:02Z",
                                                    "checks": 10, "ok": [], "problems": []}))
    sec = S.desk_health(str(h), SESSION, POST)
    assert sec.status == "ok" and sec.facts["day"] == "2026-11-19" and sec.problems == ("2026-11-19: fund files late",)
    e = tmp_path / "evening"
    e.mkdir()
    (e / "2026-11-19.json").write_text(json.dumps({"day": "2026-11-19", "computed_at": "2026-11-20T03:02:00Z",
                                                    "text": {"body": "Stocks -1.0%"}, "missing": []}))
    pre = S.desk_evening(str(e), "pre_open", SESSION, PREV, PRE)
    post = S.desk_evening(str(e), "post_close", SESSION, PREV, POST)
    assert pre.lines == ("Stocks -1.0%",) and not pre.problems
    assert "not 2026-11-20" in post.problems[0]


def test_gex_withheld_is_a_problem_scheduled_stop_is_not():
    def state(reason):
        return lambda u, t: json.dumps({"served_at": POST.timestamp(), "current_gex": {"status": "unavailable", "reason": reason},
                                        "delayed_gex": {"status": "stale", "age_seconds": 600}}).encode()
    assert S.gex_state("u", POST, fetch=state("scheduled_stop")).problems == ()
    assert S.gex_state("u", POST, fetch=state("strike_window")).problems == ("current GEX withheld: strike_window",)


def test_paper_record_not_supplied(tmp_path):
    sec = S.paper_record(None, "post_close")
    assert sec.status == "not_supplied" and "no call is graded in R" in sec.problems[0]
    missing = S.paper_record(str(tmp_path / "summary.json"), "post_close", POST)
    assert missing.status == "not_supplied"


def _paper(tmp_path, **over):
    doc = {"schema": "desk-paper-summary/1", "run_id": "r1", "as_of": "2026-11-20T21:50:30Z", "paper_only": True,
           "errors": {"selector": "ledger locked"},
           "modules": [
               {"module_id": "fly-shadow/1", "name": "Fly", "look": {"status": "COLLECTING", "have": 6, "need": 100,
                "unit": "traded sessions"}, "grades": {"graded": 6, "no_trade": 1}, "stats": None,
                "stats_withheld": "registration: one look at 100", "stats_gross_only": {"mean_r": 9.9}},
               {"module_id": "x/1", "name": "Open one", "look": {"status": "LOOK DUE", "have": 30, "need": 30},
                "grades": {"graded": 30}, "stats": {"n": 30, "mean_r": 0.125, "null_mean_r": -0.05,
                                                     "excess_mean_r": 0.175}},
               {"module_id": "selector/llm", "name": "LLM chooser", "look": {"status": "NO LOOK"}, "grades": {},
                "stats": None}],
           "desk": {"modules": 3, "graded": 36, "looks": {"COLLECTING": 1, "LOOK DUE": 1, "NO LOOK": 1}}}
    doc.update(over)
    p = tmp_path / "summary.json"
    p.write_text(json.dumps(doc))
    return str(p)


def test_paper_record_follows_its_disclosure(tmp_path):
    sec = S.paper_record(_paper(tmp_path), "post_close", POST)
    text_ = "\n".join(sec.lines)
    assert sec.status == "ok" and sec.source_clock == z("2026-11-20T21:50:30Z")
    assert "outcomes withheld (registration: one look at 100)" in text_ and "9.9" not in text_   # no gross-only
    assert "mean +0.125 vs null -0.050" in text_                     # stats copied only where supplied
    assert "the owner runs the registered look once (not a pass)" in text_
    assert "description, not evidence" in text_ and "of None" not in text_
    assert any("selector: ledger locked" in p for p in sec.problems)


def test_paper_record_after_clock_and_stale(tmp_path):
    late = S.paper_record(_paper(tmp_path, as_of="2026-11-21T07:40:00Z"), "post_close", POST)
    b = B.build_brief("post_close", SESSION, POST, [late], [], as_of_registered=True, generated_at=POST)
    assert b["sections"][0]["status"] == "after_clock" and b["sections"][0]["lines"] == []
    old = S.paper_record(_paper(tmp_path, as_of="2026-11-18T21:50:00Z"), "post_close", POST)
    assert any("older than 36 h" in p for p in old.problems)


def test_paper_record_refuses_wrong_schema_or_not_paper(tmp_path):
    assert S.paper_record(_paper(tmp_path, schema="other/1"), "post_close", POST).status == "unknown"
    assert S.paper_record(_paper(tmp_path, paper_only=False), "post_close", POST).status == "unknown"


# ── market state (sqlite engine: same table and writer as production) ────────

def test_market_state_latest_before_clock(tmp_path):
    eng = create_engine(f"sqlite:///{tmp_path / 'ms.db'}")
    MS.ensure_table(eng)
    rec = {"variables": {"vix": {"status": "ok", "label": "high", "robust_z": 3.1, "obs_date": "2026-11-19"},
                         "dix": {"status": "ok", "label": "normal", "robust_z": 0.2},
                         "dealer_gamma": {"status": "no_baseline"}},
           "gamma_state": {"status": "ok", "regime": "SHORT_GAMMA", "spot_vs_flip": "below", "obs_date": "2026-11-19"},
           "events": {"within_14d": [{"date": "2026-11-25", "event": "gdp"}]}, "coverage": {"ok": 2}}
    with eng.begin() as c:
        c.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) VALUES "
                       "('2026-11-19', 'market-state/1', :r, '2026-11-20 23:15:41')"), {"r": json.dumps(rec)})
        c.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) VALUES "
                       "('2026-11-20', 'market-state/1', :r, '2026-11-21 23:15:00')"), {"r": json.dumps(rec)})
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-19"   # 11-20's row was computed after the clock
    assert any("vix high (z +3.10" in line for line in sec.lines)
    assert any("dealer_gamma (no_baseline)" in line for line in sec.lines)
    assert sec.problems == ()                           # the writer's normal one-session lag is not a problem
    late = S.market_state(eng, "post_close", SESSION, POST, DAY)
    assert "normally has 2026-11-20" in late.problems[0]
    pre = S.market_state(eng, "pre_open", SESSION, PRE)
    assert pre.status == "empty"                       # 11-19's row was computed after 09:00 ET on 11-20


def test_market_state_2_stale_inputs_are_not_usable(tmp_path):
    # market-state/2 (#1303): a stale input keeps its reading but is not ok, so the brief must not call it notable.
    eng = create_engine(f"sqlite:///{tmp_path / 'ms.db'}")
    MS.ensure_table(eng)
    rec = {"variables": {"vix": {"status": "stale", "label": "high", "robust_z": 3.1, "obs_date": "2026-11-12",
                                 "freshness": {"status": "stale", "age": 5, "unit": "sessions", "limit": 2}}},
           "gamma_state": {"status": "stale", "regime": "SHORT_GAMMA", "obs_date": "2026-11-18"},
           "coverage": {"ok": 0, "stale": 1}}
    with eng.begin() as c:
        c.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) VALUES "
                       "('2026-11-19', 'market-state/2', :r, '2026-11-20 23:15:41')"), {"r": json.dumps(rec)})
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.status == "ok" and "rule market-state/2" in sec.lines[0]
    assert "away from normal: none" in sec.lines and "not usable: vix (stale)" in sec.lines
    assert "dealer gamma: stale" in sec.lines and any("stale 1" in line for line in sec.lines)


# ── provisional market state at the close (#1312, display only) ──────────────

_DECISION = {"variables": {"vix": {"status": "ok", "label": "high", "robust_z": 3.1, "obs_date": "2026-11-18"}},
             "events": {"within_14d": [{"date": "2026-11-25", "event": "gdp"}]}, "coverage": {"ok": 1},
             "pending": {"release_surprise": "x"}}


def _provisional_rec(day="2026-11-20", **over):
    closes = {"2026-11-20": "2026-11-20T21:00:00+00:00", "2026-11-19": "2026-11-19T21:00:00+00:00"}[day]
    rec = {"as_of": day, "rule": "market-state/2", "provisional": True, "decision_use": "display_only",
           "label": MS.PROVISIONAL_LABEL.format(day=day, cutoff=closes), "availability_cutoff": closes,
           "variables": {"hy_spread": {"status": "ok", "label": "low", "robust_z": -2.2, "obs_date": "2026-11-19"},
                         "dealer_gamma": {"status": "stale"}},
           "gamma_state": {"status": "stale", "obs_date": "2026-11-19"},
           "events": {"within_14d": [{"date": "2026-11-27", "event": "provisional-only-event"}]},
           "coverage": {"ok": 1, "stale": 1}}
    rec.update(over)
    return rec


def _ms_engine(tmp_path, decision=(), provisional=()):
    """decision / provisional: (session, computed_at as stored, record) rows."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    eng = create_engine(f"sqlite:///{tmp_path / 'ms.db'}")
    MS.ensure_table(eng)
    with eng.begin() as c:
        for day, at, rec in decision:
            c.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) VALUES "
                           "(:d, 'market-state/2', :r, :at)"), {"d": day, "r": json.dumps(rec), "at": at})
    if provisional:
        MS.ensure_provisional_table(eng)
        with eng.begin() as c:
            for day, at, rec in provisional:
                c.execute(text("INSERT INTO market_state_provisional (as_of_date, rule, record, computed_at) VALUES "
                               "(:d, 'market-state/2', :r, :at)"), {"d": day, "r": json.dumps(rec), "at": at})
    return eng


def test_post_close_shows_the_provisional_record_apart_from_the_decision_record(tmp_path):
    eng = _ms_engine(tmp_path, decision=[("2026-11-19", "2026-11-20 23:15:41", _DECISION)],
                     provisional=[("2026-11-20", "2026-11-20 21:15:30", _provisional_rec())])
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.status == "ok" and sec.problems == ()
    assert "market_state_provisional" in sec.source and sec.source.startswith("griddb market_state_daily")
    # The decision record's lines come first, exactly as without a provisional record; every line the
    # provisional record adds is prefixed.
    assert sec.lines == (
        "record for session 2026-11-19 (rule market-state/2), computed 2026-11-20T23:15:41Z",
        "coverage: ok 1",
        "away from normal: vix high (z +3.10, observed 2026-11-18)",
        ("PROVISIONAL (display only): record for session 2026-11-20 (rule market-state/2), from the inputs GRID "
         "held at the close (2026-11-20T21:00:00Z); computed 2026-11-20T21:15:30Z, 16 min after the close. The "
         "session's decision record is written the next evening (#1191) and nothing that decides reads this one"),
        "PROVISIONAL (display only): coverage: ok 1, stale 1",
        "PROVISIONAL (display only): away from normal: hy_spread low (z -2.20, observed 2026-11-19)",
        "PROVISIONAL (display only): not usable: dealer_gamma (stale)",
        "PROVISIONAL (display only): dealer gamma: stale",
    )
    alone = S.market_state(_ms_engine(tmp_path / "alone", decision=[("2026-11-19", "2026-11-20 23:15:41", _DECISION)]),
                           "post_close", SESSION, POST, PREV)
    assert sec.lines[:3] == alone.lines and alone.source == "griddb market_state_daily"
    # The decision record's facts are its own; the provisional record's sit under their own key.
    assert sec.facts["record_date"] == "2026-11-19" and sec.source_clock == z("2026-11-20T23:15:41Z")
    assert sec.facts["notable"] == ["vix high (z +3.10, observed 2026-11-18)"]
    assert sec.facts["events"] == _DECISION["events"] and sec.facts["pending"] == ["release_surprise"]
    p = sec.facts["provisional"]
    assert p["record_date"] == "2026-11-20" and p["decision_use"] == "display_only"
    assert p["table"] == "market_state_provisional" and p["computed_at"] == "2026-11-20T21:15:30Z"
    assert p["availability_cutoff"] == "2026-11-20T21:00:00Z" and p["minutes_after_close"] == 16
    assert p["notable"] == ["hy_spread low (z -2.20, observed 2026-11-19)"]
    assert p["label"].startswith("Provisional, display only")
    assert "provisional-only-event" not in json.dumps({k: v for k, v in sec.facts.items() if k != "provisional"})
    # The calendar section copies the decision record's release list, never the provisional one's.
    cal = S.calendar(SESSION, POST, market_events=sec.facts.get("events"))
    assert not any("provisional-only-event" in ln for ln in cal.lines)
    md = B.render_markdown(B.build_brief("post_close", SESSION, POST, [sec], [], as_of_registered=True,
                                         generated_at=POST))
    assert "- PROVISIONAL (display only): record for session 2026-11-20" in md


def test_provisional_record_not_shown_once_the_decision_record_is_there(tmp_path):
    eng = _ms_engine(tmp_path, decision=[("2026-11-19", "2026-11-20 23:15:41", _DECISION),
                                         ("2026-11-20", "2026-11-21 01:00:00", _DECISION)],
                     provisional=[("2026-11-20", "2026-11-20 21:15:30", _provisional_rec())])
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.facts["record_date"] == "2026-11-20" and "provisional" not in sec.facts
    assert not any("PROVISIONAL" in ln for ln in sec.lines) and sec.source == "griddb market_state_daily"


def test_pre_open_never_shows_a_provisional_record(tmp_path):
    eng = _ms_engine(tmp_path, decision=[("2026-11-18", "2026-11-19 23:15:41", _DECISION)],
                     provisional=[("2026-11-19", "2026-11-19 21:15:30", _provisional_rec("2026-11-19")),
                                  ("2026-11-20", "2026-11-20 13:00:00", _provisional_rec())])
    sec = S.market_state(eng, "pre_open", SESSION, PRE)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-18"
    assert "provisional" not in sec.facts and not any("PROVISIONAL" in ln for ln in sec.lines)
    assert sec.source == "griddb market_state_daily" and sec.problems == ()


def test_provisional_after_the_brief_clock_is_not_shown(tmp_path):
    eng = _ms_engine(tmp_path, decision=[("2026-11-19", "2026-11-20 23:15:41", _DECISION)],
                     provisional=[("2026-11-20", "2026-11-21 03:20:01", _provisional_rec())])
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert "provisional" not in sec.facts and not any("PROVISIONAL" in ln for ln in sec.lines)
    assert sec.problems == ()


@pytest.mark.parametrize("at, rec, why", [
    ("2026-11-20 20:59:00", _provisional_rec(), "before the session's close 2026-11-20T21:00:00Z"),
    ("2026-11-20 21:15:30", _provisional_rec(provisional=False), "does not declare itself provisional"),
    ("2026-11-20 21:15:30", _provisional_rec(decision_use="decision"), "does not declare itself provisional"),
    ("2026-11-20 21:15:30", _provisional_rec(as_of="2026-11-19"), "for that session"),
    # Written by a writer that read to the end of the day (no cutoff), or cut somewhere other than the close.
    ("2026-11-20 21:15:30", _provisional_rec(availability_cutoff=None), "not the session's close"),
    ("2026-11-20 23:30:00", _provisional_rec(availability_cutoff="2026-11-20T23:30:00+00:00"),
     "not the session's close 2026-11-20T21:00:00Z"),
])
def test_provisional_record_that_breaks_its_rules_is_a_problem_not_a_line(tmp_path, at, rec, why):
    eng = _ms_engine(tmp_path, decision=[("2026-11-19", "2026-11-20 23:15:41", _DECISION)],
                     provisional=[("2026-11-20", at, rec)])
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-19" and "provisional" not in sec.facts
    assert not any("PROVISIONAL" in ln for ln in sec.lines)
    assert len(sec.problems) == 1 and why in sec.problems[0] and "not shown" in sec.problems[0]


def test_provisional_without_any_decision_record_keeps_the_section_empty(tmp_path):
    eng = _ms_engine(tmp_path, provisional=[("2026-11-20", "2026-11-20 21:15:30", _provisional_rec())])
    sec = S.market_state(eng, "post_close", SESSION, POST, PREV)
    assert sec.status == "empty" and sec.problems == ("no record computed before the brief's clock",)
    assert sec.lines and all(ln.startswith("PROVISIONAL (display only): ") for ln in sec.lines)
    assert set(sec.facts) == {"provisional"}


# ── event calendar (the pinned parts in the repo) ────────────────────────────

def test_calendar_unknown_past_forward_part():
    late = B.SessionTimes(date(2026, 12, 31), z("2026-12-31T14:30:00Z"), z("2026-12-31T21:00:00Z"))
    sec = S.calendar(late, z("2026-12-31T14:00:00Z"), horizon=3)
    assert sec.status == "ok"
    assert any("do not answer" in line for line in sec.lines)       # 2027 days: unknown, not quiet


def test_calendar_session():
    sec = S.calendar(SESSION, PRE, market_events={"within_14d": [{"date": "2026-11-25", "event": "gdp"}]})
    assert sec.lines[0] == "2026-11-20 (this session): no FOMC, CPI or NFP"
    assert any("gdp 2026-11-25" in line for line in sec.lines)


# ── wording guard ────────────────────────────────────────────────────────────

def _brief():
    return B.build_brief("post_close", SESSION, POST, [B.Section("m", "M", "ok", "s", POST, ("6 sessions traded",))],
                         [_trial()], as_of_registered=True, generated_at=POST)


def _reply(text_: str):
    return lambda url, body, timeout: json.dumps({"choices": [{"message": {"content": text_}}]}).encode()


def test_narrate_refuses_paid_or_remote_hosts():
    calls = []
    out = B.narrate(_brief(), url="https://openrouter.ai/api", model="m", post=lambda *a: calls.append(a))
    assert out["text"] is None and "not a local GPU model host" in out["withheld"] and calls == []


def test_narrate_keeps_grounded_wording():
    out = B.narrate(_brief(), url="http://gridz4:8080", model="m",
                    post=_reply("<think>x</think>The fly trial has 6 of 100 traded sessions. Nothing is graded in R yet."))
    assert out["text"].startswith("The fly trial has 6 of 100")


def test_narrate_drops_invented_numbers_and_finding_words():
    assert B.narrate(_brief(), url="http://gridz4:8080", model="m",
                     post=_reply("The fly made 37.5 percent."))["text"] is None
    out = B.narrate(_brief(), url="http://gridz4:8080", model="m", post=_reply("The fly shows an edge."))
    assert out["text"] is None and "edge" in out["withheld"]


def test_narrate_model_failure_leaves_brief():
    def boom(*a):
        raise TimeoutError
    assert B.narrate(_brief(), url="http://gridz4:8080", model="m", post=boom)["withheld"].startswith("model call failed")


# ── writer ───────────────────────────────────────────────────────────────────

def test_write_once_owner_only(tmp_path):
    from scripts.desk_brief import write_once

    out = tmp_path / "briefs"
    assert write_once(out, "2026-11-20-post_close", {"a": 1}, "# x\n") is True
    assert write_once(out, "2026-11-20-post_close", {"a": 2}, "# y\n") is False
    assert stat.S_IMODE(os.stat(out).st_mode) == 0o700
    for p in out.iterdir():
        assert stat.S_IMODE(os.stat(p).st_mode) == 0o600
    assert json.loads((out / "2026-11-20-post_close.json").read_text()) == {"a": 1}


def test_sources_config_is_valid():
    cfg = S.load_config(Path(__file__).resolve().parents[1] / "config" / "desk_brief_sources.json")
    for spec in cfg["ledgers"]:
        for k in ("table", "day", "clock", "status"):
            assert S.IDENT.match(spec[k]), (spec["id"], k)
        assert spec.get("need") is None or isinstance(spec["need"], int)


# ── factory slots (panel-factory/1) ──────────────────────────────────────────

def _panel(tmp_path, slots, generated_at="2026-11-21T03:19:00+00:00", **block):
    p = tmp_path / "panel.json"
    p.write_text(json.dumps({"generated_at": generated_at, "factory": {
        "version": "panel-factory/1", "status": "ok", "family": {"k": 10, "used": 0},
        "features": {"z_history_have_min": 1, "z_history_needed": 60}, "slots": slots, **block}}))
    db = tmp_path / "signals.db"
    con = sqlite3.connect(db)
    con.execute("CREATE TABLE factory_fires (trial_id TEXT, day TEXT, decision TEXT, z REAL, fired INTEGER, side INTEGER, "
                "abstain TEXT, recorded_at TEXT)")
    con.execute("CREATE TABLE factory_feature_values (day TEXT, decision TEXT, feature TEXT, raw REAL, eligible INTEGER, "
                "reasons TEXT, recorded_at TEXT)")
    con.execute("INSERT INTO factory_fires VALUES ('s1', '2026-11-20', '14:00', 0.4, 0, 0, 'z_history_short', "
                "'2026-11-20T19:00:01+00:00')")
    con.execute("INSERT INTO factory_fires VALUES ('s2', '2026-11-20', '11:00', 2.4, 1, -1, NULL, '2026-11-20T16:00:01+00:00')")
    con.executemany("INSERT INTO factory_feature_values VALUES ('2026-11-20', '10:00', ?, 1.0, ?, '[]', "
                    "'2026-11-20T15:00:01+00:00')", [("a", 1), ("b", 0), ("c", 1)])
    con.commit()
    con.close()
    return str(p), str(db)


def test_no_slot_registered_is_said_plainly(tmp_path):
    panel, db = _panel(tmp_path, [])
    sec, trials = S.factory_slots(panel, db, "post_close", SESSION, POST)
    assert sec.status == "ok" and trials == []
    assert sec.lines[0] == "no slot registered (0 of 10)"
    assert "feature history: 1 of 60 sessions" in sec.lines[1]
    assert "2 of 3 eligible" in sec.lines[2]


def test_slots_say_fire_or_abstain_and_stay_unproven(tmp_path):
    slots = [{"trial_id": "s1", "name": "Slot one", "state": "collecting", "resolved_fires": 3, "min_n": 100,
              "citable": False},
             {"trial_id": "s2", "name": "Slot two", "state": "collecting", "resolved_fires": 7, "min_n": 100,
              "citable": False}]
    panel, db = _panel(tmp_path, slots)
    sec, trials = S.factory_slots(panel, db, "post_close", SESSION, POST)
    by = {t.trial_id: t for t in trials}
    assert by["s1"].withheld == "no fire at 14:00: z_history_short" and by["s1"].today is None
    assert by["s2"].today == "fired at 11:00 (side -1)"
    assert by["s2"].card(POST)["label"] == "unproven, 7 of 100"
    cit = B.Trial(**{**by["s2"].__dict__, "citable": True})
    assert cit.card(POST)["label"].startswith("citable per its owner's record")


def test_slot_fire_after_clock_not_shown(tmp_path):
    slots = [{"trial_id": "s1", "name": "Slot one", "state": "collecting", "resolved_fires": 3, "min_n": 100}]
    panel, db = _panel(tmp_path, slots)
    sec, trials = S.factory_slots(panel, db, "pre_open", SESSION, PRE)
    assert trials[0].today is None and trials[0].withheld is None    # recorded 19:00Z, after 09:00 ET


def _slot(tid, state, **kw):
    return {"trial_id": tid, "name": tid, "state": state, "resolved_fires": 100, "min_n": 100, **kw}


def test_slot_states_never_read_as_a_plain_pass(tmp_path):
    slots = [_slot("p", "pass", citable=True), _slot("r", "retirement_check_due", citable=False),
             _slot("l", "lapsed", citable=False), _slot("d", "look_due", citable=False),
             _slot("u", "underpowered_due", resolved_fires=40, citable=False)]
    panel, db = _panel(tmp_path, slots)
    _, trials = S.factory_slots(panel, db, "post_close", SESSION, POST)
    lab = {t.trial_id: t.card(POST)["label"] for t in trials}
    st = {t.trial_id: t.status for t in trials}
    assert lab["p"].startswith("citable per its owner's record")
    assert lab["r"] == "passed, re-check owed; not citable"
    assert lab["l"] == "passed, then lapsed; not citable"
    assert lab["d"] == "unproven, 100 of 100; sample requirement met, validation pending"
    assert st["d"] == "look due: the owner runs the one look"
    assert lab["u"] == "unproven, 40 of 100" and "owner's look is owed" in st["u"]


def test_citable_only_for_pass(tmp_path):
    panel, db = _panel(tmp_path, [_slot("x", "retirement_check_due", citable=True)])
    _, trials = S.factory_slots(panel, db, "post_close", SESSION, POST)
    assert not trials[0].citable and "re-check owed" in trials[0].card(POST)["label"]


def test_panel_factory_2_and_calendar_exhausted(tmp_path):
    slots = [_slot("p", "pass", citable=True, citable_withheld="the calendar ended; re-pin first")]
    panel, db = _panel(tmp_path, slots, version="panel-factory/2", status="calendar_exhausted",
                       calendar={"first_day": "2026-11-11", "last_day": "2027-12-31", "exhausted": True})
    sec, trials = S.factory_slots(panel, db, "post_close", SESSION, POST)
    assert sec.status == "ok"
    assert "calendar ended 2027-12-31: nothing recorded since, nothing citable" in sec.lines
    assert any("calendar ended 2027-12-31" in p for p in sec.problems)
    assert not trials[0].citable
    assert trials[0].card(POST)["label"] == "passed its look; not citable (the calendar ended; re-pin first)"


def test_factory_block_error_is_a_problem(tmp_path):
    panel, db = _panel(tmp_path, [], status="error", error="signals.db locked")
    sec, _ = S.factory_slots(panel, db, "post_close", SESSION, POST)
    assert sec.problems == ("factory block status error: signals.db locked",)


# ── desk-state sector layer (#1209) ──────────────────────────────────────────

def _sector_rec(day: str) -> dict:
    return {"schema": "desk-sector-state/1", "data_through": day, "kind": "live", "for_session": "2026-11-20",
            "read": "Market rising, speeding up (+0.1 % over 63 sessions); 3 of 11 sectors rising.",
            "market": {"phase": "rising, speeding up", "trend_pct": 0.06}, "sectors_rising": 1,
            "leaders": ["Energy"], "laggards": ["Utilities"],
            "sectors": {"Energy": {"group": "cyclical", "phase": "rising, slowing", "trend_pct": 4.0,
                                   "relative_trend_pct": 3.9, "leading": True},
                        "Utilities": {"group": "defensive", "phase": "falling, speeding up", "trend_pct": -2.0,
                                      "relative_trend_pct": -2.1, "leading": False}},
            "fed_balance_sheet": {"phase": "flat", "growth_13w_pct": -0.02},
            "vix": {"third": "middle", "spike_state": "none", "stale": False},
            "events": {"next_session": [], "within_7_days": [{"date": "2026-11-25", "event": "GDP"}]},
            "overnight_rule": {"market_gate": True, "sectors_in": ["Energy"]}}


def _sector_db(tmp_path, rows) -> str:
    import hashlib

    p = tmp_path / "state.db"
    con = sqlite3.connect(p)
    con.execute("CREATE TABLE desk_state (data_through TEXT PRIMARY KEY, kind TEXT NOT NULL, schema TEXT NOT NULL, "
                "record_json TEXT NOT NULL, record_sha256 TEXT NOT NULL, computed_at TEXT NOT NULL)")
    for day, computed, *rest in rows:
        body = json.dumps(_sector_rec(day), sort_keys=True)
        sha = rest[0] if rest else hashlib.sha256(body.encode()).hexdigest()
        schema = rest[1] if len(rest) > 1 else "desk-sector-state/1"
        con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", (day, "live", schema, body, sha, computed))
    con.commit()
    con.close()
    return str(p)


def test_sector_state_previous_close_before_the_clock(tmp_path):
    db = _sector_db(tmp_path, [("2026-11-18", "2026-11-19T06:45:08Z"), ("2026-11-19", "2026-11-20T06:45:08Z"),
                               ("2026-11-20", "2026-11-21T06:45:08Z")])
    for kind, as_of in (("pre_open", PRE), ("post_close", POST)):
        sec = B.clocked(S.desk_sector_state(db, kind, SESSION, PREV, as_of, PREV), as_of)
        assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-19" and not sec.problems, (kind, sec)
        assert sec.source_clock == z("2026-11-20T06:45:08Z")
        assert sec.lines[0] == "row for session 2026-11-19 (live), computed 2026-11-20T06:45:08Z"
        assert sec.lines[1].startswith("Market rising, speeding up")
        assert "Energy (cyclical): rising, slowing, trend +4.0 %, against the market +3.9 %, leading" in sec.lines
        assert "overnight rule's gate: market open; sectors let through: Energy" in sec.lines
        assert "releases within 7 days: GDP 2026-11-25" in sec.lines
        assert sec.facts["sectors"]["Utilities"]["phase"] == "falling, speeding up"
        assert not any(B.FINDING_WORDS.search(x) for x in sec.lines)


def test_sector_state_older_row_is_a_problem_the_lag_is_not(tmp_path):
    db = _sector_db(tmp_path, [("2026-11-18", "2026-11-19T06:45:08Z")])
    sec = S.desk_sector_state(db, "post_close", SESSION, PREV, POST, PREV)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-18"
    assert sec.problems == ("latest row is for 2026-11-18; the writer normally has 2026-11-19 by now",)


def test_sector_state_refuses_naive_clock_bad_hash_and_other_schema(tmp_path):
    db = _sector_db(tmp_path, [("2026-11-17", "2026-11-18T06:45:08Z"), ("2026-11-18", "2026-11-19 06:45:08"),
                               ("2026-11-19", "2026-11-20T06:45:08Z", "0" * 64)])
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert sec.facts["record_date"] == "2026-11-17"
    assert any("stored hash does not match" in p for p in sec.problems)
    assert any("has no offset" in p for p in sec.problems)
    assert any("normally has 2026-11-19" in p for p in sec.problems)


def test_sector_state_other_schema_not_used(tmp_path):
    import hashlib

    db = _sector_db(tmp_path, [])
    body = json.dumps(_sector_rec("2026-11-19"))
    con = sqlite3.connect(db)
    con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", ("2026-11-19", "live", "desk-sector-state/2", body,
                                                                 hashlib.sha256(body.encode()).hexdigest(), "2026-11-20T06:45:08Z"))
    con.commit()
    con.close()
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert sec.status == "empty" and any("not desk-sector-state/1" in p for p in sec.problems)


def test_sector_state_missing_empty_unread_and_not_configured(tmp_path):
    assert S.desk_sector_state(None, "pre_open", SESSION, PREV, PRE, PREV).status == "not_supplied"
    assert S.desk_sector_state(str(tmp_path / "nope.db"), "pre_open", SESSION, PREV, PRE, PREV).status == "unknown"
    empty = S.desk_sector_state(_sector_db(tmp_path, []), "pre_open", SESSION, PREV, PRE, PREV)
    assert empty.status == "empty" and empty.problems == ("no row computed before the brief's clock",)
    (tmp_path / "late").mkdir()
    late = _sector_db(tmp_path / "late", [("2026-11-19", "2026-11-20T23:00:00Z")])
    assert S.desk_sector_state(late, "pre_open", SESSION, PREV, PRE, PREV).status == "empty"


def test_sector_state_opens_read_only(tmp_path):
    db = _sector_db(tmp_path, [("2026-11-19", "2026-11-20T06:45:08Z")])
    con = S.ro_sqlite(db)
    with pytest.raises(sqlite3.OperationalError):
        con.execute("DELETE FROM desk_state")
    con.close()


def test_sector_state_finds_an_old_row_behind_many_later_ones(tmp_path):
    rows = [("2026-10-01", "2026-10-02T06:45:08Z")]
    rows += [(f"2026-11-{d:02d}", "2026-11-21T06:45:08Z") for d in range(1, 20)]   # all written after the clock
    sec = S.desk_sector_state(_sector_db(tmp_path, rows), "pre_open", SESSION, PREV, PRE, PREV)
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-10-01"
    assert "Fed balance sheet: flat" in sec.lines and "VIX: middle third, none" in sec.lines


def test_sector_state_vix_line_without_a_third(tmp_path):
    import hashlib

    rec = _sector_rec("2026-11-19")
    rec["vix"] = {"spike_state": "spike day", "stale": False}
    body = json.dumps(rec)
    db = _sector_db(tmp_path, [])
    con = sqlite3.connect(db)
    con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", ("2026-11-19", "live", "desk-sector-state/1", body,
                                                                 hashlib.sha256(body.encode()).hexdigest(), "2026-11-20T06:45:08Z"))
    con.commit()
    con.close()
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert "VIX: spike day" in sec.lines and not any("None" in x for x in sec.lines)


def _put_body(db: str, day: str, body_obj, kind: str = "live", computed: str = "2026-11-20T06:45:08Z") -> None:
    import hashlib

    body = json.dumps(body_obj)
    con = sqlite3.connect(db)
    con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", (day, kind, "desk-sector-state/1", body,
                                                                 hashlib.sha256(body.encode()).hexdigest(), computed))
    con.commit()
    con.close()


def _malformed(day: str):
    good = _sector_rec(day)
    yield "a JSON list", [], "is a JSON list, not an object"
    yield "a JSON number", 3, "is a JSON int, not an object"
    yield "body schema", dict(good, schema="desk-sector-state/2"), "body's schema"
    yield "body kind", dict(good, kind="backfill"), "body's kind"
    yield "body date", dict(good, data_through="2026-11-18"), "body's data_through"
    yield "sectors list", dict(good, sectors=[]), "sectors is not an object of objects"
    yield "sector not object", dict(good, sectors={"Energy": 4.0}), "sectors is not an object of objects"
    yield "trend text", dict(good, sectors={"Energy": dict(good["sectors"]["Energy"], trend_pct="4")}), "trend_pct is not a finite number"
    yield "leading text", dict(good, sectors={"Energy": dict(good["sectors"]["Energy"], leading="yes")}), "leading is not true or false"
    yield "vix list", dict(good, vix=[]), "vix is not an object"
    yield "leaders text", dict(good, leaders="Energy"), "leaders is not a list of names"
    yield "events list of text", dict(good, events={"within_7_days": ["GDP"]}), "within_7_days is not a list of objects"
    yield "sectors_in text", dict(good, overnight_rule={"market_gate": True, "sectors_in": "Energy"}), "sectors_in is not a list"
    yield "vix stale text", dict(good, vix=dict(good["vix"], stale="false")), "vix.stale is not true or false"
    yield "vix third number", dict(good, vix=dict(good["vix"], third=2)), "vix.third is not text"
    yield "fed date", dict(good, fed_balance_sheet={"phase": "flat", "obs_date": "11/11/2026"}), "fed_balance_sheet.obs_date is not a YYYY-MM-DD date"
    yield "market trend inf", dict(good, market={"phase": "rising", "trend_pct": float("inf")}), "market.trend_pct is not a finite number"
    yield "sector trend nan", dict(good, sectors={"Energy": dict(good["sectors"]["Energy"], trend_pct=float("nan"))}), "trend_pct is not a finite number"
    yield "gate text", dict(good, overnight_rule={"market_gate": "yes", "sectors_in": ["Energy"]}), "overnight_rule.market_gate is not true or false"
    yield "empty event", dict(good, events={"within_7_days": [{}]}), "within_7_days has an entry without an event name"
    yield "event bad date", dict(good, events={"within_7_days": [{"event": "GDP", "date": None}]}), "within_7_days has an entry without"


def test_sector_state_malformed_newer_row_falls_back_to_the_valid_one(tmp_path):
    for i, (label, body, reason) in enumerate(_malformed("2026-11-19")):
        d = tmp_path / str(i)
        d.mkdir()
        db = _sector_db(d, [("2026-11-18", "2026-11-19T06:45:08Z")])
        _put_body(db, "2026-11-19", body)
        sec = B.clocked(S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV), PRE)
        assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-18", label
        assert any(p.startswith("row for 2026-11-19:") and reason in p and p.endswith("not used") for p in sec.problems), (label, sec.problems)
        alone = tmp_path / f"alone{i}"
        alone.mkdir()
        db2 = _sector_db(alone, [])
        _put_body(db2, "2026-11-19", body)
        only = S.desk_sector_state(db2, "pre_open", SESSION, PREV, PRE, PREV)
        assert only.status == "empty" and any(reason in p for p in only.problems), label


def test_sector_state_gate_unstated_is_not_closed(tmp_path):
    d = tmp_path / "unstated"
    d.mkdir()
    db = _sector_db(d, [])
    _put_body(db, "2026-11-19", dict(_sector_rec("2026-11-19"), overnight_rule={"sectors_in": ["Energy"]}))
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert "overnight rule's gate: market not stated; sectors let through: Energy" in sec.lines
    d = tmp_path / "closed"
    d.mkdir()
    db = _sector_db(d, [])
    _put_body(db, "2026-11-19", dict(_sector_rec("2026-11-19"), overnight_rule={"market_gate": False, "sectors_in": []}))
    assert "overnight rule's gate: market closed; sectors let through: none" in \
        S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV).lines


def test_sector_state_line_building_failure_is_unknown_not_an_abort(tmp_path, monkeypatch):
    db = _sector_db(tmp_path, [("2026-11-19", "2026-11-20T06:45:08Z")])

    def boom(*a, **k):
        raise TypeError("shape")

    monkeypatch.setattr(S, "_sector_section", boom)
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert sec.status == "unknown" and sec.problems[0] == "row for 2026-11-19 could not be read into lines (TypeError)"


def test_sector_state_json_overflow_is_not_a_number(tmp_path):
    import hashlib

    db = _sector_db(tmp_path, [("2026-11-18", "2026-11-19T06:45:08Z")])
    body = json.dumps(_sector_rec("2026-11-19")).replace('"trend_pct": 0.06', '"trend_pct": 1e309')
    assert "1e309" in body
    con = sqlite3.connect(db)
    con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", ("2026-11-19", "live", "desk-sector-state/1", body,
                                                                 hashlib.sha256(body.encode()).hexdigest(), "2026-11-20T06:45:08Z"))
    con.commit()
    con.close()
    sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
    assert sec.facts["record_date"] == "2026-11-18"
    assert any("market.trend_pct is not a finite number" in p for p in sec.problems)


def test_sector_state_huge_json_integer_is_skipped_not_unknown(tmp_path):
    import hashlib

    for i, lit in enumerate(("1" + "0" * 309, "10000000000")):
        d = tmp_path / str(i)
        d.mkdir()
        db = _sector_db(d, [("2026-11-18", "2026-11-19T06:45:08Z")])
        body = json.dumps(_sector_rec("2026-11-19")).replace('"trend_pct": 0.06', '"trend_pct": ' + lit)
        assert lit in body
        con = sqlite3.connect(db)
        con.execute("INSERT INTO desk_state VALUES (?,?,?,?,?,?)", ("2026-11-19", "live", "desk-sector-state/1", body,
                                                                     hashlib.sha256(body.encode()).hexdigest(), "2026-11-20T06:45:08Z"))
        con.commit()
        con.close()
        sec = S.desk_sector_state(db, "pre_open", SESSION, PREV, PRE, PREV)
        assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-18", lit[:5]
        assert any("market.trend_pct is not a finite number" in p for p in sec.problems)
