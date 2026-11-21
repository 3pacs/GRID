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
    assert "42" not in json.dumps(t.to_dict(POST))


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
    assert "15.76" not in json.dumps(t.to_dict(POST)) and "0.99" not in json.dumps(t.to_dict(POST))


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
    assert "description, not evidence" in text_
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
