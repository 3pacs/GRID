"""Unread receipt (alpha_research.edge_stream.receipt): each source it reconciles, and how it fails."""

from __future__ import annotations

import json
import sqlite3
import stat
from datetime import date

import pytest

from alpha_research.edge_stream import families as F
from alpha_research.edge_stream import ledger as L
from alpha_research.edge_stream import policy as P
from alpha_research.edge_stream import receipt as RC
from alpha_research.edge_stream import sanity as S
from alpha_research.edge_stream import template_queue as TQ

TODAY = date(2026, 11, 13)
OLD_RUNNER = '''
def compute_look(reg_sha, reg, source, today, extras=None):
    family = FAMILIES.get(reg["family"])
    if family is None or code_sha256(family) != reg["code_sha256"]:
        raise CodeChanged("family x code differs from the registered sha")
    bars, start, end = _read(reg, source, today)
'''
READS_FIRST = '''
def compute_look(reg_sha, reg, source, today, extras=None):
    bars, start, end = _read(reg, source, today)
    if code_sha256(FAMILIES[reg["family"]]) != reg["code_sha256"]:
        raise CodeChanged("x")
'''


def _abandoned(tmp_path):
    """A policy-3-style ledger: metals registered, then abandoned with reason text only."""
    pol = json.loads(P.canonical_bytes(P.policy_payload()))
    pol.pop("unread_abandonment")
    pol["owner_decisions"] = pol["owner_decisions"][:1]
    pol["policy_version"] = "edge-stream-policy/3"
    with pytest.MonkeyPatch.context() as m:
        m.setattr(P, "policy_payload", lambda: pol)
        m.setattr(P, "policy_sha256", lambda: L.sha256_bytes(P.canonical_bytes(pol)))
        led = L.memory("2026-11-13T06:27:00+00:00")
        prop = F.build_proposal(dict(TQ.QUEUE[0]), TODAY)
        rep = S.check(prop, L.load_state(led), TODAY)
        sha = L.append_registration(led, {**prop, "sanity": rep, "recorded_at": "2026-11-13T11:03:50+00:00"})
        L.append_abandoned(led, sha, "family IM1 code differs from the registered sha", "2026-11-16T11:05:50+00:00")
    pid = prop["proposal_id"]
    summary = lambda **kw: json.dumps({"dry_run": False, "looks": [], "abandoned": [], **kw}, indent=1)  # noqa: E731
    (tmp_path / "cycle.log").write_text("\n".join([
        "2026-11-13 11:03:50 | INFO     | config:<module>:1 — GitSink attached",
        f"2026-11-13 11:15:11 | INFO     | runner:_register_or_reject:1 — edge_stream registered {pid} ({sha[:12]})",
        summary(registered=[{"registration": sha[:16]}]),
        "2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached",
        summary(abandoned=[{"registration": sha[:16], "reason": "family IM1 code differs from the registered sha"}]),
    ]) + "\n")
    return led, sha, pid


def _cache(path, rows):
    con = sqlite3.connect(path)
    con.execute("CREATE TABLE sessions (ticker TEXT, day TEXT, bars TEXT, fetched_at TEXT, PRIMARY KEY (ticker, day))")
    con.executemany("INSERT INTO sessions VALUES (?, ?, '[]', ?)", rows)
    con.commit()
    con.close()
    return path


def test_unread_when_no_source_shows_a_read(tmp_path):
    led, sha, _ = _abandoned(tmp_path)
    pre = _cache(tmp_path / "a.sqlite3", [("GLD", "2024-11-25", "2026-11-13T11:10:00+00:00")])  # pre-window only
    rc = RC.build(led, sha, tmp_path, [pre], OLD_RUNNER, "abc")
    assert rc["verdict"] == "UNREAD" and rc["failed"] == []
    assert rc["consumed_through_exclusive"] == rc["window"]["start"]
    assert rc["checks"]["cycle_log"]["detail"]["cycles_between_registration_and_abandonment"] == [
        "2026-11-16T11:05:50+00:00"]


def test_sessions_found_in_the_window_stay_consumed(tmp_path):
    led, sha, _ = _abandoned(tmp_path)
    start = L.load_state(led).registrations[sha]["window"]["start"]
    late = start  # the window's first session
    c = _cache(tmp_path / "b.sqlite3", [("SLV", late, "2026-11-22T03:41:00+00:00")])
    rc = RC.build(led, sha, tmp_path, [c], OLD_RUNNER, "abc")
    assert rc["verdict"] == "UNREAD_EXCEPT" and rc["sessions_read_on_or_after_start"] == [late]
    assert rc["consumed_through_exclusive"] > late


@pytest.mark.parametrize("break_it", ["look_line", "extra_cycle", "dry_run", "attempt", "staged", "reads_first"])
def test_any_sign_of_a_read_refuses(tmp_path, break_it):
    led, sha, pid = _abandoned(tmp_path)
    log = tmp_path / "cycle.log"
    runner = OLD_RUNNER
    if break_it == "look_line":
        log.write_text(log.read_text() + f"2026-11-16 11:06:00 | INFO     | runner:x:1 — edge_stream look {pid}: FAIL\n")
    elif break_it == "extra_cycle":
        text = log.read_text().replace("2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached",
                                       "2026-11-14 11:05:00 | INFO     | config:<module>:1 — GitSink attached\n{\n}\n"
                                       "2026-11-16 11:05:50 | INFO     | config:<module>:1 — GitSink attached")
        log.write_text(text)
    elif break_it == "dry_run":
        log.write_text(log.read_text().replace('"dry_run": false', '"dry_run": true'))
    elif break_it == "attempt":
        (tmp_path / "attempts.jsonl").write_text(json.dumps({"registration_sha256": sha, "at": "x"}) + "\n")
    elif break_it == "staged":
        (tmp_path / "looks").mkdir()
        (tmp_path / "looks" / f"{sha[:16]}.json").write_text("{}")
    else:
        runner = READS_FIRST
    rc = RC.build(led, sha, tmp_path, [], runner, "abc")
    assert rc["verdict"] == "REFUSED" and rc["failed"], break_it


def test_a_ledger_read_or_look_refuses(tmp_path):
    led, sha, _ = _abandoned(tmp_path)
    other = L.memory()
    prop = F.build_proposal(dict(TQ.QUEUE[0]), TODAY)
    osha = L.append_registration(other, {**prop, "sanity": S.check(prop, L.load_state(other), TODAY),
                                         "recorded_at": "2026-11-13T12:00:00+00:00"})
    L.append_read(other, osha, "look", "2026-11-14T11:00:00+00:00")
    L.append_abandoned(other, osha, "x", "2026-11-16T11:00:00+00:00", cause=L.CODE_CHANGED,
                       read_evidence={"ledger_reads": ["look"], "failed_attempts": 0, "report_staged": False})
    assert RC.build(other, osha, tmp_path, [], OLD_RUNNER, "abc")["verdict"] == "REFUSED"
    assert RC.build(led, "0" * 64, tmp_path, [], OLD_RUNNER, "abc")["verdict"] == "REFUSED"


def test_the_code_order_check_reads_the_shipped_runner():
    from pathlib import Path

    here = Path(F.__file__).resolve().parent / "runner.py"
    assert RC.code_check_precedes_reads(here.read_text())["ok"]
    assert RC.code_check_precedes_reads(OLD_RUNNER)["ok"]
    assert not RC.code_check_precedes_reads(READS_FIRST)["ok"]
    assert not RC.code_check_precedes_reads("def other():\n    pass\n")["ok"]


def test_the_receipt_is_written_once_and_read_only(tmp_path):
    out = tmp_path / "r.json"
    sha = RC.write({"schema": RC.SCHEMA, "verdict": "UNREAD"}, out)
    assert L.sha256_bytes(out.read_bytes()) == sha
    assert stat.S_IMODE(out.stat().st_mode) == 0o444
    with pytest.raises(FileExistsError):
        RC.write({"schema": RC.SCHEMA}, out)


def test_caches_are_opened_read_only(tmp_path):
    c = _cache(tmp_path / "c.sqlite3", [("GLD", "2026-01-02", "x")])
    before = c.read_bytes()
    assert RC.cache_sessions(c, ["GLD", "SLV"], "2026-01-01")["days"] == ["2026-01-02"]
    assert c.read_bytes() == before
    with pytest.raises(sqlite3.OperationalError):
        RC.cache_sessions(tmp_path / "missing.sqlite3", ["GLD"], "2026-01-01")
