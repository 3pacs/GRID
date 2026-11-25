"""#1191's as-of join acceptance (the 2026-11-16 addendum), on GRID's reader of ``market_state_daily``.

A consumer joins the daily market-state context by when a row was actually written (``computed_at``), never by
the session it describes: a row becomes usable for a decision only once it exists. The reader under test is the
desk brief's market-state section (``analysis/desk_brief_sources.market_state``), today the only reader of the
table outside the writer and its own spells fold. Rows are written with the production DDL on SQLite.

Session times come from the pinned XNYS calendar: 2026-11-18, -19 and -20 and 2026-11-23 open at 14:30Z and
close at 21:00Z. The writer stores a session's decision record the next weekday evening (about 23:15Z), so the
timely row in these tests is a hypothetical one written between a close and the next open. The record that meets
#1191's timing clause in production is the provisional one (owner's ruling, 2026-11-24), and it is display only:
no deciding reader reads ``market_state_provisional``. Revised rows keep their original version in
``market_state_daily_revisions``; here those archive rows are written by hand, as the Postgres trigger writes them
(``tests/test_market_state_revisions.py`` proves the trigger on real Postgres).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime

from sqlalchemy import create_engine, text

from analysis import desk_brief as B
from analysis import desk_brief_sources as S
from intelligence.regime import market_state as MS

FRI = B.SessionTimes(date(2026, 11, 20), datetime(2026, 11, 20, 14, 30, tzinfo=UTC),
                     datetime(2026, 11, 20, 21, 0, tzinfo=UTC))
MON = B.SessionTimes(date(2026, 11, 23), datetime(2026, 11, 23, 14, 30, tzinfo=UTC),
                     datetime(2026, 11, 23, 21, 0, tzinfo=UTC))


def z(raw: str) -> datetime:
    return datetime.fromisoformat(raw.replace("Z", "+00:00"))


def _record(label: str) -> dict:
    return {"variables": {"vix": {"status": "ok", "label": label, "robust_z": 2.5, "obs_date": "x"}},
            "coverage": {"ok": 1}}


def _engine(tmp_path, rows: dict[str, tuple[str, str]]):
    """``rows``: session -> (vix label, computed_at as stored, naive UTC)."""
    eng = create_engine(f"sqlite:///{tmp_path / 'ms.db'}")
    MS.ensure_table(eng)
    with eng.begin() as c:
        for day, (label, computed) in rows.items():
            c.execute(text("INSERT INTO market_state_daily (as_of_date, rule, record, computed_at) "
                           "VALUES (:d, 'market-state/3', :r, :c)"),
                      {"d": day, "r": json.dumps(_record(label)), "c": computed})
    return eng


def _vix(section) -> str:
    return next(line for line in section.lines if line.startswith("away from normal"))


# The writer's real cadence for 11-18 and 11-19, plus a timely row for 11-20 written after its close.
CADENCE = {"2026-11-18": ("normal", "2026-11-19 23:15:00"),
           "2026-11-19": ("high", "2026-11-20 23:15:00"),
           "2026-11-20": ("low", "2026-11-20 21:05:00")}


def test_a_same_day_pre_close_decision_refuses_every_row_written_after_it(tmp_path):
    """A decision at 18:00Z on 11-20, during the session: 11-20's row (written 21:05Z, after the close) and 11-19's
    row (written 23:15Z that evening) do not exist yet, so the newest usable record is 11-18's."""
    eng = _engine(tmp_path, CADENCE)
    sec = S.market_state(eng, "post_close", FRI, z("2026-11-20T18:00:00Z"))
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-18"
    assert sec.facts["computed_at"].startswith("2026-11-19T23:15:00")
    assert S.market_state(eng, "post_close", FRI, z("2026-11-20T20:59:59Z")).facts["record_date"] == "2026-11-18"
    assert S.market_state(eng, "post_close", FRI, z("2026-11-20T21:04:59Z")).facts["record_date"] == "2026-11-18"
    assert S.market_state(eng, "post_close", FRI, z("2026-11-20T21:05:00Z")).facts["record_date"] == "2026-11-20"


def test_the_registered_pre_open_clock_never_sees_the_previous_sessions_post_close_row(tmp_path):
    """The pre-open brief of 11-20 (09:00 ET): 11-19's row lands that evening, so it is refused."""
    eng = _engine(tmp_path, CADENCE)
    sec = S.market_state(eng, "pre_open", FRI, B.default_as_of("pre_open", FRI))
    assert sec.facts["record_date"] == "2026-11-18" and _vix(sec) == "away from normal: none"


def test_a_next_session_decision_accepts_a_timely_row(tmp_path):
    """11-20's row was written after its close and before the next open, so Monday's pre-open decision uses it."""
    eng = _engine(tmp_path, CADENCE)
    sec = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON))
    assert sec.status == "ok" and sec.facts["record_date"] == "2026-11-20"
    assert "vix low" in _vix(sec) and sec.problems == ()
    assert sec.facts["computed_at"].startswith("2026-11-20T21:05:00")


def test_a_late_row_is_refused_until_it_was_written_and_the_gap_is_reported(tmp_path):
    """11-20's row backfilled late (11-24 02:00Z): Monday's pre-open decision falls back to 11-19 and says the
    record it would normally have is missing; a decision after the backfill uses it."""
    late = {**CADENCE, "2026-11-20": ("low", "2026-11-24 02:00:00")}
    eng = _engine(tmp_path, late)
    sec = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON), date(2026, 11, 20))
    assert sec.facts["record_date"] == "2026-11-19"
    assert sec.problems == ("latest record is for 2026-11-19; the writer normally has 2026-11-20 by now",)
    after = S.market_state(eng, "post_close", MON, z("2026-11-24T03:00:00Z"))
    assert after.facts["record_date"] == "2026-11-20"


def _revise(eng, archive: bool) -> None:
    """11-20's row replaced on 11-25 at 04:00Z. ``archive``: also the revision Postgres's trigger writes in the same
    transaction (tests/test_market_state_revisions.py proves it does); SQLite has no trigger."""
    with eng.begin() as c:
        if archive:
            c.execute(text("INSERT INTO market_state_daily_revisions (as_of_date, rule, record, computed_at, "
                           "computed_at_by_trigger, superseded_at, superseded_by_xid, operation, reason, run_tag, "
                           "attributed) SELECT as_of_date, rule, record, computed_at, TRUE, '2026-11-25 04:00:00', "
                           "'1', 'UPDATE', 'test', 'test-run', TRUE FROM market_state_daily "
                           "WHERE as_of_date = '2026-11-20'"))
        c.execute(text("UPDATE market_state_daily SET record = :r, computed_at = '2026-11-25 04:00:00' "
                       "WHERE as_of_date = '2026-11-20'"), {"r": json.dumps(_record("high"))})


def test_a_revised_row_is_shown_as_it_stood_before_its_revision(tmp_path):
    """A replace rewrites the row and its computed_at in place, and keeps the superseded version
    (market_state_daily_revisions, #1191). A replay between the original write and the revision sees the original;
    after the revision, the revised row."""
    eng = _engine(tmp_path, CADENCE)
    before = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON))
    assert before.facts["record_date"] == "2026-11-20" and "vix low" in _vix(before)
    _revise(eng, archive=True)
    replayed = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON))
    assert replayed.status == "ok" and replayed.facts["record_date"] == "2026-11-20"
    assert "vix low" in _vix(replayed)                       # the original, not the revision
    assert replayed.facts["computed_at"].startswith("2026-11-20T21:05:00")
    assert replayed.facts["superseded_at"].startswith("2026-11-25T04:00:00")
    assert "replaced at 2026-11-25T04:00:00" in replayed.lines[0] and replayed.problems == ()
    assert "market_state_daily_revisions" in replayed.source
    at_revision = S.market_state(eng, "post_close", MON, z("2026-11-25T04:00:00Z"))
    assert "vix high" in _vix(at_revision) and "superseded_at" not in at_revision.facts
    now = S.market_state(eng, "post_close", MON, z("2026-11-25T05:00:00Z"))
    assert now.facts["record_date"] == "2026-11-20" and "vix high" in _vix(now)
    assert "superseded_at" not in now.facts


def test_without_the_archived_version_a_replay_falls_back_and_never_shows_the_revision_early(tmp_path):
    """A database whose replace kept nothing (before the archive, or SQLite): the replay shows 11-19's record, never
    the revised 11-20 one."""
    eng = _engine(tmp_path, CADENCE)
    _revise(eng, archive=False)
    replayed = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON))
    assert replayed.facts["record_date"] == "2026-11-19" and "vix high" in _vix(replayed)  # 11-19's own high


def test_a_version_that_stood_at_the_clock_never_hides_a_newer_session(tmp_path):
    """11-19's row was replaced after the clock, but 11-20's timely row stood at it: the newer session wins."""
    eng = _engine(tmp_path, CADENCE)
    with eng.begin() as c:
        c.execute(text("INSERT INTO market_state_daily_revisions (as_of_date, rule, record, computed_at, "
                       "computed_at_by_trigger, superseded_at, superseded_by_xid, operation, reason, run_tag, "
                       "attributed) SELECT as_of_date, rule, record, computed_at, TRUE, '2026-11-25 04:00:00', '1', "
                       "'UPDATE', 'test', 'test-run', TRUE FROM market_state_daily WHERE as_of_date = '2026-11-19'"))
        c.execute(text("UPDATE market_state_daily SET computed_at = '2026-11-25 04:00:00' "
                       "WHERE as_of_date = '2026-11-19'"))
    sec = S.market_state(eng, "pre_open", MON, B.default_as_of("pre_open", MON))
    assert sec.facts["record_date"] == "2026-11-20" and "superseded_at" not in sec.facts


def test_the_writer_replace_moves_computed_at_so_the_join_sees_the_revision_time(tmp_path):
    """The production writer path, not a hand-written UPDATE: a replace stamps computed_at with the database clock."""
    eng = _engine(tmp_path, CADENCE)
    state = MS.MarketState(date(2026, 11, 20))
    state.variables = _record("high")["variables"]
    assert MS.store_market_state(eng, state, replace=MS.MarketStateReplacement("test: recompute", "test-run"))
    with eng.connect() as c:
        stamped = c.execute(text("SELECT computed_at FROM market_state_daily WHERE as_of_date = '2026-11-20'")).scalar_one()
    assert str(stamped) > "2026-11-20 21:05:00"
