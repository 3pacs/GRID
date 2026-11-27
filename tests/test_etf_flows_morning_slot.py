"""The ETF flow proxies get a slot after New York midnight.

The ETF flow writer stores a session's bar only once its date is before the
current New York date (GRID #1496). The daily group runs ``ETF_Flows`` at
20:00Z, always the same New York date as the session, so on its own it stores
each session one weekday later, and a Friday on Monday evening. The morning
slot runs the same puller alone, after New York midnight in both EDT and EST.

Everything here is a local fake: no network and no database.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import ingestion.scheduler as sched

NEW_YORK = ZoneInfo("America/New_York")


def _slot(day: date) -> datetime:
    hh, mm = (int(part) for part in sched.ETF_FLOWS_MORNING_SLOT_UTC.split(":"))
    return datetime(day.year, day.month, day.day, hh, mm, tzinfo=timezone.utc)


@pytest.fixture()
def group_runs(monkeypatch):
    runs: list[tuple[str, object]] = []

    def fake_run_pull_group(group_name, db_engine, *_a, **_k):
        runs.append((group_name, db_engine))
        return {"group": group_name, "results": []}

    monkeypatch.setattr(sched, "run_pull_group", fake_run_pull_group)
    return runs


# ── Where the slot sits ────────────────────────────────────────────────────


@pytest.mark.parametrize("day", [date(2026, 7, 15), date(2026, 12, 15), date(2026, 11, 2), date(2027, 3, 15)],
                         ids=["summer", "winter", "day_after_the_autumn_change", "day_after_the_spring_change"])
def test_at_the_slot_new_yorks_date_has_already_moved_on(day):
    """So the previous session's bar is eligible to the writer."""
    assert _slot(day).astimezone(NEW_YORK).date() == day


def test_the_slot_is_after_the_yfinance_morning_slot_and_before_the_long_monthly_group():
    assert sched.YFINANCE_MORNING_SLOT_UTC < sched.ETF_FLOWS_MORNING_SLOT_UTC < sched.EXTENDED_MONTHLY_SLOT_UTC


def test_the_writers_rule_admits_the_previous_session_at_the_slot(monkeypatch):
    """The slot and the writer agree: on Saturday at the slot, Friday's bar is
    before the New York date the writer compares against."""
    from ingestion.altdata import institutional_flows as inst

    saturday = date(2026, 11, 21)
    monkeypatch.setattr(inst, "datetime", type("Clock", (), {
        "now": staticmethod(lambda tz=None: _slot(saturday).astimezone(tz))}))
    assert inst._new_york_today() == saturday
    assert inst._bar_date(date(2026, 11, 20)) < inst._new_york_today()


# ── When it runs ───────────────────────────────────────────────────────────


@pytest.mark.parametrize("day, label", [
    (date(2026, 10, 17), "Saturday stores Friday"),
    (date(2026, 10, 20), "Tuesday stores Monday"),
    (date(2026, 9, 9), "Wednesday: the post-Labor Day Tuesday was a session"),
    (date(2026, 11, 28), "Saturday: the half session after Thanksgiving was a session"),
])
def test_it_runs_the_etf_flows_group_when_the_previous_day_was_a_session(group_runs, day, label):
    engine = object()

    summary = sched.run_etf_flows_morning_slot(engine, now_utc=_slot(day))

    assert group_runs == [(sched.ETF_FLOWS_GROUP, engine)], label
    assert summary == {"group": sched.ETF_FLOWS_GROUP, "results": []}


@pytest.mark.parametrize("day, label", [
    (date(2026, 10, 18), "Sunday: Saturday was no session"),
    (date(2026, 10, 19), "Monday: Sunday was no session"),
    (date(2026, 9, 8), "Tuesday after the Labor Day holiday Monday"),
    (date(2026, 11, 27), "Friday after the Thanksgiving holiday Thursday"),
])
def test_it_skips_when_the_previous_day_was_not_a_session(group_runs, day, label):
    assert sched.run_etf_flows_morning_slot(object(), now_utc=_slot(day)) is None
    assert group_runs == [], label


def test_it_skips_when_released_too_late_by_an_earlier_job(group_runs):
    late = _slot(date(2026, 10, 17)) + timedelta(minutes=sched.DOMESTIC_SLOT_MAX_LATE_MINUTES + 1)

    assert sched.run_etf_flows_morning_slot(object(), now_utc=late) is None
    assert group_runs == []


def test_a_little_late_still_runs(group_runs):
    sched.run_etf_flows_morning_slot(object(), now_utc=_slot(date(2026, 10, 17)) + timedelta(minutes=20))

    assert len(group_runs) == 1


# ── What the group is ──────────────────────────────────────────────────────


def test_the_group_is_the_etf_flows_puller_alone_under_its_daily_name(monkeypatch):
    from ingestion.altdata import institutional_flows as inst

    class FakePuller:
        def __init__(self, db_engine):
            self.db_engine = db_engine

    monkeypatch.setattr(inst, "InstitutionalFlowsPuller", FakePuller)
    engine = object()

    pullers = sched._get_pullers_for_group(sched.ETF_FLOWS_GROUP, engine, {})

    ((name, instance, method, kwargs),) = pullers
    # One pull_log history with the daily run; the short pull, not the one from 2020.
    assert (name, method, kwargs) == ("ETF_Flows", "pull_etf_recent", {})
    assert isinstance(instance, FakePuller) and instance.db_engine is engine

    ((_, _, _, kwargs),) = sched._get_pullers_for_group(
        sched.ETF_FLOWS_GROUP, engine, {sched._ETF_FLOWS_DEADLINE_KEY: 1234.5})
    assert kwargs == {"deadline_at": 1234.5}


# ── How long it may hold the scheduler's one thread ────────────────────────
#
# Codex review 6085724628: the slot ran pull_etf_only() with no deadline, and
# the late-start guard bounds when it starts, not how long it runs.


def test_the_slot_gives_the_pull_one_deadline_that_a_retry_does_not_renew(monkeypatch):
    clock = {"now": 5000.0}
    monkeypatch.setattr(sched.time, "monotonic", lambda: clock["now"])
    monkeypatch.setattr(sched.time, "sleep", lambda seconds: clock.__setitem__("now", clock["now"] + seconds))
    monkeypatch.setattr(sched, "_slot_lateness", lambda *_a, **_k: timedelta(0))
    seen: list[dict] = []

    def _group_that_loses_its_connection_once(group_name, db_engine, config=None, *_a, **_k):
        seen.append(dict(config))
        clock["now"] += 40.0
        if len(seen) == 1:
            raise RuntimeError("server closed the connection unexpectedly")
        return {"group": group_name}

    monkeypatch.setattr(sched, "run_pull_group", _group_that_loses_its_connection_once)

    sched.run_etf_flows_morning_slot(object(), now_utc=_slot(date(2026, 10, 17)))

    deadline = 5000.0 + sched.ETF_FLOWS_MORNING_BUDGET_S
    assert seen == [{sched._ETF_FLOWS_DEADLINE_KEY: deadline}] * 2          # the retry kept the first deadline
    assert sched.ETF_FLOWS_MORNING_BUDGET_S == 300


def test_the_group_hands_that_deadline_to_the_real_short_pull(monkeypatch):
    """Through the real run_pull_group call shape: the deadline reaches
    ``pull_etf_recent`` as ``deadline_at``, which starts no ETF after it."""
    import inspect

    from ingestion.altdata import institutional_flows as inst

    accepted = inspect.signature(inst.InstitutionalFlowsPuller.pull_etf_recent).parameters
    assert {"should_continue", "deadline_at"} <= set(accepted)

    called: list[dict] = []

    class RecordingPuller:
        def __init__(self, db_engine):
            pass

        def pull_etf_recent(self, **kwargs):
            called.append(kwargs)
            return []

    monkeypatch.setattr(inst, "InstitutionalFlowsPuller", RecordingPuller)
    ((_, instance, method, kwargs),) = sched._get_pullers_for_group(
        sched.ETF_FLOWS_GROUP, object(), {sched._ETF_FLOWS_DEADLINE_KEY: 77.0})
    getattr(instance, method)(**kwargs)
    assert called == [{"deadline_at": 77.0}]


def test_the_daily_group_still_runs_etf_flows():
    """The 20:00Z run stays: it revises recent dates and is the fallback."""
    source = open(sched.__file__).read()
    daily = source[source.index('    elif group_name == "daily":'):source.index("    elif group_name == ETF_FLOWS_GROUP:")]
    assert '("ETF_Flows", InstitutionalFlowsPuller(db_engine), "pull_etf_only", {})' in daily


def test_the_slot_is_registered_every_day_on_the_utc_clock():
    source = open(sched.__file__).read()
    assert (
        'schedule.every().day.at(ETF_FLOWS_MORNING_SLOT_UTC, "UTC").do(\n'
        "            run_etf_flows_morning_slot, ext_engine, slot=ETF_FLOWS_MORNING_SLOT_UTC,"
    ) in source
