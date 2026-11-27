"""Hermes stops starting pulls it cannot finish (2026-11-27).

Production shape (grid-hermes journal, 2026-11-20 to 11-27): a repair gets
60 s; a pull that takes longer and cannot be stopped overruns it, the pass
moves on, and the pull runs to its end as an unlogged copy of the scheduled
run. The overrun was recorded for the ordinary 30-minute cooldown only, never
for the long skip, so the source was started again and again:

    SEC_XBRL_Financials   85 repairs, 62 overruns, 0 recovered (a weekly pull)
    quiverquant           20 repairs, 19 overruns, 0 recovered
    SEC_INSIDER           35 repairs, 27 overruns, 0 recovered
    EIA                   40 repairs, 23 overruns, 1 recovered

Two rules, both in scripts/hermes_fixers.py:

1. An overrun counts toward the repeated-failure long skip
   (REPAIR_OVERRAN_REASON), when the source had the whole cap.
2. A source is not started at all when more than half of its completed
   scheduled runs in pull_log took longer than the cap and a repair would
   make the same call with no way to stop or narrow it
   (_slow_repair_reason). Read from pull_log, so a restart cannot forget it.

The second rule is decided from the runs, not from the registry's
``timeout_s``: 53 of the 85 pulls that cannot be stopped are allowed more
than 60 s, and most of them finish in seconds and are repaired successfully
(GoogleTrends eight times, Social_Smart_Money six times in the same days).

No case reads the real clock for a date: the failure time and the run counts
are fixed data, the timed cases use monotonic intervals, and the PostgreSQL
cases write and read relative to the database's own NOW().

Real here: the registry, the handler resolution, the puller classes' own
signatures, diagnose_and_fix_pulls, and (``real_types`` cases) the pull_log
query on PostgreSQL. Replaced: the provider calls, the failed-pull query's
rows and the operator-issue writer.
"""
from __future__ import annotations

import os
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from scripts import hermes_fixers as hf
from scripts.hermes_health import OperatorState

_REQUIRE_ENV = "REQUIRE_HERMES_SLOW_SOURCE_PG"
FAILED_AT = datetime(2026, 11, 27, 13, 28, tzinfo=timezone.utc)

# (runs over 60 s, completed runs) per registry name, as pull_log on the
# production server answered the rule's own query on 2026-11-27 (30 days).
RUNS_20261127: dict[str, tuple[int, int]] = {
    "sec_xbrl_shares": (53, 53), "options": (221, 221), "social_attention": (47, 47),
    "sec_xbrl_financials": (7, 11), "wikidata_persons": (3, 3), "earnings_calendar": (21, 21),
    "finra_dix": (9, 15), "unusual_whales": (36, 36), "alphavantage_sentiment": (29, 29),
    "social_ticker_series": (11, 18), "etf_flows": (25, 25), "edgar": (35, 35),
    "quiverquant": (38, 38), "tiingo_fundamentals": (8, 8), "foia_cables": (4, 4),
    "tiingo_news": (94, 94), "insider_filings": (25, 47), "social_sentiment": (27, 52),
    "cftc_cot": (2, 4), "finra_ats": (0, 4), "fred": (14, 56), "gdelt": (10, 60),
    "ecb": (8, 29), "googletrends": (0, 35), "smart_money": (3, 832), "eia": (0, 29),
    "news_scraper": (0, 119), "yfinance": (0, 139), "tiingo": (1, 115),
}


_NEVER_READ = {"runs": {}, "good_at": None, "attempt_at": None, "failed_reads": 0}


def _forget_the_read() -> None:
    """What a restart does to the run times: the cache is gone, pull_log is not."""
    hf._SCHEDULED_RUNS.update(_NEVER_READ)


@pytest.fixture()
def clock(monkeypatch):
    """hermes_fixers' monotonic clock, moved by hand: clock.now += seconds."""
    fake = SimpleNamespace(now=10_000.0)
    monkeypatch.setattr(hf, "time", SimpleNamespace(monotonic=lambda: fake.now))
    return fake


@pytest.fixture(autouse=True)
def _fresh_schedule(monkeypatch):
    monkeypatch.setattr(hf, "_REPAIR_LAST_STARTED", {})
    monkeypatch.setattr(hf, "_DETERMINISTIC_SKIPS", {})
    monkeypatch.setattr(hf, "_DIAGNOSIS_CACHE", {})
    monkeypatch.setattr(hf, "_THROTTLED_WARNINGS", {})
    monkeypatch.setattr(hf, "_REPEATED_FAILURES", {})
    monkeypatch.setattr(hf, "_REPAIR_RECOVERED_AT", {})
    monkeypatch.setattr(hf, "_REPAIRS_IN_FLIGHT", {})
    monkeypatch.setattr(hf, "_UNROUTED_FAILURES_REPORTED", {})
    monkeypatch.setattr(hf, "_SCHEDULED_RUNS", dict(_NEVER_READ))
    monkeypatch.setattr(hf, "_relation_exists", lambda *_a: True)
    monkeypatch.setattr(hf, "log_issue", MagicMock())
    monkeypatch.setattr(hf, "resolve_source_issues", MagicMock(return_value=0))
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: pytest.fail("LLM not expected"))
    yield


def _failed(sources: list[str], at: datetime = FAILED_AT) -> Any:
    """An engine whose failed-pull query answers these sources."""
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [
        (name, 3, at, "synthetic failure") for name in sources]
    return engine


def _measured(monkeypatch, runs: dict[str, tuple[int, int]]) -> list[int]:
    """pull_log answers ``runs``; returns a list that grows by one per read."""
    reads: list[int] = []

    def _read(_engine):
        reads.append(1)
        return dict(runs)

    monkeypatch.setattr(hf, "_read_scheduled_runs", _read)
    return reads


# ── 1. An overrun counts toward the long skip ──────────────────────────────


def _blocking_retry(monkeypatch, release: threading.Event) -> list[str]:
    calls: list[str] = []

    def retry(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        release.wait(5)                           # a provider call nothing can stop
        return {"status": "abandoned", "reason": "deadline_passed"}

    monkeypatch.setattr(hf, "_retry_source", retry)
    return calls


def test_three_overruns_in_a_row_put_the_source_on_the_long_skip(monkeypatch):
    monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 3.0)
    monkeypatch.setattr(hf, "PER_SOURCE_REPAIR_SECONDS", 0.15)
    release = threading.Event()
    calls = _blocking_retry(monkeypatch, release)
    state = OperatorState()
    try:
        for expected_blocked in (False, False, True, True):
            state.cooldowns = OperatorState().cooldowns     # past the ordinary 30-minute cooldown
            result = hf.diagnose_and_fix_pulls(_failed(["SYNTH_SLOW"]), False, state)
            assert (hf.repair_deterministically_blocked("SYNTH_SLOW") is not None) is expected_blocked
    finally:
        release.set()

    assert calls == ["SYNTH_SLOW"] * 3, "the fourth pass does not start it again"
    assert result["skipped_deterministic"] == 1 and result.get("per_source_timeouts") == 0
    until, why = hf._DETERMINISTIC_SKIPS["synth_slow"]
    assert until - time.monotonic() > 23 * 3600
    assert "same failure 3x in a row" in why and hf.REPAIR_OVERRAN_REASON in why


def test_the_overrun_reason_is_the_same_failure_whatever_the_cap(monkeypatch):
    """No digits and no source name: note_repair_failure compares it as written."""
    assert not any(ch.isdigit() for ch in hf.REPAIR_OVERRAN_REASON)
    assert not hf.note_repair_failure("S", hf.REPAIR_OVERRAN_REASON)
    assert not hf.note_repair_failure("S", hf.REPAIR_OVERRAN_REASON)
    assert hf.note_repair_failure("S", hf.REPAIR_OVERRAN_REASON)


def test_an_overrun_on_a_budget_cut_short_by_the_pass_deadline_is_not_counted(monkeypatch):
    """The second source gets what is left of the pass, less than the cap:
    it is cooled down as before, but that says nothing about the source."""
    monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 1.0)
    monkeypatch.setattr(hf, "PER_SOURCE_REPAIR_SECONDS", 0.6)
    release = threading.Event()
    calls = _blocking_retry(monkeypatch, release)
    state = OperatorState()
    try:
        result = hf.diagnose_and_fix_pulls(_failed(["SYNTH_A", "SYNTH_B"]), False, state)
    finally:
        release.set()

    assert calls == ["SYNTH_A", "SYNTH_B"] and result["per_source_timeouts"] == 2
    assert hf._REPEATED_FAILURES["synth_a"][:2] == [hf.REPAIR_OVERRAN_REASON, 1]
    assert "synth_b" not in hf._REPEATED_FAILURES
    assert "per-source cap" in state.cooldowns.get_status("SYNTH_B")["last_error"]


def test_a_recovery_between_overruns_restarts_the_count(monkeypatch):
    monkeypatch.setattr(hf, "REPAIR_BUDGET_SECONDS", 3.0)
    monkeypatch.setattr(hf, "PER_SOURCE_REPAIR_SECONDS", 0.15)
    release = threading.Event()
    outcomes = iter(["slow", "slow", "ok", "slow", "slow"])

    def retry(name, engine, *, attempt, state, should_continue):
        if next(outcomes) == "slow":
            release.wait(5)
            return {"status": "abandoned", "reason": "deadline_passed"}
        return {"status": "SUCCESS", "outcome": "SUCCESS", "rows_inserted": 4}

    monkeypatch.setattr(hf, "_retry_source", retry)
    state = OperatorState()
    try:
        for _ in range(5):
            state.cooldowns = OperatorState().cooldowns
            hf.diagnose_and_fix_pulls(_failed(["SYNTH_SLOW"]), False, state)
            # The failed-pull query still lists it after the recovery; the
            # recovery stamp (the one real-clock write here) is dropped so the
            # next pass does not depend on how it compares with FAILED_AT.
            hf._REPAIR_RECOVERED_AT.clear()
    finally:
        release.set()

    assert hf.repair_deterministically_blocked("SYNTH_SLOW") is None
    assert hf._REPEATED_FAILURES["synth_slow"][1] == 2


# ── 2. A source whose own runs do not fit is not started ───────────────────


@pytest.mark.parametrize("source, registry_name", [
    ("SEC_XBRL_Financials", "sec_xbrl_financials"),     # its source_catalog name
    ("sec_xbrl_financials", "sec_xbrl_financials"),
    ("quiverquant", "quiverquant"),
    ("Wikipedia_Attention", "social_attention"),        # catalog name of another registry key
    ("unusual_whales", "unusual_whales"),
])
def test_a_source_most_of_whose_runs_outlast_the_cap_is_not_repaired(monkeypatch, source, registry_name):
    _measured(monkeypatch, RUNS_20261127)
    over, completed = RUNS_20261127[registry_name]

    reason = hf._slow_repair_reason(source, MagicMock())

    assert reason is not None
    assert f"{over} of its {completed} completed scheduled runs in the last 30 days" in reason
    assert "longer than the 60s a repair may run" in reason
    assert reason.endswith("its scheduled run is its only repair")


@pytest.mark.parametrize("source, why", [
    ("GoogleTrends", "no run over the cap; repairs recover it"),
    ("Social_Smart_Money", "3 of 832 runs over the cap"),
    ("EIA", "its scheduled runs fit; its overruns are left to the overrun count"),
    ("FRED", "14 of 56, and the pull stops when asked"),
    ("sec_xbrl_shares", "every run is long, but the pull stops when asked and keeps its backlog"),
    ("YFINANCE_OPTIONS", "the same: stops when asked"),
    ("tiingo_news", "every run is long, but a repair narrows start_date to a week"),
    ("SEC_INSIDER", "a repair asks for three days, not the scheduled call"),
    ("SEC_EDGAR", "a repair runs another method than the schedule"),
    ("gdelt", "a held entry: no handler runs at all"),
    ("smart:sec_xbrl_financials", "the pull_log name has no handler"),
    ("SYNTH_NOT_A_SOURCE", "no handler"),
])
def test_everything_else_is_still_repaired(monkeypatch, source, why):
    _measured(monkeypatch, {**RUNS_20261127, "gdelt": (60, 60)})
    assert hf._slow_repair_reason(source, MagicMock()) is None, why


def test_exactly_half_is_not_most_and_too_few_runs_are_not_evidence(monkeypatch):
    engine = MagicMock()
    _measured(monkeypatch, {"quiverquant": (5, 10)})
    assert hf._slow_repair_reason("quiverquant", engine) is None
    _forget_the_read()
    _measured(monkeypatch, {"quiverquant": (6, 10)})
    assert hf._slow_repair_reason("quiverquant", engine) is not None
    _forget_the_read()
    _measured(monkeypatch, {})                    # under SLOW_RUN_MIN_RUNS: the query returns no row
    assert hf._slow_repair_reason("quiverquant", engine) is None


@pytest.mark.parametrize("over, completed, held", [
    (7, 11, True),      # as read on 2026-11-27: four runs of under a second, seven of three minutes
    (6, 11, True),
    (5, 11, False),     # one more week of quick runs and the hold lifts by itself
    (0, 11, False),
])
def test_a_source_with_quick_runs_beside_slow_ones_is_held_only_while_most_are_slow(
    monkeypatch, over, completed, held
):
    """The majority is a screen, not proof: SEC_XBRL_Financials is bimodal."""
    _measured(monkeypatch, {"sec_xbrl_financials": (over, completed)})
    assert (hf._slow_repair_reason("SEC_XBRL_Financials", MagicMock()) is not None) is held


def test_the_sources_held_on_the_production_run_times_of_20261127(monkeypatch):
    """The whole effect of the rule on the day it was written: every scheduler
    registry name, by its own name and by its source_catalog name."""
    from ingestion.smart_scheduler import PULLER_REGISTRY, catalog_name_for

    _measured(monkeypatch, RUNS_20261127)
    engine = MagicMock()
    held = {
        entry["name"] for entry in PULLER_REGISTRY
        if hf._slow_repair_reason(entry["name"], engine) is not None
    }
    assert held == {
        "sec_xbrl_financials", "social_attention", "wikidata_persons", "unusual_whales",
        "alphavantage_sentiment", "quiverquant", "foia_cables",
        "social_ticker_series", "social_sentiment",
        # Its failures are reported under "smart:finra_dix", which no repair
        # is routed to (UNROUTED_GUEST_REASON); held by its own name only.
        "finra_dix",
    }
    by_catalog = {
        catalog_name_for(entry["name"]) for entry in PULLER_REGISTRY
        if hf._slow_repair_reason(catalog_name_for(entry["name"]), engine) is not None
    }
    assert by_catalog == {
        "sec_xbrl_financials", "Wikipedia_Attention", "wikidata_persons", "unusual_whales",
        "alphavantage_news_sentiment", "quiverquant", "foia_cables",
        # social_sentiment and social_ticker_series share this catalog row; a
        # repair of it runs social_sentiment's pull_all (27 of 52 over the cap).
        "SocialSentiment",
    }


def test_run_times_are_read_once_an_hour_and_only_for_a_candidate(monkeypatch, clock):
    reads = _measured(monkeypatch, RUNS_20261127)
    engine = MagicMock()

    assert hf._slow_repair_reason("SYNTH_NOT_A_SOURCE", engine) is None
    assert hf._slow_repair_reason("SEC_INSIDER", engine) is None
    assert reads == [], "a source that cannot be held costs no read"

    assert hf._slow_repair_reason("quiverquant", engine) is not None
    assert hf._slow_repair_reason("SEC_XBRL_Financials", engine) is not None
    assert reads == [1]

    clock.now += hf.SLOW_RUN_REFRESH_SECONDS - 1
    assert hf._slow_repair_reason("quiverquant", engine) is not None
    assert reads == [1]
    clock.now += 1
    assert hf._slow_repair_reason("quiverquant", engine) is not None
    assert reads == [1, 1]
    engine.connect.assert_not_called()            # _read_scheduled_runs is the only reader


# ── the hold does not outlive its evidence ─────────────────────────────────


def _failing_read(monkeypatch) -> MagicMock:
    read = MagicMock(side_effect=RuntimeError("pool exhausted"))
    monkeypatch.setattr(hf, "_read_scheduled_runs", read)
    return read


def _warnings(monkeypatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(hf.log, "warning", lambda template, **kw: seen.append(template.format(**kw)))
    return seen


def test_with_no_read_yet_nothing_is_held_and_a_failing_read_backs_off(monkeypatch, clock):
    engine = MagicMock()
    warnings = _warnings(monkeypatch)
    read = _failing_read(monkeypatch)

    for _pass in range(5):                                             # five passes inside the first wait
        assert hf._slow_repair_reason("quiverquant", engine) is None   # nothing known: repair as before
        clock.now += 59
    assert read.call_count == 1, "a failed read is an attempt: it is not repeated on every pass"
    assert len(warnings) == 1 and "RuntimeError" in warnings[0] and "pool exhausted" not in warnings[0]
    assert "next try in 5 min" in warnings[0]

    waits = []
    for _ in range(6):                                                 # each failure doubles the wait
        wait = hf._scheduled_run_retry_seconds(hf._SCHEDULED_RUNS["failed_reads"])
        waits.append(wait)
        clock.now = hf._SCHEDULED_RUNS["attempt_at"] + wait - 1
        before = read.call_count
        assert hf._slow_repair_reason("quiverquant", engine) is None
        assert read.call_count == before, "not before the wait is over"
        clock.now += 1
        assert hf._slow_repair_reason("quiverquant", engine) is None
        assert read.call_count == before + 1
    assert waits == [300.0, 600.0, 1200.0, 2400.0, 3600.0, 3600.0], "capped at the refresh interval"
    assert hf._scheduled_run_retry_seconds(10_000) == hf.SLOW_RUN_REFRESH_SECONDS

    _measured(monkeypatch, RUNS_20261127)                              # the read works again
    clock.now += hf.SLOW_RUN_REFRESH_SECONDS
    reason = hf._slow_repair_reason("quiverquant", engine)
    assert reason is not None and "failed since" not in reason
    assert hf._SCHEDULED_RUNS["failed_reads"] == 0


def test_a_hold_on_an_old_read_says_so_lapses_at_the_age_limit_and_returns_with_a_good_read(
    monkeypatch, clock
):
    engine = MagicMock()
    _measured(monkeypatch, RUNS_20261127)
    fresh = hf._slow_repair_reason("quiverquant", engine)
    assert fresh is not None and fresh.endswith("its scheduled run is its only repair")
    read_at = clock.now

    warnings = _warnings(monkeypatch)
    read = _failing_read(monkeypatch)
    clock.now = read_at + hf.SLOW_RUN_REFRESH_SECONDS                  # the hourly refresh fails
    stale = hf._slow_repair_reason("quiverquant", engine)
    assert stale == fresh + " (run times last read 60 min ago; 1 read(s) failed since)"
    assert hf._slow_repair_reason("GoogleTrends", engine) is None      # still decided per source

    clock.now = read_at + hf.SLOW_RUN_MAX_AGE_SECONDS                  # at the limit: still used
    at_limit = hf._slow_repair_reason("quiverquant", engine)
    assert at_limit is not None and "run times last read 360 min ago" in at_limit
    assert not any("too old" in w for w in warnings)

    clock.now += 1                                                     # past it: not used at all
    attempts = read.call_count
    assert hf._slow_repair_reason("quiverquant", engine) is None
    assert hf._slow_repair_reason("SEC_XBRL_Financials", engine) is None
    assert read.call_count <= attempts + 1, "and still not re-read on every call"
    lapsed = [w for w in warnings if "too old to decide on" in w]
    assert len(lapsed) == 1 and "last read 6.0h ago (limit 6h" in lapsed[0]
    assert "no source is held as too slow" in lapsed[0]

    _measured(monkeypatch, {**RUNS_20261127, "quiverquant": (2, 38)})  # the read returns; the source changed
    clock.now += hf.SLOW_RUN_REFRESH_SECONDS
    assert hf._slow_repair_reason("quiverquant", engine) is None       # decided on the new read, not the old
    assert hf._slow_repair_reason("SEC_XBRL_Financials", engine) == (
        "7 of its 11 completed scheduled runs in the last 30 days took longer than the 60s a repair "
        "may run, and a repair would start the same pull with no way to stop it; "
        "its scheduled run is its only repair")
    assert hf._SCHEDULED_RUNS["failed_reads"] == 0


def test_once_the_read_has_lapsed_the_pass_repairs_the_source_again(monkeypatch, clock):
    _measured(monkeypatch, RUNS_20261127)
    calls = _recording_retry(monkeypatch)
    state = OperatorState()
    held = hf.diagnose_and_fix_pulls(_failed(["quiverquant"]), False, state)
    assert held["skipped_too_slow"] == 1 and calls == []

    _failing_read(monkeypatch)
    clock.now += hf.SLOW_RUN_MAX_AGE_SECONDS + 1
    lapsed = hf.diagnose_and_fix_pulls(_failed(["quiverquant"]), False, state)

    assert calls == ["quiverquant"] and "skipped_too_slow" not in lapsed


def test_an_unimportable_puller_is_unknown_not_slow(monkeypatch):
    _measured(monkeypatch, RUNS_20261127)
    assert hf._repair_cannot_be_bounded({"mod": "no.such.module", "cls": "Nope"}) is False
    assert hf._repair_cannot_be_bounded({"mod": "ingestion.altdata.quiverquant", "cls": "Nope"}) is False


# ── the pass ───────────────────────────────────────────────────────────────


def _recording_retry(monkeypatch) -> list[str]:
    calls: list[str] = []

    def retry(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        return {"status": "SUCCESS", "outcome": "NO_NEW_DATA", "rows_inserted": 0}

    monkeypatch.setattr(hf, "_retry_source", retry)
    return calls


def test_the_pass_repairs_the_rest_and_reports_the_slow_source_once_per_failure(monkeypatch):
    _measured(monkeypatch, RUNS_20261127)
    calls = _recording_retry(monkeypatch)
    warnings: list[str] = []
    monkeypatch.setattr(hf.log, "warning", lambda template, **kw: warnings.append(template.format(**kw)))
    state = OperatorState()

    first = hf.diagnose_and_fix_pulls(_failed(["SEC_XBRL_Financials", "SYNTH_FAST"]), False, state)
    second = hf.diagnose_and_fix_pulls(_failed(["SEC_XBRL_Financials"]), False, state)

    assert calls == ["SYNTH_FAST"], "the slow source is never started"
    assert first["skipped_too_slow"] == second["skipped_too_slow"] == 1
    assert first["retried"] == 1 and second["retried"] == 0
    assert state.cooldowns.get_status("SEC_XBRL_Financials") is None, "nothing ran: no cooldown outcome"

    issues = [c.kwargs for c in hf.log_issue.call_args_list
              if c.kwargs.get("source") == "SEC_XBRL_Financials"]
    assert len(issues) == 1, "one operator issue per failure, not one per pass"
    assert issues[0]["title"] == "Scheduled run failed, no Hermes repair — SEC_XBRL_Financials"
    assert issues[0]["severity"] == "WARNING" and issues[0]["fix_result"] == "SKIPPED"
    assert "7 of its 11 completed scheduled runs" in issues[0]["detail"]
    assert "synthetic failure" in issues[0]["detail"]
    told = [w for w in warnings if w.startswith("Not repairing SEC_XBRL_Financials")]
    assert len(told) == 1 and "no retry handler" not in told[0]
    # It is not in the list handed on to diagnosis and repair.
    assert not any("SEC_XBRL_Financials" in w for w in warnings if w.startswith("Failed sources"))

    later = hf.diagnose_and_fix_pulls(
        _failed(["SEC_XBRL_Financials"], at=FAILED_AT + timedelta(days=7)), False, state)
    assert later["skipped_too_slow"] == 1
    assert len([c for c in hf.log_issue.call_args_list
                if c.kwargs.get("source") == "SEC_XBRL_Financials"]) == 2, "a new failure is reported"


def _issues(source: str) -> list[dict]:
    return [c.kwargs for c in hf.log_issue.call_args_list if c.kwargs.get("source") == source]


def test_a_failure_already_recovered_is_not_reported_and_the_next_failure_is_reported_once(monkeypatch):
    """fail -> recovered -> new failure. A stale refresh (not capped at 60 s)
    can complete a held source; that is recorded like any repair recovery."""
    _measured(monkeypatch, RUNS_20261127)
    calls = _recording_retry(monkeypatch)
    warnings = _warnings(monkeypatch)
    state = OperatorState()
    source = "SEC_XBRL_Financials"
    hf._REPAIR_RECOVERED_AT[hf._normalize_source_key(source)] = FAILED_AT + timedelta(hours=1)

    recovered = hf.diagnose_and_fix_pulls(_failed([source]), False, state)

    assert "skipped_too_slow" not in recovered and calls == []
    assert _issues(source) == [], "a failure that has already recovered is not an open failure"
    assert not any(w.startswith(f"Not repairing {source}") for w in warnings)

    again = FAILED_AT + timedelta(hours=2)                              # it fails again, after the recovery
    for _pass in range(3):
        result = hf.diagnose_and_fix_pulls(_failed([source], at=again), False, state)
        assert result["skipped_too_slow"] == 1
    assert len(_issues(source)) == 1, "the new failure is reported once, not once per pass"
    assert again.isoformat() in _issues(source)[0]["detail"]
    assert calls == []


def test_a_dry_run_reports_without_writing_an_issue(monkeypatch):
    _measured(monkeypatch, RUNS_20261127)
    calls = _recording_retry(monkeypatch)

    result = hf.diagnose_and_fix_pulls(_failed(["quiverquant"]), False, OperatorState(), dry_run=True)

    assert result["skipped_too_slow"] == 1 and calls == []
    hf.log_issue.assert_not_called()
    assert hf._UNROUTED_FAILURES_REPORTED == {}


def test_without_run_times_the_pass_repairs_the_source_as_before(monkeypatch):
    monkeypatch.setattr(hf, "_read_scheduled_runs", MagicMock(side_effect=RuntimeError("down")))
    calls = _recording_retry(monkeypatch)

    result = hf.diagnose_and_fix_pulls(_failed(["quiverquant"]), False, OperatorState())

    assert calls == ["quiverquant"] and "skipped_too_slow" not in result


def test_a_guest_run_keeps_its_own_report(monkeypatch):
    """The reporter's other caller is unchanged: no reason of its own, the old title."""
    hf._report_unrouted_failures(
        MagicMock(), [{"source": "smart:finra_dix", "last_fail": "2026-11-27T07:53:00+00:00",
                       "last_error": "boom"}], OperatorState(), False)
    issue = hf.log_issue.call_args.kwargs
    assert issue["title"] == "No Hermes pull handler — smart:finra_dix"
    assert hf.UNROUTED_GUEST_REASON in issue["detail"]


# ── PostgreSQL: the run-time query on TIMESTAMPTZ ──────────────────────────

_PULL_LOG_DDL = """
CREATE TABLE pull_log (
    id              SERIAL PRIMARY KEY,
    puller_name     TEXT NOT NULL,
    source_id       INTEGER,
    started_at      TIMESTAMPTZ NOT NULL,
    completed_at    TIMESTAMPTZ,
    status          TEXT NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    rows_inserted   INTEGER DEFAULT 0,
    rows_expected   INTEGER,
    error_message   TEXT,
    features_affected INTEGER[],
    node_name       TEXT DEFAULT 'test'
)
"""


@pytest.fixture()
def pg_engine(monkeypatch):
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the Hermes slow-source PG contract ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the Hermes slow-source PG contract requires a local disposable test database")

    from scripts import hermes_health

    monkeypatch.setattr(hf, "_relation_exists", hermes_health._relation_exists)   # the real probe
    schema = "hslow_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _run(engine, name: str, seconds: float | None, status: str = "SUCCESS", days_ago: float = 1.0,
         source_id: int | None = None) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO pull_log (puller_name, source_id, started_at, completed_at, status, error_message) VALUES "
            "(:n, :sid, NOW() - make_interval(secs => :ago), "
            " CASE WHEN CAST(:s AS double precision) IS NULL THEN NULL "
            "      ELSE NOW() - make_interval(secs => :ago) + make_interval(secs => CAST(:s AS double precision)) END, "
            " :st, CASE WHEN :st = 'FAILED' THEN 'synthetic failure' END)"),
            {"n": name, "sid": source_id, "ago": days_ago * 86400.0, "s": seconds, "st": status})


def test_real_types_counts_completed_scheduler_runs_over_the_cap(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_PULL_LOG_DDL))
    for seconds in (0.4, 189.6, 224.7, 60.0, 60.5):          # 60.0 is not over the cap
        _run(pg_engine, "smart:sec_xbrl_financials", seconds, status="PARTIAL" if seconds < 1 else "SUCCESS")
    _run(pg_engine, "smart:sec_xbrl_financials", 1800.0, status="FAILED")       # a timeout: left out
    _run(pg_engine, "smart:sec_xbrl_financials", None, status="RUNNING")        # not completed
    _run(pg_engine, "smart:sec_xbrl_financials", 500.0, days_ago=31.0)          # older than the window
    _run(pg_engine, "SEC_XBRL_Financials", 250.0)                               # another scheduler's run
    for _ in range(3):
        _run(pg_engine, "SEC_XBRL_Financials", 250.0)
        _run(pg_engine, "smart:googletrends", 19.4)
        _run(pg_engine, "smart:not_a_registry_name", 400.0)
    for _ in range(2):                                                          # under SLOW_RUN_MIN_RUNS
        _run(pg_engine, "smart:quiverquant", 84.5)

    assert hf._read_scheduled_runs(pg_engine) == {
        "sec_xbrl_financials": (3, 5),
        "googletrends": (0, 3),
    }


def test_real_types_the_rule_end_to_end_on_stored_runs(pg_engine):
    with pg_engine.begin() as conn:
        conn.execute(text(_PULL_LOG_DDL))
    for seconds in (0.3, 0.4, 189.6, 201.0, 224.7):
        _run(pg_engine, "smart:sec_xbrl_financials", seconds)
    for seconds in (55.0, 59.0, 61.0, 20.0):
        _run(pg_engine, "smart:quiverquant", seconds)

    reason = hf._slow_repair_reason("SEC_XBRL_Financials", pg_engine)
    assert reason is not None and "3 of its 5 completed scheduled runs" in reason
    assert hf._slow_repair_reason("quiverquant", pg_engine) is None             # 1 of 4

    # A restart forgets the cache and nothing else: the first read restores the hold.
    _forget_the_read()
    assert hf._slow_repair_reason("SEC_XBRL_Financials", pg_engine) == reason


def test_real_types_no_pull_log_means_no_hold(pg_engine):
    assert hf._read_scheduled_runs(pg_engine) == {}
    assert hf._slow_repair_reason("SEC_XBRL_Financials", pg_engine) is None


_CATALOG_DDL = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT, source_id INTEGER NOT NULL, "
    "pull_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(), raw_payload JSONB, pull_status TEXT NOT NULL)",
)


def test_real_types_a_failure_followed_by_a_good_run_is_not_reported_and_the_next_one_is(pg_engine, monkeypatch):
    """The whole pass on PostgreSQL: the real failed-pull query, the real
    run-time query and the rule. fail -> good run -> fail again."""
    with pg_engine.begin() as conn:
        conn.execute(text(_PULL_LOG_DDL))
        for ddl in _CATALOG_DDL:
            conn.execute(text(ddl))
        conn.execute(text("INSERT INTO source_catalog (id, name) VALUES (1, 'quiverquant')"))
    calls = _recording_retry(monkeypatch)
    state = OperatorState()
    hour = 1 / 24

    def run(seconds, status, hours_ago):
        _run(pg_engine, "smart:quiverquant", seconds, status=status, days_ago=hours_ago * hour, source_id=1)

    for hours_ago in (60, 48, 36):
        run(84.5, "SUCCESS", hours_ago)                                 # its ordinary runs: every one over the cap
    run(120.0, "FAILED", 3)

    first = hf.diagnose_and_fix_pulls(pg_engine, False, state)
    same = hf.diagnose_and_fix_pulls(pg_engine, False, state)
    assert first["skipped_too_slow"] == same["skipped_too_slow"] == 1
    assert len(_issues("quiverquant")) == 1
    assert "3 of its 3 completed scheduled runs" in _issues("quiverquant")[0]["detail"]

    run(88.0, "SUCCESS", 2)                                             # its scheduler's own rerun succeeds
    recovered = hf.diagnose_and_fix_pulls(pg_engine, False, state)
    assert "skipped_too_slow" not in recovered
    assert len(_issues("quiverquant")) == 1, "a failure followed by a good run is not reported again"

    run(120.0, "FAILED", 1)                                             # and it fails again
    again = hf.diagnose_and_fix_pulls(pg_engine, False, state)
    hf.diagnose_and_fix_pulls(pg_engine, False, state)
    assert again["skipped_too_slow"] == 1
    assert len(_issues("quiverquant")) == 2, "the new failure is reported, once"
    assert calls == [], "no repair was started at any point"
