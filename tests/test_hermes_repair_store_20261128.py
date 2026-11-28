"""Hermes's repair bookkeeping survives a restart (2026-11-28).

Before: the repeated-failure count, the long skip it trips, the time a repair
recovered a source and the per-source cooldown lived in process memory only.
grid-hermes started 15 times on 2026-11-27, and the sources whose long skip
had tripped (TIINGO, polygon, Asset_Registries, yfinance_options) were
repaired again after every start.

Now each counted failure, long skip, recovery and operator clear is a row in
``hermes_repair_events`` (scripts/hermes_repair_store.py), and the first pass
after a start replays them by the live rules. These cases pin:

- replay leaves exactly the tables the live calls left, for a recorded
  sequence of the live calls themselves (equivalence, not a parallel model);
- after a restart a long skip in force, a count short of the threshold, a
  recovery and a cooldown all hold, and a skip that has ended does not;
- an operator clear lets a running Hermes retry on its next pass, once;
- without the table, or when a read or write fails, Hermes behaves as before
  (in memory), says so once, and looks again on the next pass;
- skips for code defects, missing keys and missing handlers are not kept, so
  a deploy or .env fix and its restart still retry;
- on PostgreSQL: the real migration, its constraints, the store round trip,
  and the whole restart on real rows.

Replaced: provider calls (``_retry_source``), the failed-pull query's rows and
the operator-issue writer, as in the other repair tests. Every case uses its
own fixed failure time; times the code stamps come from the clock it reads.
"""
from __future__ import annotations

import importlib
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from scripts import hermes_fixers as hf
from scripts import hermes_repair_store as store_mod
from scripts.hermes_health import OperatorState
from scripts.hermes_repair_store import RepairEvent, replay

RULES = hf.repair_rules()

_REQUIRE_ENV = "REQUIRE_HERMES_REPAIR_STORE_PG"
FAILED_AT = datetime(2026, 11, 27, 13, 28, tzinfo=timezone.utc)
REASON = "puller reported FAILED: 5 of 5 items failed or partial"


def _fresh_memory(monkeypatch) -> None:
    """What a new process starts with."""
    monkeypatch.setattr(hf, "_REPAIR_LAST_STARTED", {})
    monkeypatch.setattr(hf, "_DETERMINISTIC_SKIPS", {})
    monkeypatch.setattr(hf, "_DIAGNOSIS_CACHE", {})
    monkeypatch.setattr(hf, "_THROTTLED_WARNINGS", {})
    monkeypatch.setattr(hf, "_REPEATED_FAILURES", {})
    monkeypatch.setattr(hf, "_REPAIR_RECOVERED_AT", {})
    monkeypatch.setattr(hf, "_REPAIRS_IN_FLIGHT", {})
    monkeypatch.setattr(hf, "_UNROUTED_FAILURES_REPORTED", {})
    monkeypatch.setattr(hf, "_SCHEDULED_RUNS",
                        {"runs": {}, "good_at": None, "attempt_at": None, "failed_reads": 0})
    monkeypatch.setattr(hf, "_REPAIR_STORE", {"store": None, "engine": None, "restored": False,
                                              "clear_ids": set(), "cooldowns": None})
    monkeypatch.setattr(hf, "_LAST_EVENT_AT", {})


@pytest.fixture(autouse=True)
def _isolated(monkeypatch):
    _fresh_memory(monkeypatch)
    monkeypatch.setattr(hf, "_relation_exists", lambda *_a: True)
    monkeypatch.setattr(hf, "log_issue", MagicMock())
    monkeypatch.setattr(hf, "resolve_source_issues", MagicMock(return_value=0))
    monkeypatch.setattr(hf, "_slow_repair_reason", lambda *_a: None)
    monkeypatch.setattr(store_mod, "_RELEASE", {"sha": "test-release"})
    monkeypatch.setattr("llm.router.get_llm", lambda *a, **k: pytest.fail("LLM not expected"))
    yield


class FakeStore:
    """The store's interface over a list, shared by "processes" like the table."""

    def __init__(self) -> None:
        self.events: list[RepairEvent] = []
        self.batches: list[list[str]] = []
        self.locks: list[str] = []
        self.absent = False
        self.fail_reads = False
        self.fail_writes = False
        self.reads = 0

    def available(self) -> bool:
        return not self.absent

    def record(self, event, source_key, source_name, **fields) -> int:
        return self.record_many([dict(event=event, source_key=source_key, source_name=source_name, **fields)])[0][0]

    def record_many(self, rows) -> list[tuple[int, datetime]]:
        if self.fail_writes:
            raise RuntimeError("connection reset")
        made = []
        for row in rows:                                  # all or none, like one transaction
            reason = row.get("reason")
            made.append(RepairEvent(
                len(self.events) + len(made) + 1, datetime.now(timezone.utc),
                row["source_key"], row["source_name"], row["event"], row.get("fingerprint"),
                None if reason is None else str(reason)[:300], row.get("skip_until"), row.get("strikes"),
                row.get("occurred_at")))
        self.events.extend(made)
        self.batches.append([ev.event for ev in made])
        return [(ev.id, ev.recorded_at) for ev in made]

    from contextlib import contextmanager

    @contextmanager
    def locked(self, source_key):
        if self.fail_writes:
            raise RuntimeError("connection reset")
        self.locks.append(source_key)
        yield "the transaction"

    def read_clears_for(self, _conn, source_key, since):
        return [e for e in self.events if e.event == "cleared" and e.source_key == source_key
                and e.recorded_at > since]

    def insert(self, _conn, rows):
        return self.record_many(rows)

    def read_since(self, since):
        self.reads += 1
        if self.fail_reads:
            raise RuntimeError("canceling statement due to statement timeout")
        return [e for e in self.events if e.recorded_at > since]

    def read_clears_since(self, since):
        self.reads += 1
        if self.fail_reads:
            raise RuntimeError("canceling statement due to statement timeout")
        return [e for e in self.events if e.event == "cleared" and e.recorded_at > since]


@pytest.fixture()
def table(monkeypatch) -> FakeStore:
    fake = FakeStore()
    monkeypatch.setattr(store_mod, "RepairEventStore", lambda _engine: fake)
    return fake


def _restart(monkeypatch) -> None:
    _fresh_memory(monkeypatch)


def _engine(sources: list[str], at: datetime = FAILED_AT) -> Any:
    engine = MagicMock()
    conn = engine.connect.return_value.__enter__.return_value
    conn.execute.return_value.fetchall.return_value = [(s, 3, at, "synthetic failure") for s in sources]
    return engine


def _failing_retry(monkeypatch, outcome_note: str = "5 of 5 items failed or partial") -> list[str]:
    calls: list[str] = []

    def retry(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        return {"status": "FAILED", "outcome": "FAILED", "rows_inserted": 0, "outcome_note": outcome_note}

    monkeypatch.setattr(hf, "_retry_source", retry)
    return calls


def _warnings(monkeypatch) -> list[str]:
    seen: list[str] = []
    monkeypatch.setattr(hf.log, "warning", lambda template, **kw: seen.append(template.format(**kw)))
    return seen


def _tables() -> dict:
    now = time.monotonic()
    return {
        "repeated": {k: list(v) for k, v in hf._REPEATED_FAILURES.items()},
        "skips": {k: (round(until - now, -1), why) for k, (until, why) in hf._DETERMINISTIC_SKIPS.items()},
        "recovered": dict(hf._REPAIR_RECOVERED_AT),
    }


# ── replay is the live rules ───────────────────────────────────────────────


SEQUENCES = {
    "three in a row trip a long skip": ["F:a"] * 3,
    "a second long skip doubles": ["F:a"] * 6,
    "two short of the threshold": ["F:a"] * 2,
    "a new reason restarts the count, keeps the strikes": ["F:a"] * 3 + ["F:b"] * 2,
    "alternating reasons never trip": ["F:a", "F:b"] * 4,
    "a recovery drops count and strikes": ["F:a"] * 3 + ["R"] + ["F:a"] * 2,
    "digits do not make a new reason": ["F:5 of 5", "F:4 of 4", "F:3 of 3"],
}


@pytest.mark.parametrize("steps", SEQUENCES.values(), ids=SEQUENCES.keys())
def test_replay_leaves_exactly_what_the_live_calls_left(monkeypatch, table, steps):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    for step in steps:
        if step == "R":
            hf.note_repair_recovered("Synth_Source")
        else:
            hf.note_repair_failure("Synth_Source", step[2:])
    live = _tables()

    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())

    assert _tables() == live
    assert hf._REPAIR_STORE["store"] is table


def test_a_missing_long_skip_row_is_rebuilt_from_the_tripping_failure():
    """Three counted failures and no long_skip row (it is written in the same
    transaction, so this is a row lost some other way): the replay trips the
    skip itself, from the third failure's time, with the live formula."""
    events = [RepairEvent(n, FAILED_AT + timedelta(minutes=n), "s", "S", "repair_failed", "x", "x")
              for n in range(1, 4)]
    state = replay(events, FAILED_AT + timedelta(hours=1), rules=RULES)
    assert state.sources["s"].repeated == ["x", 0, 1]
    assert state.sources["s"].skip_until == FAILED_AT + timedelta(minutes=3, seconds=RULES.long_skip_seconds(0))
    assert state.sources["s"].skip_reason.startswith("same failure 3x in a row")


def test_an_ended_skip_is_not_restored_and_a_clear_drops_everything():
    until = FAILED_AT + timedelta(hours=24)
    events = [
        RepairEvent(1, FAILED_AT, "s", "S", "long_skip", "x", "same failure 3x", until, 1),
        RepairEvent(2, FAILED_AT, "t", "T", "long_skip", "x", "same failure 3x", until, 1),
        RepairEvent(3, FAILED_AT + timedelta(hours=1), "t", "T", "cleared", None, "provider fixed"),
    ]
    in_force = replay(events, until - timedelta(seconds=1), rules=RULES)
    assert in_force.sources["s"].skip_until == until
    assert in_force.sources["t"].skip_until is None and in_force.sources["t"].repeated is None
    ended = replay(events, until, rules=RULES)
    assert ended.sources["s"].skip_until is None
    assert ended.sources["s"].repeated == ["x", 0, 1], "the strikes outlive the skip, so the next doubles"


# ── a restart keeps what it should ─────────────────────────────────────────


def test_a_long_skip_tripped_before_a_restart_still_holds_after_it(monkeypatch, table):
    """The TIINGO / polygon case: three failed repairs, a long skip, a restart."""
    calls = _failing_retry(monkeypatch)
    engine = _engine(["TIINGO"])
    state = OperatorState()
    for _ in range(3):
        state.cooldowns = OperatorState().cooldowns          # past the ordinary cooldown
        hf.diagnose_and_fix_pulls(engine, False, state)
    assert calls == ["TIINGO"] * 3 and hf.repair_deterministically_blocked("TIINGO")
    assert [e.event for e in table.events] == ["repair_failed"] * 3 + ["long_skip"]
    assert table.batches[-1] == ["repair_failed", "long_skip"], "the trip is one write, not two"

    _restart(monkeypatch)
    restarted = OperatorState()
    result = hf.diagnose_and_fix_pulls(_engine(["TIINGO"]), False, restarted)   # a new process, a new engine

    assert calls == ["TIINGO"] * 3, "not repaired again after the restart"
    assert result["skipped_deterministic"] == 1
    reason = hf.repair_deterministically_blocked("TIINGO")
    assert reason is not None and "same failure 3x in a row" in reason
    remaining = hf._DETERMINISTIC_SKIPS["tiingo"][0] - time.monotonic()
    assert 23.9 * 3600 < remaining <= 24 * 3600


def test_a_count_short_of_the_threshold_and_the_cooldown_carry_over(monkeypatch, table):
    calls = _failing_retry(monkeypatch)
    engine = _engine(["polygon"])
    state = OperatorState()
    for _ in range(2):
        state.cooldowns = OperatorState().cooldowns
        hf.diagnose_and_fix_pulls(engine, False, state)

    _restart(monkeypatch)
    engine = _engine(["polygon"])
    restarted = OperatorState()
    right_after = hf.diagnose_and_fix_pulls(engine, False, restarted)
    assert calls == ["polygon"] * 2, "the restored cooldown is still running"
    assert right_after["skipped_cooldown"] == 1
    assert restarted.cooldowns.get_status("polygon")["consecutive_fails"] == 2

    restarted.cooldowns = OperatorState().cooldowns                 # past it
    hf.diagnose_and_fix_pulls(engine, False, restarted)
    assert calls == ["polygon"] * 3
    assert hf.repair_deterministically_blocked("polygon"), "the third failure across the restart trips it"


def test_a_recovery_before_the_restart_is_still_a_recovery_after_it(monkeypatch, table):
    calls: list[str] = []

    def recovers(name, engine, *, attempt, state, should_continue):
        calls.append(name)
        return {"status": "SUCCESS", "outcome": "NO_NEW_DATA", "rows_inserted": 0}

    monkeypatch.setattr(hf, "_retry_source", recovers)
    failed_at = datetime.now(timezone.utc) - timedelta(hours=1)
    hf.diagnose_and_fix_pulls(_engine(["OppInsights"], failed_at), False, OperatorState())
    assert calls == ["OppInsights"]

    _restart(monkeypatch)
    result = hf.diagnose_and_fix_pulls(_engine(["OppInsights"], failed_at), False, OperatorState())
    assert calls == ["OppInsights"], "the same failure is not repaired again"
    assert result["retried"] == 0


def test_the_restore_never_overwrites_what_this_process_already_holds(monkeypatch, table):
    table.events.append(RepairEvent(1, FAILED_AT, "s", "S", "long_skip", "x", "old skip",
                                    datetime.now(timezone.utc) + timedelta(hours=5), 1))
    hf._DETERMINISTIC_SKIPS["s"] = (time.monotonic() + 60, "this process's own skip")
    state = OperatorState()
    state.cooldowns.record_attempt("S", success=True)

    hf._sync_repair_store("the engine", state)

    assert hf._DETERMINISTIC_SKIPS["s"][1] == "this process's own skip"
    assert state.cooldowns.get_status("S")["consecutive_fails"] == 0


@pytest.mark.parametrize("record", [
    lambda: hf._record_deterministic_failure("USDA_NASS", "USDA_NASS_API_KEY is not configured"),
    lambda: hf._record_deterministic_failure("GDELT", "AttributeError: no pull_all"),
    lambda: hf._record_deterministic_failure("YFinance_Daily", "no retry handler: none"),
], ids=["missing key", "code defect", "no handler"])
def test_skips_a_deploy_or_env_fix_ends_are_not_kept(monkeypatch, table, record):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    record()
    assert table.events == []
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert hf._DETERMINISTIC_SKIPS == {}, "the restart is the retry the fix needs"


# ── an operator clear ──────────────────────────────────────────────────────


def test_an_operator_clear_lets_the_running_process_retry_once(monkeypatch, table):
    calls = _failing_retry(monkeypatch)
    warnings = _warnings(monkeypatch)
    state = OperatorState()
    engine = _engine(["TIINGO"])
    for _ in range(4):
        state.cooldowns = OperatorState().cooldowns
        hf.diagnose_and_fix_pulls(engine, False, state)
    assert calls == ["TIINGO"] * 3

    store_mod.RepairEventStore(engine).record("cleared", "tiingo", "TIINGO", reason="FNGR delisted, list fixed")
    hf.diagnose_and_fix_pulls(engine, False, state)

    assert calls == ["TIINGO"] * 4, "retried on the next pass, without a restart"
    assert sum("cleared by an operator: FNGR delisted, list fixed" in w for w in warnings) == 1
    hf.diagnose_and_fix_pulls(engine, False, state)
    assert sum("cleared by an operator" in w for w in warnings) == 1, "applied once"


def test_a_clear_stamped_before_the_restore_but_committed_after_it_is_applied(monkeypatch, table):
    """recorded_at is set when the insert runs, not when it commits: a clear
    whose transaction was open during the restore read appears later with an
    earlier time. The rescan reaches back far enough to see it."""
    warnings = _warnings(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    late = RepairEvent(1, datetime.now(timezone.utc) - timedelta(minutes=2), "tiingo", "TIINGO",
                       "cleared", None, "opened before the restore read")
    table.events.append(late)
    hf._sync_repair_store("the engine", OperatorState())
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS
    assert sum("cleared by an operator" in w for w in warnings) == 1


def test_a_clear_stamped_eleven_minutes_before_the_start_and_committed_after_it_is_applied(monkeypatch, table):
    """No time floor: the whole replay window is rescanned for unseen clears."""
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    table.events.append(RepairEvent(1, datetime.now(timezone.utc) - timedelta(minutes=11), "tiingo", "TIINGO",
                                    "cleared", None, "a transaction open across the restore"))
    hf._sync_repair_store("the engine", OperatorState())
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS


def test_a_clear_older_than_the_sources_last_event_is_superseded_live_and_on_restart(monkeypatch, table):
    """The race in its stored form: the clear's row is ordered before the
    tripping failure and its skip. Live and replayed, the skip stands."""
    hf._sync_repair_store("the engine", OperatorState())
    early = datetime.now(timezone.utc) - timedelta(seconds=5)
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    table.events.append(RepairEvent(len(table.events) + 1, early, "tiingo", "TIINGO", "cleared", None, "late commit"))
    hf._sync_repair_store("the engine", OperatorState())
    live = _tables()
    assert "tiingo" in live["skips"], "superseded: the skip came after the clear"

    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


def test_after_a_restart_a_late_clear_older_than_the_restored_trip_is_superseded(monkeypatch, table):
    """The restore seeds each source's newest stored event, so a clear that
    becomes visible after the restart but is ordered before the trip does
    not lift the skip, as a later cold replay would not."""
    hf._sync_repair_store("the engine", OperatorState())
    early = datetime.now(timezone.utc) - timedelta(seconds=5)
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    table.events.append(RepairEvent(len(table.events) + 1, early, "tiingo", "TIINGO", "cleared", None, "late"))
    hf._sync_repair_store("the engine", OperatorState())
    assert hf.repair_deterministically_blocked("TIINGO") is not None
    live = _tables()
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


def _count(source: str) -> int:
    entry = hf._REPEATED_FAILURES.get(hf._normalize_source_key(source))
    return 0 if entry is None else entry[1]


def test_a_clear_committed_between_two_failures_restarts_the_count_before_the_next_pass(monkeypatch, table):
    """Codex 6094982867, first path: failure, a committed clear, a failure,
    no pass in between. The second failure is counted after the clear."""
    hf._sync_repair_store("the engine", OperatorState())
    hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    hf.note_repair_failure("TIINGO", REASON)
    assert _count("TIINGO") == 1
    hf._sync_repair_store("the engine", OperatorState())             # the next pass changes nothing
    live = _tables()
    assert live["repeated"]["tiingo"][1] == 1

    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


def test_a_clear_between_two_failures_and_a_third_does_not_trip_a_long_skip(monkeypatch, table):
    """Codex 6094982867, second path: two failures, a committed clear, one
    failure. Counted from memory alone the third would trip and store a
    long skip the clear had ruled out."""
    hf._sync_repair_store("the engine", OperatorState())
    hf.note_repair_failure("TIINGO", REASON)
    hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    assert hf.note_repair_failure("TIINGO", REASON) is False
    assert hf.repair_deterministically_blocked("TIINGO") is None
    assert "long_skip" not in [e.event for e in table.events]
    hf._sync_repair_store("the engine", OperatorState())
    live = _tables()

    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live and live["skips"] == {}


def test_a_clear_taken_in_by_a_repair_write_resets_the_ordinary_cooldown_as_a_restart_would(monkeypatch, table):
    """Codex 6095079697, on the public call order: two failed attempts, a
    committed clear, one more attempt and failure before the next pass. The
    clear resets the cooldown and the new attempt is counted on top of it:
    one failure in a row, live and after a restart."""
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    assert state.cooldowns.get_status("TIINGO")["consecutive_fails"] == 2
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
    hf.note_repair_failure("TIINGO", REASON)
    hf._sync_repair_store("the engine", state)                        # the next pass

    live = state.cooldowns.get_status("TIINGO")
    assert live["consecutive_fails"] == 1 and live["last_error"] == "puller reported FAILED"
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store("the engine", cold)
    assert cold.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1


def test_a_clear_taken_in_by_a_recovery_leaves_no_failure_in_the_cooldown(monkeypatch, table):
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    state.cooldowns.record_attempt("TIINGO", success=False, error="x")
    hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=True)
    hf.note_repair_recovered("TIINGO")
    assert state.cooldowns.get_status("TIINGO")["consecutive_fails"] == 0
    assert state.cooldowns.can_retry("TIINGO") is False, "the attempt just made still counts for the cadence"


def test_a_pass_clear_between_an_attempt_and_its_stored_failure_keeps_that_attempt(monkeypatch, table):
    """Codex 6095220439, the caller order: two failures, a committed clear,
    a fresh attempt recorded, the pass applies the clear, then the failure
    is noted. The fresh attempt's outcome is stored after the clear, so it
    is kept: one failure in a row and no immediate retry, live and after a
    restart."""
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
    hf._sync_repair_store("the engine", state)                        # the pass applies the clear here
    hf.note_repair_failure("TIINGO", REASON)

    live = state.cooldowns.get_status("TIINGO")
    assert live["consecutive_fails"] == 1 and state.cooldowns.can_retry("TIINGO") is False
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store("the engine", cold)
    assert cold.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1
    assert cold.cooldowns.can_retry("TIINGO") is False


def test_a_clear_drops_settled_attempts_and_keeps_only_the_unsettled_one(monkeypatch, table):
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    state.cooldowns.record_attempt("TIINGO", success=False, error="x")
    hf.note_repair_failure("TIINGO", REASON)                           # settled
    table.record("cleared", "tiingo", "TIINGO", reason="nothing pending")
    hf._sync_repair_store("the engine", state)
    assert state.cooldowns.get_status("TIINGO") is None, "a settled attempt goes with the clear"
    state.cooldowns.record_attempt("TIINGO", success=True)             # unsettled success
    table.record("cleared", "tiingo", "TIINGO", reason="success pending")
    hf._sync_repair_store("the engine", state)
    kept = state.cooldowns.get_status("TIINGO")
    assert kept["consecutive_fails"] == 0 and kept["outcome_pending"] is True


@pytest.mark.parametrize("seed", range(6))
def test_live_and_replayed_bookkeeping_agree_after_every_step(monkeypatch, table, seed):
    """Random interleavings of failures (two reasons), recoveries, clears and
    passes over three sources, with clears landing between passes: after
    every pass a cold replay of what is stored leaves exactly the live
    tables."""
    import random

    rng = random.Random(seed)
    hf._sync_repair_store("the engine", OperatorState())
    sources = ["Alpha", "Beta", "Gamma"]
    for _step in range(120):
        source = rng.choice(sources)
        roll = rng.random()
        if roll < 0.6:
            hf.note_repair_failure(source, rng.choice([REASON, "HTTP 503"]))
        elif roll < 0.75:
            hf.note_repair_recovered(source)
        elif roll < 0.9:
            table.record("cleared", hf._normalize_source_key(source), source, reason="operator")
        if rng.random() < 0.5:
            continue                                                # no pass yet: clears pile up
        hf._sync_repair_store("the engine", OperatorState())       # a pass applies new clears
        live = _tables()
        memory = {name: getattr(hf, name) for name in
                  ("_REPEATED_FAILURES", "_DETERMINISTIC_SKIPS", "_REPAIR_RECOVERED_AT", "_LAST_EVENT_AT")}
        snapshot = {k: dict(v) for k, v in memory.items()}
        _restart(monkeypatch)
        hf._sync_repair_store("the engine", OperatorState())
        assert _tables() == live, f"step {_step}"
        for name, saved in snapshot.items():                       # carry on from the live process
            monkeypatch.setattr(hf, name, saved)
        hf._REPAIR_STORE.update(store=table, engine="the engine", restored=True,
                                clear_ids={e.id for e in table.events if e.event == "cleared"})


def test_a_clear_recorded_while_hermes_was_down_is_part_of_the_restore(monkeypatch, table):
    until = datetime.now(timezone.utc) + timedelta(hours=20)
    table.record("long_skip", "tiingo", "TIINGO", fingerprint="x", reason="same failure 3x", skip_until=until, strikes=1)
    table.record("cleared", "tiingo", "TIINGO", reason="fixed")
    hf._sync_repair_store("the engine", OperatorState())
    assert hf.repair_deterministically_blocked("TIINGO") is None


# ── without the table, or when it fails ────────────────────────────────────


def test_without_the_table_hermes_works_in_memory_says_so_once_and_looks_again(monkeypatch, table):
    table.absent = True
    calls = _failing_retry(monkeypatch)
    warnings = _warnings(monkeypatch)
    state = OperatorState()
    for _ in range(4):
        state.cooldowns = OperatorState().cooldowns
        hf.diagnose_and_fix_pulls(_engine(["TIINGO"]), False, state)

    assert calls == ["TIINGO"] * 3, "the in-memory long skip works as before"
    assert table.events == []
    assert sum("hermes_repair_events is not present" in w for w in warnings) == 1

    table.absent = False                                          # the migration is applied
    hf.diagnose_and_fix_pulls(_engine(["TIINGO"]), False, state)
    assert hf._REPAIR_STORE["store"] is table


def test_a_failed_read_keeps_memory_and_is_tried_again(monkeypatch, table):
    table.fail_reads = True
    warnings = _warnings(monkeypatch)
    _failing_retry(monkeypatch)
    hf.diagnose_and_fix_pulls(_engine(["TIINGO"]), False, OperatorState())
    assert hf._REPAIR_STORE["store"] is None
    assert any("Repair history not read (RuntimeError)" in w for w in warnings)
    assert not any("statement timeout" in w for w in warnings)

    table.fail_reads = False
    hf.diagnose_and_fix_pulls(_engine(["TIINGO"]), False, OperatorState())
    assert hf._REPAIR_STORE["store"] is table


def test_a_failed_write_keeps_the_in_memory_count_and_warns_once(monkeypatch, table):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    table.fail_writes = True
    warnings = _warnings(monkeypatch)
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    assert hf.repair_deterministically_blocked("TIINGO") is not None
    assert sum("Repair event not stored (RuntimeError)" in w for w in warnings) == 1


def test_concurrent_failures_are_all_recorded(monkeypatch, table):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    threads = [threading.Thread(target=hf.note_repair_failure, args=(f"S{n}", REASON)) for n in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(e.source_key for e in table.events) == sorted(f"s{n}" for n in range(20))


# ── no secret is stored ────────────────────────────────────────────────────


# Assembled from fragments so this file holds no key-shaped literal
# (tests/test_no_literal_secrets.py scans the tree).
FAKE_KEY = "".join(("sk", "_live_", "01234", "56789", "abcdef"))
FAKE_TOKEN = "".join(("abcdef", "01234", "56789", "abcdef"))
SECRET_REASON = (f"HTTPError: 403 for url https://api.example.test/v2/aggs?apiKey={FAKE_KEY}"
                 f"&ticker=FNGR (token={FAKE_TOKEN})")


def test_reasons_are_redacted_before_they_are_stored_and_still_count_alike(monkeypatch, table):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    for _ in range(3):
        hf.note_repair_failure("TIINGO", SECRET_REASON)
    live = _tables()

    stored = " ".join(f"{e.fingerprint} {e.reason}" for e in table.events)
    assert FAKE_KEY not in stored and FAKE_TOKEN not in stored
    assert "[REDACTED]" in stored or "?" not in stored
    assert [e.event for e in table.events] == ["repair_failed"] * 3 + ["long_skip"]

    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live, "the live fingerprint is the redacted one, so a restart compares alike"


# ── the operator command ───────────────────────────────────────────────────


def test_the_clear_command_needs_a_reason(monkeypatch, capsys):
    with pytest.raises(SystemExit):
        store_mod.main(["clear", "TIINGO"])
    assert "--reason" in capsys.readouterr().err


def test_the_commands_write_one_clear_and_show_what_a_restart_restores(monkeypatch, table, capsys):
    monkeypatch.setattr("db.get_engine", lambda: "the engine")
    table.record("repair_failed", "tiingo", "TIINGO", fingerprint="x", reason="x")
    assert store_mod.main(["show"]) == 0
    assert "TIINGO: count=1 strikes=0 skip_until=- " in capsys.readouterr().out
    assert store_mod.main(["clear", "TIINGO", "--reason", "fixed"]) == 0
    assert [e.event for e in table.events] == ["repair_failed", "cleared"]
    assert table.events[-1].reason == "fixed"
    table.absent = True
    assert store_mod.main(["show"]) == 1


# ── PostgreSQL: the real migration, table and restart ──────────────────────


@pytest.fixture()
def pg_engine(monkeypatch):
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required for the Hermes repair-store PG contract ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("the Hermes repair-store PG contract requires a local disposable test database")
    schema = "hstore_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _upgrade(engine) -> None:
    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module("migrations.versions.hermes_repair_events_20261128")
    with engine.begin() as conn:
        real_op = migration.op
        migration.op = Operations(MigrationContext.configure(conn))
        try:
            migration.upgrade()
        finally:
            migration.op = real_op


def test_real_types_the_migration_creates_a_table_that_refuses_incomplete_rows(pg_engine):
    _upgrade(pg_engine)
    _upgrade(pg_engine)                                           # a second upgrade is a no-op
    real = store_mod.RepairEventStore(pg_engine)
    assert real.available()
    bad = [
        ("INSERT INTO hermes_repair_events (source_key, source_name, event) VALUES ('s','S','repair_failed')", "fingerprint"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event) VALUES ('s','S','long_skip')", "skip"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event) VALUES ('s','S','cleared')", "reason"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event, reason) VALUES ('s','S','other','x')", "event"),
    ]
    for sql, _why in bad:
        with pytest.raises(Exception, match="check constraint"):
            with pg_engine.begin() as conn:
                conn.execute(text(sql))
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM hermes_repair_events")).scalar() == 0


def test_real_types_the_store_round_trips_and_reads_only_what_it_is_asked(pg_engine):
    _upgrade(pg_engine)
    real = store_mod.RepairEventStore(pg_engine)
    until = datetime(2026, 11, 28, 13, 28, tzinfo=timezone.utc)
    first = real.record("repair_failed", "tiingo", "TIINGO", fingerprint="f", reason="r" * 400)
    second = real.record("long_skip", "tiingo", "TIINGO", fingerprint="f", reason="same", skip_until=until, strikes=1)
    clear = real.record("cleared", "tiingo", "TIINGO", reason="fixed")
    assert first < second < clear

    events = real.read_since(datetime(2000, 1, 1, tzinfo=timezone.utc))
    assert [e.event for e in events] == ["repair_failed", "long_skip", "cleared"]
    assert len(events[0].reason) == 300 and events[1].skip_until == until and events[1].strikes == 1
    assert [e.id for e in real.read_clears_since(datetime(2000, 1, 1, tzinfo=timezone.utc))] == [clear]
    assert real.read_clears_since(datetime.now(timezone.utc) + timedelta(minutes=1)) == []
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT DISTINCT release FROM hermes_repair_events")).scalar() == "test-release"
    with pytest.raises(ValueError):
        real.record("other", "s", "S")
    secret = real.record("repair_failed", "polygon", "polygon", fingerprint=SECRET_REASON, reason=SECRET_REASON)
    with pg_engine.connect() as conn:
        row = conn.execute(text("SELECT fingerprint || ' ' || reason FROM hermes_repair_events WHERE id = :i"),
                           {"i": secret}).scalar()
    assert FAKE_KEY not in row and FAKE_TOKEN not in row


def test_real_types_no_table_means_not_available(pg_engine):
    assert store_mod.RepairEventStore(pg_engine).available() is False


def test_real_types_a_long_skip_survives_a_restart_on_real_rows(pg_engine, monkeypatch):
    """The whole path on PostgreSQL: three failed repairs trip the skip and
    write their rows; a restart reads them back and does not repair; a clear
    row lets the running process repair again."""
    _upgrade(pg_engine)
    calls = _failing_retry(monkeypatch)
    rows = [("TIINGO", 3, FAILED_AT, "synthetic failure")]

    class _Failed:
        """The pass's own failed-pull query answers TIINGO; the store's reads go to PostgreSQL."""

        def __init__(self, engine):
            self.engine = engine

        def connect(self):
            outer = self

            class _Conn:
                def __enter__(self_inner):
                    self_inner.conn = outer.engine.connect()
                    return self_inner

                def __exit__(self_inner, *exc):
                    self_inner.conn.close()

                def execute(self_inner, statement, params=None):
                    if "FROM hermes_repair_events" in str(statement) or "to_regclass" in str(statement):
                        return self_inner.conn.execute(statement, params or {})
                    result = MagicMock()
                    result.fetchall.return_value = rows
                    return result

            return _Conn()

        def begin(self):
            return self.engine.begin()

    engine = _Failed(pg_engine)
    state = OperatorState()
    for _ in range(3):
        state.cooldowns = OperatorState().cooldowns
        hf.diagnose_and_fix_pulls(engine, False, state)
    assert calls == ["TIINGO"] * 3
    with pg_engine.connect() as conn:
        assert [r[0] for r in conn.execute(text(
            "SELECT event FROM hermes_repair_events ORDER BY id")).fetchall()] == ["repair_failed"] * 3 + ["long_skip"]

    _restart(monkeypatch)
    restarted = OperatorState()
    hf.diagnose_and_fix_pulls(engine, False, restarted)
    assert calls == ["TIINGO"] * 3
    assert "same failure 3x in a row" in hf.repair_deterministically_blocked("TIINGO")

    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="fixed")
    restarted.cooldowns = OperatorState().cooldowns
    hf.diagnose_and_fix_pulls(engine, False, restarted)
    assert calls == ["TIINGO"] * 4


def test_real_types_rows_cannot_be_rewritten_by_any_role_and_grid_may_only_read_and_add(pg_engine):
    _upgrade(pg_engine)
    real = store_mod.RepairEventStore(pg_engine)
    event_id = real.record("cleared", "tiingo", "TIINGO", reason="fixed")
    for sql in ("UPDATE hermes_repair_events SET reason = 'rewritten' WHERE id = :i",
                "DELETE FROM hermes_repair_events WHERE id = :i",
                "TRUNCATE hermes_repair_events"):
        with pytest.raises(Exception, match="append-only"):
            with pg_engine.begin() as conn:
                conn.execute(text(sql), {"i": event_id})
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT reason FROM hermes_repair_events WHERE id = :i"),
                            {"i": event_id}).scalar() == "fixed"
    source = importlib.import_module("migrations.versions.hermes_repair_events_20261128")
    import inspect
    body = inspect.getsource(source.upgrade)
    assert "GRANT SELECT, INSERT ON hermes_repair_events TO grid" in body
    assert "GRANT USAGE ON SEQUENCE hermes_repair_events_id_seq TO grid" in body
    assert "GRANT ALL ON hermes_repair_events" not in body


def test_real_types_the_tripping_failure_and_its_long_skip_are_stored_together_or_not_at_all(pg_engine):
    _upgrade(pg_engine)
    real = store_mod.RepairEventStore(pg_engine)
    until = datetime.now(timezone.utc) + timedelta(hours=24)
    with pytest.raises(Exception, match="check constraint"):
        real.record_many([
            dict(event="repair_failed", source_key="tiingo", source_name="TIINGO", fingerprint="f", reason="r"),
            dict(event="long_skip", source_key="tiingo", source_name="TIINGO", fingerprint="f",
                 reason="same", skip_until=until, strikes=None),          # refused: no strikes
        ])
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT count(*) FROM hermes_repair_events")).scalar() == 0
    ids = real.record_many([
        dict(event="repair_failed", source_key="tiingo", source_name="TIINGO", fingerprint="f", reason="r"),
        dict(event="long_skip", source_key="tiingo", source_name="TIINGO", fingerprint="f",
             reason="same", skip_until=until, strikes=1),
    ])
    assert len(ids) == 2 and ids[0] < ids[1]


def test_real_types_a_clear_that_commits_after_a_later_one_is_still_applied(pg_engine, monkeypatch):
    """Two operators clear two sources. The first insert gets the lower id but
    commits after the second; the pass that sees only the second must not
    skip the first when it commits (an id is not a commit order)."""
    _upgrade(pg_engine)
    warnings = _warnings(monkeypatch)
    state = OperatorState()
    hf._sync_repair_store(pg_engine, state)                       # restored: nothing yet
    assert hf._REPAIR_STORE["restored"] is True
    far = time.monotonic() + 3600
    hf._DETERMINISTIC_SKIPS.update(tiingo=(far, "held"), polygon=(far, "held"))

    slow = pg_engine.connect()
    slow_tx = slow.begin()
    try:                                                          # never leave the open transaction behind
        slow.execute(text(
            "INSERT INTO hermes_repair_events (source_key, source_name, event, reason) "
            "VALUES ('tiingo', 'TIINGO', 'cleared', 'first, slow to commit')"))
        store_mod.RepairEventStore(pg_engine).record("cleared", "polygon", "polygon", reason="second")
        with pg_engine.connect() as conn:
            lower, higher = conn.execute(text(
                "SELECT min(id), max(id) FROM hermes_repair_events")).one()
        assert lower == higher                                    # only the committed one is visible

        hf._sync_repair_store(pg_engine, state)
        assert "polygon" not in hf._DETERMINISTIC_SKIPS and "tiingo" in hf._DETERMINISTIC_SKIPS
        slow_tx.commit()
    finally:
        if slow_tx.is_active:
            slow_tx.rollback()
        slow.close()
    hf._sync_repair_store(pg_engine, state)
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS, "the lower-id clear that committed later is applied"
    with pg_engine.connect() as conn:
        ids = sorted(r[0] for r in conn.execute(text("SELECT id FROM hermes_repair_events")).fetchall())
    assert ids[0] < ids[1] and ids[0] != higher, "the late commit holds the lower id"

    hf._sync_repair_store(pg_engine, state)
    assert sum("cleared by an operator" in w for w in warnings) == 2, "each clear applied once"


@pytest.mark.parametrize("clear_first", [True, False], ids=["clear starts first", "trip starts first"])
def test_real_types_a_clear_racing_a_trip_on_one_source_is_read_alike_live_and_on_restart(
    pg_engine, monkeypatch, clear_first
):
    """Codex 6094893412 (1): a clear whose transaction starts first but commits
    after the tripping failure. The per-source lock makes the trip wait for
    the clear, so the trip is ordered after it, and the live pass and a cold
    replay both keep the skip. The other way round, both clear it."""
    _upgrade(pg_engine)
    state = OperatorState()
    hf._sync_repair_store(pg_engine, state)
    real = store_mod.RepairEventStore(pg_engine)
    if not clear_first:
        for _ in range(3):
            hf.note_repair_failure("TIINGO", REASON)

    conn = pg_engine.connect()
    tx = conn.begin()
    try:
        real.insert(conn, [dict(event="cleared", source_key="tiingo", source_name="TIINGO", reason="racing")])
        if clear_first:
            trip = threading.Thread(target=lambda: [hf.note_repair_failure("TIINGO", REASON) for _ in range(3)])
            trip.start()
            time.sleep(0.5)
            assert trip.is_alive(), "the trip waits for the open clear"
            with pg_engine.connect() as probe:
                assert probe.execute(text(
                    "SELECT count(*) FROM hermes_repair_events WHERE event = 'repair_failed'")).scalar() == 0
        tx.commit()
    finally:
        if tx.is_active:
            tx.rollback()
        conn.close()
    if clear_first:
        trip.join(10)
        assert not trip.is_alive()

    hf._sync_repair_store(pg_engine, state)                        # the live pass sees the clear
    held_live = hf.repair_deterministically_blocked("TIINGO") is not None
    _restart(monkeypatch)
    hf._sync_repair_store(pg_engine, OperatorState())              # a cold replay
    held_replayed = hf.repair_deterministically_blocked("TIINGO") is not None

    assert held_live is held_replayed is clear_first
    with pg_engine.connect() as c:
        order = [r[0] for r in c.execute(text(
            "SELECT event FROM hermes_repair_events ORDER BY recorded_at, id")).fetchall()]
    expected = ["cleared"] + ["repair_failed"] * 3 + ["long_skip"]
    assert order == (expected if clear_first else expected[1:] + ["cleared"])


def test_real_types_a_backdated_clear_committed_after_the_restore_is_applied(pg_engine, monkeypatch):
    """Codex 6094893412 (2): a clear stamped eleven minutes before the start,
    committed after the restore read. The whole window is rescanned."""
    _upgrade(pg_engine)
    state = OperatorState()
    conn = pg_engine.connect()
    tx = conn.begin()
    try:
        conn.execute(text(
            "INSERT INTO hermes_repair_events (recorded_at, source_key, source_name, event, reason) "
            "VALUES (clock_timestamp() - interval '11 minutes', 'tiingo', 'TIINGO', 'cleared', 'open')"))
        hf._sync_repair_store(pg_engine, state)                    # the restore cannot see it yet
        hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
        tx.commit()
    finally:
        if tx.is_active:
            tx.rollback()
        conn.close()
    hf._sync_repair_store(pg_engine, state)
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS


def test_real_types_a_row_is_stamped_after_the_source_lock_not_at_its_transaction_start(pg_engine):
    """A repair transaction that began before a clear but wrote after it is
    ordered after it: rows take clock_timestamp() once the source's lock is
    held, never the transaction's start time."""
    _upgrade(pg_engine)
    real = store_mod.RepairEventStore(pg_engine)
    conn = pg_engine.connect()
    tx = conn.begin()
    try:
        conn.execute(text("SELECT now()"))                         # the transaction's clock is fixed here
        time.sleep(0.05)
        real.record("cleared", "tiingo", "TIINGO", reason="committed in between")
        time.sleep(0.05)
        real.insert(conn, [dict(event="repair_failed", source_key="tiingo", source_name="TIINGO",
                                fingerprint="f", reason="r")])
        tx.commit()
    finally:
        if tx.is_active:
            tx.rollback()
        conn.close()
    with pg_engine.connect() as c:
        order = [r[0] for r in c.execute(text(
            "SELECT event FROM hermes_repair_events ORDER BY recorded_at, id")).fetchall()]
    assert order == ["cleared", "repair_failed"]


def test_real_types_a_clear_between_failures_restarts_the_count_on_real_rows(pg_engine, monkeypatch):
    """Codex 6094982867 on PostgreSQL: two failures, a committed clear, one
    failure, no pass in between: no long skip, live or after a restart."""
    _upgrade(pg_engine)
    hf._sync_repair_store(pg_engine, OperatorState())
    hf.note_repair_failure("TIINGO", REASON)
    hf.note_repair_failure("TIINGO", REASON)
    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="between polls")
    assert hf.note_repair_failure("TIINGO", REASON) is False
    assert _count("TIINGO") == 1
    hf._sync_repair_store(pg_engine, OperatorState())
    live = _tables()
    _restart(monkeypatch)
    hf._sync_repair_store(pg_engine, OperatorState())
    assert _tables() == live and live["skips"] == {}
    with pg_engine.connect() as c:
        assert [r[0] for r in c.execute(text(
            "SELECT event FROM hermes_repair_events ORDER BY recorded_at, id")).fetchall()] == \
            ["repair_failed", "repair_failed", "cleared", "repair_failed"]


def test_real_types_a_failure_waits_for_an_open_clear_before_it_reads_the_count(pg_engine, monkeypatch):
    """The failure reads the source's clears only once it holds the source's
    lock. With a clear open in another transaction after two failures, the
    third failure waits, then sees the clear and counts from it: no long
    skip, live or after a restart."""
    _upgrade(pg_engine)
    hf._sync_repair_store(pg_engine, OperatorState())
    hf.note_repair_failure("TIINGO", REASON)
    hf.note_repair_failure("TIINGO", REASON)
    real = store_mod.RepairEventStore(pg_engine)
    result: dict = {}
    conn = pg_engine.connect()
    tx = conn.begin()
    try:
        real.insert(conn, [dict(event="cleared", source_key="tiingo", source_name="TIINGO", reason="open")])
        third = threading.Thread(target=lambda: result.update(tripped=hf.note_repair_failure("TIINGO", REASON)))
        third.start()
        time.sleep(0.5)
        assert third.is_alive(), "the third failure waits on the source's lock"
        tx.commit()
    finally:
        if tx.is_active:
            tx.rollback()
        conn.close()
    third.join(10)
    assert result == {"tripped": False} and _count("TIINGO") == 1
    hf._sync_repair_store(pg_engine, OperatorState())
    live = _tables()
    _restart(monkeypatch)
    hf._sync_repair_store(pg_engine, OperatorState())
    assert _tables() == live and live["skips"] == {}


def test_real_types_a_clear_between_attempts_resets_the_cooldown_on_real_rows(pg_engine, monkeypatch):
    _upgrade(pg_engine)
    state = OperatorState()
    hf._sync_repair_store(pg_engine, state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
    hf.note_repair_failure("TIINGO", REASON)
    hf._sync_repair_store(pg_engine, state)
    assert state.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store(pg_engine, cold)
    assert cold.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1


def test_real_types_a_pass_paused_while_applying_a_clear_cannot_let_a_failure_trip_past_it(pg_engine, monkeypatch):
    """Codex 6095172882's witness: two failures and a committed clear; the
    pass is paused just as it applies the clear; a failure for the same
    source arrives. The pass holds the source's locks while it applies, so
    the failure waits, then counts after the clear: no long skip, and the
    stored sequence is failed, failed, cleared, failed."""
    _upgrade(pg_engine)
    state = OperatorState()
    hf._sync_repair_store(pg_engine, state)
    hf.note_repair_failure("TIINGO", REASON)
    hf.note_repair_failure("TIINGO", REASON)
    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="between polls")

    real_apply = hf._apply_clear
    paused, resume = threading.Event(), threading.Event()
    poll_thread: dict = {}

    def pausing_apply(ev, cooldowns):
        if threading.get_ident() == poll_thread.get("id"):
            paused.set()
            resume.wait(10)
        return real_apply(ev, cooldowns)

    monkeypatch.setattr(hf, "_apply_clear", pausing_apply)

    def poll():
        poll_thread["id"] = threading.get_ident()
        hf._sync_repair_store(pg_engine, state)

    result: dict = {}
    poller = threading.Thread(target=poll)
    poller.start()
    assert paused.wait(10), "the pass reached the clear"
    third = threading.Thread(target=lambda: result.update(tripped=hf.note_repair_failure("TIINGO", REASON)))
    third.start()
    time.sleep(0.5)
    try:
        assert third.is_alive(), "the failure waits for the pass to finish applying the clear"
    finally:
        resume.set()
        poller.join(10)
        third.join(10)

    assert result == {"tripped": False} and _count("TIINGO") == 1
    assert hf.repair_deterministically_blocked("TIINGO") is None
    with pg_engine.connect() as c:
        assert [r[0] for r in c.execute(text(
            "SELECT event FROM hermes_repair_events ORDER BY recorded_at, id")).fetchall()] == \
            ["repair_failed", "repair_failed", "cleared", "repair_failed"]
    monkeypatch.setattr(hf, "_apply_clear", real_apply)
    live = _tables()
    _restart(monkeypatch)
    hf._sync_repair_store(pg_engine, OperatorState())
    assert _tables() == live and live["skips"] == {}


def test_a_pass_applies_each_clear_once_and_then_leaves_it(monkeypatch, table):
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    table.record("cleared", "tiingo", "TIINGO", reason="once")
    hf._sync_repair_store("the engine", OperatorState())
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS and table.locks == ["tiingo"]
    for _ in range(3):
        hf._sync_repair_store("the engine", OperatorState())
    assert table.locks == ["tiingo"], "a consumed clear takes no lock on later passes"


def test_applying_a_clear_twice_changes_nothing_the_second_time(monkeypatch, table):
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    ev = RepairEvent(99, datetime.now(timezone.utc), "tiingo", "TIINGO", "cleared", None, "once")
    assert hf._apply_clear(ev, None) is True
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held again")
    assert hf._apply_clear(ev, None) is False
    assert "tiingo" in hf._DETERMINISTIC_SKIPS


def test_real_types_a_pass_clear_between_an_attempt_and_its_failure_keeps_the_attempt(pg_engine, monkeypatch):
    _upgrade(pg_engine)
    state = OperatorState()
    hf._sync_repair_store(pg_engine, state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
    hf._sync_repair_store(pg_engine, state)
    hf.note_repair_failure("TIINGO", REASON)
    assert state.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1
    assert state.cooldowns.can_retry("TIINGO") is False
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store(pg_engine, cold)
    assert cold.cooldowns.get_status("TIINGO")["consecutive_fails"] == 1
    assert cold.cooldowns.can_retry("TIINGO") is False
