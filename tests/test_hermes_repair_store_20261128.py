"""Hermes's repair bookkeeping survives a restart (2026-11-28).

Before: the repeated-failure count, the long skip it trips, the time a repair
recovered a source lived in process memory only.
grid-hermes started 15 times on 2026-11-27, and the sources whose long skip
had tripped (TIINGO, polygon, Asset_Registries, yfinance_options) were
repaired again after every start.

Now each counted failure, long skip, recovery and operator clear is a row in
``hermes_repair_events`` (scripts/hermes_repair_store.py), and the first pass
after a start replays them by the live rules. These cases pin:

- replay leaves exactly the tables the live calls left, for a recorded
  sequence of the live calls themselves (equivalence, not a parallel model);
- after a restart a long skip in force, a count short of the threshold and a
  recovery all hold, and a skip that has ended does not; the ordinary
  cooldown is not restored, and every clear drops it, so a restarted Hermes
  is never stricter than the running one;
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
    monkeypatch.setattr(hf, "_LAST_EVENT_SEQ", {})


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
                row.get("occurred_at"), self._next_seq(row["source_key"], made)))
        self.events.extend(made)
        self.batches.append([ev.event for ev in made])
        return [(ev.id, ev.seq) for ev in made]

    def _next_seq(self, source_key, pending=()):
        return 1 + max([e.seq for e in [*self.events, *pending] if e.source_key == source_key], default=0)

    def commit(self, source_key, source_name, event, *, recorded_at=None, reason=None, seq=None):
        """A row another writer committed, with the next seq unless one is given."""
        ev = RepairEvent(len(self.events) + 1, recorded_at or datetime.now(timezone.utc), source_key,
                         source_name, event, None, reason, None, None, None,
                         self._next_seq(source_key) if seq is None else seq)
        self.events.append(ev)
        return ev

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


def test_a_count_short_of_the_threshold_carries_over_and_the_ordinary_cooldown_does_not(monkeypatch, table):
    calls = _failing_retry(monkeypatch)
    engine = _engine(["polygon"])
    state = OperatorState()
    for _ in range(2):
        state.cooldowns = OperatorState().cooldowns
        hf.diagnose_and_fix_pulls(engine, False, state)

    _restart(monkeypatch)
    engine = _engine(["polygon"])
    restarted = OperatorState()
    hf.diagnose_and_fix_pulls(engine, False, restarted)
    assert calls == ["polygon"] * 3, "no ordinary cooldown after a restart: retried at once"
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
                                    datetime.now(timezone.utc) + timedelta(hours=5), 1, None, 1))
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
    table.commit("tiingo", "TIINGO", "cleared", reason="opened before the restore read",
                 recorded_at=datetime.now(timezone.utc) - timedelta(minutes=2))
    hf._sync_repair_store("the engine", OperatorState())
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS
    assert sum("cleared by an operator" in w for w in warnings) == 1


def test_a_clear_stamped_eleven_minutes_before_the_start_and_committed_after_it_is_applied(monkeypatch, table):
    """No time floor: the whole replay window is rescanned for unseen clears."""
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    table.commit("tiingo", "TIINGO", "cleared", reason="a transaction open across the restore",
                 recorded_at=datetime.now(timezone.utc) - timedelta(minutes=11))
    hf._sync_repair_store("the engine", OperatorState())
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS


def test_a_clear_numbered_before_the_sources_last_event_is_superseded_live_and_on_restart(monkeypatch, table):
    """Order is the source's seq. A clear whose number is below the trip's
    (committed before it by a writer this process did not read) is
    superseded live and in a replay alike: the skip stands."""
    hf._sync_repair_store("the engine", OperatorState())
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    table.commit("tiingo", "TIINGO", "cleared", reason="numbered first", seq=3)  # below the long skip's 4
    hf._sync_repair_store("the engine", OperatorState())
    live = _tables()
    assert "tiingo" in live["skips"]
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


@pytest.mark.parametrize("stamp", ["the same instant", "a second earlier"])
def test_a_clear_committed_after_the_trip_lifts_it_whatever_its_timestamp(monkeypatch, table, stamp):
    """Codex 6095402357 (2): wall time is not the order. A clear committed
    after the trip, stamped at the same instant or a second earlier (a
    clock step), lifts the skip live and after a restart."""
    hf._sync_repair_store("the engine", OperatorState())
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    trip_at = table.events[-1].recorded_at
    table.commit("tiingo", "TIINGO", "cleared", reason="after the trip",
                 recorded_at=trip_at if stamp == "the same instant" else trip_at - timedelta(seconds=1))
    hf._sync_repair_store("the engine", OperatorState())
    live = _tables()
    assert live["skips"] == {} and "tiingo" not in live["repeated"]
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


def test_after_a_restart_a_clear_numbered_before_the_restored_trip_is_superseded(monkeypatch, table):
    hf._sync_repair_store("the engine", OperatorState())
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    table.commit("tiingo", "TIINGO", "cleared", reason="numbered first", seq=3)  # below the long skip's 4
    hf._sync_repair_store("the engine", OperatorState())
    assert hf.repair_deterministically_blocked("TIINGO") is not None
    live = _tables()
    _restart(monkeypatch)
    hf._sync_repair_store("the engine", OperatorState())
    assert _tables() == live


def test_a_clear_typed_with_spaces_drops_the_cooldown_recorded_with_underscores(monkeypatch, table, capsys):
    """Codex 6095402357 (1): the clear command takes "Asset Registries"; the
    live cooldown is "Asset_Registries". Both the durable state and the
    cooldown are dropped."""
    monkeypatch.setattr("db.get_engine", lambda: "the engine")
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(3):
        state.cooldowns.record_attempt("Asset_Registries", success=False, error="x")
        hf.note_repair_failure("Asset_Registries", REASON)
    assert state.cooldowns.can_retry("Asset_Registries") is False
    assert store_mod.main(["clear", "Asset Registries", "--reason", "fixed upstream"]) == 0
    hf._sync_repair_store("the engine", state)
    assert hf.repair_deterministically_blocked("Asset_Registries") is None
    assert state.cooldowns.get_status("Asset_Registries") is None
    assert state.cooldowns.can_retry("Asset_Registries") is True


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


def _retry_now_live_and_after_restart(monkeypatch, state, source="TIINGO") -> tuple[bool, bool]:
    live = state.cooldowns.can_retry(source)
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store("the engine", cold)
    return live, cold.cooldowns.can_retry(source)


def test_a_clear_taken_in_by_a_repair_write_drops_the_ordinary_cooldown(monkeypatch, table):
    """Codex 6095079697's order: two failed attempts, a committed clear, one
    more attempt and failure before the next pass. The clear drops the
    cooldown; neither the running nor a restarted Hermes holds the retry."""
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
    hf.note_repair_failure("TIINGO", REASON)
    hf._sync_repair_store("the engine", state)
    assert _retry_now_live_and_after_restart(monkeypatch, state) == (True, True)


@pytest.mark.parametrize("order", ["attempt, pass, note", "attempt, note, pass", "attempt only, pass"])
def test_whatever_the_order_a_clear_leaves_the_retry_allowed_live_and_after_restart(monkeypatch, table, order):
    """Codex 6095220439 and 6095305914: the pass applying the clear between an
    attempt and its stored failure; after it; and an attempt whose outcome
    is never stored (a missing key, a budget stop). The ordinary cooldown is
    process memory and every clear drops it, so all three agree."""
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="puller reported FAILED")
        hf.note_repair_failure("TIINGO", REASON)
    table.record("cleared", "tiingo", "TIINGO", reason="between polls")
    state.cooldowns.record_attempt("TIINGO", success=False, error="TIINGO_API_KEY is not configured")
    if order == "attempt, pass, note":
        hf._sync_repair_store("the engine", state)
        hf.note_repair_failure("TIINGO", REASON)
    elif order == "attempt, note, pass":
        hf.note_repair_failure("TIINGO", REASON)
        hf._sync_repair_store("the engine", state)
    else:
        hf._sync_repair_store("the engine", state)
    assert _retry_now_live_and_after_restart(monkeypatch, state) == (True, True)


def test_after_a_restart_no_ordinary_cooldown_is_restored(monkeypatch, table):
    state = OperatorState()
    hf._sync_repair_store("the engine", state)
    for _ in range(2):
        state.cooldowns.record_attempt("TIINGO", success=False, error="x")
        hf.note_repair_failure("TIINGO", REASON)
    assert state.cooldowns.can_retry("TIINGO") is False
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store("the engine", cold)
    assert cold.cooldowns.get_status("TIINGO") is None
    assert _count("TIINGO") == 2, "the count is restored; the pacing is not"


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
                  ("_REPEATED_FAILURES", "_DETERMINISTIC_SKIPS", "_REPAIR_RECOVERED_AT", "_LAST_EVENT_SEQ")}
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
        ("INSERT INTO hermes_repair_events (source_key, source_name, event, seq) VALUES ('s','S','repair_failed', 1)", "fingerprint"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event, seq) VALUES ('s','S','long_skip', 1)", "skip"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event, seq) VALUES ('s','S','cleared', 1)", "reason"),
        ("INSERT INTO hermes_repair_events (source_key, source_name, event, reason, seq) VALUES ('s','S','other','x', 1)", "event"),
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
            "INSERT INTO hermes_repair_events (source_key, source_name, event, reason, seq) "
            "VALUES ('tiingo', 'TIINGO', 'cleared', 'first, slow to commit', 1)"))
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
            "SELECT event FROM hermes_repair_events ORDER BY seq")).fetchall()]
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
            "INSERT INTO hermes_repair_events (recorded_at, source_key, source_name, event, reason, seq) "
            "VALUES (clock_timestamp() - interval '11 minutes', 'tiingo', 'TIINGO', 'cleared', 'open', 1)"))
        hf._sync_repair_store(pg_engine, state)                    # the restore cannot see it yet
        hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
        tx.commit()
    finally:
        if tx.is_active:
            tx.rollback()
        conn.close()
    hf._sync_repair_store(pg_engine, state)
    assert "tiingo" not in hf._DETERMINISTIC_SKIPS


def test_real_types_a_row_is_numbered_after_the_source_lock_not_at_its_transaction_start(pg_engine):
    """A repair transaction that began before a clear but wrote after it is
    ordered after it: its seq is taken once the source's lock is held."""
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
            "SELECT event FROM hermes_repair_events ORDER BY seq")).fetchall()]
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
            "SELECT event FROM hermes_repair_events ORDER BY seq")).fetchall()] == \
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


def test_real_types_a_clear_between_attempts_drops_the_cooldown_on_real_rows(pg_engine, monkeypatch):
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
    assert state.cooldowns.can_retry("TIINGO") is True
    _restart(monkeypatch)
    cold = OperatorState()
    hf._sync_repair_store(pg_engine, cold)
    assert cold.cooldowns.can_retry("TIINGO") is True
    assert _count("TIINGO") == 1, "the count after the clear is restored exactly"


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
            "SELECT event FROM hermes_repair_events ORDER BY seq")).fetchall()] == \
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


class _GrowsWhileRead(dict):
    """A cooldown table that gains an entry for another source the moment
    anything iterates it: the interleaving of Codex 6095577140."""

    def __iter__(self):
        inner = super().__iter__()
        for n, key in enumerate(inner):
            yield key
            if n == 0:
                self["some_other_source"] = {"consecutive_fails": 1}


def test_a_clear_drops_the_cooldown_with_one_pop_while_other_sources_are_recorded():
    """Codex 6095577140: forget() scanned the table, so an attempt recorded
    for another source during the scan raised and the cooldown survived.
    The table is keyed by the canonical name, so forget never scans it."""
    state = OperatorState()
    state.cooldowns.record_attempt("Asset_Registries", success=False, error="x")
    state.cooldowns.record_attempt("Other", success=False, error="y")
    state.cooldowns._sources = _GrowsWhileRead(state.cooldowns._sources)
    state.cooldowns.forget("Asset Registries")
    assert state.cooldowns.get_status("Asset_Registries") is None
    assert state.cooldowns.get_status("other")["consecutive_fails"] == 1
    state.cooldowns.forget("Asset Registries")                         # idempotent


def test_one_cooldown_entry_per_source_however_it_is_spelled():
    state = OperatorState()
    state.cooldowns.record_attempt("Asset Registries", success=False, error="x")
    state.cooldowns.record_attempt("asset-registries", success=False, error="y")
    assert state.cooldowns.get_status("Asset_Registries")["consecutive_fails"] == 2
    assert list(state.cooldowns._sources) == ["asset_registries"]
    assert state.cooldowns.can_retry("ASSET REGISTRIES") is False


def test_clears_and_attempts_on_many_threads_never_raise():
    state = OperatorState()
    errors: list[BaseException] = []
    stop = threading.Event()

    def record():
        n = 0
        while not stop.is_set():
            try:
                state.cooldowns.record_attempt(f"Source {n % 50}", success=False, error="x")
                state.cooldowns.skipped_sources()
            except BaseException as exc:                              # noqa: BLE001 - the assertion
                errors.append(exc)
            n += 1

    workers = [threading.Thread(target=record) for _ in range(4)]
    for w in workers:
        w.start()
    try:
        for n in range(3000):
            state.cooldowns.forget(f"source-{n % 50}")
    finally:
        stop.set()
        for w in workers:
            w.join(10)
    assert errors == []


def test_applying_a_clear_twice_changes_nothing_the_second_time(monkeypatch, table):
    hf._sync_repair_store("the engine", OperatorState())
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held")
    ev = RepairEvent(99, datetime.now(timezone.utc), "tiingo", "TIINGO", "cleared", None, "once", None, None, None, 1)
    assert hf._apply_clear(ev, None) is True
    hf._DETERMINISTIC_SKIPS["tiingo"] = (time.monotonic() + 3600, "held again")
    assert hf._apply_clear(ev, None) is False
    assert "tiingo" in hf._DETERMINISTIC_SKIPS


@pytest.mark.parametrize("stamp", ["the same instant", "a second earlier"])
def test_real_types_a_clear_after_the_trip_lifts_it_whatever_its_timestamp(pg_engine, monkeypatch, stamp):
    """Codex 6095402357 (2) on PostgreSQL: a clear committed after the trip
    under the source's lock, stamped equal to the trip or a second before
    it. The seq orders it after the trip, live and on restart."""
    _upgrade(pg_engine)
    hf._sync_repair_store(pg_engine, OperatorState())
    for _ in range(3):
        hf.note_repair_failure("TIINGO", REASON)
    offset = "0 seconds" if stamp == "the same instant" else "1 second"
    with pg_engine.begin() as conn:
        conn.execute(text("SELECT pg_advisory_xact_lock(hashtextextended('hermes_repair_events:tiingo', 0))"))
        conn.execute(text(
            "INSERT INTO hermes_repair_events (recorded_at, source_key, source_name, event, reason, seq) "
            "SELECT max(recorded_at) - CAST(:off AS interval), 'tiingo', 'TIINGO', 'cleared', 'after', max(seq) + 1 "
            "FROM hermes_repair_events WHERE source_key = 'tiingo'"), {"off": offset})
    hf._sync_repair_store(pg_engine, OperatorState())
    held_live = hf.repair_deterministically_blocked("TIINGO") is not None
    _restart(monkeypatch)
    hf._sync_repair_store(pg_engine, OperatorState())
    assert held_live is False and hf.repair_deterministically_blocked("TIINGO") is None


def test_real_types_two_rows_cannot_share_a_sources_number(pg_engine):
    _upgrade(pg_engine)
    store_mod.RepairEventStore(pg_engine).record("cleared", "tiingo", "TIINGO", reason="first")
    with pytest.raises(Exception, match="uq_hermes_repair_events_source_seq"):
        with pg_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO hermes_repair_events (source_key, source_name, event, reason, seq) "
                "VALUES ('tiingo', 'TIINGO', 'cleared', 'skipped the lock', 1)"))
