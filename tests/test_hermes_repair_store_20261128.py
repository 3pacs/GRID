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
    monkeypatch.setattr(hf, "_REPAIR_STORE", {"store": None, "engine": None, "last_event_id": 0, "restored": False})


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
        self.absent = False
        self.fail_reads = False
        self.fail_writes = False
        self.reads = 0

    def available(self) -> bool:
        return not self.absent

    def record(self, event, source_key, source_name, *, fingerprint=None, reason=None,
               skip_until=None, strikes=None, recorded_at=None) -> int:
        if self.fail_writes:
            raise RuntimeError("connection reset")
        ev = RepairEvent(len(self.events) + 1, recorded_at or datetime.now(timezone.utc), source_key, source_name,
                         event, fingerprint, None if reason is None else str(reason)[:300],
                         skip_until, strikes)
        self.events.append(ev)
        return ev.id

    def read_since(self, since):
        self.reads += 1
        if self.fail_reads:
            raise RuntimeError("canceling statement due to statement timeout")
        return [e for e in self.events if e.recorded_at > since]

    def read_clears_after(self, event_id):
        self.reads += 1
        if self.fail_reads:
            raise RuntimeError("canceling statement due to statement timeout")
        return [e for e in self.events if e.event == "cleared" and e.id > event_id]


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


def test_a_lost_long_skip_write_leaves_the_next_failure_to_trip_it():
    """repair_failed stored, its long_skip write lost: the history the live
    code could not have left. The count stops one short, so the next failure
    trips the skip instead of the count running past the threshold."""
    events = [RepairEvent(n, FAILED_AT + timedelta(minutes=n), "s", "S", "repair_failed", "x", "x")
              for n in range(1, 4)]
    state = replay(events, FAILED_AT + timedelta(hours=1), threshold=3)
    assert state.sources["s"].repeated == ["x", 2, 0]
    assert state.sources["s"].skip_until is None


def test_an_ended_skip_is_not_restored_and_a_clear_drops_everything():
    until = FAILED_AT + timedelta(hours=24)
    events = [
        RepairEvent(1, FAILED_AT, "s", "S", "long_skip", "x", "same failure 3x", until, 1),
        RepairEvent(2, FAILED_AT, "t", "T", "long_skip", "x", "same failure 3x", until, 1),
        RepairEvent(3, FAILED_AT + timedelta(hours=1), "t", "T", "cleared", None, "provider fixed"),
    ]
    in_force = replay(events, until - timedelta(seconds=1), threshold=3)
    assert in_force.sources["s"].skip_until == until
    assert in_force.sources["t"].skip_until is None and in_force.sources["t"].repeated is None
    ended = replay(events, until, threshold=3)
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


SECRET_REASON = ("HTTPError: 403 for url https://api.example.test/v2/aggs?apiKey=sk_live_0123456789abcdef"
                 "&ticker=FNGR (token=abcdef0123456789abcdef)")


def test_reasons_are_redacted_before_they_are_stored_and_still_count_alike(monkeypatch, table):
    hf._REPAIR_STORE.update(store=table, engine="the engine")
    for _ in range(3):
        hf.note_repair_failure("TIINGO", SECRET_REASON)
    live = _tables()

    stored = " ".join(f"{e.fingerprint} {e.reason}" for e in table.events)
    assert "sk_live_0123456789abcdef" not in stored and "abcdef0123456789abcdef" not in stored
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
    secret = real.record("cleared", "tiingo", "TIINGO", fingerprint=SECRET_REASON, reason=SECRET_REASON)
    with pg_engine.connect() as conn:
        row = conn.execute(text("SELECT fingerprint || ' ' || reason FROM hermes_repair_events WHERE id = :i"),
                           {"i": secret}).scalar()
    assert "sk_live_0123456789abcdef" not in row and "abcdef0123456789abcdef" not in row
    with pg_engine.begin() as conn:
        conn.execute(text("DELETE FROM hermes_repair_events WHERE id = :i"), {"i": secret})

    events = real.read_since(datetime(2000, 1, 1, tzinfo=timezone.utc))
    assert [e.event for e in events] == ["repair_failed", "long_skip", "cleared"]
    assert len(events[0].reason) == 300 and events[1].skip_until == until and events[1].strikes == 1
    assert [e.id for e in real.read_clears_after(second)] == [clear]
    assert real.read_clears_after(clear) == []
    with pg_engine.connect() as conn:
        assert conn.execute(text("SELECT DISTINCT release FROM hermes_repair_events")).scalar() == "test-release"
    with pytest.raises(ValueError):
        real.record("other", "s", "S")


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
