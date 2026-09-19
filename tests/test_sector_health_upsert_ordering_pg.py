"""Real-PostgreSQL concurrency tests for the sector-health snapshot upsert
ordering guard and the in-process state-marker lock.

Covers two review findings from the 2026-09-19 release-controller review of
PR #580 (branch ``fable/hermes-sector-step-20260919``):

1. **Equal-``as_of`` handling** (``intelligence/sector_health.py::
   snapshot_all_sectors``): the ``ON CONFLICT ... DO UPDATE`` guard is now
   strict ``WHERE ... as_of IS NULL OR as_of < EXCLUDED.as_of`` (was
   ``<=``). Tie rule: **first committed wins on equal as_of** — whichever
   attempt's write reaches Postgres first, for a given ``(sector_name,
   snapshot_date)``, keeps its row even if a later-arriving write carries
   the exact same ``as_of``.
2. **Check-to-write race on the state marker**
   (``scripts/hermes_operator.py::_maybe_run_sector_health_snapshot``):
   ``_SECTOR_HEALTH_STATE_LOCK`` (module-level ``threading.Lock``, not on
   ``OperatorState`` because that is serialised via ``to_dict()``) now
   guards (a) the attempt-start block (token bump + attempt fields), (b)
   the whole of ``_commit`` (token check + state writes), and (c) the
   timeout-path token bump in ``_run_sector_and_intelligence_steps``, so
   an abandoned worker's belated ``_commit`` can never pass its token check
   and then lose a race to write after a newer attempt has already
   committed.

Every test here uses REAL threads and REAL Postgres transactions against a
per-test schema (``shs_order_<pid>_<n>``) holding a ``sector_health_snapshots``
table shaped exactly like ``migrations/0028_sector_health_snapshots.sql``.
This file SKIPS cleanly (via the shared ``pg_engine`` fixture) when no
PostgreSQL is reachable — it is intended to be executed by the coordinator
against a disposable database via ``GRID_TEST_DB_URL``, not merged CI-green
without ever having run for real.

The pure/no-DB coverage for the "superseded" outcome classification and for
the lock actually being acquired lives in
``tests/test_hermes_sector_schedule.py`` (``TestSupersededOutcome``,
``TestStateMarkerLock``) — this file is real-DB/real-thread coverage only.
"""
from __future__ import annotations

import itertools
import os
import re
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pytest
from sqlalchemy import create_engine, text

import intelligence.sector_health as sh
from scripts import hermes_operator as ho
from scripts.hermes_health import OperatorState

_SCHEMA_COUNTER = itertools.count()

# Two sectors is enough to prove ordering; kept small so every scenario
# below runs in well under a second of real Postgres round-trips.
_TWO_SECTORS: dict[str, Any] = {"TestSectorAlpha": {}, "TestSectorBeta": {}}


def _fresh_state() -> OperatorState:
    return OperatorState()


@pytest.fixture
def schema_engine(pg_engine):
    """A SQLAlchemy engine scoped, via ``search_path``, to a fresh, unique
    schema holding a ``sector_health_snapshots`` table shaped exactly like
    ``migrations/0028_sector_health_snapshots.sql``. The engine is what
    ``snapshot_all_sectors`` (and ``_maybe_run_sector_health_snapshot``,
    via ``engine.begin()``) writes through in these tests, so the table's
    unqualified name resolves inside the private schema instead of the
    shared ``public`` one. Always dropped (CASCADE) in a ``finally``.
    """
    n = next(_SCHEMA_COUNTER)
    schema = f"shs_order_{os.getpid()}_{n}"
    assert re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", schema), schema

    url = pg_engine.url.render_as_string(hide_password=False)
    engine = create_engine(url, connect_args={"options": f"-c search_path={schema}"})
    try:
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            conn.execute(
                text(
                    """
                    CREATE TABLE sector_health_snapshots (
                        id              SERIAL PRIMARY KEY,
                        sector_name     TEXT NOT NULL,
                        score           NUMERIC,
                        components      JSONB,
                        snapshot_date   DATE NOT NULL,
                        as_of           TIMESTAMPTZ DEFAULT NOW(),
                        UNIQUE (sector_name, snapshot_date)
                    )
                    """
                )
            )
        yield engine
    finally:
        try:
            with pg_engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        finally:
            engine.dispose()


def _rows(engine, snapshot_date: date) -> dict[str, tuple[float, datetime]]:
    """Fresh-connection read of every row for ``snapshot_date``, keyed by
    sector_name -> (score, as_of)."""
    with engine.connect() as conn:
        result = conn.execute(
            text(
                "SELECT sector_name, score, as_of FROM sector_health_snapshots "
                "WHERE snapshot_date = :d"
            ),
            {"d": snapshot_date},
        )
        return {row[0]: (float(row[1]), row[2]) for row in result.fetchall()}


def _stub(score: float):
    return lambda engine, name: {
        "score": score, "trend_30d": 0.0, "components": {"stub": True},
    }


# ─── (a) Old attempt paused before its write, newer attempt commits first ──


class TestOldAttemptPausedNewerCommitsFirst:
    def test_should_continue_aborts_old_attempt_before_its_upsert(
        self, schema_engine, monkeypatch,
    ) -> None:
        """With the in-process `should_continue` guard wired the way
        `_maybe_run_sector_health_snapshot` wires it (checked against a
        shared token), the OLD attempt — paused mid-compute while the NEW
        attempt runs to completion — must abort before ever attempting its
        upsert (`aborted_stale=True`, `snapshots_written == 0`,
        `snapshots_stale_skipped == 0`: it never got far enough to be
        DB-rejected). Real rows end up exclusively the NEW attempt's."""
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)

        snapshot_date = date(2026, 9, 19)
        t_old = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
        t_new = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        old_started = threading.Event()
        release_old = threading.Event()
        role: dict[int, str] = {}
        shared_token = {"value": 1}  # attempt 1 is "current" until flipped

        def _compute(engine, name):
            r = role.get(threading.get_ident(), "new")
            if r == "old":
                old_started.set()
                assert release_old.wait(timeout=10), "release_old never signalled"
                return {"score": 41.0, "trend_30d": 0.0, "components": {"stub": True}}
            return {"score": 77.0, "trend_30d": 0.0, "components": {"stub": True}}

        monkeypatch.setattr(sh, "compute_sector_health", _compute)

        result_holder: dict[str, Any] = {}

        def _old_attempt() -> None:
            role[threading.get_ident()] = "old"
            result_holder["old"] = sh.snapshot_all_sectors(
                schema_engine, snapshot_date=snapshot_date, computed_at=t_old,
                should_continue=lambda: shared_token["value"] == 1,
            )

        t = threading.Thread(target=_old_attempt)
        t.start()
        assert old_started.wait(timeout=10), "old attempt never started computing"

        # While attempt 1 sits paused inside its (mocked) compute call,
        # attempt 2 runs to completion, unblocked, with a newer as_of.
        new_result = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t_new,
        )
        assert new_result["snapshots_written"] == len(_TWO_SECTORS)

        # A later attempt has superseded attempt 1 — flip the shared token
        # (this is what hermes_operator's real token bump does) and let
        # attempt 1 resume.
        shared_token["value"] = 2
        release_old.set()
        t.join(timeout=10)
        assert not t.is_alive()

        old_result = result_holder["old"]
        assert old_result["snapshots_written"] == 0
        assert old_result["aborted_stale"] is True
        assert old_result["snapshots_stale_skipped"] == 0, (
            "should_continue caught it before the first upsert — nothing "
            "reached the DB guard, so nothing should be counted as "
            "DB-rejected either"
        )

        rows = _rows(schema_engine, snapshot_date)
        for name in _TWO_SECTORS:
            score, as_of = rows[name]
            assert score == 77.0
            assert as_of == t_new

    def test_db_guard_alone_rejects_old_write_when_should_continue_is_none(
        self, schema_engine, monkeypatch,
    ) -> None:
        """Variant: bypass the in-process `should_continue` short-circuit
        entirely (`should_continue=None`) so the OLD attempt genuinely
        tries to write to Postgres after the NEWER attempt has already
        committed — exercising the pure DB-level `as_of` guard, not the
        in-process guard covered above."""
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)
        snapshot_date = date(2026, 9, 19)
        t_old = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
        t_new = datetime(2026, 9, 19, 3, 5, tzinfo=timezone.utc)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(77.0))
        new_result = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t_new,
        )
        assert new_result["snapshots_written"] == len(_TWO_SECTORS)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(41.0))
        old_result = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t_old,
            should_continue=None,
        )

        assert old_result["snapshots_written"] == 0
        assert old_result["snapshots_stale_skipped"] == len(_TWO_SECTORS)
        assert old_result["upsert_failed"] == 0

        rows = _rows(schema_engine, snapshot_date)
        for name in _TWO_SECTORS:
            score, as_of = rows[name]
            assert score == 77.0
            assert as_of == t_new


# ─── (b) Equal as_of: first committed wins ──────────────────────────────


class TestEqualAsOfTieRule:
    def test_paused_first_attempt_loses_to_earlier_committed_equal_as_of(
        self, schema_engine, monkeypatch,
    ) -> None:
        """Both attempts use the SAME computed_at. The second-started
        attempt (unblocked, runs to completion) commits FIRST; the paused
        first attempt then resumes and tries to write with an EQUAL as_of.
        Strict `<` means equal never satisfies the guard, so the first
        attempt's (later-arriving) write is rejected — first committed
        wins on ties."""
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)
        snapshot_date = date(2026, 9, 19)
        shared_as_of = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)

        started = threading.Event()
        release = threading.Event()
        role: dict[int, str] = {}

        def _compute(engine, name):
            r = role.get(threading.get_ident(), "new")
            if r == "old":
                started.set()
                assert release.wait(timeout=10)
                return {"score": 41.0, "trend_30d": 0.0, "components": {"stub": True}}
            return {"score": 77.0, "trend_30d": 0.0, "components": {"stub": True}}

        monkeypatch.setattr(sh, "compute_sector_health", _compute)

        result_holder: dict[str, Any] = {}

        def _first_attempt() -> None:
            role[threading.get_ident()] = "old"
            # should_continue=None: prove the DB guard alone (not an
            # in-process short-circuit) enforces the tie rule here.
            result_holder["first"] = sh.snapshot_all_sectors(
                schema_engine, snapshot_date=snapshot_date, computed_at=shared_as_of,
                should_continue=None,
            )

        t = threading.Thread(target=_first_attempt)
        t.start()
        assert started.wait(timeout=10)

        second_result = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=shared_as_of,
        )
        assert second_result["snapshots_written"] == len(_TWO_SECTORS)

        release.set()
        t.join(timeout=10)
        assert not t.is_alive()

        first_result = result_holder["first"]
        assert first_result["snapshots_written"] == 0
        assert first_result["snapshots_stale_skipped"] == len(_TWO_SECTORS), (
            "on an exact as_of tie, whichever write reaches Postgres SECOND "
            "must be rejected regardless of which attempt started first — "
            "first committed wins on equal as_of"
        )

        rows = _rows(schema_engine, snapshot_date)
        for name in _TWO_SECTORS:
            score, as_of = rows[name]
            assert score == 77.0, "the first-COMMITTED row must survive the tie"
            assert as_of == shared_as_of

    def test_writes_first_then_equal_as_of_retry_is_skipped(
        self, schema_engine, monkeypatch,
    ) -> None:
        """Reverse order, sequential: an attempt writes first with as_of
        t0; a later call using the SAME as_of is skipped too. The tie rule
        is symmetric — it is about commit order, not which call happens to
        be logically "the retry"."""
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)
        snapshot_date = date(2026, 9, 19)
        shared_as_of = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(55.0))
        first = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=shared_as_of,
        )
        assert first["snapshots_written"] == len(_TWO_SECTORS)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(99.0))
        second = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=shared_as_of,
        )
        assert second["snapshots_written"] == 0
        assert second["snapshots_stale_skipped"] == len(_TWO_SECTORS)

        rows = _rows(schema_engine, snapshot_date)
        for name in _TWO_SECTORS:
            score, as_of = rows[name]
            assert score == 55.0, "first-committed value must survive an equal-as_of retry"
            assert as_of == shared_as_of


# ─── (c) Newer as_of always wins, regardless of thread order ───────────────


class TestNewerAsOfAlwaysWins:
    def test_newer_write_updates_older_rows_and_a_stale_retry_is_then_rejected(
        self, schema_engine, monkeypatch,
    ) -> None:
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)
        snapshot_date = date(2026, 9, 19)
        t0 = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
        t1 = t0 + timedelta(minutes=30)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(41.0))
        older = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t0,
        )
        assert older["snapshots_written"] == len(_TWO_SECTORS)

        monkeypatch.setattr(sh, "compute_sector_health", _stub(77.0))
        newer = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t1,
        )
        assert newer["snapshots_written"] == len(_TWO_SECTORS)
        assert newer["snapshots_stale_skipped"] == 0

        # A subsequent write at the OLDER as_of must now be rejected.
        monkeypatch.setattr(sh, "compute_sector_health", _stub(12.0))
        stale_retry = sh.snapshot_all_sectors(
            schema_engine, snapshot_date=snapshot_date, computed_at=t0,
        )
        assert stale_retry["snapshots_written"] == 0
        assert stale_retry["snapshots_stale_skipped"] == len(_TWO_SECTORS)

        rows = _rows(schema_engine, snapshot_date)
        for name in _TWO_SECTORS:
            score, as_of = rows[name]
            assert score == 77.0
            assert as_of == t1


# ─── (d) State-marker race under the lock ──────────────────────────────────


class TestStateMarkerLockClosesTheRace:
    def test_lock_prevents_a_newer_attempt_from_starting_inside_an_older_commit(
        self, schema_engine, monkeypatch,
    ) -> None:
        """Reproduce, via the ``_SECTOR_HEALTH_COMMIT_TEST_HOOK`` seam, the
        exact interleaving the lock closes: while the OLD attempt's
        ``_commit`` is inside its locked section (token check, hook, then
        writes), force a NEWER attempt to start. Before this fix, the
        check-then-write pair was not atomic, so a newer attempt could
        start and finish its own commit *during* that window and the old
        attempt's writes could still land afterwards, clobbering it. Now,
        because both the check-to-write window and the newer attempt's
        attempt-start block are guarded by the same
        ``ho._SECTOR_HEALTH_STATE_LOCK``, the newer attempt's attempt-start
        cannot even begin until the old attempt's entire ``_commit`` --
        hook included -- has released the lock. Proven directly: the hook
        asserts the newer attempt has NOT finished within a short window
        while the lock is still held. Final state reflects the newer
        attempt, landing strictly after the old one, never clobbered.
        """
        monkeypatch.setattr("analysis.sector_map.SECTOR_MAP", _TWO_SECTORS)
        monkeypatch.setattr(sh, "compute_sector_health", _stub(55.0))

        state = _fresh_state()
        t_old = datetime(2026, 9, 19, 3, 0, tzinfo=timezone.utc)
        t_new = t_old + timedelta(minutes=ho.SECTOR_HEALTH_RETRY_BACKOFF_MINUTES + 1)

        new_attempt_go = threading.Event()
        new_attempt_done = threading.Event()
        hook_entered = threading.Event()

        results_old: dict[str, Any] = {}
        results_new: dict[str, Any] = {}

        def _new_attempt_thread() -> None:
            assert new_attempt_go.wait(timeout=10)
            ho._maybe_run_sector_health_snapshot(schema_engine, state, t_new, results_new)
            new_attempt_done.set()

        t = threading.Thread(target=_new_attempt_thread)
        t.start()

        def _hook() -> None:
            # Runs INSIDE the old attempt's _commit, with
            # ho._SECTOR_HEALTH_STATE_LOCK held (see _commit). Trigger the
            # newer attempt and give it a real window to try (and fail) to
            # acquire the same lock for its attempt-start block.
            hook_entered.set()
            new_attempt_go.set()
            got_in_time = new_attempt_done.wait(timeout=0.5)
            assert not got_in_time, (
                "the newer attempt completed while the old attempt still "
                "held _SECTOR_HEALTH_STATE_LOCK inside _commit — the lock "
                "is not actually serialising attempt-start against a "
                "concurrent _commit"
            )

        monkeypatch.setattr(ho, "_SECTOR_HEALTH_COMMIT_TEST_HOOK", _hook)
        try:
            ho._maybe_run_sector_health_snapshot(schema_engine, state, t_old, results_old)
        finally:
            monkeypatch.setattr(ho, "_SECTOR_HEALTH_COMMIT_TEST_HOOK", None)

        assert hook_entered.is_set(), "the test hook never ran — _commit's seam did not fire"
        t.join(timeout=10)
        assert not t.is_alive()

        # The old attempt's own commit succeeded (it was never superseded
        # — nothing had bumped its token before its check).
        assert results_old.get("sector_health_snapshot", {}).get("outcome") == "success"

        # But the newer attempt, forced to wait for the lock, ran strictly
        # afterwards and its result is what's left standing.
        assert state.last_sector_health == t_new
        assert state.last_sector_health_outcome == "success"
        assert results_new.get("sector_health_snapshot", {}).get("outcome") == "success"

        rows = _rows(schema_engine, date(2026, 9, 19))
        for name in _TWO_SECTORS:
            _score, as_of = rows[name]
            assert as_of == t_new, (
                "the newer attempt's rows must be what's left in the "
                "table, consistent with the newer state marker"
            )
