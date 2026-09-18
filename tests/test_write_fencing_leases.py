"""GRID W4f — cross-process lease-guarded writes for the research loop.

Closes the two cross-process residual cases documented in
docs/handoffs/2026-09-18/fable-w4b-runstate.md ("What still lets a worker
write after loss of ownership"):

  1. The mid-iteration hypothesis_registry state UPDATE (after the
     backtest returns) was not re-checked against anything -- the single
     largest remaining window.
  2. Cross-process fencing did not exist at all: the in-process
     ``_AutoresearchGenerationTracker`` is one Python int in one process's
     memory and cannot fence a second process or a worker surviving a
     restart.

This file is pure-Python: fake connections/cursors simulate a
``research_leases`` row (and, for one test, simulate Postgres's row-lock
blocking behavior) -- no real database. The one test that needs real
``SELECT ... FOR UPDATE`` semantics against actual concurrent connections
lives in tests/test_write_fencing_live_db.py, gated on GRID_TEST_DB_URL.

Run with:
    DB_PASSWORD=testpass PYTHONUTF8=1 python -m pytest tests/test_write_fencing_leases.py -q
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import governance.leases as leases  # noqa: E402
import scripts.autoresearch as autoresearch  # noqa: E402
from governance.leases import OwnershipLost, guarded_write, guarded_write_dbapi, run_guarded_dbapi  # noqa: E402

_NOW = datetime.now(timezone.utc)
_FUTURE = _NOW + timedelta(minutes=5)
_PAST = _NOW - timedelta(minutes=5)


# ─────────────────────────────────────────────────────────────────────────
# Shared fakes
# ─────────────────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, row: Any):
        self._row = row

    def fetchone(self):
        return self._row


class _FakeSAConn:
    """Stand-in for a SQLAlchemy Connection, tracking every statement it
    was asked to execute. Only understands the ``FOR UPDATE`` lease-lock
    query (answered from ``generation``/``expires_at``) and otherwise just
    records the statement -- enough to prove whether a sink statement
    (e.g. the validation_results INSERT) was ever reached.
    """

    def __init__(self, generation: int, expires_at: Any):
        self.generation = generation
        self.expires_at = expires_at
        self.executed: list[tuple[str, Any]] = []

    def execute(self, stmt: Any, params: Any = None):
        sql = str(stmt)
        self.executed.append((sql, params))
        if "FOR UPDATE" in sql:
            return _FakeResult((self.generation, self.expires_at))
        return _FakeResult(None)


class _FakeCursor:
    """psycopg2-cursor-flavored equivalent of _FakeSAConn, %s-style."""

    def __init__(self, generation: int, expires_at: Any):
        self.generation = generation
        self.expires_at = expires_at
        self.executed: list[tuple[str, Any]] = []
        self._last: Any = None

    def execute(self, sql: str, params: Any = None):
        self.executed.append((sql, params))
        if "FOR UPDATE" in sql:
            self._last = (self.generation, self.expires_at)
        else:
            self._last = None

    def fetchone(self):
        return self._last


class _FakePgConn:
    """Stand-in for a psycopg2 connection: one fixed cursor (so a test can
    inspect everything that was ever executed through it), autocommit
    toggling, and commit()/rollback() bookkeeping."""

    def __init__(self, generation: int, expires_at: Any):
        self.autocommit = True
        self._cur = _FakeCursor(generation, expires_at)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True

    def rollback(self):
        self.rolled_back = True


# ─────────────────────────────────────────────────────────────────────────
# (a) every sink from the trace: an abandoned (stale-generation) worker
#     cannot commit ANY of them.
# ─────────────────────────────────────────────────────────────────────────

# Each entry mirrors the LITERAL statement at its named call site in
# scripts/autoresearch.py / validation/backtest.py, so this is a direct
# trace-to-test mapping, not a stand-in approximation.
DBAPI_SINKS = [
    (
        "hypothesis_insert (scripts/autoresearch.py::_insert_hypothesis)",
        "INSERT INTO hypothesis_registry "
        "(statement, layer, feature_ids, lag_structure, proposed_metric, proposed_threshold, state) "
        "VALUES (%s, %s, %s, %s, %s, %s, 'TESTING') RETURNING id",
        ("stmt", "REGIME", [1, 2], "{}", "sharpe", 0.5),
    ),
    (
        "hypothesis_update_failed_on_backtest_exception (scripts/autoresearch.py::_mark_failed)",
        "UPDATE hypothesis_registry SET state='FAILED', kill_reason=%s WHERE id=%s",
        ("Backtest error: boom", 1),
    ),
    (
        "hypothesis_update_state_after_backtest -- residual case #1, mid-iteration "
        "(scripts/autoresearch.py::_update_state)",
        "UPDATE hypothesis_registry SET state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s",
        ("PASSED", None, 1),
    ),
    (
        "model_registry_insert (scripts/autoresearch.py::_create_model_from_hypothesis)",
        "INSERT INTO model_registry "
        "(name, layer, version, state, hypothesis_id, validation_run_id, "
        " feature_set, parameter_snapshot) "
        "VALUES (%s, %s, %s, 'CANDIDATE', %s, %s, %s, %s) RETURNING id",
        ("hyp-1-regime", "REGIME", "20260918-000000", 1, None, [1, 2], "{}"),
    ),
]


@pytest.mark.parametrize("label,sql,params", DBAPI_SINKS, ids=[s[0] for s in DBAPI_SINKS])
def test_stale_generation_cannot_commit_any_dbapi_sink(label, sql, params):
    """A worker holding generation 4 while the lease is now at generation 5
    (the real, live current generation) must be rejected BEFORE its
    statement ever reaches the cursor -- for every one of the four raw
    psycopg2 write sites scripts/autoresearch.py owns."""
    pg = _FakePgConn(generation=5, expires_at=_FUTURE)

    def fn(cur):
        cur.execute(sql, params)
        return "should never happen"

    with pytest.raises(OwnershipLost) as excinfo:
        run_guarded_dbapi(pg, "autoresearch", generation=4, fn=fn)

    assert excinfo.value.generation == 4
    assert excinfo.value.current_generation == 5
    # The lock-check query ran (exactly once); the sink statement did not.
    assert len(pg._cur.executed) == 1
    assert "FOR UPDATE" in pg._cur.executed[0][0]
    assert not any(sql in executed_sql for executed_sql, _ in pg._cur.executed)
    # And the transaction was rolled back, never committed.
    assert pg.committed is False
    assert pg.rolled_back is True


@pytest.mark.parametrize("label,sql,params", DBAPI_SINKS, ids=[s[0] for s in DBAPI_SINKS])
def test_current_generation_commits_the_same_dbapi_sink(label, sql, params):
    """Mirror case: a worker whose generation IS current must be allowed
    through to the real write (proves the guard doesn't false-positive)."""
    pg = _FakePgConn(generation=5, expires_at=_FUTURE)

    def fn(cur):
        cur.execute(sql, params)
        return "ok"

    result = run_guarded_dbapi(pg, "autoresearch", generation=5, fn=fn)

    assert result == "ok"
    assert pg.committed is True
    assert pg.rolled_back is False
    assert any(sql in executed_sql for executed_sql, _ in pg._cur.executed)


def test_stale_generation_cannot_commit_validation_results_insert():
    """The fifth sink -- validation_results, written inside
    validation/backtest.py::_store_result via the SQLAlchemy flavor
    (WalkForwardBacktest's optional write_guard hook)."""
    conn = _FakeSAConn(generation=5, expires_at=_FUTURE)
    insert_sql = text(
        "INSERT INTO validation_results "
        "(hypothesis_id, vintage_policy, era_results, full_period_metrics, "
        " baseline_comparison, simplicity_comparison, walk_forward_splits, "
        " cost_assumption_bps, overall_verdict, gate_detail) "
        "VALUES (:hid, :vp, :er, :fpm, :bc, :sc, :wfs, :cab, :ov, :gd)"
    )

    def fn(conn_):
        conn_.execute(insert_sql, {"hid": 1})

    with pytest.raises(OwnershipLost):
        guarded_write(conn, "autoresearch", generation=4, fn=fn)

    assert not any("INSERT INTO validation_results" in sql for sql, _ in conn.executed)


def test_expired_lease_rejects_even_a_matching_generation():
    """generation matching is not enough on its own -- an expired lease
    (nobody heartbeated it in time) must also be rejected, matching
    heartbeat's own "renew only if not yet expired" contract."""
    pg = _FakePgConn(generation=5, expires_at=_PAST)

    def fn(cur):
        cur.execute("INSERT INTO hypothesis_registry (statement) VALUES (%s)", ("x",))

    with pytest.raises(OwnershipLost):
        run_guarded_dbapi(pg, "autoresearch", generation=5, fn=fn)
    assert pg.committed is False


# ─────────────────────────────────────────────────────────────────────────
# (b) ordering: a write that started before loss must be impossible to
#     have "commit after loss" -- the row lock holds for the transaction's
#     whole lifetime, so a concurrent generation bump cannot land while
#     `fn` is still running.
# ─────────────────────────────────────────────────────────────────────────

class WouldBlock(RuntimeError):
    """Test-only stand-in for what a REAL Postgres FOR UPDATE does here:
    BLOCKS the second transaction until the first commits or rolls back.
    This fake cannot block a single Python thread against itself, so it
    raises instead -- the point is the same either way: the second
    transaction cannot see or change the row while the first holds it."""


class _SharedLeaseRow:
    def __init__(self, generation: int, expires_at: Any):
        self.generation = generation
        self.expires_at = expires_at
        self.locked = False


class _LockingCursor:
    def __init__(self, row: _SharedLeaseRow):
        self.row = row
        self.executed: list[str] = []
        self._last: Any = None
        self._holds_lock = False

    def execute(self, sql: str, params: Any = None):
        self.executed.append(sql)
        if "FOR UPDATE" in sql:
            if self.row.locked and not self._holds_lock:
                raise WouldBlock(
                    "row already locked by another transaction -- real "
                    "Postgres blocks here instead of raising, until that "
                    "transaction commits/rolls back"
                )
            self.row.locked = True
            self._holds_lock = True
            self._last = (self.row.generation, self.row.expires_at)
        else:
            self._last = None

    def fetchone(self):
        return self._last

    def release(self):
        if self._holds_lock:
            self.row.locked = False
            self._holds_lock = False


class _LockingPgConn:
    def __init__(self, row: _SharedLeaseRow):
        self.autocommit = True
        self.row = row
        self._cur = _LockingCursor(row)
        self.committed = False
        self.rolled_back = False

    def cursor(self):
        return self._cur

    def commit(self):
        self.committed = True
        self._cur.release()

    def rollback(self):
        self.rolled_back = True
        self._cur.release()


def test_lock_ordering_makes_commit_after_loss_impossible():
    """Documents and demonstrates the exact ordering that
    governance/leases.py's module docstring argues for:

      1. Transaction A takes the row lock (FOR UPDATE) and passes its
         generation check.
      2. WHILE A's transaction is still open (fn is running, lock still
         held), a concurrent attempt to advance the generation --
         representing a competing acquire()'s own FOR UPDATE -- cannot
         even READ the row, let alone change it. In real Postgres this
         blocks; here it raises WouldBlock, which is the observable proxy
         for "cannot happen concurrently".
      3. Only after A's transaction ends (commit releases the lock) can
         the generation actually advance.
      4. A SUBSEQUENT attempt using A's now-stale generation is rejected
         with OwnershipLost, never resurrecting a write.

    This is what makes "started before loss, committed after loss"
    structurally impossible rather than merely unlikely: there is no
    window in which the row can change while A's write is in flight.
    """
    row = _SharedLeaseRow(generation=5, expires_at=_FUTURE)
    committed: list[str] = []

    def fn(cur):
        # Simulate a concurrent acquire() attempted WHILE this transaction
        # (which already passed its generation check and holds the lock)
        # is still mid-flight.
        concurrent = _LockingPgConn(row)
        with pytest.raises(WouldBlock):
            concurrent.cursor().execute(
                "SELECT generation, expires_at FROM research_leases WHERE name = %s FOR UPDATE",
                ("autoresearch",),
            )
        # The row could not have changed underneath us -- proceed to write.
        cur.execute(
            "UPDATE hypothesis_registry SET state=%s, kill_reason=%s, updated_at=NOW() WHERE id=%s",
            ("PASSED", None, 1),
        )
        committed.append("A")

    conn_a = _LockingPgConn(row)
    run_guarded_dbapi(conn_a, "autoresearch", generation=5, fn=fn)

    assert committed == ["A"]
    assert conn_a.committed is True

    # NOW that A's transaction has ended (lock released), the generation
    # can actually advance -- this is what the earlier concurrent attempt
    # was blocked from doing while A was in flight.
    row.generation = 6

    # A's own generation (5) is now stale. Any later attempt under it --
    # even one that believes it "started before" this bump -- must be
    # rejected before it can touch anything.
    def fn_stale(cur):
        committed.append("STALE — SHOULD NEVER RUN")

    with pytest.raises(OwnershipLost):
        run_guarded_dbapi(_LockingPgConn(row), "autoresearch", generation=5, fn=fn_stale)

    assert committed == ["A"]  # the stale attempt never executed fn


# ─────────────────────────────────────────────────────────────────────────
# (c) heartbeat expiry
# ─────────────────────────────────────────────────────────────────────────

class _FakeHeartbeatConn:
    def __init__(self, table: dict):
        self.table = table

    def execute(self, stmt: Any, params: Any):
        sql = str(stmt)
        assert "UPDATE research_leases" in sql and "heartbeat_at" in sql, f"unexpected SQL: {sql}"
        row = self.table.get(params["name"])
        if row is None:
            return _FakeResult(None)
        if (
            row["owner_id"] == params["owner"]
            and row["generation"] == params["gen"]
            and row["expires_at"] > params["now"]
        ):
            row["expires_at"] = params["expires_at"]
            row["heartbeat_at"] = params["now"]
            return _FakeResult((row["generation"],))
        return _FakeResult(None)


class _CtxWrap:
    def __init__(self, conn: Any):
        self.conn = conn

    def __enter__(self):
        return self.conn

    def __exit__(self, *exc):
        return False


class _FakeHeartbeatEngine:
    def __init__(self, table: dict):
        self.table = table

    def begin(self):
        return _CtxWrap(_FakeHeartbeatConn(self.table))


def test_heartbeat_renews_a_live_lease_but_refuses_an_already_expired_one():
    now = datetime.now(timezone.utc)
    table = {
        "autoresearch": {
            "owner_id": "worker-1",
            "generation": 5,
            "expires_at": now + timedelta(seconds=30),
            "heartbeat_at": now,
        }
    }
    engine = _FakeHeartbeatEngine(table)

    assert leases.heartbeat(engine, "autoresearch", "worker-1", 5, ttl_seconds=30) is True
    assert table["autoresearch"]["expires_at"] > now + timedelta(seconds=29)

    # Simulate the lease having gone stale (no heartbeat arrived in time,
    # e.g. the operator gave up and stopped calling heartbeat()).
    table["autoresearch"]["expires_at"] = now - timedelta(seconds=1)
    assert leases.heartbeat(engine, "autoresearch", "worker-1", 5, ttl_seconds=30) is False

    # A mismatched owner/generation (superseded, even if not yet expired)
    # must also be refused.
    table["autoresearch"]["expires_at"] = now + timedelta(seconds=30)
    assert leases.heartbeat(engine, "autoresearch", "worker-1", 4, ttl_seconds=30) is False
    assert leases.heartbeat(engine, "autoresearch", "worker-2", 5, ttl_seconds=30) is False


# ─────────────────────────────────────────────────────────────────────────
# (e) no notification or promotion call on a fenced path -- integration
#     test through the real run_autoresearch() loop, closing residual
#     case #1 (mid-iteration state UPDATE) end-to-end.
# ─────────────────────────────────────────────────────────────────────────

class _RecordingSnapshotStore:
    events: list[dict[str, Any]] = []

    def __init__(self, db_engine: Any = None) -> None:
        self.db_engine = db_engine

    def save_snapshot(self, category, payload, as_of_date=None, subcategory=None, metrics=None, actor_name=None):
        _RecordingSnapshotStore.events.append(
            {"category": category, "subcategory": subcategory, "payload": dict(payload)}
        )
        return len(_RecordingSnapshotStore.events)


class _FakeDB:
    def __init__(self) -> None:
        self.hypotheses: dict[int, dict[str, Any]] = {}
        self.models: dict[int, int] = {}
        self.next_hyp_id = 1
        self.next_model_id = 1
        self.hyp_insert_count = 0
        self.model_insert_count = 0


class _RetryCursor:
    def __init__(self, db: _FakeDB):
        self.db = db
        self._result: Any = None

    def execute(self, sql: str, params: Any = None) -> None:
        s = " ".join(sql.split())
        if s.startswith("SELECT id, state FROM hypothesis_registry"):
            statement, layer = params
            match = None
            for hyp_id, row in self.db.hypotheses.items():
                if row["statement"] == statement and row["layer"] == layer:
                    match = (hyp_id, row["state"])
            self._result = match
        elif s.startswith("INSERT INTO hypothesis_registry"):
            self.db.hyp_insert_count += 1
            statement, layer = params[0], params[1]
            hyp_id = self.db.next_hyp_id
            self.db.next_hyp_id += 1
            self.db.hypotheses[hyp_id] = {"statement": statement, "layer": layer, "state": "TESTING"}
            self._result = (hyp_id,)
        elif s.startswith("UPDATE hypothesis_registry SET state"):
            # This is the mid-iteration write under test -- the fake
            # run_guarded_dbapi below raises OwnershipLost BEFORE this
            # cursor.execute ever runs for that call, so reaching here at
            # all on the fenced attempt would itself be a test failure.
            state, _kill_reason, hyp_id = params
            self.db.hypotheses[hyp_id]["state"] = state
            self._result = None
        elif s.startswith("SELECT id FROM model_registry WHERE hypothesis_id"):
            (hyp_id,) = params
            self._result = (self.db.models[hyp_id],) if hyp_id in self.db.models else None
        elif s.startswith("INSERT INTO model_registry"):
            self.db.model_insert_count += 1
            hyp_id = params[3]
            model_id = self.db.next_model_id
            self.db.next_model_id += 1
            self.db.models[hyp_id] = model_id
            self._result = (model_id,)
        else:
            self._result = None

    def fetchone(self):
        return self._result

    def fetchall(self):
        return []

    def close(self):
        pass


class _RetryConnection:
    def __init__(self, db: _FakeDB):
        self.db = db
        self.autocommit = False
        self.closed = False

    def cursor(self):
        return _RetryCursor(self.db)

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


class _PassingBacktester:
    def run_validation(self, **kwargs):
        return {
            "overall_verdict": "PASS",
            "full_period_metrics": {"sharpe": 1.2, "return": 0.1, "max_drawdown": 0.05},
            "baseline_comparison": {"sharpe": 0.1},
            "era_results": [],
        }


class _ChattyOllama:
    is_available = True

    def chat(self, *a, **k):
        return "irrelevant — parse_hypothesis_json is stubbed"


@pytest.fixture(autouse=True)
def _reset_recording_store(monkeypatch):
    _RecordingSnapshotStore.events = []
    import store.snapshots as snapshots_module

    monkeypatch.setattr(snapshots_module, "AnalyticalSnapshotStore", _RecordingSnapshotStore)
    monkeypatch.setattr(autoresearch, "_ortho_cache", None)
    monkeypatch.setattr(autoresearch, "get_engine", lambda: object())
    monkeypatch.setattr(autoresearch, "PITStore", lambda engine: object())
    monkeypatch.setattr(autoresearch, "OllamaReasoner", lambda ollama: object())
    monkeypatch.setattr(autoresearch, "get_feature_list", lambda cur: "(no features)")
    monkeypatch.setattr(autoresearch, "get_feature_name_map", lambda cur: {})
    monkeypatch.setattr(autoresearch, "get_market_snapshot", lambda cur: "(no data)")
    monkeypatch.setattr(autoresearch, "_select_orthogonal_features", lambda cur, **kw: [])
    yield


def test_lease_loss_mid_iteration_fences_the_state_write_and_suppresses_notify_and_model(monkeypatch):
    """The core end-to-end proof: a hypothesis PASSES its backtest, but the
    cross-process lease is lost exactly at the mid-iteration
    hypothesis_registry state UPDATE (residual case #1) -- the write must
    be rejected, and NEITHER the model_registry insert NOR
    notify_on_pass may fire afterward, because run_autoresearch() breaks
    out of the loop the moment OwnershipLost is raised.
    """
    db = _FakeDB()
    notify_calls: list[Any] = []

    fixed_hyp = {
        "statement": "When VIX spikes, SP500 mean-reverts within 5 days",
        "feature_ids": [1, 2],
        "lag_structure": {"1": 0, "2": 5},
        "layer": "REGIME",
        "proposed_metric": "sharpe",
        "proposed_threshold": 0.5,
    }

    import psycopg2
    import scripts.notify as notify_module

    monkeypatch.setattr(psycopg2, "connect", lambda **kwargs: _RetryConnection(db))
    monkeypatch.setattr(autoresearch, "WalkForwardBacktest", lambda engine, pit, **kw: _PassingBacktester())
    monkeypatch.setattr(autoresearch, "get_ollama", lambda: _ChattyOllama())
    monkeypatch.setattr(autoresearch, "parse_hypothesis_json", lambda text: dict(fixed_hyp))
    monkeypatch.setattr(notify_module, "notify_on_pass", lambda attempt: notify_calls.append(attempt))

    call_count = {"n": 0}

    def _fake_run_guarded_dbapi(pg_conn, lease_name, generation, fn):
        call_count["n"] += 1
        # Call #1 is the hypothesis INSERT — let it through normally so
        # there is a real hyp_id to have "passed" against. Call #2 is the
        # mid-iteration state UPDATE (residual case #1) — this is where
        # the lease is lost.
        if call_count["n"] == 2:
            raise OwnershipLost(lease_name, generation, generation + 1)
        return fn(pg_conn.cursor())

    monkeypatch.setattr(leases, "run_guarded_dbapi", _fake_run_guarded_dbapi)

    result = autoresearch.run_autoresearch(
        max_iterations=1, run_id="fenced-run-1", lease_generation=99, lease_owner_id="test-owner",
    )

    assert result["status"] == "fenced"
    assert result["fenced"] is True
    assert result["all_attempts"][-1]["fenced"] is True
    assert result["all_attempts"][-1]["error"] == "fenced: cross-process lease lost"

    # The hypothesis was inserted (call #1 succeeded) but its state was
    # NEVER updated to PASSED — the fenced write never landed.
    assert db.hyp_insert_count == 1
    assert db.hypotheses[1]["state"] == "TESTING"

    # Neither downstream side effect happened.
    assert db.model_insert_count == 0
    assert notify_calls == []

    # The run record itself reports "fenced" (this write is deliberately
    # NOT lease-guarded -- see run_autoresearch's comment at final_status
    # -- so it is allowed to succeed and report the loss).
    end_event = _RecordingSnapshotStore.events[-1]
    assert end_event["payload"]["status"] == "fenced"
    assert any(r.startswith("lease_lost_") for r in end_event["payload"]["skip_reasons"])
