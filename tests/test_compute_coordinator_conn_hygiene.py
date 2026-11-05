"""Connection hygiene for the compute coordinator.

Regression for 2026-10-11: after a Postgres restart, /stats scans of the 22M-row
``compute_jobs`` table hit statement_timeout; the handler had no rollback/close
on error, so each failed request left a backend ``idle in transaction (aborted)``
for 10 minutes and leaked the client socket.
"""

from __future__ import annotations

import threading
from typing import Callable, Optional

import psycopg2
import psycopg2.errors
import pytest
import requests
from fastapi import HTTPException

from scripts import compute_coordinator as coordinator
from scripts import compute_job_producer as producer


def query_canceled() -> psycopg2.errors.QueryCanceled:
    return psycopg2.errors.QueryCanceled("canceling statement due to statement timeout")


class FakeCursor:
    def __init__(self, conn: "FakeConn") -> None:
        self.conn = conn
        self.last_sql = ""

    def execute(self, sql: str, params=None) -> None:
        self.conn.executed.append((sql, params))
        self.last_sql = sql
        if self.conn.on_execute:
            self.conn.on_execute(sql, len(self.conn.executed))
        exc = self.conn.fail(sql, len(self.conn.executed)) if self.conn.fail else None
        if exc is not None:
            raise exc

    def fetchone(self):
        return self.conn.rows(self.last_sql, one=True)

    def fetchall(self):
        return self.conn.rows(self.last_sql, one=False)


class FakeConn:
    def __init__(
        self,
        fail: Optional[Callable[[str, int], Optional[BaseException]]] = None,
        rows: Optional[Callable[[str, bool], object]] = None,
        on_execute: Optional[Callable[[str, int], None]] = None,
    ) -> None:
        self.autocommit = False
        self.fail = fail
        self.on_execute = on_execute
        self._rows = rows
        self.executed: list[tuple[str, object]] = []
        self.close_count = 0
        self.rollback_count = 0
        self.commit_count = 0

    def cursor(self, cursor_factory=None) -> FakeCursor:
        return FakeCursor(self)

    def rows(self, sql: str, one: bool):
        if self._rows is not None:
            return self._rows(sql, one)
        return default_rows(sql, one)

    def close(self) -> None:
        self.close_count += 1

    def rollback(self) -> None:
        self.rollback_count += 1

    def commit(self) -> None:
        self.commit_count += 1


def default_rows(sql: str, one: bool):
    if "FOR UPDATE" in sql and "compute_workers" in sql:
        return {"active_jobs": 0, "max_concurrent": 2}
    if "SELECT state FROM compute_jobs" in sql:
        return {"state": "QUEUED"} if one else [{"state": "QUEUED"}]
    if "RETURNING" in sql or "SELECT * FROM compute_jobs WHERE id" in sql:
        return {"id": 1, "state": "QUEUED"}
    if "SELECT id FROM compute_jobs WHERE state='QUEUED'" in sql:
        return {"id": 1}
    if "FROM compute_workers" in sql and "count(*)" in sql:
        return {"total": 2, "idle": 1, "busy": 1}
    return None if one else []


class HandedOut(list):
    """FakeConns given out by the patched get_conn, in order."""

    def __init__(self) -> None:
        super().__init__()
        self.factory: dict[str, Callable[[], FakeConn]] = {"make": FakeConn}


@pytest.fixture
def conns(monkeypatch) -> HandedOut:
    """Patch get_conn and reset the /stats cache for each test."""
    handed = HandedOut()

    def fake_get_conn():
        conn = handed.factory["make"]()
        handed.append(conn)
        return conn

    monkeypatch.setattr(coordinator, "get_conn", fake_get_conn)
    monkeypatch.setattr(coordinator, "_stats_cache", coordinator._StatsCache())
    return handed


def use(conns, **kwargs) -> None:
    conns.factory["make"] = lambda: FakeConn(**kwargs)


# ── get_conn / db_conn ─────────────────────────────────────────────


def test_get_conn_tags_sessions_with_application_name(monkeypatch):
    seen = {}
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: seen.update(kw) or object())
    coordinator.get_conn()
    assert seen["application_name"] == "grid-coordinator"
    assert seen["connect_timeout"] == coordinator.DB_CONNECT_TIMEOUT_SECONDS


def test_db_conn_closes_without_rollback_on_success(conns):
    with coordinator.db_conn() as conn:
        conn.cursor().execute("SELECT 1")
    assert conns[0].close_count == 1
    assert conns[0].rollback_count == 0


def test_db_conn_rolls_back_closes_and_reraises_on_error(conns):
    with pytest.raises(RuntimeError):
        with coordinator.db_conn():
            raise RuntimeError("boom")
    assert conns[0].rollback_count == 1
    assert conns[0].close_count == 1


def test_db_conn_autocommit_closes_without_rollback_on_error(conns):
    with pytest.raises(RuntimeError):
        with coordinator.db_conn(autocommit=True) as conn:
            assert conn.autocommit is True
            raise RuntimeError("boom")
    assert conns[0].rollback_count == 0
    assert conns[0].close_count == 1


def test_db_conn_still_closes_and_keeps_original_error_when_rollback_fails(conns):
    class BrokenRollback(FakeConn):
        def rollback(self) -> None:
            super().rollback()
            raise psycopg2.InterfaceError("connection already closed")

    conns.factory["make"] = BrokenRollback
    with pytest.raises(RuntimeError, match="boom"):
        with coordinator.db_conn():
            raise RuntimeError("boom")
    assert conns[0].close_count == 1


# ── every endpoint releases its connection on error ───────────────

ENDPOINTS: list[tuple[str, bool, Callable[[], object]]] = [
    ("register_worker", True, lambda: coordinator.register_worker(
        coordinator.WorkerRegister(hostname="h", tailscale_ip="100.0.0.1"))),
    ("list_workers", False, lambda: coordinator.list_workers()),
    ("worker_heartbeat", True, lambda: coordinator.worker_heartbeat(1)),
    ("worker_heartbeat_active", True, lambda: coordinator.worker_heartbeat(1, active_jobs=2)),
    ("create_job", True, lambda: coordinator.create_job(
        coordinator.JobCreate(job_type=coordinator.JobType.BACKTEST, name="t"))),
    ("list_jobs", False, lambda: coordinator.list_jobs()),
    ("get_job", False, lambda: coordinator.get_job(1)),
    ("compute_inputs", False, lambda: coordinator.compute_inputs()),
    ("cancel_job", True, lambda: coordinator.cancel_job(1)),
    ("claim_job", False, lambda: coordinator.claim_job(1)),
    ("start_job", True, lambda: coordinator.start_job(1, 1)),
    ("complete_job", False, lambda: coordinator.complete_job(
        1, coordinator.JobResult(job_id=1, worker_id=1))),
    ("validate_job", True, lambda: coordinator.validate_job(1)),
    ("assimilate_job", True, lambda: coordinator.assimilate_job(1)),
]


@pytest.mark.parametrize("name,autocommit,call", ENDPOINTS, ids=[e[0] for e in ENDPOINTS])
def test_endpoint_releases_connection_when_a_query_is_canceled(conns, name, autocommit, call):
    use(conns, fail=lambda sql, n: query_canceled())
    with pytest.raises(psycopg2.errors.QueryCanceled):
        call()
    assert len(conns) == 1
    assert conns[0].close_count == 1
    assert conns[0].rollback_count == (0 if autocommit else 1)


TRANSITION_ENDPOINTS = [
    ("cancel_job", lambda: coordinator.cancel_job(1)),
    ("start_job", lambda: coordinator.start_job(1, 1)),
    ("validate_job", lambda: coordinator.validate_job(1)),
    ("assimilate_job", lambda: coordinator.assimilate_job(1)),
]


@pytest.mark.parametrize("name,call", TRANSITION_ENDPOINTS, ids=[e[0] for e in TRANSITION_ENDPOINTS])
def test_transition_http_errors_still_close_the_connection(conns, name, call):
    # Unknown job -> transition_job raises HTTPException(404) mid-request.
    use(conns, rows=lambda sql, one: None if "FOR UPDATE" in sql else default_rows(sql, one))
    with pytest.raises(HTTPException) as exc:
        call()
    assert exc.value.status_code == 404
    assert conns[0].close_count == 1


def test_claim_unknown_worker_rolls_back_and_closes(conns):
    use(conns, rows=lambda sql, one: None if "compute_workers" in sql and "FOR UPDATE" in sql else default_rows(sql, one))
    with pytest.raises(HTTPException) as exc:
        coordinator.claim_job(99)
    assert exc.value.status_code == 404
    assert (conns[0].commit_count, conns[0].rollback_count, conns[0].close_count) == (0, 1, 1)


def test_claim_commits_once_and_closes_on_success(conns):
    job = coordinator.claim_job(1)
    assert job["id"] == 1
    assert (conns[0].commit_count, conns[0].rollback_count, conns[0].close_count) == (1, 0, 1)


def test_claim_rolls_back_when_the_dispatch_transition_fails(conns):
    use(conns, fail=lambda sql, n: query_canceled() if sql.startswith("UPDATE compute_jobs SET state") else None)
    with pytest.raises(psycopg2.errors.QueryCanceled):
        coordinator.claim_job(1)
    assert (conns[0].commit_count, conns[0].rollback_count, conns[0].close_count) == (0, 1, 1)


def test_complete_idempotent_ack_rolls_back_and_closes(conns):
    def rows(sql, one):
        if "SELECT state FROM compute_jobs" in sql:
            return {"state": "COMPLETED"}
        if "FROM compute_results" in sql:
            return {"?column?": 1}
        return default_rows(sql, one)

    use(conns, rows=rows)
    out = coordinator.complete_job(1, coordinator.JobResult(job_id=1, worker_id=1))
    assert out["status"] == "already_recorded"
    assert conns[0].commit_count == 0
    assert conns[0].rollback_count == 1
    assert conns[0].close_count == 1


# ── /stats ────────────────────────────────────────────────────────


def stats_rows(sql, one):
    if "GROUP BY state, job_type" in sql:
        return [
            {"state": "QUEUED", "job_type": "BACKTEST", "count": 3},
            {"state": "QUEUED", "job_type": "SIMULATION", "count": 2},
            {"state": "ASSIMILATED", "job_type": "BACKTEST", "count": 10},
        ]
    return default_rows(sql, one)


def test_stats_does_one_bounded_scan_and_keeps_the_response_shape(conns):
    use(conns, rows=stats_rows)
    out = coordinator.coordinator_stats()
    assert out["job_states"] == {"QUEUED": 5, "ASSIMILATED": 10}
    assert out["jobs_by_type"] == {"BACKTEST": 13, "SIMULATION": 2}
    assert out["workers"] == {"total": 2, "idle": 1, "busy": 1}
    assert "timestamp" in out and "stale" not in out

    sqls = [sql for sql, _ in conns[0].executed]
    assert sum("FROM compute_jobs" in s for s in sqls) == 1
    timeout_calls = [p for s, p in conns[0].executed if "set_config('statement_timeout'" in s]
    assert timeout_calls == [(str(coordinator.STATS_STATEMENT_TIMEOUT_MS),)]
    assert sqls[0].startswith("SELECT set_config")  # limit applies before the scan
    assert (conns[0].rollback_count, conns[0].close_count) == (0, 1)


def test_stats_serves_cached_result_without_touching_the_database(conns):
    use(conns, rows=stats_rows)
    first = coordinator.coordinator_stats()
    second = coordinator.coordinator_stats()
    assert second is first
    assert len(conns) == 1


def test_stats_rescans_after_the_cache_expires(conns):
    use(conns, rows=stats_rows)
    coordinator.coordinator_stats()
    coordinator._stats_cache.stored_at -= coordinator.STATS_CACHE_TTL_SECONDS + 1
    coordinator.coordinator_stats()
    assert len(conns) == 2


def test_stats_timeout_without_cache_is_503_and_releases_connection(conns):
    use(conns, fail=lambda sql, n: query_canceled() if "GROUP BY state, job_type" in sql else None)
    with pytest.raises(HTTPException) as exc:
        coordinator.coordinator_stats()
    assert exc.value.status_code == 503
    assert (conns[0].rollback_count, conns[0].close_count) == (1, 1)


def test_stats_timeout_with_cache_serves_last_good_result_marked_stale(conns):
    use(conns, rows=stats_rows)
    good = coordinator.coordinator_stats()
    coordinator._stats_cache.stored_at -= coordinator.STATS_CACHE_TTL_SECONDS + 1
    use(conns, fail=lambda sql, n: query_canceled() if "GROUP BY state, job_type" in sql else None)
    out = coordinator.coordinator_stats()
    assert out["stale"] is True
    assert out["job_states"] == good["job_states"]
    assert conns[-1].close_count == 1


def test_stats_restart_errors_also_fall_back_to_stale(conns):
    use(conns, rows=stats_rows)
    coordinator.coordinator_stats()
    coordinator._stats_cache.stored_at -= coordinator.STATS_CACHE_TTL_SECONDS + 1
    use(conns, fail=lambda sql, n: psycopg2.errors.AdminShutdown("terminating connection due to administrator command"))
    assert coordinator.coordinator_stats()["stale"] is True


def test_failed_scan_backs_off_instead_of_rescanning(conns):
    use(conns, fail=lambda sql, n: query_canceled() if "GROUP BY state, job_type" in sql else None)
    for _ in range(3):
        with pytest.raises(HTTPException) as exc:
            coordinator.coordinator_stats()
        assert exc.value.status_code == 503
    assert len(conns) == 1  # later callers did not start their own scans

    coordinator._stats_cache.failed_at -= coordinator.STATS_CACHE_TTL_SECONDS + 1
    use(conns, rows=stats_rows)
    assert "stale" not in coordinator.coordinator_stats()
    assert len(conns) == 2  # retried once the back-off expired


def test_waiters_behind_a_failing_scan_do_not_rescan(conns):
    release = threading.Event()
    started = threading.Event()

    def block_then_fail(sql, n):
        if "GROUP BY state, job_type" in sql:
            started.set()
            assert release.wait(5)

    use(conns, on_execute=block_then_fail,
        fail=lambda sql, n: query_canceled() if "GROUP BY state, job_type" in sql else None)
    codes: list[int] = []

    def call() -> None:
        try:
            coordinator.coordinator_stats()
        except HTTPException as exc:
            codes.append(exc.status_code)

    threads = [threading.Thread(target=call) for _ in range(4)]
    threads[0].start()
    assert started.wait(5)
    for t in threads[1:]:
        t.start()
    release.set()
    for t in threads:
        t.join(5)
    assert codes == [503] * 4
    assert len(conns) == 1


def test_unexpected_stats_error_propagates_and_releases_the_lock(conns):
    use(conns, fail=lambda sql, n: RuntimeError("bug") if "GROUP BY state, job_type" in sql else None)
    with pytest.raises(RuntimeError):
        coordinator.coordinator_stats()
    assert not coordinator._stats_cache.lock.locked()
    use(conns, rows=stats_rows)
    coordinator.coordinator_stats()
    assert len(conns) == 2


def test_concurrent_stats_callers_share_a_single_scan(conns):
    release = threading.Event()
    started = threading.Event()

    def block_scan(sql, n):
        if "GROUP BY state, job_type" in sql:
            started.set()
            assert release.wait(5)

    use(conns, rows=stats_rows, on_execute=block_scan)
    results: list[dict] = []
    threads = [threading.Thread(target=lambda: results.append(coordinator.coordinator_stats())) for _ in range(4)]
    threads[0].start()
    assert started.wait(5)
    for t in threads[1:]:
        t.start()
    release.set()
    for t in threads:
        t.join(5)
    assert len(results) == 4
    assert len(conns) == 1  # one scan served all four callers


# ── /stats/queue ──────────────────────────────────────────────────


def test_queue_stats_counts_only_active_states_and_zero_fills(conns):
    use(conns, rows=lambda sql, one: [{"state": "QUEUED", "count": 7}] if "ANY" in sql else default_rows(sql, one))
    out = coordinator.coordinator_queue_stats()
    assert out["job_states"] == {"CREATED": 0, "QUEUED": 7, "DISPATCHED": 0, "IN_PROGRESS": 0}
    scans = [(s, p) for s, p in conns[0].executed if "FROM compute_jobs" in s]
    assert len(scans) == 1
    sql, params = scans[0]
    assert "state = ANY(%s)" in sql
    assert params == (list(coordinator.ACTIVE_JOB_STATES),)
    assert conns[0].close_count == 1


def test_queue_stats_timeout_is_503_and_releases_connection(conns):
    use(conns, fail=lambda sql, n: query_canceled() if "ANY" in sql else None)
    with pytest.raises(HTTPException) as exc:
        coordinator.coordinator_queue_stats()
    assert exc.value.status_code == 503
    assert (conns[0].rollback_count, conns[0].close_count) == (1, 1)


# ── producer ──────────────────────────────────────────────────────


class FakeResponse:
    def __init__(self, status: int, body: dict) -> None:
        self.status_code = status
        self._body = body

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code}", response=self)

    def json(self) -> dict:
        return self._body


def test_producer_reads_queue_depth_from_cheap_endpoint(monkeypatch):
    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        return FakeResponse(200, {"job_states": {"QUEUED": 4}})

    monkeypatch.setattr(producer.requests, "get", fake_get)
    assert producer.existing_queued_jobs("http://c") == 4
    assert calls == ["http://c/stats/queue"]


def test_producer_falls_back_to_full_stats_on_older_coordinator(monkeypatch):
    calls = []

    def fake_get(url, timeout):
        calls.append(url)
        if url.endswith("/stats/queue"):
            return FakeResponse(404, {})
        return FakeResponse(200, {"job_states": {"QUEUED": 9, "ASSIMILATED": 100}})

    monkeypatch.setattr(producer.requests, "get", fake_get)
    assert producer.existing_queued_jobs("http://c") == 9
    assert calls == ["http://c/stats/queue", "http://c/stats"]


def test_producer_does_not_mask_server_errors(monkeypatch):
    monkeypatch.setattr(producer.requests, "get", lambda url, timeout: FakeResponse(503, {}))
    with pytest.raises(requests.HTTPError):
        producer.existing_queued_jobs("http://c")
