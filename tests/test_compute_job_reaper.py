from datetime import datetime, timedelta, timezone

import pytest

from scripts import compute_coordinator as coordinator
from scripts import compute_job_reaper as reaper


def test_job_is_stale_after_timeout_plus_grace_when_worker_is_idle():
    now = datetime(2026, 5, 13, 6, 40, tzinfo=timezone.utc)
    job = {
        "state": "IN_PROGRESS",
        "started_at": now - timedelta(seconds=1900),
        "timeout_seconds": 1800,
    }
    worker = {"active_jobs": 0, "last_heartbeat": now - timedelta(seconds=10)}

    assert reaper.should_reap_job(job, worker, now=now, grace_seconds=60)


def test_job_is_not_reaped_when_worker_reports_active_recently():
    now = datetime(2026, 5, 13, 6, 40, tzinfo=timezone.utc)
    job = {
        "state": "IN_PROGRESS",
        "started_at": now - timedelta(seconds=7200),
        "timeout_seconds": 1800,
    }
    worker = {"active_jobs": 1, "last_heartbeat": now - timedelta(seconds=20)}

    assert not reaper.should_reap_job(job, worker, now=now, grace_seconds=60)


def test_reconcile_worker_counts_preserves_offline_state():
    workers = [
        {"id": 1, "state": "IDLE"},
        {"id": 2, "state": "OFFLINE"},
        {"id": 3, "state": "BUSY"},
    ]
    # No threshold given: only the explicit OFFLINE state applies.
    active_jobs = [
        {"assigned_worker": 1},
        {"assigned_worker": 1},
        {"assigned_worker": 2},
    ]

    assert reaper.reconciled_worker_states(workers, active_jobs) == {
        1: {"active_jobs": 2, "state": "BUSY"},
        2: {"active_jobs": 1, "state": "OFFLINE"},
        3: {"active_jobs": 0, "state": "IDLE"},
    }


def test_stale_job_query_locks_jobs_not_outer_joined_workers():
    assert "FOR UPDATE OF j SKIP LOCKED" in reaper.STALE_JOB_SELECT_SQL


def test_reaper_query_reclaims_capability_incompatible_jobs():
    assert "j.requires_ollama = TRUE AND COALESCE(w.has_ollama, FALSE) = FALSE" in reaper.STALE_JOB_SELECT_SQL
    assert "j.requires_gpu = TRUE AND w.gpu_model IS NULL" in reaper.STALE_JOB_SELECT_SQL


def test_reaper_query_reclaims_orphans_after_worker_reports_zero_active_jobs():
    assert "COALESCE(w.active_jobs, 0) = 0" in reaper.STALE_JOB_SELECT_SQL
    assert "COALESCE(j.started_at, j.dispatched_at)" in reaper.STALE_JOB_SELECT_SQL


# ── OFFLINE marking (2026-11-06) ────────────────────────────────────────
# Seven of twelve compute_workers rows (redbox, panda, koala, p9d, forge,
# the Mac mini, grid-svr-llm) still read IDLE weeks or months after their
# last heartbeat, which inflated the producer's open-capacity estimate.


def test_silent_worker_is_marked_offline_after_threshold():
    now = datetime(2026, 11, 6, 0, 10, tzinfo=timezone.utc)
    workers = [
        {"id": 1, "state": "BUSY", "last_heartbeat": now - timedelta(seconds=10)},
        {"id": 9, "state": "IDLE", "last_heartbeat": now - timedelta(days=26)},
        {"id": 12, "state": "IDLE", "last_heartbeat": now - timedelta(minutes=14)},
        {"id": 13, "state": "IDLE", "last_heartbeat": None},
    ]

    states = reaper.reconciled_worker_states(
        workers, [{"assigned_worker": 1}], now=now, offline_after_seconds=15 * 60
    )

    assert states[1] == {"active_jobs": 1, "state": "BUSY"}
    assert states[9]["state"] == "OFFLINE"
    assert states[12]["state"] == "IDLE"
    assert states[13]["state"] == "OFFLINE"


def test_reconcile_sql_marks_stale_heartbeats_offline_with_bound_threshold():
    sql = reaper.RECONCILE_WORKERS_SQL

    assert "w.last_heartbeat < NOW() - (%s * INTERVAL '1 second') THEN 'OFFLINE'" in sql
    assert "w.last_heartbeat IS NULL" in sql
    # The OFFLINE checks come before the BUSY/IDLE derivation.
    assert sql.index("THEN 'OFFLINE'") < sql.index("THEN 'BUSY'")
    assert "prev.state AS previous_state" in sql


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append((sql, params))

    def fetchall(self):
        return self.rows


def test_reconcile_workers_binds_offline_seconds_and_reports_transitions():
    cur = _FakeCursor([
        {"id": 9, "hostname": "redbox", "state": "OFFLINE", "previous_state": "IDLE"},
        {"id": 10, "hostname": "z400", "state": "OFFLINE", "previous_state": "OFFLINE"},
        {"id": 1, "hostname": "grid-svr", "state": "BUSY", "previous_state": "BUSY"},
    ])

    rows = reaper.reconcile_workers(cur, offline_after_seconds=900)

    assert cur.calls == [(reaper.RECONCILE_WORKERS_SQL, (900,))]
    assert reaper.newly_offline_workers(rows) == ["redbox"]


def test_offline_threshold_is_configurable_by_env(monkeypatch):
    monkeypatch.delenv(reaper.OFFLINE_AFTER_ENV, raising=False)
    assert reaper.default_offline_after_minutes() == 15
    monkeypatch.setenv(reaper.OFFLINE_AFTER_ENV, "45")
    assert reaper.default_offline_after_minutes() == 45
    for bad in ("0", "-3", "soon"):
        monkeypatch.setenv(reaper.OFFLINE_AFTER_ENV, bad)
        assert reaper.default_offline_after_minutes() == 15


def test_main_passes_offline_minutes_to_run_reaper(monkeypatch, capsys):
    seen = {}

    def fake_run_reaper(**kwargs):
        seen.update(kwargs)
        return {}

    monkeypatch.setattr(reaper, "run_reaper", fake_run_reaper)

    assert reaper.main(["--dry-run", "--offline-after-minutes", "30"]) == 0
    assert seen["offline_after_minutes"] == 30
    assert seen["dry_run"] is True


# ── Orphans of a restarted worker (W2-7, 2026-11-06) ────────────────────
# Five BACKTEST jobs sat IN_PROGRESS after worker restarts: the restarted
# worker heartbeats with new jobs, so the timeout rule (which waits for
# active_jobs = 0 or a silent heartbeat) never fired. The coordinator now
# restamps registered_at on every registration (one per process start), and
# a job claimed before it belongs to a process that is gone.

T_RESTART = datetime(2026, 11, 6, 1, 45, 50, tzinfo=timezone.utc)


def _busy_worker(registered_at):
    return {
        "active_jobs": 1,
        "last_heartbeat": T_RESTART + timedelta(minutes=10),
        "registered_at": registered_at,
    }


def test_job_claimed_before_worker_reregistered_is_reaped():
    # grid-svr: job 24668086 started 01:36:54, worker restarted 01:45:50
    now = T_RESTART + timedelta(minutes=11)
    job = {
        "state": "IN_PROGRESS",
        "dispatched_at": datetime(2026, 11, 6, 1, 36, 54, tzinfo=timezone.utc),
        "started_at": datetime(2026, 11, 6, 1, 36, 54, tzinfo=timezone.utc),
        "timeout_seconds": 600,
    }
    worker = _busy_worker(T_RESTART)

    assert reaper.worker_restarted_since_claim(job, worker)
    assert reaper.should_reap_job(job, worker, now=now, grace_seconds=300)


def test_job_claimed_by_the_current_process_is_not_reaped():
    now = T_RESTART + timedelta(hours=2)
    job = {
        "state": "IN_PROGRESS",
        "dispatched_at": T_RESTART + timedelta(seconds=1),
        "started_at": T_RESTART + timedelta(seconds=1),
        "timeout_seconds": 600,  # long past its timeout, but the worker is live
    }
    worker = _busy_worker(T_RESTART)
    worker["last_heartbeat"] = now - timedelta(seconds=20)

    assert not reaper.worker_restarted_since_claim(job, worker)
    assert not reaper.should_reap_job(job, worker, now=now, grace_seconds=300)


def test_restart_rule_needs_a_registration_and_a_claim_time():
    job = {"state": "IN_PROGRESS", "timeout_seconds": 600}
    assert not reaper.worker_restarted_since_claim(job, _busy_worker(T_RESTART))
    job["started_at"] = T_RESTART - timedelta(minutes=5)
    assert not reaper.worker_restarted_since_claim(job, _busy_worker(None))
    assert not reaper.worker_restarted_since_claim(job, None)
    assert not reaper.worker_restarted_since_claim({**job, "state": "COMPLETED"}, _busy_worker(T_RESTART))


def test_worker_silent_longer_than_the_job_timeout_is_reaped_even_if_it_said_busy():
    now = datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
    job = {
        "state": "IN_PROGRESS",
        "dispatched_at": now - timedelta(minutes=20),
        "started_at": now - timedelta(minutes=20),
        "timeout_seconds": 600,
    }
    worker = {"active_jobs": 1, "registered_at": now - timedelta(hours=3),
              "last_heartbeat": now - timedelta(seconds=600 + 300 + 1)}
    assert reaper.should_reap_job(job, worker, now=now, grace_seconds=300)
    worker["last_heartbeat"] = now - timedelta(seconds=600 + 300 - 1)
    # within timeout+grace of silence: not by this rule (heartbeat rule of the
    # timeout clause needs the job past its timeout too, which it is not)
    job["started_at"] = job["dispatched_at"] = now - timedelta(minutes=5)
    assert not reaper.should_reap_job(job, worker, now=now, grace_seconds=300)


def test_stale_query_carries_the_restart_and_silence_rules():
    sql = reaper.STALE_JOB_SELECT_SQL
    assert sql.count(reaper.WORKER_RESTARTED_SQL) == 2  # WHERE clause and reap_reason
    assert "w.registered_at AS worker_registered_at" in sql
    assert "THEN 'worker_restarted' ELSE 'timeout' END AS reap_reason" in sql
    assert "w.last_heartbeat\n                          < NOW() - ((j.timeout_seconds + %s) * INTERVAL '1 second')" in sql


def test_select_stale_jobs_binds_one_value_per_placeholder():
    cur = _FakeCursor([])
    reaper.select_stale_jobs(cur, grace_seconds=300, limit=50)
    sql, params = cur.calls[0]
    assert sql.count("%s") == len(params) == 5
    assert params == (300, 300, 300, 300, 50)


def test_requeue_logs_the_restart_reason_per_job():
    cur = _FakeCursor([])
    jobs = [
        {"id": 24668086, "state": "IN_PROGRESS", "assigned_worker": 1,
         "reap_reason": "worker_restarted", "worker_registered_at": T_RESTART},
        {"id": 7, "state": "IN_PROGRESS", "assigned_worker": 2, "reap_reason": "timeout"},
    ]
    assert reaper.requeue_stale_jobs(cur, jobs, reason="timeout reason") == [24668086, 7]
    log_params = [p for s, p in cur.calls if "compute_state_log" in s]
    assert "worker re-registered at 2026-11-06 01:45:50+00:00" in log_params[0][2]
    assert log_params[1][2] == "timeout reason"


def test_coordinator_restamps_registered_at_on_every_registration():
    sql = coordinator.worker_register_upsert_sql()
    update_clause = sql.split("ON CONFLICT (hostname) DO UPDATE SET", 1)[1]
    assert "registered_at=NOW()" in update_clause


# ── Operator-listed jobs (--job-ids) ─────────────────────────────────────


def test_parse_job_ids():
    assert reaper.parse_job_ids("24666722, 24666535,24666722,") == [24666535, 24666722]
    for bad in ("", " , ", "12,abc", "0", "-4"):
        try:
            reaper.parse_job_ids(bad)
        except ValueError:
            continue
        raise AssertionError(f"accepted {bad!r}")


def test_reclaim_listed_jobs_requeues_only_still_active_ids():
    active = [{"id": 24666535, "state": "IN_PROGRESS", "assigned_worker": 788}]
    cur = _FakeCursor(active)
    result = reaper.reclaim_listed_jobs(cur, [24666533, 24666535], close=False, note="W2-7 orphans")

    select_sql, select_params = cur.calls[0]
    assert select_sql == reaper.LISTED_JOB_SELECT_SQL
    assert "FOR UPDATE OF j" in select_sql and "j.state IN ('DISPATCHED', 'IN_PROGRESS')" in select_sql
    assert select_params == ([24666533, 24666535],)
    assert result["requeued_job_ids"] == [24666535]
    assert result["skipped_job_ids"] == [24666533]  # already COMPLETED
    assert result["cancelled_job_ids"] == []
    assert not any("CANCELLED" in s for s, _ in cur.calls)


def test_reclaim_listed_jobs_close_cancels_in_the_same_transaction():
    active = [{"id": 24666722, "state": "IN_PROGRESS", "assigned_worker": 1}]
    cur = _FakeCursor(active)
    result = reaper.reclaim_listed_jobs(cur, [24666722], close=True, note="orphan")

    assert result["cancelled_job_ids"] == [24666722]
    cancel = [(s, p) for s, p in cur.calls if s == reaper.CANCEL_REQUEUED_JOB_SQL]
    assert cancel and cancel[0][1][1] == 24666722
    assert "AND state='QUEUED'" in reaper.CANCEL_REQUEUED_JOB_SQL


def test_main_routes_job_ids_and_rejects_close_alone(monkeypatch, capsys):
    seen = {}

    def fake_run_listed(job_ids, **kwargs):
        seen.update(kwargs, job_ids=job_ids)
        return {}

    monkeypatch.setattr(reaper, "run_listed", fake_run_listed)
    monkeypatch.setattr(reaper, "run_reaper", lambda **kw: (_ for _ in ()).throw(AssertionError("scan ran")))
    assert reaper.main(["--job-ids", "3,1", "--close", "--dry-run", "--note", "x"]) == 0
    assert seen == {"job_ids": [1, 3], "close": True, "dry_run": True, "note": "x"}

    with pytest.raises(SystemExit):
        reaper.main(["--close"])
