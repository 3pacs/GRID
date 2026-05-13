from datetime import datetime, timedelta, timezone

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
