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
