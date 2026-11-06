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
