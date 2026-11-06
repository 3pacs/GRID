#!/usr/bin/env python3
"""Maintenance reaper for stale GRID compute jobs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log

ACTIVE_JOB_STATES = ("DISPATCHED", "IN_PROGRESS")

# A worker heartbeats every 30 s (scripts/worker.py HEARTBEAT_INTERVAL), so 15
# minutes of silence is ~30 missed beats: the process is gone, not slow.
# Override with GRID_COMPUTE_WORKER_OFFLINE_MINUTES or --offline-after-minutes.
DEFAULT_OFFLINE_AFTER_MINUTES = 15
OFFLINE_AFTER_ENV = "GRID_COMPUTE_WORKER_OFFLINE_MINUTES"


def default_offline_after_minutes() -> int:
    """Minutes without a heartbeat before the reaper marks a worker OFFLINE."""
    raw = os.environ.get(OFFLINE_AFTER_ENV, "").strip()
    if not raw:
        return DEFAULT_OFFLINE_AFTER_MINUTES
    try:
        value = int(raw)
    except ValueError:
        return DEFAULT_OFFLINE_AFTER_MINUTES
    return value if value > 0 else DEFAULT_OFFLINE_AFTER_MINUTES

# Orphans of a restarted worker (W2-7, 2026-11-06). A worker registers once,
# at process start (scripts/worker.py main), and the coordinator stamps
# ``registered_at`` on every registration. A job claimed before the worker's
# current registration belongs to a process that no longer exists. The
# timeout rule below never caught these: the restarted worker heartbeats with
# new jobs, so ``active_jobs`` stays above zero and the orphan stays
# IN_PROGRESS forever (24666535 and 24666536 on i7gen10-w1; 24666722,
# 24666723 and 24668086 on grid-svr). Each worker process must register under
# its own hostname (the fleet units use ``--hostname <host>-wN``); two live
# processes sharing one hostname would requeue each other's jobs.
WORKER_RESTARTED_SQL = (
    "j.assigned_worker IS NOT NULL AND w.registered_at IS NOT NULL "
    "AND COALESCE(j.dispatched_at, j.started_at) < w.registered_at"
)

# One literal (no SQL string building); tests pin that both copies of
# WORKER_RESTARTED_SQL appear in it verbatim.
STALE_JOB_SELECT_SQL = """
        SELECT j.id, j.state, j.assigned_worker, j.timeout_seconds,
               j.created_at, j.queued_at, j.dispatched_at, j.started_at,
               w.active_jobs AS worker_active_jobs,
               w.last_heartbeat AS worker_last_heartbeat,
               w.registered_at AS worker_registered_at,
               CASE WHEN j.assigned_worker IS NOT NULL AND w.registered_at IS NOT NULL AND COALESCE(j.dispatched_at, j.started_at) < w.registered_at
                    THEN 'worker_restarted' ELSE 'timeout' END AS reap_reason
        FROM compute_jobs j
        LEFT JOIN compute_workers w ON w.id = j.assigned_worker
        WHERE j.state IN ('DISPATCHED', 'IN_PROGRESS')
          AND (
              (j.assigned_worker IS NOT NULL AND w.registered_at IS NOT NULL AND COALESCE(j.dispatched_at, j.started_at) < w.registered_at)
              OR (
                  -- the worker has been silent for longer than the job's timeout
                  j.assigned_worker IS NOT NULL
                  AND (w.last_heartbeat IS NULL
                       OR w.last_heartbeat
                          < NOW() - ((j.timeout_seconds + %s) * INTERVAL '1 second'))
              )
              OR
              (
                  COALESCE(j.started_at, j.dispatched_at, j.queued_at, j.created_at)
                      < NOW() - ((j.timeout_seconds + %s) * INTERVAL '1 second')
                  AND (
                      j.assigned_worker IS NULL
                      OR COALESCE(w.active_jobs, 0) = 0
                      OR w.last_heartbeat < NOW() - (%s * INTERVAL '1 second')
                  )
              )
              OR (j.requires_ollama = TRUE AND COALESCE(w.has_ollama, FALSE) = FALSE)
              OR (j.requires_gpu = TRUE AND w.gpu_model IS NULL)
              OR (
                  j.assigned_worker IS NOT NULL
                  AND COALESCE(w.active_jobs, 0) = 0
                  AND COALESCE(j.started_at, j.dispatched_at)
                      < NOW() - (%s * INTERVAL '1 second')
              )
          )
        ORDER BY j.id
        LIMIT %s
        FOR UPDATE OF j SKIP LOCKED
        """


def _as_utc(dt):
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def job_reference_time(job: dict):
    return _as_utc(
        job.get("started_at")
        or job.get("dispatched_at")
        or job.get("queued_at")
        or job.get("created_at")
    )


def worker_recently_active(worker: dict | None, *, now: datetime, grace_seconds: int) -> bool:
    if not worker:
        return False
    if int(worker.get("active_jobs") or 0) <= 0:
        return False
    last_heartbeat = _as_utc(worker.get("last_heartbeat"))
    if last_heartbeat is None:
        return False
    return (now - last_heartbeat).total_seconds() <= max(grace_seconds, 0)


def should_reap_job(job: dict, worker: dict | None, *, now: datetime, grace_seconds: int) -> bool:
    if job.get("state") not in ACTIVE_JOB_STATES:
        return False
    if worker_restarted_since_claim(job, worker):
        return True
    timeout_seconds = max(int(job.get("timeout_seconds") or 0), 0)
    if worker is not None and worker_heartbeat_is_stale(
        worker, now=now, offline_after_seconds=timeout_seconds + max(grace_seconds, 0)
    ):
        return True
    reference = job_reference_time(job)
    if reference is None:
        return False
    age_seconds = (now - reference).total_seconds()
    if age_seconds <= timeout_seconds + max(grace_seconds, 0):
        return False
    return not worker_recently_active(worker, now=now, grace_seconds=grace_seconds)


def worker_heartbeat_is_stale(
    worker: dict, *, now: datetime, offline_after_seconds: int | None
) -> bool:
    """True when the worker has been silent longer than the OFFLINE threshold.

    Mirrors the ``last_heartbeat`` test in :data:`RECONCILE_WORKERS_SQL`.
    """
    if offline_after_seconds is None:
        return False
    last_heartbeat = _as_utc(worker.get("last_heartbeat"))
    if last_heartbeat is None:
        return True
    return (now - last_heartbeat).total_seconds() > max(offline_after_seconds, 0)


def reconciled_worker_states(
    workers: list[dict],
    active_jobs: list[dict],
    *,
    now: datetime | None = None,
    offline_after_seconds: int | None = None,
) -> dict[int, dict]:
    """Pure-Python twin of :data:`RECONCILE_WORKERS_SQL` (kept for tests)."""
    now = now or datetime.now(timezone.utc)
    counts = Counter(job.get("assigned_worker") for job in active_jobs if job.get("assigned_worker"))
    result = {}
    for worker in workers:
        worker_id = int(worker["id"])
        active_count = int(counts.get(worker_id, 0))
        if worker.get("state") == "OFFLINE" or worker_heartbeat_is_stale(
            worker, now=now, offline_after_seconds=offline_after_seconds
        ):
            state = "OFFLINE"
        else:
            state = "BUSY" if active_count > 0 else "IDLE"
        result[worker_id] = {"active_jobs": active_count, "state": state}
    return result


def _dict_rows(rows):
    return [dict(row) for row in rows]


def select_stale_jobs(cur, *, grace_seconds: int, limit: int):
    cur.execute(
        STALE_JOB_SELECT_SQL,
        (grace_seconds, grace_seconds, grace_seconds, grace_seconds, limit),
    )
    return _dict_rows(cur.fetchall())


def worker_restarted_since_claim(job: dict, worker: dict | None) -> bool:
    """Pure-Python twin of :data:`WORKER_RESTARTED_SQL`."""
    if not worker or job.get("state") not in ACTIVE_JOB_STATES:
        return False
    registered = _as_utc(worker.get("registered_at"))
    claimed = _as_utc(job.get("dispatched_at") or job.get("started_at"))
    return registered is not None and claimed is not None and claimed < registered


def job_reap_reason(job: dict, default: str) -> str:
    """compute_state_log text for one requeued job."""
    if job.get("reap_reason") == "worker_restarted":
        return (
            "requeued by compute_job_reaper: worker re-registered at "
            f"{job.get('worker_registered_at')} after claiming the job (process restarted)"
        )
    return default


def requeue_stale_jobs(cur, stale_jobs: list[dict], *, reason: str):
    requeued = []
    for job in stale_jobs:
        job_id = int(job["id"])
        old_state = job["state"]
        job_reason = job_reap_reason(job, reason)
        cur.execute(
            """
            UPDATE compute_jobs
            SET state='QUEUED',
                assigned_worker=NULL,
                dispatched_at=NULL,
                started_at=NULL,
                error_message=%s
            WHERE id=%s
            """,
            (job_reason, job_id),
        )
        cur.execute(
            """
            INSERT INTO compute_state_log (job_id, from_state, to_state, reason, worker_id)
            VALUES (%s, %s, 'QUEUED', %s, %s)
            """,
            (job_id, old_state, job_reason, job.get("assigned_worker")),
        )
        requeued.append(job_id)
    return requeued


# A worker silent for longer than the threshold is OFFLINE; an OFFLINE row
# stays OFFLINE until the worker itself comes back. The coordinator's
# register, heartbeat and claim paths all write IDLE/BUSY, so a reconnecting
# worker flips itself back without the reaper's help. ``prev`` is the
# pre-update snapshot, so RETURNING reports what each row changed from.
RECONCILE_WORKERS_SQL = """
        WITH counts AS (
            SELECT w.id,
                   COUNT(j.id) FILTER (WHERE j.state IN ('DISPATCHED', 'IN_PROGRESS')) AS active_count
            FROM compute_workers w
            LEFT JOIN compute_jobs j ON j.assigned_worker = w.id
            GROUP BY w.id
        )
        UPDATE compute_workers w
        SET active_jobs = counts.active_count,
            state = CASE
                WHEN w.state = 'OFFLINE' THEN 'OFFLINE'
                WHEN w.last_heartbeat IS NULL
                     OR w.last_heartbeat < NOW() - (%s * INTERVAL '1 second') THEN 'OFFLINE'
                WHEN counts.active_count > 0 THEN 'BUSY'
                ELSE 'IDLE'
            END
        FROM counts, compute_workers prev
        WHERE w.id = counts.id AND prev.id = w.id
        RETURNING w.id, w.hostname, w.active_jobs, w.state,
                  prev.state AS previous_state, w.last_heartbeat
        """


def reconcile_workers(cur, *, offline_after_seconds: int):
    cur.execute(RECONCILE_WORKERS_SQL, (max(int(offline_after_seconds), 0),))
    rows = _dict_rows(cur.fetchall())
    for row in rows:
        if row.get("state") == "OFFLINE" and row.get("previous_state") != "OFFLINE":
            log.info(
                "Worker {id} ({host}) marked OFFLINE: last heartbeat {hb}",
                id=row.get("id"), host=row.get("hostname"), hb=row.get("last_heartbeat"),
            )
    return rows


def newly_offline_workers(worker_updates: list[dict]) -> list[str]:
    return [
        str(row.get("hostname"))
        for row in worker_updates
        if row.get("state") == "OFFLINE" and row.get("previous_state") != "OFFLINE"
    ]


def run_reaper(
    *,
    grace_seconds: int,
    limit: int,
    dry_run: bool,
    offline_after_minutes: int = DEFAULT_OFFLINE_AFTER_MINUTES,
):
    import psycopg2.extras
    from scripts.compute_coordinator import get_conn

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        stale_jobs = select_stale_jobs(cur, grace_seconds=grace_seconds, limit=limit)
        reason = f"requeued by compute_job_reaper after timeout plus {grace_seconds}s grace"
        requeued = [] if dry_run else requeue_stale_jobs(cur, stale_jobs, reason=reason)
        worker_updates = reconcile_workers(cur, offline_after_seconds=offline_after_minutes * 60)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return {
            "dry_run": dry_run,
            "stale_job_ids": [int(job["id"]) for job in stale_jobs],
            "requeued_job_ids": requeued,
            "worker_updates": worker_updates,
            "offline_after_minutes": offline_after_minutes,
            "newly_offline_workers": newly_offline_workers(worker_updates),
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


# ── Operator-listed jobs (W2-7) ──────────────────────────────────────────
# Orphans left before the coordinator stamped registered_at on re-register
# (or any job an operator has checked by hand) are reclaimed by id. The
# state guard and row lock make it a no-op for a job that finished or moved
# on in the meantime.
LISTED_JOB_SELECT_SQL = """
        SELECT j.id, j.state, j.assigned_worker
        FROM compute_jobs j
        WHERE j.id = ANY(%s)
          AND j.state IN ('DISPATCHED', 'IN_PROGRESS')
        ORDER BY j.id
        FOR UPDATE OF j
        """

CANCEL_REQUEUED_JOB_SQL = """
            UPDATE compute_jobs
            SET state='CANCELLED', error_message=%s
            WHERE id=%s AND state='QUEUED'
            """


def parse_job_ids(raw: str) -> list[int]:
    """``"1,2, 3"`` -> ``[1, 2, 3]``; rejects anything that is not a positive id."""
    ids = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        value = int(part)
        if value <= 0:
            raise ValueError(f"job id must be positive: {part}")
        ids.append(value)
    if not ids:
        raise ValueError("no job ids given")
    return sorted(set(ids))


def reclaim_listed_jobs(cur, job_ids: list[int], *, close: bool, note: str) -> dict:
    """Requeue (or requeue and cancel, atomically) the still-active *job_ids*."""
    cur.execute(LISTED_JOB_SELECT_SQL, (list(job_ids),))
    active = _dict_rows(cur.fetchall())
    reason = f"requeued by operator via compute_job_reaper --job-ids: {note}".strip()
    requeued = requeue_stale_jobs(cur, active, reason=reason)
    cancelled = []
    if close:
        cancel_note = f"cancelled by operator via compute_job_reaper --close: {note}".strip()
        for job_id in requeued:
            cur.execute(CANCEL_REQUEUED_JOB_SQL, (cancel_note, job_id))
            cur.execute(
                """
                INSERT INTO compute_state_log (job_id, from_state, to_state, reason, worker_id)
                VALUES (%s, 'QUEUED', 'CANCELLED', %s, NULL)
                """,
                (job_id, cancel_note),
            )
            cancelled.append(job_id)
    return {
        "listed_job_ids": list(job_ids),
        "active_job_ids": [int(j["id"]) for j in active],
        "skipped_job_ids": sorted(set(job_ids) - {int(j["id"]) for j in active}),
        "requeued_job_ids": requeued,
        "cancelled_job_ids": cancelled,
    }


def run_listed(job_ids: list[int], *, close: bool, dry_run: bool, note: str) -> dict:
    import psycopg2.extras

    from scripts.compute_coordinator import get_conn

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        result = reclaim_listed_jobs(cur, job_ids, close=close, note=note)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        result["dry_run"] = dry_run
        return result
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Requeue stale GRID compute jobs and reconcile worker counts.")
    parser.add_argument("--grace-seconds", type=int, default=300, help="Seconds beyond job timeout before reaping")
    parser.add_argument("--limit", type=int, default=100, help="Maximum stale jobs to requeue per run")
    parser.add_argument(
        "--offline-after-minutes",
        type=int,
        default=default_offline_after_minutes(),
        help=(
            "Mark a worker OFFLINE after this many minutes without a heartbeat "
            f"(default {DEFAULT_OFFLINE_AFTER_MINUTES}, env {OFFLINE_AFTER_ENV})"
        ),
    )
    parser.add_argument("--dry-run", action="store_true", help="Inspect without modifying DB")
    parser.add_argument(
        "--job-ids",
        default="",
        help="Comma-separated job ids to requeue if still DISPATCHED/IN_PROGRESS (skips the stale scan)",
    )
    parser.add_argument(
        "--close",
        action="store_true",
        help="With --job-ids: cancel the jobs instead of leaving them QUEUED (one transaction)",
    )
    parser.add_argument("--note", default="", help="With --job-ids: reason text for compute_state_log")
    args = parser.parse_args(argv)

    if args.job_ids:
        result = run_listed(
            parse_job_ids(args.job_ids), close=args.close, dry_run=args.dry_run, note=args.note,
        )
        result["timestamp"] = datetime.now(timezone.utc).isoformat()
        print(json.dumps(result, indent=2, sort_keys=True, default=str))
        return 0
    if args.close:
        parser.error("--close needs --job-ids")

    result = run_reaper(
        grace_seconds=max(args.grace_seconds, 0),
        limit=max(args.limit, 1),
        dry_run=args.dry_run,
        offline_after_minutes=max(args.offline_after_minutes, 1),
    )
    result["timestamp"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
