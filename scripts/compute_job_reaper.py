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

ACTIVE_JOB_STATES = ("DISPATCHED", "IN_PROGRESS")


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
    reference = job_reference_time(job)
    if reference is None:
        return False
    timeout_seconds = max(int(job.get("timeout_seconds") or 0), 0)
    age_seconds = (now - reference).total_seconds()
    if age_seconds <= timeout_seconds + max(grace_seconds, 0):
        return False
    return not worker_recently_active(worker, now=now, grace_seconds=grace_seconds)


def reconciled_worker_states(workers: list[dict], active_jobs: list[dict]) -> dict[int, dict]:
    counts = Counter(job.get("assigned_worker") for job in active_jobs if job.get("assigned_worker"))
    result = {}
    for worker in workers:
        worker_id = int(worker["id"])
        active_count = int(counts.get(worker_id, 0))
        if worker.get("state") == "OFFLINE":
            state = "OFFLINE"
        else:
            state = "BUSY" if active_count > 0 else "IDLE"
        result[worker_id] = {"active_jobs": active_count, "state": state}
    return result


def _dict_rows(rows):
    return [dict(row) for row in rows]


def select_stale_jobs(cur, *, grace_seconds: int, limit: int):
    cur.execute(
        """
        SELECT j.id, j.state, j.assigned_worker, j.timeout_seconds,
               j.created_at, j.queued_at, j.dispatched_at, j.started_at,
               w.active_jobs AS worker_active_jobs,
               w.last_heartbeat AS worker_last_heartbeat
        FROM compute_jobs j
        LEFT JOIN compute_workers w ON w.id = j.assigned_worker
        WHERE j.state IN ('DISPATCHED', 'IN_PROGRESS')
          AND COALESCE(j.started_at, j.dispatched_at, j.queued_at, j.created_at)
              < NOW() - ((j.timeout_seconds + %s) * INTERVAL '1 second')
          AND (
              j.assigned_worker IS NULL
              OR COALESCE(w.active_jobs, 0) = 0
              OR w.last_heartbeat < NOW() - (%s * INTERVAL '1 second')
          )
        ORDER BY j.id
        LIMIT %s
        FOR UPDATE SKIP LOCKED
        """,
        (grace_seconds, grace_seconds, limit),
    )
    return _dict_rows(cur.fetchall())


def requeue_stale_jobs(cur, stale_jobs: list[dict], *, reason: str):
    requeued = []
    for job in stale_jobs:
        job_id = int(job["id"])
        old_state = job["state"]
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
            (reason, job_id),
        )
        cur.execute(
            """
            INSERT INTO compute_state_log (job_id, from_state, to_state, reason, worker_id)
            VALUES (%s, %s, 'QUEUED', %s, %s)
            """,
            (job_id, old_state, reason, job.get("assigned_worker")),
        )
        requeued.append(job_id)
    return requeued


def reconcile_workers(cur):
    cur.execute(
        """
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
                WHEN counts.active_count > 0 THEN 'BUSY'
                ELSE 'IDLE'
            END
        FROM counts
        WHERE w.id = counts.id
        RETURNING w.id, w.hostname, w.active_jobs, w.state
        """
    )
    return _dict_rows(cur.fetchall())


def run_reaper(*, grace_seconds: int, limit: int, dry_run: bool):
    import psycopg2.extras
    from scripts.compute_coordinator import get_conn

    conn = get_conn()
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    try:
        stale_jobs = select_stale_jobs(cur, grace_seconds=grace_seconds, limit=limit)
        reason = f"requeued by compute_job_reaper after timeout plus {grace_seconds}s grace"
        requeued = [] if dry_run else requeue_stale_jobs(cur, stale_jobs, reason=reason)
        worker_updates = reconcile_workers(cur)
        if dry_run:
            conn.rollback()
        else:
            conn.commit()
        return {
            "dry_run": dry_run,
            "stale_job_ids": [int(job["id"]) for job in stale_jobs],
            "requeued_job_ids": requeued,
            "worker_updates": worker_updates,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Requeue stale GRID compute jobs and reconcile worker counts.")
    parser.add_argument("--grace-seconds", type=int, default=300, help="Seconds beyond job timeout before reaping")
    parser.add_argument("--limit", type=int, default=100, help="Maximum stale jobs to requeue per run")
    parser.add_argument("--dry-run", action="store_true", help="Inspect without modifying DB")
    args = parser.parse_args(argv)

    result = run_reaper(
        grace_seconds=max(args.grace_seconds, 0),
        limit=max(args.limit, 1),
        dry_run=args.dry_run,
    )
    result["timestamp"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(result, indent=2, sort_keys=True, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
