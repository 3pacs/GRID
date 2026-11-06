#!/usr/bin/env python3
"""Cancel GRID compute jobs through the coordinator, dry-run by default.

Selects jobs by id or by filter (state, job type, name prefix, created-before),
prints what it would cancel, and cancels only with ``--execute`` plus an
``--expect-count`` that matches the selection. Cancellation goes through the
coordinator's ``POST /jobs/{id}/cancel``, so the state machine (only CREATED
and QUEUED can be cancelled) and the ``compute_state_log`` audit row are the
coordinator's, not re-implemented here.

Examples::

    # What would go? (no writes)
    python3 scripts/compute_job_cancel.py --job-type LLM_INFERENCE \\
        --name-prefix "producer llm synthesis" --created-before 2026-10-12

    # Cancel exactly the 79 jobs the dry run listed
    python3 scripts/compute_job_cancel.py --job-type LLM_INFERENCE \\
        --name-prefix "producer llm synthesis" --created-before 2026-10-12 \\
        --reason "stale producer filler, no consumer" --execute --expect-count 79
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Callable, Iterable
from datetime import datetime, timezone
from typing import Any

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.compute_job_producer import DEFAULT_COORDINATOR

#: The coordinator's VALID_TRANSITIONS allow CANCELLED only from these.
CANCELLABLE_STATES = ("CREATED", "QUEUED")
DEFAULT_FETCH_LIMIT = 1000


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


def select_jobs(
    jobs: Iterable[dict],
    *,
    ids: set[int] | None = None,
    states: Iterable[str] = ("QUEUED",),
    job_type: str | None = None,
    name_prefix: str | None = None,
    created_before: datetime | None = None,
) -> list[dict]:
    """Apply every selector; a job must pass all of them."""
    wanted_states = set(states)
    selected = []
    for job in jobs:
        if ids is not None and int(job.get("id", 0)) not in ids:
            continue
        if job.get("state") not in wanted_states:
            continue
        if job_type and job.get("job_type") != job_type:
            continue
        if name_prefix and not str(job.get("name", "")).startswith(name_prefix):
            continue
        if created_before is not None:
            created = _parse_ts(job.get("created_at"))
            if created is None or created >= created_before:
                continue
        selected.append(job)
    return sorted(selected, key=lambda job: int(job["id"]))


_TRAILING_STAMP = re.compile(r"[\s-]*[0-9][0-9TZ:.-]*$")


def name_group(name: Any) -> str:
    """Job name without its trailing run stamp/index ("... 20261010T231756Z-007")."""
    return _TRAILING_STAMP.sub("", str(name or ""))


def summarize(jobs: list[dict], *, now: datetime | None = None) -> dict:
    now = now or datetime.now(timezone.utc)
    created = [ts for ts in (_parse_ts(job.get("created_at")) for job in jobs) if ts]
    groups = Counter((job.get("job_type"), job.get("state"), name_group(job.get("name"))) for job in jobs)
    return {
        "count": len(jobs),
        "ids": [int(job["id"]) for job in jobs],
        "oldest_created_at": min(created).isoformat() if created else None,
        "newest_created_at": max(created).isoformat() if created else None,
        "max_age_days": round((now - min(created)).total_seconds() / 86400, 1) if created else None,
        "groups": [
            {"job_type": jt, "state": st, "name_prefix": prefix, "count": n}
            for (jt, st, prefix), n in sorted(groups.items(), key=lambda item: -item[1])
        ],
    }


def execution_refusal(
    *, selected: int, expect_count: int | None, truncated: bool
) -> str | None:
    """Why ``--execute`` must not proceed, or None when it may."""
    if truncated:
        return "selection may be truncated by --fetch-limit; narrow the filter or raise the limit"
    if expect_count is None:
        return "--execute requires --expect-count (the count the dry run printed)"
    if expect_count != selected:
        return f"--expect-count {expect_count} does not match the {selected} selected jobs"
    if selected == 0:
        return "nothing selected"
    return None


def fetch_candidates(
    coordinator: str,
    *,
    ids: list[int],
    states: list[str],
    job_type: str | None,
    fetch_limit: int,
    get: Callable[..., Any] | None = None,
) -> tuple[list[dict], bool]:
    """Return candidate jobs and whether the listing may have been truncated."""
    get = get or requests.get
    if ids:
        jobs = []
        for job_id in ids:
            response = get(f"{coordinator}/jobs/{job_id}", timeout=15)
            if response.status_code == 404:
                continue
            response.raise_for_status()
            jobs.append(response.json())
        return jobs, False
    jobs = []
    truncated = False
    for state in states:
        params = {"state": state, "limit": fetch_limit}
        if job_type:
            params["job_type"] = job_type
        response = get(f"{coordinator}/jobs", params=params, timeout=30)
        response.raise_for_status()
        batch = response.json()
        truncated = truncated or len(batch) >= fetch_limit
        jobs.extend(batch)
    return jobs, truncated


def cancel_jobs(
    coordinator: str,
    job_ids: list[int],
    *,
    reason: str,
    post: Callable[..., Any] | None = None,
) -> dict:
    post = post or requests.post
    cancelled, failed = [], []
    for job_id in job_ids:
        try:
            response = post(
                f"{coordinator}/jobs/{job_id}/cancel", params={"reason": reason}, timeout=15
            )
            response.raise_for_status()
            cancelled.append(job_id)
        except (requests.RequestException, OSError) as exc:  # claimed since the dry run: 400
            failed.append({"id": job_id, "error": str(exc)[:200]})
    return {"cancelled_ids": cancelled, "failed": failed}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Cancel GRID compute jobs (dry-run by default).")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR)
    parser.add_argument("--id", dest="ids", type=int, action="append", default=[], help="Job id (repeatable)")
    parser.add_argument("--state", dest="states", action="append", choices=CANCELLABLE_STATES,
                        help="Job state to match (repeatable; default QUEUED)")
    parser.add_argument("--job-type", default=None)
    parser.add_argument("--name-prefix", default=None)
    parser.add_argument("--created-before", default=None, help="ISO date/time, UTC if no zone")
    parser.add_argument("--fetch-limit", type=int, default=DEFAULT_FETCH_LIMIT)
    parser.add_argument("--reason", default="compute_job_cancel", help="Recorded in compute_state_log")
    parser.add_argument("--execute", action="store_true", help="Actually cancel (default: dry run)")
    parser.add_argument("--expect-count", type=int, default=None,
                        help="Required with --execute; must equal the selected count")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.ids and not (args.job_type or args.name_prefix):
        print("refusing: give --id, or --job-type / --name-prefix (an empty filter matches everything)",
              file=sys.stderr)
        return 2
    created_before = _parse_ts(args.created_before) if args.created_before else None
    if args.created_before and created_before is None:
        print(f"refusing: unparseable --created-before {args.created_before!r}", file=sys.stderr)
        return 2

    coordinator = args.coordinator.rstrip("/")
    states = args.states or ["QUEUED"]
    candidates, truncated = fetch_candidates(
        coordinator, ids=args.ids, states=states, job_type=args.job_type,
        fetch_limit=max(args.fetch_limit, 1),
    )
    selected = select_jobs(
        candidates,
        ids=set(args.ids) if args.ids else None,
        states=states,
        job_type=args.job_type,
        name_prefix=args.name_prefix,
        created_before=created_before,
    )
    report: dict[str, Any] = {
        "coordinator": coordinator,
        "dry_run": not args.execute,
        "truncated": truncated,
        "selection": summarize(selected),
    }
    if not args.execute:
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0

    refusal = execution_refusal(selected=len(selected), expect_count=args.expect_count, truncated=truncated)
    if refusal:
        report["refused"] = refusal
        print(json.dumps(report, indent=2, sort_keys=True))
        return 2
    report["result"] = cancel_jobs(coordinator, report["selection"]["ids"], reason=args.reason)
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if not report["result"]["failed"] else 1


if __name__ == "__main__":
    sys.exit(main())
