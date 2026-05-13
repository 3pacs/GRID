#!/usr/bin/env python3
"""Capability-aware producer for GRID compute coordinator jobs."""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from typing import Iterable

import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

DEFAULT_COORDINATOR = "http://100.75.185.36:8100"


def _as_int(value, default=0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _dedupe_ints(values: Iterable[int]) -> list[int]:
    seen = set()
    result = []
    for value in values:
        item = _as_int(value)
        if item and item not in seen:
            seen.add(item)
            result.append(item)
    return result


def worker_open_capacity(worker: dict) -> int:
    if worker.get("state") == "OFFLINE":
        return 0
    max_concurrent = max(_as_int(worker.get("max_concurrent"), 1), 1)
    active_jobs = max(_as_int(worker.get("active_jobs"), 0), 0)
    return max(max_concurrent - active_jobs, 0)


def recommended_queue_target(workers: list[dict], multiplier=6, floor=24, ceiling=128) -> int:
    open_capacity = sum(worker_open_capacity(worker) for worker in workers)
    if open_capacity <= 0:
        return 0
    return min(max(open_capacity * multiplier, floor), ceiling)


def _has_ollama_worker(workers: list[dict]) -> bool:
    return any(worker.get("state") != "OFFLINE" and worker.get("has_ollama") for worker in workers)


def _simulation_job(index: int, stamp: str, feature_ids: list[int]) -> dict:
    return {
        "job_type": "SIMULATION",
        "name": f"producer simulation {stamp}-{index:03d}",
        "description": "Producer-seeded Monte Carlo simulation using model-eligible features.",
        "params": {
            "n_paths": 2000,
            "horizon_days": 42,
            "feature_ids": feature_ids[:10],
        },
        "priority": 4,
        "timeout_seconds": 1800,
        "requires_gpu": False,
        "requires_ollama": False,
    }


def _regime_job(index: int, stamp: str, feature_ids: list[int]) -> dict:
    return {
        "job_type": "REGIME_DETECT",
        "name": f"producer regime detect {stamp}-{index:03d}",
        "description": "Producer-seeded regime detection over model-eligible features.",
        "params": {
            "n_components": 4,
            "start_date": "2024-04-01",
            "feature_ids": feature_ids[:20],
        },
        "priority": 4,
        "timeout_seconds": 2400,
        "requires_gpu": False,
        "requires_ollama": False,
    }


def _backtest_job(index: int, stamp: str, model_ids: list[int]) -> dict:
    model_id = model_ids[index % len(model_ids)]
    return {
        "job_type": "BACKTEST",
        "name": f"producer model {model_id} walk-forward {stamp}-{index:03d}",
        "description": "Producer-seeded walk-forward backtest for a DB-valid model.",
        "params": {
            "kind": "model_walkforward",
            "model_id": model_id,
            "n_splits": 5,
            "train_pct": 0.7,
        },
        "priority": 5,
        "timeout_seconds": 3600,
        "requires_gpu": False,
        "requires_ollama": False,
    }


def _llm_job(index: int, stamp: str) -> dict:
    model = os.environ.get("GRID_PRODUCER_LLM_MODEL", "llama3.2")
    return {
        "job_type": "LLM_INFERENCE",
        "name": f"producer llm synthesis {stamp}-{index:03d}",
        "description": "Producer-seeded local Ollama synthesis task.",
        "params": {
            "model": model,
            "prompt": (
                "Summarize one concrete risk and one useful signal a GRID "
                "operator should inspect before the next research cycle."
            ),
        },
        "priority": 3,
        "timeout_seconds": 900,
        "requires_gpu": False,
        "requires_ollama": True,
    }


def build_job_plan(
    *,
    workers: list[dict],
    existing_queued: int,
    target_queued: int,
    max_create: int,
    valid_model_ids: list[int],
    feature_ids: list[int],
    stamp: str | None = None,
) -> list[dict]:
    needed = max(min(target_queued - existing_queued, max_create), 0)
    if needed == 0:
        return []

    model_ids = _dedupe_ints(valid_model_ids)
    usable_feature_ids = _dedupe_ints(feature_ids)

    builders = []
    if usable_feature_ids:
        builders.append(lambda i: _simulation_job(i, stamp or "run", usable_feature_ids))
        builders.append(lambda i: _regime_job(i, stamp or "run", usable_feature_ids))
    if model_ids:
        builders.append(lambda i: _backtest_job(i, stamp or "run", model_ids))
    if _has_ollama_worker(workers):
        builders.append(lambda i: _llm_job(i, stamp or "run"))

    if not builders:
        return []

    plan = []
    for index in range(needed):
        builder = builders[index % len(builders)]
        plan.append(builder(index))
    return plan


def parse_compute_inputs(payload: dict) -> tuple[list[int], list[int]]:
    return (
        _dedupe_ints(payload.get("model_ids", [])),
        _dedupe_ints(payload.get("feature_ids", [])),
    )


def discover_compute_inputs(coordinator: str) -> tuple[list[int], list[int]]:
    response = requests.get(f"{coordinator}/metadata/compute-inputs", timeout=15)
    response.raise_for_status()
    return parse_compute_inputs(response.json())


def discover_model_ids(limit=8) -> list[int]:
    from sqlalchemy import text
    from db import get_engine

    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id FROM model_registry "
                "WHERE feature_set IS NOT NULL AND cardinality(feature_set) > 0 "
                "ORDER BY id LIMIT :limit"
            ),
            {"limit": limit},
        ).fetchall()
    return [int(row[0]) for row in rows]


def discover_feature_ids(limit=10) -> list[int]:
    from sqlalchemy import text
    from db import get_engine

    with get_engine().connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id FROM feature_registry "
                "WHERE model_eligible=TRUE ORDER BY id LIMIT :limit"
            ),
            {"limit": limit},
        ).fetchall()
    return [int(row[0]) for row in rows]


def get_json(coordinator: str, path: str):
    response = requests.get(f"{coordinator}{path}", timeout=15)
    response.raise_for_status()
    return response.json()


def post_job(coordinator: str, payload: dict) -> dict:
    response = requests.post(f"{coordinator}/jobs", json=payload, timeout=20)
    response.raise_for_status()
    return response.json()


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Seed GRID compute jobs from live cluster state.")
    parser.add_argument("--coordinator", default=DEFAULT_COORDINATOR, help="Compute coordinator URL")
    parser.add_argument("--target-queued", type=int, default=0, help="Desired queued jobs; 0 auto-scales")
    parser.add_argument("--max-create", type=int, default=64, help="Maximum jobs to create in this run")
    parser.add_argument("--model-id", type=int, action="append", default=[], help="Known-valid model ID")
    parser.add_argument("--feature-id", type=int, action="append", default=[], help="Known-valid feature ID")
    parser.add_argument("--dry-run", action="store_true", help="Print the plan without creating jobs")
    args = parser.parse_args(argv)

    coordinator = args.coordinator.rstrip("/")
    workers = get_json(coordinator, "/workers")
    stats = get_json(coordinator, "/stats")
    existing_queued = _as_int(stats.get("job_states", {}).get("QUEUED"), 0)

    target_queued = args.target_queued or recommended_queue_target(workers)
    if args.model_id or args.feature_id:
        model_ids = _dedupe_ints(args.model_id)
        feature_ids = _dedupe_ints(args.feature_id)
    else:
        try:
            model_ids, feature_ids = discover_compute_inputs(coordinator)
        except Exception:
            model_ids = discover_model_ids()
            feature_ids = discover_feature_ids()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    plan = build_job_plan(
        workers=workers,
        existing_queued=existing_queued,
        target_queued=target_queued,
        max_create=args.max_create,
        valid_model_ids=model_ids,
        feature_ids=feature_ids,
        stamp=stamp,
    )

    summary = {
        "coordinator": coordinator,
        "existing_queued": existing_queued,
        "target_queued": target_queued,
        "planned": len(plan),
        "model_ids": model_ids,
        "feature_ids": feature_ids,
        "dry_run": args.dry_run,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    if args.dry_run:
        print(json.dumps(plan, indent=2, sort_keys=True))
        return 0

    created = []
    for payload in plan:
        created.append(post_job(coordinator, payload)["id"])
    print(json.dumps({"created_job_ids": created}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
