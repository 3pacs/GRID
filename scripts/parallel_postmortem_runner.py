"""Fan-out batch_postmortem across the cluster's LLM nodes.

Each failed prediction needs an LLM call to generate the postmortem
narrative. Sequential execution on a single LLM provider takes hours
for the kind of batch we expect post-2026-05-15 (~100K failures).

This runner partitions the candidate set N ways and spawns N parallel
worker processes, each pinned to a different cluster LLM provider via
``LLM_ORACLE_PROVIDER`` env. The LLM router already knows how to talk
to:

  llamacpp_z4    — gridz4 Blackwell (RTX PRO 4000 24GB)
  ollama_panda   — panda Ollama (2×P100, qwen3.6:27b)
  ollama_ocr     — ocr-node (2×Ampere 8GB, gemma3:12b-it-q4_K_M)
  ollama_koala   — koala (2× Titan X 12GB, gemma3:12b)

Each worker pulls only its slice (``id % partitions == idx``), calls
``intelligence.postmortem.generate_postmortem`` per row, writes back.

Coordination is via partition-by-modulus — no shared queue, no
coordination overhead. If a worker dies mid-run, the orphaned rows
just stay unpostmortemed and get picked up on the next runner pass.

CLI
---

    # default: spawn one worker per known provider
    python -m scripts.parallel_postmortem_runner

    # explicit provider list (in order of preference)
    python -m scripts.parallel_postmortem_runner --providers llamacpp_z4 ollama_panda ollama_ocr

    # dry-run (count candidates, print plan, don't fork)
    python -m scripts.parallel_postmortem_runner --dry-run

    # process at most N rows total across all workers
    python -m scripts.parallel_postmortem_runner --max-rows 1000
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from db import get_engine


_DEFAULT_PROVIDERS = [
    "llamacpp_z4",
    "ollama_panda",
    "ollama_ocr",
    "ollama_koala",
]


def _count_candidates(engine, days: int = 30) -> tuple[int, int]:
    """Return (failed_trades, missed_predictions) eligible for postmortem."""
    with engine.connect() as c:
        n_trade = int(c.execute(text("""
            SELECT COUNT(*) FROM options_recommendations
            WHERE outcome IN ('LOSS', 'EXPIRED')
              AND closed_at >= CURRENT_DATE - (:d || ' days')::interval
              AND id NOT IN (SELECT trade_id FROM trade_postmortems WHERE trade_id IS NOT NULL)
        """), {"d": int(days)}).scalar() or 0)
        n_pred = int(c.execute(text("""
            SELECT COUNT(*) FROM oracle_predictions
            WHERE verdict = 'miss'
              AND scored_at >= CURRENT_DATE - (:d || ' days')::interval
              AND id NOT IN (SELECT prediction_id FROM trade_postmortems WHERE prediction_id IS NOT NULL)
        """), {"d": int(days)}).scalar() or 0)
    return n_trade, n_pred


def _spawn_worker(
    provider: str,
    partition_idx: int,
    partitions: int,
    days: int,
    max_rows: int | None,
    log_dir: Path,
) -> subprocess.Popen[bytes]:
    """Fork a single-worker subprocess pinned to ``provider``."""
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"worker-{partition_idx}-{provider}.log"

    env = os.environ.copy()
    env["LLM_ORACLE_PROVIDER"] = provider
    env["GRID_POSTMORTEM_PARTITION_IDX"] = str(partition_idx)
    env["GRID_POSTMORTEM_PARTITIONS"] = str(partitions)
    env["GRID_POSTMORTEM_DAYS"] = str(days)
    if max_rows is not None:
        env["GRID_POSTMORTEM_MAX_ROWS"] = str(max_rows // max(partitions, 1))

    log.info(
        "parallel_postmortem: spawning worker {i}/{n} on provider={p} → {l}",
        i=partition_idx, n=partitions, p=provider, l=log_path,
    )
    return subprocess.Popen(
        [sys.executable, "-m", "scripts.parallel_postmortem_runner", "--worker"],
        env=env,
        stdout=log_path.open("ab"),
        stderr=subprocess.STDOUT,
        cwd=str(Path(__file__).resolve().parent.parent),
    )


def _worker_main() -> int:
    """Worker mode: process one partition of the candidate set."""
    idx = int(os.environ.get("GRID_POSTMORTEM_PARTITION_IDX", "0"))
    partitions = int(os.environ.get("GRID_POSTMORTEM_PARTITIONS", "1"))
    days = int(os.environ.get("GRID_POSTMORTEM_DAYS", "30"))
    max_rows_env = os.environ.get("GRID_POSTMORTEM_MAX_ROWS", "")
    max_rows = int(max_rows_env) if max_rows_env.strip() else None
    provider = os.environ.get("LLM_ORACLE_PROVIDER", "default")

    engine = get_engine()
    from intelligence.postmortem import generate_postmortem, generate_prediction_postmortem

    started = datetime.now(timezone.utc)
    log.info(
        "postmortem_worker {i}/{n} starting on provider={p}, days={d}, max={m}",
        i=idx, n=partitions, p=provider, d=days, m=max_rows,
    )

    # Pull candidate ids ordered + partition by hash. Use the BigInt id
    # column's modulus to slice — no coordination needed across workers.
    with engine.connect() as c:
        trade_rows = c.execute(text("""
            SELECT id FROM options_recommendations
            WHERE outcome IN ('LOSS', 'EXPIRED')
              AND closed_at >= CURRENT_DATE - (:d || ' days')::interval
              AND id NOT IN (SELECT trade_id FROM trade_postmortems WHERE trade_id IS NOT NULL)
              AND MOD(ABS(id), :n) = :i
            ORDER BY closed_at DESC
        """), {"d": days, "n": partitions, "i": idx}).fetchall()
        pred_rows = c.execute(text("""
            SELECT id FROM oracle_predictions
            WHERE verdict = 'miss'
              AND scored_at >= CURRENT_DATE - (:d || ' days')::interval
              AND id NOT IN (SELECT prediction_id FROM trade_postmortems WHERE prediction_id IS NOT NULL)
              AND MOD(ABS(HASHTEXT(id::text)), :n) = :i
            ORDER BY scored_at DESC
        """), {"d": days, "n": partitions, "i": idx}).fetchall()

    log.info(
        "worker {i}: {nt} trade candidates, {np} pred candidates",
        i=idx, nt=len(trade_rows), np=len(pred_rows),
    )

    processed = 0
    for (tid,) in trade_rows:
        if max_rows is not None and processed >= max_rows:
            break
        try:
            generate_postmortem(engine, tid)
            processed += 1
        except Exception as exc:  # noqa: BLE001
            log.debug("worker {i} trade {t} failed: {e}", i=idx, t=tid, e=str(exc))
    for (pid,) in pred_rows:
        if max_rows is not None and processed >= max_rows:
            break
        try:
            generate_prediction_postmortem(engine, pid)
            processed += 1
        except Exception as exc:  # noqa: BLE001
            log.debug("worker {i} pred {p} failed: {e}", i=idx, p=pid, e=str(exc))

    elapsed = (datetime.now(timezone.utc) - started).total_seconds()
    log.info(
        "worker {i} done: {p} postmortems in {e:.1f}s on provider={prov}",
        i=idx, p=processed, e=elapsed, prov=provider,
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--worker", action="store_true",
                    help="Internal: this process is one partition worker.")
    ap.add_argument("--providers", nargs="+", default=_DEFAULT_PROVIDERS,
                    help="LLM provider names to fan out across.")
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--max-rows", type=int, default=None,
                    help="Cap total rows processed across all workers.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print plan + candidate counts, don't fork workers.")
    ap.add_argument("--log-dir", type=str,
                    default="/home/grid/logs/postmortem-workers")
    args = ap.parse_args(argv)

    if args.worker:
        return _worker_main()

    engine = get_engine()
    n_trade, n_pred = _count_candidates(engine, days=args.days)
    total = n_trade + n_pred
    providers = list(args.providers)
    partitions = len(providers)

    print(f"=== parallel_postmortem_runner ===")
    print(f"  candidates: {n_trade:,} trades + {n_pred:,} predictions = {total:,}")
    print(f"  partitions: {partitions} (providers: {', '.join(providers)})")
    print(f"  per worker (approx): {total // max(partitions, 1):,} candidates")
    print(f"  max-rows total: {args.max_rows if args.max_rows is not None else 'unlimited'}")
    print(f"  log dir: {args.log_dir}")

    if args.dry_run:
        print("\nDry-run, not spawning workers.")
        return 0

    if total == 0:
        print("\nNo candidates — nothing to do.")
        return 0

    log_dir = Path(args.log_dir)
    started = time.time()
    procs: list[tuple[subprocess.Popen[bytes], str, int]] = []
    for idx, provider in enumerate(providers):
        p = _spawn_worker(provider, idx, partitions, args.days, args.max_rows, log_dir)
        procs.append((p, provider, idx))

    print(f"\n{len(procs)} workers spawned. Tailing logs in {log_dir}/")

    # Wait for all workers
    exit_codes = []
    for p, provider, idx in procs:
        rc = p.wait()
        exit_codes.append(rc)
        print(f"  worker {idx} ({provider}) exited rc={rc}")

    elapsed = time.time() - started
    print(f"\nAll workers complete in {elapsed:.1f}s")
    return max(exit_codes) if exit_codes else 0


if __name__ == "__main__":
    sys.exit(main())
