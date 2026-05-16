"""Resumable, idempotent backfill driver for the ~4,832 pre-fix inconclusive
``hypothesis_boost_log`` rows that need local-LLM scoring.

History
-------
The 2026-05-15 fix (PR #173) corrected the direction-vocabulary bug in
``intelligence.hypothesis_engine._normalize_direction`` and the
short-circuit logic in ``_check_ticker_move``. From that point forward,
new boost-log rows are scored mechanically with the correct verdict
distribution (~51% confirmed / 28% invalidated / 21% inconclusive on the
9,340 backlog re-run done the same day).

But the rows written BEFORE the fix carry a stale ``outcome='inconclusive'``
verdict even when the underlying ticker actually moved 13-78% over the
evaluation window. ``scripts/score_boost_log_with_local_llm.py`` reads
those rows and writes a corrected ``opus_outcome`` (plus ``opus_reasoning``
and provenance) using the local LLM with the framework documented in
``docs/scoring/boost_log_scoring_v1.md``.

That underlying scorer is BATCH-ONE: it pulls a single batch and exits.
This driver wraps it in a loop so an operator can fire-and-forget the
full backfill, watch progress, and resume after any interruption.

Idempotency
-----------
Resumability is free: the underlying scorer only selects rows where
``opus_outcome IS NULL`` and its UPDATE is constrained the same way.
Re-running the script after a kill, a crash, or even a successful
completion is therefore a no-op once the queue is drained.

Usage
-----
::

    # Count what's left without writing anything
    python3 scripts/backfill_inconclusive_boost_log.py --dry-run

    # Drain the queue with default batch size 50, tier REASON, no row cap
    python3 scripts/backfill_inconclusive_boost_log.py

    # Wall-clock cap (recommended when starting an overnight run)
    python3 scripts/backfill_inconclusive_boost_log.py --max-runtime-h 12

    # Bound the work explicitly for a metered tier
    python3 scripts/backfill_inconclusive_boost_log.py --tier ORACLE --max-rows 200

The recommended production kickoff (do NOT run from this worktree — kick
it on grid-svr inside tmux so it survives ssh drops) is::

    ssh grid@grid-svr 'tmux new -d -s boostlog-backfill \
        "cd /data/grid_v4/grid_repo && \
         python3 scripts/backfill_inconclusive_boost_log.py \
         --batch-size 50 --tier REASON --progress-every 5 \
         2>&1 | tee /data/grid_v4/logs/boostlog_backfill_$(date +%Y%m%d_%H%M).log"'

Wall-clock estimate: ~33 hours at the 2026-05-15 measured throughput of
~0.04 rows/sec for the local Nemotron-3-Super-120B CPU tier (the scoring
prompt has ~1.5 KB of system_knowledge + 4 few-shot examples). The driver
logs every ``--progress-every`` batches plus a final summary so an
operator can monitor without attaching to the tmux session.

The script is intentionally a *driver* — not a re-implementation. All
scoring logic, LLM client wiring, parsing, and DB writes live in
``scripts/score_boost_log_with_local_llm.py``. Keep them in sync; do not
fork the prompt or the framework doc.
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from loguru import logger as log
from sqlalchemy import text

# Allow ``python3 scripts/...`` from repo root
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db import get_engine  # noqa: E402


REMAINING_SQL = text(
    """
    SELECT COUNT(*)
    FROM hypothesis_boost_log bl
    JOIN discovered_hypotheses dh ON dh.id = bl.hypothesis_id
    WHERE bl.outcome = 'inconclusive'
      AND bl.opus_outcome IS NULL
      AND dh.pattern_type = 'convergence'
    """
)


def _count_remaining(engine) -> int:
    """Return the number of inconclusive convergence rows still unscored."""
    with engine.connect() as conn:
        row = conn.execute(REMAINING_SQL).fetchone()
    return int(row[0]) if row else 0


def _run_one_batch(batch_size: int, tier: str, dry_run: bool) -> int:
    """Invoke the underlying scorer once. Returns its exit code (0 on success)."""
    from scripts.score_boost_log_with_local_llm import main as score_main

    argv = ["--batch-size", str(batch_size), "--tier", tier]
    if dry_run:
        argv.append("--dry-run")
    return score_main(argv)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--batch-size", type=int, default=50,
        help="Rows per inner-scorer batch (default: 50).",
    )
    parser.add_argument(
        "--tier", default="REASON", choices=["LOCAL", "REASON", "ORACLE"],
        help="LLM tier. REASON is the production default; "
             "ORACLE only for spot-checks (frontier cost).",
    )
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Stop after scoring approximately this many rows. "
             "Useful for tier=ORACLE spot-runs.",
    )
    parser.add_argument(
        "--max-runtime-h", type=float, default=None,
        help="Stop after this many wall-clock hours (default: unbounded).",
    )
    parser.add_argument(
        "--progress-every", type=int, default=5,
        help="Log a progress line every N batches (default: 5).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Score the next batch but don't write opus_outcome — useful "
             "for verifying LLM availability before kicking off the full "
             "33h run.",
    )
    parser.add_argument(
        "--count-only", action="store_true",
        help="Print remaining-row count and exit (no LLM calls). "
             "Equivalent to: SELECT count(*) FROM hypothesis_boost_log "
             "WHERE outcome='inconclusive' AND opus_outcome IS NULL.",
    )
    args = parser.parse_args(argv)

    engine = get_engine()
    start_remaining = _count_remaining(engine)
    log.info(
        "Backfill starting: {n} unscored inconclusive convergence rows remain",
        n=start_remaining,
    )

    if args.count_only:
        return 0
    if start_remaining == 0:
        log.info("Nothing to do — queue is already drained.")
        return 0

    start = time.monotonic()
    started_at = datetime.now(timezone.utc).isoformat()
    deadline_s = args.max_runtime_h * 3600 if args.max_runtime_h else None
    batch_idx = 0
    total_scored = 0

    while True:
        elapsed = time.monotonic() - start
        if deadline_s and elapsed >= deadline_s:
            log.info(
                "Stopping: hit --max-runtime-h={h} after {n} batches",
                h=args.max_runtime_h, n=batch_idx,
            )
            break

        before = _count_remaining(engine)
        if before == 0:
            log.info("Queue drained — exiting cleanly.")
            break

        rc = _run_one_batch(args.batch_size, args.tier, args.dry_run)
        if rc != 0:
            log.error(
                "Inner scorer returned non-zero exit code {rc} on batch {b}; "
                "aborting backfill so an operator can investigate "
                "(re-run is safe — idempotent).",
                rc=rc, b=batch_idx,
            )
            return rc

        after = _count_remaining(engine)
        # Dry-run never writes so ``after == before``; bail out to avoid an
        # infinite loop chewing LLM tokens for zero progress.
        scored_this_batch = before - after
        total_scored += scored_this_batch
        batch_idx += 1

        if args.dry_run:
            log.info("Dry-run batch complete — exiting (no DB writes).")
            break

        if args.max_rows and total_scored >= args.max_rows:
            log.info(
                "Stopping: hit --max-rows={m} after {n} batches "
                "({s} scored this run)",
                m=args.max_rows, n=batch_idx, s=total_scored,
            )
            break

        if scored_this_batch == 0:
            # Either an LLM outage (every parse failed) or the SELECT has
            # converged to "nothing eligible left". Either way, looping
            # would burn cycles for no benefit. Surface and stop.
            log.warning(
                "Batch {b} scored 0 rows — stopping. Inspect inner-scorer "
                "logs above for LLM/parsing failures, then re-run "
                "(idempotent).", b=batch_idx,
            )
            break

        if batch_idx % args.progress_every == 0:
            rate = total_scored / max(elapsed, 1.0)
            eta_s = after / rate if rate > 0 else float("inf")
            log.info(
                "Progress: batch={b} scored_this_batch={s} "
                "total_scored={t} remaining={r} rate={rate:.3f}/s "
                "eta={eta_h:.1f}h",
                b=batch_idx, s=scored_this_batch, t=total_scored,
                r=after, rate=rate, eta_h=eta_s / 3600 if eta_s != float("inf") else -1,
            )

    final_remaining = _count_remaining(engine)
    runtime_h = (time.monotonic() - start) / 3600
    log.info(
        "Backfill done: started_at={start} runtime_h={rt:.2f} "
        "started_remaining={sr} scored_this_run={sc} final_remaining={fr}",
        start=started_at, rt=runtime_h, sr=start_remaining,
        sc=total_scored, fr=final_remaining,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
