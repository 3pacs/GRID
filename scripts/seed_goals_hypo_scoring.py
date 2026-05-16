"""Seed ``score_active_hypothesis`` goals from overdue ``discovered_hypotheses``.

Day 1 helper for IDLE-FLEET-AGENT-LOOP. Mirrors the due-query in
``intelligence.hypothesis_engine.score_due_active_hypotheses`` so we
enqueue exactly the rows that would otherwise sit in the
hermes_operator backlog.

Defaults to ``--dry-run`` (prints what it would enqueue, writes nothing).
Pass ``--commit`` to actually insert into ``goal_queue``.

Examples:

    # Show top 100 candidates without writing anything.
    python scripts/seed_goals_hypo_scoring.py --limit 100

    # Enqueue 500 due hypotheses at priority 120 for medium-GPU nodes.
    python scripts/seed_goals_hypo_scoring.py --limit 500 \\
        --priority 120 --hardware-tier medium_gpu --commit
"""

from __future__ import annotations

import argparse
import sys
from typing import Sequence

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from intelligence.goal_queue import TIER_ORDER, enqueue_goal


DUE_HYPOTHESES_SQL = text(
    """
    SELECT id
    FROM discovered_hypotheses
    WHERE status = 'active'
      AND (
          created_at + make_interval(
              days => COALESCE((test_criteria->>'window_days')::int, 7)
          ) < NOW()
      )
    ORDER BY last_tested ASC NULLS FIRST, created_at ASC
    LIMIT :limit
    """
)


def fetch_due_hypothesis_ids(engine, limit: int) -> list[str]:
    """Return up to ``limit`` active-and-overdue hypothesis ids."""
    with engine.connect() as conn:
        rows = conn.execute(DUE_HYPOTHESES_SQL, {"limit": limit}).fetchall()
    return [str(r[0]) for r in rows]


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--limit", type=int, default=200,
        help="max number of hypotheses to enqueue (default 200)",
    )
    parser.add_argument(
        "--priority", type=int, default=100,
        help="goal_queue priority (default 100)",
    )
    parser.add_argument(
        "--hardware-tier", default="cpu", choices=TIER_ORDER,
        help="hardware tier the goals require (default cpu)",
    )
    parser.add_argument(
        "--dedupe-window", default="global",
        help="dedupe window key (default 'global')",
    )
    parser.add_argument(
        "--commit", action="store_true",
        help="actually insert; default is dry-run.",
    )
    parser.add_argument(
        "--dry-run", dest="commit", action="store_false",
        help="(default) preview only — do not insert. Mutually exclusive "
             "with --commit; if both are given, the last one wins.",
    )
    parser.set_defaults(commit=False)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    if args.limit <= 0:
        log.error("--limit must be positive")
        return 2

    engine = get_engine()
    ids = fetch_due_hypothesis_ids(engine, args.limit)
    log.info(
        "seed_goals_hypo_scoring: {n} overdue active hypotheses found (limit={lim})",
        n=len(ids), lim=args.limit,
    )

    if not ids:
        log.info("seed_goals_hypo_scoring: nothing to enqueue")
        return 0

    if not args.commit:
        preview = ", ".join(ids[:5])
        log.info(
            "DRY-RUN: would enqueue {n} score_active_hypothesis goals "
            "(priority={p}, tier={t}). First ids: {preview}{ell}",
            n=len(ids), p=args.priority, t=args.hardware_tier,
            preview=preview, ell="..." if len(ids) > 5 else "",
        )
        return 0

    inserted = 0
    skipped = 0
    for hid in ids:
        new_id = enqueue_goal(
            engine,
            goal_type="score_active_hypothesis",
            target_id=hid,
            payload={"source": "seed_goals_hypo_scoring"},
            priority=args.priority,
            hardware_tier=args.hardware_tier,
            allow_cloud=False,
            dedupe_window=args.dedupe_window,
        )
        if new_id is None:
            skipped += 1
        else:
            inserted += 1

    log.info(
        "seed_goals_hypo_scoring: inserted={i} skipped(dedupe)={s}",
        i=inserted, s=skipped,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
