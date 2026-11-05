#!/usr/bin/env python3
"""Rescore the legacy grid-crypto-v1 predictions against their coin (one-shot).

Until PR #968 these 42,488 rows stored ``target_symbols`` as a character
list, and the scorer before 766d3058 graded 31,329 of them against SPY.
Readers now exclude those SPY scores (``store/astrogrid.py``,
``_effective_scores_sql``); this runner writes the replacement scores.

* Dry run by default: every transaction is READ ONLY (psycopg2
  ``postgresql_readonly``) and nothing is written. It prints
  the counts an ``--apply`` run would produce.
* ``--apply`` inserts rows into ``astrogrid.prediction_score_legacy_rescore``
  (``ON CONFLICT DO NOTHING``) and bounded-skip rows for predictions that
  cannot be scored. It never updates or deletes anything. Re-running adds
  nothing for predictions already rescored.
* Each legacy prediction is measured at its horizon end (start + 7 or 30
  days) with the scorer's canonical-price rules; immature or unpriced
  predictions stay pending.

Usage::

    python scripts/rescore_legacy_crypto_predictions.py                 # dry run
    python scripts/rescore_legacy_crypto_predictions.py --apply --batch-size 500
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy.engine import Engine

from store.astrogrid import AstroGridStore


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true", help="write rescores (default: read-only dry run)")
    parser.add_argument("--batch-size", type=int, default=500, help="predictions per transaction (default 500)")
    parser.add_argument("--limit", type=int, default=None, help="stop after this many candidates")
    parser.add_argument(
        "--as-of", type=date.fromisoformat, default=None,
        help="evaluation date for the maturity check (YYYY-MM-DD, default: today UTC)",
    )
    args = parser.parse_args(argv)
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")
    if args.limit is not None and args.limit < 1:
        parser.error("--limit must be >= 1")
    return args


def read_only_engine(engine: Engine) -> Engine:
    """The same engine and pool, but every transaction is READ ONLY (psycopg2)."""
    return engine.execution_options(postgresql_readonly=True)


def log_progress(summary: dict[str, Any]) -> None:
    log.info(
        "batch {b}: candidates={c} scored={s} inserted={i} not_mature={n} unscored={u}",
        b=summary["batches"], c=summary["candidates"], s=summary["scored"],
        i=summary["inserted"], n=summary["not_mature"],
        u=sum(summary["unscored_by_reason"].values()),
    )


def main(argv: list[str] | None = None, *, engine: Engine | None = None) -> int:
    args = parse_args(argv)
    if engine is None:
        from db import get_engine

        engine = get_engine()
    store_engine = engine if args.apply else read_only_engine(engine)
    log.info("legacy crypto rescore: mode={m}", m="APPLY" if args.apply else "dry run (read-only)")
    summary = AstroGridStore(store_engine).rescore_legacy_crypto_predictions(
        apply=args.apply,
        as_of_date=args.as_of,
        batch_size=args.batch_size,
        limit=args.limit,
        progress=log_progress,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
