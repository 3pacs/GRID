#!/usr/bin/env python3
"""
GRID — FinBERT sentiment scoring CLI.

Scores text from raw_series using ProsusAI/finbert and writes compound
sentiment values back as finbert.* series.

Usage:
    python3 scripts/run_finbert.py                          # score all sources
    python3 scripts/run_finbert.py --source GDELT           # score one source
    python3 scripts/run_finbert.py --batch-size 128 --limit 5000
    python3 scripts/run_finbert.py --rescore --device cuda   # force GPU, re-score all
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

# Ensure grid/ is on sys.path regardless of working directory
_GRID_DIR = str(Path(__file__).resolve().parent.parent)
os.chdir(_GRID_DIR)
if _GRID_DIR not in sys.path:
    sys.path.insert(0, _GRID_DIR)

from loguru import logger as log


def main() -> None:
    parser = argparse.ArgumentParser(
        description="GRID FinBERT sentiment scoring pipeline",
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Specific source to score (e.g. GDELT, social_sentiment). "
             "Omit to score all configured sources.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=64,
        help="Batch size for model inference (default: 64).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=10000,
        help="Max rows to score per source (default: 10000).",
    )
    parser.add_argument(
        "--rescore",
        action="store_true",
        help="Re-score previously scored rows.",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=None,
        choices=["cuda", "cpu"],
        help="Force device (auto-detects if omitted).",
    )
    args = parser.parse_args()

    log.info("FinBERT scoring pipeline starting")
    t0 = time.time()

    from db import get_engine
    from ingestion.ml.finbert_scorer import FinBERTScorer

    engine = get_engine()
    scorer = FinBERTScorer(
        db_engine=engine,
        batch_size=args.batch_size,
        device=args.device,
    )

    if args.source:
        results = [scorer.score_source(
            source_name=args.source,
            limit=args.limit,
            rescore=args.rescore,
        )]
    else:
        results = scorer.score_all_sources()

    # Print summary
    total_elapsed = time.time() - t0
    total_scored = sum(r.get("rows_scored", 0) for r in results)

    log.info("=" * 60)
    log.info("FinBERT scoring complete — {n} rows in {t:.1f}s", n=total_scored, t=total_elapsed)
    log.info("-" * 60)

    for r in results:
        status = r.get("status", "UNKNOWN")
        scored = r.get("rows_scored", 0)
        avg = r.get("avg_sentiment", 0.0)
        elapsed = r.get("elapsed_s", 0.0)
        source = r.get("source", "?")

        if status == "SUCCESS" and scored > 0:
            log.info(
                "  {s:<30} {n:>6} rows  avg={avg:+.4f}  ({t:.1f}s)",
                s=source, n=scored, avg=avg, t=elapsed,
            )
        else:
            log.info("  {s:<30} {st}", s=source, st=status)

    log.info("=" * 60)


if __name__ == "__main__":
    main()
