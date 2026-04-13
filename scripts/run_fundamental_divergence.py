#!/usr/bin/env python3
"""Runner for ``intelligence.fundamental_divergence.snapshot_all``.

Scores every ticker in ``analysis.sector_map`` for fundamental-vs-price
divergence and upserts the result into ``fundamental_divergence``.

Usage:
    python scripts/run_fundamental_divergence.py
    python scripts/run_fundamental_divergence.py --as-of 2026-04-11
    python scripts/run_fundamental_divergence.py --dry-run
    python scripts/run_fundamental_divergence.py --verbose
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import date
from pathlib import Path

# Ensure repo root is importable.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from intelligence.fundamental_divergence import (  # noqa: E402
    compute_divergence,
    snapshot_all,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute + upsert fundamental-vs-price divergence rows.",
    )
    parser.add_argument(
        "--as-of",
        type=str,
        default=None,
        help="YYYY-MM-DD snapshot date (defaults to today).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute but do not write to fundamental_divergence.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable debug logging.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    if args.verbose:
        log.remove()
        log.add(sys.stderr, level="DEBUG")

    as_of: date | None = None
    if args.as_of:
        try:
            as_of = date.fromisoformat(args.as_of)
        except ValueError:
            log.error("Invalid --as-of {v!r}; must be YYYY-MM-DD", v=args.as_of)
            return 2

    engine = get_engine()

    if args.dry_run:
        rows = compute_divergence(engine, as_of=as_of)
        counts: dict[str, int] = {}
        for r in rows:
            counts[r["classification"]] = counts.get(r["classification"], 0) + 1
        print(f"fundamental_divergence dry-run: {len(rows)} rows")
        for k, v in sorted(counts.items()):
            print(f"  {k}: {v}")
        return 0

    summary = snapshot_all(engine, as_of=as_of)
    print("fundamental_divergence summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
    if summary.get("error"):
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
