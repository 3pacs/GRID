#!/usr/bin/env python3
"""Runner for ``intelligence.news_contagion_listener``.

Scans recent ``news_articles`` headlines for shock-worthy events, resolves
every entity mention against ``sector_map`` + ``supply_chain_nodes``, and
fires ``simulate_contagion`` for every resolved hit. Results persist to
``contagion_predictions`` with ``source='news_listener'`` and a back-pointer
(``trigger_news_id``/``trigger_url``) to the article that caused the fire.

Usage
-----
    python scripts/run_news_contagion_listener.py
    python scripts/run_news_contagion_listener.py --since-hours 168 --dry-run
    python scripts/run_news_contagion_listener.py --since-hours 24 --limit 1000

``--dry-run`` prints exactly what WOULD have been triggered (pattern, news
id, resolved node, magnitude) without calling the simulator or writing any
rows. Idempotent across real runs — previously-fired (news_id, shock_node,
shock_type) triples are skipped.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

# Ensure repo root is importable.
_REPO_ROOT = str(Path(__file__).resolve().parent.parent)
if _REPO_ROOT not in sys.path:
    sys.path.insert(0, _REPO_ROOT)
os.chdir(_REPO_ROOT)

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from intelligence.news_contagion_listener import run_once  # noqa: E402


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Scan news_articles for shocks and auto-fire chain_contagion "
            "simulations."
        )
    )
    parser.add_argument(
        "--since-hours",
        type=int,
        default=24,
        help="Lookback window in hours (default: 24).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=500,
        help="Max news articles to scan (default: 500).",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Print what would be triggered without running simulations or "
            "writing rows."
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print the full report as JSON instead of a summary table.",
    )
    return parser.parse_args()


def _print_summary(report: dict) -> None:
    print("=" * 72)
    print(
        f"news_contagion_listener  "
        f"[dry_run={report['dry_run']}]  "
        f"since={report['since_hours']}h"
    )
    print("=" * 72)
    print(f"Articles scanned        : {report['scanned_articles']}")
    print(f"Candidates total        : {report['candidates_total']}")
    print(
        f"  resolved              : {report['resolved']} "
        f"(fired={report['fired']}, dup={report['skipped_duplicate']}, "
        f"errors={report['errors']})"
    )
    print(f"  unresolved            : {report['unresolved']}")
    if report.get("by_pattern"):
        print("By pattern:")
        for name, count in sorted(
            report["by_pattern"].items(), key=lambda x: -x[1]
        ):
            print(f"  {name:<24} {count}")
    preds = report.get("predictions") or []
    if preds:
        print("\nFired (first 20):")
        for p in preds[:20]:
            marker = "DRY" if p.get("dry_run") else f"id={p.get('prediction_id')}"
            print(
                f"  [{marker}] news={p['news_id']:>6}  "
                f"{p['pattern']:<18} {p['shock_node']:<24} "
                f"mag={p['magnitude']}"
            )
            print(f"         title: {p['title']}")
    samples = report.get("unresolved_samples") or []
    if samples:
        print("\nUnresolved samples (first 10):")
        for s in samples[:10]:
            print(
                f"  news={s['news_id']:>6}  pattern={s['pattern']:<18} "
                f"raw={s['raw_entity']!r}"
            )
            print(f"         title: {s['title']}")
    print("=" * 72)


def main() -> int:
    args = _parse_args()
    log.info(
        "news_contagion_listener: since_hours={h} limit={l} dry_run={d}",
        h=args.since_hours,
        l=args.limit,
        d=args.dry_run,
    )
    engine = get_engine()
    report = run_once(
        engine,
        since_hours=args.since_hours,
        dry_run=args.dry_run,
        limit=args.limit,
    )
    if args.json:
        print(json.dumps(report, indent=2, default=str))
    else:
        _print_summary(report)
    return 0


if __name__ == "__main__":
    sys.exit(main())
