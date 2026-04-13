#!/usr/bin/env python3
"""Runner for the actor news puller.

Usage::

    python scripts/run_actor_news.py --limit 50
    python scripts/run_actor_news.py --priority-only
    python scripts/run_actor_news.py --limit 200 --sources google_news,gdelt,wikipedia

Iterates every actor in analysis/sector_map.py (ordered by weight desc)
and pulls free-source news mentions + Wikipedia bio context. Writes to
``actor_news`` and ``actor_bio`` idempotently.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from loguru import logger as log  # noqa: E402

from db import get_engine  # noqa: E402
from ingestion.altdata.actor_news_puller import (  # noqa: E402
    ActorNewsPuller,
    enumerate_sector_map_actors,
)

_VALID_SOURCES = ["google_news", "gdelt", "wikipedia", "sec_edgar", "crossref"]


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Pull free-source news mentions and Wikipedia bios for sector_map actors."
    )
    parser.add_argument(
        "--limit", type=int, default=50,
        help="Max actors to process (default: 50, ordered by weight desc)",
    )
    parser.add_argument(
        "--priority-only", action="store_true",
        help="Only process actors with weight >= 0.04",
    )
    parser.add_argument(
        "--sources", type=str, default=",".join(_VALID_SOURCES),
        help=f"Comma-separated sources. Valid: {','.join(_VALID_SOURCES)}",
    )
    parser.add_argument(
        "--offset", type=int, default=0,
        help="Skip first N actors (for resuming)",
    )
    parser.add_argument(
        "--only-non-ticker", action="store_true",
        help="Only process actors without a ticker (family offices, activists, etc.)",
    )
    args = parser.parse_args()

    sources = [s.strip() for s in args.sources.split(",") if s.strip()]
    unknown = [s for s in sources if s not in _VALID_SOURCES]
    if unknown:
        parser.error(f"unknown sources: {unknown}. Valid: {_VALID_SOURCES}")

    actors = enumerate_sector_map_actors(priority_only=args.priority_only)
    if args.only_non_ticker:
        actors = [a for a in actors if not a.get("ticker")]

    actors = actors[args.offset : args.offset + args.limit]
    log.info("Processing {n} actors across {s} sources", n=len(actors), s=sources)

    engine = get_engine()
    puller = ActorNewsPuller(db_engine=engine)

    totals: dict[str, int] = {s: 0 for s in sources}
    per_actor: list[dict[str, object]] = []
    t0 = time.time()

    for i, actor in enumerate(actors, 1):
        try:
            counts = puller.pull_one_actor(actor, sources)
        except Exception as exc:  # noqa: BLE001
            log.error("actor {n} failed: {e}", n=actor["name"], e=exc)
            continue
        for k, v in counts.items():
            totals[k] = totals.get(k, 0) + v
        total_rows = sum(counts.values())
        per_actor.append({
            "actor_id": actor["actor_id"],
            "name": actor["name"],
            "ticker": actor.get("ticker"),
            "counts": counts,
            "total": total_rows,
        })
        log.info(
            "[{i}/{n}] {name} ({t}) — {c}",
            i=i, n=len(actors),
            name=actor["name"],
            t=actor.get("ticker") or actor.get("type"),
            c=counts,
        )

    elapsed = time.time() - t0
    summary = {
        "actors_processed": len(per_actor),
        "elapsed_sec": round(elapsed, 1),
        "totals_by_source": totals,
        "top10_by_volume": sorted(per_actor, key=lambda r: r["total"], reverse=True)[:10],
    }
    print(json.dumps(summary, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
