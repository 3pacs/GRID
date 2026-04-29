#!/usr/bin/env python3
"""Queue Surfacer missing-data requests for local LLM backfill.

This warms the same deduped ``llm_task_backlog`` path used by
``/api/v1/surfacer/candidates``. It is intentionally conservative: Surfacer
identifies gaps, this script queues model research/planning tasks, and the
deterministic pullers/backfills remain responsible for writing raw market data.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger as log

from api.dependencies import get_db_engine
from api.routers.surfacer import (
    _attach_conviction,
    _fetch_hypothesis_candidates,
    _fetch_oracle_candidates,
    _fetch_signal_candidates,
    _queue_missing_data_requests,
)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Queue Surfacer missing-data backfill tasks for local LLMs.",
    )
    parser.add_argument("--limit", type=int, default=50, help="Candidate scan limit per source.")
    parser.add_argument("--dry-run", action="store_true", help="Report gaps without enqueueing.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    engine = get_db_engine()
    candidates = []
    with engine.begin() as conn:
        per_source_limit = max(args.limit, 12)
        candidates.extend(_fetch_oracle_candidates(conn, per_source_limit))
        candidates.extend(_fetch_signal_candidates(conn, per_source_limit))
        candidates.extend(_fetch_hypothesis_candidates(conn, max(40, args.limit * 2)))
        candidates = _attach_conviction(conn, candidates)

        requests = [
            request
            for candidate in candidates
            for request in (candidate.get("conviction") or {}).get("missing_data_requests") or []
        ]

        if args.dry_run:
            result = {"queued": 0, "skipped": 0, "by_type": {}, "dry_run": True}
        else:
            result = _queue_missing_data_requests(conn, candidates)

    output = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "candidates_scanned": len(candidates),
        "missing_data_requests": len(requests),
        **result,
    }
    log.info("surfacer backfill queue result: {r}", r=output)
    print(json.dumps(output, indent=2, default=str))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
