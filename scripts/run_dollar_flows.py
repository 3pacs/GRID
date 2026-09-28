#!/usr/bin/env python3
"""Scheduled writer for ``dollar_flows`` (Wave 3, W3.3).

Normalizes ``signal_sources`` + ``raw_series`` rows into
``dollar_flows.amount_usd`` figures, then persists via a DELETE-then-INSERT
over the touched date range. See ``intelligence/dollar_flows.py`` for the
per-source-type conversion rules.

Two honesty guards (GRID-WAVE3-HELD-WRITERS-TRIAGE-20260927 §4.3):
  - No real VWAP observation for a dark-pool row -> the row is dropped,
    never fabricated from the old ``_DEFAULT_VWAP_ESTIMATE`` ($50 flat).
  - A future-dated ``signal_date``/``obs_date`` -> dropped, never persisted.

Both are counted in the printed summary (``skipped_no_vwap`` /
``skipped_future_date``), never silently absorbed.

Usage:
    python3 scripts/run_dollar_flows.py                    # last 90 days, persist
    python3 scripts/run_dollar_flows.py --days 7            # last 7 days, persist
    python3 scripts/run_dollar_flows.py --dry-run --json    # compute only, no writes

Timer template (not installed): deploy/systemd/grid-dollar-flows.timer.template
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--days", type=int, default=90, help="lookback window in days (default 90)")
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="compute and count without touching the dollar_flows table",
    )
    p.add_argument("--json", action="store_true", help="print the run summary as JSON")
    return p


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)
    if args.days < 1:
        print("--days must be >= 1", file=sys.stderr)
        return 2

    from intelligence.dollar_flows import normalize_all_flows

    if engine is None:
        from db import get_engine

        engine = get_engine()

    summary = normalize_all_flows(engine, days=args.days, dry_run=args.dry_run)
    out = summary.to_dict()

    if args.json:
        print(json.dumps(out, sort_keys=True))
    else:
        print(
            f"dollar-flows run: {out['flows_count']} normalized, "
            f"{out['persisted']} persisted, "
            f"skipped_no_vwap={out['skipped_no_vwap']}, "
            f"skipped_future_date={out['skipped_future_date']}, "
            f"by_source={out['by_source']}"
            + (" [dry-run]" if out["dry_run"] else "")
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
