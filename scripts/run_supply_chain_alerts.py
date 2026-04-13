#!/usr/bin/env python3
"""GRID — Supply Chain Pulse runner.

CLI entrypoint for the supply chain alert watchdog. Runs every detector,
optionally sends the digest email, and persists a fresh snapshot table.

Usage:
    python3 scripts/run_supply_chain_alerts.py                  # dry-run
    python3 scripts/run_supply_chain_alerts.py --send-email
    python3 scripts/run_supply_chain_alerts.py --since-hours 48
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Allow running from either the repo root or /data/grid_v4/astrogrid_dedup.
_HERE = Path(__file__).resolve().parent
_ROOT = _HERE.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from loguru import logger as log  # noqa: E402


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the GRID supply chain alert watchdog.",
    )
    parser.add_argument(
        "--since-hours", type=int, default=24,
        help="Lookback window for new-row detectors (default: 24).",
    )
    parser.add_argument(
        "--send-email", action="store_true",
        help="Actually dispatch the digest email.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Do not send the digest email (default).",
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Print full findings as JSON to stdout.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    opts = parser.parse_args(argv)
    send_email = bool(opts.send_email and not opts.dry_run)

    try:
        from db import get_engine
    except Exception as exc:
        log.error("Cannot import db.get_engine: {e}", e=str(exc))
        return 2

    try:
        engine = get_engine()
    except Exception as exc:
        log.error("Cannot build DB engine: {e}", e=str(exc))
        return 2

    from alerts.supply_chain_alerts import run_all

    result = run_all(
        engine, since_hours=opts.since_hours, send_email=send_email
    )

    counts = {k: len(v) for k, v in result["findings"].items()}
    log.info(
        "Supply Chain Pulse run: total={t} sent={s} snapshots={n} counts={c}",
        t=result["total"],
        s=result["sent"],
        n=result["snapshots_written"],
        c=counts,
    )
    print(
        f"[supply_chain_alerts] total={result['total']} "
        f"sent={result['sent']} snapshots={result['snapshots_written']}"
    )
    for group, items in result["findings"].items():
        print(f"  {group}: {len(items)}")

    if opts.json:
        print(json.dumps(result, default=str, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
