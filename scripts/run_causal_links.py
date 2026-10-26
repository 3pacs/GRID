#!/usr/bin/env python3
"""Scheduled writer for ``causal_links`` (slice N2).

Links each recent insider / congressional trade to the public events on the
same ticker that were knowable BEFORE the trade day (reported earnings
releases; contract awards GRID had already seen), and upserts them with
provenance: run id, code sha, and the known_at of the trade, the event and
the edge. See ``intelligence/causal_links.py`` for the rules.

Bounded: at most ``--max-tickers`` tickers (most recently active first),
processed ``--batch-size`` tickers per transaction. Idempotent: edges are
keyed on ``edge_key``, so re-runs update rather than duplicate. Each run is
recorded in ``causal_link_runs`` (status running -> succeeded/failed).

Requires the ``causal_links_provenance_20260927`` migration; exits 2 without
writing if it is missing.

Usage:
    python3 scripts/run_causal_links.py                      # last 30 days
    python3 scripts/run_causal_links.py --days 90 --tickers AAPL,NVDA
    python3 scripts/run_causal_links.py --dry-run --json     # compute only

Timer template (not installed): deploy/systemd/grid-causal-links.timer.template
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _parse_as_of(raw: str | None) -> datetime | None:
    if not raw:
        return None
    dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument("--days", type=int, default=30, help="trade-date lookback (default 30)")
    p.add_argument("--tickers", default="", help="comma-separated tickers (default: all active)")
    p.add_argument("--batch-size", type=int, default=25, help="tickers per transaction")
    p.add_argument("--max-tickers", type=int, default=500, help="upper bound on tickers per run")
    p.add_argument("--as-of", default=None, help="ISO timestamp; only data known by then (default now)")
    p.add_argument("--code-sha", default=None, help="override the recorded code sha")
    p.add_argument("--dry-run", action="store_true", help="compute without writing")
    p.add_argument("--json", action="store_true", help="print the run summary as JSON")
    return p


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)
    if args.days < 1 or args.batch_size < 1 or args.max_tickers < 1:
        print("--days, --batch-size and --max-tickers must be >= 1", file=sys.stderr)
        return 2

    from intelligence.causal_links import (
        CausalLinksSchemaMissing,
        resolve_code_sha,
        run_causal_links,
    )

    if engine is None:
        from db import get_engine

        engine = get_engine()

    tickers = [t.strip().upper() for t in args.tickers.split(",") if t.strip()] or None
    try:
        summary = run_causal_links(
            engine,
            days=args.days,
            as_of=_parse_as_of(args.as_of),
            tickers=tickers,
            batch_size=args.batch_size,
            max_tickers=args.max_tickers,
            code_sha=args.code_sha or resolve_code_sha(_ROOT),
            dry_run=args.dry_run,
        )
    except CausalLinksSchemaMissing as exc:
        print(f"causal-links: {exc}", file=sys.stderr)
        return 2

    out = summary.to_dict()
    if args.json:
        print(json.dumps(out, sort_keys=True))
    else:
        print(
            f"causal-links run {out['run_id']} {out['status']}: "
            f"{out['tickers_processed']} tickers, {out['actions_processed']} trades, "
            f"{out['edges_found']} edges ({out['edges_written']} written), "
            f"as_of={out['as_of']} sha={out['code_sha'][:12]}"
            + (" [dry-run]" if out["dry_run"] else "")
        )
    return 0 if summary.status == "succeeded" else 1


if __name__ == "__main__":
    sys.exit(main())
