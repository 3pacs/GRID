#!/usr/bin/env python3
"""Scheduled writer for ``regime_state_vectors`` (Wave 3 W3.2).

This is the ONLY writer of ``regime_state_vectors``. Computes and persists
the macro state vector for the prior completed trading session — never the
current/in-progress one. The ``/regime`` and ``/regime/analogs`` GET routes
(``api/routers/intelligence_regime.py``) are read-only: they call
``get_or_compute_state_vector(..., persist=False)``, so a request never
writes. When this job has run, a GET serves the row it wrote
(``cached: true``); otherwise a GET computes a vector in memory for that one
response only (``cached: false``, never stored).

See ``intelligence/regime/state_vector.py`` for the PIT readers
(``store.observations.read_window``, the resolved ``spy_full`` feature with
a raw ``YF:SPY:close`` fallback) and the ``MIN_CACHE_COMPLETENESS`` floor
that decides whether a computed vector is persisted at all.

Target date
-----------
``resolve_target_date()`` always resolves to a date strictly before
"today" (the day this process runs), via
``ingestion.market_calendar.last_trading_day(today - 1 day)``. That holds
regardless of what time of day this runs (the 23:00Z schedule, a manual
retry at 09:00Z, whatever) — it can never pick up today's still-open, or
just-closed, session. Pass ``--as-of`` to backfill one specific prior
session instead; a value that is not strictly before today is refused.

Usage:
    python3 scripts/run_regime_state_vectors.py                   # prior session
    python3 scripts/run_regime_state_vectors.py --dry-run --json
    python3 scripts/run_regime_state_vectors.py --as-of 2026-09-24 # backfill one day

Timer template (not installed):
    deploy/systemd/grid-regime-state-vectors.timer.template
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def _parse_as_of(raw: str) -> date:
    return datetime.strptime(raw, "%Y-%m-%d").date()


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    p.add_argument(
        "--as-of", default=None, type=_parse_as_of,
        help="backfill this one session's date (YYYY-MM-DD) instead of the "
             "prior completed session; must be strictly before today",
    )
    p.add_argument("--dry-run", action="store_true", help="compute without writing")
    p.add_argument("--json", action="store_true", help="print the run summary as JSON")
    p.add_argument(
        "--force-recompute", action="store_true",
        help="recompute even if a cached row for the target date already exists",
    )
    return p


def resolve_target_date(today: date | None = None) -> date:
    """The prior completed trading session — never today, however this runs.

    ``last_trading_day`` is on-or-before, so calling it on ``today - 1 day``
    guarantees the result is strictly earlier than ``today`` regardless of
    what time this process runs.
    """
    from ingestion.market_calendar import last_trading_day

    if today is None:
        today = date.today()
    return last_trading_day(today - timedelta(days=1))


def main(argv: list[str] | None = None, engine=None) -> int:
    args = build_parser().parse_args(argv)

    today = date.today()
    target = args.as_of if args.as_of is not None else resolve_target_date(today)
    if target >= today:
        print(
            f"regime-state-vectors: --as-of {target.isoformat()} is not strictly "
            f"before today ({today.isoformat()}) — refusing to compute a "
            "same-day/current-session vector",
            file=sys.stderr,
        )
        return 2

    from intelligence.regime.state_vector import MIN_CACHE_COMPLETENESS, get_or_compute_state_vector

    if engine is None:
        from db import get_engine

        engine = get_engine()

    # A dry run always computes fresh (never just echoes back whatever is
    # already cached) so the printed completeness/basis reflect the current
    # data, not a stale row from a previous real run.
    force_recompute = args.force_recompute or args.dry_run

    sv = get_or_compute_state_vector(
        engine,
        as_of=target,
        force_recompute=force_recompute,
        persist=not args.dry_run,
    )

    would_persist = sv.completeness >= MIN_CACHE_COMPLETENESS
    # sv.cached is only True when an existing row was served without being
    # recomputed (force_recompute was False and a sufficiently-complete
    # cached row existed) — nothing was written *this run* in that case.
    persisted = would_persist and not args.dry_run and not sv.cached

    summary = {
        "job": "run_regime_state_vectors",
        "as_of_date": sv.as_of_date.isoformat(),
        "completeness": sv.completeness,
        "min_cache_completeness": MIN_CACHE_COMPLETENESS,
        "price_basis": sv.price_basis,
        "stale_dimensions": list(sv.stale_dimensions),
        "cached": sv.cached,
        "persisted": persisted,
        "would_persist": would_persist,
        "dry_run": args.dry_run,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }

    if args.json:
        print(json.dumps(summary, sort_keys=True))
    else:
        if args.dry_run:
            status = "dry-run (not written)"
        elif persisted:
            status = "persisted"
        elif sv.cached:
            status = "already cached (unchanged)"
        else:
            status = "skipped (below completeness floor)"
        print(
            f"regime-state-vectors {summary['as_of_date']}: "
            f"completeness={sv.completeness:.0%} basis={sv.price_basis or 'unavailable'} "
            f"stale={len(sv.stale_dimensions)} -> {status}"
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
