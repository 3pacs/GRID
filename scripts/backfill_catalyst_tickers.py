#!/usr/bin/env python3
"""Backfill ``catalyst_calendar.ticker`` — replace sponsor-name fragments.

The trial ingestor populates ``catalyst_calendar.ticker`` via
``_resolve_ticker_sec(sponsor) or sponsor[:10]`` (grid/ingestors/trial_ingestor.py).
When the SEC fuzzy match fails, it stores the first 10 characters of the
SPONSOR NAME (e.g. ``"Moderna, I"``) in the ticker column. The
``upcoming_catalysts`` view then joins ``catalyst_calendar.ticker =
trial_signals.ticker`` on that garbage and returns nothing.

This script finds rows whose ``ticker`` is not a plausible exchange symbol,
re-resolves the sponsor (preferring ``notes``, which the ingestor seeds with
the sponsor name, then the ticker column itself) through the canonical
``grid.signals.trial_signal._resolve_ticker_sec`` resolver, and updates the
row ONLY when a confident ticker is found. Unresolvable rows are left
untouched (we never overwrite with another guess), and rows are optionally
deactivated via ``--deactivate-unresolved`` so the view stops joining on junk.

Usage:
    python3 scripts/backfill_catalyst_tickers.py --dry-run        # default: no writes
    python3 scripts/backfill_catalyst_tickers.py --apply          # perform updates
    python3 scripts/backfill_catalyst_tickers.py --apply --deactivate-unresolved
    python3 scripts/backfill_catalyst_tickers.py --limit 50       # sample

Safe to re-run — the resolver is deterministic. DRY-RUN IS THE DEFAULT; this
script does not write unless ``--apply`` is passed.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path
from typing import Callable, Optional

# Ensure we can import grid modules even when run as a script.
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from loguru import logger as log  # noqa: E402

_BATCH: int = 1000

# A plausible US exchange ticker: 1-5 uppercase letters, optional class suffix
# like ``.A`` / ``-B``. Sponsor-name fragments ("Moderna, I", "Genentech ")
# contain spaces/commas/lowercase or are too long, so they fail this.
_TICKER_RE = re.compile(r"^[A-Z]{1,5}([.\-][A-Z]{1,2})?$")


def looks_like_ticker(value: Optional[str]) -> bool:
    """Return True if ``value`` looks like a real exchange ticker.

    Pure + offline so it can be unit-tested. Used to decide which
    catalyst_calendar rows need re-resolution.
    """
    if not value:
        return False
    v = value.strip()
    if not v or len(v) > 8:
        return False
    return bool(_TICKER_RE.match(v))


def resolve_catalyst_ticker(
    current_ticker: Optional[str],
    sponsor_name: Optional[str],
    notes: Optional[str],
    resolver: Callable[[str], Optional[str]],
) -> Optional[str]:
    """Resolve the correct ticker for a catalyst_calendar row.

    Pure (the SEC lookup is injected as ``resolver``) so the resolution policy
    is unit-testable without network or DB.

    Policy:
        * If ``current_ticker`` already looks like a real ticker, keep it
          (return None — no update needed).
        * Otherwise try to resolve a sponsor name. The sponsor name is taken
          from ``sponsor_name`` if given, else parsed from ``notes`` (the
          ingestor writes ``"Sponsor: <name>"``-style notes), else the
          (garbage) ``current_ticker`` value itself as a last resort.
        * Return the resolved ticker only if it differs from the current
          value and looks like a ticker; otherwise None (leave row alone).

    Returns:
        The new ticker to write, or None if no confident change should be made.
    """
    if looks_like_ticker(current_ticker):
        return None

    candidate_name = (sponsor_name or "").strip()
    if not candidate_name and notes:
        # Notes may carry the sponsor, e.g. "Sponsor: Moderna, Inc." or
        # "leadSponsor=Moderna, Inc.".
        m = re.search(r"(?:sponsor|leadsponsor)\s*[:=]\s*(.+)", notes, re.I)
        if m:
            candidate_name = m.group(1).strip().rstrip(".")
    if not candidate_name:
        candidate_name = (current_ticker or "").strip()
    if not candidate_name:
        return None

    resolved = resolver(candidate_name)
    if not resolved:
        return None
    resolved = resolved.strip().upper()
    if not looks_like_ticker(resolved):
        return None
    if resolved == (current_ticker or "").strip().upper():
        return None
    return resolved


def _get_engine():
    """Return a DB engine using the project config."""
    from sqlalchemy import create_engine

    from config import settings
    return create_engine(settings.DB_URL)


def run(
    *,
    apply: bool,
    limit: Optional[int],
    deactivate_unresolved: bool,
) -> dict:
    """Scan catalyst_calendar and (optionally) backfill tickers.

    Returns a summary dict; performs writes only when ``apply`` is True.
    """
    from sqlalchemy import text

    from grid.signals.trial_signal import _resolve_ticker_sec

    engine = _get_engine()
    summary = {
        "scanned": 0,
        "needs_fix": 0,
        "resolved": 0,
        "deactivated": 0,
        "writes": 0,
        "dry_run": not apply,
    }

    sql = "SELECT id, ticker, notes FROM catalyst_calendar"
    if limit:
        sql += f" LIMIT {int(limit)}"

    with engine.connect() as conn:
        rows = conn.execute(text(sql)).fetchall()

    pending_updates: list[tuple[int, str]] = []
    pending_deactivate: list[int] = []

    for row_id, ticker, notes in rows:
        summary["scanned"] += 1
        if looks_like_ticker(ticker):
            continue
        summary["needs_fix"] += 1
        new_ticker = resolve_catalyst_ticker(ticker, None, notes, _resolve_ticker_sec)
        if new_ticker:
            summary["resolved"] += 1
            pending_updates.append((row_id, new_ticker))
        elif deactivate_unresolved:
            pending_deactivate.append(row_id)

    log.info(
        "catalyst_calendar backfill: scanned={s} needs_fix={n} resolved={r} "
        "unresolved={u}",
        s=summary["scanned"], n=summary["needs_fix"], r=summary["resolved"],
        u=summary["needs_fix"] - summary["resolved"],
    )
    for rid, new_t in pending_updates[:20]:
        log.info("  would set catalyst_calendar.id={i} ticker -> {t}", i=rid, t=new_t)

    if not apply:
        log.warning("DRY-RUN: no writes performed (pass --apply to update)")
        summary["deactivated"] = len(pending_deactivate)
        return summary

    with engine.begin() as conn:
        for rid, new_t in pending_updates:
            conn.execute(
                text("UPDATE catalyst_calendar SET ticker = :t WHERE id = :i"),
                {"t": new_t, "i": rid},
            )
            summary["writes"] += 1
        for rid in pending_deactivate:
            conn.execute(
                text("UPDATE catalyst_calendar SET is_active = FALSE WHERE id = :i"),
                {"i": rid},
            )
            summary["deactivated"] += 1

    log.info(
        "catalyst_calendar backfill applied: {w} tickers updated, {d} deactivated",
        w=summary["writes"], d=summary["deactivated"],
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply", action="store_true",
        help="Perform updates (default is dry-run, no writes).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Explicit dry-run (the default behaviour).",
    )
    parser.add_argument("--limit", type=int, default=None, help="Row cap (sampling).")
    parser.add_argument(
        "--deactivate-unresolved", action="store_true",
        help="Set is_active=FALSE for rows whose sponsor can't be resolved.",
    )
    args = parser.parse_args()

    run(
        apply=args.apply and not args.dry_run,
        limit=args.limit,
        deactivate_unresolved=args.deactivate_unresolved,
    )


if __name__ == "__main__":
    main()
