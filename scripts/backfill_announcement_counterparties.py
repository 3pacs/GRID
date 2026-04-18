"""One-shot back-fill: resolve counterparty_id for historical
``period_type='announcement'`` rows in ``capital_flows`` that were
written before the alias dict in ``corporate_actions_parser.py`` was
expanded.

Background
----------
The 8-K corporate actions parser writes rows via an ``INSERT ... ON
CONFLICT DO UPDATE`` that keyed on ``counterparty_id``. Before the
alias dict was widened (FIX-E10), many real M&A deals landed with
``counterparty_id IS NULL`` because the target company name did not
map to a ticker. Re-running the parser did not help: the narrow unique
constraint matched, and the ``DO UPDATE`` clause did not touch
``counterparty_id``. Known misses include PFE-SGEN ($43B), NFLX-WBD
($82.7B), T (two $23B deals), PLD ($23B).

This script:
  1. Queries every NULL-counterparty announcement row.
  2. For each distinct (actor, accession) pair, re-fetches the 8-K text
     through the existing parser and re-extracts events.
  3. Where a newly-resolved counterparty matches a NULL row
     (same actor/period/flow_type/source_filing), it UPDATEs that row
     in-place — no new inserts, no duplicates.

Idempotency
-----------
Safe to re-run. If a row is already resolved the UPDATE's
``counterparty_id IS NULL`` guard skips it. The parser's upsert path
was also tightened (see ``_upsert_events``) so ongoing re-runs will
back-fill automatically; this script only exists to close the gap on
rows written before that tightening shipped.

Usage
-----
    python3 scripts/backfill_announcement_counterparties.py
    python3 scripts/backfill_announcement_counterparties.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from datetime import datetime
from typing import Any

from loguru import logger as log
from sqlalchemy import text

from db import get_engine
from ingestion.altdata.corporate_actions_parser import (
    CorporateActionsParser,
    ExtractedEvent,
)


def _load_unresolved(engine: Any) -> list[dict[str, Any]]:
    """Return every announcement row with no counterparty."""
    query = text(
        """
        SELECT id, actor_id, fiscal_period, flow_type,
               amount_usd, source_filing
          FROM capital_flows
         WHERE period_type = 'announcement'
           AND (counterparty_id IS NULL OR counterparty_id = '')
         ORDER BY actor_id, fiscal_period
        """
    )
    with engine.begin() as conn:
        rows = conn.execute(query).mappings().all()
    return [dict(r) for r in rows]


def _parse_source_filing(source_filing: str | None) -> tuple[str, str] | None:
    """Extract (YYYY-MM-DD, accession) from '8-K 2024-07-31 0000012927-24-000051'."""
    if not source_filing:
        return None
    parts = source_filing.split()
    if len(parts) < 3:
        return None
    try:
        datetime.strptime(parts[1], "%Y-%m-%d")
    except ValueError:
        return None
    return parts[1], parts[2]


def _reparse_filing(
    parser: CorporateActionsParser,
    ticker: str,
    date_str: str,
    accession: str,
) -> list[ExtractedEvent]:
    """Re-fetch a single 8-K by accession and re-run extraction."""
    cik = parser._resolve_cik(ticker.upper())
    if not cik:
        log.debug("no CIK for {t}", t=ticker)
        return []
    # The accession → primary_doc mapping is unknown without the index,
    # but _fetch_document falls back to enumerating the filing's doc
    # index and fetching press-release exhibits. Pass a safe default.
    doc_text = parser._fetch_document(cik, accession, primary_doc="")
    if not doc_text:
        log.debug("no doc text for {t}/{a}", t=ticker, a=accession)
        return []
    try:
        ann = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return []
    return parser._extract_events(ticker, ann, accession, doc_text)


def _apply_backfill(
    engine: Any,
    row: dict[str, Any],
    resolved_cp: str,
    dry_run: bool,
) -> bool:
    """UPDATE a single NULL row in-place. Returns True if row changed.

    If the unique key (actor, period, type, flow, cp, source_filing)
    already exists with a non-NULL counterparty_id (because an earlier
    re-run inserted a sibling row), the UPDATE would violate the
    constraint. In that case we delete the NULL row instead so the
    resolved row is the sole survivor.
    """
    if dry_run:
        log.info(
            "[DRY] would set {a}/{f}/{fp}/{amt:.0f} → cp={cp}",
            a=row["actor_id"],
            f=row["flow_type"],
            fp=row["fiscal_period"],
            amt=float(row["amount_usd"]),
            cp=resolved_cp,
        )
        return True
    # Check if a sibling row already exists with this counterparty.
    sibling_q = text(
        """
        SELECT id FROM capital_flows
         WHERE actor_id = :actor_id
           AND fiscal_period = :fiscal_period
           AND period_type = 'announcement'
           AND flow_type = :flow_type
           AND counterparty_id = :cp
           AND source_filing = :source_filing
           AND id <> :id
         LIMIT 1
        """
    )
    update_stmt = text(
        """
        UPDATE capital_flows
           SET counterparty_id = :cp,
               as_of = NOW()
         WHERE id = :id
           AND (counterparty_id IS NULL OR counterparty_id = '')
        """
    )
    delete_stmt = text(
        """
        DELETE FROM capital_flows
         WHERE id = :id
           AND (counterparty_id IS NULL OR counterparty_id = '')
        """
    )
    with engine.begin() as conn:
        sibling = conn.execute(
            sibling_q.bindparams(
                actor_id=row["actor_id"],
                fiscal_period=row["fiscal_period"],
                flow_type=row["flow_type"],
                cp=resolved_cp,
                source_filing=row["source_filing"],
                id=row["id"],
            )
        ).scalar()
        if sibling is not None:
            log.info(
                "sibling exists (id={sid}), deleting NULL row id={id}",
                sid=sibling,
                id=row["id"],
            )
            res = conn.execute(delete_stmt.bindparams(id=row["id"]))
        else:
            res = conn.execute(
                update_stmt.bindparams(cp=resolved_cp, id=row["id"])
            )
    return (res.rowcount or 0) > 0


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true", help="don't write, just report")
    args = ap.parse_args(argv)

    engine = get_engine()
    parser = CorporateActionsParser(engine)

    unresolved = _load_unresolved(engine)
    log.info("loaded {n} NULL-counterparty announcement rows", n=len(unresolved))
    if not unresolved:
        return 0

    # Group rows by (actor_id, accession) so we fetch each 8-K once.
    by_filing: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    unparsable = 0
    for row in unresolved:
        parsed = _parse_source_filing(row["source_filing"])
        if parsed is None:
            unparsable += 1
            continue
        date_str, accession = parsed
        key = (row["actor_id"], date_str, accession)
        by_filing[key].append(row)
    log.info(
        "deduped to {n} distinct (actor, filing) pairs ({u} rows had unparsable source_filing)",
        n=len(by_filing),
        u=unparsable,
    )

    fixed = 0
    scanned = 0
    try:
        for (ticker, date_str, accession), rows in by_filing.items():
            scanned += 1
            events = _reparse_filing(parser, ticker, date_str, accession)
            if not events:
                continue
            resolved_by_flow: dict[str, str] = {}
            for ev in events:
                if ev.counterparty_id and ev.flow_type not in resolved_by_flow:
                    resolved_by_flow[ev.flow_type] = ev.counterparty_id
            if not resolved_by_flow:
                continue
            for row in rows:
                cp = resolved_by_flow.get(row["flow_type"])
                if not cp:
                    continue
                if _apply_backfill(engine, row, cp, args.dry_run):
                    fixed += 1
                    log.info(
                        "fixed row id={id} {a}/{f} {fp} ${amt:.0f}M → cp={cp}",
                        id=row["id"],
                        a=row["actor_id"],
                        f=row["flow_type"],
                        fp=row["fiscal_period"],
                        amt=float(row["amount_usd"]) / 1e6,
                        cp=cp,
                    )
            if scanned % 10 == 0:
                log.info("progress: {s} filings scanned, {f} rows fixed", s=scanned, f=fixed)
    finally:
        parser.close()

    log.info(
        "backfill done: scanned {s} filings, fixed {f} rows (dry_run={d})",
        s=scanned,
        f=fixed,
        d=args.dry_run,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
