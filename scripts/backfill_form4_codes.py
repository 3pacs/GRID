"""Back-fill Form 4 transaction codes onto historical insider signals.

Background
----------
``ingestion/altdata/insider_filings.py`` has always parsed the Form 4
``<transactionCode>`` and stored it in ``raw_series.raw_payload``, but
``_emit_signal`` dropped it before writing ``signal_sources.signal_value``.
``intelligence/lever_pullers.py::assess_motivation`` therefore had nothing to
separate a planned 10b5-1 sale, an option exercise or a grant from a
discretionary trade, and narrated "Unknown motivation" for the bulk of the
insider lever events.

The puller now carries ``transaction_code``, ``is_10b5_1`` and
``direct_or_indirect`` through to ``signal_value``. This script patches the
rows that were written before that, using two sources in order:

1. ``raw_series.raw_payload`` — the puller's own record of the filing. Always
   has ``transaction_code``; has ``is_10b5_1`` / ``direct_or_indirect`` only
   for rows ingested after this change.
2. The filing itself, re-fetched from ``raw_payload->>'filing_url'`` and
   re-parsed (``--refetch``). Only rows ingested after this change carry the
   URL, so this is a no-op on the current backlog and exists so the next
   backfill does not have to guess.

Fields the sources cannot supply are left absent rather than defaulted, so a
later run can still fill them.

Idempotency
-----------
Only rows whose ``signal_value`` lacks ``transaction_code`` are considered,
and the write is a jsonb merge (``signal_value || :patch``) that adds keys
without disturbing ``price`` (read by ``trust_scorer._extract_price``),
``value``, ``shares`` or ``is_unusual_size``. Re-running is a no-op.

The query and the update are both bounded by ``signal_date`` so neither walks
the whole table. ``decision_journal`` is never touched.

Usage
-----
    python3 scripts/backfill_form4_codes.py --dry-run
    python3 scripts/backfill_form4_codes.py
    python3 scripts/backfill_form4_codes.py --days 120 --batch-size 500
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine

# The insider feed written by ingestion/altdata/insider_filings.py. The
# QuiverQuant feed ('quiverquant:insider') already echoes its own
# TransactionCode field and is read through the alias table in lever_pullers,
# so it needs no patch.
SOURCE_TYPE: str = "insider"

DEFAULT_DAYS: int = 120
DEFAULT_BATCH_SIZE: int = 500

# Keys this script is allowed to add. Anything else in raw_payload stays out
# of signal_value.
PATCH_KEYS: tuple[str, ...] = (
    "transaction_code",
    "is_10b5_1",
    "direct_or_indirect",
)

_SELECT_CANDIDATES = text(
    """
    SELECT source_id, ticker, signal_date, signal_type
      FROM signal_sources
     WHERE source_type = :source_type
       AND signal_date >= :start_date
       AND signal_date <= :end_date
       AND NOT (signal_value ? 'transaction_code')
     ORDER BY signal_date DESC
    """
)

# raw_series is a TimescaleDB hypertable: obs_date is bounded on both sides so
# the planner prunes to the window's chunks instead of scanning every one. The
# payloads are read once and matched in Python rather than through a
# correlated per-row lookup on an unindexed jsonb expression.
_SELECT_PAYLOADS = text(
    """
    SELECT obs_date, raw_payload
      FROM raw_series
     WHERE series_id LIKE 'INSIDER:%'
       AND obs_date >= :start_date
       AND obs_date <= :end_date
    """
)

_UPDATE_ROW = text(
    """
    UPDATE signal_sources
       SET signal_value = signal_value || CAST(:patch AS jsonb)
     WHERE source_type = :source_type
       AND source_id = :source_id
       AND ticker = :ticker
       AND signal_date = :signal_date
       AND signal_type = :signal_type
       AND NOT (signal_value ? 'transaction_code')
    """
)


def _index_payloads(rows: Any) -> dict[tuple[str, str, date], dict[str, Any]]:
    """Index Form 4 raw payloads by the signal_sources natural key.

    ``_emit_signal`` writes ``source_id = insider_name`` (raw, not
    normalised), ``ticker`` and ``signal_date = transaction_date``, so those
    three fields identify the filing that produced a signal row.

    Parameters:
        rows: Result rows of ``(obs_date, raw_payload)``.

    Returns:
        ``{(ticker, insider_name, obs_date): payload}``.
    """
    indexed: dict[tuple[str, str, date], dict[str, Any]] = {}
    for obs_date, raw_payload in rows:
        payload = raw_payload
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except (json.JSONDecodeError, TypeError):
                continue
        if not isinstance(payload, dict):
            continue
        ticker = payload.get("ticker")
        insider_name = payload.get("insider_name")
        if not ticker or not insider_name:
            continue
        # A single filing can hold several transactions on the same day; the
        # one carrying a code wins so the patch is never empty by accident.
        key = (str(ticker), str(insider_name), obs_date)
        if key not in indexed or (
            payload.get("transaction_code")
            and not indexed[key].get("transaction_code")
        ):
            indexed[key] = payload
    return indexed


def _build_patch(payload: dict[str, Any] | None) -> dict[str, Any]:
    """Select the patchable Form 4 fields present in a raw_series payload.

    Parameters:
        payload: ``raw_series.raw_payload`` for the matching filing, or None.

    Returns:
        Patch dict with only the keys the payload actually supplies. Empty
        when nothing is recoverable — the caller skips such a row rather
        than stamping a default.
    """
    if not payload:
        return {}

    patch: dict[str, Any] = {}

    code = payload.get("transaction_code")
    if code:
        patch["transaction_code"] = str(code).strip().upper()[:1]

    if "is_10b5_1" in payload:
        patch["is_10b5_1"] = bool(payload["is_10b5_1"])

    nature = payload.get("direct_or_indirect")
    if nature:
        patch["direct_or_indirect"] = str(nature).strip().upper()[:1]

    return {k: v for k, v in patch.items() if k in PATCH_KEYS}


def _refetch_patch(payload: dict[str, Any] | None, engine: Engine) -> dict[str, Any]:
    """Re-parse the source Form 4 to recover fields the payload lacks.

    Only rows ingested after the puller started storing ``filing_url`` can be
    re-parsed; everything older returns ``{}``.

    Parameters:
        payload: ``raw_series.raw_payload`` for the matching filing.
        engine: SQLAlchemy engine (the puller needs one to construct).

    Returns:
        Patch dict from the re-parsed filing, or ``{}``.
    """
    if not payload:
        return {}
    filing_url = str(payload.get("filing_url") or "")
    if not filing_url:
        return {}

    from ingestion.altdata.insider_filings import InsiderFilingsPuller

    puller = InsiderFilingsPuller(engine)
    try:
        xml_text = puller._fetch_filing_detail(filing_url)
    except Exception as exc:  # noqa: BLE001 — upstream/network, not a bug here
        log.warning("re-fetch failed for {u}: {e}", u=filing_url, e=str(exc))
        return {}
    if not xml_text:
        return {}

    wanted_code = str(payload.get("transaction_code") or "").upper()[:1]
    for trade in puller._parse_form4_xml(xml_text):
        if wanted_code and str(trade.get("transaction_code", "")).upper()[:1] != wanted_code:
            continue
        return _build_patch(trade)
    return {}


def backfill(
    engine: Engine,
    days: int = DEFAULT_DAYS,
    dry_run: bool = True,
    batch_size: int = DEFAULT_BATCH_SIZE,
    refetch: bool = False,
) -> dict[str, int]:
    """Patch Form 4 codes onto insider rows missing them.

    Parameters:
        engine: SQLAlchemy engine.
        days: Look-back window, bounded on both sides of ``signal_date``.
        dry_run: When True, report what would change and write nothing.
        batch_size: Rows per committed transaction.
        refetch: Also re-parse the source filing when the payload is thin.

    Returns:
        Counts dict: examined, patched, skipped_no_payload, skipped_no_fields.
    """
    end_date = date.today()
    start_date = end_date - timedelta(days=days)

    window = {"start_date": start_date, "end_date": end_date}

    with engine.connect() as conn:
        candidates = conn.execute(
            _SELECT_CANDIDATES, {"source_type": SOURCE_TYPE, **window},
        ).mappings().all()
        payloads = _index_payloads(conn.execute(_SELECT_PAYLOADS, window))

    stats = {
        "examined": len(candidates),
        "patched": 0,
        "skipped_no_payload": 0,
        "skipped_no_fields": 0,
    }
    log.info(
        "Form 4 backfill: {n} insider rows without transaction_code "
        "between {s} and {e}",
        n=len(candidates),
        s=start_date,
        e=end_date,
    )

    pending: list[dict[str, Any]] = []

    for row in candidates:
        payload = payloads.get(
            (row["ticker"], row["source_id"], row["signal_date"])
        )
        if payload is None:
            stats["skipped_no_payload"] += 1
            continue

        patch = _build_patch(payload)
        if refetch and "is_10b5_1" not in patch:
            patch.update(_refetch_patch(payload, engine))

        if not patch:
            stats["skipped_no_fields"] += 1
            continue

        pending.append({
            "source_type": SOURCE_TYPE,
            "source_id": row["source_id"],
            "ticker": row["ticker"],
            "signal_date": row["signal_date"],
            "signal_type": row["signal_type"],
            "patch": json.dumps(patch),
        })

        if dry_run:
            stats["patched"] += 1
            pending.clear()
            continue

        if len(pending) >= batch_size:
            stats["patched"] += _flush(engine, pending)
            pending.clear()

    if pending and not dry_run:
        stats["patched"] += _flush(engine, pending)

    log.info(
        "Form 4 backfill {mode}: examined={ex} patched={p} "
        "no_payload={np} no_fields={nf}",
        mode="DRY RUN" if dry_run else "applied",
        ex=stats["examined"],
        p=stats["patched"],
        np=stats["skipped_no_payload"],
        nf=stats["skipped_no_fields"],
    )
    return stats


def _flush(engine: Engine, batch: list[dict[str, Any]]) -> int:
    """Apply one batch of jsonb merges, returning the row count changed."""
    changed = 0
    with engine.begin() as conn:
        for params in batch:
            changed += conn.execute(_UPDATE_ROW, params).rowcount or 0
    return changed


def main(argv: list[str] | None = None) -> int:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Back-fill Form 4 transaction codes onto insider signals.",
    )
    parser.add_argument(
        "--days", type=int, default=DEFAULT_DAYS,
        help=f"Look-back window in days (default: {DEFAULT_DAYS}).",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Report what would change without writing.",
    )
    parser.add_argument(
        "--batch-size", type=int, default=DEFAULT_BATCH_SIZE,
        help=f"Rows per committed transaction (default: {DEFAULT_BATCH_SIZE}).",
    )
    parser.add_argument(
        "--refetch", action="store_true",
        help="Re-parse the source filing when raw_payload lacks the plan flag "
             "(only possible for rows that stored filing_url).",
    )
    args = parser.parse_args(argv)

    if args.days < 1:
        parser.error("--days must be >= 1")
    if args.batch_size < 1:
        parser.error("--batch-size must be >= 1")

    stats = backfill(
        engine=get_engine(),
        days=args.days,
        dry_run=args.dry_run,
        batch_size=args.batch_size,
        refetch=args.refetch,
    )
    print(json.dumps(stats, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
