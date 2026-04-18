#!/usr/bin/env python3
"""
Backfill the ``signals`` JSONB column on existing oracle_predictions rows
with the 4-key conviction context (``regime``, ``fci_regime``, ``vix_level``,
``signal_contributions``) that the 11-layer conviction stack depends on.

Historical rows did not capture per-prediction Shapley weights, so
``signal_contributions`` is intentionally skipped on backfill (a single
``log.warning`` is emitted) — future rows written by
``oracle/engine.py::_store_predictions`` will populate it going forward.

Usage
-----

    # Dry run the entire backlog (no UPDATEs issued):
    python3 -m scripts.backfill_oracle_prediction_context --dry-run

    # Backfill everything older than 30 days, 1000 rows max:
    python3 -m scripts.backfill_oracle_prediction_context --days 30 --limit 1000

    # Target only rows missing the regime key:
    python3 -m scripts.backfill_oracle_prediction_context

Flags
-----

    --dry-run    Count target rows, compute context, but never write.
    --limit N    Stop after updating/inspecting N rows.
    --days N     Only touch rows whose created_at is within the last N days.

The script never raises on per-row errors — it logs + increments the skip
counter and moves on. Every UPDATE is parameterized (no string interpolation).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.prediction_context import build_prediction_context


LOG_EVERY = 100


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Backfill oracle_predictions signals with regime context.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compute context for every target row but never write.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after examining this many rows.",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help="Only touch rows with created_at within the last N days.",
    )
    return parser.parse_args(argv)


def _fetch_target_rows(
    engine: Engine,
    *,
    days: int | None,
    limit: int | None,
) -> list[tuple[Any, datetime | None, Any]]:
    """Fetch (id, created_at, signals) for rows missing signals.regime.

    Uses parameterized SQL. The WHERE clause covers three cases:
      * signals IS NULL
      * signals is the legacy list shape (`->>'regime'` returns NULL)
      * signals is a dict but regime key is missing/null
    """
    params: dict[str, Any] = {}
    clauses = [
        "(signals IS NULL OR (signals->>'regime') IS NULL)",
    ]
    if days is not None:
        clauses.append("created_at >= :cutoff")
        params["cutoff"] = datetime.now(timezone.utc) - timedelta(days=days)
    where = " AND ".join(clauses)

    sql = f"""
        SELECT id, created_at, signals
        FROM oracle_predictions
        WHERE {where}
        ORDER BY created_at DESC
    """
    if limit is not None:
        sql += " LIMIT :limit"
        params["limit"] = int(limit)

    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [(r[0], r[1], r[2]) for r in rows]


def _merge_context_into_signals(existing: Any, context: dict[str, Any]) -> dict[str, Any]:
    """Merge context keys into an existing signals payload, preserving shape.

    If the existing payload is a list (legacy shape), wrap under ``items``.
    Pre-existing keys in the dict payload are never overwritten.
    """
    if isinstance(existing, dict):
        merged: dict[str, Any] = dict(existing)
        if "items" not in merged:
            merged["items"] = []
    elif isinstance(existing, list):
        merged = {"items": list(existing)}
    elif existing is None:
        merged = {"items": []}
    else:
        # Unknown shape — wrap it for safety, don't drop it.
        merged = {"items": [], "legacy": existing}

    for key, value in context.items():
        merged.setdefault(key, value)
    return merged


def backfill(
    engine: Engine,
    *,
    dry_run: bool = False,
    limit: int | None = None,
    days: int | None = None,
) -> dict[str, int]:
    """Run the backfill and return a counter dict."""
    counters = {
        "examined": 0,
        "updated": 0,
        "dry_run": 0,
        "skipped": 0,
    }

    log.warning(
        "Historical backfill cannot recover per-prediction Shapley weights — "
        "``signal_contributions`` will be set to an empty dict on all backfilled rows."
    )

    try:
        rows = _fetch_target_rows(engine, days=days, limit=limit)
    except Exception as exc:
        log.error("failed to fetch target rows: {e}", e=str(exc))
        return counters

    log.info(
        "backfill target: {n} rows (dry_run={d}, days={days}, limit={l})",
        n=len(rows),
        d=dry_run,
        days=days,
        l=limit,
    )

    for row_id, created_at, existing_signals in rows:
        counters["examined"] += 1

        try:
            if isinstance(created_at, datetime):
                as_of_date = created_at.date()
            elif isinstance(created_at, date):
                as_of_date = created_at
            else:
                as_of_date = date.today()

            context = build_prediction_context(
                engine,
                as_of=as_of_date,
                # Historical rows have no shapley / votes — leave empty.
                model_weights=None,
                model_votes=None,
            )
            # Backfill cannot recover per-signal contributions.
            context["signal_contributions"] = {}

            merged = _merge_context_into_signals(existing_signals, context)

            if dry_run:
                counters["dry_run"] += 1
            else:
                with engine.begin() as conn:
                    conn.execute(
                        text(
                            """
                            UPDATE oracle_predictions
                            SET signals = CAST(:payload AS jsonb)
                            WHERE id = :id
                            """
                        ),
                        {"id": row_id, "payload": json.dumps(merged, default=str)},
                    )
                counters["updated"] += 1
        except Exception as exc:
            counters["skipped"] += 1
            log.debug("backfill row {rid} skipped: {e}", rid=row_id, e=str(exc))
            continue

        if counters["examined"] % LOG_EVERY == 0:
            log.info(
                "backfill progress — examined={ex} updated={up} dry={dr} skipped={sk}",
                ex=counters["examined"],
                up=counters["updated"],
                dr=counters["dry_run"],
                sk=counters["skipped"],
            )

    log.info(
        "backfill complete — examined={ex} updated={up} dry={dr} skipped={sk}",
        ex=counters["examined"],
        up=counters["updated"],
        dr=counters["dry_run"],
        sk=counters["skipped"],
    )
    return counters


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        from api.dependencies import get_db_engine

        engine = get_db_engine()
    except Exception as exc:
        log.error("cannot initialise DB engine: {e}", e=str(exc))
        return 2

    backfill(
        engine,
        dry_run=args.dry_run,
        limit=args.limit,
        days=args.days,
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
