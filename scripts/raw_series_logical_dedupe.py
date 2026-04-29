#!/usr/bin/env python3
"""Safely clean logical duplicates from raw_series.

The legacy raw_series uniqueness model includes pull_timestamp, so re-pulls can
store multiple successful observations for the same logical key:
series_id/source_id/obs_date. This operator script removes duplicates in
batches, keeping the newest pull_timestamp/id per logical key.

Default mode is dry-run. Use --execute to delete. Scope by --source-id whenever
possible; --all-sources is intentionally explicit because raw_series is large.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any

import psycopg2
from loguru import logger as log

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import settings


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    scope = parser.add_mutually_exclusive_group(required=True)
    scope.add_argument("--source-id", type=int, help="Limit cleanup to one source_catalog.id.")
    scope.add_argument("--all-sources", action="store_true", help="Clean every source. Expensive.")
    parser.add_argument("--series-prefix", help="Optional series_id prefix scope, e.g. TIINGO: or YF:.")
    parser.add_argument("--batch-size", type=int, default=10_000, help="Duplicate rows to delete per batch.")
    parser.add_argument("--max-batches", type=int, default=1, help="Maximum batches to process in this run.")
    parser.add_argument("--execute", action="store_true", help="Actually delete duplicate rows.")
    parser.add_argument(
        "--create-logical-index",
        action="store_true",
        help="Create uq_raw_series_logical concurrently after cleanup. Requires no duplicates.",
    )
    parser.add_argument(
        "--drop-timestamp-index",
        action="store_true",
        help="Drop uq_raw_series_composite concurrently. Only use after every writer is verified.",
    )
    parser.add_argument("--statement-timeout-ms", type=int, default=0)
    return parser.parse_args(argv)


def _connect():
    conn = psycopg2.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        dbname=settings.DB_NAME,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
    )
    conn.autocommit = True
    return conn


def _where(args: argparse.Namespace) -> tuple[str, dict[str, Any]]:
    parts = ["pull_status = 'SUCCESS'"]
    params: dict[str, Any] = {}
    if args.source_id is not None:
        parts.append("source_id = %(source_id)s")
        params["source_id"] = args.source_id
    if args.series_prefix:
        parts.append("series_id LIKE %(series_prefix)s")
        params["series_prefix"] = f"{args.series_prefix}%"
    return " AND ".join(parts), params


def _count_sample(cur: Any, args: argparse.Namespace) -> int:
    where_sql, params = _where(args)
    params["limit"] = args.batch_size
    cur.execute(
        f"""
        WITH duplicate_ids AS (
            SELECT id
            FROM (
                SELECT id,
                       row_number() OVER (
                           PARTITION BY series_id, source_id, obs_date
                           ORDER BY pull_timestamp DESC, id DESC
                       ) AS rn
                FROM raw_series
                WHERE {where_sql}
            ) ranked
            WHERE rn > 1
            LIMIT %(limit)s
        )
        SELECT count(*) FROM duplicate_ids
        """,
        params,
    )
    return int(cur.fetchone()[0] or 0)


def _delete_batch(cur: Any, args: argparse.Namespace) -> int:
    where_sql, params = _where(args)
    params["limit"] = args.batch_size
    cur.execute(
        f"""
        WITH duplicate_ids AS (
            SELECT id
            FROM (
                SELECT id,
                       row_number() OVER (
                           PARTITION BY series_id, source_id, obs_date
                           ORDER BY pull_timestamp DESC, id DESC
                       ) AS rn
                FROM raw_series
                WHERE {where_sql}
            ) ranked
            WHERE rn > 1
            LIMIT %(limit)s
        )
        DELETE FROM raw_series r
        USING duplicate_ids d
        WHERE r.id = d.id
        """,
        params,
    )
    return int(cur.rowcount or 0)


def _create_logical_index(cur: Any) -> None:
    cur.execute(
        """
        CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS uq_raw_series_logical
        ON raw_series(series_id, source_id, obs_date)
        WHERE pull_status = 'SUCCESS'
        """
    )


def _drop_timestamp_index(cur: Any) -> None:
    cur.execute("DROP INDEX CONCURRENTLY IF EXISTS uq_raw_series_composite")


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    if args.batch_size <= 0 or args.max_batches <= 0:
        raise SystemExit("--batch-size and --max-batches must be positive")

    conn = _connect()
    cur = conn.cursor()
    cur.execute("SET statement_timeout = %s", (args.statement_timeout_ms,))

    scope = f"source_id={args.source_id}" if args.source_id is not None else "all sources"
    if args.series_prefix:
        scope += f", series_prefix={args.series_prefix}"

    if not args.execute:
        sample = _count_sample(cur, args)
        log.info(
            "Dry run: found up to {n} duplicate rows in scope ({scope}). "
            "Use --execute to delete.",
            n=sample,
            scope=scope,
        )
        conn.close()
        return 0

    deleted_total = 0
    for batch in range(1, args.max_batches + 1):
        deleted = _delete_batch(cur, args)
        deleted_total += deleted
        log.info("Deleted {n} duplicate rows in batch {b}", n=deleted, b=batch)
        if deleted < args.batch_size:
            break

    if args.create_logical_index:
        log.info("Creating uq_raw_series_logical concurrently")
        _create_logical_index(cur)

    if args.drop_timestamp_index:
        log.info("Dropping uq_raw_series_composite concurrently")
        _drop_timestamp_index(cur)

    log.info("raw_series logical dedupe complete: deleted={n}, scope={scope}", n=deleted_total, scope=scope)
    conn.close()
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
