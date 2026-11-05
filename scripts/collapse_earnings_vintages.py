"""Collapse repeated EARNINGS:* vintages in raw_series (run-length, PIT-safe).

Until 2026-11-04 ``EarningsCalendarPuller`` re-inserted every historical
earnings row on every pull, so ``raw_series`` held ~2.53M EARNINGS SUCCESS
rows for ~16.5k (series_id, obs_date) keys. Within each key, ordered by
``pull_timestamp``, a row is kept when it is the first or its value differs
from the previous row; the rest are exact repeats and are deleted. The first
time each value was seen survives, so every as-of read returns what it did
before, and genuine revisions (~1.4k) are kept.

``scripts/raw_series_logical_dedupe.py`` must NOT be used for this: it keeps
only the newest row per key (deleting revisions and moving known-at times)
and its logical index would block future revision writes.

Dry run by default (counts only). ``--apply`` deletes in per-series batches,
each its own short transaction with statement/lock timeouts, never touching
a row referenced by ``astrogrid.price_close_receipt``. A JSON receipt is
written either way.

Usage:
    python scripts/collapse_earnings_vintages.py --source-id 351
    python scripts/collapse_earnings_vintages.py --source-id 351 --apply
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Connection, Engine

SERIES_PREFIX = "EARNINGS:"

_SERIES_SQL = text(
    "SELECT DISTINCT series_id FROM raw_series "
    "WHERE source_id = :src AND series_id LIKE :prefix AND pull_status = 'SUCCESS' "
    "ORDER BY series_id"
)

# Rows that are exact repeats of the previous vintage of the same key.
_REPEATS_CTE = (
    "WITH ordered AS ("
    "  SELECT id, value, LAG(value) OVER ("
    "    PARTITION BY series_id, obs_date ORDER BY pull_timestamp, id) AS prev "
    "  FROM raw_series "
    "  WHERE source_id = :src AND series_id = ANY(:sids) AND pull_status = 'SUCCESS'"
    "), repeats AS ("
    "  SELECT o.id FROM ordered o "
    "  WHERE o.prev IS NOT NULL AND o.value = o.prev "
    "    AND NOT EXISTS (SELECT 1 FROM astrogrid.price_close_receipt p WHERE p.raw_series_id = o.id)"
    ") "
)
_COUNT_SQL = text(
    _REPEATS_CTE
    + "SELECT (SELECT count(*) FROM ordered), (SELECT count(*) FROM repeats)"
)
_DELETE_SQL = text(
    _REPEATS_CTE
    + "DELETE FROM raw_series r USING repeats WHERE r.id = repeats.id"
)


def _price_receipt_table_exists(conn: Connection) -> bool:
    return bool(conn.execute(text("SELECT to_regclass('astrogrid.price_close_receipt') IS NOT NULL")).scalar())


def collapse(
    engine: Engine,
    source_id: int,
    *,
    apply: bool = False,
    batch_series: int = 20,
    statement_timeout_s: int = 60,
) -> dict[str, Any]:
    """Count (dry run) or delete exact-repeat EARNINGS vintages.

    Returns totals: rows scanned, rows that are repeats, rows deleted.
    """
    started = time.monotonic()
    with engine.connect() as conn:
        series = [r[0] for r in conn.execute(
            _SERIES_SQL, {"src": source_id, "prefix": SERIES_PREFIX + "%"}).fetchall()]
        if not _price_receipt_table_exists(conn):
            raise RuntimeError("astrogrid.price_close_receipt missing; refusing to run without the FK guard")
    out: dict[str, Any] = {
        "source_id": source_id, "mode": "apply" if apply else "dry_run",
        "series": len(series), "rows_scanned": 0, "repeat_rows": 0, "deleted": 0,
        "batches": 0, "errors": [],
    }
    for i in range(0, len(series), batch_series):
        sids = series[i:i + batch_series]
        params = {"src": source_id, "sids": sids}
        try:
            with engine.begin() as conn:
                conn.execute(text("SELECT set_config('statement_timeout', :t, true)"),
                             {"t": f"{statement_timeout_s}s"})
                conn.execute(text("SELECT set_config('lock_timeout', '5s', true)"))
                scanned, repeats = conn.execute(_COUNT_SQL, params).one()
                out["rows_scanned"] += int(scanned)
                out["repeat_rows"] += int(repeats)
                if apply and repeats:
                    deleted = conn.execute(_DELETE_SQL, params).rowcount
                    if deleted != repeats:
                        raise RuntimeError(f"deleted {deleted} != counted {repeats}; rolled back")
                    out["deleted"] += int(deleted)
        except Exception as exc:  # one failed batch rolls back alone; others continue
            log.warning("batch {b} ({first}..{last}) failed: {e}",
                        b=out["batches"], first=sids[0], last=sids[-1], e=str(exc))
            out["errors"].append({"first_series": sids[0], "error": str(exc)[:300]})
        out["batches"] += 1
    out["kept"] = out["rows_scanned"] - (out["deleted"] if apply else out["repeat_rows"])
    out["elapsed_s"] = round(time.monotonic() - started, 1)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--source-id", type=int, required=True,
                        help="source_catalog id that owns the EARNINGS:* rows (351 on griddb)")
    parser.add_argument("--apply", action="store_true", help="delete (default: dry run, counts only)")
    parser.add_argument("--batch-series", type=int, default=20)
    parser.add_argument("--statement-timeout-s", type=int, default=60)
    parser.add_argument("--receipt-dir", default=".server-logs")
    args = parser.parse_args()

    from db import get_engine

    result = collapse(get_engine(), args.source_id, apply=args.apply,
                      batch_series=args.batch_series, statement_timeout_s=args.statement_timeout_s)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    receipt = Path(args.receipt_dir) / f"collapse_earnings_vintages_{result['mode']}_{stamp}.json"
    receipt.parent.mkdir(parents=True, exist_ok=True)
    receipt.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps({k: v for k, v in result.items() if k != "errors"}), f"errors={len(result['errors'])}",
          f"receipt={receipt}")
    return 1 if result["errors"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
