"""One-shot backfill: pull full Tiingo daily price history for the 224 VS1
v3 issuer tickers the 2026-09-28 GD4 price-admission probe found with no
price series at all (reason ``no_source_series`` — zero TIINGO rows,
source_catalog id 524).

Context: docs handed to this script live outside the repo —
``GRID-VS1-V3-PRICE-PROBE-20260928.md`` and the probe's
``probe_report.json`` on grid-svr at ``/data/sec/vs3/probe_20260928/``.
The 224-ticker list this script defaults to
(``data/vs1_v3_tiingo_backfill_tickers.json``) was extracted from that
report's ``tickers[*].reasons`` field.

This does NOT invent a new writer. It drives the existing
``ingestion.tiingo_pull.TiingoPuller`` unchanged:
  * same source_id (524, resolved via ``TIINGO`` in source_catalog)
  * same series naming (``YF:{ticker}:close``, ``YF:{ticker}:adj_close``, …)
  * same idempotent insert (``WHERE NOT EXISTS`` in ``pull_ticker``, keyed on
    series_id + source_id + obs_date + pull_status='SUCCESS')
  * same rate limiting (``TiingoPuller.pull_all``'s 0.2s/call delay)

This file only supplies the ticker list, a full-history start date, and a
structured per-ticker report (rows inserted this run, total rows now on
file, first/last obs_date, and which tickers Tiingo has no data for at
all). It performs no raw_series deletes or updates, creates no databases,
and does not touch resolved_series or any downstream table.

Run on grid-svr (needs TIINGO_API_KEY in the environment / .env and DB
access):

    python3 scripts/tiingo_vs1_v3_backfill.py \\
        --tickers-file data/vs1_v3_tiingo_backfill_tickers.json \\
        --start-date 1990-01-01 \\
        --report-out /tmp/vs1_tiingo_backfill_report.json

Idempotency: safe to re-run. Tickers/dates already present with
pull_status='SUCCESS' are skipped by the existing puller's own dedup
check; re-running only fills gaps (e.g. a prior partial run, or a new
trading day).
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from db import get_engine
from ingestion.tiingo_pull import TiingoPuller


def load_tickers(path: str) -> list[str]:
    """Load and validate a JSON list of ticker strings from ``path``."""
    with open(path) as f:
        data = json.load(f)
    if not isinstance(data, list) or not all(isinstance(t, str) for t in data):
        raise ValueError(f"{path} must contain a JSON list of ticker strings")
    return data


def obs_date_summary(
    engine: Engine, source_id: int, ticker: str
) -> tuple[str | None, str | None, int]:
    """Read-only: first/last obs_date and row count for one ticker's adj_close.

    Reads only — no write, no delete, no update.
    """
    sql = text(
        "SELECT MIN(obs_date), MAX(obs_date), COUNT(*) "
        "FROM raw_series "
        "WHERE source_id = :sid AND series_id = :series "
        "AND pull_status = 'SUCCESS'"
    )
    with engine.connect() as conn:
        row = conn.execute(
            sql, {"sid": source_id, "series": f"YF:{ticker}:adj_close"}
        ).one()
    first, last, count = row
    return (str(first) if first else None, str(last) if last else None, int(count))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--tickers-file",
        default="data/vs1_v3_tiingo_backfill_tickers.json",
        help="JSON list of tickers to backfill (default: the 224 VS1 v3 issuers)",
    )
    parser.add_argument(
        "--start-date",
        default="1990-01-01",
        help="Earliest date to request from Tiingo (Tiingo clamps to actual listing date)",
    )
    parser.add_argument(
        "--report-out",
        required=True,
        help="Path to write the JSON per-ticker report",
    )
    args = parser.parse_args()

    tickers = load_tickers(args.tickers_file)
    engine = get_engine()
    puller = TiingoPuller(engine)  # raises ValueError if TIINGO_API_KEY unset

    results = puller.pull_all(ticker_list=tickers, start_date=args.start_date)

    per_ticker = []
    no_data: list[str] = []
    for res in results:
        ticker = res["ticker"]
        first, last, total_rows = obs_date_summary(engine, puller.source_id, ticker)
        entry = {
            "ticker": ticker,
            "status": res["status"],
            "rows_inserted_this_run": res["rows_inserted"],
            "rows_total_adj_close": total_rows,
            "first_date": first,
            "last_date": last,
            "errors": res["errors"],
        }
        per_ticker.append(entry)
        if total_rows == 0:
            no_data.append(ticker)

    report = {
        "generated": str(datetime.now(tz=timezone.utc).date()),
        "start_date": args.start_date,
        "tickers_requested": len(tickers),
        "tickers_with_no_tiingo_data": no_data,
        "results": per_ticker,
    }
    with open(args.report_out, "w") as f:
        json.dump(report, f, indent=2)

    got_data = len(tickers) - len(no_data)
    print(
        f"Done. {got_data}/{len(tickers)} tickers now have Tiingo rows; "
        f"{len(no_data)} have none. Report: {args.report_out}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
