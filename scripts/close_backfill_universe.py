#!/usr/bin/env python3
"""Fill ticker_metrics_daily closes from GRID's own stored prices.

Replaces the two Twelve Data backfills (owner decision, 2026-11-23: "drop the
twelve stuff, get it from our other sources instead"):

* ``universe``: every ticker the daily freshness audit grades DEAD, STALE_30+
  or STALE_7_30 (``data_freshness_audit``, built by
  ``scripts/freshness_audit_universe.sql``). Window 365 / 180 / 60 days.
* ``gem``: every gem-alert ticker with no close in the last 7 days. Window 60
  days.

For each ticker the closes come from ``raw_series`` (``YF:{TICKER}:close``),
the same rows ``ingestion/altdata/sec_xbrl_shares.py`` prices market cap
with, read through ``store.observations.read_window`` (accepted rows only,
one per date, newest vintage). TIINGO and yfinance both write that series;
one source is used per ticker: the one whose stored closes reach the latest
date, TIINGO on a tie. No external request is made and no API key is read.

A ticker with no stored close in its window is named and skipped. That is a
coverage gap upstream (the routine Tiingo list takes the freshness universe
from ``TiingoPuller.freshness_universe_tickers``), not a failure of this job.

Source-precedence policy (task #171, unchanged): this writer is subordinate
to ``sec_xbrl_shares``. It writes only ``close_price``, ``source`` and
``as_of``, and never touches a row whose ``shares_outstanding`` is set.

Environment: ``DRY_RUN=1`` reads and reports but writes nothing;
``ONLY_BUCKET=DEAD`` and ``LIMIT=20`` narrow the universe run.

Exit code: 1 when a ticker's write failed and no ticker was filled; otherwise
0, including a run where nothing had a stored close.
"""
from __future__ import annotations

import math
import os
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from store.observations import read_window  # noqa: E402

# Order of preference when two sources reach the same latest date.
PRICE_SOURCES: tuple[str, ...] = ("TIINGO", "yfinance")
SOURCE_LABEL = "stored_close_backfill"

STALE_BUCKETS: tuple[str, ...] = ("DEAD", "STALE_30+", "STALE_7_30")
WINDOW_BY_BUCKET: dict[str, int] = {"DEAD": 365, "STALE_30+": 180, "STALE_7_30": 60}
GEM_FRESH_DAYS = 7
GEM_WINDOW_DAYS = 60

_UNIVERSE_WORK_SQL = text(
    "SELECT ticker, bucket FROM data_freshness_audit "
    "WHERE bucket = ANY(:buckets) "
    "AND audited_at = (SELECT MAX(audited_at) FROM data_freshness_audit) "
    "ORDER BY CASE bucket WHEN 'DEAD' THEN 0 WHEN 'STALE_30+' THEN 1 ELSE 2 END, ticker"
)
_GEM_WORK_SQL = text(
    "SELECT g.t FROM ("
    " SELECT backtest_ticker AS t FROM gem_alerts WHERE backtest_ticker IS NOT NULL"
    " UNION"
    " SELECT subject_id AS t FROM gem_alerts WHERE subject_kind = 'ticker' AND subject_id IS NOT NULL"
    ") g "
    "WHERE g.t NOT IN ('MACRO', 'NONE', '') AND g.t !~ '[[:space:]]' "
    "AND NOT EXISTS ("
    " SELECT 1 FROM ticker_metrics_daily m"
    " WHERE m.ticker = g.t AND m.close_price IS NOT NULL AND m.obs_date >= :fresh_since"
    ") ORDER BY 1"
)
# Never a row sec_xbrl_shares owns (shares_outstanding set), and no rewrite of
# a close that is already the stored one.
_UPSERT_SQL = text(
    "INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of) "
    "VALUES (:ticker, :obs_date, :close, :source, now()) "
    "ON CONFLICT (ticker, obs_date) DO UPDATE "
    "SET close_price = EXCLUDED.close_price, source = EXCLUDED.source, as_of = now() "
    "WHERE ticker_metrics_daily.shares_outstanding IS NULL "
    "AND ticker_metrics_daily.close_price IS DISTINCT FROM EXCLUDED.close_price"
)


def stored_closes(conn: Any, ticker: str, start: date, end: date) -> tuple[str | None, list[tuple[date, float]]]:
    """``(source, [(obs_date, close), ...])`` from one stored price source.

    The source whose closes reach the latest date wins; ``PRICE_SOURCES``
    order breaks a tie. ``(None, [])`` when neither holds a positive, finite
    close in ``[start, end]``.
    """
    best: tuple[str | None, list[tuple[date, float]]] = (None, [])
    for source in PRICE_SOURCES:
        rows = [
            (o.obs_date, float(o.value))
            for o in read_window(conn, f"YF:{ticker}:close", source=source, start=start, as_of=end)
            if o.value is not None and math.isfinite(float(o.value)) and float(o.value) > 0
        ]
        if rows and (not best[1] or rows[-1][0] > best[1][-1][0]):
            best = (source, rows)
    return best


def _universe_work(conn: Any, only_bucket: str | None, limit: int) -> list[tuple[str, int]]:
    buckets = [only_bucket] if only_bucket else list(STALE_BUCKETS)
    rows = conn.execute(_UNIVERSE_WORK_SQL, {"buckets": buckets}).fetchall()
    work = [(str(t), WINDOW_BY_BUCKET.get(str(b), 60)) for t, b in rows]
    return work[:limit] if limit else work


def _gem_work(conn: Any, today: date) -> list[tuple[str, int]]:
    rows = conn.execute(_GEM_WORK_SQL, {"fresh_since": today - timedelta(days=GEM_FRESH_DAYS)}).fetchall()
    return [(str(r[0]), GEM_WINDOW_DAYS) for r in rows]


def run(engine: Any, mode: str, *, today: date | None = None, dry_run: bool = False,
        only_bucket: str | None = None, limit: int = 0) -> dict[str, Any]:
    """Fill closes for ``mode`` ("universe" or "gem"); returns the run's counts."""
    if mode not in ("universe", "gem"):
        raise ValueError(f"unknown mode {mode!r}")
    today = today or date.today()
    with engine.connect() as conn:
        work = _universe_work(conn, only_bucket, limit) if mode == "universe" else _gem_work(conn, today)
    print(f"close backfill ({mode}): {len(work)} tickers (dry_run={dry_run})")

    summary: dict[str, Any] = {
        "mode": mode, "tickers": len(work), "filled": 0, "written": 0, "left_as_is": 0,
        "no_stored_close": [], "failed": [], "by_source": {},
    }
    for i, (ticker, days) in enumerate(work, 1):
        start = today - timedelta(days=days)
        try:
            with engine.connect() as conn:
                source, closes = stored_closes(conn, ticker, start, today)
            if not closes:
                summary["no_stored_close"].append(ticker)
                print(f"[{i}/{len(work)}] {ticker}: skipped, no stored close since {start} "
                      f"({', '.join(PRICE_SOURCES)})")
                continue
            written = 0
            if not dry_run:
                with engine.begin() as conn:
                    for obs_date, close in closes:
                        result = conn.execute(_UPSERT_SQL, {
                            "ticker": ticker, "obs_date": obs_date, "close": close,
                            "source": f"{SOURCE_LABEL}:{source}",
                        })
                        written += 1 if (result.rowcount or 0) > 0 else 0
        except Exception as exc:  # noqa: BLE001 -- one ticker must not stop the rest
            summary["failed"].append(ticker)
            print(f"[{i}/{len(work)}] {ticker}: FAILED ({type(exc).__name__}: {exc})", file=sys.stderr)
            continue
        summary["filled"] += 1
        summary["written"] += written
        summary["left_as_is"] += len(closes) - written
        summary["by_source"][source] = summary["by_source"].get(source, 0) + 1
        verb = "would write up to" if dry_run else "wrote"
        count = len(closes) if dry_run else written
        print(f"[{i}/{len(work)}] {ticker}: {len(closes)} stored closes from {source} "
              f"({closes[0][0]}..{closes[-1][0]}), {verb} {count}")

    print(f"\nDONE filled={summary['filled']}/{len(work)} written={summary['written']} "
          f"left_as_is={summary['left_as_is']} no_stored_close={len(summary['no_stored_close'])} "
          f"failed={len(summary['failed'])} by_source={summary['by_source']}")
    if summary["no_stored_close"]:
        print(f"no stored close: {summary['no_stored_close']}")
    if summary["failed"]:
        print(f"failed tickers: {summary['failed']}", file=sys.stderr)
    return summary


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    mode = args[0] if args else "universe"
    if mode not in ("universe", "gem"):
        print("usage: close_backfill_universe.py [universe|gem]", file=sys.stderr)
        return 2
    from db import get_engine

    summary = run(
        get_engine(), mode,
        dry_run=bool(os.environ.get("DRY_RUN")),
        only_bucket=os.environ.get("ONLY_BUCKET") or None,
        limit=int(os.environ.get("LIMIT", "0") or 0),
    )
    if summary["failed"] and summary["filled"] == 0:
        print(f"total failure: 0/{len(summary['failed'])} attempted tickers were filled", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
