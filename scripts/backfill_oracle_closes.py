#!/usr/bin/env python3
"""Backfill raw closes for every ticker the oracle needs (S15, 2026-11-06).

Why
---
~45 biotech / small-cap tickers the oracle predicts on (ERAS, TGTX, GOSS,
MESO, ...) had no close after 2026-09-09. Their only routine close writer was
``small_cap_enrichment.ensure_prices``; its sub-$2B cap gate dropped them from
its universe after their first enrichment on 2026-09-10, and neither routine
price list (yfinance ``YF_TICKER_LIST``, Tiingo's ``default_tickers``) held
them. ``TiingoPuller.default_tickers`` now includes the oracle's close
universe, so collection continues from here; this script fills the gap.

What
----
For each ticker (default: every ticker of an oracle prediction expiring on or
after ``--start``, plus the oracle's close universe; equities only) whose
TIINGO ``YF:{ticker}:close`` series is missing any NYSE session in
``[--start, --end]``, it runs the existing ``TiingoPuller.pull_ticker`` over
that window. Tickers Tiingo does not serve fall back to the existing
``YFinancePuller.pull_ticker`` (``--apply`` only; the dry run lists them).

* Dry run by default: Tiingo is fetched, nothing is written (the DB session is
  ``default_transaction_read_only``) and the JSON report gives the rows that
  ``--apply`` would add.
* Idempotent: both pullers insert only where this source holds no SUCCESS row
  for (series_id, obs_date); a re-run adds nothing.
* PIT: rows are appended with the database's ``pull_timestamp`` = the moment
  of the backfill, never back-dated. A point-in-time read ``as_of`` an
  earlier instant does not see them. ``--end`` defaults to the latest session
  whose Tiingo EOD bar is published, so an unsettled bar is never stored.
* Raw closes are stored as Tiingo/yfinance report them (``close`` is
  unadjusted; ``adj_close`` is written alongside). Nothing here computes a
  return, so a split inside the window is not interpreted (the split-aware
  read is W2-10's helper in store/observations.py).

Run on grid-svr with the service environment (TIINGO_API_KEY, DB):

    python scripts/backfill_oracle_closes.py                  # dry run
    python scripts/backfill_oracle_closes.py --apply          # write
"""

from __future__ import annotations

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from config import settings
from ingestion.market_calendar import is_market_open

DEFAULT_START = date(2026, 9, 10)

_ORACLE_EXPIRY_TICKERS_SQL = """
    SELECT DISTINCT ticker FROM oracle_predictions
    WHERE expiry >= :start
"""

_TIINGO_CLOSE_DATES_SQL = """
    SELECT obs_date FROM raw_series
    WHERE series_id = :sid AND source_id = :src
      AND pull_status = 'SUCCESS'
      AND obs_date BETWEEN :start AND :end
"""


def sessions_between(start: date, end: date) -> list[date]:
    """NYSE sessions in ``[start, end]``."""
    out: list[date] = []
    d = start
    while d <= end:
        if is_market_open(d):
            out.append(d)
        d += timedelta(days=1)
    return out


def _as_date(v: Any) -> date:
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    return date.fromisoformat(str(v)[:10])


def candidate_tickers(engine: Engine, start: date) -> list[str]:
    """Oracle tickers with an expiry on/after ``start`` ∪ the close universe."""
    from ingestion.tiingo_pull import TiingoPuller, is_tiingo_equity

    with engine.connect() as conn:
        expiring = {
            str(r[0]).strip().upper()
            for r in conn.execute(text(_ORACLE_EXPIRY_TICKERS_SQL), {"start": start}).fetchall()
            if r[0]
        }
    universe = set(TiingoPuller.oracle_tickers(engine))
    return sorted(universe | {t for t in expiring if is_tiingo_equity(t)})


def missing_sessions(engine: Engine, source_id: int, ticker: str, sessions: list[date]) -> list[date]:
    """Sessions with no TIINGO SUCCESS close for ``ticker``. Read only."""
    if not sessions:
        return []
    with engine.connect() as conn:
        held = {
            _as_date(r[0]) for r in conn.execute(text(_TIINGO_CLOSE_DATES_SQL), {
                "sid": f"YF:{ticker}:close", "src": source_id,
                "start": sessions[0], "end": sessions[-1],
            }).fetchall()
        }
    return [d for d in sessions if d not in held]


def run(
    engine: Engine,
    *,
    start: date,
    end: date,
    apply: bool,
    tickers: list[str] | None = None,
    tiingo: Any = None,
    yfinance: Any = None,
    yfinance_fallback: bool = True,
) -> dict[str, Any]:
    """Backfill (or, dry run, project) closes. Returns the JSON report."""
    from ingestion.tiingo_pull import DEFAULT_MIN_INTERVAL_S, TiingoPuller, _RateLimiter

    if tiingo is None:
        tiingo = TiingoPuller(engine)  # raises ValueError without TIINGO_API_KEY
    tickers = sorted({t.strip().upper() for t in tickers if t.strip()}) if tickers else candidate_tickers(engine, start)
    sessions = sessions_between(start, end)
    limiter = _RateLimiter(DEFAULT_MIN_INTERVAL_S)

    report: dict[str, Any] = {
        "mode": "apply" if apply else "dry_run",
        "start": start.isoformat(), "end": end.isoformat(),
        "sessions": len(sessions), "tickers": len(tickers),
        "current": 0, "fetched": 0,
        "missing_close_sessions": 0,
        "tiingo_close_rows": 0, "tiingo_rows": 0,
        "tiingo_unserved": [], "yfinance_rows": 0, "failed": [],
        "per_ticker": {},
    }
    for ticker in tickers:
        gap = missing_sessions(engine, tiingo.source_id, ticker, sessions)
        if not gap:
            report["current"] += 1
            continue
        report["fetched"] += 1
        report["missing_close_sessions"] += len(gap)
        fetch_start = gap[0]
        res = tiingo.pull_ticker(ticker, start_date=fetch_start, end_date=end,
                                 limiter=limiter, dry_run=not apply)
        entry: dict[str, Any] = {"missing": len(gap), "first_missing": gap[0].isoformat(),
                                 "tiingo_status": res.get("status")}
        if res.get("status") == "SUCCESS":
            if apply:
                entry["tiingo_rows"] = int(res.get("rows_inserted") or 0)
            else:
                closes = {_as_date(d) for d, _v in res.get("close_obs") or []}
                entry["tiingo_close_rows"] = len(closes & set(gap))
                entry["tiingo_rows"] = int(res.get("rows_would_insert") or 0)
                report["tiingo_close_rows"] += entry["tiingo_close_rows"]
            report["tiingo_rows"] += entry["tiingo_rows"]
        elif res.get("status") == "PARTIAL":
            # 404 or an empty window: Tiingo does not serve this ticker.
            report["tiingo_unserved"].append(ticker)
            if apply and yfinance_fallback:
                try:
                    if yfinance is None:
                        from ingestion.yfinance_pull import YFinancePuller
                        yfinance = YFinancePuller(engine)
                    yres = yfinance.pull_ticker(ticker, fetch_start, end + timedelta(days=1))
                    entry["yfinance_status"] = yres.get("status")
                    entry["yfinance_rows"] = int(yres.get("rows_inserted") or 0)
                    report["yfinance_rows"] += entry["yfinance_rows"]
                except Exception as exc:  # noqa: BLE001 -- report, keep going
                    log.warning("backfill {t}: yfinance fallback failed: {e}", t=ticker, e=str(exc))
                    entry["yfinance_status"] = "FAILED"
                    report["failed"].append(ticker)
        else:
            report["failed"].append(ticker)
            entry["errors"] = [str(e)[:200] for e in res.get("errors") or []]
        report["per_ticker"][ticker] = entry
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START,
                    help=f"First session to fill (default {DEFAULT_START}).")
    ap.add_argument("--end", type=date.fromisoformat, default=None,
                    help="Last session to fill (default: latest published Tiingo EOD session).")
    ap.add_argument("--tickers", default="",
                    help="Comma-separated tickers (default: oracle tickers, see module doc).")
    ap.add_argument("--apply", action="store_true", help="Write. Without it nothing is written.")
    ap.add_argument("--no-yfinance-fallback", action="store_true",
                    help="With --apply: do not fall back to yfinance for tickers Tiingo lacks.")
    args = ap.parse_args(argv)
    if args.end is not None and args.end < args.start:
        ap.error("--end is before --start")
    return args


def main(argv: list[str] | None = None) -> None:
    from ingestion.tiingo_pull import expected_latest_session

    args = _parse_args(argv)
    end = args.end or expected_latest_session(datetime.now(timezone.utc))
    connect_args: dict[str, Any] = {}
    if not args.apply and settings.DB_URL.startswith("postgresql"):
        # Dry run: the server refuses every write in this session.
        connect_args = {"options": "-c default_transaction_read_only=on -c statement_timeout=120000"}
    engine = create_engine(settings.DB_URL, connect_args=connect_args)
    tickers = [t for t in args.tickers.split(",") if t.strip()] or None
    report = run(engine, start=args.start, end=end, apply=args.apply, tickers=tickers,
                 yfinance_fallback=not args.no_yfinance_fallback)
    print(json.dumps(report, indent=2, default=str))


if __name__ == "__main__":
    main()
