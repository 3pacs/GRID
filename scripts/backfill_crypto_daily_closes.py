#!/usr/bin/env python3
"""Backfill gap-free crypto UTC daily closes from Binance (dry run by default).

Crypto trades 24/7, but ``btc_full``/``eth_full``/``sol_full`` had closes on
only 139/139/74 days since 2026-03-01, which left 28,571 legacy crypto-v1
predictions unscoreable (#973). This fills the missing days through the
normal ingestion path:

* Source and convention: Binance spot ``{COIN}USDT`` 1d klines with
  ``timeZone=0`` under the ``binance_utc_close_v1`` contract
  (``binance_close_contract.py``): ``obs_date`` is the UTC day the bar
  covers, the bar is available only at ``obs_date + 1 00:00 UTC``.
* Writes go to ``raw_series`` as ``binance.{SYMBOL}.{field}`` (the series the
  daily puller already writes; ``normalization/entity_map.py`` maps the close
  to ``*_full``). Rows are INSERT only, ``pull_timestamp`` = capture time, so
  the resolved ``release_date`` is the backfill date (never back-dated).
  The next Hermes resolve cycle picks them up by ``pull_timestamp``.
* Idempotent: a day that already holds a completed-contract row is skipped,
  so a re-run inserts nothing.
* Dry run (default): every transaction is READ ONLY (``postgresql_readonly``)
  and nothing is written. It prints, per symbol, the raw and feature-level
  gap counts before and the projected count after.

Usage::

    python scripts/backfill_crypto_daily_closes.py                  # dry run
    python scripts/backfill_crypto_daily_closes.py --apply          # write
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from datetime import date, datetime, timedelta, timezone
from typing import Any

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from binance_close_contract import last_completed_obs_date

DEFAULT_START = date(2026, 3, 1)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    from ingestion.altdata.binance_puller import _SYMBOLS, BACKFILL_DEFAULT_FIELDS

    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--apply", action="store_true",
                        help="append missing bars (default: read-only dry run)")
    parser.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START,
                        help=f"first UTC obs_date (default {DEFAULT_START.isoformat()})")
    parser.add_argument("--end", type=date.fromisoformat, default=None,
                        help="last UTC obs_date (default: last completed UTC day)")
    parser.add_argument("--symbols", default=",".join(_SYMBOLS),
                        help="comma-separated Binance symbols (default: the puller's list)")
    parser.add_argument("--fields", default=",".join(BACKFILL_DEFAULT_FIELDS),
                        help="comma-separated kline fields; must include close")
    args = parser.parse_args(argv)
    args.symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    args.fields = tuple(f.strip().lower() for f in args.fields.split(",") if f.strip())
    if not args.symbols:
        parser.error("--symbols is empty")
    if "close" not in args.fields:
        parser.error("--fields must include close")
    if args.end is not None and args.end < args.start:
        parser.error("--end is before --start")
    return args


def read_only_engine(engine: Engine) -> Engine:
    """The same engine and pool, but every transaction is READ ONLY (psycopg2)."""
    return engine.execution_options(postgresql_readonly=True)


def feature_for_symbol(symbol: str) -> str | None:
    """The canonical feature the symbol's close resolves into, if mapped."""
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

    series_id = f"binance.{symbol}.close"
    return SEED_MAPPINGS.get(series_id) or NEW_MAPPINGS_V2.get(series_id)


def resolved_days(engine: Engine, feature: str, start: date, end: date) -> set[date]:
    """UTC days on which ``feature`` has any resolved value in ``[start, end]``."""
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT DISTINCT rs.obs_date
            FROM resolved_series rs
            JOIN feature_registry fr ON fr.id = rs.feature_id
            WHERE fr.name = :feature
              AND rs.obs_date BETWEEN :start AND :end
        """), {"feature": feature, "start": start, "end": end}).fetchall()
    return {row[0] for row in rows}


def feature_gap_report(
    engine: Engine, symbol: str, summary: dict[str, Any],
) -> dict[str, Any] | None:
    """Gap count of the mapped feature before, and projected after resolution."""
    feature = feature_for_symbol(symbol)
    if feature is None or summary["days_in_range"] == 0:
        return None
    start = date.fromisoformat(summary["start"])
    end = date.fromisoformat(summary["end"])
    days = {start + timedelta(days=i) for i in range((end - start).days + 1)}
    have = resolved_days(engine, feature, start, end)
    unfillable = {date.fromisoformat(d) for d in summary["unfillable_days"]}
    stored = {date.fromisoformat(d) for d in summary["stored_close_days"]}
    missing = days - have
    # A day whose completed raw close is already stored is not re-written by
    # the backfill; it resolves only if a resolver run still covers its pull
    # time. Count it as a remaining gap (conservative) and list it.
    raw_present_unresolved = missing & stored
    after = (missing & unfillable) | raw_present_unresolved
    return {
        "feature": feature,
        "days_in_range": len(days),
        "days_with_close_before": len(days & have),
        "gap_days_before": len(missing),
        "gap_days_after_projected": len(after),
        "gap_dates_after_projected": sorted(d.isoformat() for d in after),
        "raw_present_unresolved_dates": sorted(d.isoformat() for d in raw_present_unresolved),
    }


def main(
    argv: list[str] | None = None, *, engine: Engine | None = None,
    puller_factory: Callable[[Engine], Any] | None = None,
    now: datetime | None = None,
) -> int:
    args = parse_args(argv)
    if engine is None:
        from db import get_engine

        engine = get_engine()
    if puller_factory is None:
        from ingestion.altdata.binance_puller import BinancePuller

        puller_factory = BinancePuller
    now = now or datetime.now(timezone.utc)
    end = args.end or last_completed_obs_date(now)
    work_engine = engine if args.apply else read_only_engine(engine)
    log.info("crypto daily close backfill: mode={m} {s}..{e} symbols={y}",
             m="APPLY" if args.apply else "dry run (read-only)",
             s=args.start, e=end, y=args.symbols)
    puller = puller_factory(work_engine)
    report: dict[str, Any] = {
        "mode": "apply" if args.apply else "dry_run",
        "contract": "binance_utc_close_v1",
        "start": args.start.isoformat(), "end": end.isoformat(),
        "symbols": {},
        "totals": {"rows_to_insert": 0, "rows_inserted": 0,
                   "close_days_to_fill": 0, "missing_close_days_after": 0},
    }
    for symbol in args.symbols:
        summary = puller.backfill_klines(
            symbol, args.start, end, apply=args.apply, now=now, fields=args.fields,
        )
        summary["feature_gaps"] = feature_gap_report(work_engine, symbol, summary)
        report["symbols"][symbol] = summary
        for key in report["totals"]:
            report["totals"][key] += summary[key]
        log.info(
            "{s}: missing_before={b} fill={f} rows_to_insert={r} inserted={i} "
            "missing_after={a}",
            s=symbol, b=summary["missing_close_days_before"],
            f=summary["close_days_to_fill"], r=summary["rows_to_insert"],
            i=summary["rows_inserted"], a=summary["missing_close_days_after"],
        )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
