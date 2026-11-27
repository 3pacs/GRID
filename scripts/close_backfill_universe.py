#!/usr/bin/env python3
"""Fill missing ticker_metrics_daily closes from GRID's own stored prices.

The replacement for the two Twelve Data backfills (owner, 2026-11-23: "drop
the twelve stuff, get it from our other sources instead"). **Wired for the
universe cohort only**: ``scripts/td_backfill_universe.py``, the entry point of
``grid-td-universe-backfill.service``, delegates here (``universe`` mode).
``scripts/td_backfill_gem_tickers.py`` keeps running Twelve Data until the gem
cohort's coverage is good enough (the gem tickers that are not on the Tiingo
list have no stored close to fill from). ``DRY_RUN=1`` reads and reports and
writes nothing; read its receipt line to measure coverage. Joining the Tiingo
list is not coverage.

Cohorts:

* ``universe``: every ticker in the latest ``data_freshness_audit`` that is
  either graded DEAD, STALE_30+ or STALE_7_30 (window 365 / 180 / 60 days) or
  **behind**: its latest stored close is older than the most recent US
  session whose Tiingo EOD bar should exist (``expected_latest_session``),
  whatever the audit grades it (window 30 days). The behind cohort is what
  keeps the universe current now that Twelve Data no longer does: selecting
  only the already-stale left every ticker up to 7 days behind (2026-11-27:
  135 tickers whose last close was Friday 11-20 had Tiingo closes through
  11-25 and were skipped until the audit graded them STALE_7_30).
* ``gem``: every gem-alert ticker with no close in the last 7 days. Window 60
  days.

Which closes count. Only a **completed** close of the ticker's own
instrument, as ``store.observations.read_completed_closes`` defines it with
the US-listed boundary of ``evaluation.prices``:

* the label is a NYSE session and the row was pulled at or after 17:00 New
  York on that day, so a same-day provisional price never qualifies;
* accepted rows only, from ``yfinance`` or ``TIINGO``; a TIINGO row after the
  ticker's last session (another instrument's price) is dropped;
* a payload that marks the row not policy-eligible, or on another price
  basis, is dropped;
* when the stored vintages of one day disagree by more than the reader's
  tolerance (two sources, or a restated split basis) the day is
  **ambiguous** and withheld, not resolved by recency.

A ticker that is not a US-listed symbol (crypto pairs, foreign suffixes) is
unsupported and withheld whole.

What is written. Only a missing close: a new ``(ticker, obs_date)`` row, or a
row whose ``close_price`` is NULL and that ``sec_xbrl_shares`` does not own
(``shares_outstanding`` NULL). An existing close is never replaced, whoever
wrote it; correcting one needs its own reviewed change.

Basis guard. Closes already stored for the ticker are compared with the
eligible stored closes on the same days. When any pair differs by more than
1% and more than 0.01 (a split-adjusted close next to a raw one, for example),
the ticker is **withheld whole** and listed under ``basis_conflict`` in the
receipt, so a series never gets two price bases side by side. Two-decimal
rounding of a sub-dollar price is under that bar and does not count. A ticker
that has stored closes but no day in common with the eligible ones cannot be
compared: it is still filled and is listed under ``basis_unverified`` (unknown,
not verified same-basis).

Environment: ``DRY_RUN=1`` reads and reports but writes nothing (this is the
coverage measurement); ``ONLY_BUCKET=DEAD`` (or ``BEHIND`` for the behind
cohort alone) and ``LIMIT=20`` narrow the universe run.

Exit code: 1 when a ticker's write failed and no ticker was filled; otherwise
0, including a run where nothing had an eligible close.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from evaluation.prices import (  # noqa: E402
    _CRYPTO_TICKER_PATTERN,
    _US_TICKER_PATTERN,
    COMPLETED_CLOSE_SOURCES,
    us_listed_close_boundary,
)
from store.observations import read_completed_closes  # noqa: E402

SOURCE_LABEL = "stored_close_backfill"

STALE_BUCKETS: tuple[str, ...] = ("DEAD", "STALE_30+", "STALE_7_30")
BEHIND = "BEHIND"        # not an audit bucket: a ticker whose latest stored close trails the expected session
WINDOW_BY_BUCKET: dict[str, int] = {"DEAD": 365, "STALE_30+": 180, "STALE_7_30": 60, BEHIND: 30}
GEM_FRESH_DAYS = 7
GEM_WINDOW_DAYS = 60

# The universe is the latest audit's tickers. A ticker is work when it is graded stale, or when its latest
# stored close (read live, not from the audit's own snapshot) is older than ``:expected``; a NULL ``:expected``
# (a single stale bucket was asked for) or a ticker with no stored close selects nothing extra.
_UNIVERSE_WORK_SQL = text(
    "SELECT a.ticker, CASE WHEN a.bucket = ANY(:stale) THEN a.bucket ELSE 'BEHIND' END AS cohort "
    "FROM data_freshness_audit a "
    "LEFT JOIN LATERAL (SELECT MAX(m.obs_date) AS last_close FROM ticker_metrics_daily m "
    "                   WHERE m.ticker = a.ticker AND m.close_price IS NOT NULL) l ON TRUE "
    "WHERE a.audited_at = (SELECT MAX(audited_at) FROM data_freshness_audit) "
    "AND (a.bucket = ANY(:stale) OR l.last_close < :expected) "
    "ORDER BY CASE WHEN a.bucket = ANY(:stale) THEN "
    "  CASE a.bucket WHEN 'DEAD' THEN 0 WHEN 'STALE_30+' THEN 1 ELSE 2 END ELSE 3 END, a.ticker"
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
_EXISTING_CLOSES_SQL = text(
    "SELECT obs_date, close_price FROM ticker_metrics_daily "
    "WHERE ticker = :ticker AND obs_date >= :start AND close_price IS NOT NULL"
)
BASIS_REL_TOLERANCE = 0.01
BASIS_ABS_TOLERANCE = 0.01
# Fill only: a new row, or a row with no close that sec_xbrl_shares does not
# own. An existing close is never replaced.
_FILL_SQL = text(
    "INSERT INTO ticker_metrics_daily (ticker, obs_date, close_price, source, as_of) "
    "VALUES (:ticker, :obs_date, :close, :source, now()) "
    "ON CONFLICT (ticker, obs_date) DO UPDATE "
    "SET close_price = EXCLUDED.close_price, source = EXCLUDED.source, as_of = now() "
    "WHERE ticker_metrics_daily.close_price IS NULL "
    "AND ticker_metrics_daily.shares_outstanding IS NULL"
)


def is_supported_instrument(ticker: str) -> bool:
    """A US-listed symbol the completed-close boundary applies to."""
    return bool(_US_TICKER_PATTERN.match(ticker)) and not _CRYPTO_TICKER_PATTERN.match(ticker)


def eligible_closes(
    conn: Any, ticker: str, start: date, end: date, evaluated_at: datetime,
) -> tuple[list[tuple[date, float, str]], int]:
    """``([(obs_date, close, sources), ...], ambiguous_days)`` for ``ticker``.

    Completed closes only (see the module docstring); ambiguous days are
    counted and withheld. Raises ``ValueError`` for an unsupported instrument.
    """
    if not is_supported_instrument(ticker):
        raise ValueError(f"not a US-listed ticker: {ticker!r}")
    closes = read_completed_closes(
        conn, f"YF:{ticker}:close", start=start, end=end,
        boundary=us_listed_close_boundary, source=COMPLETED_CLOSE_SOURCES,
        as_of_ts=evaluated_at,
    )
    rows = [(d, float(c.latest.value), str(c.source or "unknown"))
            for d, c in sorted(closes.items()) if not c.ambiguous]
    return rows, sum(1 for c in closes.values() if c.ambiguous)


def basis_conflicts(existing: list[tuple[date, Any]], closes: list[tuple[date, float, str]]) -> list[date]:
    """Days where a stored close and the eligible stored close disagree beyond
    rounding: more than ``BASIS_REL_TOLERANCE`` and more than ``BASIS_ABS_TOLERANCE``."""
    ours = {d: c for d, c, _s in closes}
    out = []
    for d, stored in existing:
        mine = ours.get(d)
        if mine is None or stored is None or float(stored) == 0:
            continue
        diff = abs(float(stored) - mine)
        if diff > BASIS_ABS_TOLERANCE and diff / abs(float(stored)) > BASIS_REL_TOLERANCE:
            out.append(d)
    return out


def basis_overlap(existing: list[tuple[date, Any]], closes: list[tuple[date, float, str]]) -> int:
    """Days on which a stored close and an eligible stored close can be compared."""
    ours = {d for d, _c, _s in closes}
    return sum(1 for d, stored in existing if d in ours and stored is not None and float(stored) != 0)


def _universe_work(conn: Any, only_bucket: str | None, limit: int, evaluated_at: datetime) -> list[tuple[str, int]]:
    """``[(ticker, window_days), ...]``: the stale buckets first, then the behind cohort."""
    if only_bucket == BEHIND:
        stale: list[str] = []
    elif only_bucket:
        stale = [only_bucket]
    else:
        stale = list(STALE_BUCKETS)
    expected: date | None = None
    if only_bucket is None or only_bucket == BEHIND:
        # Lazy: the session rule lives with the Tiingo price pull ("the most recent US session whose
        # EOD bar should already exist"), one definition for both.
        from ingestion.tiingo_pull import expected_latest_session

        expected = expected_latest_session(evaluated_at)
    rows = conn.execute(_UNIVERSE_WORK_SQL, {"stale": stale, "expected": expected}).fetchall()
    work = [(str(t), WINDOW_BY_BUCKET.get(str(b), 60)) for t, b in rows]
    return work[:limit] if limit else work


def _gem_work(conn: Any, today: date) -> list[tuple[str, int]]:
    rows = conn.execute(_GEM_WORK_SQL, {"fresh_since": today - timedelta(days=GEM_FRESH_DAYS)}).fetchall()
    return [(str(r[0]), GEM_WINDOW_DAYS) for r in rows]


def run(engine: Any, mode: str, *, evaluated_at: datetime | None = None, dry_run: bool = False,
        only_bucket: str | None = None, limit: int = 0) -> dict[str, Any]:
    """Fill closes for ``mode`` ("universe" or "gem"); returns the run's receipt."""
    if mode not in ("universe", "gem"):
        raise ValueError(f"unknown mode {mode!r}")
    evaluated_at = evaluated_at or datetime.now(timezone.utc)
    if evaluated_at.tzinfo is None:
        raise ValueError("evaluated_at must be timezone-aware")
    today = evaluated_at.date()
    with engine.connect() as conn:
        work = (_universe_work(conn, only_bucket, limit, evaluated_at) if mode == "universe"
                else _gem_work(conn, today))
    print(f"close backfill ({mode}): {len(work)} tickers (dry_run={dry_run})")

    receipt: dict[str, Any] = {
        "mode": mode, "dry_run": dry_run, "evaluated_at": evaluated_at.isoformat(),
        "tickers": len(work),
        "behind_cohort": sum(1 for _t, d in work if mode == "universe" and d == WINDOW_BY_BUCKET[BEHIND]),
        "eligible_tickers": 0, "eligible_closes": 0,
        "written": 0, "left_as_is": 0, "ambiguous_days_withheld": 0,
        "no_completed_close": [], "unsupported": [], "basis_conflict": [], "basis_unverified": [], "failed": [], "by_source": {},
    }
    for i, (ticker, days) in enumerate(work, 1):
        start = today - timedelta(days=days)
        if not is_supported_instrument(ticker):
            receipt["unsupported"].append(ticker)
            print(f"[{i}/{len(work)}] {ticker}: withheld, not a US-listed symbol")
            continue
        try:
            with engine.connect() as conn:
                closes, ambiguous = eligible_closes(conn, ticker, start, today, evaluated_at)
            receipt["ambiguous_days_withheld"] += ambiguous
            if not closes:
                receipt["no_completed_close"].append(ticker)
                print(f"[{i}/{len(work)}] {ticker}: skipped, no completed close since {start}"
                      + (f" ({ambiguous} ambiguous days withheld)" if ambiguous else ""))
                continue
            with engine.connect() as conn:
                existing = [(r[0], r[1]) for r in conn.execute(
                    _EXISTING_CLOSES_SQL, {"ticker": ticker, "start": start}).fetchall()]
            clashes = basis_conflicts(existing, closes)
            if clashes:
                receipt["basis_conflict"].append(ticker)
                print(f"[{i}/{len(work)}] {ticker}: withheld, {len(clashes)} stored closes on another price basis "
                      f"({clashes[0]}..{clashes[-1]})")
                continue
            if existing and basis_overlap(existing, closes) == 0:
                # Stored closes exist but none shares a day with an eligible
                # one: the basis could not be compared. Filled, and named.
                receipt["basis_unverified"].append(ticker)
            written = 0
            if not dry_run:
                with engine.begin() as conn:
                    for obs_date, close, sources in closes:
                        result = conn.execute(_FILL_SQL, {
                            "ticker": ticker, "obs_date": obs_date, "close": close,
                            "source": f"{SOURCE_LABEL}:{sources}",
                        })
                        written += 1 if (result.rowcount or 0) > 0 else 0
        except Exception as exc:  # noqa: BLE001 -- one ticker must not stop the rest
            receipt["failed"].append(ticker)
            print(f"[{i}/{len(work)}] {ticker}: FAILED ({type(exc).__name__}: {exc})", file=sys.stderr)
            continue
        receipt["eligible_tickers"] += 1
        receipt["eligible_closes"] += len(closes)
        receipt["written"] += written
        receipt["left_as_is"] += 0 if dry_run else len(closes) - written
        for sources in {s for _d, _c, s in closes}:
            receipt["by_source"][sources] = receipt["by_source"].get(sources, 0) + 1
        verb = "eligible" if dry_run else "filled"
        print(f"[{i}/{len(work)}] {ticker}: {len(closes)} completed closes "
              f"({closes[0][0]}..{closes[-1][0]}), {verb} {len(closes) if dry_run else written}"
              + (f", {ambiguous} ambiguous days withheld" if ambiguous else ""))

    print(f"\nDONE eligible_tickers={receipt['eligible_tickers']}/{len(work)} behind_cohort={receipt['behind_cohort']} "
          f"eligible_closes={receipt['eligible_closes']} written={receipt['written']} "
          f"left_as_is={receipt['left_as_is']} ambiguous_days_withheld={receipt['ambiguous_days_withheld']} "
          f"no_completed_close={len(receipt['no_completed_close'])} unsupported={len(receipt['unsupported'])} "
          f"basis_conflict={len(receipt['basis_conflict'])} basis_unverified={len(receipt['basis_unverified'])} "
          f"failed={len(receipt['failed'])}")
    print("RECEIPT " + json.dumps(receipt, sort_keys=True))
    return receipt


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    mode = args[0] if args else "universe"
    if mode not in ("universe", "gem"):
        print("usage: close_backfill_universe.py [universe|gem]", file=sys.stderr)
        return 2
    from db import get_engine

    receipt = run(
        get_engine(), mode,
        dry_run=bool(os.environ.get("DRY_RUN")),
        only_bucket=os.environ.get("ONLY_BUCKET") or None,
        limit=int(os.environ.get("LIMIT", "0") or 0),
    )
    if receipt["failed"] and receipt["eligible_tickers"] == 0:
        print(f"total failure: 0/{len(receipt['failed'])} attempted tickers were filled", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
