"""
GRID earnings calendar data ingestion module.

Pulls upcoming and recent earnings data from Yahoo Finance using the
``yfinance`` library. For each watchlist ticker, fetches:
  - Earnings dates (upcoming and recent)
  - EPS estimates and actuals
  - Revenue estimates and actuals
  - Surprise % and beat/miss/inline classification

Series stored with pattern: EARNINGS:{ticker}:{field}
Fields: date, estimate, actual, surprise_pct, revenue_estimate, revenue_actual

Schedule: daily pull via hermes operator.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


# Surprise classification thresholds
_INLINE_THRESHOLD_PCT = 2.0  # +/- 2% is considered inline


def _classify_surprise(estimate: float | None, actual: float | None) -> str:
    """Classify an earnings result as beat/miss/inline."""
    if estimate is None or actual is None:
        return "pending"
    if estimate == 0:
        return "beat" if actual > 0 else "miss" if actual < 0 else "inline"
    surprise_pct = (actual - estimate) / abs(estimate) * 100
    if surprise_pct > _INLINE_THRESHOLD_PCT:
        return "beat"
    elif surprise_pct < -_INLINE_THRESHOLD_PCT:
        return "miss"
    return "inline"


def _safe_float(val) -> float | None:
    """Convert a value to float, returning None for NaN/None."""
    if val is None:
        return None
    try:
        import math
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None


class EarningsCalendarPuller(BasePuller):
    """Pulls earnings calendar data from Yahoo Finance into the DB.

    Stores earnings events in a dedicated table with estimates, actuals,
    and surprise classification.
    """

    SOURCE_NAME: str = "yfinance_earnings"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://finance.yahoo.com",
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": False,
        "revision_behavior": "FREQUENT",
        "trust_score": "MED",
        "priority_rank": 45,
    }

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._ensure_earnings_table()
        log.info("EarningsCalendarPuller initialised — source_id={sid}", sid=self.source_id)

    def _ensure_earnings_table(self) -> None:
        """Create the earnings_calendar table if it doesn't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS earnings_calendar (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT NOT NULL,
                    earnings_date DATE NOT NULL,
                    fiscal_quarter TEXT,
                    eps_estimate DOUBLE PRECISION,
                    eps_actual DOUBLE PRECISION,
                    eps_surprise_pct DOUBLE PRECISION,
                    revenue_estimate DOUBLE PRECISION,
                    revenue_actual DOUBLE PRECISION,
                    revenue_surprise_pct DOUBLE PRECISION,
                    classification TEXT DEFAULT 'pending',
                    reported BOOLEAN DEFAULT FALSE,
                    pull_timestamp TIMESTAMPTZ DEFAULT NOW(),
                    raw_payload JSONB,
                    UNIQUE (ticker, earnings_date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_earnings_ticker
                ON earnings_calendar (ticker, earnings_date DESC)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_earnings_date
                ON earnings_calendar (earnings_date)
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_earnings_upcoming
                ON earnings_calendar (earnings_date)
                WHERE reported = FALSE
            """))

    def _get_watchlist_tickers(self) -> list[str]:
        """Get tickers from the watchlist table."""
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT DISTINCT ticker FROM watchlist WHERE active = TRUE ORDER BY ticker"
                )).fetchall()
                tickers = [r[0] for r in rows]
                if tickers:
                    return tickers
        except Exception as e:
            log.debug("Watchlist query failed: {e}", e=str(e))

        # Fallback: tickers from options_daily_signals
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT ticker FROM options_daily_signals
                    WHERE signal_date >= CURRENT_DATE - 30
                    AND total_oi >= 1000
                    ORDER BY ticker
                """)).fetchall()
                return [r[0] for r in rows]
        except Exception:
            return []

    def pull_ticker_earnings(self, ticker: str) -> dict[str, Any]:
        """Pull earnings data for a single ticker.

        Fetches both .earnings_dates (for upcoming/recent dates with
        EPS estimates/actuals) and .calendar (for revenue estimates).

        Returns:
            dict with ticker, rows_inserted, rows_updated, status, errors.
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "rows_updated": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        try:
            stock = yf.Ticker(ticker)

            # ── Earnings dates (EPS estimates/actuals) ──
            earnings_dates = None
            try:
                earnings_dates = stock.earnings_dates
            except Exception as e:
                log.debug("No earnings_dates for {t}: {e}", t=ticker, e=str(e))

            if earnings_dates is not None and not earnings_dates.empty:
                with self.engine.begin() as conn:
                    for idx, row in earnings_dates.iterrows():
                        try:
                            # idx is the earnings date (Timestamp)
                            earn_date = idx.date() if hasattr(idx, "date") else idx

                            eps_est = _safe_float(row.get("EPS Estimate"))
                            eps_act = _safe_float(row.get("Reported EPS"))
                            surprise_pct_raw = _safe_float(row.get("Surprise(%)"))

                            # Compute surprise % ourselves if yfinance doesn't provide it
                            if surprise_pct_raw is None and eps_est is not None and eps_act is not None and eps_est != 0:
                                surprise_pct_raw = (eps_act - eps_est) / abs(eps_est) * 100

                            reported = eps_act is not None
                            classification = _classify_surprise(eps_est, eps_act)

                            conn.execute(text("""
                                INSERT INTO earnings_calendar
                                    (ticker, earnings_date, eps_estimate, eps_actual,
                                     eps_surprise_pct, classification, reported, raw_payload)
                                VALUES
                                    (:ticker, :edate, :eps_est, :eps_act,
                                     :surprise, :cls, :reported, :payload)
                                ON CONFLICT (ticker, earnings_date)
                                DO UPDATE SET
                                    eps_estimate = COALESCE(EXCLUDED.eps_estimate, earnings_calendar.eps_estimate),
                                    eps_actual = COALESCE(EXCLUDED.eps_actual, earnings_calendar.eps_actual),
                                    eps_surprise_pct = COALESCE(EXCLUDED.eps_surprise_pct, earnings_calendar.eps_surprise_pct),
                                    classification = CASE
                                        WHEN EXCLUDED.reported = TRUE THEN EXCLUDED.classification
                                        ELSE earnings_calendar.classification
                                    END,
                                    reported = EXCLUDED.reported OR earnings_calendar.reported,
                                    pull_timestamp = NOW()
                            """), {
                                "ticker": ticker,
                                "edate": earn_date,
                                "eps_est": eps_est,
                                "eps_act": eps_act,
                                "surprise": round(surprise_pct_raw, 2) if surprise_pct_raw is not None else None,
                                "cls": classification,
                                "reported": reported,
                                "payload": json.dumps({
                                    "eps_estimate": eps_est,
                                    "eps_actual": eps_act,
                                    "surprise_pct": round(surprise_pct_raw, 2) if surprise_pct_raw else None,
                                }),
                            })
                            result["rows_inserted"] += 1

                        except Exception as row_err:
                            log.debug("Row error for {t} at {d}: {e}", t=ticker, d=idx, e=str(row_err))

            # ── Calendar (revenue estimates) ──
            try:
                cal = stock.calendar
                if cal is not None and isinstance(cal, dict):
                    rev_est = _safe_float(cal.get("Revenue Average") or cal.get("Revenue Estimate"))
                    earn_date_raw = cal.get("Earnings Date")
                    if earn_date_raw:
                        # Can be a list of dates
                        dates = earn_date_raw if isinstance(earn_date_raw, list) else [earn_date_raw]
                        for d in dates:
                            try:
                                ed = d.date() if hasattr(d, "date") else d
                                if rev_est is not None:
                                    with self.engine.begin() as conn:
                                        conn.execute(text("""
                                            INSERT INTO earnings_calendar
                                                (ticker, earnings_date, revenue_estimate)
                                            VALUES (:ticker, :edate, :rev_est)
                                            ON CONFLICT (ticker, earnings_date)
                                            DO UPDATE SET
                                                revenue_estimate = COALESCE(
                                                    EXCLUDED.revenue_estimate,
                                                    earnings_calendar.revenue_estimate
                                                ),
                                                pull_timestamp = NOW()
                                        """), {
                                            "ticker": ticker,
                                            "edate": ed,
                                            "rev_est": rev_est,
                                        })
                            except Exception as exc:
                                log.warning("Earnings estimate upsert failed for {t}: {e}", t=ticker, e=exc)
            except Exception as e:
                log.debug("No calendar for {t}: {e}", t=ticker, e=str(e))

            # ── Also store as raw_series for PIT tracking ──
            if earnings_dates is not None and not earnings_dates.empty:
                with self.engine.begin() as conn:
                    for idx, row in earnings_dates.iterrows():
                        earn_date = idx.date() if hasattr(idx, "date") else idx
                        eps_est = _safe_float(row.get("EPS Estimate"))
                        if eps_est is not None:
                            self._insert_raw(
                                conn,
                                series_id=f"EARNINGS:{ticker}:estimate",
                                obs_date=earn_date,
                                value=eps_est,
                            )
                        eps_act = _safe_float(row.get("Reported EPS"))
                        if eps_act is not None:
                            self._insert_raw(
                                conn,
                                series_id=f"EARNINGS:{ticker}:actual",
                                obs_date=earn_date,
                                value=eps_act,
                            )

        except Exception as exc:
            log.error("Earnings pull failed for {t}: {err}", t=ticker, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def pull_all(
        self, ticker_list: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Pull earnings data for all watchlist tickers.

        Parameters:
            ticker_list: Override list; defaults to watchlist tickers.

        Returns:
            list[dict]: One result dict per ticker.
        """
        if ticker_list is None:
            ticker_list = self._get_watchlist_tickers()

        if not ticker_list:
            log.warning("No tickers for earnings calendar pull")
            return []

        log.info(
            "Starting earnings calendar pull — {n} tickers",
            n=len(ticker_list),
        )
        results: list[dict[str, Any]] = []
        for ticker in ticker_list:
            res = self.pull_ticker_earnings(ticker)
            results.append(res)

        ok = sum(1 for r in results if r["status"] == "SUCCESS")
        log.info(
            "Earnings calendar pull complete — {ok}/{total} succeeded",
            ok=ok, total=len(results),
        )
        return results


# ── Convenience Functions ────────────────────────────────────────────────

def get_upcoming_earnings(engine: Engine, days_ahead: int = 30) -> list[dict]:
    """Get upcoming earnings events from the database.

    Args:
        engine: SQLAlchemy engine.
        days_ahead: How many days ahead to look.

    Returns:
        List of earnings event dicts.
    """
    cutoff = date.today() + timedelta(days=days_ahead)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ticker, earnings_date, fiscal_quarter,
                   eps_estimate, revenue_estimate, reported
            FROM earnings_calendar
            WHERE earnings_date >= CURRENT_DATE
              AND earnings_date <= :cutoff
            ORDER BY earnings_date ASC, ticker ASC
        """), {"cutoff": cutoff}).fetchall()

    return [
        {
            "ticker": r[0],
            "earnings_date": r[1].isoformat() if r[1] else None,
            "fiscal_quarter": r[2],
            "eps_estimate": r[3],
            "revenue_estimate": r[4],
            "reported": r[5],
        }
        for r in rows
    ]


def get_recent_earnings(engine: Engine, days_back: int = 30) -> list[dict]:
    """Get recent reported earnings from the database.

    Args:
        engine: SQLAlchemy engine.
        days_back: How many days back to look.

    Returns:
        List of reported earnings event dicts.
    """
    cutoff = date.today() - timedelta(days=days_back)
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ticker, earnings_date, fiscal_quarter,
                   eps_estimate, eps_actual, eps_surprise_pct,
                   revenue_estimate, revenue_actual, revenue_surprise_pct,
                   classification
            FROM earnings_calendar
            WHERE reported = TRUE
              AND earnings_date >= :cutoff
            ORDER BY earnings_date DESC, ticker ASC
        """), {"cutoff": cutoff}).fetchall()

    return [
        {
            "ticker": r[0],
            "earnings_date": r[1].isoformat() if r[1] else None,
            "fiscal_quarter": r[2],
            "eps_estimate": r[3],
            "eps_actual": r[4],
            "eps_surprise_pct": round(r[5], 2) if r[5] is not None else None,
            "revenue_estimate": r[6],
            "revenue_actual": r[7],
            "revenue_surprise_pct": round(r[8], 2) if r[8] is not None else None,
            "classification": r[9],
        }
        for r in rows
    ]


def get_earnings_history(engine: Engine, ticker: str, limit: int = 20) -> list[dict]:
    """Get full earnings history for a single ticker.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        limit: Max rows to return.

    Returns:
        List of historical earnings dicts.
    """
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT earnings_date, fiscal_quarter,
                   eps_estimate, eps_actual, eps_surprise_pct,
                   revenue_estimate, revenue_actual, revenue_surprise_pct,
                   classification, reported
            FROM earnings_calendar
            WHERE ticker = :ticker
            ORDER BY earnings_date DESC
            LIMIT :lim
        """), {"ticker": ticker, "lim": limit}).fetchall()

    return [
        {
            "earnings_date": r[0].isoformat() if r[0] else None,
            "fiscal_quarter": r[1],
            "eps_estimate": r[2],
            "eps_actual": r[3],
            "eps_surprise_pct": round(r[4], 2) if r[4] is not None else None,
            "revenue_estimate": r[5],
            "revenue_actual": r[6],
            "revenue_surprise_pct": round(r[7], 2) if r[7] is not None else None,
            "classification": r[8],
            "reported": r[9],
        }
        for r in rows
    ]


if __name__ == "__main__":
    from db import get_engine

    puller = EarningsCalendarPuller(db_engine=get_engine())
    results = puller.pull_all()
    for r in results:
        print(f"  {r['ticker']}: {r['status']} ({r['rows_inserted']} rows)")
