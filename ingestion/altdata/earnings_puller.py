"""
GRID earnings data puller — fills the 'earnings' feature family in raw_series.

Pulls comprehensive earnings data from Yahoo Finance via yfinance:
  - Earnings dates (next/past announcements)
  - Quarterly earnings (EPS actual vs estimate, surprise %)
  - Revenue (quarterly actual vs estimate)
  - Earnings history

Series stored with pattern: earnings:{ticker}:{field}
Fields: eps_actual, eps_estimate, surprise_pct, revenue_actual,
        revenue_estimate, revenue_surprise_pct, beat_flag

Schedule: daily pull via hermes operator.
"""

from __future__ import annotations

import math
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import yfinance as yf
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

# ── Ticker Universe ────────────────────────────────────────────────────────

EARNINGS_TICKERS: list[str] = [
    # NASDAQ 100 + key others (~120 tickers)
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "BRK-B",
    "UNH", "JNJ", "JPM", "V", "PG", "MA", "HD", "ABBV", "MRK", "PFE",
    "KO", "PEP", "COST", "AVGO", "TMO", "MCD", "ACN", "CSCO", "ABT",
    "DHR", "WMT", "CRM", "LIN", "AMD", "TXN", "NEE", "BMY", "UNP",
    "PM", "RTX", "HON", "LOW", "QCOM", "SCHW", "INTC", "AMAT", "GS",
    "BLK", "ISRG", "INTU", "SYK", "ADP", "MDLZ", "GILD", "DE", "VRTX",
    "CI", "REGN", "ADI", "MMC", "CVS", "ETN", "ZTS", "NOW", "PYPL",
    "CME", "DUK", "SO", "CL", "ICE", "SHW", "CB", "MO", "NFLX",
    "LRCX", "PGR", "FIS", "HUM", "KLAC", "ORLY", "EL", "GD", "F",
    "GM", "DAL", "LUV", "BA", "CAT", "MMM", "COIN", "MARA", "SQ",
    "SHOP", "PLTR", "SMCI", "ARM", "CRWD", "SNOW", "DDOG", "NET", "ZS",
]

# Rate limit between tickers (seconds)
_RATE_LIMIT_DELAY = 0.5

# Beat/miss threshold for flagging
_SIGNIFICANT_SURPRISE_PCT = 10.0


def _safe_float(val: Any) -> float | None:
    """Convert a value to float, returning None for NaN/None/Inf."""
    if val is None:
        return None
    try:
        f = float(val)
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None


def compute_surprise_pct(actual: float | None, estimate: float | None) -> float | None:
    """Compute earnings surprise percentage.

    Formula: (actual - estimate) / abs(estimate) * 100

    Returns None if either value is missing or estimate is zero.
    """
    if actual is None or estimate is None:
        return None
    if estimate == 0:
        return None
    return (actual - estimate) / abs(estimate) * 100


def classify_beat_miss(surprise_pct: float | None) -> str:
    """Classify earnings result based on surprise percentage.

    Returns:
        'significant_beat' if surprise > 10%
        'significant_miss' if surprise < -10%
        'beat' if 0 < surprise <= 10%
        'miss' if -10% <= surprise < 0%
        'inline' if surprise == 0
        'unknown' if surprise is None
    """
    if surprise_pct is None:
        return "unknown"
    if surprise_pct > _SIGNIFICANT_SURPRISE_PCT:
        return "significant_beat"
    elif surprise_pct < -_SIGNIFICANT_SURPRISE_PCT:
        return "significant_miss"
    elif surprise_pct > 0:
        return "beat"
    elif surprise_pct < 0:
        return "miss"
    return "inline"


class EarningsPuller(BasePuller):
    """Pulls comprehensive earnings data into raw_series.

    Fetches from yfinance:
      - .earnings_dates — upcoming and recent EPS estimates/actuals
      - .quarterly_earnings — historical quarterly EPS
      - .earnings_history — EPS history with surprise data

    All data stored in raw_series with series_id pattern:
      earnings:{ticker}:{field}
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
        log.info("EarningsPuller initialised — source_id={sid}", sid=self.source_id)

    @retry_on_failure(max_attempts=3, backoff=2.0)
    def _fetch_ticker_data(self, ticker: str) -> yf.Ticker:
        """Fetch yfinance Ticker object with retry on failure."""
        return yf.Ticker(ticker)

    def _store_series_point(
        self,
        conn: Any,
        ticker: str,
        field: str,
        obs_date: date,
        value: float,
        raw_payload: dict[str, Any] | None = None,
    ) -> bool:
        """Store a single earnings data point in raw_series.

        Uses dedup via _get_existing_dates for efficiency.

        Returns:
            True if inserted, False if skipped (duplicate).
        """
        series_id = f"earnings:{ticker}:{field}"
        self._insert_raw(
            conn=conn,
            series_id=series_id,
            obs_date=obs_date,
            value=value,
            raw_payload=raw_payload,
        )
        return True

    def _process_earnings_dates(
        self,
        conn: Any,
        ticker: str,
        stock: yf.Ticker,
    ) -> int:
        """Process .earnings_dates — EPS estimates, actuals, surprise.

        Returns number of rows inserted.
        """
        inserted = 0

        try:
            earnings_dates = stock.earnings_dates
        except Exception as exc:
            log.debug("No earnings_dates for {t}: {e}", t=ticker, e=str(exc))
            return 0

        if earnings_dates is None or earnings_dates.empty:
            return 0

        # Pre-fetch existing dates for dedup
        existing_eps_actual = self._get_existing_dates(
            f"earnings:{ticker}:eps_actual", conn
        )
        existing_eps_estimate = self._get_existing_dates(
            f"earnings:{ticker}:eps_estimate", conn
        )
        existing_surprise = self._get_existing_dates(
            f"earnings:{ticker}:surprise_pct", conn
        )

        for idx, row in earnings_dates.iterrows():
            try:
                obs = idx.date() if hasattr(idx, "date") else idx

                eps_est = _safe_float(row.get("EPS Estimate"))
                eps_act = _safe_float(row.get("Reported EPS"))
                surprise_raw = _safe_float(row.get("Surprise(%)"))

                # Compute surprise ourselves if yfinance did not provide
                if surprise_raw is None:
                    surprise_raw = compute_surprise_pct(eps_act, eps_est)

                payload = {
                    "source": "earnings_dates",
                    "eps_estimate": eps_est,
                    "eps_actual": eps_act,
                    "surprise_pct": round(surprise_raw, 4) if surprise_raw is not None else None,
                    "classification": classify_beat_miss(surprise_raw),
                }

                if eps_est is not None and obs not in existing_eps_estimate:
                    self._store_series_point(
                        conn, ticker, "eps_estimate", obs, eps_est, payload
                    )
                    inserted += 1

                if eps_act is not None and obs not in existing_eps_actual:
                    self._store_series_point(
                        conn, ticker, "eps_actual", obs, eps_act, payload
                    )
                    inserted += 1

                if surprise_raw is not None and obs not in existing_surprise:
                    self._store_series_point(
                        conn, ticker, "surprise_pct", obs, round(surprise_raw, 4), payload
                    )
                    inserted += 1

                    # Flag significant beats/misses with a dedicated series
                    if abs(surprise_raw) > _SIGNIFICANT_SURPRISE_PCT:
                        beat_val = 1.0 if surprise_raw > 0 else -1.0
                        self._store_series_point(
                            conn, ticker, "beat_flag", obs, beat_val, payload
                        )
                        inserted += 1

            except Exception as row_exc:
                log.debug(
                    "Row error processing earnings_dates for {t} at {d}: {e}",
                    t=ticker, d=idx, e=str(row_exc),
                )

        return inserted

    def _process_quarterly_earnings(
        self,
        conn: Any,
        ticker: str,
        stock: yf.Ticker,
    ) -> int:
        """Process .quarterly_earnings — historical quarterly EPS data.

        Returns number of rows inserted.
        """
        inserted = 0

        try:
            qe = stock.quarterly_earnings
        except Exception as exc:
            log.debug("No quarterly_earnings for {t}: {e}", t=ticker, e=str(exc))
            return 0

        if qe is None or (isinstance(qe, pd.DataFrame) and qe.empty):
            return 0

        existing_revenue = self._get_existing_dates(
            f"earnings:{ticker}:revenue_actual", conn
        )

        for idx, row in qe.iterrows():
            try:
                # Index is typically a date or quarter string
                if hasattr(idx, "date"):
                    obs = idx.date()
                elif isinstance(idx, str):
                    # Try parsing quarter string like "4Q2024"
                    try:
                        obs = pd.to_datetime(idx).date()
                    except Exception:
                        continue
                else:
                    obs = idx

                revenue = _safe_float(row.get("Revenue"))
                earnings = _safe_float(row.get("Earnings"))

                payload = {
                    "source": "quarterly_earnings",
                    "revenue": revenue,
                    "earnings": earnings,
                }

                if revenue is not None and obs not in existing_revenue:
                    self._store_series_point(
                        conn, ticker, "revenue_actual", obs, revenue, payload
                    )
                    inserted += 1

                if earnings is not None:
                    # earnings from quarterly_earnings may overlap with eps_actual
                    # Store under quarterly_earnings field to avoid collision
                    self._store_series_point(
                        conn, ticker, "quarterly_earnings", obs, earnings, payload
                    )
                    inserted += 1

            except Exception as row_exc:
                log.debug(
                    "Row error processing quarterly_earnings for {t} at {d}: {e}",
                    t=ticker, d=idx, e=str(row_exc),
                )

        return inserted

    def _process_earnings_history(
        self,
        conn: Any,
        ticker: str,
        stock: yf.Ticker,
    ) -> int:
        """Process .earnings_history — historical EPS with surprise data.

        Returns number of rows inserted.
        """
        inserted = 0

        try:
            eh = stock.earnings_history
        except Exception as exc:
            log.debug("No earnings_history for {t}: {e}", t=ticker, e=str(exc))
            return 0

        if eh is None or (isinstance(eh, pd.DataFrame) and eh.empty):
            return 0

        existing_hist = self._get_existing_dates(
            f"earnings:{ticker}:history_surprise_pct", conn
        )

        for idx, row in eh.iterrows():
            try:
                # Determine obs_date from index or column
                if hasattr(idx, "date"):
                    obs = idx.date()
                elif "Quarter End" in eh.columns:
                    qe_val = row.get("Quarter End")
                    if qe_val is not None and hasattr(qe_val, "date"):
                        obs = qe_val.date()
                    else:
                        continue
                else:
                    continue

                eps_est = _safe_float(row.get("epsEstimate") or row.get("EPS Estimate"))
                eps_act = _safe_float(row.get("epsActual") or row.get("Reported EPS"))
                surprise = _safe_float(row.get("surprisePercent") or row.get("Surprise(%)"))

                if surprise is None:
                    surprise = compute_surprise_pct(eps_act, eps_est)

                payload = {
                    "source": "earnings_history",
                    "eps_estimate": eps_est,
                    "eps_actual": eps_act,
                    "surprise_pct": round(surprise, 4) if surprise is not None else None,
                    "classification": classify_beat_miss(surprise),
                }

                if surprise is not None and obs not in existing_hist:
                    self._store_series_point(
                        conn, ticker, "history_surprise_pct", obs,
                        round(surprise, 4), payload,
                    )
                    inserted += 1

            except Exception as row_exc:
                log.debug(
                    "Row error processing earnings_history for {t}: {e}",
                    t=ticker, e=str(row_exc),
                )

        return inserted

    def pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull all earnings data for a single ticker.

        Fetches earnings_dates, quarterly_earnings, and earnings_history
        and stores everything in raw_series.

        Parameters:
            ticker: Stock ticker symbol.

        Returns:
            dict with ticker, rows_inserted, status, errors, significant_surprises.
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
            "significant_surprises": [],
        }

        try:
            stock = self._fetch_ticker_data(ticker)

            with self.engine.begin() as conn:
                # 1. Earnings dates (EPS estimates/actuals/surprise)
                n1 = self._process_earnings_dates(conn, ticker, stock)
                result["rows_inserted"] += n1

                # 2. Quarterly earnings (revenue + earnings)
                n2 = self._process_quarterly_earnings(conn, ticker, stock)
                result["rows_inserted"] += n2

                # 3. Earnings history (historical surprise data)
                n3 = self._process_earnings_history(conn, ticker, stock)
                result["rows_inserted"] += n3

            # Detect significant surprises for reporting
            result["significant_surprises"] = self._detect_significant_surprises(
                ticker, stock
            )

            if result["rows_inserted"] == 0:
                result["status"] = "PARTIAL"
                result["errors"].append("No earnings data available")

        except Exception as exc:
            log.error("Earnings pull failed for {t}: {err}", t=ticker, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        return result

    def _detect_significant_surprises(
        self,
        ticker: str,
        stock: yf.Ticker,
    ) -> list[dict[str, Any]]:
        """Detect tickers with >10% earnings surprise.

        Returns list of surprise events for reporting.
        """
        surprises: list[dict[str, Any]] = []

        try:
            earnings_dates = stock.earnings_dates
            if earnings_dates is None or earnings_dates.empty:
                return surprises

            for idx, row in earnings_dates.iterrows():
                eps_est = _safe_float(row.get("EPS Estimate"))
                eps_act = _safe_float(row.get("Reported EPS"))
                surprise_raw = _safe_float(row.get("Surprise(%)"))

                if surprise_raw is None:
                    surprise_raw = compute_surprise_pct(eps_act, eps_est)

                if surprise_raw is not None and abs(surprise_raw) > _SIGNIFICANT_SURPRISE_PCT:
                    obs = idx.date() if hasattr(idx, "date") else idx
                    surprises.append({
                        "ticker": ticker,
                        "date": str(obs),
                        "eps_estimate": eps_est,
                        "eps_actual": eps_act,
                        "surprise_pct": round(surprise_raw, 2),
                        "classification": classify_beat_miss(surprise_raw),
                    })
        except Exception:
            pass

        return surprises

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        rate_limit: float = _RATE_LIMIT_DELAY,
    ) -> list[dict[str, Any]]:
        """Pull earnings data for all tickers in the universe.

        Parameters:
            ticker_list: Override list; defaults to EARNINGS_TICKERS.
            rate_limit: Seconds to wait between tickers (default: 0.5).

        Returns:
            list[dict]: One result dict per ticker.
        """
        if ticker_list is None:
            ticker_list = EARNINGS_TICKERS

        log.info(
            "Starting earnings pull — {n} tickers",
            n=len(ticker_list),
        )

        results: list[dict[str, Any]] = []
        all_significant: list[dict[str, Any]] = []

        for i, ticker in enumerate(ticker_list):
            res = self.pull_ticker(ticker)
            results.append(res)

            if res["significant_surprises"]:
                all_significant.extend(res["significant_surprises"])

            # Rate limit to avoid yfinance throttling
            if i < len(ticker_list) - 1:
                time.sleep(rate_limit)

            # Progress logging every 20 tickers
            if (i + 1) % 20 == 0:
                log.info(
                    "Earnings pull progress: {done}/{total}",
                    done=i + 1,
                    total=len(ticker_list),
                )

        ok = sum(1 for r in results if r["status"] == "SUCCESS")
        total_rows = sum(r["rows_inserted"] for r in results)

        log.info(
            "Earnings pull complete — {ok}/{total} succeeded, {rows} rows inserted",
            ok=ok,
            total=len(results),
            rows=total_rows,
        )

        if all_significant:
            log.info(
                "Significant earnings surprises (>10%): {n} events",
                n=len(all_significant),
            )
            for s in all_significant:
                log.info(
                    "  {t} on {d}: {pct:+.1f}% ({cls})",
                    t=s["ticker"],
                    d=s["date"],
                    pct=s["surprise_pct"],
                    cls=s["classification"],
                )

        return results

    def get_summary(self, results: list[dict[str, Any]]) -> dict[str, Any]:
        """Generate a summary report from pull results.

        Parameters:
            results: List of per-ticker result dicts from pull_all.

        Returns:
            Summary dict with counts, failures, and significant surprises.
        """
        succeeded = [r for r in results if r["status"] == "SUCCESS"]
        failed = [r for r in results if r["status"] == "FAILED"]
        partial = [r for r in results if r["status"] == "PARTIAL"]
        all_surprises = []
        for r in results:
            all_surprises.extend(r.get("significant_surprises", []))

        return {
            "total_tickers": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "partial": len(partial),
            "total_rows_inserted": sum(r["rows_inserted"] for r in results),
            "failed_tickers": [r["ticker"] for r in failed],
            "significant_surprises": all_surprises,
            "significant_beats": [
                s for s in all_surprises if "beat" in s.get("classification", "")
            ],
            "significant_misses": [
                s for s in all_surprises if "miss" in s.get("classification", "")
            ],
        }


if __name__ == "__main__":
    from db import get_engine

    puller = EarningsPuller(db_engine=get_engine())
    results = puller.pull_all()
    summary = puller.get_summary(results)
    print(f"\nEarnings Pull Summary:")
    print(f"  Succeeded: {summary['succeeded']}/{summary['total_tickers']}")
    print(f"  Failed: {summary['failed']} — {summary['failed_tickers']}")
    print(f"  Total rows: {summary['total_rows_inserted']}")
    print(f"  Significant beats: {len(summary['significant_beats'])}")
    print(f"  Significant misses: {len(summary['significant_misses'])}")
    for s in summary["significant_surprises"]:
        print(f"    {s['ticker']} {s['date']}: {s['surprise_pct']:+.1f}% ({s['classification']})")
