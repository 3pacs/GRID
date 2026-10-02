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

import logging
import math
import time
from datetime import date
from types import SimpleNamespace
from typing import Any

import pandas as pd
from loguru import logger as log
from sqlalchemy import exc as sa_exc
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, log_pull_failure, retry_on_failure

try:
    import yfinance as yf
except Exception as exc:  # yfinance can fail if optional websocket deps drift.
    yf = None  # type: ignore[assignment]
    _YFINANCE_IMPORT_ERROR: Exception | None = exc
else:
    _YFINANCE_IMPORT_ERROR = None
    logging.getLogger("yfinance").setLevel(logging.CRITICAL)

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

# Bound modified rows across all earnings fields, not upstream observations.
STORE_BATCH_ROWS = 50
MAX_CONSECUTIVE_CONNECTION_FAILURES = 2


class EarningsStoreAborted(RuntimeError):
    """Database unavailable; ``stored`` counts committed rows in this batch."""

    def __init__(
        self, message: str, stored: int = 0, failed: int = 0,
        errors: list[str] | None = None, commit_outcome_unknown: bool = False,
    ) -> None:
        super().__init__(message)
        self.stored = stored
        self.failed = failed
        self.errors = errors or []
        self.commit_outcome_unknown = commit_outcome_unknown


class _ConnectionFailure(Exception):
    """A transaction could not open, or its connection became unusable."""

    def __init__(self, cause: BaseException, commit_outcome_unknown: bool = False) -> None:
        super().__init__(str(cause))
        self.commit_outcome_unknown = commit_outcome_unknown


def _sqlstate(exc: BaseException) -> str | None:
    """Read the server's SQLSTATE through psycopg2/psycopg3 wrappers."""
    original = getattr(exc, "orig", exc)
    return getattr(original, "sqlstate", None) or getattr(original, "pgcode", None)


def _is_connection_error(exc: BaseException) -> bool:
    # OperationalError also covers answered deadlocks, lock timeouts and
    # cancellations. Those leave a usable connection after rollback.
    state = _sqlstate(exc)
    return (
        isinstance(exc, (sa_exc.DisconnectionError, sa_exc.TimeoutError))
        or bool(getattr(exc, "connection_invalidated", False))
        or bool(state and state.startswith("08"))
    )


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
        if yf is None:
            raise RuntimeError(f"yfinance unavailable: {_YFINANCE_IMPORT_ERROR}")
        return yf.Ticker(ticker)

    def _collect_series_point(
        self,
        points: list[dict[str, Any]],
        ticker: str,
        field: str,
        obs_date: date,
        value: float,
        raw_payload: dict[str, Any] | None = None,
    ) -> None:
        """Collect one point in memory; database writes happen after fetching."""
        points.append({
            "series_id": f"earnings:{ticker}:{field}",
            "obs_date": obs_date,
            "value": value,
            "raw_payload": raw_payload,
        })

    def _process_earnings_dates(
        self,
        ticker: str,
        stock: yf.Ticker,
    ) -> list[dict[str, Any]]:
        """Process .earnings_dates — EPS estimates, actuals, surprise.

        Returns points to store, without opening a database transaction.
        """
        points: list[dict[str, Any]] = []

        try:
            earnings_dates = stock.earnings_dates
        except Exception as exc:
            log.debug("No earnings_dates for {t}: {e}", t=ticker, e=str(exc))
            return points

        if earnings_dates is None or earnings_dates.empty:
            return points

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

                if eps_est is not None:
                    self._collect_series_point(
                        points, ticker, "eps_estimate", obs, eps_est, payload
                    )

                if eps_act is not None:
                    self._collect_series_point(
                        points, ticker, "eps_actual", obs, eps_act, payload
                    )

                if surprise_raw is not None:
                    self._collect_series_point(
                        points, ticker, "surprise_pct", obs, round(surprise_raw, 4), payload
                    )

                    # Flag significant beats/misses with a dedicated series
                    if abs(surprise_raw) > _SIGNIFICANT_SURPRISE_PCT:
                        beat_val = 1.0 if surprise_raw > 0 else -1.0
                        self._collect_series_point(
                            points, ticker, "beat_flag", obs, beat_val, payload
                        )

            except Exception as row_exc:
                log.debug(
                    "Row error processing earnings_dates for {t} at {d}: {e}",
                    t=ticker, d=idx, e=str(row_exc),
                )

        return points

    def _process_quarterly_earnings(
        self,
        ticker: str,
        stock: yf.Ticker,
    ) -> list[dict[str, Any]]:
        """Process .quarterly_earnings — historical quarterly EPS data.

        Returns points to store, without opening a database transaction.
        """
        points: list[dict[str, Any]] = []

        try:
            qe = stock.quarterly_earnings
        except Exception as exc:
            log.debug("No quarterly_earnings for {t}: {e}", t=ticker, e=str(exc))
            return points

        if qe is None or (isinstance(qe, pd.DataFrame) and qe.empty):
            return points

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

                if revenue is not None:
                    self._collect_series_point(
                        points, ticker, "revenue_actual", obs, revenue, payload
                    )

                if earnings is not None:
                    # earnings from quarterly_earnings may overlap with eps_actual
                    # Store under quarterly_earnings field to avoid collision
                    self._collect_series_point(
                        points, ticker, "quarterly_earnings", obs, earnings, payload
                    )

            except Exception as row_exc:
                log.debug(
                    "Row error processing quarterly_earnings for {t} at {d}: {e}",
                    t=ticker, d=idx, e=str(row_exc),
                )

        return points

    def _process_earnings_history(
        self,
        ticker: str,
        stock: yf.Ticker,
    ) -> list[dict[str, Any]]:
        """Process .earnings_history — historical EPS with surprise data.

        Returns points to store, without opening a database transaction.
        """
        points: list[dict[str, Any]] = []

        try:
            eh = stock.earnings_history
        except Exception as exc:
            log.debug("No earnings_history for {t}: {e}", t=ticker, e=str(exc))
            return points

        if eh is None or (isinstance(eh, pd.DataFrame) and eh.empty):
            return points

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

                if surprise is not None:
                    self._collect_series_point(
                        points, ticker, "history_surprise_pct", obs,
                        round(surprise, 4), payload,
                    )

            except Exception as row_exc:
                log.debug(
                    "Row error processing earnings_history for {t}: {e}",
                    t=ticker, e=str(row_exc),
                )

        return points

    def _write(self, fn) -> None:
        """Commit one short transaction; distinguish unavailable connections."""
        opened = False
        statements_completed = False
        try:
            with self.engine.begin() as conn:
                opened = True
                fn(conn)
                statements_completed = True
        except Exception as exc:
            # An unanswered COMMIT may already have committed on the server.
            # Keep it unknown and never replay it, even if the driver failed
            # to mark its connection invalidated. An answered non-08 SQLSTATE
            # instead confirms a rejected transaction and permits fallback.
            unanswered_commit = statements_completed and _sqlstate(exc) is None
            if not opened or _is_connection_error(exc) or unanswered_commit:
                raise _ConnectionFailure(exc, commit_outcome_unknown=statements_completed) from exc
            raise

    def _store_points(self, conn: Any, points: list[dict[str, Any]]) -> int:
        """Insert at most 50 points, deduping successful observations only."""
        dates: dict[str, set[date]] = {}
        for point in points:
            dates.setdefault(point["series_id"], set()).add(point["obs_date"])
        existing = {
            sid: self._get_existing_dates(sid, conn, start_date=min(days), end_date=max(days))
            for sid, days in dates.items()
        }
        stored = 0
        for point in points:
            days = existing[point["series_id"]]
            if point["obs_date"] in days:
                continue
            self._insert_raw(conn=conn, **point)
            days.add(point["obs_date"])
            stored += 1
        return stored

    @staticmethod
    def _note_connection_failure(
        streak: dict[str, int], exc: BaseException | None, stored: int,
        failed: int = 0, errors: list[str] | None = None,
    ) -> None:
        streak["connection_failures"] += 1
        if streak["connection_failures"] >= MAX_CONSECUTIVE_CONNECTION_FAILURES:
            raise EarningsStoreAborted(f"database unavailable: {exc}", stored=stored, failed=failed, errors=errors) from exc

    def _store_batch(
        self, ticker: str, batch: list[dict[str, Any]], streak: dict[str, int],
    ) -> tuple[int, int, list[str]]:
        """Rollback a failed batch, then retry each point in its own transaction."""
        if len(batch) > STORE_BATCH_ROWS:
            raise ValueError(f"earnings batch exceeds {STORE_BATCH_ROWS} rows")
        counter = {"stored": 0}

        def store_all(conn: Any) -> None:
            counter["stored"] = self._store_points(conn, batch)

        try:
            self._write(store_all)
            streak["connection_failures"] = 0
            return counter["stored"], 0, []
        except _ConnectionFailure as exc:
            if exc.commit_outcome_unknown:
                # The server may have committed before the connection died.
                # Neither zero nor the tentative count is confirmed. Do not
                # turn a retry/dedupe response into an invented exact count.
                raise EarningsStoreAborted("database connection failed during commit; outcome unknown",
                                           commit_outcome_unknown=True) from exc
            self._note_connection_failure(streak, exc.__cause__, stored=0)
        except Exception as exc:
            streak["connection_failures"] = 0
            log.warning("Earnings: {t} batch failed; retrying one row per transaction: {e}", t=ticker, e=str(exc))

        stored = failed = 0
        errors: list[str] = []
        for point in batch:
            def store_one(conn: Any, point: dict[str, Any] = point) -> None:
                counter["stored"] = self._store_points(conn, [point])

            try:
                self._write(store_one)
                streak["connection_failures"] = 0
                stored += counter["stored"]
            except _ConnectionFailure as exc:
                if exc.commit_outcome_unknown:
                    raise EarningsStoreAborted(
                        "database connection failed during commit; outcome unknown",
                        stored=stored, failed=failed, errors=errors, commit_outcome_unknown=True,
                    ) from exc
                failed += 1
                errors.append(f"{point['series_id']} @ {point['obs_date']}: connection failure")
                self._note_connection_failure(streak, exc.__cause__, stored=stored, failed=failed, errors=errors)
            except Exception as exc:
                # An answered data error proves the database is reachable.
                streak["connection_failures"] = 0
                failed += 1
                message = f"{point['series_id']} @ {point['obs_date']}: {str(exc)[:200]}"
                errors.append(message)
                log.warning("Earnings row failed: {e}", e=message)
        return stored, failed, errors

    def pull_ticker(self, ticker: str) -> dict[str, Any]:
        """Pull all earnings data for a single ticker.

        Fetches earnings_dates, quarterly_earnings, and earnings_history
        and stores everything in raw_series.

        Parameters:
            ticker: Stock ticker symbol.

        Returns:
            dict with ticker, rows_inserted, status, errors, significant_surprises.
            rows_inserted counts acknowledged commits; commit_outcome_unknown
            flags a lost commit acknowledgement rather than claiming an exact total.
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "rows_failed": 0,
            "status": "SUCCESS",
            "errors": [],
            "significant_surprises": [],
        }

        try:
            stock = self._fetch_ticker_data(ticker)

            # yfinance properties can each make network calls. Fetch them
            # once, before opening any transaction, including the reporting
            # input so surprise detection never re-fetches after writes.
            snapshot = SimpleNamespace()
            points: list[dict[str, Any]] = []
            for label, phase_fn in (
                ("earnings_dates", self._process_earnings_dates),
                ("quarterly_earnings", self._process_quarterly_earnings),
                ("earnings_history", self._process_earnings_history),
            ):
                try:
                    setattr(snapshot, label, getattr(stock, label))
                    points.extend(phase_fn(ticker, snapshot))
                except Exception as phase_exc:
                    setattr(snapshot, label, None)
                    log.warning("earnings phase {p} failed for {t}: {e}", p=label, t=ticker, e=str(phase_exc))
                    result["errors"].append(f"{label}: {phase_exc}")

            result["significant_surprises"] = self._detect_significant_surprises(
                ticker, snapshot
            )

            streak = {"connection_failures": 0}
            for start in range(0, len(points), STORE_BATCH_ROWS):
                try:
                    stored, failed, errors = self._store_batch(ticker, points[start:start + STORE_BATCH_ROWS], streak)
                except EarningsStoreAborted as exc:
                    result["rows_inserted"] += exc.stored
                    result["rows_failed"] += exc.failed
                    result["errors"].extend(exc.errors)
                    result["status"] = "PARTIAL" if result["rows_inserted"] else "FAILED"
                    result["aborted"] = True
                    if exc.commit_outcome_unknown:
                        result["commit_outcome_unknown"] = True
                    result["errors"].append(str(exc))
                    return result
                result["rows_inserted"] += stored
                result["rows_failed"] += failed
                result["errors"].extend(errors)

            if result["rows_failed"] and not result["rows_inserted"]:
                result["status"] = "FAILED"
            elif result["errors"] or result["rows_inserted"] == 0:
                result["status"] = "PARTIAL"
            if not points and not result["errors"]:
                result["errors"].append("No earnings data available")

        except Exception as exc:
            # yfinance throws plain RuntimeError/HTTPError on rate-limit
            # and JSON parse failures — those are upstream noise.
            # Genuine code bugs (KeyError, AttributeError, ImportError)
            # still escalate to ERROR via log_pull_failure.
            log_pull_failure("Earnings", ticker, exc)
            result["status"] = "PARTIAL" if result["rows_inserted"] else "FAILED"
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

            if res.get("aborted"):
                log.warning("Earnings scan stopped: database unavailable at {t}", t=ticker)
                break

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

        summary = self.get_summary(results)
        log.info(
            "Earnings pull complete — {ok}/{total} succeeded, {ack} rows acknowledged; actual rows inserted: {actual}",
            ok=summary["succeeded"],
            total=len(results),
            ack=summary["acknowledged_rows_inserted"],
            actual="unknown (COMMIT outcome unknown)" if summary["commit_outcome_unknown"] else summary["actual_rows_inserted"],
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
            Actual/total inserted rows are None after any unknown COMMIT;
            acknowledged_rows_inserted remains a confirmed lower bound.
        """
        succeeded = [r for r in results if r["status"] == "SUCCESS"]
        failed = [r for r in results if r["status"] == "FAILED"]
        partial = [r for r in results if r["status"] == "PARTIAL"]
        all_surprises = []
        for r in results:
            all_surprises.extend(r.get("significant_surprises", []))
        acknowledged_rows = sum(r["rows_inserted"] for r in results)
        unknown_commit = any(r.get("commit_outcome_unknown", False) for r in results)
        actual_rows = None if unknown_commit else acknowledged_rows

        return {
            "total_tickers": len(results),
            "succeeded": len(succeeded),
            "failed": len(failed),
            "partial": len(partial),
            "total_rows_inserted": actual_rows,
            "actual_rows_inserted": actual_rows,
            "acknowledged_rows_inserted": acknowledged_rows,
            "commit_outcome_unknown": unknown_commit,
            "failed_tickers": [r["ticker"] for r in failed],
            "significant_surprises": all_surprises,
            "significant_beats": [
                s for s in all_surprises if "beat" in s.get("classification", "")
            ],
            "significant_misses": [
                s for s in all_surprises if "miss" in s.get("classification", "")
            ],
        }


def main() -> None:
    from db import get_engine

    puller = EarningsPuller(db_engine=get_engine())
    results = puller.pull_all()
    summary = puller.get_summary(results)
    print("\nEarnings Pull Summary:")
    print(f"  Succeeded: {summary['succeeded']}/{summary['total_tickers']}")
    print(f"  Failed: {summary['failed']} — {summary['failed_tickers']}")
    print(f"  Acknowledged rows: {summary['acknowledged_rows_inserted']}")
    actual = "unknown (COMMIT outcome unknown)" if summary["commit_outcome_unknown"] else summary["actual_rows_inserted"]
    print(f"  Actual total rows: {actual}")
    print(f"  Significant beats: {len(summary['significant_beats'])}")
    print(f"  Significant misses: {len(summary['significant_misses'])}")
    for s in summary["significant_surprises"]:
        print(f"    {s['ticker']} {s['date']}: {s['surprise_pct']:+.1f}% ({s['classification']})")


if __name__ == "__main__":
    main()
