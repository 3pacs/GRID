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
import math
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller


class _UnavailableYFinance:
    def __init__(self, error: Exception) -> None:
        self._error = error

    def Ticker(self, *_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError(
            "yfinance is unavailable; install/update yfinance and its dependencies "
            f"before running earnings calendar pulls ({self._error})"
        )


try:
    import yfinance as yf
except Exception as exc:  # yfinance can fail if optional websocket deps drift.
    yf = _UnavailableYFinance(exc)


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


# One budget-stop WARNING per process; later stops in the same process log at
# INFO (the scheduler still records every PARTIAL in pull_log).
_BUDGET_STOP_WARNED = False


def _same_value(a: float, b: float) -> bool:
    """True when two stored EPS values are the same observation."""
    return math.isclose(float(a), float(b), rel_tol=1e-9, abs_tol=1e-12)


def _dedupe_earnings_rows(earnings_dates: Any) -> list[dict[str, Any]]:
    """Collapse a yfinance ``earnings_dates`` frame to one record per date.

    yfinance can return several rows for the same calendar day (different
    announcement timestamps, or the SAME timestamp with two estimates — seen
    for IVA on 2022-03-10). raw_series' unique index is
    ``(series_id, source_id, obs_date, pull_timestamp)`` and pull_timestamp
    is the transaction time, so two rows for one obs_date inside one
    transaction raise UniqueViolation and abort the whole ticker.

    The kept row is chosen deterministically, independent of yfinance's row
    order (so the choice cannot flip between pulls and mint a fake vintage):
    a row with a Reported EPS wins, then the latest announcement timestamp,
    then a row with an EPS Estimate, then the lower estimate.

    Returns:
        list of dicts (earn_date, eps_est, eps_act, surprise_pct), sorted by
        earn_date ascending. Rows whose index cannot be read as a date are
        skipped with a debug log.
    """
    best: dict[date, tuple[tuple, dict[str, Any]]] = {}
    for idx, row in earnings_dates.iterrows():
        try:
            earn_date = idx.date() if hasattr(idx, "date") else idx
            if not isinstance(earn_date, date):
                raise TypeError(f"earnings date index is {type(idx).__name__}")
            eps_est = _safe_float(row.get("EPS Estimate"))
            eps_act = _safe_float(row.get("Reported EPS"))
            surprise = _safe_float(row.get("Surprise(%)"))
            ts = float(idx.timestamp()) if hasattr(idx, "timestamp") else 0.0
        except Exception as row_err:
            log.debug("Earnings row skipped at {d}: {e}", d=idx, e=str(row_err))
            continue
        rank = (
            eps_act is not None,
            ts,
            eps_est is not None,
            -eps_est if eps_est is not None else 0.0,
            -eps_act if eps_act is not None else 0.0,
        )
        record = {
            "earn_date": earn_date,
            "eps_est": eps_est,
            "eps_act": eps_act,
            "surprise_pct": surprise,
        }
        current = best.get(earn_date)
        if current is None or rank > current[0]:
            best[earn_date] = (rank, record)
    return [best[d][1] for d in sorted(best)]


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

    def _order_least_recently_pulled(self, tickers: list[str]) -> list[str]:
        """Order tickers so the least recently refreshed are pulled first.

        A budget-limited run (``should_continue``) stops part-way through
        the universe; ordering by the last ``earnings_calendar`` refresh
        means the next run continues with the tickers this one did not
        reach instead of re-pulling the same alphabetical prefix. Tickers
        never seen (no earnings_calendar row) come first. On any lookup
        failure the original order is kept.
        """
        if not tickers:
            return tickers
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT ticker, MAX(pull_timestamp)
                    FROM earnings_calendar
                    WHERE ticker = ANY(:tickers)
                    GROUP BY ticker
                """), {"tickers": list(tickers)}).fetchall()
        except Exception as exc:
            log.debug("Earnings rotation lookup failed: {e}", e=str(exc))
            return tickers
        last = {r[0]: r[1] for r in rows if r[1] is not None}
        position = {t: i for i, t in enumerate(tickers)}
        return sorted(
            tickers,
            key=lambda t: (t in last, last[t].timestamp() if t in last else 0.0, position[t]),
        )

    def _latest_success_values(
        self,
        conn: Any,
        keys: list[tuple[str, date]],
    ) -> dict[tuple[str, date], float]:
        """Latest SUCCESS value per (series_id, obs_date), in ONE statement.

        One index probe per key on ``uq_raw_series_composite``
        (series_id, source_id, obs_date, pull_timestamp), walked backward so
        only the newest vintage is read. Measured on grid-svr 2026-11-04:
        0.5 ms for 5 keys of EARNINGS:AMD, whose two series hold ~21k
        duplicate vintages; a DISTINCT ON over the same two series took
        5.4 s because it visits every vintage.
        """
        if not keys:
            return {}
        rows = conn.execute(text("""
            SELECT k.sid, k.od, l.value
            FROM unnest(CAST(:sids AS text[]), CAST(:ods AS date[])) AS k(sid, od)
            CROSS JOIN LATERAL (
                SELECT r.value
                FROM raw_series r
                WHERE r.series_id = k.sid
                  AND r.obs_date = k.od
                  AND r.source_id = :src
                  AND r.pull_status = 'SUCCESS'
                ORDER BY r.pull_timestamp DESC
                LIMIT 1
            ) l
        """), {
            "sids": [k[0] for k in keys],
            "ods": [k[1] for k in keys],
            "src": self.source_id,
        }).fetchall()
        return {(r[0], r[1]): r[2] for r in rows if r[2] is not None}

    def _store_raw_vintages(
        self,
        ticker: str,
        records: list[dict[str, Any]],
    ) -> tuple[int, int]:
        """Write EARNINGS:{ticker}:estimate|actual rows that are new or changed.

        A row is written only when its (series_id, obs_date) has no SUCCESS
        row yet, or the latest SUCCESS value differs (a revised estimate or
        actual is a new PIT vintage and is still recorded). Re-pulling an
        unchanged history writes nothing — before this, every pull appended
        every row again (EARNINGS:AMD:actual held 10,349 rows for 27 dates).

        The per-ticker transaction-scoped advisory lock serialises an
        orphaned SmartScheduler thread with a concurrent retry for the same
        ticker, so both cannot read "absent" and insert the same vintage.

        Returns:
            (rows_inserted, rows_unchanged) — counted only after commit.
        """
        candidates: list[tuple[str, date, float]] = []
        for rec in records:
            if rec["eps_est"] is not None:
                candidates.append((f"EARNINGS:{ticker}:estimate", rec["earn_date"], rec["eps_est"]))
            if rec["eps_act"] is not None:
                candidates.append((f"EARNINGS:{ticker}:actual", rec["earn_date"], rec["eps_act"]))
        if not candidates:
            return 0, 0

        inserted = 0
        unchanged = 0
        with self.engine.begin() as conn:
            self._file_advisory_lock(conn, "EARNINGS_RAW", ticker)
            latest = self._latest_success_values(conn, [(c[0], c[1]) for c in candidates])
            for series_id, obs_date, value in candidates:
                prev = latest.get((series_id, obs_date))
                if prev is not None and _same_value(prev, value):
                    unchanged += 1
                    continue
                self._insert_raw(conn, series_id=series_id, obs_date=obs_date, value=value)
                inserted += 1
        return inserted, unchanged

    def pull_ticker_earnings(self, ticker: str) -> dict[str, Any]:
        """Pull earnings data for a single ticker.

        Fetches both .earnings_dates (for upcoming/recent dates with
        EPS estimates/actuals) and .calendar (for revenue estimates).

        Returns:
            dict with ticker, status, errors and committed write counts:
            ``rows_inserted`` (earnings_calendar upserts + raw_series rows,
            the scheduler's write count), ``calendar_rows_upserted``,
            ``raw_series_inserted`` and ``raw_series_unchanged`` (vintages
            skipped because the latest stored value is identical).
        """
        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "rows_updated": 0,
            "calendar_rows_upserted": 0,
            "raw_series_inserted": 0,
            "raw_series_unchanged": 0,
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

            records: list[dict[str, Any]] = []
            if earnings_dates is not None and not earnings_dates.empty:
                records = _dedupe_earnings_rows(earnings_dates)
                if len(records) < len(earnings_dates):
                    log.debug(
                        "Earnings {t}: {n} yfinance rows collapsed to {m} dates",
                        t=ticker, n=len(earnings_dates), m=len(records),
                    )

            if records:
                upserted = 0
                with self.engine.begin() as conn:
                    for rec in records:
                        eps_est = rec["eps_est"]
                        eps_act = rec["eps_act"]
                        surprise_pct_raw = rec["surprise_pct"]

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
                            "edate": rec["earn_date"],
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
                        upserted += 1
                # Counted only once the transaction has committed.
                result["calendar_rows_upserted"] = upserted

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

            # ── Also store as raw_series for PIT tracking (new/changed only) ──
            if records:
                inserted, unchanged = self._store_raw_vintages(ticker, records)
                result["raw_series_inserted"] = inserted
                result["raw_series_unchanged"] = unchanged

        except Exception as exc:
            log.error("Earnings pull failed for {t}: {err}", t=ticker, err=str(exc))
            result["status"] = "FAILED"
            result["errors"].append(str(exc))

        result["rows_inserted"] = result["calendar_rows_upserted"] + result["raw_series_inserted"]
        return result

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        should_continue: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Pull earnings data for all watchlist tickers.

        Parameters:
            ticker_list: Override list; defaults to watchlist tickers
                (ordered least-recently-refreshed first, so a budget-limited
                run resumes where the previous one stopped).
            should_continue: Optional cooperative-budget check polled
                BETWEEN tickers (SmartScheduler._run_puller and
                hermes_fixers._retry_source wire one automatically). When it
                returns False the pull stops before the next ticker instead
                of being orphaned by the scheduler's hard join timeout.

        Returns:
            - should_continue is None: list[dict], one result per ticker
              (unchanged legacy shape).
            - should_continue given: {"status": "SUCCESS" | "PARTIAL" |
              "SKIPPED", "stopped_by_budget": bool, "results": [...],
              "tickers_not_attempted": [...], "rows_inserted": int,
              "raw_series_inserted": int, "raw_series_unchanged": int}.
              A budget stop is PARTIAL with honest committed counts; the
              unattempted tickers are listed, never reported as checked.
        """
        global _BUDGET_STOP_WARNED

        if ticker_list is None:
            ticker_list = self._order_least_recently_pulled(self._get_watchlist_tickers())

        if not ticker_list:
            log.warning("No tickers for earnings calendar pull")
            if should_continue is None:
                return []
            return {
                "status": "SKIPPED",
                "skipped_reason": "no tickers for earnings calendar pull",
                "stopped_by_budget": False,
                "results": [],
                "tickers_not_attempted": [],
                "rows_inserted": 0,
                "raw_series_inserted": 0,
                "raw_series_unchanged": 0,
            }

        log.info(
            "Starting earnings calendar pull — {n} tickers",
            n=len(ticker_list),
        )
        results: list[dict[str, Any]] = []
        stopped_by_budget = False
        for ticker in ticker_list:
            if should_continue is not None and not should_continue():
                stopped_by_budget = True
                log_fn = log.info if _BUDGET_STOP_WARNED else log.warning
                _BUDGET_STOP_WARNED = True
                log_fn(
                    "Earnings calendar pull: time budget reached after {n}/{total} "
                    "tickers — returning PARTIAL, the rest go first next run",
                    n=len(results), total=len(ticker_list),
                )
                break
            res = self.pull_ticker_earnings(ticker)
            results.append(res)

        ok = sum(1 for r in results if r["status"] == "SUCCESS")
        raw_inserted = sum(r.get("raw_series_inserted", 0) for r in results)
        raw_unchanged = sum(r.get("raw_series_unchanged", 0) for r in results)
        log.info(
            "Earnings calendar pull complete — {ok}/{total} succeeded, "
            "{ins} raw_series vintages written, {same} unchanged skipped",
            ok=ok, total=len(ticker_list), ins=raw_inserted, same=raw_unchanged,
        )
        if should_continue is None:
            return results

        return {
            "status": "PARTIAL" if stopped_by_budget else "SUCCESS",
            "stopped_by_budget": stopped_by_budget,
            "results": results,
            "tickers_not_attempted": list(ticker_list[len(results):]),
            "rows_inserted": sum(r["rows_inserted"] for r in results),
            "raw_series_inserted": raw_inserted,
            "raw_series_unchanged": raw_unchanged,
        }


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
