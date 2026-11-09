"""
Tiingo Fundamentals Puller — daily market cap, PE, PB, enterprise value.

Pulls daily fundamental data from Tiingo and stores each metric as a
separate feature in raw_series, following GRID's {ticker}_{metric} naming.

Available fields per ticker per day:
  - marketCap
  - enterpriseVal
  - peRatio
  - pbRatio
  - trailingPEG1Y

Derived fields we compute and store:
  - shares_outstanding (marketCap / price)
  - shares_30d_change (rolling % change in shares)

DOW 30 available on free/power tier.
All tickers on commercial tier.
"""

from __future__ import annotations

import os
import math
import time
from collections.abc import Callable
from datetime import date, timedelta
from typing import Any

import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import text

from ingestion.base import BasePuller

_TIINGO_API_KEY = os.getenv("TIINGO_API_KEY", "")
_BASE_URL = "https://api.tiingo.com"
_RATE_LIMIT_DELAY = 0.3

# Full-history start for a ticker with nothing stored yet.
_BACKFILL_START = date(2020, 1, 1)
# Incremental pulls re-request this many days before the latest stored date
# so holidays, late prints and a missed day are still covered; existing rows
# are skipped by the insert's NOT EXISTS guard.
_INCREMENTAL_LOOKBACK_DAYS = 7

# Fields from Tiingo daily fundamentals → GRID feature suffix
_FIELD_MAP: dict[str, str] = {
    "marketCap": "market_cap",
    "enterpriseVal": "enterprise_value",
    "peRatio": "pe_ratio",
    "pbRatio": "pb_ratio",
    "trailingPEG1Y": "peg_ratio",
}


# Only these locally authored diagnostics may cross the persistence boundary.
_SAFE_LOCAL_ERRORS = (
    "Tiingo daily response must be a list",
    "Malformed Tiingo daily record",
    "Invalid Tiingo fundamental value",
    "Unknown affected row count",
)


# Fixed phrase in Tiingo's 400 body when the plan does not cover a ticker
# ("Error: Free and Power plans are limited to the DOW 30. ..."). The body is
# matched against this literal only; it is never stored or logged.
_PLAN_LIMIT_MARKER = "limited to the DOW 30"
_PLAN_LIMIT_BODY_PREFIX = 512


def _is_plan_limited(exc: Exception) -> bool:
    """True when Tiingo refused a ticker because the plan does not cover it.

    requests' HTTPError text never includes the response body, so the old
    ``"Free" in str(e)`` check never matched: on 2026-11-07 89 of the 138
    default tickers came back 400 with the DOW-30 plan notice and every run
    was reported PARTIAL ("Tiingo HTTP request failed (status 400)").
    """
    if "400" in str(exc) and "Free" in str(exc):
        return True
    response = getattr(exc, "response", None)
    if getattr(response, "status_code", None) != 400:
        return False
    try:
        body = response.text
    except (AttributeError, UnicodeError, ValueError):
        return False
    return type(body) is str and _PLAN_LIMIT_MARKER in body[:_PLAN_LIMIT_BODY_PREFIX]


def _safe_failure_message(exc: Exception) -> str:
    """Exclude URLs, headers, bodies and nested exception text from results."""
    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        status = getattr(response, "status_code", None)
        if type(status) is int and 100 <= status <= 599:
            return f"Tiingo HTTP request failed (status {status})"
        return "Tiingo HTTP request failed"
    if (
        type(exc) is ValueError
        and len(exc.args) == 1
        and type(exc.args[0]) is str
        and exc.args[0] in _SAFE_LOCAL_ERRORS
    ):
        return exc.args[0]
    return "Tiingo fundamentals pull failed"


class TiingoFundamentalsPuller(BasePuller):
    """Pulls daily fundamental data from Tiingo into raw_series."""

    SOURCE_NAME: str = "TIINGO_FUNDAMENTALS"
    SOURCE_CONFIG: dict[str, Any] = {
        "base_url": "https://api.tiingo.com/tiingo/fundamentals",
        "cost_tier": "PAID",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 7,
    }

    def pull_ticker(
        self,
        ticker: str,
        start_date: str | date = "2020-01-01",
        end_date: str | date | None = None,
    ) -> dict[str, Any]:
        """Pull daily fundamentals for a single ticker."""
        if not _TIINGO_API_KEY:
            return {"ticker": ticker, "status": "FAILED", "errors": ["No TIINGO_API_KEY"], "rows_inserted": 0}

        if end_date is None:
            end_date = date.today()

        result: dict[str, Any] = {
            "ticker": ticker,
            "rows_inserted": 0,
            "status": "SUCCESS",
            "errors": [],
        }

        transaction_started = False
        try:
            url = f"{_BASE_URL}/tiingo/fundamentals/{ticker}/daily"
            params = {
                "startDate": str(start_date),
                "endDate": str(end_date),
            }

            headers = {"Authorization": f"Token {_TIINGO_API_KEY}"}
            r = requests.get(url, params=params, headers=headers, timeout=30)
            r.raise_for_status()
            data = r.json()

            if type(data) is not list:
                raise ValueError("Tiingo daily response must be a list")
            if data == []:
                result["status"] = "NO_NEW_DATA"
                return result

            rows_inserted = 0

            transaction_started = True
            with self.engine.begin() as conn:
                for row in data:
                    if not isinstance(row, dict) or "date" not in row or not any(k in row and row[k] is not None for k in _FIELD_MAP):
                        raise ValueError("Malformed Tiingo daily record")
                    obs_date = pd.to_datetime(row["date"]).date()

                    for tiingo_field, grid_suffix in _FIELD_MAP.items():
                        value = row.get(tiingo_field)
                        if value is None:
                            continue
                        if isinstance(value, bool) or not math.isfinite(float(value)):
                            raise ValueError("Invalid Tiingo fundamental value")

                        series_id = f"TIINGO_FUND:{ticker}:{grid_suffix}"
                        f"{ticker.lower()}_{grid_suffix}"

                        res = conn.execute(
                            text("""
                                INSERT INTO raw_series (source_id, series_id, obs_date, value, pull_status, pull_timestamp)
                                SELECT :sid, :series, :obs, :val, 'SUCCESS', NOW()
                                WHERE NOT EXISTS (
                                    SELECT 1 FROM raw_series
                                    WHERE source_id = :sid
                                      AND series_id = :series
                                      AND obs_date = :obs
                                      AND pull_status = 'SUCCESS'
                                )
                            """),
                            {
                                "sid": self.source_id,
                                "series": series_id,
                                "obs": obs_date,
                                "val": float(value),
                            },
                        )
                        if type(res.rowcount) is not int or res.rowcount < 0:
                            raise ValueError("Unknown affected row count")
                        rows_inserted += res.rowcount

            result["rows_inserted"] = rows_inserted
            log.info("Tiingo fundamentals {t}: {n} rows", t=ticker, n=rows_inserted)

        except requests.exceptions.HTTPError as e:
            result["rows_inserted"] = None if transaction_started else 0
            if _is_plan_limited(e):
                result["status"] = "PAYWALLED"
                result["errors"].append("Free tier: DOW 30 only")
            else:
                result["status"] = "FAILED"
                result["errors"].append(_safe_failure_message(e))
        except Exception as e:
            result["rows_inserted"] = None if transaction_started else 0
            result["status"] = "FAILED"
            result["errors"].append(_safe_failure_message(e))

        return result

    def _incremental_start(self, ticker: str) -> date:
        """Start date for an incremental pull of one ticker.

        Before 2026-11-04 every scheduled run re-requested the full history
        from 2020-01-01 and ran ~7,500 per-row NOT EXISTS inserts per ticker
        that all wrote 0 rows (5-13 s per ticker), so the DOW-30 universe
        overran SmartScheduler's 120 s timeout every day. Resume from the
        latest stored market_cap date minus a short lookback instead; a
        ticker with nothing stored still backfills from 2020-01-01. One
        point lookup per ticker (``MAX(obs_date)`` on the series' index).
        """
        latest = self._get_latest_date(f"TIINGO_FUND:{ticker}:market_cap")
        if latest is None:
            return _BACKFILL_START
        return max(_BACKFILL_START, latest - timedelta(days=_INCREMENTAL_LOOKBACK_DAYS))

    def pull_all(
        self,
        ticker_list: list[str] | None = None,
        start_date: str | date | None = None,
        should_continue: Callable[[], bool] | None = None,
    ) -> list[dict[str, Any]] | dict[str, Any]:
        """Pull fundamentals for multiple tickers.

        Parameters:
            ticker_list: Override list; defaults to DOW 30 + ``*_full`` tickers.
            start_date: Explicit start for every ticker (hermes repair passes a
                bounded window). None (the scheduled default) pulls each
                ticker incrementally from its latest stored date — see
                ``_incremental_start``.
            should_continue: Optional cooperative-budget check polled between
                tickers (SmartScheduler wires one automatically). When it
                returns False the pull stops before the next ticker instead
                of being orphaned by the scheduler's hard join timeout.

        Returns:
            should_continue is None: list[dict], one result per ticker
            (legacy shape). should_continue given: {"status": "SUCCESS" |
            "PARTIAL", "stopped_by_budget": bool, "results": [...],
            "tickers_not_attempted": [...]}; the row count is carried by the
            per-ticker results, so an unknown leaf count stays unknown.
        """
        default_universe = ticker_list is None
        if ticker_list is None:
            # Default: DOW 30 + any ticker with _full data
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT DISTINCT UPPER(REPLACE(fr.name, '_full', ''))
                    FROM feature_registry fr
                    JOIN resolved_series rs ON fr.id = rs.feature_id
                    WHERE fr.name LIKE '%_full'
                    GROUP BY fr.name HAVING COUNT(*) > 200
                """)).fetchall()
            ticker_list = [r[0] for r in rows]

        results = []
        succeeded = 0
        paywalled = 0
        stopped_by_budget = False

        for ticker in ticker_list:
            if should_continue is not None and not should_continue():
                stopped_by_budget = True
                log.warning(
                    "Tiingo fundamentals bulk: time budget reached after {n}/{t} "
                    "tickers — returning PARTIAL",
                    n=len(results), t=len(ticker_list),
                )
                break
            ticker_start = start_date if start_date is not None else self._incremental_start(ticker)
            res = self.pull_ticker(ticker, start_date=ticker_start)
            results.append(res)
            if res["status"] == "SUCCESS":
                succeeded += 1
            elif res["status"] == "PAYWALLED":
                paywalled += 1
            time.sleep(_RATE_LIMIT_DELAY)

        log.info(
            "Tiingo fundamentals bulk: {s} ok, {p} paywalled, {f} failed/no-new-data out of {t}",
            s=succeeded, p=paywalled,
            f=len(results) - succeeded - paywalled,
            t=len(ticker_list),
        )
        not_attempted = list(ticker_list[len(results):])
        if default_universe and paywalled:
            # The default universe is every *_full ticker; the ones the plan
            # does not cover are outside this pull's scope, not failed
            # coverage. They are still requested each run, so a plan upgrade
            # starts filling them with no code change. An explicit
            # ticker_list keeps its PAYWALLED results (the caller asked).
            results = [r for r in results if r["status"] != "PAYWALLED"]
        if should_continue is None:
            return results
        return {
            "status": "PARTIAL" if stopped_by_budget else "SUCCESS",
            "stopped_by_budget": stopped_by_budget,
            "results": results,
            "tickers_not_attempted": not_attempted,
        }
